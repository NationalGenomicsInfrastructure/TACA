import csv
import glob
import json
import logging
import os
import re
import shutil
import subprocess

import pandas as pd

from taca.utils.config import CONFIG
from taca.utils.statusdb import NanoporeRunsConnection
from taca.utils.transfer import RsyncError

logger = logging.getLogger(__name__)

ONT_RUN_PATTERN = re.compile(
    r"^(\d{8})_(\d{4})_([0-9a-zA-Z]+)_([0-9a-zA-Z]+)_([0-9a-zA-Z]+)$"
)


class ONT_run:
    """General Nanopore run.

    Expects instantiation from absolute path of run directory on preprocessing server.
    """

    def __init__(self, run_abspath: str):
        # Parse args
        self.run_abspath = run_abspath

        # Parse run name
        self.run_name = os.path.basename(run_abspath)
        assert re.match(ONT_RUN_PATTERN, self.run_name), (
            f"Run {self.run_name} doesn't look like a run dir"
        )

        # Parse MinKNOW sample and experiment name
        with open(self.get_file("/run_path.txt")) as stream:
            self.experiment_name, self.sample_name, _ = stream.read().split("/")

        # Get info from run name
        (
            self.date,
            self.time,
            self.position,
            self.flowcell_id,
            self.run_hash,
        ) = self.run_name.split("_")

        # Get instrument
        # - For PromethION, the run position will be one of "1A", "2A", ..., "3G".
        # - For MinION, the position will be the instrument ID e.g. "MN19414".
        self.instrument = "promethion" if len(self.position) == 2 else "minion"

        # Get general attributes from config
        self.minknow_reports_dir = CONFIG["nanopore_analysis"]["minknow_reports_dir"]

        # Get run-type and instrument-specific attributes from config
        _conf = CONFIG["nanopore_analysis"]["instruments"][self.instrument]
        self.archive_dir = _conf["archive_dir"]
        self.metadata_dir = _conf["metadata_dir"]

        # Get DB
        self.db = NanoporeRunsConnection(CONFIG["statusdb"], dbname="nanopore_runs")

    def has_file(self, content_pattern: str) -> bool:
        """Checks within run dir for pattern, e.g. '/report*.json', returns bool."""
        query_path = self.run_abspath + content_pattern
        query_glob = glob.glob(query_path)

        if len(query_glob) > 0:
            return True
        else:
            return False

    def get_file(self, content_pattern) -> str:
        """Checks within run dir for pattern, e.g. '/report*.json', returns file abspath as string."""
        query_path = self.run_abspath + content_pattern
        query_glob = glob.glob(query_path)

        if len(query_glob) == 1:
            return query_glob[0]
        elif len(query_glob) == 0:
            raise AssertionError(f"Could not find {query_path}")
        else:
            raise AssertionError(f"Found multiple instances of {query_path}")

    @property
    def is_synced(self) -> bool:
        return self.has_file("/.sync_finished")

    def assert_contents(self):
        """Checklist function to assure run has all files necessary to proceed with processing"""

        # Completion indicators
        assert self.has_file("/.sync_finished")
        assert self.has_file("/final_summary*.txt")

        # NGI files from instrument
        assert self.has_file("/pore_count_history.csv")
        assert self.has_file("/run_path.txt")

        # MinKNOW reports
        assert self.has_file("/report_*.json")
        assert self.has_file("/report_*.html")

        # MinKNOW auxillary files
        assert self.has_file("/pore_activity*.csv")

    def touch_db_entry(self):
        """Check run vs statusdb. Create entry if there is none."""

        if not self.db.check_run_exists(self):
            logger.info(
                f"{self.run_name}: Run does not exist in the database, creating entry for ongoing run."
            )

            run_path_file = os.path.join(self.run_abspath, "run_path.txt")
            assert os.path.isfile(run_path_file), f"Couldn't find {run_path_file}"

            pore_count_history_file = os.path.join(
                self.run_abspath, "pore_count_history.csv"
            )
            assert os.path.isfile(pore_count_history_file), (
                f"Couldn't find {pore_count_history_file}"
            )

            self.db.create_ongoing_run(self, run_path_file, pore_count_history_file)
            logger.info(
                f"{self.run_name}: Successfully created database entry for ongoing run."
            )

    def update_db_entry(self, force_update=False):
        """Check run vs statusdb. Create or update run entry."""

        # If no run document exists in the database, create an ongoing run document
        self.touch_db_entry()

        # If the run document is marked as "ongoing" or database is being manually updated
        if self.db.check_run_status(self) == "ongoing" or force_update is True:
            logger.info(
                f"{self.run_name}: Run exists in the database with run status: {self.db.check_run_status(self)}."
            )

            logger.info(f"{self.run_name}: Updating...")

            # Instantiate json (dict) to update the db with
            db_update = {}

            # Parse run path
            db_update["run_path"] = (
                open(f"{self.run_abspath}/run_path.txt").read().strip()
            )

            # Parse pore counts
            pore_counts = []
            with open(f"{self.run_abspath}/pore_count_history.csv") as stream:
                for line in csv.DictReader(stream):
                    pore_counts.append(line)
            db_update["pore_count_history"] = pore_counts

            # Parse report_*.json
            self.parse_minknow_json(db_update)

            # Parse pore_activity_*.csv
            self.parse_pore_activity(db_update)

            # Update the DB entry
            self.db.finish_ongoing_run(self, db_update)

        # If the run document is marked as "finished"
        elif self.db.check_run_status(self) == "finished":
            logger.info(
                f"Run {self.run_name} exists in the database as an finished run, do nothing."
            )

    def parse_pore_activity(self, db_update):
        logger.info(f"{self.run_name}: Parsing pore activity...")

        pore_activity = {}

        # Use pandas to pivot the data into a more manipulable dataframe
        df = pd.read_csv(self.get_file("/pore_activity_*.csv"))
        df.sort_values(by="Experiment Time (minutes)", inplace=True)
        df = df.pivot_table(
            "State Time (samples)", "Experiment Time (minutes)", "Channel State"
        )

        # Use pore counts to calculate new metrics
        df["all"] = df.sum(axis=1)
        df["healthy"] = df.strand + df.adapter + df.pore
        df["productive"] = df.strand + df.adapter

        df["health"] = df["healthy"] / df["all"]
        df["efficacy"] = df["productive"] / df["healthy"]

        # Look at peaks within 1st hour of the run and define some metrics
        df_h1 = df[0:60]
        pore_activity["peak_pore_health_pc"] = round(
            100
            * float(df_h1.loc[df_h1.health == df_h1.health.max(), "health"].values[0]),
            2,
        )
        if not all(df["efficacy"].isna()):
            pore_activity["peak_pore_efficacy_pc"] = round(
                100
                * float(
                    df_h1.loc[
                        df_h1.efficacy == df_h1.efficacy.max(), "efficacy"
                    ].values[0]
                ),
                2,
            )
        else:
            pore_activity["peak_pore_efficacy_pc"] = None

        # Calculate the T90
        # -- Get the cumulative sum of all productive pores
        df["cum_productive"] = df["productive"].cumsum()
        # -- Find the timepoint (h) at which the cumulative sum >= 90% of the absolute sum
        if not df["productive"].sum() == 0:
            t90_min = df[df["cum_productive"] >= 0.9 * df["productive"].sum()].index[0]
            pore_activity["t90_h"] = round(t90_min / 60, 1)
        else:
            pore_activity["t90_h"] = None

        # Add to the db update
        db_update["pore_activity"] = pore_activity

    def parse_minknow_json(self, db_update):
        """Parse useful stuff from the MinKNOW .json report to add to CouchDB"""

        logger.info(f"{self.run_name}: Parsing report JSON...")

        dict_json_report = json.load(open(self.get_file("/report*.json")))

        # Initialize return dict
        parsed_data = {}

        # These sections of the .json can be added as they are
        for section in [
            "host",
            "protocol_run_info",
            "user_messages",
        ]:
            parsed_data[section] = dict_json_report[section]

        # Only parse the last acquisition section, which contains the actual sequencing data
        seq_metadata = dict_json_report["acquisitions"][-1]
        seq_metadata_trimmed = {}

        # -- Run info subsection
        seq_metadata_trimmed["acquisition_run_info"] = {}

        seq_metadata_trimmed["acquisition_run_info"]["yield_summary"] = seq_metadata[
            "acquisition_run_info"
        ]["yield_summary"]

        # -- Run output subsection
        seq_metadata_trimmed["acquisition_output"] = []
        for section in seq_metadata["acquisition_output"]:
            if "type" not in section.keys() or section["type"] in [
                "AllData",
                "SplitByBarcode",
            ]:
                seq_metadata_trimmed["acquisition_output"].append(section)

        # -- Read length subsection
        seq_metadata_trimmed["read_length_histogram"] = seq_metadata[
            "read_length_histogram"
        ]

        # Add the trimmed acquisition section to the parsed data
        parsed_data["acquisitions"] = []
        parsed_data["acquisitions"].append(seq_metadata_trimmed)

        # Add the parsed data to the db update
        db_update.update(parsed_data)

    def copy_metadata(self):
        """Copies run dir (excluding seq data) to metadata dir"""

        src = self.run_abspath
        dst = self.metadata_dir

        exclude_patterns = [
            # Main seq dirs
            "**/bam*/***",
            "**/fast5*/***",
            "**/fastq*/***",
            "**/pod5*/***",
            # Any files found elsewhere
            "*.bam*",
            "*.bai*",
            "*.fast5*",
            "*.fastq*",
            "*.pod5*",
        ]

        # Build the rsync command
        command = [
            "rsync",
            "-aq",  # Archive, quiet
        ]
        for pattern in exclude_patterns:
            command.append(f"--exclude={pattern}")
        command.extend([src, dst])
        logger.info(f"Calling rsync command: {' '.join(command)}")

        try:
            subprocess.run(command, check=True)
        except subprocess.CalledProcessError as e:
            raise RsyncError(
                f"{self.run_name}: Error occurred when copying metadata from {src} to {dst}. {e}"
            )

    def copy_html_report(self):
        logger.info(f"{self.run_name}: Transferring .html report to ngi-internal...")

        # Transfer the MinKNOW .html report file to ngi-internal, renaming it to the full run ID. Requires password-free SSH access.
        report_src_path = self.get_file("/report*.html")
        report_dest_path = os.path.join(
            self.minknow_reports_dir,
            f"report_{self.run_name}.html",
        )

        command = [
            "rsync",
            "-aq",
            report_src_path,
            report_dest_path,
        ]
        logger.info(f"Calling rsync command: {' '.join(command)}")

        try:
            subprocess.run(command, check=True)
        except subprocess.CalledProcessError as e:
            raise RsyncError(
                f"{self.run_name}: An error occurred while attempting to transfer the report {report_src_path} to {report_dest_path}. {e}"
            )

    def archive_run(self):
        """Move directory to nosync."""
        src = self.run_abspath
        dst = os.path.join(self.run_abspath, os.pardir, "nosync")

        logger.info(f"{self.run_name}: Moving run from {src} to {dst}...")
        shutil.move(src, dst)
