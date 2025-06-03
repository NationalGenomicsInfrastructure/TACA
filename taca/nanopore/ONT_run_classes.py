import csv
import glob
import json
import logging
import os
import re
import shutil
import subprocess
from datetime import datetime

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
        self.transfer_details = CONFIG["nanopore_analysis"]["transfer_details"]
        self.minknow_reports_dir = CONFIG["nanopore_analysis"]["minknow_reports_dir"]
        self.toulligqc_reports_dir = CONFIG["nanopore_analysis"][
            "toulligqc_reports_dir"
        ]
        self.toulligqc_executable = CONFIG["nanopore_analysis"]["toulligqc_executable"]

        # Get run-type and instrument-specific attributes from config
        _conf = CONFIG["nanopore_analysis"]["instruments"][self.instrument]
        self.transfer_log = _conf["transfer_log"]
        self.archive_dir = _conf["archive_dir"]
        self.metadata_dir = _conf["metadata_dir"]
        self.destination = _conf["destination"]

        # Get DB
        self.db = NanoporeRunsConnection(CONFIG["statusdb"], dbname="nanopore_runs")

        # Define paths of rsync indicator files
        self.transfer_indicator = os.path.join(self.run_abspath, ".rsync_ongoing")
        self.rsync_exit_file = os.path.join(self.run_abspath, ".rsync_exit_status")

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

        # Raw seq files
        assert any(
            [
                dir in os.listdir(self.run_abspath)
                for dir in ["pod5", "pod5_pass", "fast5", "fast5_pass"]
            ]
        )

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

    def toulligqc_report(self):
        """Generate a QC report for the run using ToulligQC and publish it to GenStat."""

        report_dir_name = "toulligqc_report"
        exit_code_path = os.path.join(self.run_abspath, report_dir_name, "exit_code")

        # Check for previous exit code
        if os.path.exists(exit_code_path):
            with open(exit_code_path) as f:
                exit_code = int(f.read().strip())
            if exit_code == 0:
                logger.info(f"{self.run_name}: ToulligQC report already generated.")
            else:
                logger.error(
                    f"{self.run_name}: ToulligQC report generation failed with exit code {exit_code}, skipping."
                )
                raise AssertionError()

        else:
            # Run ToulligQC

            # Get sequencing summary file
            glob_summary = glob.glob(f"{self.run_abspath}/sequencing_summary*.txt")
            assert len(glob_summary) == 1, f"Found {len(glob_summary)} summary files"
            summary = glob_summary[0]

            # Determine the format of the raw sequencing data, sorted by preference
            raw_data_dir_options = [
                "pod5_pass",
                "pod5",
                "fast5_pass",
                "fast5",
            ]
            raw_data_path = None
            for raw_data_dir_option in raw_data_dir_options:
                if os.path.exists(f"{self.run_abspath}/{raw_data_dir_option}"):
                    raw_data_path = f"{self.run_abspath}/{raw_data_dir_option}"
                    raw_data_format = (
                        "pod5" if "pod5" in raw_data_dir_option else "fast5"
                    )
                    break
            if raw_data_path is None:
                raise AssertionError(f"No seq data found in {self.run_abspath}")

            # Load samplesheet, if any
            ss_glob = glob.glob(f"{self.run_abspath}/sample_sheet*.csv")
            if len(ss_glob) == 0:
                samplesheet = None
            elif len(ss_glob) > 1:
                # If multiple samplesheets, use latest one
                samplesheet = ss_glob.sort()[-1]
                logger.info(
                    f"{self.run_name}: Multiple samplesheets found, using latest '{samplesheet}'"
                )
            else:
                samplesheet = ss_glob[0]

            # Determine barcodes
            if samplesheet:
                ss_df = pd.read_csv(samplesheet)
                if "barcode" in ss_df.columns:
                    ss_barcodes = list(ss_df["barcode"].unique())
                    ss_barcodes.sort()
                    barcode_nums = [int(bc[-2:]) for bc in ss_barcodes]
                    # If barcodes are numbered sequentially, write arg as range
                    if barcode_nums == list(
                        range(barcode_nums[0], barcode_nums[-1] + 1)
                    ):
                        barcodes_arg = f"{ss_barcodes[0]}:{ss_barcodes[-1]}"
                    else:
                        barcodes_arg = ":".join(ss_barcodes)
                else:
                    ss_barcodes = None

            command_args = {
                "--sequencing-summary-source": summary,
                f"--{raw_data_format}-source": raw_data_path,
                "--output-directory": self.run_abspath,
                "--report-name": report_dir_name,
            }
            if samplesheet and ss_barcodes:
                command_args["--barcoding"] = ""
                command_args["--samplesheet"] = samplesheet
                command_args["--barcodes"] = barcodes_arg

            # Build command list
            command_list = [self.toulligqc_executable]
            for k, v in command_args.items():
                command_list.append(k)
                if v:
                    command_list.append(v)

            # Run the command
            # Small enough to wait for, should be done in 1-5 minutes
            process = subprocess.run(command_list)

            # Dump exit status
            with open(exit_code_path, "w") as f:
                f.write(str(process.returncode))

            # Check if the command was successful
            if process.returncode == 0:
                logger.info(
                    f"{self.run_name}: ToulligQC report generated successfully."
                )
            else:
                raise subprocess.CalledProcessError(process.returncode, command_list)

        # Transfer the ToulligQC .html report file to ngi-internal, renaming it to the full run ID. Requires password-free SSH access.
        logger.info(
            f"{self.run_name}: Transferring ToulligQC report to ngi-internal..."
        )
        report_src_path = self.get_file(f"/{report_dir_name}/report.html")
        report_dest_path = os.path.join(
            self.toulligqc_reports_dir,
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

    def transfer(self):
        """Transfer dir to destination specified in config file via rsync"""

        logger.info(
            f"{self.run_name}: Transferring to {self.transfer_details['host']}..."
        )

        command = (
            "rsync"
            + " -aq"  # archive, quiet
            + " --size-only"  # Only transfer files that are different sizes, prevents overwriting other syncs
            + f" --chown={self.transfer_details['owner']}"
            + f" --chmod={self.transfer_details['permissions']}"
            + f" {self.run_abspath}"
            + f" {self.transfer_details['user']}@{self.transfer_details['host']}:{self.destination}"
            + f"; echo $? > {os.path.join(self.run_abspath, '.rsync_exit_status')}"
        )

        p_handle = subprocess.Popen(command, stdout=subprocess.PIPE, shell=True)
        """The PID of p_handle will map to a background subshell
        calling bash to run the command. The rsync process itself
        will be a child process of that subshell.

        'ps -ef | grep <pid>' will show both the invoked subprocess
        and its children.
        """
        logger.info(
            "Transfer to analysis cluster "
            f"started for run {self.run_name} on {datetime.now()} "
            f"with PID {p_handle.pid} and command '{p_handle.args}'."
        )
        self._make_transfer_indicator(str(p_handle.pid))

    def _make_transfer_indicator(self, contents: str = ""):
        with open(self.transfer_indicator, "w") as f:
            f.write(contents)

    @property
    def rsync_pid(self) -> str | None:
        if not os.path.exists(self.transfer_indicator):
            return None

        with open(self.transfer_indicator) as f:
            contents = f.read()

        if contents == "":
            return None

        assert contents.isdigit()
        return contents

    def remove_transfer_indicator(self):
        os.remove(self.transfer_indicator)

    def update_transfer_log(self):
        try:
            with open(self.transfer_log, "a") as f:
                tsv_writer = csv.writer(f, delimiter="\t")
                tsv_writer.writerow([self.run_name, str(datetime.now())])
        except OSError:
            msg = f"{self.run_name}: Could not update the transfer logfile {self.transfer_details['transfer_log']}"
            logger.error(msg)
            raise OSError(msg)

    @property
    def transfer_status(self):
        if self.in_transfer_log:
            return "transferred"
        elif self.rsync_complete:
            if self.rsync_successful:
                return "rsync done"
            else:
                return "rsync failed"
        elif self.transfer_ongoing:
            return "ongoing"
        else:
            return "not started"

    @property
    def in_transfer_log(self):
        with open(self.transfer_log) as transfer_log:
            for row in transfer_log.readlines():
                if row.startswith(self.run_name):
                    return True
        return False

    @property
    def transfer_ongoing(self):
        return os.path.isfile(self.transfer_indicator)

    @property
    def rsync_complete(self):
        return os.path.isfile(self.rsync_exit_file)

    @property
    def rsync_successful(self):
        with open(self.rsync_exit_file) as rsync_exit_file:
            rsync_exit_status = int(rsync_exit_file.read().strip())
        if rsync_exit_status == 0:
            return True
        else:
            return False

    def archive_run(self):
        """Move directory to nosync."""
        src = self.run_abspath
        dst = os.path.join(self.run_abspath, os.pardir, "nosync")

        logger.info(f"{self.run_name}: Moving run from {src} to {dst}...")
        shutil.move(src, dst)
