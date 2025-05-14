#!/usr/bin/env python3

"""This is a stand-alone script run on ONT instrument computers to handle ONT runs.
It handles metadata file creation, syncing to storage, local archiving and cleanup.
The script is written in pure Python to avoid installing external dependencies.
"""

__version__ = "1.0.15"

import argparse
import logging
import os
import re
import shutil
import subprocess
from datetime import datetime as dt
from glob import glob
from pathlib import Path

RUN_PATTERN = re.compile(
    # Run folder name expected as yyyymmdd_HHMM_positionOrInstrument_flowCellId_randomHash
    # Flow cell names starting with "CTC" are configuration test cells and should not be included
    r"^\d{8}_\d{4}_(([1-3][A-H])|(MN19414))_(?!CTC)[A-Za-z0-9]+_[A-Za-z0-9]+$"
)


def main(args):
    """Find ONT runs and transfer them to storage.
    Archives the run when the transfer is complete."""

    # Start script
    logging.info(f"Starting script version {__version__}.")

    run_paths = find_runs(dir_to_search=args.prom_runs, exclude_dirs=args.exclude_dirs)
    positions = sorted([os.path.basename(path).split("_")[2] for path in run_paths])

    if run_paths:
        logging.info(f"Parsing instrument logs for positions {positions}...")
        position_logs = parse_position_logs(
            minknow_logs=args.minknow_logs, positions=positions
        )
        logging.info("Subsetting QC and MUX metrics...")
        pore_counts = get_pore_counts(position_logs=position_logs)

        handle_runs(run_paths=run_paths, pore_counts=pore_counts, args=args)

    delete_archived_runs(prom_archive=args.prom_archive, nas_runs=args.nas_runs)


def find_runs(dir_to_search, exclude_dirs):
    logging.info("Finding runs...")
    # Look for dirs matching run pattern 3 levels deep from source, excluding certain dirs
    run_paths = [
        path
        for path in glob(os.path.join(dir_to_search, "*", "*", "*"), recursive=True)
        if re.match(RUN_PATTERN, os.path.basename(path))
        and path.split(os.sep)[-3] not in exclude_dirs
    ]
    logging.info(f"Found {len(run_paths)} runs...")
    return run_paths


def handle_runs(
    run_paths,
    pore_counts,
    args,
):
    # Iterate over runs
    for run_path in run_paths:
        logging.info(f"{os.path.basename(run_path)}: Processing run...")

        logging.info(f"{os.path.basename(run_path)}: Dumping run path...")
        dump_path(run_path)
        logging.info(f"{os.path.basename(run_path)}: Dumping QC and MUX history...")
        dump_pore_count_history(run_path, pore_counts)

        if not sequencing_finished(run_path):
            sync_to_storage(
                run_path=run_path,
                destination=args.nas_runs,
                rsync_log=args.rsync_log,
                background=True,
            )
            sync_to_storage(
                run_path=run_path,
                destination=args.miarka_runs,
                rsync_log=args.rsync_log,
                background=True,
                settings=args.miarka_settings,
            )
        else:
            final_sync_and_archive(run_path, args)


def delete_archived_runs(prom_archive, nas_runs):
    logging.info("Finding locally archived runs...")
    # Look for dirs matching run pattern inside archive dir
    run_paths = [
        path
        for path in glob(os.path.join(prom_archive, "*"), recursive=True)
        if re.match(RUN_PATTERN, os.path.basename(path))
    ]
    logging.info(f"Found {len(run_paths)} locally archived runs...")

    preproc_archive_contents = set(
        os.listdir(os.path.join(nas_runs, "nosync"))
        + os.listdir(os.path.join(nas_runs, "nosync", "archived"))
    )
    # Iterate over runs
    for run_path in run_paths:
        logging.info(f"{os.path.basename(run_path)}: Processing archived run...")
        run_name = os.path.basename(run_path)

        if run_name in preproc_archive_contents:
            logging.info(
                f"{os.path.basename(run_path)}: Found in the preproc archive. Deleting..."
            )
            shutil.rmtree(run_path)
        else:
            logging.info(
                f"{os.path.basename(run_path)}: Not found in the preproc archive. Skipping..."
            )


def sequencing_finished(run_path: str) -> bool:
    sequencing_finished_indicator = "final_summary"
    run_dir_content = os.listdir(run_path)
    for item in run_dir_content:
        if sequencing_finished_indicator in item:
            return True
    return False


def dump_path(run_path: str):
    """Dump path <minknow_experiment_id>/<minknow_sample_id>/<minknow_run_id>
    to a file. Used for transferring info on ongoing runs to StatusDB."""
    new_file = os.path.join(run_path, "run_path.txt")
    proj, sample, run = run_path.split(os.sep)[-3:]
    path_to_write = os.path.join(proj, sample, run)
    with open(new_file, "w") as f:
        f.write(path_to_write)
    return path_to_write


def write_finished_indicator(run_path):
    """Write a hidden file to indicate
    when the final rsync is finished."""
    finished_indicator = ".sync_finished"
    new_file_path = os.path.join(run_path, finished_indicator)
    Path(new_file_path).touch(exist_ok=True)
    return new_file_path


def sync_to_storage(
    run_path: str,
    destination: str,
    rsync_log: str,
    background: bool,
    settings: list = [],
):
    """Sync the run to storage using rsync.
    Skip if rsync is already running on the run."""

    command = (
        [
            "run-one",
            "rsync",
            "-auq",
            "--log-file=" + rsync_log,
        ]
        + settings
        + [
            run_path,
            destination,
        ]
    )

    if background:
        p = subprocess.Popen(command)
        logging.info(
            f"{os.path.basename(run_path)}: Initiated rsync to {destination}"
            + f" with PID {p.pid} and the following command: '{' '.join(command)}'"
        )
    else:
        p = subprocess.run(command)
        if p.returncode == 0:
            logging.info(
                f"{os.path.basename(run_path)}: Rsync to {destination} finished successfully."
            )
            return True
        else:
            logging.error(
                f"{os.path.basename(run_path)}: Rsync to {destination} failed with error code {p.returncode}."
            )
            return False


def final_sync_and_archive(
    run_path: str,
    args,
):
    """Do a final sync of the run to storage, then archive it."""

    logging.info(f"{os.path.basename(run_path)}: Performing a final sync to storage...")

    if sync_to_storage(
        run_path=run_path,
        destination=args.nas_runs,
        rsync_log=args.rsync_log,
        background=False,
    ) and sync_to_storage(
        run_path=run_path,
        destination=args.miarka_runs,
        rsync_log=args.rsync_log,
        background=False,
        settings=args.miarka_settings,
    ):
        logging.info(
            f"{os.path.basename(run_path)}: All rsyncs finished successfully, syncing finished indicator..."
        )
    else:
        raise AssertionError(
            f"{os.path.basename(run_path)}: Rsync failed, aborting run archiving."
        )

    logging.info(f"{os.path.basename(run_path)}: Creating and syncing indicator file.")
    write_finished_indicator(run_path)

    if sync_to_storage(
        run_path=run_path,
        destination=args.nas_runs,
        rsync_log=args.rsync_log,
        background=False,
    ) and sync_to_storage(
        run_path=run_path,
        destination=args.miarka_runs,
        rsync_log=args.rsync_log,
        background=False,
        settings=args.miarka_settings,
    ):
        logging.info(
            f"{os.path.basename(run_path)}: Indicator file synced successfully, archiving run..."
        )
        archive_finished_run(run_path, args.prom_archive)
        logging.info(f"{os.path.basename(run_path)}: Finished archiving run.")
    else:
        raise AssertionError(
            f"{os.path.basename(run_path)}: Rsync failed, aborting run archiving."
        )


def archive_finished_run(run_path: str, prom_archive: str):
    """Move finished run to archive (nosync)."""

    sample_dir = os.path.dirname(run_path)
    exp_dir = os.path.dirname(sample_dir)

    logging.info(f"{os.path.basename(run_path)}: Archiving to {prom_archive}.")
    shutil.move(run_path, prom_archive)
    logging.info(f"{os.path.basename(run_path)}: Finished archiving run.")

    # Remove sample dir, if empty
    if not os.listdir(sample_dir):
        logging.info(f"Sample folder {sample_dir} is empty. Removing it.")
        os.rmdir(sample_dir)
    else:
        logging.info(
            f"Sample folder {sample_dir} is not empty ({os.listdir(sample_dir)}), leaving it."
        )
    # Remove experiment group dir, if empty
    if not os.listdir(exp_dir):
        logging.info(f"Experiment group folder {exp_dir} is empty. Removing it.")
        os.rmdir(exp_dir)
    else:
        logging.info(
            f"Experiment group folder {exp_dir} is not empty ({os.listdir(exp_dir)}), leaving it."
        )


def parse_position_logs(minknow_logs: str, positions) -> list:
    """Look through position logs and boil down into a structured list of dicts

    Example output:
    [{
        "timestamp": "2023-07-10 15:44:31.481512",
        "category": "INFO: platform_qc.report (user_messages)",
        "body": {
            "flow_cell_id": "PAO33763"
            "num_pores": "8378"
        }
    } ... ]

    """

    headers = []
    header = None
    for position in positions:
        log_files = glob(
            os.path.join(minknow_logs, position, "control_server_log-*.txt")
        )
        if not log_files:
            logging.info(f"No log files found for {position}, continuing.")
            continue

        log_files.sort()

        for log_file in log_files:
            with open(log_file) as f:
                lines = f.readlines()

            # Iterate across log lines
            for line in lines:
                if not line[0:4] == "    ":
                    # Line is log header
                    split_header = line.split(" ")
                    timestamp = " ".join(split_header[0:2])
                    category = " ".join(split_header[2:])

                    header = {
                        "position": position,
                        "timestamp": timestamp.strip(),
                        "category": category.strip(),
                    }
                    headers.append(header)

                elif header:
                    # Line is log body
                    if "body" not in header.keys():
                        body: dict = {}
                        header["body"] = body
                    key = line.split(": ")[0].strip()
                    val = ": ".join(line.split(": ")[1:]).strip()
                    header["body"][key] = val

    headers.sort(key=lambda x: x["timestamp"])
    logging.info(f"Parsed {len(headers)} log entries.")

    return headers


def get_pore_counts(position_logs: list) -> list:
    """Take the flowcell log list output by parse_position_logs() and subset to contain only QC and MUX info."""

    pore_counts = []
    for entry in position_logs:
        if "INFO: platform_qc.report (user_messages)" in entry["category"]:
            entry_type = "qc"
        elif "INFO: mux_scan_result (user_messages)" in entry["category"]:
            entry_type = "mux"
        else:
            entry_type = "other"

        if entry_type in ["qc", "mux"]:
            new_entry = {
                "flow_cell_id": entry["body"]["flow_cell_id"],
                "timestamp": entry["timestamp"],
                "position": entry["position"],
                "type": entry_type,
                "num_pores": entry["body"]["num_pores"],
            }

            new_entry["total_pores"] = (
                entry["body"]["num_pores"]
                if entry_type == "qc"
                else entry["body"]["total_pores"]
            )

            pore_counts.append(new_entry)

    logging.info(f"Subset {len(pore_counts)} QC and MUX log entries.")

    return pore_counts


def dump_pore_count_history(run: str, pore_counts: list) -> str:
    """For a recently started run, dump all QC and MUX events that the instrument remembers
    for the flow cell as a file in the run dir."""

    flowcell_id = os.path.basename(run).split("_")[-2]
    run_start_time = dt.strptime(os.path.basename(run)[0:13], "%Y%m%d_%H%M")
    log_time_pattern = "%Y-%m-%d %H:%M:%S.%f"

    new_file_path = os.path.join(run, "pore_count_history.csv")

    flowcell_pore_counts = [
        log_entry
        for log_entry in pore_counts
        if (
            log_entry["flow_cell_id"] == flowcell_id
            and dt.strptime(log_entry["timestamp"], log_time_pattern) <= run_start_time
        )
    ]

    if flowcell_pore_counts:
        flowcell_pore_counts_sorted = sorted(
            flowcell_pore_counts, key=lambda x: x["timestamp"], reverse=True
        )

        header = flowcell_pore_counts_sorted[0].keys()
        rows = [e.values() for e in flowcell_pore_counts_sorted]

        with open(new_file_path, "w") as f:
            f.write(",".join(header) + "\n")
            for row in rows:
                f.write(",".join(row) + "\n")
    else:
        # Create an empty file if there is not one already
        Path(new_file_path).touch(exist_ok=True)

    return new_file_path


if __name__ == "__main__":  # pragma: no cover
    # Parse args
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--prom_runs",
        required=True,
        type=os.path.abspath,
        help="Path to directory where ONT runs are created by the instrument.",
    )
    parser.add_argument(
        "--exclude_dirs",
        required=True,
        type=lambda s: s.split(","),
        help="Comma-separated names of dirs inside prom_runs to exclude from the search.",
    )
    parser.add_argument(
        "--nas_runs",
        required=True,
        type=os.path.abspath,
        help="Path to NAS directory to sync ONT runs to.",
    )
    parser.add_argument(
        "--miarka_runs",
        required=True,
        type=os.path.abspath,
        help="Path to Miarka directory to sync ONT runs to.",
    )
    parser.add_argument(
        "--miarka_settings",
        required=True,
        type=lambda s: s.split(" "),
        help="String of Miarka extra rsync options, e.g. '--chown=:ngi2016003 --chmod=Dg+s,g+rw'.",
    )
    parser.add_argument(
        "--prom_archive",
        required=True,
        type=os.path.abspath,
        help="Path to local archive directory for ONT runs.",
    )
    parser.add_argument(
        "--minknow_logs",
        required=True,
        type=os.path.abspath,
        help="Path to directory containing the MinKNOW position logs.",
    )
    parser.add_argument(
        "--log",
        required=True,
        type=os.path.abspath,
        help="Path to script log file.",
    )
    parser.add_argument(
        "--rsync_log",
        required=True,
        type=os.path.abspath,
        help="Path to rsync log file.",
    )
    parser.add_argument("--version", action="version", version=__version__)
    args = parser.parse_args()

    # Set up logging
    log_format = "%(asctime)s - %(levelname)s - %(message)s"
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # File handler
    file_handler = logging.FileHandler(args.log)
    file_handler.setFormatter(logging.Formatter(log_format))
    root_logger.addHandler(file_handler)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(log_format))
    root_logger.addHandler(console_handler)

    main(args)
