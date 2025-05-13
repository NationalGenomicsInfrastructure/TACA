"""This is a stand-alone script run on ONT instrument computers. It transfers new ONT runs to NAS using rsync."""

__version__ = "1.0.15"

import argparse
import logging
import os
import re
import shutil
import subprocess
from datetime import datetime as dt
from glob import glob

RUN_PATTERN = re.compile(
    # Run folder name expected as yyyymmdd_HHMM_positionOrInstrument_flowCellId_randomHash
    # Flow cell names starting with "CTC" are configuration test cells and should not be included
    r"^\d{8}_\d{4}_(([1-3][A-H])|(MN19414))_(?!CTC)[A-Za-z0-9]+_[A-Za-z0-9]+$"
)


def main(args):
    """Find ONT runs and transfer them to storage.
    Archives the run when the transfer is complete."""

    # Set up logging
    logging.basicConfig(
        filename=args.log,
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    rsync_log = os.path.join(args.prom_runs, "rsync_log.txt")

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

        handle_runs(
            run_paths=run_paths,
            pore_counts=pore_counts,
            destination_nas=args.nas_runs,
            destination_miarka=args.miarka_runs,
            local_archive=args.prom_archive,
            rsync_log=rsync_log,
        )

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
    destination_nas,
    destination_miarka,
    local_archive,
    rsync_log,
):
    # Iterate over runs
    for run_path in run_paths:
        logging.info(f"{os.path.basename(run_path)}: Processing run...")

        logging.info(f"{os.path.basename(run_path)}: Dumping run path...")
        dump_path(run_path)
        logging.info(f"{os.path.basename(run_path)}: Dumping QC and MUX history...")
        dump_pore_count_history(run_path, pore_counts)

        if not sequencing_finished(run_path):
            sync_to_storage(run_path, destination_nas, destination_miarka, rsync_log)
        else:
            final_sync_to_storage(
                run_path, destination_nas, destination_miarka, local_archive, rsync_log
            )


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
            continue


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
    when the finial rsync is finished."""
    finished_indicator = ".sync_finished"
    new_file_path = os.path.join(run_path, finished_indicator)
    open(new_file_path, "w").close()
    return new_file_path


def sync_to_storage(
    run_path: str, destination_nas: str, destination_miarka: str, rsync_log: str
):
    """Sync the run to storage using rsync.
    Skip if rsync is already running on the run."""

    for remote_runs_dir in [destination_nas, destination_miarka]:
        command = [
            "run-one",
            "rsync",
            "-auv",
            "--log-file=" + rsync_log,
            run_path,
            remote_runs_dir,
        ]

        p = subprocess.Popen(command)
        logging.info(
            f"{os.path.basename(run_path)}: Initiated rsync to {remote_runs_dir} with PID {p.pid} and the following command: {command}"
        )


def final_sync_to_storage(
    run_path: str,
    destination_nas: str,
    destination_miarka: str,
    prom_archive: str,
    rsync_log: str,
):
    """Do a final sync of the run to storage, then archive it.
    Skip if rsync is already running on the run."""

    logging.info(f"{os.path.basename(run_path)}: Performing a final sync to storage...")

    syncs_done = []

    for remote_runs_dir in [destination_nas, destination_miarka]:
        command = [
            "run-one",
            "rsync",
            "-auv",
            "--log-file=" + rsync_log,
            run_path,
            destination_nas,
        ]

        p = subprocess.run(command)

        if p.returncode == 0:
            syncs_done.append(True)
        else:
            syncs_done.append(False)
            logging.info(
                f"{os.path.basename(run_path)}: Previous rsync might be running still. Skipping..."
            )
            return

    if all(syncs_done):
        logging.info(f"{run_path}: All rsyncs finished successfully, archiving...")
        finished_indicator_path = write_finished_indicator(run_path)
        for remote_runs_dir in [destination_nas, destination_miarka]:
            run_dir_dst = os.path.join(
                remote_runs_dir, os.path.basename(run_path), os.path.sep
            )
            sync_finished_indicator_command = [
                "rsync",
                finished_indicator_path,
                run_dir_dst,
            ]
            p = subprocess.run(sync_finished_indicator_command)
            if p.returncode != 0:
                logging.error(
                    f"{os.path.basename(run_path)}: Failed to sync finished indicator to {run_dir_dst}"
                )

        archive_finished_run(run_path, prom_archive)


def archive_finished_run(run_path: str, prom_archive: str):
    """Move finished run to archive (nosync)."""

    logging.info(f"{os.path.basename(run_path)}: Archiving to {prom_archive}.")
    shutil.move(run_path, prom_archive)


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
            lines = open(log_file).readlines()

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
            type = "qc"
        elif "INFO: mux_scan_result (user_messages)" in entry["category"]:
            type = "mux"
        else:
            type = "other"

        if type in ["qc", "mux"]:
            new_entry = {
                "flow_cell_id": entry["body"]["flow_cell_id"],
                "timestamp": entry["timestamp"],
                "position": entry["position"],
                "type": type,
                "num_pores": entry["body"]["num_pores"],
            }

            new_entry["total_pores"] = (
                entry["body"]["num_pores"]
                if type == "qc"
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
        if not os.path.exists(new_file_path):
            open(new_file_path, "w").close()

    return new_file_path


if __name__ == "__main__":  # pragma: no cover
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--prom_runs",
        dest="prom_runs",
        help="Path to directory where ONT runs are created by the instrument.",
    )
    parser.add_argument(
        "--exclude_dirs",
        type=lambda s: s.split(","),
        help="Comma-separated names of dirs inside prom_runs to exclude from the search.",
    )
    parser.add_argument(
        "--nas_runs",
        dest="nas_runs",
        help="Path to NAS directory to sync ONT runs to.",
    )
    parser.add_argument(
        "--miarka_runs",
        dest="miarka_runs",
        help="Path to Miarka directory to sync ONT runs to.",
    )
    parser.add_argument(
        "--prom_archive",
        dest="prom_archive",
        help="Path to local archive directory for ONT runs.",
    )
    parser.add_argument(
        "--minknow_logs",
        dest="minknow_logs",
        help="Full path to the directory containing the MinKNOW position logs.",
    )
    parser.add_argument(
        "--log",
        dest="log",
        help="Full path to the script log file.",
    )
    parser.add_argument("--version", action="version", version=__version__)
    args = parser.parse_args()

    main(args)
