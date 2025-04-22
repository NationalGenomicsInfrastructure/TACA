"""Nanopore analysis methods for TACA."""

import logging
import os
import re
import traceback

from taca.nanopore.ONT_run_classes import (
    ONT_RUN_PATTERN,
    ONT_qc_run,
    ONT_run,
    ONT_user_run,
)
from taca.utils.config import CONFIG
from taca.utils.misc import send_mail

logger = logging.getLogger(__name__)


def find_run_dirs(dir_to_search: str, skip_dirs: list):
    """Takes an input dir, expected to contain ONT run dirs.
    Append all found ONT run dirs to a list and return it"""

    logger.info(f"Looking for ONT runs in {dir_to_search}...")

    found_run_dirs = []
    for found_dir in os.listdir(dir_to_search):
        if (
            os.path.isdir(os.path.join(dir_to_search, found_dir))
            and found_dir not in skip_dirs
            and re.match(ONT_RUN_PATTERN, found_dir)
        ):
            logger.info(f"Found ONT run {found_dir} in {dir_to_search}")
            found_run_dirs.append(os.path.join(dir_to_search, found_dir))

    return found_run_dirs


def send_error_mail(run_name, error: BaseException):
    email_subject = f"Run processed with errors: {run_name}"
    email_message = f"{str(error)}\n\n{traceback.format_exc()}"
    email_recipients = CONFIG["mail"]["recipients"]

    logger.warning(f"Error encountered for run {run_name}.\n{email_message}")

    send_mail(email_subject, email_message, email_recipients)

    logger.info("Sent error mail.")


def process_user_run(run: ONT_user_run):
    """This control function orchestrates the sequential execution of the ONT_user_run class methods.

    For a single ONT user run...

        - Ensure there is a StatusDB entry
        - Ensure run is fully synced from instrument
        - Ensure all necessary files to proceed with processing are present
        - Update the StatusDB entry and set to "finished"
        - Copy HTML report to GenStat
        - Generate and publish TouliggQC report
        - Copy metadata to ngi-nas-ns
        - Transfer run to cluster
        - Update transfer log
        - Archive run

    Any errors raised here-in should be sent with traceback as an email.
    """

    logger.info(f"{run.run_name}: Touching StatusDB...")
    run.touch_db_entry()

    # Is the run fully synced?
    if not run.is_synced:
        raise WaitForRun(f"{run.run_name}: Run is not fully synced, skipping.")

    # Assert all files are in place
    logger.info(f"{run.run_name}: Asserting run contents...")
    run.assert_contents()

    # Update StatusDB
    logger.info(f"{run.run_name}: Updating StatusDB...")
    run.update_db_entry()

    # Copy HTML report
    logger.info(f"{run.run_name}: Putting HTML report on GenStat...")
    run.copy_html_report()

    # Generate and publish TouliggQC report
    logger.info(f"{run.run_name}: Generating and publishing ToulligQC report...")
    run.toulligqc_report()

    # Copy metadata
    logger.info(f"{run.run_name}: Copying metadata...")
    run.copy_metadata()

    # Transfer to analysis server
    if run.transfer_status == "not started":
        logger.info(f"{run.run_name}: Starting transfer...")
        run.transfer()
    elif run.transfer_status == "ongoing":
        raise WaitForRun(f"{run.run_name}: Transfer is ongoing, skipping.")
    elif run.transfer_status == "rsync done":
        logger.info(f"{run.run_name}: Transfer complete. Archiving...")
        run.update_transfer_log()
        run.remove_transfer_indicator()
        run.archive_run()
    elif run.transfer_status == "rsync failed":
        raise AssertionError(f"{run.run_name}: Transfer failed, please investigate.")
    elif run.transfer_status == "transferred":
        logger.warning(
            f"{run.run_name}: Run is already logged as transferred, skipping."
        )
        raise WaitForRun("Run is already logged as transferred.")
    else:
        raise AssertionError(
            f"{run.run_name}: Undetermined transfer status, please investigate."
        )


def process_qc_run(run: ONT_qc_run):
    f"""This control function orchestrates the sequential execution of the {ONT_qc_run} methods.

        For a single ONT QC run...
        │
        ├── Ensure there is a database entry corresponding to an ongoing run
        ├── If not fully synced
        │   └── Skip run
        ├── Ensure all necessary files to proceed with processing are present
        ├── Update the StatusDB entry
        ├── Copy HTML report to GenStat
        ├── If there is sequencing raw data
        │   ├── If no fastq output
        │   │   └── Skip run
        │   ├── If Anglerfish has not been run
        │   │   ├── If Anglerfish is ongoing
        │   │   │   └── Skip run
        │   │   ├── If Anglerfish samplesheet could not be found  
        │   │   │   └── Skip run
        │   │   ├── Run Anglerfish as subprocess
        │   │   └── Skip run
        │   └── If Anglerfish has failed
        │       └── Throw error
        ├── If run has already been transferred
        │   └── Skip run
        ├── Copy metadata
        ├── Transfer run to cluster
        ├── Update transfer log
        └── Archive run

    Any errors raised here-in should be sent with traceback as an email.
    """

    logger.info(f"{run.run_name}: Touching StatusDB...")
    run.touch_db_entry()

    # Is the run fully synced?
    if not run.is_synced:
        raise WaitForRun(f"{run.run_name}: Run is not fully synced, skipping.")

    # Assert all files are in place
    logger.info(f"{run.run_name}: Asserting run contents...")
    run.assert_contents()

    # Update StatusDB
    logger.info(f"{run.run_name}: Updating StatusDB...")
    run.update_db_entry()

    # Copy HTML report
    logger.info(f"{run.run_name}: Putting HTML report on GenStat...")
    run.copy_html_report()

    # Generate and publish TouliggQC report
    logger.info(f"{run.run_name}: Generating and publishing ToulligQC report...")
    run.toulligqc_report()

    # Look at seq data
    if not run.has_raw_seq_output():
        logger.info(f"{run.run_name}: Run has no sequencing output, continuing")

        if not run.has_fastq_output():
            raise WaitForRun(f"{run.run_name}: Run has no fastq output, skipping.")

        # Anglerfish
        logger.info(f"{run.run_name}: Checking whether Anglerfish has been run...")

        anglerfish_exit_code = run.get_anglerfish_exit_code()

        # Anglerfish not run
        if anglerfish_exit_code is None:
            logger.info(f"{run.run_name}: Anglerfish has not been run, continuing.")

            logger.info(f"{run.run_name}: Checking whether Anglerfish is ongoing...")

            anglerfish_pid = run.get_anglerfish_pid()

            # Anglerfish ongoing
            if anglerfish_pid:
                logger.info(
                    f"{run.run_name}: Anglerfish is ongoing with process ID {anglerfish_pid}, skipping."
                )
                raise WaitForRun("Anglerfish is ongoing, skipping.")

            logger.info(f"{run.run_name}: Anglerfish is not ongoing, continuing.")

            logger.info(f"{run.run_name}: Fetching Anglerfish samplesheet...")

            if not run.fetch_anglerfish_samplesheet():
                raise WaitForRun("Could not find Anglerfish sample sheet, skipping.")

            logger.info(f"{run.run_name}: Starting Anglerfish...")
            run.run_anglerfish()
            raise WaitForRun("Anglerfish has been started, skipping.")

        # Anglerfish run
        elif isinstance(anglerfish_exit_code, int):
            if anglerfish_exit_code == 0:
                logger.info(
                    f"{run.run_name}: Anglerfish has finished successfully, continuing."
                )
            elif anglerfish_exit_code > 0:
                logger.error(f"{run.run_name}: Anglerfish has failed, throwing error.")
                raise AssertionError(f"{run.run_name}: Anglerfish failed.")
        else:
            raise AssertionError("Unexpected Anglerfish exit code.")

    # Check transfer status
    if run.is_transferred():
        logger.warning(
            f"{run.run_name}: Run is already logged as transferred, skipping."
        )
        raise WaitForRun("Run is already logged as transferred.")

    # Copy metadata
    logger.info(f"{run.run_name}: Copying metadata...")
    run.copy_metadata()

    # Transfer run
    logger.info(f"{run.run_name}: Transferring to cluster...")
    run.transfer_run()

    # Update transfer log
    logger.info(f"{run.run_name}: Updating transfer log...")
    run.update_transfer_log()

    # Archive run
    logger.info(f"{run.run_name}: Archiving run...")
    run.archive_run()


def ont_transfer(run_abspath: str | None, qc: bool = False):
    """CLI entry function.

    Find finished ONT runs in ngi-nas and transfer to HPC cluster.
    """

    if run_abspath:
        if qc:
            process_qc_run(ONT_qc_run(run_abspath))
        else:
            process_user_run(ONT_user_run(run_abspath))

    # If no run is specified, locate all runs
    else:
        for run_type in ["user_run", "qc_run"]:
            logger.info(f"Looking for runs of type '{run_type}'...")

            data_dirs = CONFIG["nanopore_analysis"]["run_types"][run_type]["data_dirs"]
            ignore_dirs = CONFIG["nanopore_analysis"]["run_types"][run_type][
                "ignore_dirs"
            ]

            for data_dir in data_dirs:
                run_dirs = find_run_dirs(data_dir, ignore_dirs)

                for run_dir in run_dirs:
                    # Send error mails at run-level
                    try:
                        if run_type == "user_run":
                            process_user_run(ONT_user_run(run_dir))
                        else:
                            process_qc_run(ONT_qc_run(run_dir))
                    except WaitForRun as e:
                        logger.info(e)
                    except Exception as e:
                        send_error_mail(os.path.basename(run_dir), e)


class WaitForRun(Exception):
    """Exception defined to exit processing the current run and continue
    with the next one without sending an error email.
    """

    def __init__(self, message: str):
        super().__init__(message)


def ont_updatedb(run_abspath: str):
    """CLI entry function."""

    ont_run = ONT_run(os.path.abspath(run_abspath))

    logger.info(
        f"{ont_run.run_name}: Manually updating StatusDB, ignoring run status..."
    )
    ont_run.update_db_entry(force_update=True)
