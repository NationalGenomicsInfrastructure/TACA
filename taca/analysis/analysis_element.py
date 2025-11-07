"""Analysis methods for sequencing runs produced by Element instruments."""

import glob
import logging
import os

from taca.element.Aviti_Runs import Aviti_Run
from taca.utils.config import CONFIG
from taca.utils.misc import send_mail

logger = logging.getLogger(__name__)


def run_preprocessing(given_run):
    """Run demultiplexing in all data directories.

    :param str given_run: Process a particular run instead of looking for runs
    """

    def _process(run_to_process):
        """Process a run/flowcell and transfer to analysis server.

        :param taca.element.Run run: Run to be processed and transferred
        """
        logger.info(f"Working on {run_to_process}")
        try:
            run = Aviti_Run(run_to_process, CONFIG)
            run.parse_run_parameters()
        except FileNotFoundError:
            logger.warning(
                f"Cannot reliably set NGI_run_id for {run} due to missing "
                "RunParameters.json. Aborting run processing"
            )
            email_subject = f"Issues processing {run}"
            email_message = (
                f"RunParameters.json missing for {run}. Processing was aborted."
            )
            send_mail(email_subject, email_message, CONFIG["mail"]["recipients"])
            raise

        #### Sequencing status ####
        try:
            sequencing_done = run.check_sequencing_status()
        except RuntimeError as e:
            logger.warning(f"The sequencing FAILED for {run}: {e}")
            email_subject = f"Issues processing {run}"
            email_message = f"The sequencing of {run} FAILED: {e}"
            send_mail(email_subject, email_message, CONFIG["mail"]["recipients"])
            raise
        if not sequencing_done:
            run.status = "sequencing"
            logger.info(f"{run} is still sequencing")
            if run.status_changed():
                run.update_statusdb()
            return

        #### Demultiplexing status ####
        if run.run_type == "Cytoprofiling":
            logger.info(
                f"Skipping demultiplexing for {run} as it is a Cytoprofiling run"
            )
        else:
            demultiplexing_status = run.get_demultiplexing_status()
            if demultiplexing_status == "not started":
                lims_zip_path = run.find_lims_zip()
                if lims_zip_path is not None:
                    os.mkdir(run.demux_dir)
                    run.copy_manifests(lims_zip_path)
                    demux_manifests = run.make_demux_manifests(
                        manifest_to_split=run.lims_manifest
                    )
                    sub_demux_count = 0
                    for demux_manifest in sorted(demux_manifests):
                        sub_demux_dir = os.path.join(
                            run.run_dir, f"Demultiplexing_{sub_demux_count}"
                        )
                        os.mkdir(sub_demux_dir)
                        run.start_demux(demux_manifest, sub_demux_dir)
                        sub_demux_count += 1
                    run.status = "demultiplexing"
                    if run.status_changed():
                        run.update_statusdb()
                    return
                else:
                    logger.warning(
                        f"Run manifest is missing for {run}, demultiplexing aborted"
                    )
                    email_subject = f"Issues processing {run}"
                    email_message = (
                        f"Run manifest is missing for {run}, demultiplexing aborted"
                    )
                    send_mail(
                        email_subject, email_message, CONFIG["mail"]["recipients"]
                    )
                    return
            elif demultiplexing_status == "ongoing":
                run.status = "demultiplexing"
                if run.status_changed():
                    run.update_statusdb()
                return

            elif demultiplexing_status != "finished":
                logger.warning(
                    f"Unknown demultiplexing status {demultiplexing_status} of run {run}. "
                    "Please investigate."
                )
                email_subject = f"Issues processing {run}"
                email_message = (
                    f"Unknown demultiplexing status {demultiplexing_status} of "
                    f"run {run}. Please investigate."
                )
                send_mail(email_subject, email_message, CONFIG["mail"]["recipients"])
                return

            email_subject = f"Demultiplexing completed for {run}"
            email_message = (
                f"{run} has been demultiplexed without any errors or warnings.\n"
                "The run will be transferred to the analysis cluster for further analysis.\n"
                f"It is available at https://genomics-status.scilifelab.se/flowcells_element/{run.NGI_run_id}"
            )
            send_mail(email_subject, email_message, CONFIG["mail"]["recipients"])

        #### Transfer status ####
        transfer_status = run.get_transfer_status()
        if transfer_status == "not started":
            if run.run_type == "Cytoprofiling":
                run.move_pressure_check_dir()
            else:
                demux_results_dirs = glob.glob(
                    os.path.join(run.run_dir, "Demultiplexing_*")
                )
                run.aggregate_demux_results(demux_results_dirs)
            run.sync_metadata()
            run.make_transfer_indicator()
            run.status = "transferring"
            if run.status_changed():
                run.update_statusdb()
            run.transfer()
            return
        elif transfer_status == "ongoing":
            run.status = "transferring"
            if run.status_changed():
                run.update_statusdb()
            logger.info(f"{run} is being transferred. Skipping.")
            return
        elif transfer_status == "rsync done":
            run.remove_transfer_indicator()
            run.update_transfer_log()
            run.status = "transferred"
            if run.status_changed():
                run.update_statusdb()
            run.move_to_nosync()
            email_subject = f"{run} has been transferred to the analysis cluster"
            email_message = (
                f"Rsync of data for run {run} to the analysis cluster has finished!\n"
                f"The run is available at https://genomics-status.scilifelab.se/flowcells_element/{run}"
            )
            send_mail(email_subject, email_message, CONFIG["mail"]["recipients"])

        elif transfer_status == "rsync failed":
            run.status = "transfer failed"
            logger.warning(
                f"An issue occurred while transfering {run} to the analysis cluster."
            )
            email_subject = f"Issues processing {run}"
            email_message = (
                f"An issue occurred while transfering {run} to the analysis cluster."
            )
            send_mail(email_subject, email_message, CONFIG["mail"]["recipients"])
            return
        else:
            logger.warning(
                f"Unexpected transfer status {transfer_status} of run {run}, please investigate."
            )
            email_subject = f"Issues processing {run}"
            email_message = f"Unknown transfer status {transfer_status} of run {run}, please investigate."
            send_mail(email_subject, email_message, CONFIG["mail"]["recipients"])
            return

    if given_run:
        logger.info(f"Starting processing of run {given_run}")
        _process(given_run)
        logger.info(f"Finished processing run {given_run}")
    else:
        logger.info("Starting processing of all runs in data directories")
        data_dirs = CONFIG.get("element_analysis").get("data_dirs")
        for data_dir in data_dirs:
            # Run folder looks like DATE_*_*, the last section is the FC side (A/B) and ID (PID for teton runs)
            runs = glob.glob(os.path.join(data_dir, "[1-9]*_*_*"))
            for run in runs:
                if "FlowcellPressureCheck" in run:
                    # Skip the pressure check runs (Teton runs)
                    logger.info(f"Skipping {run}")
                    continue
                try:
                    _process(run)
                except Exception as e:
                    # This function might throw an exception,
                    # it is better to continue processing other runs
                    email_subject = f"Issues processing {run}"
                    email_message = (
                        f"An error occurred while processing {run}. Error: {e}"
                    )
                    send_mail(
                        email_subject, email_message, CONFIG["mail"]["recipients"]
                    )
                    logger.warning(
                        f"There was an error processing the run {run}. Error: {e}"
                    )
                    pass
        logger.info("Finished processing all runs in data directories")
