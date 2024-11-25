"""Flowcell organisation methods for TACA."""

import logging

from taca.organise.flowcells import ElementFlowcell, IlluminaFlowcell, NanoporeFlowcell

logger = logging.getLogger(__name__)


def get_flowcell_type(flowcell):
    """Return flowcell type based on flowcell name"""
    pass


def organise_flowcell(flowcell, project):
    """Determine flowcell type and organise the data accordingly."""
    flowcell_type = get_flowcell_type(flowcell)

    if flowcell_type == "nanopore":
        flowcell_object = NanoporeFlowcell(flowcell=flowcell, project_id=project)
    elif flowcell_type == "illumina":
        flowcell_object = IlluminaFlowcell(flowcell=flowcell, project_id=project)
    elif flowcell_type == "element":
        flowcell_object = ElementFlowcell(flowcell=flowcell, project_id=project)
    else:
        logger.warning(
            f"Flowcell type could not be recognised for flowcell {flowcell}, skipping it."
        )
        return

    flowcell_object.create_org_dir()
    flowcell_object.organise_data()

    logger.info(f"Finished organisation of flowcell {flowcell}.")
