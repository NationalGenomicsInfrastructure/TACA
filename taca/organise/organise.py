"""Flowcell organisation methods for TACA."""

import logging

from taca.organise.flowcells import get_flowcell_object

logger = logging.getLogger(__name__)


def organise_flowcell(flowcell, project, include_pod5=False):
    """Determine flowcell type and organise the data accordingly."""
    flowcell_object = get_flowcell_object(flowcell, project, include_pod5)
    flowcell_object.create_org_dir()
    flowcell_object.organise_data()
    logger.info(f"Finished organisation of flowcell {flowcell}.")
