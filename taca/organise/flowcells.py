"""Flowcell classes for TACA."""

import logging
import os

from taca.utils.config import CONFIG

logger = logging.getLogger(__name__)


class Flowcell:
    """Defines a generic Flowcell"""

    def __init__(self, flowcell, project_id):
        self.fc_id = flowcell
        self.project_id = project_id
        self.detsination_path = CONFIG.get("organise", None).get(
            self.fc_type + "_path", None
        )
        self.organised_project_dir = os.path.join(self.detsination_path, project_id)

    def create_org_dir(self):
        """Create a project directory that the data should be organised to."""
        if not os.path.exists(self.organised_project_dir):
            os.mkdir(self.organised_project_dir)
        return


class NanoporeFlowcell(Flowcell):
    """Defines a Nanopore Flowcell"""

    def __init__(self, flowcell, project_id):
        super().__init__(flowcell, project_id)
        self.fc_type = "nanopore"

    def organise_data(self):
        """Tarball data into ONT_TAR"""
        # generate tarball (detached process? how to signal when tarballing is done?)
        pass


class IlluminaFlowcell(Flowcell):
    """Defines a Illumina Flowcell"""

    def __init__(self, flowcell, project_id):
        super().__init__(flowcell, project_id)
        self.fc_type = "illumina"

    def organise_data(self):
        """Symlink data into DATA"""
        pass


class ElementFlowcell(Flowcell):
    """Defines a Element Flowcell"""

    def __init__(self, flowcell):
        super().__init__(flowcell)
        self.fc_type = "element"

    def organise_data(self):
        """Symlink data into DATA"""
        pass
