import logging

from taca.element.Element_Runs import Run

logger = logging.getLogger(__name__)


class Aviti_Run(Run):
    def __init__(self, run_dir, configuration):
        self.sequencer_type = "Aviti"
        self.demux_dir = "Demultiplexing"
        super().__init__(run_dir, configuration)

    def check_side_letter(self):
        if self.side_letter != self.run_dir.split("_")[-1][0]:
            logger.warning(
                f"Side specified by sequencing operator does not match side from instrument for {self}. Aborting."
            )
            raise AssertionError(f"Inconcistencies in side assignments for {self}")
