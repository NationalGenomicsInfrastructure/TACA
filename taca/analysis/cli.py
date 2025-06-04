"""CLI for the analysis subcommand."""

import click

from taca.analysis import analysis as an
from taca.analysis import analysis_element, analysis_nanopore


@click.group()
def analysis():
    """Analysis methods entry point."""
    pass


# Illumina analysis subcommands


@analysis.command()
@click.option(
    "-r",
    "--run",
    type=click.Path(exists=True),
    default=None,
    help="Demultiplex only a particular run",
)
@click.option(
    "-s",
    "--software",
    type=click.Choice(["bcl2fastq", "bclconvert"]),
    default="bcl2fastq",
    help="Available software for demultiplexing: bcl2fastq (default), bclconvert",
)
def demultiplex(run, software):
    """Demultiplex and transfer all runs present in the data directories."""
    an.run_preprocessing(run, software)


@analysis.command()
@click.option(
    "--runfolder-project",
    is_flag=False,
    help="Project IDs for runfolder transfer separated by comma",
)
@click.option("--exclude-lane", default="", help="Lanes to exclude separated by comma")
@click.option(
    "-s",
    "--software",
    type=click.Choice(["bcl2fastq", "bclconvert"]),
    default="bcl2fastq",
    help="Available software for demultiplexing: bcl2fastq (default), bclconvert",
)
@click.argument("rundir")
def transfer(rundir, runfolder_project, exclude_lane, software):
    """Transfers the run without qc."""
    if not runfolder_project:
        an.transfer_run(rundir, software)
    else:
        an.transfer_runfolder(rundir, pid=runfolder_project, exclude_lane=exclude_lane)


@analysis.command()
@click.option(
    "-s",
    "--software",
    type=click.Choice(["bcl2fastq", "bclconvert"]),
    default="bcl2fastq",
    help="Available software for demultiplexing: bcl2fastq (default), bclconvert",
)
@click.argument("rundir")
def updatedb(rundir, software):
    """Save the run to statusdb."""
    an.upload_to_statusdb(rundir, software)


# Element analysis subcommands


@analysis.command()
@click.option(
    "-r",
    "--run",
    type=click.Path(exists=True),
    default=None,
    help="Demultiplex only a particular run",
)
def demultiplex_element(run):
    """Demultiplex and transfer all runs present in the data directories."""
    analysis_element.run_preprocessing(run)


@analysis.command()
@click.argument("run")
def element_updatedb(run):
    """Save the run to statusdb."""
    analysis_element.upload_to_statusdb(run)


# Nanopore analysis subcommands


@analysis.command()
@click.option(
    "-r",
    "--run",
    type=click.Path(exists=True),
    default=None,
    help="Process only a particular run",
)
def ont_transfer(run):
    """Find and process all runs"""
    analysis_nanopore.ont_transfer(run)


@analysis.command()
@click.argument("run")
def ont_updatedb(run):
    analysis_nanopore.ont_updatedb(run)
