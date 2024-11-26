"""CLI for the organise subcommand."""

import click

from taca.organise import organise


@click.command(name="organise")
@click.option(
    "-p",
    "--project",
    type=str,
    required=True,
    help="Project ID (e.g. P12345)",
) # future todo: option to organise all flowcells in a project
@click.argument("flowcells")
def organise_flowcells(flowcells, project):
    """Organise FLOWCELLS.

    FLOWCELLS is the name of one or more sequencing flowcells, separated by a comma. e.g.:
    241122_VH00204_464_AAG77JJN5,241120_VH00202_453_AAG76JJM7
    """
    flowcells_to_organise = flowcells.split(",")
    for fc in flowcells_to_organise:
        organise.organise_flowcell(fc, project)
