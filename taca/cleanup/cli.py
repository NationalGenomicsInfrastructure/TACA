"""CLI for the storage subcommand."""

import click

from taca.cleanup import cleanup as cln


@click.group()
@click.pass_context
@click.option(
    "--status_db_config",
    type=click.Path(exists=True, dir_okay=False),
    envvar="STATUS_DB_CONFIG",
    help="Path to statusdb-configuration.",
)
def cleanup(ctx, status_db_config):
    """Cleaning up servers - management methods and utilities."""
    pass


# cleanup subcommands
@cleanup.command()
@click.option(
    "--days_fastq",
    type=click.IntRange(min=1),
    help='Days to consider as thershold for removing "fastq" files',
)
@click.option(
    "--days_analysis",
    type=click.IntRange(min=1),
    help="Days to consider as thershold for removing analysis data",
)
@click.option("--only_fastq", is_flag=True, help='Clean only fastq data in "miarka"')
@click.option(
    "--only_analysis", is_flag=True, help='Clean only analysis data in "miarka"'
)
@click.option(
    "--date",
    type=click.STRING,
    help="Consider the given date instead of today while collecting closed projects. "
    'Date format should be "YYYY-MM-DD", ex: "2016-01-31"',
)
@click.option(
    "--exclude_projects",
    type=click.STRING,
    help="A project or a file with a list of projects to exclude from deleting. "
    "Either name or id can be given. Examples: --exclude_projects P1234 or "
    "--exclude_projects P1234,P5678 or "
    "--exclude_projects file_with_projects_id.txt",
)
@click.option(
    "--clean_undetermined",
    is_flag=True,
    help="Remove only the undetermined reads for a flowcell that have "
    "all project cleaned. All other parameters are ignored if this "
    "flag is called.",
)
@click.option(
    "-l",
    "--list_only",
    is_flag=True,
    help="Only build the project list that will be cleaned",
)
@click.option(
    "-n", "--dry_run", is_flag=True, help="Perform dry run i.e. execute nothing but log"
)
@click.pass_context
def miarka(
    ctx,
    days_fastq,
    days_analysis,
    only_fastq,
    only_analysis,
    clean_undetermined,
    date,
    exclude_projects,
    list_only,
    dry_run,
):
    """Do appropriate cleanup on Miarka."""
    status_db_config = ctx.parent.params["status_db_config"]
    if only_fastq and only_analysis:
        raise SystemExit(
            'ERROR: Both option "only_fastq" and "only_analysis" is given, should only give either one'
        )
    if not days_fastq and not only_analysis and not clean_undetermined:
        raise SystemExit(
            'ERROR: "days_fastq" is not given while not selecting "only_analysis" option'
        )
    if not days_analysis and not only_fastq and not clean_undetermined:
        raise SystemExit(
            'ERROR: "days_analysis" is not given while not selecting "only_fastq" option'
        )
    cln.cleanup_miarka(
        days_fastq,
        days_analysis,
        only_fastq,
        only_analysis,
        clean_undetermined,
        status_db_config,
        exclude_projects,
        list_only,
        date,
        dry_run,
    )
