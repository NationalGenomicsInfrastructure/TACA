import glob

from setuptools import find_packages, setup

from taca import __version__

try:
    with open("requirements.txt") as f:
        install_requires = [x.strip() for x in f.readlines()]
except OSError:
    install_requires = []

try:
    with open("dependency_links.txt") as f:
        dependency_links = [x.strip() for x in f.readlines()]
except OSError:
    dependency_links = []


setup(
    name="taca",
    version=__version__,
    description="Tool for the Automation of Cleanup and Analyses",
    long_description="This package contains a set of functionalities that are "
    "useful in the day-to-day tasks of bioinformatitians in "
    "National Genomics Infrastructure in Stockholm, Sweden.",
    keywords="bioinformatics",
    author="NGI-stockholm",
    author_email="ngi_pipeline_operators@scilifelab.se",
    python_requires=">=3.11.5",
    url="http://taca.readthedocs.org/en/latest/",
    license="MIT",
    packages=find_packages(exclude=["ez_setup", "examples", "tests"]),
    scripts=glob.glob("scripts/*.py"),
    include_package_data=True,
    zip_safe=False,
    entry_points={
        "console_scripts": ["taca = taca.cli:cli"],
        "taca.subcommands": [
            "cleanup = taca.cleanup.cli:cleanup",
            "analysis = taca.analysis.cli:analysis",
            "bioinfo_deliveries = taca.utils.cli:bioinfo_deliveries",
            "server_status = taca.server_status.cli:server_status",
            "backup = taca.backup.cli:backup",
            "create_env = taca.testing.cli:uppmax_env",
            "organise = taca.organise.cli:organise_flowcells",
        ],
    },
    install_requires=install_requires,
    dependency_links=dependency_links,
)
