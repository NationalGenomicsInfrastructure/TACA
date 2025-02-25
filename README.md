<p align="center">
  <a href="https://github.com/SciLifeLab/TACA">
    <img width="512" height="175" src="artwork/logo.png"/>
  </a>
</p>

# Tool for the Automation of Cleanup and Analyses

[![PyPI version](https://badge.fury.io/py/taca.svg)](http://badge.fury.io/py/taca)
[![Documentation Status](https://readthedocs.org/projects/taca/badge/?version=latest)](https://readthedocs.org/projects/taca/?badge=latest)
[![codecov](https://codecov.io/gh/nationalgenomicsinfrastructure/taca/branch/master/graph/badge.svg)](https://codecov.io/gh/nationalgenomicsinfrastructure/taca)

This package contains several tools for projects and data management in the [National Genomics Infrastructure](https://ngisweden.scilifelab.se/) in Stockholm, Sweden.

### Run tests in docker

```shell
git clone https://github.com/SciLifeLab/TACA.git
cd TACA
docker build -t taca_testing --target testing .
docker run -it taca_testing
```

## Installation

Inside the repo, run `pip install .`

## Development

Run `pip install requirements-dev.txt` to install packages used for development and `pip install -e .` to make the installation editable.

### Automated linting

This repo is configured for automated linting. Linter parameters are defined in `pyproject.toml`.

As of now, we use:

- [ruff](https://docs.astral.sh/ruff/) to perform automated formatting and a variety of lint checks.
  - Run with `ruff check .` and `ruff format .`
- [mypy](https://mypy.readthedocs.io/en/stable/) for static type checking and to prevent contradictory type annotation.
  - Run with `mypy **/*.py`
- [pipreqs](https://github.com/bndr/pipreqs) to check that the requirement files are up-to-date with the code.

  - This is run with a custom Bash script in GitHub Actions which will only compare the list of package names.

    ```
    # Extract and sort package names
    awk '{print $1}' $1 | sort -u > "$1".compare
    awk -F'==' '{print $1}' $2 | sort -u > "$2".compare

    # Compare package lists
    if cmp -s "$1".compare "$2".compare
    then
      echo "Requirements are the same"
      exit 0
    else
      echo "Requirements are different"
      exit 1
    fi
    ```

- [prettier](https://prettier.io/) to format common languages.
  - Run with `prettier .`
- [editorconfig-checker](https://github.com/editorconfig-checker/editorconfig-checker) to enforce `.editorconfig` rules for all files not covered by the tools above.
  - Run with
    ```
    editorconfig-checker $(git ls-files | grep -v '.py\|.md\|.json\|.yml\|.yaml\|.html')
    ```

#### [GitHub Actions](https://docs.github.com/en/actions)

Configured in `.github/workflows/lint-code.yml`. Will test all commits in pushes or pull requests, but not change code or prevent merges.

#### [Pre-commit](https://pre-commit.com/)

Will prevent local commits that fail linting checks. Configured in `.pre-commit-config.yml`.

To set up pre-commit checking:

1. Run `pip install pre-commit`
2. Navigate to the repo root
3. Run `pre-commit install`

This can be disabled with `pre-commit uninstall`

#### VS Code automation

To enable automated linting in VS Code, go the the user `settings.json` and include the following lines:

```
"[python]": {
    "editor.defaultFormatter": "charliermarsh.ruff",
}
```

This will run the `ruff`-mediated linting with the same parameters as the `GitHub Actions` and `pre-commit` every time VS Code is used to format the code in the repository.

To run formatting on save, include the lines:

```
"[python]": {
    "editor.formatOnSave": true,
}
```

### Git blame suppression

When a non-invasive tool is used to tidy up a lot of code, it is useful to supress the Git blame for that particular commit, so the original author can still be traced.

To do this, add the hash of the commit containing the changes to `.git-blame-ignore-revs`, headed by an explanatory comment.

### Deliver command

There is also a [plugin for the deliver command](https://github.com/NationalGenomicsInfrastructure/taca-ngi-pipeline). To install this in the same development environment:

```
# Install taca delivery plugin for development
git clone https://github.com/<username>/TACA.git
cd ../taca-ngi-pipeline
python setup.py develop
pip install -r ./requirements-dev.txt

# add required config files and env for taca delivery plugin
echo "foo:bar" >> ~/.ngipipeline/ngi_config.yaml
echo "foo:bar" >> ~/.ngipipeline/ngi_config.yaml
mkdir ~/.taca && cp tests/data/taca_test_cfg.yaml ~/.taca/taca.yaml
export CHARON_BASE_URL="http://tracking.database.org"
export CHARON_API_TOKEN="charonapitokengoeshere"

# Check that tests pass:
cd tests && nosetests -v -s
```

For a more detailed documentation please go to [the documentation page](http://taca.readthedocs.org/en/latest/).
