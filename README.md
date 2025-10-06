# Pythia

Pythia is a two-part forecasting platform that combines a humanitarian data
resolver with an automated forecasting engine. The repository keeps the full
history of both components while presenting them under a unified name and
workspace.

## Repository layout

- `resolver/` – database-first humanitarian data pipeline for collecting,
  cleaning, and publishing structured datasets that power downstream analysis.
- `forecaster/` – the autonomous forecasting engine (previously known as
  **Spagbot**) responsible for research, ensemble forecasting, and result
  logging.
- `run_forecaster.py` – thin launcher that invokes `forecaster.cli.main` for
  local or automated runs.

## Getting started

### Prerequisites

- Python 3.11+
- [Poetry](https://python-poetry.org/) for dependency management (recommended)

### Installation

```bash
poetry install
```

This command creates a virtual environment and installs all resolver and
forecaster dependencies. If you prefer `pip`, use `pip install -e .` from the
repository root.

### Running the forecaster engine

```bash
poetry run python run_forecaster.py --help
```

Common operations include:

- `poetry run python run_forecaster.py --mode test_questions --limit 1`
- `poetry run python run_forecaster.py --pid <question_id>`
- Add `--submit` to push forecasts back to Metaculus when API credentials are
  configured.

### Resolver ingestion jobs

Resolver connectors and utilities live under `resolver/ingestion/`. Each module
contains a `main` or `run` entry point for on-demand refreshes. Refer to the
module docstrings for job-specific instructions.

## Testing & validation

The repository is wired for GitHub Actions. Locally you can emulate the CI
checks with:

```bash
poetry run pytest resolver/tests
poetry run pytest forecaster/tests
```

If a module lacks automated coverage, run targeted smoke tests such as
`poetry run python run_forecaster.py --help` or the relevant resolver ingestion
script.

## Continuous integration

Workflows in `.github/workflows/` install dependencies, run the resolver and
forecaster test suites, and build dashboard assets. Update repository secrets in
`Settings → Secrets and variables → Actions` to enable production runs.

## License

This project retains the original license information from the source modules.
Review the headers within `resolver/` and `forecaster/` for component-specific
terms.
