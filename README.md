# Data Engineering Analysis MVP

MVP only: CSV -> raw table -> transform -> 3 metrics -> Streamlit dashboard.

## Local setup (Python 3.11 + uv)

1) Create a virtual environment:

```bash
uv venv -p 3.11 .venv
```

2) Install dependencies (including pytest):

```bash
uv pip install -e ".[dev]"
```

## Ingest sample CSV

```bash
uv run python -m backend.ingest --db data/events.duckdb --csv data/sample.csv
```

This prints the row count after ingest.

## Run the Streamlit app

```bash
uv run streamlit run app/app.py
```

## Run tests

```bash
uv run pytest
```
