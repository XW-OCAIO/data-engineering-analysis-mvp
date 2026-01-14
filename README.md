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

## Local Demo

```bash
uv run python -m backend.ingest --db data/events.duckdb --csv data/sample.csv
uv run python -c "from backend.pipeline import run_pipeline; run_pipeline()"
uv run streamlit run app/app.py
```

## Verify

Rebuild DB, ingest sample data, run transforms, and run pytest:

```bash
uv run python -m backend.verify
```

## Sanity checks

Run transform, then check raw and fact counts (PowerShell):

```powershell
uv run python -c "from backend.pipeline import run_pipeline; run_pipeline()"

@'
import duckdb

conn = duckdb.connect("data/events.duckdb")
try:
    raw_count = conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0]
    fct_count = conn.execute("SELECT COUNT(*) FROM fct_events").fetchone()[0]
    print(f"raw_events={raw_count}")
    print(f"fct_events={fct_count}")
finally:
    conn.close()
'@ | uv run python -
```

## Verification

Run ingest twice; row count should remain 50:

```bash
uv run python -m backend.ingest --db data/events.duckdb --csv data/sample.csv
uv run python -m backend.ingest --db data/events.duckdb --csv data/sample.csv
```

## Run the Streamlit app

```bash
uv run streamlit run app/app.py
```

## Run tests

```bash
uv run pytest
```
