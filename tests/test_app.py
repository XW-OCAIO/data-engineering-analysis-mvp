from pathlib import Path

from app.app import load_daily_counts
from backend.db import get_connection
from backend.ingest import create_raw_table, ingest_csv, validate_csv
from backend.pipeline import run_pipeline


def test_app_smoke(tmp_path: Path) -> None:
    db_path = tmp_path / "events.duckdb"
    csv_path = Path("data/sample.csv")
    schema_path = Path("backend/sql/raw_events.sql")

    validate_csv(csv_path)
    conn = get_connection(db_path)
    try:
        create_raw_table(conn, schema_path)
        ingest_csv(conn, csv_path)
    finally:
        conn.close()

    run_pipeline(db_path)
    daily_counts = load_daily_counts(db_path)

    assert {"day", "daily_count"} <= set(daily_counts.columns)
