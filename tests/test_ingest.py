from pathlib import Path

from backend.db import get_connection
from backend.ingest import create_raw_table, get_row_count, ingest_csv, validate_csv


def test_ingest_idempotent(tmp_path: Path) -> None:
    db_path = tmp_path / "events.duckdb"
    csv_path = Path("data/sample.csv")
    schema_path = Path("backend/sql/raw_events.sql")

    validate_csv(csv_path)

    conn = get_connection(db_path)
    try:
        create_raw_table(conn, schema_path)
        ingest_csv(conn, csv_path)
        first_count = get_row_count(conn)
        ingest_csv(conn, csv_path)
        second_count = get_row_count(conn)
    finally:
        conn.close()

    assert first_count == 50
    assert second_count == first_count
