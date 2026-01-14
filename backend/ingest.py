from __future__ import annotations

import argparse
import csv
from datetime import datetime
from pathlib import Path

from backend.db import get_connection

REQUIRED_COLUMNS = {"event_time", "user_id", "event_name", "category", "amount"}


def load_schema(schema_path: Path) -> str:
    return schema_path.read_text(encoding="utf-8")


def create_raw_table(conn, schema_path: Path) -> None:
    conn.execute(load_schema(schema_path))


def validate_csv(csv_path: Path) -> None:
    with csv_path.open(encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError("CSV missing header row")
        missing = REQUIRED_COLUMNS - set(reader.fieldnames)
        if missing:
            missing_list = ", ".join(sorted(missing))
            raise ValueError(f"CSV missing required columns: {missing_list}")
        for line_number, row in enumerate(reader, start=2):
            event_time = row.get("event_time", "")
            try:
                datetime.fromisoformat(event_time)
            except ValueError as exc:
                raise ValueError(
                    f"Invalid event_time at line {line_number}: {event_time}"
                ) from exc
            amount = row.get("amount", "")
            try:
                float(amount)
            except ValueError as exc:
                raise ValueError(f"Invalid amount at line {line_number}: {amount}") from exc


def ingest_csv(conn, csv_path: Path) -> None:
    conn.execute(
        """
        WITH incoming AS (
            SELECT * FROM read_csv_auto(?, header=True)
        )
        INSERT INTO raw_events
        SELECT * FROM incoming
        EXCEPT SELECT * FROM raw_events
        """,
        [str(csv_path)],
    )


def get_row_count(conn) -> int:
    return conn.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0]


def main() -> None:
    parser = argparse.ArgumentParser(description="Ingest CSV into DuckDB raw_events.")
    parser.add_argument("--db", default="data/events.duckdb", help="Path to DuckDB file")
    parser.add_argument("--csv", default="data/sample.csv", help="Path to CSV file")
    args = parser.parse_args()

    db_path = Path(args.db)
    csv_path = Path(args.csv)
    schema_path = Path("backend/sql/raw_events.sql")

    conn = get_connection(db_path)
    try:
        validate_csv(csv_path)
        create_raw_table(conn, schema_path)
        ingest_csv(conn, csv_path)
        print(f"row_count={get_row_count(conn)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
