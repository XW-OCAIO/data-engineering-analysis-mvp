from __future__ import annotations

import argparse
from pathlib import Path

from backend.db import get_connection


def load_schema(schema_path: Path) -> str:
    return schema_path.read_text(encoding="utf-8")


def create_raw_table(conn, schema_path: Path) -> None:
    conn.execute(load_schema(schema_path))


def ingest_csv(conn, csv_path: Path) -> None:
    conn.execute(
        "INSERT INTO raw_events SELECT * FROM read_csv_auto(?, header=True)",
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
        create_raw_table(conn, schema_path)
        ingest_csv(conn, csv_path)
        print(f"row_count={get_row_count(conn)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
