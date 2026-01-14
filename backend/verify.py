from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

from backend.db import get_connection
from backend.ingest import create_raw_table, ingest_csv, validate_csv
from backend.pipeline import run_pipeline

BASE_DIR = Path(__file__).resolve().parents[1]
DB_PATH = BASE_DIR / "data" / "events.duckdb"
CSV_PATH = BASE_DIR / "data" / "sample.csv"
SCHEMA_PATH = BASE_DIR / "backend" / "sql" / "raw_events.sql"


def rebuild_db() -> None:
    if DB_PATH.exists():
        DB_PATH.unlink()

    conn = get_connection(DB_PATH)
    try:
        create_raw_table(conn, SCHEMA_PATH)
        ingest_csv(conn, CSV_PATH)
    finally:
        conn.close()


def run_pytest() -> None:
    env = os.environ.copy()
    env["PYTHONPATH"] = str(BASE_DIR)
    subprocess.run([sys.executable, "-m", "pytest"], check=True, env=env)


def main() -> None:
    validate_csv(CSV_PATH)
    rebuild_db()
    run_pipeline(DB_PATH)
    run_pytest()


if __name__ == "__main__":
    main()
