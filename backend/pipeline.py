from __future__ import annotations

from pathlib import Path

from backend.db import get_connection


def load_sql(sql_path: Path) -> str:
    return sql_path.read_text(encoding="utf-8")


def run_pipeline(db_path: str | Path = "data/events.duckdb") -> None:
    conn = get_connection(db_path)
    try:
        transform_path = Path("backend/sql/010_fct_events.sql")
        conn.execute(load_sql(transform_path))
    finally:
        conn.close()
