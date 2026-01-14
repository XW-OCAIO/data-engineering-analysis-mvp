from __future__ import annotations

from pathlib import Path

from backend.db import get_connection
from backend.models import Metrics

BASE_DIR = Path(__file__).resolve().parents[1]


def load_sql(sql_path: Path) -> str:
    return sql_path.read_text(encoding="utf-8")


def get_metrics(db_path: str | Path = BASE_DIR / "data" / "events.duckdb") -> Metrics:
    conn = get_connection(db_path)
    try:
        sql_path = BASE_DIR / "backend" / "sql" / "020_metrics.sql"
        result = conn.execute(load_sql(sql_path))
        row = result.fetchone()
        columns = [col[0] for col in result.description]
    finally:
        conn.close()

    data = dict(zip(columns, row))
    return Metrics(**data)
