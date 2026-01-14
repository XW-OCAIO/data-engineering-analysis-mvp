from __future__ import annotations

from pathlib import Path

import duckdb


def get_connection(db_path: str | Path) -> duckdb.DuckDBPyConnection:
    return duckdb.connect(str(db_path))
