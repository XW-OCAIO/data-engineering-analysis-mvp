from __future__ import annotations

import sys
from pathlib import Path

import duckdb
import streamlit as st

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from backend.metrics import get_metrics
from backend.pipeline import run_pipeline


def load_daily_counts(db_path: str | Path = REPO_ROOT / "data" / "events.duckdb"):
    conn = duckdb.connect(str(db_path))
    try:
        query = """
        SELECT
            DATE_TRUNC('day', event_time) AS day,
            COUNT(*)::BIGINT AS daily_count
        FROM fct_events
        GROUP BY 1
        ORDER BY 1
        """
        return conn.execute(query).fetch_df()
    finally:
        conn.close()


def run_app() -> None:
    st.set_page_config(page_title="Events MVP", layout="centered")

    st.title("Events MVP")
    st.caption("MVP dashboard from sample events data.")

    db_path = REPO_ROOT / "data" / "events.duckdb"
    try:
        run_pipeline(db_path)
        metrics = get_metrics(db_path)
        daily_counts = load_daily_counts(db_path)
    except Exception as exc:  # noqa: BLE001 - show helpful UI error
        st.error(f"Failed to load data. Ensure ingest has run. Details: {exc}")
        return

    col1, col2, col3 = st.columns(3)
    col1.metric("Total events", f"{metrics.total_events:,}")
    col2.metric("Total users", f"{metrics.total_users:,}")
    col3.metric("Total amount", f"{metrics.total_amount:,.2f}")

    st.subheader("Daily event count")
    st.line_chart(daily_counts, x="day", y="daily_count")


if __name__ == "__main__":
    run_app()
