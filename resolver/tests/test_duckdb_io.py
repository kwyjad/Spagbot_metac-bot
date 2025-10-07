from __future__ import annotations

import pandas as pd
import pytest

duckdb = pytest.importorskip("duckdb")

from resolver.db.duckdb_io import FACTS_RESOLVED_KEYS, upsert_dataframe


def _connect(path: str) -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect(path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS facts_resolved (
            ym TEXT,
            iso3 TEXT,
            hazard_code TEXT,
            metric TEXT,
            series_semantics TEXT,
            value INTEGER
        )
        """
    )
    return conn


def test_upsert_dataframe_respects_column_alignment(tmp_path):
    conn = duckdb.connect(str(tmp_path / "columns.duckdb"))
    conn.execute(
        "CREATE TABLE sample (iso3 TEXT, metric TEXT, value INTEGER)"
    )

    frame = pd.DataFrame(
        [
            {"iso3": "KEN", "metric": "cases", "value": 10},
            {"iso3": "UGA", "metric": "cases", "value": 20},
        ]
    )
    shuffled = frame[["value", "iso3", "metric"]]

    upsert_dataframe(conn, "sample", shuffled, keys=["iso3", "metric"])

    rows = conn.execute(
        "SELECT iso3, metric, value FROM sample ORDER BY iso3"
    ).fetchall()
    assert rows == [("KEN", "cases", 10), ("UGA", "cases", 20)]


def test_upsert_dataframe_canonicalises_before_dedup(tmp_path):
    conn = _connect(str(tmp_path / "facts.duckdb"))

    frame = pd.DataFrame(
        [
            {
                "ym": "2024-01",
                "iso3": "KEN",
                "hazard_code": "drought",
                "metric": "in_need",
                "series_semantics": "Stock estimate",
                "value": 5,
                "extra": "ignored",
            },
            {
                "ym": "2024-01",
                "iso3": "KEN",
                "hazard_code": "drought",
                "metric": "in_need",
                "series_semantics": "stock",
                "value": 7,
            },
        ]
    )

    upsert_dataframe(
        conn,
        "facts_resolved",
        frame,
        keys=FACTS_RESOLVED_KEYS,
        drop_columns=["extra"],
    )

    rows = conn.execute(
        """
        SELECT ym, iso3, hazard_code, metric, series_semantics, value
        FROM facts_resolved
        ORDER BY iso3, metric
        """
    ).fetchall()

    assert rows == [("2024-01", "KEN", "drought", "in_need", "stock", 7)]
