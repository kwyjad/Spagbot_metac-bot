"""DuckDB helpers for resolver snapshots and batch upserts.

This module intentionally keeps all DuckDB writes in one place so that
we can reason about canonicalisation, de-duplication, and delete scope
in a single spot.  The helper functions are a lightweight port of the
behaviour exercised in resolver's DuckDB integration tests.
"""

from __future__ import annotations

import json
import logging
import math
import uuid
from typing import Iterable, Mapping, MutableMapping, Optional, Sequence

import duckdb
import pandas as pd

LOGGER = logging.getLogger(__name__)

# Natural key for resolved facts tables used throughout the snapshot flow.
FACTS_RESOLVED_KEYS: Sequence[str] = (
    "ym",
    "iso3",
    "hazard_code",
    "metric",
    "series_semantics",
)


def _is_missing(value: object) -> bool:
    """Return True if ``value`` should be treated as NULL/empty."""

    if value is None:
        return True
    if isinstance(value, float) and math.isnan(value):
        return True
    if isinstance(value, str) and value.strip() == "":
        return True
    return False


def _canonicalise_series_semantics(value: object) -> str:
    """Map any incoming value onto "", "new" or "stock".

    The upstream data sources have a variety of string formats that we fold
    into the canonical set expected by our schema.  Anything we do not
    understand falls back to the trimmed, lower-cased string to avoid data
    loss, but in practice the recognised values cover the real inputs.
    """

    if _is_missing(value):
        return ""

    text = str(value).strip()
    if not text:
        return ""

    lowered = text.lower()
    if lowered in {"none", "null", "nan"}:
        return ""
    if "stock" in lowered:
        return "stock"
    if "new" in lowered or "flow" in lowered:
        return "new"
    if lowered in {"stock", "stocks"}:
        return "stock"
    if lowered in {"new", "new_flow", "newflow"}:
        return "new"
    # Fallback: keep the trimmed, lower-cased value to avoid surprising
    # behaviour, but log so that we can expand the mapping when needed.
    LOGGER.debug("Unrecognised series_semantics value %r; keeping %s", value, lowered)
    return lowered


def _normalise_key_columns(frame: pd.DataFrame, keys: Sequence[str]) -> pd.DataFrame:
    """Trim and string-cast key columns to ensure deterministic comparisons."""

    normalised = frame.copy()
    for column in keys:
        if column not in normalised.columns:
            normalised[column] = ""
        normalised[column] = normalised[column].astype(str).str.strip()
    if "series_semantics" in normalised.columns:
        normalised["series_semantics"] = normalised["series_semantics"].map(
            _canonicalise_series_semantics
        )
    return normalised


def _prepare_for_upsert(frame: pd.DataFrame, keys: Sequence[str]) -> pd.DataFrame:
    """Return a copy of ``frame`` canonicalised and de-duplicated on ``keys``."""

    if frame is None:
        raise ValueError("frame must not be None")
    prepared = _normalise_key_columns(frame, keys)
    if keys:
        prepared = prepared.drop_duplicates(subset=list(keys), keep="last")
    return prepared


def _register_temp_table(conn: duckdb.DuckDBPyConnection, frame: pd.DataFrame) -> str:
    """Register ``frame`` as a temporary DuckDB view and return its name."""

    temp_name = f"temp_{uuid.uuid4().hex}"
    conn.register(temp_name, frame)
    return temp_name


def upsert_dataframe(
    conn: duckdb.DuckDBPyConnection,
    table: str,
    frame: pd.DataFrame,
    *,
    keys: Sequence[str],
    drop_columns: Optional[Iterable[str]] = None,
) -> int:
    """Upsert ``frame`` into ``table`` using ``keys`` as the natural key.

    The function performs a two-step write consisting of a delete scoped to
    the key columns followed by an insert from a registered temporary view.
    ``series_semantics`` is canonicalised *before* the de-duplication so that
    mixed representations (e.g. "Stock estimate" and "stock") collapse onto a
    single key prior to the write.
    """

    if frame is None or frame.empty:
        LOGGER.debug("Nothing to upsert into %s; skipping", table)
        return 0

    if not keys:
        raise ValueError("keys must not be empty")

    prepared = _prepare_for_upsert(frame, keys)

    if drop_columns:
        drop_list = [col for col in drop_columns if col in prepared.columns]
        if drop_list:
            LOGGER.debug(
                "Dropping columns %s before writing to %s (expected for staging-only fields)",
                drop_list,
                table,
            )
            prepared = prepared.drop(columns=drop_list)

    table_columns = [
        row[0]
        for row in conn.execute("SELECT name FROM pragma_table_info(?)", [table]).fetchall()
    ]
    insert_columns = [column for column in prepared.columns if column in table_columns]
    if not insert_columns:
        raise ValueError(
            f"No overlapping columns between prepared frame and DuckDB table {table!r}"
        )

    temp_name = _register_temp_table(conn, prepared)

    key_predicates = [
        f"coalesce(t.{column}, '') = coalesce(s.{column}, '')" for column in keys
    ]
    where_clause = " AND ".join(key_predicates)

    count_query = (
        f'SELECT COUNT(*) FROM "{table}" AS t '
        f'WHERE EXISTS (SELECT 1 FROM "{temp_name}" AS s WHERE {where_clause})'
    )
    delete_query = (
        f'DELETE FROM "{table}" AS t '
        f'WHERE EXISTS (SELECT 1 FROM "{temp_name}" AS s WHERE {where_clause})'
    )
    columns_csv = ", ".join(f'"{column}"' for column in insert_columns)
    insert_query = (
        f'INSERT INTO "{table}" ({columns_csv}) SELECT {columns_csv} FROM "{temp_name}"'
    )

    try:
        count = conn.execute(count_query).fetchone()[0]
        LOGGER.info("Deleted %s existing rows from %s before upsert", count, table)
        conn.execute(delete_query)
        conn.execute(insert_query)
    finally:
        conn.unregister(temp_name)

    return len(prepared)


def _delete_month(conn: duckdb.DuckDBPyConnection, table: str, ym: str) -> None:
    conn.execute(f'DELETE FROM "{table}" WHERE ym = ?', [ym])


def _manifest_to_dataframe(manifests: Optional[pd.DataFrame], ym: str) -> Optional[pd.DataFrame]:
    if manifests is None:
        return None
    if isinstance(manifests, pd.DataFrame):
        manifest_df = manifests.copy()
    else:
        manifest_df = pd.DataFrame(manifests)
    if "ym" not in manifest_df.columns:
        manifest_df["ym"] = ym
    return manifest_df


def _ensure_connection(conn_or_url: Optional[object]) -> duckdb.DuckDBPyConnection:
    if isinstance(conn_or_url, duckdb.DuckDBPyConnection):
        return conn_or_url
    if conn_or_url is None:
        raise ValueError("A DuckDB connection or database path is required")
    if isinstance(conn_or_url, str):
        if conn_or_url.startswith("duckdb:///"):
            path = conn_or_url.split("duckdb:///")[1]
        else:
            path = conn_or_url
        return duckdb.connect(path)
    raise TypeError(f"Unsupported connection type: {type(conn_or_url)!r}")


def write_snapshot(
    conn_or_url: object,
    ym: str,
    facts_resolved: Optional[pd.DataFrame],
    facts_deltas: Optional[pd.DataFrame] = None,
    manifests: Optional[pd.DataFrame] = None,
    meta: Optional[Mapping[str, object]] = None,
) -> MutableMapping[str, int]:
    """Write a resolver snapshot for ``ym`` into DuckDB.

    The snapshot write uses month-scoped deletes for the facts tables and
    records a summary row in the ``snapshots`` table.  ``facts_resolved`` and
    ``facts_deltas`` are canonicalised and de-duplicated on their natural
    keys prior to the write.
    """

    conn = _ensure_connection(conn_or_url)
    summary: MutableMapping[str, int] = {
        "facts_resolved_rows": 0,
        "facts_deltas_rows": 0,
        "manifests_rows": 0,
    }

    conn.execute("BEGIN")
    try:
        if facts_resolved is not None and not facts_resolved.empty:
            prepared_resolved = _prepare_for_upsert(facts_resolved, FACTS_RESOLVED_KEYS)
            _delete_month(conn, "facts_resolved", ym)
            summary["facts_resolved_rows"] = upsert_dataframe(
                conn,
                "facts_resolved",
                prepared_resolved,
                keys=FACTS_RESOLVED_KEYS,
            )
        else:
            conn.execute('DELETE FROM "facts_resolved" WHERE ym = ?', [ym])

        if facts_deltas is not None and not facts_deltas.empty:
            delta_keys = [
                column
                for column in (
                    "ym",
                    "iso3",
                    "hazard_code",
                    "metric",
                    "series_semantics",
                    "delta_type",
                )
                if column in facts_deltas.columns
            ]
            if not delta_keys:
                delta_keys = FACTS_RESOLVED_KEYS
            prepared_deltas = _prepare_for_upsert(facts_deltas, delta_keys)
            _delete_month(conn, "facts_deltas", ym)
            summary["facts_deltas_rows"] = upsert_dataframe(
                conn,
                "facts_deltas",
                prepared_deltas,
                keys=delta_keys,
            )
        elif facts_deltas is not None:
            conn.execute('DELETE FROM "facts_deltas" WHERE ym = ?', [ym])

        manifest_df = _manifest_to_dataframe(manifests, ym)
        if manifest_df is not None and not manifest_df.empty:
            manifest_keys = [
                column
                for column in ("ym", "name", "path", "kind")
                if column in manifest_df.columns
            ]
            if not manifest_keys:
                manifest_keys = manifest_df.columns.tolist()
            summary["manifests_rows"] = upsert_dataframe(
                conn,
                "manifests",
                manifest_df,
                keys=manifest_keys,
            )

        # Snapshot summary row.  Delete any existing month entries first to
        # keep the table idempotent.
        conn.execute('DELETE FROM "snapshots" WHERE ym = ?', [ym])
        summary_row: MutableMapping[str, object] = {
            "ym": ym,
            "facts_rows": summary["facts_resolved_rows"],
            "deltas_rows": summary["facts_deltas_rows"],
            "manifests_rows": summary["manifests_rows"],
            "meta": json.dumps(meta or {}, sort_keys=True),
        }
        if meta:
            for field in ("git_sha", "export_version"):
                if field in meta:
                    summary_row[field] = meta[field]
        columns = ", ".join(summary_row.keys())
        placeholders = ", ".join(["?"] * len(summary_row))
        conn.execute(
            f'INSERT INTO "snapshots" ({columns}) VALUES ({placeholders})',
            list(summary_row.values()),
        )
        conn.execute("COMMIT")
    except Exception:
        conn.execute("ROLLBACK")
        raise

    return summary


def schema_initialised(conn: duckdb.DuckDBPyConnection) -> bool:
    """Fast-path check used by tests to confirm schema bootstrap."""

    required_tables = {"facts_resolved", "facts_deltas", "manifests", "snapshots"}
    existing = {
        row[0]
        for row in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'main'"
        ).fetchall()
    }
    if required_tables.issubset(existing):
        LOGGER.debug("DuckDB schema already initialised; skipping DDL execution")
        return True
    return False
