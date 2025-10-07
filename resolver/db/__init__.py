"""Database utilities for the resolver package."""

from .duckdb_io import write_snapshot, upsert_dataframe  # noqa: F401

__all__ = ["write_snapshot", "upsert_dataframe"]
