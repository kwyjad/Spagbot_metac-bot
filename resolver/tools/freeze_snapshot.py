#!/usr/bin/env python3
"""freeze_snapshot.py — validate and freeze a monthly snapshot.

In addition to the historical filesystem output (Parquet facts, optional
deltas and a JSON manifest) the freezer can write directly to DuckDB when a
``RESOLVER_DB_URL`` is provided.  All DuckDB writes are channelled through
``resolver.db.duckdb_io.write_snapshot`` so that canonicalisation and
delete/insert behaviour remains consistent between the exporter and the
freezer.
"""

import argparse
import datetime as dt
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    print("Please 'pip install pandas pyarrow' to run the freezer.", file=sys.stderr)
    sys.exit(2)

ROOT = Path(__file__).resolve().parents[1]      # .../resolver
TOOLS = ROOT / "tools"
SNAPSHOTS = ROOT / "snapshots"
VALIDATOR = TOOLS / "validate_facts.py"

try:
    from resolver.db import duckdb_io
except Exception:  # pragma: no cover - optional when DuckDB isn't available
    duckdb_io = None  # type: ignore[assignment]

def run_validator(facts_path: Path) -> None:
    """Invoke the validator script as a subprocess for simplicity."""
    if not VALIDATOR.exists():
        print(f"Validator not found at {VALIDATOR}", file=sys.stderr)
        sys.exit(2)
    cmd = [sys.executable, str(VALIDATOR), "--facts", str(facts_path)]
    res = subprocess.run(cmd)
    if res.returncode != 0:
        print("Validation failed; aborting snapshot.", file=sys.stderr)
        sys.exit(res.returncode)

def load_table(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext in [".csv", ".tsv"]:
        return pd.read_csv(path, dtype=str).fillna("")
    elif ext == ".parquet":
        return pd.read_parquet(path)
    else:
        raise SystemExit(f"Unsupported input extension: {ext}. Use .csv or .parquet")

def write_parquet(df: pd.DataFrame, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    # Ensure string columns are strings (avoid mixed dtypes)
    for c in df.columns:
        if df[c].dtype.name not in ("float64","int64","bool"):
            df[c] = df[c].astype(str)
    df.to_parquet(out_path, index=False)


def _maybe_write_db(
    ym: str,
    facts_df: pd.DataFrame,
    deltas_df: pd.DataFrame | None,
    manifest: dict,
    *,
    db_url: str | None = None,
) -> None:
    """Write the snapshot payload to DuckDB when configured."""

    if duckdb_io is None:
        return

    db_url = db_url or os.environ.get("RESOLVER_DB_URL")
    if not db_url:
        return

    manifests_df = pd.DataFrame([manifest])
    meta = {
        "git_sha": manifest.get("source_commit_sha") or os.environ.get("GITHUB_SHA", ""),
        "export_version": manifest.get("source_commit_sha", ""),
        "rows": manifest.get("rows", 0),
    }
    duckdb_io.write_snapshot(
        db_url,
        ym,
        facts_df,
        facts_deltas=deltas_df,
        manifests=manifests_df,
        meta=meta,
    )

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--facts", required=True, help="Path to the facts CSV/Parquet to freeze")
    ap.add_argument("--month", help="Target month YYYY-MM; defaults to current UTC year-month")
    ap.add_argument("--overwrite", action="store_true", help="Allow overwriting existing snapshot files")
    ap.add_argument(
        "--deltas",
        help="Optional path to deltas.csv to include in the snapshot (defaults to sibling of --facts)",
    )
    args = ap.parse_args()

    facts_path = Path(args.facts)
    if not facts_path.exists():
        print(f"Facts not found: {facts_path}", file=sys.stderr)
        sys.exit(2)

    # Validate first
    run_validator(facts_path)

    # Determine month partition
    if args.month:
        try:
            year, month = map(int, args.month.split("-"))
            dt.date(year, month, 1)  # sanity check
            ym = f"{year:04d}-{month:02d}"
        except Exception:
            print("--month must be YYYY-MM (e.g., 2025-09)", file=sys.stderr)
            sys.exit(2)
    else:
        now = dt.datetime.utcnow()
        ym = f"{now.year:04d}-{now.month:02d}"

    out_dir = SNAPSHOTS / ym
    facts_out = out_dir / "facts.parquet"
    manifest_out = out_dir / "manifest.json"

    if args.deltas:
        deltas_path = Path(args.deltas)
        if not deltas_path.exists():
            print(f"Deltas file not found: {deltas_path}", file=sys.stderr)
            sys.exit(2)
    else:
        default_deltas = facts_path.with_name("deltas.csv")
        deltas_path = default_deltas if default_deltas.exists() else None

    deltas_out = out_dir / "deltas.csv" if deltas_path else None

    existing_targets = [p for p in [facts_out, manifest_out, deltas_out] if p and p.exists()]
    if existing_targets and not args.overwrite:
        print(f"Snapshot already exists for {ym}: {out_dir}", file=sys.stderr)
        print("Use --overwrite to replace.", file=sys.stderr)
        sys.exit(1)

    # Load and write snapshot
    df = load_table(facts_path)
    write_parquet(df, facts_out)

    # Build manifest
    manifest = {
        "created_at_utc": dt.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source_file": str(facts_path),
        "target_month": ym,
        # If running in CI, this env var will auto-populate
        "source_commit_sha": os.environ.get("GITHUB_SHA", ""),
        "rows": int(len(df)),
    }
    out_dir.mkdir(parents=True, exist_ok=True)
    with open(manifest_out, "w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2)

    if deltas_path:
        deltas_df = load_table(deltas_path)
        shutil.copy2(deltas_path, deltas_out)
    else:
        deltas_df = None

    _maybe_write_db(ym, df, deltas_df, manifest)

    print(f"✅ Snapshot written:\n - {facts_out}\n - {manifest_out}")
    if deltas_out:
        print(f" - {deltas_out}")

if __name__ == "__main__":
    main()
