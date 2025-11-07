"""Lightweight diagnostics helpers."""
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Mapping


def write_jsonl(path: str | os.PathLike[str], record: Mapping[str, Any]) -> None:
    """Append a JSON object with a timestamp to ``path`` as JSONL."""
    try:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        payload = dict(record)
        payload.setdefault("_ts", int(time.time()))
        with p.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except Exception:
        # Diagnostics should never block the main flow; swallow errors.
        pass
