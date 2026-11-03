"""Bronze layer: raw, immutable, replayable Parquet batches with lineage metadata."""

from __future__ import annotations

import json
import os
from datetime import datetime
from decimal import Decimal
from pathlib import Path

import pandas as pd

META_COLS = ["_source_table", "_source_pk", "_watermark", "_batch_id", "_run_id", "_ingested_at", "payload"]


def _json_default(o):
    if isinstance(o, (datetime,)):
        return o.isoformat()
    if isinstance(o, Decimal):
        return str(o)
    return str(o)


def to_payload(row: dict) -> str:
    return json.dumps(row, sort_keys=True, default=_json_default)


def make_bronze_frame(
    table: str, pk: str, wm_col: str, rows: list[dict], batch_id: str, run_id: str, ingested_at: datetime
) -> pd.DataFrame:
    """Build a bronze frame from source rows (also used by unit tests)."""
    return pd.DataFrame(
        {
            "_source_table": table,
            "_source_pk": [int(r[pk]) for r in rows],
            "_watermark": pd.to_datetime([r[wm_col] for r in rows], utc=True).tz_convert(None),
            "_batch_id": batch_id,
            "_run_id": run_id,
            "_ingested_at": pd.Timestamp(ingested_at).tz_convert(None)
            if pd.Timestamp(ingested_at).tzinfo
            else pd.Timestamp(ingested_at),
            "payload": [to_payload(r) for r in rows],
        },
        columns=META_COLS,
    )


def batch_path(bronze_dir: Path, table: str, ingested_at: datetime, batch_id: str) -> Path:
    return bronze_dir / table / f"ingest_date={ingested_at:%Y-%m-%d}" / f"batch_{batch_id}.parquet"


def write_batch(df: pd.DataFrame, bronze_dir: Path, table: str, ingested_at: datetime, batch_id: str) -> Path:
    """Atomic write. A re-run of the same batch replaces the old file (even in another date partition)."""
    for old in (bronze_dir / table).glob(f"ingest_date=*/batch_{batch_id}.parquet"):
        old.unlink()
    path = batch_path(bronze_dir, table, ingested_at, batch_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    df.to_parquet(tmp, index=False)
    os.replace(tmp, path)
    return path


def read_table(bronze_dir: Path, table: str) -> pd.DataFrame:
    files = sorted((bronze_dir / table).glob("ingest_date=*/batch_*.parquet"))
    if not files:
        return pd.DataFrame(columns=META_COLS)
    return pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
