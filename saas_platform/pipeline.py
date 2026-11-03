"""Pipeline stages as plain functions (the Airflow DAG and the CLI both call these)."""

from __future__ import annotations

import json
import shutil
import time
import uuid
from datetime import datetime, timezone

from saas_platform import analytics, gold, ingestion, silver
from saas_platform.config import Settings
from saas_platform.db import connect
from saas_platform.logging_utils import get_logger, kv
from saas_platform.quality import run_quality

log = get_logger("pipeline")


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S") + "_" + uuid.uuid4().hex[:4]


def run_pipeline(
    settings: Settings, run_id: str | None = None, skip_quality_gate_raise: bool = False
) -> dict:
    run_id = run_id or new_run_id()
    metrics: dict = {"run_id": run_id, "stages": {}}
    conn = connect(settings)
    try:
        t = time.perf_counter()
        ing = ingestion.ingest_all(conn, settings, run_id)
        metrics["stages"]["ingest"] = {
            "seconds": round(time.perf_counter() - t, 3),
            "rows": {k: v.rows for k, v in ing.items()},
            "extra_columns": {k: v.extra_columns for k, v in ing.items() if v.extra_columns},
        }
        t = time.perf_counter()
        sv = silver.build_silver(settings)
        metrics["stages"]["silver"] = {"seconds": round(time.perf_counter() - t, 3), "tables": sv}
        t = time.perf_counter()
        gd = gold.build_gold(settings)
        metrics["stages"]["gold"] = {"seconds": round(time.perf_counter() - t, 3), "rows": gd}
        t = time.perf_counter()
        try:
            report = run_quality(settings, conn, run_id, metrics)
        finally:
            metrics["stages"]["quality"] = {"seconds": round(time.perf_counter() - t, 3)}
        t = time.perf_counter()
        analytics.run_all(settings)
        metrics["stages"]["analytics"] = {"seconds": round(time.perf_counter() - t, 3)}
        metrics["total_seconds"] = round(sum(s["seconds"] for s in metrics["stages"].values()), 3)
        (settings.quality_dir / f"run_metrics_{run_id}.json").write_text(
            json.dumps(metrics, indent=2, default=str)
        )
        log.info(kv(event="pipeline_done", run_id=run_id, total_seconds=metrics["total_seconds"]))
        metrics["quality"] = report["summary"]
        return metrics
    finally:
        conn.close()


def reset_local_data(settings: Settings) -> list[str]:
    """Delete generated layer folders inside DATA_DIR only (never anything outside it)."""
    removed = []
    for sub in ("bronze", "silver", "gold", "quality", "outputs"):
        p = settings.data_dir / sub
        if p.exists():
            shutil.rmtree(p)
            removed.append(str(p))
    return removed
