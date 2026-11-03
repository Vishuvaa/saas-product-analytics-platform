"""Airflow DAG. Contains NO business logic: every task calls a plain, unit-tested function in saas_platform.

ingest_* (parallel) -> build_silver -> build_gold -> quality_gate -> export_analytics
Retries: 2 (2 min apart). Quality failures raise, so the task and DAG run show as FAILED.
Silver/Gold are deterministic full rebuilds from Bronze, so retries, clears and backfills are safe/idempotent.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from saas_platform import analytics, gold, ingestion, silver
from saas_platform.config import Settings
from saas_platform.db import connect
from saas_platform.quality import run_quality
from saas_platform.schema import SOURCE_TABLES

DEFAULT_ARGS = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=30),
}


def _ingest(table: str, run_id: str) -> None:
    settings = Settings.from_env()
    conn = connect(settings)
    try:
        ingestion.ingest_table(conn, settings, table, run_id)
    finally:
        conn.close()


def _silver() -> None:
    silver.build_silver(Settings.from_env())


def _gold() -> None:
    gold.build_gold(Settings.from_env())


def _quality(run_id: str) -> None:
    settings = Settings.from_env()
    conn = connect(settings)
    try:
        run_quality(
            settings, conn, run_id.replace(":", "").replace("+", "_")
        )  # raises QualityGateError on failure
    finally:
        conn.close()


def _export() -> None:
    analytics.run_all(Settings.from_env())


with DAG(
    dag_id="saas_product_analytics",
    description="Postgres -> Bronze -> Silver -> Gold (Parquet) -> quality gate -> DuckDB analytics",
    default_args=DEFAULT_ARGS,
    schedule=os.environ.get("PIPELINE_SCHEDULE", "@daily"),  # configurable processing interval
    start_date=datetime(2025, 1, 1),
    catchup=False,  # set True + `airflow dags backfill` to reprocess history; tasks are idempotent
    max_active_runs=1,
    tags=["portfolio", "data-engineering"],
) as dag:
    ingest_tasks = [
        PythonOperator(
            task_id=f"ingest_{t}", python_callable=_ingest, op_kwargs={"table": t, "run_id": "{{ run_id }}"}
        )
        for t in SOURCE_TABLES
    ]
    build_silver = PythonOperator(task_id="build_silver", python_callable=_silver)
    build_gold = PythonOperator(task_id="build_gold", python_callable=_gold)
    quality_gate = PythonOperator(
        task_id="quality_gate", python_callable=_quality, op_kwargs={"run_id": "{{ run_id }}"}
    )
    export_analytics = PythonOperator(task_id="export_analytics", python_callable=_export)

    ingest_tasks >> build_silver >> build_gold >> quality_gate >> export_analytics
