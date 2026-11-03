"""Gold layer: analytics-ready dimensions and facts built with DuckDB SQL over Silver Parquet."""

from __future__ import annotations

import os
from pathlib import Path

import duckdb

from saas_platform.config import Settings
from saas_platform.logging_utils import get_logger, kv
from saas_platform.schema import SQL_DIR

log = get_logger("gold")
SILVER_VIEWS = {
    "s_users": "users",
    "s_subscriptions": "subscriptions",
    "s_history": "subscription_plan_history",
    "s_payments": "payments",
    "s_events": "product_events",
}
GOLD_TABLES = [
    "dim_plan",
    "dim_user",
    "dim_date",
    "fact_subscription_plan_period",
    "fact_mrr_movement",
    "fact_payments",
    "fact_product_activity_daily",
]


def silver_connection(settings: Settings) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(":memory:")
    for view, table in SILVER_VIEWS.items():
        path = (settings.silver_dir / f"{table}.parquet").as_posix()
        con.execute(f"CREATE VIEW {view} AS SELECT * FROM read_parquet('{path}')")
    return con


def build_gold(settings: Settings) -> dict[str, int]:
    con = silver_connection(settings)
    settings.gold_dir.mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}
    for sql_file in sorted((SQL_DIR / "gold").glob("*.sql")):
        name = sql_file.stem.split("_", 1)[1]  # strip numeric prefix
        con.execute(f"CREATE OR REPLACE TABLE gold_{name} AS {sql_file.read_text(encoding='utf-8')}")
        out = settings.gold_dir / f"{name}.parquet"
        tmp = out.with_suffix(".tmp")
        con.execute(f"COPY (SELECT * FROM gold_{name} ORDER BY ALL) TO '{tmp.as_posix()}' (FORMAT PARQUET)")
        os.replace(tmp, out)
        counts[name] = con.execute(f"SELECT COUNT(*) FROM gold_{name}").fetchone()[0]
        log.info(kv(event="gold_built", table=name, rows=counts[name]))
    con.close()
    return counts


def gold_connection(settings: Settings) -> duckdb.DuckDBPyConnection:
    """DuckDB connection with a view gold_<name> over each gold Parquet file."""
    con = duckdb.connect(":memory:")
    for name in GOLD_TABLES:
        path = Path(settings.gold_dir / f"{name}.parquet").as_posix()
        con.execute(f"CREATE VIEW gold_{name} AS SELECT * FROM read_parquet('{path}')")
    return con
