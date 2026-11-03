"""Run the analytics SQL queries against the Gold Parquet files using DuckDB."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from saas_platform.config import Settings
from saas_platform.gold import gold_connection
from saas_platform.schema import SQL_DIR


def available_queries() -> list[str]:
    return sorted(p.stem for p in (SQL_DIR / "analytics").glob("*.sql"))


def run_query(settings: Settings, name: str) -> pd.DataFrame:
    sql = (SQL_DIR / "analytics" / f"{name}.sql").read_text(encoding="utf-8")
    con = gold_connection(settings)
    try:
        return con.execute(sql).df()
    finally:
        con.close()


def run_all(settings: Settings, export: bool = True) -> dict[str, pd.DataFrame]:
    results = {n: run_query(settings, n) for n in available_queries()}
    if export:
        out: Path = settings.outputs_dir / "analytics"
        out.mkdir(parents=True, exist_ok=True)
        for name, df in results.items():
            df.to_csv(out / f"{name}.csv", index=False)
    return results
