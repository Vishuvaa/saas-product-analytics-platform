"""Repeatable benchmark: full reset -> generate -> load -> pipeline at several dataset sizes.

Usage: python scripts/benchmark.py [--sizes 500 3000] [--out docs/benchmark.md]
Numbers depend entirely on the machine; the report records the environment so results are not over-claimed.
"""

from __future__ import annotations

import argparse
import json
import platform
import time
from datetime import date, datetime, timezone
from pathlib import Path

import duckdb
import pandas as pd

from saas_platform import loader
from saas_platform.config import Settings
from saas_platform.db import connect
from saas_platform.generator import generate
from saas_platform.pipeline import reset_local_data, run_pipeline


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sizes", type=int, nargs="+", default=[500, 3000])
    ap.add_argument("--days", type=int, default=120)
    ap.add_argument("--out", default="docs/benchmark.md")
    args = ap.parse_args()
    st = Settings.from_env()
    rows = []
    for n in args.sizes:
        conn = connect(st)
        loader.reset_db(conn)
        reset_local_data(st)
        t = time.perf_counter()
        data = generate(42, n, date(2025, 1, 1), args.days)
        t_gen = time.perf_counter() - t
        t = time.perf_counter()
        loaded = loader.load_as_of(conn, data)
        t_load = time.perf_counter() - t
        m = run_pipeline(st)
        m2 = run_pipeline(st)  # second run: nothing new to ingest
        src_rows = sum(loaded.values())
        rows.append(
            {
                "users": n,
                "source_rows": src_rows,
                "events": loaded["product_events"],
                "generate_s": round(t_gen, 2),
                "load_to_postgres_s": round(t_load, 2),
                "ingest_s": m["stages"]["ingest"]["seconds"],
                "silver_s": m["stages"]["silver"]["seconds"],
                "gold_s": m["stages"]["gold"]["seconds"],
                "quality_s": m["stages"]["quality"]["seconds"],
                "analytics_s": m["stages"]["analytics"]["seconds"],
                "pipeline_total_s": m["total_seconds"],
                "rerun_total_s_no_new_data": m2["total_seconds"],
                "rows_per_s_pipeline": round(src_rows / max(m["total_seconds"], 1e-9)),
                "quality_checks_passed": f'{m["quality"]["passed"]}/{m["quality"]["total"]}',
            }
        )
        conn.close()
    df = pd.DataFrame(rows)
    env = (
        f"Python {platform.python_version()}, {platform.system()} {platform.release()}, "
        f"pandas {pd.__version__}, duckdb {duckdb.__version__}"
    )
    md = [
        f"# Benchmark ({datetime.now(timezone.utc):%Y-%m-%d})",
        "",
        f"Environment: {env}.",
        "Single run per size, local PostgreSQL, no tuning, no warm-up. Treat as indicative only.",
        "",
        df.to_markdown(index=False) if hasattr(df, "to_markdown") else df.to_string(index=False),
        "",
    ]
    Path(args.out).write_text("\n".join(md), encoding="utf-8")
    print(json.dumps(rows, indent=2))


if __name__ == "__main__":
    main()
