"""Command line interface: python -m saas_platform <command>."""

from __future__ import annotations

import argparse
import json
from datetime import date, timedelta

from saas_platform import analytics, gold, ingestion, loader, silver
from saas_platform.config import Settings
from saas_platform.db import wait_for_db
from saas_platform.generator import generate
from saas_platform.pipeline import reset_local_data, run_pipeline
from saas_platform.quality import QualityGateError, run_quality


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="saas-platform")
    sub = p.add_subparsers(dest="cmd", required=True)
    sub.add_parser("init-db", help="create source + etl schemas (idempotent)")
    g = sub.add_parser("generate", help="generate synthetic data and load it into PostgreSQL")
    g.add_argument("--seed", type=int, default=42)
    g.add_argument("--users", type=int, default=500)
    g.add_argument("--days", type=int, default=120)
    g.add_argument("--start", type=date.fromisoformat, default=date(2025, 1, 1))
    g.add_argument("--until-day", type=int, default=None, help="only load rows visible by this day offset")
    g.add_argument("--add-device-column", action="store_true", help="apply the additive schema change first")
    for name in ("ingest", "silver", "gold", "quality", "analytics", "run"):
        sp = sub.add_parser(name)
        if name == "analytics":
            sp.add_argument("--show", type=int, default=5, help="rows to print per query")
    r = sub.add_parser("reset", help="drop src/etl schemas and delete local generated data")
    r.add_argument("--yes", action="store_true")
    args = p.parse_args(argv)
    st = Settings.from_env()

    if args.cmd == "reset":
        if not args.yes:
            print("Refusing to reset without --yes (drops schemas src, etl and deletes local data layers).")
            return 2
        conn = wait_for_db(st)
        loader.reset_db(conn)
        print("removed:", reset_local_data(st))
        return 0
    if args.cmd in ("silver", "gold", "analytics"):
        if args.cmd == "silver":
            print(json.dumps(silver.build_silver(st), indent=2, default=str))
        elif args.cmd == "gold":
            print(gold.build_gold(st))
        else:
            for name, df in analytics.run_all(st).items():
                print(f"\n=== {name} ({len(df)} rows) ===")
                print(df.head(args.show).to_string(index=False))
        return 0

    conn = wait_for_db(st)
    try:
        if args.cmd == "init-db":
            loader.init_db(conn)
            print("schemas ready")
        elif args.cmd == "generate":
            loader.init_db(conn)
            if args.add_device_column:
                loader.apply_additive_schema_change(conn)
            data = generate(args.seed, args.users, args.start, args.days)
            until = None if args.until_day is None else data.start + timedelta(days=args.until_day)
            print("loaded rows:", loader.load_as_of(conn, data, until))
        elif args.cmd == "ingest":
            print({k: v.rows for k, v in ingestion.ingest_all(conn, st).items()})
        elif args.cmd == "quality":
            try:
                rep = run_quality(st, conn)
            except QualityGateError as exc:
                print(f"QUALITY GATE FAILED: {exc}")
                return 1
            print(json.dumps(rep["summary"]), "->", st.quality_dir / "latest.json")
        elif args.cmd == "run":
            try:
                m = run_pipeline(st)
            except QualityGateError as exc:
                print(f"QUALITY GATE FAILED: {exc}")
                return 1
            print(
                json.dumps(
                    {
                        "run_id": m["run_id"],
                        "total_seconds": m["total_seconds"],
                        "quality": m["quality"],
                        "ingested": m["stages"]["ingest"]["rows"],
                    },
                    indent=2,
                )
            )
    finally:
        conn.close()
    return 0
