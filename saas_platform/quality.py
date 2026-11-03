"""Data quality checks + machine-readable report. Raises QualityGateError if any error-severity check fails."""

from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

import duckdb

from saas_platform.config import Settings
from saas_platform.logging_utils import get_logger, kv
from saas_platform.schema import (
    EVENT_TYPES,
    PAYMENT_KINDS,
    PAYMENT_STATUSES,
    PLANS,
    SEGMENTS,
    SOURCE_TABLES,
    SUBSCRIPTION_STATUSES,
)

log = get_logger("quality")


class QualityGateError(RuntimeError):
    def __init__(self, failed: list[str], report_path: str):
        super().__init__(f"Quality gate failed ({len(failed)} checks): {failed}. Report: {report_path}")
        self.failed, self.report_path = failed, report_path


@dataclass
class Check:
    name: str
    category: str
    severity: str  # error | warn
    passed: bool
    observed: object
    expected: object


def _in(values) -> str:
    return ", ".join("'" + v + "'" for v in values)


def run_checks(settings: Settings, conn=None) -> list[Check]:
    s, g, b = settings.silver_dir.as_posix(), settings.gold_dir.as_posix(), settings.bronze_dir.as_posix()
    con = duckdb.connect(":memory:")
    checks: list[Check] = []

    def add(name, category, observed, expected, severity="error", passed=None):
        ok = (observed == expected) if passed is None else passed
        checks.append(Check(name, category, severity, bool(ok), observed, expected))

    def q1(sql):
        return con.execute(sql).fetchone()[0]

    silver_tables = list(SOURCE_TABLES)
    for t in silver_tables:
        con.execute(f"CREATE VIEW s_{t} AS SELECT * FROM read_parquet('{s}/{t}.parquet')")
        con.execute(f"CREATE VIEW q_{t} AS SELECT * FROM read_parquet('{s}/quarantine/{t}.parquet')")
        has_bronze = any((settings.bronze_dir / t).glob("ingest_date=*/batch_*.parquet"))
        if has_bronze:
            con.execute(f"CREATE VIEW b_{t} AS SELECT * FROM read_parquet('{b}/{t}/*/*.parquet')")
        else:
            con.execute(
                f"CREATE VIEW b_{t} AS SELECT 0::BIGINT AS _source_pk, NULL::TIMESTAMP AS _watermark WHERE FALSE"
            )
    for t in [
        "dim_user",
        "dim_date",
        "fact_subscription_plan_period",
        "fact_mrr_movement",
        "fact_payments",
        "fact_product_activity_daily",
    ]:
        con.execute(f"CREATE VIEW g_{t} AS SELECT * FROM read_parquet('{g}/{t}.parquet')")

    # ---- reconciliation: source -> bronze -> silver(+quarantine)
    for t in SOURCE_TABLES:
        if conn is not None:
            with conn.cursor() as cur:
                cur.execute(f"SELECT COUNT(*) FROM src.{t}")
                src_n = cur.fetchone()[0]
            add(
                f"reconcile_source_to_bronze[{t}]",
                "reconciliation",
                q1(f"SELECT COUNT(DISTINCT _source_pk) FROM b_{t}"),
                src_n,
            )
        add(
            f"reconcile_bronze_to_silver[{t}]",
            "reconciliation",
            q1(f"SELECT (SELECT COUNT(*) FROM s_{t}) + (SELECT COUNT(DISTINCT source_pk) FROM q_{t})"),
            q1(f"SELECT COUNT(DISTINCT _source_pk) FROM b_{t}"),
        )

    # ---- uniqueness
    for name, view, cols in [
        ("users.user_id", "s_users", "user_id"),
        ("subscriptions.subscription_id", "s_subscriptions", "subscription_id"),
        ("history.history_id", "s_subscription_plan_history", "history_id"),
        ("payments.payment_id", "s_payments", "payment_id"),
        ("events.event_id", "s_product_events", "event_id"),
        ("events.event_uid", "s_product_events", "event_uid"),
        ("gold.dim_user.user_id", "g_dim_user", "user_id"),
        ("gold.fact_payments.payment_id", "g_fact_payments", "payment_id"),
        (
            "gold.fact_activity.(user_id,activity_date)",
            "g_fact_product_activity_daily",
            "user_id, activity_date",
        ),
        (
            "gold.fact_mrr_movement.(subscription_id,movement_type,event_ts)",
            "g_fact_mrr_movement",
            "subscription_id, movement_type, event_ts",
        ),
    ]:
        dups = q1(f"SELECT COUNT(*) FROM (SELECT {cols} FROM {view} GROUP BY ALL HAVING COUNT(*) > 1)")
        add(f"unique[{name}]", "uniqueness", dups, 0)

    # ---- not null
    for view, cols in [
        ("s_users", ["user_id", "email", "signup_ts"]),
        ("s_subscriptions", ["subscription_id", "user_id", "plan_id", "status", "mrr_amount"]),
        ("s_payments", ["payment_id", "subscription_id", "amount", "paid_at"]),
        ("s_product_events", ["event_id", "event_uid", "user_id", "event_ts", "event_type"]),
    ]:
        for c in cols:
            add(f"not_null[{view}.{c}]", "not_null", q1(f"SELECT COUNT(*) FROM {view} WHERE {c} IS NULL"), 0)

    # ---- accepted values
    for view, col, vals in [
        ("s_users", "segment", list(SEGMENTS) + ["unknown"]),
        ("s_subscriptions", "status", SUBSCRIPTION_STATUSES),
        ("s_subscriptions", "plan_id", list(PLANS)),
        ("s_payments", "kind", PAYMENT_KINDS),
        ("s_payments", "status", PAYMENT_STATUSES),
        ("s_product_events", "event_type", EVENT_TYPES),
    ]:
        add(
            f"accepted_values[{view}.{col}]",
            "accepted_values",
            q1(f"SELECT COUNT(*) FROM {view} WHERE {col} NOT IN ({_in(vals)})"),
            0,
        )

    # ---- referential integrity
    for name, sql in [
        (
            "subscriptions.user_id->users",
            "SELECT COUNT(*) FROM s_subscriptions WHERE user_id NOT IN (SELECT user_id FROM s_users)",
        ),
        (
            "payments.subscription_id->subscriptions",
            "SELECT COUNT(*) FROM s_payments WHERE subscription_id NOT IN (SELECT subscription_id FROM s_subscriptions)",
        ),
        (
            "history.subscription_id->subscriptions",
            "SELECT COUNT(*) FROM s_subscription_plan_history WHERE subscription_id NOT IN (SELECT subscription_id FROM s_subscriptions)",
        ),
        (
            "events.user_id->users",
            "SELECT COUNT(*) FROM s_product_events WHERE user_id NOT IN (SELECT user_id FROM s_users)",
        ),
        (
            "gold.fact_payments.user_id->dim_user",
            "SELECT COUNT(*) FROM g_fact_payments WHERE user_id NOT IN (SELECT user_id FROM g_dim_user)",
        ),
    ]:
        add(f"referential_integrity[{name}]", "referential_integrity", q1(sql), 0)

    # ---- row counts / rejects
    for t in silver_tables:
        observed = q1(f"SELECT COUNT(*) FROM s_{t}")
        pass_if_nonzero = t != "subscription_plan_history"
        add(
            f"row_count_nonzero[{t}]",
            "row_count",
            observed > 0 if pass_if_nonzero else True,
            True,
        )
        total = q1(f"SELECT COUNT(DISTINCT _source_pk) FROM b_{t}") or 1
        invalid = q1(f"SELECT COUNT(*) FROM q_{t} WHERE reject_reason <> 'duplicate_event_uid'")
        rate = round(invalid / total, 4)
        add(
            f"rejected_rate[{t}]",
            "rejected_records",
            rate,
            f"<= {settings.quarantine_max_rate}",
            passed=rate <= settings.quarantine_max_rate,
        )
    add(
        "row_count[gold.fact_payments == silver.payments]",
        "row_count",
        q1("SELECT COUNT(*) FROM g_fact_payments"),
        q1("SELECT COUNT(*) FROM s_payments"),
    )
    add(
        "row_count[gold.dim_user == silver.users]",
        "row_count",
        q1("SELECT COUNT(*) FROM g_dim_user"),
        q1("SELECT COUNT(*) FROM s_users"),
    )
    add(
        "row_count[gold activity events == silver events]",
        "row_count",
        q1("SELECT COALESCE(SUM(events_count),0) FROM g_fact_product_activity_daily"),
        q1("SELECT COUNT(*) FROM s_product_events"),
    )

    # ---- business reconciliation: MRR movements must sum to the current MRR of active subscriptions
    add(
        "reconcile[sum(mrr movements) == active subscription MRR]",
        "reconciliation",
        round(q1("SELECT COALESCE(SUM(mrr_delta),0) FROM g_fact_mrr_movement"), 2),
        round(
            q1(
                "SELECT COALESCE(SUM(mrr_amount),0) FROM s_subscriptions WHERE started_at IS NOT NULL AND status <> 'canceled'"
            ),
            2,
        ),
    )

    # ---- duplicate processing (same source row ingested into more than one bronze batch)
    for t in silver_tables:
        dups = q1(
            f"SELECT COUNT(*) FROM (SELECT _source_pk, _watermark FROM b_{t} GROUP BY ALL HAVING COUNT(*) > 1)"
        )
        add(f"duplicate_processing[{t}]", "duplicate_processing", dups, 0, severity="warn")

    # ---- freshness (needs the database)
    if conn is not None:
        for t, spec in SOURCE_TABLES.items():
            with conn.cursor() as cur:
                cur.execute(f"SELECT MAX({spec.watermark}) FROM src.{t}")
                src_max = cur.fetchone()[0]
                cur.execute("SELECT last_wm, last_run_at FROM etl.watermarks WHERE table_name=%s", (t,))
                row = cur.fetchone()
            wm, last_run = row if row else (None, None)
            lag = 0 if src_max is None else (src_max != wm)
            add(
                f"freshness_lag[{t}]",
                "freshness",
                "source has rows not yet ingested" if lag else "up to date",
                "up to date",
                passed=not lag,
            )
            age_h = (
                None
                if last_run is None
                else round((datetime.now(timezone.utc) - last_run).total_seconds() / 3600, 3)
            )
            add(
                f"freshness_last_run_age_hours[{t}]",
                "freshness",
                age_h,
                f"<= {settings.freshness_max_hours}",
                passed=age_h is not None and age_h <= settings.freshness_max_hours,
            )
    con.close()
    return checks


def run_quality(settings: Settings, conn=None, run_id: str = "adhoc", metrics: dict | None = None) -> dict:
    started = datetime.now(timezone.utc)
    checks = run_checks(settings, conn)
    failed_err = [c.name for c in checks if not c.passed and c.severity == "error"]
    failed_warn = [c.name for c in checks if not c.passed and c.severity == "warn"]
    report = {
        "run_id": run_id,
        "generated_at": started.isoformat(),
        "status": "failed" if failed_err else "passed",
        "summary": {
            "total": len(checks),
            "passed": sum(c.passed for c in checks),
            "failed_errors": len(failed_err),
            "failed_warnings": len(failed_warn),
        },
        "failed_error_checks": failed_err,
        "failed_warning_checks": failed_warn,
        "metrics": metrics or {},
        "checks": [asdict(c) for c in checks],
    }
    settings.quality_dir.mkdir(parents=True, exist_ok=True)
    path = settings.quality_dir / f"quality_report_{run_id}.json"
    for p in (path, settings.quality_dir / "latest.json"):
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        os.replace(tmp, p)
    log.info(kv(event="quality_done", status=report["status"], **report["summary"]))
    if failed_err:
        raise QualityGateError(failed_err, str(path))
    return report
