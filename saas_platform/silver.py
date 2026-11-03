"""Silver layer: parse, normalize, deduplicate, validate; invalid rows go to quarantine with a reason.

Silver is rebuilt deterministically from Bronze on every run (full replay), so reruns are idempotent and
late-arriving rows are always placed correctly. All timestamps are stored as naive UTC.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

import pandas as pd

from saas_platform import bronze
from saas_platform.config import Settings
from saas_platform.logging_utils import get_logger, kv
from saas_platform.schema import (
    EVENT_TYPES,
    PAYMENT_KINDS,
    PAYMENT_STATUSES,
    PLANS,
    SEGMENTS,
    SUBSCRIPTION_STATUSES,
)

log = get_logger("silver")
LINEAGE = ["_source_pk", "_batch_id", "_ingested_at"]
QUARANTINE_COLS = ["source_table", "source_pk", "reject_reason", "payload", "batch_id", "ingested_at"]
LATE_AFTER = pd.Timedelta(days=1)


def _ts(s: pd.Series) -> pd.Series:
    return pd.to_datetime(s, utc=True, errors="coerce", format="mixed").dt.tz_convert(None)


def parse_payload(raw: pd.DataFrame) -> pd.DataFrame:
    """Expand the JSON payload into columns, keeping bronze lineage columns."""
    if raw.empty:
        return raw.copy()
    body = pd.DataFrame([json.loads(p) for p in raw["payload"]], index=raw.index)
    meta = raw[["_source_pk", "_batch_id", "_ingested_at", "_watermark", "payload"]]
    return pd.concat([body, meta], axis=1)


def _apply_rules(df: pd.DataFrame, rules: list[tuple[pd.Series, str]]) -> pd.Series:
    reason = pd.Series(pd.NA, index=df.index, dtype="object")
    for mask, text in rules:
        reason = reason.where(reason.notna() | ~mask.fillna(True), text)
    return reason


def _split(df: pd.DataFrame, reason: pd.Series, table: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    bad = reason.notna()
    q = pd.DataFrame(
        {
            "source_table": table,
            "source_pk": df.loc[bad, "_source_pk"],
            "reject_reason": reason[bad],
            "payload": df.loc[bad, "payload"],
            "batch_id": df.loc[bad, "_batch_id"],
            "ingested_at": df.loc[bad, "_ingested_at"],
        },
        columns=QUARANTINE_COLS,
    )
    return df.loc[~bad].copy(), q


def _latest_version(df: pd.DataFrame, pk: str, order_col: str) -> pd.DataFrame:
    """Keep the newest version of each business key (ties broken by ingestion time, then batch id)."""
    return (
        df.sort_values([pk, order_col, "_ingested_at", "_batch_id"])
        .drop_duplicates(pk, keep="last")
        .reset_index(drop=True)
    )


def clean_users(raw: pd.DataFrame):
    df = parse_payload(raw)
    if df.empty:
        return _empty(
            [
                "user_id",
                "email",
                "signup_ts",
                "country",
                "segment",
                "acquisition_channel",
                "updated_at",
                "_batch_id",
                "_ingested_at",
            ],
            {
                "user_id": "int64",
                "email": "string",
                "signup_ts": "datetime64[ns]",
                "country": "string",
                "segment": "string",
                "acquisition_channel": "string",
                "updated_at": "datetime64[ns]",
                "_batch_id": "string",
                "_ingested_at": "datetime64[ns]",
            },
        ), _empty_q()
    df["user_id"] = pd.to_numeric(df["user_id"], errors="coerce")
    df["signup_ts"], df["updated_at"] = _ts(df["signup_ts"]), _ts(df["updated_at"])
    df = _latest_version(df, "_source_pk", "updated_at")
    df["segment_n"] = df["segment"].astype("string").str.strip().str.lower()
    reason = _apply_rules(
        df,
        [
            (df["user_id"].isna(), "missing_user_id"),
            (df["email"].isna() | ~df["email"].astype("string").str.contains("@", na=False), "invalid_email"),
            (df["signup_ts"].isna(), "invalid_signup_ts"),
            (df["segment_n"].notna() & ~df["segment_n"].isin(SEGMENTS), "invalid_segment"),
        ],
    )
    ok, q = _split(df, reason, "users")
    out = (
        pd.DataFrame(
            {
                "user_id": ok["user_id"].astype("int64"),
                "email": ok["email"],
                "signup_ts": ok["signup_ts"],
                "country": ok["country"].fillna("unknown"),
                "segment": ok["segment_n"].fillna("unknown"),
                "acquisition_channel": ok["acquisition_channel"].fillna("unknown"),
                "updated_at": ok["updated_at"],
                "_batch_id": ok["_batch_id"],
                "_ingested_at": ok["_ingested_at"],
            }
        )
        .sort_values("user_id")
        .reset_index(drop=True)
    )
    return out, q


def clean_subscriptions(raw: pd.DataFrame, user_ids: set):
    df = parse_payload(raw)
    if df.empty:
        return _empty(
            [
                "subscription_id",
                "user_id",
                "plan_id",
                "status",
                "started_at",
                "canceled_at",
                "mrr_amount",
                "created_at",
                "updated_at",
                "_batch_id",
                "_ingested_at",
            ],
            {
                "subscription_id": "int64",
                "user_id": "int64",
                "plan_id": "string",
                "status": "string",
                "started_at": "datetime64[ns]",
                "canceled_at": "datetime64[ns]",
                "mrr_amount": "float64",
                "created_at": "datetime64[ns]",
                "updated_at": "datetime64[ns]",
                "_batch_id": "string",
                "_ingested_at": "datetime64[ns]",
            },
        ), _empty_q()
    for c in ("subscription_id", "user_id", "mrr_amount"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in ("started_at", "canceled_at", "created_at", "updated_at"):
        df[c] = _ts(df[c])
    df = _latest_version(df, "_source_pk", "updated_at")
    reason = _apply_rules(
        df,
        [
            (df["subscription_id"].isna(), "missing_subscription_id"),
            (~df["user_id"].isin(user_ids), "orphan_user"),
            (~df["status"].isin(SUBSCRIPTION_STATUSES), "invalid_status"),
            (~df["plan_id"].isin(PLANS), "invalid_plan"),
            (df["mrr_amount"].isna() | (df["mrr_amount"] < 0), "invalid_mrr"),
            ((df["status"] != "trialing") & df["started_at"].isna(), "missing_started_at"),
            ((df["status"] == "canceled") & df["canceled_at"].isna(), "missing_canceled_at"),
            (
                df["canceled_at"].notna() & df["started_at"].notna() & (df["canceled_at"] < df["started_at"]),
                "canceled_before_started",
            ),
        ],
    )
    ok, q = _split(df, reason, "subscriptions")
    cols = [
        "subscription_id",
        "user_id",
        "plan_id",
        "status",
        "started_at",
        "canceled_at",
        "mrr_amount",
        "created_at",
        "updated_at",
        "_batch_id",
        "_ingested_at",
    ]
    out = ok[cols].astype({"subscription_id": "int64", "user_id": "int64", "mrr_amount": "float64"})
    return out.sort_values("subscription_id").reset_index(drop=True), q


def clean_history(raw: pd.DataFrame, sub_ids: set):
    df = parse_payload(raw)
    if df.empty:
        return _empty(
            [
                "history_id",
                "subscription_id",
                "old_plan",
                "new_plan",
                "old_mrr",
                "new_mrr",
                "change_ts",
                "_batch_id",
                "_ingested_at",
            ],
            {
                "history_id": "int64",
                "subscription_id": "int64",
                "old_plan": "string",
                "new_plan": "string",
                "old_mrr": "float64",
                "new_mrr": "float64",
                "change_ts": "datetime64[ns]",
                "_batch_id": "string",
                "_ingested_at": "datetime64[ns]",
            },
        ), _empty_q()
    for c in ("history_id", "subscription_id", "old_mrr", "new_mrr"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["change_ts"], df["updated_at"] = _ts(df["change_ts"]), _ts(df["updated_at"])
    df = _latest_version(df, "_source_pk", "updated_at")
    reason = _apply_rules(
        df,
        [
            (~df["subscription_id"].isin(sub_ids), "orphan_subscription"),
            (df["change_ts"].isna(), "invalid_change_ts"),
            (~df["old_plan"].isin(PLANS) | ~df["new_plan"].isin(PLANS), "invalid_plan"),
            (df["old_mrr"].isna() | df["new_mrr"].isna(), "invalid_mrr"),
        ],
    )
    ok, q = _split(df, reason, "subscription_plan_history")
    cols = [
        "history_id",
        "subscription_id",
        "old_plan",
        "new_plan",
        "old_mrr",
        "new_mrr",
        "change_ts",
        "_batch_id",
        "_ingested_at",
    ]
    out = ok[cols].astype({"history_id": "int64", "subscription_id": "int64"})
    return out.sort_values("history_id").reset_index(drop=True), q


def clean_payments(raw: pd.DataFrame, sub_ids: set):
    df = parse_payload(raw)
    if df.empty:
        return _empty(
            [
                "payment_id",
                "subscription_id",
                "user_id",
                "kind",
                "status",
                "amount",
                "currency",
                "paid_at",
                "_batch_id",
                "_ingested_at",
            ],
            {
                "payment_id": "int64",
                "subscription_id": "int64",
                "user_id": "int64",
                "kind": "string",
                "status": "string",
                "amount": "float64",
                "currency": "string",
                "paid_at": "datetime64[ns]",
                "_batch_id": "string",
                "_ingested_at": "datetime64[ns]",
            },
        ), _empty_q()
    for c in ("payment_id", "subscription_id", "user_id", "amount"):
        df[c] = pd.to_numeric(df[c], errors="coerce")
    df["paid_at"], df["updated_at"] = _ts(df["paid_at"]), _ts(df["updated_at"])
    df = _latest_version(df, "_source_pk", "updated_at")
    reason = _apply_rules(
        df,
        [
            (~df["subscription_id"].isin(sub_ids), "orphan_subscription"),
            (~df["kind"].isin(PAYMENT_KINDS), "invalid_kind"),
            (~df["status"].isin(PAYMENT_STATUSES), "invalid_status"),
            (df["amount"].isna() | (df["amount"] <= 0), "invalid_amount"),
            (df["currency"] != "USD", "unsupported_currency"),
            (df["paid_at"].isna(), "invalid_paid_at"),
        ],
    )
    ok, q = _split(df, reason, "payments")
    cols = [
        "payment_id",
        "subscription_id",
        "user_id",
        "kind",
        "status",
        "amount",
        "currency",
        "paid_at",
        "_batch_id",
        "_ingested_at",
    ]
    out = ok[cols].astype(
        {"payment_id": "int64", "subscription_id": "int64", "user_id": "int64", "amount": "float64"}
    )
    return out.sort_values("payment_id").reset_index(drop=True), q


def clean_events(raw: pd.DataFrame, user_ids: set):
    df = parse_payload(raw)
    if df.empty:
        return _empty(
            [
                "event_id",
                "event_uid",
                "user_id",
                "event_type",
                "event_ts",
                "received_at",
                "is_late",
                "device",
                "properties_json",
                "schema_version",
                "_batch_id",
                "_ingested_at",
            ],
            {
                "event_id": "int64",
                "event_uid": "string",
                "user_id": "int64",
                "event_type": "string",
                "event_ts": "datetime64[ns]",
                "received_at": "datetime64[ns]",
                "is_late": "bool",
                "device": "string",
                "properties_json": "string",
                "schema_version": "int64",
                "_batch_id": "string",
                "_ingested_at": "datetime64[ns]",
            },
        ), _empty_q()
    df["event_id"] = pd.to_numeric(df["event_id"], errors="coerce")
    df["user_id"] = pd.to_numeric(df["user_id"], errors="coerce")
    df["event_ts"], df["received_at"] = _ts(df["event_ts_raw"]), _ts(df["received_at"])
    # Duplicates: same event_uid emitted more than once -> keep the first received, quarantine the rest.
    df = df.sort_values(["event_uid", "received_at", "_source_pk"]).reset_index(drop=True)
    dup = df.duplicated("event_uid", keep="first")
    reason = _apply_rules(df, [(dup, "duplicate_event_uid")])
    reason = reason.where(
        reason.notna(),
        _apply_rules(
            df,
            [
                (df["event_ts"].isna(), "invalid_timestamp"),
                (~df["event_type"].isin(EVENT_TYPES), "invalid_event_type"),
                (df["event_ts"] > df["received_at"] + LATE_AFTER, "future_timestamp"),
                (~df["user_id"].isin(user_ids), "orphan_user"),
            ],
        ),
    )
    ok, q = _split(df, reason, "product_events")
    if "device" not in ok.columns:
        ok["device"] = None
    props = ok["properties"].map(
        lambda p: json.dumps(p, sort_keys=True) if isinstance(p, (dict, list)) else None
    )
    dev = ok["device"].where(
        ok["device"].notna(), ok["properties"].map(lambda p: p.get("device") if isinstance(p, dict) else None)
    )
    out = (
        pd.DataFrame(
            {
                "event_id": ok["event_id"].astype("int64"),
                "event_uid": ok["event_uid"],
                "user_id": ok["user_id"].astype("int64"),
                "event_type": ok["event_type"],
                "event_ts": ok["event_ts"],
                "received_at": ok["received_at"],
                "is_late": (ok["received_at"] - ok["event_ts"]) > LATE_AFTER,
                "device": dev,
                "properties_json": props,
                "schema_version": ok["schema_version"],
                "_batch_id": ok["_batch_id"],
                "_ingested_at": ok["_ingested_at"],
            }
        )
        .sort_values("event_id")
        .reset_index(drop=True)
    )
    return out, q


def _empty(cols: list[str], dtypes: dict[str, str] | None = None) -> pd.DataFrame:
    df = pd.DataFrame(columns=cols)
    if dtypes:
        df = df.astype(dtypes)
    return df


def _empty_q() -> pd.DataFrame:
    return pd.DataFrame(columns=QUARANTINE_COLS)


def _write(df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    # microsecond timestamps: DuckDB would overflow nanosecond timestamps on far-future sentinels (9999-12-31)
    df.to_parquet(tmp, index=False, coerce_timestamps="us", allow_truncated_timestamps=True)
    os.replace(tmp, path)


class EmptyBronzeError(RuntimeError):
    """Raised when there is no bronze data to build silver from (fail loudly instead of producing empty tables)."""


def build_silver(settings: Settings) -> dict:
    """Rebuild every silver table + quarantine from bronze. Returns row-count metrics."""
    cache: dict = {}

    def b(t: str) -> pd.DataFrame:
        if t not in cache:
            cache[t] = bronze.read_table(settings.bronze_dir, t)
        return cache[t]

    for required in ("users", "subscriptions", "payments", "product_events"):
        if b(required).empty:
            raise EmptyBronzeError(f"No bronze data for '{required}'. Run generate + ingest first.")
    metrics: dict = {}
    users, q_users = clean_users(b("users"))
    subs, q_subs = clean_subscriptions(b("subscriptions"), set(users["user_id"]) if len(users) else set())
    hist, q_hist = clean_history(
        b("subscription_plan_history"), set(subs["subscription_id"]) if len(subs) else set()
    )
    pays, q_pays = clean_payments(b("payments"), set(subs["subscription_id"]) if len(subs) else set())
    events, q_events = clean_events(b("product_events"), set(users["user_id"]) if len(users) else set())
    outputs = {
        "users": (users, q_users),
        "subscriptions": (subs, q_subs),
        "subscription_plan_history": (hist, q_hist),
        "payments": (pays, q_pays),
        "product_events": (events, q_events),
    }
    for name, (ok, q) in outputs.items():
        _write(ok, settings.silver_dir / f"{name}.parquet")
        _write(q, settings.silver_dir / "quarantine" / f"{name}.parquet")
        by_reason = q["reject_reason"].value_counts().to_dict() if len(q) else {}
        metrics[name] = {
            "bronze_rows": int(len(b(name))),
            "accepted": int(len(ok)),
            "quarantined": int(len(q)),
            "quarantine_reasons": by_reason,
        }
        log.info(kv(event="silver_built", table=name, accepted=len(ok), quarantined=len(q)))
    return metrics
