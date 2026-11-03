"""Source schema (DDL), table contracts and accepted values shared by all layers."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

SQL_DIR = Path(__file__).parent / "sql"

PLANS = {"starter": 29.0, "pro": 99.0, "business": 299.0}
SEGMENTS = ("smb", "mid_market", "enterprise")
SUBSCRIPTION_STATUSES = ("trialing", "active", "past_due", "canceled")
PAYMENT_KINDS = ("charge", "refund")
PAYMENT_STATUSES = ("succeeded", "failed")
EVENT_TYPES = ("login", "page_view", "feature_used", "report_created", "invite_sent", "export_data")


@dataclass(frozen=True)
class TableSpec:
    name: str
    pk: str
    watermark: str  # column used (with pk) as the incremental cursor
    columns: tuple[str, ...]  # contract: columns that MUST exist in the source


SOURCE_TABLES: dict[str, TableSpec] = {
    "users": TableSpec(
        "users",
        "user_id",
        "updated_at",
        (
            "user_id",
            "email",
            "signup_ts",
            "country",
            "segment",
            "acquisition_channel",
            "created_at",
            "updated_at",
        ),
    ),
    "subscriptions": TableSpec(
        "subscriptions",
        "subscription_id",
        "updated_at",
        (
            "subscription_id",
            "user_id",
            "plan_id",
            "status",
            "started_at",
            "canceled_at",
            "mrr_amount",
            "created_at",
            "updated_at",
        ),
    ),
    "subscription_plan_history": TableSpec(
        "subscription_plan_history",
        "history_id",
        "updated_at",
        (
            "history_id",
            "subscription_id",
            "old_plan",
            "new_plan",
            "old_mrr",
            "new_mrr",
            "change_ts",
            "updated_at",
        ),
    ),
    "payments": TableSpec(
        "payments",
        "payment_id",
        "updated_at",
        (
            "payment_id",
            "subscription_id",
            "user_id",
            "kind",
            "status",
            "amount",
            "currency",
            "paid_at",
            "updated_at",
        ),
    ),
    "product_events": TableSpec(
        "product_events",
        "event_id",
        "received_at",
        (
            "event_id",
            "event_uid",
            "user_id",
            "event_type",
            "event_ts_raw",
            "properties",
            "schema_version",
            "received_at",
        ),
    ),
}
# FK-safe load order
LOAD_ORDER = ("users", "subscriptions", "subscription_plan_history", "payments", "product_events")


def ddl() -> str:
    return (SQL_DIR / "ddl.sql").read_text(encoding="utf-8")
