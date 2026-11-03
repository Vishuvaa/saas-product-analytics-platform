"""Pure-pandas tests of Silver cleaning rules (no database)."""

from datetime import datetime, timezone

import pandas as pd

from saas_platform import bronze, silver

NOW = datetime(2025, 3, 1, tzinfo=timezone.utc)


def frame(table, pk, wm, rows, batch="b1"):
    return bronze.make_bronze_frame(table, pk, wm, rows, batch, "r1", NOW)


def ev(i, uid, **kw):
    base = {
        "event_id": i,
        "event_uid": uid,
        "user_id": 1,
        "event_type": "login",
        "event_ts_raw": "2025-01-10T10:00:00Z",
        "properties": None,
        "schema_version": 1,
        "received_at": "2025-01-10T10:00:05+00:00",
    }
    base.update(kw)
    return base


def test_empty_history_schema_keeps_gold_columns():
    raw = bronze.make_bronze_frame(
        "subscription_plan_history",
        "history_id",
        "updated_at",
        [],
        "b1",
        "r1",
        NOW,
    )
    ok, q = silver.clean_history(raw, set())
    assert list(ok.columns) == [
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
    assert ok.empty and q.empty
    assert str(ok.dtypes["old_plan"]) == "string"
    assert str(ok.dtypes["old_mrr"]) == "float64"
    assert str(ok.dtypes["change_ts"]) == "datetime64[ns]"


def test_duplicate_events_keep_first_and_quarantine_rest():
    raw = frame(
        "product_events",
        "event_id",
        "received_at",
        [ev(1, "a"), ev(2, "a", received_at="2025-01-10T10:01:00+00:00"), ev(3, "b")],
    )
    ok, q = silver.clean_events(raw, {1})
    assert sorted(ok["event_uid"]) == ["a", "b"]
    assert list(q["reject_reason"]) == ["duplicate_event_uid"]
    assert int(ok.loc[ok.event_uid == "a", "event_id"].iloc[0]) == 1


def test_event_validation_reasons():
    raw = frame(
        "product_events",
        "event_id",
        "received_at",
        [
            ev(1, "a", event_ts_raw="garbage"),
            ev(2, "b", event_type="nope"),
            ev(3, "c", user_id=999),
            ev(4, "d", event_ts_raw="2026-01-01T00:00:00Z"),
            ev(5, "e"),
        ],
    )
    ok, q = silver.clean_events(raw, {1})
    reasons = dict(zip(q["source_pk"], q["reject_reason"]))
    assert reasons == {
        1: "invalid_timestamp",
        2: "invalid_event_type",
        3: "orphan_user",
        4: "future_timestamp",
    }
    assert list(ok["event_uid"]) == ["e"]


def test_late_arriving_event_is_flagged_but_accepted_with_event_time():
    raw = frame(
        "product_events",
        "event_id",
        "received_at",
        [ev(1, "late", event_ts_raw="2025-01-01T00:00:00Z", received_at="2025-01-05T00:00:00+00:00")],
    )
    ok, q = silver.clean_events(raw, {1})
    assert len(q) == 0 and bool(ok["is_late"].iloc[0])
    assert ok["event_ts"].iloc[0] == pd.Timestamp("2025-01-01")


def test_mixed_timestamp_formats_normalize_to_same_value():
    raw = frame(
        "product_events",
        "event_id",
        "received_at",
        [
            ev(1, "a", event_ts_raw="2025-01-10T10:00:00Z"),
            ev(2, "b", event_ts_raw="2025-01-10 10:00:00"),
            ev(3, "c", event_ts_raw="2025-01-10T10:00:00+00:00"),
        ],
    )
    ok, _ = silver.clean_events(raw, {1})
    assert ok["event_ts"].nunique() == 1


def test_users_latest_version_wins_and_invalid_are_quarantined():
    rows = [
        {
            "user_id": 1,
            "email": "a@x.test",
            "signup_ts": "2025-01-01T00:00:00+00:00",
            "country": None,
            "segment": " SMB ",
            "acquisition_channel": None,
            "created_at": "x",
            "updated_at": "2025-01-01T00:00:00+00:00",
        },
        {
            "user_id": 1,
            "email": "a@x.test",
            "signup_ts": "2025-01-01T00:00:00+00:00",
            "country": "US",
            "segment": "smb",
            "acquisition_channel": "organic",
            "created_at": "x",
            "updated_at": "2025-02-01T00:00:00+00:00",
        },
        {
            "user_id": 2,
            "email": None,
            "signup_ts": "2025-01-01T00:00:00+00:00",
            "country": "US",
            "segment": "smb",
            "acquisition_channel": None,
            "created_at": "x",
            "updated_at": "2025-01-01T00:00:00+00:00",
        },
        {
            "user_id": 3,
            "email": "c@x.test",
            "signup_ts": "2025-01-01T00:00:00+00:00",
            "country": "US",
            "segment": "gibberish",
            "acquisition_channel": None,
            "created_at": "x",
            "updated_at": "2025-01-01T00:00:00+00:00",
        },
    ]
    raw = bronze.make_bronze_frame("users", "user_id", "updated_at", rows, "b1", "r1", NOW)
    ok, q = silver.clean_users(raw)
    assert list(ok["user_id"]) == [1] and ok["country"].iloc[0] == "US"
    assert dict(zip(q["source_pk"], q["reject_reason"])) == {2: "invalid_email", 3: "invalid_segment"}


def test_payment_rules():
    base = {
        "subscription_id": 1,
        "user_id": 1,
        "kind": "charge",
        "status": "succeeded",
        "amount": 29.0,
        "currency": "USD",
        "paid_at": "2025-01-02T00:00:00+00:00",
        "updated_at": "2025-01-02T00:00:00+00:00",
    }
    rows = [
        dict(base, payment_id=1),
        dict(base, payment_id=2, status="succeded"),
        dict(base, payment_id=3, amount=-5),
        dict(base, payment_id=4, subscription_id=77),
        dict(base, payment_id=5, currency="EUR"),
    ]
    raw = bronze.make_bronze_frame("payments", "payment_id", "updated_at", rows, "b", "r", NOW)
    ok, q = silver.clean_payments(raw, {1})
    assert list(ok["payment_id"]) == [1]
    assert dict(zip(q["source_pk"], q["reject_reason"])) == {
        2: "invalid_status",
        3: "invalid_amount",
        4: "orphan_subscription",
        5: "unsupported_currency",
    }
