import json
from datetime import datetime, timedelta, timezone

import pandas as pd
import pytest

from saas_platform import analytics, loader
from saas_platform.generator import generate
from saas_platform.pipeline import run_pipeline
from saas_platform.quality import QualityGateError, run_quality
from saas_platform.silver import EmptyBronzeError

pytestmark = pytest.mark.integration


@pytest.fixture()
def loaded(conn, settings):
    data = generate(11, 150, days=90)
    loader.load_as_of(conn, data)
    return data


def read(settings, layer, name):
    return pd.read_parquet(getattr(settings, f"{layer}_dir") / f"{name}.parquet")


def test_end_to_end_creates_all_layers_and_passes_quality(conn, settings, loaded):
    m = run_pipeline(settings)
    assert m["quality"]["failed_errors"] == 0
    assert list(settings.bronze_dir.glob("*/*/*.parquet"))
    assert (settings.silver_dir / "users.parquet").exists()
    for t in ("dim_user", "fact_mrr_movement", "fact_payments", "fact_product_activity_daily"):
        assert len(read(settings, "gold", t)) > 0
    report = json.loads((settings.quality_dir / "latest.json").read_text())
    assert report["status"] == "passed" and report["checks"] and report["metrics"]["stages"]["ingest"]["rows"]


def test_short_window_without_plan_history_does_not_fail_quality(conn, settings):
    data = generate(11, 150, days=30)
    loader.load_as_of(conn, data)
    m = run_pipeline(settings)
    assert m["quality"]["failed_errors"] == 0
    assert len(read(settings, "silver", "subscription_plan_history")) == 0


def test_idempotent_rerun_leaves_outputs_unchanged(conn, settings, loaded):
    run_pipeline(settings)
    before = {
        t: read(settings, "gold", t)
        for t in ("fact_payments", "fact_mrr_movement", "dim_user", "fact_product_activity_daily")
    }
    silver_before = read(settings, "silver", "product_events")
    m = run_pipeline(settings)  # same input, second time
    assert sum(m["stages"]["ingest"]["rows"].values()) == 0
    for t, df in before.items():
        pd.testing.assert_frame_equal(df, read(settings, "gold", t))
    pd.testing.assert_frame_equal(silver_before, read(settings, "silver", "product_events"))


def test_duplicate_source_event_does_not_change_metrics(conn, settings, loaded):
    run_pipeline(settings)
    dau_before = analytics.run_query(settings, "dau")
    with conn.cursor() as cur:
        cur.execute(
            "SELECT * FROM src.product_events WHERE event_type='login' AND user_id <= 150 "
            "AND event_ts_raw LIKE '2025%' ORDER BY event_id LIMIT 1"
        )
        cols = [d[0] for d in cur.description]
        row = dict(zip(cols, cur.fetchone()))
        cur.execute("SELECT MAX(event_id), MAX(received_at) FROM src.product_events")
        mx, mr = cur.fetchone()
    row.update(event_id=mx + 1, received_at=mr + timedelta(seconds=5))  # same event_uid, new row
    loader.insert_rows(conn, "product_events", [row])
    m = run_pipeline(settings)
    assert m["stages"]["ingest"]["rows"]["product_events"] == 1
    assert m["stages"]["silver"]["tables"]["product_events"]["quarantine_reasons"]["duplicate_event_uid"] >= 1
    pd.testing.assert_frame_equal(dau_before, analytics.run_query(settings, "dau"))


def test_late_arriving_event_lands_on_its_event_date(conn, settings, loaded):
    run_pipeline(settings)
    day = datetime(2025, 1, 3, 12, tzinfo=timezone.utc)
    with conn.cursor() as cur:
        cur.execute(
            "SELECT user_id FROM src.users WHERE signup_ts < %s ORDER BY user_id LIMIT 1",
            (day - timedelta(days=1),),
        )
        uid = cur.fetchone()[0]
        cur.execute("SELECT MAX(event_id), MAX(received_at) FROM src.product_events")
        mx, mr = cur.fetchone()
    before = analytics.run_query(settings, "dau").set_index("activity_date")["events"]
    late = {
        "event_id": mx + 1,
        "event_uid": "late-1",
        "user_id": uid,
        "event_type": "login",
        "event_ts_raw": "2025-01-03T12:00:00Z",
        "properties": None,
        "schema_version": 1,
        "received_at": mr + timedelta(seconds=1),
    }  # arrives ~3 months late
    loader.insert_rows(conn, "product_events", [late])
    run_pipeline(settings)
    after = analytics.run_query(settings, "dau").set_index("activity_date")["events"]
    d = pd.Timestamp("2025-01-03").date()
    key = next(k for k in after.index if pd.Timestamp(k).date() == d)
    prev = before.get(key, 0) if key in before.index else 0
    assert after[key] == prev + 1
    assert bool(read(settings, "silver", "product_events").query("event_uid == 'late-1'")["is_late"].iloc[0])


def test_quality_gate_fails_loudly_when_silver_is_corrupted(conn, settings, loaded):
    run_pipeline(settings)
    p = settings.silver_dir / "users.parquet"
    users = pd.read_parquet(p)
    users.iloc[:5].to_parquet(p, index=False)  # drop most users -> orphans + reconciliation mismatch
    with pytest.raises(QualityGateError) as exc:
        run_quality(settings, conn, "corrupt")
    report = json.loads((settings.quality_dir / "quality_report_corrupt.json").read_text())
    assert report["status"] == "failed"
    assert any("reconcile_bronze_to_silver[users]" in n for n in exc.value.failed)
    assert any("referential_integrity" in n for n in report["failed_error_checks"])


def test_quality_detects_source_rows_not_yet_ingested(conn, settings, loaded):
    run_pipeline(settings)
    with conn.cursor() as cur:
        cur.execute("SELECT MAX(received_at) FROM src.product_events")
        mr = cur.fetchone()[0]
    loader.insert_rows(
        conn,
        "users",
        [
            {
                "user_id": 99999,
                "email": "z@x.test",
                "signup_ts": mr,
                "country": "US",
                "segment": "smb",
                "acquisition_channel": None,
                "created_at": mr,
                "updated_at": mr + timedelta(seconds=1),
            }
        ],
    )
    with pytest.raises(QualityGateError) as exc:
        run_quality(settings, conn, "stale")
    assert any("freshness_lag[users]" in n for n in exc.value.failed)


def test_missing_gold_input_raises_clear_error(conn, settings):
    with pytest.raises(EmptyBronzeError):
        run_pipeline(settings)  # nothing loaded -> silver/gold cannot be built; must not pass silently
