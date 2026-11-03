"""Business-metric correctness on a tiny hand-built dataset with known answers."""

from datetime import datetime, timezone

import pytest

from saas_platform import analytics, loader
from saas_platform.pipeline import run_pipeline

pytestmark = pytest.mark.integration


def dt(m, d, h=12):
    return datetime(2025, m, d, h, tzinfo=timezone.utc)


def u(i, ts, seg="smb"):
    return {
        "user_id": i,
        "email": f"u{i}@x.test",
        "signup_ts": ts,
        "country": "US",
        "segment": seg,
        "acquisition_channel": "organic",
        "created_at": ts,
        "updated_at": ts,
    }


def sub(i, uid, plan, mrr, status, started, canceled, updated, created=None):
    return {
        "subscription_id": i,
        "user_id": uid,
        "plan_id": plan,
        "status": status,
        "started_at": started,
        "canceled_at": canceled,
        "mrr_amount": mrr,
        "created_at": created or started or updated,
        "updated_at": updated,
    }


def pay(i, sid, uid, kind, amount, ts, status="succeeded"):
    return {
        "payment_id": i,
        "subscription_id": sid,
        "user_id": uid,
        "kind": kind,
        "status": status,
        "amount": amount,
        "currency": "USD",
        "paid_at": ts,
        "updated_at": ts,
    }


def ev(i, uid, ts, typ="login"):
    return {
        "event_id": i,
        "event_uid": f"e{i}",
        "user_id": uid,
        "event_type": typ,
        "event_ts_raw": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "properties": None,
        "schema_version": 1,
        "received_at": ts,
    }


@pytest.fixture()
def scenario(conn, settings):
    loader.insert_rows(conn, "users", [u(1, dt(1, 3)), u(2, dt(1, 10)), u(3, dt(3, 20))])
    loader.insert_rows(
        conn,
        "subscriptions",
        [
            # A: starter 29 from Jan 5, upgraded to pro 99 on Feb 10, still active
            sub(1, 1, "pro", 99, "active", dt(1, 5), None, dt(2, 10)),
            # B: business 299 from Jan 20, canceled Mar 3
            sub(2, 2, "business", 299, "canceled", dt(1, 20), dt(3, 3), dt(3, 3)),
            # C: trial only -> never MRR
            sub(3, 3, "starter", 0, "trialing", None, None, dt(3, 20)),
        ],
    )
    loader.insert_rows(
        conn,
        "subscription_plan_history",
        [
            {
                "history_id": 1,
                "subscription_id": 1,
                "old_plan": "starter",
                "new_plan": "pro",
                "old_mrr": 29,
                "new_mrr": 99,
                "change_ts": dt(2, 10),
                "updated_at": dt(2, 10),
            }
        ],
    )
    loader.insert_rows(
        conn,
        "payments",
        [
            pay(1, 1, 1, "charge", 29, dt(1, 5)),
            pay(2, 1, 1, "charge", 29, dt(2, 4)),
            pay(3, 1, 1, "charge", 99, dt(3, 6)),
            pay(4, 2, 2, "charge", 299, dt(1, 20)),
            pay(5, 2, 2, "charge", 299, dt(2, 19)),
            pay(6, 2, 2, "refund", 299, dt(2, 22)),
            pay(7, 1, 1, "charge", 99, dt(4, 5), status="failed"),
        ],
    )
    loader.insert_rows(
        conn,
        "product_events",
        [
            ev(1, 1, dt(1, 6)),
            ev(2, 1, dt(1, 6, 15)),
            ev(3, 2, dt(1, 6)),
            ev(4, 1, dt(2, 6)),
            ev(5, 3, dt(3, 21)),
            ev(6, 1, dt(3, 6)),
        ],
    )
    run_pipeline(settings)
    return settings


def test_mrr_waterfall_matches_hand_calculation(scenario):
    m = analytics.run_query(scenario, "mrr_monthly").set_index("month")
    rows = {k.strftime("%Y-%m"): v for k, v in m.iterrows()}
    assert rows["2025-01"]["new_mrr"] == 328 and rows["2025-01"]["ending_mrr"] == 328  # 29 + 299
    assert rows["2025-02"]["expansion_mrr"] == 70 and rows["2025-02"]["ending_mrr"] == 398
    assert rows["2025-03"]["churned_mrr"] == -299 and rows["2025-03"]["ending_mrr"] == 99
    assert 0 not in m.index  # trial never added MRR
    assert m["new_mrr"].sum() == 328


def test_refunds_and_failed_payments_do_not_change_mrr_but_change_cash(scenario):
    rev = analytics.run_query(scenario, "revenue_by_plan")
    biz_feb = rev[(rev.plan_id == "business") & (rev.month.astype(str).str.startswith("2025-02"))].iloc[0]
    assert biz_feb["gross_payments"] == 299 and biz_feb["refunds"] == 299 and biz_feb["net_cash"] == 0
    assert biz_feb["ending_mrr"] == 299  # MRR unaffected by the refund (still subscribed at month end)
    april = rev[rev.month.astype(str).str.startswith("2025-04")]
    assert april["gross_payments"].sum() == 0  # failed charge is not revenue


def test_churn_rates(scenario):
    c = analytics.run_query(scenario, "churn_monthly").set_index("month")
    mar = c[c.index.astype(str).str.startswith("2025-03")].iloc[0]
    assert mar["canceled_subs"] == 1 and mar["starting_subs"] == 2
    assert mar["logo_churn_pct"] == 50.0 and mar["revenue_churn_pct"] == round(100 * 299 / 398, 1)


def test_active_users_and_conversion(scenario):
    mau = analytics.run_query(scenario, "mau").set_index("activity_month")["mau"]
    assert [int(x) for x in mau.values] == [2, 1, 2]
    conv = analytics.run_query(scenario, "conversion").set_index("signup_month")
    assert conv.iloc[0]["signups"] == 2 and conv.iloc[0]["converted_30d"] == 2
    assert conv.iloc[1]["signups"] == 1 and conv.iloc[1]["converted_30d"] == 0  # trial is not conversion


def test_activity_plan_attribution_uses_plan_at_that_day(scenario):
    a = analytics.run_query(scenario, "activity_by_plan")
    by = {(str(r.month)[:7], r.plan_id): r.events for r in a.itertuples()}
    assert by[("2025-02", "starter")] == 1  # user 1 on Feb 6: upgrade only happens Feb 10
    assert by[("2025-03", "pro")] == 1  # user 1 on Mar 6: already on pro
    assert by[("2025-03", "free")] == 1  # user 3 (trial only) is on no paid plan
