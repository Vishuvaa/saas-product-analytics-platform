"""Deterministic synthetic SaaS data generator (pure Python, no database access).

Same (seed, n_users, start, days) => identical output. Injects realistic data problems:
duplicate events, missing optional fields, late-arriving events, invalid timestamps/statuses/types,
orphan user ids, and an additive schema change (events.device) after ~75% of the window.
"""

from __future__ import annotations

import json
import random
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

from saas_platform.schema import EVENT_TYPES, PLANS

UTC = timezone.utc
PLAN_ORDER = ["starter", "pro", "business"]
COUNTRIES = ["US", "IN", "GB", "DE", "BR", "CA", "AU"]
CHANNELS = ["organic", "paid_search", "referral", "social", "partner"]
SEGMENT_W = {"smb": 0.6, "mid_market": 0.3, "enterprise": 0.1}
CONVERT_P = {"smb": 0.25, "mid_market": 0.40, "enterprise": 0.55}
START_PLAN_W = {
    "smb": [0.8, 0.18, 0.02],
    "mid_market": [0.2, 0.6, 0.2],
    "enterprise": [0.0, 0.3, 0.7],
}
EVENT_W = [0.15, 0.4, 0.25, 0.1, 0.05, 0.05]


@dataclass
class SubRecord:
    subscription_id: int
    user_id: int
    created_at: datetime
    initial_plan: str
    started_at: datetime | None  # None => trial only
    changes: list[tuple[datetime, str]] = field(default_factory=list)
    canceled_at: datetime | None = None
    dirty_status: bool = False

    def state_asof(self, t: datetime | None) -> dict | None:
        if t is not None and self.created_at > t:
            return None
        if self.started_at is None:
            return {
                "subscription_id": self.subscription_id,
                "user_id": self.user_id,
                "plan_id": self.initial_plan,
                "status": "trialing",
                "started_at": None,
                "canceled_at": None,
                "mrr_amount": 0.0,
                "created_at": self.created_at,
                "updated_at": self.created_at,
            }
        plan = self.initial_plan
        updated = self.started_at
        for ts, new_plan in self.changes:
            if t is None or ts <= t:
                plan, updated = new_plan, max(updated, ts)
        canceled = self.canceled_at is not None and (t is None or self.canceled_at <= t)
        if canceled:
            updated = max(updated, self.canceled_at)
        status = "canceled" if canceled else "active"
        if self.dirty_status:
            status = "activ"  # typo -> invalid accepted value
        return {
            "subscription_id": self.subscription_id,
            "user_id": self.user_id,
            "plan_id": plan,
            "status": status,
            "started_at": self.started_at,
            "canceled_at": self.canceled_at if canceled else None,
            "mrr_amount": PLANS[plan],
            "created_at": self.created_at,
            "updated_at": updated,
        }


@dataclass
class GeneratedData:
    seed: int
    start: datetime
    end: datetime
    schema_change_ts: datetime
    users: list[dict]
    subs: list[SubRecord]
    history: list[dict]
    payments: list[dict]
    events: list[dict]

    def as_of(self, until: datetime | None) -> dict[str, list[dict]]:
        """Rows visible in the source system at time `until` (None = everything)."""

        def vis(rows, col):
            return [r for r in rows if until is None or r[col] <= until]

        subs = [s for s in (x.state_asof(until) for x in self.subs) if s is not None]
        return {
            "users": vis(self.users, "updated_at"),
            "subscriptions": subs,
            "subscription_plan_history": vis(self.history, "updated_at"),
            "payments": vis(self.payments, "updated_at"),
            "product_events": vis(self.events, "received_at"),
        }


def _fmt_ts(rng: random.Random, ts: datetime) -> str:
    r = rng.random()
    if r < 0.80:
        return ts.strftime("%Y-%m-%dT%H:%M:%SZ")
    if r < 0.95:
        return ts.strftime("%Y-%m-%d %H:%M:%S")
    return ts.strftime("%Y-%m-%dT%H:%M:%S+00:00")


def _wchoice(rng: random.Random, weights: dict | list, keys: list | None = None):
    if isinstance(weights, dict):
        keys, w = list(weights), list(weights.values())
    else:
        w = weights
    return rng.choices(keys, weights=w, k=1)[0]


def generate(
    seed: int = 42, n_users: int = 500, start: date = date(2025, 1, 1), days: int = 120
) -> GeneratedData:
    rng = random.Random(seed)
    t0 = datetime(start.year, start.month, start.day, tzinfo=UTC)
    end = t0 + timedelta(days=days)
    schema_change_ts = t0 + timedelta(days=int(days * 0.75))

    users: list[dict] = []
    subs: list[SubRecord] = []
    history: list[dict] = []
    payments: list[dict] = []
    raw_events: list[tuple] = []  # (received_at, uid, user_id, type, ts_raw, props, version)
    sub_id = hist_id = pay_id = 0
    evt_n = 0

    for uid in range(1, n_users + 1):
        signup = t0 + timedelta(seconds=rng.randrange(int(days * 86400)))
        segment = _wchoice(rng, SEGMENT_W)
        users.append(
            {
                "user_id": uid,
                "email": None if rng.random() < 0.003 else f"user{uid}@example.test",
                "signup_ts": signup,
                "country": None if rng.random() < 0.05 else rng.choice(COUNTRIES),
                "segment": "gibberish" if rng.random() < 0.003 else segment,
                "acquisition_channel": None if rng.random() < 0.08 else rng.choice(CHANNELS),
                "created_at": signup,
                "updated_at": signup,
            }
        )

        paying_until = None  # activity horizon for paying users
        started = None
        if rng.random() < CONVERT_P[segment]:
            started = signup + timedelta(seconds=rng.randrange(14 * 86400))
            if started >= end:
                started = None
        if started is not None:
            sub_id += 1
            plan = _wchoice(rng, START_PLAN_W[segment], PLAN_ORDER)
            sub = SubRecord(sub_id, uid, started, plan, started)
            idx = PLAN_ORDER.index(plan)
            cur_plan = plan
            if rng.random() < 0.20 and idx < 2:  # upgrade
                ts = started + timedelta(days=rng.randint(30, 80), seconds=rng.randrange(86400))
                if ts < end:
                    sub.changes.append((ts, PLAN_ORDER[idx + 1]))
            elif rng.random() < 0.07 and idx > 0:  # downgrade
                ts = started + timedelta(days=rng.randint(30, 80), seconds=rng.randrange(86400))
                if ts < end:
                    sub.changes.append((ts, PLAN_ORDER[idx - 1]))
            if rng.random() < 0.25:
                c = started + timedelta(days=rng.randint(31, 120), seconds=rng.randrange(86400))
                if c < end:
                    sub.canceled_at = c
            if sub.canceled_at is not None:  # drop plan changes after cancellation
                sub.changes = [x for x in sub.changes if x[0] < sub.canceled_at]
            sub.dirty_status = rng.random() < 0.005
            for ts, new_plan in sub.changes:
                hist_id += 1
                history.append(
                    {
                        "history_id": hist_id,
                        "subscription_id": sub_id,
                        "old_plan": cur_plan,
                        "new_plan": new_plan,
                        "old_mrr": PLANS[cur_plan],
                        "new_mrr": PLANS[new_plan],
                        "change_ts": ts,
                        "updated_at": ts,
                    }
                )
                cur_plan = new_plan
            subs.append(sub)
            # payments: monthly charge every 30 days while the subscription lives
            k = 0
            while True:
                pts = started + timedelta(days=30 * k)
                if pts >= end or (sub.canceled_at and pts >= sub.canceled_at):
                    break
                plan_at = sub.initial_plan
                for cts, cplan in sub.changes:
                    if cts <= pts:
                        plan_at = cplan
                pay_id += 1
                failed = rng.random() < 0.04
                status = "failed" if failed else ("succeded" if rng.random() < 0.003 else "succeeded")
                payments.append(
                    {
                        "payment_id": pay_id,
                        "subscription_id": sub_id,
                        "user_id": uid,
                        "kind": "charge",
                        "status": status,
                        "amount": PLANS[plan_at],
                        "currency": "USD",
                        "paid_at": pts,
                        "updated_at": pts,
                    }
                )
                if status == "succeeded" and rng.random() < 0.02:
                    rts = pts + timedelta(days=rng.randint(1, 5), seconds=rng.randrange(3600))
                    if rts < end:
                        pay_id += 1
                        payments.append(
                            {
                                "payment_id": pay_id,
                                "subscription_id": sub_id,
                                "user_id": uid,
                                "kind": "refund",
                                "status": "succeeded",
                                "amount": PLANS[plan_at],
                                "currency": "USD",
                                "paid_at": rts,
                                "updated_at": rts,
                            }
                        )
                k += 1
            paying_until = sub.canceled_at or end
        elif signup > end - timedelta(days=14) and rng.random() < 0.5:
            sub_id += 1
            created = signup + timedelta(hours=1)
            subs.append(SubRecord(sub_id, uid, created, "starter", None))

        # ---- product events ----
        active_p = 0.28 if paying_until else 0.08
        horizon = (
            (paying_until + timedelta(days=rng.randint(0, 10)))
            if paying_until
            else signup + timedelta(days=15)
        )
        horizon = min(horizon, end)
        day = signup.replace(hour=0, minute=0, second=0, microsecond=0)
        while day < horizon:
            if rng.random() < active_p:
                for _ in range(rng.randint(1, 6)):
                    ts = day + timedelta(seconds=rng.randrange(86400))
                    if ts < signup or ts >= end:
                        continue
                    evt_n += 1
                    late = rng.random() < 0.03
                    lag = (
                        timedelta(days=rng.randint(1, 4), seconds=rng.randrange(86400))
                        if late
                        else timedelta(seconds=rng.randint(1, 30))
                    )
                    ev_type = _wchoice(rng, EVENT_W, list(EVENT_TYPES))
                    ts_raw = _fmt_ts(rng, ts)
                    user_ref = uid
                    r = rng.random()
                    if r < 0.005:
                        ts_raw = "not-a-timestamp"
                    elif r < 0.010:
                        ev_type = "unknown_event"
                    elif r < 0.013:
                        user_ref = n_users + 10_000 + rng.randint(1, 50)
                    v2 = ts >= schema_change_ts
                    props = None if rng.random() < 0.05 else {"page": f"/p{rng.randint(1, 20)}"}
                    if v2:
                        props = dict(props or {}, device=rng.choice(["web", "ios", "android"]))
                    uid_s = f"e{seed}-{evt_n}"
                    recv = ts + lag
                    row = (recv, uid_s, user_ref, ev_type, ts_raw, props, 2 if v2 else 1)
                    raw_events.append(row)
                    if rng.random() < 0.02:  # duplicate emission
                        raw_events.append((recv + timedelta(seconds=rng.randint(1, 120)),) + row[1:])
            day += timedelta(days=1)

    raw_events.sort(key=lambda r: (r[0], r[1]))
    events = []
    for i, (recv, uid_s, user_ref, ev_type, ts_raw, props, ver) in enumerate(raw_events, start=1):
        events.append(
            {
                "event_id": i,
                "event_uid": uid_s,
                "user_id": user_ref,
                "event_type": ev_type,
                "event_ts_raw": ts_raw,
                "properties": props,
                "schema_version": ver,
                "received_at": recv,
                "device": (props or {}).get("device"),
            }
        )
    return GeneratedData(seed, t0, end, schema_change_ts, users, subs, history, payments, events)


def fingerprint(data: GeneratedData) -> str:
    import hashlib

    h = hashlib.sha256()
    tables = data.as_of(None)
    for name in sorted(tables):
        for row in tables[name]:
            h.update(json.dumps(row, sort_keys=True, default=str).encode())
    return h.hexdigest()
