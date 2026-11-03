from datetime import date

from saas_platform.generator import fingerprint, generate


def test_same_seed_is_deterministic():
    a, b = generate(7, 80, date(2025, 1, 1), 60), generate(7, 80, date(2025, 1, 1), 60)
    assert fingerprint(a) == fingerprint(b)


def test_different_seed_differs():
    assert fingerprint(generate(7, 80)) != fingerprint(generate(8, 80))


def test_dataset_size_is_configurable():
    assert len(generate(1, 50).users) == 50
    assert len(generate(1, 200).users) == 200


def test_injects_realistic_data_problems():
    d = generate(42, 400)
    uids = [e["event_uid"] for e in d.events]
    assert len(uids) > len(set(uids)), "duplicate events expected"
    assert any(e["event_ts_raw"] == "not-a-timestamp" for e in d.events)
    assert any(e["event_type"] == "unknown_event" for e in d.events)
    assert any(e["properties"] is None for e in d.events), "missing optional fields expected"
    assert any((e["received_at"] - d.start).days >= 0 for e in d.events)
    late = [e for e in d.events if e["event_ts_raw"][:4] == "2025" and e["event_ts_raw"] != "not-a-timestamp"]
    assert late


def test_as_of_filters_rows_and_never_shows_future_state():
    d = generate(42, 200)
    from datetime import timedelta

    t = d.start + timedelta(days=40)
    visible = d.as_of(t)
    assert all(r["updated_at"] <= t for r in visible["users"])
    assert all(r["received_at"] <= t for r in visible["product_events"])
    assert all(s["created_at"] <= t and s["updated_at"] <= t for s in visible["subscriptions"])
    assert len(d.as_of(None)["users"]) >= len(visible["users"])
