-- Source (operational) schema and ETL control tables. Idempotent.
CREATE SCHEMA IF NOT EXISTS src;
CREATE SCHEMA IF NOT EXISTS etl;

CREATE TABLE IF NOT EXISTS src.users (
    user_id BIGINT PRIMARY KEY,
    email TEXT,
    signup_ts TIMESTAMPTZ NOT NULL,
    country TEXT,
    segment TEXT,
    acquisition_channel TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS src.subscriptions (
    subscription_id BIGINT PRIMARY KEY,
    user_id BIGINT NOT NULL REFERENCES src.users (user_id),
    plan_id TEXT,
    status TEXT,
    started_at TIMESTAMPTZ,          -- first paid start; NULL while trialing
    canceled_at TIMESTAMPTZ,
    mrr_amount NUMERIC(10, 2),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS src.subscription_plan_history (
    history_id BIGINT PRIMARY KEY,
    subscription_id BIGINT NOT NULL REFERENCES src.subscriptions (subscription_id),
    old_plan TEXT,
    new_plan TEXT,
    old_mrr NUMERIC(10, 2),
    new_mrr NUMERIC(10, 2),
    change_ts TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE IF NOT EXISTS src.payments (
    payment_id BIGINT PRIMARY KEY,
    subscription_id BIGINT NOT NULL REFERENCES src.subscriptions (subscription_id),
    user_id BIGINT NOT NULL,
    kind TEXT,                        -- charge | refund (amount always positive)
    status TEXT,                      -- succeeded | failed
    amount NUMERIC(10, 2),
    currency TEXT,
    paid_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ NOT NULL
);

-- Client telemetry: no FK on user_id, raw text timestamp, duplicates possible.
CREATE TABLE IF NOT EXISTS src.product_events (
    event_id BIGINT PRIMARY KEY,
    event_uid TEXT NOT NULL,
    user_id BIGINT,
    event_type TEXT,
    event_ts_raw TEXT,
    properties JSONB,
    schema_version INT,
    received_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_users_wm ON src.users (updated_at, user_id);
CREATE INDEX IF NOT EXISTS ix_subs_wm ON src.subscriptions (updated_at, subscription_id);
CREATE INDEX IF NOT EXISTS ix_hist_wm ON src.subscription_plan_history (updated_at, history_id);
CREATE INDEX IF NOT EXISTS ix_pay_wm ON src.payments (updated_at, payment_id);
CREATE INDEX IF NOT EXISTS ix_events_wm ON src.product_events (received_at, event_id);

CREATE TABLE IF NOT EXISTS etl.watermarks (
    table_name TEXT PRIMARY KEY,
    last_wm TIMESTAMPTZ,
    last_pk BIGINT,
    last_run_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS etl.ingest_batches (
    batch_id TEXT PRIMARY KEY,
    table_name TEXT NOT NULL,
    run_id TEXT NOT NULL,
    row_count INT NOT NULL,
    from_wm TIMESTAMPTZ,
    to_wm TIMESTAMPTZ,
    file_path TEXT NOT NULL,
    extra_columns TEXT,
    finished_at TIMESTAMPTZ NOT NULL
);
