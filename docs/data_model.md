# Data model

## Source (PostgreSQL schema `src`)
| Table | PK | FKs | Cursor | Notes |
|---|---|---|---|---|
| users | user_id | – | updated_at | segment/country/channel optional |
| subscriptions | subscription_id | user_id → users | updated_at | `started_at` NULL while trialing; mutable (plan/status change) |
| subscription_plan_history | history_id | subscription_id → subscriptions | updated_at | append-only upgrades/downgrades with old/new MRR |
| payments | payment_id | subscription_id → subscriptions | updated_at | `kind` charge/refund, amount always positive |
| product_events | event_id | none (client telemetry) | received_at | raw text `event_ts_raw`, JSONB `properties`, duplicates possible |

ETL control tables (`etl.watermarks`, `etl.ingest_batches`) hold the cursor and an audit row per Bronze batch.

## Injected data problems (generator)
Duplicate events (~2%), late events (~3%, 1-4 days), missing optional fields, invalid timestamps / event types / statuses / segments, orphan user ids, mixed timestamp formats, additive schema change (`product_events.device`) after 75% of the window, repeated ingestion (rerun) .

## Layers
- **Bronze** `data/bronze/<table>/ingest_date=YYYY-MM-DD/batch_<id>.parquet`: `_source_table, _source_pk, _watermark, _batch_id, _run_id, _ingested_at, payload(JSON of the full source row)`. Immutable; partitioned by ingest date (low volume; coarse partitions avoid tiny files).
- **Silver** `data/silver/<table>.parquet` + `silver/quarantine/<table>.parquet` (`source_pk, reject_reason, payload, batch_id, ingested_at`). Naive-UTC timestamps, one row per business key. Event dedup key: `event_uid` (first received wins; copies quarantined as `duplicate_event_uid`).
- **Gold** `data/gold/*.parquet`

| Table | Grain |
|---|---|
| dim_user | one row per accepted user (first_paid_at, current_plan) |
| dim_plan | one row per plan |
| dim_date | one row per calendar day |
| fact_subscription_plan_period | subscription × plan period `[valid_from, valid_to)` with MRR |
| fact_mrr_movement | subscription × MRR-changing event (new/expansion/contraction/churn) |
| fact_payments | one row per payment/refund, with `signed_amount` cash view and plan at payment time |
| fact_product_activity_daily | user × activity day (plan at end of day) |

## Rejection reasons
users: invalid_email, invalid_segment, invalid_signup_ts · subscriptions: orphan_user, invalid_status, invalid_plan, invalid_mrr, missing_started_at, missing_canceled_at, canceled_before_started · history: orphan_subscription, invalid_change_ts, invalid_plan, invalid_mrr · payments: orphan_subscription, invalid_kind, invalid_status, invalid_amount, unsupported_currency, invalid_paid_at · events: duplicate_event_uid, invalid_timestamp, invalid_event_type, future_timestamp, orphan_user. Cascades are intentional: a rejected subscription also orphans its payments.
