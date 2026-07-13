-- CeleRS Deduplication Schema Reconciliation for PostgreSQL
-- Adds columns that celers-broker-postgres/src/deduplication.rs already queries
-- but that were missing from the original 004_deduplication.sql schema.
-- Purely additive/idempotent: safe to run against an existing celers_deduplication table.

ALTER TABLE celers_deduplication ADD COLUMN IF NOT EXISTS idempotency_key VARCHAR(255);
ALTER TABLE celers_deduplication ADD COLUMN IF NOT EXISTS task_name VARCHAR(255);
ALTER TABLE celers_deduplication ADD COLUMN IF NOT EXISTS queue_name VARCHAR(255) NOT NULL DEFAULT 'default';
ALTER TABLE celers_deduplication ADD COLUMN IF NOT EXISTS first_seen_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();
ALTER TABLE celers_deduplication ADD COLUMN IF NOT EXISTS last_seen_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW();
ALTER TABLE celers_deduplication ADD COLUMN IF NOT EXISTS duplicate_count INTEGER NOT NULL DEFAULT 0;

-- dedup_key is the existing PRIMARY KEY but the application code's INSERT never
-- populates it directly; give it a generated default so the existing Rust
-- INSERT (which lists idempotency_key, task_id, task_name, queue_name,
-- first_seen_at, last_seen_at, expires_at, duplicate_count but NOT dedup_key)
-- continues to satisfy the NOT NULL/PRIMARY KEY constraint.
-- gen_random_uuid() is already used and confirmed available in 001_init.sql (move_to_dlq()).
ALTER TABLE celers_deduplication ALTER COLUMN dedup_key SET DEFAULT gen_random_uuid()::text;

-- Required for the code's `ON CONFLICT (idempotency_key, queue_name) DO NOTHING` clause
-- in enqueue_idempotent() to be valid — Postgres requires the ON CONFLICT target to
-- match an existing unique index/constraint.
CREATE UNIQUE INDEX IF NOT EXISTS celers_deduplication_idempotency_queue_uniq
    ON celers_deduplication (idempotency_key, queue_name);
