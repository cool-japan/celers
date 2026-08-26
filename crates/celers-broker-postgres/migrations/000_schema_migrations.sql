-- CeleRS PostgreSQL broker — migration ledger.
--
-- `PostgresBroker::migrate()` used to replay every migration file on every
-- call. Each file is idempotent (`IF NOT EXISTS` / `OR REPLACE`), so the
-- *result* was correct, but replaying DDL against a live queue is not free:
--
--  * `CREATE INDEX IF NOT EXISTS` takes a `ShareLock` on `celers_tasks` even
--    when the index already exists, while an ordinary `INSERT`/`DELETE` holds
--    `RowExclusiveLock` on the same table and reaches for another. Postgres
--    resolves the cycle by killing one side: `ERROR: deadlock detected`.
--  * `CREATE OR REPLACE FUNCTION` races another session running the same
--    statement on the same `pg_proc` row: `ERROR: tuple concurrently updated`.
--  * `007_queue_identity.sql` *replaces* the `move_to_dlq()` defined in
--    `001_init.sql`, so a replay briefly reinstates the older definition —
--    the one that does not carry `queue_name` across into the dead-letter
--    queue. A worker rejecting a task inside that window files the DLQ row
--    under the `default` queue and it is never seen again.
--
-- None of that is hypothetical: it is what a fleet of workers all calling
-- `migrate()` on start-up does to a queue that is already serving traffic.
--
-- This table is the fix. Every migration file records its name here once it
-- has been applied, inside the same transaction that applies it, and
-- `migrate()` skips the files already listed. A second call — from another
-- worker, another process, or the next test — reads this table and does no
-- DDL at all.
--
-- This file is the one exception to its own rule: it bootstraps the ledger,
-- so it runs on every `migrate()` call and is never recorded in it. A bare
-- `CREATE TABLE IF NOT EXISTS` on a table that already exists touches no
-- other relation, so it cannot deadlock against queue traffic.

CREATE TABLE IF NOT EXISTS celers_schema_migrations (
    name       VARCHAR(128) PRIMARY KEY,
    applied_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
