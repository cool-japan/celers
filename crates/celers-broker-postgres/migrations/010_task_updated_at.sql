-- CeleRS PostgreSQL broker — `celers_tasks.updated_at`.
--
-- # Why this file exists
--
-- Two analytics entry points read a `celers_tasks.updated_at` column that no
-- migration ever created, so both failed against a live server with
-- `ERROR: column "updated_at" does not exist`:
--
--  * `analytics.rs`'s `get_state_transition_history` — both branches
--    (`task_id = Some(..)` and `None`) project `updated_at`, window over it
--    (`LAG(...) OVER (PARTITION BY id ORDER BY updated_at)`), filter on it
--    (`updated_at >= NOW() - INTERVAL '1 hour' * $n`) and order by it;
--  * `analytics.rs`'s `detect_abnormal_state_duration("processing", ..)` —
--    `EXTRACT(EPOCH FROM (NOW() - COALESCE(started_at, updated_at)))`, which
--    is how a task claimed by a worker that never recorded `started_at` is
--    still aged correctly.
--
-- # Why it is maintained explicitly rather than by a trigger
--
-- A `BEFORE UPDATE` trigger would keep the column current with no call-site
-- changes, but it hides the write: `EXPLAIN`, `pg_stat_statements` and a
-- plain reading of the SQL would all show a statement that does not touch
-- `updated_at` while it silently does, and the behaviour would live in the
-- database rather than in the crate that is being reviewed. Every
-- `UPDATE celers_tasks` in this crate therefore carries an explicit
-- `updated_at = NOW()` clause instead (roughly thirty statements across
-- `sql.rs`, `convenience.rs`, `advanced_ops.rs`, `workflows.rs`,
-- `analytics.rs`, `dlq.rs`, `db_monitoring.rs` and `scheduling.rs`), which is
-- also what makes the column portable to the SQLite/MySQL sibling brokers.
--
-- # Backfill, and why the column is added nullable first
--
-- `ADD COLUMN ... NOT NULL DEFAULT NOW()` in one statement would stamp every
-- pre-existing row with the *migration's* timestamp, which would make every
-- historical task look as if it had just been touched — and
-- `detect_abnormal_state_duration("processing", ..)` would under-report every
-- genuinely stuck task for as long as the threshold window. Adding the column
-- nullable, backfilling it from the row's real last-touch time
-- (`completed_at`, else `started_at`, else `created_at` — the first of those
-- is `NOT NULL`, so the `COALESCE` can never stay `NULL`) and only then
-- pinning `DEFAULT NOW()` / `NOT NULL` gives every row an honest value.
--
-- The `WHERE updated_at IS NULL` guard is what makes the file replayable: a
-- second run finds no unbacked-filled rows and does not reset the values the
-- crate has been maintaining since. `ADD COLUMN IF NOT EXISTS`,
-- `SET DEFAULT` and `SET NOT NULL` are all idempotent in their own right.

ALTER TABLE celers_tasks
    ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITH TIME ZONE;

UPDATE celers_tasks
   SET updated_at = COALESCE(completed_at, started_at, created_at)
 WHERE updated_at IS NULL;

ALTER TABLE celers_tasks
    ALTER COLUMN updated_at SET DEFAULT NOW();

ALTER TABLE celers_tasks
    ALTER COLUMN updated_at SET NOT NULL;

-- `get_state_transition_history` scans one queue's recently touched rows in
-- `updated_at DESC` order; without this it is a full scan plus a sort of the
-- whole queue.
CREATE INDEX IF NOT EXISTS idx_tasks_queue_updated_at
    ON celers_tasks (queue_name, updated_at DESC);
