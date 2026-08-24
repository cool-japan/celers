-- CeleRS logical queue isolation
--
-- `MysqlBroker::with_queue(url, "payments")` advertises a logical queue for
-- multi-tenancy, but the queue label lived only inside the `metadata` JSON
-- document and no read path filtered on it: two brokers pointed at the same
-- database with different queue names consumed each other's tasks, and
-- `queue_size` / `get_statistics` reported the global backlog.
--
-- This migration promotes the label to a real, indexable column. Existing
-- rows are backfilled from the JSON label they were enqueued with, so tasks
-- already in flight land in the queue they were meant for.
--
-- `ADD COLUMN IF NOT EXISTS` / `CREATE INDEX IF NOT EXISTS` are MariaDB-only
-- syntax that MySQL 8 rejects; this migration is version-tracked in
-- `celers_migrations` and runs exactly once, so the guards are unnecessary.

ALTER TABLE celers_tasks
    ADD COLUMN queue_name VARCHAR(255) NOT NULL DEFAULT 'default';

-- Backfill from the JSON label written by every pre-migration enqueue path.
-- `JSON_EXTRACT` returns SQL NULL when the path is absent, so rows written by
-- paths that never stored a queue label fall back to 'default'.
--
-- Operational note: this is an unbounded UPDATE. On a large existing
-- `celers_tasks` it will hold row locks for its duration, so apply it during
-- a maintenance window on a busy deployment.
UPDATE celers_tasks
SET queue_name = COALESCE(JSON_UNQUOTE(JSON_EXTRACT(metadata, '$.queue')), 'default');

-- Composite index matching the dequeue predicate and ordering exactly:
-- WHERE queue_name = ? AND state = 'pending' AND scheduled_at <= NOW()
-- ORDER BY priority DESC, created_at ASC
CREATE INDEX idx_tasks_queue_dequeue
    ON celers_tasks(queue_name, state, scheduled_at, priority, created_at);
