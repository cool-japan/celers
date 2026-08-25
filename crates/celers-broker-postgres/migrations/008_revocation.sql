-- CeleRS PostgreSQL broker — durable task revocation.
--
-- Mirrors celers-broker-redis's `<queue>:revoked` sorted set: a durable,
-- expiring record of every revoked task id, scoped per logical queue, plus
-- LISTEN/NOTIFY (issued directly by `PostgresBroker::revoke`, not by a
-- trigger — `pg_notify(channel, payload)` is a plain function call, so both
-- arguments bind as ordinary parameters) for workers that are already
-- executing the task.
--
-- Every statement here is idempotent: `PostgresBroker::migrate()` runs the
-- whole migration set on every call, including from the env-gated
-- integration tests.

CREATE TABLE IF NOT EXISTS celers_revoked_tasks (
    task_id    UUID NOT NULL,
    queue_name VARCHAR(255) NOT NULL,
    terminate  BOOLEAN NOT NULL DEFAULT FALSE,
    revoked_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (queue_name, task_id)
);

-- `is_revoked()` looks up one (queue_name, task_id) pair — already covered by
-- the primary key. This index is for the opportunistic prune `revoke()` runs
-- on every call (`DELETE ... WHERE expires_at < NOW()`), which would
-- otherwise be a sequential scan as the table grows.
CREATE INDEX IF NOT EXISTS idx_revoked_tasks_expiry
    ON celers_revoked_tasks (expires_at);
