-- CeleRS MySQL broker — durable task revocation.
--
-- Mirrors celers-broker-redis's `<queue>:revoked` sorted set and
-- celers-broker-postgres's `celers_revoked_tasks` (LISTEN/NOTIFY-backed)
-- table of the same name: a durable, expiring record of every revoked task
-- id, scoped per logical queue. MySQL has no LISTEN/NOTIFY equivalent, so
-- there is no push channel here — `MysqlBroker::subscribe_revocations`
-- POLLS this table instead (see `revocation.rs`'s `MysqlRevocationStream`),
-- which trades notification latency (bounded by the poll interval, a few
-- seconds by default) for needing no extra long-lived connection or
-- database feature.
--
-- `revoked_at`/`expires_at` are `DATETIME(6)` (microsecond precision) rather
-- than the crate's usual bare `DATETIME`/`TIMESTAMP`: the poller's read
-- cursor is a strict `revoked_at > <last seen>` (see `revocation.rs`'s
-- `POLL_REVOCATIONS` doc for why strict, not `>=`), and coarser (second)
-- precision would make two distinct revocations tie at that cursor boundary
-- far more likely — the one failure mode a strict `>` cursor cannot recover
-- from (it would silently skip the tied row rather than merely redeliver
-- something).

CREATE TABLE IF NOT EXISTS celers_revoked_tasks (
    task_id    CHAR(36) NOT NULL,
    queue_name VARCHAR(255) NOT NULL,
    terminate  TINYINT(1) NOT NULL DEFAULT 0,
    revoked_at DATETIME(6) NOT NULL,
    expires_at DATETIME(6) NOT NULL,
    PRIMARY KEY (queue_name, task_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- `is_revoked()` looks up one (queue_name, task_id) pair — already covered by
-- the primary key. This index backs the opportunistic prune `revoke()` runs
-- on every call.
CREATE INDEX idx_revoked_tasks_expiry ON celers_revoked_tasks(expires_at);

-- Backs the poller's `queue_name = ? AND revoked_at > ?` cursor query.
CREATE INDEX idx_revoked_tasks_poll ON celers_revoked_tasks(queue_name, revoked_at);
