-- CeleRS PostgreSQL broker — queue identity, delivery accounting and
-- first-class schedule/group storage.
--
-- Every statement here is idempotent (IF NOT EXISTS / OR REPLACE / guarded
-- UPDATE): `PostgresBroker::migrate()` runs the whole migration set on every
-- call, including from the env-gated integration tests.
--
-- 1. `queue_name` becomes a REAL column on `celers_tasks` and
--    `celers_dead_letter_queue`. It used to live only inside
--    `metadata->>'queue'`, while roughly thirty public APIs filtered on a
--    `queue_name` column that did not exist — every one of those calls failed
--    with `column "queue_name" does not exist`. Existing rows are backfilled
--    from the JSON label so no task loses its queue identity.
-- 2. `attempt_count` separates "how many times has this been delivered" from
--    `retry_count` ("how many times has this been retried"). `dequeue()` used
--    to increment `retry_count`, silently consuming one retry on the very
--    first delivery.
-- 3. Periodic schedules and task groups get real tables instead of being
--    smuggled into the dispatch table (where a worker would claim a schedule
--    row as if it were a task) or dropped on the floor entirely.

-- ── 1. Queue identity ──────────────────────────────────────────────────────

ALTER TABLE celers_tasks
    ADD COLUMN IF NOT EXISTS queue_name VARCHAR(255) NOT NULL DEFAULT 'default';

ALTER TABLE celers_dead_letter_queue
    ADD COLUMN IF NOT EXISTS queue_name VARCHAR(255) NOT NULL DEFAULT 'default';

-- Backfill from the legacy JSON label. The `IS DISTINCT FROM` guard makes a
-- re-run a no-op instead of a full-table rewrite.
UPDATE celers_tasks
   SET queue_name = metadata->>'queue'
 WHERE metadata->>'queue' IS NOT NULL
   AND metadata->>'queue' <> ''
   AND queue_name IS DISTINCT FROM metadata->>'queue';

UPDATE celers_dead_letter_queue
   SET queue_name = metadata->>'queue'
 WHERE metadata->>'queue' IS NOT NULL
   AND metadata->>'queue' <> ''
   AND queue_name IS DISTINCT FROM metadata->>'queue';

-- ── 2. Delivery accounting ─────────────────────────────────────────────────

ALTER TABLE celers_tasks
    ADD COLUMN IF NOT EXISTS attempt_count INTEGER NOT NULL DEFAULT 0;

-- Rows written by an older build had their first delivery charged to
-- `retry_count`; move that charge onto `attempt_count` for tasks that are
-- currently in flight so their remaining retry budget is correct.
UPDATE celers_tasks
   SET attempt_count = GREATEST(attempt_count, retry_count),
       retry_count = GREATEST(retry_count - 1, 0)
 WHERE state = 'processing'
   AND attempt_count = 0
   AND retry_count > 0;

-- ── 3. Dispatch indexes matching the queue-scoped predicates ───────────────

-- The dequeue predicate is (queue_name, state='pending', scheduled_at<=NOW())
-- ordered by (priority DESC, created_at ASC). A partial index keeps its size
-- proportional to the live backlog rather than to lifetime throughput.
CREATE INDEX IF NOT EXISTS idx_tasks_queue_dispatch
    ON celers_tasks (queue_name, priority DESC, created_at ASC)
    WHERE state = 'pending';

CREATE INDEX IF NOT EXISTS idx_tasks_queue_state
    ON celers_tasks (queue_name, state);

-- Retention/purge sweeps terminal rows by completion time.
CREATE INDEX IF NOT EXISTS idx_tasks_terminal_completed_at
    ON celers_tasks (queue_name, completed_at)
    WHERE state IN ('completed', 'cancelled', 'failed');

CREATE INDEX IF NOT EXISTS idx_dlq_queue_failed_at
    ON celers_dead_letter_queue (queue_name, failed_at DESC);

-- ── 4. Idempotent DLQ promotion ────────────────────────────────────────────

-- Two concurrent rejects reading the same pre-delete snapshot used to write
-- two DLQ rows for one task. Collapse any pre-existing duplicates (keeping the
-- physically first row per task), then enforce uniqueness.
DELETE FROM celers_dead_letter_queue d
      USING celers_dead_letter_queue e
      WHERE d.task_id = e.task_id
        AND d.ctid > e.ctid;

CREATE UNIQUE INDEX IF NOT EXISTS celers_dlq_task_id_uniq
    ON celers_dead_letter_queue (task_id);

-- Carry `queue_name` across into the DLQ and make the promotion idempotent.
CREATE OR REPLACE FUNCTION move_to_dlq(task_uuid UUID) RETURNS VOID AS $$
BEGIN
    INSERT INTO celers_dead_letter_queue
        (id, task_id, task_name, payload, retry_count, error_message, metadata, queue_name)
    SELECT
        gen_random_uuid(),
        id,
        task_name,
        payload,
        retry_count,
        error_message,
        metadata,
        queue_name
    FROM celers_tasks
    WHERE id = task_uuid
    ON CONFLICT (task_id) DO NOTHING;

    DELETE FROM celers_tasks WHERE id = task_uuid;
END;
$$ LANGUAGE plpgsql;

-- ── 5. Periodic schedules ──────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS celers_periodic_schedules (
    schedule_id     VARCHAR(36) PRIMARY KEY,
    queue_name      VARCHAR(255) NOT NULL DEFAULT 'default',
    task_name       VARCHAR(255) NOT NULL,
    cron_expression VARCHAR(255) NOT NULL,
    payload         JSONB NOT NULL DEFAULT '{}'::jsonb,
    priority        INTEGER NOT NULL DEFAULT 0,
    enabled         BOOLEAN NOT NULL DEFAULT TRUE,
    last_run        TIMESTAMP WITH TIME ZONE,
    next_run        TIMESTAMP WITH TIME ZONE,
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_periodic_schedules_queue
    ON celers_periodic_schedules (queue_name, enabled);

-- ── 6. Task groups ─────────────────────────────────────────────────────────

CREATE TABLE IF NOT EXISTS celers_task_groups (
    group_id    UUID PRIMARY KEY,
    queue_name  VARCHAR(255) NOT NULL DEFAULT 'default',
    group_name  VARCHAR(255) NOT NULL,
    description TEXT,
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_task_groups_queue_name
    ON celers_task_groups (queue_name, group_name);
