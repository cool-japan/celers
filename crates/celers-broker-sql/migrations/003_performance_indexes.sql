-- CeleRS Performance Indexes
-- Additional indexes for improved query performance.
--
-- NOTE: `CREATE INDEX IF NOT EXISTS` is MariaDB-only syntax; MySQL 8 rejects
-- it with a parse error. This migration is version-tracked in
-- `celers_migrations`, so it runs exactly once and does not need the guard.

-- Composite index for efficient task dequeuing
CREATE INDEX idx_tasks_dequeue ON celers_tasks(state, priority DESC, scheduled_at ASC, created_at ASC);

-- Index for stale task detection (tasks stuck in processing state)
CREATE INDEX idx_tasks_stale ON celers_tasks(state, started_at);

-- Index for task name queries
CREATE INDEX idx_tasks_name ON celers_tasks(task_name);

-- Index for created_at range queries
CREATE INDEX idx_tasks_created_range ON celers_tasks(created_at);

-- Index for completed_at cleanup queries
CREATE INDEX idx_tasks_completed ON celers_tasks(completed_at);
