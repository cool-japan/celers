-- CeleRS task result store
--
-- `celers_task_results` is read and written by `store_result`, `get_result`,
-- `delete_result`, `archive_results`, the batch result helpers and the
-- recurring-task scheduler (`register_recurring_task` /
-- `process_recurring_tasks`, which store each recurring configuration as a
-- JSON document in `result` and claim it with a compare-and-swap on that
-- exact text). No earlier migration ever created the table, so every one of
-- those paths failed at runtime with "table doesn't exist".
--
-- `task_id` is the primary key because `store_result` and
-- `register_recurring_task` both upsert via ON DUPLICATE KEY UPDATE.
--
-- `result` is LONGTEXT rather than JSON on purpose: the scheduler's
-- compare-and-swap claim compares the column against the exact text it
-- previously read, and a JSON column would normalise the stored document.

CREATE TABLE IF NOT EXISTS celers_task_results (
    task_id CHAR(36) PRIMARY KEY,
    task_name VARCHAR(255) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    result LONGTEXT,
    error TEXT,
    traceback TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP NULL,
    runtime_ms BIGINT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

CREATE INDEX idx_task_results_name ON celers_task_results(task_name);

CREATE INDEX idx_task_results_status ON celers_task_results(status);

CREATE INDEX idx_task_results_completed ON celers_task_results(completed_at);
