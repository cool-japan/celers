-- CeleRS broker-side task result store
--
-- `celers_broker_results` is read and written by `store_result`, `get_result`,
-- `delete_result`, `archive_results`, the batch result helpers and the
-- recurring-task scheduler (`register_recurring_task` /
-- `process_recurring_tasks`, which store each recurring configuration as a
-- JSON document in `result` and claim it with a compare-and-swap on that
-- exact text). No earlier migration ever created the table, so every one of
-- those paths failed at runtime with "table doesn't exist".
--
-- # Why not `celers_task_results`
--
-- This table used to be called `celers_task_results` — the same name
-- `celers-backend-db`'s `MysqlResultBackend` auto-migrates, with a completely
-- different schema (`result_state`/`result_data`/`extra` there,
-- `status`/`result`/`traceback` here). Pointing both crates at one MySQL
-- database therefore broke whichever migrated second: `CREATE TABLE IF NOT
-- EXISTS` silently no-op'd against the other crate's table and the follow-up
-- `CREATE INDEX` died with `ERROR 1072 (42000): Key column 'status' doesn't
-- exist in table`.
--
-- `celers-backend-db` is CeleRS's result *backend* and keeps the
-- `celers_task_results` name; this broker-internal store is
-- `celers_broker_results`. An existing database that still carries the old
-- name is upgraded in Rust before this file runs — see
-- `MysqlBroker::rename_legacy_broker_results_table`, which renames the legacy
-- table (preserving its rows) only when it actually carries *this* schema,
-- and leaves `celers-backend-db`'s identically-named table untouched.
--
-- `task_id` is the primary key because `store_result` and
-- `register_recurring_task` both upsert via ON DUPLICATE KEY UPDATE.
--
-- `result` is LONGTEXT rather than JSON on purpose: the scheduler's
-- compare-and-swap claim compares the column against the exact text it
-- previously read, and a JSON column would normalise the stored document.

CREATE TABLE IF NOT EXISTS celers_broker_results (
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

-- The index names are deliberately unchanged from the pre-rename schema:
-- `RENAME TABLE` carries a table's indexes across under their existing names,
-- so an upgraded database already has these three. Re-issuing them here fails
-- with `ERROR 1061 (42000): Duplicate key name`, which
-- `execute_migration_statement` already treats as "the post-condition already
-- holds" — whereas fresh names would leave an upgraded database carrying two
-- indexes per column. MySQL scopes index names per table, so there is no
-- clash with `celers-backend-db`'s `idx_task_results_expires` /
-- `idx_task_results_state` / `idx_task_results_created` on its own
-- `celers_task_results`.

CREATE INDEX idx_task_results_name ON celers_broker_results(task_name);

CREATE INDEX idx_task_results_status ON celers_broker_results(status);

CREATE INDEX idx_task_results_completed ON celers_broker_results(completed_at);
