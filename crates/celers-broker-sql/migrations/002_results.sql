-- CeleRS Results Table
-- Stores task execution results for later retrieval
--
-- # Why the CHECK constraint is not called `chk_result_state`
--
-- MySQL scopes CHECK constraint names to the **schema**, not the table
-- (`ERROR 3822 (HY000): Duplicate check constraint name`).
-- `celers-backend-db`'s `celers_task_results` declares a `chk_result_state`
-- of its own, and a normal deployment points both crates at one database, so
-- the shorter name made whichever crate migrated second fail — the same class
-- of collision as the `celers_task_results` table name itself (Known gaps
-- #16). Every constraint this crate creates is therefore named for the
-- broker. Databases created before the rename are upgraded in Rust: see
-- `MysqlBroker::rename_legacy_result_state_constraint`.

CREATE TABLE IF NOT EXISTS celers_results (
    id CHAR(36) PRIMARY KEY,
    task_id CHAR(36) NOT NULL,
    task_name VARCHAR(255) NOT NULL,
    result MEDIUMBLOB,
    error_message TEXT,
    state VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP NULL,
    expires_at TIMESTAMP NULL,
    CONSTRAINT chk_broker_result_state CHECK (state IN ('pending', 'success', 'failure'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Index for task_id lookups
CREATE INDEX idx_results_task_id ON celers_results(task_id);

-- Index for expiry cleanup
CREATE INDEX idx_results_expires_at ON celers_results(expires_at);

-- Index for state queries
CREATE INDEX idx_results_state ON celers_results(state);
