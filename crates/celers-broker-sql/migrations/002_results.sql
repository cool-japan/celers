-- CeleRS Results Table
-- Stores task execution results for later retrieval

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
    CONSTRAINT chk_result_state CHECK (state IN ('pending', 'success', 'failure'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Index for task_id lookups
CREATE INDEX idx_results_task_id ON celers_results(task_id);

-- Index for expiry cleanup
CREATE INDEX idx_results_expires_at ON celers_results(expires_at);

-- Index for state queries
CREATE INDEX idx_results_state ON celers_results(state);
