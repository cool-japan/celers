-- CeleRS Idempotency Keys Table
-- Supports idempotent task submission by tracking unique keys

CREATE TABLE IF NOT EXISTS celers_idempotency_keys (
    idempotency_key VARCHAR(255) PRIMARY KEY,
    task_id CHAR(36) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP NOT NULL,
    UNIQUE KEY uk_idempotency_task (idempotency_key, task_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Index for expiry cleanup
CREATE INDEX idx_idempotency_expires ON celers_idempotency_keys(expires_at);

-- Index for task_id lookups
CREATE INDEX idx_idempotency_task_id ON celers_idempotency_keys(task_id);
