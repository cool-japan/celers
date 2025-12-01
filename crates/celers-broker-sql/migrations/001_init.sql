-- CeleRS Task Queue Schema for MySQL
-- This schema implements a robust task queue with priorities, retries, and DLQ support

-- Main task queue table
CREATE TABLE IF NOT EXISTS celers_tasks (
    id CHAR(36) PRIMARY KEY,
    task_name VARCHAR(255) NOT NULL,
    payload MEDIUMBLOB NOT NULL,
    state VARCHAR(20) NOT NULL DEFAULT 'pending',
    priority INT NOT NULL DEFAULT 0,
    retry_count INT NOT NULL DEFAULT 0,
    max_retries INT NOT NULL DEFAULT 3,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    scheduled_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP NULL,
    completed_at TIMESTAMP NULL,
    worker_id VARCHAR(255),
    error_message TEXT,
    metadata JSON,
    CONSTRAINT chk_state CHECK (state IN ('pending', 'processing', 'completed', 'failed', 'cancelled'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Dead Letter Queue for permanently failed tasks
CREATE TABLE IF NOT EXISTS celers_dead_letter_queue (
    id CHAR(36) PRIMARY KEY,
    task_id CHAR(36) NOT NULL,
    task_name VARCHAR(255) NOT NULL,
    payload MEDIUMBLOB NOT NULL,
    retry_count INT NOT NULL,
    error_message TEXT,
    failed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    metadata JSON
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Task execution history (optional, for auditing)
CREATE TABLE IF NOT EXISTS celers_task_history (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    task_id CHAR(36) NOT NULL,
    state VARCHAR(20) NOT NULL,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    worker_id VARCHAR(255),
    message TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Indexes for performance
-- Covering index for pending tasks ordered by priority
CREATE INDEX idx_tasks_state_priority ON celers_tasks(state, priority DESC, created_at ASC);

-- Index for scheduled tasks
CREATE INDEX idx_tasks_scheduled ON celers_tasks(scheduled_at, state);

-- Index for worker tracking
CREATE INDEX idx_tasks_worker ON celers_tasks(worker_id, state);

-- Index for DLQ queries
CREATE INDEX idx_dlq_failed_at ON celers_dead_letter_queue(failed_at DESC);

-- Index for history queries
CREATE INDEX idx_history_task_id ON celers_task_history(task_id, timestamp DESC);

-- Stored procedure to move tasks to DLQ when they exceed max retries
DELIMITER //

CREATE PROCEDURE move_to_dlq(IN task_uuid CHAR(36))
BEGIN
    -- Insert into DLQ
    INSERT INTO celers_dead_letter_queue (id, task_id, task_name, payload, retry_count, error_message, metadata)
    SELECT
        UUID(),
        id,
        task_name,
        payload,
        retry_count,
        error_message,
        metadata
    FROM celers_tasks
    WHERE id = task_uuid;

    -- Delete from tasks table
    DELETE FROM celers_tasks WHERE id = task_uuid;
END//

DELIMITER ;
