-- CeleRS Result Backend Schema for MySQL
-- This schema stores task results, metadata, and chord synchronization state

-- Task results table
CREATE TABLE IF NOT EXISTS celers_task_results (
    task_id CHAR(36) PRIMARY KEY,
    task_name VARCHAR(255) NOT NULL,
    result_state VARCHAR(20) NOT NULL,
    result_data JSON,
    error_message TEXT,
    retry_count INT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP NULL,
    completed_at TIMESTAMP NULL,
    worker VARCHAR(255),
    expires_at TIMESTAMP NULL,
    CONSTRAINT chk_result_state CHECK (result_state IN ('pending', 'started', 'success', 'failure', 'revoked', 'retry'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Chord synchronization state
CREATE TABLE IF NOT EXISTS celers_chord_state (
    chord_id CHAR(36) PRIMARY KEY,
    total INT NOT NULL,
    completed INT NOT NULL DEFAULT 0,
    callback TEXT,
    task_ids JSON NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    timeout_seconds BIGINT,
    cancelled BOOLEAN NOT NULL DEFAULT FALSE,
    cancellation_reason TEXT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Indexes for performance
CREATE INDEX idx_task_results_expires ON celers_task_results(expires_at);

CREATE INDEX idx_task_results_state ON celers_task_results(result_state);

CREATE INDEX idx_task_results_created ON celers_task_results(created_at DESC);

CREATE INDEX idx_chord_completed ON celers_chord_state(completed, total);

-- Stored procedure to clean up expired results
DELIMITER //

CREATE PROCEDURE cleanup_expired_results(OUT deleted_count INT)
BEGIN
    DELETE FROM celers_task_results
    WHERE expires_at IS NOT NULL AND expires_at < NOW();

    SET deleted_count = ROW_COUNT();
END//

DELIMITER ;

-- Stored procedure to atomically increment chord counter
DELIMITER //

CREATE PROCEDURE chord_increment_counter(IN chord_uuid CHAR(36), OUT new_count INT)
BEGIN
    UPDATE celers_chord_state
    SET completed = completed + 1
    WHERE chord_id = chord_uuid;

    SELECT completed INTO new_count
    FROM celers_chord_state
    WHERE chord_id = chord_uuid;
END//

DELIMITER ;
