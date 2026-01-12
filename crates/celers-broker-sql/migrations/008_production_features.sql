-- CeleRS Production Features
-- Additional tables and features for production deployments

-- Rate limiting table
CREATE TABLE IF NOT EXISTS celers_rate_limits (
    id CHAR(36) PRIMARY KEY,
    key_name VARCHAR(255) NOT NULL,
    bucket_name VARCHAR(255) NOT NULL,
    tokens INT NOT NULL DEFAULT 0,
    max_tokens INT NOT NULL,
    refill_rate DOUBLE NOT NULL,
    last_refill TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uk_rate_limit (key_name, bucket_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Task scheduling locks (for distributed scheduling)
CREATE TABLE IF NOT EXISTS celers_locks (
    lock_name VARCHAR(255) PRIMARY KEY,
    owner_id VARCHAR(255) NOT NULL,
    acquired_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Task tags for grouping and filtering
CREATE TABLE IF NOT EXISTS celers_task_tags (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    task_id CHAR(36) NOT NULL,
    tag_name VARCHAR(255) NOT NULL,
    tag_value VARCHAR(1024),
    UNIQUE KEY uk_task_tag (task_id, tag_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Task metrics aggregation
CREATE TABLE IF NOT EXISTS celers_metrics (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    metric_name VARCHAR(255) NOT NULL,
    metric_value DOUBLE NOT NULL,
    task_name VARCHAR(255),
    recorded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    dimensions JSON
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Indexes for production features
CREATE INDEX idx_rate_limits_key ON celers_rate_limits(key_name);
CREATE INDEX idx_locks_expires ON celers_locks(expires_at);
CREATE INDEX idx_task_tags_task ON celers_task_tags(task_id);
CREATE INDEX idx_task_tags_name ON celers_task_tags(tag_name);
CREATE INDEX idx_metrics_name ON celers_metrics(metric_name);
CREATE INDEX idx_metrics_recorded ON celers_metrics(recorded_at);
CREATE INDEX idx_metrics_task ON celers_metrics(task_name);
