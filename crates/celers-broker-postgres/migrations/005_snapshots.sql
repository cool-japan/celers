-- Queue snapshot metadata table
CREATE TABLE IF NOT EXISTS celers_queue_snapshots (
    snapshot_id VARCHAR(36) PRIMARY KEY,
    queue_name  VARCHAR(255) NOT NULL,
    created_at  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    task_count  BIGINT NOT NULL DEFAULT 0,
    total_size_bytes BIGINT NOT NULL DEFAULT 0,
    includes_results BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE INDEX IF NOT EXISTS idx_queue_snapshots_queue_name
    ON celers_queue_snapshots(queue_name, created_at DESC);
