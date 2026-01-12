-- CeleRS Deduplication Table for PostgreSQL
-- Supports task deduplication by tracking unique keys

CREATE TABLE IF NOT EXISTS celers_deduplication (
    dedup_key VARCHAR(255) PRIMARY KEY,
    task_id UUID NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP WITH TIME ZONE NOT NULL
);

-- Index for expiry cleanup
CREATE INDEX IF NOT EXISTS idx_dedup_expires_at ON celers_deduplication(expires_at);

-- Index for task_id lookups
CREATE INDEX IF NOT EXISTS idx_dedup_task_id ON celers_deduplication(task_id);

-- Function to clean expired deduplication keys
CREATE OR REPLACE FUNCTION clean_expired_dedup_keys() RETURNS VOID AS $$
BEGIN
    DELETE FROM celers_deduplication WHERE expires_at < NOW();
END;
$$ LANGUAGE plpgsql;
