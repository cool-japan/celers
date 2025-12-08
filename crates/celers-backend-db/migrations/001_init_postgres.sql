-- CeleRS Result Backend Schema for PostgreSQL
-- This schema stores task results, metadata, and chord synchronization state

-- Task results table
CREATE TABLE IF NOT EXISTS celers_task_results (
    task_id UUID PRIMARY KEY,
    task_name VARCHAR(255) NOT NULL,
    result_state VARCHAR(20) NOT NULL,
    result_data JSONB,
    error_message TEXT,
    retry_count INTEGER,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    worker VARCHAR(255),
    expires_at TIMESTAMP WITH TIME ZONE,
    CHECK (result_state IN ('pending', 'started', 'success', 'failure', 'revoked', 'retry'))
);

-- Chord synchronization state
CREATE TABLE IF NOT EXISTS celers_chord_state (
    chord_id UUID PRIMARY KEY,
    total INTEGER NOT NULL,
    completed INTEGER NOT NULL DEFAULT 0,
    callback TEXT,
    task_ids JSONB NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    timeout_seconds BIGINT,
    cancelled BOOLEAN NOT NULL DEFAULT FALSE,
    cancellation_reason TEXT
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_task_results_expires ON celers_task_results(expires_at)
    WHERE expires_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_task_results_state ON celers_task_results(result_state);

CREATE INDEX IF NOT EXISTS idx_task_results_created ON celers_task_results(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_chord_completed ON celers_chord_state(completed, total);

-- Function to clean up expired results
CREATE OR REPLACE FUNCTION cleanup_expired_results() RETURNS INTEGER AS $$
DECLARE
    deleted_count INTEGER;
BEGIN
    DELETE FROM celers_task_results
    WHERE expires_at IS NOT NULL AND expires_at < NOW();

    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RETURN deleted_count;
END;
$$ LANGUAGE plpgsql;

-- Function to atomically increment chord counter
CREATE OR REPLACE FUNCTION chord_increment_counter(chord_uuid UUID) RETURNS INTEGER AS $$
DECLARE
    new_count INTEGER;
BEGIN
    UPDATE celers_chord_state
    SET completed = completed + 1
    WHERE chord_id = chord_uuid
    RETURNING completed INTO new_count;

    RETURN new_count;
END;
$$ LANGUAGE plpgsql;
