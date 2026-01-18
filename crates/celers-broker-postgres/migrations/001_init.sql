-- CeleRS Task Queue Schema
-- This schema implements a robust task queue with priorities, retries, and DLQ support

-- Main task queue table
CREATE TABLE IF NOT EXISTS celers_tasks (
    id UUID PRIMARY KEY,
    task_name VARCHAR(255) NOT NULL,
    payload BYTEA NOT NULL,
    state VARCHAR(20) NOT NULL DEFAULT 'pending',
    priority INTEGER NOT NULL DEFAULT 0,
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    scheduled_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    started_at TIMESTAMP WITH TIME ZONE,
    completed_at TIMESTAMP WITH TIME ZONE,
    worker_id VARCHAR(255),
    error_message TEXT,
    metadata JSONB,
    CHECK (state IN ('pending', 'processing', 'completed', 'failed', 'cancelled'))
);

-- Dead Letter Queue for permanently failed tasks
CREATE TABLE IF NOT EXISTS celers_dead_letter_queue (
    id UUID PRIMARY KEY,
    task_id UUID NOT NULL,
    task_name VARCHAR(255) NOT NULL,
    payload BYTEA NOT NULL,
    retry_count INTEGER NOT NULL,
    error_message TEXT,
    failed_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    metadata JSONB
);

-- Task execution history (optional, for auditing)
CREATE TABLE IF NOT EXISTS celers_task_history (
    id BIGSERIAL PRIMARY KEY,
    task_id UUID NOT NULL,
    state VARCHAR(20) NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    worker_id VARCHAR(255),
    message TEXT
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_tasks_state_priority ON celers_tasks(state, priority DESC, created_at ASC)
    WHERE state = 'pending';

CREATE INDEX IF NOT EXISTS idx_tasks_scheduled ON celers_tasks(scheduled_at)
    WHERE state = 'pending';

CREATE INDEX IF NOT EXISTS idx_tasks_worker ON celers_tasks(worker_id)
    WHERE state = 'processing';

CREATE INDEX IF NOT EXISTS idx_dlq_failed_at ON celers_dead_letter_queue(failed_at DESC);

CREATE INDEX IF NOT EXISTS idx_history_task_id ON celers_task_history(task_id, timestamp DESC);

-- Function to move tasks to DLQ when they exceed max retries
CREATE OR REPLACE FUNCTION move_to_dlq(task_uuid UUID) RETURNS VOID AS $$
BEGIN
    INSERT INTO celers_dead_letter_queue (id, task_id, task_name, payload, retry_count, error_message, metadata)
    SELECT
        gen_random_uuid(),
        id,
        task_name,
        payload,
        retry_count,
        error_message,
        metadata
    FROM celers_tasks
    WHERE id = task_uuid;

    DELETE FROM celers_tasks WHERE id = task_uuid;
END;
$$ LANGUAGE plpgsql;
