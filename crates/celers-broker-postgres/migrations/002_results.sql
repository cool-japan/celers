-- CeleRS Results Table for PostgreSQL
-- Stores task execution results for later retrieval

CREATE TABLE IF NOT EXISTS celers_results (
    id UUID PRIMARY KEY,
    task_id UUID NOT NULL,
    task_name VARCHAR(255) NOT NULL,
    result BYTEA,
    error_message TEXT,
    state VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMP WITH TIME ZONE,
    expires_at TIMESTAMP WITH TIME ZONE,
    CHECK (state IN ('pending', 'success', 'failure'))
);

-- Index for task_id lookups
CREATE INDEX IF NOT EXISTS idx_results_task_id ON celers_results(task_id);

-- Index for expiry cleanup
CREATE INDEX IF NOT EXISTS idx_results_expires_at ON celers_results(expires_at)
    WHERE expires_at IS NOT NULL;

-- Index for state queries
CREATE INDEX IF NOT EXISTS idx_results_state ON celers_results(state);
