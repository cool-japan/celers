-- CeleRS Workflow DAG Support
-- Tables for workflow orchestration and task dependencies

-- Workflow definitions table
CREATE TABLE IF NOT EXISTS celers_workflows (
    id CHAR(36) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    state VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP NULL,
    completed_at TIMESTAMP NULL,
    metadata JSON,
    CONSTRAINT chk_workflow_state CHECK (state IN ('pending', 'running', 'completed', 'failed', 'cancelled'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Workflow task nodes table
CREATE TABLE IF NOT EXISTS celers_workflow_nodes (
    id CHAR(36) PRIMARY KEY,
    workflow_id CHAR(36) NOT NULL,
    task_id CHAR(36),
    node_name VARCHAR(255) NOT NULL,
    node_type VARCHAR(50) NOT NULL DEFAULT 'task',
    state VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP NULL,
    completed_at TIMESTAMP NULL,
    result MEDIUMBLOB,
    error_message TEXT,
    CONSTRAINT fk_workflow_nodes_workflow FOREIGN KEY (workflow_id) REFERENCES celers_workflows(id) ON DELETE CASCADE,
    CONSTRAINT chk_node_state CHECK (state IN ('pending', 'running', 'completed', 'failed', 'skipped'))
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Workflow edges (dependencies between nodes)
CREATE TABLE IF NOT EXISTS celers_workflow_edges (
    id CHAR(36) PRIMARY KEY,
    workflow_id CHAR(36) NOT NULL,
    from_node_id CHAR(36) NOT NULL,
    to_node_id CHAR(36) NOT NULL,
    edge_type VARCHAR(50) NOT NULL DEFAULT 'dependency',
    CONSTRAINT fk_workflow_edges_workflow FOREIGN KEY (workflow_id) REFERENCES celers_workflows(id) ON DELETE CASCADE,
    CONSTRAINT fk_workflow_edges_from FOREIGN KEY (from_node_id) REFERENCES celers_workflow_nodes(id) ON DELETE CASCADE,
    CONSTRAINT fk_workflow_edges_to FOREIGN KEY (to_node_id) REFERENCES celers_workflow_nodes(id) ON DELETE CASCADE,
    UNIQUE KEY uk_workflow_edge (workflow_id, from_node_id, to_node_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- Indexes for workflow queries
CREATE INDEX idx_workflows_state ON celers_workflows(state);
CREATE INDEX idx_workflows_name ON celers_workflows(name);
CREATE INDEX idx_workflow_nodes_workflow ON celers_workflow_nodes(workflow_id);
CREATE INDEX idx_workflow_nodes_state ON celers_workflow_nodes(state);
CREATE INDEX idx_workflow_edges_workflow ON celers_workflow_edges(workflow_id);
CREATE INDEX idx_workflow_edges_from ON celers_workflow_edges(from_node_id);
CREATE INDEX idx_workflow_edges_to ON celers_workflow_edges(to_node_id);
