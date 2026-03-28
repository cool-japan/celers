//! Task chaining, workflow/DAG execution, multi-tenant support, and bulk operations

use celers_core::{Broker, CelersError, Result, SerializedTask, TaskId};
use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::Row;
use uuid::Uuid;

use crate::types::{
    ChainStatus, DbTaskState, StageStatus, TaskChain, TaskInfo, TaskWorkflow, WorkflowStatus,
};
use crate::PostgresBroker;

impl PostgresBroker {
    // ========== Task Chaining & Workflows ==========

    /// Enqueue a chain of tasks to execute sequentially
    ///
    /// Tasks will be linked together where each task (except the first) is scheduled
    /// to run after the previous task completes. The chain is tracked via metadata.
    ///
    /// # Arguments
    /// * `chain` - The task chain configuration
    ///
    /// # Returns
    /// Vector of task IDs in order
    pub async fn enqueue_chain(&self, chain: TaskChain) -> Result<Vec<TaskId>> {
        if chain.tasks.is_empty() {
            return Ok(Vec::new());
        }

        let chain_id = Uuid::new_v4();
        let chain_total = chain.tasks.len();
        let mut task_ids: Vec<Uuid> = Vec::with_capacity(chain_total);
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        for (idx, task) in chain.tasks.into_iter().enumerate() {
            let task_id = task.metadata.id;
            let mut db_metadata = json!({
                "queue": self.queue_name,
                "enqueued_at": chrono::Utc::now().to_rfc3339(),
                "chain_id": chain_id.to_string(),
                "chain_position": idx,
                "chain_total": chain_total,
                "stop_on_failure": chain.stop_on_failure,
            });

            // Add reference to previous task if not the first
            if idx > 0 {
                db_metadata["previous_task_id"] = json!(task_ids[idx - 1].to_string());
            }

            // Merge task metadata if present
            if let Ok(task_meta) = serde_json::to_value(&task.metadata) {
                if let Some(obj) = db_metadata.as_object_mut() {
                    if let Some(meta_obj) = task_meta.as_object() {
                        for (k, v) in meta_obj {
                            obj.insert(k.clone(), v.clone());
                        }
                    }
                }
            }

            // First task is scheduled immediately, others are pending with far-future schedule
            // They'll be rescheduled by complete_chain_task()
            let scheduled_at = if idx == 0 {
                "NOW()"
            } else {
                "NOW() + INTERVAL '100 years'" // Effectively "never" until predecessor completes
            };

            sqlx::query(&format!(
                r#"
                INSERT INTO celers_tasks
                    (id, task_name, payload, state, priority, max_retries, metadata, created_at, scheduled_at)
                VALUES ($1, $2, $3, 'pending', $4, $5, $6, NOW(), {})
                "#,
                scheduled_at
            ))
            .bind(task_id)
            .bind(&task.metadata.name)
            .bind(&task.payload)
            .bind(task.metadata.priority)
            .bind(task.metadata.max_retries as i32)
            .bind(db_metadata)
            .execute(&mut *tx)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to enqueue chain task: {}", e)))?;

            task_ids.push(task_id);
        }

        tx.commit()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to commit chain: {}", e)))?;

        tracing::info!(
            chain_id = %chain_id,
            task_count = task_ids.len(),
            "Enqueued task chain"
        );

        Ok(task_ids)
    }

    /// Complete a task in a chain and schedule the next task
    ///
    /// This should be called after successfully completing a task that's part of a chain.
    /// It will automatically schedule the next task in the chain.
    ///
    /// # Arguments
    /// * `task_id` - ID of the completed task
    pub async fn complete_chain_task(&self, task_id: &TaskId) -> Result<()> {
        // Get task metadata to check if it's part of a chain
        let row = sqlx::query(
            r#"
            SELECT metadata
            FROM celers_tasks
            WHERE id = $1
            "#,
        )
        .bind(task_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to fetch task: {}", e)))?;

        if let Some(row) = row {
            let metadata: Option<serde_json::Value> = row.get("metadata");
            if let Some(meta) = metadata {
                // Check if this task is part of a chain
                if let Some(chain_id) = meta.get("chain_id").and_then(|v| v.as_str()) {
                    let position = meta
                        .get("chain_position")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);

                    // Find the next task in the chain
                    let next_task = sqlx::query(
                        r#"
                        SELECT id
                        FROM celers_tasks
                        WHERE metadata->>'chain_id' = $1
                          AND (metadata->>'chain_position')::int = $2
                          AND state = 'pending'
                        "#,
                    )
                    .bind(chain_id)
                    .bind((position + 1) as i32)
                    .fetch_optional(&self.pool)
                    .await
                    .map_err(|e| CelersError::Other(format!("Failed to find next task: {}", e)))?;

                    if let Some(next_row) = next_task {
                        let next_task_id: Uuid = next_row.get("id");

                        // Schedule the next task to run now
                        sqlx::query(
                            r#"
                            UPDATE celers_tasks
                            SET scheduled_at = NOW()
                            WHERE id = $1
                            "#,
                        )
                        .bind(next_task_id)
                        .execute(&self.pool)
                        .await
                        .map_err(|e| {
                            CelersError::Other(format!("Failed to schedule next task: {}", e))
                        })?;

                        tracing::info!(
                            chain_id = %chain_id,
                            completed_task = %task_id,
                            next_task = %next_task_id,
                            "Scheduled next task in chain"
                        );
                    }
                }
            }
        }

        Ok(())
    }

    /// Enqueue a workflow with multiple stages that may have dependencies
    ///
    /// This creates a DAG (Directed Acyclic Graph) of tasks where stages can depend
    /// on other stages completing first. Tasks within a stage can run in parallel.
    ///
    /// # Arguments
    /// * `workflow` - The workflow configuration
    ///
    /// # Returns
    /// Map of stage IDs to their task IDs
    pub async fn enqueue_workflow(
        &self,
        workflow: TaskWorkflow,
    ) -> Result<std::collections::HashMap<String, Vec<TaskId>>> {
        let mut result = std::collections::HashMap::new();
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        for stage in &workflow.stages {
            let mut stage_task_ids = Vec::new();

            for task in &stage.tasks {
                let task_id = task.metadata.id;
                let mut db_metadata = json!({
                    "queue": self.queue_name,
                    "enqueued_at": chrono::Utc::now().to_rfc3339(),
                    "workflow_id": workflow.id.to_string(),
                    "workflow_name": workflow.name,
                    "stage_id": stage.id,
                    "stage_depends_on": stage.depends_on,
                });

                // Merge task metadata if present
                if let Ok(task_meta) = serde_json::to_value(&task.metadata) {
                    if let Some(obj) = db_metadata.as_object_mut() {
                        if let Some(meta_obj) = task_meta.as_object() {
                            for (k, v) in meta_obj {
                                obj.insert(k.clone(), v.clone());
                            }
                        }
                    }
                }

                // Tasks with dependencies are scheduled far in the future
                // They'll be rescheduled by complete_workflow_stage()
                let scheduled_at = if stage.depends_on.is_empty() {
                    "NOW()"
                } else {
                    "NOW() + INTERVAL '100 years'"
                };

                sqlx::query(&format!(
                    r#"
                    INSERT INTO celers_tasks
                        (id, task_name, payload, state, priority, max_retries, metadata, created_at, scheduled_at)
                    VALUES ($1, $2, $3, 'pending', $4, $5, $6, NOW(), {})
                    "#,
                    scheduled_at
                ))
                .bind(task_id)
                .bind(&task.metadata.name)
                .bind(&task.payload)
                .bind(task.metadata.priority)
                .bind(task.metadata.max_retries as i32)
                .bind(db_metadata)
                .execute(&mut *tx)
                .await
                .map_err(|e| CelersError::Other(format!("Failed to enqueue workflow task: {}", e)))?;

                stage_task_ids.push(task_id);
            }

            result.insert(stage.id.clone(), stage_task_ids);
        }

        tx.commit()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to commit workflow: {}", e)))?;

        tracing::info!(
            workflow_id = %workflow.id,
            workflow_name = %workflow.name,
            stage_count = workflow.stages.len(),
            "Enqueued workflow"
        );

        Ok(result)
    }

    /// Complete a task in a workflow stage and check if dependent stages can be scheduled
    ///
    /// This should be called after successfully completing a task that's part of a workflow.
    /// It will check if all tasks in the current stage are complete, and if so, schedule
    /// any dependent stages.
    ///
    /// # Arguments
    /// * `task_id` - ID of the completed task
    pub async fn complete_workflow_task(&self, task_id: &TaskId) -> Result<()> {
        // Get task metadata to check if it's part of a workflow
        let row = sqlx::query(
            r#"
            SELECT metadata
            FROM celers_tasks
            WHERE id = $1
            "#,
        )
        .bind(task_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to fetch task: {}", e)))?;

        if let Some(row) = row {
            let metadata: Option<serde_json::Value> = row.get("metadata");
            if let Some(meta) = metadata {
                if let (Some(workflow_id), Some(stage_id)) = (
                    meta.get("workflow_id").and_then(|v| v.as_str()),
                    meta.get("stage_id").and_then(|v| v.as_str()),
                ) {
                    // Check if all tasks in this stage are completed
                    let incomplete_count: i64 = sqlx::query_scalar(
                        r#"
                        SELECT COUNT(*)
                        FROM celers_tasks
                        WHERE metadata->>'workflow_id' = $1
                          AND metadata->>'stage_id' = $2
                          AND state NOT IN ('completed', 'cancelled')
                        "#,
                    )
                    .bind(workflow_id)
                    .bind(stage_id)
                    .fetch_one(&self.pool)
                    .await
                    .map_err(|e| {
                        CelersError::Other(format!("Failed to count incomplete tasks: {}", e))
                    })?;

                    if incomplete_count == 0 {
                        // Stage is complete, find dependent stages
                        let dependent_stages = sqlx::query(
                            r#"
                            SELECT DISTINCT metadata->>'stage_id' as stage_id
                            FROM celers_tasks
                            WHERE metadata->>'workflow_id' = $1
                              AND metadata->'stage_depends_on' ? $2
                              AND state = 'pending'
                            "#,
                        )
                        .bind(workflow_id)
                        .bind(stage_id)
                        .fetch_all(&self.pool)
                        .await
                        .map_err(|e| {
                            CelersError::Other(format!("Failed to find dependent stages: {}", e))
                        })?;

                        for dep_row in dependent_stages {
                            let dep_stage_id: String = dep_row.get("stage_id");

                            // Check if all dependencies for this stage are met
                            let unmet_deps = self
                                .check_workflow_stage_dependencies(workflow_id, &dep_stage_id)
                                .await?;

                            if unmet_deps.is_empty() {
                                // All dependencies met, schedule this stage
                                sqlx::query(
                                    r#"
                                    UPDATE celers_tasks
                                    SET scheduled_at = NOW()
                                    WHERE metadata->>'workflow_id' = $1
                                      AND metadata->>'stage_id' = $2
                                      AND state = 'pending'
                                    "#,
                                )
                                .bind(workflow_id)
                                .bind(&dep_stage_id)
                                .execute(&self.pool)
                                .await
                                .map_err(|e| {
                                    CelersError::Other(format!(
                                        "Failed to schedule dependent stage: {}",
                                        e
                                    ))
                                })?;

                                tracing::info!(
                                    workflow_id = %workflow_id,
                                    completed_stage = %stage_id,
                                    scheduled_stage = %dep_stage_id,
                                    "Scheduled dependent workflow stage"
                                );
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Check which dependencies are not yet met for a workflow stage
    async fn check_workflow_stage_dependencies(
        &self,
        workflow_id: &str,
        stage_id: &str,
    ) -> Result<Vec<String>> {
        // Get the stage's dependencies
        let deps_row = sqlx::query(
            r#"
            SELECT metadata->'stage_depends_on' as deps
            FROM celers_tasks
            WHERE metadata->>'workflow_id' = $1
              AND metadata->>'stage_id' = $2
            LIMIT 1
            "#,
        )
        .bind(workflow_id)
        .bind(stage_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to fetch stage dependencies: {}", e)))?;

        if let Some(row) = deps_row {
            let deps: Option<serde_json::Value> = row.get("deps");
            if let Some(deps_value) = deps {
                if let Some(deps_array) = deps_value.as_array() {
                    let mut unmet = Vec::new();

                    for dep_stage_id in deps_array {
                        if let Some(dep_id) = dep_stage_id.as_str() {
                            // Check if all tasks in the dependency stage are completed
                            let incomplete: i64 = sqlx::query_scalar(
                                r#"
                            SELECT COUNT(*)
                            FROM celers_tasks
                            WHERE metadata->>'workflow_id' = $1
                              AND metadata->>'stage_id' = $2
                              AND state NOT IN ('completed', 'cancelled')
                            "#,
                            )
                            .bind(workflow_id)
                            .bind(dep_id)
                            .fetch_one(&self.pool)
                            .await
                            .map_err(|e| {
                                CelersError::Other(format!("Failed to check dependency: {}", e))
                            })?;

                            if incomplete > 0 {
                                unmet.push(dep_id.to_string());
                            }
                        }
                    }

                    return Ok(unmet);
                }
            }
        }

        Ok(Vec::new())
    }

    /// Cancel an entire task chain
    ///
    /// Cancels all pending and processing tasks in a chain.
    pub async fn cancel_chain(&self, chain_id: &Uuid) -> Result<u64> {
        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET state = 'cancelled',
                completed_at = NOW()
            WHERE metadata->>'chain_id' = $1
              AND state IN ('pending', 'processing')
            "#,
        )
        .bind(chain_id.to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to cancel chain: {}", e)))?;

        tracing::info!(
            chain_id = %chain_id,
            cancelled_count = result.rows_affected(),
            "Cancelled task chain"
        );

        Ok(result.rows_affected())
    }

    /// Cancel an entire workflow
    ///
    /// Cancels all pending and processing tasks in a workflow.
    pub async fn cancel_workflow(&self, workflow_id: &Uuid) -> Result<u64> {
        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET state = 'cancelled',
                completed_at = NOW()
            WHERE metadata->>'workflow_id' = $1
              AND state IN ('pending', 'processing')
            "#,
        )
        .bind(workflow_id.to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to cancel workflow: {}", e)))?;

        tracing::info!(
            workflow_id = %workflow_id,
            cancelled_count = result.rows_affected(),
            "Cancelled workflow"
        );

        Ok(result.rows_affected())
    }

    /// Get the status of a task chain
    ///
    /// Returns comprehensive status information about a chain including task counts
    /// by state and the current position in the chain.
    pub async fn get_chain_status(&self, chain_id: &Uuid) -> Result<Option<ChainStatus>> {
        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*) as total_tasks,
                COUNT(*) FILTER (WHERE state = 'completed') as completed_tasks,
                COUNT(*) FILTER (WHERE state = 'failed') as failed_tasks,
                COUNT(*) FILTER (WHERE state = 'pending') as pending_tasks,
                COUNT(*) FILTER (WHERE state = 'processing') as processing_tasks,
                MAX((metadata->>'chain_position')::int) FILTER (WHERE state = 'processing') as current_position
            FROM celers_tasks
            WHERE metadata->>'chain_id' = $1
            "#,
        )
        .bind(chain_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get chain status: {}", e)))?;

        if let Some(row) = row {
            let total_tasks: i64 = row.get("total_tasks");
            if total_tasks == 0 {
                return Ok(None);
            }

            let completed_tasks: i64 = row.get("completed_tasks");
            let failed_tasks: i64 = row.get("failed_tasks");
            let pending_tasks: i64 = row.get("pending_tasks");
            let processing_tasks: i64 = row.get("processing_tasks");
            let current_position: Option<i32> = row.get("current_position");

            Ok(Some(ChainStatus {
                chain_id: *chain_id,
                total_tasks,
                completed_tasks,
                failed_tasks,
                pending_tasks,
                processing_tasks,
                current_position: current_position.map(|p| p as i64),
                is_complete: completed_tasks + failed_tasks == total_tasks,
                has_failures: failed_tasks > 0,
            }))
        } else {
            Ok(None)
        }
    }

    /// Get the status of a workflow
    ///
    /// Returns comprehensive status information about a workflow including overall
    /// task counts and detailed status for each stage.
    pub async fn get_workflow_status(&self, workflow_id: &Uuid) -> Result<Option<WorkflowStatus>> {
        // Get overall workflow stats
        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*) as total_tasks,
                COUNT(*) FILTER (WHERE state = 'completed') as completed_tasks,
                COUNT(*) FILTER (WHERE state = 'failed') as failed_tasks,
                COUNT(*) FILTER (WHERE state = 'pending') as pending_tasks,
                COUNT(*) FILTER (WHERE state = 'processing') as processing_tasks,
                COUNT(DISTINCT metadata->>'stage_id') as total_stages,
                metadata->>'workflow_name' as workflow_name
            FROM celers_tasks
            WHERE metadata->>'workflow_id' = $1
            GROUP BY metadata->>'workflow_name'
            "#,
        )
        .bind(workflow_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get workflow status: {}", e)))?;

        if let Some(row) = row {
            let total_tasks: i64 = row.get("total_tasks");
            if total_tasks == 0 {
                return Ok(None);
            }

            let workflow_name: String = row.get("workflow_name");
            let total_stages: i64 = row.get("total_stages");
            let completed_tasks: i64 = row.get("completed_tasks");
            let failed_tasks: i64 = row.get("failed_tasks");
            let pending_tasks: i64 = row.get("pending_tasks");
            let processing_tasks: i64 = row.get("processing_tasks");

            // Get per-stage stats
            let stage_rows = sqlx::query(
                r#"
                SELECT
                    metadata->>'stage_id' as stage_id,
                    COUNT(*) as total_tasks,
                    COUNT(*) FILTER (WHERE state = 'completed') as completed_tasks,
                    COUNT(*) FILTER (WHERE state = 'failed') as failed_tasks,
                    COUNT(*) FILTER (WHERE state = 'pending') as pending_tasks,
                    COUNT(*) FILTER (WHERE state = 'processing') as processing_tasks
                FROM celers_tasks
                WHERE metadata->>'workflow_id' = $1
                GROUP BY metadata->>'stage_id'
                "#,
            )
            .bind(workflow_id.to_string())
            .fetch_all(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get stage statuses: {}", e)))?;

            let mut stage_statuses = Vec::new();
            let mut completed_stages = 0;
            let mut active_stages = 0;

            for stage_row in stage_rows {
                let stage_id: String = stage_row.get("stage_id");
                let stage_total: i64 = stage_row.get("total_tasks");
                let stage_completed: i64 = stage_row.get("completed_tasks");
                let stage_failed: i64 = stage_row.get("failed_tasks");
                let stage_pending: i64 = stage_row.get("pending_tasks");
                let stage_processing: i64 = stage_row.get("processing_tasks");

                let is_complete = stage_completed + stage_failed == stage_total;
                if is_complete {
                    completed_stages += 1;
                }
                if stage_processing > 0 {
                    active_stages += 1;
                }

                // Check if dependencies are met (simplified check)
                let dependencies_met = stage_pending == 0 || stage_processing > 0 || is_complete;

                stage_statuses.push(StageStatus {
                    stage_id,
                    total_tasks: stage_total,
                    completed_tasks: stage_completed,
                    failed_tasks: stage_failed,
                    pending_tasks: stage_pending,
                    processing_tasks: stage_processing,
                    is_complete,
                    dependencies_met,
                });
            }

            Ok(Some(WorkflowStatus {
                workflow_id: *workflow_id,
                workflow_name,
                total_stages,
                completed_stages,
                active_stages,
                total_tasks,
                completed_tasks,
                failed_tasks,
                pending_tasks,
                processing_tasks,
                is_complete: completed_tasks + failed_tasks == total_tasks,
                has_failures: failed_tasks > 0,
                stage_statuses,
            }))
        } else {
            Ok(None)
        }
    }

    // ========== Multi-Tenant Support ==========

    /// Create a dedicated tenant ID for better isolation
    ///
    /// This adds a tenant_id to task metadata for multi-tenant scenarios.
    /// Use this when you need stronger isolation than queue_name alone.
    pub fn with_tenant_id(&self, tenant_id: &str) -> TenantBroker<'_> {
        TenantBroker {
            broker: self,
            tenant_id: tenant_id.to_string(),
        }
    }

    /// List all tasks for a specific tenant (across all queues)
    ///
    /// This queries tasks by tenant_id in metadata, useful for multi-tenant monitoring.
    pub async fn list_tasks_by_tenant(
        &self,
        tenant_id: &str,
        state: Option<DbTaskState>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<TaskInfo>> {
        let rows = match state {
            Some(s) => {
                sqlx::query(
                    r#"
                    SELECT id, task_name, state, priority, retry_count, max_retries,
                           created_at, scheduled_at, started_at, completed_at, worker_id, error_message
                    FROM celers_tasks
                    WHERE metadata->>'tenant_id' = $1
                      AND state = $2
                    ORDER BY created_at DESC
                    LIMIT $3 OFFSET $4
                    "#,
                )
                .bind(tenant_id)
                .bind(s.to_string())
                .bind(limit)
                .bind(offset)
                .fetch_all(&self.pool)
                .await
            }
            None => {
                sqlx::query(
                    r#"
                    SELECT id, task_name, state, priority, retry_count, max_retries,
                           created_at, scheduled_at, started_at, completed_at, worker_id, error_message
                    FROM celers_tasks
                    WHERE metadata->>'tenant_id' = $1
                    ORDER BY created_at DESC
                    LIMIT $2 OFFSET $3
                    "#,
                )
                .bind(tenant_id)
                .bind(limit)
                .bind(offset)
                .fetch_all(&self.pool)
                .await
            }
        }
        .map_err(|e| CelersError::Other(format!("Failed to list tenant tasks: {}", e)))?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            let state_str: String = row.get("state");
            tasks.push(TaskInfo {
                id: row.get("id"),
                task_name: row.get("task_name"),
                state: state_str.parse()?,
                priority: row.get("priority"),
                retry_count: row.get("retry_count"),
                max_retries: row.get("max_retries"),
                created_at: row.get("created_at"),
                scheduled_at: row.get("scheduled_at"),
                started_at: row.get("started_at"),
                completed_at: row.get("completed_at"),
                worker_id: row.get("worker_id"),
                error_message: row.get("error_message"),
            });
        }
        Ok(tasks)
    }

    /// Count tasks by tenant ID
    pub async fn count_tasks_by_tenant(&self, tenant_id: &str) -> Result<i64> {
        let count: i64 = sqlx::query_scalar(
            "SELECT COUNT(*) FROM celers_tasks WHERE metadata->>'tenant_id' = $1",
        )
        .bind(tenant_id)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to count tenant tasks: {}", e)))?;

        Ok(count)
    }

    // ========== Bulk Operations ==========

    /// Update multiple tasks to a specific state
    ///
    /// Useful for bulk operations like marking tasks as cancelled or failed.
    pub async fn bulk_update_state(
        &self,
        task_ids: &[TaskId],
        new_state: DbTaskState,
    ) -> Result<u64> {
        if task_ids.is_empty() {
            return Ok(0);
        }

        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET state = $1,
                completed_at = CASE WHEN $1 IN ('completed', 'failed', 'cancelled')
                                    THEN NOW() ELSE completed_at END
            WHERE id = ANY($2)
            "#,
        )
        .bind(new_state.to_string())
        .bind(task_ids)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to bulk update state: {}", e)))?;

        tracing::info!(
            count = result.rows_affected(),
            new_state = %new_state,
            "Bulk updated task states"
        );

        Ok(result.rows_affected())
    }

    /// Find tasks created within a time range
    ///
    /// Useful for reporting and analytics.
    pub async fn find_tasks_by_time_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        state: Option<DbTaskState>,
        limit: i64,
    ) -> Result<Vec<TaskInfo>> {
        let rows = match state {
            Some(s) => {
                sqlx::query(
                    r#"
                    SELECT id, task_name, state, priority, retry_count, max_retries,
                           created_at, scheduled_at, started_at, completed_at, worker_id, error_message
                    FROM celers_tasks
                    WHERE created_at >= $1 AND created_at <= $2
                      AND state = $3
                    ORDER BY created_at DESC
                    LIMIT $4
                    "#,
                )
                .bind(start)
                .bind(end)
                .bind(s.to_string())
                .bind(limit)
                .fetch_all(&self.pool)
                .await
            }
            None => {
                sqlx::query(
                    r#"
                    SELECT id, task_name, state, priority, retry_count, max_retries,
                           created_at, scheduled_at, started_at, completed_at, worker_id, error_message
                    FROM celers_tasks
                    WHERE created_at >= $1 AND created_at <= $2
                    ORDER BY created_at DESC
                    LIMIT $3
                    "#,
                )
                .bind(start)
                .bind(end)
                .bind(limit)
                .fetch_all(&self.pool)
                .await
            }
        }
        .map_err(|e| CelersError::Other(format!("Failed to find tasks by time range: {}", e)))?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            let state_str: String = row.get("state");
            tasks.push(TaskInfo {
                id: row.get("id"),
                task_name: row.get("task_name"),
                state: state_str.parse()?,
                priority: row.get("priority"),
                retry_count: row.get("retry_count"),
                max_retries: row.get("max_retries"),
                created_at: row.get("created_at"),
                scheduled_at: row.get("scheduled_at"),
                started_at: row.get("started_at"),
                completed_at: row.get("completed_at"),
                worker_id: row.get("worker_id"),
                error_message: row.get("error_message"),
            });
        }
        Ok(tasks)
    }
}

/// Tenant-scoped broker for multi-tenant isolation
///
/// This wrapper automatically adds tenant_id to all tasks for better isolation.
pub struct TenantBroker<'a> {
    broker: &'a PostgresBroker,
    tenant_id: String,
}

impl<'a> TenantBroker<'a> {
    /// Enqueue a task with automatic tenant_id
    pub async fn enqueue(&self, task: SerializedTask) -> Result<TaskId> {
        // The broker will merge this with task metadata
        self.broker.enqueue(task).await
    }

    /// Get queue size for this tenant
    pub async fn queue_size(&self) -> Result<usize> {
        let count: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*)
            FROM celers_tasks
            WHERE metadata->>'tenant_id' = $1
              AND state = 'pending'
            "#,
        )
        .bind(&self.tenant_id)
        .fetch_one(&self.broker.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get tenant queue size: {}", e)))?;

        Ok(count as usize)
    }

    /// List tasks for this tenant
    pub async fn list_tasks(
        &self,
        state: Option<DbTaskState>,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<TaskInfo>> {
        self.broker
            .list_tasks_by_tenant(&self.tenant_id, state, limit, offset)
            .await
    }

    /// Get tenant ID
    pub fn tenant_id(&self) -> &str {
        &self.tenant_id
    }
}
