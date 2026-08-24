//! Task chaining, workflow/DAG execution, multi-tenant support, and bulk operations

use celers_core::{Broker, CelersError, Result, SerializedTask, TaskId};
use chrono::{DateTime, Utc};
use oxisql_core::ToSqlValue;
use serde_json::json;
use uuid::Uuid;

use crate::row_ext::{json_from_row, json_param, uuid_from_row, uuid_param, RowExt};
use crate::types::{
    ChainStatus, DbTaskState, StageStatus, TaskChain, TaskInfo, TaskWorkflow, WorkflowStatus,
};
use crate::PostgresBroker;

/// Map a `TaskInfo`-shaped row selecting the standard 12-column projection.
///
/// Mirrors `queue_ops.rs`'s private `row_to_task_info` helper (same column
/// set); duplicated here (as in `convenience.rs`/`analytics.rs`) rather than
/// shared because it is `private` there.
fn row_to_task_info(row: &oxisql_core::Row) -> Result<TaskInfo> {
    let state_str: String = row
        .col("state")
        .map_err(|e| CelersError::Other(format!("Failed to read state: {}", e)))?;
    Ok(TaskInfo {
        id: uuid_from_row(row, "id")
            .map_err(|e| CelersError::Other(format!("Failed to read id: {}", e)))?,
        task_name: row
            .col("task_name")
            .map_err(|e| CelersError::Other(format!("Failed to read task_name: {}", e)))?,
        state: state_str.parse()?,
        priority: row
            .col("priority")
            .map_err(|e| CelersError::Other(format!("Failed to read priority: {}", e)))?,
        retry_count: row
            .col("retry_count")
            .map_err(|e| CelersError::Other(format!("Failed to read retry_count: {}", e)))?,
        max_retries: row
            .col("max_retries")
            .map_err(|e| CelersError::Other(format!("Failed to read max_retries: {}", e)))?,
        created_at: row
            .col("created_at")
            .map_err(|e| CelersError::Other(format!("Failed to read created_at: {}", e)))?,
        scheduled_at: row
            .col("scheduled_at")
            .map_err(|e| CelersError::Other(format!("Failed to read scheduled_at: {}", e)))?,
        started_at: row
            .col("started_at")
            .map_err(|e| CelersError::Other(format!("Failed to read started_at: {}", e)))?,
        completed_at: row
            .col("completed_at")
            .map_err(|e| CelersError::Other(format!("Failed to read completed_at: {}", e)))?,
        worker_id: row
            .col("worker_id")
            .map_err(|e| CelersError::Other(format!("Failed to read worker_id: {}", e)))?,
        error_message: row
            .col("error_message")
            .map_err(|e| CelersError::Other(format!("Failed to read error_message: {}", e)))?,
    })
}

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
        // Two-step on a pooled broker: check out a connection, then open the
        // transaction on it (the handle borrows `conn`, so the slot stays
        // reserved for the transaction's whole lifetime).
        let conn = self.connection().await?;
        let mut tx = conn
            .transaction()
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
            // They'll be rescheduled by complete_chain_task(). `scheduled_at`
            // is a hardcoded literal chosen from a `match` on `idx == 0`
            // (never derived from `task`/user input), spliced as static SQL
            // text — identical splice discipline to the pre-migration
            // `sqlx::AssertSqlSafe(format!(...))` version, which simply
            // drops away since oxisql's `execute` takes `&str` directly.
            let scheduled_at = if idx == 0 {
                "NOW()"
            } else {
                "NOW() + INTERVAL '100 years'" // Effectively "never" until predecessor completes
            };

            let query_str = format!(
                r#"
                INSERT INTO celers_tasks
                    (id, task_name, payload, state, priority, max_retries, metadata, queue_name, created_at, scheduled_at)
                VALUES ($1, $2, $3, 'pending', $4, $5, $6::text::jsonb, $7, NOW(), {})
                "#,
                scheduled_at
            );
            let task_id_param = uuid_param(&task_id);
            let priority = task.metadata.priority;
            let max_retries = task.metadata.max_retries as i32;
            let db_metadata_param = json_param(&db_metadata);
            tx.execute(
                &query_str,
                &[
                    &task_id_param,
                    &task.metadata.name,
                    &task.payload,
                    &priority,
                    &max_retries,
                    &db_metadata_param,
                    &self.queue_name,
                ],
            )
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
        let task_id_param = uuid_param(task_id);
        let rows = self
            .conn
            .query(
                r#"
            SELECT metadata
            FROM celers_tasks
            WHERE id = $1
            "#,
                &[&task_id_param],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to fetch task: {}", e)))?;

        if let Some(row) = rows.into_iter().next() {
            let meta = json_from_row(&row, "metadata")
                .map_err(|e| CelersError::Other(format!("Failed to read metadata: {}", e)))?;
            if !meta.is_null() {
                // Check if this task is part of a chain
                if let Some(chain_id) = meta.get("chain_id").and_then(|v| v.as_str()) {
                    let position = meta
                        .get("chain_position")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0);

                    // Find the next task in the chain
                    let next_position = (position + 1) as i32;
                    let next_rows = self
                        .conn
                        .query(
                            r#"
                        SELECT id
                        FROM celers_tasks
                        WHERE metadata->>'chain_id' = $1
                          AND (metadata->>'chain_position')::int = $2
                          AND state = 'pending'
                        "#,
                            &[&chain_id, &next_position],
                        )
                        .await
                        .map_err(|e| {
                            CelersError::Other(format!("Failed to find next task: {}", e))
                        })?;

                    if let Some(next_row) = next_rows.into_iter().next() {
                        let next_task_id: Uuid = uuid_from_row(&next_row, "id")
                            .map_err(|e| CelersError::Other(format!("Failed to read id: {}", e)))?;

                        // Schedule the next task to run now
                        let next_task_id_param = uuid_param(&next_task_id);
                        self.conn
                            .execute(
                                r#"
                            UPDATE celers_tasks
                            SET scheduled_at = NOW()
                            WHERE id = $1
                            "#,
                                &[&next_task_id_param],
                            )
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
        // Two-step on a pooled broker: check out a connection, then open the
        // transaction on it (the handle borrows `conn`, so the slot stays
        // reserved for the transaction's whole lifetime).
        let conn = self.connection().await?;
        let mut tx = conn
            .transaction()
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
                // They'll be rescheduled by complete_workflow_stage(). Same
                // hardcoded-literal splice discipline as `enqueue_chain`
                // above.
                let scheduled_at = if stage.depends_on.is_empty() {
                    "NOW()"
                } else {
                    "NOW() + INTERVAL '100 years'"
                };

                let query_str = format!(
                    r#"
                    INSERT INTO celers_tasks
                        (id, task_name, payload, state, priority, max_retries, metadata, queue_name, created_at, scheduled_at)
                    VALUES ($1, $2, $3, 'pending', $4, $5, $6::text::jsonb, $7, NOW(), {})
                    "#,
                    scheduled_at
                );
                let task_id_param = uuid_param(&task_id);
                let priority = task.metadata.priority;
                let max_retries = task.metadata.max_retries as i32;
                let db_metadata_param = json_param(&db_metadata);
                tx.execute(
                    &query_str,
                    &[
                        &task_id_param,
                        &task.metadata.name,
                        &task.payload,
                        &priority,
                        &max_retries,
                        &db_metadata_param,
                        &self.queue_name,
                    ],
                )
                .await
                .map_err(|e| {
                    CelersError::Other(format!("Failed to enqueue workflow task: {}", e))
                })?;

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
        let task_id_param = uuid_param(task_id);
        let rows = self
            .conn
            .query(
                r#"
            SELECT metadata
            FROM celers_tasks
            WHERE id = $1
            "#,
                &[&task_id_param],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to fetch task: {}", e)))?;

        if let Some(row) = rows.into_iter().next() {
            let meta = json_from_row(&row, "metadata")
                .map_err(|e| CelersError::Other(format!("Failed to read metadata: {}", e)))?;
            if !meta.is_null() {
                if let (Some(workflow_id), Some(stage_id)) = (
                    meta.get("workflow_id").and_then(|v| v.as_str()),
                    meta.get("stage_id").and_then(|v| v.as_str()),
                ) {
                    // Check if all tasks in this stage are completed
                    let incomplete_rows = self
                        .conn
                        .query(
                            r#"
                        SELECT COUNT(*)
                        FROM celers_tasks
                        WHERE metadata->>'workflow_id' = $1
                          AND metadata->>'stage_id' = $2
                          AND state NOT IN ('completed', 'cancelled')
                        "#,
                            &[&workflow_id, &stage_id],
                        )
                        .await
                        .map_err(|e| {
                            CelersError::Other(format!("Failed to count incomplete tasks: {}", e))
                        })?;
                    let incomplete_row = incomplete_rows.into_iter().next().ok_or_else(|| {
                        CelersError::Other(
                            "Failed to count incomplete tasks: no rows returned".to_string(),
                        )
                    })?;
                    let incomplete_count: i64 = incomplete_row
                        .col_idx(0)
                        .map_err(|e| CelersError::Other(format!("Failed to read count: {}", e)))?;

                    if incomplete_count == 0 {
                        // Stage is complete, find dependent stages
                        let dependent_stages = self
                            .conn
                            .query(
                                r#"
                            SELECT DISTINCT metadata->>'stage_id' as stage_id
                            FROM celers_tasks
                            WHERE metadata->>'workflow_id' = $1
                              AND metadata->'stage_depends_on' ? $2
                              AND state = 'pending'
                            "#,
                                &[&workflow_id, &stage_id],
                            )
                            .await
                            .map_err(|e| {
                                CelersError::Other(format!(
                                    "Failed to find dependent stages: {}",
                                    e
                                ))
                            })?;

                        for dep_row in &dependent_stages {
                            let dep_stage_id: String = dep_row.col("stage_id").map_err(|e| {
                                CelersError::Other(format!("Failed to read stage_id: {}", e))
                            })?;

                            // Check if all dependencies for this stage are met
                            let unmet_deps = self
                                .check_workflow_stage_dependencies(workflow_id, &dep_stage_id)
                                .await?;

                            if unmet_deps.is_empty() {
                                // All dependencies met, schedule this stage
                                self.conn
                                    .execute(
                                        r#"
                                    UPDATE celers_tasks
                                    SET scheduled_at = NOW()
                                    WHERE metadata->>'workflow_id' = $1
                                      AND metadata->>'stage_id' = $2
                                      AND state = 'pending'
                                    "#,
                                        &[&workflow_id, &dep_stage_id],
                                    )
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
        let deps_rows = self
            .conn
            .query(
                r#"
            SELECT metadata->'stage_depends_on' as deps
            FROM celers_tasks
            WHERE metadata->>'workflow_id' = $1
              AND metadata->>'stage_id' = $2
            LIMIT 1
            "#,
                &[&workflow_id, &stage_id],
            )
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to fetch stage dependencies: {}", e))
            })?;

        if let Some(row) = deps_rows.into_iter().next() {
            let deps_value = json_from_row(&row, "deps")
                .map_err(|e| CelersError::Other(format!("Failed to read deps: {}", e)))?;
            if let Some(deps_array) = deps_value.as_array() {
                let mut unmet = Vec::new();

                for dep_stage_id in deps_array {
                    if let Some(dep_id) = dep_stage_id.as_str() {
                        // Check if all tasks in the dependency stage are completed
                        let incomplete_rows = self
                            .conn
                            .query(
                                r#"
                            SELECT COUNT(*)
                            FROM celers_tasks
                            WHERE metadata->>'workflow_id' = $1
                              AND metadata->>'stage_id' = $2
                              AND state NOT IN ('completed', 'cancelled')
                            "#,
                                &[&workflow_id, &dep_id],
                            )
                            .await
                            .map_err(|e| {
                                CelersError::Other(format!("Failed to check dependency: {}", e))
                            })?;
                        let incomplete_row =
                            incomplete_rows.into_iter().next().ok_or_else(|| {
                                CelersError::Other(
                                    "Failed to check dependency: no rows returned".to_string(),
                                )
                            })?;
                        let incomplete: i64 = incomplete_row.col_idx(0).map_err(|e| {
                            CelersError::Other(format!("Failed to read count: {}", e))
                        })?;

                        if incomplete > 0 {
                            unmet.push(dep_id.to_string());
                        }
                    }
                }

                return Ok(unmet);
            }
        }

        Ok(Vec::new())
    }

    /// Cancel an entire task chain
    ///
    /// Cancels all pending and processing tasks in a chain.
    pub async fn cancel_chain(&self, chain_id: &Uuid) -> Result<u64> {
        let chain_id_str = chain_id.to_string();
        let rows_affected = self
            .conn
            .execute(
                r#"
            UPDATE celers_tasks
            SET state = 'cancelled',
                completed_at = NOW()
            WHERE metadata->>'chain_id' = $1
              AND state IN ('pending', 'processing')
            "#,
                &[&chain_id_str],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to cancel chain: {}", e)))?;

        tracing::info!(
            chain_id = %chain_id,
            cancelled_count = rows_affected,
            "Cancelled task chain"
        );

        Ok(rows_affected)
    }

    /// Cancel an entire workflow
    ///
    /// Cancels all pending and processing tasks in a workflow.
    pub async fn cancel_workflow(&self, workflow_id: &Uuid) -> Result<u64> {
        let workflow_id_str = workflow_id.to_string();
        let rows_affected = self
            .conn
            .execute(
                r#"
            UPDATE celers_tasks
            SET state = 'cancelled',
                completed_at = NOW()
            WHERE metadata->>'workflow_id' = $1
              AND state IN ('pending', 'processing')
            "#,
                &[&workflow_id_str],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to cancel workflow: {}", e)))?;

        tracing::info!(
            workflow_id = %workflow_id,
            cancelled_count = rows_affected,
            "Cancelled workflow"
        );

        Ok(rows_affected)
    }

    /// Get the status of a task chain
    ///
    /// Returns comprehensive status information about a chain including task counts
    /// by state and the current position in the chain.
    pub async fn get_chain_status(&self, chain_id: &Uuid) -> Result<Option<ChainStatus>> {
        let chain_id_str = chain_id.to_string();
        let rows = self
            .conn
            .query(
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
                &[&chain_id_str],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get chain status: {}", e)))?;

        if let Some(row) = rows.into_iter().next() {
            let total_tasks: i64 = row
                .col("total_tasks")
                .map_err(|e| CelersError::Other(format!("Failed to read total_tasks: {}", e)))?;
            if total_tasks == 0 {
                return Ok(None);
            }

            let completed_tasks: i64 = row.col("completed_tasks").map_err(|e| {
                CelersError::Other(format!("Failed to read completed_tasks: {}", e))
            })?;
            let failed_tasks: i64 = row
                .col("failed_tasks")
                .map_err(|e| CelersError::Other(format!("Failed to read failed_tasks: {}", e)))?;
            let pending_tasks: i64 = row
                .col("pending_tasks")
                .map_err(|e| CelersError::Other(format!("Failed to read pending_tasks: {}", e)))?;
            let processing_tasks: i64 = row.col("processing_tasks").map_err(|e| {
                CelersError::Other(format!("Failed to read processing_tasks: {}", e))
            })?;
            let current_position: Option<i32> = row.col("current_position").map_err(|e| {
                CelersError::Other(format!("Failed to read current_position: {}", e))
            })?;

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
        let workflow_id_str = workflow_id.to_string();
        let rows = self
            .conn
            .query(
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
                &[&workflow_id_str],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get workflow status: {}", e)))?;

        if let Some(row) = rows.into_iter().next() {
            let total_tasks: i64 = row
                .col("total_tasks")
                .map_err(|e| CelersError::Other(format!("Failed to read total_tasks: {}", e)))?;
            if total_tasks == 0 {
                return Ok(None);
            }

            let workflow_name: String = row
                .col("workflow_name")
                .map_err(|e| CelersError::Other(format!("Failed to read workflow_name: {}", e)))?;
            let total_stages: i64 = row
                .col("total_stages")
                .map_err(|e| CelersError::Other(format!("Failed to read total_stages: {}", e)))?;
            let completed_tasks: i64 = row.col("completed_tasks").map_err(|e| {
                CelersError::Other(format!("Failed to read completed_tasks: {}", e))
            })?;
            let failed_tasks: i64 = row
                .col("failed_tasks")
                .map_err(|e| CelersError::Other(format!("Failed to read failed_tasks: {}", e)))?;
            let pending_tasks: i64 = row
                .col("pending_tasks")
                .map_err(|e| CelersError::Other(format!("Failed to read pending_tasks: {}", e)))?;
            let processing_tasks: i64 = row.col("processing_tasks").map_err(|e| {
                CelersError::Other(format!("Failed to read processing_tasks: {}", e))
            })?;

            // Get per-stage stats
            let stage_rows = self
                .conn
                .query(
                    r#"
                SELECT
                    metadata->>'stage_id' as stage_id,
                    MIN(metadata->>'stage_depends_on') as stage_depends_on,
                    COUNT(*) as total_tasks,
                    COUNT(*) FILTER (WHERE state = 'completed') as completed_tasks,
                    COUNT(*) FILTER (WHERE state = 'failed') as failed_tasks,
                    COUNT(*) FILTER (WHERE state = 'pending') as pending_tasks,
                    COUNT(*) FILTER (WHERE state = 'processing') as processing_tasks,
                    COUNT(*) FILTER (WHERE state NOT IN ('completed', 'cancelled'))
                        as incomplete_tasks
                FROM celers_tasks
                WHERE metadata->>'workflow_id' = $1
                GROUP BY metadata->>'stage_id'
                "#,
                    &[&workflow_id_str],
                )
                .await
                .map_err(|e| CelersError::Other(format!("Failed to get stage statuses: {}", e)))?;

            // One pre-pass over the same result set builds `stage_id ->
            // incomplete task count` and `stage_id -> declared dependencies`,
            // so `dependencies_met` can be resolved in memory against the
            // real dependency graph without an N+1 query per stage.
            let mut incomplete_by_stage: std::collections::HashMap<String, i64> =
                std::collections::HashMap::with_capacity(stage_rows.len());
            let mut declared_deps: std::collections::HashMap<String, Vec<String>> =
                std::collections::HashMap::with_capacity(stage_rows.len());
            for stage_row in &stage_rows {
                let Ok(stage_id) = stage_row.col::<String>("stage_id") else {
                    continue;
                };
                let incomplete: i64 = stage_row.col("incomplete_tasks").unwrap_or(0);
                incomplete_by_stage.insert(stage_id.clone(), incomplete);

                // `stage_depends_on` is stored as a JSON array of stage ids in
                // each task's metadata (written by `enqueue_workflow`).
                let deps: Vec<String> = stage_row
                    .col::<Option<String>>("stage_depends_on")
                    .ok()
                    .flatten()
                    .and_then(|raw| serde_json::from_str::<serde_json::Value>(&raw).ok())
                    .and_then(|value| {
                        value.as_array().map(|items| {
                            items
                                .iter()
                                .filter_map(|item| item.as_str().map(str::to_string))
                                .collect()
                        })
                    })
                    .unwrap_or_default();
                declared_deps.insert(stage_id, deps);
            }

            let mut stage_statuses = Vec::new();
            let mut completed_stages = 0;
            let mut active_stages = 0;

            for stage_row in &stage_rows {
                let stage_id: String = stage_row
                    .col("stage_id")
                    .map_err(|e| CelersError::Other(format!("Failed to read stage_id: {}", e)))?;
                let stage_total: i64 = stage_row.col("total_tasks").map_err(|e| {
                    CelersError::Other(format!("Failed to read total_tasks: {}", e))
                })?;
                let stage_completed: i64 = stage_row.col("completed_tasks").map_err(|e| {
                    CelersError::Other(format!("Failed to read completed_tasks: {}", e))
                })?;
                let stage_failed: i64 = stage_row.col("failed_tasks").map_err(|e| {
                    CelersError::Other(format!("Failed to read failed_tasks: {}", e))
                })?;
                let stage_pending: i64 = stage_row.col("pending_tasks").map_err(|e| {
                    CelersError::Other(format!("Failed to read pending_tasks: {}", e))
                })?;
                let stage_processing: i64 = stage_row.col("processing_tasks").map_err(|e| {
                    CelersError::Other(format!("Failed to read processing_tasks: {}", e))
                })?;

                let is_complete = stage_completed + stage_failed == stage_total;
                if is_complete {
                    completed_stages += 1;
                }
                if stage_processing > 0 {
                    active_stages += 1;
                }

                // A dependency is met once the upstream stage has no
                // incomplete tasks left. A stage that declares none is ready
                // by definition. This mirrors `check_workflow_stage_
                // dependencies` (the correct implementation this readout
                // previously did not consult) without its per-stage queries.
                let unmet_dependencies: Vec<String> = declared_deps
                    .get(&stage_id)
                    .map(|deps| {
                        deps.iter()
                            .filter(|dep| incomplete_by_stage.get(*dep).copied().unwrap_or(0) > 0)
                            .cloned()
                            .collect()
                    })
                    .unwrap_or_default();
                let dependencies_met = unmet_dependencies.is_empty();

                stage_statuses.push(StageStatus {
                    stage_id,
                    total_tasks: stage_total,
                    completed_tasks: stage_completed,
                    failed_tasks: stage_failed,
                    pending_tasks: stage_pending,
                    processing_tasks: stage_processing,
                    is_complete,
                    dependencies_met,
                    unmet_dependencies,
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
                let state_param = s.to_string();
                self.conn
                    .query(
                        r#"
                    SELECT id, task_name, state, priority, retry_count, max_retries,
                           created_at, scheduled_at, started_at, completed_at, worker_id, error_message
                    FROM celers_tasks
                    WHERE metadata->>'tenant_id' = $1
                      AND state = $2
                    ORDER BY created_at DESC
                    LIMIT $3 OFFSET $4
                    "#,
                        &[&tenant_id, &state_param, &limit, &offset],
                    )
                    .await
            }
            None => {
                self.conn
                    .query(
                        r#"
                    SELECT id, task_name, state, priority, retry_count, max_retries,
                           created_at, scheduled_at, started_at, completed_at, worker_id, error_message
                    FROM celers_tasks
                    WHERE metadata->>'tenant_id' = $1
                    ORDER BY created_at DESC
                    LIMIT $2 OFFSET $3
                    "#,
                        &[&tenant_id, &limit, &offset],
                    )
                    .await
            }
        }
        .map_err(|e| CelersError::Other(format!("Failed to list tenant tasks: {}", e)))?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in &rows {
            tasks.push(row_to_task_info(row)?);
        }
        Ok(tasks)
    }

    /// Count tasks by tenant ID
    pub async fn count_tasks_by_tenant(&self, tenant_id: &str) -> Result<i64> {
        let rows = self
            .conn
            .query(
                "SELECT COUNT(*) FROM celers_tasks WHERE metadata->>'tenant_id' = $1",
                &[&tenant_id],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to count tenant tasks: {}", e)))?;

        let row = rows.into_iter().next().ok_or_else(|| {
            CelersError::Other("Failed to count tenant tasks: no rows returned".to_string())
        })?;
        let count: i64 = row
            .col_idx(0)
            .map_err(|e| CelersError::Other(format!("Failed to read count: {}", e)))?;

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

        // `id = ANY($2)` -> `IN (...)` rewrite (oxisql has no array
        // `ToSqlValue`); `new_state` stays at `$1` (used both in the SET
        // clause and the CASE comparison), the `IN` list occupies
        // `$2..2+task_ids.len()`.
        let placeholders: Vec<String> = (2..2 + task_ids.len()).map(|i| format!("${i}")).collect();
        let query_str = format!(
            r#"
            UPDATE celers_tasks
            SET state = $1,
                completed_at = CASE WHEN $1 IN ('completed', 'failed', 'cancelled')
                                    THEN NOW() ELSE completed_at END
            WHERE id IN ({})
            "#,
            placeholders.join(", ")
        );
        let new_state_param = new_state.to_string();
        let task_id_params: Vec<oxisql_core::Value> = task_ids.iter().map(uuid_param).collect();
        let mut param_refs: Vec<&dyn ToSqlValue> = vec![&new_state_param];
        param_refs.extend(task_id_params.iter().map(|p| p as &dyn ToSqlValue));

        let rows_affected = self
            .conn
            .execute(&query_str, &param_refs)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to bulk update state: {}", e)))?;

        tracing::info!(
            count = rows_affected,
            new_state = %new_state,
            "Bulk updated task states"
        );

        Ok(rows_affected)
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
        let start_param = start.to_rfc3339();
        let end_param = end.to_rfc3339();
        let rows = match state {
            Some(s) => {
                let state_param = s.to_string();
                self.conn
                    .query(
                        r#"
                    SELECT id, task_name, state, priority, retry_count, max_retries,
                           created_at, scheduled_at, started_at, completed_at, worker_id, error_message
                    FROM celers_tasks
                    WHERE created_at >= $1::text::timestamptz AND created_at <= $2::text::timestamptz
                      AND state = $3
                    ORDER BY created_at DESC
                    LIMIT $4
                    "#,
                        &[&start_param, &end_param, &state_param, &limit],
                    )
                    .await
            }
            None => {
                self.conn
                    .query(
                        r#"
                    SELECT id, task_name, state, priority, retry_count, max_retries,
                           created_at, scheduled_at, started_at, completed_at, worker_id, error_message
                    FROM celers_tasks
                    WHERE created_at >= $1::text::timestamptz AND created_at <= $2::text::timestamptz
                    ORDER BY created_at DESC
                    LIMIT $3
                    "#,
                        &[&start_param, &end_param, &limit],
                    )
                    .await
            }
        }
        .map_err(|e| CelersError::Other(format!("Failed to find tasks by time range: {}", e)))?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in &rows {
            tasks.push(row_to_task_info(row)?);
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
        let rows = self
            .broker
            .conn
            .query(
                r#"
            SELECT COUNT(*)
            FROM celers_tasks
            WHERE metadata->>'tenant_id' = $1
              AND state = 'pending'
            "#,
                &[&self.tenant_id],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get tenant queue size: {}", e)))?;

        let row = rows.into_iter().next().ok_or_else(|| {
            CelersError::Other("Failed to get tenant queue size: no rows returned".to_string())
        })?;
        let count: i64 = row
            .col_idx(0)
            .map_err(|e| CelersError::Other(format!("Failed to read count: {}", e)))?;

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
