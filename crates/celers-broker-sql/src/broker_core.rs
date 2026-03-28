//! Core MysqlBroker struct and primary implementation
//!
//! Contains the broker struct definition, constructors, migration,
//! and core enqueue/dequeue/ack/reject operations.

use crate::circuit_breaker::{CircuitBreakerConfig, CircuitBreakerStateInternal};
use crate::types::*;
use crate::workflow::TaskHooks;
use celers_core::{BrokerMessage, CelersError, Result, SerializedTask, TaskId};
use chrono::Utc;
use serde_json::json;
use sqlx::{mysql::MySqlPoolOptions, MySqlPool, Row};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;
use uuid::Uuid;

#[cfg(feature = "metrics")]
use celers_metrics::{TASKS_ENQUEUED_BY_TYPE, TASKS_ENQUEUED_TOTAL};

/// MySQL-based broker implementation using SKIP LOCKED
pub struct MysqlBroker {
    pub(crate) pool: MySqlPool,
    pub(crate) queue_name: String,
    pub(crate) paused: AtomicBool,
    pub(crate) circuit_breaker: Arc<RwLock<CircuitBreakerStateInternal>>,
    pub(crate) hooks: Arc<tokio::sync::RwLock<TaskHooks>>,
}

impl MysqlBroker {
    /// Create a new MySQL broker
    ///
    /// # Arguments
    /// * `database_url` - MySQL connection string (e.g., "mysql://user:pass@localhost/db")
    /// * `queue_name` - Logical queue name for multi-tenancy (optional, defaults to "default")
    pub async fn new(database_url: &str) -> Result<Self> {
        Self::with_queue(database_url, "default").await
    }

    /// Create a new MySQL broker with a specific queue name
    pub async fn with_queue(database_url: &str, queue_name: &str) -> Result<Self> {
        Self::with_config(database_url, queue_name, PoolConfig::default()).await
    }

    /// Create a new MySQL broker with custom connection pool configuration
    pub async fn with_config(
        database_url: &str,
        queue_name: &str,
        config: PoolConfig,
    ) -> Result<Self> {
        let mut pool_options = MySqlPoolOptions::new()
            .max_connections(config.max_connections)
            .min_connections(config.min_connections)
            .acquire_timeout(Duration::from_secs(config.acquire_timeout_secs));

        if let Some(max_lifetime) = config.max_lifetime_secs {
            pool_options = pool_options.max_lifetime(Duration::from_secs(max_lifetime));
        }

        if let Some(idle_timeout) = config.idle_timeout_secs {
            pool_options = pool_options.idle_timeout(Duration::from_secs(idle_timeout));
        }

        let pool = pool_options
            .connect(database_url)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to connect to database: {}", e)))?;

        Ok(Self {
            pool,
            queue_name: queue_name.to_string(),
            paused: AtomicBool::new(false),
            circuit_breaker: Arc::new(RwLock::new(CircuitBreakerStateInternal::new(
                CircuitBreakerConfig::default(),
            ))),
            hooks: Arc::new(tokio::sync::RwLock::new(TaskHooks::new())),
        })
    }

    /// Create a new MySQL broker with custom circuit breaker configuration
    pub async fn with_circuit_breaker_config(
        database_url: &str,
        queue_name: &str,
        pool_config: PoolConfig,
        circuit_breaker_config: CircuitBreakerConfig,
    ) -> Result<Self> {
        let mut pool_options = MySqlPoolOptions::new()
            .max_connections(pool_config.max_connections)
            .min_connections(pool_config.min_connections)
            .acquire_timeout(Duration::from_secs(pool_config.acquire_timeout_secs));

        if let Some(max_lifetime) = pool_config.max_lifetime_secs {
            pool_options = pool_options.max_lifetime(Duration::from_secs(max_lifetime));
        }

        if let Some(idle_timeout) = pool_config.idle_timeout_secs {
            pool_options = pool_options.idle_timeout(Duration::from_secs(idle_timeout));
        }

        let pool = pool_options
            .connect(database_url)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to connect to database: {}", e)))?;

        Ok(Self {
            pool,
            queue_name: queue_name.to_string(),
            paused: AtomicBool::new(false),
            circuit_breaker: Arc::new(RwLock::new(CircuitBreakerStateInternal::new(
                circuit_breaker_config,
            ))),
            hooks: Arc::new(tokio::sync::RwLock::new(TaskHooks::new())),
        })
    }

    /// Run database migrations
    pub async fn migrate(&self) -> Result<()> {
        // First, create migrations table if it doesn't exist
        self.run_migration_untracked(include_str!("../migrations/000_migrations.sql"))
            .await?;

        // Run migrations with tracking
        self.run_migration_tracked(
            "001",
            "initial_schema",
            include_str!("../migrations/001_init.sql"),
        )
        .await?;

        self.run_migration_tracked(
            "002",
            "results_table",
            include_str!("../migrations/002_results.sql"),
        )
        .await?;

        self.run_migration_tracked(
            "003",
            "performance_indexes",
            include_str!("../migrations/003_performance_indexes.sql"),
        )
        .await?;

        self.run_migration_tracked(
            "006",
            "idempotency_keys",
            include_str!("../migrations/006_idempotency.sql"),
        )
        .await?;

        self.run_migration_tracked(
            "007",
            "workflow_dag",
            include_str!("../migrations/007_workflow.sql"),
        )
        .await?;

        self.run_migration_tracked(
            "008",
            "production_features",
            include_str!("../migrations/008_production_features.sql"),
        )
        .await?;

        Ok(())
    }

    /// Check if a migration has been applied
    async fn is_migration_applied(&self, version: &str) -> Result<bool> {
        let count: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM celers_migrations WHERE version = ?")
                .bind(version)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| {
                    CelersError::Other(format!("Failed to check migration status: {}", e))
                })?;

        Ok(count > 0)
    }

    /// Mark a migration as applied
    async fn mark_migration_applied(&self, version: &str, name: &str) -> Result<()> {
        sqlx::query("INSERT INTO celers_migrations (version, name) VALUES (?, ?)")
            .bind(version)
            .bind(name)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to mark migration as applied: {}", e))
            })?;

        tracing::info!(version = %version, name = %name, "Migration applied");
        Ok(())
    }

    /// Run a migration with tracking
    async fn run_migration_tracked(
        &self,
        version: &str,
        name: &str,
        migration_sql: &str,
    ) -> Result<()> {
        // Check if already applied
        if self.is_migration_applied(version).await? {
            tracing::debug!(version = %version, name = %name, "Migration already applied, skipping");
            return Ok(());
        }

        // Run the migration
        self.run_migration_untracked(migration_sql).await?;

        // Mark as applied
        self.mark_migration_applied(version, name).await?;

        Ok(())
    }

    /// Run a migration without tracking (for the migrations table itself)
    async fn run_migration_untracked(&self, migration_sql: &str) -> Result<()> {
        self.run_migration(migration_sql).await
    }

    /// Run a single migration file
    async fn run_migration(&self, migration_sql: &str) -> Result<()> {
        // MySQL doesn't support multi-statement execution by default in sqlx
        // We need to split and execute each statement separately
        let statements: Vec<&str> = migration_sql.split("DELIMITER //").collect();

        // Execute the main DDL statements (before DELIMITER)
        if let Some(main_sql) = statements.first() {
            for statement in main_sql.split(';') {
                let trimmed = statement.trim();
                if !trimmed.is_empty() && !trimmed.starts_with("--") {
                    sqlx::query(trimmed)
                        .execute(&self.pool)
                        .await
                        .map_err(|e| CelersError::Other(format!("Migration failed: {}", e)))?;
                }
            }
        }

        // Execute the stored procedure (between DELIMITER // and DELIMITER ;)
        if statements.len() > 1 {
            let proc_section = statements[1];
            if let Some(proc_sql) = proc_section.split("DELIMITER ;").next() {
                let trimmed = proc_sql.trim();
                if !trimmed.is_empty() {
                    sqlx::query(trimmed)
                        .execute(&self.pool)
                        .await
                        .map_err(|e| {
                            CelersError::Other(format!("Stored procedure creation failed: {}", e))
                        })?;
                }
            }
        }

        Ok(())
    }

    /// Get the underlying connection pool
    pub fn pool(&self) -> &MySqlPool {
        &self.pool
    }

    /// Move a task to the Dead Letter Queue
    pub(crate) async fn move_to_dlq(&self, task_id: &TaskId) -> Result<()> {
        sqlx::query("CALL move_to_dlq(?)")
            .bind(task_id.to_string())
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to move task to DLQ: {}", e)))?;

        Ok(())
    }

    // ========== Queue Control ==========

    /// Pause the queue (dequeue will return None while paused)
    pub fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
        tracing::info!(queue = %self.queue_name, "Queue paused");
    }

    /// Resume the queue
    pub fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
        tracing::info!(queue = %self.queue_name, "Queue resumed");
    }

    /// Check if the queue is paused
    pub fn is_paused(&self) -> bool {
        self.paused.load(Ordering::SeqCst)
    }

    // ========== Task Inspection ==========

    /// Get detailed information about a specific task
    pub async fn get_task(&self, task_id: &TaskId) -> Result<Option<TaskInfo>> {
        let row = sqlx::query(
            r#"
            SELECT id, task_name, state, priority, retry_count, max_retries,
                   created_at, scheduled_at, started_at, completed_at, worker_id, error_message
            FROM celers_tasks
            WHERE id = ?
            "#,
        )
        .bind(task_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get task: {}", e)))?;

        match row {
            Some(row) => {
                let task_id_str: String = row.get("id");
                let state_str: String = row.get("state");
                Ok(Some(TaskInfo {
                    id: Uuid::parse_str(&task_id_str)
                        .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?,
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
                }))
            }
            None => Ok(None),
        }
    }

    /// List tasks by state with pagination
    pub async fn list_tasks(
        &self,
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
                    WHERE state = ?
                    ORDER BY created_at DESC
                    LIMIT ? OFFSET ?
                    "#,
                )
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
                    ORDER BY created_at DESC
                    LIMIT ? OFFSET ?
                    "#,
                )
                .bind(limit)
                .bind(offset)
                .fetch_all(&self.pool)
                .await
            }
        }
        .map_err(|e| CelersError::Other(format!("Failed to list tasks: {}", e)))?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            let task_id_str: String = row.get("id");
            let state_str: String = row.get("state");
            tasks.push(TaskInfo {
                id: Uuid::parse_str(&task_id_str)
                    .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?,
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

    /// Get queue statistics
    pub async fn get_statistics(&self) -> Result<QueueStatistics> {
        // MySQL doesn't have FILTER clause, so we use CASE WHEN
        let row = sqlx::query(
            r#"
            SELECT
                SUM(CASE WHEN state = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN state = 'processing' THEN 1 ELSE 0 END) as processing,
                SUM(CASE WHEN state = 'completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END) as failed,
                SUM(CASE WHEN state = 'cancelled' THEN 1 ELSE 0 END) as cancelled,
                COUNT(*) as total
            FROM celers_tasks
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get statistics: {}", e)))?;

        let dlq_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM celers_dead_letter_queue")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get DLQ count: {}", e)))?;

        // MySQL returns DECIMAL for SUM, need to handle potential NULL
        let pending: Option<rust_decimal::Decimal> = row.get("pending");
        let processing: Option<rust_decimal::Decimal> = row.get("processing");
        let completed: Option<rust_decimal::Decimal> = row.get("completed");
        let failed: Option<rust_decimal::Decimal> = row.get("failed");
        let cancelled: Option<rust_decimal::Decimal> = row.get("cancelled");
        let total: i64 = row.get("total");

        Ok(QueueStatistics {
            pending: pending
                .map(|d| d.to_string().parse().unwrap_or(0))
                .unwrap_or(0),
            processing: processing
                .map(|d| d.to_string().parse().unwrap_or(0))
                .unwrap_or(0),
            completed: completed
                .map(|d| d.to_string().parse().unwrap_or(0))
                .unwrap_or(0),
            failed: failed
                .map(|d| d.to_string().parse().unwrap_or(0))
                .unwrap_or(0),
            cancelled: cancelled
                .map(|d| d.to_string().parse().unwrap_or(0))
                .unwrap_or(0),
            dlq: dlq_count,
            total,
        })
    }

    // ========== DLQ Operations ==========

    /// List tasks in the dead letter queue
    pub async fn list_dlq(&self, limit: i64, offset: i64) -> Result<Vec<DlqTaskInfo>> {
        let rows = sqlx::query(
            r#"
            SELECT id, task_id, task_name, retry_count, error_message, failed_at
            FROM celers_dead_letter_queue
            ORDER BY failed_at DESC
            LIMIT ? OFFSET ?
            "#,
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to list DLQ: {}", e)))?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            let id_str: String = row.get("id");
            let task_id_str: String = row.get("task_id");
            tasks.push(DlqTaskInfo {
                id: Uuid::parse_str(&id_str)
                    .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?,
                task_id: Uuid::parse_str(&task_id_str)
                    .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?,
                task_name: row.get("task_name"),
                retry_count: row.get("retry_count"),
                error_message: row.get("error_message"),
                failed_at: row.get("failed_at"),
            });
        }
        Ok(tasks)
    }

    /// Requeue a task from the dead letter queue
    ///
    /// This moves the task back to the main queue with reset retry count.
    pub async fn requeue_from_dlq(&self, dlq_id: &Uuid) -> Result<TaskId> {
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        // Get task from DLQ
        let row = sqlx::query(
            r#"
            SELECT task_id, task_name, payload, metadata
            FROM celers_dead_letter_queue
            WHERE id = ?
            "#,
        )
        .bind(dlq_id.to_string())
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to fetch DLQ task: {}", e)))?;

        let row = row.ok_or_else(|| CelersError::Other("DLQ task not found".to_string()))?;

        let task_id_str: String = row.get("task_id");
        let task_id = Uuid::parse_str(&task_id_str)
            .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?;
        let task_name: String = row.get("task_name");
        let payload: Vec<u8> = row.get("payload");
        let metadata: Option<String> = row.get("metadata");

        // Create new task in main queue
        let new_task_id = Uuid::new_v4();
        sqlx::query(
            r#"
            INSERT INTO celers_tasks
                (id, task_name, payload, state, priority, retry_count, max_retries, metadata, created_at, scheduled_at)
            VALUES (?, ?, ?, 'pending', 0, 0, 3, ?, NOW(), NOW())
            "#,
        )
        .bind(new_task_id.to_string())
        .bind(&task_name)
        .bind(&payload)
        .bind(metadata)
        .execute(&mut *tx)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to requeue task: {}", e)))?;

        // Delete from DLQ
        sqlx::query("DELETE FROM celers_dead_letter_queue WHERE id = ?")
            .bind(dlq_id.to_string())
            .execute(&mut *tx)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to delete from DLQ: {}", e)))?;

        tx.commit()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to commit requeue: {}", e)))?;

        tracing::info!(original_task_id = %task_id, new_task_id = %new_task_id, task_name = %task_name, "Requeued task from DLQ");

        Ok(new_task_id)
    }

    /// Purge (delete) a task from the dead letter queue
    pub async fn purge_dlq(&self, dlq_id: &Uuid) -> Result<bool> {
        let result = sqlx::query("DELETE FROM celers_dead_letter_queue WHERE id = ?")
            .bind(dlq_id.to_string())
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to purge DLQ task: {}", e)))?;

        Ok(result.rows_affected() > 0)
    }

    /// Purge all tasks from the dead letter queue
    pub async fn purge_all_dlq(&self) -> Result<u64> {
        let result = sqlx::query("DELETE FROM celers_dead_letter_queue")
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to purge all DLQ: {}", e)))?;

        tracing::info!(count = result.rows_affected(), "Purged all DLQ tasks");
        Ok(result.rows_affected())
    }

    // ========== Health & Maintenance ==========

    /// Check database health
    pub async fn check_health(&self) -> Result<HealthStatus> {
        // Test connection and get MySQL version
        let version: String = sqlx::query_scalar("SELECT VERSION()")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Health check failed: {}", e)))?;

        // Get queue counts
        let stats = self.get_statistics().await?;

        Ok(HealthStatus {
            healthy: true,
            connection_pool_size: self.pool.options().get_max_connections(),
            idle_connections: self.pool.num_idle() as u32,
            pending_tasks: stats.pending,
            processing_tasks: stats.processing,
            dlq_tasks: stats.dlq,
            database_version: version,
        })
    }

    /// Archive completed tasks older than the specified duration
    ///
    /// Returns the number of tasks archived (deleted).
    pub async fn archive_completed_tasks(&self, older_than: Duration) -> Result<u64> {
        let cutoff = Utc::now() - chrono::Duration::seconds(older_than.as_secs() as i64);
        let cutoff_str = cutoff.format("%Y-%m-%d %H:%M:%S").to_string();

        let result = sqlx::query(
            r#"
            DELETE FROM celers_tasks
            WHERE state IN ('completed', 'failed', 'cancelled')
              AND completed_at < ?
            "#,
        )
        .bind(cutoff_str)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to archive tasks: {}", e)))?;

        tracing::info!(count = result.rows_affected(), cutoff = %cutoff, "Archived completed tasks");
        Ok(result.rows_affected())
    }

    /// Clean up stuck processing tasks (tasks that have been processing too long)
    ///
    /// This can happen if a worker crashes. Tasks are requeued with incremented retry count.
    pub async fn recover_stuck_tasks(&self, stuck_threshold: Duration) -> Result<u64> {
        let cutoff = Utc::now() - chrono::Duration::seconds(stuck_threshold.as_secs() as i64);
        let cutoff_str = cutoff.format("%Y-%m-%d %H:%M:%S").to_string();

        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET state = 'pending',
                started_at = NULL,
                worker_id = NULL,
                error_message = 'Recovered from stuck processing state'
            WHERE state = 'processing'
              AND started_at < ?
            "#,
        )
        .bind(cutoff_str)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to recover stuck tasks: {}", e)))?;

        if result.rows_affected() > 0 {
            tracing::warn!(
                count = result.rows_affected(),
                "Recovered stuck processing tasks"
            );
        }
        Ok(result.rows_affected())
    }

    /// Purge all tasks (dangerous - use with caution)
    pub async fn purge_all(&self) -> Result<u64> {
        let result = sqlx::query("DELETE FROM celers_tasks")
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to purge all tasks: {}", e)))?;

        tracing::warn!(count = result.rows_affected(), "Purged all tasks");
        Ok(result.rows_affected())
    }

    // ========== Task Result Storage ==========

    /// Store a task result in the database
    ///
    /// This creates or updates the result for a given task ID.
    #[allow(clippy::too_many_arguments)]
    pub async fn store_result(
        &self,
        task_id: &TaskId,
        task_name: &str,
        status: TaskResultStatus,
        result: Option<serde_json::Value>,
        error: Option<&str>,
        traceback: Option<&str>,
        runtime_ms: Option<i64>,
    ) -> Result<()> {
        let completed_at = match status {
            TaskResultStatus::Success | TaskResultStatus::Failure | TaskResultStatus::Revoked => {
                Some(Utc::now().format("%Y-%m-%d %H:%M:%S").to_string())
            }
            _ => None,
        };

        // MySQL uses INSERT ... ON DUPLICATE KEY UPDATE instead of ON CONFLICT
        sqlx::query(
            r#"
            INSERT INTO celers_task_results
                (task_id, task_name, status, result, error, traceback, created_at, completed_at, runtime_ms)
            VALUES (?, ?, ?, ?, ?, ?, NOW(), ?, ?)
            ON DUPLICATE KEY UPDATE
                status = VALUES(status),
                result = VALUES(result),
                error = VALUES(error),
                traceback = VALUES(traceback),
                completed_at = VALUES(completed_at),
                runtime_ms = VALUES(runtime_ms)
            "#,
        )
        .bind(task_id.to_string())
        .bind(task_name)
        .bind(status.to_string())
        .bind(result.map(|v| serde_json::to_string(&v).unwrap_or_else(|_| "null".to_string())))
        .bind(error)
        .bind(traceback)
        .bind(completed_at)
        .bind(runtime_ms)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to store result: {}", e)))?;

        Ok(())
    }

    /// Get a task result from the database
    pub async fn get_result(&self, task_id: &TaskId) -> Result<Option<TaskResult>> {
        let row = sqlx::query(
            r#"
            SELECT task_id, task_name, status, result, error, traceback,
                   created_at, completed_at, runtime_ms
            FROM celers_task_results
            WHERE task_id = ?
            "#,
        )
        .bind(task_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get result: {}", e)))?;

        match row {
            Some(row) => {
                let task_id_str: String = row.get("task_id");
                let status_str: String = row.get("status");
                let result_str: Option<String> = row.get("result");
                Ok(Some(TaskResult {
                    task_id: Uuid::parse_str(&task_id_str)
                        .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?,
                    task_name: row.get("task_name"),
                    status: status_str.parse()?,
                    result: result_str.and_then(|s| serde_json::from_str(&s).ok()),
                    error: row.get("error"),
                    traceback: row.get("traceback"),
                    created_at: row.get("created_at"),
                    completed_at: row.get("completed_at"),
                    runtime_ms: row.get("runtime_ms"),
                }))
            }
            None => Ok(None),
        }
    }

    /// Delete a task result from the database
    pub async fn delete_result(&self, task_id: &TaskId) -> Result<bool> {
        let result = sqlx::query("DELETE FROM celers_task_results WHERE task_id = ?")
            .bind(task_id.to_string())
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to delete result: {}", e)))?;

        Ok(result.rows_affected() > 0)
    }

    /// Archive old task results
    ///
    /// Deletes results older than the specified duration.
    pub async fn archive_results(&self, older_than: Duration) -> Result<u64> {
        let cutoff = Utc::now() - chrono::Duration::seconds(older_than.as_secs() as i64);
        let cutoff_str = cutoff.format("%Y-%m-%d %H:%M:%S").to_string();

        let result = sqlx::query(
            r#"
            DELETE FROM celers_task_results
            WHERE completed_at < ?
            "#,
        )
        .bind(cutoff_str)
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to archive results: {}", e)))?;

        tracing::info!(count = result.rows_affected(), cutoff = %cutoff, "Archived old results");
        Ok(result.rows_affected())
    }

    // ========== Database Monitoring ==========

    /// Get table size information for CeleRS tables
    pub async fn get_table_sizes(&self) -> Result<Vec<TableSizeInfo>> {
        let rows = sqlx::query(
            r#"
            SELECT
                TABLE_NAME as table_name,
                TABLE_ROWS as row_count,
                DATA_LENGTH as data_size_bytes,
                INDEX_LENGTH as index_size_bytes
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME LIKE 'celers_%'
            ORDER BY DATA_LENGTH DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get table sizes: {}", e)))?;

        let mut tables = Vec::with_capacity(rows.len());
        for row in rows {
            let row_count: Option<i64> = row.get("row_count");
            let data_size: Option<i64> = row.get("data_size_bytes");
            let index_size: Option<i64> = row.get("index_size_bytes");
            tables.push(TableSizeInfo {
                table_name: row.get("table_name"),
                row_count: row_count.unwrap_or(0),
                data_size_bytes: data_size.unwrap_or(0),
                index_size_bytes: index_size.unwrap_or(0),
            });
        }
        Ok(tables)
    }

    /// Optimize CeleRS tables (MySQL-specific)
    ///
    /// This should be run periodically for optimal performance.
    pub async fn optimize_tables(&self) -> Result<()> {
        sqlx::query("OPTIMIZE TABLE celers_tasks")
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to optimize celers_tasks: {}", e)))?;

        sqlx::query("OPTIMIZE TABLE celers_dead_letter_queue")
            .execute(&self.pool)
            .await
            .map_err(|e| {
                CelersError::Other(format!(
                    "Failed to optimize celers_dead_letter_queue: {}",
                    e
                ))
            })?;

        sqlx::query("OPTIMIZE TABLE celers_task_results")
            .execute(&self.pool)
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to optimize celers_task_results: {}", e))
            })?;

        tracing::info!("Optimized all CeleRS tables");
        Ok(())
    }

    /// Analyze CeleRS tables for query optimization
    pub async fn analyze_tables(&self) -> Result<()> {
        sqlx::query("ANALYZE TABLE celers_tasks")
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to analyze celers_tasks: {}", e)))?;

        sqlx::query("ANALYZE TABLE celers_dead_letter_queue")
            .execute(&self.pool)
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to analyze celers_dead_letter_queue: {}", e))
            })?;

        sqlx::query("ANALYZE TABLE celers_task_results")
            .execute(&self.pool)
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to analyze celers_task_results: {}", e))
            })?;

        tracing::info!("Analyzed all CeleRS tables");
        Ok(())
    }

    // ========== Advanced Task Inspection ==========

    /// Get task counts grouped by task name
    ///
    /// Returns statistics for each unique task name including counts by state.
    pub async fn count_by_task_name(&self) -> Result<Vec<TaskNameCount>> {
        let rows = sqlx::query(
            r#"
            SELECT
                task_name,
                SUM(CASE WHEN state = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN state = 'processing' THEN 1 ELSE 0 END) as processing,
                SUM(CASE WHEN state = 'completed' THEN 1 ELSE 0 END) as completed,
                SUM(CASE WHEN state = 'failed' THEN 1 ELSE 0 END) as failed,
                COUNT(*) as total
            FROM celers_tasks
            GROUP BY task_name
            ORDER BY total DESC
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to count by task name: {}", e)))?;

        let mut counts = Vec::with_capacity(rows.len());
        for row in rows {
            let pending: Option<rust_decimal::Decimal> = row.get("pending");
            let processing: Option<rust_decimal::Decimal> = row.get("processing");
            let completed: Option<rust_decimal::Decimal> = row.get("completed");
            let failed: Option<rust_decimal::Decimal> = row.get("failed");
            let total: i64 = row.get("total");

            counts.push(TaskNameCount {
                task_name: row.get("task_name"),
                pending: pending
                    .map(|d| d.to_string().parse().unwrap_or(0))
                    .unwrap_or(0),
                processing: processing
                    .map(|d| d.to_string().parse().unwrap_or(0))
                    .unwrap_or(0),
                completed: completed
                    .map(|d| d.to_string().parse().unwrap_or(0))
                    .unwrap_or(0),
                failed: failed
                    .map(|d| d.to_string().parse().unwrap_or(0))
                    .unwrap_or(0),
                total,
            });
        }
        Ok(counts)
    }

    /// Get all currently processing tasks
    ///
    /// Useful for monitoring worker activity and detecting stuck tasks.
    pub async fn get_processing_tasks(&self, limit: i64, offset: i64) -> Result<Vec<TaskInfo>> {
        self.list_tasks(Some(DbTaskState::Processing), limit, offset)
            .await
    }

    /// Get tasks currently being processed by a specific worker
    pub async fn get_tasks_by_worker(&self, worker_id: &str) -> Result<Vec<TaskInfo>> {
        let rows = sqlx::query(
            r#"
            SELECT id, task_name, state, priority, retry_count, max_retries,
                   created_at, scheduled_at, started_at, completed_at, worker_id, error_message
            FROM celers_tasks
            WHERE worker_id = ?
            ORDER BY started_at DESC
            "#,
        )
        .bind(worker_id)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get tasks by worker: {}", e)))?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            let task_id_str: String = row.get("id");
            let state_str: String = row.get("state");
            tasks.push(TaskInfo {
                id: Uuid::parse_str(&task_id_str)
                    .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?,
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

    /// List scheduled tasks (tasks with scheduled_at in the future)
    pub async fn list_scheduled_tasks(
        &self,
        limit: i64,
        offset: i64,
    ) -> Result<Vec<ScheduledTaskInfo>> {
        let rows = sqlx::query(
            r#"
            SELECT id, task_name, priority, scheduled_at, created_at,
                   TIMESTAMPDIFF(SECOND, NOW(), scheduled_at) as delay_remaining_secs
            FROM celers_tasks
            WHERE state = 'pending'
              AND scheduled_at > NOW()
            ORDER BY scheduled_at ASC
            LIMIT ? OFFSET ?
            "#,
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to list scheduled tasks: {}", e)))?;

        let mut tasks = Vec::with_capacity(rows.len());
        for row in rows {
            let task_id_str: String = row.get("id");
            let delay: Option<i64> = row.get("delay_remaining_secs");
            tasks.push(ScheduledTaskInfo {
                id: Uuid::parse_str(&task_id_str)
                    .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?,
                task_name: row.get("task_name"),
                priority: row.get("priority"),
                scheduled_at: row.get("scheduled_at"),
                created_at: row.get("created_at"),
                delay_remaining_secs: delay.unwrap_or(0),
            });
        }
        Ok(tasks)
    }

    /// Count scheduled tasks (tasks with scheduled_at in the future)
    pub async fn count_scheduled_tasks(&self) -> Result<i64> {
        let count: i64 = sqlx::query_scalar(
            r#"
            SELECT COUNT(*) FROM celers_tasks
            WHERE state = 'pending' AND scheduled_at > NOW()
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to count scheduled tasks: {}", e)))?;

        Ok(count)
    }

    // ========== Task Updates ==========

    /// Update the error message on a task
    ///
    /// Useful for recording error details during task execution.
    pub async fn update_error_message(
        &self,
        task_id: &TaskId,
        error_message: &str,
    ) -> Result<bool> {
        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET error_message = ?
            WHERE id = ?
            "#,
        )
        .bind(error_message)
        .bind(task_id.to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to update error message: {}", e)))?;

        Ok(result.rows_affected() > 0)
    }

    /// Set the worker ID on a processing task
    ///
    /// This allows tracking which worker is processing which task.
    pub async fn set_worker_id(&self, task_id: &TaskId, worker_id: &str) -> Result<bool> {
        let result = sqlx::query(
            r#"
            UPDATE celers_tasks
            SET worker_id = ?
            WHERE id = ? AND state = 'processing'
            "#,
        )
        .bind(worker_id)
        .bind(task_id.to_string())
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to set worker ID: {}", e)))?;

        Ok(result.rows_affected() > 0)
    }

    /// Dequeue a task and set the worker ID atomically
    ///
    /// This is a convenience method that dequeues a task and sets the worker ID
    /// in a single transaction, which is useful for worker tracking.
    pub async fn dequeue_with_worker_id(&self, worker_id: &str) -> Result<Option<BrokerMessage>> {
        // Check if queue is paused
        if self.paused.load(Ordering::SeqCst) {
            return Ok(None);
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        let row = sqlx::query(
            r#"
            SELECT id, task_name, payload, retry_count
            FROM celers_tasks
            WHERE state = 'pending'
              AND scheduled_at <= NOW()
            ORDER BY priority DESC, created_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
            "#,
        )
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to dequeue task: {}", e)))?;

        if let Some(row) = row {
            let task_id_str: String = row.get("id");
            let _task_id = Uuid::parse_str(&task_id_str)
                .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?;
            let task_name: String = row.get("task_name");
            let payload: Vec<u8> = row.get("payload");
            let retry_count: i32 = row.get("retry_count");

            // Mark as processing with worker ID
            sqlx::query(
                r#"
                UPDATE celers_tasks
                SET state = 'processing',
                    started_at = NOW(),
                    retry_count = retry_count + 1,
                    worker_id = ?
                WHERE id = ?
                "#,
            )
            .bind(worker_id)
            .bind(&task_id_str)
            .execute(&mut *tx)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to mark task as processing: {}", e)))?;

            tx.commit()
                .await
                .map_err(|e| CelersError::Other(format!("Failed to commit transaction: {}", e)))?;

            Ok(Some(BrokerMessage {
                task: SerializedTask::new(task_name, payload),
                receipt_handle: Some(retry_count.to_string()),
            }))
        } else {
            tx.rollback().await.map_err(|e| {
                CelersError::Other(format!("Failed to rollback transaction: {}", e))
            })?;
            Ok(None)
        }
    }

    // ========== Selective Cleanup ==========

    /// Purge tasks by state
    ///
    /// Deletes all tasks with the specified state. Use with caution.
    pub async fn purge_by_state(&self, state: DbTaskState) -> Result<u64> {
        let result = sqlx::query("DELETE FROM celers_tasks WHERE state = ?")
            .bind(state.to_string())
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to purge tasks by state: {}", e)))?;

        tracing::info!(state = %state, count = result.rows_affected(), "Purged tasks by state");
        Ok(result.rows_affected())
    }

    /// Purge completed tasks only
    pub async fn purge_completed(&self) -> Result<u64> {
        self.purge_by_state(DbTaskState::Completed).await
    }

    /// Purge failed tasks only
    pub async fn purge_failed(&self) -> Result<u64> {
        self.purge_by_state(DbTaskState::Failed).await
    }

    /// Purge cancelled tasks only
    pub async fn purge_cancelled(&self) -> Result<u64> {
        self.purge_by_state(DbTaskState::Cancelled).await
    }

    /// Purge tasks by task name
    ///
    /// Deletes all tasks with the specified task name. Use with caution.
    pub async fn purge_by_task_name(&self, task_name: &str) -> Result<u64> {
        let result = sqlx::query("DELETE FROM celers_tasks WHERE task_name = ?")
            .bind(task_name)
            .execute(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to purge tasks by name: {}", e)))?;

        tracing::info!(task_name = %task_name, count = result.rows_affected(), "Purged tasks by name");
        Ok(result.rows_affected())
    }

    // ========== Migration Management ==========

    /// List all applied migrations
    pub async fn list_migrations(&self) -> Result<Vec<MigrationInfo>> {
        let rows = sqlx::query(
            r#"
            SELECT version, name, applied_at
            FROM celers_migrations
            ORDER BY applied_at ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to list migrations: {}", e)))?;

        let mut migrations = Vec::with_capacity(rows.len());
        for row in rows {
            migrations.push(MigrationInfo {
                version: row.get("version"),
                name: row.get("name"),
                applied_at: row.get("applied_at"),
            });
        }
        Ok(migrations)
    }

    // ========== Query Performance Tracking ==========

    /// Get query performance statistics from MySQL performance schema
    ///
    /// Note: This requires performance_schema to be enabled in MySQL configuration.
    pub async fn get_query_stats(&self) -> Result<Vec<QueryStats>> {
        let rows = sqlx::query(
            r#"
            SELECT
                DIGEST_TEXT as query_name,
                COUNT_STAR as execution_count,
                SUM_TIMER_WAIT / 1000000000 as total_time_ms,
                AVG_TIMER_WAIT / 1000000000 as avg_time_ms,
                MIN_TIMER_WAIT / 1000000000 as min_time_ms,
                MAX_TIMER_WAIT / 1000000000 as max_time_ms
            FROM performance_schema.events_statements_summary_by_digest
            WHERE SCHEMA_NAME = DATABASE()
              AND DIGEST_TEXT LIKE '%celers%'
            ORDER BY SUM_TIMER_WAIT DESC
            LIMIT 50
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get query stats: {}", e)))?;

        let mut stats = Vec::with_capacity(rows.len());
        for row in rows {
            let query_name: String = row.get("query_name");
            let execution_count: i64 = row.get("execution_count");
            let total_time: Option<rust_decimal::Decimal> = row.get("total_time_ms");
            let avg_time: Option<rust_decimal::Decimal> = row.get("avg_time_ms");
            let min_time: Option<rust_decimal::Decimal> = row.get("min_time_ms");
            let max_time: Option<rust_decimal::Decimal> = row.get("max_time_ms");

            stats.push(QueryStats {
                query_name,
                execution_count,
                total_time_ms: total_time
                    .map(|d| d.to_string().parse().unwrap_or(0))
                    .unwrap_or(0),
                avg_time_ms: avg_time
                    .map(|d| d.to_string().parse().unwrap_or(0.0))
                    .unwrap_or(0.0),
                min_time_ms: min_time
                    .map(|d| d.to_string().parse().unwrap_or(0))
                    .unwrap_or(0),
                max_time_ms: max_time
                    .map(|d| d.to_string().parse().unwrap_or(0))
                    .unwrap_or(0),
            });
        }
        Ok(stats)
    }

    /// Reset query performance statistics
    ///
    /// Clears the performance_schema statistics. Useful for benchmarking.
    pub async fn reset_query_stats(&self) -> Result<()> {
        sqlx::query(
            r#"
            CALL sys.ps_truncate_all_tables(FALSE)
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to reset query stats: {}", e)))?;

        tracing::info!("Reset query performance statistics");
        Ok(())
    }

    // ========== Index Usage and Query Optimization ==========

    /// Get index statistics for CeleRS tables
    ///
    /// Returns information about all indexes on CeleRS tables including cardinality
    /// and whether they are unique.
    pub async fn get_index_stats(&self) -> Result<Vec<IndexStats>> {
        let rows = sqlx::query(
            r#"
            SELECT
                TABLE_NAME as table_name,
                INDEX_NAME as index_name,
                CARDINALITY as cardinality,
                NON_UNIQUE as non_unique
            FROM information_schema.STATISTICS
            WHERE TABLE_SCHEMA = DATABASE()
              AND TABLE_NAME LIKE 'celers_%'
            ORDER BY TABLE_NAME, INDEX_NAME
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get index stats: {}", e)))?;

        let mut stats = Vec::with_capacity(rows.len());
        for row in rows {
            let cardinality: Option<i64> = row.get("cardinality");
            let non_unique: i32 = row.get("non_unique");
            stats.push(IndexStats {
                table_name: row.get("table_name"),
                index_name: row.get("index_name"),
                cardinality: cardinality.unwrap_or(0),
                unique_values: non_unique == 0,
            });
        }
        Ok(stats)
    }

    /// Explain a query plan for the dequeue operation
    ///
    /// Returns the MySQL EXPLAIN output for the dequeue query.
    /// Useful for query optimization and performance tuning.
    pub async fn explain_dequeue(&self) -> Result<Vec<QueryPlan>> {
        let rows = sqlx::query(
            r#"
            EXPLAIN
            SELECT id, task_name, payload, retry_count
            FROM celers_tasks
            WHERE state = 'pending'
              AND scheduled_at <= NOW()
            ORDER BY priority DESC, created_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to explain query: {}", e)))?;

        let mut plans = Vec::with_capacity(rows.len());
        for row in rows {
            let rows_examined: Option<i64> = row.try_get("rows").ok();
            let filtered: Option<rust_decimal::Decimal> = row.try_get("filtered").ok();
            plans.push(QueryPlan {
                id: row.get("id"),
                select_type: row.get("select_type"),
                table: row.try_get("table").ok(),
                query_type: row.try_get("type").ok(),
                possible_keys: row.try_get("possible_keys").ok(),
                key_used: row.try_get("key").ok(),
                key_length: row.try_get("key_len").ok(),
                rows_examined,
                filtered: filtered.map(|d| d.to_string().parse().unwrap_or(0.0)),
                extra: row.try_get("Extra").ok(),
            });
        }
        Ok(plans)
    }

    /// Explain a custom query plan
    ///
    /// Returns the MySQL EXPLAIN output for any SELECT query.
    pub async fn explain_query(&self, query: &str) -> Result<Vec<QueryPlan>> {
        let explain_query = format!("EXPLAIN {}", query);
        let rows = sqlx::query(&explain_query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to explain query: {}", e)))?;

        let mut plans = Vec::with_capacity(rows.len());
        for row in rows {
            let rows_examined: Option<i64> = row.try_get("rows").ok();
            let filtered: Option<rust_decimal::Decimal> = row.try_get("filtered").ok();
            plans.push(QueryPlan {
                id: row.get("id"),
                select_type: row.get("select_type"),
                table: row.try_get("table").ok(),
                query_type: row.try_get("type").ok(),
                possible_keys: row.try_get("possible_keys").ok(),
                key_used: row.try_get("key").ok(),
                key_length: row.try_get("key_len").ok(),
                rows_examined,
                filtered: filtered.map(|d| d.to_string().parse().unwrap_or(0.0)),
                extra: row.try_get("Extra").ok(),
            });
        }
        Ok(plans)
    }

    /// Check if indexes are being used effectively
    ///
    /// Analyzes the explain plan for common queries and returns warnings
    /// if indexes are not being used properly.
    pub async fn check_index_usage(&self) -> Result<Vec<String>> {
        let mut warnings = Vec::new();

        // Check dequeue query
        let dequeue_plan = self.explain_dequeue().await?;
        for plan in dequeue_plan {
            if plan.key_used.is_none() {
                warnings.push(format!(
                    "Dequeue query on table {:?} is not using an index (full table scan)",
                    plan.table
                ));
            }
            if let Some(extra) = &plan.extra {
                if extra.contains("Using filesort") {
                    warnings.push("Dequeue query requires filesort - consider adding composite index on (state, priority, created_at)".to_string());
                }
            }
        }

        // Check index cardinality
        let index_stats = self.get_index_stats().await?;
        for stat in index_stats {
            if stat.cardinality == 0 && !stat.index_name.eq("PRIMARY") {
                warnings.push(format!(
                    "Index {} on table {} has zero cardinality - consider running ANALYZE TABLE",
                    stat.index_name, stat.table_name
                ));
            }
        }

        Ok(warnings)
    }

    // ========== Connection Diagnostics and Performance Monitoring ==========

    /// Get connection pool diagnostics
    ///
    /// Returns detailed information about the connection pool state.
    pub fn get_connection_diagnostics(&self) -> ConnectionDiagnostics {
        let max_conns = self.pool.options().get_max_connections();
        let idle_conns = self.pool.num_idle() as u32;
        let min_conns = self.pool.options().get_min_connections();

        // Total connections is at least idle, but could be up to max
        let total_conns = idle_conns.max(min_conns);
        let active_conns = total_conns.saturating_sub(idle_conns);

        let utilization = if max_conns > 0 {
            (total_conns as f64 / max_conns as f64) * 100.0
        } else {
            0.0
        };

        ConnectionDiagnostics {
            total_connections: total_conns,
            idle_connections: idle_conns,
            active_connections: active_conns,
            max_connections: max_conns,
            connection_wait_time_ms: None, // MySQL doesn't expose this easily
            pool_utilization_percent: utilization,
        }
    }

    /// Get comprehensive performance metrics snapshot
    ///
    /// This method collects various performance metrics including queue sizes,
    /// connection pool status, and database statistics.
    pub async fn get_performance_metrics(&self) -> Result<PerformanceMetrics> {
        let stats = self.get_statistics().await?;
        let conn_diag = self.get_connection_diagnostics();

        // Calculate tasks per second (requires historical data, placeholder for now)
        // In production, this would track enqueue/dequeue rates over time
        let tasks_per_second = 0.0;

        // Get average query times from performance schema (if available)
        let (avg_dequeue_ms, avg_enqueue_ms) = match self.get_query_stats().await {
            Ok(stats) => {
                let dequeue_stat = stats
                    .iter()
                    .find(|s| {
                        s.query_name.contains("SELECT") && s.query_name.contains("celers_tasks")
                    })
                    .map(|s| s.avg_time_ms)
                    .unwrap_or(0.0);

                let enqueue_stat = stats
                    .iter()
                    .find(|s| {
                        s.query_name.contains("INSERT") && s.query_name.contains("celers_tasks")
                    })
                    .map(|s| s.avg_time_ms)
                    .unwrap_or(0.0);

                (dequeue_stat, enqueue_stat)
            }
            Err(_) => (0.0, 0.0),
        };

        Ok(PerformanceMetrics {
            timestamp: Utc::now(),
            tasks_per_second,
            avg_dequeue_time_ms: avg_dequeue_ms,
            avg_enqueue_time_ms: avg_enqueue_ms,
            queue_depth: stats.pending,
            processing_tasks: stats.processing,
            dlq_size: stats.dlq,
            connection_pool: conn_diag,
        })
    }

    /// Check if the broker is healthy and ready to process tasks
    ///
    /// Returns true if:
    /// - Database connection is active
    /// - Connection pool has idle connections available
    /// - No critical errors detected
    pub async fn is_ready(&self) -> bool {
        // Try a simple query
        let version_check = sqlx::query_scalar::<_, String>("SELECT VERSION()")
            .fetch_one(&self.pool)
            .await;

        if version_check.is_err() {
            return false;
        }

        // Check connection pool has capacity
        let idle = self.pool.num_idle();
        if idle == 0 {
            let max_conns = self.pool.options().get_max_connections();
            // If pool is at max and no idle connections, might be saturated
            if max_conns > 0 && self.pool.size() >= max_conns {
                return false;
            }
        }

        true
    }

    /// Get detailed database server variables
    ///
    /// Returns key MySQL server configuration variables that affect performance.
    pub async fn get_server_variables(&self) -> Result<std::collections::HashMap<String, String>> {
        let rows = sqlx::query(
            r#"
            SHOW VARIABLES WHERE Variable_name IN (
                'max_connections',
                'innodb_buffer_pool_size',
                'innodb_log_file_size',
                'query_cache_size',
                'query_cache_type',
                'innodb_flush_log_at_trx_commit',
                'innodb_flush_method',
                'binlog_format',
                'expire_logs_days'
            )
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get server variables: {}", e)))?;

        let mut variables = std::collections::HashMap::new();
        for row in rows {
            let var_name: String = row.get("Variable_name");
            let var_value: String = row.get("Value");
            variables.insert(var_name, var_value);
        }

        Ok(variables)
    }

    /// Enqueue multiple tasks in a single transaction (batch operation)
    ///
    /// This is significantly faster than individual enqueue calls when
    /// inserting many tasks. Uses a single transaction and prepared statement.
    ///
    /// # Returns
    /// Vector of task IDs in the same order as input tasks
    pub async fn enqueue_batch_impl(&self, tasks: Vec<SerializedTask>) -> Result<Vec<TaskId>> {
        if tasks.is_empty() {
            return Ok(Vec::new());
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        let mut task_ids = Vec::with_capacity(tasks.len());

        for task in &tasks {
            let task_id = task.metadata.id;
            let mut db_metadata = json!({
                "queue": self.queue_name,
                "enqueued_at": chrono::Utc::now().to_rfc3339(),
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

            sqlx::query(
                r#"
                INSERT INTO celers_tasks
                    (id, task_name, payload, state, priority, max_retries, metadata, created_at, scheduled_at)
                VALUES (?, ?, ?, 'pending', ?, ?, ?, NOW(), NOW())
                "#,
            )
            .bind(task_id.to_string())
            .bind(&task.metadata.name)
            .bind(&task.payload)
            .bind(task.metadata.priority)
            .bind(task.metadata.max_retries as i32)
            .bind(serde_json::to_string(&db_metadata).unwrap_or_else(|_| "{}".to_string()))
            .execute(&mut *tx)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to enqueue task in batch: {}", e)))?;

            task_ids.push(task_id);
        }

        tx.commit()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to commit batch enqueue: {}", e)))?;

        #[cfg(feature = "metrics")]
        {
            TASKS_ENQUEUED_TOTAL.inc_by(tasks.len() as f64);

            // Track per-task-type metrics
            for task in &tasks {
                TASKS_ENQUEUED_BY_TYPE
                    .with_label_values(&[&task.metadata.name])
                    .inc();
            }
        }

        Ok(task_ids)
    }

    /// Dequeue multiple tasks atomically (batch operation)
    ///
    /// Fetches up to `limit` tasks in a single transaction using
    /// FOR UPDATE SKIP LOCKED for distributed worker safety.
    ///
    /// # Arguments
    /// * `limit` - Maximum number of tasks to dequeue
    ///
    /// # Returns
    /// Vector of broker messages (may be less than limit if queue has fewer tasks)
    pub async fn dequeue_batch_impl(&self, limit: usize) -> Result<Vec<BrokerMessage>> {
        if limit == 0 || self.paused.load(Ordering::SeqCst) {
            return Ok(Vec::new());
        }

        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        let rows = sqlx::query(
            r#"
            SELECT id, task_name, payload, retry_count
            FROM celers_tasks
            WHERE state = 'pending'
              AND scheduled_at <= NOW()
            ORDER BY priority DESC, created_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT ?
            "#,
        )
        .bind(limit as i64)
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to dequeue batch: {}", e)))?;

        if rows.is_empty() {
            tx.rollback().await.map_err(|e| {
                CelersError::Other(format!("Failed to rollback transaction: {}", e))
            })?;
            return Ok(Vec::new());
        }

        let mut messages = Vec::with_capacity(rows.len());
        let mut task_id_strings = Vec::with_capacity(rows.len());

        for row in rows {
            let task_id_str: String = row.get("id");
            let _task_id = Uuid::parse_str(&task_id_str)
                .map_err(|e| CelersError::Other(format!("Invalid UUID: {}", e)))?;
            let task_name: String = row.get("task_name");
            let payload: Vec<u8> = row.get("payload");
            let retry_count: i32 = row.get("retry_count");

            messages.push(BrokerMessage {
                task: SerializedTask::new(task_name, payload),
                receipt_handle: Some(retry_count.to_string()),
            });

            task_id_strings.push(task_id_str);
        }

        // Mark all fetched tasks as processing
        // MySQL doesn't support array parameters like PostgreSQL's ANY($1)
        // So we need to use IN clause with placeholders
        if !task_id_strings.is_empty() {
            let placeholders = task_id_strings
                .iter()
                .map(|_| "?")
                .collect::<Vec<_>>()
                .join(", ");
            let update_query = format!(
                r#"
                UPDATE celers_tasks
                SET state = 'processing',
                    started_at = NOW(),
                    retry_count = retry_count + 1
                WHERE id IN ({})
                "#,
                placeholders
            );

            let mut query = sqlx::query(&update_query);
            for task_id in task_id_strings {
                query = query.bind(task_id);
            }

            query.execute(&mut *tx).await.map_err(|e| {
                CelersError::Other(format!("Failed to mark batch as processing: {}", e))
            })?;
        }

        tx.commit()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to commit batch dequeue: {}", e)))?;

        Ok(messages)
    }
}
