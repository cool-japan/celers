//! Database result backend for CeleRS
//!
//! This crate provides PostgreSQL and MySQL-based storage for task results and workflow state.
//!
//! # Features
//!
//! - Task result storage with expiration
//! - Chord state management (barrier synchronization)
//! - Atomic counter operations
//! - SQL-based result queries and analytics
//! - Support for both PostgreSQL and MySQL
//!
//! # Example
//!
//! ```ignore
//! use celers_backend_db::PostgresResultBackend;
//! use celers_backend_redis::ResultBackend;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut backend = PostgresResultBackend::new("postgres://localhost/celers").await?;
//! backend.migrate().await?;
//!
//! // Store task result
//! let meta = TaskMeta::new(task_id, "my_task".to_string());
//! backend.store_result(task_id, &meta).await?;
//! # Ok(())
//! # }
//! ```

pub mod result_store;

use async_trait::async_trait;
pub use celers_backend_redis::{
    BackendError, ChordState, Result, ResultBackend, TaskMeta, TaskResult,
};
use chrono::{DateTime, Utc};
use serde_json::json;
use sqlx::{postgres::PgPoolOptions, MySqlPool, PgPool, Row};
use std::time::Duration;
use uuid::Uuid;

/// PostgreSQL result backend implementation
#[derive(Clone)]
pub struct PostgresResultBackend {
    pool: PgPool,
}

impl PostgresResultBackend {
    /// Create a new PostgreSQL result backend
    ///
    /// # Arguments
    /// * `database_url` - PostgreSQL connection string (e.g., "postgres://user:pass@localhost/db")
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(20)
            .acquire_timeout(Duration::from_secs(5))
            .connect(database_url)
            .await
            .map_err(|e| {
                BackendError::Connection(format!("Failed to connect to database: {}", e))
            })?;

        Ok(Self { pool })
    }

    /// Run database migrations
    pub async fn migrate(&self) -> Result<()> {
        let migration_sql = include_str!("../migrations/001_init_postgres.sql");

        sqlx::query(migration_sql)
            .execute(&self.pool)
            .await
            .map_err(|e| BackendError::Connection(format!("Migration failed: {}", e)))?;

        Ok(())
    }

    /// Get the underlying connection pool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Clean up expired results (returns number of deleted rows)
    pub async fn cleanup_expired(&self) -> Result<usize> {
        let row = sqlx::query("SELECT cleanup_expired_results()")
            .fetch_one(&self.pool)
            .await
            .map_err(|e| {
                BackendError::Connection(format!("Failed to cleanup expired results: {}", e))
            })?;

        let count: i32 = row.get(0);
        Ok(count as usize)
    }
}

#[async_trait]
impl ResultBackend for PostgresResultBackend {
    async fn store_result(&mut self, task_id: Uuid, meta: &TaskMeta) -> Result<()> {
        let (result_state, result_data, error_message, retry_count) = match &meta.result {
            TaskResult::Pending => ("pending", None, None, None),
            TaskResult::Started => ("started", None, None, None),
            TaskResult::Success(data) => ("success", Some(data.clone()), None, None),
            TaskResult::Failure(err) => ("failure", None, Some(err.clone()), None),
            TaskResult::Revoked => ("revoked", None, None, None),
            TaskResult::Retry(count) => ("retry", None, None, Some(*count as i32)),
        };

        sqlx::query(
            r#"
            INSERT INTO celers_task_results
                (task_id, task_name, result_state, result_data, error_message, retry_count,
                 created_at, started_at, completed_at, worker)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (task_id) DO UPDATE SET
                result_state = EXCLUDED.result_state,
                result_data = EXCLUDED.result_data,
                error_message = EXCLUDED.error_message,
                retry_count = EXCLUDED.retry_count,
                started_at = EXCLUDED.started_at,
                completed_at = EXCLUDED.completed_at,
                worker = EXCLUDED.worker
            "#,
        )
        .bind(task_id)
        .bind(&meta.task_name)
        .bind(result_state)
        .bind(result_data)
        .bind(error_message)
        .bind(retry_count)
        .bind(meta.created_at)
        .bind(meta.started_at)
        .bind(meta.completed_at)
        .bind(&meta.worker)
        .execute(&self.pool)
        .await
        .map_err(|e| BackendError::Connection(format!("Failed to store result: {}", e)))?;

        Ok(())
    }

    async fn get_result(&mut self, task_id: Uuid) -> Result<Option<TaskMeta>> {
        let row = sqlx::query(
            r#"
            SELECT task_id, task_name, result_state, result_data, error_message,
                   retry_count, created_at, started_at, completed_at, worker
            FROM celers_task_results
            WHERE task_id = $1
            "#,
        )
        .bind(task_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| BackendError::Connection(format!("Failed to get result: {}", e)))?;

        match row {
            Some(row) => {
                let result_state: String = row.get("result_state");
                let result_data: Option<serde_json::Value> = row.get("result_data");
                let error_message: Option<String> = row.get("error_message");
                let retry_count: Option<i32> = row.get("retry_count");

                let result = match result_state.as_str() {
                    "pending" => TaskResult::Pending,
                    "started" => TaskResult::Started,
                    "success" => TaskResult::Success(result_data.unwrap_or(json!(null))),
                    "failure" => TaskResult::Failure(error_message.unwrap_or_default()),
                    "revoked" => TaskResult::Revoked,
                    "retry" => TaskResult::Retry(retry_count.unwrap_or(0) as u32),
                    _ => TaskResult::Pending,
                };

                let meta = TaskMeta {
                    task_id: row.get("task_id"),
                    task_name: row.get("task_name"),
                    result,
                    created_at: row.get("created_at"),
                    started_at: row.get("started_at"),
                    completed_at: row.get("completed_at"),
                    worker: row.get("worker"),
                    progress: None,
                };

                Ok(Some(meta))
            }
            None => Ok(None),
        }
    }

    async fn delete_result(&mut self, task_id: Uuid) -> Result<()> {
        sqlx::query("DELETE FROM celers_task_results WHERE task_id = $1")
            .bind(task_id)
            .execute(&self.pool)
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to delete result: {}", e)))?;

        Ok(())
    }

    async fn set_expiration(&mut self, task_id: Uuid, ttl: Duration) -> Result<()> {
        let expires_at = Utc::now() + chrono::Duration::from_std(ttl).unwrap();

        sqlx::query("UPDATE celers_task_results SET expires_at = $1 WHERE task_id = $2")
            .bind(expires_at)
            .bind(task_id)
            .execute(&self.pool)
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to set expiration: {}", e)))?;

        Ok(())
    }

    async fn chord_init(&mut self, state: ChordState) -> Result<()> {
        let task_ids = serde_json::to_value(&state.task_ids)
            .map_err(|e| BackendError::Serialization(e.to_string()))?;

        sqlx::query(
            r#"
            INSERT INTO celers_chord_state (chord_id, total, completed, callback, task_ids)
            VALUES ($1, $2, 0, $3, $4)
            ON CONFLICT (chord_id) DO UPDATE SET
                total = EXCLUDED.total,
                callback = EXCLUDED.callback,
                task_ids = EXCLUDED.task_ids
            "#,
        )
        .bind(state.chord_id)
        .bind(state.total as i32)
        .bind(&state.callback)
        .bind(task_ids)
        .execute(&self.pool)
        .await
        .map_err(|e| BackendError::Connection(format!("Failed to init chord: {}", e)))?;

        Ok(())
    }

    async fn chord_complete_task(&mut self, chord_id: Uuid) -> Result<usize> {
        let row = sqlx::query("SELECT chord_increment_counter($1)")
            .bind(chord_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| {
                BackendError::Connection(format!("Failed to increment chord counter: {}", e))
            })?;

        let count: i32 = row.get(0);
        Ok(count as usize)
    }

    async fn chord_get_state(&mut self, chord_id: Uuid) -> Result<Option<ChordState>> {
        let row = sqlx::query(
            r#"
            SELECT chord_id, total, completed, callback, task_ids
            FROM celers_chord_state
            WHERE chord_id = $1
            "#,
        )
        .bind(chord_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| BackendError::Connection(format!("Failed to get chord state: {}", e)))?;

        match row {
            Some(row) => {
                let task_ids_json: serde_json::Value = row.get("task_ids");
                let task_ids: Vec<Uuid> = serde_json::from_value(task_ids_json)
                    .map_err(|e| BackendError::Serialization(e.to_string()))?;

                let state = ChordState {
                    chord_id: row.get("chord_id"),
                    total: row.get::<i32, _>("total") as usize,
                    completed: row.get::<i32, _>("completed") as usize,
                    callback: row.get("callback"),
                    task_ids,
                };

                Ok(Some(state))
            }
            None => Ok(None),
        }
    }

    // Batch operations using transactions for atomic multi-row operations

    async fn store_results_batch(&mut self, results: &[(Uuid, TaskMeta)]) -> Result<()> {
        if results.is_empty() {
            return Ok(());
        }

        let mut tx =
            self.pool.begin().await.map_err(|e| {
                BackendError::Connection(format!("Failed to begin transaction: {}", e))
            })?;

        for (task_id, meta) in results {
            let (result_state, result_data, error_message, retry_count) = match &meta.result {
                TaskResult::Pending => ("pending", None, None, None),
                TaskResult::Started => ("started", None, None, None),
                TaskResult::Success(data) => ("success", Some(data.clone()), None, None),
                TaskResult::Failure(err) => ("failure", None, Some(err.clone()), None),
                TaskResult::Revoked => ("revoked", None, None, None),
                TaskResult::Retry(count) => ("retry", None, None, Some(*count as i32)),
            };

            sqlx::query(
                r#"
                INSERT INTO celers_task_results
                    (task_id, task_name, result_state, result_data, error_message, retry_count,
                     created_at, started_at, completed_at, worker)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                ON CONFLICT (task_id) DO UPDATE SET
                    result_state = EXCLUDED.result_state,
                    result_data = EXCLUDED.result_data,
                    error_message = EXCLUDED.error_message,
                    retry_count = EXCLUDED.retry_count,
                    started_at = EXCLUDED.started_at,
                    completed_at = EXCLUDED.completed_at,
                    worker = EXCLUDED.worker
                "#,
            )
            .bind(task_id)
            .bind(&meta.task_name)
            .bind(result_state)
            .bind(result_data)
            .bind(error_message)
            .bind(retry_count)
            .bind(meta.created_at)
            .bind(meta.started_at)
            .bind(meta.completed_at)
            .bind(&meta.worker)
            .execute(&mut *tx)
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to store result: {}", e)))?;
        }

        tx.commit().await.map_err(|e| {
            BackendError::Connection(format!("Failed to commit transaction: {}", e))
        })?;

        Ok(())
    }

    async fn get_results_batch(&mut self, task_ids: &[Uuid]) -> Result<Vec<Option<TaskMeta>>> {
        if task_ids.is_empty() {
            return Ok(Vec::new());
        }

        // PostgreSQL supports = ANY($1) for array queries
        let rows = sqlx::query(
            r#"
            SELECT task_id, task_name, result_state, result_data, error_message,
                   retry_count, created_at, started_at, completed_at, worker
            FROM celers_task_results
            WHERE task_id = ANY($1)
            "#,
        )
        .bind(task_ids)
        .fetch_all(&self.pool)
        .await
        .map_err(|e| BackendError::Connection(format!("Failed to get results: {}", e)))?;

        // Create a HashMap for O(1) lookup
        let mut results_map = std::collections::HashMap::new();
        for row in rows {
            let task_id: Uuid = row.get("task_id");
            let result_state: String = row.get("result_state");
            let result_data: Option<serde_json::Value> = row.get("result_data");
            let error_message: Option<String> = row.get("error_message");
            let retry_count: Option<i32> = row.get("retry_count");

            let result = match result_state.as_str() {
                "pending" => TaskResult::Pending,
                "started" => TaskResult::Started,
                "success" => TaskResult::Success(result_data.unwrap_or(json!(null))),
                "failure" => TaskResult::Failure(error_message.unwrap_or_default()),
                "revoked" => TaskResult::Revoked,
                "retry" => TaskResult::Retry(retry_count.unwrap_or(0) as u32),
                _ => TaskResult::Pending,
            };

            let meta = TaskMeta {
                task_id: row.get("task_id"),
                task_name: row.get("task_name"),
                result,
                created_at: row.get("created_at"),
                started_at: row.get("started_at"),
                completed_at: row.get("completed_at"),
                worker: row.get("worker"),
                progress: None,
            };

            results_map.insert(task_id, meta);
        }

        // Return results in the same order as input task_ids
        Ok(task_ids
            .iter()
            .map(|id| results_map.get(id).cloned())
            .collect())
    }

    async fn delete_results_batch(&mut self, task_ids: &[Uuid]) -> Result<()> {
        if task_ids.is_empty() {
            return Ok(());
        }

        sqlx::query("DELETE FROM celers_task_results WHERE task_id = ANY($1)")
            .bind(task_ids)
            .execute(&self.pool)
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to delete results: {}", e)))?;

        Ok(())
    }
}

/// MySQL result backend implementation
#[derive(Clone)]
pub struct MysqlResultBackend {
    pool: MySqlPool,
}

impl MysqlResultBackend {
    /// Create a new MySQL result backend
    ///
    /// # Arguments
    /// * `database_url` - MySQL connection string (e.g., "mysql://user:pass@localhost/db")
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = sqlx::mysql::MySqlPoolOptions::new()
            .max_connections(20)
            .acquire_timeout(Duration::from_secs(5))
            .connect(database_url)
            .await
            .map_err(|e| {
                BackendError::Connection(format!("Failed to connect to database: {}", e))
            })?;

        Ok(Self { pool })
    }

    /// Run database migrations
    pub async fn migrate(&self) -> Result<()> {
        let migration_sql = include_str!("../migrations/001_init_mysql.sql");

        // Split and execute MySQL migration (handle DELIMITER sections)
        let statements: Vec<&str> = migration_sql.split("DELIMITER //").collect();

        // Execute main DDL
        if let Some(main_sql) = statements.first() {
            for statement in main_sql.split(';') {
                let trimmed = statement.trim();
                if !trimmed.is_empty() && !trimmed.starts_with("--") {
                    sqlx::query(trimmed)
                        .execute(&self.pool)
                        .await
                        .map_err(|e| {
                            BackendError::Connection(format!("Migration failed: {}", e))
                        })?;
                }
            }
        }

        // Execute stored procedures
        for &proc_section in statements.iter().skip(1) {
            if let Some(proc_sql) = proc_section.split("DELIMITER ;").next() {
                let trimmed = proc_sql.trim();
                if !trimmed.is_empty() {
                    sqlx::query(trimmed)
                        .execute(&self.pool)
                        .await
                        .map_err(|e| {
                            BackendError::Connection(format!(
                                "Stored procedure creation failed: {}",
                                e
                            ))
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
}

#[async_trait]
impl ResultBackend for MysqlResultBackend {
    async fn store_result(&mut self, task_id: Uuid, meta: &TaskMeta) -> Result<()> {
        let (result_state, result_data, error_message, retry_count) = match &meta.result {
            TaskResult::Pending => ("pending", None, None, None),
            TaskResult::Started => ("started", None, None, None),
            TaskResult::Success(data) => ("success", Some(data.clone()), None, None),
            TaskResult::Failure(err) => ("failure", None, Some(err.clone()), None),
            TaskResult::Revoked => ("revoked", None, None, None),
            TaskResult::Retry(count) => ("retry", None, None, Some(*count as i32)),
        };

        let result_data_str =
            result_data.map(|v| serde_json::to_string(&v).unwrap_or_else(|_| "null".to_string()));

        sqlx::query(
            r#"
            INSERT INTO celers_task_results
                (task_id, task_name, result_state, result_data, error_message, retry_count,
                 created_at, started_at, completed_at, worker)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON DUPLICATE KEY UPDATE
                result_state = VALUES(result_state),
                result_data = VALUES(result_data),
                error_message = VALUES(error_message),
                retry_count = VALUES(retry_count),
                started_at = VALUES(started_at),
                completed_at = VALUES(completed_at),
                worker = VALUES(worker)
            "#,
        )
        .bind(task_id.to_string())
        .bind(&meta.task_name)
        .bind(result_state)
        .bind(result_data_str)
        .bind(error_message)
        .bind(retry_count)
        .bind(meta.created_at)
        .bind(meta.started_at)
        .bind(meta.completed_at)
        .bind(&meta.worker)
        .execute(&self.pool)
        .await
        .map_err(|e| BackendError::Connection(format!("Failed to store result: {}", e)))?;

        Ok(())
    }

    async fn get_result(&mut self, task_id: Uuid) -> Result<Option<TaskMeta>> {
        let row = sqlx::query(
            r#"
            SELECT task_id, task_name, result_state, result_data, error_message,
                   retry_count, created_at, started_at, completed_at, worker
            FROM celers_task_results
            WHERE task_id = ?
            "#,
        )
        .bind(task_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| BackendError::Connection(format!("Failed to get result: {}", e)))?;

        match row {
            Some(row) => {
                let task_id_str: String = row.get("task_id");
                let result_state: String = row.get("result_state");
                let result_data_str: Option<String> = row.get("result_data");
                let error_message: Option<String> = row.get("error_message");
                let retry_count: Option<i32> = row.get("retry_count");

                let result_data = result_data_str.and_then(|s| serde_json::from_str(&s).ok());

                let result = match result_state.as_str() {
                    "pending" => TaskResult::Pending,
                    "started" => TaskResult::Started,
                    "success" => TaskResult::Success(result_data.unwrap_or(json!(null))),
                    "failure" => TaskResult::Failure(error_message.unwrap_or_default()),
                    "revoked" => TaskResult::Revoked,
                    "retry" => TaskResult::Retry(retry_count.unwrap_or(0) as u32),
                    _ => TaskResult::Pending,
                };

                let meta = TaskMeta {
                    task_id: Uuid::parse_str(&task_id_str)
                        .map_err(|e| BackendError::Serialization(e.to_string()))?,
                    task_name: row.get("task_name"),
                    result,
                    created_at: row.get::<DateTime<Utc>, _>("created_at"),
                    started_at: row.get("started_at"),
                    completed_at: row.get("completed_at"),
                    worker: row.get("worker"),
                    progress: None,
                };

                Ok(Some(meta))
            }
            None => Ok(None),
        }
    }

    async fn delete_result(&mut self, task_id: Uuid) -> Result<()> {
        sqlx::query("DELETE FROM celers_task_results WHERE task_id = ?")
            .bind(task_id.to_string())
            .execute(&self.pool)
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to delete result: {}", e)))?;

        Ok(())
    }

    async fn set_expiration(&mut self, task_id: Uuid, ttl: Duration) -> Result<()> {
        let expires_at = Utc::now() + chrono::Duration::from_std(ttl).unwrap();

        sqlx::query("UPDATE celers_task_results SET expires_at = ? WHERE task_id = ?")
            .bind(expires_at)
            .bind(task_id.to_string())
            .execute(&self.pool)
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to set expiration: {}", e)))?;

        Ok(())
    }

    async fn chord_init(&mut self, state: ChordState) -> Result<()> {
        let task_ids = serde_json::to_string(&state.task_ids)
            .map_err(|e| BackendError::Serialization(e.to_string()))?;

        sqlx::query(
            r#"
            INSERT INTO celers_chord_state (chord_id, total, completed, callback, task_ids)
            VALUES (?, ?, 0, ?, ?)
            ON DUPLICATE KEY UPDATE
                total = VALUES(total),
                callback = VALUES(callback),
                task_ids = VALUES(task_ids)
            "#,
        )
        .bind(state.chord_id.to_string())
        .bind(state.total as i32)
        .bind(&state.callback)
        .bind(task_ids)
        .execute(&self.pool)
        .await
        .map_err(|e| BackendError::Connection(format!("Failed to init chord: {}", e)))?;

        Ok(())
    }

    async fn chord_complete_task(&mut self, chord_id: Uuid) -> Result<usize> {
        // MySQL doesn't support function returns in SELECT, use procedure with OUT parameter
        // For now, use a simpler UPDATE + SELECT approach
        sqlx::query("UPDATE celers_chord_state SET completed = completed + 1 WHERE chord_id = ?")
            .bind(chord_id.to_string())
            .execute(&self.pool)
            .await
            .map_err(|e| {
                BackendError::Connection(format!("Failed to increment chord counter: {}", e))
            })?;

        let row = sqlx::query("SELECT completed FROM celers_chord_state WHERE chord_id = ?")
            .bind(chord_id.to_string())
            .fetch_one(&self.pool)
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to get chord counter: {}", e)))?;

        let count: i32 = row.get("completed");
        Ok(count as usize)
    }

    async fn chord_get_state(&mut self, chord_id: Uuid) -> Result<Option<ChordState>> {
        let row = sqlx::query(
            r#"
            SELECT chord_id, total, completed, callback, task_ids
            FROM celers_chord_state
            WHERE chord_id = ?
            "#,
        )
        .bind(chord_id.to_string())
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| BackendError::Connection(format!("Failed to get chord state: {}", e)))?;

        match row {
            Some(row) => {
                let chord_id_str: String = row.get("chord_id");
                let task_ids_str: String = row.get("task_ids");
                let task_ids: Vec<Uuid> = serde_json::from_str(&task_ids_str)
                    .map_err(|e| BackendError::Serialization(e.to_string()))?;

                let state = ChordState {
                    chord_id: Uuid::parse_str(&chord_id_str)
                        .map_err(|e| BackendError::Serialization(e.to_string()))?,
                    total: row.get::<i32, _>("total") as usize,
                    completed: row.get::<i32, _>("completed") as usize,
                    callback: row.get("callback"),
                    task_ids,
                };

                Ok(Some(state))
            }
            None => Ok(None),
        }
    }

    // Batch operations using transactions for atomic multi-row operations

    async fn store_results_batch(&mut self, results: &[(Uuid, TaskMeta)]) -> Result<()> {
        if results.is_empty() {
            return Ok(());
        }

        let mut tx =
            self.pool.begin().await.map_err(|e| {
                BackendError::Connection(format!("Failed to begin transaction: {}", e))
            })?;

        for (task_id, meta) in results {
            let (result_state, result_data, error_message, retry_count) = match &meta.result {
                TaskResult::Pending => ("pending", None, None, None),
                TaskResult::Started => ("started", None, None, None),
                TaskResult::Success(data) => ("success", Some(data.clone()), None, None),
                TaskResult::Failure(err) => ("failure", None, Some(err.clone()), None),
                TaskResult::Revoked => ("revoked", None, None, None),
                TaskResult::Retry(count) => ("retry", None, None, Some(*count as i32)),
            };

            sqlx::query(
                r#"
                INSERT INTO celers_task_results
                    (task_id, task_name, result_state, result_data, error_message, retry_count,
                     created_at, started_at, completed_at, worker)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    result_state = VALUES(result_state),
                    result_data = VALUES(result_data),
                    error_message = VALUES(error_message),
                    retry_count = VALUES(retry_count),
                    started_at = VALUES(started_at),
                    completed_at = VALUES(completed_at),
                    worker = VALUES(worker)
                "#,
            )
            .bind(task_id)
            .bind(&meta.task_name)
            .bind(result_state)
            .bind(result_data)
            .bind(error_message)
            .bind(retry_count)
            .bind(meta.created_at)
            .bind(meta.started_at)
            .bind(meta.completed_at)
            .bind(&meta.worker)
            .execute(&mut *tx)
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to store result: {}", e)))?;
        }

        tx.commit().await.map_err(|e| {
            BackendError::Connection(format!("Failed to commit transaction: {}", e))
        })?;

        Ok(())
    }

    async fn get_results_batch(&mut self, task_ids: &[Uuid]) -> Result<Vec<Option<TaskMeta>>> {
        if task_ids.is_empty() {
            return Ok(Vec::new());
        }

        // MySQL requires IN clause with placeholders
        let placeholders = task_ids.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let query_str = format!(
            r#"
            SELECT task_id, task_name, result_state, result_data, error_message,
                   retry_count, created_at, started_at, completed_at, worker
            FROM celers_task_results
            WHERE task_id IN ({})
            "#,
            placeholders
        );

        let mut query = sqlx::query(&query_str);
        for task_id in task_ids {
            query = query.bind(task_id);
        }

        let rows = query
            .fetch_all(&self.pool)
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to get results: {}", e)))?;

        // Create a HashMap for O(1) lookup
        let mut results_map = std::collections::HashMap::new();
        for row in rows {
            let task_id: Uuid = row.get("task_id");
            let result_state: String = row.get("result_state");
            let result_data: Option<serde_json::Value> = row.get("result_data");
            let error_message: Option<String> = row.get("error_message");
            let retry_count: Option<i32> = row.get("retry_count");

            let result = match result_state.as_str() {
                "pending" => TaskResult::Pending,
                "started" => TaskResult::Started,
                "success" => TaskResult::Success(result_data.unwrap_or(json!(null))),
                "failure" => TaskResult::Failure(error_message.unwrap_or_default()),
                "revoked" => TaskResult::Revoked,
                "retry" => TaskResult::Retry(retry_count.unwrap_or(0) as u32),
                _ => TaskResult::Pending,
            };

            let meta = TaskMeta {
                task_id: row.get("task_id"),
                task_name: row.get("task_name"),
                result,
                created_at: row.get("created_at"),
                started_at: row.get("started_at"),
                completed_at: row.get("completed_at"),
                worker: row.get("worker"),
                progress: None,
            };

            results_map.insert(task_id, meta);
        }

        // Return results in the same order as input task_ids
        Ok(task_ids
            .iter()
            .map(|id| results_map.get(id).cloned())
            .collect())
    }

    async fn delete_results_batch(&mut self, task_ids: &[Uuid]) -> Result<()> {
        if task_ids.is_empty() {
            return Ok(());
        }

        // MySQL requires IN clause with placeholders
        let placeholders = task_ids.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
        let query_str = format!(
            "DELETE FROM celers_task_results WHERE task_id IN ({})",
            placeholders
        );

        let mut query = sqlx::query(&query_str);
        for task_id in task_ids {
            query = query.bind(task_id);
        }

        query
            .execute(&self.pool)
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to delete results: {}", e)))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    #[ignore] // Requires PostgreSQL running
    async fn test_postgres_backend_creation() {
        let database_url = std::env::var("DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost/celers_test".to_string());

        let backend = PostgresResultBackend::new(&database_url).await;
        assert!(backend.is_ok());
    }

    #[tokio::test]
    #[ignore] // Requires MySQL running
    async fn test_mysql_backend_creation() {
        let database_url = std::env::var("MYSQL_URL")
            .unwrap_or_else(|_| "mysql://root:password@localhost/celers_test".to_string());

        let backend = MysqlResultBackend::new(&database_url).await;
        assert!(backend.is_ok());
    }
}
