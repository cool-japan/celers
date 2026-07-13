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

pub mod analytics;
pub mod event_persistence;
#[cfg(feature = "distributed-locks")]
pub mod lock;
pub mod result_store;
mod row_ext;
mod tls_mode;

pub use analytics::{
    MysqlAnalytics, PercentileLatencies, PostgresAnalytics, StorageStats, TaskStats, WorkerStat,
};
pub use event_persistence::{DbEventPersister, DbEventPersisterConfig};

use async_trait::async_trait;
pub use celers_backend_redis::{
    BackendError, ChordState, Result, ResultBackend, TaskMeta, TaskResult, TaskTtlConfig,
};
use chrono::{DateTime, Utc};
use oxisql_core::{Connection, ToSqlValue};
use row_ext::{json_from_row, json_param, uuid_param, RowExt};
use serde_json::json;
use std::time::Duration;
use uuid::Uuid;

/// PostgreSQL result backend implementation
#[derive(Clone)]
pub struct PostgresResultBackend {
    conn: oxisql_postgres::PgConnection,
    ttl_config: TaskTtlConfig,
}

impl PostgresResultBackend {
    /// Create a new PostgreSQL result backend
    ///
    /// # Arguments
    /// * `database_url` - PostgreSQL connection string (e.g., "postgres://user:pass@localhost/db")
    pub async fn new(database_url: &str) -> Result<Self> {
        let tls = tls_mode::pg_tls_mode_for_url(database_url)
            .map_err(|e| BackendError::Connection(format!("Failed to resolve TLS mode: {e}")))?;
        let conn = oxisql_postgres::PgConnection::connect_with_timeout(
            database_url,
            tls,
            Duration::from_secs(5),
        )
        .await
        .map_err(|e| BackendError::Connection(format!("Failed to connect to database: {}", e)))?;

        Ok(Self {
            conn,
            ttl_config: TaskTtlConfig::new(),
        })
    }

    /// Configure per-task-type TTL
    pub fn with_ttl_config(mut self, config: TaskTtlConfig) -> Self {
        self.ttl_config = config;
        self
    }

    /// Get the TTL configuration
    pub fn ttl_config(&self) -> &TaskTtlConfig {
        &self.ttl_config
    }

    /// Get a mutable reference to the TTL configuration
    pub fn ttl_config_mut(&mut self) -> &mut TaskTtlConfig {
        &mut self.ttl_config
    }

    /// Run database migrations
    pub async fn migrate(&self) -> Result<()> {
        let migration_sql = include_str!("../migrations/001_init_postgres.sql");

        // `execute_batch` (simple-query protocol, `batch_execute` under the
        // hood) is used rather than `execute` (extended/prepared-statement
        // protocol) because the migration file contains multiple `;`-
        // separated DDL statements in one string — the extended protocol
        // `oxisql_postgres::Connection::execute`/`query` use rejects
        // multi-statement text, matching the same constraint the
        // pre-migration `sqlx::query(...).execute(...)` call relied on
        // sqlx's own simple-query fallback for.
        self.conn
            .execute_batch(migration_sql)
            .await
            .map_err(|e| BackendError::Connection(format!("Migration failed: {}", e)))?;

        Ok(())
    }

    /// Get the underlying connection
    pub fn connection(&self) -> &oxisql_postgres::PgConnection {
        &self.conn
    }

    /// Return an analytics helper bound to the same connection.
    pub fn analytics(&self) -> PostgresAnalytics {
        PostgresAnalytics::new(self.conn.clone())
    }

    /// Clean up expired results (returns number of deleted rows)
    pub async fn cleanup_expired(&self) -> Result<usize> {
        let rows = self
            .conn
            .query("SELECT cleanup_expired_results()", &[])
            .await
            .map_err(|e| {
                BackendError::Connection(format!("Failed to cleanup expired results: {}", e))
            })?;
        let row = rows.into_iter().next().ok_or_else(|| {
            BackendError::Connection("cleanup_expired_results() returned no rows".to_string())
        })?;

        let count: i64 = row.col_idx(0).map_err(|e| {
            BackendError::Connection(format!("Failed to read cleanup_expired count: {e}"))
        })?;
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

        let created_at_param = meta.created_at.to_rfc3339();
        let started_at_param = meta.started_at.map(|dt| dt.to_rfc3339());
        let completed_at_param = meta.completed_at.map(|dt| dt.to_rfc3339());

        self.conn
            .execute(
                r#"
                INSERT INTO celers_task_results
                    (task_id, task_name, result_state, result_data, error_message, retry_count,
                     created_at, started_at, completed_at, worker)
                VALUES ($1, $2, $3, $4, $5, $6,
                        $7::text::timestamptz, $8::text::timestamptz, $9::text::timestamptz, $10)
                ON CONFLICT (task_id) DO UPDATE SET
                    result_state = EXCLUDED.result_state,
                    result_data = EXCLUDED.result_data,
                    error_message = EXCLUDED.error_message,
                    retry_count = EXCLUDED.retry_count,
                    started_at = EXCLUDED.started_at,
                    completed_at = EXCLUDED.completed_at,
                    worker = EXCLUDED.worker
                "#,
                &[
                    &uuid_param(&task_id),
                    &meta.task_name,
                    &result_state,
                    &result_data.map(|v| json_param(&v)),
                    &error_message,
                    &retry_count,
                    &created_at_param,
                    &started_at_param,
                    &completed_at_param,
                    &meta.worker,
                ],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to store result: {}", e)))?;

        // Apply per-task TTL if configured
        if let Some(ttl) = self.ttl_config.get_ttl(&meta.task_name) {
            self.set_expiration(task_id, ttl).await?;
        }

        Ok(())
    }

    async fn get_result(&mut self, task_id: Uuid) -> Result<Option<TaskMeta>> {
        let rows = self
            .conn
            .query(
                r#"
                SELECT task_id, task_name, result_state, result_data, error_message,
                       retry_count, created_at, started_at, completed_at, worker
                FROM celers_task_results
                WHERE task_id = $1
                "#,
                &[&uuid_param(&task_id)],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to get result: {}", e)))?;

        match rows.into_iter().next() {
            Some(row) => {
                let result_state: String = row
                    .col("result_state")
                    .map_err(|e| BackendError::Connection(format!("Failed to get result: {e}")))?;
                let result_data = json_from_row(&row, "result_data")
                    .map_err(|e| BackendError::Connection(format!("Failed to get result: {e}")))?;
                let result_data = if result_data.is_null() {
                    None
                } else {
                    Some(result_data)
                };
                let error_message: Option<String> = row
                    .col("error_message")
                    .map_err(|e| BackendError::Connection(format!("Failed to get result: {e}")))?;
                let retry_count: Option<i32> = row
                    .col("retry_count")
                    .map_err(|e| BackendError::Connection(format!("Failed to get result: {e}")))?;

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
                    task_id: row_ext::uuid_from_row(&row, "task_id").map_err(|e| {
                        BackendError::Connection(format!("Failed to get result: {e}"))
                    })?,
                    task_name: row.col("task_name").map_err(|e| {
                        BackendError::Connection(format!("Failed to get result: {e}"))
                    })?,
                    result,
                    created_at: row.col("created_at").map_err(|e| {
                        BackendError::Connection(format!("Failed to get result: {e}"))
                    })?,
                    started_at: row.col("started_at").map_err(|e| {
                        BackendError::Connection(format!("Failed to get result: {e}"))
                    })?,
                    completed_at: row.col("completed_at").map_err(|e| {
                        BackendError::Connection(format!("Failed to get result: {e}"))
                    })?,
                    worker: row.col("worker").map_err(|e| {
                        BackendError::Connection(format!("Failed to get result: {e}"))
                    })?,
                    progress: None,
                    version: 0,
                    tags: Vec::new(),
                    metadata: std::collections::HashMap::new(),
                    worker_hostname: None,
                    runtime_ms: None,
                    memory_bytes: None,
                    retries: None,
                    queue: None,
                };

                Ok(Some(meta))
            }
            None => Ok(None),
        }
    }

    async fn delete_result(&mut self, task_id: Uuid) -> Result<()> {
        self.conn
            .execute(
                "DELETE FROM celers_task_results WHERE task_id = $1",
                &[&uuid_param(&task_id)],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to delete result: {}", e)))?;

        Ok(())
    }

    async fn set_expiration(&mut self, task_id: Uuid, ttl: Duration) -> Result<()> {
        let expires_at = Utc::now()
            + chrono::Duration::from_std(ttl)
                .map_err(|e| BackendError::Serialization(format!("Invalid TTL duration: {}", e)))?;
        let expires_at_param = expires_at.to_rfc3339();

        self.conn
            .execute(
                "UPDATE celers_task_results SET expires_at = $1::text::timestamptz WHERE task_id = $2",
                &[&expires_at_param, &uuid_param(&task_id)],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to set expiration: {}", e)))?;

        Ok(())
    }

    async fn chord_init(&mut self, state: ChordState) -> Result<()> {
        let task_ids = serde_json::to_value(&state.task_ids)
            .map_err(|e| BackendError::Serialization(e.to_string()))?;
        let created_at_param = state.created_at.to_rfc3339();
        let timeout_secs_param = state.timeout.map(|d| d.as_secs() as i64);

        self.conn
            .execute(
                r#"
                INSERT INTO celers_chord_state (chord_id, total, completed, callback, task_ids, created_at, timeout_seconds, cancelled, cancellation_reason)
                VALUES ($1, $2, 0, $3, $4, $5::text::timestamptz, $6, $7, $8)
                ON CONFLICT (chord_id) DO UPDATE SET
                    total = EXCLUDED.total,
                    callback = EXCLUDED.callback,
                    task_ids = EXCLUDED.task_ids,
                    timeout_seconds = EXCLUDED.timeout_seconds,
                    cancelled = EXCLUDED.cancelled,
                    cancellation_reason = EXCLUDED.cancellation_reason
                "#,
                &[
                    &uuid_param(&state.chord_id),
                    &(state.total as i64),
                    &state.callback,
                    &json_param(&task_ids),
                    &created_at_param,
                    &timeout_secs_param,
                    &state.cancelled,
                    &state.cancellation_reason,
                ],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to init chord: {}", e)))?;

        Ok(())
    }

    async fn chord_complete_task(&mut self, chord_id: Uuid) -> Result<usize> {
        let rows = self
            .conn
            .query(
                "SELECT chord_increment_counter($1)",
                &[&uuid_param(&chord_id)],
            )
            .await
            .map_err(|e| {
                BackendError::Connection(format!("Failed to increment chord counter: {}", e))
            })?;
        let row = rows.into_iter().next().ok_or_else(|| {
            BackendError::Connection("chord_increment_counter() returned no rows".to_string())
        })?;

        let count: i64 = row
            .col_idx(0)
            .map_err(|e| BackendError::Connection(format!("Failed to read chord counter: {e}")))?;
        Ok(count as usize)
    }

    async fn chord_get_state(&mut self, chord_id: Uuid) -> Result<Option<ChordState>> {
        let rows = self
            .conn
            .query(
                r#"
                SELECT chord_id, total, completed, callback, task_ids, created_at, timeout_seconds, cancelled, cancellation_reason
                FROM celers_chord_state
                WHERE chord_id = $1
                "#,
                &[&uuid_param(&chord_id)],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to get chord state: {}", e)))?;

        match rows.into_iter().next() {
            Some(row) => {
                let task_ids_json = json_from_row(&row, "task_ids").map_err(|e| {
                    BackendError::Connection(format!("Failed to get chord state: {e}"))
                })?;
                let task_ids: Vec<Uuid> = serde_json::from_value(task_ids_json)
                    .map_err(|e| BackendError::Serialization(e.to_string()))?;

                let total: i64 = row.col("total").map_err(|e| {
                    BackendError::Connection(format!("Failed to get chord state: {e}"))
                })?;
                let completed: i64 = row.col("completed").map_err(|e| {
                    BackendError::Connection(format!("Failed to get chord state: {e}"))
                })?;
                let timeout_secs: Option<i64> = row.col("timeout_seconds").map_err(|e| {
                    BackendError::Connection(format!("Failed to get chord state: {e}"))
                })?;

                let state = ChordState {
                    chord_id: row_ext::uuid_from_row(&row, "chord_id").map_err(|e| {
                        BackendError::Connection(format!("Failed to get chord state: {e}"))
                    })?,
                    total: total as usize,
                    completed: completed as usize,
                    callback: row.col("callback").map_err(|e| {
                        BackendError::Connection(format!("Failed to get chord state: {e}"))
                    })?,
                    task_ids,
                    created_at: row.col("created_at").map_err(|e| {
                        BackendError::Connection(format!("Failed to get chord state: {e}"))
                    })?,
                    timeout: timeout_secs.map(|s| std::time::Duration::from_secs(s as u64)),
                    cancelled: row.col("cancelled").map_err(|e| {
                        BackendError::Connection(format!("Failed to get chord state: {e}"))
                    })?,
                    cancellation_reason: row.col("cancellation_reason").map_err(|e| {
                        BackendError::Connection(format!("Failed to get chord state: {e}"))
                    })?,
                    retry_count: 0,
                    max_retries: None,
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
            self.conn.transaction().await.map_err(|e| {
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
            let created_at_param = meta.created_at.to_rfc3339();
            let started_at_param = meta.started_at.map(|dt| dt.to_rfc3339());
            let completed_at_param = meta.completed_at.map(|dt| dt.to_rfc3339());

            tx.execute(
                r#"
                INSERT INTO celers_task_results
                    (task_id, task_name, result_state, result_data, error_message, retry_count,
                     created_at, started_at, completed_at, worker)
                VALUES ($1, $2, $3, $4, $5, $6,
                        $7::text::timestamptz, $8::text::timestamptz, $9::text::timestamptz, $10)
                ON CONFLICT (task_id) DO UPDATE SET
                    result_state = EXCLUDED.result_state,
                    result_data = EXCLUDED.result_data,
                    error_message = EXCLUDED.error_message,
                    retry_count = EXCLUDED.retry_count,
                    started_at = EXCLUDED.started_at,
                    completed_at = EXCLUDED.completed_at,
                    worker = EXCLUDED.worker
                "#,
                &[
                    &uuid_param(task_id),
                    &meta.task_name,
                    &result_state,
                    &result_data.map(|v| json_param(&v)),
                    &error_message,
                    &retry_count,
                    &created_at_param,
                    &started_at_param,
                    &completed_at_param,
                    &meta.worker,
                ],
            )
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

        // oxisql-core has no `ToSqlValue` impl for `Vec<Uuid>` (or any
        // non-`Vec<u8>` `Vec<T>` — verified exhaustively against
        // oxisql-core's `traits.rs`), and building a `Value::TypedArray`
        // literal to bind against `= ANY($1)` would hit the exact same
        // Postgres binary/text wire-format hazard documented in
        // `row_ext.rs` for `DateTime<Utc>` (a `uuid[]`-inferred placeholder
        // expects a structured binary array, not the array-literal text
        // `value_to_param` would actually send). A dynamically-built
        // `IN (...)` with one `$n` placeholder per element sidesteps the
        // array-binding hazard entirely — every value still goes through a
        // parameter placeholder, only the *number* of placeholders (a count,
        // not a value) is spliced into the SQL text.
        let placeholders: String = (1..=task_ids.len())
            .map(|i| format!("${i}"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!(
            r#"
            SELECT task_id, task_name, result_state, result_data, error_message,
                   retry_count, created_at, started_at, completed_at, worker
            FROM celers_task_results
            WHERE task_id IN ({placeholders})
            "#
        );
        let params: Vec<oxisql_core::Value> = task_ids.iter().map(uuid_param).collect();
        let param_refs: Vec<&dyn ToSqlValue> =
            params.iter().map(|v| v as &dyn ToSqlValue).collect();

        let rows = self
            .conn
            .query(&sql, &param_refs)
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to get results: {}", e)))?;

        // Create a HashMap for O(1) lookup
        let mut results_map = std::collections::HashMap::new();
        for row in rows {
            let task_id = row_ext::uuid_from_row(&row, "task_id")
                .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?;
            let result_state: String = row
                .col("result_state")
                .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?;
            let result_data = json_from_row(&row, "result_data")
                .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?;
            let result_data = if result_data.is_null() {
                None
            } else {
                Some(result_data)
            };
            let error_message: Option<String> = row
                .col("error_message")
                .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?;
            let retry_count: Option<i32> = row
                .col("retry_count")
                .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?;

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
                task_id,
                task_name: row
                    .col("task_name")
                    .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?,
                result,
                created_at: row
                    .col("created_at")
                    .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?,
                started_at: row
                    .col("started_at")
                    .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?,
                completed_at: row
                    .col("completed_at")
                    .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?,
                worker: row
                    .col("worker")
                    .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?,
                progress: None,
                version: 0,
                tags: Vec::new(),
                metadata: std::collections::HashMap::new(),
                worker_hostname: None,
                runtime_ms: None,
                memory_bytes: None,
                retries: None,
                queue: None,
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

        // See the comment in `get_results_batch` for why a dynamically-built
        // `IN (...)` with one `$n` placeholder per element is used instead
        // of `= ANY($1)`.
        let placeholders: String = (1..=task_ids.len())
            .map(|i| format!("${i}"))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = format!("DELETE FROM celers_task_results WHERE task_id IN ({placeholders})");
        let params: Vec<oxisql_core::Value> = task_ids.iter().map(uuid_param).collect();
        let param_refs: Vec<&dyn ToSqlValue> =
            params.iter().map(|v| v as &dyn ToSqlValue).collect();

        self.conn
            .execute(&sql, &param_refs)
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to delete results: {}", e)))?;

        Ok(())
    }
}

/// MySQL result backend implementation
#[derive(Clone)]
pub struct MysqlResultBackend {
    conn: oxisql_mysql::MyConnection,
    ttl_config: TaskTtlConfig,
}

impl MysqlResultBackend {
    /// Create a new MySQL result backend
    ///
    /// # Arguments
    /// * `database_url` - MySQL connection string (e.g., "mysql://user:pass@localhost/db")
    pub async fn new(database_url: &str) -> Result<Self> {
        let tls = tls_mode::mysql_tls_mode_for_url(database_url)
            .map_err(|e| BackendError::Connection(format!("Failed to resolve TLS mode: {e}")))?;
        let conn = oxisql_mysql::MyConnection::connect(database_url, tls)
            .await
            .map_err(|e| {
                BackendError::Connection(format!("Failed to connect to database: {}", e))
            })?;

        Ok(Self {
            conn,
            ttl_config: TaskTtlConfig::new(),
        })
    }

    /// Configure per-task-type TTL
    pub fn with_ttl_config(mut self, config: TaskTtlConfig) -> Self {
        self.ttl_config = config;
        self
    }

    /// Get the TTL configuration
    pub fn ttl_config(&self) -> &TaskTtlConfig {
        &self.ttl_config
    }

    /// Get a mutable reference to the TTL configuration
    pub fn ttl_config_mut(&mut self) -> &mut TaskTtlConfig {
        &mut self.ttl_config
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
                    self.conn.execute(trimmed, &[]).await.map_err(|e| {
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
                    self.conn.execute(trimmed, &[]).await.map_err(|e| {
                        BackendError::Connection(format!("Stored procedure creation failed: {}", e))
                    })?;
                }
            }
        }

        Ok(())
    }

    /// Return an analytics helper bound to the same connection.
    pub fn analytics(&self) -> MysqlAnalytics {
        MysqlAnalytics::new(self.conn.clone())
    }

    /// Get the underlying connection
    pub fn connection(&self) -> &oxisql_mysql::MyConnection {
        &self.conn
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
        // MySQL DATETIME/TIMESTAMP grammar convention — see `row_ext.rs`'s
        // "DateTime<Utc> parameter convention (MySQL)" section for why
        // `.to_rfc3339()` is unsafe here and this format is the verified
        // MySQL-server-accepted one.
        let created_at_param = meta.created_at.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
        let started_at_param = meta
            .started_at
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string());
        let completed_at_param = meta
            .completed_at
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string());

        self.conn
            .execute(
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
                &[
                    &task_id.to_string(),
                    &meta.task_name,
                    &result_state,
                    &result_data_str,
                    &error_message,
                    &retry_count,
                    &created_at_param,
                    &started_at_param,
                    &completed_at_param,
                    &meta.worker,
                ],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to store result: {}", e)))?;

        // Apply per-task TTL if configured
        if let Some(ttl) = self.ttl_config.get_ttl(&meta.task_name) {
            self.set_expiration(task_id, ttl).await?;
        }

        Ok(())
    }

    async fn get_result(&mut self, task_id: Uuid) -> Result<Option<TaskMeta>> {
        let rows = self
            .conn
            .query(
                r#"
                SELECT task_id, task_name, result_state, result_data, error_message,
                       retry_count, created_at, started_at, completed_at, worker
                FROM celers_task_results
                WHERE task_id = ?
                "#,
                &[&task_id.to_string()],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to get result: {}", e)))?;

        match rows.into_iter().next() {
            Some(row) => {
                let task_id_str: String = row
                    .col("task_id")
                    .map_err(|e| BackendError::Connection(format!("Failed to get result: {e}")))?;
                let result_state: String = row
                    .col("result_state")
                    .map_err(|e| BackendError::Connection(format!("Failed to get result: {e}")))?;
                let result_data_str: Option<String> = row
                    .col("result_data")
                    .map_err(|e| BackendError::Connection(format!("Failed to get result: {e}")))?;
                let error_message: Option<String> = row
                    .col("error_message")
                    .map_err(|e| BackendError::Connection(format!("Failed to get result: {e}")))?;
                let retry_count: Option<i32> = row
                    .col("retry_count")
                    .map_err(|e| BackendError::Connection(format!("Failed to get result: {e}")))?;

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
                    task_name: row.col("task_name").map_err(|e| {
                        BackendError::Connection(format!("Failed to get result: {e}"))
                    })?,
                    result,
                    created_at: row.col::<DateTime<Utc>>("created_at").map_err(|e| {
                        BackendError::Connection(format!("Failed to get result: {e}"))
                    })?,
                    started_at: row.col("started_at").map_err(|e| {
                        BackendError::Connection(format!("Failed to get result: {e}"))
                    })?,
                    completed_at: row.col("completed_at").map_err(|e| {
                        BackendError::Connection(format!("Failed to get result: {e}"))
                    })?,
                    worker: row.col("worker").map_err(|e| {
                        BackendError::Connection(format!("Failed to get result: {e}"))
                    })?,
                    progress: None,
                    version: 0,
                    tags: Vec::new(),
                    metadata: std::collections::HashMap::new(),
                    worker_hostname: None,
                    runtime_ms: None,
                    memory_bytes: None,
                    retries: None,
                    queue: None,
                };

                Ok(Some(meta))
            }
            None => Ok(None),
        }
    }

    async fn delete_result(&mut self, task_id: Uuid) -> Result<()> {
        self.conn
            .execute(
                "DELETE FROM celers_task_results WHERE task_id = ?",
                &[&task_id.to_string()],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to delete result: {}", e)))?;

        Ok(())
    }

    async fn set_expiration(&mut self, task_id: Uuid, ttl: Duration) -> Result<()> {
        let expires_at = Utc::now()
            + chrono::Duration::from_std(ttl)
                .map_err(|e| BackendError::Serialization(format!("Invalid TTL duration: {}", e)))?;
        let expires_at_param = expires_at.format("%Y-%m-%d %H:%M:%S%.6f").to_string();

        self.conn
            .execute(
                "UPDATE celers_task_results SET expires_at = ? WHERE task_id = ?",
                &[&expires_at_param, &task_id.to_string()],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to set expiration: {}", e)))?;

        Ok(())
    }

    async fn chord_init(&mut self, state: ChordState) -> Result<()> {
        let task_ids = serde_json::to_string(&state.task_ids)
            .map_err(|e| BackendError::Serialization(e.to_string()))?;

        let created_at_param = state.created_at.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
        let timeout_secs_param = state.timeout.map(|d| d.as_secs() as i64);

        self.conn
            .execute(
                r#"
                INSERT INTO celers_chord_state (chord_id, total, completed, callback, task_ids, created_at, timeout_seconds, cancelled, cancellation_reason)
                VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?)
                ON DUPLICATE KEY UPDATE
                    total = VALUES(total),
                    callback = VALUES(callback),
                    task_ids = VALUES(task_ids),
                    timeout_seconds = VALUES(timeout_seconds),
                    cancelled = VALUES(cancelled),
                    cancellation_reason = VALUES(cancellation_reason)
                "#,
                &[
                    &state.chord_id.to_string(),
                    &(state.total as i64),
                    &state.callback,
                    &task_ids,
                    &created_at_param,
                    &timeout_secs_param,
                    &state.cancelled,
                    &state.cancellation_reason,
                ],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to init chord: {}", e)))?;

        Ok(())
    }

    async fn chord_complete_task(&mut self, chord_id: Uuid) -> Result<usize> {
        // MySQL doesn't support function returns in SELECT, use procedure with OUT parameter
        // For now, use a simpler UPDATE + SELECT approach
        self.conn
            .execute(
                "UPDATE celers_chord_state SET completed = completed + 1 WHERE chord_id = ?",
                &[&chord_id.to_string()],
            )
            .await
            .map_err(|e| {
                BackendError::Connection(format!("Failed to increment chord counter: {}", e))
            })?;

        let rows = self
            .conn
            .query(
                "SELECT completed FROM celers_chord_state WHERE chord_id = ?",
                &[&chord_id.to_string()],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to get chord counter: {}", e)))?;
        let row = rows.into_iter().next().ok_or_else(|| {
            BackendError::Connection("chord counter query returned no rows".to_string())
        })?;

        let count: i64 = row
            .col("completed")
            .map_err(|e| BackendError::Connection(format!("Failed to read chord counter: {e}")))?;
        Ok(count as usize)
    }

    async fn chord_get_state(&mut self, chord_id: Uuid) -> Result<Option<ChordState>> {
        let rows = self
            .conn
            .query(
                r#"
                SELECT chord_id, total, completed, callback, task_ids, created_at, timeout_seconds, cancelled, cancellation_reason
                FROM celers_chord_state
                WHERE chord_id = ?
                "#,
                &[&chord_id.to_string()],
            )
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to get chord state: {}", e)))?;

        match rows.into_iter().next() {
            Some(row) => {
                let chord_id_str: String = row.col("chord_id").map_err(|e| {
                    BackendError::Connection(format!("Failed to get chord state: {e}"))
                })?;
                let task_ids_str: String = row.col("task_ids").map_err(|e| {
                    BackendError::Connection(format!("Failed to get chord state: {e}"))
                })?;
                let task_ids: Vec<Uuid> = serde_json::from_str(&task_ids_str)
                    .map_err(|e| BackendError::Serialization(e.to_string()))?;

                let total: i64 = row.col("total").map_err(|e| {
                    BackendError::Connection(format!("Failed to get chord state: {e}"))
                })?;
                let completed: i64 = row.col("completed").map_err(|e| {
                    BackendError::Connection(format!("Failed to get chord state: {e}"))
                })?;
                let timeout_secs: Option<i64> = row.col("timeout_seconds").map_err(|e| {
                    BackendError::Connection(format!("Failed to get chord state: {e}"))
                })?;

                let state = ChordState {
                    chord_id: Uuid::parse_str(&chord_id_str)
                        .map_err(|e| BackendError::Serialization(e.to_string()))?,
                    total: total as usize,
                    completed: completed as usize,
                    callback: row.col("callback").map_err(|e| {
                        BackendError::Connection(format!("Failed to get chord state: {e}"))
                    })?,
                    task_ids,
                    created_at: row.col("created_at").map_err(|e| {
                        BackendError::Connection(format!("Failed to get chord state: {e}"))
                    })?,
                    timeout: timeout_secs.map(|s| std::time::Duration::from_secs(s as u64)),
                    cancelled: row.col("cancelled").map_err(|e| {
                        BackendError::Connection(format!("Failed to get chord state: {e}"))
                    })?,
                    cancellation_reason: row.col("cancellation_reason").map_err(|e| {
                        BackendError::Connection(format!("Failed to get chord state: {e}"))
                    })?,
                    retry_count: 0,
                    max_retries: None,
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
            self.conn.transaction().await.map_err(|e| {
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
            let result_data_str = result_data
                .map(|v| serde_json::to_string(&v).unwrap_or_else(|_| "null".to_string()));
            let created_at_param = meta.created_at.format("%Y-%m-%d %H:%M:%S%.6f").to_string();
            let started_at_param = meta
                .started_at
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string());
            let completed_at_param = meta
                .completed_at
                .map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string());

            tx.execute(
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
                &[
                    &task_id.to_string(),
                    &meta.task_name,
                    &result_state,
                    &result_data_str,
                    &error_message,
                    &retry_count,
                    &created_at_param,
                    &started_at_param,
                    &completed_at_param,
                    &meta.worker,
                ],
            )
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
        // oxisql_mysql::MyConnection::execute/query take `&str` directly —
        // sqlx's `AssertSqlSafe` opt-out wrapper has no equivalent (and none
        // is needed): only the placeholder *count* (never a value) was
        // spliced into `query_str` above, matching the exact same
        // static-fragment-only discipline `sqlx::AssertSqlSafe` was
        // previously asserting.
        let id_params: Vec<String> = task_ids.iter().map(|id| id.to_string()).collect();
        let param_refs: Vec<&dyn ToSqlValue> =
            id_params.iter().map(|s| s as &dyn ToSqlValue).collect();

        let rows = self
            .conn
            .query(&query_str, &param_refs)
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to get results: {}", e)))?;

        // Create a HashMap for O(1) lookup
        let mut results_map = std::collections::HashMap::new();
        for row in rows {
            let task_id_str: String = row
                .col("task_id")
                .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?;
            let task_id = Uuid::parse_str(&task_id_str)
                .map_err(|e| BackendError::Serialization(e.to_string()))?;
            let result_state: String = row
                .col("result_state")
                .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?;
            let result_data_str: Option<String> = row
                .col("result_data")
                .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?;
            let result_data = result_data_str.and_then(|s| serde_json::from_str(&s).ok());
            let error_message: Option<String> = row
                .col("error_message")
                .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?;
            let retry_count: Option<i32> = row
                .col("retry_count")
                .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?;

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
                task_id,
                task_name: row
                    .col("task_name")
                    .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?,
                result,
                created_at: row
                    .col("created_at")
                    .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?,
                started_at: row
                    .col("started_at")
                    .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?,
                completed_at: row
                    .col("completed_at")
                    .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?,
                worker: row
                    .col("worker")
                    .map_err(|e| BackendError::Connection(format!("Failed to get results: {e}")))?,
                progress: None,
                version: 0,
                tags: Vec::new(),
                metadata: std::collections::HashMap::new(),
                worker_hostname: None,
                runtime_ms: None,
                memory_bytes: None,
                retries: None,
                queue: None,
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
        let id_params: Vec<String> = task_ids.iter().map(|id| id.to_string()).collect();
        let param_refs: Vec<&dyn ToSqlValue> =
            id_params.iter().map(|s| s as &dyn ToSqlValue).collect();

        self.conn
            .execute(&query_str, &param_refs)
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
