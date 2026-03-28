//! Task deduplication support for preventing duplicate task execution

use celers_core::{Broker, CelersError, Result, SerializedTask, TaskId};
use serde_json::json;
use sqlx::Row;
use uuid::Uuid;

use crate::types::{DeduplicationConfig, DeduplicationInfo};
use crate::PostgresBroker;

#[cfg(feature = "metrics")]
use celers_metrics::{TASKS_ENQUEUED_BY_TYPE, TASKS_ENQUEUED_TOTAL};

impl PostgresBroker {
    /// Enqueue a task with idempotency guarantee
    ///
    /// If a task with the same idempotency key was enqueued within the deduplication
    /// window, this returns the ID of the existing task instead of creating a duplicate.
    ///
    /// # Arguments
    /// * `task` - The task to enqueue
    /// * `idempotency_key` - Unique key to identify duplicate requests
    /// * `config` - Deduplication configuration
    ///
    /// # Returns
    /// The task ID (either newly created or existing)
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::{PostgresBroker, DeduplicationConfig};
    /// use celers_core::{Broker, SerializedTask};
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let config = DeduplicationConfig::default();
    ///
    /// let task = SerializedTask::new("process_order".to_string(), vec![]);
    /// let task_id = broker.enqueue_idempotent(task, "order-123", &config).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn enqueue_idempotent(
        &self,
        task: SerializedTask,
        idempotency_key: &str,
        config: &DeduplicationConfig,
    ) -> Result<TaskId> {
        if !config.enabled {
            // Deduplication disabled, just enqueue normally
            return self.enqueue(task).await;
        }

        // Start a transaction for atomicity
        let mut tx = self
            .pool
            .begin()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        // Check for existing task with this idempotency key
        let existing = sqlx::query(
            r#"
            SELECT task_id, duplicate_count
            FROM celers_task_deduplication
            WHERE idempotency_key = $1
              AND expires_at > NOW()
              AND queue_name = $2
            FOR UPDATE
            "#,
        )
        .bind(idempotency_key)
        .bind(&self.queue_name)
        .fetch_optional(&mut *tx)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to check deduplication: {}", e)))?;

        if let Some(row) = existing {
            let task_id: Uuid = row.get("task_id");
            let duplicate_count: i32 = row.get("duplicate_count");

            // Increment duplicate count
            sqlx::query(
                r#"
                UPDATE celers_task_deduplication
                SET duplicate_count = duplicate_count + 1,
                    last_seen_at = NOW()
                WHERE idempotency_key = $1
                  AND queue_name = $2
                "#,
            )
            .bind(idempotency_key)
            .bind(&self.queue_name)
            .execute(&mut *tx)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to update duplicate count: {}", e)))?;

            tx.commit()
                .await
                .map_err(|e| CelersError::Other(format!("Failed to commit transaction: {}", e)))?;

            tracing::info!(
                idempotency_key = %idempotency_key,
                task_id = %task_id,
                duplicate_count = duplicate_count + 1,
                "Blocked duplicate task enqueue"
            );

            return Ok(task_id);
        }

        // No existing task, enqueue normally
        let task_id = task.metadata.id;
        let mut db_metadata = json!({
            "queue": self.queue_name,
            "enqueued_at": chrono::Utc::now().to_rfc3339(),
            "idempotency_key": idempotency_key,
        });

        // Merge task metadata
        if let Ok(task_meta) = serde_json::to_value(&task.metadata) {
            if let Some(obj) = db_metadata.as_object_mut() {
                if let Some(meta_obj) = task_meta.as_object() {
                    for (k, v) in meta_obj {
                        obj.insert(k.clone(), v.clone());
                    }
                }
            }
        }

        // Insert task
        sqlx::query(
            r#"
            INSERT INTO celers_tasks
                (id, task_name, payload, state, priority, max_retries, metadata, created_at, scheduled_at)
            VALUES ($1, $2, $3, 'pending', $4, $5, $6, NOW(), NOW())
            "#,
        )
        .bind(task_id)
        .bind(&task.metadata.name)
        .bind(&task.payload)
        .bind(task.metadata.priority)
        .bind(task.metadata.max_retries as i32)
        .bind(db_metadata)
        .execute(&mut *tx)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to enqueue task: {}", e)))?;

        // Insert deduplication entry
        let expires_at = chrono::Utc::now() + chrono::Duration::seconds(config.window_secs);
        sqlx::query(
            r#"
            INSERT INTO celers_task_deduplication
                (idempotency_key, task_id, task_name, queue_name, first_seen_at, last_seen_at, expires_at, duplicate_count)
            VALUES ($1, $2, $3, $4, NOW(), NOW(), $5, 0)
            ON CONFLICT (idempotency_key, queue_name) DO NOTHING
            "#,
        )
        .bind(idempotency_key)
        .bind(task_id)
        .bind(&task.metadata.name)
        .bind(&self.queue_name)
        .bind(expires_at)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            CelersError::Other(format!("Failed to insert deduplication entry: {}", e))
        })?;

        tx.commit()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to commit transaction: {}", e)))?;

        #[cfg(feature = "metrics")]
        {
            TASKS_ENQUEUED_TOTAL.inc();
            TASKS_ENQUEUED_BY_TYPE
                .with_label_values(&[&task.metadata.name])
                .inc();
        }

        tracing::info!(
            idempotency_key = %idempotency_key,
            task_id = %task_id,
            task_name = %task.metadata.name,
            "Enqueued task with deduplication"
        );

        Ok(task_id)
    }

    /// Check if a task with the given idempotency key exists
    ///
    /// Returns Some(DeduplicationInfo) if a task exists, None otherwise.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// if let Some(info) = broker.check_deduplication("order-123").await? {
    ///     println!("Task already exists: {}", info.task_id);
    ///     println!("Duplicates blocked: {}", info.duplicate_count);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn check_deduplication(
        &self,
        idempotency_key: &str,
    ) -> Result<Option<DeduplicationInfo>> {
        let row = sqlx::query(
            r#"
            SELECT idempotency_key, task_id, task_name, first_seen_at, expires_at, duplicate_count
            FROM celers_task_deduplication
            WHERE idempotency_key = $1
              AND queue_name = $2
              AND expires_at > NOW()
            "#,
        )
        .bind(idempotency_key)
        .bind(&self.queue_name)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to check deduplication: {}", e)))?;

        match row {
            Some(row) => Ok(Some(DeduplicationInfo {
                idempotency_key: row.get("idempotency_key"),
                task_id: row.get("task_id"),
                task_name: row.get("task_name"),
                first_seen_at: row.get("first_seen_at"),
                expires_at: row.get("expires_at"),
                duplicate_count: row.get("duplicate_count"),
            })),
            None => Ok(None),
        }
    }

    /// Clean up expired deduplication entries
    ///
    /// Removes deduplication entries that have expired to prevent table bloat.
    /// This should be run periodically (e.g., via a maintenance task).
    ///
    /// # Returns
    /// The number of entries deleted
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let deleted = broker.cleanup_deduplication().await?;
    /// println!("Cleaned up {} expired deduplication entries", deleted);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn cleanup_deduplication(&self) -> Result<i64> {
        let result = sqlx::query(
            r#"
            DELETE FROM celers_task_deduplication
            WHERE expires_at < NOW()
            "#,
        )
        .execute(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to cleanup deduplication: {}", e)))?;

        let deleted = result.rows_affected() as i64;

        tracing::info!(
            deleted = deleted,
            "Cleaned up expired deduplication entries"
        );

        Ok(deleted)
    }

    /// Get deduplication statistics
    ///
    /// Returns statistics about deduplication entries in the system.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let (active, total_duplicates) = broker.get_deduplication_stats().await?;
    /// println!("Active dedup entries: {}", active);
    /// println!("Total duplicates blocked: {}", total_duplicates);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_deduplication_stats(&self) -> Result<(i64, i64)> {
        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*) as active_entries,
                COALESCE(SUM(duplicate_count), 0) as total_duplicates
            FROM celers_task_deduplication
            WHERE expires_at > NOW()
              AND queue_name = $1
            "#,
        )
        .bind(&self.queue_name)
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to get deduplication stats: {}", e)))?;

        let active_entries: i64 = row.get("active_entries");
        let total_duplicates: i64 = row.get("total_duplicates");

        Ok((active_entries, total_duplicates))
    }
}
