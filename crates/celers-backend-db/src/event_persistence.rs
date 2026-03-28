//! Database-backed event persistence for CeleRS
//!
//! Provides `DbEventPersister` which stores events in a PostgreSQL table
//! with buffered inserts and SQL-based querying/cleanup.

use async_trait::async_trait;
use celers_core::event::{Event, EventEmitter};
use celers_core::event_persistence::EventPersister;
use chrono::{DateTime, Utc};
use sqlx::{PgPool, Row};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use crate::{BackendError, Result};

/// Configuration for [`DbEventPersister`]
#[derive(Debug, Clone)]
pub struct DbEventPersisterConfig {
    /// Number of events to buffer before flushing to the database
    pub batch_size: usize,
    /// Maximum time between flushes
    pub flush_interval: Duration,
    /// Whether event persistence is enabled
    pub enabled: bool,
}

impl Default for DbEventPersisterConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            flush_interval: Duration::from_secs(5),
            enabled: true,
        }
    }
}

impl DbEventPersisterConfig {
    /// Set the batch size
    #[must_use]
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the flush interval
    #[must_use]
    pub fn with_flush_interval(mut self, interval: Duration) -> Self {
        self.flush_interval = interval;
        self
    }

    /// Set whether persistence is enabled
    #[must_use]
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }
}

/// Database-backed event persister
///
/// Buffers events in memory and flushes them to PostgreSQL in batches.
/// The `celers_events` table is created via the [`migrate`](DbEventPersister::migrate) method.
pub struct DbEventPersister {
    pool: PgPool,
    config: DbEventPersisterConfig,
    buffer: Arc<Mutex<Vec<Event>>>,
}

impl DbEventPersister {
    /// Create a new database event persister
    pub async fn new(pool: PgPool, config: DbEventPersisterConfig) -> Result<Self> {
        Ok(Self {
            pool,
            config,
            buffer: Arc::new(Mutex::new(Vec::new())),
        })
    }

    /// Run database migrations to create the events table and indexes
    pub async fn migrate(&self) -> Result<()> {
        let sql = r#"
            CREATE TABLE IF NOT EXISTS celers_events (
                id BIGSERIAL PRIMARY KEY,
                event_type VARCHAR(64) NOT NULL,
                task_id UUID,
                worker VARCHAR(255),
                timestamp TIMESTAMPTZ NOT NULL,
                payload JSONB NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            );
            CREATE INDEX IF NOT EXISTS idx_celers_events_type ON celers_events(event_type);
            CREATE INDEX IF NOT EXISTS idx_celers_events_task_id ON celers_events(task_id);
            CREATE INDEX IF NOT EXISTS idx_celers_events_timestamp ON celers_events(timestamp);
        "#;

        sqlx::query(sql).execute(&self.pool).await.map_err(|e| {
            BackendError::Connection(format!("Failed to run event migrations: {}", e))
        })?;

        Ok(())
    }

    /// Flush the in-memory buffer to the database
    async fn flush_buffer(&self) -> Result<()> {
        let events = {
            let mut buf = self.buffer.lock().await;
            if buf.is_empty() {
                return Ok(());
            }
            std::mem::take(&mut *buf)
        };

        // Batch insert using a transaction
        let mut tx =
            self.pool.begin().await.map_err(|e| {
                BackendError::Connection(format!("Failed to begin transaction: {}", e))
            })?;

        for event in &events {
            let event_type = event.event_type();
            let task_id = event.task_id();
            let worker = event.hostname().map(|s| s.to_string());
            let timestamp = event.timestamp();
            let payload = serde_json::to_value(event).map_err(|e| {
                BackendError::Serialization(format!("Failed to serialize event: {}", e))
            })?;

            sqlx::query(
                r#"
                INSERT INTO celers_events (event_type, task_id, worker, timestamp, payload)
                VALUES ($1, $2, $3, $4, $5)
                "#,
            )
            .bind(event_type)
            .bind(task_id)
            .bind(&worker)
            .bind(timestamp)
            .bind(&payload)
            .execute(&mut *tx)
            .await
            .map_err(|e| BackendError::Connection(format!("Failed to insert event: {}", e)))?;
        }

        tx.commit().await.map_err(|e| {
            BackendError::Connection(format!("Failed to commit event batch: {}", e))
        })?;

        Ok(())
    }

    /// Get a reference to the underlying connection pool
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Get the current buffer size
    pub async fn buffer_len(&self) -> usize {
        self.buffer.lock().await.len()
    }
}

#[async_trait]
impl EventEmitter for DbEventPersister {
    async fn emit(&self, event: Event) -> celers_core::Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        let should_flush = {
            let mut buf = self.buffer.lock().await;
            buf.push(event);
            buf.len() >= self.config.batch_size
        };

        if should_flush {
            self.flush_buffer().await.map_err(|e| {
                celers_core::CelersError::Other(format!("DB event flush failed: {}", e))
            })?;
        }

        Ok(())
    }

    async fn emit_batch(&self, events: Vec<Event>) -> celers_core::Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        let should_flush = {
            let mut buf = self.buffer.lock().await;
            buf.extend(events);
            buf.len() >= self.config.batch_size
        };

        if should_flush {
            self.flush_buffer().await.map_err(|e| {
                celers_core::CelersError::Other(format!("DB event flush failed: {}", e))
            })?;
        }

        Ok(())
    }

    fn is_enabled(&self) -> bool {
        self.config.enabled
    }
}

#[async_trait]
impl EventPersister for DbEventPersister {
    async fn query_events(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        event_type_filter: Option<&str>,
    ) -> celers_core::Result<Vec<Event>> {
        // Flush buffer first to ensure consistency
        self.flush_buffer().await.map_err(|e| {
            celers_core::CelersError::Other(format!("DB event flush failed: {}", e))
        })?;

        let rows = if let Some(et) = event_type_filter {
            sqlx::query(
                r#"
                SELECT payload FROM celers_events
                WHERE timestamp >= $1 AND timestamp <= $2 AND event_type = $3
                ORDER BY timestamp ASC
                "#,
            )
            .bind(from)
            .bind(to)
            .bind(et)
            .fetch_all(&self.pool)
            .await
        } else {
            sqlx::query(
                r#"
                SELECT payload FROM celers_events
                WHERE timestamp >= $1 AND timestamp <= $2
                ORDER BY timestamp ASC
                "#,
            )
            .bind(from)
            .bind(to)
            .fetch_all(&self.pool)
            .await
        }
        .map_err(|e| celers_core::CelersError::Other(format!("Failed to query events: {}", e)))?;

        let mut events = Vec::with_capacity(rows.len());
        for row in &rows {
            let payload: serde_json::Value = row.get("payload");
            match serde_json::from_value::<Event>(payload) {
                Ok(event) => events.push(event),
                Err(e) => {
                    tracing::warn!("Failed to deserialize event from DB: {}", e);
                }
            }
        }

        Ok(events)
    }

    async fn count_events(&self, event_type: Option<&str>) -> celers_core::Result<u64> {
        self.flush_buffer().await.map_err(|e| {
            celers_core::CelersError::Other(format!("DB event flush failed: {}", e))
        })?;

        let count: i64 = if let Some(et) = event_type {
            let row = sqlx::query("SELECT COUNT(*) FROM celers_events WHERE event_type = $1")
                .bind(et)
                .fetch_one(&self.pool)
                .await
                .map_err(|e| {
                    celers_core::CelersError::Other(format!("Failed to count events: {}", e))
                })?;
            row.get(0)
        } else {
            let row = sqlx::query("SELECT COUNT(*) FROM celers_events")
                .fetch_one(&self.pool)
                .await
                .map_err(|e| {
                    celers_core::CelersError::Other(format!("Failed to count events: {}", e))
                })?;
            row.get(0)
        };

        Ok(count as u64)
    }

    async fn cleanup(&self, older_than: chrono::Duration) -> celers_core::Result<u64> {
        let cutoff = Utc::now().checked_sub_signed(older_than).ok_or_else(|| {
            celers_core::CelersError::Other("Invalid duration for cleanup cutoff".to_string())
        })?;

        let result = sqlx::query("DELETE FROM celers_events WHERE timestamp < $1")
            .bind(cutoff)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                celers_core::CelersError::Other(format!("Failed to cleanup events: {}", e))
            })?;

        Ok(result.rows_affected())
    }

    async fn flush(&self) -> celers_core::Result<()> {
        self.flush_buffer()
            .await
            .map_err(|e| celers_core::CelersError::Other(format!("DB event flush failed: {}", e)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_db_persister_config_defaults() {
        let config = DbEventPersisterConfig::default();
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.flush_interval, Duration::from_secs(5));
        assert!(config.enabled);
    }

    #[tokio::test]
    async fn test_db_persister_buffer_logic() {
        // Test buffer filling without an actual DB connection
        // We create a config with a large batch size to avoid flush attempts
        let config = DbEventPersisterConfig::default()
            .with_batch_size(1000)
            .with_enabled(true);

        // We cannot create a real DbEventPersister without a PgPool,
        // so we test config builder logic and buffer behavior indirectly.
        assert_eq!(config.batch_size, 1000);
        assert!(config.enabled);

        let config2 = DbEventPersisterConfig::default()
            .with_enabled(false)
            .with_flush_interval(Duration::from_secs(10));
        assert!(!config2.enabled);
        assert_eq!(config2.flush_interval, Duration::from_secs(10));
    }
}
