//! Database-backed event persistence for CeleRS
//!
//! Provides `DbEventPersister` which stores events in a PostgreSQL table
//! with buffered inserts and SQL-based querying/cleanup.
//!
//! This whole module is gated behind the `postgres` Cargo feature (see its
//! `#[cfg(feature = "postgres")]` declaration in `lib.rs`) since
//! `DbEventPersister` is hardcoded to `oxisql_postgres::PgConnection`.

use async_trait::async_trait;
use celers_core::event::{Event, EventEmitter};
use celers_core::event_persistence::EventPersister;
use chrono::{DateTime, Utc};
use oxisql_core::Connection;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;

use crate::row_ext::{json_from_row, json_param, RowExt};
use crate::{BackendError, Result};

/// Configuration for [`DbEventPersister`]
#[derive(Debug, Clone)]
pub struct DbEventPersisterConfig {
    /// Number of events to buffer before flushing to the database
    pub batch_size: usize,
    /// Maximum time between flushes. A background task honours this by
    /// calling `flush_buffer` on every tick (see [`DbEventPersister::new`]);
    /// set to `Duration::ZERO` to disable the background flush task
    /// entirely (flushes then only happen at `batch_size` or on an explicit
    /// `flush()`/`query_events()`/`count_events()` call).
    pub flush_interval: Duration,
    /// Whether event persistence is enabled
    pub enabled: bool,
    /// Upper bound on how many events the in-memory buffer may hold after a
    /// failed flush re-queues them. If re-queuing would exceed this, the
    /// OLDEST events are dropped (logged via `tracing::error!`) rather than
    /// growing the buffer unboundedly during a prolonged DB outage.
    pub max_buffer_events: usize,
    /// Upper bound on the number of rows [`DbEventPersister::query_events`]
    /// (via [`EventPersister::query_events`]) will return in one call, to
    /// bound memory on a caller-supplied wide `from`/`to` range. A truncated
    /// result is logged via `tracing::warn!`.
    pub max_query_events: usize,
}

impl Default for DbEventPersisterConfig {
    fn default() -> Self {
        Self {
            batch_size: 100,
            flush_interval: Duration::from_secs(5),
            enabled: true,
            max_buffer_events: 10_000,
            max_query_events: 100_000,
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

    /// Set the maximum buffered-event count retained after a failed flush.
    #[must_use]
    pub fn with_max_buffer_events(mut self, max: usize) -> Self {
        self.max_buffer_events = max;
        self
    }

    /// Set the maximum row count returned by `query_events` in one call.
    #[must_use]
    pub fn with_max_query_events(mut self, max: usize) -> Self {
        self.max_query_events = max;
        self
    }
}

/// Merge a failed flush's drained batch back into the live buffer, then
/// enforce `max_buffer_events` by dropping the OLDEST entries first if the
/// combined size exceeds it.
///
/// `failed_batch` (older events, drained from the buffer before the flush
/// attempt) is placed BEFORE whatever `buffer` accumulated concurrently
/// (newer events, pushed by `emit`/`emit_batch` while the flush was in
/// flight), preserving chronological order. Returns the number of events
/// dropped (0 if the combined size was within bounds).
///
/// A pure, DB-free function so the core of the "don't silently lose events
/// on a failed flush" fix is deterministically unit-testable without a live
/// database connection.
fn requeue_after_failed_flush(
    buffer: &mut Vec<Event>,
    failed_batch: Vec<Event>,
    max_buffer_events: usize,
) -> usize {
    let mut merged = failed_batch;
    merged.append(buffer); // moves buffer's current (newer) contents onto the end; buffer is now empty
    let dropped = merged.len().saturating_sub(max_buffer_events);
    if dropped > 0 {
        merged.drain(0..dropped);
    }
    *buffer = merged;
    dropped
}

/// The event insert.
///
/// `payload` is a `JSONB` column, so `$5` must go through `::text::jsonb`: a
/// bare `$5` makes PostgreSQL infer the parameter's type as `jsonb`, after
/// which the client sends JSON text where the binary jsonb version byte
/// belongs and the server rejects it with `unsupported jsonb version number`.
/// Hoisted to a const so `tests::event_insert_casts_the_jsonb_payload_from_text`
/// can hold that line without a live database. See `row_ext::json_param`.
const SQL_INSERT_EVENT: &str = r#"
            INSERT INTO celers_events (event_type, task_id, worker, timestamp, payload)
            VALUES ($1, $2::text::uuid, $3, $4::text::timestamptz, $5::text::jsonb)
            "#;

/// Insert `events` in one transaction. Does not touch the in-memory buffer —
/// callers are responsible for draining/re-queuing around this call.
async fn try_flush_events(conn: &oxisql_postgres::PgConnection, events: &[Event]) -> Result<()> {
    if events.is_empty() {
        return Ok(());
    }

    let mut tx = conn
        .transaction()
        .await
        .map_err(|e| BackendError::Connection(format!("Failed to begin transaction: {}", e)))?;

    for event in events {
        let event_type = event.event_type();
        let task_id_param = event.task_id().map(|id| id.to_string());
        let worker = event.hostname().map(|s| s.to_string());
        // See row_ext.rs's "DateTime<Utc> parameter convention (PostgreSQL)"
        // section: bind the RFC3339 string, cast server-side via `::text::timestamptz`.
        let timestamp_param = event.timestamp().to_rfc3339();
        let payload = serde_json::to_value(event).map_err(|e| {
            BackendError::Serialization(format!("Failed to serialize event: {}", e))
        })?;

        tx.execute(
            SQL_INSERT_EVENT,
            &[
                &event_type,
                &task_id_param,
                &worker,
                &timestamp_param,
                &json_param(&payload),
            ],
        )
        .await
        .map_err(|e| BackendError::Connection(format!("Failed to insert event: {}", e)))?;
    }

    tx.commit()
        .await
        .map_err(|e| BackendError::Connection(format!("Failed to commit event batch: {}", e)))?;

    Ok(())
}

/// Delete events with `timestamp < now() - older_than`. Shared by both
/// [`EventPersister::cleanup`] and [`DbEventPersister::spawn_periodic_cleanup`]'s
/// background task, taking the connection by reference/handle rather than
/// `&self` for the same reason as [`flush_buffer_impl`]: both an instance
/// method and a detached `tokio::spawn` task need to call it without
/// requiring `DbEventPersister: Clone`.
async fn cleanup_events_impl(
    conn: &oxisql_postgres::PgConnection,
    older_than: chrono::Duration,
) -> Result<u64> {
    let cutoff = Utc::now().checked_sub_signed(older_than).ok_or_else(|| {
        BackendError::Serialization("Invalid duration for cleanup cutoff".to_string())
    })?;
    let cutoff_param = cutoff.to_rfc3339();

    let rows_affected = conn
        .execute(
            "DELETE FROM celers_events WHERE timestamp < $1::text::timestamptz",
            &[&cutoff_param],
        )
        .await
        .map_err(|e| BackendError::Connection(format!("Failed to cleanup events: {}", e)))?;

    Ok(rows_affected)
}

/// Drain-and-flush-or-requeue: the shared logic behind both
/// [`DbEventPersister::flush_buffer`] and the periodic background flush
/// task, taking its dependencies by reference/handle rather than `&self` so
/// both call sites (an instance method and a detached `tokio::spawn` task)
/// can share it without requiring `DbEventPersister: Clone`.
async fn flush_buffer_impl(
    conn: &oxisql_postgres::PgConnection,
    buffer: &Mutex<Vec<Event>>,
    max_buffer_events: usize,
) -> Result<()> {
    let events = {
        let mut buf = buffer.lock().await;
        if buf.is_empty() {
            return Ok(());
        }
        std::mem::take(&mut *buf)
    };

    if let Err(e) = try_flush_events(conn, &events).await {
        // Do NOT drop the events on a failed flush (DB restart, connection
        // drop, deadlock, disk-full, ...) — re-queue them so they are
        // retried on the next flush, bounded so a prolonged outage cannot
        // grow the buffer without limit.
        let mut buf = buffer.lock().await;
        let dropped = requeue_after_failed_flush(&mut buf, events, max_buffer_events);
        if dropped > 0 {
            tracing::error!(
                dropped_events = dropped,
                max_buffer_events,
                "DbEventPersister: buffer exceeded max_buffer_events after a failed flush; oldest events dropped"
            );
        }
        return Err(e);
    }

    Ok(())
}

/// Database-backed event persister
///
/// Buffers events in memory and flushes them to PostgreSQL in batches.
/// The `celers_events` table is created via the [`migrate`](DbEventPersister::migrate) method.
///
/// Honors `config.flush_interval` via a background task spawned in
/// [`DbEventPersister::new`] (unless `flush_interval` is zero), so events sit
/// in the buffer for at most `flush_interval` even below `batch_size` — call
/// [`DbEventPersister::shutdown`] to stop that task and perform one final
/// flush (e.g. on process shutdown, so buffered events are not lost).
pub struct DbEventPersister {
    conn: oxisql_postgres::PgConnection,
    config: DbEventPersisterConfig,
    buffer: Arc<Mutex<Vec<Event>>>,
    /// Handle to the periodic background flush task, if `flush_interval` is
    /// non-zero. `None` after [`DbEventPersister::shutdown`] has been called
    /// (or if `flush_interval` was zero at construction).
    flush_task: Option<tokio::task::JoinHandle<()>>,
}

impl DbEventPersister {
    /// Create a new database event persister.
    ///
    /// If `config.flush_interval` is non-zero, spawns a background task that
    /// flushes the buffer on every tick, so events do not sit unwritten for
    /// up to `flush_interval` — before this, the field existed on
    /// [`DbEventPersisterConfig`] but nothing ever read it, and events only
    /// ever flushed at the `batch_size` threshold or on an explicit
    /// `flush()`/`query_events()`/`count_events()` call.
    pub async fn new(
        conn: oxisql_postgres::PgConnection,
        config: DbEventPersisterConfig,
    ) -> Result<Self> {
        let buffer = Arc::new(Mutex::new(Vec::new()));

        let flush_task = if config.enabled && !config.flush_interval.is_zero() {
            let task_conn = conn.clone();
            let task_buffer = Arc::clone(&buffer);
            let interval = config.flush_interval;
            let max_buffer_events = config.max_buffer_events;
            Some(tokio::spawn(async move {
                let mut ticker = tokio::time::interval(interval);
                // The first tick fires immediately; skip it so the first
                // background flush happens after one full `interval`, not
                // at task-spawn time (the `batch_size` threshold in `emit`
                // already covers the "flush right away" case).
                ticker.tick().await;
                loop {
                    ticker.tick().await;
                    if let Err(e) =
                        flush_buffer_impl(&task_conn, &task_buffer, max_buffer_events).await
                    {
                        tracing::error!(error = %e, "DbEventPersister: periodic background flush failed");
                    }
                }
            }))
        } else {
            None
        };

        Ok(Self {
            conn,
            config,
            buffer,
            flush_task,
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

        // `execute_batch` (simple-query protocol) is required here, same as
        // `PostgresResultBackend::migrate` in lib.rs — this SQL text has
        // multiple `;`-separated statements, which the extended/prepared-
        // statement protocol `execute`/`query` use rejects.
        //
        // Wrapped in the shared migration advisory lock for the same reason:
        // concurrent auto-migration would otherwise race on the catalog. See
        // `pg_ddl`.
        let sql = crate::pg_ddl::advisory_locked_migration(sql);
        self.conn.execute_batch(&sql).await.map_err(|e| {
            BackendError::Connection(format!("Failed to run event migrations: {}", e))
        })?;

        Ok(())
    }

    /// Flush the in-memory buffer to the database.
    ///
    /// On failure, the drained events are re-queued into the buffer (see
    /// [`requeue_after_failed_flush`]) rather than dropped, bounded by
    /// `config.max_buffer_events`.
    async fn flush_buffer(&self) -> Result<()> {
        flush_buffer_impl(&self.conn, &self.buffer, self.config.max_buffer_events).await
    }

    /// Stop the periodic background flush task (if running) and perform one
    /// final flush of whatever remains buffered.
    ///
    /// Call this on graceful shutdown so buffered-but-not-yet-flushed events
    /// (up to `batch_size - 1` of them, or more if recent flushes have been
    /// failing) are not silently lost when the process exits.
    pub async fn shutdown(&mut self) -> Result<()> {
        if let Some(handle) = self.flush_task.take() {
            handle.abort();
        }
        self.flush_buffer().await
    }

    /// Get a reference to the underlying connection
    pub fn connection(&self) -> &oxisql_postgres::PgConnection {
        &self.conn
    }

    /// Get the current buffer size
    pub async fn buffer_len(&self) -> usize {
        self.buffer.lock().await.len()
    }

    /// Spawn a background task that calls [`EventPersister::cleanup`]
    /// (deleting events with `timestamp` older than `retention`) on
    /// `interval` forever, logging (rather than propagating) any error so
    /// one failed cleanup pass never kills the scheduler.
    ///
    /// Purely opt-in: nothing calls this automatically, so `celers_events`
    /// otherwise grows without bound (the same omission `flush_interval` had
    /// before [`DbEventPersister::new`] started honouring it — see its doc
    /// comment). Wire this in from application start-up if periodic
    /// `celers_events` retention is desired; mirrors
    /// `PostgresResultBackend::spawn_periodic_cleanup` and
    /// `DbLockBackend::spawn_periodic_cleanup` in the sibling `lib.rs`/
    /// `lock.rs` modules.
    pub fn spawn_periodic_cleanup(
        &self,
        interval: Duration,
        retention: chrono::Duration,
    ) -> tokio::task::JoinHandle<()> {
        let conn = self.conn.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            // The first tick fires immediately; skip it so the first real
            // cleanup happens after one full `interval`, not at t=0.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                match cleanup_events_impl(&conn, retention).await {
                    Ok(n) if n > 0 => tracing::debug!(deleted = n, "cleaned up expired events"),
                    Ok(_) => {}
                    Err(e) => tracing::error!(error = %e, "periodic event cleanup failed"),
                }
            }
        })
    }
}

impl Drop for DbEventPersister {
    fn drop(&mut self) {
        // Best-effort: stop the background task so it does not keep running
        // (and keep the connection/buffer alive) after this persister is
        // dropped. Cannot `.await` a final flush here (Drop is sync) — call
        // `shutdown()` explicitly before dropping when a final flush matters.
        if let Some(handle) = self.flush_task.take() {
            handle.abort();
        }
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

        // See row_ext.rs's DateTime<Utc> parameter convention (PostgreSQL):
        // RFC3339 string bound + `::text::timestamptz` cast at every
        // placeholder targeting a `TIMESTAMPTZ` column.
        let from_param = from.to_rfc3339();
        let to_param = to.to_rfc3339();
        // Bounded by `max_query_events` (a `usize` config value, not
        // user-controlled input, so direct interpolation carries no
        // injection risk — same discipline as the `INTERVAL {secs} SECOND`
        // interpolation in analytics.rs) so a caller passing a wide
        // `from`/`to` range cannot OOM the process by materialising an
        // unbounded `Vec<Event>`.
        let limit = self.config.max_query_events;
        let rows = if let Some(et) = event_type_filter {
            self.conn
                .query(
                    &format!(
                        r#"
                        -- `payload` is JSONB and is read by name, so it is
                        -- selected as `::text AS payload`: oxisql-postgres
                        -- reads JSON/JSONB by asking tokio-postgres for a
                        -- String, which it only implements for text-ish
                        -- types, so an uncast JSONB column fails in the
                        -- driver. See row_ext::json_from_row.
                        SELECT payload::text AS payload FROM celers_events
                        WHERE timestamp >= $1::text::timestamptz AND timestamp <= $2::text::timestamptz AND event_type = $3
                        ORDER BY timestamp ASC
                        LIMIT {limit}
                        "#
                    ),
                    &[&from_param, &to_param, &et],
                )
                .await
        } else {
            self.conn
                .query(
                    &format!(
                        r#"
                        -- `payload` is JSONB and is read by name, so it is
                        -- selected as `::text AS payload`: oxisql-postgres
                        -- reads JSON/JSONB by asking tokio-postgres for a
                        -- String, which it only implements for text-ish
                        -- types, so an uncast JSONB column fails in the
                        -- driver. See row_ext::json_from_row.
                        SELECT payload::text AS payload FROM celers_events
                        WHERE timestamp >= $1::text::timestamptz AND timestamp <= $2::text::timestamptz
                        ORDER BY timestamp ASC
                        LIMIT {limit}
                        "#
                    ),
                    &[&from_param, &to_param],
                )
                .await
        }
        .map_err(|e| celers_core::CelersError::Other(format!("Failed to query events: {}", e)))?;

        if rows.len() >= limit {
            tracing::warn!(
                limit,
                from = %from,
                to = %to,
                "DbEventPersister::query_events: result truncated at max_query_events; \
                 narrow the from/to range or event_type filter to see the rest"
            );
        }

        let mut events = Vec::with_capacity(rows.len());
        for row in &rows {
            let payload = json_from_row(row, "payload").map_err(|e| {
                celers_core::CelersError::Other(format!("Failed to query events: {e}"))
            })?;
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
            let rows = self
                .conn
                .query(
                    "SELECT COUNT(*) FROM celers_events WHERE event_type = $1",
                    &[&et],
                )
                .await
                .map_err(|e| {
                    celers_core::CelersError::Other(format!("Failed to count events: {}", e))
                })?;
            let row = rows.into_iter().next().ok_or_else(|| {
                celers_core::CelersError::Other("count_events query returned no rows".to_string())
            })?;
            row.col_idx(0).map_err(|e| {
                celers_core::CelersError::Other(format!("Failed to count events: {e}"))
            })?
        } else {
            let rows = self
                .conn
                .query("SELECT COUNT(*) FROM celers_events", &[])
                .await
                .map_err(|e| {
                    celers_core::CelersError::Other(format!("Failed to count events: {}", e))
                })?;
            let row = rows.into_iter().next().ok_or_else(|| {
                celers_core::CelersError::Other("count_events query returned no rows".to_string())
            })?;
            row.col_idx(0).map_err(|e| {
                celers_core::CelersError::Other(format!("Failed to count events: {e}"))
            })?
        };

        Ok(count as u64)
    }

    async fn cleanup(&self, older_than: chrono::Duration) -> celers_core::Result<u64> {
        cleanup_events_impl(&self.conn, older_than)
            .await
            .map_err(|e| celers_core::CelersError::Other(e.to_string()))
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
    use celers_core::event::TaskEventBuilder;
    use uuid::Uuid;

    /// Build a distinguishable `task-received` event for a fresh random
    /// task id -- distinct `Uuid`s serve as the identity marker for the
    /// ordering assertions below (there is no generic free-text field on
    /// `Event` to tag with a label).
    fn sample_event() -> Event {
        TaskEventBuilder::new(Uuid::new_v4(), "test_task")
            .hostname("host-1")
            .received()
    }

    /// `celers_events.payload` is a `JSONB` column, and `json_param` binds
    /// JSON *text*. Without the `::text::jsonb` cast PostgreSQL infers the
    /// parameter as `jsonb` and the client sends the text in the binary jsonb
    /// encoding, so the server rejects every event insert with
    /// `unsupported jsonb version number`. Event persistence would fail
    /// wholesale — and only against a live server, which is why this guard
    /// runs without one. Sibling of
    /// `postgres_backend::tests::every_jsonb_parameter_is_cast_from_text`.
    #[test]
    fn event_insert_casts_the_jsonb_payload_from_text() {
        assert!(
            SQL_INSERT_EVENT.contains("$5::text::jsonb"),
            "payload ($5) must be bound as `$5::text::jsonb`, not a bare `$5`. \
             Statement:\n{SQL_INSERT_EVENT}"
        );
        assert!(
            SQL_INSERT_EVENT.contains("timestamp, payload)"),
            "payload must stay the 5th column, or $5 is no longer the JSONB \
             parameter. Statement:\n{SQL_INSERT_EVENT}"
        );
    }

    #[test]
    fn test_db_persister_config_defaults() {
        let config = DbEventPersisterConfig::default();
        assert_eq!(config.batch_size, 100);
        assert_eq!(config.flush_interval, Duration::from_secs(5));
        assert!(config.enabled);
        assert_eq!(config.max_buffer_events, 10_000);
        assert_eq!(config.max_query_events, 100_000);
    }

    #[tokio::test]
    async fn test_db_persister_buffer_logic() {
        // Test buffer filling without an actual DB connection
        // We create a config with a large batch size to avoid flush attempts
        let config = DbEventPersisterConfig::default()
            .with_batch_size(1000)
            .with_enabled(true);

        // We cannot create a real DbEventPersister without a live PgConnection,
        // so we test config builder logic and buffer behavior indirectly.
        assert_eq!(config.batch_size, 1000);
        assert!(config.enabled);

        let config2 = DbEventPersisterConfig::default()
            .with_enabled(false)
            .with_flush_interval(Duration::from_secs(10))
            .with_max_buffer_events(500)
            .with_max_query_events(1000);
        assert!(!config2.enabled);
        assert_eq!(config2.flush_interval, Duration::from_secs(10));
        assert_eq!(config2.max_buffer_events, 500);
        assert_eq!(config2.max_query_events, 1000);
    }

    // ── requeue_after_failed_flush: the core of the "don't lose events on a
    // failed flush" fix, fully deterministic and DB-free ─────────────────

    #[test]
    fn requeue_puts_failed_batch_back_when_buffer_was_empty() {
        let mut buffer = Vec::new();
        let failed = vec![sample_event(), sample_event()];
        let dropped = requeue_after_failed_flush(&mut buffer, failed, 100);
        assert_eq!(dropped, 0);
        assert_eq!(buffer.len(), 2);
    }

    #[test]
    fn requeue_preserves_order_failed_batch_before_newly_buffered() {
        // Events accumulated in `buffer` WHILE the flush was in flight are
        // newer than the failed (already-drained) batch, so they must come
        // AFTER it once merged back. Distinct random task_ids on each
        // constructed event serve as identity markers for order comparison.
        let newer1 = sample_event();
        let newer2 = sample_event();
        let older1 = sample_event();
        let older2 = sample_event();
        let expected_order = vec![
            older1.task_id(),
            older2.task_id(),
            newer1.task_id(),
            newer2.task_id(),
        ];

        let mut buffer = vec![newer1, newer2];
        let failed = vec![older1, older2];
        requeue_after_failed_flush(&mut buffer, failed, 100);

        let actual_order: Vec<_> = buffer.iter().map(|e| e.task_id()).collect();
        assert_eq!(actual_order, expected_order);
    }

    #[test]
    fn requeue_drops_oldest_when_exceeding_max_buffer_events() {
        let newer = sample_event();
        let older1 = sample_event();
        let older2 = sample_event();
        let older3 = sample_event();
        let expected_survivors = vec![older3.task_id(), newer.task_id()];

        let mut buffer = vec![newer];
        let failed = vec![older1, older2, older3];
        // Combined size is 4; cap at 2 -> must drop the 2 OLDEST (older1, older2).
        let dropped = requeue_after_failed_flush(&mut buffer, failed, 2);
        assert_eq!(dropped, 2);
        assert_eq!(buffer.len(), 2);
        let survivors: Vec<_> = buffer.iter().map(|e| e.task_id()).collect();
        assert_eq!(survivors, expected_survivors);
    }

    #[test]
    fn requeue_never_drops_when_within_bounds() {
        let mut buffer = Vec::new();
        let failed: Vec<Event> = (0..50).map(|_| sample_event()).collect();
        let dropped = requeue_after_failed_flush(&mut buffer, failed, 50);
        assert_eq!(dropped, 0);
        assert_eq!(buffer.len(), 50);
    }

    #[test]
    fn requeue_handles_zero_cap_by_dropping_everything() {
        let mut buffer = Vec::new();
        let failed = vec![sample_event(), sample_event()];
        let dropped = requeue_after_failed_flush(&mut buffer, failed, 0);
        assert_eq!(dropped, 2);
        assert!(buffer.is_empty());
    }

    // ── Background flush ticker mechanics (deterministic via paused time) ──

    #[tokio::test(start_paused = true)]
    async fn periodic_ticker_pattern_waits_one_full_interval_between_ticks() {
        // Verifies the exact ticker pattern `DbEventPersister::new` uses for
        // its background flush task (an initial skipped tick, then one tick
        // per `interval`) actually waits a full `interval` between ticks —
        // deterministic under Tokio's paused virtual clock (no real
        // sleeping, no DB needed). `#[tokio::test(start_paused = true)]`
        // auto-advances the virtual clock to the next timer deadline
        // whenever the test task is idle waiting on one, so awaiting
        // `ticker.tick()` directly (no `tokio::spawn`/manual
        // `time::advance()` needed) resolves instantly in wall-clock time
        // while `Instant::now()` still reports the advanced virtual time.
        // The full `DbEventPersister` background task additionally needs a
        // live Postgres connection to actually flush, covered by the
        // `#[ignore]`d live-DB tests instead.
        let interval_dur = Duration::from_secs(5);
        let mut ticker = tokio::time::interval(interval_dur);
        ticker.tick().await; // skip immediate first tick, matching flush_task's own loop

        let start = tokio::time::Instant::now();
        ticker.tick().await;
        assert_eq!(tokio::time::Instant::now() - start, interval_dur);

        let start2 = tokio::time::Instant::now();
        ticker.tick().await;
        assert_eq!(tokio::time::Instant::now() - start2, interval_dur);
    }
}
