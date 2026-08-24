//! LISTEN/NOTIFY support for real-time task event notifications

use celers_core::{CelersError, Result};
use oxisql_postgres::{NotificationStream, PgConnection};
use std::time::Duration;

use crate::tls_mode;
use crate::types::TaskNotification;
use crate::PostgresBroker;

// ========== LISTEN/NOTIFY Support ==========

/// Task notification listener for real-time task events
///
/// This allows workers to be notified immediately when new tasks are enqueued,
/// reducing polling overhead. Uses PostgreSQL's LISTEN/NOTIFY mechanism.
///
/// # Example
///
/// ```no_run
/// use celers_broker_postgres::PostgresBroker;
/// use std::time::Duration;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let broker = PostgresBroker::new("postgres://localhost/db").await?;
/// broker.migrate().await?;
///
/// // Create a listener on a separate task
/// let mut listener = broker.create_notification_listener().await?;
///
/// // Enable notifications (sends NOTIFY after each enqueue)
/// broker.enable_notifications(true).await?;
///
/// // Wait for notifications
/// tokio::spawn(async move {
///     loop {
///         match listener.wait_for_notification(Duration::from_secs(30)).await {
///             Ok(Some(notification)) => {
///                 println!("New task enqueued: {:?}", notification);
///                 // Dequeue and process task
///             }
///             Ok(None) => {
///                 println!("Timeout, no notification received");
///             }
///             Err(e) => {
///                 eprintln!("Listener error: {}", e);
///                 break;
///             }
///         }
///     }
/// });
/// # Ok(())
/// # }
/// ```
pub struct TaskNotificationListener {
    /// The dedicated `PgConnection` this listener owns.
    ///
    /// Kept alive for the listener's whole lifetime (dropping it would tear
    /// down the underlying `tokio_postgres::Connection` driver task that
    /// forwards notifications into `stream`). Not read directly after
    /// construction — `stream` is what's actually polled — but must stay
    /// alive as long as `stream` does.
    _conn: PgConnection,
    /// The live subscription obtained from `PgConnection::listen`.
    ///
    /// `oxisql_postgres::notify::NotificationStream::recv_timeout` has a
    /// built-in timeout (it loops internally against a deadline computed
    /// from the passed `Duration`), so the manual `tokio::time::timeout`
    /// wrapper the pre-migration `sqlx::postgres::PgListener`-based version
    /// needed is dropped here — it would be redundant double-timeout logic.
    stream: NotificationStream,
    channel: String,
}

impl TaskNotificationListener {
    /// Wait for a task notification with timeout
    ///
    /// Returns:
    /// - `Ok(Some(notification))` if a notification was received
    /// - `Ok(None)` if timeout occurred
    /// - `Err(...)` if an error occurred
    pub async fn wait_for_notification(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<TaskNotification>> {
        // `recv_timeout` returns `None` on BOTH "timeout elapsed" and
        // "the broadcast channel was closed" (the underlying connection
        // driver task exited) — oxisql-postgres's `NotificationStream`
        // does not distinguish these two cases in its return type (see
        // `recv_timeout`'s doc comment in `oxisql-postgres/src/notify.rs`:
        // "Returns `Some(notification)` if a matching notification arrives
        // within the timeout, or `None` if the timeout elapses" — no
        // separate error variant for a closed channel). This means a
        // dropped/broken connection surfaces identically to a normal
        // timeout (`Ok(None)`) rather than as an `Err(..)` here, which is a
        // real (if narrow) observability gap versus the pre-migration
        // `sqlx::postgres::PgListener`-based version — that version's
        // `self.listener.recv()` returned a distinguishable `Err` from
        // `sqlx` on a lost connection, which this rewrite's manual
        // `tokio_timeout(...).await` mapped to `Err(CelersError::Other(...))`
        // via its `Ok(Err(e))` arm (see the old code's second match arm).
        // Flagged explicitly in the migration report; not silently
        // downgraded to a TODO comment because it changes what callers can
        // observe when the listener connection dies (a caller looping on
        // `Ok(None)` "just keep waiting" will now loop forever on a dead
        // connection instead of seeing an error break the loop, exactly the
        // pattern this struct's own doc example above uses:
        // `Err(e) => { eprintln!(...); break; }` never fires in that case).
        match self.stream.recv_timeout(timeout).await {
            Some(notification) => {
                let payload: TaskNotification = serde_json::from_str(&notification.payload)
                    .map_err(|e| {
                        CelersError::Other(format!("Failed to parse notification: {}", e))
                    })?;
                Ok(Some(payload))
            }
            None => Ok(None), // Timeout (or, per the caveat above, a closed connection)
        }
    }

    /// Try to receive a notification without blocking
    ///
    /// Returns immediately with either a notification or None.
    ///
    /// `oxisql_postgres::notify::NotificationStream` has no dedicated
    /// non-blocking `try_recv` (unlike `sqlx::postgres::PgListener`, which
    /// this method's pre-migration implementation called directly) — the
    /// closest equivalent is `recv_timeout` with a zero duration, which
    /// polls the underlying `tokio::sync::broadcast::Receiver` once and
    /// returns immediately if nothing is queued (see `recv_timeout`'s
    /// implementation: it computes a deadline `Instant::now() + timeout`
    /// and returns `None` the instant `remaining` reaches zero, which for
    /// `timeout = Duration::ZERO` is immediately on the first loop
    /// iteration after, at most, one non-blocking poll).
    pub async fn try_recv_notification(&mut self) -> Result<Option<TaskNotification>> {
        self.wait_for_notification(Duration::ZERO).await
    }

    /// Get the channel name this listener is subscribed to
    pub fn channel(&self) -> &str {
        &self.channel
    }
}

impl PostgresBroker {
    /// Create a notification listener for real-time task events
    ///
    /// The listener will receive notifications when tasks are enqueued.
    /// Call `enable_notifications(true)` to start sending notifications.
    ///
    /// Opens a DEDICATED `PgConnection` (via `PostgresBroker::database_url`)
    /// rather than reusing `PostgresBroker::conn` (the shared query
    /// connection) — a long-lived LISTEN connection should not share the
    /// query connection, per Postgres best practice, and this matches the
    /// pre-migration `sqlx::postgres::PgListener::connect_with(&self.pool)`
    /// behavior, which drew a fresh connection from the pool for the
    /// listener rather than reusing a specific already-checked-out one.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    /// use std::time::Duration;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let mut listener = broker.create_notification_listener().await?;
    ///
    /// // Wait for notifications
    /// while let Some(notification) = listener.wait_for_notification(Duration::from_secs(30)).await? {
    ///     println!("Task enqueued: {}", notification.task_id);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn create_notification_listener(&self) -> Result<TaskNotificationListener> {
        let channel = format!("celers_tasks_{}", self.queue_name);

        // See `broker_core.rs`'s constructors for how the TLS mode is
        // derived from `self.database_url`'s `sslmode` query parameter —
        // same helper, same rationale: this dedicated LISTEN connection
        // must not silently downgrade to plain-text when the broker's
        // connection URL explicitly requested TLS.
        let tls_mode = tls_mode::pg_tls_mode_for_url(&self.database_url).map_err(|e| {
            CelersError::Other(format!(
                "Failed to resolve TLS mode for LISTEN connection: {}",
                e
            ))
        })?;
        let conn = PgConnection::connect(&self.database_url, tls_mode)
            .await
            .map_err(|e| {
                CelersError::Other(format!(
                    "Failed to create dedicated LISTEN connection: {}",
                    e
                ))
            })?;

        let stream = conn.listen(&channel).await.map_err(|e| {
            CelersError::Other(format!("Failed to listen on channel {}: {}", channel, e))
        })?;

        tracing::info!(channel = %channel, "Created task notification listener");

        Ok(TaskNotificationListener {
            _conn: conn,
            stream,
            channel,
        })
    }

    /// Enable or disable NOTIFY on task enqueue
    ///
    /// When enabled, a PostgreSQL NOTIFY will be sent whenever a task is enqueued,
    /// allowing listeners to be notified immediately without polling.
    ///
    /// # Arguments
    /// * `enabled` - true to enable notifications, false to disable
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// broker.enable_notifications(true).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn enable_notifications(&self, enabled: bool) -> Result<()> {
        // Create or drop the trigger that sends NOTIFY
        let channel = format!("celers_tasks_{}", self.queue_name);

        if enabled {
            // Create trigger function if it doesn't exist.
            //
            // `channel` is interpolated into `pg_notify('{}', ...)` because
            // a NOTIFY channel name cannot be a bind parameter. It is built
            // from `self.queue_name`, which `PostgresBroker::with_pool_config`
            // validates at construction against `[A-Za-z0-9_-]{1,64}` — so it
            // cannot carry a quote, a semicolon or a comment marker into this
            // literal no matter where the caller sourced the label from.
            //
            // The DDL below goes through `execute_batch` (simple-query
            // protocol): the function body is dollar-quoted and the trigger
            // block is two `;`-separated statements, neither of which the
            // extended/prepared-statement path accepts.
            let function_sql = format!(
                r#"
                CREATE OR REPLACE FUNCTION notify_task_enqueued()
                RETURNS TRIGGER AS $$
                DECLARE
                    payload JSON;
                BEGIN
                    payload := json_build_object(
                        'task_id', NEW.id,
                        'task_name', NEW.task_name,
                        'queue_name', NEW.queue_name,
                        'priority', NEW.priority,
                        'enqueued_at', NEW.created_at
                    );
                    PERFORM pg_notify('{}', payload::text);
                    RETURN NEW;
                END;
                $$ LANGUAGE plpgsql;
                "#,
                channel
            );

            self.conn.execute_batch(&function_sql).await.map_err(|e| {
                CelersError::Other(format!("Failed to create notification function: {}", e))
            })?;

            // Create trigger
            let trigger_sql = r#"
                DROP TRIGGER IF EXISTS trigger_notify_task_enqueued ON celers_tasks;
                CREATE TRIGGER trigger_notify_task_enqueued
                    AFTER INSERT ON celers_tasks
                    FOR EACH ROW
                    EXECUTE FUNCTION notify_task_enqueued();
                "#;

            self.conn.execute_batch(trigger_sql).await.map_err(|e| {
                CelersError::Other(format!("Failed to create notification trigger: {}", e))
            })?;

            tracing::info!(channel = %channel, "Enabled task notifications");
        } else {
            // Drop trigger
            let drop_sql = r#"
                DROP TRIGGER IF EXISTS trigger_notify_task_enqueued ON celers_tasks;
                "#;

            self.conn.execute_batch(drop_sql).await.map_err(|e| {
                CelersError::Other(format!("Failed to disable notification trigger: {}", e))
            })?;

            tracing::info!(channel = %channel, "Disabled task notifications");
        }

        Ok(())
    }

    /// Check if notifications are enabled
    ///
    /// Returns true if the notification trigger exists, false otherwise.
    pub async fn notifications_enabled(&self) -> Result<bool> {
        use crate::row_ext::RowExt;

        let rows = self
            .conn
            .query(
                r#"
            SELECT EXISTS (
                SELECT 1
                FROM pg_trigger
                WHERE tgname = 'trigger_notify_task_enqueued'
                  AND tgrelid = 'celers_tasks'::regclass
            )
            "#,
                &[],
            )
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to check notification status: {}", e))
            })?;

        // .fetch_one (via query_scalar in the original): error if no row.
        let row = rows.into_iter().next().ok_or_else(|| {
            CelersError::Other("Failed to check notification status: no rows returned".to_string())
        })?;
        // Unaliased `EXISTS (...)` expression -> positional access, per the
        // RowExt convention documented in `row_ext.rs` (Postgres assigns the
        // implicit name `"exists"` to a bare `EXISTS(...)` subquery, which
        // IS well-defined and stable, but `col_idx` is used here to avoid
        // depending on that driver-assigned name at all, matching this
        // crate's convention of only using `col` for genuinely
        // aliased/named columns or single unadorned function calls).
        row.col_idx(0)
            .map_err(|e| CelersError::Other(format!("Failed to read exists: {}", e)))
    }
}
