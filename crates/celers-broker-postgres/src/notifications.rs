//! LISTEN/NOTIFY support for real-time task event notifications

use celers_core::{CelersError, Result};
use std::time::Duration;

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
    listener: sqlx::postgres::PgListener,
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
        use tokio::time::timeout as tokio_timeout;

        match tokio_timeout(timeout, self.listener.recv()).await {
            Ok(Ok(notification)) => {
                let payload: TaskNotification = serde_json::from_str(notification.payload())
                    .map_err(|e| {
                        CelersError::Other(format!("Failed to parse notification: {}", e))
                    })?;
                Ok(Some(payload))
            }
            Ok(Err(e)) => Err(CelersError::Other(format!("Listener error: {}", e))),
            Err(_) => Ok(None), // Timeout
        }
    }

    /// Try to receive a notification without blocking
    ///
    /// Returns immediately with either a notification or None.
    pub async fn try_recv_notification(&mut self) -> Result<Option<TaskNotification>> {
        match self.listener.try_recv().await {
            Ok(Some(notification)) => {
                let payload: TaskNotification = serde_json::from_str(notification.payload())
                    .map_err(|e| {
                        CelersError::Other(format!("Failed to parse notification: {}", e))
                    })?;
                Ok(Some(payload))
            }
            Ok(None) => Ok(None),
            Err(e) => Err(CelersError::Other(format!("Listener error: {}", e))),
        }
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
        let mut listener = sqlx::postgres::PgListener::connect_with(&self.pool)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to create listener: {}", e)))?;

        listener.listen(&channel).await.map_err(|e| {
            CelersError::Other(format!("Failed to listen on channel {}: {}", channel, e))
        })?;

        tracing::info!(channel = %channel, "Created task notification listener");

        Ok(TaskNotificationListener { listener, channel })
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
            // Create trigger function if it doesn't exist
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
                        'queue_name', NEW.metadata->>'queue',
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

            sqlx::query(&function_sql)
                .execute(&self.pool)
                .await
                .map_err(|e| {
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

            sqlx::query(trigger_sql)
                .execute(&self.pool)
                .await
                .map_err(|e| {
                    CelersError::Other(format!("Failed to create notification trigger: {}", e))
                })?;

            tracing::info!(channel = %channel, "Enabled task notifications");
        } else {
            // Drop trigger
            let drop_sql = r#"
                DROP TRIGGER IF EXISTS trigger_notify_task_enqueued ON celers_tasks;
                "#;

            sqlx::query(drop_sql)
                .execute(&self.pool)
                .await
                .map_err(|e| {
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
        let exists: bool = sqlx::query_scalar(
            r#"
            SELECT EXISTS (
                SELECT 1
                FROM pg_trigger
                WHERE tgname = 'trigger_notify_task_enqueued'
                  AND tgrelid = 'celers_tasks'::regclass
            )
            "#,
        )
        .fetch_one(&self.pool)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to check notification status: {}", e)))?;

        Ok(exists)
    }
}
