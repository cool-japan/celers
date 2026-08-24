//! Alerting and statistics built on top of the event stream.
//!
//! Split out of [`crate::event`] to keep that module a manageable size. This
//! module holds the alert condition/severity model, the [`AlertManager`]
//! fan-out, and the [`EventMonitor`] statistics collector.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use tokio::sync::RwLock;

use super::{aggregate_failures, Event, TaskEvent, WorkerEvent};

/// Event alert condition for triggering notifications
#[derive(Clone)]
pub enum AlertCondition {
    /// Alert on specific event type
    EventType(String),
    /// Alert when task fails
    TaskFailed,
    /// Alert when task exceeds retry count
    TaskRetryExceeded(u32),
    /// Alert when worker goes offline
    WorkerOffline,
    /// Alert when event rate exceeds threshold (events per second)
    RateExceeds { threshold: f64, window_secs: u64 },
    /// Alert on custom condition
    Custom(Arc<dyn Fn(&Event) -> bool + Send + Sync>),
}

impl std::fmt::Debug for AlertCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlertCondition::EventType(event_type) => {
                f.debug_tuple("EventType").field(event_type).finish()
            }
            AlertCondition::TaskFailed => write!(f, "TaskFailed"),
            AlertCondition::TaskRetryExceeded(max) => {
                f.debug_tuple("TaskRetryExceeded").field(max).finish()
            }
            AlertCondition::WorkerOffline => write!(f, "WorkerOffline"),
            AlertCondition::RateExceeds {
                threshold,
                window_secs,
            } => f
                .debug_struct("RateExceeds")
                .field("threshold", threshold)
                .field("window_secs", window_secs)
                .finish(),
            AlertCondition::Custom(_) => write!(f, "Custom(<closure>)"),
        }
    }
}

impl AlertCondition {
    /// Check if an event triggers this alert condition
    #[must_use]
    pub fn check(&self, event: &Event, context: &AlertContext) -> bool {
        match self {
            AlertCondition::EventType(event_type) => event.event_type() == event_type,
            AlertCondition::TaskFailed => matches!(event, Event::Task(TaskEvent::Failed { .. })),
            AlertCondition::TaskRetryExceeded(max) => {
                if let Event::Task(TaskEvent::Retried { retries, .. }) = event {
                    retries >= max
                } else {
                    false
                }
            }
            AlertCondition::WorkerOffline => {
                matches!(event, Event::Worker(WorkerEvent::Offline { .. }))
            }
            AlertCondition::RateExceeds {
                threshold,
                window_secs,
            } => {
                let rate = context.get_event_rate(*window_secs);
                rate > *threshold
            }
            AlertCondition::Custom(predicate) => predicate(event),
        }
    }
}

/// Number of one-second buckets kept for event-rate calculation.
///
/// This is the longest window [`AlertContext::get_event_rate`] can measure;
/// longer windows are clamped to it.
pub const RATE_WINDOW_BUCKETS: u64 = 600;

/// Lock-free ring of per-second event counters.
///
/// Each slot holds the unix second it represents plus the number of events seen
/// in that second, so a read is an infallible sum of atomics: unlike a
/// lock-guarded buffer, it can never degrade to "0.0 because the lock was
/// contended" — which, for a rate *threshold*, would mean failing open exactly
/// when the rate is highest.
#[derive(Debug)]
struct RateWindow {
    /// Unix second each slot currently accounts for (`i64::MIN` when unused).
    seconds: Vec<std::sync::atomic::AtomicI64>,
    /// Event count for the second held in the matching slot.
    counts: Vec<std::sync::atomic::AtomicU64>,
}

impl Default for RateWindow {
    fn default() -> Self {
        let len = RATE_WINDOW_BUCKETS as usize;
        Self {
            seconds: (0..len)
                .map(|_| std::sync::atomic::AtomicI64::new(i64::MIN))
                .collect(),
            counts: (0..len)
                .map(|_| std::sync::atomic::AtomicU64::new(0))
                .collect(),
        }
    }
}

impl RateWindow {
    /// Record one event that happened at unix second `second`.
    fn record(&self, second: i64) {
        let idx = second.rem_euclid(RATE_WINDOW_BUCKETS as i64) as usize;
        let previous = self.seconds[idx].swap(second, std::sync::atomic::Ordering::AcqRel);
        if previous == second {
            self.counts[idx].fetch_add(1, std::sync::atomic::Ordering::AcqRel);
        } else {
            // The slot was holding a different (older) second: start over.
            self.counts[idx].store(1, std::sync::atomic::Ordering::Release);
        }
    }

    /// Count the events recorded in the `window_secs` seconds ending at `now`.
    fn count_since(&self, now: i64, window_secs: u64) -> u64 {
        let window = window_secs.min(RATE_WINDOW_BUCKETS);
        let oldest = now.saturating_sub(window.saturating_sub(1) as i64);
        let mut total = 0u64;
        for (second, count) in self.seconds.iter().zip(self.counts.iter()) {
            let slot_second = second.load(std::sync::atomic::Ordering::Acquire);
            if slot_second >= oldest && slot_second <= now {
                total = total.saturating_add(count.load(std::sync::atomic::Ordering::Acquire));
            }
        }
        total
    }
}

/// Context information for alert conditions
#[derive(Debug, Clone, Default)]
pub struct AlertContext {
    /// Lock-free per-second counters used for rate calculation.
    rate_window: Arc<RateWindow>,
}

impl AlertContext {
    /// Create a new alert context
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an event timestamp
    pub async fn record_event(&self, timestamp: DateTime<Utc>) {
        self.rate_window.record(timestamp.timestamp());
    }

    /// Record an event timestamp without requiring an async context.
    pub fn record_event_sync(&self, timestamp: DateTime<Utc>) {
        self.rate_window.record(timestamp.timestamp());
    }

    /// Get event rate (events per second) over a time window.
    ///
    /// Infallible and lock-free: the counters are atomics, so a busy context
    /// cannot make this under-report. Windows longer than
    /// [`RATE_WINDOW_BUCKETS`] seconds are clamped to that ceiling, and a window
    /// of `0` yields `0.0` (no division by zero).
    #[allow(clippy::cast_precision_loss)]
    fn get_event_rate(&self, window_secs: u64) -> f64 {
        if window_secs == 0 {
            return 0.0;
        }
        let window = window_secs.min(RATE_WINDOW_BUCKETS);
        let count = self.rate_window.count_since(Utc::now().timestamp(), window);
        count as f64 / window as f64
    }

    /// Get event rate (async version)
    ///
    /// Identical to the synchronous calculation; kept for API compatibility.
    pub async fn get_event_rate_async(&self, window_secs: u64) -> f64 {
        self.get_event_rate(window_secs)
    }
}

/// Alert severity level
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AlertSeverity {
    /// Informational alert
    Info,
    /// Warning alert
    Warning,
    /// Error alert
    Error,
    /// Critical alert requiring immediate attention
    Critical,
}

/// Alert triggered by an event
#[derive(Debug, Clone)]
pub struct Alert {
    /// Alert severity
    pub severity: AlertSeverity,
    /// Alert title/summary
    pub title: String,
    /// Alert description
    pub message: String,
    /// Event that triggered the alert
    pub event: Event,
    /// Timestamp when alert was triggered
    pub timestamp: DateTime<Utc>,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

impl Alert {
    /// Create a new alert
    pub fn new(
        severity: AlertSeverity,
        title: impl Into<String>,
        message: impl Into<String>,
        event: Event,
    ) -> Self {
        Self {
            severity,
            title: title.into(),
            message: message.into(),
            event,
            timestamp: Utc::now(),
            metadata: HashMap::new(),
        }
    }

    /// Add metadata to the alert
    #[must_use]
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

/// Alert handler trait
#[async_trait]
pub trait AlertHandler: Send + Sync {
    /// Handle an alert
    async fn handle(&self, alert: &Alert) -> crate::Result<()>;
}

/// Logging alert handler that logs alerts using tracing
#[derive(Debug, Clone, Default)]
pub struct LoggingAlertHandler;

impl LoggingAlertHandler {
    /// Create a new logging alert handler
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl AlertHandler for LoggingAlertHandler {
    async fn handle(&self, alert: &Alert) -> crate::Result<()> {
        let task_id = alert.event.task_id();
        let hostname = alert.event.hostname();

        match alert.severity {
            AlertSeverity::Info => {
                tracing::info!(
                    severity = "info",
                    title = %alert.title,
                    message = %alert.message,
                    event_type = alert.event.event_type(),
                    task_id = ?task_id,
                    hostname = ?hostname,
                    "Alert triggered"
                );
            }
            AlertSeverity::Warning => {
                tracing::warn!(
                    severity = "warning",
                    title = %alert.title,
                    message = %alert.message,
                    event_type = alert.event.event_type(),
                    task_id = ?task_id,
                    hostname = ?hostname,
                    "Alert triggered"
                );
            }
            AlertSeverity::Error => {
                tracing::error!(
                    severity = "error",
                    title = %alert.title,
                    message = %alert.message,
                    event_type = alert.event.event_type(),
                    task_id = ?task_id,
                    hostname = ?hostname,
                    "Alert triggered"
                );
            }
            AlertSeverity::Critical => {
                tracing::error!(
                    severity = "critical",
                    title = %alert.title,
                    message = %alert.message,
                    event_type = alert.event.event_type(),
                    task_id = ?task_id,
                    hostname = ?hostname,
                    "CRITICAL ALERT"
                );
            }
        }
        Ok(())
    }
}

/// Type alias for alert handler registry entries
type AlertHandlerEntry = (AlertCondition, AlertSeverity, String, Arc<dyn AlertHandler>);

/// Alert manager for monitoring events and triggering alerts
pub struct AlertManager {
    handlers: Arc<RwLock<Vec<AlertHandlerEntry>>>,
    context: AlertContext,
}

impl AlertManager {
    /// Create a new alert manager
    #[must_use]
    pub fn new() -> Self {
        Self {
            handlers: Arc::new(RwLock::new(Vec::new())),
            context: AlertContext::new(),
        }
    }

    /// Register an alert handler
    pub async fn register<H: AlertHandler + 'static>(
        &self,
        condition: AlertCondition,
        severity: AlertSeverity,
        title: impl Into<String>,
        handler: H,
    ) {
        let mut handlers = self.handlers.write().await;
        handlers.push((condition, severity, title.into(), Arc::new(handler)));
    }

    /// Process an event and trigger alerts if conditions match
    ///
    /// Alert delivery is best-effort: every matching handler is invoked even if
    /// an earlier one fails, so one broken notification channel cannot silence
    /// the rest.
    ///
    /// # Errors
    ///
    /// Returns an aggregate error *after* all handlers have been attempted if
    /// one or more of them failed.
    pub async fn process_event(&self, event: Event) -> crate::Result<()> {
        // Record event for rate tracking
        self.context.record_event(event.timestamp()).await;

        let handlers = self.handlers.read().await;

        let mut failures: Vec<String> = Vec::new();
        for (condition, severity, title, handler) in handlers.iter() {
            if condition.check(&event, &self.context) {
                let message = format!("Event {} triggered alert condition", event.event_type());
                let alert = Alert::new(*severity, title.clone(), message, event.clone());
                if let Err(e) = handler.handle(&alert).await {
                    tracing::warn!(
                        alert_title = %title,
                        event_type = event.event_type(),
                        error = %e,
                        "Alert handler failed; continuing with the remaining handlers"
                    );
                    failures.push(format!("{title}: {e}"));
                }
            }
        }

        aggregate_failures("alert handlers", failures)
    }

    /// Get the number of registered alert handlers
    pub async fn handler_count(&self) -> usize {
        self.handlers.read().await.len()
    }

    /// Clear all alert handlers
    pub async fn clear(&self) {
        self.handlers.write().await.clear();
    }
}

impl Default for AlertManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Maximum number of distinct hostnames tracked by [`EventMonitor`].
///
/// Events carry a remote-supplied hostname, so the per-hostname map is capped;
/// counts beyond the cap are folded into [`OTHER_HOSTNAMES_BUCKET`].
pub const MAX_TRACKED_HOSTNAMES: usize = 1000;

/// Key used to accumulate events from hostnames beyond
/// [`MAX_TRACKED_HOSTNAMES`].
pub const OTHER_HOSTNAMES_BUCKET: &str = "<other>";

/// Event monitor for collecting statistics
#[derive(Debug, Clone, Default)]
pub struct EventMonitor {
    stats: Arc<RwLock<EventStats>>,
}

#[derive(Debug, Clone, Default)]
pub struct EventStats {
    pub total_events: u64,
    pub task_events: u64,
    pub worker_events: u64,
    pub events_by_type: HashMap<String, u64>,
    pub events_by_hostname: HashMap<String, u64>,
    pub last_event_time: Option<DateTime<Utc>>,
}

impl EventMonitor {
    /// Create a new event monitor
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an event
    pub async fn record(&self, event: &Event) {
        let mut stats = self.stats.write().await;

        stats.total_events += 1;
        stats.last_event_time = Some(event.timestamp());

        match event {
            Event::Task(_) => stats.task_events += 1,
            Event::Worker(_) => stats.worker_events += 1,
        }

        let event_type = event.event_type().to_string();
        *stats.events_by_type.entry(event_type).or_insert(0) += 1;

        if let Some(hostname) = match event {
            Event::Task(task_event) => match task_event {
                TaskEvent::Received { hostname, .. }
                | TaskEvent::Started { hostname, .. }
                | TaskEvent::Succeeded { hostname, .. }
                | TaskEvent::Failed { hostname, .. }
                | TaskEvent::Retried { hostname, .. }
                | TaskEvent::Rejected { hostname, .. } => Some(hostname.clone()),
                _ => None,
            },
            Event::Worker(worker_event) => match worker_event {
                WorkerEvent::Online { hostname, .. }
                | WorkerEvent::Offline { hostname, .. }
                | WorkerEvent::Heartbeat { hostname, .. } => Some(hostname.clone()),
            },
        } {
            // Hostnames come verbatim from remote events, so the map must be
            // bounded: under churn (pods, autoscaled workers) or hostile input
            // it would otherwise grow for the process lifetime. Overflow is
            // folded into a single bucket rather than dropped.
            if stats.events_by_hostname.contains_key(&hostname)
                || stats.events_by_hostname.len() < MAX_TRACKED_HOSTNAMES
            {
                *stats.events_by_hostname.entry(hostname).or_insert(0) += 1;
            } else {
                *stats
                    .events_by_hostname
                    .entry(OTHER_HOSTNAMES_BUCKET.to_string())
                    .or_insert(0) += 1;
            }
        }
    }

    /// Get current statistics
    pub async fn get_stats(&self) -> EventStats {
        self.stats.read().await.clone()
    }

    /// Reset statistics
    pub async fn reset(&self) {
        let mut stats = self.stats.write().await;
        *stats = EventStats::default();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn sample_event() -> Event {
        Event::Task(TaskEvent::Started {
            task_id: Uuid::new_v4(),
            task_name: "t".to_string(),
            hostname: "w1".to_string(),
            timestamp: Utc::now(),
            pid: 1,
        })
    }

    #[tokio::test]
    async fn test_get_event_rate_sync_matches_async() {
        let ctx = AlertContext::new();
        let now = Utc::now();
        // Record 6 events in the last 2 seconds (window)
        for _ in 0..6 {
            ctx.record_event(now).await;
        }
        // Sync rate over 2 seconds = 6/2 = 3.0
        let sync_rate = ctx.get_event_rate(2);
        assert!(sync_rate > 0.0, "Expected sync rate > 0, got {sync_rate}");
        // Async rate should also be > 0
        let async_rate = ctx.get_event_rate_async(2).await;
        assert!(
            async_rate > 0.0,
            "Expected async rate > 0, got {async_rate}"
        );
    }

    #[test]
    fn test_get_event_rate_empty_context() {
        let ctx = AlertContext::new();
        // With no events, rate should be 0.0
        let rate = ctx.get_event_rate(10);
        assert_eq!(rate, 0.0);
    }

    #[test]
    fn test_event_rate_is_exact_and_lock_free() {
        // Regression: the rate used to be read through `try_read()` and
        // silently reported 0.0 whenever the lock was contended, i.e. exactly
        // when the rate was high enough to matter.
        let ctx = AlertContext::new();
        let now = Utc::now();
        for _ in 0..10 {
            ctx.record_event_sync(now);
        }
        // 10 events inside a 5s window => 2 events/s.
        let rate = ctx.get_event_rate(5);
        assert!(
            (rate - 2.0).abs() < f64::EPSILON,
            "expected exactly 2.0 events/s, got {rate}"
        );

        // Events far in the past are outside the window.
        let old = now - chrono::Duration::seconds(120);
        for _ in 0..100 {
            ctx.record_event_sync(old);
        }
        let rate = ctx.get_event_rate(5);
        assert!(
            (rate - 2.0).abs() < f64::EPSILON,
            "stale events leaked: {rate}"
        );
    }

    #[test]
    fn test_event_rate_zero_window_is_not_infinite() {
        // Regression: the async twin divided by `window_secs` with no guard,
        // producing `inf`/`NaN` that then flowed into a `>` comparison.
        let ctx = AlertContext::new();
        ctx.record_event_sync(Utc::now());
        let rate = ctx.get_event_rate(0);
        assert!(rate.is_finite());
        assert_eq!(rate, 0.0);
    }

    #[tokio::test]
    async fn test_event_rate_async_matches_sync_with_zero_window() {
        let ctx = AlertContext::new();
        ctx.record_event(Utc::now()).await;
        let rate = ctx.get_event_rate_async(0).await;
        assert!(rate.is_finite());
        assert_eq!(rate, 0.0);
    }

    #[test]
    fn test_rate_window_is_bounded() {
        // Recording millions of events cannot grow the context: the ring is a
        // fixed number of per-second buckets.
        let ctx = AlertContext::new();
        let now = Utc::now();
        for i in 0..10_000i64 {
            ctx.record_event_sync(now - chrono::Duration::seconds(i % 5_000));
        }
        assert_eq!(ctx.rate_window.seconds.len(), RATE_WINDOW_BUCKETS as usize);
        assert_eq!(ctx.rate_window.counts.len(), RATE_WINDOW_BUCKETS as usize);
    }

    /// Alert handler that always fails.
    struct FailingAlertHandler;

    #[async_trait]
    impl AlertHandler for FailingAlertHandler {
        async fn handle(&self, _alert: &Alert) -> crate::Result<()> {
            Err(crate::CelersError::Other("pager is down".to_string()))
        }
    }

    /// Alert handler that counts the alerts it receives.
    #[derive(Clone, Default)]
    struct CountingAlertHandler {
        count: Arc<RwLock<usize>>,
    }

    #[async_trait]
    impl AlertHandler for CountingAlertHandler {
        async fn handle(&self, _alert: &Alert) -> crate::Result<()> {
            *self.count.write().await += 1;
            Ok(())
        }
    }

    #[tokio::test]
    async fn test_alert_manager_is_best_effort() {
        let manager = AlertManager::new();
        manager
            .register(
                AlertCondition::EventType("task-started".to_string()),
                AlertSeverity::Error,
                "pager",
                FailingAlertHandler,
            )
            .await;
        let counting = CountingAlertHandler::default();
        manager
            .register(
                AlertCondition::EventType("task-started".to_string()),
                AlertSeverity::Warning,
                "log",
                counting.clone(),
            )
            .await;

        let err = manager.process_event(sample_event()).await.unwrap_err();
        assert!(err.to_string().contains("pager is down"));
        assert_eq!(*counting.count.read().await, 1);
    }

    #[tokio::test]
    async fn test_event_monitor_hostname_map_is_bounded() {
        // Regression: `events_by_hostname` was keyed by a hostname taken
        // verbatim from remote events and grew without bound.
        let monitor = EventMonitor::new();
        for i in 0..(MAX_TRACKED_HOSTNAMES + 500) {
            let event = Event::Worker(WorkerEvent::Heartbeat {
                hostname: format!("worker-{i}"),
                timestamp: Utc::now(),
                active: 0,
                processed: 0,
                loadavg: None,
                freq: 1.0,
            });
            monitor.record(&event).await;
        }

        let stats = monitor.get_stats().await;
        assert!(stats.events_by_hostname.len() <= MAX_TRACKED_HOSTNAMES + 1);
        assert_eq!(
            stats.events_by_hostname.get(OTHER_HOSTNAMES_BUCKET),
            Some(&500)
        );
        assert_eq!(stats.total_events as usize, MAX_TRACKED_HOSTNAMES + 500);
    }
}
