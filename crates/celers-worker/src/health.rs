//! Health check support for CeleRS workers
//!
//! Provides health status information useful for monitoring, load balancers,
//! and orchestration systems like Kubernetes.
//!
//! Regression guard for the fix in idx 190 (`expect()` on production
//! paths, e.g. the five `SystemTime` expectations this module used to
//! carry): denies `clippy::unwrap_used`/`clippy::expect_used` outside the
//! test module so a future change cannot silently reintroduce a panic on
//! these hot health-check paths.
#![deny(clippy::unwrap_used, clippy::expect_used)]

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Current wall-clock time as whole seconds since the Unix epoch.
///
/// `SystemTime::now()` is not guaranteed to be at or after `UNIX_EPOCH` (a
/// misconfigured clock can be set earlier), so this never panics/expects on
/// that: an unrepresentable duration is reported as `0` rather than
/// crashing a production health-check path. Callers that need an elapsed
/// *duration* (as opposed to an absolute timestamp to expose over the
/// wire) should prefer a monotonic [`Instant`] instead, which cannot go
/// backwards and cannot fail.
fn now_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Health status of a worker
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthStatus {
    /// Worker is healthy and processing tasks
    Healthy,
    /// Worker is degraded but still operational
    Degraded,
    /// Worker is unhealthy and should not receive traffic
    Unhealthy,
}

impl HealthStatus {
    /// Check if the worker is healthy
    pub fn is_healthy(&self) -> bool {
        matches!(self, HealthStatus::Healthy)
    }

    /// Check if the worker is degraded
    pub fn is_degraded(&self) -> bool {
        matches!(self, HealthStatus::Degraded)
    }

    /// Check if the worker is unhealthy
    pub fn is_unhealthy(&self) -> bool {
        matches!(self, HealthStatus::Unhealthy)
    }

    /// Check if the worker can accept traffic (healthy or degraded)
    pub fn can_accept_traffic(&self) -> bool {
        !self.is_unhealthy()
    }
}

/// Detailed health information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthInfo {
    /// Overall health status
    pub status: HealthStatus,
    /// Worker uptime in seconds
    pub uptime_seconds: u64,
    /// Total tasks processed successfully
    pub tasks_processed: u64,
    /// Total tasks that ended in failure (not reset by an intervening
    /// success, unlike `consecutive_failures`)
    #[serde(default)]
    pub tasks_failed: u64,
    /// Number of consecutive failures
    pub consecutive_failures: u64,
    /// Whether worker is actively processing
    pub is_processing: bool,
    /// Timestamp of last successful task
    pub last_success_timestamp: Option<u64>,
    /// Custom message (optional)
    pub message: Option<String>,
}

impl HealthInfo {
    /// Check if worker is healthy
    pub fn is_healthy(&self) -> bool {
        self.status.is_healthy()
    }

    /// Check if worker is degraded
    pub fn is_degraded(&self) -> bool {
        self.status.is_degraded()
    }

    /// Check if worker is unhealthy
    pub fn is_unhealthy(&self) -> bool {
        self.status.is_unhealthy()
    }

    /// Check if worker can accept traffic
    pub fn can_accept_traffic(&self) -> bool {
        self.status.can_accept_traffic()
    }

    /// Check if worker has processed any tasks
    pub fn has_processed_tasks(&self) -> bool {
        self.tasks_processed > 0
    }

    /// Check if worker has recent activity (successful task in last 5 minutes)
    pub fn has_recent_activity(&self) -> bool {
        if let Some(last_success) = self.last_success_timestamp {
            // `saturating_sub` guards against a backwards wall-clock step
            // (e.g. an NTP correction) making `last_success > now`; in that
            // case we treat the gap as zero rather than underflowing.
            now_epoch_secs().saturating_sub(last_success) <= 300 // 5 minutes
        } else {
            false
        }
    }

    /// Check if worker has a status message
    pub fn has_message(&self) -> bool {
        self.message.is_some()
    }

    /// Get uptime as a human-readable duration
    pub fn uptime_duration(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.uptime_seconds)
    }
}

/// Health check tracker for workers
#[derive(Clone)]
pub struct HealthChecker {
    /// Monotonic start marker used for uptime. Deliberately *not*
    /// wall-clock: `SystemTime` can jump backwards (NTP correction, manual
    /// clock change), which would make a naive `now - start_time`
    /// subtraction panic (debug) or wrap to an enormous value (release).
    start_time: Instant,
    tasks_processed: Arc<AtomicU64>,
    tasks_failed: Arc<AtomicU64>,
    consecutive_failures: Arc<AtomicU64>,
    is_processing: Arc<AtomicBool>,
    last_success_timestamp: Arc<AtomicU64>,
}

impl HealthChecker {
    /// Create a new health checker
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            tasks_processed: Arc::new(AtomicU64::new(0)),
            tasks_failed: Arc::new(AtomicU64::new(0)),
            consecutive_failures: Arc::new(AtomicU64::new(0)),
            is_processing: Arc::new(AtomicBool::new(false)),
            last_success_timestamp: Arc::new(AtomicU64::new(now_epoch_secs())),
        }
    }

    /// Record a successful task execution
    pub fn record_success(&self) {
        self.tasks_processed.fetch_add(1, Ordering::Relaxed);
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.last_success_timestamp
            .store(now_epoch_secs(), Ordering::Relaxed);
    }

    /// Record a failed task execution
    pub fn record_failure(&self) {
        self.consecutive_failures.fetch_add(1, Ordering::Relaxed);
        self.tasks_failed.fetch_add(1, Ordering::Relaxed);
    }

    /// Mark worker as actively processing
    pub fn set_processing(&self, processing: bool) {
        self.is_processing.store(processing, Ordering::Relaxed);
    }

    /// Get current health information
    pub fn get_health(&self) -> HealthInfo {
        // Monotonic: cannot panic, cannot go backwards.
        let uptime_seconds = self.start_time.elapsed().as_secs();
        let now = now_epoch_secs();
        let tasks_processed = self.tasks_processed.load(Ordering::Relaxed);
        let tasks_failed = self.tasks_failed.load(Ordering::Relaxed);
        let consecutive_failures = self.consecutive_failures.load(Ordering::Relaxed);
        let is_processing = self.is_processing.load(Ordering::Relaxed);
        let last_success = self.last_success_timestamp.load(Ordering::Relaxed);

        // Determine health status. `saturating_sub` guards against a
        // backwards wall-clock step making `last_success > now`.
        let status = if consecutive_failures >= 10 {
            HealthStatus::Unhealthy
        } else if consecutive_failures >= 5 {
            HealthStatus::Degraded
        } else if uptime_seconds > 60 && now.saturating_sub(last_success) > 300 {
            // No success in 5 minutes after running for 1 minute
            HealthStatus::Degraded
        } else {
            HealthStatus::Healthy
        };

        let message = match status {
            HealthStatus::Unhealthy => {
                Some(format!("{} consecutive failures", consecutive_failures))
            }
            HealthStatus::Degraded => {
                if consecutive_failures >= 5 {
                    Some(format!("{} consecutive failures", consecutive_failures))
                } else {
                    Some("No recent successful tasks".to_string())
                }
            }
            HealthStatus::Healthy => None,
        };

        HealthInfo {
            status,
            uptime_seconds,
            tasks_processed,
            tasks_failed,
            consecutive_failures,
            is_processing,
            last_success_timestamp: Some(last_success),
            message,
        }
    }

    /// Check if worker is healthy
    pub fn is_healthy(&self) -> bool {
        matches!(self.get_health().status, HealthStatus::Healthy)
    }

    /// Check if worker is ready to accept work
    pub fn is_ready(&self) -> bool {
        !matches!(self.get_health().status, HealthStatus::Unhealthy)
    }

    /// Get health status as JSON string
    pub fn get_health_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(&self.get_health())
    }

    /// Get health status as pretty JSON string
    pub fn get_health_json_pretty(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(&self.get_health())
    }

    /// Get success rate (0.0 - 1.0)
    ///
    /// Computed as `successes / (successes + failures)` over the lifetime
    /// of the checker (or since the last [`reset`](Self::reset)), using an
    /// independent failure counter rather than the *consecutive*-failure
    /// gauge (which resets to zero on any success and so cannot be mixed
    /// with a lifetime success count).
    pub fn get_success_rate(&self) -> f64 {
        let successes = self.tasks_processed.load(Ordering::Relaxed);
        let failures = self.tasks_failed.load(Ordering::Relaxed);
        let total = successes.saturating_add(failures);

        if total > 0 {
            successes as f64 / total as f64
        } else {
            1.0
        }
    }

    /// Get failure rate (0.0 - 1.0)
    pub fn get_failure_rate(&self) -> f64 {
        1.0 - self.get_success_rate()
    }

    /// Reset all counters (useful for testing)
    pub fn reset(&self) {
        self.tasks_processed.store(0, Ordering::Relaxed);
        self.tasks_failed.store(0, Ordering::Relaxed);
        self.consecutive_failures.store(0, Ordering::Relaxed);
        self.last_success_timestamp.store(0, Ordering::Relaxed);
        self.is_processing.store(false, Ordering::Relaxed);
    }

    /// Get time since last success in seconds
    pub fn time_since_last_success_seconds(&self) -> u64 {
        let last_success = self.last_success_timestamp.load(Ordering::Relaxed);
        if last_success == 0 {
            return u64::MAX; // Never succeeded
        }

        now_epoch_secs().saturating_sub(last_success)
    }

    /// Check if worker has been idle for more than the specified duration
    pub fn is_idle(&self, idle_threshold_seconds: u64) -> bool {
        !self.is_processing.load(Ordering::Relaxed)
            && self.time_since_last_success_seconds() > idle_threshold_seconds
    }
}

impl Default for HealthChecker {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HealthStatus::Healthy => write!(f, "Healthy"),
            HealthStatus::Degraded => write!(f, "Degraded"),
            HealthStatus::Unhealthy => write!(f, "Unhealthy"),
        }
    }
}

impl std::fmt::Display for HealthInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Status: {}, Uptime: {}s, Tasks: {} ok / {} failed, Consecutive failures: {}, Processing: {}",
            self.status,
            self.uptime_seconds,
            self.tasks_processed,
            self.tasks_failed,
            self.consecutive_failures,
            self.is_processing
        )?;

        if let Some(msg) = &self.message {
            write!(f, ", Message: {}", msg)?;
        }

        Ok(())
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn test_health_checker_new() {
        let checker = HealthChecker::new();
        let health = checker.get_health();

        assert_eq!(health.status, HealthStatus::Healthy);
        assert_eq!(health.tasks_processed, 0);
        assert_eq!(health.consecutive_failures, 0);
        assert!(!health.is_processing);
    }

    #[test]
    fn test_record_success() {
        let checker = HealthChecker::new();

        checker.record_success();
        let health = checker.get_health();

        assert_eq!(health.tasks_processed, 1);
        assert_eq!(health.consecutive_failures, 0);
        assert_eq!(health.status, HealthStatus::Healthy);
    }

    #[test]
    fn test_record_failure() {
        let checker = HealthChecker::new();

        for _ in 0..3 {
            checker.record_failure();
        }

        let health = checker.get_health();
        assert_eq!(health.consecutive_failures, 3);
        assert_eq!(health.status, HealthStatus::Healthy);

        // Add more failures to trigger degraded
        for _ in 0..3 {
            checker.record_failure();
        }

        let health = checker.get_health();
        assert_eq!(health.consecutive_failures, 6);
        assert_eq!(health.status, HealthStatus::Degraded);

        // Add more failures to trigger unhealthy
        for _ in 0..5 {
            checker.record_failure();
        }

        let health = checker.get_health();
        assert_eq!(health.consecutive_failures, 11);
        assert_eq!(health.status, HealthStatus::Unhealthy);
    }

    #[test]
    fn test_failure_reset_on_success() {
        let checker = HealthChecker::new();

        for _ in 0..6 {
            checker.record_failure();
        }

        assert_eq!(checker.get_health().status, HealthStatus::Degraded);

        checker.record_success();
        let health = checker.get_health();

        assert_eq!(health.consecutive_failures, 0);
        assert_eq!(health.status, HealthStatus::Healthy);
        assert_eq!(health.tasks_processed, 1);
    }

    #[test]
    fn test_is_processing() {
        let checker = HealthChecker::new();

        assert!(!checker.get_health().is_processing);

        checker.set_processing(true);
        assert!(checker.get_health().is_processing);

        checker.set_processing(false);
        assert!(!checker.get_health().is_processing);
    }

    #[test]
    fn test_is_healthy() {
        let checker = HealthChecker::new();
        assert!(checker.is_healthy());

        for _ in 0..10 {
            checker.record_failure();
        }

        assert!(!checker.is_healthy());
    }

    #[test]
    fn test_is_ready() {
        let checker = HealthChecker::new();
        assert!(checker.is_ready());

        // Degraded is still ready
        for _ in 0..6 {
            checker.record_failure();
        }
        assert!(checker.is_ready());

        // Unhealthy is not ready
        for _ in 0..5 {
            checker.record_failure();
        }
        assert!(!checker.is_ready());
    }

    // --- Regression tests -------------------------------------------------

    /// Regression test: `get_success_rate` used to divide successes by
    /// `tasks_processed` and subtract `consecutive_failures` (a gauge that
    /// resets on any success), producing numbers unrelated to the true
    /// success rate. 10 successes followed by 3 failures used to report
    /// 0.70 (`(10-3)/10`); the real rate over 13 attempts is 10/13.
    #[test]
    fn test_success_rate_is_true_ratio_not_consecutive_failure_gauge() {
        let checker = HealthChecker::new();

        for _ in 0..10 {
            checker.record_success();
        }
        for _ in 0..3 {
            checker.record_failure();
        }

        let expected = 10.0 / 13.0;
        assert!(
            (checker.get_success_rate() - expected).abs() < 1e-9,
            "expected {expected}, got {}",
            checker.get_success_rate()
        );
        assert!((checker.get_failure_rate() - (1.0 - expected)).abs() < 1e-9);
    }

    /// Regression test: previously a single success sandwiched between
    /// failures reset `consecutive_failures` to 0, which drove the bogus
    /// formula straight back to 1.0 even though real failures had
    /// occurred. The true ratio must account for every recorded failure.
    #[test]
    fn test_success_rate_survives_intervening_success() {
        let checker = HealthChecker::new();

        checker.record_failure();
        checker.record_failure();
        checker.record_success(); // resets consecutive_failures, not history
        checker.record_failure();

        // 1 success, 3 failures => 0.25, not 1.0.
        let expected = 1.0 / 4.0;
        assert!((checker.get_success_rate() - expected).abs() < 1e-9);
    }

    /// Regression test: previously the rate was computed from
    /// `tasks_processed` (successes only) and `consecutive_failures`
    /// (which saturates conceptually at the failure streak length, not
    /// the lifetime failure count), so a long failure streak after some
    /// successes could saturate the reported rate at 0.0 despite real
    /// successes. The rate must remain a true ratio of the lifetime
    /// totals instead.
    #[test]
    fn test_success_rate_does_not_saturate_to_zero_on_long_failure_streak() {
        let checker = HealthChecker::new();

        for _ in 0..10 {
            checker.record_success();
        }
        for _ in 0..20 {
            checker.record_failure();
        }

        let expected = 10.0 / 30.0;
        assert!((checker.get_success_rate() - expected).abs() < 1e-9);
    }

    /// Regression test: `tasks_failed` must be tracked independently of
    /// `consecutive_failures` and survive an intervening success.
    #[test]
    fn test_tasks_failed_is_a_lifetime_counter() {
        let checker = HealthChecker::new();

        checker.record_failure();
        checker.record_success();
        checker.record_failure();
        checker.record_failure();

        let health = checker.get_health();
        assert_eq!(health.tasks_failed, 3);
        assert_eq!(health.consecutive_failures, 2);
        assert_eq!(health.tasks_processed, 1);
    }

    /// Regression test: `uptime_seconds` must be derived from a monotonic
    /// clock and must never panic/underflow, unlike the previous
    /// `wall_clock_now - start_time` computation which could go negative
    /// (and thus panic in debug builds / wrap in release) after a
    /// backwards NTP correction.
    #[test]
    fn test_uptime_never_panics_and_is_monotonic_non_negative() {
        let checker = HealthChecker::new();
        // Uptime must be well-defined immediately after construction, and
        // must never be able to underflow regardless of wall-clock state.
        let health = checker.get_health();
        assert!(
            health.uptime_seconds < 5,
            "fresh checker should read ~0s uptime"
        );
    }

    /// Regression test: a wall-clock `last_success_timestamp` recorded in
    /// the future relative to "now" (simulating a backwards NTP step
    /// between recording success and checking health) must not panic and
    /// must be treated as "no gap" rather than underflowing to a huge
    /// number.
    #[test]
    fn test_has_recent_activity_handles_future_last_success_without_panic() {
        let info = HealthInfo {
            status: HealthStatus::Healthy,
            uptime_seconds: 120,
            tasks_processed: 1,
            tasks_failed: 0,
            consecutive_failures: 0,
            is_processing: false,
            // A timestamp "in the future" relative to the wall clock at
            // check time simulates a backwards clock step between the two
            // `SystemTime::now()` samples.
            last_success_timestamp: Some(u64::MAX),
            message: None,
        };
        // Must not panic (raw subtraction previously could underflow) and
        // must report recent activity (the gap saturates to zero).
        assert!(info.has_recent_activity());
    }
}
