//! Health check support for CeleRS workers
//!
//! Provides health status information useful for monitoring, load balancers,
//! and orchestration systems like Kubernetes.

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

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
    /// Total tasks processed
    pub tasks_processed: u64,
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
            let now = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
            (now - last_success) <= 300 // 5 minutes
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
    start_time: u64,
    tasks_processed: Arc<AtomicU64>,
    consecutive_failures: Arc<AtomicU64>,
    is_processing: Arc<AtomicBool>,
    last_success_timestamp: Arc<AtomicU64>,
}

impl HealthChecker {
    /// Create a new health checker
    pub fn new() -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            start_time: now,
            tasks_processed: Arc::new(AtomicU64::new(0)),
            consecutive_failures: Arc::new(AtomicU64::new(0)),
            is_processing: Arc::new(AtomicBool::new(false)),
            last_success_timestamp: Arc::new(AtomicU64::new(now)),
        }
    }

    /// Record a successful task execution
    pub fn record_success(&self) {
        self.tasks_processed.fetch_add(1, Ordering::Relaxed);
        self.consecutive_failures.store(0, Ordering::Relaxed);

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.last_success_timestamp.store(now, Ordering::Relaxed);
    }

    /// Record a failed task execution
    pub fn record_failure(&self) {
        self.consecutive_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Mark worker as actively processing
    pub fn set_processing(&self, processing: bool) {
        self.is_processing.store(processing, Ordering::Relaxed);
    }

    /// Get current health information
    pub fn get_health(&self) -> HealthInfo {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let uptime_seconds = now - self.start_time;
        let tasks_processed = self.tasks_processed.load(Ordering::Relaxed);
        let consecutive_failures = self.consecutive_failures.load(Ordering::Relaxed);
        let is_processing = self.is_processing.load(Ordering::Relaxed);
        let last_success = self.last_success_timestamp.load(Ordering::Relaxed);

        // Determine health status
        let status = if consecutive_failures >= 10 {
            HealthStatus::Unhealthy
        } else if consecutive_failures >= 5 {
            HealthStatus::Degraded
        } else if uptime_seconds > 60 && now - last_success > 300 {
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
    pub fn get_success_rate(&self) -> f64 {
        let processed = self.tasks_processed.load(Ordering::Relaxed);
        let failures = self.consecutive_failures.load(Ordering::Relaxed);

        if processed > 0 {
            let successes = processed.saturating_sub(failures);
            successes as f64 / processed as f64
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

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        now.saturating_sub(last_success)
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
            "Status: {}, Uptime: {}s, Tasks: {}, Failures: {}, Processing: {}",
            self.status,
            self.uptime_seconds,
            self.tasks_processed,
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
}
