//! Monitoring and diagnostics utilities for Redis backend
//!
//! This module provides health checks, diagnostics, and monitoring utilities
//! to help operators understand and debug Redis backend operations.

use crate::{BackendError, RedisResultBackend, ResultBackend};
use std::time::{Duration, Instant};
use uuid::Uuid;

/// Health check result
#[derive(Debug, Clone, PartialEq)]
pub enum HealthStatus {
    /// Backend is healthy
    Healthy,
    /// Backend is degraded but operational
    Degraded { reason: String },
    /// Backend is unhealthy
    Unhealthy { reason: String },
}

impl HealthStatus {
    /// Check if the status is healthy
    pub fn is_healthy(&self) -> bool {
        matches!(self, HealthStatus::Healthy)
    }

    /// Check if the status is degraded
    pub fn is_degraded(&self) -> bool {
        matches!(self, HealthStatus::Degraded { .. })
    }

    /// Check if the status is unhealthy
    pub fn is_unhealthy(&self) -> bool {
        matches!(self, HealthStatus::Unhealthy { .. })
    }

    /// Get the reason if degraded or unhealthy
    pub fn reason(&self) -> Option<&str> {
        match self {
            HealthStatus::Healthy => None,
            HealthStatus::Degraded { reason } | HealthStatus::Unhealthy { reason } => Some(reason),
        }
    }
}

impl std::fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HealthStatus::Healthy => write!(f, "Healthy"),
            HealthStatus::Degraded { reason } => write!(f, "Degraded: {}", reason),
            HealthStatus::Unhealthy { reason } => write!(f, "Unhealthy: {}", reason),
        }
    }
}

/// Detailed health check report
#[derive(Debug, Clone)]
pub struct HealthReport {
    /// Overall health status
    pub status: HealthStatus,
    /// Redis connection check (latency in ms)
    pub redis_latency_ms: Option<f64>,
    /// Backend statistics available
    pub stats_available: bool,
    /// Cache hit rate (0.0-1.0)
    pub cache_hit_rate: Option<f64>,
    /// Error rate (0.0-1.0)
    pub error_rate: Option<f64>,
    /// Time taken for health check
    pub check_duration_ms: f64,
}

impl std::fmt::Display for HealthReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Redis Backend Health Report")?;
        writeln!(f, "  Status: {}", self.status)?;
        if let Some(latency) = self.redis_latency_ms {
            writeln!(f, "  Redis Latency: {:.2}ms", latency)?;
        }
        writeln!(f, "  Stats Available: {}", self.stats_available)?;
        if let Some(rate) = self.cache_hit_rate {
            writeln!(f, "  Cache Hit Rate: {:.1}%", rate * 100.0)?;
        }
        if let Some(rate) = self.error_rate {
            writeln!(f, "  Error Rate: {:.1}%", rate * 100.0)?;
        }
        writeln!(f, "  Check Duration: {:.2}ms", self.check_duration_ms)?;
        Ok(())
    }
}

/// Monitoring utilities for Redis backend
pub struct BackendMonitor;

impl BackendMonitor {
    /// Perform a comprehensive health check
    ///
    /// This checks:
    /// - Redis connectivity (PING)
    /// - Basic read/write operations
    /// - Metrics availability
    /// - Cache performance
    /// - Error rates
    pub async fn health_check(
        backend: &mut RedisResultBackend,
    ) -> Result<HealthReport, BackendError> {
        let start = Instant::now();

        // Check Redis connection with PING
        let ping_start = Instant::now();
        let ping_ok = backend.health_check().await.unwrap_or(false);
        let redis_latency_ms = if ping_ok {
            Some(ping_start.elapsed().as_secs_f64() * 1000.0)
        } else {
            None
        };

        // Try to get stats
        let stats_available = backend.get_stats().await.is_ok();

        // Get metrics
        let metrics = backend.metrics();
        let total_ops = metrics.store_count() + metrics.get_count() + metrics.delete_count();

        let cache_hit_rate = if total_ops > 0 {
            Some(metrics.cache_hit_rate())
        } else {
            None
        };

        let error_rate = if total_ops > 0 {
            let total_errors = metrics.error_count();
            Some(total_errors as f64 / total_ops as f64)
        } else {
            None
        };

        // Determine overall status
        let status = if !ping_ok {
            HealthStatus::Unhealthy {
                reason: "Redis connection failed".to_string(),
            }
        } else if let Some(latency) = redis_latency_ms {
            if latency > 100.0 {
                HealthStatus::Degraded {
                    reason: format!("High Redis latency: {:.2}ms", latency),
                }
            } else if let Some(err_rate) = error_rate {
                if err_rate > 0.05 {
                    HealthStatus::Degraded {
                        reason: format!("High error rate: {:.1}%", err_rate * 100.0),
                    }
                } else {
                    HealthStatus::Healthy
                }
            } else {
                HealthStatus::Healthy
            }
        } else {
            HealthStatus::Unhealthy {
                reason: "Unable to measure Redis latency".to_string(),
            }
        };

        let check_duration_ms = start.elapsed().as_secs_f64() * 1000.0;

        Ok(HealthReport {
            status,
            redis_latency_ms,
            stats_available,
            cache_hit_rate,
            error_rate,
            check_duration_ms,
        })
    }

    /// Test round-trip time for a write-read operation
    ///
    /// This creates a temporary task result, stores it, reads it back,
    /// and cleans up. Returns the total time in milliseconds.
    pub async fn measure_roundtrip(
        backend: &mut RedisResultBackend,
    ) -> Result<Duration, BackendError> {
        use crate::TaskMeta;

        let start = Instant::now();
        let test_id = Uuid::new_v4();

        // Store test result
        let meta = TaskMeta::new(test_id, "monitoring.roundtrip_test".to_string());
        backend.store_result(test_id, &meta).await?;

        // Read it back
        let _retrieved = backend.get_result(test_id).await?;

        // Clean up
        backend.delete_result(test_id).await?;

        Ok(start.elapsed())
    }

    /// Measure batch operation performance
    ///
    /// Tests batch store and batch get operations with the specified number of items.
    /// Returns (store_duration, get_duration) in milliseconds.
    pub async fn measure_batch_performance(
        backend: &mut RedisResultBackend,
        batch_size: usize,
    ) -> Result<(Duration, Duration), BackendError> {
        use crate::TaskMeta;

        // Generate test data
        let test_items: Vec<(Uuid, TaskMeta)> = (0..batch_size)
            .map(|i| {
                let id = Uuid::new_v4();
                let meta = TaskMeta::new(id, format!("monitoring.batch_test_{}", i));
                (id, meta)
            })
            .collect();

        let task_ids: Vec<Uuid> = test_items.iter().map(|(id, _)| *id).collect();

        // Measure batch store
        let store_start = Instant::now();
        backend.store_results_batch(&test_items).await?;
        let store_duration = store_start.elapsed();

        // Measure batch get
        let get_start = Instant::now();
        let _results = backend.get_results_batch(&task_ids).await?;
        let get_duration = get_start.elapsed();

        // Clean up
        backend.delete_results_batch(&task_ids).await?;

        Ok((store_duration, get_duration))
    }

    /// Check if backend is responsive within timeout
    pub async fn check_responsive(backend: &mut RedisResultBackend, timeout: Duration) -> bool {
        tokio::time::timeout(timeout, backend.health_check())
            .await
            .map(|r| r.unwrap_or(false))
            .unwrap_or(false)
    }

    /// Get a diagnostic report with key metrics
    pub async fn diagnostic_report(backend: &mut RedisResultBackend) -> String {
        let mut report = String::new();

        report.push_str("=== Redis Backend Diagnostics ===\n\n");

        // Health check
        match Self::health_check(backend).await {
            Ok(health) => report.push_str(&format!("{}\n", health)),
            Err(e) => report.push_str(&format!("Health check failed: {}\n", e)),
        }

        // Backend stats
        match backend.get_stats().await {
            Ok(stats) => report.push_str(&format!("\n{}\n", stats)),
            Err(e) => report.push_str(&format!("\nStats unavailable: {}\n", e)),
        }

        // Metrics snapshot
        let metrics = backend.metrics().snapshot();
        report.push_str(&format!("\n{}\n", metrics));

        // Cache stats
        let cache_stats = backend.cache().stats();
        report.push_str(&format!("\n{}\n", cache_stats));

        report
    }
}

/// Continuous health monitoring helper
pub struct HealthMonitor {
    check_interval: Duration,
    consecutive_failures: usize,
    max_failures: usize,
}

impl Default for HealthMonitor {
    /// Create a health monitor with default settings (30s interval, 3 max failures)
    fn default() -> Self {
        Self::new(Duration::from_secs(30), 3)
    }
}

impl HealthMonitor {
    /// Create a new health monitor
    ///
    /// # Arguments
    /// * `check_interval` - Time between health checks
    /// * `max_failures` - Number of consecutive failures before unhealthy
    pub fn new(check_interval: Duration, max_failures: usize) -> Self {
        Self {
            check_interval,
            consecutive_failures: 0,
            max_failures,
        }
    }

    /// Run continuous health monitoring
    ///
    /// Calls the provided callback with each health report.
    /// Continues until the callback returns `false`.
    pub async fn monitor<F>(
        &mut self,
        backend: &mut RedisResultBackend,
        mut callback: F,
    ) -> Result<(), BackendError>
    where
        F: FnMut(HealthReport) -> bool,
    {
        loop {
            let report = BackendMonitor::health_check(backend).await?;

            // Track consecutive failures
            if report.status.is_unhealthy() {
                self.consecutive_failures += 1;
            } else {
                self.consecutive_failures = 0;
            }

            // Call callback
            if !callback(report) {
                break;
            }

            // Sleep until next check
            tokio::time::sleep(self.check_interval).await;
        }

        Ok(())
    }

    /// Check if backend is considered unhealthy (exceeded max failures)
    pub fn is_unhealthy(&self) -> bool {
        self.consecutive_failures >= self.max_failures
    }

    /// Get the number of consecutive failures
    pub fn consecutive_failures(&self) -> usize {
        self.consecutive_failures
    }

    /// Reset the failure counter
    pub fn reset(&mut self) {
        self.consecutive_failures = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_health_status() {
        let healthy = HealthStatus::Healthy;
        assert!(healthy.is_healthy());
        assert!(!healthy.is_degraded());
        assert!(!healthy.is_unhealthy());
        assert!(healthy.reason().is_none());

        let degraded = HealthStatus::Degraded {
            reason: "High latency".to_string(),
        };
        assert!(!degraded.is_healthy());
        assert!(degraded.is_degraded());
        assert!(!degraded.is_unhealthy());
        assert_eq!(degraded.reason(), Some("High latency"));

        let unhealthy = HealthStatus::Unhealthy {
            reason: "Connection failed".to_string(),
        };
        assert!(!unhealthy.is_healthy());
        assert!(!unhealthy.is_degraded());
        assert!(unhealthy.is_unhealthy());
        assert_eq!(unhealthy.reason(), Some("Connection failed"));
    }

    #[test]
    fn test_health_status_display() {
        let healthy = HealthStatus::Healthy;
        assert_eq!(healthy.to_string(), "Healthy");

        let degraded = HealthStatus::Degraded {
            reason: "Slow".to_string(),
        };
        assert_eq!(degraded.to_string(), "Degraded: Slow");

        let unhealthy = HealthStatus::Unhealthy {
            reason: "Down".to_string(),
        };
        assert_eq!(unhealthy.to_string(), "Unhealthy: Down");
    }

    #[test]
    fn test_health_monitor_defaults() {
        let monitor = HealthMonitor::default();
        assert_eq!(monitor.check_interval, Duration::from_secs(30));
        assert_eq!(monitor.max_failures, 3);
        assert_eq!(monitor.consecutive_failures, 0);
        assert!(!monitor.is_unhealthy());
    }

    #[test]
    fn test_health_monitor_default_trait() {
        let monitor = <HealthMonitor as Default>::default();
        assert_eq!(monitor.check_interval, Duration::from_secs(30));
        assert_eq!(monitor.max_failures, 3);
    }

    #[test]
    fn test_health_monitor_tracking() {
        let mut monitor = HealthMonitor::new(Duration::from_secs(10), 2);

        assert!(!monitor.is_unhealthy());
        assert_eq!(monitor.consecutive_failures(), 0);

        // Simulate failures
        monitor.consecutive_failures = 1;
        assert!(!monitor.is_unhealthy());

        monitor.consecutive_failures = 2;
        assert!(monitor.is_unhealthy());

        // Reset
        monitor.reset();
        assert!(!monitor.is_unhealthy());
        assert_eq!(monitor.consecutive_failures(), 0);
    }
}
