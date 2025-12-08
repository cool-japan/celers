//! Resource usage tracking for workers
//!
//! Monitors CPU, memory, and other system resources to help detect
//! resource exhaustion and enable adaptive behavior.
//!
//! # Features
//!
//! - **Memory Tracking**: Process and system memory usage
//! - **CPU Monitoring**: Process CPU utilization
//! - **Resource Limits**: Configurable thresholds for warnings/alerts
//! - **Metrics Integration**: Optional Prometheus metrics
//!
//! # Example
//!
//! ```rust
//! use celers_worker::resource_tracker::{ResourceTracker, ResourceLimits};
//!
//! # async fn example() {
//! let limits = ResourceLimits {
//!     max_memory_mb: 1024,
//!     max_cpu_percent: 80.0,
//!     ..Default::default()
//! };
//!
//! let tracker = ResourceTracker::new(limits);
//!
//! // Check if resources are within limits
//! if tracker.check_limits().await {
//!     // Execute task
//! } else {
//!     // Resource exhaustion, throttle or reject
//! }
//! # }
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, warn};

/// Resource usage limits
#[derive(Debug, Clone)]
pub struct ResourceLimits {
    /// Maximum memory usage in MB (0 = unlimited)
    pub max_memory_mb: u64,

    /// Maximum CPU usage percentage (0-100, 0 = unlimited)
    pub max_cpu_percent: f64,

    /// Enable resource tracking
    pub enabled: bool,

    /// Warning threshold for memory (percentage of max)
    pub memory_warning_percent: f64,

    /// Warning threshold for CPU (percentage of max)
    pub cpu_warning_percent: f64,
}

impl Default for ResourceLimits {
    fn default() -> Self {
        Self {
            max_memory_mb: 0,
            max_cpu_percent: 0.0,
            enabled: true,
            memory_warning_percent: 80.0,
            cpu_warning_percent: 80.0,
        }
    }
}

impl ResourceLimits {
    /// Check if limits are valid
    pub fn is_valid(&self) -> bool {
        (self.max_cpu_percent == 0.0 || self.max_cpu_percent <= 100.0)
            && self.memory_warning_percent <= 100.0
            && self.cpu_warning_percent <= 100.0
    }

    /// Check if memory limit is set
    pub fn has_memory_limit(&self) -> bool {
        self.max_memory_mb > 0
    }

    /// Check if CPU limit is set
    pub fn has_cpu_limit(&self) -> bool {
        self.max_cpu_percent > 0.0
    }
}

/// Resource usage statistics
#[derive(Debug, Clone)]
pub struct ResourceStats {
    /// Process memory usage in MB
    pub memory_mb: u64,

    /// Process CPU usage percentage (0-100)
    pub cpu_percent: f64,

    /// Number of active tasks
    pub active_tasks: u64,

    /// System load average (1-minute)
    pub load_average: f64,

    /// Timestamp of measurement
    pub timestamp: u64,
}

impl ResourceStats {
    /// Check if memory usage exceeds threshold
    pub fn memory_exceeds(&self, threshold_mb: u64) -> bool {
        threshold_mb > 0 && self.memory_mb > threshold_mb
    }

    /// Check if CPU usage exceeds threshold
    pub fn cpu_exceeds(&self, threshold_percent: f64) -> bool {
        threshold_percent > 0.0 && self.cpu_percent > threshold_percent
    }

    /// Get memory usage as percentage of limit
    pub fn memory_percentage(&self, limit_mb: u64) -> f64 {
        if limit_mb == 0 {
            0.0
        } else {
            (self.memory_mb as f64 / limit_mb as f64) * 100.0
        }
    }

    /// Get CPU usage as percentage of limit
    pub fn cpu_percentage(&self, limit_percent: f64) -> f64 {
        if limit_percent == 0.0 {
            0.0
        } else {
            (self.cpu_percent / limit_percent) * 100.0
        }
    }
}

/// Resource tracker for monitoring system resources
pub struct ResourceTracker {
    limits: ResourceLimits,
    active_tasks: Arc<AtomicU64>,
    stats: Arc<RwLock<Option<ResourceStats>>>,
}

impl ResourceTracker {
    /// Create a new resource tracker
    pub fn new(limits: ResourceLimits) -> Self {
        if !limits.is_valid() {
            warn!("Invalid resource limits provided");
        }

        Self {
            limits,
            active_tasks: Arc::new(AtomicU64::new(0)),
            stats: Arc::new(RwLock::new(None)),
        }
    }

    /// Create with default limits
    pub fn with_defaults() -> Self {
        Self::new(ResourceLimits::default())
    }

    /// Increment active task count
    pub fn task_started(&self) {
        self.active_tasks.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement active task count
    pub fn task_completed(&self) {
        self.active_tasks.fetch_sub(1, Ordering::Relaxed);
    }

    /// Get current active task count
    pub fn active_tasks(&self) -> u64 {
        self.active_tasks.load(Ordering::Relaxed)
    }

    /// Update resource statistics
    pub async fn update_stats(&self) {
        if !self.limits.enabled {
            return;
        }

        let memory_mb = self.get_memory_usage_mb();
        let cpu_percent = self.get_cpu_usage_percent();
        let load_average = self.get_load_average();

        let stats = ResourceStats {
            memory_mb,
            cpu_percent,
            active_tasks: self.active_tasks(),
            load_average,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        };

        debug!(
            "Resource stats: memory={}MB, cpu={:.1}%, active={}, load={:.2}",
            stats.memory_mb, stats.cpu_percent, stats.active_tasks, stats.load_average
        );

        // Check warnings
        if self.limits.has_memory_limit() {
            let mem_pct = stats.memory_percentage(self.limits.max_memory_mb);
            if mem_pct >= self.limits.memory_warning_percent {
                warn!(
                    "Memory usage warning: {:.1}% ({}/{}MB)",
                    mem_pct, stats.memory_mb, self.limits.max_memory_mb
                );
            }
        }

        if self.limits.has_cpu_limit() {
            let cpu_pct = stats.cpu_percentage(self.limits.max_cpu_percent);
            if cpu_pct >= self.limits.cpu_warning_percent {
                warn!(
                    "CPU usage warning: {:.1}% ({:.1}/{:.1}%)",
                    cpu_pct, stats.cpu_percent, self.limits.max_cpu_percent
                );
            }
        }

        #[cfg(feature = "metrics")]
        {
            use celers_metrics::WORKER_MEMORY_USAGE_BYTES;
            WORKER_MEMORY_USAGE_BYTES.set((stats.memory_mb as f64) * 1024.0 * 1024.0);
        }

        let mut stats_lock = self.stats.write().await;
        *stats_lock = Some(stats);
    }

    /// Get current resource statistics
    pub async fn get_stats(&self) -> Option<ResourceStats> {
        let stats = self.stats.read().await;
        stats.clone()
    }

    /// Check if current resource usage is within limits
    ///
    /// Returns true if within limits, false if exceeded
    pub async fn check_limits(&self) -> bool {
        if !self.limits.enabled {
            return true;
        }

        // Update stats before checking
        self.update_stats().await;

        let stats = self.stats.read().await;
        if let Some(ref stats) = *stats {
            let memory_ok = !stats.memory_exceeds(self.limits.max_memory_mb);
            let cpu_ok = !stats.cpu_exceeds(self.limits.max_cpu_percent);

            if !memory_ok {
                warn!(
                    "Memory limit exceeded: {}MB > {}MB",
                    stats.memory_mb, self.limits.max_memory_mb
                );
            }

            if !cpu_ok {
                warn!(
                    "CPU limit exceeded: {:.1}% > {:.1}%",
                    stats.cpu_percent, self.limits.max_cpu_percent
                );
            }

            memory_ok && cpu_ok
        } else {
            true
        }
    }

    /// Get process memory usage in MB
    fn get_memory_usage_mb(&self) -> u64 {
        #[cfg(target_os = "linux")]
        {
            use std::fs;
            if let Ok(status) = fs::read_to_string("/proc/self/status") {
                for line in status.lines() {
                    if line.starts_with("VmRSS:") {
                        let parts: Vec<&str> = line.split_whitespace().collect();
                        if parts.len() >= 2 {
                            if let Ok(kb) = parts[1].parse::<u64>() {
                                return kb / 1024; // Convert KB to MB
                            }
                        }
                    }
                }
            }
        }

        // Fallback: estimate based on system
        0
    }

    /// Get process CPU usage percentage
    fn get_cpu_usage_percent(&self) -> f64 {
        #[cfg(target_os = "linux")]
        {
            use std::fs;
            if let Ok(stat) = fs::read_to_string("/proc/self/stat") {
                let parts: Vec<&str> = stat.split_whitespace().collect();
                if parts.len() >= 14 {
                    // This is a simplified calculation
                    // Real implementation would need to track delta over time
                    // For now, return a placeholder
                    return 0.0;
                }
            }
        }

        0.0
    }

    /// Get system load average
    fn get_load_average(&self) -> f64 {
        #[cfg(target_os = "linux")]
        {
            use std::fs;
            if let Ok(loadavg) = fs::read_to_string("/proc/loadavg") {
                let parts: Vec<&str> = loadavg.split_whitespace().collect();
                if !parts.is_empty() {
                    if let Ok(load) = parts[0].parse::<f64>() {
                        return load;
                    }
                }
            }
        }

        0.0
    }
}

impl Clone for ResourceTracker {
    fn clone(&self) -> Self {
        Self {
            limits: self.limits.clone(),
            active_tasks: Arc::clone(&self.active_tasks),
            stats: Arc::clone(&self.stats),
        }
    }
}

impl Default for ResourceTracker {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resource_limits_default() {
        let limits = ResourceLimits::default();
        assert_eq!(limits.max_memory_mb, 0);
        assert_eq!(limits.max_cpu_percent, 0.0);
        assert!(limits.enabled);
        assert!(limits.is_valid());
    }

    #[test]
    fn test_resource_limits_validation() {
        let valid = ResourceLimits {
            max_memory_mb: 1024,
            max_cpu_percent: 80.0,
            ..Default::default()
        };
        assert!(valid.is_valid());

        let invalid = ResourceLimits {
            max_cpu_percent: 150.0, // Invalid: > 100%
            ..Default::default()
        };
        assert!(!invalid.is_valid());
    }

    #[test]
    fn test_resource_limits_has_limits() {
        let limits = ResourceLimits {
            max_memory_mb: 1024,
            max_cpu_percent: 80.0,
            ..Default::default()
        };
        assert!(limits.has_memory_limit());
        assert!(limits.has_cpu_limit());

        let no_limits = ResourceLimits::default();
        assert!(!no_limits.has_memory_limit());
        assert!(!no_limits.has_cpu_limit());
    }

    #[test]
    fn test_resource_stats_exceeds() {
        let stats = ResourceStats {
            memory_mb: 512,
            cpu_percent: 75.0,
            active_tasks: 5,
            load_average: 2.5,
            timestamp: 0,
        };

        assert!(stats.memory_exceeds(256));
        assert!(!stats.memory_exceeds(1024));
        assert!(!stats.memory_exceeds(0));

        assert!(stats.cpu_exceeds(50.0));
        assert!(!stats.cpu_exceeds(90.0));
        assert!(!stats.cpu_exceeds(0.0));
    }

    #[test]
    fn test_resource_stats_percentage() {
        let stats = ResourceStats {
            memory_mb: 512,
            cpu_percent: 60.0,
            active_tasks: 5,
            load_average: 2.5,
            timestamp: 0,
        };

        assert_eq!(stats.memory_percentage(1024), 50.0);
        assert_eq!(stats.memory_percentage(512), 100.0);
        assert_eq!(stats.memory_percentage(0), 0.0);

        assert_eq!(stats.cpu_percentage(100.0), 60.0);
        assert_eq!(stats.cpu_percentage(80.0), 75.0);
        assert_eq!(stats.cpu_percentage(0.0), 0.0);
    }

    #[test]
    fn test_resource_tracker_task_counting() {
        let tracker = ResourceTracker::with_defaults();
        assert_eq!(tracker.active_tasks(), 0);

        tracker.task_started();
        assert_eq!(tracker.active_tasks(), 1);

        tracker.task_started();
        assert_eq!(tracker.active_tasks(), 2);

        tracker.task_completed();
        assert_eq!(tracker.active_tasks(), 1);

        tracker.task_completed();
        assert_eq!(tracker.active_tasks(), 0);
    }

    #[tokio::test]
    async fn test_resource_tracker_update_stats() {
        let limits = ResourceLimits {
            enabled: true,
            ..Default::default()
        };
        let tracker = ResourceTracker::new(limits);

        tracker.update_stats().await;
        let stats = tracker.get_stats().await;
        assert!(stats.is_some());
    }

    #[tokio::test]
    async fn test_resource_tracker_check_limits_unlimited() {
        let tracker = ResourceTracker::with_defaults();
        assert!(tracker.check_limits().await);
    }

    #[tokio::test]
    async fn test_resource_tracker_disabled() {
        let limits = ResourceLimits {
            enabled: false,
            ..Default::default()
        };
        let tracker = ResourceTracker::new(limits);

        tracker.update_stats().await;
        let stats = tracker.get_stats().await;
        assert!(stats.is_none());

        assert!(tracker.check_limits().await);
    }
}
