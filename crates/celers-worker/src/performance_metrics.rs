//! Worker performance metrics tracking
//!
//! This module provides comprehensive performance monitoring for workers, including:
//! - Tasks per second (throughput)
//! - Average task latency
//! - Latency percentiles (P50, P95, P99)
//! - Worker utilization percentage
//! - Task failure rate by type
//!
//! # Example
//!
//! ```
//! use celers_worker::performance_metrics::{PerformanceTracker, PerformanceConfig};
//! use std::time::Duration;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = PerformanceConfig {
//!     sample_window_secs: 60,
//!     percentile_window_size: 1000,
//! };
//!
//! let tracker = PerformanceTracker::new(config);
//!
//! // Record task execution
//! tracker.record_task_start().await;
//! // ... task execution ...
//! tracker.record_task_completion(Duration::from_millis(100), "my_task", true).await;
//!
//! // Get performance statistics
//! let stats = tracker.get_stats().await;
//! println!("Tasks per second: {:.2}", stats.tasks_per_second);
//! println!("Average latency: {:?}", stats.avg_latency);
//! println!("P95 latency: {:?}", stats.p95_latency);
//! # Ok(())
//! # }
//! ```

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

#[cfg(feature = "metrics")]
use crate::metrics;

/// Configuration for performance tracking
#[derive(Debug, Clone)]
pub struct PerformanceConfig {
    /// Time window for throughput calculation (seconds)
    pub sample_window_secs: u64,
    /// Number of samples to keep for percentile calculations
    pub percentile_window_size: usize,
}

impl Default for PerformanceConfig {
    fn default() -> Self {
        Self {
            sample_window_secs: 60,
            percentile_window_size: 1000,
        }
    }
}

/// Task execution record
#[derive(Debug, Clone)]
struct TaskRecord {
    #[allow(dead_code)]
    started_at: Instant,
    completed_at: Instant,
    duration: Duration,
    task_type: String,
    success: bool,
}

/// Performance statistics
#[derive(Debug, Clone)]
pub struct PerformanceStats {
    /// Tasks completed per second
    pub tasks_per_second: f64,
    /// Average task latency
    pub avg_latency: Duration,
    /// Minimum latency
    pub min_latency: Duration,
    /// Maximum latency
    pub max_latency: Duration,
    /// 50th percentile (median) latency
    pub p50_latency: Duration,
    /// 95th percentile latency
    pub p95_latency: Duration,
    /// 99th percentile latency
    pub p99_latency: Duration,
    /// Worker utilization percentage (0.0-100.0)
    pub utilization_percent: f64,
    /// Total tasks completed in window
    pub total_completed: usize,
    /// Total tasks failed in window
    pub total_failed: usize,
    /// Failure rate (0.0-1.0)
    pub failure_rate: f64,
    /// Failure rate by task type
    pub failure_rate_by_type: HashMap<String, f64>,
}

impl Default for PerformanceStats {
    fn default() -> Self {
        Self {
            tasks_per_second: 0.0,
            avg_latency: Duration::ZERO,
            min_latency: Duration::MAX,
            max_latency: Duration::ZERO,
            p50_latency: Duration::ZERO,
            p95_latency: Duration::ZERO,
            p99_latency: Duration::ZERO,
            utilization_percent: 0.0,
            total_completed: 0,
            total_failed: 0,
            failure_rate: 0.0,
            failure_rate_by_type: HashMap::new(),
        }
    }
}

impl std::fmt::Display for PerformanceStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Performance(tps={:.2}, avg_latency={:?}, p95={:?}, p99={:?}, util={:.1}%, failure_rate={:.2}%)",
            self.tasks_per_second,
            self.avg_latency,
            self.p95_latency,
            self.p99_latency,
            self.utilization_percent,
            self.failure_rate * 100.0
        )
    }
}

/// Internal state for performance tracking
#[derive(Debug)]
struct PerformanceState {
    /// Recent task records within the sample window
    records: VecDeque<TaskRecord>,
    /// Currently active tasks (started but not completed)
    active_tasks: usize,
    /// Window start time for throughput calculation
    window_start: Instant,
    /// Configuration
    config: PerformanceConfig,
}

/// Worker performance tracker
#[derive(Debug, Clone)]
pub struct PerformanceTracker {
    state: Arc<RwLock<PerformanceState>>,
}

impl PerformanceTracker {
    /// Create a new performance tracker
    pub fn new(config: PerformanceConfig) -> Self {
        Self {
            state: Arc::new(RwLock::new(PerformanceState {
                records: VecDeque::new(),
                active_tasks: 0,
                window_start: Instant::now(),
                config,
            })),
        }
    }

    /// Record task start
    pub async fn record_task_start(&self) {
        let mut state = self.state.write().await;
        state.active_tasks += 1;

        #[cfg(feature = "metrics")]
        {
            metrics::BUSY_WORKERS.inc();
        }
    }

    /// Record task completion
    pub async fn record_task_completion(&self, duration: Duration, task_type: &str, success: bool) {
        let mut state = self.state.write().await;

        if state.active_tasks > 0 {
            state.active_tasks -= 1;
        }

        let now = Instant::now();
        let record = TaskRecord {
            started_at: now - duration,
            completed_at: now,
            duration,
            task_type: task_type.to_string(),
            success,
        };

        state.records.push_back(record);

        // Remove old records outside the sample window
        let window_duration = Duration::from_secs(state.config.sample_window_secs);
        while let Some(front) = state.records.front() {
            if now.duration_since(front.completed_at) > window_duration {
                state.records.pop_front();
            } else {
                break;
            }
        }

        // Limit the size to percentile window
        while state.records.len() > state.config.percentile_window_size {
            state.records.pop_front();
        }

        #[cfg(feature = "metrics")]
        {
            if state.active_tasks > 0 {
                metrics::BUSY_WORKERS.dec();
            }
        }
    }

    /// Get current performance statistics
    pub async fn get_stats(&self) -> PerformanceStats {
        let state = self.state.read().await;

        if state.records.is_empty() {
            return PerformanceStats::default();
        }

        let now = Instant::now();
        let window_duration = Duration::from_secs(state.config.sample_window_secs);

        // Calculate throughput
        let elapsed_secs = now.duration_since(state.window_start).as_secs_f64();
        let tasks_per_second = if elapsed_secs > 0.0 {
            state.records.len() as f64 / elapsed_secs.min(window_duration.as_secs_f64())
        } else {
            0.0
        };

        // Collect latencies and calculate statistics
        let mut latencies: Vec<Duration> = state.records.iter().map(|r| r.duration).collect();
        latencies.sort();

        let total_completed = state.records.len();
        let total_failed = state.records.iter().filter(|r| !r.success).count();
        let failure_rate = if total_completed > 0 {
            total_failed as f64 / total_completed as f64
        } else {
            0.0
        };

        // Calculate failure rate by type
        let mut type_total: HashMap<String, usize> = HashMap::new();
        let mut type_failed: HashMap<String, usize> = HashMap::new();

        for record in &state.records {
            *type_total.entry(record.task_type.clone()).or_insert(0) += 1;
            if !record.success {
                *type_failed.entry(record.task_type.clone()).or_insert(0) += 1;
            }
        }

        let failure_rate_by_type = type_total
            .iter()
            .map(|(task_type, &total)| {
                let failed = type_failed.get(task_type).copied().unwrap_or(0);
                let rate = if total > 0 {
                    failed as f64 / total as f64
                } else {
                    0.0
                };
                (task_type.clone(), rate)
            })
            .collect();

        let avg_latency = if !latencies.is_empty() {
            let sum: Duration = latencies.iter().sum();
            sum / latencies.len() as u32
        } else {
            Duration::ZERO
        };

        let min_latency = latencies.first().copied().unwrap_or(Duration::MAX);
        let max_latency = latencies.last().copied().unwrap_or(Duration::ZERO);

        let p50_latency = percentile(&latencies, 50.0);
        let p95_latency = percentile(&latencies, 95.0);
        let p99_latency = percentile(&latencies, 99.0);

        // Calculate utilization
        let total_task_time: Duration = state.records.iter().map(|r| r.duration).sum();
        let utilization_percent = if elapsed_secs > 0.0 {
            (total_task_time.as_secs_f64() / elapsed_secs * 100.0).min(100.0)
        } else {
            0.0
        };

        #[cfg(feature = "metrics")]
        {
            // Update Prometheus metrics
            crate::metrics::WORKER_UTILIZATION_PERCENT.set(utilization_percent);
        }

        PerformanceStats {
            tasks_per_second,
            avg_latency,
            min_latency,
            max_latency,
            p50_latency,
            p95_latency,
            p99_latency,
            utilization_percent,
            total_completed,
            total_failed,
            failure_rate,
            failure_rate_by_type,
        }
    }

    /// Get the number of currently active tasks
    pub async fn active_tasks(&self) -> usize {
        let state = self.state.read().await;
        state.active_tasks
    }

    /// Reset all statistics
    pub async fn reset(&self) {
        let mut state = self.state.write().await;
        state.records.clear();
        state.window_start = Instant::now();
    }
}

/// Calculate percentile from sorted latencies
fn percentile(sorted_latencies: &[Duration], percentile: f64) -> Duration {
    if sorted_latencies.is_empty() {
        return Duration::ZERO;
    }

    let index = (percentile / 100.0 * (sorted_latencies.len() - 1) as f64).round() as usize;
    sorted_latencies[index.min(sorted_latencies.len() - 1)]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_performance_tracker_empty() {
        let tracker = PerformanceTracker::new(PerformanceConfig::default());
        let stats = tracker.get_stats().await;

        assert_eq!(stats.tasks_per_second, 0.0);
        assert_eq!(stats.total_completed, 0);
        assert_eq!(stats.failure_rate, 0.0);
    }

    #[tokio::test]
    async fn test_performance_tracker_single_task() {
        let tracker = PerformanceTracker::new(PerformanceConfig::default());

        tracker.record_task_start().await;
        tracker
            .record_task_completion(Duration::from_millis(100), "test_task", true)
            .await;

        let stats = tracker.get_stats().await;
        assert_eq!(stats.total_completed, 1);
        assert_eq!(stats.total_failed, 0);
        assert_eq!(stats.failure_rate, 0.0);
        assert!(stats.avg_latency >= Duration::from_millis(99));
        assert!(stats.avg_latency <= Duration::from_millis(101));
    }

    #[tokio::test]
    async fn test_performance_tracker_multiple_tasks() {
        let tracker = PerformanceTracker::new(PerformanceConfig::default());

        for i in 0..10 {
            tracker.record_task_start().await;
            let success = i % 3 != 0; // Fail every 3rd task
            tracker
                .record_task_completion(Duration::from_millis(50 + i * 10), "test_task", success)
                .await;
        }

        let stats = tracker.get_stats().await;
        assert_eq!(stats.total_completed, 10);
        assert_eq!(stats.total_failed, 4); // Tasks 0, 3, 6, 9
        assert!((stats.failure_rate - 0.4).abs() < 0.01);
        assert!(stats.avg_latency > Duration::ZERO);
    }

    #[tokio::test]
    async fn test_performance_tracker_percentiles() {
        let tracker = PerformanceTracker::new(PerformanceConfig::default());

        // Record tasks with known latencies
        let latencies = vec![10, 20, 30, 40, 50, 60, 70, 80, 90, 100];
        for latency in latencies {
            tracker.record_task_start().await;
            tracker
                .record_task_completion(Duration::from_millis(latency), "test_task", true)
                .await;
        }

        let stats = tracker.get_stats().await;
        assert_eq!(stats.total_completed, 10);
        // With 10 values, p50 is at index 4.5, which rounds to 5 (60ms)
        assert!(stats.p50_latency >= Duration::from_millis(50));
        assert!(stats.p50_latency <= Duration::from_millis(60));
        // p95 is at index 8.55, which rounds to 9 (100ms)
        assert!(stats.p95_latency >= Duration::from_millis(90));
        // p99 is at index 8.91, which rounds to 9 (100ms)
        assert!(stats.p99_latency >= Duration::from_millis(90));
    }

    #[tokio::test]
    async fn test_performance_tracker_active_tasks() {
        let tracker = PerformanceTracker::new(PerformanceConfig::default());

        assert_eq!(tracker.active_tasks().await, 0);

        tracker.record_task_start().await;
        assert_eq!(tracker.active_tasks().await, 1);

        tracker.record_task_start().await;
        assert_eq!(tracker.active_tasks().await, 2);

        tracker
            .record_task_completion(Duration::from_millis(100), "test_task", true)
            .await;
        assert_eq!(tracker.active_tasks().await, 1);
    }

    #[tokio::test]
    async fn test_performance_tracker_reset() {
        let tracker = PerformanceTracker::new(PerformanceConfig::default());

        tracker.record_task_start().await;
        tracker
            .record_task_completion(Duration::from_millis(100), "test_task", true)
            .await;

        let stats = tracker.get_stats().await;
        assert_eq!(stats.total_completed, 1);

        tracker.reset().await;
        let stats = tracker.get_stats().await;
        assert_eq!(stats.total_completed, 0);
    }

    #[tokio::test]
    async fn test_performance_tracker_failure_by_type() {
        let tracker = PerformanceTracker::new(PerformanceConfig::default());

        // Task type A: 3 success, 1 failure
        for i in 0..4 {
            tracker.record_task_start().await;
            tracker
                .record_task_completion(Duration::from_millis(100), "task_a", i != 3)
                .await;
        }

        // Task type B: 2 success, 2 failures
        for i in 0..4 {
            tracker.record_task_start().await;
            tracker
                .record_task_completion(Duration::from_millis(100), "task_b", i % 2 == 0)
                .await;
        }

        let stats = tracker.get_stats().await;
        assert_eq!(stats.total_completed, 8);
        assert_eq!(stats.total_failed, 3);

        let rate_a = stats.failure_rate_by_type.get("task_a").unwrap();
        assert!((rate_a - 0.25).abs() < 0.01);

        let rate_b = stats.failure_rate_by_type.get("task_b").unwrap();
        assert!((rate_b - 0.5).abs() < 0.01);
    }

    #[test]
    fn test_percentile_calculation() {
        let latencies = vec![
            Duration::from_millis(10),
            Duration::from_millis(20),
            Duration::from_millis(30),
            Duration::from_millis(40),
            Duration::from_millis(50),
        ];

        assert_eq!(percentile(&latencies, 0.0), Duration::from_millis(10));
        assert_eq!(percentile(&latencies, 50.0), Duration::from_millis(30));
        assert_eq!(percentile(&latencies, 100.0), Duration::from_millis(50));
    }

    #[test]
    fn test_percentile_empty() {
        let latencies: Vec<Duration> = vec![];
        assert_eq!(percentile(&latencies, 50.0), Duration::ZERO);
    }
}
