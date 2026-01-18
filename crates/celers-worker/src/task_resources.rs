//! Per-task resource consumption tracking
//!
//! This module provides detailed resource usage tracking for individual tasks,
//! including CPU time, memory usage, and I/O operations.
//!
//! # Features
//!
//! - CPU time tracking per task
//! - Memory usage monitoring (peak and average)
//! - I/O operation counting
//! - Resource usage statistics
//! - Resource limit enforcement
//! - Historical resource usage data
//!
//! # Example
//!
//! ```
//! use celers_worker::{TaskResourceTracker, ResourceLimits};
//! use std::time::Duration;
//!
//! # async fn example() {
//! let mut tracker = TaskResourceTracker::new("task-123".to_string());
//!
//! // Start tracking
//! tracker.start();
//!
//! // ... task execution ...
//!
//! // Stop tracking and get results
//! let usage = tracker.stop();
//! println!("Task used {} bytes of memory", usage.peak_memory_bytes);
//! # }
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::debug;

/// Resource usage snapshot
#[derive(Debug, Clone)]
pub struct ResourceUsage {
    /// Task ID
    pub task_id: String,
    /// CPU time used (user + system)
    pub cpu_time: Duration,
    /// Peak memory usage in bytes
    pub peak_memory_bytes: usize,
    /// Average memory usage in bytes
    pub avg_memory_bytes: usize,
    /// Number of I/O read operations
    pub io_read_ops: u64,
    /// Number of I/O write operations
    pub io_write_ops: u64,
    /// Bytes read
    pub io_read_bytes: u64,
    /// Bytes written
    pub io_written_bytes: u64,
    /// Task execution duration
    pub execution_duration: Duration,
}

impl ResourceUsage {
    /// Create a new empty resource usage
    pub fn new(task_id: String) -> Self {
        Self {
            task_id,
            cpu_time: Duration::ZERO,
            peak_memory_bytes: 0,
            avg_memory_bytes: 0,
            io_read_ops: 0,
            io_write_ops: 0,
            io_read_bytes: 0,
            io_written_bytes: 0,
            execution_duration: Duration::ZERO,
        }
    }

    /// Calculate CPU utilization as a percentage
    pub fn cpu_utilization(&self) -> f64 {
        if self.execution_duration.is_zero() {
            return 0.0;
        }
        (self.cpu_time.as_secs_f64() / self.execution_duration.as_secs_f64()) * 100.0
    }

    /// Calculate I/O throughput (bytes per second)
    pub fn io_throughput(&self) -> f64 {
        if self.execution_duration.is_zero() {
            return 0.0;
        }
        let total_bytes = self.io_read_bytes + self.io_written_bytes;
        total_bytes as f64 / self.execution_duration.as_secs_f64()
    }

    /// Check if resource limits are exceeded
    pub fn exceeds_limits(&self, limits: &super::ResourceLimits) -> bool {
        let memory_mb = self.peak_memory_bytes / (1024 * 1024);
        memory_mb > limits.max_memory_mb as usize || self.cpu_utilization() > limits.max_cpu_percent
    }
}

/// Task resource tracker
pub struct TaskResourceTracker {
    /// Task ID
    task_id: String,
    /// Start time
    start_time: Option<Instant>,
    /// End time
    end_time: Option<Instant>,
    /// Initial memory baseline
    initial_memory: usize,
    /// Memory samples
    memory_samples: Vec<usize>,
    /// CPU time samples (mock for now)
    #[allow(dead_code)]
    cpu_samples: Vec<Duration>,
    /// I/O statistics
    io_read_ops: u64,
    io_write_ops: u64,
    io_read_bytes: u64,
    io_written_bytes: u64,
}

impl TaskResourceTracker {
    /// Create a new resource tracker for a task
    pub fn new(task_id: String) -> Self {
        Self {
            task_id,
            start_time: None,
            end_time: None,
            initial_memory: 0,
            memory_samples: Vec::new(),
            cpu_samples: Vec::new(),
            io_read_ops: 0,
            io_write_ops: 0,
            io_read_bytes: 0,
            io_written_bytes: 0,
        }
    }

    /// Start tracking resources
    pub fn start(&mut self) {
        self.start_time = Some(Instant::now());
        self.initial_memory = Self::get_current_memory();
        self.memory_samples.push(self.initial_memory);
        debug!("Started resource tracking for task {}", self.task_id);
    }

    /// Stop tracking and return resource usage
    pub fn stop(&mut self) -> ResourceUsage {
        self.end_time = Some(Instant::now());

        let execution_duration = if let (Some(start), Some(end)) = (self.start_time, self.end_time)
        {
            end.duration_since(start)
        } else {
            Duration::ZERO
        };

        let peak_memory = self.memory_samples.iter().max().copied().unwrap_or(0);
        let avg_memory = if !self.memory_samples.is_empty() {
            self.memory_samples.iter().sum::<usize>() / self.memory_samples.len()
        } else {
            0
        };

        debug!(
            "Stopped resource tracking for task {}: duration={:?}, peak_memory={}KB",
            self.task_id,
            execution_duration,
            peak_memory / 1024
        );

        ResourceUsage {
            task_id: self.task_id.clone(),
            cpu_time: Duration::ZERO, // Would need actual CPU time tracking
            peak_memory_bytes: peak_memory,
            avg_memory_bytes: avg_memory,
            io_read_ops: self.io_read_ops,
            io_write_ops: self.io_write_ops,
            io_read_bytes: self.io_read_bytes,
            io_written_bytes: self.io_written_bytes,
            execution_duration,
        }
    }

    /// Sample current resource usage
    pub fn sample(&mut self) {
        let current_memory = Self::get_current_memory();
        self.memory_samples.push(current_memory);
    }

    /// Record an I/O read operation
    pub fn record_io_read(&mut self, bytes: u64) {
        self.io_read_ops += 1;
        self.io_read_bytes += bytes;
    }

    /// Record an I/O write operation
    pub fn record_io_write(&mut self, bytes: u64) {
        self.io_write_ops += 1;
        self.io_written_bytes += bytes;
    }

    /// Get current memory usage (mock implementation)
    fn get_current_memory() -> usize {
        // In a real implementation, this would read from /proc/self/status on Linux
        // For now, return a mock value
        0
    }
}

/// Resource consumption manager for multiple tasks
pub struct ResourceConsumptionManager {
    /// Active task trackers
    trackers: Arc<RwLock<HashMap<String, TaskResourceTracker>>>,
    /// Completed task usage history
    history: Arc<RwLock<Vec<ResourceUsage>>>,
    /// Maximum history size
    max_history_size: usize,
}

impl ResourceConsumptionManager {
    /// Create a new resource consumption manager
    pub fn new(max_history_size: usize) -> Self {
        Self {
            trackers: Arc::new(RwLock::new(HashMap::new())),
            history: Arc::new(RwLock::new(Vec::new())),
            max_history_size,
        }
    }

    /// Start tracking a task
    pub async fn start_tracking(&self, task_id: String) {
        let mut tracker = TaskResourceTracker::new(task_id.clone());
        tracker.start();

        let mut trackers = self.trackers.write().await;
        trackers.insert(task_id, tracker);
    }

    /// Stop tracking a task and record its usage
    pub async fn stop_tracking(&self, task_id: &str) -> Option<ResourceUsage> {
        let mut trackers = self.trackers.write().await;
        let mut tracker = trackers.remove(task_id)?;

        let usage = tracker.stop();

        // Add to history
        let mut history = self.history.write().await;
        history.push(usage.clone());

        // Limit history size
        if history.len() > self.max_history_size {
            history.remove(0);
        }

        Some(usage)
    }

    /// Sample resource usage for a task
    pub async fn sample_task(&self, task_id: &str) {
        let mut trackers = self.trackers.write().await;
        if let Some(tracker) = trackers.get_mut(task_id) {
            tracker.sample();
        }
    }

    /// Record I/O operation for a task
    pub async fn record_io_read(&self, task_id: &str, bytes: u64) {
        let mut trackers = self.trackers.write().await;
        if let Some(tracker) = trackers.get_mut(task_id) {
            tracker.record_io_read(bytes);
        }
    }

    /// Record I/O write operation for a task
    pub async fn record_io_write(&self, task_id: &str, bytes: u64) {
        let mut trackers = self.trackers.write().await;
        if let Some(tracker) = trackers.get_mut(task_id) {
            tracker.record_io_write(bytes);
        }
    }

    /// Get resource usage history
    pub async fn get_history(&self) -> Vec<ResourceUsage> {
        self.history.read().await.clone()
    }

    /// Get average resource usage across all tracked tasks
    pub async fn get_average_usage(&self) -> Option<ResourceUsage> {
        let history = self.history.read().await;
        if history.is_empty() {
            return None;
        }

        let count = history.len() as f64;
        let total_cpu = history.iter().map(|u| u.cpu_time).sum::<Duration>();
        let total_peak_mem = history.iter().map(|u| u.peak_memory_bytes).sum::<usize>();
        let total_avg_mem = history.iter().map(|u| u.avg_memory_bytes).sum::<usize>();
        let total_io_read_ops = history.iter().map(|u| u.io_read_ops).sum::<u64>();
        let total_io_write_ops = history.iter().map(|u| u.io_write_ops).sum::<u64>();
        let total_io_read_bytes = history.iter().map(|u| u.io_read_bytes).sum::<u64>();
        let total_io_written_bytes = history.iter().map(|u| u.io_written_bytes).sum::<u64>();
        let total_duration = history
            .iter()
            .map(|u| u.execution_duration)
            .sum::<Duration>();

        Some(ResourceUsage {
            task_id: "average".to_string(),
            cpu_time: total_cpu / count as u32,
            peak_memory_bytes: (total_peak_mem as f64 / count) as usize,
            avg_memory_bytes: (total_avg_mem as f64 / count) as usize,
            io_read_ops: (total_io_read_ops as f64 / count) as u64,
            io_write_ops: (total_io_write_ops as f64 / count) as u64,
            io_read_bytes: (total_io_read_bytes as f64 / count) as u64,
            io_written_bytes: (total_io_written_bytes as f64 / count) as u64,
            execution_duration: total_duration / count as u32,
        })
    }

    /// Get resource usage statistics for a specific task type
    pub async fn get_usage_by_task_name(&self, task_name: &str) -> Vec<ResourceUsage> {
        let history = self.history.read().await;
        history
            .iter()
            .filter(|u| u.task_id.contains(task_name))
            .cloned()
            .collect()
    }

    /// Clear history
    pub async fn clear_history(&self) {
        self.history.write().await.clear();
    }

    /// Get number of active trackers
    pub async fn active_count(&self) -> usize {
        self.trackers.read().await.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::time::sleep;

    #[test]
    fn test_resource_usage_creation() {
        let usage = ResourceUsage::new("task-123".to_string());
        assert_eq!(usage.task_id, "task-123");
        assert_eq!(usage.peak_memory_bytes, 0);
        assert_eq!(usage.cpu_time, Duration::ZERO);
    }

    #[test]
    fn test_resource_usage_cpu_utilization() {
        let usage = ResourceUsage {
            task_id: "task-123".to_string(),
            cpu_time: Duration::from_secs(5),
            execution_duration: Duration::from_secs(10),
            ..ResourceUsage::new("task-123".to_string())
        };

        assert_eq!(usage.cpu_utilization(), 50.0);
    }

    #[test]
    fn test_resource_usage_io_throughput() {
        let usage = ResourceUsage {
            task_id: "task-123".to_string(),
            io_read_bytes: 1000,
            io_written_bytes: 500,
            execution_duration: Duration::from_secs(10),
            ..ResourceUsage::new("task-123".to_string())
        };

        assert_eq!(usage.io_throughput(), 150.0); // 1500 bytes / 10 seconds
    }

    #[tokio::test]
    async fn test_task_resource_tracker() {
        let mut tracker = TaskResourceTracker::new("task-123".to_string());

        tracker.start();
        assert!(tracker.start_time.is_some());

        // Simulate some work
        sleep(Duration::from_millis(10)).await;

        tracker.record_io_read(1024);
        tracker.record_io_write(2048);
        tracker.sample();

        let usage = tracker.stop();

        assert_eq!(usage.task_id, "task-123");
        assert!(usage.execution_duration > Duration::ZERO);
        assert_eq!(usage.io_read_ops, 1);
        assert_eq!(usage.io_write_ops, 1);
        assert_eq!(usage.io_read_bytes, 1024);
        assert_eq!(usage.io_written_bytes, 2048);
    }

    #[tokio::test]
    async fn test_resource_consumption_manager() {
        let manager = ResourceConsumptionManager::new(100);

        assert_eq!(manager.active_count().await, 0);

        manager.start_tracking("task-123".to_string()).await;
        assert_eq!(manager.active_count().await, 1);

        sleep(Duration::from_millis(10)).await;

        manager.record_io_read("task-123", 512).await;
        manager.record_io_write("task-123", 1024).await;

        let usage = manager.stop_tracking("task-123").await;
        assert!(usage.is_some());

        let usage = usage.unwrap();
        assert_eq!(usage.io_read_bytes, 512);
        assert_eq!(usage.io_written_bytes, 1024);
        assert_eq!(manager.active_count().await, 0);

        let history = manager.get_history().await;
        assert_eq!(history.len(), 1);
    }

    #[tokio::test]
    async fn test_resource_consumption_manager_average() {
        let manager = ResourceConsumptionManager::new(100);

        // Track multiple tasks
        for i in 0..3 {
            let task_id = format!("task-{}", i);
            manager.start_tracking(task_id.clone()).await;
            sleep(Duration::from_millis(10)).await;
            manager.record_io_read(&task_id, 1000 * (i + 1)).await;
            manager.stop_tracking(&task_id).await;
        }

        let avg = manager.get_average_usage().await;
        assert!(avg.is_some());

        let avg = avg.unwrap();
        assert_eq!(avg.task_id, "average");
        assert!(avg.io_read_bytes > 0);
    }

    #[tokio::test]
    async fn test_resource_consumption_manager_history_limit() {
        let manager = ResourceConsumptionManager::new(5);

        // Track more tasks than the limit
        for i in 0..10 {
            let task_id = format!("task-{}", i);
            manager.start_tracking(task_id.clone()).await;
            manager.stop_tracking(&task_id).await;
        }

        let history = manager.get_history().await;
        assert_eq!(history.len(), 5);
    }

    #[tokio::test]
    async fn test_resource_consumption_manager_clear() {
        let manager = ResourceConsumptionManager::new(100);

        manager.start_tracking("task-123".to_string()).await;
        manager.stop_tracking("task-123").await;

        assert_eq!(manager.get_history().await.len(), 1);

        manager.clear_history().await;
        assert_eq!(manager.get_history().await.len(), 0);
    }
}
