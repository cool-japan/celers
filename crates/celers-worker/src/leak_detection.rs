//! Memory leak detection for worker tasks
//!
//! This module provides tools to detect potential memory leaks in worker tasks
//! by monitoring memory usage patterns over time.
//!
//! # Features
//!
//! - Per-task memory usage tracking
//! - Baseline memory measurement
//! - Memory growth rate detection
//! - Leak thresholds and alerts
//! - Historical memory usage analysis
//! - Automatic leak detection
//!
//! # Example
//!
//! ```
//! use celers_worker::{LeakDetector, LeakDetectorConfig};
//! use std::time::Duration;
//!
//! # async fn example() {
//! let config = LeakDetectorConfig::new()
//!     .with_sample_interval(Duration::from_secs(60))
//!     .with_growth_threshold(1.5);  // 50% growth triggers alert
//!
//! let mut detector = LeakDetector::new(config);
//!
//! // Start monitoring a task
//! detector.start_monitoring("task-123", "my_task").await;
//!
//! // ... task execution ...
//!
//! // Stop monitoring and check for leaks
//! if let Some(leak) = detector.stop_monitoring("task-123").await {
//!     println!("Potential leak detected: {}", leak.description());
//! }
//! # }
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Get current process memory usage in bytes (RSS)
#[cfg(target_os = "linux")]
fn get_process_memory() -> Result<usize, String> {
    use std::fs;

    let status = fs::read_to_string("/proc/self/status")
        .map_err(|e| format!("Failed to read /proc/self/status: {}", e))?;

    for line in status.lines() {
        if line.starts_with("VmRSS:") {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                let kb: usize = parts[1]
                    .parse()
                    .map_err(|e| format!("Failed to parse memory: {}", e))?;
                return Ok(kb * 1024); // Convert KB to bytes
            }
        }
    }

    Err("VmRSS not found in /proc/self/status".to_string())
}

#[cfg(not(target_os = "linux"))]
fn get_process_memory() -> Result<usize, String> {
    // Fallback for non-Linux systems - return 0
    // In production, you'd use platform-specific APIs
    Ok(0)
}

/// Memory sample taken at a specific time
#[derive(Debug, Clone)]
pub struct MemorySample {
    /// When the sample was taken
    pub timestamp: Instant,
    /// Memory usage in bytes
    pub bytes: usize,
}

impl MemorySample {
    /// Create a new memory sample
    pub fn new(bytes: usize) -> Self {
        Self {
            timestamp: Instant::now(),
            bytes,
        }
    }

    /// Get the age of this sample
    pub fn age(&self) -> Duration {
        self.timestamp.elapsed()
    }
}

/// Task memory tracking data
#[derive(Debug)]
struct TaskMemoryTracker {
    /// Task ID
    task_id: String,
    /// Task name/type
    task_name: String,
    /// When monitoring started
    started_at: Instant,
    /// Baseline memory (memory when task started)
    baseline_memory: usize,
    /// Historical memory samples
    samples: Vec<MemorySample>,
    /// Maximum samples to keep
    max_samples: usize,
}

impl TaskMemoryTracker {
    fn new(task_id: String, task_name: String, baseline_memory: usize, max_samples: usize) -> Self {
        Self {
            task_id,
            task_name,
            started_at: Instant::now(),
            baseline_memory,
            samples: vec![MemorySample::new(baseline_memory)],
            max_samples,
        }
    }

    /// Add a memory sample
    fn add_sample(&mut self, memory: usize) {
        self.samples.push(MemorySample::new(memory));

        // Keep only the most recent samples
        if self.samples.len() > self.max_samples {
            self.samples.remove(0);
        }
    }

    /// Get the current memory delta from baseline
    fn memory_delta(&self) -> i64 {
        if let Some(latest) = self.samples.last() {
            latest.bytes as i64 - self.baseline_memory as i64
        } else {
            0
        }
    }

    /// Get memory growth rate (bytes per second)
    fn growth_rate(&self) -> f64 {
        if self.samples.len() < 2 {
            return 0.0;
        }

        let first = &self.samples[0];
        let last = self.samples.last().unwrap();
        let duration_secs = (last.timestamp - first.timestamp).as_secs_f64();

        if duration_secs <= 0.0 {
            return 0.0;
        }

        (last.bytes as f64 - first.bytes as f64) / duration_secs
    }

    /// Get average memory usage
    fn avg_memory(&self) -> usize {
        if self.samples.is_empty() {
            return 0;
        }

        let total: usize = self.samples.iter().map(|s| s.bytes).sum();
        total / self.samples.len()
    }

    /// Get maximum memory usage
    fn max_memory(&self) -> usize {
        self.samples.iter().map(|s| s.bytes).max().unwrap_or(0)
    }

    /// Get task execution time
    fn execution_time(&self) -> Duration {
        self.started_at.elapsed()
    }
}

/// Potential memory leak information
#[derive(Debug, Clone)]
pub struct LeakInfo {
    /// Task ID
    pub task_id: String,
    /// Task name/type
    pub task_name: String,
    /// Baseline memory in bytes
    pub baseline_memory: usize,
    /// Final memory in bytes
    pub final_memory: usize,
    /// Memory delta (final - baseline)
    pub memory_delta: i64,
    /// Memory growth rate (bytes per second)
    pub growth_rate: f64,
    /// Average memory usage
    pub avg_memory: usize,
    /// Maximum memory usage
    pub max_memory: usize,
    /// Task execution time
    pub execution_time: Duration,
    /// Number of samples taken
    pub sample_count: usize,
}

impl LeakInfo {
    /// Get a human-readable description of the leak
    pub fn description(&self) -> String {
        format!(
            "Task {} ({}) potential leak: baseline={}KB, final={}KB, delta={}KB, rate={:.2}KB/s, max={}KB over {:?}",
            self.task_id,
            self.task_name,
            self.baseline_memory / 1024,
            self.final_memory / 1024,
            self.memory_delta / 1024,
            self.growth_rate / 1024.0,
            self.max_memory / 1024,
            self.execution_time
        )
    }

    /// Check if this is a significant leak
    pub fn is_significant(&self, growth_threshold: f64) -> bool {
        if self.baseline_memory == 0 {
            return false;
        }

        let growth_ratio = self.final_memory as f64 / self.baseline_memory as f64;
        growth_ratio >= growth_threshold
    }
}

/// Leak detector configuration
#[derive(Clone, Debug)]
pub struct LeakDetectorConfig {
    /// Interval between memory samples
    pub sample_interval: Duration,
    /// Maximum number of samples to keep per task
    pub max_samples: usize,
    /// Memory growth threshold (ratio of final/baseline) to trigger leak alert
    /// For example, 1.5 means 50% growth triggers an alert
    pub growth_threshold: f64,
    /// Minimum memory delta (in bytes) to consider as a leak
    pub min_leak_bytes: usize,
    /// Enable leak detection
    pub enabled: bool,
}

impl LeakDetectorConfig {
    /// Create a new leak detector configuration
    pub fn new() -> Self {
        Self {
            sample_interval: Duration::from_secs(30),
            max_samples: 100,
            growth_threshold: 1.5,            // 50% growth
            min_leak_bytes: 10 * 1024 * 1024, // 10MB
            enabled: true,
        }
    }

    /// Set the sample interval
    pub fn with_sample_interval(mut self, interval: Duration) -> Self {
        self.sample_interval = interval;
        self
    }

    /// Set the maximum number of samples to keep
    pub fn with_max_samples(mut self, max_samples: usize) -> Self {
        self.max_samples = max_samples;
        self
    }

    /// Set the growth threshold
    pub fn with_growth_threshold(mut self, threshold: f64) -> Self {
        self.growth_threshold = threshold;
        self
    }

    /// Set the minimum leak bytes
    pub fn with_min_leak_bytes(mut self, bytes: usize) -> Self {
        self.min_leak_bytes = bytes;
        self
    }

    /// Enable or disable leak detection
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.growth_threshold <= 1.0 {
            return Err("Growth threshold must be greater than 1.0".to_string());
        }
        if self.max_samples == 0 {
            return Err("Max samples must be at least 1".to_string());
        }
        Ok(())
    }
}

impl Default for LeakDetectorConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Memory leak detector
pub struct LeakDetector {
    /// Configuration
    config: LeakDetectorConfig,
    /// Active task trackers
    trackers: Arc<RwLock<HashMap<String, TaskMemoryTracker>>>,
    /// Detected leaks (historical)
    leaks: Arc<RwLock<Vec<LeakInfo>>>,
}

impl LeakDetector {
    /// Create a new leak detector
    pub fn new(config: LeakDetectorConfig) -> Self {
        Self {
            config,
            trackers: Arc::new(RwLock::new(HashMap::new())),
            leaks: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Start monitoring a task
    pub async fn start_monitoring(&self, task_id: impl Into<String>, task_name: impl Into<String>) {
        if !self.config.enabled {
            return;
        }

        let task_id = task_id.into();
        let task_name = task_name.into();

        let baseline_memory = match get_process_memory() {
            Ok(mem) => mem,
            Err(e) => {
                warn!("Failed to get process memory: {}", e);
                return;
            }
        };

        let tracker = TaskMemoryTracker::new(
            task_id.clone(),
            task_name.clone(),
            baseline_memory,
            self.config.max_samples,
        );

        let mut trackers = self.trackers.write().await;
        trackers.insert(task_id.clone(), tracker);

        debug!(
            "Started leak monitoring for task {} ({}), baseline: {}KB",
            task_id,
            task_name,
            baseline_memory / 1024
        );
    }

    /// Stop monitoring a task and return leak info if detected
    pub async fn stop_monitoring(&self, task_id: &str) -> Option<LeakInfo> {
        if !self.config.enabled {
            return None;
        }

        let mut trackers = self.trackers.write().await;
        let tracker = trackers.remove(task_id)?;

        let final_memory = match get_process_memory() {
            Ok(mem) => mem,
            Err(e) => {
                warn!("Failed to get process memory: {}", e);
                return None;
            }
        };

        let memory_delta = final_memory as i64 - tracker.baseline_memory as i64;
        let growth_rate = tracker.growth_rate();

        let leak_info = LeakInfo {
            task_id: tracker.task_id.clone(),
            task_name: tracker.task_name.clone(),
            baseline_memory: tracker.baseline_memory,
            final_memory,
            memory_delta,
            growth_rate,
            avg_memory: tracker.avg_memory(),
            max_memory: tracker.max_memory(),
            execution_time: tracker.execution_time(),
            sample_count: tracker.samples.len(),
        };

        // Check if this is a leak
        let is_leak = leak_info.is_significant(self.config.growth_threshold)
            && memory_delta > 0
            && memory_delta as usize >= self.config.min_leak_bytes;

        if is_leak {
            info!(
                "Potential memory leak detected: {}",
                leak_info.description()
            );

            // Record the leak
            let mut leaks = self.leaks.write().await;
            leaks.push(leak_info.clone());

            Some(leak_info)
        } else {
            debug!(
                "No leak detected for task {}: delta={}KB",
                tracker.task_id,
                memory_delta / 1024
            );
            None
        }
    }

    /// Take a memory sample for a task
    pub async fn sample_task(&self, task_id: &str) -> Result<(), String> {
        if !self.config.enabled {
            return Ok(());
        }

        let memory = get_process_memory()?;

        let mut trackers = self.trackers.write().await;
        if let Some(tracker) = trackers.get_mut(task_id) {
            tracker.add_sample(memory);
            debug!(
                "Sampled task {}: {}KB (delta: {}KB)",
                task_id,
                memory / 1024,
                tracker.memory_delta() / 1024
            );
        }

        Ok(())
    }

    /// Get the number of tasks currently being monitored
    pub async fn active_count(&self) -> usize {
        self.trackers.read().await.len()
    }

    /// Get all detected leaks
    pub async fn get_leaks(&self) -> Vec<LeakInfo> {
        self.leaks.read().await.clone()
    }

    /// Clear leak history
    pub async fn clear_leaks(&self) {
        self.leaks.write().await.clear();
    }

    /// Get configuration
    pub fn config(&self) -> &LeakDetectorConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_sample_creation() {
        let sample = MemorySample::new(1024 * 1024);
        assert_eq!(sample.bytes, 1024 * 1024);
        assert!(sample.age() < Duration::from_secs(1));
    }

    #[test]
    fn test_leak_detector_config_default() {
        let config = LeakDetectorConfig::default();
        assert_eq!(config.sample_interval, Duration::from_secs(30));
        assert_eq!(config.max_samples, 100);
        assert_eq!(config.growth_threshold, 1.5);
        assert_eq!(config.min_leak_bytes, 10 * 1024 * 1024);
        assert!(config.enabled);
    }

    #[test]
    fn test_leak_detector_config_builder() {
        let config = LeakDetectorConfig::new()
            .with_sample_interval(Duration::from_secs(60))
            .with_max_samples(50)
            .with_growth_threshold(2.0)
            .with_min_leak_bytes(5 * 1024 * 1024)
            .enabled(false);

        assert_eq!(config.sample_interval, Duration::from_secs(60));
        assert_eq!(config.max_samples, 50);
        assert_eq!(config.growth_threshold, 2.0);
        assert_eq!(config.min_leak_bytes, 5 * 1024 * 1024);
        assert!(!config.enabled);
    }

    #[test]
    fn test_leak_detector_config_validation() {
        let config = LeakDetectorConfig::new().with_growth_threshold(0.5);
        assert!(config.validate().is_err());

        let config = LeakDetectorConfig::new().with_max_samples(0);
        assert!(config.validate().is_err());

        let config = LeakDetectorConfig::new();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_leak_info_is_significant() {
        let leak = LeakInfo {
            task_id: "task-123".to_string(),
            task_name: "test_task".to_string(),
            baseline_memory: 100 * 1024 * 1024, // 100MB
            final_memory: 200 * 1024 * 1024,    // 200MB
            memory_delta: 100 * 1024 * 1024,
            growth_rate: 1000.0,
            avg_memory: 150 * 1024 * 1024,
            max_memory: 200 * 1024 * 1024,
            execution_time: Duration::from_secs(60),
            sample_count: 10,
        };

        // 2.0 growth ratio, threshold 1.5 -> significant
        assert!(leak.is_significant(1.5));
        // 2.0 growth ratio, threshold 2.5 -> not significant
        assert!(!leak.is_significant(2.5));
    }

    #[test]
    fn test_leak_info_description() {
        let leak = LeakInfo {
            task_id: "task-123".to_string(),
            task_name: "test_task".to_string(),
            baseline_memory: 100 * 1024 * 1024,
            final_memory: 150 * 1024 * 1024,
            memory_delta: 50 * 1024 * 1024,
            growth_rate: 1024.0 * 1024.0, // 1MB/s
            avg_memory: 125 * 1024 * 1024,
            max_memory: 150 * 1024 * 1024,
            execution_time: Duration::from_secs(60),
            sample_count: 10,
        };

        let desc = leak.description();
        assert!(desc.contains("task-123"));
        assert!(desc.contains("test_task"));
        assert!(desc.contains("baseline=102400KB"));
        assert!(desc.contains("final=153600KB"));
    }

    #[tokio::test]
    async fn test_leak_detector_start_stop() {
        let config = LeakDetectorConfig::new();
        let detector = LeakDetector::new(config);

        assert_eq!(detector.active_count().await, 0);

        detector.start_monitoring("task-123", "test_task").await;
        assert_eq!(detector.active_count().await, 1);

        // Stop monitoring (likely no leak since we didn't do anything)
        let _result = detector.stop_monitoring("task-123").await;
        assert_eq!(detector.active_count().await, 0);
    }

    #[tokio::test]
    async fn test_leak_detector_disabled() {
        let config = LeakDetectorConfig::new().enabled(false);
        let detector = LeakDetector::new(config);

        detector.start_monitoring("task-123", "test_task").await;
        assert_eq!(detector.active_count().await, 0); // Should not track when disabled

        let result = detector.stop_monitoring("task-123").await;
        assert!(result.is_none());
    }

    #[tokio::test]
    async fn test_leak_detector_get_clear_leaks() {
        let config = LeakDetectorConfig::new();
        let detector = LeakDetector::new(config);

        assert_eq!(detector.get_leaks().await.len(), 0);

        // Manually add a leak to the history
        let leak = LeakInfo {
            task_id: "task-123".to_string(),
            task_name: "test_task".to_string(),
            baseline_memory: 100 * 1024 * 1024,
            final_memory: 200 * 1024 * 1024,
            memory_delta: 100 * 1024 * 1024,
            growth_rate: 1000.0,
            avg_memory: 150 * 1024 * 1024,
            max_memory: 200 * 1024 * 1024,
            execution_time: Duration::from_secs(60),
            sample_count: 10,
        };

        {
            let mut leaks = detector.leaks.write().await;
            leaks.push(leak);
        }

        assert_eq!(detector.get_leaks().await.len(), 1);

        detector.clear_leaks().await;
        assert_eq!(detector.get_leaks().await.len(), 0);
    }

    #[test]
    fn test_task_memory_tracker() {
        let mut tracker = TaskMemoryTracker::new(
            "task-123".to_string(),
            "test_task".to_string(),
            100 * 1024 * 1024,
            10,
        );

        assert_eq!(tracker.baseline_memory, 100 * 1024 * 1024);
        assert_eq!(tracker.memory_delta(), 0);

        tracker.add_sample(110 * 1024 * 1024);
        assert_eq!(tracker.memory_delta(), 10 * 1024 * 1024);
        assert_eq!(tracker.max_memory(), 110 * 1024 * 1024);
    }
}
