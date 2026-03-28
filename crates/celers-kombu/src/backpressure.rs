//! Backpressure configuration and poison message detection.

use std::collections::HashMap;
use std::time::Duration;

/// Backpressure configuration for flow control
///
/// # Examples
///
/// ```
/// use celers_kombu::BackpressureConfig;
///
/// let config = BackpressureConfig::new()
///     .with_max_pending(1000)
///     .with_max_queue_size(10000)
///     .with_high_watermark(0.8)
///     .with_low_watermark(0.6);
/// ```
#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    /// Maximum number of pending (unacknowledged) messages
    pub max_pending: usize,
    /// Maximum queue size before applying backpressure
    pub max_queue_size: usize,
    /// High watermark ratio (0.0-1.0) to start backpressure
    pub high_watermark: f64,
    /// Low watermark ratio (0.0-1.0) to stop backpressure
    pub low_watermark: f64,
}

impl BackpressureConfig {
    /// Create a new backpressure configuration with defaults
    pub fn new() -> Self {
        Self {
            max_pending: 1000,
            max_queue_size: 10000,
            high_watermark: 0.8,
            low_watermark: 0.6,
        }
    }

    /// Set maximum pending messages
    pub fn with_max_pending(mut self, max: usize) -> Self {
        self.max_pending = max;
        self
    }

    /// Set maximum queue size
    pub fn with_max_queue_size(mut self, max: usize) -> Self {
        self.max_queue_size = max;
        self
    }

    /// Set high watermark ratio
    pub fn with_high_watermark(mut self, ratio: f64) -> Self {
        self.high_watermark = ratio.clamp(0.0, 1.0);
        self
    }

    /// Set low watermark ratio
    pub fn with_low_watermark(mut self, ratio: f64) -> Self {
        self.low_watermark = ratio.clamp(0.0, 1.0);
        self
    }

    /// Check if backpressure should be applied based on pending count
    pub fn should_apply_backpressure(&self, pending: usize) -> bool {
        pending >= (self.max_pending as f64 * self.high_watermark) as usize
    }

    /// Check if backpressure should be released based on pending count
    pub fn should_release_backpressure(&self, pending: usize) -> bool {
        pending <= (self.max_pending as f64 * self.low_watermark) as usize
    }

    /// Check if queue is at capacity
    pub fn is_at_capacity(&self, queue_size: usize) -> bool {
        queue_size >= self.max_queue_size
    }
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Poison message detector - identifies repeatedly failing messages
///
/// # Examples
///
/// ```
/// use celers_kombu::PoisonMessageDetector;
///
/// let detector = PoisonMessageDetector::new()
///     .with_max_failures(5)
///     .with_failure_window(std::time::Duration::from_secs(3600));
/// ```
#[derive(Debug, Clone)]
pub struct PoisonMessageDetector {
    /// Maximum failures before marking as poison
    pub max_failures: u32,
    /// Time window to track failures
    pub failure_window: Duration,
    /// Failure tracking map: task_id -> (failures, last_failure_time)
    failures: std::sync::Arc<std::sync::Mutex<HashMap<uuid::Uuid, (u32, u64)>>>,
}

impl PoisonMessageDetector {
    /// Create a new poison message detector
    pub fn new() -> Self {
        Self {
            max_failures: 5,
            failure_window: Duration::from_secs(3600),
            failures: std::sync::Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    /// Set maximum failures threshold
    pub fn with_max_failures(mut self, max: u32) -> Self {
        self.max_failures = max;
        self
    }

    /// Set failure tracking window
    pub fn with_failure_window(mut self, window: Duration) -> Self {
        self.failure_window = window;
        self
    }

    /// Record a message failure
    pub fn record_failure(&self, task_id: uuid::Uuid) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut failures = self.failures.lock().unwrap();

        let entry = failures.entry(task_id).or_insert((0, now));

        // Reset if outside window
        if now - entry.1 > self.failure_window.as_secs() {
            *entry = (1, now);
        } else {
            entry.0 += 1;
            entry.1 = now;
        }
    }

    /// Check if a message is a poison message
    pub fn is_poison(&self, task_id: uuid::Uuid) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let failures = self.failures.lock().unwrap();

        if let Some((count, last_failure)) = failures.get(&task_id) {
            if now - last_failure <= self.failure_window.as_secs() {
                return *count >= self.max_failures;
            }
        }

        false
    }

    /// Get failure count for a message
    pub fn failure_count(&self, task_id: uuid::Uuid) -> u32 {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let failures = self.failures.lock().unwrap();

        if let Some((count, last_failure)) = failures.get(&task_id) {
            if now - last_failure <= self.failure_window.as_secs() {
                return *count;
            }
        }

        0
    }

    /// Clear failure history for a message
    pub fn clear_failures(&self, task_id: uuid::Uuid) {
        let mut failures = self.failures.lock().unwrap();
        failures.remove(&task_id);
    }

    /// Clear all failure history
    pub fn clear_all(&self) {
        let mut failures = self.failures.lock().unwrap();
        failures.clear();
    }
}

impl Default for PoisonMessageDetector {
    fn default() -> Self {
        Self::new()
    }
}
