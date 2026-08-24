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
            .expect("SystemTime should be after UNIX_EPOCH")
            .as_secs();

        let mut failures = self.failures.lock().unwrap_or_else(|e| e.into_inner());

        // Opportunistically prune entries that have been idle for more
        // than one full failure window (a 2x margin so an entry mid-reset
        // on another thread is never evicted out from under it). Without
        // this, `failures` grows by one permanent entry per distinct
        // task_id ever seen for the lifetime of the process: a task that
        // fails once and is never retried (or succeeds and is never
        // explicitly cleared via `clear_failures`) would otherwise never
        // leave the map.
        let window_secs = self.failure_window.as_secs();
        failures.retain(|_, (_, last_failure)| {
            now.saturating_sub(*last_failure) < window_secs.saturating_mul(2)
        });

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
            .expect("SystemTime should be after UNIX_EPOCH")
            .as_secs();

        let failures = self.failures.lock().unwrap_or_else(|e| e.into_inner());

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
            .expect("SystemTime should be after UNIX_EPOCH")
            .as_secs();

        let failures = self.failures.lock().unwrap_or_else(|e| e.into_inner());

        if let Some((count, last_failure)) = failures.get(&task_id) {
            if now - last_failure <= self.failure_window.as_secs() {
                return *count;
            }
        }

        0
    }

    /// Clear failure history for a message
    pub fn clear_failures(&self, task_id: uuid::Uuid) {
        let mut failures = self.failures.lock().unwrap_or_else(|e| e.into_inner());
        failures.remove(&task_id);
    }

    /// Clear all failure history
    pub fn clear_all(&self) {
        let mut failures = self.failures.lock().unwrap_or_else(|e| e.into_inner());
        failures.clear();
    }
}

impl Default for PoisonMessageDetector {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod hardening_tests {
    use super::*;

    // -------------------------------------------------------------------
    // idx126: PoisonMessageDetector must not accumulate a permanent entry
    // per distinct task_id, matching the ResourceQuotaMiddleware /
    // SLAMonitoringMiddleware fix in middleware_monitoring.rs.
    // -------------------------------------------------------------------

    #[test]
    fn poison_detector_prunes_long_idle_task_entries() {
        let detector = PoisonMessageDetector::new().with_failure_window(Duration::from_secs(1));
        let stale_task = uuid::Uuid::new_v4();

        // Simulate a task whose last failure is long past 2x the failure
        // window, by writing directly into the (module-private) failures
        // map rather than waiting in real time.
        {
            let mut failures = detector.failures.lock().unwrap_or_else(|e| e.into_inner());
            failures.insert(stale_task, (1, 0));
        }
        assert_eq!(detector.failure_count(stale_task), 0); // outside window already

        {
            let failures = detector.failures.lock().unwrap_or_else(|e| e.into_inner());
            assert!(
                failures.contains_key(&stale_task),
                "entry must still be present before any pruning sweep runs"
            );
        }

        // Recording a failure for a *different* task_id must sweep the
        // stale entry out of the map, not just leave it there forever.
        let active_task = uuid::Uuid::new_v4();
        detector.record_failure(active_task);

        let failures = detector.failures.lock().unwrap_or_else(|e| e.into_inner());
        assert!(
            !failures.contains_key(&stale_task),
            "long-idle task entry must be pruned, not retained indefinitely"
        );
        assert!(failures.contains_key(&active_task));
    }

    #[test]
    fn poison_detector_does_not_prune_entries_still_within_the_grace_margin() {
        let detector = PoisonMessageDetector::new().with_failure_window(Duration::from_secs(3600));
        let task_id = uuid::Uuid::new_v4();

        detector.record_failure(task_id);
        assert_eq!(detector.failure_count(task_id), 1);

        // A second failure for the same (freshly active) task must never
        // be pruned out from under itself.
        detector.record_failure(task_id);
        assert_eq!(detector.failure_count(task_id), 2);
    }
}
