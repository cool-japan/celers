//! Memory usage utilities and tracking

use std::sync::atomic::{AtomicUsize, Ordering};

/// Memory usage tracker for worker runtime
pub struct MemoryTracker {
    /// Current estimated memory usage in bytes
    current_usage: AtomicUsize,
}

impl MemoryTracker {
    pub fn new() -> Self {
        Self {
            current_usage: AtomicUsize::new(0),
        }
    }

    /// Record task result size
    pub fn record_task_result(&self, size_bytes: usize) {
        self.current_usage.fetch_add(size_bytes, Ordering::Relaxed);

        #[cfg(feature = "metrics")]
        {
            use celers_metrics::TASK_RESULT_SIZE_BYTES;
            TASK_RESULT_SIZE_BYTES.observe(size_bytes as f64);
        }
    }

    /// Release task result memory
    pub fn release_task_result(&self, size_bytes: usize) {
        self.current_usage.fetch_sub(size_bytes, Ordering::Relaxed);
    }

    /// Get current memory usage estimate
    pub fn current_usage_bytes(&self) -> usize {
        self.current_usage.load(Ordering::Relaxed)
    }

    /// Update metrics with current usage
    #[cfg(feature = "metrics")]
    pub fn update_metrics(&self) {
        use celers_metrics::WORKER_MEMORY_USAGE_BYTES;
        WORKER_MEMORY_USAGE_BYTES.set(self.current_usage_bytes() as f64);
    }
}

impl Default for MemoryTracker {
    fn default() -> Self {
        Self::new()
    }
}

/// Check if result size exceeds limit
pub fn check_result_size(result: &[u8], max_size: usize) -> Result<(), String> {
    if max_size > 0 && result.len() > max_size {
        #[cfg(feature = "metrics")]
        {
            use celers_metrics::OVERSIZED_RESULTS_TOTAL;
            OVERSIZED_RESULTS_TOTAL.inc();
        }

        Err(format!(
            "Task result size ({} bytes) exceeds limit ({} bytes)",
            result.len(),
            max_size
        ))
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_tracker() {
        let tracker = MemoryTracker::new();
        assert_eq!(tracker.current_usage_bytes(), 0);

        tracker.record_task_result(1000);
        assert_eq!(tracker.current_usage_bytes(), 1000);

        tracker.record_task_result(500);
        assert_eq!(tracker.current_usage_bytes(), 1500);

        tracker.release_task_result(500);
        assert_eq!(tracker.current_usage_bytes(), 1000);
    }

    #[test]
    fn test_check_result_size() {
        let small_result = vec![0u8; 100];
        assert!(check_result_size(&small_result, 1000).is_ok());

        let large_result = vec![0u8; 2000];
        assert!(check_result_size(&large_result, 1000).is_err());

        // Unlimited (0 means no limit)
        assert!(check_result_size(&large_result, 0).is_ok());
    }
}
