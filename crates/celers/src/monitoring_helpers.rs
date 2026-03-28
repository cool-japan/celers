use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

/// Task execution monitor
///
/// Tracks task execution statistics and provides insights into task performance.
#[derive(Clone)]
pub struct TaskMonitor {
    metrics: Arc<Mutex<MonitorMetrics>>,
}

#[derive(Debug, Clone)]
struct MonitorMetrics {
    total_tasks: usize,
    successful_tasks: usize,
    failed_tasks: usize,
    total_execution_time_ms: u128,
    start_time: u64,
}

impl TaskMonitor {
    /// Creates a new task monitor
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(Mutex::new(MonitorMetrics {
                total_tasks: 0,
                successful_tasks: 0,
                failed_tasks: 0,
                total_execution_time_ms: 0,
                start_time: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            })),
        }
    }

    /// Records a successful task execution
    pub fn record_success(&self, execution_time_ms: u128) {
        let mut metrics = self.metrics.lock().expect("lock should not be poisoned");
        metrics.total_tasks += 1;
        metrics.successful_tasks += 1;
        metrics.total_execution_time_ms += execution_time_ms;
    }

    /// Records a failed task execution
    pub fn record_failure(&self, execution_time_ms: u128) {
        let mut metrics = self.metrics.lock().expect("lock should not be poisoned");
        metrics.total_tasks += 1;
        metrics.failed_tasks += 1;
        metrics.total_execution_time_ms += execution_time_ms;
    }

    /// Gets the total number of tasks processed
    pub fn total_tasks(&self) -> usize {
        self.metrics
            .lock()
            .expect("lock should not be poisoned")
            .total_tasks
    }

    /// Gets the number of successful tasks
    pub fn successful_tasks(&self) -> usize {
        self.metrics
            .lock()
            .expect("lock should not be poisoned")
            .successful_tasks
    }

    /// Gets the number of failed tasks
    pub fn failed_tasks(&self) -> usize {
        self.metrics
            .lock()
            .expect("lock should not be poisoned")
            .failed_tasks
    }

    /// Gets the average execution time in milliseconds
    pub fn average_execution_time_ms(&self) -> u128 {
        let metrics = self.metrics.lock().expect("lock should not be poisoned");
        if metrics.total_tasks == 0 {
            0
        } else {
            metrics.total_execution_time_ms / metrics.total_tasks as u128
        }
    }

    /// Gets the success rate as a percentage
    pub fn success_rate(&self) -> f64 {
        let metrics = self.metrics.lock().expect("lock should not be poisoned");
        if metrics.total_tasks == 0 {
            0.0
        } else {
            (metrics.successful_tasks as f64 / metrics.total_tasks as f64) * 100.0
        }
    }

    /// Resets all metrics
    pub fn reset(&self) {
        let mut metrics = self.metrics.lock().expect("lock should not be poisoned");
        metrics.total_tasks = 0;
        metrics.successful_tasks = 0;
        metrics.failed_tasks = 0;
        metrics.total_execution_time_ms = 0;
        metrics.start_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
    }

    /// Generates a summary report
    pub fn summary(&self) -> String {
        let metrics = self.metrics.lock().expect("lock should not be poisoned");
        format!(
            "Task Monitor Summary:\n\
             - Total Tasks: {}\n\
             - Successful: {} ({:.2}%)\n\
             - Failed: {} ({:.2}%)\n\
             - Avg Execution Time: {}ms\n\
             - Uptime: {}s",
            metrics.total_tasks,
            metrics.successful_tasks,
            if metrics.total_tasks > 0 {
                (metrics.successful_tasks as f64 / metrics.total_tasks as f64) * 100.0
            } else {
                0.0
            },
            metrics.failed_tasks,
            if metrics.total_tasks > 0 {
                (metrics.failed_tasks as f64 / metrics.total_tasks as f64) * 100.0
            } else {
                0.0
            },
            if metrics.total_tasks > 0 {
                metrics.total_execution_time_ms / metrics.total_tasks as u128
            } else {
                0
            },
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs()
                - metrics.start_time
        )
    }
}

impl Default for TaskMonitor {
    fn default() -> Self {
        Self::new()
    }
}
