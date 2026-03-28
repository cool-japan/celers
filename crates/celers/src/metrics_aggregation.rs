use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Time-series data point
#[derive(Debug, Clone)]
pub struct DataPoint {
    /// Timestamp of the data point
    pub timestamp: Instant,
    /// Value of the metric
    pub value: f64,
}

/// Histogram for tracking value distributions
pub struct Histogram {
    buckets: Vec<(f64, usize)>, // (upper_bound, count)
    total_count: usize,
    sum: f64,
}

impl Histogram {
    /// Creates a new histogram with default buckets
    pub fn new() -> Self {
        Self::with_buckets(vec![
            10.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0,
        ])
    }

    /// Creates a histogram with custom buckets
    pub fn with_buckets(bucket_bounds: Vec<f64>) -> Self {
        let buckets = bucket_bounds.into_iter().map(|b| (b, 0)).collect();
        Self {
            buckets,
            total_count: 0,
            sum: 0.0,
        }
    }

    /// Records a value
    pub fn record(&mut self, value: f64) {
        self.total_count += 1;
        self.sum += value;

        for (bound, count) in &mut self.buckets {
            if value <= *bound {
                *count += 1;
                break;
            }
        }
    }

    /// Gets the total count of recorded values
    pub fn count(&self) -> usize {
        self.total_count
    }

    /// Gets the sum of all recorded values
    pub fn sum(&self) -> f64 {
        self.sum
    }

    /// Gets the mean value
    pub fn mean(&self) -> f64 {
        if self.total_count == 0 {
            0.0
        } else {
            self.sum / self.total_count as f64
        }
    }

    /// Gets the percentile value (approximate)
    pub fn percentile(&self, p: f64) -> f64 {
        if self.total_count == 0 {
            return 0.0;
        }

        let target_count = (self.total_count as f64 * p / 100.0) as usize;
        let mut cumulative = 0;

        for (bound, count) in &self.buckets {
            cumulative += count;
            if cumulative >= target_count {
                return *bound;
            }
        }

        // Return the last bucket bound if we didn't find it
        self.buckets.last().map(|(b, _)| *b).unwrap_or(0.0)
    }

    /// Resets the histogram
    pub fn reset(&mut self) {
        for (_, count) in &mut self.buckets {
            *count = 0;
        }
        self.total_count = 0;
        self.sum = 0.0;
    }
}

impl Default for Histogram {
    fn default() -> Self {
        Self::new()
    }
}

/// Metrics aggregator for task execution metrics
pub struct MetricsAggregator {
    task_durations: Arc<Mutex<HashMap<String, Histogram>>>,
    task_counts: Arc<Mutex<HashMap<String, usize>>>,
    task_errors: Arc<Mutex<HashMap<String, usize>>>,
    time_series: Arc<Mutex<HashMap<String, Vec<DataPoint>>>>,
    start_time: Instant,
}

impl MetricsAggregator {
    /// Creates a new metrics aggregator
    pub fn new() -> Self {
        Self {
            task_durations: Arc::new(Mutex::new(HashMap::new())),
            task_counts: Arc::new(Mutex::new(HashMap::new())),
            task_errors: Arc::new(Mutex::new(HashMap::new())),
            time_series: Arc::new(Mutex::new(HashMap::new())),
            start_time: Instant::now(),
        }
    }

    /// Records task execution duration
    pub fn record_duration(&self, task_name: &str, duration: Duration) {
        let duration_ms = duration.as_secs_f64() * 1000.0;

        // Update histogram
        let mut durations = self
            .task_durations
            .lock()
            .expect("lock should not be poisoned");
        durations
            .entry(task_name.to_string())
            .or_default()
            .record(duration_ms);

        // Update count
        let mut counts = self
            .task_counts
            .lock()
            .expect("lock should not be poisoned");
        *counts.entry(task_name.to_string()).or_insert(0) += 1;

        // Update time series
        let mut series = self
            .time_series
            .lock()
            .expect("lock should not be poisoned");
        series
            .entry(task_name.to_string())
            .or_default()
            .push(DataPoint {
                timestamp: Instant::now(),
                value: duration_ms,
            });
    }

    /// Records task error
    pub fn record_error(&self, task_name: &str) {
        let mut errors = self
            .task_errors
            .lock()
            .expect("lock should not be poisoned");
        *errors.entry(task_name.to_string()).or_insert(0) += 1;
    }

    /// Gets task execution count
    pub fn task_count(&self, task_name: &str) -> usize {
        self.task_counts
            .lock()
            .unwrap()
            .get(task_name)
            .copied()
            .unwrap_or(0)
    }

    /// Gets task error count
    pub fn error_count(&self, task_name: &str) -> usize {
        self.task_errors
            .lock()
            .unwrap()
            .get(task_name)
            .copied()
            .unwrap_or(0)
    }

    /// Gets task success rate
    pub fn success_rate(&self, task_name: &str) -> f64 {
        let total = self.task_count(task_name);
        if total == 0 {
            return 100.0;
        }
        let errors = self.error_count(task_name);
        ((total - errors) as f64 / total as f64) * 100.0
    }

    /// Gets mean duration for a task
    pub fn mean_duration(&self, task_name: &str) -> f64 {
        self.task_durations
            .lock()
            .unwrap()
            .get(task_name)
            .map(|h| h.mean())
            .unwrap_or(0.0)
    }

    /// Gets percentile duration for a task
    pub fn percentile_duration(&self, task_name: &str, percentile: f64) -> f64 {
        self.task_durations
            .lock()
            .unwrap()
            .get(task_name)
            .map(|h| h.percentile(percentile))
            .unwrap_or(0.0)
    }

    /// Gets throughput (tasks per second) for a task
    pub fn throughput(&self, task_name: &str) -> f64 {
        let elapsed = self.start_time.elapsed().as_secs_f64();
        if elapsed == 0.0 {
            return 0.0;
        }
        self.task_count(task_name) as f64 / elapsed
    }

    /// Gets all task names
    pub fn task_names(&self) -> Vec<String> {
        self.task_counts
            .lock()
            .expect("lock should not be poisoned")
            .keys()
            .cloned()
            .collect()
    }

    /// Generates a summary report
    pub fn summary(&self, task_name: &str) -> String {
        let count = self.task_count(task_name);
        let errors = self.error_count(task_name);
        let success_rate = self.success_rate(task_name);
        let mean = self.mean_duration(task_name);
        let p50 = self.percentile_duration(task_name, 50.0);
        let p95 = self.percentile_duration(task_name, 95.0);
        let p99 = self.percentile_duration(task_name, 99.0);
        let throughput = self.throughput(task_name);

        format!(
            "Task Metrics: {}\n\
             - Total Executions: {}\n\
             - Errors: {} ({:.2}% success rate)\n\
             - Mean Duration: {:.2}ms\n\
             - P50 Duration: {:.2}ms\n\
             - P95 Duration: {:.2}ms\n\
             - P99 Duration: {:.2}ms\n\
             - Throughput: {:.2} tasks/sec",
            task_name, count, errors, success_rate, mean, p50, p95, p99, throughput
        )
    }

    /// Resets all metrics
    pub fn reset(&self) {
        self.task_durations
            .lock()
            .expect("lock should not be poisoned")
            .clear();
        self.task_counts
            .lock()
            .expect("lock should not be poisoned")
            .clear();
        self.task_errors
            .lock()
            .expect("lock should not be poisoned")
            .clear();
        self.time_series
            .lock()
            .expect("lock should not be poisoned")
            .clear();
    }
}

impl Default for MetricsAggregator {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for MetricsAggregator {
    fn clone(&self) -> Self {
        Self {
            task_durations: Arc::clone(&self.task_durations),
            task_counts: Arc::clone(&self.task_counts),
            task_errors: Arc::clone(&self.task_errors),
            time_series: Arc::clone(&self.time_series),
            start_time: self.start_time,
        }
    }
}
