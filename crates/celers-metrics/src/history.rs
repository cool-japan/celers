//! Metric history, time-series analysis, scaling, cost, forecasting,
//! correlation, windowed stats, exponential smoothing, cost optimization,
//! and cardinality protection.

use crate::backends::CurrentMetrics;
use crate::slo::calculate_percentile;
use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::{SystemTime, UNIX_EPOCH};

// ============================================================================
// Metric History and Time-Series Analysis
// ============================================================================

/// A time-stamped metric sample for historical tracking
#[derive(Debug, Clone)]
pub struct MetricSample {
    /// Unix timestamp in seconds
    pub timestamp: u64,
    /// Metric value
    pub value: f64,
}

/// Snapshot of metric history statistics
#[derive(Debug, Clone)]
pub struct MetricHistorySnapshot {
    /// Number of samples
    pub count: usize,
    /// Minimum value
    pub min: f64,
    /// Maximum value
    pub max: f64,
    /// Mean value
    pub mean: f64,
    /// Variance
    pub variance: f64,
    /// Standard deviation
    pub std_dev: f64,
    /// Trend (rate of change per second)
    pub trend: Option<f64>,
    /// Latest value
    pub latest: Option<f64>,
}

impl Default for MetricHistorySnapshot {
    fn default() -> Self {
        Self {
            count: 0,
            min: 0.0,
            max: 0.0,
            mean: 0.0,
            variance: 0.0,
            std_dev: 0.0,
            trend: None,
            latest: None,
        }
    }
}

/// Time-series history tracker for metrics
#[derive(Debug)]
pub struct MetricHistory {
    samples: Mutex<VecDeque<MetricSample>>,
    max_samples: usize,
}

impl MetricHistory {
    /// Create a new metric history tracker with a maximum number of samples
    pub fn new(max_samples: usize) -> Self {
        Self {
            samples: Mutex::new(VecDeque::with_capacity(max_samples)),
            max_samples,
        }
    }

    /// Record a new sample with current timestamp
    pub fn record(&self, value: f64) {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let sample = MetricSample { timestamp, value };

        let mut samples = self.samples.lock().unwrap();
        if samples.len() >= self.max_samples {
            samples.pop_front();
        }
        samples.push_back(sample);
    }

    /// Get all samples as a vector
    pub fn get_samples(&self) -> Vec<MetricSample> {
        self.samples.lock().unwrap().iter().cloned().collect()
    }

    /// Get the most recent sample
    pub fn latest(&self) -> Option<MetricSample> {
        self.samples.lock().unwrap().back().cloned()
    }

    /// Calculate the trend (rate of change per second)
    pub fn trend(&self) -> Option<f64> {
        let samples = self.samples.lock().unwrap();
        if samples.len() < 2 {
            return None;
        }

        let first = samples.front().unwrap();
        let last = samples.back().unwrap();

        let time_delta = (last.timestamp - first.timestamp) as f64;
        if time_delta == 0.0 {
            return None;
        }

        let value_delta = last.value - first.value;
        Some(value_delta / time_delta)
    }

    /// Calculate moving average over all samples
    pub fn moving_average(&self) -> Option<f64> {
        let samples = self.samples.lock().unwrap();
        if samples.is_empty() {
            return None;
        }

        let sum: f64 = samples.iter().map(|s| s.value).sum();
        Some(sum / samples.len() as f64)
    }

    /// Calculate moving average over a specific window size
    pub fn moving_average_window(&self, window: usize) -> Option<f64> {
        let samples = self.samples.lock().unwrap();
        if samples.is_empty() {
            return None;
        }

        let window_size = window.min(samples.len());
        let start = samples.len().saturating_sub(window_size);
        let sum: f64 = samples.iter().skip(start).map(|s| s.value).sum();
        Some(sum / window_size as f64)
    }

    /// Record multiple samples at once (more efficient than individual records)
    pub fn record_batch(&self, values: &[(u64, f64)]) {
        let mut samples = self.samples.lock().unwrap();
        for (timestamp, value) in values {
            let sample = MetricSample {
                timestamp: *timestamp,
                value: *value,
            };

            if samples.len() >= self.max_samples {
                samples.pop_front();
            }
            samples.push_back(sample);
        }
    }

    /// Get a comprehensive snapshot of all statistics in a single lock acquisition
    pub fn snapshot(&self) -> MetricHistorySnapshot {
        let samples = self.samples.lock().unwrap();

        if samples.is_empty() {
            return MetricHistorySnapshot::default();
        }

        let values: Vec<f64> = samples.iter().map(|s| s.value).collect();
        let sum: f64 = values.iter().sum();
        let count = values.len();
        let mean = sum / count as f64;

        let min = values
            .iter()
            .copied()
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();
        let max = values
            .iter()
            .copied()
            .max_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap();

        let variance = if count > 1 {
            let sq_diff_sum: f64 = values.iter().map(|v| (v - mean).powi(2)).sum();
            sq_diff_sum / (count - 1) as f64
        } else {
            0.0
        };

        let std_dev = variance.sqrt();

        let trend = if samples.len() >= 2 {
            let first = samples.front().unwrap();
            let last = samples.back().unwrap();
            let time_delta = (last.timestamp - first.timestamp) as f64;
            if time_delta > 0.0 {
                Some((last.value - first.value) / time_delta)
            } else {
                None
            }
        } else {
            None
        };

        MetricHistorySnapshot {
            count,
            min,
            max,
            mean,
            variance,
            std_dev,
            trend,
            latest: samples.back().map(|s| s.value),
        }
    }

    /// Get the minimum value in history
    pub fn min(&self) -> Option<f64> {
        let samples = self.samples.lock().unwrap();
        samples
            .iter()
            .map(|s| s.value)
            .min_by(|a, b| a.partial_cmp(b).unwrap())
    }

    /// Get the maximum value in history
    pub fn max(&self) -> Option<f64> {
        let samples = self.samples.lock().unwrap();
        samples
            .iter()
            .map(|s| s.value)
            .max_by(|a, b| a.partial_cmp(b).unwrap())
    }

    /// Clear all samples
    pub fn clear(&self) {
        self.samples.lock().unwrap().clear();
    }

    /// Get number of samples
    pub fn len(&self) -> usize {
        self.samples.lock().unwrap().len()
    }

    /// Check if history is empty
    pub fn is_empty(&self) -> bool {
        self.samples.lock().unwrap().is_empty()
    }
}

// ============================================================================
// Auto-Scaling Recommendations
// ============================================================================

/// Auto-scaling recommendation based on current metrics
#[derive(Debug, Clone, PartialEq)]
pub enum ScalingRecommendation {
    /// Scale up by the specified number of workers
    ScaleUp { workers: usize, reason: String },
    /// Scale down by the specified number of workers
    ScaleDown { workers: usize, reason: String },
    /// No scaling needed
    NoChange,
}

/// Configuration for auto-scaling recommendations
#[derive(Debug, Clone)]
pub struct AutoScalingConfig {
    /// Target queue size per worker
    pub target_queue_per_worker: f64,
    /// Minimum number of workers
    pub min_workers: usize,
    /// Maximum number of workers
    pub max_workers: usize,
    /// Worker utilization threshold for scaling up (0.0-1.0)
    pub scale_up_threshold: f64,
    /// Worker utilization threshold for scaling down (0.0-1.0)
    pub scale_down_threshold: f64,
    /// Minimum time between scaling decisions (seconds)
    pub cooldown_seconds: u64,
}

impl Default for AutoScalingConfig {
    fn default() -> Self {
        Self {
            target_queue_per_worker: 10.0,
            min_workers: 1,
            max_workers: 100,
            scale_up_threshold: 0.8,
            scale_down_threshold: 0.3,
            cooldown_seconds: 300, // 5 minutes
        }
    }
}

impl AutoScalingConfig {
    /// Create a new auto-scaling configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set target queue size per worker
    pub fn with_target_queue_per_worker(mut self, target: f64) -> Self {
        self.target_queue_per_worker = target;
        self
    }

    /// Set minimum workers
    pub fn with_min_workers(mut self, min: usize) -> Self {
        self.min_workers = min;
        self
    }

    /// Set maximum workers
    pub fn with_max_workers(mut self, max: usize) -> Self {
        self.max_workers = max;
        self
    }

    /// Set scale-up threshold
    pub fn with_scale_up_threshold(mut self, threshold: f64) -> Self {
        self.scale_up_threshold = threshold.clamp(0.0, 1.0);
        self
    }

    /// Set scale-down threshold
    pub fn with_scale_down_threshold(mut self, threshold: f64) -> Self {
        self.scale_down_threshold = threshold.clamp(0.0, 1.0);
        self
    }

    /// Set cooldown period
    pub fn with_cooldown_seconds(mut self, seconds: u64) -> Self {
        self.cooldown_seconds = seconds;
        self
    }
}

/// Generate auto-scaling recommendation based on current metrics
pub fn recommend_scaling(config: &AutoScalingConfig) -> ScalingRecommendation {
    let metrics = CurrentMetrics::capture();

    let current_workers = metrics.active_workers as usize;
    if current_workers == 0 {
        return ScalingRecommendation::ScaleUp {
            workers: config.min_workers,
            reason: "No workers currently active".to_string(),
        };
    }

    let queue_size = metrics.queue_size;
    let processing = metrics.processing_queue_size;

    // Calculate utilization
    let busy_ratio = if current_workers > 0 {
        (processing / metrics.active_workers).min(1.0)
    } else {
        0.0
    };

    // Check if queue is growing too large
    let queue_per_worker = queue_size / metrics.active_workers;
    if queue_per_worker > config.target_queue_per_worker * 2.0 {
        let additional_workers_needed = ((queue_size / config.target_queue_per_worker).ceil()
            as usize)
            .saturating_sub(current_workers)
            .min(config.max_workers - current_workers);

        if additional_workers_needed > 0 && current_workers < config.max_workers {
            return ScalingRecommendation::ScaleUp {
                workers: additional_workers_needed,
                reason: format!(
                    "Queue size ({:.0}) exceeds target ({:.0} per worker)",
                    queue_size, config.target_queue_per_worker
                ),
            };
        }
    }

    // Check utilization for scaling up
    if busy_ratio > config.scale_up_threshold && current_workers < config.max_workers {
        let workers_to_add = (current_workers as f64 * 0.5).ceil() as usize; // Scale by 50%
        let workers_to_add = workers_to_add
            .max(1)
            .min(config.max_workers - current_workers);

        return ScalingRecommendation::ScaleUp {
            workers: workers_to_add,
            reason: format!(
                "High worker utilization ({:.1}% > {:.1}%)",
                busy_ratio * 100.0,
                config.scale_up_threshold * 100.0
            ),
        };
    }

    // Check utilization for scaling down
    if busy_ratio < config.scale_down_threshold
        && queue_size < config.target_queue_per_worker
        && current_workers > config.min_workers
    {
        let workers_to_remove = (current_workers as f64 * 0.3).ceil() as usize; // Scale down by 30%
        let workers_to_remove = workers_to_remove
            .max(1)
            .min(current_workers - config.min_workers);

        return ScalingRecommendation::ScaleDown {
            workers: workers_to_remove,
            reason: format!(
                "Low worker utilization ({:.1}% < {:.1}%) and small queue ({:.0})",
                busy_ratio * 100.0,
                config.scale_down_threshold * 100.0,
                queue_size
            ),
        };
    }

    ScalingRecommendation::NoChange
}

// ============================================================================
// Cost Estimation
// ============================================================================

/// Cost estimation configuration
#[derive(Debug, Clone)]
pub struct CostConfig {
    /// Cost per worker-hour (e.g., EC2 instance cost)
    pub cost_per_worker_hour: f64,
    /// Cost per million task executions
    pub cost_per_million_tasks: f64,
    /// Cost per GB of data processed
    pub cost_per_gb: f64,
}

impl Default for CostConfig {
    fn default() -> Self {
        Self {
            cost_per_worker_hour: 0.10,  // $0.10/hour default
            cost_per_million_tasks: 1.0, // $1.00 per million tasks
            cost_per_gb: 0.01,           // $0.01 per GB
        }
    }
}

impl CostConfig {
    /// Create a new cost configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set cost per worker-hour
    pub fn with_cost_per_worker_hour(mut self, cost: f64) -> Self {
        self.cost_per_worker_hour = cost;
        self
    }

    /// Set cost per million tasks
    pub fn with_cost_per_million_tasks(mut self, cost: f64) -> Self {
        self.cost_per_million_tasks = cost;
        self
    }

    /// Set cost per GB
    pub fn with_cost_per_gb(mut self, cost: f64) -> Self {
        self.cost_per_gb = cost;
        self
    }
}

/// Cost breakdown estimate
#[derive(Debug, Clone)]
pub struct CostEstimate {
    /// Estimated compute cost
    pub compute_cost: f64,
    /// Estimated task execution cost
    pub task_cost: f64,
    /// Estimated data transfer cost
    pub data_cost: f64,
    /// Total estimated cost
    pub total_cost: f64,
}

/// Estimate costs based on metrics and time period
pub fn estimate_costs(config: &CostConfig, time_period_hours: f64) -> CostEstimate {
    let metrics = CurrentMetrics::capture();

    // Compute cost: workers * hours * cost_per_hour
    let compute_cost = metrics.active_workers * time_period_hours * config.cost_per_worker_hour;

    // Task cost: tasks * (cost_per_million / 1_000_000)
    let total_tasks = metrics.tasks_completed + metrics.tasks_failed;
    let task_cost = total_tasks * (config.cost_per_million_tasks / 1_000_000.0);

    // Data cost: Estimate from result sizes (if tracked)
    // This is a placeholder - actual implementation would need result size tracking
    let data_cost = 0.0;

    let total_cost = compute_cost + task_cost + data_cost;

    CostEstimate {
        compute_cost,
        task_cost,
        data_cost,
        total_cost,
    }
}

/// Calculate cost per task
pub fn cost_per_task(config: &CostConfig, time_period_hours: f64) -> f64 {
    let metrics = CurrentMetrics::capture();
    let estimate = estimate_costs(config, time_period_hours);

    let total_tasks = metrics.tasks_completed + metrics.tasks_failed;
    if total_tasks == 0.0 {
        return 0.0;
    }

    estimate.total_cost / total_tasks
}

// ============================================================================
// Metric Forecasting
// ============================================================================

/// Simple linear regression forecast
#[derive(Debug, Clone)]
pub struct ForecastResult {
    /// Predicted value at the forecast time
    pub predicted_value: f64,
    /// Confidence in prediction (0.0-1.0)
    pub confidence: f64,
    /// Trend direction (positive = increasing, negative = decreasing)
    pub trend: f64,
}

/// Forecast future metric value using linear regression on historical data
pub fn forecast_metric(history: &MetricHistory, seconds_ahead: u64) -> Option<ForecastResult> {
    let samples = history.get_samples();
    if samples.len() < 3 {
        return None; // Need at least 3 samples for reasonable forecast
    }

    // Simple linear regression: y = mx + b
    let n = samples.len() as f64;
    let sum_x: f64 = samples.iter().map(|s| s.timestamp as f64).sum();
    let sum_y: f64 = samples.iter().map(|s| s.value).sum();
    let sum_xy: f64 = samples.iter().map(|s| s.timestamp as f64 * s.value).sum();
    let sum_x2: f64 = samples.iter().map(|s| (s.timestamp as f64).powi(2)).sum();

    let denominator = n * sum_x2 - sum_x.powi(2);
    if denominator.abs() < 1e-10 {
        return None; // Avoid division by zero
    }

    let slope = (n * sum_xy - sum_x * sum_y) / denominator;
    let intercept = (sum_y - slope * sum_x) / n;

    // Forecast value
    let latest_timestamp = samples.last()?.timestamp;
    let future_timestamp = latest_timestamp + seconds_ahead;
    let predicted_value = slope * future_timestamp as f64 + intercept;

    // Calculate confidence based on R^2
    let mean_y = sum_y / n;
    let ss_tot: f64 = samples.iter().map(|s| (s.value - mean_y).powi(2)).sum();
    let ss_res: f64 = samples
        .iter()
        .map(|s| {
            let predicted = slope * s.timestamp as f64 + intercept;
            (s.value - predicted).powi(2)
        })
        .sum();

    let r_squared = if ss_tot > 0.0 {
        1.0 - (ss_res / ss_tot)
    } else {
        0.0
    };

    Some(ForecastResult {
        predicted_value,
        confidence: r_squared.clamp(0.0, 1.0),
        trend: slope,
    })
}

// ============================================================================
// Metric Cardinality Protection
// ============================================================================

/// Cardinality limiter to prevent label explosion
#[derive(Debug)]
pub struct CardinalityLimiter {
    seen_labels: std::sync::Mutex<std::collections::HashSet<String>>,
    max_cardinality: usize,
}

impl CardinalityLimiter {
    /// Create a new cardinality limiter with maximum allowed unique label combinations
    pub fn new(max_cardinality: usize) -> Self {
        Self {
            seen_labels: std::sync::Mutex::new(std::collections::HashSet::new()),
            max_cardinality,
        }
    }

    /// Check if a label combination is allowed (within cardinality limit)
    /// Returns true if the label should be recorded, false if it would exceed the limit
    pub fn check_and_record(&self, label_key: &str) -> bool {
        let mut seen = self.seen_labels.lock().unwrap();

        if seen.contains(label_key) {
            return true; // Already seen, always allowed
        }

        if seen.len() >= self.max_cardinality {
            return false; // Would exceed limit
        }

        seen.insert(label_key.to_string());
        true
    }

    /// Get current cardinality (number of unique label combinations)
    pub fn current_cardinality(&self) -> usize {
        self.seen_labels.lock().unwrap().len()
    }

    /// Check if cardinality limit has been reached
    pub fn is_at_limit(&self) -> bool {
        self.current_cardinality() >= self.max_cardinality
    }

    /// Reset the limiter
    pub fn reset(&self) {
        self.seen_labels.lock().unwrap().clear();
    }
}

// ============================================================================
// Metric Correlation Analysis
// ============================================================================

/// Correlation coefficient between two metrics
#[derive(Debug, Clone)]
pub struct CorrelationResult {
    /// Pearson correlation coefficient (-1.0 to 1.0)
    pub coefficient: f64,
    /// Statistical significance (p-value approximation)
    pub significance: f64,
    /// Number of samples used in calculation
    pub sample_count: usize,
}

/// Calculate Pearson correlation coefficient between two metric histories
pub fn calculate_correlation(
    history_a: &MetricHistory,
    history_b: &MetricHistory,
) -> Option<CorrelationResult> {
    let samples_a = history_a.get_samples();
    let samples_b = history_b.get_samples();

    if samples_a.len() < 3 || samples_b.len() < 3 {
        return None; // Need at least 3 samples
    }

    // Align samples by timestamp (use only overlapping timestamps)
    let mut paired_values: Vec<(f64, f64)> = Vec::new();

    for sample_a in &samples_a {
        if let Some(sample_b) = samples_b.iter().find(|s| s.timestamp == sample_a.timestamp) {
            paired_values.push((sample_a.value, sample_b.value));
        }
    }

    if paired_values.len() < 3 {
        return None; // Not enough overlapping samples
    }

    let n = paired_values.len() as f64;
    let sum_x: f64 = paired_values.iter().map(|(x, _)| x).sum();
    let sum_y: f64 = paired_values.iter().map(|(_, y)| y).sum();
    let sum_xy: f64 = paired_values.iter().map(|(x, y)| x * y).sum();
    let sum_x2: f64 = paired_values.iter().map(|(x, _)| x.powi(2)).sum();
    let sum_y2: f64 = paired_values.iter().map(|(_, y)| y.powi(2)).sum();

    let numerator = n * sum_xy - sum_x * sum_y;
    let denominator_x = (n * sum_x2 - sum_x.powi(2)).sqrt();
    let denominator_y = (n * sum_y2 - sum_y.powi(2)).sqrt();

    if denominator_x.abs() < 1e-10 || denominator_y.abs() < 1e-10 {
        return None; // Avoid division by zero
    }

    let coefficient = numerator / (denominator_x * denominator_y);

    // Approximate significance using t-statistic
    // For large samples, |r| > 2/sqrt(n) is roughly significant at p < 0.05
    let significance_threshold = 2.0 / (n.sqrt());
    let significance = if coefficient.abs() > significance_threshold {
        0.05 // Approximation: significant
    } else {
        0.5 // Approximation: not significant
    };

    Some(CorrelationResult {
        coefficient: coefficient.clamp(-1.0, 1.0),
        significance,
        sample_count: paired_values.len(),
    })
}

/// Detect if two metrics are strongly correlated
pub fn are_metrics_correlated(
    history_a: &MetricHistory,
    history_b: &MetricHistory,
    threshold: f64,
) -> bool {
    match calculate_correlation(history_a, history_b) {
        Some(result) => result.coefficient.abs() >= threshold,
        None => false,
    }
}

// ============================================================================
// Metric Aggregation Windows
// ============================================================================

/// Time window for metric aggregation
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum TimeWindow {
    /// Last 1 minute
    OneMinute,
    /// Last 5 minutes
    FiveMinutes,
    /// Last 15 minutes
    FifteenMinutes,
    /// Last 1 hour
    OneHour,
    /// Last 24 hours
    OneDay,
}

impl TimeWindow {
    /// Get duration in seconds
    pub fn as_seconds(&self) -> u64 {
        match self {
            TimeWindow::OneMinute => 60,
            TimeWindow::FiveMinutes => 300,
            TimeWindow::FifteenMinutes => 900,
            TimeWindow::OneHour => 3600,
            TimeWindow::OneDay => 86400,
        }
    }
}

/// Windowed metric statistics
#[derive(Debug, Clone)]
pub struct WindowedStats {
    /// Mean value in the window
    pub mean: f64,
    /// Minimum value in the window
    pub min: f64,
    /// Maximum value in the window
    pub max: f64,
    /// Standard deviation in the window
    pub std_dev: f64,
    /// 50th percentile (median)
    pub p50: f64,
    /// 95th percentile
    pub p95: f64,
    /// 99th percentile
    pub p99: f64,
    /// Number of samples in the window
    pub sample_count: usize,
}

/// Calculate statistics for a specific time window
pub fn calculate_windowed_stats(
    history: &MetricHistory,
    window: TimeWindow,
) -> Option<WindowedStats> {
    let samples = history.get_samples();
    if samples.is_empty() {
        return None;
    }

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let window_start = now.saturating_sub(window.as_seconds());

    // Filter samples within the window
    let windowed_samples: Vec<f64> = samples
        .iter()
        .filter(|s| s.timestamp >= window_start)
        .map(|s| s.value)
        .collect();

    if windowed_samples.is_empty() {
        return None;
    }

    let count = windowed_samples.len();
    let sum: f64 = windowed_samples.iter().sum();
    let mean = sum / count as f64;

    let min = windowed_samples
        .iter()
        .min_by(|a, b| a.partial_cmp(b).unwrap())
        .copied()?;

    let max = windowed_samples
        .iter()
        .max_by(|a, b| a.partial_cmp(b).unwrap())
        .copied()?;

    let variance: f64 = windowed_samples
        .iter()
        .map(|v| (v - mean).powi(2))
        .sum::<f64>()
        / count as f64;

    let std_dev = variance.sqrt();

    // Calculate percentiles (requires sorted data)
    let mut sorted_samples = windowed_samples.clone();
    sorted_samples.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let p50 = calculate_percentile(&sorted_samples, 0.50).unwrap_or(mean);
    let p95 = calculate_percentile(&sorted_samples, 0.95).unwrap_or(max);
    let p99 = calculate_percentile(&sorted_samples, 0.99).unwrap_or(max);

    Some(WindowedStats {
        mean,
        min,
        max,
        std_dev,
        p50,
        p95,
        p99,
        sample_count: count,
    })
}

// ============================================================================
// Exponential Smoothing Forecasting (Holt-Winters)
// ============================================================================

/// Exponential smoothing forecast result with seasonality support
#[derive(Debug, Clone)]
pub struct ExponentialForecast {
    /// Predicted value at the forecast time
    pub predicted_value: f64,
    /// Level component (deseasonalized average)
    pub level: f64,
    /// Trend component (rate of change)
    pub trend: f64,
    /// Confidence score (0.0-1.0)
    pub confidence: f64,
}

/// Exponential smoothing parameters
#[derive(Debug, Clone)]
pub struct ExponentialSmoothingConfig {
    /// Alpha: smoothing factor for level (0.0-1.0)
    pub alpha: f64,
    /// Beta: smoothing factor for trend (0.0-1.0)
    pub beta: f64,
}

impl Default for ExponentialSmoothingConfig {
    fn default() -> Self {
        Self {
            alpha: 0.3, // Standard value for level smoothing
            beta: 0.1,  // Standard value for trend smoothing
        }
    }
}

impl ExponentialSmoothingConfig {
    /// Create new config with custom parameters
    pub fn new(alpha: f64, beta: f64) -> Self {
        Self {
            alpha: alpha.clamp(0.0, 1.0),
            beta: beta.clamp(0.0, 1.0),
        }
    }
}

/// Forecast using double exponential smoothing (Holt's method)
/// More accurate than linear regression for trending data
pub fn forecast_exponential(
    history: &MetricHistory,
    seconds_ahead: u64,
    config: &ExponentialSmoothingConfig,
) -> Option<ExponentialForecast> {
    let samples = history.get_samples();
    if samples.len() < 3 {
        return None;
    }

    let values: Vec<f64> = samples.iter().map(|s| s.value).collect();

    // Initialize level and trend
    let mut level = values[0];
    let mut trend = if values.len() > 1 {
        values[1] - values[0]
    } else {
        0.0
    };

    // Apply double exponential smoothing
    for &value in values.iter().skip(1) {
        let prev_level = level;
        level = config.alpha * value + (1.0 - config.alpha) * (level + trend);
        trend = config.beta * (level - prev_level) + (1.0 - config.beta) * trend;
    }

    // Forecast ahead
    let avg_time_delta = if samples.len() > 1 {
        let total_time = (samples[samples.len() - 1].timestamp - samples[0].timestamp) as f64;
        let avg_delta = total_time / (samples.len() - 1) as f64;
        avg_delta.max(1.0) // Minimum 1 second to prevent division issues
    } else {
        1.0
    };

    let periods_ahead = (seconds_ahead as f64 / avg_time_delta).min(1000.0); // Cap at 1000 periods
    let predicted_value = level + trend * periods_ahead;

    // Calculate confidence based on recent forecast accuracy
    let recent_errors: Vec<f64> = values
        .windows(2)
        .map(|w| {
            let forecast = w[0] + trend;
            (w[1] - forecast).abs() / w[1].max(1.0)
        })
        .collect();

    let mean_error = if !recent_errors.is_empty() {
        recent_errors.iter().sum::<f64>() / recent_errors.len() as f64
    } else {
        0.0
    };

    let confidence = (1.0 - mean_error.min(1.0)).max(0.0);

    Some(ExponentialForecast {
        predicted_value,
        level,
        trend,
        confidence,
    })
}

// ============================================================================
// Cost Optimization Recommendations
// ============================================================================

/// Cost optimization recommendation
#[derive(Debug, Clone, PartialEq)]
pub enum CostOptimization {
    /// Scale down workers to save costs
    ScaleDown {
        /// Current worker count
        current_workers: usize,
        /// Recommended worker count
        recommended_workers: usize,
        /// Estimated monthly savings
        estimated_savings: f64,
    },
    /// Increase task batching to reduce overhead
    IncreaseBatching {
        /// Current average batch size
        current_batch_size: f64,
        /// Recommended batch size
        recommended_batch_size: usize,
        /// Estimated cost reduction percentage
        cost_reduction_percent: f64,
    },
    /// Switch to spot/preemptible instances
    UseSpotInstances {
        /// Estimated monthly savings
        estimated_savings: f64,
        /// Cost reduction percentage
        cost_reduction_percent: f64,
    },
    /// Optimize queue polling frequency
    OptimizePolling {
        /// Recommended poll interval in seconds
        recommended_poll_interval: u64,
        /// Estimated cost reduction percentage
        cost_reduction_percent: f64,
    },
    /// No optimization needed - costs are optimal
    NoOptimizationNeeded,
}

/// Configuration for cost optimization analysis
#[derive(Debug, Clone)]
pub struct CostOptimizationConfig {
    /// Current worker count
    pub current_workers: usize,
    /// Average worker utilization (0.0-1.0)
    pub avg_utilization: f64,
    /// Average batch size
    pub avg_batch_size: f64,
    /// Cost per worker per hour
    pub cost_per_worker_hour: f64,
    /// Spot instance discount percentage (0.0-1.0)
    pub spot_discount: f64,
    /// Current poll interval in seconds
    pub current_poll_interval: u64,
}

impl Default for CostOptimizationConfig {
    fn default() -> Self {
        Self {
            current_workers: 1,
            avg_utilization: 0.5,
            avg_batch_size: 1.0,
            cost_per_worker_hour: 0.10,
            spot_discount: 0.70, // 70% discount typical for spot instances
            current_poll_interval: 1,
        }
    }
}

/// Analyze current metrics and recommend cost optimizations
pub fn recommend_cost_optimizations(config: &CostOptimizationConfig) -> Vec<CostOptimization> {
    let mut recommendations = Vec::new();

    // 1. Check if we can scale down workers (low utilization)
    if config.avg_utilization < 0.3 && config.current_workers > 1 {
        let target_utilization = 0.7;
        let recommended_workers = ((config.current_workers as f64 * config.avg_utilization)
            / target_utilization)
            .ceil() as usize;
        let recommended_workers = recommended_workers.max(1);

        if recommended_workers < config.current_workers {
            let workers_to_remove = config.current_workers - recommended_workers;
            let monthly_hours = 24.0 * 30.0;
            let estimated_savings =
                workers_to_remove as f64 * config.cost_per_worker_hour * monthly_hours;

            recommendations.push(CostOptimization::ScaleDown {
                current_workers: config.current_workers,
                recommended_workers,
                estimated_savings,
            });
        }
    }

    // 2. Check if batch size can be increased (small batches = high overhead)
    if config.avg_batch_size < 10.0 {
        let recommended_batch_size = 50;
        let overhead_reduction = (recommended_batch_size as f64 / config.avg_batch_size) * 0.1;
        let cost_reduction_percent = overhead_reduction.min(0.3) * 100.0; // Cap at 30%

        recommendations.push(CostOptimization::IncreaseBatching {
            current_batch_size: config.avg_batch_size,
            recommended_batch_size,
            cost_reduction_percent,
        });
    }

    // 3. Check if spot instances would be beneficial (stable workload)
    if config.avg_utilization > 0.5 && config.avg_utilization < 0.9 {
        let monthly_hours = 24.0 * 30.0;
        let current_monthly_cost =
            config.current_workers as f64 * config.cost_per_worker_hour * monthly_hours;
        let estimated_savings = current_monthly_cost * config.spot_discount;
        let cost_reduction_percent = config.spot_discount * 100.0;

        recommendations.push(CostOptimization::UseSpotInstances {
            estimated_savings,
            cost_reduction_percent,
        });
    }

    // 4. Check if polling frequency can be optimized (high frequency polling = waste)
    if config.current_poll_interval < 5 && config.avg_utilization < 0.6 {
        let recommended_poll_interval = 10;
        let cpu_reduction =
            (config.current_poll_interval as f64 / recommended_poll_interval as f64) * 0.15; // Polling typically uses 15% of CPU
        let cost_reduction_percent = cpu_reduction * 100.0;

        recommendations.push(CostOptimization::OptimizePolling {
            recommended_poll_interval,
            cost_reduction_percent,
        });
    }

    if recommendations.is_empty() {
        recommendations.push(CostOptimization::NoOptimizationNeeded);
    }

    recommendations
}
