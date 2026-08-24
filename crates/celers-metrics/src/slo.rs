//! SLO/SLA tracking, anomaly detection, and percentile calculations.

// ============================================================================
// SLO/SLA Tracking
// ============================================================================

/// SLO (Service Level Objective) target
#[derive(Debug, Clone)]
pub struct SloTarget {
    /// Target success rate (0.0 to 1.0)
    pub success_rate: f64,
    /// Target latency in seconds (p95 or p99)
    pub latency_seconds: f64,
    /// Target throughput (tasks per second)
    pub throughput: f64,
}

impl Default for SloTarget {
    fn default() -> Self {
        Self {
            success_rate: 0.99,   // 99% success rate
            latency_seconds: 5.0, // 5 seconds p95 latency
            throughput: 100.0,    // 100 tasks per second
        }
    }
}

/// SLO compliance status (snapshot, point-in-time check).
///
/// For stateful, rolling-window SLO attainment and error-budget burn-rate
/// tracking see [`crate::slo_tracker::SloTracker`] and its richer
/// [`crate::slo_tracker::SloStatus`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SloComplianceStatus {
    /// Meeting SLO targets
    Compliant,
    /// Not meeting SLO targets
    NonCompliant,
    /// Insufficient data to determine compliance
    Unknown,
}

/// Check if current metrics meet SLO targets
pub fn check_slo_compliance(
    success_rate: f64,
    p95_latency_seconds: f64,
    throughput: f64,
    target: &SloTarget,
) -> SloComplianceStatus {
    if success_rate < 0.0 || p95_latency_seconds < 0.0 || throughput < 0.0 {
        return SloComplianceStatus::Unknown;
    }

    let meets_success = success_rate >= target.success_rate;
    let meets_latency = p95_latency_seconds <= target.latency_seconds;
    let meets_throughput = throughput >= target.throughput;

    if meets_success && meets_latency && meets_throughput {
        SloComplianceStatus::Compliant
    } else {
        SloComplianceStatus::NonCompliant
    }
}

/// Calculate error budget remaining (1.0 = 100% budget remaining)
pub fn calculate_error_budget(
    total_requests: f64,
    failed_requests: f64,
    target_success_rate: f64,
) -> f64 {
    if total_requests <= 0.0 {
        return 1.0; // Full budget if no requests
    }

    let allowed_failures = total_requests * (1.0 - target_success_rate);
    if allowed_failures <= 0.0 {
        // A 100% (or effectively 100%) objective allows zero failures. The
        // budget is untouched only if none actually occurred; otherwise it
        // is immediately blown. Falling through would compute
        // `failed_requests / 0.0`, which is `NaN` when there were no
        // failures either -- and `(1.0 - NaN).max(0.0)` silently evaluates
        // to `0.0` regardless of whether any failures actually happened,
        // reporting a fully-exhausted budget for a perfectly healthy
        // service. Mirrors `slo_tracker::ErrorBudget::compute`'s handling of
        // `allowed == 0`.
        return if failed_requests <= 0.0 { 1.0 } else { 0.0 };
    }
    let budget_used = failed_requests / allowed_failures;
    (1.0 - budget_used).max(0.0)
}

// ============================================================================
// Anomaly Detection Helpers
// ============================================================================

/// Statistical thresholds for anomaly detection
#[derive(Debug, Clone)]
pub struct AnomalyThreshold {
    /// Mean baseline value
    pub mean: f64,
    /// Standard deviation
    pub std_dev: f64,
    /// Number of standard deviations for anomaly detection
    pub sigma_threshold: f64,
}

impl AnomalyThreshold {
    /// Create a new anomaly threshold with mean, `std_dev`, and sigma threshold
    pub fn new(mean: f64, std_dev: f64, sigma_threshold: f64) -> Self {
        Self {
            mean,
            std_dev,
            sigma_threshold,
        }
    }

    /// Calculate threshold from a sample of values
    pub fn from_samples(samples: &[f64], sigma_threshold: f64) -> Option<Self> {
        if samples.is_empty() {
            return None;
        }

        let mean = samples.iter().sum::<f64>() / samples.len() as f64;
        let variance =
            samples.iter().map(|x| (x - mean).powi(2)).sum::<f64>() / samples.len() as f64;
        let std_dev = variance.sqrt();

        Some(Self {
            mean,
            std_dev,
            sigma_threshold,
        })
    }

    /// Check if a value is anomalous (outside threshold)
    pub fn is_anomalous(&self, value: f64) -> bool {
        let deviation = (value - self.mean).abs();
        deviation > self.std_dev * self.sigma_threshold
    }

    /// Get upper threshold
    pub fn upper_bound(&self) -> f64 {
        self.mean + (self.std_dev * self.sigma_threshold)
    }

    /// Get lower threshold
    pub fn lower_bound(&self) -> f64 {
        self.mean - (self.std_dev * self.sigma_threshold)
    }
}

/// Anomaly detection result
#[derive(Debug, Clone, PartialEq)]
pub enum AnomalyStatus {
    /// Normal value within expected range
    Normal,
    /// Value is abnormally high
    High,
    /// Value is abnormally low
    Low,
}

/// Detect anomalies in a metric value
pub fn detect_anomaly(value: f64, threshold: &AnomalyThreshold) -> AnomalyStatus {
    if value > threshold.upper_bound() {
        AnomalyStatus::High
    } else if value < threshold.lower_bound() {
        AnomalyStatus::Low
    } else {
        AnomalyStatus::Normal
    }
}

/// Exponential weighted moving average for trend detection
#[derive(Debug, Clone)]
pub struct MovingAverage {
    /// Current average value
    pub value: f64,
    /// Smoothing factor (0.0 to 1.0)
    pub alpha: f64,
}

impl MovingAverage {
    /// Create a new moving average with initial value and smoothing factor
    pub fn new(initial_value: f64, alpha: f64) -> Self {
        Self {
            value: initial_value,
            alpha: alpha.clamp(0.0, 1.0),
        }
    }

    /// Update with a new observation and return the new average
    pub fn update(&mut self, new_value: f64) -> f64 {
        self.value = self.alpha * new_value + (1.0 - self.alpha) * self.value;
        self.value
    }

    /// Get current average value
    pub fn get(&self) -> f64 {
        self.value
    }
}

/// Detect sudden spikes or drops in metrics
pub fn detect_spike(current: f64, baseline: f64, threshold_ratio: f64) -> bool {
    if baseline <= 0.0 {
        return false;
    }
    let ratio = current / baseline;
    ratio > threshold_ratio || ratio < (1.0 / threshold_ratio)
}

// ============================================================================
// Percentile Calculation Helpers
// ============================================================================

/// Calculate percentile from a sorted list of values
///
/// # Arguments
///
/// * `values` - Sorted list of values
/// * `percentile` - Percentile to calculate (0.0 to 1.0)
///
/// # Examples
///
/// ```
/// use celers_metrics::calculate_percentile;
///
/// let values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
///
/// let p50 = calculate_percentile(&values, 0.50).unwrap();
/// let p95 = calculate_percentile(&values, 0.95).unwrap();
/// let p99 = calculate_percentile(&values, 0.99).unwrap();
///
/// assert!((p50 - 5.5).abs() < 0.01);
/// assert!((p95 - 9.55).abs() < 0.01);
/// assert!((p99 - 9.91).abs() < 0.01);
/// ```
pub fn calculate_percentile(values: &[f64], percentile: f64) -> Option<f64> {
    if values.is_empty() || !(0.0..=1.0).contains(&percentile) {
        return None;
    }

    if values.len() == 1 {
        return Some(values[0]);
    }

    // Use the "R-7" method (default in R and NumPy)
    // Calculate the fractional index
    let index = percentile * (values.len() as f64 - 1.0);
    let lower_index = index.floor() as usize;
    let upper_index = index.ceil() as usize;
    let fraction = index - lower_index as f64;

    // Linear interpolation
    let lower_value = values[lower_index];
    let upper_value = values[upper_index];
    Some(lower_value + fraction * (upper_value - lower_value))
}

/// Batch percentile calculation for common percentiles (p50, p95, p99)
///
/// Returns a tuple of (p50, p95, p99)
///
/// # Examples
///
/// ```
/// use celers_metrics::calculate_percentiles;
///
/// let mut values = vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
/// values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
///
/// let (p50, p95, p99) = calculate_percentiles(&values).unwrap();
/// println!("p50: {:.2}, p95: {:.2}, p99: {:.2}", p50, p95, p99);
/// ```
pub fn calculate_percentiles(values: &[f64]) -> Option<(f64, f64, f64)> {
    if values.is_empty() {
        return None;
    }

    let p50 = calculate_percentile(values, 0.50)?;
    let p95 = calculate_percentile(values, 0.95)?;
    let p99 = calculate_percentile(values, 0.99)?;

    Some((p50, p95, p99))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_budget_full_at_100_percent_objective_with_no_failures() {
        // Regression: `allowed_failures` is exactly 0.0 at a 100% objective,
        // so `failed_requests / allowed_failures` was `0.0 / 0.0 == NaN`, and
        // `(1.0 - NaN).max(0.0)` silently evaluated to `0.0` -- reporting a
        // fully-exhausted budget for a perfectly healthy service.
        assert_eq!(calculate_error_budget(100.0, 0.0, 1.0), 1.0);
    }

    #[test]
    fn error_budget_blown_at_100_percent_objective_with_any_failure() {
        // At a 100% objective a single failure exhausts the (zero-sized)
        // budget; distinct from the untouched case above (both previously
        // collapsed to the same `0.0`, so this case could never be told
        // apart from "healthy").
        assert_eq!(calculate_error_budget(100.0, 1.0, 1.0), 0.0);
        assert_eq!(calculate_error_budget(100.0, 50.0, 1.0), 0.0);
    }

    #[test]
    fn error_budget_ordinary_objective_still_behaves_as_before() {
        // 99% objective over 1000 requests allows 10 failures.
        assert_eq!(calculate_error_budget(1000.0, 0.0, 0.99), 1.0);
        assert!((calculate_error_budget(1000.0, 5.0, 0.99) - 0.5).abs() < 1e-10);
        assert!(calculate_error_budget(1000.0, 10.0, 0.99).abs() < 1e-10);
        // Budget exceeded clamps to 0, not negative.
        assert_eq!(calculate_error_budget(1000.0, 20.0, 0.99), 0.0);
        // No requests yet: full budget regardless of objective.
        assert_eq!(calculate_error_budget(0.0, 0.0, 0.99), 1.0);
    }

    #[test]
    fn error_budget_is_never_nan() {
        for (total, failed, objective) in [
            (0.0, 0.0, 1.0),
            (100.0, 0.0, 1.0),
            (100.0, 1.0, 1.0),
            (100.0, 5.0, 0.99),
            (1.0, 1.0, 1.0),
        ] {
            let budget = calculate_error_budget(total, failed, objective);
            assert!(
                !budget.is_nan(),
                "calculate_error_budget({total}, {failed}, {objective}) was NaN"
            );
        }
    }
}
