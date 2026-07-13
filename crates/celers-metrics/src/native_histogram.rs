//! Native (dependency-free) Prometheus histogram implementation.
//!
//! This module provides a self-contained cumulative histogram that does not rely
//! on the external `prometheus` client. It supports configurable cumulative
//! buckets, thread-safe [`NativeHistogram::observe`], linear/exponential bucket
//! helper constructors, and Prometheus text-format exposition emitting
//! `<name>_bucket{le="..."}` (cumulative, including the mandatory `le="+Inf"`
//! bucket), `<name>_sum`, and `<name>_count` series.
//!
//! # Examples
//!
//! ```
//! use celers_metrics::NativeHistogram;
//!
//! let hist = NativeHistogram::new(
//!     "request_latency_seconds",
//!     "Request latency in seconds",
//!     vec![0.1, 0.5, 1.0],
//! )
//! .expect("valid buckets");
//!
//! hist.observe(0.3);
//! hist.observe(0.7);
//! hist.observe(2.0);
//!
//! assert_eq!(hist.count(), 3);
//! // 0.3 lands in the le="0.5" bucket; 0.7 in le="1.0"; 2.0 overflows to +Inf.
//! assert_eq!(hist.cumulative_count(0.5), 1);
//! assert_eq!(hist.cumulative_count(1.0), 2);
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex as StdMutex;

use crate::format_float;

/// Error returned when constructing a [`NativeHistogram`] with invalid buckets.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HistogramError {
    /// No finite upper bounds were supplied.
    EmptyBuckets,
    /// Bucket bounds were not strictly increasing.
    NonMonotonicBuckets,
    /// A bucket bound was not finite (NaN or infinite).
    NonFiniteBucket,
    /// A bucket helper was given a non-positive `count`.
    InvalidCount,
    /// An exponential bucket helper was given a non-positive `start` or a
    /// `factor` that is not strictly greater than `1.0`.
    InvalidExponentialParameters,
    /// A linear bucket helper was given a non-positive `width`.
    InvalidLinearParameters,
}

impl std::fmt::Display for HistogramError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::EmptyBuckets => "histogram requires at least one finite upper bound",
            Self::NonMonotonicBuckets => "histogram bucket bounds must be strictly increasing",
            Self::NonFiniteBucket => "histogram bucket bounds must be finite",
            Self::InvalidCount => "bucket count must be greater than zero",
            Self::InvalidExponentialParameters => {
                "exponential buckets require start > 0 and factor > 1.0"
            }
            Self::InvalidLinearParameters => "linear buckets require width > 0",
        };
        f.write_str(message)
    }
}

impl std::error::Error for HistogramError {}

/// Generate `count` linearly spaced upper bounds beginning at `start` and
/// increasing by `width`.
///
/// This mirrors the semantics of the Prometheus `LinearBuckets` helper: the
/// returned slice contains the explicit finite upper bounds only (the implicit
/// `+Inf` bucket is added automatically during exposition).
///
/// # Examples
///
/// ```
/// use celers_metrics::linear_buckets;
///
/// let buckets = linear_buckets(0.0, 5.0, 4).expect("valid parameters");
/// assert_eq!(buckets, vec![0.0, 5.0, 10.0, 15.0]);
/// ```
///
/// # Errors
///
/// Returns [`HistogramError::InvalidCount`] when `count == 0` and
/// [`HistogramError::InvalidLinearParameters`] when `width <= 0.0`.
pub fn linear_buckets(start: f64, width: f64, count: usize) -> Result<Vec<f64>, HistogramError> {
    if count == 0 {
        return Err(HistogramError::InvalidCount);
    }
    if width <= 0.0 || !width.is_finite() || !start.is_finite() {
        return Err(HistogramError::InvalidLinearParameters);
    }

    let mut bounds = Vec::with_capacity(count);
    let mut upper = start;
    for _ in 0..count {
        bounds.push(upper);
        upper += width;
    }
    Ok(bounds)
}

/// Generate `count` exponentially spaced upper bounds beginning at `start` and
/// multiplied by `factor` for each successive bucket.
///
/// This mirrors the semantics of the Prometheus `ExponentialBuckets` helper.
///
/// # Examples
///
/// ```
/// use celers_metrics::exponential_buckets;
///
/// let buckets = exponential_buckets(1.0, 2.0, 4).expect("valid parameters");
/// assert_eq!(buckets, vec![1.0, 2.0, 4.0, 8.0]);
/// ```
///
/// # Errors
///
/// Returns [`HistogramError::InvalidCount`] when `count == 0` and
/// [`HistogramError::InvalidExponentialParameters`] when `start <= 0.0` or
/// `factor <= 1.0`.
pub fn exponential_buckets(
    start: f64,
    factor: f64,
    count: usize,
) -> Result<Vec<f64>, HistogramError> {
    if count == 0 {
        return Err(HistogramError::InvalidCount);
    }
    if start <= 0.0 || !start.is_finite() || factor <= 1.0 || !factor.is_finite() {
        return Err(HistogramError::InvalidExponentialParameters);
    }

    let mut bounds = Vec::with_capacity(count);
    let mut upper = start;
    for _ in 0..count {
        bounds.push(upper);
        upper *= factor;
    }
    Ok(bounds)
}

/// A native cumulative histogram with configurable buckets.
///
/// Internally the histogram keeps one atomic counter per explicit bucket plus a
/// dedicated counter for the implicit `+Inf` bucket, an atomic observation
/// count, and a mutex-guarded running sum (`f64` cannot be stored atomically on
/// stable Rust without bit-casting, so a small lock is used for correctness).
#[derive(Debug)]
pub struct NativeHistogram {
    name: String,
    help: String,
    /// Strictly increasing finite upper bounds (excluding `+Inf`).
    upper_bounds: Vec<f64>,
    /// Per-bucket *non-cumulative* counts, aligned with `upper_bounds`.
    bucket_counts: Vec<AtomicU64>,
    /// Count of observations that exceeded every finite bound (`+Inf` bucket).
    inf_count: AtomicU64,
    /// Total number of observations.
    total_count: AtomicU64,
    /// Running sum of observed values.
    sum: StdMutex<f64>,
}

impl NativeHistogram {
    /// Create a new histogram with the given name, help text, and explicit
    /// finite upper bounds.
    ///
    /// The bounds are sorted and validated: they must be non-empty, finite, and
    /// strictly increasing after sorting. The implicit `+Inf` bucket is managed
    /// automatically and must not be supplied.
    ///
    /// # Errors
    ///
    /// Returns a [`HistogramError`] if the bounds are empty, contain a
    /// non-finite value, or are not strictly increasing.
    pub fn new(
        name: impl Into<String>,
        help: impl Into<String>,
        buckets: Vec<f64>,
    ) -> Result<Self, HistogramError> {
        if buckets.is_empty() {
            return Err(HistogramError::EmptyBuckets);
        }

        let mut upper_bounds = buckets;
        upper_bounds.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

        for window in upper_bounds.windows(2) {
            if !window[0].is_finite() || !window[1].is_finite() {
                return Err(HistogramError::NonFiniteBucket);
            }
            if window[0] >= window[1] {
                return Err(HistogramError::NonMonotonicBuckets);
            }
        }
        // Single-element slices skip the windowed loop above.
        if !upper_bounds[0].is_finite() {
            return Err(HistogramError::NonFiniteBucket);
        }

        let bucket_counts = upper_bounds.iter().map(|_| AtomicU64::new(0)).collect();

        Ok(Self {
            name: name.into(),
            help: help.into(),
            upper_bounds,
            bucket_counts,
            inf_count: AtomicU64::new(0),
            total_count: AtomicU64::new(0),
            sum: StdMutex::new(0.0),
        })
    }

    /// Create a histogram with linearly spaced buckets.
    ///
    /// # Errors
    ///
    /// Propagates errors from [`linear_buckets`].
    pub fn with_linear_buckets(
        name: impl Into<String>,
        help: impl Into<String>,
        start: f64,
        width: f64,
        count: usize,
    ) -> Result<Self, HistogramError> {
        let buckets = linear_buckets(start, width, count)?;
        Self::new(name, help, buckets)
    }

    /// Create a histogram with exponentially spaced buckets.
    ///
    /// # Errors
    ///
    /// Propagates errors from [`exponential_buckets`].
    pub fn with_exponential_buckets(
        name: impl Into<String>,
        help: impl Into<String>,
        start: f64,
        factor: f64,
        count: usize,
    ) -> Result<Self, HistogramError> {
        let buckets = exponential_buckets(start, factor, count)?;
        Self::new(name, help, buckets)
    }

    /// Record a single observation.
    ///
    /// The value is added to the running sum, the total count is incremented,
    /// and the *smallest* bucket whose upper bound is `>= value` has its
    /// non-cumulative counter incremented. Values exceeding every finite bound
    /// land in the implicit `+Inf` bucket. Non-finite values (`NaN`, infinities)
    /// are ignored to keep the sum and bucket counters well defined.
    pub fn observe(&self, value: f64) {
        if !value.is_finite() {
            return;
        }

        // Locate the first finite bound that is >= value (cumulative semantics).
        let idx = self.upper_bounds.partition_point(|&bound| bound < value);

        if idx < self.bucket_counts.len() {
            self.bucket_counts[idx].fetch_add(1, Ordering::Relaxed);
        } else {
            self.inf_count.fetch_add(1, Ordering::Relaxed);
        }

        self.total_count.fetch_add(1, Ordering::Relaxed);
        let mut sum = self.sum.lock().unwrap_or_else(|e| e.into_inner());
        *sum += value;
    }

    /// Total number of observations recorded.
    pub fn count(&self) -> u64 {
        self.total_count.load(Ordering::Relaxed)
    }

    /// Sum of all observed values.
    pub fn sum(&self) -> f64 {
        *self.sum.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Metric name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The explicit finite upper bounds (excluding `+Inf`).
    pub fn upper_bounds(&self) -> &[f64] {
        &self.upper_bounds
    }

    /// Cumulative count for the bucket with the given finite upper bound.
    ///
    /// Returns the number of observations whose value is `<= bound`. If `bound`
    /// does not exactly match a configured bound, the cumulative count up to and
    /// including all configured bounds `<= bound` is returned.
    pub fn cumulative_count(&self, bound: f64) -> u64 {
        let mut cumulative = 0u64;
        for (idx, &upper) in self.upper_bounds.iter().enumerate() {
            if upper <= bound {
                cumulative += self.bucket_counts[idx].load(Ordering::Relaxed);
            } else {
                break;
            }
        }
        cumulative
    }

    /// Snapshot of cumulative bucket counts aligned with [`Self::upper_bounds`],
    /// followed by the `+Inf` cumulative count (which always equals
    /// [`Self::count`]).
    ///
    /// The returned vector has `upper_bounds().len() + 1` entries; the last
    /// entry is the `+Inf` bucket.
    pub fn cumulative_counts(&self) -> Vec<u64> {
        let mut result = Vec::with_capacity(self.upper_bounds.len() + 1);
        let mut cumulative = 0u64;
        for counter in &self.bucket_counts {
            cumulative += counter.load(Ordering::Relaxed);
            result.push(cumulative);
        }
        cumulative += self.inf_count.load(Ordering::Relaxed);
        result.push(cumulative);
        result
    }

    /// Reset every bucket, the sum, and the observation count to zero.
    pub fn reset(&self) {
        for counter in &self.bucket_counts {
            counter.store(0, Ordering::Relaxed);
        }
        self.inf_count.store(0, Ordering::Relaxed);
        self.total_count.store(0, Ordering::Relaxed);
        *self.sum.lock().unwrap_or_else(|e| e.into_inner()) = 0.0;
    }

    /// Render this histogram in Prometheus text exposition format.
    ///
    /// The output contains `# HELP` and `# TYPE` header lines, one
    /// `<name>_bucket{le="..."}` series per finite bound plus the mandatory
    /// `le="+Inf"` series, then `<name>_sum` and `<name>_count`. All bucket
    /// series report cumulative counts and are monotonically non-decreasing.
    pub fn encode(&self) -> String {
        let mut output = String::new();
        self.encode_into(&mut output);
        output
    }

    /// Append this histogram's exposition text to an existing buffer.
    pub fn encode_into(&self, output: &mut String) {
        use std::fmt::Write as _;

        let _ = writeln!(output, "# HELP {} {}", self.name, self.help);
        let _ = writeln!(output, "# TYPE {} histogram", self.name);

        let mut cumulative = 0u64;
        for (idx, &bound) in self.upper_bounds.iter().enumerate() {
            cumulative += self.bucket_counts[idx].load(Ordering::Relaxed);
            let _ = writeln!(
                output,
                "{}_bucket{{le=\"{}\"}} {}",
                self.name,
                format_float(bound),
                cumulative
            );
        }
        cumulative += self.inf_count.load(Ordering::Relaxed);
        let _ = writeln!(output, "{}_bucket{{le=\"+Inf\"}} {}", self.name, cumulative);

        let _ = writeln!(output, "{}_sum {}", self.name, format_float(self.sum()));
        let _ = writeln!(output, "{}_count {}", self.name, cumulative);
    }
}
