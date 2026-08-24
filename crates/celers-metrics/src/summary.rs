//! Native (dependency-free) Prometheus summary with streaming quantile
//! estimation.
//!
//! This module implements a self-contained [`NativeSummary`] metric whose
//! quantiles are estimated online using the **P² (P-Square) algorithm** of
//! Raj Jain and Imrich Chlamtac, *"The P² Algorithm for Dynamic Calculation of
//! Quantiles and Histograms Without Storing Observations"* (Communications of
//! the ACM, 1985). The estimator maintains only five markers per target
//! quantile, so memory usage is constant regardless of how many observations
//! are recorded — no samples are stored.
//!
//! The summary emits Prometheus text exposition of the form
//! `<name>{quantile="0.5"} <value>`, one line per configured target quantile,
//! followed by `<name>_sum` and `<name>_count`.
//!
//! # Examples
//!
//! ```
//! use celers_metrics::NativeSummary;
//!
//! let summary = NativeSummary::new(
//!     "request_latency_seconds",
//!     "Request latency in seconds",
//!     vec![0.5, 0.9, 0.99],
//! )
//! .expect("valid quantiles");
//!
//! for value in 1..=1000 {
//!     summary.observe(f64::from(value));
//! }
//!
//! assert_eq!(summary.count(), 1000);
//! let p50 = summary.quantile(0.5).expect("p50 estimated");
//! assert!((p50 - 500.0).abs() < 25.0, "p50 estimate was {p50}");
//! ```

use std::sync::Mutex as StdMutex;

use crate::format_float;

/// Error returned when constructing a [`NativeSummary`] with invalid target
/// quantiles.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SummaryError {
    /// No target quantiles were supplied.
    EmptyQuantiles,
    /// A target quantile was outside the open interval `(0.0, 1.0)`.
    QuantileOutOfRange,
}

impl std::fmt::Display for SummaryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let message = match self {
            Self::EmptyQuantiles => "summary requires at least one target quantile",
            Self::QuantileOutOfRange => "summary quantiles must lie strictly between 0.0 and 1.0",
        };
        f.write_str(message)
    }
}

impl std::error::Error for SummaryError {}

/// A single-quantile P² (P-Square) streaming estimator.
///
/// The estimator keeps five markers. For the first five observations it simply
/// buffers and sorts them to initialize the marker heights; thereafter each new
/// observation adjusts the marker positions and, when a marker drifts from its
/// desired position, its height is re-estimated with a piecewise-parabolic (or,
/// as a fallback, linear) interpolation formula.
#[derive(Debug, Clone)]
struct PSquareEstimator {
    /// Target quantile in `(0.0, 1.0)`.
    p: f64,
    /// Number of observations seen so far.
    count: u64,
    /// Marker heights `q[0..5]` (the running quantile estimates of the markers).
    heights: [f64; 5],
    /// Actual integer marker positions `n[0..5]` (1-based, as in the paper).
    positions: [f64; 5],
    /// Desired marker positions `n'[0..5]`.
    desired: [f64; 5],
    /// Increments of the desired positions `dn'[0..5]`.
    increments: [f64; 5],
    /// Buffer for the first five observations used to initialize markers.
    init_buffer: Vec<f64>,
}

impl PSquareEstimator {
    fn new(p: f64) -> Self {
        Self {
            p,
            count: 0,
            heights: [0.0; 5],
            positions: [0.0; 5],
            desired: [0.0; 5],
            increments: [0.0, p / 2.0, p, (1.0 + p) / 2.0, 1.0],
            init_buffer: Vec::with_capacity(5),
        }
    }

    /// Initialize the markers once the fifth observation has arrived.
    fn initialize(&mut self) {
        self.init_buffer
            .sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        for i in 0..5 {
            self.heights[i] = self.init_buffer[i];
            // 1-based marker positions.
            self.positions[i] = (i + 1) as f64;
        }
        let p = self.p;
        self.desired = [1.0, 1.0 + 2.0 * p, 1.0 + 4.0 * p, 3.0 + 2.0 * p, 5.0];
    }

    /// Record one observation.
    fn observe(&mut self, value: f64) {
        if !value.is_finite() {
            return;
        }
        self.count += 1;

        if self.count <= 5 {
            self.init_buffer.push(value);
            if self.count == 5 {
                self.initialize();
            }
            return;
        }

        // Step B.1: find the cell k such that q[k] <= value < q[k+1] and update
        // the extreme marker heights if the value falls outside the range.
        let k = if value < self.heights[0] {
            self.heights[0] = value;
            0
        } else if value >= self.heights[4] {
            self.heights[4] = value;
            3
        } else {
            let mut cell = 0;
            for i in 0..4 {
                if self.heights[i] <= value && value < self.heights[i + 1] {
                    cell = i;
                    break;
                }
            }
            cell
        };

        // Step B.2: increment positions of markers k+1..=4 and all desired
        // positions.
        for i in (k + 1)..5 {
            self.positions[i] += 1.0;
        }
        for i in 0..5 {
            self.desired[i] += self.increments[i];
        }

        // Step B.3: adjust interior marker heights/positions if necessary.
        //
        // Jain & Chlamtac's condition for a downward move is
        // `d <= -1 and n[i-1] - n[i] < -1`. `pos_diff_prev` must therefore be
        // `n[i-1] - n[i]` (which is always <= 0 since positions are
        // non-decreasing), not `n[i] - n[i-1]` (always >= 0, which would make
        // the `< -1.0` check unsatisfiable and the marker could never move
        // down).
        for i in 1..4 {
            let d = self.desired[i] - self.positions[i];
            let pos_diff_next = self.positions[i + 1] - self.positions[i];
            let pos_diff_prev = self.positions[i - 1] - self.positions[i];

            if (d >= 1.0 && pos_diff_next > 1.0) || (d <= -1.0 && pos_diff_prev < -1.0) {
                let d_sign = if d >= 0.0 { 1.0 } else { -1.0 };

                let parabolic = self.parabolic(i, d_sign);
                if self.heights[i - 1] < parabolic && parabolic < self.heights[i + 1] {
                    self.heights[i] = parabolic;
                } else {
                    self.heights[i] = self.linear(i, d_sign);
                }
                self.positions[i] += d_sign;
            }
        }
    }

    /// Piecewise-parabolic prediction (P² formula) for marker `i`.
    fn parabolic(&self, i: usize, d: f64) -> f64 {
        let qi = self.heights[i];
        let qi_next = self.heights[i + 1];
        let qi_prev = self.heights[i - 1];
        let ni = self.positions[i];
        let ni_next = self.positions[i + 1];
        let ni_prev = self.positions[i - 1];

        qi + (d / (ni_next - ni_prev))
            * ((ni - ni_prev + d) * (qi_next - qi) / (ni_next - ni)
                + (ni_next - ni - d) * (qi - qi_prev) / (ni - ni_prev))
    }

    /// Linear prediction fallback for marker `i`.
    fn linear(&self, i: usize, d: f64) -> f64 {
        let idx = if d >= 0.0 { i + 1 } else { i - 1 };
        self.heights[i]
            + d * (self.heights[idx] - self.heights[i]) / (self.positions[idx] - self.positions[i])
    }

    /// Current quantile estimate.
    ///
    /// Returns `None` until at least one observation has been recorded. With
    /// fewer than five observations the estimate is read directly from the
    /// sorted initialization buffer using nearest-rank, so the estimator is
    /// usable from the very first sample.
    fn estimate(&self) -> Option<f64> {
        match self.count {
            0 => None,
            1..=4 => {
                let mut buffer = self.init_buffer.clone();
                buffer.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                let rank = (self.p * (buffer.len() as f64 - 1.0)).round() as usize;
                buffer.get(rank).copied()
            }
            _ => Some(self.heights[2]),
        }
    }
}

/// A native streaming summary metric.
///
/// The summary tracks a running sum and total count exactly, and estimates each
/// configured target quantile online via an independent `PSquareEstimator`.
/// All mutable state is guarded by a single mutex so the type is `Sync`.
#[derive(Debug)]
pub struct NativeSummary {
    name: String,
    help: String,
    /// Configured target quantiles, sorted ascending and de-duplicated.
    quantiles: Vec<f64>,
    state: StdMutex<SummaryState>,
}

#[derive(Debug)]
struct SummaryState {
    estimators: Vec<PSquareEstimator>,
    sum: f64,
    count: u64,
}

impl NativeSummary {
    /// Create a new summary with the given name, help text, and target
    /// quantiles (each strictly between `0.0` and `1.0`).
    ///
    /// Quantiles are sorted ascending and de-duplicated.
    ///
    /// # Errors
    ///
    /// Returns [`SummaryError::EmptyQuantiles`] when no quantiles are supplied,
    /// or [`SummaryError::QuantileOutOfRange`] when any quantile is not in the
    /// open interval `(0.0, 1.0)`.
    pub fn new(
        name: impl Into<String>,
        help: impl Into<String>,
        quantiles: Vec<f64>,
    ) -> Result<Self, SummaryError> {
        if quantiles.is_empty() {
            return Err(SummaryError::EmptyQuantiles);
        }
        for &q in &quantiles {
            if !(q > 0.0 && q < 1.0) {
                return Err(SummaryError::QuantileOutOfRange);
            }
        }

        let mut sorted = quantiles;
        sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        sorted.dedup_by(|a, b| (*a - *b).abs() < f64::EPSILON);

        let estimators = sorted.iter().map(|&q| PSquareEstimator::new(q)).collect();

        Ok(Self {
            name: name.into(),
            help: help.into(),
            quantiles: sorted,
            state: StdMutex::new(SummaryState {
                estimators,
                sum: 0.0,
                count: 0,
            }),
        })
    }

    /// Create a summary tracking the conventional p50/p90/p99 quantiles.
    ///
    /// # Errors
    ///
    /// This never fails in practice; it returns the same error type as
    /// [`NativeSummary::new`] for signature consistency.
    pub fn with_default_quantiles(
        name: impl Into<String>,
        help: impl Into<String>,
    ) -> Result<Self, SummaryError> {
        Self::new(name, help, vec![0.5, 0.9, 0.99])
    }

    /// Record a single observation. Non-finite values are ignored.
    pub fn observe(&self, value: f64) {
        if !value.is_finite() {
            return;
        }
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.sum += value;
        state.count += 1;
        for estimator in &mut state.estimators {
            estimator.observe(value);
        }
    }

    /// Total number of observations recorded.
    pub fn count(&self) -> u64 {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).count
    }

    /// Sum of all observed values.
    pub fn sum(&self) -> f64 {
        self.state.lock().unwrap_or_else(|e| e.into_inner()).sum
    }

    /// Metric name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The configured target quantiles (sorted ascending).
    pub fn quantiles(&self) -> &[f64] {
        &self.quantiles
    }

    /// Current estimate for a configured target quantile.
    ///
    /// Returns `None` if `target` is not one of the configured quantiles or if
    /// no observations have been recorded yet.
    pub fn quantile(&self, target: f64) -> Option<f64> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        let idx = self
            .quantiles
            .iter()
            .position(|&q| (q - target).abs() < f64::EPSILON)?;
        state.estimators[idx].estimate()
    }

    /// Snapshot of `(quantile, estimate)` pairs for every configured quantile.
    ///
    /// Quantiles without an estimate yet (zero observations) are reported with a
    /// value of `0.0`, matching the Prometheus convention for an empty summary.
    pub fn quantile_estimates(&self) -> Vec<(f64, f64)> {
        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        self.quantiles
            .iter()
            .zip(state.estimators.iter())
            .map(|(&q, estimator)| (q, estimator.estimate().unwrap_or(0.0)))
            .collect()
    }

    /// Reset every estimator, the sum, and the observation count to zero.
    pub fn reset(&self) {
        let mut state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        state.sum = 0.0;
        state.count = 0;
        for estimator in &mut state.estimators {
            *estimator = PSquareEstimator::new(estimator.p);
        }
    }

    /// Render this summary in Prometheus text exposition format.
    ///
    /// The output contains `# HELP` and `# TYPE` header lines, one
    /// `<name>{quantile="..."}` series per configured quantile, then
    /// `<name>_sum` and `<name>_count`.
    pub fn encode(&self) -> String {
        let mut output = String::new();
        self.encode_into(&mut output);
        output
    }

    /// Append this summary's exposition text to an existing buffer.
    pub fn encode_into(&self, output: &mut String) {
        use std::fmt::Write as _;

        let _ = writeln!(output, "# HELP {} {}", self.name, self.help);
        let _ = writeln!(output, "# TYPE {} summary", self.name);

        let state = self.state.lock().unwrap_or_else(|e| e.into_inner());
        for (&q, estimator) in self.quantiles.iter().zip(state.estimators.iter()) {
            let value = estimator.estimate().unwrap_or(0.0);
            let _ = writeln!(
                output,
                "{}{{quantile=\"{}\"}} {}",
                self.name,
                format_float(q),
                format_float(value)
            );
        }

        let _ = writeln!(output, "{}_sum {}", self.name, format_float(state.sum));
        let _ = writeln!(output, "{}_count {}", self.name, state.count);
    }
}
