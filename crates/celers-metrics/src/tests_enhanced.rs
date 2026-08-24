//! Unit tests for the enhanced Prometheus metrics: native histograms with
//! cumulative buckets and native summaries with P-Square streaming quantile
//! estimation.

#![cfg(test)]

use crate::exposition::{
    format_float, gather_enhanced_metrics, global_enhanced_registry, register_global_histogram,
    register_global_summary, EnhancedMetricsRegistry,
};
use crate::native_histogram::{
    exponential_buckets, linear_buckets, HistogramError, NativeHistogram,
};
use crate::summary::{NativeSummary, SummaryError};

use serial_test::serial;

// ============================================================================
// Bucket helper constructors
// ============================================================================

#[test]
fn test_linear_buckets() {
    let buckets = linear_buckets(0.0, 5.0, 4).expect("valid linear buckets");
    assert_eq!(buckets, vec![0.0, 5.0, 10.0, 15.0]);

    let buckets = linear_buckets(1.0, 0.5, 3).expect("valid linear buckets");
    assert_eq!(buckets, vec![1.0, 1.5, 2.0]);
}

#[test]
fn test_linear_buckets_errors() {
    assert_eq!(
        linear_buckets(0.0, 5.0, 0),
        Err(HistogramError::InvalidCount)
    );
    assert_eq!(
        linear_buckets(0.0, 0.0, 4),
        Err(HistogramError::InvalidLinearParameters)
    );
    assert_eq!(
        linear_buckets(0.0, -1.0, 4),
        Err(HistogramError::InvalidLinearParameters)
    );
}

#[test]
fn test_exponential_buckets() {
    let buckets = exponential_buckets(1.0, 2.0, 5).expect("valid exponential buckets");
    assert_eq!(buckets, vec![1.0, 2.0, 4.0, 8.0, 16.0]);

    let buckets = exponential_buckets(0.001, 10.0, 4).expect("valid exponential buckets");
    assert_eq!(buckets, vec![0.001, 0.01, 0.1, 1.0]);
}

#[test]
fn test_exponential_buckets_errors() {
    assert_eq!(
        exponential_buckets(1.0, 2.0, 0),
        Err(HistogramError::InvalidCount)
    );
    assert_eq!(
        exponential_buckets(0.0, 2.0, 4),
        Err(HistogramError::InvalidExponentialParameters)
    );
    assert_eq!(
        exponential_buckets(1.0, 1.0, 4),
        Err(HistogramError::InvalidExponentialParameters)
    );
    assert_eq!(
        exponential_buckets(-1.0, 2.0, 4),
        Err(HistogramError::InvalidExponentialParameters)
    );
}

// ============================================================================
// Histogram construction and validation
// ============================================================================

#[test]
fn test_histogram_construction_sorts_buckets() {
    let hist =
        NativeHistogram::new("h", "help", vec![1.0, 0.1, 0.5]).expect("valid (unsorted) buckets");
    assert_eq!(hist.upper_bounds(), &[0.1, 0.5, 1.0]);
}

#[test]
fn test_histogram_construction_errors() {
    assert_eq!(
        NativeHistogram::new("h", "help", vec![]).unwrap_err(),
        HistogramError::EmptyBuckets
    );
    assert_eq!(
        NativeHistogram::new("h", "help", vec![1.0, 1.0]).unwrap_err(),
        HistogramError::NonMonotonicBuckets
    );
    assert_eq!(
        NativeHistogram::new("h", "help", vec![1.0, f64::NAN]).unwrap_err(),
        HistogramError::NonFiniteBucket
    );
}

#[test]
fn test_histogram_with_linear_buckets_constructor() {
    let hist =
        NativeHistogram::with_linear_buckets("h", "help", 0.0, 10.0, 5).expect("valid linear hist");
    assert_eq!(hist.upper_bounds(), &[0.0, 10.0, 20.0, 30.0, 40.0]);
}

#[test]
fn test_histogram_with_exponential_buckets_constructor() {
    let hist = NativeHistogram::with_exponential_buckets("h", "help", 1.0, 2.0, 4)
        .expect("valid exponential hist");
    assert_eq!(hist.upper_bounds(), &[1.0, 2.0, 4.0, 8.0]);
}

// ============================================================================
// Bucket counting + cumulative monotonicity
// ============================================================================

#[test]
fn test_histogram_bucket_counting() {
    let hist = NativeHistogram::new("h", "help", vec![1.0, 2.0, 5.0]).expect("valid buckets");

    // Values placed into specific buckets.
    hist.observe(0.5); // -> le=1
    hist.observe(1.0); // -> le=1 (boundary inclusive)
    hist.observe(1.5); // -> le=2
    hist.observe(4.9); // -> le=5
    hist.observe(100.0); // -> +Inf

    assert_eq!(hist.count(), 5);
    assert!((hist.sum() - (0.5 + 1.0 + 1.5 + 4.9 + 100.0)).abs() < 1e-9);

    // Cumulative counts.
    assert_eq!(hist.cumulative_count(1.0), 2);
    assert_eq!(hist.cumulative_count(2.0), 3);
    assert_eq!(hist.cumulative_count(5.0), 4);
}

#[test]
fn test_histogram_cumulative_monotonicity_and_inf_equals_count() {
    let hist =
        NativeHistogram::new("h", "help", vec![1.0, 5.0, 10.0, 50.0]).expect("valid buckets");

    // Spread observations across all buckets including the overflow.
    let samples = [0.5, 0.9, 3.0, 4.0, 7.0, 9.5, 20.0, 49.0, 1000.0, 5000.0];
    for &s in &samples {
        hist.observe(s);
    }

    let cumulative = hist.cumulative_counts();
    // One entry per finite bound plus +Inf.
    assert_eq!(cumulative.len(), hist.upper_bounds().len() + 1);

    // Cumulative counts must be monotonically non-decreasing.
    for window in cumulative.windows(2) {
        assert!(
            window[1] >= window[0],
            "cumulative counts must be non-decreasing: {cumulative:?}"
        );
    }

    // The +Inf bucket (last entry) must equal the total observation count.
    let inf_count = *cumulative.last().expect("at least one bucket");
    assert_eq!(inf_count, hist.count());
    assert_eq!(inf_count, samples.len() as u64);
}

#[test]
fn test_histogram_ignores_non_finite_observations() {
    let hist = NativeHistogram::new("h", "help", vec![1.0, 2.0]).expect("valid buckets");
    hist.observe(f64::NAN);
    hist.observe(f64::INFINITY);
    hist.observe(1.5);
    assert_eq!(hist.count(), 1);
    assert!((hist.sum() - 1.5).abs() < 1e-9);
}

#[test]
fn test_histogram_reset() {
    let hist = NativeHistogram::new("h", "help", vec![1.0, 2.0]).expect("valid buckets");
    hist.observe(0.5);
    hist.observe(1.5);
    assert_eq!(hist.count(), 2);
    hist.reset();
    assert_eq!(hist.count(), 0);
    assert_eq!(hist.sum(), 0.0);
    assert_eq!(hist.cumulative_count(2.0), 0);
}

// ============================================================================
// Histogram exact exposition format
// ============================================================================

#[test]
fn test_histogram_exposition_format_exact() {
    let hist = NativeHistogram::new("celers_demo_seconds", "Demo histogram", vec![0.5, 1.0, 2.5])
        .expect("valid buckets");
    hist.observe(0.3); // le=0.5
    hist.observe(0.4); // le=0.5
    hist.observe(0.8); // le=1.0
    hist.observe(2.0); // le=2.5
    hist.observe(9.0); // +Inf

    let expected = "\
# HELP celers_demo_seconds Demo histogram
# TYPE celers_demo_seconds histogram
celers_demo_seconds_bucket{le=\"0.5\"} 2
celers_demo_seconds_bucket{le=\"1\"} 3
celers_demo_seconds_bucket{le=\"2.5\"} 4
celers_demo_seconds_bucket{le=\"+Inf\"} 5
celers_demo_seconds_sum 12.5
celers_demo_seconds_count 5
";
    assert_eq!(hist.encode(), expected);
}

// ============================================================================
// Summary construction and validation
// ============================================================================

#[test]
fn test_summary_construction_sorts_and_dedups() {
    let summary =
        NativeSummary::new("s", "help", vec![0.99, 0.5, 0.9, 0.5]).expect("valid quantiles");
    assert_eq!(summary.quantiles(), &[0.5, 0.9, 0.99]);
}

#[test]
fn test_summary_construction_errors() {
    assert_eq!(
        NativeSummary::new("s", "help", vec![]).unwrap_err(),
        SummaryError::EmptyQuantiles
    );
    assert_eq!(
        NativeSummary::new("s", "help", vec![0.0]).unwrap_err(),
        SummaryError::QuantileOutOfRange
    );
    assert_eq!(
        NativeSummary::new("s", "help", vec![1.0]).unwrap_err(),
        SummaryError::QuantileOutOfRange
    );
    assert_eq!(
        NativeSummary::new("s", "help", vec![1.5]).unwrap_err(),
        SummaryError::QuantileOutOfRange
    );
}

#[test]
fn test_summary_default_quantiles() {
    let summary = NativeSummary::with_default_quantiles("s", "help").expect("valid summary");
    assert_eq!(summary.quantiles(), &[0.5, 0.9, 0.99]);
}

// ============================================================================
// Summary sum/count accuracy
// ============================================================================

#[test]
fn test_summary_sum_and_count_exact() {
    let summary = NativeSummary::new("s", "help", vec![0.5]).expect("valid summary");
    for v in 1..=100 {
        summary.observe(f64::from(v));
    }
    assert_eq!(summary.count(), 100);
    // Sum of 1..=100 = 5050.
    assert!((summary.sum() - 5050.0).abs() < 1e-6);
}

#[test]
fn test_summary_ignores_non_finite() {
    let summary = NativeSummary::new("s", "help", vec![0.5]).expect("valid summary");
    summary.observe(f64::NAN);
    summary.observe(f64::NEG_INFINITY);
    summary.observe(42.0);
    assert_eq!(summary.count(), 1);
    assert!((summary.sum() - 42.0).abs() < 1e-9);
}

// ============================================================================
// Quantile accuracy on a known distribution (uniform 1..=1000)
// ============================================================================

#[test]
fn test_summary_quantile_accuracy_uniform() {
    let summary = NativeSummary::new("s", "help", vec![0.5, 0.9, 0.99]).expect("valid quantiles");

    // Uniform distribution 1..=1000. For this distribution the true quantile
    // p corresponds (nearest-rank) to value ~ p * 1000.
    for v in 1..=1000 {
        summary.observe(f64::from(v));
    }

    let p50 = summary.quantile(0.5).expect("p50");
    let p90 = summary.quantile(0.9).expect("p90");
    let p99 = summary.quantile(0.99).expect("p99");

    // P-Square is an approximation; allow a tolerance generous enough for the
    // algorithm yet tight enough to prove correctness (well under 5% of range).
    assert!((p50 - 500.0).abs() < 25.0, "p50 estimate was {p50}");
    assert!((p90 - 900.0).abs() < 25.0, "p90 estimate was {p90}");
    assert!((p99 - 990.0).abs() < 25.0, "p99 estimate was {p99}");

    // Quantile estimates must be ordered.
    assert!(p50 < p90, "p50 ({p50}) should be < p90 ({p90})");
    assert!(p90 < p99, "p90 ({p90}) should be < p99 ({p99})");
}

#[test]
fn test_summary_quantile_accuracy_shuffled_input() {
    // The same uniform population fed in a deterministic non-sorted order to
    // exercise the streaming marker adjustment thoroughly.
    let summary = NativeSummary::new("s", "help", vec![0.5, 0.95]).expect("valid quantiles");

    // Interleave low and high halves to avoid a monotone stream.
    for i in 0..500 {
        summary.observe(f64::from(i * 2 + 1)); // odd values 1,3,...,999
        summary.observe(f64::from((i + 1) * 2)); // even values 2,4,...,1000
    }
    assert_eq!(summary.count(), 1000);

    let p50 = summary.quantile(0.5).expect("p50");
    let p95 = summary.quantile(0.95).expect("p95");

    assert!((p50 - 500.0).abs() < 30.0, "p50 estimate was {p50}");
    assert!((p95 - 950.0).abs() < 30.0, "p95 estimate was {p95}");
}

#[test]
fn test_summary_quantile_accuracy_decreasing_stream() {
    // Regression test for the P-Square downward-marker-adjustment bug
    // (interior markers must be able to move *down*, not just up). A
    // strictly decreasing stream is exactly the case where the original,
    // broken downward condition (`n[i] - n[i-1] < -1.0`, unsatisfiable since
    // marker positions never decrease) left every estimate pinned near its
    // initialization value: pre-fix this reported p50 ~= 998 instead of
    // ~500, a ~100% error.
    let summary = NativeSummary::new("s", "help", vec![0.5, 0.9, 0.99]).expect("valid quantiles");

    for v in (1..=1000).rev() {
        summary.observe(f64::from(v));
    }

    let p50 = summary.quantile(0.5).expect("p50");
    let p90 = summary.quantile(0.9).expect("p90");
    let p99 = summary.quantile(0.99).expect("p99");

    // True percentiles of {1..=1000} are ~500/~900/~990 regardless of feed
    // order; same tolerance as the increasing-stream case above.
    assert!((p50 - 500.0).abs() < 25.0, "p50 estimate was {p50}");
    assert!((p90 - 900.0).abs() < 25.0, "p90 estimate was {p90}");
    assert!((p99 - 990.0).abs() < 25.0, "p99 estimate was {p99}");

    assert!(p50 < p90, "p50 ({p50}) should be < p90 ({p90})");
    assert!(p90 < p99, "p90 ({p90}) should be < p99 ({p99})");
}

#[test]
fn test_summary_quantile_accuracy_seeded_pseudo_random_stream() {
    // Deterministic pseudo-random (unordered) stream, matching the audit's
    // reproduction case for this bug (~36% p50 error before the fix). A
    // small LCG keeps the test hermetic and reproducible without `rand`.
    let mut state: u64 = 0x2545_F491_4F6C_DD1D;
    let mut next = || {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        // Top 53 bits -> uniform in [0, 1), scaled to an integer in [1, 1000].
        ((state >> 11) as f64 / (1_u64 << 53) as f64 * 1000.0).floor() + 1.0
    };

    let mut values: Vec<f64> = Vec::with_capacity(2000);
    let summary = NativeSummary::new("s", "help", vec![0.5, 0.9, 0.99]).expect("valid quantiles");
    for _ in 0..2000 {
        let v = next();
        values.push(v);
        summary.observe(v);
    }

    // Ground truth: exact nearest-rank percentile of the same stream, sorted.
    values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let true_percentile = |p: f64| -> f64 {
        let idx = ((p * values.len() as f64).round() as usize).min(values.len() - 1);
        values[idx]
    };
    let true_p50 = true_percentile(0.5);
    let true_p90 = true_percentile(0.9);
    let true_p99 = true_percentile(0.99);

    let p50 = summary.quantile(0.5).expect("p50");
    let p90 = summary.quantile(0.9).expect("p90");
    let p99 = summary.quantile(0.99).expect("p99");

    // The P-Square estimate is approximate; require it within 10% of the
    // true value -- well under the ~36% error the audit measured pre-fix.
    assert!(
        (p50 - true_p50).abs() < true_p50 * 0.10,
        "p50 estimate {p50} too far from true {true_p50}"
    );
    assert!(
        (p90 - true_p90).abs() < true_p90 * 0.10,
        "p90 estimate {p90} too far from true {true_p90}"
    );
    assert!(
        (p99 - true_p99).abs() < true_p99 * 0.10,
        "p99 estimate {p99} too far from true {true_p99}"
    );
}

#[test]
fn test_summary_quantile_small_sample() {
    // Fewer than five observations should still yield a sensible estimate from
    // the initialization buffer (nearest-rank).
    let summary = NativeSummary::new("s", "help", vec![0.5]).expect("valid summary");
    assert_eq!(summary.quantile(0.5), None); // no data yet
    summary.observe(10.0);
    summary.observe(20.0);
    summary.observe(30.0);
    let p50 = summary.quantile(0.5).expect("p50");
    assert!((p50 - 20.0).abs() < 1e-9, "p50 was {p50}");
}

#[test]
fn test_summary_quantile_unknown_target_returns_none() {
    let summary = NativeSummary::new("s", "help", vec![0.5, 0.9]).expect("valid summary");
    summary.observe(1.0);
    assert!(summary.quantile(0.75).is_none());
}

#[test]
fn test_summary_quantile_estimates_snapshot() {
    let summary = NativeSummary::new("s", "help", vec![0.5, 0.9]).expect("valid summary");
    for v in 1..=200 {
        summary.observe(f64::from(v));
    }
    let estimates = summary.quantile_estimates();
    assert_eq!(estimates.len(), 2);
    assert!((estimates[0].0 - 0.5).abs() < f64::EPSILON);
    assert!((estimates[1].0 - 0.9).abs() < f64::EPSILON);
    assert!((estimates[0].1 - 100.0).abs() < 10.0);
    assert!((estimates[1].1 - 180.0).abs() < 10.0);
}

#[test]
fn test_summary_reset() {
    let summary = NativeSummary::new("s", "help", vec![0.5]).expect("valid summary");
    for v in 1..=50 {
        summary.observe(f64::from(v));
    }
    assert_eq!(summary.count(), 50);
    summary.reset();
    assert_eq!(summary.count(), 0);
    assert_eq!(summary.sum(), 0.0);
    assert_eq!(summary.quantile(0.5), None);
}

// ============================================================================
// Summary exact exposition format
// ============================================================================

#[test]
fn test_summary_exposition_format_exact_empty() {
    // An empty summary reports 0 for each quantile, sum and count.
    let summary = NativeSummary::new("celers_demo_summary", "Demo summary", vec![0.5, 0.9])
        .expect("valid summary");

    let expected = "\
# HELP celers_demo_summary Demo summary
# TYPE celers_demo_summary summary
celers_demo_summary{quantile=\"0.5\"} 0
celers_demo_summary{quantile=\"0.9\"} 0
celers_demo_summary_sum 0
celers_demo_summary_count 0
";
    assert_eq!(summary.encode(), expected);
}

#[test]
fn test_summary_exposition_format_structure() {
    // With data we cannot hard-code the estimate, but we can assert the exact
    // structure of header lines, quantile labels, sum and count.
    let summary = NativeSummary::new("celers_lat", "Latency", vec![0.5]).expect("valid summary");
    summary.observe(2.0);
    summary.observe(4.0);
    summary.observe(6.0);

    let text = summary.encode();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines[0], "# HELP celers_lat Latency");
    assert_eq!(lines[1], "# TYPE celers_lat summary");
    assert!(lines[2].starts_with("celers_lat{quantile=\"0.5\"} "));
    assert_eq!(lines[3], "celers_lat_sum 12");
    assert_eq!(lines[4], "celers_lat_count 3");
}

// ============================================================================
// format_float helper
// ============================================================================

#[test]
fn test_format_float() {
    assert_eq!(format_float(0.5), "0.5");
    assert_eq!(format_float(1.0), "1");
    assert_eq!(format_float(2.5), "2.5");
    assert_eq!(format_float(0.001), "0.001");
    assert_eq!(format_float(0.0), "0");
    assert_eq!(format_float(f64::INFINITY), "+Inf");
    assert_eq!(format_float(f64::NEG_INFINITY), "-Inf");
    assert_eq!(format_float(f64::NAN), "NaN");
}

// ============================================================================
// EnhancedMetricsRegistry
// ============================================================================

#[test]
fn test_registry_register_and_retrieve() {
    let registry = EnhancedMetricsRegistry::new();
    let hist = NativeHistogram::new("reg_hist", "h", vec![1.0, 2.0]).expect("valid hist");
    let handle = registry.register_histogram(hist);
    handle.observe(0.5);

    assert_eq!(registry.histogram_count(), 1);
    let retrieved = registry.histogram("reg_hist").expect("registered");
    assert_eq!(retrieved.count(), 1);

    let summary = NativeSummary::new("reg_summary", "s", vec![0.5]).expect("valid summary");
    let s_handle = registry.register_summary(summary);
    s_handle.observe(10.0);
    assert_eq!(registry.summary_count(), 1);
    assert_eq!(
        registry.summary("reg_summary").expect("registered").count(),
        1
    );
}

#[test]
fn test_registry_register_is_idempotent() {
    let registry = EnhancedMetricsRegistry::new();
    let h1 =
        registry.register_histogram(NativeHistogram::new("dup", "h", vec![1.0]).expect("valid"));
    h1.observe(0.5);
    // Registering the same name again returns the existing handle.
    let h2 =
        registry.register_histogram(NativeHistogram::new("dup", "h", vec![1.0]).expect("valid"));
    h2.observe(0.5);
    assert_eq!(registry.histogram_count(), 1);
    // Both handles point at the same underlying histogram.
    assert_eq!(registry.histogram("dup").expect("registered").count(), 2);
}

#[test]
fn test_registry_encode_combines_metrics() {
    let registry = EnhancedMetricsRegistry::new();
    let hist =
        registry.register_histogram(NativeHistogram::new("a_hist", "AH", vec![1.0]).expect("ok"));
    hist.observe(0.5);
    let summary =
        registry.register_summary(NativeSummary::new("z_summary", "ZS", vec![0.5]).expect("ok"));
    summary.observe(5.0);

    let text = registry.encode();
    assert!(text.contains("# TYPE a_hist histogram"));
    assert!(text.contains("a_hist_bucket{le=\"+Inf\"} 1"));
    assert!(text.contains("# TYPE z_summary summary"));
    assert!(text.contains("z_summary_count 1"));

    // Histograms are emitted before summaries.
    let hist_pos = text.find("a_hist").expect("histogram present");
    let summary_pos = text.find("z_summary").expect("summary present");
    assert!(hist_pos < summary_pos);
}

#[test]
fn test_registry_clear() {
    let registry = EnhancedMetricsRegistry::new();
    registry.register_histogram(NativeHistogram::new("h", "h", vec![1.0]).expect("ok"));
    registry.register_summary(NativeSummary::new("s", "s", vec![0.5]).expect("ok"));
    assert_eq!(registry.histogram_count(), 1);
    assert_eq!(registry.summary_count(), 1);
    registry.clear();
    assert_eq!(registry.histogram_count(), 0);
    assert_eq!(registry.summary_count(), 0);
}

// ============================================================================
// Global enhanced registry + combined gather
// ============================================================================

#[test]
#[serial]
fn test_global_enhanced_gather() {
    global_enhanced_registry().clear();

    let hist = register_global_histogram(
        NativeHistogram::new("celers_global_hist", "Global hist", vec![1.0, 2.0]).expect("ok"),
    );
    hist.observe(0.5);
    hist.observe(1.5);

    let summary = register_global_summary(
        NativeSummary::new("celers_global_summary", "Global summary", vec![0.5]).expect("ok"),
    );
    summary.observe(10.0);

    let output = gather_enhanced_metrics();
    assert!(output.contains("celers_global_hist_bucket{le=\"+Inf\"} 2"));
    assert!(output.contains("celers_global_hist_count 2"));
    assert!(output.contains("# TYPE celers_global_summary summary"));
    assert!(output.contains("celers_global_summary_count 1"));

    global_enhanced_registry().clear();
}
