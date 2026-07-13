//! Shared Prometheus text-exposition helpers and an enhanced registry that
//! aggregates native histograms and summaries.
//!
//! The [`EnhancedMetricsRegistry`] owns [`NativeHistogram`] and [`NativeSummary`]
//! instances created at runtime and renders them, together, into a single
//! Prometheus text-format block. It can be combined with the static
//! `prometheus`-backed metrics via [`gather_enhanced_metrics`].

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex as StdMutex};

use crate::native_histogram::NativeHistogram;
use crate::summary::NativeSummary;

/// Format an `f64` for Prometheus text exposition.
///
/// Prometheus follows Go's `strconv.FormatFloat(f, 'g', -1, 64)` which emits the
/// shortest decimal string that round-trips to the same value, and renders
/// non-finite values as `+Inf`, `-Inf`, and `NaN`. Rust's default `f64`
/// `Display` already produces the shortest round-tripping representation for
/// finite values (e.g. `0.5` -> `"0.5"`, `1.0` -> `"1"`), so finite values are
/// delegated to it while the non-finite cases are mapped explicitly.
///
/// # Examples
///
/// ```
/// use celers_metrics::format_float;
///
/// assert_eq!(format_float(0.5), "0.5");
/// assert_eq!(format_float(1.0), "1");
/// assert_eq!(format_float(f64::INFINITY), "+Inf");
/// assert_eq!(format_float(f64::NEG_INFINITY), "-Inf");
/// assert_eq!(format_float(f64::NAN), "NaN");
/// ```
#[must_use]
pub fn format_float(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value.is_infinite() {
        if value.is_sign_positive() {
            "+Inf".to_string()
        } else {
            "-Inf".to_string()
        }
    } else {
        format!("{value}")
    }
}

/// A runtime registry of native histograms and summaries.
///
/// Metrics are keyed by name and stored behind [`Arc`] so callers can keep a
/// handle to observe values while the registry retains a reference for
/// exposition. Names are kept in a [`BTreeMap`] so the rendered output is
/// deterministically ordered, which keeps exposition snapshots stable in tests.
#[derive(Debug, Default)]
pub struct EnhancedMetricsRegistry {
    histograms: StdMutex<BTreeMap<String, Arc<NativeHistogram>>>,
    summaries: StdMutex<BTreeMap<String, Arc<NativeSummary>>>,
}

impl EnhancedMetricsRegistry {
    /// Create an empty registry.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a native histogram, returning a shared handle for recording
    /// observations.
    ///
    /// If a histogram with the same name already exists, the existing handle is
    /// returned and the supplied `histogram` is discarded, so repeated
    /// registration is idempotent.
    pub fn register_histogram(&self, histogram: NativeHistogram) -> Arc<NativeHistogram> {
        let mut histograms = self.histograms.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(existing) = histograms.get(histogram.name()) {
            return Arc::clone(existing);
        }
        let handle = Arc::new(histogram);
        histograms.insert(handle.name().to_string(), Arc::clone(&handle));
        handle
    }

    /// Register a native summary, returning a shared handle for recording
    /// observations.
    ///
    /// Registration is idempotent by name, mirroring
    /// [`Self::register_histogram`].
    pub fn register_summary(&self, summary: NativeSummary) -> Arc<NativeSummary> {
        let mut summaries = self.summaries.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(existing) = summaries.get(summary.name()) {
            return Arc::clone(existing);
        }
        let handle = Arc::new(summary);
        summaries.insert(handle.name().to_string(), Arc::clone(&handle));
        handle
    }

    /// Retrieve a previously registered histogram by name.
    pub fn histogram(&self, name: &str) -> Option<Arc<NativeHistogram>> {
        self.histograms
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(name)
            .map(Arc::clone)
    }

    /// Retrieve a previously registered summary by name.
    pub fn summary(&self, name: &str) -> Option<Arc<NativeSummary>> {
        self.summaries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(name)
            .map(Arc::clone)
    }

    /// Number of registered histograms.
    pub fn histogram_count(&self) -> usize {
        self.histograms
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }

    /// Number of registered summaries.
    pub fn summary_count(&self) -> usize {
        self.summaries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .len()
    }

    /// Remove all registered histograms and summaries.
    pub fn clear(&self) {
        self.histograms
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
        self.summaries
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }

    /// Render every registered histogram and summary into a single Prometheus
    /// text-format string.
    ///
    /// Histograms are emitted first (sorted by name), then summaries (sorted by
    /// name), each as a complete `# HELP` / `# TYPE` block.
    pub fn encode(&self) -> String {
        let mut output = String::new();
        {
            let histograms = self.histograms.lock().unwrap_or_else(|e| e.into_inner());
            for histogram in histograms.values() {
                histogram.encode_into(&mut output);
            }
        }
        {
            let summaries = self.summaries.lock().unwrap_or_else(|e| e.into_inner());
            for summary in summaries.values() {
                summary.encode_into(&mut output);
            }
        }
        output
    }
}

use lazy_static::lazy_static;

lazy_static! {
    /// Global enhanced metrics registry for process-wide native histograms and
    /// summaries.
    static ref GLOBAL_ENHANCED_REGISTRY: EnhancedMetricsRegistry = EnhancedMetricsRegistry::new();
}

/// Access the process-global [`EnhancedMetricsRegistry`].
#[must_use]
pub fn global_enhanced_registry() -> &'static EnhancedMetricsRegistry {
    &GLOBAL_ENHANCED_REGISTRY
}

/// Register a native histogram in the global enhanced registry.
pub fn register_global_histogram(histogram: NativeHistogram) -> Arc<NativeHistogram> {
    GLOBAL_ENHANCED_REGISTRY.register_histogram(histogram)
}

/// Register a native summary in the global enhanced registry.
pub fn register_global_summary(summary: NativeSummary) -> Arc<NativeSummary> {
    GLOBAL_ENHANCED_REGISTRY.register_summary(summary)
}

/// Gather both the static `prometheus`-backed metrics and the global enhanced
/// (native histogram/summary) metrics into a single Prometheus text-format
/// string.
///
/// The static metrics from [`crate::gather_metrics`] are emitted first, followed
/// by the enhanced registry's output, so a single scrape endpoint can serve
/// every metric type.
#[must_use]
pub fn gather_enhanced_metrics() -> String {
    let mut output = crate::gather_metrics();
    if !output.is_empty() && !output.ends_with('\n') {
        output.push('\n');
    }
    output.push_str(&GLOBAL_ENHANCED_REGISTRY.encode());
    output
}
