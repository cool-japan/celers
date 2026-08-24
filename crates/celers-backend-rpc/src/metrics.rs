//! Client-side Prometheus-compatible metrics for the gRPC result backend.
//!
//! Tracks per-operation request counts, error counts, and latency distributions
//! (p50/p95/p99) using a ring-buffer of the most recent 1 000 samples.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use std::time::Duration;

// ──────────────────────────────────────────────────────────────────────────────
// Public enum
// ──────────────────────────────────────────────────────────────────────────────

/// Which RPC operation was executed.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum RpcOperation {
    StoreResult,
    GetResult,
    DeleteResult,
    SetExpiration,
    ChordInit,
    ChordUpdateState,
    ChordCompleteTask,
    ChordGetState,
}

impl RpcOperation {
    /// Iterate over all variants — used to pre-populate the per-op map.
    fn all() -> &'static [RpcOperation] {
        &[
            RpcOperation::StoreResult,
            RpcOperation::GetResult,
            RpcOperation::DeleteResult,
            RpcOperation::SetExpiration,
            RpcOperation::ChordInit,
            RpcOperation::ChordUpdateState,
            RpcOperation::ChordCompleteTask,
            RpcOperation::ChordGetState,
        ]
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Snapshot types (cheap to clone, non-atomic)
// ──────────────────────────────────────────────────────────────────────────────

/// Per-operation statistics at a single point in time.
pub struct OperationStats {
    pub requests: u64,
    pub errors: u64,
    /// Error rate in the range `[0.0, 1.0]`.
    pub error_rate: f64,
    pub mean_latency: Option<Duration>,
    pub p50_latency: Option<Duration>,
    pub p95_latency: Option<Duration>,
    pub p99_latency: Option<Duration>,
}

/// Snapshot of all metrics.  Cheap to clone; all values are plain integers /
/// `Option<Duration>`.
pub struct RpcMetricsSnapshot {
    pub total_requests: u64,
    pub total_errors: u64,
    pub operations: HashMap<RpcOperation, OperationStats>,
}

// ──────────────────────────────────────────────────────────────────────────────
// Internal per-operation state
// ──────────────────────────────────────────────────────────────────────────────

/// Maximum number of latency samples kept in the ring buffer.
const MAX_SAMPLES: usize = 1_000;

/// Thread-safe per-operation counter and latency ring buffer.
struct OperationMetrics {
    requests: AtomicU64,
    errors: AtomicU64,
    /// Raw latency samples in microseconds, capped at `MAX_SAMPLES`.
    latencies_us: Mutex<VecDeque<u64>>,
}

impl OperationMetrics {
    fn new() -> Self {
        Self {
            requests: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            latencies_us: Mutex::new(VecDeque::with_capacity(MAX_SAMPLES)),
        }
    }

    /// Record a single completed RPC call.
    ///
    /// `elapsed_us` — duration in microseconds.
    /// `is_error`   — whether the call returned an error.
    fn record(&self, elapsed_us: u64, is_error: bool) {
        self.requests.fetch_add(1, Ordering::Relaxed);
        if is_error {
            self.errors.fetch_add(1, Ordering::Relaxed);
        }

        // A poisoned lock still holds a perfectly usable ring buffer of
        // latency samples (metrics are best-effort observability data, not
        // correctness-critical state), so recover it instead of letting one
        // panicking thread turn every subsequent `record()` call — i.e.
        // every RPC on this backend — into a panic too.
        let mut guard = self
            .latencies_us
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if guard.len() >= MAX_SAMPLES {
            guard.pop_front();
        }
        guard.push_back(elapsed_us);
    }

    /// Snapshot the counters and return a sorted copy of the latency samples.
    ///
    /// Returns `(requests, errors, sorted_latencies)`.  `sorted_latencies` is
    /// `None` when no samples have been recorded yet.
    fn snapshot(&self) -> (u64, u64, Option<Vec<u64>>) {
        let requests = self.requests.load(Ordering::Relaxed);
        let errors = self.errors.load(Ordering::Relaxed);

        let guard = self
            .latencies_us
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let sorted = if guard.is_empty() {
            None
        } else {
            let mut v: Vec<u64> = guard.iter().copied().collect();
            v.sort_unstable();
            Some(v)
        };

        (requests, errors, sorted)
    }

    /// Reset counters and clear the latency ring buffer.
    fn reset(&self) {
        self.requests.store(0, Ordering::Relaxed);
        self.errors.store(0, Ordering::Relaxed);
        let mut guard = self
            .latencies_us
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        guard.clear();
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Percentile helper
// ──────────────────────────────────────────────────────────────────────────────

/// Compute the `p`-th percentile of a **sorted** slice using linear
/// interpolation between the two surrounding elements.
///
/// `p` must be in `[0.0, 100.0]`.  Returns the first element for an empty
/// slice.
pub fn percentile(sorted: &[u64], p: f64) -> u64 {
    assert!(
        (0.0..=100.0).contains(&p),
        "percentile p must be in [0.0, 100.0]"
    );

    let n = sorted.len();
    if n == 0 {
        return 0;
    }
    if n == 1 {
        return sorted[0];
    }

    // Compute the floating-point index.
    let float_idx = (p / 100.0) * (n - 1) as f64;
    let lo = float_idx.floor() as usize;
    let hi = float_idx.ceil() as usize;

    if lo == hi {
        return sorted[lo];
    }

    // Linear interpolation.
    let frac = float_idx - lo as f64;
    let interpolated = sorted[lo] as f64 + frac * (sorted[hi] as f64 - sorted[lo] as f64);
    interpolated.round() as u64
}

// ──────────────────────────────────────────────────────────────────────────────
// Central metrics object
// ──────────────────────────────────────────────────────────────────────────────

/// Central metrics object — `Arc`-shared between `GrpcResultBackend` instances
/// and callers who need to read or reset the metrics.
///
/// All fields (`HashMap<RpcOperation, OperationMetrics>`, where
/// `OperationMetrics` holds only `AtomicU64`s and a `Mutex<VecDeque<u64>>`)
/// are already `Send + Sync` on their own, so `RpcMetrics` derives both
/// automatically — no `unsafe impl` needed. See `test_rpc_metrics_is_send_sync`
/// below for a compile-time check that stays enforced if a future field
/// changes that.
pub struct RpcMetrics {
    per_op: HashMap<RpcOperation, OperationMetrics>,
}

impl RpcMetrics {
    /// Create a new `RpcMetrics` with all operation counters zeroed.
    pub fn new() -> Self {
        let per_op = RpcOperation::all()
            .iter()
            .map(|&op| (op, OperationMetrics::new()))
            .collect();
        Self { per_op }
    }

    /// Record a completed RPC call for `op`.
    pub fn record(&self, op: RpcOperation, elapsed: Duration, is_error: bool) {
        let elapsed_us = elapsed.as_micros() as u64;
        if let Some(metrics) = self.per_op.get(&op) {
            metrics.record(elapsed_us, is_error);
        }
    }

    /// Produce a point-in-time snapshot of all counters and latency stats.
    pub fn snapshot(&self) -> RpcMetricsSnapshot {
        let mut total_requests: u64 = 0;
        let mut total_errors: u64 = 0;
        let mut operations = HashMap::with_capacity(self.per_op.len());

        for (&op, op_metrics) in &self.per_op {
            let (requests, errors, maybe_sorted) = op_metrics.snapshot();
            total_requests += requests;
            total_errors += errors;

            let error_rate = if requests == 0 {
                0.0_f64
            } else {
                errors as f64 / requests as f64
            };

            let (mean_latency, p50_latency, p95_latency, p99_latency) =
                if let Some(ref sorted) = maybe_sorted {
                    let mean_us: f64 = sorted.iter().sum::<u64>() as f64 / sorted.len() as f64;
                    let mean = Duration::from_micros(mean_us.round() as u64);
                    let p50 = Duration::from_micros(percentile(sorted, 50.0));
                    let p95 = Duration::from_micros(percentile(sorted, 95.0));
                    let p99 = Duration::from_micros(percentile(sorted, 99.0));
                    (Some(mean), Some(p50), Some(p95), Some(p99))
                } else {
                    (None, None, None, None)
                };

            let stats = OperationStats {
                requests,
                errors,
                error_rate,
                mean_latency,
                p50_latency,
                p95_latency,
                p99_latency,
            };
            operations.insert(op, stats);
        }

        RpcMetricsSnapshot {
            total_requests,
            total_errors,
            operations,
        }
    }

    /// Reset all counters and latency samples to zero.
    pub fn reset(&self) {
        for op_metrics in self.per_op.values() {
            op_metrics.reset();
        }
    }
}

impl Default for RpcMetrics {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;

    fn fresh() -> RpcMetrics {
        RpcMetrics::new()
    }

    /// Compile-time check that `RpcMetrics` is `Send + Sync` via its
    /// fields' own auto-trait derivation (no `unsafe impl` involved). If a
    /// future field addition breaks this — e.g. an `Rc` or raw pointer
    /// sneaks in — this fails to *compile*, which is exactly the
    /// protection a hand-written `unsafe impl Send + Sync` would silently
    /// remove.
    #[test]
    fn test_rpc_metrics_is_send_sync() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<RpcMetrics>();
    }

    /// Regression test for the poisoning-intolerant `.expect("latencies_us
    /// poisoned")` this crate used to carry on `record()`/`snapshot()`/
    /// `reset()`. Deliberately poison the ring-buffer mutex by panicking
    /// while holding it on another thread, then confirm every subsequent
    /// operation on the *same* `RpcMetrics` still works instead of
    /// panicking too (which is exactly what would happen if a single
    /// transient panic anywhere in the process turned into every future
    /// RPC on the backend panicking forever).
    #[test]
    fn test_metrics_survive_poisoned_lock() {
        let metrics = std::sync::Arc::new(fresh());
        metrics.record(RpcOperation::GetResult, Duration::from_millis(1), false);

        // Poison the `latencies_us` mutex from another thread by panicking
        // while the lock is held.
        let poisoning = std::sync::Arc::clone(&metrics);
        let joined = std::thread::spawn(move || {
            let op_metrics = poisoning.per_op.get(&RpcOperation::GetResult).unwrap();
            let _guard = op_metrics.latencies_us.lock().unwrap();
            panic!("deliberately poisoning the lock for the regression test");
        })
        .join();
        assert!(joined.is_err(), "the spawned thread must have panicked");

        // Every operation that touches the now-poisoned mutex must still
        // work — recovering the guard instead of propagating the poison as
        // a panic on this (uninvolved) thread.
        metrics.record(RpcOperation::GetResult, Duration::from_millis(2), false);
        let snap = metrics.snapshot();
        assert_eq!(snap.operations[&RpcOperation::GetResult].requests, 2);
        metrics.reset();
        let snap = metrics.snapshot();
        assert_eq!(snap.operations[&RpcOperation::GetResult].requests, 0);
    }

    #[test]
    fn test_record_increments_request_count() {
        let m = fresh();
        m.record(RpcOperation::StoreResult, Duration::from_millis(5), false);
        m.record(RpcOperation::StoreResult, Duration::from_millis(10), false);
        let snap = m.snapshot();
        assert_eq!(snap.operations[&RpcOperation::StoreResult].requests, 2);
    }

    #[test]
    fn test_error_increments_error_count() {
        let m = fresh();
        m.record(RpcOperation::GetResult, Duration::from_millis(1), false);
        m.record(RpcOperation::GetResult, Duration::from_millis(2), true);
        m.record(RpcOperation::GetResult, Duration::from_millis(3), true);
        let snap = m.snapshot();
        let stats = &snap.operations[&RpcOperation::GetResult];
        assert_eq!(stats.requests, 3);
        assert_eq!(stats.errors, 2);
        assert!((stats.error_rate - 2.0 / 3.0).abs() < 1e-9);
    }

    #[test]
    fn test_empty_snapshot_has_none_latencies() {
        let snap = fresh().snapshot();
        for stats in snap.operations.values() {
            assert!(stats.mean_latency.is_none());
            assert!(stats.p50_latency.is_none());
            assert!(stats.p95_latency.is_none());
            assert!(stats.p99_latency.is_none());
        }
    }

    #[test]
    fn test_percentile_known_array() {
        // [10,20,30,40,50]: p50 idx=2 → 30; p75 idx=3 → 40
        let sorted: Vec<u64> = vec![10, 20, 30, 40, 50];
        assert_eq!(percentile(&sorted, 0.0), 10);
        assert_eq!(percentile(&sorted, 50.0), 30);
        assert_eq!(percentile(&sorted, 75.0), 40);
        assert_eq!(percentile(&sorted, 100.0), 50);
    }

    #[test]
    fn test_max_samples_cap() {
        let m = fresh();
        for i in 0..1_500_u64 {
            m.record(
                RpcOperation::DeleteResult,
                Duration::from_micros(i + 1),
                false,
            );
        }
        let (_, _, maybe_sorted) = m
            .per_op
            .get(&RpcOperation::DeleteResult)
            .unwrap()
            .snapshot();
        let sorted = maybe_sorted.expect("samples must be present");
        assert_eq!(
            sorted.len(),
            MAX_SAMPLES,
            "ring buffer must be capped at {MAX_SAMPLES}"
        );
    }

    #[test]
    fn test_reset_clears_all_counters() {
        let m = fresh();
        m.record(RpcOperation::ChordInit, Duration::from_millis(3), false);
        m.record(RpcOperation::ChordInit, Duration::from_millis(4), true);
        m.reset();
        let snap = m.snapshot();
        assert_eq!(snap.total_requests, 0);
        assert_eq!(snap.total_errors, 0);
        for stats in snap.operations.values() {
            assert_eq!(stats.requests, 0);
            assert!(stats.mean_latency.is_none());
        }
    }

    #[test]
    fn test_all_rpc_operation_variants_in_snapshot() {
        let snap = fresh().snapshot();
        for op in RpcOperation::all() {
            assert!(snap.operations.contains_key(op), "missing {:?}", op);
        }
        assert_eq!(snap.operations.len(), 8);
    }

    #[test]
    fn test_total_requests_sum_of_per_op() {
        let m = fresh();
        m.record(RpcOperation::StoreResult, Duration::from_millis(1), false);
        m.record(RpcOperation::StoreResult, Duration::from_millis(2), false);
        m.record(RpcOperation::GetResult, Duration::from_millis(1), false);
        m.record(RpcOperation::ChordGetState, Duration::from_millis(1), true);
        let snap = m.snapshot();
        let per_op_sum: u64 = snap.operations.values().map(|s| s.requests).sum();
        assert_eq!(snap.total_requests, per_op_sum);
        assert_eq!(snap.total_requests, 4);
        assert_eq!(snap.total_errors, 1);
    }

    #[test]
    fn test_latency_percentiles_computed() {
        let m = fresh();
        for i in 1_u64..=100 {
            m.record(RpcOperation::SetExpiration, Duration::from_micros(i), false);
        }
        let snap = m.snapshot();
        let stats = &snap.operations[&RpcOperation::SetExpiration];
        // p50 of [1..=100]: idx=49.5 → lerp(50,51)=50.5 → rounds to 50 or 51
        let p50_us = stats.p50_latency.unwrap().as_micros() as u64;
        assert!(p50_us == 50 || p50_us == 51, "p50={p50_us}");
        // p99: idx=98.01 → lerp(99,100)≈99
        let p99_us = stats.p99_latency.unwrap().as_micros() as u64;
        assert!((98..=100).contains(&p99_us), "p99={p99_us}");
        // mean of 1..=100 is 50.5
        let mean_us = stats.mean_latency.unwrap().as_micros() as u64;
        assert!(mean_us == 50 || mean_us == 51, "mean={mean_us}");
    }

    #[test]
    fn test_percentile_single_element() {
        let sorted = vec![42_u64];
        assert_eq!(percentile(&sorted, 0.0), 42);
        assert_eq!(percentile(&sorted, 50.0), 42);
        assert_eq!(percentile(&sorted, 99.0), 42);
        assert_eq!(percentile(&sorted, 100.0), 42);
    }
}
