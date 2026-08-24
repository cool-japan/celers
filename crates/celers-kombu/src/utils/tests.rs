//! Regression tests for `celers_kombu::utils` (the `mod.rs`-level
//! functions specifically -- `analysis` and `forecasting` carry their own
//! `tests` submodules colocated in their own files).
//!
//! Split out of `mod.rs` to keep that file under the workspace's
//! 2000-line-per-file policy.

use super::*;

// --- calculate_backoff_delay --------------------------------------------

#[test]
fn backoff_delay_without_jitter_is_deterministic() {
    assert_eq!(calculate_backoff_delay(0, 100, 60_000, 0.0), 100);
    assert_eq!(calculate_backoff_delay(2, 100, 60_000, 0.0), 400);
}

#[test]
fn backoff_jitter_is_randomized_not_a_pure_function_of_attempt() {
    // Regression test: the previous jitter formula derived its
    // "randomness" purely from `attempt` (`(attempt * 17) % ...`), so
    // every call with the same `attempt` returned the exact same
    // delay -- clients that all started backing off together would
    // retry in lockstep. With real randomness we expect to observe
    // more than one distinct value across enough samples.
    let delays: std::collections::HashSet<u64> = (0..50)
        .map(|_| calculate_backoff_delay(3, 10_000, 60_000, 1.0))
        .collect();
    assert!(
        delays.len() > 1,
        "expected randomized jitter to produce varying delays, got {delays:?}"
    );
}

#[test]
fn backoff_jitter_never_exceeds_the_unjittered_cap() {
    for _ in 0..100 {
        let delay = calculate_backoff_delay(3, 10_000, 60_000, 1.0);
        assert!(delay <= 60_000);
    }
}

// --- generate_deduplication_id ------------------------------------------

#[test]
fn dedup_id_is_stable_and_content_sensitive() {
    let id1 = generate_deduplication_id("my_task", b"args");
    let id2 = generate_deduplication_id("my_task", b"args");
    assert_eq!(id1, id2);

    let id3 = generate_deduplication_id("my_task", b"different");
    assert_ne!(id1, id3);
}

#[test]
fn dedup_id_does_not_collide_across_the_task_name_arg_boundary() {
    // Without length-prefixing, `("ab", b"cd")` and `("abc", b"d")`
    // would concatenate to the same byte stream and hash identically,
    // treating two unrelated tasks as duplicates of each other.
    let a = generate_deduplication_id("ab", b"cd");
    let b = generate_deduplication_id("abc", b"d");
    assert_ne!(a, b);
}

#[test]
fn fnv1a_matches_reference_vector() {
    // FNV-1a 64-bit of the empty string is the offset basis itself;
    // of b"a" it's a well-known reference value. Pinning these to a
    // literal confirms the constants/algorithm are the standard FNV-1a
    // definition (i.e. genuinely stable across toolchains), not just
    // "some" hash function.
    assert_eq!(fnv1a_hash(b""), 0xcbf2_9ce4_8422_2325);
    assert_eq!(fnv1a_hash(b"a"), 0xaf63_dc4c_8601_ec8c);
}

// --- calculate_load_distribution ----------------------------------------

#[test]
fn load_distribution_never_exceeds_total_workers() {
    // Regression test: `(proportion * total_workers).round()` summed
    // per-queue could exceed `total_workers` (e.g. `[1, 1]` with 3
    // workers: both queues round `1.5 -> 2`, summing to 4).
    let distribution = calculate_load_distribution(&[1, 1], 3);
    let total: usize = distribution.iter().map(|(_, w)| w).sum();
    assert_eq!(total, 3);
    // Spread, not piled onto a single queue.
    assert!(distribution.iter().all(|&(_, w)| w <= 2));
}

#[test]
fn load_distribution_sums_exactly_across_many_shapes() {
    let cases: &[(&[usize], usize)] = &[
        (&[100, 50, 200], 10),
        (&[1, 1, 1], 3),
        (&[1, 1, 1], 4),
        (&[7, 0, 0], 5),
        (&[3, 3, 3, 3, 3], 11),
        (&[1, 1, 1, 1, 1], 2),
    ];

    for &(sizes, total_workers) in cases {
        let distribution = calculate_load_distribution(sizes, total_workers);
        let total: usize = distribution.iter().map(|(_, w)| w).sum();
        assert_eq!(
            total, total_workers,
            "sizes={sizes:?} total_workers={total_workers}"
        );
    }
}

#[test]
fn load_distribution_empty_queue_gets_no_workers() {
    let distribution = calculate_load_distribution(&[0, 100], 4);
    let empty_queue_workers = distribution.iter().find(|&&(idx, _)| idx == 0).unwrap().1;
    assert_eq!(empty_queue_workers, 0);
}

// --- suggest_worker_scaling / estimate_processing_capacity -------------

#[test]
fn worker_scaling_never_adds_workers_for_an_empty_slow_queue() {
    // Regression test: integer division truncated
    // `1000 / avg_processing_time_ms` to 0 for any task slower than a
    // second, which made the function recommend *adding* workers
    // (current_workers scaled up by the 20% headroom) even for an
    // empty queue.
    let (workers, action) = suggest_worker_scaling(0, 5, 2000, 60);
    assert_ne!(action, "add");
    assert!(workers <= 5);
}

#[test]
fn worker_scaling_doctest_cases_hold() {
    let (workers, action) = suggest_worker_scaling(5000, 5, 100, 60);
    assert!(workers > 5);
    assert_eq!(action, "add");

    let (workers, action) = suggest_worker_scaling(10, 10, 100, 60);
    assert!(workers < 10);
    assert_eq!(action, "remove");
}

#[test]
fn processing_capacity_nonzero_for_sub_one_second_tasks() {
    // Regression test: integer division truncated
    // `1000 / avg_processing_time_ms` to 0 for any task slower than a
    // second, which then cascaded into an all-zero per-minute/per-hour
    // capacity even though the workload clearly processes messages.
    let (per_sec, per_min, per_hour) = estimate_processing_capacity(1, 2000, 1);
    assert_eq!(per_sec, 0); // < 1 msg/sec truly rounds down at 1-second granularity
    assert_eq!(per_min, 30);
    assert_eq!(per_hour, 1800);
}

#[test]
fn processing_capacity_doctest_case_holds() {
    let (per_sec, per_min, per_hour) = estimate_processing_capacity(10, 100, 4);
    assert_eq!(per_sec, 400);
    assert_eq!(per_min, 24_000);
    assert_eq!(per_hour, 1_440_000);
}

// --- detect_anomalies ----------------------------------------------------

#[test]
fn detect_anomalies_zero_stddev_baseline_scales_severity_by_relative_deviation() {
    // Regression test: against a perfectly flat baseline (stddev ==
    // 0.0), `deviation / (baseline_stddev * 3.0)` divided by zero into
    // `inf`, which `.min(1.0)` clamped to *maximum* severity for even
    // a one-unit blip.
    let baseline = vec![100u64, 100, 100];
    let tiny_blip = vec![101u64, 101, 101];
    let (is_anomaly, severity, _) = detect_anomalies(&tiny_blip, &baseline, 0.001);
    assert!(is_anomaly);
    assert!(
        severity < 0.5,
        "a 1% deviation off a flat baseline should not read as maximal severity, got {severity}"
    );

    let huge_spike = vec![1000u64, 1000, 1000];
    let (is_anomaly, severity, _) = detect_anomalies(&huge_spike, &baseline, 0.001);
    assert!(is_anomaly);
    assert!(
        severity > 0.9,
        "a 10x spike should still read as severe, got {severity}"
    );
}

#[test]
fn detect_anomalies_no_severity_is_ever_nan() {
    let (_, severity, _) = detect_anomalies(&[0, 0, 0], &[0, 0, 0], 1.0);
    assert!(!severity.is_nan());

    let (_, severity, _) = detect_anomalies(&[5, 5, 5], &[0, 0, 0], 1.0);
    assert!(!severity.is_nan());
}

#[test]
fn detect_anomalies_doctest_case_holds() {
    let current = vec![100u64, 105, 98, 102, 500];
    let baseline = vec![100u64, 105, 98, 102, 100];
    let (is_anomaly, severity, _) = detect_anomalies(&current, &baseline, 2.0);
    assert!(is_anomaly);
    assert!(severity > 0.5);
}
