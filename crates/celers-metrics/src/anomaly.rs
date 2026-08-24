//! Online statistical anomaly detection for numeric metric streams.
//!
//! This module provides a *stateful* anomaly detector built on an exponentially
//! weighted moving average (EWMA) of both the mean and the variance of a
//! stream. Each new observation is scored as a rolling z-score against the
//! baseline accumulated from the preceding points; values whose absolute
//! z-score exceeds a configurable sigma threshold are flagged.
//!
//! It complements the stateless helpers in [`crate::slo`]
//! ([`crate::slo::AnomalyThreshold`], [`crate::slo::detect_anomaly`]), which
//! require a precomputed baseline, by maintaining the baseline incrementally in
//! constant memory and adapting it as the stream evolves.
//!
//! ## Algorithm
//!
//! The incremental EWMA mean/variance recurrence (West / Finch) is:
//!
//! ```text
//! diff      = x - mean
//! increment = alpha * diff
//! mean'     = mean + increment
//! variance' = (1 - alpha) * (variance + diff * increment)
//! ```
//!
//! where `alpha` is the smoothing factor in `(0, 1]`. A point is scored
//! against the mean/variance accumulated *before* it is folded in, so a single
//! large spike is measured against the undisturbed baseline rather than a
//! baseline it has already shifted.
//!
//! ## Warm-up
//!
//! Until `warmup` observations have been seen, the variance estimate is not yet
//! trustworthy and the detector returns [`AnomalyVerdict::Warmup`] without
//! flagging anything.
//!
//! ## Baseline poisoning
//!
//! By default a flagged anomaly is *not* folded into the baseline
//! ([`AnomalyDetectorConfig::update_on_anomaly`] = `false`), so a sustained
//! outage or a one-off spike does not silently redefine "normal". Set it to
//! `true` for change-point-style adaptation.
//!
//! ```
//! use celers_metrics::{AnomalyDetector, AnomalyVerdict};
//!
//! // alpha 0.1, flag beyond 3 sigma, warm up over 20 samples.
//! let mut detector = AnomalyDetector::new(0.1, 3.0, 20);
//!
//! // Feed in-distribution noise around 100.
//! let noise = [100.0, 101.0, 99.0, 100.5, 99.5, 100.2, 99.8];
//! for _ in 0..3 {
//!     for &v in &noise {
//!         detector.observe(v);
//!     }
//! }
//!
//! // A large spike is flagged as high.
//! let verdict = detector.observe(180.0);
//! assert!(matches!(verdict, AnomalyVerdict::High { .. }));
//! ```

// ============================================================================
// Configuration
// ============================================================================

/// Configuration for an [`AnomalyDetector`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AnomalyDetectorConfig {
    /// EWMA smoothing factor in `(0.0, 1.0]`. Larger values weight recent
    /// observations more heavily (faster adaptation, noisier baseline);
    /// smaller values produce a smoother, slower baseline.
    pub alpha: f64,
    /// Number of standard deviations beyond which a point is flagged. A common
    /// choice is `3.0` (~99.7% of normal points fall within for a Gaussian).
    pub sigma_threshold: f64,
    /// Number of observations to accumulate before the detector starts
    /// flagging anomalies. During warm-up [`AnomalyVerdict::Warmup`] is
    /// returned.
    pub warmup: usize,
    /// Whether to fold a flagged anomaly into the EWMA baseline. Defaults to
    /// `false` to avoid baseline poisoning by outliers.
    pub update_on_anomaly: bool,
    /// Floor applied to the estimated standard deviation when scoring, to avoid
    /// dividing by a (near-)zero variance for perfectly flat streams. Points on
    /// a flat stream that differ by more than this floor are still detectable.
    pub min_std_dev: f64,
}

impl Default for AnomalyDetectorConfig {
    fn default() -> Self {
        Self {
            alpha: 0.1,
            sigma_threshold: 3.0,
            warmup: 20,
            update_on_anomaly: false,
            min_std_dev: 1e-9,
        }
    }
}

impl AnomalyDetectorConfig {
    /// Create a configuration with the core parameters, leaving
    /// `update_on_anomaly` and `min_std_dev` at their defaults.
    pub fn new(alpha: f64, sigma_threshold: f64, warmup: usize) -> Self {
        Self {
            alpha: alpha.clamp(f64::MIN_POSITIVE, 1.0),
            sigma_threshold: sigma_threshold.max(0.0),
            warmup,
            ..Self::default()
        }
    }

    /// Set whether flagged anomalies update the baseline.
    pub fn with_update_on_anomaly(mut self, update: bool) -> Self {
        self.update_on_anomaly = update;
        self
    }

    /// Set the minimum standard deviation used during scoring.
    pub fn with_min_std_dev(mut self, min_std_dev: f64) -> Self {
        self.min_std_dev = min_std_dev.max(0.0);
        self
    }
}

// ============================================================================
// Verdict
// ============================================================================

/// The outcome of feeding a single observation to an [`AnomalyDetector`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum AnomalyVerdict {
    /// The detector is still warming up; no judgement is made.
    Warmup,
    /// The observation is within the expected range.
    Normal {
        /// Signed z-score of the observation against the baseline.
        z_score: f64,
    },
    /// The observation is anomalously high (z-score above `+sigma_threshold`).
    High {
        /// Signed z-score (positive) of the observation.
        z_score: f64,
    },
    /// The observation is anomalously low (z-score below `-sigma_threshold`).
    Low {
        /// Signed z-score (negative) of the observation.
        z_score: f64,
    },
    /// The observation was `NaN` or infinite and was discarded without
    /// affecting the baseline, the variance, or the warm-up count.
    ///
    /// This is not a judgement about the underlying metric (unlike
    /// [`Warmup`](AnomalyVerdict::Warmup), it does not mean "not enough data
    /// yet") -- it means this particular reading was unusable and the
    /// detector's state is exactly as it was before `observe`/`score` was
    /// called. Retry with the next finite sample.
    Ignored,
}

impl AnomalyVerdict {
    /// Whether this verdict represents a flagged anomaly (high or low).
    pub fn is_anomaly(&self) -> bool {
        matches!(
            self,
            AnomalyVerdict::High { .. } | AnomalyVerdict::Low { .. }
        )
    }

    /// The signed z-score, if one was computed (`None` during warm-up, and
    /// for a discarded non-finite observation).
    pub fn z_score(&self) -> Option<f64> {
        match self {
            AnomalyVerdict::Warmup | AnomalyVerdict::Ignored => None,
            AnomalyVerdict::Normal { z_score }
            | AnomalyVerdict::High { z_score }
            | AnomalyVerdict::Low { z_score } => Some(*z_score),
        }
    }
}

// ============================================================================
// Detector
// ============================================================================

/// Online EWMA-based anomaly detector over a numeric stream.
///
/// Maintains an exponentially weighted mean and variance and scores each new
/// observation as a rolling z-score against the baseline. Constant memory,
/// O(1) per observation.
///
/// The detector is not internally synchronised; wrap it in a `Mutex` for shared
/// use, mirroring the convention used elsewhere in this crate.
#[derive(Debug, Clone)]
pub struct AnomalyDetector {
    config: AnomalyDetectorConfig,
    /// EWMA mean of the stream.
    mean: f64,
    /// EWMA variance of the stream.
    variance: f64,
    /// Total observations folded into the baseline.
    count: u64,
}

impl AnomalyDetector {
    /// Create a detector with the given smoothing factor, sigma threshold, and
    /// warm-up length (other parameters use [`AnomalyDetectorConfig`] defaults).
    pub fn new(alpha: f64, sigma_threshold: f64, warmup: usize) -> Self {
        Self::with_config(AnomalyDetectorConfig::new(alpha, sigma_threshold, warmup))
    }

    /// Create a detector from a full configuration.
    pub fn with_config(config: AnomalyDetectorConfig) -> Self {
        Self {
            config,
            mean: 0.0,
            variance: 0.0,
            count: 0,
        }
    }

    /// Borrow the detector configuration.
    pub fn config(&self) -> &AnomalyDetectorConfig {
        &self.config
    }

    /// Number of observations folded into the baseline so far.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Current EWMA mean estimate.
    pub fn mean(&self) -> f64 {
        self.mean
    }

    /// Current EWMA variance estimate.
    pub fn variance(&self) -> f64 {
        self.variance
    }

    /// Current EWMA standard deviation estimate.
    pub fn std_dev(&self) -> f64 {
        self.variance.max(0.0).sqrt()
    }

    /// Whether the detector has finished warming up.
    pub fn is_warmed_up(&self) -> bool {
        self.count >= self.config.warmup as u64
    }

    /// Reset the detector to its initial (empty) state.
    pub fn reset(&mut self) {
        self.mean = 0.0;
        self.variance = 0.0;
        self.count = 0;
    }

    /// Fold an observation into the EWMA baseline.
    fn update_baseline(&mut self, value: f64) {
        if self.count == 0 {
            // Seed the mean with the first value; variance starts at 0.
            self.mean = value;
            self.variance = 0.0;
        } else {
            let diff = value - self.mean;
            let increment = self.config.alpha * diff;
            self.mean += increment;
            self.variance = (1.0 - self.config.alpha) * (self.variance + diff * increment);
            if self.variance < 0.0 {
                self.variance = 0.0;
            }
        }
        self.count = self.count.saturating_add(1);
    }

    /// Score `value` against the current baseline without mutating state.
    ///
    /// Returns `Some(`[`AnomalyVerdict::Ignored`]`)` for a non-finite `value`
    /// regardless of warm-up state (mirroring [`observe`](Self::observe)),
    /// and `None` while warming up on an otherwise-finite value (the same
    /// condition under which `observe` returns [`AnomalyVerdict::Warmup`]).
    /// The returned verdict's z-score is computed against the pre-update
    /// baseline.
    pub fn score(&self, value: f64) -> Option<AnomalyVerdict> {
        if !value.is_finite() {
            return Some(AnomalyVerdict::Ignored);
        }
        if self.count < self.config.warmup as u64 {
            return None;
        }
        Some(self.classify(value))
    }

    /// Classify `value` against the current baseline (assumes warm-up complete).
    fn classify(&self, value: f64) -> AnomalyVerdict {
        let std_dev = self.std_dev().max(self.config.min_std_dev);
        let z_score = (value - self.mean) / std_dev;

        if z_score > self.config.sigma_threshold {
            AnomalyVerdict::High { z_score }
        } else if z_score < -self.config.sigma_threshold {
            AnomalyVerdict::Low { z_score }
        } else {
            AnomalyVerdict::Normal { z_score }
        }
    }

    /// Feed an observation and obtain a verdict.
    ///
    /// The point is scored against the baseline accumulated from preceding
    /// observations, then folded into the baseline (unless it was flagged as an
    /// anomaly and [`AnomalyDetectorConfig::update_on_anomaly`] is `false`).
    ///
    /// A non-finite `value` (`NaN` or infinite) is never folded into the
    /// baseline and returns [`AnomalyVerdict::Ignored`] without affecting
    /// `mean`, `variance`, or the warm-up count. Unlike an ordinary
    /// out-of-range value, `NaN` would otherwise poison `mean`/`variance`
    /// permanently: every comparison against `NaN` is `false`, so once folded
    /// in, the baseline stops updating (each new `diff`/`increment` also
    /// becomes `NaN`) and `classify` would silently return `Normal` for every
    /// subsequent observation for the life of the detector.
    pub fn observe(&mut self, value: f64) -> AnomalyVerdict {
        if !value.is_finite() {
            return AnomalyVerdict::Ignored;
        }

        // Still warming up: just absorb the value, no judgement yet.
        if self.count < self.config.warmup as u64 {
            self.update_baseline(value);
            return AnomalyVerdict::Warmup;
        }

        let verdict = self.classify(value);

        let should_update = !verdict.is_anomaly() || self.config.update_on_anomaly;
        if should_update {
            self.update_baseline(value);
        }

        verdict
    }

    /// Feed many observations, returning the verdict for each in order.
    pub fn observe_all(&mut self, values: &[f64]) -> Vec<AnomalyVerdict> {
        values.iter().map(|&v| self.observe(v)).collect()
    }
}

impl Default for AnomalyDetector {
    fn default() -> Self {
        Self::with_config(AnomalyDetectorConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Deterministic pseudo-random in-distribution noise generator.
    ///
    /// A small linear-congruential generator keeps the tests dependency-free
    /// and fully reproducible (no `rand`). Produces values centered on `mean`
    /// with a bounded spread.
    struct Noise {
        state: u64,
    }

    impl Noise {
        fn new(seed: u64) -> Self {
            Self { state: seed }
        }

        /// Next value in `[mean - spread, mean + spread)`.
        fn next(&mut self, mean: f64, spread: f64) -> f64 {
            // LCG constants (Numerical Recipes).
            self.state = self
                .state
                .wrapping_mul(6_364_136_223_846_793_005)
                .wrapping_add(1_442_695_040_888_963_407);
            let unit = (self.state >> 11) as f64 / (1u64 << 53) as f64; // [0,1)
            mean + (unit * 2.0 - 1.0) * spread
        }
    }

    #[test]
    fn warmup_returns_no_anomaly() {
        let mut detector = AnomalyDetector::new(0.2, 3.0, 10);
        // The first `warmup` observations must all be Warmup, even a huge one.
        for i in 0..10 {
            let v = if i == 5 { 1_000_000.0 } else { 100.0 };
            assert_eq!(detector.observe(v), AnomalyVerdict::Warmup);
        }
        assert!(detector.is_warmed_up());
    }

    #[test]
    fn flags_injected_spike_high() {
        let mut detector = AnomalyDetector::new(0.1, 3.0, 30);
        let mut noise = Noise::new(42);
        // Warm up + establish a stable baseline around 100 with small spread.
        for _ in 0..200 {
            let v = noise.next(100.0, 2.0);
            detector.observe(v);
        }
        assert!(detector.is_warmed_up());
        assert!(
            (detector.mean() - 100.0).abs() < 5.0,
            "mean {}",
            detector.mean()
        );

        // A clear upward spike must be flagged High.
        let verdict = detector.observe(200.0);
        assert!(
            matches!(verdict, AnomalyVerdict::High { .. }),
            "verdict {verdict:?}"
        );
        let z = verdict.z_score().expect("z-score present");
        assert!(z > 3.0, "z-score {z}");
    }

    #[test]
    fn flags_injected_spike_low() {
        let mut detector = AnomalyDetector::new(0.1, 3.0, 30);
        let mut noise = Noise::new(7);
        for _ in 0..200 {
            let v = noise.next(100.0, 2.0);
            detector.observe(v);
        }
        // A clear downward spike must be flagged Low.
        let verdict = detector.observe(10.0);
        assert!(
            matches!(verdict, AnomalyVerdict::Low { .. }),
            "verdict {verdict:?}"
        );
        assert!(verdict.z_score().expect("z") < -3.0);
    }

    #[test]
    fn does_not_flag_in_distribution_noise() {
        let mut detector = AnomalyDetector::new(0.05, 3.0, 50);
        let mut noise = Noise::new(123);
        // Warm up.
        for _ in 0..50 {
            detector.observe(noise.next(100.0, 3.0));
        }
        // Feed a long run of in-distribution noise; none should be flagged at
        // 3 sigma for a tight, well-behaved distribution.
        let mut false_positives = 0;
        for _ in 0..2000 {
            let v = noise.next(100.0, 3.0);
            if detector.observe(v).is_anomaly() {
                false_positives += 1;
            }
        }
        assert_eq!(
            false_positives, 0,
            "expected no false positives on bounded in-distribution noise"
        );
    }

    #[test]
    fn spike_does_not_poison_baseline_by_default() {
        let mut detector = AnomalyDetector::new(0.2, 3.0, 30);
        let mut noise = Noise::new(99);
        for _ in 0..100 {
            detector.observe(noise.next(100.0, 1.0));
        }
        let mean_before = detector.mean();
        let count_before = detector.count();

        // A flagged spike should NOT move the baseline (update_on_anomaly=false).
        let verdict = detector.observe(500.0);
        assert!(verdict.is_anomaly());
        assert_eq!(detector.count(), count_before, "anomaly was folded in");
        assert!(
            (detector.mean() - mean_before).abs() < 1e-9,
            "baseline shifted from {mean_before} to {}",
            detector.mean()
        );

        // A subsequent normal value is still judged against the clean baseline.
        let after = detector.observe(101.0);
        assert!(!after.is_anomaly(), "verdict {after:?}");
    }

    #[test]
    fn update_on_anomaly_adapts_baseline() {
        let cfg = AnomalyDetectorConfig::new(0.5, 3.0, 5).with_update_on_anomaly(true);
        let mut detector = AnomalyDetector::with_config(cfg);
        for _ in 0..5 {
            detector.observe(10.0);
        }
        let mean_before = detector.mean();
        // With update_on_anomaly, the flagged value is folded in and moves mean.
        let verdict = detector.observe(1000.0);
        assert!(verdict.is_anomaly());
        assert!(
            detector.mean() > mean_before,
            "baseline did not adapt: {} -> {}",
            mean_before,
            detector.mean()
        );
        assert!(detector.count() > 5);
    }

    #[test]
    fn flat_stream_uses_min_std_dev_floor() {
        // A perfectly flat stream has zero variance. The min_std_dev floor must
        // keep z-scores finite and allow a clear jump to still be detected.
        let cfg = AnomalyDetectorConfig::new(0.3, 3.0, 5).with_min_std_dev(1e-6);
        let mut detector = AnomalyDetector::with_config(cfg);
        for _ in 0..5 {
            assert_eq!(detector.observe(50.0), AnomalyVerdict::Warmup);
        }
        // Next identical value: z-score == 0, not an anomaly, finite.
        let same = detector.observe(50.0);
        assert!(!same.is_anomaly());
        assert!(same.z_score().expect("z").is_finite());

        // A large jump on the (near-)flat stream is detectable.
        let jump = detector.observe(50_000.0);
        assert!(jump.is_anomaly(), "verdict {jump:?}");
        assert!(jump.z_score().expect("z").is_finite());
    }

    #[test]
    fn score_does_not_mutate_state() {
        let mut detector = AnomalyDetector::new(0.2, 3.0, 3);
        for _ in 0..3 {
            detector.observe(100.0);
        }
        let count = detector.count();
        let mean = detector.mean();
        // score() must be read-only.
        let _ = detector.score(100.0);
        let _ = detector.score(10_000.0);
        assert_eq!(detector.count(), count);
        assert!((detector.mean() - mean).abs() < 1e-12);
    }

    #[test]
    fn score_returns_none_during_warmup() {
        let detector = AnomalyDetector::new(0.2, 3.0, 5);
        assert!(detector.score(100.0).is_none());
    }

    #[test]
    fn observe_all_returns_per_value_verdicts() {
        let mut detector = AnomalyDetector::new(0.3, 3.0, 3);
        let verdicts = detector.observe_all(&[1.0, 1.0, 1.0, 1.0, 1.0]);
        assert_eq!(verdicts.len(), 5);
        // First three are warmup.
        assert_eq!(verdicts[0], AnomalyVerdict::Warmup);
        assert_eq!(verdicts[2], AnomalyVerdict::Warmup);
        // Remaining identical values are not anomalies.
        assert!(!verdicts[3].is_anomaly());
        assert!(!verdicts[4].is_anomaly());
    }

    #[test]
    fn reset_clears_state() {
        let mut detector = AnomalyDetector::new(0.2, 3.0, 3);
        detector.observe_all(&[100.0, 100.0, 100.0, 100.0]);
        assert!(detector.count() > 0);
        detector.reset();
        assert_eq!(detector.count(), 0);
        assert_eq!(detector.mean(), 0.0);
        assert_eq!(detector.variance(), 0.0);
        assert!(!detector.is_warmed_up());
    }

    #[test]
    fn verdict_helpers() {
        let high = AnomalyVerdict::High { z_score: 5.0 };
        assert!(high.is_anomaly());
        assert_eq!(high.z_score(), Some(5.0));
        let normal = AnomalyVerdict::Normal { z_score: 0.5 };
        assert!(!normal.is_anomaly());
        assert_eq!(AnomalyVerdict::Warmup.z_score(), None);
        assert_eq!(AnomalyVerdict::Ignored.z_score(), None);
        assert!(!AnomalyVerdict::Ignored.is_anomaly());
    }

    #[test]
    fn nan_observation_is_ignored_and_does_not_poison_baseline() {
        let mut detector = AnomalyDetector::new(0.1, 3.0, 30);
        let mut noise = Noise::new(11);
        for _ in 0..200 {
            detector.observe(noise.next(100.0, 2.0));
        }
        let mean_before = detector.mean();
        let variance_before = detector.variance();
        let count_before = detector.count();

        // A NaN reading must be discarded, not folded into the EWMA
        // baseline: state must be bit-for-bit unchanged.
        assert_eq!(detector.observe(f64::NAN), AnomalyVerdict::Ignored);
        assert_eq!(
            detector.count(),
            count_before,
            "NaN changed the observation count"
        );
        assert_eq!(detector.mean(), mean_before, "NaN poisoned the mean");
        assert_eq!(
            detector.variance(),
            variance_before,
            "NaN poisoned the variance"
        );
        assert!(!detector.mean().is_nan());

        // Positive and negative infinity must be discarded too.
        assert_eq!(detector.observe(f64::INFINITY), AnomalyVerdict::Ignored);
        assert_eq!(detector.observe(f64::NEG_INFINITY), AnomalyVerdict::Ignored);
        assert_eq!(detector.count(), count_before);
        assert_eq!(detector.mean(), mean_before);

        // The detector must still be able to flag a real spike afterwards --
        // before the fix, one NaN permanently disabled detection because
        // every subsequent z-score comparison against a NaN mean/variance is
        // `false`, so `classify` always fell through to `Normal`.
        let verdict = detector.observe(200.0);
        assert!(
            matches!(verdict, AnomalyVerdict::High { .. }),
            "verdict {verdict:?}"
        );
        let z = verdict.z_score().expect("z-score present");
        assert!(z.is_finite() && z > 3.0, "z-score {z}");
    }

    #[test]
    fn nan_observation_during_warmup_does_not_count_or_poison() {
        let mut detector = AnomalyDetector::new(0.2, 3.0, 5);

        assert_eq!(detector.observe(f64::NAN), AnomalyVerdict::Ignored);
        assert_eq!(detector.count(), 0, "NaN must not count toward warm-up");
        assert!(!detector.is_warmed_up());

        for _ in 0..5 {
            assert_eq!(detector.observe(10.0), AnomalyVerdict::Warmup);
        }
        assert!(detector.is_warmed_up());
        assert!(!detector.mean().is_nan());
        assert!((detector.mean() - 10.0).abs() < 1e-9);
    }

    #[test]
    fn score_ignores_non_finite_value_regardless_of_warmup() {
        // Still warming up: an ordinary value scores `None`, but a
        // non-finite one is reported as `Ignored`, not conflated with
        // "insufficient data yet".
        let warming_up = AnomalyDetector::new(0.2, 3.0, 5);
        assert_eq!(warming_up.score(f64::NAN), Some(AnomalyVerdict::Ignored));
        assert_eq!(warming_up.score(100.0), None);

        let mut warmed_up = AnomalyDetector::new(0.2, 3.0, 3);
        warmed_up.observe_all(&[100.0, 100.0, 100.0]);
        assert_eq!(warmed_up.score(f64::NAN), Some(AnomalyVerdict::Ignored));
        assert_eq!(
            warmed_up.score(f64::INFINITY),
            Some(AnomalyVerdict::Ignored)
        );
    }
}
