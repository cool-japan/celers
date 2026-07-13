//! Task affinity: worker-to-task matching by labels.
//!
//! This module implements a label-based affinity model that lets a worker
//! advertise a flat set of capability *labels* (for example
//! `{"gpu", "region:eu", "mem:high"}`) and lets a task declare *affinity
//! requirements* against those labels:
//!
//! - **Required** labels — the worker must carry *all* of them (hard constraint).
//! - **Preferred** labels — scored but not mandatory; each contributes a
//!   configurable integer weight to the match score when present.
//! - **Anti-affinity** labels — the worker must carry *none* of them
//!   (hard constraint, the inverse of *required*).
//!
//! The matcher exposes two primitives:
//!
//! - [`can_run`] — `true` when every required label is present and no
//!   anti-affinity label is present (admission gate).
//! - [`match_score`] — a deterministic [`i64`] preference score (higher is a
//!   better fit). Integer weights are used deliberately so that scores order
//!   total-and-stably without floating-point comparison hazards.
//!
//! Unlike [`crate::routing`] (tags + key/value capabilities) and
//! [`crate::feature_flags`] (feature gating), this module uses a single flat
//! label namespace and adds true *anti-affinity* (must-NOT-have) matching,
//! making it a drop-in admission check for label-scheduling style workloads.
//!
//! # Integration
//!
//! A worker holds an [`AffinityRegistry`] mapping task names to their
//! [`TaskAffinity`]. Affinity is **off by default**: a worker with no registry,
//! or a task with no registered affinity, admits everything (no-op). When a task
//! *does* declare affinity, the worker's [`WorkerLabels`] are matched against it
//! before execution and the task is deferred (requeued) if the worker cannot
//! serve it.
//!
//! # Example
//!
//! ```
//! use celers_worker::affinity::{WorkerLabels, TaskAffinity};
//!
//! // Worker advertises its capabilities.
//! let worker = WorkerLabels::from_iter(["gpu", "region:eu", "mem:high"]);
//!
//! // A task requires a GPU in the EU region, prefers high memory, and must
//! // never land on a "spot" (preemptible) worker.
//! let affinity = TaskAffinity::new()
//!     .require("gpu")
//!     .require("region:eu")
//!     .prefer("mem:high")
//!     .anti("spot");
//!
//! assert!(affinity.can_run(&worker));
//! assert!(affinity.match_score(&worker) > 0);
//! ```

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashSet};

/// Default weight contributed by a preferred label with no explicit weight.
pub const DEFAULT_PREFERENCE_WEIGHT: i64 = 1;

/// A flat set of capability labels advertised by a worker.
///
/// Labels are arbitrary strings; common conventions include bare flags
/// (`"gpu"`) and `key:value` pairs (`"region:eu"`). Matching is exact and
/// case-sensitive.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerLabels {
    labels: HashSet<String>,
}

impl WorkerLabels {
    /// Create an empty label set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a label (builder style).
    #[must_use]
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.labels.insert(label.into());
        self
    }

    /// Insert a label, returning `true` if it was newly added.
    pub fn insert(&mut self, label: impl Into<String>) -> bool {
        self.labels.insert(label.into())
    }

    /// Remove a label, returning `true` if it was present.
    pub fn remove(&mut self, label: &str) -> bool {
        self.labels.remove(label)
    }

    /// Check whether a specific label is present.
    pub fn has(&self, label: &str) -> bool {
        self.labels.contains(label)
    }

    /// Check whether *all* of the given labels are present.
    pub fn has_all<'a, I>(&self, labels: I) -> bool
    where
        I: IntoIterator<Item = &'a String>,
    {
        labels.into_iter().all(|l| self.labels.contains(l))
    }

    /// Check whether *any* of the given labels are present.
    pub fn has_any<'a, I>(&self, labels: I) -> bool
    where
        I: IntoIterator<Item = &'a String>,
    {
        labels.into_iter().any(|l| self.labels.contains(l))
    }

    /// Borrow the underlying label set.
    pub fn labels(&self) -> &HashSet<String> {
        &self.labels
    }

    /// Number of labels advertised.
    pub fn len(&self) -> usize {
        self.labels.len()
    }

    /// Whether no labels are advertised.
    pub fn is_empty(&self) -> bool {
        self.labels.is_empty()
    }

    /// Merge another label set into this one.
    pub fn merge(&mut self, other: &WorkerLabels) {
        self.labels.extend(other.labels.iter().cloned());
    }

    /// Remove every label.
    pub fn clear(&mut self) {
        self.labels.clear();
    }

    /// Convenience: does this worker satisfy the hard constraints of `affinity`?
    ///
    /// Equivalent to [`TaskAffinity::can_run`].
    pub fn can_run(&self, affinity: &TaskAffinity) -> bool {
        affinity.can_run(self)
    }

    /// Convenience: deterministic preference score for `affinity`.
    ///
    /// Equivalent to [`TaskAffinity::match_score`].
    pub fn match_score(&self, affinity: &TaskAffinity) -> i64 {
        affinity.match_score(self)
    }
}

impl<S> FromIterator<S> for WorkerLabels
where
    S: Into<String>,
{
    fn from_iter<I: IntoIterator<Item = S>>(iter: I) -> Self {
        Self {
            labels: iter.into_iter().map(Into::into).collect(),
        }
    }
}

impl std::fmt::Display for WorkerLabels {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Sort for a stable, deterministic rendering.
        let mut labels: Vec<&str> = self.labels.iter().map(String::as_str).collect();
        labels.sort_unstable();
        write!(f, "WorkerLabels[{}]", labels.join(", "))
    }
}

/// Affinity requirements declared by a task against worker labels.
///
/// Three independent constraints are combined:
///
/// - `required`: every label here must be present on the worker.
/// - `anti`: no label here may be present on the worker (anti-affinity).
/// - `preferred`: weighted labels that boost the match score when present but
///   never affect admission.
///
/// An empty [`TaskAffinity`] places no constraints: it admits any worker
/// ([`can_run`] is `true`) with a score of `0`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskAffinity {
    /// Labels the worker must carry (all).
    #[serde(skip_serializing_if = "HashSet::is_empty", default)]
    required: HashSet<String>,

    /// Labels the worker must NOT carry (any of these disqualifies the worker).
    #[serde(skip_serializing_if = "HashSet::is_empty", default)]
    anti: HashSet<String>,

    /// Preferred labels with their scoring weights.
    ///
    /// A [`BTreeMap`] is used so iteration order is deterministic, which keeps
    /// score accumulation and any derived ordering stable across runs.
    #[serde(skip_serializing_if = "BTreeMap::is_empty", default)]
    preferred: BTreeMap<String, i64>,
}

impl TaskAffinity {
    /// Create empty affinity requirements (matches every worker).
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a required label (builder style).
    #[must_use]
    pub fn require(mut self, label: impl Into<String>) -> Self {
        self.required.insert(label.into());
        self
    }

    /// Add several required labels (builder style).
    #[must_use]
    pub fn require_all<I, S>(mut self, labels: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.required.extend(labels.into_iter().map(Into::into));
        self
    }

    /// Add an anti-affinity label the worker must NOT carry (builder style).
    #[must_use]
    pub fn anti(mut self, label: impl Into<String>) -> Self {
        self.anti.insert(label.into());
        self
    }

    /// Add several anti-affinity labels (builder style).
    #[must_use]
    pub fn anti_all<I, S>(mut self, labels: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.anti.extend(labels.into_iter().map(Into::into));
        self
    }

    /// Add a preferred label with the [default weight](DEFAULT_PREFERENCE_WEIGHT)
    /// (builder style).
    #[must_use]
    pub fn prefer(self, label: impl Into<String>) -> Self {
        self.prefer_weighted(label, DEFAULT_PREFERENCE_WEIGHT)
    }

    /// Add a preferred label with an explicit weight (builder style).
    ///
    /// Re-adding the same label overwrites its weight. Non-positive weights are
    /// permitted (a `0` weight contributes nothing; a negative weight can be
    /// used to softly penalize an otherwise admissible worker).
    #[must_use]
    pub fn prefer_weighted(mut self, label: impl Into<String>, weight: i64) -> Self {
        self.preferred.insert(label.into(), weight);
        self
    }

    /// Whether this task imposes any *hard* constraint (required or anti).
    ///
    /// When `false`, [`can_run`](Self::can_run) is always `true`.
    pub fn has_constraints(&self) -> bool {
        !self.required.is_empty() || !self.anti.is_empty()
    }

    /// Whether this task carries any preferences.
    pub fn has_preferences(&self) -> bool {
        !self.preferred.is_empty()
    }

    /// Whether this affinity is entirely empty (no constraints, no preferences).
    pub fn is_empty(&self) -> bool {
        self.required.is_empty() && self.anti.is_empty() && self.preferred.is_empty()
    }

    /// Required labels.
    pub fn required(&self) -> &HashSet<String> {
        &self.required
    }

    /// Anti-affinity labels.
    pub fn anti_labels(&self) -> &HashSet<String> {
        &self.anti
    }

    /// Preferred labels with their weights.
    pub fn preferred(&self) -> &BTreeMap<String, i64> {
        &self.preferred
    }

    /// Maximum achievable preference score (sum of all positive weights).
    ///
    /// Useful to normalize [`match_score`](Self::match_score) into a ratio.
    /// Negative weights are excluded since they cannot raise the score.
    pub fn max_score(&self) -> i64 {
        self.preferred.values().filter(|w| **w > 0).copied().sum()
    }

    /// Hard-constraint check: can a worker with `labels` run this task?
    ///
    /// Returns `true` iff *all* required labels are present and *no*
    /// anti-affinity label is present. Empty affinity admits everything.
    pub fn can_run(&self, labels: &WorkerLabels) -> bool {
        // Every required label must be present.
        if !labels.has_all(self.required.iter()) {
            return false;
        }
        // No anti-affinity label may be present.
        if labels.has_any(self.anti.iter()) {
            return false;
        }
        true
    }

    /// Deterministic preference score for a worker with `labels`.
    ///
    /// Returns the summed weight of every preferred label the worker carries.
    /// Workers that fail the hard constraints score [`i64::MIN`] so that an
    /// inadmissible worker can never out-rank an admissible one in an ordering.
    /// An admissible worker matching no preferred labels scores `0`.
    pub fn match_score(&self, labels: &WorkerLabels) -> i64 {
        if !self.can_run(labels) {
            return i64::MIN;
        }
        // Deterministic accumulation: BTreeMap iterates in sorted key order.
        self.preferred
            .iter()
            .filter(|(label, _)| labels.has(label))
            .map(|(_, weight)| *weight)
            .sum()
    }

    /// Number of preferred labels the worker matches.
    pub fn matched_preferences(&self, labels: &WorkerLabels) -> usize {
        self.preferred
            .keys()
            .filter(|label| labels.has(label))
            .count()
    }

    /// Validate internal consistency.
    ///
    /// A label that is both *required* and *anti* can never be satisfied, so it
    /// is reported as an error. (A label that is both required and preferred is
    /// allowed — it simply also contributes to the score.)
    pub fn validate(&self) -> Result<(), String> {
        for label in &self.required {
            if self.anti.contains(label) {
                return Err(format!(
                    "label '{}' cannot be both required and anti-affinity",
                    label
                ));
            }
        }
        Ok(())
    }
}

impl std::fmt::Display for TaskAffinity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut required: Vec<&str> = self.required.iter().map(String::as_str).collect();
        required.sort_unstable();
        let mut anti: Vec<&str> = self.anti.iter().map(String::as_str).collect();
        anti.sort_unstable();
        // BTreeMap already yields sorted keys.
        let preferred: Vec<String> = self
            .preferred
            .iter()
            .map(|(label, weight)| format!("{}={}", label, weight))
            .collect();
        write!(
            f,
            "TaskAffinity[required:{}; anti:{}; preferred:{}]",
            required.join(","),
            anti.join(","),
            preferred.join(",")
        )
    }
}

/// Free-function form of [`TaskAffinity::can_run`].
///
/// Returns `true` when `worker_labels` satisfies the hard constraints
/// (all required present, no anti-affinity present) of `task_affinity`.
pub fn can_run(worker_labels: &WorkerLabels, task_affinity: &TaskAffinity) -> bool {
    task_affinity.can_run(worker_labels)
}

/// Free-function form of [`TaskAffinity::match_score`].
///
/// Returns the deterministic [`i64`] preference score; [`i64::MIN`] when the
/// worker is inadmissible.
pub fn match_score(worker_labels: &WorkerLabels, task_affinity: &TaskAffinity) -> i64 {
    task_affinity.match_score(worker_labels)
}

/// Outcome of an affinity admission check for a single task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AffinityDecision {
    /// No affinity is configured for this task — admit unconditionally.
    NoAffinity,
    /// The worker satisfies the task's affinity; admit with this preference
    /// score (higher is a better fit).
    Admit(i64),
    /// The worker cannot satisfy the task's hard constraints; defer/skip it.
    Defer,
}

impl AffinityDecision {
    /// Whether the task should be admitted (either no affinity, or satisfied).
    pub fn is_admitted(&self) -> bool {
        !matches!(self, AffinityDecision::Defer)
    }

    /// Whether the task must be deferred (worker cannot serve it).
    pub fn is_deferred(&self) -> bool {
        matches!(self, AffinityDecision::Defer)
    }

    /// The preference score, if admitted with one (`0` for [`AffinityDecision::NoAffinity`]).
    pub fn score(&self) -> Option<i64> {
        match self {
            AffinityDecision::Admit(score) => Some(*score),
            AffinityDecision::NoAffinity => Some(0),
            AffinityDecision::Defer => None,
        }
    }
}

/// A registry mapping task names to their affinity requirements.
///
/// Held by a worker to perform admission checks. Tasks without a registered
/// entry are admitted unconditionally ([`AffinityDecision::NoAffinity`]), so
/// the registry is a no-op until populated — affinity is opt-in per task type.
#[derive(Debug, Clone, Default)]
pub struct AffinityRegistry {
    by_task: std::collections::HashMap<String, TaskAffinity>,
}

impl AffinityRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register (or replace) the affinity requirements for a task name
    /// (builder style).
    #[must_use]
    pub fn with_task(mut self, task_name: impl Into<String>, affinity: TaskAffinity) -> Self {
        self.by_task.insert(task_name.into(), affinity);
        self
    }

    /// Register (or replace) the affinity requirements for a task name.
    pub fn set(&mut self, task_name: impl Into<String>, affinity: TaskAffinity) {
        self.by_task.insert(task_name.into(), affinity);
    }

    /// Remove the affinity requirements for a task name, returning the previous
    /// value if any.
    pub fn remove(&mut self, task_name: &str) -> Option<TaskAffinity> {
        self.by_task.remove(task_name)
    }

    /// Look up the affinity requirements for a task name.
    pub fn get(&self, task_name: &str) -> Option<&TaskAffinity> {
        self.by_task.get(task_name)
    }

    /// Whether any affinity is registered for a task name that imposes a hard
    /// constraint.
    pub fn has_constraints(&self, task_name: &str) -> bool {
        self.by_task
            .get(task_name)
            .is_some_and(TaskAffinity::has_constraints)
    }

    /// Number of registered task affinities.
    pub fn len(&self) -> usize {
        self.by_task.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.by_task.is_empty()
    }

    /// Clear all registered affinities.
    pub fn clear(&mut self) {
        self.by_task.clear();
    }

    /// Decide whether a worker with `worker_labels` should admit a task named
    /// `task_name`.
    ///
    /// - No registered affinity (or an empty one) → [`AffinityDecision::NoAffinity`].
    /// - Registered affinity satisfied → [`AffinityDecision::Admit`] with score.
    /// - Registered affinity unsatisfiable → [`AffinityDecision::Defer`].
    pub fn decide(&self, task_name: &str, worker_labels: &WorkerLabels) -> AffinityDecision {
        match self.by_task.get(task_name) {
            None => AffinityDecision::NoAffinity,
            Some(affinity) if affinity.is_empty() => AffinityDecision::NoAffinity,
            Some(affinity) => {
                if affinity.can_run(worker_labels) {
                    AffinityDecision::Admit(affinity.match_score(worker_labels))
                } else {
                    AffinityDecision::Defer
                }
            }
        }
    }

    /// Validate every registered affinity, returning the first error found
    /// (prefixed with the offending task name).
    pub fn validate(&self) -> Result<(), String> {
        // Sort task names for deterministic error reporting.
        let mut names: Vec<&String> = self.by_task.keys().collect();
        names.sort_unstable();
        for name in names {
            if let Some(affinity) = self.by_task.get(name) {
                affinity
                    .validate()
                    .map_err(|e| format!("task '{}': {}", name, e))?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn worker(labels: &[&str]) -> WorkerLabels {
        WorkerLabels::from_iter(labels.iter().copied())
    }

    #[test]
    fn empty_affinity_matches_everything() {
        let affinity = TaskAffinity::new();
        assert!(affinity.is_empty());

        // Even an empty worker is admitted by empty affinity.
        let empty_worker = WorkerLabels::new();
        assert!(affinity.can_run(&empty_worker));
        assert_eq!(affinity.match_score(&empty_worker), 0);

        // A labeled worker is also admitted, still scoring 0 (no preferences).
        let labeled = worker(&["gpu", "region:eu"]);
        assert!(can_run(&labeled, &affinity));
        assert_eq!(match_score(&labeled, &affinity), 0);
    }

    #[test]
    fn required_all_match_passes() {
        let affinity = TaskAffinity::new()
            .require("gpu")
            .require("region:eu")
            .require("mem:high");
        let w = worker(&["gpu", "region:eu", "mem:high", "ssd"]);
        assert!(affinity.can_run(&w));
        // No preferred labels => score 0 even though admitted.
        assert_eq!(affinity.match_score(&w), 0);
    }

    #[test]
    fn missing_required_fails() {
        let affinity = TaskAffinity::new().require("gpu").require("region:eu");

        // Missing one of the two required labels.
        let w = worker(&["gpu", "region:us"]);
        assert!(!affinity.can_run(&w));
        // Inadmissible workers score i64::MIN.
        assert_eq!(affinity.match_score(&w), i64::MIN);

        // Missing all required labels.
        let w2 = worker(&["cpu"]);
        assert!(!can_run(&w2, &affinity));
    }

    #[test]
    fn anti_affinity_rejects() {
        let affinity = TaskAffinity::new().require("gpu").anti("spot");

        // Worker carries the anti-affinity label -> rejected even though the
        // required label is present.
        let w = worker(&["gpu", "spot"]);
        assert!(!affinity.can_run(&w));
        assert_eq!(affinity.match_score(&w), i64::MIN);

        // Same worker minus the forbidden label -> admitted.
        let w_ok = worker(&["gpu"]);
        assert!(affinity.can_run(&w_ok));
    }

    #[test]
    fn anti_affinity_only_no_required() {
        // Anti-affinity is a hard constraint on its own.
        let affinity = TaskAffinity::new().anti("gpu");
        assert!(affinity.has_constraints());

        assert!(!affinity.can_run(&worker(&["gpu"])));
        assert!(affinity.can_run(&worker(&["cpu"])));
        // Empty worker has no forbidden label, so it is admitted.
        assert!(affinity.can_run(&WorkerLabels::new()));
    }

    #[test]
    fn preferred_increases_score() {
        let affinity = TaskAffinity::new()
            .require("gpu")
            .prefer("mem:high")
            .prefer("ssd");

        // Required only -> base 0.
        let base = worker(&["gpu"]);
        assert_eq!(affinity.match_score(&base), 0);

        // One preferred -> +1.
        let one_pref = worker(&["gpu", "mem:high"]);
        assert_eq!(affinity.match_score(&one_pref), 1);
        assert_eq!(affinity.matched_preferences(&one_pref), 1);

        // Both preferred -> +2 and strictly greater than the one-pref worker.
        let both_pref = worker(&["gpu", "mem:high", "ssd"]);
        assert_eq!(affinity.match_score(&both_pref), 2);
        assert!(affinity.match_score(&both_pref) > affinity.match_score(&one_pref));
        assert!(affinity.match_score(&one_pref) > affinity.match_score(&base));
    }

    #[test]
    fn weighted_preferences_accumulate() {
        let affinity = TaskAffinity::new()
            .prefer_weighted("gpu", 10)
            .prefer_weighted("ssd", 3)
            .prefer_weighted("mem:high", 5);

        assert_eq!(affinity.max_score(), 18);

        let none = WorkerLabels::new();
        assert_eq!(affinity.match_score(&none), 0);

        let some = worker(&["gpu", "ssd"]);
        assert_eq!(affinity.match_score(&some), 13);

        let all = worker(&["gpu", "ssd", "mem:high"]);
        assert_eq!(affinity.match_score(&all), 18);
    }

    #[test]
    fn negative_and_zero_weights() {
        let affinity = TaskAffinity::new()
            .require("gpu")
            .prefer_weighted("noisy-neighbor", -5)
            .prefer_weighted("free-flag", 0)
            .prefer_weighted("fast", 4);

        // max_score ignores non-positive weights.
        assert_eq!(affinity.max_score(), 4);

        // Penalty applies to an otherwise-admissible worker.
        let penalized = worker(&["gpu", "noisy-neighbor"]);
        assert!(penalized.can_run(&affinity));
        assert_eq!(penalized.match_score(&affinity), -5);

        // Zero-weight label contributes nothing.
        let zero = worker(&["gpu", "free-flag"]);
        assert_eq!(zero.match_score(&affinity), 0);

        // Positive beats penalty.
        let mixed = worker(&["gpu", "fast", "noisy-neighbor"]);
        assert_eq!(mixed.match_score(&affinity), -1);
    }

    #[test]
    fn re_adding_preferred_overwrites_weight() {
        let affinity = TaskAffinity::new()
            .prefer_weighted("gpu", 1)
            .prefer_weighted("gpu", 7);
        assert_eq!(affinity.preferred().get("gpu"), Some(&7));
        assert_eq!(affinity.match_score(&worker(&["gpu"])), 7);
    }

    #[test]
    fn scoring_is_deterministic_and_ordering_stable() {
        // Build affinity from intentionally unsorted insertions.
        let affinity = TaskAffinity::new()
            .require("gpu")
            .prefer_weighted("z-label", 2)
            .prefer_weighted("a-label", 3)
            .prefer_weighted("m-label", 1);

        let w = worker(&["gpu", "z-label", "a-label", "m-label"]);

        // Recomputing many times yields identical results (no HashMap-iteration
        // nondeterminism leaks into the score).
        let first = affinity.match_score(&w);
        for _ in 0..256 {
            assert_eq!(affinity.match_score(&w), first);
        }
        assert_eq!(first, 6);

        // Ordering of candidate workers by score is stable and correct.
        let candidates = [
            ("w_none", worker(&["gpu"])),
            ("w_best", worker(&["gpu", "a-label", "z-label", "m-label"])),
            ("w_mid", worker(&["gpu", "a-label"])),
            ("w_bad", worker(&["cpu"])), // inadmissible
        ];
        let mut scored: Vec<(&str, i64)> = candidates
            .iter()
            .map(|(name, labels)| (*name, affinity.match_score(labels)))
            .collect();
        scored.sort_by_key(|entry| std::cmp::Reverse(entry.1));
        let order: Vec<&str> = scored.iter().map(|(name, _)| *name).collect();
        assert_eq!(order, vec!["w_best", "w_mid", "w_none", "w_bad"]);
        // The inadmissible worker is pinned to the bottom.
        assert_eq!(scored.last().map(|(_, s)| *s), Some(i64::MIN));
    }

    #[test]
    fn inadmissible_never_outranks_admissible() {
        let affinity = TaskAffinity::new()
            .require("gpu")
            .prefer_weighted("any", i64::MAX / 2);
        // Admissible worker with zero preferred matches.
        let admit = worker(&["gpu"]);
        // Inadmissible worker that *would* match the huge-weight preference.
        let reject = worker(&["any"]);
        assert!(affinity.match_score(&admit) > affinity.match_score(&reject));
    }

    #[test]
    fn worker_labels_helpers() {
        let mut w = WorkerLabels::new();
        assert!(w.is_empty());
        assert!(w.insert("gpu"));
        assert!(!w.insert("gpu")); // already present
        assert!(w.has("gpu"));
        assert_eq!(w.len(), 1);
        assert!(w.remove("gpu"));
        assert!(!w.remove("gpu"));
        assert!(w.is_empty());

        let a = worker(&["gpu", "ssd"]);
        let b = WorkerLabels::new().with_label("mem:high");
        let mut merged = a.clone();
        merged.merge(&b);
        assert!(merged.has("gpu") && merged.has("ssd") && merged.has("mem:high"));

        merged.clear();
        assert!(merged.is_empty());
    }

    #[test]
    fn validate_detects_required_and_anti_conflict() {
        let bad = TaskAffinity::new().require("gpu").anti("gpu");
        assert!(bad.validate().is_err());

        // required + preferred is allowed.
        let ok = TaskAffinity::new().require("gpu").prefer("gpu");
        assert!(ok.validate().is_ok());
        // The label being both required and preferred still scores.
        assert_eq!(ok.match_score(&worker(&["gpu"])), 1);
    }

    #[test]
    fn predicates() {
        assert!(!TaskAffinity::new().has_constraints());
        assert!(TaskAffinity::new().require("x").has_constraints());
        assert!(TaskAffinity::new().anti("x").has_constraints());
        assert!(!TaskAffinity::new().prefer("x").has_constraints());
        assert!(TaskAffinity::new().prefer("x").has_preferences());
        assert!(!TaskAffinity::new().require("x").has_preferences());
    }

    #[test]
    fn registry_no_affinity_admits() {
        let reg = AffinityRegistry::new();
        let w = worker(&["gpu"]);
        let decision = reg.decide("any_task", &w);
        assert_eq!(decision, AffinityDecision::NoAffinity);
        assert!(decision.is_admitted());
        assert!(!decision.is_deferred());
        assert_eq!(decision.score(), Some(0));
    }

    #[test]
    fn registry_empty_affinity_admits() {
        // An explicitly-registered but empty affinity is still a no-op.
        let reg = AffinityRegistry::new().with_task("t", TaskAffinity::new());
        let decision = reg.decide("t", &WorkerLabels::new());
        assert_eq!(decision, AffinityDecision::NoAffinity);
    }

    #[test]
    fn registry_admit_and_defer() {
        let reg = AffinityRegistry::new().with_task(
            "gpu_task",
            TaskAffinity::new()
                .require("gpu")
                .prefer("mem:high")
                .anti("spot"),
        );

        // Satisfied with a matched preference -> admit with score 1.
        let good = worker(&["gpu", "mem:high"]);
        assert_eq!(reg.decide("gpu_task", &good), AffinityDecision::Admit(1));

        // Satisfied, no preference -> admit with score 0.
        let plain = worker(&["gpu"]);
        assert_eq!(reg.decide("gpu_task", &plain), AffinityDecision::Admit(0));

        // Missing required -> defer.
        let no_gpu = worker(&["cpu"]);
        assert_eq!(reg.decide("gpu_task", &no_gpu), AffinityDecision::Defer);
        assert!(reg.decide("gpu_task", &no_gpu).is_deferred());

        // Anti-affinity present -> defer.
        let spot = worker(&["gpu", "spot"]);
        assert_eq!(reg.decide("gpu_task", &spot), AffinityDecision::Defer);

        // Unregistered task -> no affinity.
        assert_eq!(
            reg.decide("other_task", &no_gpu),
            AffinityDecision::NoAffinity
        );
    }

    #[test]
    fn registry_mutation_and_predicates() {
        let mut reg = AffinityRegistry::new();
        assert!(reg.is_empty());
        reg.set("a", TaskAffinity::new().require("gpu"));
        reg.set("b", TaskAffinity::new().prefer("ssd"));
        assert_eq!(reg.len(), 2);
        assert!(reg.has_constraints("a"));
        assert!(!reg.has_constraints("b")); // only a preference
        assert!(!reg.has_constraints("missing"));

        assert!(reg.get("a").is_some());
        let removed = reg.remove("a");
        assert!(removed.is_some());
        assert!(reg.get("a").is_none());

        reg.clear();
        assert!(reg.is_empty());
    }

    #[test]
    fn registry_validate_reports_task_name() {
        let reg = AffinityRegistry::new()
            .with_task("good", TaskAffinity::new().require("gpu"))
            .with_task("bad", TaskAffinity::new().require("x").anti("x"));
        let err = reg.validate().expect_err("conflict must be reported");
        assert!(err.contains("bad"), "error should name the task: {err}");
    }

    #[test]
    fn affinity_decision_helpers() {
        assert_eq!(AffinityDecision::Admit(5).score(), Some(5));
        assert_eq!(AffinityDecision::NoAffinity.score(), Some(0));
        assert_eq!(AffinityDecision::Defer.score(), None);
        assert!(AffinityDecision::Admit(0).is_admitted());
        assert!(AffinityDecision::NoAffinity.is_admitted());
        assert!(AffinityDecision::Defer.is_deferred());
    }

    #[test]
    fn display_is_deterministic() {
        let affinity = TaskAffinity::new()
            .require("b")
            .require("a")
            .anti("z")
            .prefer_weighted("p", 2);
        // Required labels rendered in sorted order regardless of insertion.
        let s = format!("{affinity}");
        assert!(s.contains("required:a,b"));
        assert!(s.contains("anti:z"));
        assert!(s.contains("p=2"));

        let labels = worker(&["b", "a"]);
        let ls = format!("{labels}");
        assert_eq!(ls, "WorkerLabels[a, b]");
    }
}
