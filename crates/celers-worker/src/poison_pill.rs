//! Poison-pill detection and quarantine
//!
//! A *poison pill* is a task that repeatedly fails or gets redelivered. Without
//! protection a broker will keep handing the same task back to a worker, which
//! burns CPU forever and can wedge an entire queue behind a single bad message.
//!
//! This module tracks per-task-id failure / redelivery counters and, once a
//! configurable threshold is crossed, *quarantines* the task: it is moved into a
//! holding set and marked for the dead-letter queue instead of being redelivered
//! again.
//!
//! # Design
//!
//! - Each task id has a `FailureRecord` holding a monotonically increasing
//!   failure count, redelivery count, the timestamp of the first and most recent
//!   failure, and the last error message.
//! - [`PoisonPillConfig`] controls the quarantine threshold, an optional
//!   *decay window* (a record older than the window resets to zero on the next
//!   observation, so a task that fails sporadically over a long period is not
//!   treated as poison), and an optional hard cap on the size of the quarantine
//!   set.
//! - A successful execution clears the record for that task id, so a task that
//!   eventually succeeds is forgiven.
//!
//! # Example
//!
//! ```
//! use celers_worker::poison_pill::{PoisonPillConfig, PoisonPillDetector};
//! use celers_core::TaskId;
//!
//! # async fn example() {
//! let detector = PoisonPillDetector::new(PoisonPillConfig::new().with_threshold(3));
//! let task_id = TaskId::new_v4();
//!
//! // Three failures trip the quarantine.
//! detector.record_failure(task_id, "boom").await;
//! detector.record_failure(task_id, "boom").await;
//! let verdict = detector.record_failure(task_id, "boom").await;
//! assert!(verdict.is_quarantined());
//! assert!(detector.is_poison(&task_id).await);
//! # }
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use celers_core::TaskId;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Configuration for the [`PoisonPillDetector`].
#[derive(Clone, Debug)]
pub struct PoisonPillConfig {
    /// Number of failures / redeliveries for a single task id that trips
    /// quarantine. Must be at least 1.
    pub threshold: usize,
    /// Optional decay window. If set, a failure record whose *last* failure is
    /// older than this window is considered stale and reset to zero before the
    /// new failure is counted. This prevents a task that fails very rarely over
    /// a long lifetime from eventually being flagged as poison.
    pub decay_window: Option<Duration>,
    /// Optional hard cap on the number of quarantined task ids retained. When
    /// the cap is exceeded the oldest quarantine entries are evicted. `None`
    /// means unbounded.
    pub max_quarantine: Option<usize>,
    /// Whether the detector is enabled. When disabled every observation is a
    /// no-op and nothing is ever quarantined.
    pub enabled: bool,
}

impl PoisonPillConfig {
    /// Create a new configuration with sensible defaults (threshold of 5,
    /// no decay window, an unbounded quarantine set, enabled).
    pub fn new() -> Self {
        Self {
            threshold: 5,
            decay_window: None,
            max_quarantine: None,
            enabled: true,
        }
    }

    /// Set the quarantine threshold. Values below 1 are clamped to 1.
    pub fn with_threshold(mut self, threshold: usize) -> Self {
        self.threshold = threshold.max(1);
        self
    }

    /// Set the decay / reset window.
    pub fn with_decay_window(mut self, window: Duration) -> Self {
        self.decay_window = Some(window);
        self
    }

    /// Remove the decay window (failures accumulate forever until success).
    pub fn without_decay_window(mut self) -> Self {
        self.decay_window = None;
        self
    }

    /// Cap the number of retained quarantine entries.
    pub fn with_max_quarantine(mut self, max: usize) -> Self {
        self.max_quarantine = Some(max);
        self
    }

    /// Enable or disable the detector.
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), String> {
        if self.threshold == 0 {
            return Err("Poison-pill threshold must be at least 1".to_string());
        }
        if let Some(window) = self.decay_window {
            if window.is_zero() {
                return Err("Poison-pill decay window must be non-zero".to_string());
            }
        }
        if let Some(max) = self.max_quarantine {
            if max == 0 {
                return Err("Poison-pill max_quarantine must be at least 1".to_string());
            }
        }
        Ok(())
    }
}

impl Default for PoisonPillConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// How a strike against a task id was classified.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StrikeKind {
    /// The task executed and raised an error.
    Failure,
    /// The task was redelivered by the broker without a recorded result
    /// (typically because a previous attempt crashed or was not acked).
    Redelivery,
}

impl std::fmt::Display for StrikeKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StrikeKind::Failure => write!(f, "failure"),
            StrikeKind::Redelivery => write!(f, "redelivery"),
        }
    }
}

/// Per-task failure / redelivery accounting.
#[derive(Debug, Clone)]
struct FailureRecord {
    /// Number of recorded execution failures.
    failures: usize,
    /// Number of recorded redeliveries.
    redeliveries: usize,
    /// When the first strike was recorded (after the most recent reset).
    first_strike: Instant,
    /// When the most recent strike was recorded.
    last_strike: Instant,
    /// The most recent error message, if any.
    last_error: Option<String>,
}

impl FailureRecord {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            failures: 0,
            redeliveries: 0,
            first_strike: now,
            last_strike: now,
            last_error: None,
        }
    }

    /// Total strikes (failures + redeliveries).
    fn total(&self) -> usize {
        self.failures + self.redeliveries
    }

    /// Reset the counters while keeping the record alive (used after the decay
    /// window has elapsed).
    fn reset(&mut self) {
        let now = Instant::now();
        self.failures = 0;
        self.redeliveries = 0;
        self.first_strike = now;
        self.last_strike = now;
        self.last_error = None;
    }
}

/// A task that has been moved into quarantine.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuarantinedTask {
    /// The offending task id.
    pub task_id: TaskId,
    /// Number of execution failures observed before quarantine.
    pub failures: usize,
    /// Number of redeliveries observed before quarantine.
    pub redeliveries: usize,
    /// The most recent error message, if any.
    pub last_error: Option<String>,
    /// Unix timestamp (seconds) when the task was quarantined.
    pub quarantined_at: u64,
}

impl QuarantinedTask {
    /// Total strikes accumulated before quarantine.
    pub fn total_strikes(&self) -> usize {
        self.failures + self.redeliveries
    }
}

/// The result of recording a strike or success against a task id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PoisonPillVerdict {
    /// The task is still considered healthy; `strikes` records the current
    /// accumulated strike count.
    Healthy {
        /// Current accumulated strike count after this observation.
        strikes: usize,
    },
    /// This observation tripped the threshold and the task was moved into
    /// quarantine for the first time.
    Quarantined {
        /// Total strikes at the moment of quarantine.
        strikes: usize,
    },
    /// The task was already in quarantine before this observation.
    AlreadyQuarantined,
    /// The detector is disabled, or the observation did not apply (e.g. a
    /// success for an unknown task id).
    Ignored,
}

impl PoisonPillVerdict {
    /// Whether this verdict indicates the task is (now or already) quarantined.
    pub fn is_quarantined(&self) -> bool {
        matches!(
            self,
            PoisonPillVerdict::Quarantined { .. } | PoisonPillVerdict::AlreadyQuarantined
        )
    }

    /// Whether this observation *newly* tripped the quarantine.
    pub fn just_quarantined(&self) -> bool {
        matches!(self, PoisonPillVerdict::Quarantined { .. })
    }

    /// Whether the task is still considered healthy.
    pub fn is_healthy(&self) -> bool {
        matches!(self, PoisonPillVerdict::Healthy { .. })
    }
}

/// Aggregate statistics for the detector.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PoisonPillStats {
    /// Number of task ids currently being tracked (not yet quarantined).
    pub tracked_tasks: usize,
    /// Number of task ids currently quarantined.
    pub quarantined_tasks: usize,
    /// Total quarantine events since creation (cumulative).
    pub total_quarantined: usize,
    /// Total failures recorded since creation (cumulative).
    pub total_failures: usize,
    /// Total redeliveries recorded since creation (cumulative).
    pub total_redeliveries: usize,
    /// Total successes recorded since creation (cumulative).
    pub total_successes: usize,
}

/// Internal mutable state guarded by a single lock.
#[derive(Default)]
struct DetectorState {
    /// Active per-task accounting for not-yet-quarantined task ids.
    records: HashMap<TaskId, FailureRecord>,
    /// Quarantined tasks keyed by id.
    quarantine: HashMap<TaskId, QuarantinedTask>,
    /// Insertion order of quarantine entries (for bounded eviction).
    quarantine_order: Vec<TaskId>,
    /// Cumulative counters.
    total_quarantined: usize,
    total_failures: usize,
    total_redeliveries: usize,
    total_successes: usize,
}

/// Detects poison-pill tasks and holds the quarantine set.
///
/// The detector is cheaply cloneable; clones share the same underlying state.
#[derive(Clone)]
pub struct PoisonPillDetector {
    config: PoisonPillConfig,
    state: Arc<RwLock<DetectorState>>,
}

impl PoisonPillDetector {
    /// Create a new detector with the given configuration.
    pub fn new(config: PoisonPillConfig) -> Self {
        Self {
            config,
            state: Arc::new(RwLock::new(DetectorState::default())),
        }
    }

    /// Create a detector with default configuration.
    pub fn with_default_config() -> Self {
        Self::new(PoisonPillConfig::default())
    }

    /// Borrow the configuration.
    pub fn config(&self) -> &PoisonPillConfig {
        &self.config
    }

    /// Record a task execution failure and return the resulting verdict.
    pub async fn record_failure(
        &self,
        task_id: TaskId,
        error: impl Into<String>,
    ) -> PoisonPillVerdict {
        self.record_strike(task_id, StrikeKind::Failure, Some(error.into()))
            .await
    }

    /// Record a broker redelivery (no result) and return the resulting verdict.
    pub async fn record_redelivery(&self, task_id: TaskId) -> PoisonPillVerdict {
        self.record_strike(task_id, StrikeKind::Redelivery, None)
            .await
    }

    /// Record a generic strike with an explicit kind. Useful when the caller
    /// already knows whether the strike is a failure or a redelivery.
    pub async fn record_strike(
        &self,
        task_id: TaskId,
        kind: StrikeKind,
        error: Option<String>,
    ) -> PoisonPillVerdict {
        if !self.config.enabled {
            return PoisonPillVerdict::Ignored;
        }

        let mut state = self.state.write().await;

        // Already quarantined: nothing more to count.
        if state.quarantine.contains_key(&task_id) {
            match kind {
                StrikeKind::Failure => state.total_failures += 1,
                StrikeKind::Redelivery => state.total_redeliveries += 1,
            }
            return PoisonPillVerdict::AlreadyQuarantined;
        }

        // Fetch-or-create the record, applying decay first.
        let decay = self.config.decay_window;
        let total = {
            let record = state
                .records
                .entry(task_id)
                .or_insert_with(FailureRecord::new);
            if let Some(window) = decay {
                if record.last_strike.elapsed() > window {
                    debug!(%task_id, "Poison-pill record decayed; resetting strikes");
                    record.reset();
                }
            }

            record.last_strike = Instant::now();
            match kind {
                StrikeKind::Failure => record.failures += 1,
                StrikeKind::Redelivery => record.redeliveries += 1,
            }
            if let Some(err) = error {
                record.last_error = Some(err);
            }
            record.total()
        };

        // Update cumulative counters after the per-record borrow ends.
        match kind {
            StrikeKind::Failure => state.total_failures += 1,
            StrikeKind::Redelivery => state.total_redeliveries += 1,
        }

        if total >= self.config.threshold {
            // Promote to quarantine.
            let (failures, redeliveries, last_error) = state
                .records
                .remove(&task_id)
                .map(|r| (r.failures, r.redeliveries, r.last_error))
                .unwrap_or((0, 0, None));
            self.quarantine_locked(&mut state, task_id, failures, redeliveries, last_error);
            warn!(
                %task_id,
                strikes = total,
                threshold = self.config.threshold,
                last_kind = %kind,
                "Task quarantined as poison pill"
            );
            PoisonPillVerdict::Quarantined { strikes: total }
        } else {
            debug!(%task_id, strikes = total, "Recorded poison-pill strike");
            PoisonPillVerdict::Healthy { strikes: total }
        }
    }

    /// Insert a quarantine entry while holding the state lock, applying the
    /// optional size cap.
    fn quarantine_locked(
        &self,
        state: &mut DetectorState,
        task_id: TaskId,
        failures: usize,
        redeliveries: usize,
        last_error: Option<String>,
    ) {
        let quarantined_at = unix_now_secs();
        let entry = QuarantinedTask {
            task_id,
            failures,
            redeliveries,
            last_error,
            quarantined_at,
        };
        if state.quarantine.insert(task_id, entry).is_none() {
            state.quarantine_order.push(task_id);
            state.total_quarantined += 1;
        }

        if let Some(max) = self.config.max_quarantine {
            while state.quarantine.len() > max {
                if state.quarantine_order.is_empty() {
                    break;
                }
                let oldest = state.quarantine_order.remove(0);
                state.quarantine.remove(&oldest);
                debug!(task_id = %oldest, "Evicted oldest quarantine entry (cap reached)");
            }
        }
    }

    /// Record a successful execution, clearing any accumulated strikes for the
    /// task id. A success does *not* release a task that is already quarantined;
    /// use [`PoisonPillDetector::release`] for that.
    pub async fn record_success(&self, task_id: &TaskId) -> PoisonPillVerdict {
        if !self.config.enabled {
            return PoisonPillVerdict::Ignored;
        }
        let mut state = self.state.write().await;
        state.total_successes += 1;
        if state.quarantine.contains_key(task_id) {
            return PoisonPillVerdict::AlreadyQuarantined;
        }
        if state.records.remove(task_id).is_some() {
            debug!(%task_id, "Cleared poison-pill strikes after success");
        }
        PoisonPillVerdict::Healthy { strikes: 0 }
    }

    /// Whether the given task id is currently quarantined.
    pub async fn is_poison(&self, task_id: &TaskId) -> bool {
        self.state.read().await.quarantine.contains_key(task_id)
    }

    /// Current accumulated strike count for a task id (0 if untracked or
    /// quarantined). Quarantined tasks report 0 here because they are no longer
    /// in the active tracking map; use [`PoisonPillDetector::quarantined`] to
    /// inspect them.
    pub async fn strike_count(&self, task_id: &TaskId) -> usize {
        self.state
            .read()
            .await
            .records
            .get(task_id)
            .map(FailureRecord::total)
            .unwrap_or(0)
    }

    /// Snapshot of all quarantined tasks, ordered oldest-first.
    pub async fn quarantine_list(&self) -> Vec<QuarantinedTask> {
        let state = self.state.read().await;
        state
            .quarantine_order
            .iter()
            .filter_map(|id| state.quarantine.get(id).cloned())
            .collect()
    }

    /// Fetch a single quarantine entry by id.
    pub async fn quarantined(&self, task_id: &TaskId) -> Option<QuarantinedTask> {
        self.state.read().await.quarantine.get(task_id).cloned()
    }

    /// Number of currently quarantined task ids.
    pub async fn quarantine_len(&self) -> usize {
        self.state.read().await.quarantine.len()
    }

    /// Release a task from quarantine (e.g. after manual remediation), clearing
    /// all accounting for it. Returns the removed entry if it was quarantined.
    pub async fn release(&self, task_id: &TaskId) -> Option<QuarantinedTask> {
        let mut state = self.state.write().await;
        state.records.remove(task_id);
        let removed = state.quarantine.remove(task_id);
        if removed.is_some() {
            state.quarantine_order.retain(|id| id != task_id);
            info!(%task_id, "Released task from quarantine");
        }
        removed
    }

    /// Drain (remove and return) all quarantined tasks. Useful for flushing the
    /// quarantine set into a dead-letter queue.
    pub async fn drain_quarantine(&self) -> Vec<QuarantinedTask> {
        let mut state = self.state.write().await;
        let order = std::mem::take(&mut state.quarantine_order);
        let mut drained = Vec::with_capacity(order.len());
        for id in order {
            if let Some(entry) = state.quarantine.remove(&id) {
                drained.push(entry);
            }
        }
        if !drained.is_empty() {
            info!(count = drained.len(), "Drained quarantine set");
        }
        drained
    }

    /// Remove tracking records (not quarantine entries) that are older than the
    /// decay window. No-op if no decay window is configured. Returns the number
    /// of stale records pruned. This is an optional housekeeping helper for
    /// long-running detectors so the tracking map does not grow unbounded with
    /// one-off failures that never reach the threshold.
    pub async fn prune_stale(&self) -> usize {
        let window = match self.config.decay_window {
            Some(w) => w,
            None => return 0,
        };
        let mut state = self.state.write().await;
        let before = state.records.len();
        state
            .records
            .retain(|_, record| record.last_strike.elapsed() <= window);
        before - state.records.len()
    }

    /// Snapshot of aggregate statistics.
    pub async fn stats(&self) -> PoisonPillStats {
        let state = self.state.read().await;
        PoisonPillStats {
            tracked_tasks: state.records.len(),
            quarantined_tasks: state.quarantine.len(),
            total_quarantined: state.total_quarantined,
            total_failures: state.total_failures,
            total_redeliveries: state.total_redeliveries,
            total_successes: state.total_successes,
        }
    }

    /// Clear all state (tracking records, quarantine set, and cumulative
    /// counters). Primarily useful in tests.
    pub async fn clear(&self) {
        let mut state = self.state.write().await;
        *state = DetectorState::default();
    }
}

/// Current Unix time in seconds, saturating to 0 on clock errors.
fn unix_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn detector(threshold: usize) -> PoisonPillDetector {
        PoisonPillDetector::new(PoisonPillConfig::new().with_threshold(threshold))
    }

    #[test]
    fn test_config_builder_and_validate() {
        let cfg = PoisonPillConfig::new()
            .with_threshold(0) // clamped to 1
            .with_decay_window(Duration::from_secs(60))
            .with_max_quarantine(10)
            .enabled(true);
        assert_eq!(cfg.threshold, 1);
        assert_eq!(cfg.decay_window, Some(Duration::from_secs(60)));
        assert_eq!(cfg.max_quarantine, Some(10));
        assert!(cfg.validate().is_ok());
    }

    #[test]
    fn test_config_validate_rejects_zero_window() {
        let mut cfg = PoisonPillConfig::new();
        cfg.decay_window = Some(Duration::ZERO);
        assert!(cfg.validate().is_err());

        cfg.decay_window = None;
        cfg.max_quarantine = Some(0);
        assert!(cfg.validate().is_err());

        cfg.max_quarantine = None;
        cfg.threshold = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn test_verdict_helpers() {
        assert!(PoisonPillVerdict::Quarantined { strikes: 3 }.is_quarantined());
        assert!(PoisonPillVerdict::Quarantined { strikes: 3 }.just_quarantined());
        assert!(PoisonPillVerdict::AlreadyQuarantined.is_quarantined());
        assert!(!PoisonPillVerdict::AlreadyQuarantined.just_quarantined());
        assert!(PoisonPillVerdict::Healthy { strikes: 1 }.is_healthy());
        assert!(!PoisonPillVerdict::Ignored.is_quarantined());
    }

    #[tokio::test]
    async fn test_threshold_trips_quarantine() {
        let d = detector(3);
        let id = TaskId::new_v4();

        let v1 = d.record_failure(id, "boom").await;
        assert_eq!(v1, PoisonPillVerdict::Healthy { strikes: 1 });
        assert!(!d.is_poison(&id).await);

        let v2 = d.record_failure(id, "boom").await;
        assert_eq!(v2, PoisonPillVerdict::Healthy { strikes: 2 });

        let v3 = d.record_failure(id, "boom again").await;
        assert_eq!(v3, PoisonPillVerdict::Quarantined { strikes: 3 });
        assert!(v3.just_quarantined());
        assert!(d.is_poison(&id).await);

        // Subsequent strikes report already-quarantined.
        let v4 = d.record_failure(id, "still bad").await;
        assert_eq!(v4, PoisonPillVerdict::AlreadyQuarantined);

        let entry = d.quarantined(&id).await.expect("entry present");
        assert_eq!(entry.failures, 3);
        assert_eq!(entry.redeliveries, 0);
        assert_eq!(entry.total_strikes(), 3);
        assert_eq!(entry.last_error.as_deref(), Some("boom again"));
    }

    #[tokio::test]
    async fn test_redeliveries_count_toward_threshold() {
        let d = detector(2);
        let id = TaskId::new_v4();

        let v1 = d.record_redelivery(id).await;
        assert_eq!(v1, PoisonPillVerdict::Healthy { strikes: 1 });
        let v2 = d.record_redelivery(id).await;
        assert_eq!(v2, PoisonPillVerdict::Quarantined { strikes: 2 });
        assert!(d.is_poison(&id).await);

        let entry = d.quarantined(&id).await.expect("entry present");
        assert_eq!(entry.redeliveries, 2);
        assert_eq!(entry.failures, 0);
    }

    #[tokio::test]
    async fn test_mixed_strikes_count_toward_threshold() {
        let d = detector(3);
        let id = TaskId::new_v4();

        assert!(d.record_failure(id, "e1").await.is_healthy());
        assert!(d.record_redelivery(id).await.is_healthy());
        assert_eq!(d.strike_count(&id).await, 2);
        let v = d.record_failure(id, "e2").await;
        assert!(v.just_quarantined());

        let entry = d.quarantined(&id).await.expect("entry present");
        assert_eq!(entry.failures, 2);
        assert_eq!(entry.redeliveries, 1);
    }

    #[tokio::test]
    async fn test_success_resets_strikes() {
        let d = detector(3);
        let id = TaskId::new_v4();

        d.record_failure(id, "boom").await;
        d.record_failure(id, "boom").await;
        assert_eq!(d.strike_count(&id).await, 2);

        d.record_success(&id).await;
        assert_eq!(d.strike_count(&id).await, 0);
        assert!(!d.is_poison(&id).await);

        // After a reset it takes the full threshold again to quarantine.
        d.record_failure(id, "boom").await;
        d.record_failure(id, "boom").await;
        assert!(!d.is_poison(&id).await);
        let v = d.record_failure(id, "boom").await;
        assert!(v.just_quarantined());
    }

    #[tokio::test]
    async fn test_decay_window_resets_stale_records() {
        let d = PoisonPillDetector::new(
            PoisonPillConfig::new()
                .with_threshold(3)
                .with_decay_window(Duration::from_millis(50)),
        );
        let id = TaskId::new_v4();

        d.record_failure(id, "boom").await;
        d.record_failure(id, "boom").await;
        assert_eq!(d.strike_count(&id).await, 2);

        // Let the record go stale.
        tokio::time::sleep(Duration::from_millis(80)).await;

        // The next failure should reset to a single strike, not three.
        let v = d.record_failure(id, "boom").await;
        assert_eq!(v, PoisonPillVerdict::Healthy { strikes: 1 });
        assert!(!d.is_poison(&id).await);
    }

    #[tokio::test]
    async fn test_no_decay_accumulates() {
        let d = detector(3);
        let id = TaskId::new_v4();
        d.record_failure(id, "boom").await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        d.record_failure(id, "boom").await;
        // No decay window: strikes persist.
        assert_eq!(d.strike_count(&id).await, 2);
    }

    #[tokio::test]
    async fn test_prune_stale() {
        let d = PoisonPillDetector::new(
            PoisonPillConfig::new()
                .with_threshold(5)
                .with_decay_window(Duration::from_millis(40)),
        );
        let id1 = TaskId::new_v4();
        let id2 = TaskId::new_v4();
        d.record_failure(id1, "boom").await;
        tokio::time::sleep(Duration::from_millis(60)).await;
        d.record_failure(id2, "boom").await;

        let pruned = d.prune_stale().await;
        assert_eq!(pruned, 1);
        assert_eq!(d.strike_count(&id1).await, 0);
        assert_eq!(d.strike_count(&id2).await, 1);
    }

    #[tokio::test]
    async fn test_prune_noop_without_window() {
        let d = detector(5);
        let id = TaskId::new_v4();
        d.record_failure(id, "boom").await;
        assert_eq!(d.prune_stale().await, 0);
        assert_eq!(d.strike_count(&id).await, 1);
    }

    #[tokio::test]
    async fn test_quarantine_list_ordering() {
        let d = detector(1);
        let id1 = TaskId::new_v4();
        let id2 = TaskId::new_v4();
        let id3 = TaskId::new_v4();
        d.record_failure(id1, "a").await;
        d.record_failure(id2, "b").await;
        d.record_failure(id3, "c").await;

        let list = d.quarantine_list().await;
        assert_eq!(list.len(), 3);
        assert_eq!(list[0].task_id, id1);
        assert_eq!(list[1].task_id, id2);
        assert_eq!(list[2].task_id, id3);
        assert_eq!(d.quarantine_len().await, 3);
    }

    #[tokio::test]
    async fn test_max_quarantine_evicts_oldest() {
        let d = PoisonPillDetector::new(
            PoisonPillConfig::new()
                .with_threshold(1)
                .with_max_quarantine(2),
        );
        let id1 = TaskId::new_v4();
        let id2 = TaskId::new_v4();
        let id3 = TaskId::new_v4();
        d.record_failure(id1, "a").await;
        d.record_failure(id2, "b").await;
        d.record_failure(id3, "c").await;

        assert_eq!(d.quarantine_len().await, 2);
        // Oldest (id1) evicted.
        assert!(!d.is_poison(&id1).await);
        assert!(d.is_poison(&id2).await);
        assert!(d.is_poison(&id3).await);
    }

    #[tokio::test]
    async fn test_release_clears_state() {
        let d = detector(1);
        let id = TaskId::new_v4();
        d.record_failure(id, "boom").await;
        assert!(d.is_poison(&id).await);

        let released = d.release(&id).await;
        assert!(released.is_some());
        assert!(!d.is_poison(&id).await);
        assert_eq!(d.quarantine_len().await, 0);

        // Releasing again returns None.
        assert!(d.release(&id).await.is_none());
    }

    #[tokio::test]
    async fn test_drain_quarantine() {
        let d = detector(1);
        let id1 = TaskId::new_v4();
        let id2 = TaskId::new_v4();
        d.record_failure(id1, "a").await;
        d.record_failure(id2, "b").await;

        let drained = d.drain_quarantine().await;
        assert_eq!(drained.len(), 2);
        assert_eq!(drained[0].task_id, id1);
        assert_eq!(d.quarantine_len().await, 0);
        assert!(d.quarantine_list().await.is_empty());
    }

    #[tokio::test]
    async fn test_disabled_detector_is_noop() {
        let d = PoisonPillDetector::new(PoisonPillConfig::new().with_threshold(1).enabled(false));
        let id = TaskId::new_v4();
        assert_eq!(
            d.record_failure(id, "boom").await,
            PoisonPillVerdict::Ignored
        );
        assert!(!d.is_poison(&id).await);
        assert_eq!(d.record_success(&id).await, PoisonPillVerdict::Ignored);
    }

    #[tokio::test]
    async fn test_stats() {
        let d = detector(2);
        let id1 = TaskId::new_v4();
        let id2 = TaskId::new_v4();

        d.record_failure(id1, "boom").await; // 1 failure, tracked
        d.record_redelivery(id2).await; // 1 redelivery, tracked
        d.record_failure(id2, "boom").await; // id2 -> quarantined (2 strikes)
        d.record_success(&id1).await; // clears id1 tracking

        let stats = d.stats().await;
        assert_eq!(stats.total_failures, 2);
        assert_eq!(stats.total_redeliveries, 1);
        assert_eq!(stats.total_successes, 1);
        assert_eq!(stats.total_quarantined, 1);
        assert_eq!(stats.quarantined_tasks, 1);
        assert_eq!(stats.tracked_tasks, 0); // id1 cleared, id2 quarantined
    }

    #[tokio::test]
    async fn test_success_on_quarantined_does_not_release() {
        let d = detector(1);
        let id = TaskId::new_v4();
        d.record_failure(id, "boom").await;
        assert!(d.is_poison(&id).await);

        let v = d.record_success(&id).await;
        assert_eq!(v, PoisonPillVerdict::AlreadyQuarantined);
        assert!(d.is_poison(&id).await);
    }

    #[tokio::test]
    async fn test_clear() {
        let d = detector(1);
        let id = TaskId::new_v4();
        d.record_failure(id, "boom").await;
        assert!(d.is_poison(&id).await);

        d.clear().await;
        assert!(!d.is_poison(&id).await);
        let stats = d.stats().await;
        assert_eq!(stats.total_quarantined, 0);
        assert_eq!(stats.quarantined_tasks, 0);
    }

    #[tokio::test]
    async fn test_clone_shares_state() {
        let d = detector(2);
        let d2 = d.clone();
        let id = TaskId::new_v4();
        d.record_failure(id, "boom").await;
        // Strike recorded via the clone is visible on the original.
        assert_eq!(d2.strike_count(&id).await, 1);
        d2.record_failure(id, "boom").await;
        assert!(d.is_poison(&id).await);
    }
}
