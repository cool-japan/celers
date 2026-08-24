#![allow(clippy::cast_precision_loss, clippy::cast_possible_truncation)]
//! Event snapshots and event-based alerting.
//!
//! This module builds on the real-time event types in [`crate::event`] to
//! provide two cooperating pieces:
//!
//! 1. **Snapshots** ([`EventSnapshot`], [`EventSnapshotBuilder`]) — a periodic
//!    aggregate of cluster state: counts of tasks by lifecycle state, queue
//!    depth, worker count and a timestamp. Snapshots are cheap to produce, easy
//!    to serialize, and are themselves a useful input to alerting.
//!
//! 2. **A rule engine** ([`AlertRule`], [`AlertRuleKind`], [`AlertEvaluator`])
//!    that consumes a stream of [`Event`]s and/or [`EventSnapshot`]s and emits
//!    [`RuleAlert`]s when a rule trips. Rules support *hysteresis* (separate
//!    trip / clear thresholds) and a *cooldown* so a flapping condition does not
//!    produce a storm of alerts. The engine also emits a recovery alert when a
//!    previously-firing rule clears.
//!
//! It deliberately reuses the existing [`crate::event::AlertSeverity`] type
//! rather than introducing a parallel severity enum.
//!
//! # Example
//!
//! ```rust
//! use celers_core::alerting::{AlertEvaluator, AlertRule, EventSnapshotBuilder};
//! use celers_core::event::{AlertSeverity, Event, TaskEventBuilder};
//! use uuid::Uuid;
//!
//! // Fail if more than 50% of tasks fail over the recent window, recover under 20%.
//! let rule = AlertRule::failure_rate("high-failures", AlertSeverity::Error, 0.5)
//!     .with_clear_threshold(0.2)
//!     .with_min_samples(4);
//!
//! let mut eval = AlertEvaluator::new().with_rule(rule);
//!
//! // Feed some failing task events; once the window has enough samples and the
//! // failure rate crosses 0.5, an alert is produced.
//! let mut last = Vec::new();
//! for _ in 0..4 {
//!     let ev = TaskEventBuilder::new(Uuid::new_v4(), "t").hostname("w1").failed("Boom");
//!     last = eval.observe_event(&ev);
//! }
//! assert!(last.iter().any(|a| a.rule_name == "high-failures"));
//! ```

use crate::event::{AlertSeverity, Event, TaskEvent, WorkerEvent};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::collections::VecDeque;
use std::time::Duration;

/// Periodic aggregate snapshot of cluster state.
///
/// A snapshot is a point-in-time summary suitable for dashboards, persistence,
/// and as an input to [`AlertEvaluator`]. All counts are plain integers so the
/// type serializes compactly.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EventSnapshot {
    /// When the snapshot was taken.
    pub timestamp: DateTime<Utc>,
    /// Number of tasks currently pending (sent but not started).
    pub pending: u64,
    /// Number of tasks currently active/running.
    pub active: u64,
    /// Number of tasks that have succeeded (in the aggregation period).
    pub succeeded: u64,
    /// Number of tasks that have failed (in the aggregation period).
    pub failed: u64,
    /// Number of tasks retried (in the aggregation period).
    pub retried: u64,
    /// Number of tasks revoked (in the aggregation period).
    pub revoked: u64,
    /// Total depth across all queues.
    pub queue_depth: u64,
    /// Number of workers considered online.
    pub worker_count: u64,
    /// Optional per-queue depth breakdown.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub queue_depths: HashMap<String, u64>,
}

impl EventSnapshot {
    /// Total number of terminal task outcomes recorded (succeeded + failed).
    #[inline]
    #[must_use]
    pub const fn completed(&self) -> u64 {
        self.succeeded + self.failed
    }

    /// Failure rate over completed tasks (`failed / (succeeded + failed)`).
    ///
    /// Returns `0.0` when no tasks have completed.
    #[inline]
    #[must_use]
    pub fn failure_rate(&self) -> f64 {
        let total = self.completed();
        if total == 0 {
            0.0
        } else {
            self.failed as f64 / total as f64
        }
    }
}

/// Builder for [`EventSnapshot`].
///
/// Either set fields explicitly, or fold a batch of [`Event`]s into the running
/// counts via [`EventSnapshotBuilder::observe`] / [`EventSnapshotBuilder::observe_all`].
#[derive(Debug, Clone, Default)]
pub struct EventSnapshotBuilder {
    timestamp: Option<DateTime<Utc>>,
    pending: u64,
    active: u64,
    succeeded: u64,
    failed: u64,
    retried: u64,
    revoked: u64,
    /// Explicit aggregate depth set via [`EventSnapshotBuilder::total_queue_depth`].
    /// When absent, the aggregate is the sum of `queue_depths` at build time.
    total_queue_depth: Option<u64>,
    queue_depths: HashMap<String, u64>,
    workers: std::collections::HashSet<String>,
    worker_count_override: Option<u64>,
}

impl EventSnapshotBuilder {
    /// Create a new, empty snapshot builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the snapshot timestamp (defaults to [`Utc::now`] at build time).
    #[must_use]
    pub fn timestamp(mut self, ts: DateTime<Utc>) -> Self {
        self.timestamp = Some(ts);
        self
    }

    /// Set the pending count.
    #[must_use]
    pub fn pending(mut self, n: u64) -> Self {
        self.pending = n;
        self
    }

    /// Set the active count.
    #[must_use]
    pub fn active(mut self, n: u64) -> Self {
        self.active = n;
        self
    }

    /// Set the succeeded count.
    #[must_use]
    pub fn succeeded(mut self, n: u64) -> Self {
        self.succeeded = n;
        self
    }

    /// Set the failed count.
    #[must_use]
    pub fn failed(mut self, n: u64) -> Self {
        self.failed = n;
        self
    }

    /// Set the retried count.
    #[must_use]
    pub fn retried(mut self, n: u64) -> Self {
        self.retried = n;
        self
    }

    /// Set the revoked count.
    #[must_use]
    pub fn revoked(mut self, n: u64) -> Self {
        self.revoked = n;
        self
    }

    /// Set the worker count explicitly (overrides workers inferred from events).
    #[must_use]
    pub fn worker_count(mut self, n: u64) -> Self {
        self.worker_count_override = Some(n);
        self
    }

    /// Set the depth for a specific queue.
    ///
    /// The aggregate depth is derived from the per-queue map at build time (see
    /// [`EventSnapshotBuilder::build`]), so calls may be made in any order and
    /// overwriting a queue's depth can never underflow the total.
    #[must_use]
    pub fn queue_depth(mut self, queue: impl Into<String>, depth: u64) -> Self {
        self.queue_depths.insert(queue.into(), depth);
        self
    }

    /// Set the aggregate queue depth directly (when per-queue detail is absent).
    ///
    /// This overrides the sum of the per-queue depths.
    #[must_use]
    pub fn total_queue_depth(mut self, depth: u64) -> Self {
        self.total_queue_depth = Some(depth);
        self
    }

    /// Fold a single event into the running aggregate.
    ///
    /// Task lifecycle events bump the corresponding counters; worker
    /// online/heartbeat events register a worker as present, and worker offline
    /// removes it.
    pub fn observe(&mut self, event: &Event) -> &mut Self {
        match event {
            Event::Task(task) => match task {
                TaskEvent::Sent { .. } => self.pending += 1,
                TaskEvent::Started { .. } => self.active += 1,
                TaskEvent::Succeeded { .. } => self.succeeded += 1,
                TaskEvent::Failed { .. } => self.failed += 1,
                TaskEvent::Retried { .. } => self.retried += 1,
                TaskEvent::Revoked { .. } => self.revoked += 1,
                TaskEvent::Received { .. } | TaskEvent::Rejected { .. } => {}
            },
            Event::Worker(worker) => match worker {
                WorkerEvent::Online { hostname, .. } | WorkerEvent::Heartbeat { hostname, .. } => {
                    self.workers.insert(hostname.clone());
                }
                WorkerEvent::Offline { hostname, .. } => {
                    self.workers.remove(hostname);
                }
            },
        }
        self
    }

    /// Fold a batch of events into the running aggregate.
    pub fn observe_all<'a, I>(&mut self, events: I) -> &mut Self
    where
        I: IntoIterator<Item = &'a Event>,
    {
        for event in events {
            self.observe(event);
        }
        self
    }

    /// Finalize the snapshot.
    #[must_use]
    pub fn build(self) -> EventSnapshot {
        let worker_count = self
            .worker_count_override
            .unwrap_or(self.workers.len() as u64);
        let queue_depth = self.total_queue_depth.unwrap_or_else(|| {
            self.queue_depths
                .values()
                .fold(0u64, |acc, depth| acc.saturating_add(*depth))
        });
        EventSnapshot {
            timestamp: self.timestamp.unwrap_or_else(Utc::now),
            pending: self.pending,
            active: self.active,
            succeeded: self.succeeded,
            failed: self.failed,
            retried: self.retried,
            revoked: self.revoked,
            queue_depth,
            worker_count,
            queue_depths: self.queue_depths,
        }
    }
}

/// The condition a rule monitors.
#[derive(Debug, Clone)]
pub enum AlertRuleKind {
    /// Trip when the failure rate over the recent event window meets/exceeds
    /// `trip_threshold` (0.0..=1.0). Requires at least `min_samples` completed
    /// tasks in the window before it can fire.
    FailureRate {
        /// Rate (0.0..=1.0) at or above which the rule trips.
        trip_threshold: f64,
        /// Rate strictly below which the rule clears (hysteresis).
        clear_threshold: f64,
        /// Sliding window over which completions are counted.
        window: Duration,
        /// Minimum completed samples in the window before the rule may fire.
        min_samples: u64,
    },
    /// Trip when the observed queue depth meets/exceeds `trip_depth`.
    QueueDepth {
        /// Depth at or above which the rule trips.
        trip_depth: u64,
        /// Depth at or below which the rule clears (hysteresis).
        clear_depth: u64,
        /// Optional specific queue to watch; `None` uses total depth.
        queue: Option<String>,
    },
    /// Trip when no worker heartbeat (or snapshot with workers) has been seen
    /// within `timeout`.
    NoHeartbeat {
        /// Maximum allowed silence before tripping.
        timeout: Duration,
    },
}

/// A configured alerting rule.
///
/// Each rule carries a unique `name`, a [`AlertSeverity`], the monitored
/// [`AlertRuleKind`], and a `cooldown` that suppresses repeat alerts while the
/// rule remains continuously tripped.
#[derive(Debug, Clone)]
pub struct AlertRule {
    /// Unique rule identifier (also used as the alert's `rule_name`).
    pub name: String,
    /// Severity attached to alerts produced by this rule.
    pub severity: AlertSeverity,
    /// The condition being monitored.
    pub kind: AlertRuleKind,
    /// Minimum time between successive *firing* alerts for this rule.
    pub cooldown: Duration,
}

impl AlertRule {
    /// Build a failure-rate rule. By default the clear threshold equals the trip
    /// threshold (no hysteresis), the window is 60s, `min_samples` is 1 and the
    /// cooldown is 60s; adjust with the builder methods.
    #[must_use]
    pub fn failure_rate(
        name: impl Into<String>,
        severity: AlertSeverity,
        trip_threshold: f64,
    ) -> Self {
        Self {
            name: name.into(),
            severity,
            kind: AlertRuleKind::FailureRate {
                trip_threshold,
                clear_threshold: trip_threshold,
                window: Duration::from_secs(60),
                min_samples: 1,
            },
            cooldown: Duration::from_secs(60),
        }
    }

    /// Build a queue-depth rule. Clear depth defaults to the trip depth.
    #[must_use]
    pub fn queue_depth(name: impl Into<String>, severity: AlertSeverity, trip_depth: u64) -> Self {
        Self {
            name: name.into(),
            severity,
            kind: AlertRuleKind::QueueDepth {
                trip_depth,
                clear_depth: trip_depth,
                queue: None,
            },
            cooldown: Duration::from_secs(60),
        }
    }

    /// Build a no-heartbeat rule.
    #[must_use]
    pub fn no_heartbeat(
        name: impl Into<String>,
        severity: AlertSeverity,
        timeout: Duration,
    ) -> Self {
        Self {
            name: name.into(),
            severity,
            kind: AlertRuleKind::NoHeartbeat { timeout },
            cooldown: Duration::from_secs(60),
        }
    }

    /// Set the cooldown between repeated firing alerts.
    #[must_use]
    pub fn with_cooldown(mut self, cooldown: Duration) -> Self {
        self.cooldown = cooldown;
        self
    }

    /// Set the clear threshold for a failure-rate rule (enables hysteresis).
    #[must_use]
    pub fn with_clear_threshold(mut self, clear: f64) -> Self {
        if let AlertRuleKind::FailureRate {
            clear_threshold, ..
        } = &mut self.kind
        {
            *clear_threshold = clear;
        }
        self
    }

    /// Set the sliding window for a failure-rate rule.
    #[must_use]
    pub fn with_window(mut self, window: Duration) -> Self {
        if let AlertRuleKind::FailureRate { window: w, .. } = &mut self.kind {
            *w = window;
        }
        self
    }

    /// Set the minimum samples for a failure-rate rule.
    #[must_use]
    pub fn with_min_samples(mut self, min_samples: u64) -> Self {
        if let AlertRuleKind::FailureRate { min_samples: m, .. } = &mut self.kind {
            *m = min_samples;
        }
        self
    }

    /// Set the clear depth for a queue-depth rule (enables hysteresis).
    #[must_use]
    pub fn with_clear_depth(mut self, clear: u64) -> Self {
        if let AlertRuleKind::QueueDepth { clear_depth, .. } = &mut self.kind {
            *clear_depth = clear;
        }
        self
    }

    /// Watch a specific queue for a queue-depth rule.
    #[must_use]
    pub fn for_queue(mut self, queue: impl Into<String>) -> Self {
        if let AlertRuleKind::QueueDepth { queue: q, .. } = &mut self.kind {
            *q = Some(queue.into());
        }
        self
    }
}

/// Whether an emitted alert signals a rule firing or recovering.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AlertStatus {
    /// The rule's condition is currently met.
    Firing,
    /// The rule's condition cleared after previously firing.
    Resolved,
}

/// An alert emitted by [`AlertEvaluator`] when a rule trips or recovers.
///
/// Named distinctly from [`crate::event::Alert`] so both can coexist; this one
/// is rule-centric (carries the rule name, status, and observed value) whereas
/// [`crate::event::Alert`] is event-centric.
#[derive(Debug, Clone)]
pub struct RuleAlert {
    /// Name of the rule that produced this alert.
    pub rule_name: String,
    /// Severity inherited from the rule.
    pub severity: AlertSeverity,
    /// Whether the rule is firing or has resolved.
    pub status: AlertStatus,
    /// Human-readable description of why the alert fired.
    pub message: String,
    /// The observed value that drove the decision (rate, depth, or seconds of
    /// silence depending on the rule kind).
    pub observed_value: f64,
    /// When the alert was generated.
    pub timestamp: DateTime<Utc>,
}

impl RuleAlert {
    /// Returns `true` if this alert indicates the rule is currently firing.
    #[inline]
    #[must_use]
    pub fn is_firing(&self) -> bool {
        self.status == AlertStatus::Firing
    }
}

/// Hard cap on the number of completion samples a single rule may retain.
///
/// Events arrive from many workers, so a single skewed-ahead clock could
/// otherwise keep the window from ever draining. The cap bounds memory
/// regardless of what timestamps arrive.
pub const MAX_WINDOW_SAMPLES: usize = 10_000;

/// Per-rule mutable evaluation state held by the evaluator.
#[derive(Debug)]
struct RuleState {
    /// Whether the rule is currently considered tripped.
    firing: bool,
    /// Last time a *firing* alert was emitted (for cooldown).
    last_fired: Option<DateTime<Utc>>,
    /// For failure-rate rules: recent completions as (timestamp, was_failure).
    completions: VecDeque<(DateTime<Utc>, bool)>,
    /// Highest event timestamp seen so far, used as a monotonic "now" so that
    /// an out-of-order (or clock-skewed) event cannot rewind the window.
    max_seen_ts: Option<DateTime<Utc>>,
}

impl RuleState {
    fn new() -> Self {
        Self {
            firing: false,
            last_fired: None,
            completions: VecDeque::new(),
            max_seen_ts: None,
        }
    }

    /// Fold `now` into the monotonic clock and return the effective "now".
    fn observe_time(&mut self, now: DateTime<Utc>) -> DateTime<Utc> {
        let effective = match self.max_seen_ts {
            Some(seen) if seen > now => seen,
            _ => now,
        };
        self.max_seen_ts = Some(effective);
        effective
    }
}

/// Consumes events and snapshots, applying [`AlertRule`]s and emitting
/// [`RuleAlert`]s with hysteresis and cooldown.
///
/// The evaluator is `!Sync`-free (it is plain `Send` state) and intended to be
/// driven from a single monitoring task. Call [`observe_event`](AlertEvaluator::observe_event)
/// for each incoming [`Event`] and [`observe_snapshot`](AlertEvaluator::observe_snapshot)
/// for each periodic [`EventSnapshot`]; both return any alerts produced.
#[derive(Debug)]
pub struct AlertEvaluator {
    rules: Vec<AlertRule>,
    states: Vec<RuleState>,
    /// Last time any worker heartbeat / presence was observed.
    last_heartbeat: Option<DateTime<Utc>>,
}

impl Default for AlertEvaluator {
    fn default() -> Self {
        Self::new()
    }
}

impl AlertEvaluator {
    /// Create an evaluator with no rules.
    #[must_use]
    pub fn new() -> Self {
        Self {
            rules: Vec::new(),
            states: Vec::new(),
            last_heartbeat: None,
        }
    }

    /// Add a rule (builder style).
    #[must_use]
    pub fn with_rule(mut self, rule: AlertRule) -> Self {
        self.add_rule(rule);
        self
    }

    /// Add a rule in place.
    pub fn add_rule(&mut self, rule: AlertRule) {
        self.rules.push(rule);
        self.states.push(RuleState::new());
    }

    /// Number of configured rules.
    #[inline]
    #[must_use]
    pub fn rule_count(&self) -> usize {
        self.rules.len()
    }

    /// Whether the rule with `name` is currently firing.
    #[must_use]
    pub fn is_firing(&self, name: &str) -> bool {
        self.rules
            .iter()
            .position(|r| r.name == name)
            .and_then(|i| self.states.get(i))
            .is_some_and(|s| s.firing)
    }

    /// Observe a single event, updating internal windows and evaluating rules.
    ///
    /// Returns any alerts (firing or resolved) produced as a result.
    pub fn observe_event(&mut self, event: &Event) -> Vec<RuleAlert> {
        let now = event.timestamp();

        // Track heartbeat/presence for no-heartbeat rules.
        if matches!(
            event,
            Event::Worker(WorkerEvent::Heartbeat { .. } | WorkerEvent::Online { .. })
        ) {
            self.last_heartbeat = Some(now);
        }

        // Record completions for failure-rate rules.
        let completion = match event {
            Event::Task(TaskEvent::Succeeded { .. }) => Some(false),
            Event::Task(TaskEvent::Failed { .. }) => Some(true),
            _ => None,
        };

        let mut alerts = Vec::new();
        for idx in 0..self.rules.len() {
            let kind = self.rules[idx].kind.clone();
            match kind {
                AlertRuleKind::FailureRate {
                    trip_threshold,
                    clear_threshold,
                    window,
                    min_samples,
                } => {
                    let rule = self.rules[idx].clone();
                    let state = &mut self.states[idx];
                    if let Some(is_failure) = completion {
                        state.completions.push_back((now, is_failure));
                    }
                    // Events arrive from several workers, so timestamps are not
                    // monotonic: evaluate against the highest timestamp seen so
                    // far rather than this event's own.
                    let now = state.observe_time(now);
                    Self::prune_completions(&mut state.completions, now, window);
                    let total = state.completions.len() as u64;
                    let failed = state.completions.iter().filter(|&&(_, f)| f).count() as u64;
                    let rate = if total == 0 {
                        0.0
                    } else {
                        failed as f64 / total as f64
                    };
                    let can_fire = total >= min_samples;
                    if let Some(alert) = Self::evaluate_threshold(
                        &rule,
                        state,
                        now,
                        rate,
                        trip_threshold,
                        clear_threshold,
                        can_fire,
                        true,
                        format!(
                            "failure rate {:.0}% ({failed}/{total}) over window",
                            rate * 100.0
                        ),
                    ) {
                        alerts.push(alert);
                    }
                }
                AlertRuleKind::NoHeartbeat { timeout } => {
                    let rule = self.rules[idx].clone();
                    if let Some(alert) = self.no_heartbeat_alert(idx, now, timeout, rule) {
                        alerts.push(alert);
                    }
                }
                AlertRuleKind::QueueDepth { .. } => {
                    // Queue depth is snapshot-driven; events do not change it.
                }
            }
        }
        alerts
    }

    /// Observe a periodic snapshot, evaluating snapshot-driven rules.
    ///
    /// Returns any alerts produced. Queue-depth and no-heartbeat rules are
    /// primarily evaluated here; failure-rate rules also incorporate the
    /// snapshot's completed counts as additional samples.
    pub fn observe_snapshot(&mut self, snapshot: &EventSnapshot) -> Vec<RuleAlert> {
        let now = snapshot.timestamp;
        if snapshot.worker_count > 0 {
            self.last_heartbeat = Some(now);
        }

        let mut alerts = Vec::new();
        for idx in 0..self.rules.len() {
            let kind = self.rules[idx].kind.clone();
            match kind {
                AlertRuleKind::QueueDepth {
                    trip_depth,
                    clear_depth,
                    queue,
                } => {
                    let depth = queue.as_ref().map_or(snapshot.queue_depth, |q| {
                        snapshot.queue_depths.get(q).copied().unwrap_or(0)
                    });
                    let rule = self.rules[idx].clone();
                    let state = &mut self.states[idx];
                    if let Some(alert) = Self::evaluate_threshold(
                        &rule,
                        state,
                        now,
                        depth as f64,
                        trip_depth as f64,
                        clear_depth as f64,
                        true,
                        false,
                        format!("queue depth {depth}"),
                    ) {
                        alerts.push(alert);
                    }
                }
                AlertRuleKind::NoHeartbeat { timeout } => {
                    let rule = self.rules[idx].clone();
                    if let Some(alert) = self.no_heartbeat_alert(idx, now, timeout, rule) {
                        alerts.push(alert);
                    }
                }
                AlertRuleKind::FailureRate { .. } => {
                    // Failure-rate is event-driven; snapshots leave it unchanged.
                }
            }
        }
        alerts
    }

    /// Explicitly evaluate time-based rules (e.g. no-heartbeat) at `now`.
    ///
    /// Useful to call on a timer even when no events arrive, so a stalled
    /// cluster still trips the no-heartbeat rule.
    pub fn tick(&mut self, now: DateTime<Utc>) -> Vec<RuleAlert> {
        let mut alerts = Vec::new();
        for idx in 0..self.rules.len() {
            if let AlertRuleKind::NoHeartbeat { timeout } = self.rules[idx].kind {
                let rule = self.rules[idx].clone();
                if let Some(alert) = self.no_heartbeat_alert(idx, now, timeout, rule) {
                    alerts.push(alert);
                }
            }
        }
        alerts
    }

    /// Evaluate a no-heartbeat rule against `now`, returning an alert if state
    /// transitions or a re-fire is warranted.
    fn no_heartbeat_alert(
        &mut self,
        idx: usize,
        now: DateTime<Utc>,
        timeout: Duration,
        rule: AlertRule,
    ) -> Option<RuleAlert> {
        let secs_silent = match self.last_heartbeat {
            Some(last) => (now - last).num_milliseconds().max(0) as f64 / 1000.0,
            None => f64::INFINITY,
        };
        let timeout_secs = timeout.as_secs_f64();
        let tripped = secs_silent > timeout_secs;
        let state = &mut self.states[idx];
        let display = if secs_silent.is_finite() {
            format!("no heartbeat for {secs_silent:.1}s")
        } else {
            "no heartbeat observed yet".to_string()
        };
        Self::transition(&rule, state, now, tripped, secs_silent, display)
    }

    /// Generic threshold evaluation with hysteresis + cooldown.
    ///
    /// `trip_when_ge` selects the comparison direction: `true` trips when
    /// `value >= trip` and clears when `value < clear` (used by both failure
    /// rate and queue depth, which are "high is bad" metrics).
    #[allow(clippy::too_many_arguments)]
    fn evaluate_threshold(
        rule: &AlertRule,
        state: &mut RuleState,
        now: DateTime<Utc>,
        value: f64,
        trip: f64,
        clear: f64,
        can_fire: bool,
        _trip_when_ge: bool,
        display: String,
    ) -> Option<RuleAlert> {
        let tripped = if state.firing {
            // Stay firing until value drops below the clear threshold.
            value >= clear
        } else {
            can_fire && value >= trip
        };
        Self::transition(rule, state, now, tripped, value, display)
    }

    /// Apply a desired tripped/cleared decision to `state`, honouring cooldown,
    /// and produce an alert if a transition or cooldown-permitted re-fire occurs.
    fn transition(
        rule: &AlertRule,
        state: &mut RuleState,
        now: DateTime<Utc>,
        tripped: bool,
        observed_value: f64,
        display: String,
    ) -> Option<RuleAlert> {
        if tripped {
            let was_firing = state.firing;
            state.firing = true;
            let cooldown_ok = match state.last_fired {
                None => true,
                Some(last) => {
                    let elapsed = (now - last).num_milliseconds().max(0) as u128;
                    elapsed >= rule.cooldown.as_millis()
                }
            };
            if !was_firing || cooldown_ok {
                state.last_fired = Some(now);
                return Some(RuleAlert {
                    rule_name: rule.name.clone(),
                    severity: rule.severity,
                    status: AlertStatus::Firing,
                    message: format!("{}: {display}", rule.name),
                    observed_value,
                    timestamp: now,
                });
            }
            None
        } else {
            if state.firing {
                state.firing = false;
                state.last_fired = None;
                return Some(RuleAlert {
                    rule_name: rule.name.clone(),
                    severity: rule.severity,
                    status: AlertStatus::Resolved,
                    message: format!("{} resolved: {display}", rule.name),
                    observed_value,
                    timestamp: now,
                });
            }
            None
        }
    }

    /// Drop completions that fall outside `window`, and enforce the hard cap.
    ///
    /// Pruning is order-independent (`retain`, not "pop while the *front* is
    /// old"): entries arrive from multiple workers, so a single newer entry at
    /// the front used to pin every older entry behind it in the window forever,
    /// stretching the effective window far beyond the configured one.
    fn prune_completions(
        completions: &mut VecDeque<(DateTime<Utc>, bool)>,
        now: DateTime<Utc>,
        window: Duration,
    ) {
        let cutoff = now - chrono::Duration::milliseconds(window.as_millis() as i64);
        completions.retain(|(ts, _)| *ts >= cutoff);
        // Belt and braces: a clock skewed far into the future would otherwise
        // keep entries indefinitely, so the deque is also hard-capped.
        while completions.len() > MAX_WINDOW_SAMPLES {
            completions.pop_front();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{TaskEventBuilder, WorkerEventBuilder};
    use uuid::Uuid;

    fn failed_event(at: DateTime<Utc>) -> Event {
        // Construct directly so we control the timestamp.
        Event::Task(TaskEvent::Failed {
            task_id: Uuid::new_v4(),
            task_name: "t".to_string(),
            hostname: "w1".to_string(),
            timestamp: at,
            exception: "Boom".to_string(),
            traceback: None,
        })
    }

    fn succeeded_event(at: DateTime<Utc>) -> Event {
        Event::Task(TaskEvent::Succeeded {
            task_id: Uuid::new_v4(),
            task_name: "t".to_string(),
            hostname: "w1".to_string(),
            timestamp: at,
            runtime: 0.1,
            result: None,
        })
    }

    fn heartbeat_event(at: DateTime<Utc>) -> Event {
        Event::Worker(WorkerEvent::Heartbeat {
            hostname: "w1".to_string(),
            timestamp: at,
            active: 1,
            processed: 1,
            loadavg: None,
            freq: 1.0,
        })
    }

    #[test]
    fn test_snapshot_builder_from_events() {
        let now = Utc::now();
        let events = vec![
            TaskEventBuilder::new(Uuid::new_v4(), "t").sent("celery"),
            succeeded_event(now),
            failed_event(now),
            WorkerEventBuilder::new("w1").online(),
            WorkerEventBuilder::new("w2").online(),
        ];
        let mut b = EventSnapshotBuilder::new();
        b.observe_all(&events);
        let snap = b.queue_depth("celery", 7).build();
        assert_eq!(snap.pending, 1);
        assert_eq!(snap.succeeded, 1);
        assert_eq!(snap.failed, 1);
        assert_eq!(snap.worker_count, 2);
        assert_eq!(snap.queue_depth, 7);
        assert!((snap.failure_rate() - 0.5).abs() < 1e-9);
    }

    #[test]
    fn test_snapshot_serialization_roundtrip() {
        let snap = EventSnapshotBuilder::new()
            .pending(3)
            .active(2)
            .succeeded(10)
            .failed(2)
            .worker_count(4)
            .queue_depth("q", 5)
            .build();
        let json = serde_json::to_string(&snap).unwrap();
        let parsed: EventSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(snap, parsed);
    }

    #[test]
    fn test_failure_rate_trips_and_recovers() {
        let rule = AlertRule::failure_rate("fail", AlertSeverity::Error, 0.5)
            .with_clear_threshold(0.2)
            .with_min_samples(4)
            .with_window(Duration::from_secs(3600))
            .with_cooldown(Duration::from_secs(0));
        let mut eval = AlertEvaluator::new().with_rule(rule);

        let base = Utc::now();
        // 4 failures -> rate 1.0 >= 0.5, fires.
        let mut fired = false;
        for i in 0..4 {
            let at = base + chrono::Duration::seconds(i);
            let alerts = eval.observe_event(&failed_event(at));
            if alerts.iter().any(|a| a.is_firing()) {
                fired = true;
            }
        }
        assert!(fired, "rule should fire on high failure rate");
        assert!(eval.is_firing("fail"));

        // Now feed many successes to push rate below clear threshold (0.2).
        let mut resolved = false;
        for i in 4..40 {
            let at = base + chrono::Duration::seconds(i);
            let alerts = eval.observe_event(&succeeded_event(at));
            if alerts.iter().any(|a| a.status == AlertStatus::Resolved) {
                resolved = true;
            }
        }
        assert!(resolved, "rule should resolve once failure rate drops");
        assert!(!eval.is_firing("fail"));
    }

    #[test]
    fn test_failure_rate_respects_min_samples() {
        let rule = AlertRule::failure_rate("fail", AlertSeverity::Warning, 0.5)
            .with_min_samples(10)
            .with_cooldown(Duration::from_secs(0));
        let mut eval = AlertEvaluator::new().with_rule(rule);
        let base = Utc::now();
        // 3 failures: rate 1.0 but only 3 samples (< 10) -> no fire.
        for i in 0..3 {
            let at = base + chrono::Duration::seconds(i);
            let alerts = eval.observe_event(&failed_event(at));
            assert!(alerts.is_empty());
        }
        assert!(!eval.is_firing("fail"));
    }

    #[test]
    fn test_queue_depth_hysteresis() {
        let rule = AlertRule::queue_depth("depth", AlertSeverity::Warning, 100)
            .with_clear_depth(50)
            .with_cooldown(Duration::from_secs(0));
        let mut eval = AlertEvaluator::new().with_rule(rule);
        let now = Utc::now();

        let low = EventSnapshotBuilder::new()
            .timestamp(now)
            .total_queue_depth(40)
            .build();
        assert!(eval.observe_snapshot(&low).is_empty());

        let high = EventSnapshotBuilder::new()
            .timestamp(now)
            .total_queue_depth(150)
            .build();
        let alerts = eval.observe_snapshot(&high);
        assert!(alerts.iter().any(|a| a.is_firing()));
        assert!(eval.is_firing("depth"));

        // Drop to 60: above clear(50) -> still firing, no new alert (cooldown 0
        // but no transition since stays firing... actually re-fire allowed).
        let mid = EventSnapshotBuilder::new()
            .timestamp(now)
            .total_queue_depth(60)
            .build();
        let _ = eval.observe_snapshot(&mid);
        assert!(eval.is_firing("depth"), "60 > clear threshold 50");

        // Drop to 40: below clear -> resolves.
        let cleared = EventSnapshotBuilder::new()
            .timestamp(now)
            .total_queue_depth(40)
            .build();
        let alerts = eval.observe_snapshot(&cleared);
        assert!(alerts.iter().any(|a| a.status == AlertStatus::Resolved));
        assert!(!eval.is_firing("depth"));
    }

    #[test]
    fn test_queue_depth_specific_queue() {
        let rule = AlertRule::queue_depth("q-depth", AlertSeverity::Error, 10).for_queue("urgent");
        let mut eval = AlertEvaluator::new().with_rule(rule);
        let now = Utc::now();
        let snap = EventSnapshotBuilder::new()
            .timestamp(now)
            .queue_depth("urgent", 20)
            .queue_depth("bulk", 5)
            .build();
        let alerts = eval.observe_snapshot(&snap);
        assert!(alerts.iter().any(|a| a.is_firing()));
    }

    #[test]
    fn test_no_heartbeat_trips_via_tick() {
        let rule = AlertRule::no_heartbeat("hb", AlertSeverity::Critical, Duration::from_secs(30));
        let mut eval = AlertEvaluator::new().with_rule(rule);
        let base = Utc::now();

        // Record a heartbeat.
        let alerts = eval.observe_event(&heartbeat_event(base));
        assert!(alerts.is_empty());

        // Tick 10s later: still within timeout.
        let _ = eval.tick(base + chrono::Duration::seconds(10));
        assert!(!eval.is_firing("hb"));

        // Tick 45s after last heartbeat: trips.
        let alerts = eval.tick(base + chrono::Duration::seconds(45));
        assert!(alerts.iter().any(|a| a.is_firing()));
        assert!(eval.is_firing("hb"));

        // New heartbeat resolves it.
        let alerts = eval.observe_event(&heartbeat_event(base + chrono::Duration::seconds(46)));
        assert!(alerts.iter().any(|a| a.status == AlertStatus::Resolved));
        assert!(!eval.is_firing("hb"));
    }

    #[test]
    fn test_cooldown_suppresses_repeat_firing() {
        let rule = AlertRule::queue_depth("depth", AlertSeverity::Warning, 10)
            .with_cooldown(Duration::from_secs(300));
        let mut eval = AlertEvaluator::new().with_rule(rule);
        let now = Utc::now();

        let high = EventSnapshotBuilder::new()
            .timestamp(now)
            .total_queue_depth(50)
            .build();
        let first = eval.observe_snapshot(&high);
        assert_eq!(first.iter().filter(|a| a.is_firing()).count(), 1);

        // Same condition shortly after: within cooldown, suppressed.
        let again = EventSnapshotBuilder::new()
            .timestamp(now + chrono::Duration::seconds(5))
            .total_queue_depth(50)
            .build();
        let second = eval.observe_snapshot(&again);
        assert!(second.iter().all(|a| !a.is_firing()));

        // After cooldown elapses, it re-fires.
        let later = EventSnapshotBuilder::new()
            .timestamp(now + chrono::Duration::seconds(400))
            .total_queue_depth(50)
            .build();
        let third = eval.observe_snapshot(&later);
        assert_eq!(third.iter().filter(|a| a.is_firing()).count(), 1);
    }

    #[test]
    fn test_multiple_rules_evaluated() {
        let mut eval = AlertEvaluator::new()
            .with_rule(AlertRule::queue_depth("d", AlertSeverity::Warning, 10))
            .with_rule(AlertRule::no_heartbeat(
                "hb",
                AlertSeverity::Critical,
                Duration::from_secs(5),
            ));
        assert_eq!(eval.rule_count(), 2);
        let now = Utc::now();
        // High depth + no heartbeat ever -> both fire on snapshot with 0 workers.
        let snap = EventSnapshotBuilder::new()
            .timestamp(now)
            .total_queue_depth(99)
            .worker_count(0)
            .build();
        let alerts = eval.observe_snapshot(&snap);
        assert!(alerts.iter().any(|a| a.rule_name == "d" && a.is_firing()));
        assert!(alerts.iter().any(|a| a.rule_name == "hb" && a.is_firing()));
    }

    #[test]
    fn test_failure_rate_window_pruning() {
        let rule = AlertRule::failure_rate("fail", AlertSeverity::Error, 0.5)
            .with_min_samples(2)
            .with_window(Duration::from_secs(10))
            .with_cooldown(Duration::from_secs(0));
        let mut eval = AlertEvaluator::new().with_rule(rule);
        let base = Utc::now();

        // Two old failures, then much later two successes: old ones pruned.
        eval.observe_event(&failed_event(base));
        eval.observe_event(&failed_event(base + chrono::Duration::seconds(1)));
        assert!(eval.is_firing("fail"));

        // 100s later, successes only -> failures pruned, rate 0 -> resolves.
        let mut resolved = false;
        for i in 0..3 {
            let at = base + chrono::Duration::seconds(100 + i);
            let alerts = eval.observe_event(&succeeded_event(at));
            if alerts.iter().any(|a| a.status == AlertStatus::Resolved) {
                resolved = true;
            }
        }
        assert!(resolved);
        assert!(!eval.is_firing("fail"));
    }

    #[test]
    fn test_queue_depth_builder_cannot_underflow() {
        // Regression: the builder maintained a running u64 total with
        // `total + depth - prev`, which panicked (debug) or wrapped to ~1.8e19
        // (release) when `total_queue_depth` desynchronised it.
        let snap = EventSnapshotBuilder::new()
            .queue_depth("a", 10)
            .total_queue_depth(5)
            .queue_depth("a", 0)
            .build();
        assert_eq!(snap.queue_depth, 5, "explicit total wins");
        assert_eq!(snap.queue_depths.get("a"), Some(&0));

        // Without an explicit total, the aggregate is the sum of the map and is
        // independent of call ordering.
        let snap = EventSnapshotBuilder::new()
            .queue_depth("a", 10)
            .queue_depth("b", 4)
            .queue_depth("a", 1)
            .build();
        assert_eq!(snap.queue_depth, 5);

        // Overwriting a queue downwards is safe from any starting point.
        let snap = EventSnapshotBuilder::new()
            .queue_depth("a", u64::MAX)
            .queue_depth("a", 0)
            .build();
        assert_eq!(snap.queue_depth, 0);
    }

    #[test]
    fn test_failure_window_prunes_out_of_order_events() {
        // Regression: pruning only popped from the *front*, so once a newer
        // entry sat at the front every older entry behind it stayed in the
        // window forever and the failure rate was computed over hours of data.
        let rule = AlertRule::failure_rate("fail", AlertSeverity::Error, 0.9)
            .with_window(Duration::from_secs(60))
            .with_min_samples(1);
        let mut eval = AlertEvaluator::new().with_rule(rule);

        let now = Utc::now();
        // A newer event arrives first (clock skew / delivery reordering)...
        eval.observe_event(&succeeded_event(now));
        // ...followed by a batch of older failures, all outside the window.
        for i in 1..=20 {
            eval.observe_event(&failed_event(now - chrono::Duration::seconds(600 + i)));
        }

        // Only the in-window sample survives, so the rule is not firing.
        assert_eq!(eval.states[0].completions.len(), 1);
        assert!(!eval.is_firing("fail"));
    }

    #[test]
    fn test_failure_window_is_bounded_under_clock_skew() {
        // A worker whose clock is far ahead must not be able to pin the window
        // open: the deque is hard-capped regardless of timestamps.
        let rule = AlertRule::failure_rate("fail", AlertSeverity::Error, 0.5)
            .with_window(Duration::from_secs(1))
            .with_min_samples(1);
        let mut eval = AlertEvaluator::new().with_rule(rule);

        let skewed = Utc::now() + chrono::Duration::days(3650);
        // Push past the cap; every sample shares one (skewed) timestamp inside
        // the window, so the hard cap is the only thing that can bound it.
        for _ in 0..(MAX_WINDOW_SAMPLES + 250) {
            eval.observe_event(&failed_event(skewed));
        }
        assert_eq!(eval.states[0].completions.len(), MAX_WINDOW_SAMPLES);

        // A subsequent in-order event does not rewind the monotonic clock.
        eval.observe_event(&succeeded_event(Utc::now()));
        assert_eq!(eval.states[0].max_seen_ts, Some(skewed));
    }
}
