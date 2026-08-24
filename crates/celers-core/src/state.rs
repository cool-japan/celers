use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

/// Task state enumeration with strict state machine transitions
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskState {
    /// Task is queued but not yet picked up by a worker
    Pending,

    /// Task has been received by a worker
    Received,

    /// Task has been reserved by a worker but not yet executed
    Reserved,

    /// Task is currently being executed
    Running,

    /// Task is being retried (includes retry count)
    Retrying(u32),

    /// Task completed successfully with result
    Succeeded(Vec<u8>),

    /// Task failed with error message
    Failed(String),

    /// Task has been revoked
    Revoked,

    /// Task has been rejected
    Rejected,

    /// Custom user-defined state with optional metadata
    Custom {
        /// Custom state name
        name: String,
        /// Optional custom metadata as JSON bytes
        metadata: Option<Vec<u8>>,
    },
}

impl TaskState {
    /// Check if the task is in a terminal state
    #[inline]
    #[must_use]
    pub const fn is_terminal(&self) -> bool {
        matches!(
            self,
            TaskState::Succeeded(_)
                | TaskState::Failed(_)
                | TaskState::Revoked
                | TaskState::Rejected
        )
    }

    /// Create a custom state with a name
    #[must_use]
    pub fn custom(name: impl Into<String>) -> Self {
        Self::Custom {
            name: name.into(),
            metadata: None,
        }
    }

    /// Create a custom state with name and JSON metadata
    #[must_use]
    pub fn custom_with_metadata(name: impl Into<String>, metadata: Vec<u8>) -> Self {
        Self::Custom {
            name: name.into(),
            metadata: Some(metadata),
        }
    }

    /// Check if this is a custom state
    #[inline]
    #[must_use]
    pub const fn is_custom(&self) -> bool {
        matches!(self, TaskState::Custom { .. })
    }

    /// Get the custom state name if this is a custom state
    #[inline]
    #[must_use]
    pub fn custom_name(&self) -> Option<&str> {
        match self {
            TaskState::Custom { name, .. } => Some(name),
            _ => None,
        }
    }

    /// Get the custom state metadata if this is a custom state with metadata
    #[inline]
    #[must_use]
    pub fn custom_metadata(&self) -> Option<&[u8]> {
        match self {
            TaskState::Custom { metadata, .. } => metadata.as_deref(),
            _ => None,
        }
    }

    /// Check if the task is revoked
    #[inline]
    #[must_use]
    pub const fn is_revoked(&self) -> bool {
        matches!(self, TaskState::Revoked)
    }

    /// Check if the task is rejected
    #[inline]
    #[must_use]
    pub const fn is_rejected(&self) -> bool {
        matches!(self, TaskState::Rejected)
    }

    /// Check if the task is received
    #[inline]
    #[must_use]
    pub const fn is_received(&self) -> bool {
        matches!(self, TaskState::Received)
    }

    /// Check if the task can be retried
    #[inline]
    #[must_use]
    pub const fn can_retry(&self, max_retries: u32) -> bool {
        match self {
            TaskState::Failed(_) => true,
            TaskState::Retrying(count) => *count < max_retries,
            _ => false,
        }
    }

    /// Get the retry count
    #[inline]
    #[must_use]
    pub const fn retry_count(&self) -> u32 {
        match self {
            TaskState::Retrying(count) => *count,
            _ => 0,
        }
    }

    /// Check if the task is in an active (non-terminal) state
    #[inline]
    #[must_use]
    pub const fn is_active(&self) -> bool {
        !self.is_terminal()
    }

    /// Check if the task is pending
    #[inline]
    #[must_use]
    pub const fn is_pending(&self) -> bool {
        matches!(self, TaskState::Pending)
    }

    /// Check if the task is reserved
    #[inline]
    #[must_use]
    pub const fn is_reserved(&self) -> bool {
        matches!(self, TaskState::Reserved)
    }

    /// Check if the task is running
    #[inline]
    #[must_use]
    pub const fn is_running(&self) -> bool {
        matches!(self, TaskState::Running)
    }

    /// Check if the task is retrying
    #[inline]
    #[must_use]
    pub const fn is_retrying(&self) -> bool {
        matches!(self, TaskState::Retrying(_))
    }

    /// Check if the task succeeded
    #[inline]
    #[must_use]
    pub const fn is_succeeded(&self) -> bool {
        matches!(self, TaskState::Succeeded(_))
    }

    /// Check if the task failed
    #[inline]
    #[must_use]
    pub const fn is_failed(&self) -> bool {
        matches!(self, TaskState::Failed(_))
    }

    /// Get the success result if the task succeeded
    #[inline]
    #[must_use]
    pub fn success_result(&self) -> Option<&[u8]> {
        match self {
            TaskState::Succeeded(result) => Some(result),
            _ => None,
        }
    }

    /// Get the error message if the task failed
    #[inline]
    #[must_use]
    pub fn error_message(&self) -> Option<&str> {
        match self {
            TaskState::Failed(error) => Some(error),
            _ => None,
        }
    }
}

impl fmt::Display for TaskState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TaskState::Pending => write!(f, "PENDING"),
            TaskState::Received => write!(f, "RECEIVED"),
            TaskState::Reserved => write!(f, "RESERVED"),
            TaskState::Running => write!(f, "RUNNING"),
            TaskState::Retrying(count) => write!(f, "RETRYING({count})"),
            TaskState::Succeeded(_) => write!(f, "SUCCEEDED"),
            TaskState::Failed(err) => write!(f, "FAILED: {err}"),
            TaskState::Revoked => write!(f, "REVOKED"),
            TaskState::Rejected => write!(f, "REJECTED"),
            TaskState::Custom { name, .. } => write!(f, "CUSTOM({name})"),
        }
    }
}

impl TaskState {
    /// Get a short string representation of the state name
    #[inline]
    #[must_use]
    pub fn name(&self) -> &str {
        match self {
            TaskState::Pending => "PENDING",
            TaskState::Received => "RECEIVED",
            TaskState::Reserved => "RESERVED",
            TaskState::Running => "RUNNING",
            TaskState::Retrying(_) => "RETRYING",
            TaskState::Succeeded(_) => "SUCCESS",
            TaskState::Failed(_) => "FAILURE",
            TaskState::Revoked => "REVOKED",
            TaskState::Rejected => "REJECTED",
            TaskState::Custom { name, .. } => name,
        }
    }

    /// Whether the state machine permits moving from `self` to `next`.
    ///
    /// The type has always documented "strict state machine transitions", but no
    /// validation existed, so `Succeeded -> Pending`, `Revoked -> Running` and
    /// `Failed -> Received` were all recorded silently. The legal graph is:
    ///
    /// ```text
    /// Pending  -> Received | Reserved | Running | Revoked | Rejected | Failed
    /// Received -> Reserved | Running  | Revoked | Rejected | Failed
    /// Reserved -> Running  | Revoked  | Rejected | Failed | Pending (requeue)
    /// Running  -> Succeeded | Failed | Retrying | Revoked | Rejected
    /// Retrying -> Pending | Received | Reserved | Running | Failed | Revoked | Rejected
    /// terminal -> (nothing)
    /// ```
    ///
    /// A [`TaskState::Custom`] state is an explicit escape hatch: it may be
    /// entered from any non-terminal state and may move anywhere, because its
    /// meaning is defined by the application rather than by this machine. A
    /// self-transition (same state name) is always allowed so that
    /// `Retrying(1) -> Retrying(2)` and progress updates work.
    #[must_use]
    pub fn can_transition_to(&self, next: &Self) -> bool {
        // Re-entering the same logical state is always fine (retry counters,
        // progress updates on a custom state, ...).
        if self.name() == next.name() {
            return !self.is_terminal();
        }

        // Terminal states are final.
        if self.is_terminal() {
            return false;
        }

        // Application-defined states bypass the machine in both directions.
        if matches!(self, TaskState::Custom { .. }) || matches!(next, TaskState::Custom { .. }) {
            return true;
        }

        match self {
            TaskState::Pending => matches!(
                next,
                TaskState::Received
                    | TaskState::Reserved
                    | TaskState::Running
                    | TaskState::Revoked
                    | TaskState::Rejected
                    | TaskState::Failed(_)
            ),
            TaskState::Received => matches!(
                next,
                TaskState::Reserved
                    | TaskState::Running
                    | TaskState::Revoked
                    | TaskState::Rejected
                    | TaskState::Failed(_)
            ),
            // A reserved task can be released back to the queue.
            TaskState::Reserved => matches!(
                next,
                TaskState::Pending
                    | TaskState::Running
                    | TaskState::Revoked
                    | TaskState::Rejected
                    | TaskState::Failed(_)
            ),
            TaskState::Running => matches!(
                next,
                TaskState::Succeeded(_)
                    | TaskState::Failed(_)
                    | TaskState::Retrying(_)
                    | TaskState::Revoked
                    | TaskState::Rejected
            ),
            // A retry goes back to the head of the lifecycle.
            TaskState::Retrying(_) => matches!(
                next,
                TaskState::Pending
                    | TaskState::Received
                    | TaskState::Reserved
                    | TaskState::Running
                    | TaskState::Failed(_)
                    | TaskState::Revoked
                    | TaskState::Rejected
            ),
            // Unreachable: handled by the terminal check above.
            TaskState::Succeeded(_)
            | TaskState::Failed(_)
            | TaskState::Revoked
            | TaskState::Rejected => false,
            // Unreachable: handled by the custom check above.
            TaskState::Custom { .. } => true,
        }
    }
}

// ============================================================================
// State Transitions
// ============================================================================

/// A state transition record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateTransition {
    /// Previous state
    pub from: TaskState,
    /// New state
    pub to: TaskState,
    /// Unix timestamp when the transition occurred
    pub timestamp: f64,
    /// Optional reason for the transition
    pub reason: Option<String>,
    /// Optional additional metadata
    pub metadata: Option<HashMap<String, serde_json::Value>>,
}

impl StateTransition {
    /// Create a new state transition
    #[must_use]
    pub fn new(from: TaskState, to: TaskState) -> Self {
        Self {
            from,
            to,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64(),
            reason: None,
            metadata: None,
        }
    }

    /// Add a reason for the transition
    #[must_use]
    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    /// Add metadata to the transition
    #[must_use]
    pub fn with_metadata(mut self, metadata: HashMap<String, serde_json::Value>) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Add a single metadata key-value pair
    #[must_use]
    pub fn with_meta(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.metadata
            .get_or_insert_with(HashMap::new)
            .insert(key.into(), value);
        self
    }
}

/// An attempted transition the state machine does not allow.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidTransition {
    /// State the task was in.
    pub from: String,
    /// State the caller tried to move it to.
    pub to: String,
}

impl fmt::Display for InvalidTransition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid task state transition from {} to {}",
            self.from, self.to
        )
    }
}

impl std::error::Error for InvalidTransition {}

impl From<InvalidTransition> for crate::CelersError {
    fn from(err: InvalidTransition) -> Self {
        crate::CelersError::InvalidStateTransition {
            from: err.from,
            to: err.to,
        }
    }
}

/// Default cap on the number of transitions a [`StateHistory`] retains.
///
/// A task that retries many times would otherwise grow the log without bound.
/// When the cap is reached the oldest transitions are dropped; the current state
/// and the most recent history are what diagnostics actually need.
pub const DEFAULT_MAX_TRANSITIONS: usize = 256;

/// Serde default for [`StateHistory::max_transitions`].
const fn default_max_transitions() -> usize {
    DEFAULT_MAX_TRANSITIONS
}

/// Tracks state transitions for a task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StateHistory {
    /// Current state
    pub current: Option<TaskState>,
    /// List of state transitions (bounded by `max_transitions`, oldest dropped)
    pub transitions: Vec<StateTransition>,
    /// Unix timestamp when this history was created.
    ///
    /// Without it the initial state has no entry timestamp, so
    /// [`Self::time_in_state`] reported `None` for it — indistinguishable from
    /// "never in that state" — even though queue-wait time is exactly the
    /// metric that function is most often asked for.
    #[serde(default = "current_unix_time")]
    pub created_at: f64,
    /// Maximum number of transitions retained.
    #[serde(default = "default_max_transitions")]
    pub max_transitions: usize,
}

/// Current Unix time in fractional seconds.
fn current_unix_time() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

impl Default for StateHistory {
    fn default() -> Self {
        Self {
            current: None,
            transitions: Vec::new(),
            created_at: current_unix_time(),
            max_transitions: DEFAULT_MAX_TRANSITIONS,
        }
    }
}

impl StateHistory {
    /// Create a new state history
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new state history with initial state
    #[must_use]
    pub fn with_initial(state: TaskState) -> Self {
        Self {
            current: Some(state),
            ..Self::default()
        }
    }

    /// Set the maximum number of retained transitions.
    ///
    /// A value of `0` is treated as `1`.
    #[must_use]
    pub fn with_max_transitions(mut self, max: usize) -> Self {
        self.max_transitions = max.max(1);
        self.trim_transitions();
        self
    }

    /// Drop the oldest transitions until the retention bound is respected.
    fn trim_transitions(&mut self) {
        let max = self.max_transitions.max(1);
        if self.transitions.len() > max {
            let excess = self.transitions.len() - max;
            self.transitions.drain(..excess);
        }
    }

    /// Record a transition, honouring the retention bound.
    fn push_transition(&mut self, transition: StateTransition) {
        self.transitions.push(transition);
        self.trim_transitions();
    }

    /// Transition to a new state, without checking whether it is legal.
    ///
    /// Prefer [`Self::try_transition`], which rejects transitions the state
    /// machine does not permit (`Succeeded -> Pending`, `Revoked -> Running`,
    /// any move out of a terminal state, ...). This permissive variant remains
    /// for callers that are replaying externally-authoritative state.
    pub fn force_transition(&mut self, to: TaskState) -> Option<StateTransition> {
        let from = self.current.take()?;
        let transition = StateTransition::new(from, to.clone());
        self.current = Some(to);
        self.push_transition(transition.clone());
        Some(transition)
    }

    /// Transition to a new state, without checking whether it is legal.
    ///
    /// Alias of [`Self::force_transition`], kept for backwards compatibility.
    pub fn transition(&mut self, to: TaskState) -> Option<StateTransition> {
        self.force_transition(to)
    }

    /// Transition to a new state, rejecting illegal transitions.
    ///
    /// # Errors
    ///
    /// Returns [`InvalidTransition`] when the current state does not permit the
    /// requested target (see [`TaskState::can_transition_to`]). Returns `Ok(None)`
    /// when there is no current state to transition *from*.
    pub fn try_transition(
        &mut self,
        to: TaskState,
    ) -> Result<Option<StateTransition>, InvalidTransition> {
        let Some(from) = self.current.as_ref() else {
            return Ok(None);
        };
        if !from.can_transition_to(&to) {
            return Err(InvalidTransition {
                from: from.name().to_string(),
                to: to.name().to_string(),
            });
        }
        Ok(self.force_transition(to))
    }

    /// Transition to a new state with a reason, rejecting illegal transitions.
    ///
    /// # Errors
    ///
    /// As for [`Self::try_transition`].
    pub fn try_transition_with_reason(
        &mut self,
        to: TaskState,
        reason: impl Into<String>,
    ) -> Result<Option<StateTransition>, InvalidTransition> {
        let Some(from) = self.current.as_ref() else {
            return Ok(None);
        };
        if !from.can_transition_to(&to) {
            return Err(InvalidTransition {
                from: from.name().to_string(),
                to: to.name().to_string(),
            });
        }
        Ok(self.transition_with_reason(to, reason))
    }

    /// Transition to a new state with a reason
    pub fn transition_with_reason(
        &mut self,
        to: TaskState,
        reason: impl Into<String>,
    ) -> Option<StateTransition> {
        let from = self.current.take()?;
        let transition = StateTransition::new(from, to.clone()).with_reason(reason);
        self.current = Some(to);
        self.push_transition(transition.clone());
        Some(transition)
    }

    /// Get the current state
    #[inline]
    #[must_use]
    pub fn current_state(&self) -> Option<&TaskState> {
        self.current.as_ref()
    }

    /// Get all transitions
    #[inline]
    #[must_use]
    pub fn get_transitions(&self) -> &[StateTransition] {
        &self.transitions
    }

    /// Get the last transition
    #[inline]
    #[must_use]
    pub fn last_transition(&self) -> Option<&StateTransition> {
        self.transitions.last()
    }

    /// Get the number of transitions
    #[inline]
    #[must_use]
    pub const fn transition_count(&self) -> usize {
        self.transitions.len()
    }

    /// Check if task has ever been in a specific state
    #[inline]
    #[must_use]
    pub fn has_been_in_state(&self, state_name: &str) -> bool {
        self.transitions.iter().any(|t| t.to.name() == state_name)
            || self
                .current
                .as_ref()
                .is_some_and(|s| s.name() == state_name)
    }

    /// Get the time spent in a specific state (returns None if never in that state)
    #[must_use]
    pub fn time_in_state(&self, state_name: &str) -> Option<f64> {
        let mut total_time = 0.0;
        // The initial state is entered when the history is created; no
        // transition records it, so seed the entry time from `created_at`.
        let mut entry_time: Option<f64> = self
            .transitions
            .first()
            .map_or_else(
                || self.current.as_ref().map(TaskState::name),
                |first| Some(first.from.name()),
            )
            .filter(|initial| *initial == state_name)
            .map(|_| self.created_at);

        for transition in &self.transitions {
            if transition.from.name() == state_name {
                if let Some(entry) = entry_time {
                    total_time += transition.timestamp - entry;
                    entry_time = None;
                }
            }
            if transition.to.name() == state_name {
                entry_time = Some(transition.timestamp);
            }
        }

        // If still in the state, add time until now
        if let Some(entry) = entry_time {
            if self
                .current
                .as_ref()
                .is_some_and(|s| s.name() == state_name)
            {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs_f64();
                total_time += now - entry;
            }
        }

        if total_time > 0.0 || entry_time.is_some() {
            Some(total_time)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_terminal_states() {
        assert!(TaskState::Succeeded(vec![]).is_terminal());
        assert!(TaskState::Failed("error".to_string()).is_terminal());
        assert!(!TaskState::Pending.is_terminal());
        assert!(!TaskState::Running.is_terminal());
    }

    #[test]
    fn test_retry_logic() {
        assert!(TaskState::Failed("error".to_string()).can_retry(3));
        assert!(TaskState::Retrying(2).can_retry(3));
        assert!(!TaskState::Retrying(3).can_retry(3));
        assert!(!TaskState::Succeeded(vec![]).can_retry(3));
    }

    // ------------------------------------------------------------------
    // Regression tests
    // ------------------------------------------------------------------

    /// Regression: the type documented "strict state machine transitions" but no
    /// validation existed, so terminal states could be left and the lifecycle
    /// could run backwards.
    #[test]
    fn test_state_machine_rejects_illegal_transitions() {
        let illegal: &[(TaskState, TaskState)] = &[
            (TaskState::Succeeded(vec![]), TaskState::Pending),
            (TaskState::Succeeded(vec![]), TaskState::Running),
            (TaskState::Revoked, TaskState::Running),
            (TaskState::Rejected, TaskState::Received),
            (TaskState::Failed("boom".into()), TaskState::Received),
            (TaskState::Pending, TaskState::Succeeded(vec![])),
            (TaskState::Pending, TaskState::Retrying(1)),
            (TaskState::Received, TaskState::Pending),
            (TaskState::Running, TaskState::Received),
            (TaskState::Running, TaskState::Pending),
        ];
        for (from, to) in illegal {
            assert!(
                !from.can_transition_to(to),
                "{} -> {} must be rejected",
                from.name(),
                to.name()
            );
        }

        let legal: &[(TaskState, TaskState)] = &[
            (TaskState::Pending, TaskState::Received),
            (TaskState::Pending, TaskState::Running),
            (TaskState::Received, TaskState::Reserved),
            (TaskState::Reserved, TaskState::Running),
            (TaskState::Reserved, TaskState::Pending),
            (TaskState::Running, TaskState::Succeeded(vec![1])),
            (TaskState::Running, TaskState::Failed("boom".into())),
            (TaskState::Running, TaskState::Retrying(1)),
            (TaskState::Running, TaskState::Revoked),
            (TaskState::Retrying(1), TaskState::Pending),
            (TaskState::Retrying(1), TaskState::Retrying(2)),
            (TaskState::Running, TaskState::custom("PROGRESS")),
            (TaskState::custom("PROGRESS"), TaskState::Running),
        ];
        for (from, to) in legal {
            assert!(
                from.can_transition_to(to),
                "{} -> {} must be allowed",
                from.name(),
                to.name()
            );
        }
    }

    #[test]
    fn test_try_transition_enforces_the_state_machine() {
        let mut history = StateHistory::with_initial(TaskState::Pending);

        assert!(history
            .try_transition(TaskState::Running)
            .expect("pending -> running is legal")
            .is_some());
        assert!(history
            .try_transition(TaskState::Succeeded(vec![1, 2]))
            .expect("running -> succeeded is legal")
            .is_some());

        let err = history
            .try_transition(TaskState::Pending)
            .expect_err("a terminal state must not be left");
        assert_eq!(err.from, "SUCCESS");
        assert_eq!(err.to, "PENDING");
        // The rejected transition is not recorded and the state is unchanged.
        assert_eq!(history.transition_count(), 2);
        assert_eq!(
            history.current_state().map(TaskState::name),
            Some("SUCCESS")
        );

        // The permissive variant still exists for replaying external state.
        assert!(history.force_transition(TaskState::Pending).is_some());
        assert_eq!(
            history.current_state().map(TaskState::name),
            Some("PENDING")
        );

        // With no current state there is nothing to transition from.
        let mut empty = StateHistory::new();
        assert!(empty
            .try_transition(TaskState::Running)
            .expect("no current state is not an error")
            .is_none());
    }

    #[test]
    fn test_try_transition_with_reason_enforces_the_state_machine() {
        let mut history = StateHistory::with_initial(TaskState::Running);
        let transition = history
            .try_transition_with_reason(TaskState::Retrying(1), "transient failure")
            .expect("running -> retrying is legal")
            .expect("a transition should be recorded");
        assert_eq!(transition.reason.as_deref(), Some("transient failure"));

        let mut done = StateHistory::with_initial(TaskState::Revoked);
        assert!(done
            .try_transition_with_reason(TaskState::Running, "nope")
            .is_err());
    }

    #[test]
    fn test_invalid_transition_converts_to_celers_error() {
        let err = InvalidTransition {
            from: "SUCCESS".to_string(),
            to: "PENDING".to_string(),
        };
        assert!(err.to_string().contains("SUCCESS"));
        let converted: crate::CelersError = err.into();
        assert!(matches!(
            converted,
            crate::CelersError::InvalidStateTransition { .. }
        ));
    }

    /// Regression: `transitions` was an unbounded `Vec`, so a task that retried
    /// many times grew it for the life of the history.
    #[test]
    fn test_transition_history_is_bounded() {
        let mut history = StateHistory::with_initial(TaskState::Pending).with_max_transitions(4);

        for i in 0..20u32 {
            history.force_transition(TaskState::Retrying(i));
        }

        assert_eq!(history.transition_count(), 4);
        // The newest transitions survive.
        let last = history
            .last_transition()
            .expect("a transition should exist");
        assert_eq!(last.to, TaskState::Retrying(19));
        assert_eq!(history.current_state(), Some(&TaskState::Retrying(19)));

        // Zero is clamped to one.
        let mut tiny = StateHistory::with_initial(TaskState::Pending).with_max_transitions(0);
        assert_eq!(tiny.max_transitions, 1);
        tiny.force_transition(TaskState::Running);
        tiny.force_transition(TaskState::Revoked);
        assert_eq!(tiny.transition_count(), 1);
    }

    /// Regression: `with_initial` recorded no entry timestamp, so the time spent
    /// in the initial state (queue wait) was reported as `None`.
    #[test]
    fn test_time_in_initial_state_is_measured() {
        let mut history = StateHistory::with_initial(TaskState::Pending);
        // Back-date creation so the interval is unambiguously positive.
        history.created_at -= 5.0;

        history.force_transition(TaskState::Running);
        let pending_time = history
            .time_in_state("PENDING")
            .expect("time in the initial state must be measurable");
        assert!(
            pending_time >= 5.0,
            "expected at least 5s of queue wait, got {pending_time}"
        );

        history.force_transition(TaskState::Succeeded(vec![]));
        let pending_time_after = history
            .time_in_state("PENDING")
            .expect("time in the initial state must still be measurable");
        assert!((pending_time_after - pending_time).abs() < 1.0);

        // A state the task was never in still reports None.
        assert!(history.time_in_state("REVOKED").is_none());
    }

    #[test]
    fn test_time_in_initial_state_while_still_there() {
        let mut history = StateHistory::with_initial(TaskState::Pending);
        history.created_at -= 3.0;
        let waited = history
            .time_in_state("PENDING")
            .expect("still-pending time must be measurable");
        assert!(waited >= 3.0, "got {waited}");
    }

    #[test]
    fn test_state_history_serde_roundtrip_keeps_created_at() {
        let mut history = StateHistory::with_initial(TaskState::Pending);
        history.force_transition(TaskState::Running);

        let json = serde_json::to_string(&history).expect("serialize");
        let parsed: StateHistory = serde_json::from_str(&json).expect("deserialize");
        assert!((parsed.created_at - history.created_at).abs() < f64::EPSILON);
        assert_eq!(parsed.max_transitions, history.max_transitions);

        // Legacy payloads without the new fields still load.
        let legacy: StateHistory =
            serde_json::from_str(r#"{"current":null,"transitions":[]}"#).expect("legacy");
        assert_eq!(legacy.max_transitions, DEFAULT_MAX_TRANSITIONS);
        assert!(legacy.created_at > 0.0);
    }

    // Property-based tests
    #[cfg(test)]
    mod proptests {
        use super::*;
        use proptest::prelude::*;

        // Strategy for generating TaskState
        fn task_state_strategy() -> impl Strategy<Value = TaskState> {
            prop_oneof![
                Just(TaskState::Pending),
                Just(TaskState::Received),
                Just(TaskState::Reserved),
                Just(TaskState::Running),
                (0u32..100).prop_map(TaskState::Retrying),
                prop::collection::vec(any::<u8>(), 0..100).prop_map(TaskState::Succeeded),
                any::<String>().prop_map(TaskState::Failed),
                Just(TaskState::Revoked),
                Just(TaskState::Rejected),
            ]
        }

        proptest! {
            #[test]
            fn test_terminal_states_are_consistent(state in task_state_strategy()) {
                // Property: If a state is terminal, it should not be active
                if state.is_terminal() {
                    prop_assert!(!state.is_active());
                } else {
                    prop_assert!(state.is_active());
                }
            }

            #[test]
            fn test_retry_count_is_non_negative(count in 0u32..1000) {
                let state = TaskState::Retrying(count);
                prop_assert_eq!(state.retry_count(), count);
                prop_assert!(state.is_retrying());
            }

            #[test]
            fn test_can_retry_respects_max_retries(current_retry in 0u32..100, max_retries in 0u32..100) {
                let state = TaskState::Retrying(current_retry);
                let can_retry = state.can_retry(max_retries);

                if current_retry < max_retries {
                    prop_assert!(can_retry, "Should be able to retry when current_retry < max_retries");
                } else {
                    prop_assert!(!can_retry, "Should not be able to retry when current_retry >= max_retries");
                }
            }

            #[test]
            fn test_failed_state_can_always_retry_once(max_retries in 1u32..100) {
                let state = TaskState::Failed("error".to_string());
                prop_assert!(state.can_retry(max_retries));
            }

            #[test]
            fn test_terminal_states_cannot_retry(max_retries in 1u32..100) {
                let terminal_states = vec![
                    TaskState::Succeeded(vec![1, 2, 3]),
                    TaskState::Revoked,
                    TaskState::Rejected,
                ];

                for state in terminal_states {
                    if !matches!(state, TaskState::Failed(_)) {
                        prop_assert!(!state.can_retry(max_retries) || state.is_failed());
                    }
                }
            }

            #[test]
            fn test_state_name_is_consistent(state in task_state_strategy()) {
                let name = state.name();
                prop_assert!(!name.is_empty(), "State name should never be empty");

                // Name should match the state type
                match &state {
                    TaskState::Pending => prop_assert_eq!(name, "PENDING"),
                    TaskState::Received => prop_assert_eq!(name, "RECEIVED"),
                    TaskState::Reserved => prop_assert_eq!(name, "RESERVED"),
                    TaskState::Running => prop_assert_eq!(name, "RUNNING"),
                    TaskState::Retrying(_) => prop_assert_eq!(name, "RETRYING"),
                    TaskState::Succeeded(_) => prop_assert_eq!(name, "SUCCESS"),
                    TaskState::Failed(_) => prop_assert_eq!(name, "FAILURE"),
                    TaskState::Revoked => prop_assert_eq!(name, "REVOKED"),
                    TaskState::Rejected => prop_assert_eq!(name, "REJECTED"),
                    TaskState::Custom { name: custom_name, .. } => prop_assert_eq!(name, custom_name),
                }
            }

            #[test]
            fn test_success_result_only_for_succeeded(result in prop::collection::vec(any::<u8>(), 0..100)) {
                let success_state = TaskState::Succeeded(result.clone());
                prop_assert_eq!(success_state.success_result(), Some(result.as_slice()));

                let other_states = vec![
                    TaskState::Pending,
                    TaskState::Running,
                    TaskState::Failed("error".to_string()),
                ];

                for state in other_states {
                    prop_assert_eq!(state.success_result(), None);
                }
            }

            #[test]
            fn test_error_message_only_for_failed(error_msg in any::<String>()) {
                let failed_state = TaskState::Failed(error_msg.clone());
                prop_assert_eq!(failed_state.error_message(), Some(error_msg.as_str()));

                let other_states = vec![
                    TaskState::Pending,
                    TaskState::Running,
                    TaskState::Succeeded(vec![]),
                ];

                for state in other_states {
                    prop_assert_eq!(state.error_message(), None);
                }
            }

            #[test]
            fn test_state_history_transitions_accumulate(
                num_transitions in 1usize..20,
            ) {
                let mut history = StateHistory::with_initial(TaskState::Pending);

                for i in 0..num_transitions {
                    let new_state = if i % 2 == 0 {
                        TaskState::Running
                    } else {
                        TaskState::Pending
                    };
                    history.transition(new_state);
                }

                prop_assert_eq!(history.transition_count(), num_transitions);
                prop_assert!(history.last_transition().is_some());
            }

            #[test]
            fn test_state_history_current_state_is_latest(state in task_state_strategy()) {
                let mut history = StateHistory::with_initial(TaskState::Pending);
                history.transition(state.clone());

                prop_assert_eq!(history.current_state(), Some(&state));
            }
        }
    }
}
