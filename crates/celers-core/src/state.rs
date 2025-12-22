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
    pub fn is_terminal(&self) -> bool {
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
    pub fn is_custom(&self) -> bool {
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
    pub fn is_revoked(&self) -> bool {
        matches!(self, TaskState::Revoked)
    }

    /// Check if the task is rejected
    #[inline]
    #[must_use]
    pub fn is_rejected(&self) -> bool {
        matches!(self, TaskState::Rejected)
    }

    /// Check if the task is received
    #[inline]
    #[must_use]
    pub fn is_received(&self) -> bool {
        matches!(self, TaskState::Received)
    }

    /// Check if the task can be retried
    #[inline]
    #[must_use]
    pub fn can_retry(&self, max_retries: u32) -> bool {
        match self {
            TaskState::Failed(_) => true,
            TaskState::Retrying(count) => *count < max_retries,
            _ => false,
        }
    }

    /// Get the retry count
    #[inline]
    #[must_use]
    pub fn retry_count(&self) -> u32 {
        match self {
            TaskState::Retrying(count) => *count,
            _ => 0,
        }
    }

    /// Check if the task is in an active (non-terminal) state
    #[inline]
    #[must_use]
    pub fn is_active(&self) -> bool {
        !self.is_terminal()
    }

    /// Check if the task is pending
    #[inline]
    #[must_use]
    pub fn is_pending(&self) -> bool {
        matches!(self, TaskState::Pending)
    }

    /// Check if the task is reserved
    #[inline]
    #[must_use]
    pub fn is_reserved(&self) -> bool {
        matches!(self, TaskState::Reserved)
    }

    /// Check if the task is running
    #[inline]
    #[must_use]
    pub fn is_running(&self) -> bool {
        matches!(self, TaskState::Running)
    }

    /// Check if the task is retrying
    #[inline]
    #[must_use]
    pub fn is_retrying(&self) -> bool {
        matches!(self, TaskState::Retrying(_))
    }

    /// Check if the task succeeded
    #[inline]
    #[must_use]
    pub fn is_succeeded(&self) -> bool {
        matches!(self, TaskState::Succeeded(_))
    }

    /// Check if the task failed
    #[inline]
    #[must_use]
    pub fn is_failed(&self) -> bool {
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
            TaskState::Retrying(count) => write!(f, "RETRYING({})", count),
            TaskState::Succeeded(_) => write!(f, "SUCCEEDED"),
            TaskState::Failed(err) => write!(f, "FAILED: {}", err),
            TaskState::Revoked => write!(f, "REVOKED"),
            TaskState::Rejected => write!(f, "REJECTED"),
            TaskState::Custom { name, .. } => write!(f, "CUSTOM({})", name),
        }
    }
}

impl TaskState {
    /// Get a short string representation of the state name
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

/// Tracks state transitions for a task
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct StateHistory {
    /// Current state
    pub current: Option<TaskState>,
    /// List of state transitions
    pub transitions: Vec<StateTransition>,
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
            transitions: Vec::new(),
        }
    }

    /// Transition to a new state
    pub fn transition(&mut self, to: TaskState) -> Option<StateTransition> {
        let from = self.current.take()?;
        let transition = StateTransition::new(from, to.clone());
        self.current = Some(to);
        self.transitions.push(transition.clone());
        Some(transition)
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
        self.transitions.push(transition.clone());
        Some(transition)
    }

    /// Get the current state
    #[must_use]
    pub fn current_state(&self) -> Option<&TaskState> {
        self.current.as_ref()
    }

    /// Get all transitions
    #[must_use]
    pub fn get_transitions(&self) -> &[StateTransition] {
        &self.transitions
    }

    /// Get the last transition
    #[must_use]
    pub fn last_transition(&self) -> Option<&StateTransition> {
        self.transitions.last()
    }

    /// Get the number of transitions
    #[must_use]
    pub fn transition_count(&self) -> usize {
        self.transitions.len()
    }

    /// Check if task has ever been in a specific state
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
        let mut entry_time: Option<f64> = None;

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
