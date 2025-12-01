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
    pub fn custom(name: impl Into<String>) -> Self {
        Self::Custom {
            name: name.into(),
            metadata: None,
        }
    }

    /// Create a custom state with name and JSON metadata
    pub fn custom_with_metadata(name: impl Into<String>, metadata: Vec<u8>) -> Self {
        Self::Custom {
            name: name.into(),
            metadata: Some(metadata),
        }
    }

    /// Check if this is a custom state
    pub fn is_custom(&self) -> bool {
        matches!(self, TaskState::Custom { .. })
    }

    /// Get the custom state name if this is a custom state
    pub fn custom_name(&self) -> Option<&str> {
        match self {
            TaskState::Custom { name, .. } => Some(name),
            _ => None,
        }
    }

    /// Get the custom state metadata if this is a custom state with metadata
    pub fn custom_metadata(&self) -> Option<&[u8]> {
        match self {
            TaskState::Custom { metadata, .. } => metadata.as_deref(),
            _ => None,
        }
    }

    /// Check if the task is revoked
    pub fn is_revoked(&self) -> bool {
        matches!(self, TaskState::Revoked)
    }

    /// Check if the task is rejected
    pub fn is_rejected(&self) -> bool {
        matches!(self, TaskState::Rejected)
    }

    /// Check if the task is received
    pub fn is_received(&self) -> bool {
        matches!(self, TaskState::Received)
    }

    /// Check if the task can be retried
    pub fn can_retry(&self, max_retries: u32) -> bool {
        match self {
            TaskState::Failed(_) => true,
            TaskState::Retrying(count) => *count < max_retries,
            _ => false,
        }
    }

    /// Get the retry count
    pub fn retry_count(&self) -> u32 {
        match self {
            TaskState::Retrying(count) => *count,
            _ => 0,
        }
    }

    /// Check if the task is in an active (non-terminal) state
    pub fn is_active(&self) -> bool {
        !self.is_terminal()
    }

    /// Check if the task is pending
    pub fn is_pending(&self) -> bool {
        matches!(self, TaskState::Pending)
    }

    /// Check if the task is reserved
    pub fn is_reserved(&self) -> bool {
        matches!(self, TaskState::Reserved)
    }

    /// Check if the task is running
    pub fn is_running(&self) -> bool {
        matches!(self, TaskState::Running)
    }

    /// Check if the task is retrying
    pub fn is_retrying(&self) -> bool {
        matches!(self, TaskState::Retrying(_))
    }

    /// Check if the task succeeded
    pub fn is_succeeded(&self) -> bool {
        matches!(self, TaskState::Succeeded(_))
    }

    /// Check if the task failed
    pub fn is_failed(&self) -> bool {
        matches!(self, TaskState::Failed(_))
    }

    /// Get the success result if the task succeeded
    pub fn success_result(&self) -> Option<&[u8]> {
        match self {
            TaskState::Succeeded(result) => Some(result),
            _ => None,
        }
    }

    /// Get the error message if the task failed
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
    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    /// Add metadata to the transition
    pub fn with_metadata(mut self, metadata: HashMap<String, serde_json::Value>) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Add a single metadata key-value pair
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
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new state history with initial state
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
    pub fn current_state(&self) -> Option<&TaskState> {
        self.current.as_ref()
    }

    /// Get all transitions
    pub fn get_transitions(&self) -> &[StateTransition] {
        &self.transitions
    }

    /// Get the last transition
    pub fn last_transition(&self) -> Option<&StateTransition> {
        self.transitions.last()
    }

    /// Get the number of transitions
    pub fn transition_count(&self) -> usize {
        self.transitions.len()
    }

    /// Check if task has ever been in a specific state
    pub fn has_been_in_state(&self, state_name: &str) -> bool {
        self.transitions.iter().any(|t| t.to.name() == state_name)
            || self
                .current
                .as_ref()
                .is_some_and(|s| s.name() == state_name)
    }

    /// Get the time spent in a specific state (returns None if never in that state)
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
}
