use serde::{Deserialize, Serialize};
use std::fmt;

/// Task state enumeration with strict state machine transitions
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TaskState {
    /// Task is queued but not yet picked up by a worker
    Pending,

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
}

impl TaskState {
    /// Check if the task is in a terminal state
    pub fn is_terminal(&self) -> bool {
        matches!(self, TaskState::Succeeded(_) | TaskState::Failed(_))
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
            TaskState::Reserved => write!(f, "RESERVED"),
            TaskState::Running => write!(f, "RUNNING"),
            TaskState::Retrying(count) => write!(f, "RETRYING({})", count),
            TaskState::Succeeded(_) => write!(f, "SUCCEEDED"),
            TaskState::Failed(err) => write!(f, "FAILED: {}", err),
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
