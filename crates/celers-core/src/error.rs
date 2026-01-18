use thiserror::Error;

pub type Result<T> = std::result::Result<T, CelersError>;

#[derive(Error, Debug)]
pub enum CelersError {
    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Deserialization error: {0}")]
    Deserialization(String),

    #[error("Broker error: {0}")]
    Broker(String),

    #[error("Task not found: {0}")]
    TaskNotFound(String),

    #[error("Task execution failed: {0}")]
    TaskExecution(String),

    #[error("Task was revoked: {0}")]
    TaskRevoked(crate::TaskId),

    #[error("Task timeout: {0}")]
    Timeout(String),

    #[error("Invalid task state transition from {from:?} to {to:?}")]
    InvalidStateTransition { from: String, to: String },

    #[error("Configuration error: {0}")]
    Configuration(String),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Other error: {0}")]
    Other(String),
}

impl CelersError {
    /// Check if the error is serialization-related
    #[inline]
    #[must_use]
    pub const fn is_serialization(&self) -> bool {
        matches!(self, CelersError::Serialization(_))
    }

    /// Check if the error is deserialization-related
    #[inline]
    #[must_use]
    pub const fn is_deserialization(&self) -> bool {
        matches!(self, CelersError::Deserialization(_))
    }

    /// Check if the error is broker-related
    #[inline]
    #[must_use]
    pub const fn is_broker(&self) -> bool {
        matches!(self, CelersError::Broker(_))
    }

    /// Check if the error is task-not-found
    #[inline]
    #[must_use]
    pub const fn is_task_not_found(&self) -> bool {
        matches!(self, CelersError::TaskNotFound(_))
    }

    /// Check if the error is task-execution-related
    #[inline]
    #[must_use]
    pub const fn is_task_execution(&self) -> bool {
        matches!(self, CelersError::TaskExecution(_))
    }

    /// Check if the error is task-revoked
    #[inline]
    #[must_use]
    pub const fn is_task_revoked(&self) -> bool {
        matches!(self, CelersError::TaskRevoked(_))
    }

    /// Check if the error is timeout-related
    #[inline]
    #[must_use]
    pub const fn is_timeout(&self) -> bool {
        matches!(self, CelersError::Timeout(_))
    }

    /// Check if the error is configuration-related
    #[inline]
    #[must_use]
    pub const fn is_configuration(&self) -> bool {
        matches!(self, CelersError::Configuration(_))
    }

    /// Check if the error is IO-related
    #[inline]
    #[must_use]
    pub const fn is_io(&self) -> bool {
        matches!(self, CelersError::Io(_))
    }

    /// Check if the error is an invalid state transition
    #[inline]
    #[must_use]
    pub const fn is_invalid_state_transition(&self) -> bool {
        matches!(self, CelersError::InvalidStateTransition { .. })
    }

    /// Check if this is a retryable error
    ///
    /// Returns true for broker errors and IO errors, which are typically transient.
    /// Returns false for serialization, configuration, and state transition errors.
    #[inline]
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(
            self,
            CelersError::Broker(_) | CelersError::Io(_) | CelersError::TaskExecution(_)
        )
    }

    /// Get the error category as a string
    #[inline]
    #[must_use]
    pub const fn category(&self) -> &'static str {
        match self {
            CelersError::Serialization(_) => "serialization",
            CelersError::Deserialization(_) => "deserialization",
            CelersError::Broker(_) => "broker",
            CelersError::TaskNotFound(_) => "task_not_found",
            CelersError::TaskExecution(_) => "task_execution",
            CelersError::TaskRevoked(_) => "task_revoked",
            CelersError::Timeout(_) => "timeout",
            CelersError::InvalidStateTransition { .. } => "invalid_state_transition",
            CelersError::Configuration(_) => "configuration",
            CelersError::Io(_) => "io",
            CelersError::Other(_) => "other",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialization_error() {
        let err = CelersError::Serialization("test".to_string());
        assert!(err.is_serialization());
        assert!(!err.is_deserialization());
        assert!(!err.is_broker());
        assert!(!err.is_retryable());
        assert_eq!(err.category(), "serialization");
        assert_eq!(err.to_string(), "Serialization error: test");
    }

    #[test]
    fn test_deserialization_error() {
        let err = CelersError::Deserialization("invalid json".to_string());
        assert!(err.is_deserialization());
        assert!(!err.is_serialization());
        assert!(!err.is_retryable());
        assert_eq!(err.category(), "deserialization");
    }

    #[test]
    fn test_broker_error() {
        let err = CelersError::Broker("connection lost".to_string());
        assert!(err.is_broker());
        assert!(!err.is_serialization());
        assert!(err.is_retryable()); // Broker errors are retryable
        assert_eq!(err.category(), "broker");
    }

    #[test]
    fn test_task_not_found_error() {
        let err = CelersError::TaskNotFound("task123".to_string());
        assert!(err.is_task_not_found());
        assert!(!err.is_task_execution());
        assert!(!err.is_retryable());
        assert_eq!(err.category(), "task_not_found");
    }

    #[test]
    fn test_task_execution_error() {
        let err = CelersError::TaskExecution("panic occurred".to_string());
        assert!(err.is_task_execution());
        assert!(!err.is_task_not_found());
        assert!(err.is_retryable()); // Task execution errors are retryable
        assert_eq!(err.category(), "task_execution");
    }

    #[test]
    fn test_invalid_state_transition_error() {
        let err = CelersError::InvalidStateTransition {
            from: "pending".to_string(),
            to: "success".to_string(),
        };
        assert!(err.is_invalid_state_transition());
        assert!(!err.is_retryable());
        assert_eq!(err.category(), "invalid_state_transition");
        assert!(err
            .to_string()
            .contains("Invalid task state transition from"));
    }

    #[test]
    fn test_configuration_error() {
        let err = CelersError::Configuration("missing redis url".to_string());
        assert!(err.is_configuration());
        assert!(!err.is_retryable());
        assert_eq!(err.category(), "configuration");
    }

    #[test]
    fn test_io_error() {
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
        let err = CelersError::from(io_err);
        assert!(err.is_io());
        assert!(err.is_retryable()); // IO errors are retryable
        assert_eq!(err.category(), "io");
    }

    #[test]
    fn test_other_error() {
        let err = CelersError::Other("unknown error".to_string());
        assert!(!err.is_serialization());
        assert!(!err.is_broker());
        assert!(!err.is_retryable());
        assert_eq!(err.category(), "other");
    }

    #[test]
    fn test_is_retryable_logic() {
        // Retryable errors
        assert!(CelersError::Broker("timeout".to_string()).is_retryable());
        assert!(CelersError::TaskExecution("temporary failure".to_string()).is_retryable());
        assert!(CelersError::from(std::io::Error::new(
            std::io::ErrorKind::ConnectionAborted,
            "connection aborted"
        ))
        .is_retryable());

        // Non-retryable errors
        assert!(!CelersError::Serialization("bad format".to_string()).is_retryable());
        assert!(!CelersError::Configuration("invalid config".to_string()).is_retryable());
        assert!(!CelersError::InvalidStateTransition {
            from: "a".to_string(),
            to: "b".to_string()
        }
        .is_retryable());
    }
}
