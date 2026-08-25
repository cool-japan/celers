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

    /// A running task observed its cancellation signal and stopped
    /// cooperatively.
    ///
    /// This is the *task body's* way of reporting "I was asked to stop and I
    /// did", which is what lets a cooperative task propagate cancellation with
    /// `?` instead of mapping the runtime's cancellation error by hand:
    ///
    /// ```
    /// use celers_core::CelersError;
    /// use uuid::Uuid;
    ///
    /// # struct Token(bool, Uuid);
    /// # impl Token {
    /// #     fn is_cancelled(&self) -> bool { self.0 }
    /// #     fn id(&self) -> Uuid { self.1 }
    /// # }
    /// fn step(token: &Token) -> celers_core::Result<()> {
    ///     if token.is_cancelled() {
    ///         return Err(CelersError::cancelled(token.id()));
    ///     }
    ///     // ... real work ...
    ///     Ok(())
    /// }
    ///
    /// let cancelled = Token(true, Uuid::new_v4());
    /// let err = step(&cancelled).expect_err("a cancelled token stops the task");
    /// assert!(err.is_cancelled());
    /// assert!(!err.is_retryable());
    /// ```
    ///
    /// # Not the same as [`CelersError::TaskRevoked`]
    ///
    /// [`TaskRevoked`](CelersError::TaskRevoked) is a *dispatch-time* refusal:
    /// the worker never started the task because the broker reported the id as
    /// revoked. `Cancelled` means the task **did** start, ran for a while, and
    /// then stopped at a cancellation checkpoint of its own choosing. A monitor
    /// telling the two apart learns whether any of the task's side effects can
    /// have happened.
    ///
    /// Like a revocation, this is **not** retryable: the request was withdrawn,
    /// so re-running it is the opposite of what was asked for.
    #[error("Task was cancelled: {0}")]
    Cancelled(crate::TaskId),

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
    /// Report that a running task stopped at one of its cancellation
    /// checkpoints.
    ///
    /// See [`CelersError::Cancelled`] for how this differs from
    /// [`CelersError::TaskRevoked`].
    #[inline]
    #[must_use]
    pub const fn cancelled(task_id: crate::TaskId) -> Self {
        CelersError::Cancelled(task_id)
    }

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

    /// Check if the error is a cooperative cancellation
    #[inline]
    #[must_use]
    pub const fn is_cancelled(&self) -> bool {
        matches!(self, CelersError::Cancelled(_))
    }

    /// The task id a withdrawal-of-work error refers to.
    ///
    /// Returns the id for both [`CelersError::Cancelled`] and
    /// [`CelersError::TaskRevoked`] — the two errors that mean "this work was
    /// called off" — and `None` for every other variant. A caller deciding
    /// whether to record a terminal state needs the id, and should not have to
    /// know which of the two it got.
    #[inline]
    #[must_use]
    pub const fn withdrawn_task_id(&self) -> Option<crate::TaskId> {
        match self {
            CelersError::Cancelled(id) | CelersError::TaskRevoked(id) => Some(*id),
            _ => None,
        }
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

    /// Check if this *transport-level* error is worth retrying
    ///
    /// Returns `true` for broker, IO and timeout errors: transient conditions
    /// where the same operation may well succeed on a later attempt.
    ///
    /// Returns `false` for everything else, including
    /// [`CelersError::TaskExecution`]. A task failing in user code is not a
    /// transport problem — retrying it blindly turns a deterministic bug or bad
    /// input into an infinite retry loop. Whether *that* failure should be
    /// retried is decided by
    /// [`ExceptionPolicy`](crate::exception::ExceptionPolicy) /
    /// [`RetryStrategy`](crate::retry::RetryStrategy), which classify the actual
    /// exception.
    #[inline]
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        matches!(
            self,
            CelersError::Broker(_) | CelersError::Io(_) | CelersError::Timeout(_)
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
            CelersError::Cancelled(_) => "cancelled",
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
        // User-code failures are NOT transport-retryable: retryability there is
        // decided by ExceptionPolicy / RetryStrategy, not by the error type.
        assert!(!err.is_retryable());
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
    fn test_cancelled_error() {
        let task_id = uuid::Uuid::new_v4();
        let err = CelersError::cancelled(task_id);

        assert!(err.is_cancelled());
        assert_eq!(err.category(), "cancelled");
        assert_eq!(err.to_string(), format!("Task was cancelled: {task_id}"));

        // A withdrawn request is never retried: re-running it is the opposite
        // of what the caller asked for.
        assert!(!err.is_retryable());

        // It is a distinct classification from every neighbouring variant, so
        // a handler keyed on one of them cannot swallow it.
        assert!(!err.is_task_revoked());
        assert!(!err.is_task_execution());
        assert!(!err.is_timeout());
    }

    #[test]
    fn test_cancelled_is_not_task_revoked() {
        // Regression guard for the distinction the variant exists to make:
        // `TaskRevoked` is refused before dispatch, `Cancelled` stopped partway
        // through. A monitor uses the difference to know whether side effects
        // can have happened, so the two must never collapse into one.
        let task_id = uuid::Uuid::new_v4();
        let cancelled = CelersError::cancelled(task_id);
        let revoked = CelersError::TaskRevoked(task_id);

        assert!(cancelled.is_cancelled() && !cancelled.is_task_revoked());
        assert!(revoked.is_task_revoked() && !revoked.is_cancelled());
        assert_ne!(cancelled.category(), revoked.category());
        assert_ne!(cancelled.to_string(), revoked.to_string());

        // Both are "the work was called off", and both surrender the id.
        assert_eq!(cancelled.withdrawn_task_id(), Some(task_id));
        assert_eq!(revoked.withdrawn_task_id(), Some(task_id));
        assert_eq!(
            CelersError::Other("unrelated".to_string()).withdrawn_task_id(),
            None
        );
    }

    #[test]
    fn test_cancelled_propagates_with_question_mark() {
        // The whole point of the variant: a cooperative task body can use `?`
        // instead of hand-mapping a runtime cancellation error.
        fn checkpoint(cancelled: bool, task_id: crate::TaskId) -> Result<()> {
            if cancelled {
                return Err(CelersError::cancelled(task_id));
            }
            Ok(())
        }

        fn task_body(cancelled: bool, task_id: crate::TaskId) -> Result<u32> {
            checkpoint(cancelled, task_id)?;
            Ok(42)
        }

        let task_id = uuid::Uuid::new_v4();
        assert_eq!(task_body(false, task_id).expect("not cancelled"), 42);

        let err = task_body(true, task_id).expect_err("cancelled");
        assert!(err.is_cancelled());
        assert_eq!(err.withdrawn_task_id(), Some(task_id));
    }

    #[test]
    fn test_is_retryable_logic() {
        // Retryable errors: transient transport-level conditions.
        assert!(CelersError::Broker("timeout".to_string()).is_retryable());
        assert!(CelersError::from(std::io::Error::new(
            std::io::ErrorKind::ConnectionAborted,
            "connection aborted"
        ))
        .is_retryable());
        // Regression: Timeout is the canonical transient failure in a task
        // queue and used to be classified non-retryable.
        assert!(CelersError::Timeout("deadline exceeded".to_string()).is_retryable());

        // Non-retryable errors
        assert!(!CelersError::Serialization("bad format".to_string()).is_retryable());
        assert!(!CelersError::Configuration("invalid config".to_string()).is_retryable());
        assert!(!CelersError::InvalidStateTransition {
            from: "a".to_string(),
            to: "b".to_string()
        }
        .is_retryable());
        // Regression: a poison task would otherwise be retried forever by any
        // caller trusting this classifier.
        assert!(!CelersError::TaskExecution("deterministic bug".to_string()).is_retryable());
        // Withdrawn work is not retried either, by either spelling.
        assert!(!CelersError::cancelled(uuid::Uuid::new_v4()).is_retryable());
        assert!(!CelersError::TaskRevoked(uuid::Uuid::new_v4()).is_retryable());
    }
}
