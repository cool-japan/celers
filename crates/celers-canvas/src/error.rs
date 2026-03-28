/// Canvas errors
#[derive(Debug, thiserror::Error)]
pub enum CanvasError {
    #[error("Invalid workflow: {0}")]
    Invalid(String),

    #[error("Broker error: {0}")]
    Broker(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Workflow cancelled: {0}")]
    Cancelled(String),

    #[error("Workflow timeout: {0}")]
    Timeout(String),
}

impl CanvasError {
    /// Check if error is invalid workflow
    pub fn is_invalid(&self) -> bool {
        matches!(self, CanvasError::Invalid(_))
    }

    /// Check if error is broker-related
    pub fn is_broker(&self) -> bool {
        matches!(self, CanvasError::Broker(_))
    }

    /// Check if error is serialization-related
    pub fn is_serialization(&self) -> bool {
        matches!(self, CanvasError::Serialization(_))
    }

    /// Check if error is cancellation-related
    pub fn is_cancelled(&self) -> bool {
        matches!(self, CanvasError::Cancelled(_))
    }

    /// Check if error is timeout-related
    pub fn is_timeout(&self) -> bool {
        matches!(self, CanvasError::Timeout(_))
    }

    /// Check if this error is retryable
    ///
    /// Broker errors are typically retryable (transient network issues).
    /// Invalid workflow, serialization, cancellation, and timeout errors are not retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(self, CanvasError::Broker(_))
    }

    /// Get the error category as a string
    pub fn category(&self) -> &'static str {
        match self {
            CanvasError::Invalid(_) => "invalid",
            CanvasError::Broker(_) => "broker",
            CanvasError::Serialization(_) => "serialization",
            CanvasError::Cancelled(_) => "cancelled",
            CanvasError::Timeout(_) => "timeout",
        }
    }
}
