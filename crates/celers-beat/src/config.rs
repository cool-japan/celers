//! Schedule error types and configuration
//!
//! Contains the core error type and related configuration types.

use thiserror::Error;

/// Schedule errors
#[derive(Debug, Error)]
pub enum ScheduleError {
    #[error("Invalid schedule: {0}")]
    Invalid(String),

    #[error("Not implemented: {0}")]
    NotImplemented(String),

    #[error("Parsing error: {0}")]
    Parse(String),

    #[error("Persistence error: {0}")]
    Persistence(String),
}

impl ScheduleError {
    /// Check if error is an invalid schedule configuration
    pub fn is_invalid(&self) -> bool {
        matches!(self, ScheduleError::Invalid(_))
    }

    /// Check if error is a not-implemented feature
    pub fn is_not_implemented(&self) -> bool {
        matches!(self, ScheduleError::NotImplemented(_))
    }

    /// Check if error is a parsing error
    pub fn is_parse(&self) -> bool {
        matches!(self, ScheduleError::Parse(_))
    }

    /// Check if error is a persistence error
    pub fn is_persistence(&self) -> bool {
        matches!(self, ScheduleError::Persistence(_))
    }

    /// Check if this error is retryable
    ///
    /// Persistence errors are retryable (transient I/O issues).
    /// Invalid schedules, parse errors, and not-implemented features are not retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(self, ScheduleError::Persistence(_))
    }

    /// Get the error category as a string
    pub fn category(&self) -> &'static str {
        match self {
            ScheduleError::Invalid(_) => "invalid",
            ScheduleError::NotImplemented(_) => "not_implemented",
            ScheduleError::Parse(_) => "parse",
            ScheduleError::Persistence(_) => "persistence",
        }
    }
}
