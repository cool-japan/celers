//! Queue mode definitions.

/// Queue mode
///
/// # Examples
///
/// ```
/// use celers_kombu::QueueMode;
///
/// let fifo = QueueMode::Fifo;
/// assert!(fifo.is_fifo());
/// assert!(!fifo.is_priority());
/// assert_eq!(fifo.to_string(), "FIFO");
///
/// let priority = QueueMode::Priority;
/// assert!(priority.is_priority());
/// assert!(!priority.is_fifo());
/// assert_eq!(priority.to_string(), "Priority");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueMode {
    /// First-In-First-Out
    Fifo,
    /// Priority-based
    Priority,
}

impl QueueMode {
    /// Check if this is FIFO mode
    pub fn is_fifo(&self) -> bool {
        matches!(self, QueueMode::Fifo)
    }

    /// Check if this is Priority mode
    pub fn is_priority(&self) -> bool {
        matches!(self, QueueMode::Priority)
    }
}

impl std::fmt::Display for QueueMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            QueueMode::Fifo => write!(f, "FIFO"),
            QueueMode::Priority => write!(f, "Priority"),
        }
    }
}
