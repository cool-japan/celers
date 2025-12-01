use crate::state::TaskState;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fmt;
use uuid::Uuid;

/// Unique identifier for a task
pub type TaskId = Uuid;

/// Task metadata including execution information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskMetadata {
    /// Unique task identifier
    pub id: TaskId,

    /// Task name/type identifier
    pub name: String,

    /// Current state of the task
    pub state: TaskState,

    /// When the task was created
    pub created_at: DateTime<Utc>,

    /// When the task was last updated
    pub updated_at: DateTime<Utc>,

    /// Maximum number of retry attempts
    pub max_retries: u32,

    /// Task timeout in seconds
    pub timeout_secs: Option<u64>,

    /// Task priority (higher = more important)
    pub priority: i32,

    /// Group ID (for workflow grouping)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_id: Option<Uuid>,

    /// Chord ID (for barrier synchronization)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chord_id: Option<Uuid>,
}

impl TaskMetadata {
    pub fn new(name: String) -> Self {
        let now = Utc::now();
        Self {
            id: Uuid::new_v4(),
            name,
            state: TaskState::Pending,
            created_at: now,
            updated_at: now,
            max_retries: 3,
            timeout_secs: None,
            priority: 0,
            group_id: None,
            chord_id: None,
        }
    }

    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    pub fn with_timeout(mut self, timeout_secs: u64) -> Self {
        self.timeout_secs = Some(timeout_secs);
        self
    }

    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// Set the group ID for workflow grouping
    pub fn with_group_id(mut self, group_id: Uuid) -> Self {
        self.group_id = Some(group_id);
        self
    }

    /// Set the chord ID for barrier synchronization
    pub fn with_chord_id(mut self, chord_id: Uuid) -> Self {
        self.chord_id = Some(chord_id);
        self
    }

    /// Get the age of the task (time since creation)
    pub fn age(&self) -> chrono::Duration {
        Utc::now() - self.created_at
    }

    /// Check if the task has expired based on its timeout
    pub fn is_expired(&self) -> bool {
        if let Some(timeout_secs) = self.timeout_secs {
            let elapsed = (Utc::now() - self.created_at).num_seconds();
            elapsed > timeout_secs as i64
        } else {
            false
        }
    }

    /// Check if the task is in a terminal state (Succeeded or Failed)
    pub fn is_terminal(&self) -> bool {
        self.state.is_terminal()
    }

    /// Check if the task is in a running or active state
    pub fn is_active(&self) -> bool {
        matches!(
            self.state,
            TaskState::Pending | TaskState::Reserved | TaskState::Running | TaskState::Retrying(_)
        )
    }

    /// Validate the task metadata
    ///
    /// Returns an error if any of the metadata fields are invalid:
    /// - Name must not be empty
    /// - Max retries must be reasonable (< 1000)
    /// - Timeout must be at least 1 second if set
    /// - Priority must be in valid range (-2147483648 to 2147483647)
    pub fn validate(&self) -> Result<(), String> {
        if self.name.is_empty() {
            return Err("Task name cannot be empty".to_string());
        }

        if self.max_retries > 1000 {
            return Err("Max retries cannot exceed 1000".to_string());
        }

        if let Some(timeout) = self.timeout_secs {
            if timeout == 0 {
                return Err("Timeout must be at least 1 second".to_string());
            }
            if timeout > 86400 {
                return Err("Timeout cannot exceed 24 hours (86400 seconds)".to_string());
            }
        }

        Ok(())
    }

    /// Check if task has a timeout configured
    pub fn has_timeout(&self) -> bool {
        self.timeout_secs.is_some()
    }

    /// Check if task is part of a group
    pub fn has_group_id(&self) -> bool {
        self.group_id.is_some()
    }

    /// Check if task is part of a chord
    pub fn has_chord_id(&self) -> bool {
        self.chord_id.is_some()
    }

    /// Check if task has custom priority (non-zero)
    pub fn has_priority(&self) -> bool {
        self.priority != 0
    }

    /// Check if task has high priority (priority > 0)
    pub fn is_high_priority(&self) -> bool {
        self.priority > 0
    }

    /// Check if task has low priority (priority < 0)
    pub fn is_low_priority(&self) -> bool {
        self.priority < 0
    }
}

impl fmt::Display for TaskMetadata {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Task[{}] name={} state={} priority={} retries={}/{}",
            &self.id.to_string()[..8],
            self.name,
            self.state,
            self.priority,
            self.state.retry_count(),
            self.max_retries
        )?;

        if let Some(timeout) = self.timeout_secs {
            write!(f, " timeout={}s", timeout)?;
        }

        if let Some(chord_id) = self.chord_id {
            write!(f, " chord={}", &chord_id.to_string()[..8])?;
        }

        Ok(())
    }
}

/// Trait for tasks that can be executed
#[async_trait::async_trait]
pub trait Task: Send + Sync {
    /// The input type for this task
    type Input: Serialize + for<'de> Deserialize<'de> + Send;

    /// The output type for this task
    type Output: Serialize + for<'de> Deserialize<'de> + Send;

    /// Execute the task with the given input
    async fn execute(&self, input: Self::Input) -> crate::Result<Self::Output>;

    /// Get the task name
    fn name(&self) -> &str;
}

/// Serialized task ready for queue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializedTask {
    /// Task metadata
    pub metadata: TaskMetadata,

    /// Serialized task payload
    pub payload: Vec<u8>,
}

impl SerializedTask {
    pub fn new(name: String, payload: Vec<u8>) -> Self {
        Self {
            metadata: TaskMetadata::new(name),
            payload,
        }
    }

    pub fn with_priority(mut self, priority: i32) -> Self {
        self.metadata.priority = priority;
        self
    }

    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.metadata.max_retries = max_retries;
        self
    }

    pub fn with_timeout(mut self, timeout_secs: u64) -> Self {
        self.metadata.timeout_secs = Some(timeout_secs);
        self
    }

    /// Set the group ID for workflow grouping
    pub fn with_group_id(mut self, group_id: Uuid) -> Self {
        self.metadata.group_id = Some(group_id);
        self
    }

    /// Set the chord ID for barrier synchronization
    pub fn with_chord_id(mut self, chord_id: Uuid) -> Self {
        self.metadata.chord_id = Some(chord_id);
        self
    }

    /// Get the age of the task (time since creation)
    pub fn age(&self) -> chrono::Duration {
        self.metadata.age()
    }

    /// Check if the task has expired based on its timeout
    pub fn is_expired(&self) -> bool {
        self.metadata.is_expired()
    }

    /// Check if the task is in a terminal state (Success or Failure)
    pub fn is_terminal(&self) -> bool {
        self.metadata.is_terminal()
    }

    /// Check if the task is in a running or active state
    pub fn is_active(&self) -> bool {
        self.metadata.is_active()
    }

    /// Validate the serialized task
    ///
    /// Validates both metadata and payload constraints:
    /// - Delegates metadata validation to TaskMetadata::validate()
    /// - Checks payload size (must be < 1MB by default)
    pub fn validate(&self) -> Result<(), String> {
        self.metadata.validate()?;

        if self.payload.is_empty() {
            return Err("Task payload cannot be empty".to_string());
        }

        if self.payload.len() > 1_048_576 {
            return Err(format!(
                "Task payload too large: {} bytes (max 1MB)",
                self.payload.len()
            ));
        }

        Ok(())
    }

    /// Validate with custom payload size limit
    pub fn validate_with_limit(&self, max_payload_bytes: usize) -> Result<(), String> {
        self.metadata.validate()?;

        if self.payload.is_empty() {
            return Err("Task payload cannot be empty".to_string());
        }

        if self.payload.len() > max_payload_bytes {
            return Err(format!(
                "Task payload too large: {} bytes (max {} bytes)",
                self.payload.len(),
                max_payload_bytes
            ));
        }

        Ok(())
    }

    /// Check if task has a timeout configured
    pub fn has_timeout(&self) -> bool {
        self.metadata.has_timeout()
    }

    /// Check if task is part of a group
    pub fn has_group_id(&self) -> bool {
        self.metadata.has_group_id()
    }

    /// Check if task is part of a chord
    pub fn has_chord_id(&self) -> bool {
        self.metadata.has_chord_id()
    }

    /// Check if task has custom priority (non-zero)
    pub fn has_priority(&self) -> bool {
        self.metadata.has_priority()
    }

    /// Get payload size in bytes
    pub fn payload_size(&self) -> usize {
        self.payload.len()
    }

    /// Check if payload is empty
    pub fn has_empty_payload(&self) -> bool {
        self.payload.is_empty()
    }
}

impl fmt::Display for SerializedTask {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SerializedTask[{}] name={} payload={}B state={}",
            &self.metadata.id.to_string()[..8],
            self.metadata.name,
            self.payload.len(),
            self.metadata.state
        )?;
        if self.metadata.has_priority() {
            write!(f, " priority={}", self.metadata.priority)?;
        }
        if let Some(group_id) = self.metadata.group_id {
            write!(f, " group={}", &group_id.to_string()[..8])?;
        }
        if let Some(chord_id) = self.metadata.chord_id {
            write!(f, " chord={}", &chord_id.to_string()[..8])?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_metadata_creation() {
        let metadata = TaskMetadata::new("test_task".to_string())
            .with_max_retries(5)
            .with_timeout(60)
            .with_priority(10);

        assert_eq!(metadata.name, "test_task");
        assert_eq!(metadata.max_retries, 5);
        assert_eq!(metadata.timeout_secs, Some(60));
        assert_eq!(metadata.priority, 10);
        assert_eq!(metadata.state, TaskState::Pending);
    }
}
