use crate::state::TaskState;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
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

    /// Task dependencies (tasks that must complete before this task can execute)
    #[serde(skip_serializing_if = "HashSet::is_empty", default)]
    pub dependencies: HashSet<TaskId>,
}

impl TaskMetadata {
    #[inline]
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
            dependencies: HashSet::new(),
        }
    }

    #[inline]
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    #[inline]
    pub fn with_timeout(mut self, timeout_secs: u64) -> Self {
        self.timeout_secs = Some(timeout_secs);
        self
    }

    #[inline]
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }

    /// Set the group ID for workflow grouping
    #[inline]
    pub fn with_group_id(mut self, group_id: Uuid) -> Self {
        self.group_id = Some(group_id);
        self
    }

    /// Set the chord ID for barrier synchronization
    #[inline]
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

    /// Add a task dependency
    #[inline]
    pub fn with_dependency(mut self, dependency: TaskId) -> Self {
        self.dependencies.insert(dependency);
        self
    }

    /// Add multiple task dependencies
    pub fn with_dependencies(mut self, dependencies: impl IntoIterator<Item = TaskId>) -> Self {
        self.dependencies.extend(dependencies);
        self
    }

    /// Check if task has any dependencies
    #[inline]
    pub fn has_dependencies(&self) -> bool {
        !self.dependencies.is_empty()
    }

    /// Get the number of dependencies
    #[inline]
    pub fn dependency_count(&self) -> usize {
        self.dependencies.len()
    }

    /// Check if a specific task is a dependency
    #[inline]
    pub fn depends_on(&self, task_id: &TaskId) -> bool {
        self.dependencies.contains(task_id)
    }

    /// Remove a dependency
    pub fn remove_dependency(&mut self, task_id: &TaskId) -> bool {
        self.dependencies.remove(task_id)
    }

    /// Clear all dependencies
    pub fn clear_dependencies(&mut self) {
        self.dependencies.clear();
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
///
/// # Zero-Copy Serialization Considerations
///
/// The current implementation uses `Vec<u8>` for the payload, which requires
/// copying data during serialization and deserialization. For high-performance
/// scenarios, consider these alternatives:
///
/// 1. **`Bytes` from `bytes` crate**: Provides cheap cloning via reference counting
///    ```ignore
///    use bytes::Bytes;
///    pub payload: Bytes,
///    ```
///
/// 2. **`Arc<[u8]>`**: Reference-counted slice for shared ownership
///    ```ignore
///    use std::sync::Arc;
///    pub payload: Arc<[u8]>,
///    ```
///
/// 3. **Borrowed payloads with lifetimes**: For truly zero-copy deserialization
///    ```ignore
///    #[derive(Deserialize)]
///    pub struct SerializedTask<'a> {
///        #[serde(borrow)]
///        pub payload: &'a [u8],
///    }
///    ```
///
/// Trade-offs:
/// - `Vec<u8>`: Simple, owned data, but requires copying
/// - `Bytes`: Cheap cloning, but requires external dependency
/// - `Arc<[u8]>`: Cheap cloning, but atomic operations have overhead
/// - Borrowed: True zero-copy, but lifetime complexity and limited use cases
///
/// For most use cases, the current `Vec<u8>` approach provides a good balance
/// of simplicity and performance. Consider alternatives only when profiling shows
/// serialization as a bottleneck.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializedTask {
    /// Task metadata
    pub metadata: TaskMetadata,

    /// Serialized task payload
    pub payload: Vec<u8>,
}

impl SerializedTask {
    #[inline]
    pub fn new(name: String, payload: Vec<u8>) -> Self {
        Self {
            metadata: TaskMetadata::new(name),
            payload,
        }
    }

    #[inline]
    pub fn with_priority(mut self, priority: i32) -> Self {
        self.metadata.priority = priority;
        self
    }

    #[inline]
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.metadata.max_retries = max_retries;
        self
    }

    #[inline]
    pub fn with_timeout(mut self, timeout_secs: u64) -> Self {
        self.metadata.timeout_secs = Some(timeout_secs);
        self
    }

    /// Set the group ID for workflow grouping
    #[inline]
    pub fn with_group_id(mut self, group_id: Uuid) -> Self {
        self.metadata.group_id = Some(group_id);
        self
    }

    /// Set the chord ID for barrier synchronization
    #[inline]
    pub fn with_chord_id(mut self, chord_id: Uuid) -> Self {
        self.metadata.chord_id = Some(chord_id);
        self
    }

    /// Get the age of the task (time since creation)
    #[inline]
    pub fn age(&self) -> chrono::Duration {
        self.metadata.age()
    }

    /// Check if the task has expired based on its timeout
    #[inline]
    pub fn is_expired(&self) -> bool {
        self.metadata.is_expired()
    }

    /// Check if the task is in a terminal state (Success or Failure)
    #[inline]
    pub fn is_terminal(&self) -> bool {
        self.metadata.is_terminal()
    }

    /// Check if the task is in a running or active state
    #[inline]
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
    #[inline]
    pub fn has_timeout(&self) -> bool {
        self.metadata.has_timeout()
    }

    /// Check if task is part of a group
    #[inline]
    pub fn has_group_id(&self) -> bool {
        self.metadata.has_group_id()
    }

    /// Check if task is part of a chord
    #[inline]
    pub fn has_chord_id(&self) -> bool {
        self.metadata.has_chord_id()
    }

    /// Check if task has custom priority (non-zero)
    #[inline]
    pub fn has_priority(&self) -> bool {
        self.metadata.has_priority()
    }

    /// Get payload size in bytes
    #[inline]
    pub fn payload_size(&self) -> usize {
        self.payload.len()
    }

    /// Check if payload is empty
    #[inline]
    pub fn has_empty_payload(&self) -> bool {
        self.payload.is_empty()
    }

    /// Add a task dependency
    #[inline]
    pub fn with_dependency(mut self, dependency: TaskId) -> Self {
        self.metadata.dependencies.insert(dependency);
        self
    }

    /// Add multiple task dependencies
    pub fn with_dependencies(mut self, dependencies: impl IntoIterator<Item = TaskId>) -> Self {
        self.metadata.dependencies.extend(dependencies);
        self
    }

    /// Check if task has any dependencies
    #[inline]
    pub fn has_dependencies(&self) -> bool {
        self.metadata.has_dependencies()
    }

    /// Get the number of dependencies
    #[inline]
    pub fn dependency_count(&self) -> usize {
        self.metadata.dependency_count()
    }

    /// Check if a specific task is a dependency
    #[inline]
    pub fn depends_on(&self, task_id: &TaskId) -> bool {
        self.metadata.depends_on(task_id)
    }

    /// Check if task has high priority (priority > 0)
    #[inline]
    pub fn is_high_priority(&self) -> bool {
        self.metadata.is_high_priority()
    }

    /// Check if task has low priority (priority < 0)
    #[inline]
    pub fn is_low_priority(&self) -> bool {
        self.metadata.is_low_priority()
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

    #[test]
    fn test_task_dependencies() {
        let dep1 = TaskId::new_v4();
        let dep2 = TaskId::new_v4();

        let metadata = TaskMetadata::new("test_task".to_string())
            .with_dependency(dep1)
            .with_dependency(dep2);

        assert!(metadata.has_dependencies());
        assert_eq!(metadata.dependency_count(), 2);
        assert!(metadata.depends_on(&dep1));
        assert!(metadata.depends_on(&dep2));
    }

    #[test]
    fn test_task_with_dependencies() {
        let dep1 = TaskId::new_v4();
        let dep2 = TaskId::new_v4();
        let deps = vec![dep1, dep2];

        let metadata = TaskMetadata::new("test_task".to_string()).with_dependencies(deps);

        assert_eq!(metadata.dependency_count(), 2);
        assert!(metadata.depends_on(&dep1));
        assert!(metadata.depends_on(&dep2));
    }

    #[test]
    fn test_remove_dependency() {
        let dep1 = TaskId::new_v4();
        let dep2 = TaskId::new_v4();

        let mut metadata = TaskMetadata::new("test_task".to_string())
            .with_dependency(dep1)
            .with_dependency(dep2);

        assert_eq!(metadata.dependency_count(), 2);

        let removed = metadata.remove_dependency(&dep1);
        assert!(removed);
        assert_eq!(metadata.dependency_count(), 1);
        assert!(!metadata.depends_on(&dep1));
        assert!(metadata.depends_on(&dep2));
    }

    #[test]
    fn test_clear_dependencies() {
        let dep1 = TaskId::new_v4();
        let dep2 = TaskId::new_v4();

        let mut metadata = TaskMetadata::new("test_task".to_string())
            .with_dependency(dep1)
            .with_dependency(dep2);

        assert!(metadata.has_dependencies());
        metadata.clear_dependencies();
        assert!(!metadata.has_dependencies());
        assert_eq!(metadata.dependency_count(), 0);
    }

    #[test]
    fn test_serialized_task_dependencies() {
        let dep1 = TaskId::new_v4();
        let dep2 = TaskId::new_v4();

        let task = SerializedTask::new("test_task".to_string(), vec![1, 2, 3])
            .with_dependency(dep1)
            .with_dependency(dep2);

        assert!(task.has_dependencies());
        assert_eq!(task.dependency_count(), 2);
        assert!(task.depends_on(&dep1));
        assert!(task.depends_on(&dep2));
    }

    // Integration tests for full task lifecycle
    #[cfg(test)]
    mod integration_tests {
        use super::*;
        use crate::{StateHistory, TaskState};

        #[test]
        fn test_complete_task_lifecycle() {
            // Create a task
            let mut task = SerializedTask::new("process_data".to_string(), vec![1, 2, 3, 4, 5])
                .with_priority(5)
                .with_max_retries(3)
                .with_timeout(60);

            assert_eq!(task.metadata.state, TaskState::Pending);
            assert!(task.is_active());
            assert!(!task.is_terminal());

            // Simulate task progression through states
            let mut history = StateHistory::with_initial(task.metadata.state.clone());

            // Task is received by worker
            task.metadata.state = TaskState::Received;
            history.transition(task.metadata.state.clone());

            // Task is reserved
            task.metadata.state = TaskState::Reserved;
            history.transition(task.metadata.state.clone());

            // Task starts running
            task.metadata.state = TaskState::Running;
            history.transition(task.metadata.state.clone());

            // Task completes successfully
            task.metadata.state = TaskState::Succeeded(vec![10, 20, 30]);
            history.transition(task.metadata.state.clone());

            assert!(task.is_terminal());
            assert!(!task.is_active());
            assert_eq!(history.transition_count(), 4);
        }

        #[test]
        fn test_task_retry_lifecycle() {
            let mut task =
                SerializedTask::new("failing_task".to_string(), vec![1, 2, 3]).with_max_retries(3);

            let mut history = StateHistory::with_initial(TaskState::Pending);

            // First attempt fails
            task.metadata.state = TaskState::Running;
            history.transition(task.metadata.state.clone());

            task.metadata.state = TaskState::Failed("Network error".to_string());
            history.transition(task.metadata.state.clone());

            // Check if can retry
            assert!(task.metadata.state.can_retry(task.metadata.max_retries));

            // First retry
            task.metadata.state = TaskState::Retrying(1);
            history.transition(task.metadata.state.clone());
            assert_eq!(task.metadata.state.retry_count(), 1);

            task.metadata.state = TaskState::Failed("Still failing".to_string());
            history.transition(task.metadata.state.clone());

            // Second retry
            task.metadata.state = TaskState::Retrying(2);
            history.transition(task.metadata.state.clone());
            assert_eq!(task.metadata.state.retry_count(), 2);

            // Finally succeeds
            task.metadata.state = TaskState::Succeeded(vec![]);
            history.transition(task.metadata.state.clone());

            assert!(task.is_terminal());
            assert_eq!(history.transition_count(), 6);
        }

        #[test]
        fn test_task_with_dependencies_lifecycle() {
            // Create parent tasks
            let parent1_id = TaskId::new_v4();
            let parent2_id = TaskId::new_v4();

            // Create child task that depends on parents
            let child_task = SerializedTask::new("child_task".to_string(), vec![1, 2, 3])
                .with_dependency(parent1_id)
                .with_dependency(parent2_id)
                .with_priority(10);

            assert!(child_task.has_dependencies());
            assert_eq!(child_task.dependency_count(), 2);
            assert!(child_task.depends_on(&parent1_id));
            assert!(child_task.depends_on(&parent2_id));

            // Verify task properties
            assert_eq!(child_task.metadata.priority, 10);
            assert!(child_task.is_high_priority());
        }

        #[test]
        fn test_task_serialization_roundtrip() {
            let original = SerializedTask::new("test_task".to_string(), vec![1, 2, 3, 4, 5])
                .with_priority(5)
                .with_max_retries(3)
                .with_timeout(120)
                .with_dependency(TaskId::new_v4());

            // Serialize to JSON
            let json = serde_json::to_string(&original).expect("Failed to serialize");

            // Deserialize back
            let deserialized: SerializedTask =
                serde_json::from_str(&json).expect("Failed to deserialize");

            assert_eq!(deserialized.metadata.name, original.metadata.name);
            assert_eq!(deserialized.metadata.priority, original.metadata.priority);
            assert_eq!(
                deserialized.metadata.max_retries,
                original.metadata.max_retries
            );
            assert_eq!(
                deserialized.metadata.timeout_secs,
                original.metadata.timeout_secs
            );
            assert_eq!(deserialized.payload, original.payload);
            assert_eq!(deserialized.dependency_count(), original.dependency_count());
        }

        #[test]
        fn test_task_validation_lifecycle() {
            // Valid task
            let valid_task = SerializedTask::new("valid_task".to_string(), vec![1, 2, 3])
                .with_max_retries(5)
                .with_timeout(30);

            assert!(valid_task.validate().is_ok());

            // Invalid task - empty name
            let mut invalid_task = SerializedTask::new("".to_string(), vec![1, 2, 3]);
            assert!(invalid_task.metadata.validate().is_err());

            // Invalid task - excessive retries
            invalid_task =
                SerializedTask::new("task".to_string(), vec![1, 2, 3]).with_max_retries(10000);
            assert!(invalid_task.metadata.validate().is_err());

            // Invalid task - zero timeout
            let mut invalid_metadata = TaskMetadata::new("task".to_string());
            invalid_metadata.timeout_secs = Some(0);
            assert!(invalid_metadata.validate().is_err());
        }

        #[test]
        fn test_task_expiration_lifecycle() {
            // Create task with 1 second timeout
            let task =
                SerializedTask::new("expiring_task".to_string(), vec![1, 2, 3]).with_timeout(1);

            // Task should not be expired immediately
            assert!(!task.is_expired());

            // Wait for task to expire
            std::thread::sleep(std::time::Duration::from_secs(2));

            // Task should now be expired
            assert!(task.is_expired());
        }

        #[test]
        fn test_workflow_with_multiple_dependencies() {
            use crate::TaskDag;

            // Create a workflow: task1 -> task2 -> task3
            //                      \-> task4 -^
            let mut dag = TaskDag::new();

            let task1 = TaskId::new_v4();
            let task2 = TaskId::new_v4();
            let task3 = TaskId::new_v4();
            let task4 = TaskId::new_v4();

            dag.add_node(task1, "load_data");
            dag.add_node(task2, "transform_data");
            dag.add_node(task3, "save_results");
            dag.add_node(task4, "validate_data");

            // task2 depends on task1
            dag.add_dependency(task2, task1).unwrap();
            // task4 depends on task1
            dag.add_dependency(task4, task1).unwrap();
            // task3 depends on both task2 and task4
            dag.add_dependency(task3, task2).unwrap();
            dag.add_dependency(task3, task4).unwrap();

            // Validate DAG
            assert!(dag.validate().is_ok());

            // Get execution order
            let order = dag.topological_sort().unwrap();
            assert_eq!(order.len(), 4);

            // task1 should be first
            assert_eq!(order[0], task1);
            // task3 should be last
            assert_eq!(order[3], task3);
        }

        #[test]
        fn test_task_state_history_full_lifecycle() {
            let mut history = StateHistory::with_initial(TaskState::Pending);

            // Simulate complete lifecycle
            history.transition(TaskState::Received);
            history.transition(TaskState::Reserved);
            history.transition(TaskState::Running);
            history.transition(TaskState::Failed("Temporary error".to_string()));
            history.transition(TaskState::Retrying(1));
            history.transition(TaskState::Running);
            history.transition(TaskState::Succeeded(vec![1, 2, 3]));

            assert_eq!(history.transition_count(), 7);
            assert!(history.current_state().unwrap().is_terminal());

            // Check states we transitioned TO
            assert!(history.has_been_in_state("RECEIVED"));
            assert!(history.has_been_in_state("RESERVED"));
            assert!(history.has_been_in_state("RUNNING"));
            assert!(history.has_been_in_state("FAILURE"));
            assert!(history.has_been_in_state("RETRYING"));
            assert!(history.has_been_in_state("SUCCESS"));

            // Current state should be SUCCESS
            assert_eq!(history.current_state().unwrap().name(), "SUCCESS");
        }
    }
}
