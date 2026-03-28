use crate::{WorkflowRetryPolicy, WorkflowState};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Workflow checkpoint for recovery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowCheckpoint {
    /// Workflow ID
    pub workflow_id: Uuid,
    /// Checkpoint timestamp (Unix timestamp)
    pub timestamp: u64,
    /// Completed task IDs
    pub completed_tasks: Vec<Uuid>,
    /// Failed task IDs with errors
    pub failed_tasks: Vec<(Uuid, String)>,
    /// In-progress task IDs
    pub in_progress_tasks: Vec<Uuid>,
    /// Workflow state snapshot
    pub state: WorkflowState,
    /// Checkpoint version (for compatibility)
    pub version: u32,
}

impl WorkflowCheckpoint {
    /// Create a new checkpoint
    pub fn new(workflow_id: Uuid, state: WorkflowState) -> Self {
        Self {
            workflow_id,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            completed_tasks: Vec::new(),
            failed_tasks: Vec::new(),
            in_progress_tasks: Vec::new(),
            state,
            version: 1,
        }
    }

    /// Record completed task
    pub fn record_completed(&mut self, task_id: Uuid) {
        self.completed_tasks.push(task_id);
        // Remove from in-progress if present
        self.in_progress_tasks.retain(|&id| id != task_id);
    }

    /// Record failed task
    pub fn record_failed(&mut self, task_id: Uuid, error: String) {
        self.failed_tasks.push((task_id, error));
        // Remove from in-progress if present
        self.in_progress_tasks.retain(|&id| id != task_id);
    }

    /// Record in-progress task
    pub fn record_in_progress(&mut self, task_id: Uuid) {
        if !self.in_progress_tasks.contains(&task_id) {
            self.in_progress_tasks.push(task_id);
        }
    }

    /// Check if task is completed
    pub fn is_completed(&self, task_id: &Uuid) -> bool {
        self.completed_tasks.contains(task_id)
    }

    /// Check if task failed
    pub fn is_failed(&self, task_id: &Uuid) -> bool {
        self.failed_tasks.iter().any(|(id, _)| id == task_id)
    }

    /// Get tasks that need to be retried (in-progress at checkpoint)
    pub fn tasks_to_retry(&self) -> &[Uuid] {
        &self.in_progress_tasks
    }

    /// Serialize checkpoint to JSON
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Deserialize checkpoint from JSON
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

impl std::fmt::Display for WorkflowCheckpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WorkflowCheckpoint[id={}, completed={}, failed={}, in_progress={}]",
            self.workflow_id,
            self.completed_tasks.len(),
            self.failed_tasks.len(),
            self.in_progress_tasks.len()
        )
    }
}

/// Workflow recovery policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowRecoveryPolicy {
    /// Whether to enable automatic recovery
    pub auto_recovery: bool,
    /// Whether to resume from last checkpoint
    pub resume_from_checkpoint: bool,
    /// Whether to replay failed stages
    pub replay_failed: bool,
    /// Maximum age of checkpoint to use (seconds)
    pub max_checkpoint_age: Option<u64>,
    /// Retry policy for recovered tasks
    pub retry_policy: Option<WorkflowRetryPolicy>,
}

impl WorkflowRecoveryPolicy {
    /// Create a new recovery policy with auto-recovery enabled
    pub fn auto_recover() -> Self {
        Self {
            auto_recovery: true,
            resume_from_checkpoint: true,
            replay_failed: true,
            max_checkpoint_age: Some(3600), // 1 hour
            retry_policy: None,
        }
    }

    /// Disable auto-recovery
    pub fn manual() -> Self {
        Self {
            auto_recovery: false,
            resume_from_checkpoint: true,
            replay_failed: false,
            max_checkpoint_age: None,
            retry_policy: None,
        }
    }

    /// Set maximum checkpoint age
    pub fn with_max_checkpoint_age(mut self, seconds: u64) -> Self {
        self.max_checkpoint_age = Some(seconds);
        self
    }

    /// Set retry policy for recovered tasks
    pub fn with_retry_policy(mut self, policy: WorkflowRetryPolicy) -> Self {
        self.retry_policy = Some(policy);
        self
    }

    /// Check if checkpoint is valid based on age
    pub fn is_checkpoint_valid(&self, checkpoint: &WorkflowCheckpoint) -> bool {
        if let Some(max_age) = self.max_checkpoint_age {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let age = now.saturating_sub(checkpoint.timestamp);
            age <= max_age
        } else {
            true
        }
    }
}

impl std::fmt::Display for WorkflowRecoveryPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WorkflowRecoveryPolicy[")?;
        if self.auto_recovery {
            write!(f, "auto")?;
        } else {
            write!(f, "manual")?;
        }
        if self.resume_from_checkpoint {
            write!(f, " resume")?;
        }
        if self.replay_failed {
            write!(f, " replay_failed")?;
        }
        write!(f, "]")
    }
}

// ============================================================================
// State Versioning
// ============================================================================

/// State version for workflow state evolution
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateVersion {
    /// Major version (breaking changes)
    pub major: u32,
    /// Minor version (backward-compatible changes)
    pub minor: u32,
    /// Patch version (bug fixes)
    pub patch: u32,
}

impl StateVersion {
    /// Create a new state version
    pub fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    /// Current version
    pub fn current() -> Self {
        Self {
            major: 1,
            minor: 0,
            patch: 0,
        }
    }

    /// Check if this version can be migrated to another version
    /// (same major version, this minor version <= target minor version)
    pub fn is_compatible(&self, other: &StateVersion) -> bool {
        self.major == other.major && self.minor <= other.minor
    }
}

impl std::fmt::Display for StateVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// State migration error
#[derive(Debug, Clone)]
pub enum StateMigrationError {
    /// Incompatible version
    IncompatibleVersion {
        from: StateVersion,
        to: StateVersion,
    },
    /// Migration failed
    MigrationFailed(String),
    /// Unsupported version
    UnsupportedVersion(StateVersion),
}

impl std::fmt::Display for StateMigrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IncompatibleVersion { from, to } => {
                write!(f, "Incompatible state version: {} -> {}", from, to)
            }
            Self::MigrationFailed(msg) => write!(f, "State migration failed: {}", msg),
            Self::UnsupportedVersion(version) => {
                write!(f, "Unsupported state version: {}", version)
            }
        }
    }
}

impl std::error::Error for StateMigrationError {}

/// State migration strategy
pub trait StateMigration {
    /// Migrate state from one version to another
    fn migrate(&self, from: StateVersion, to: StateVersion) -> Result<(), StateMigrationError>;
}

/// Versioned workflow state with migration support
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionedWorkflowState {
    /// State version
    pub version: StateVersion,
    /// Workflow state
    pub state: WorkflowState,
    /// Migration history
    pub migration_history: Vec<(StateVersion, StateVersion, u64)>, // (from, to, timestamp)
}

impl VersionedWorkflowState {
    /// Create a new versioned state
    pub fn new(state: WorkflowState) -> Self {
        Self {
            version: StateVersion::current(),
            state,
            migration_history: Vec::new(),
        }
    }

    /// Migrate to a new version
    pub fn migrate_to(&mut self, target: StateVersion) -> Result<(), StateMigrationError> {
        if self.version == target {
            return Ok(());
        }

        if !self.version.is_compatible(&target) {
            return Err(StateMigrationError::IncompatibleVersion {
                from: self.version,
                to: target,
            });
        }

        // Record migration
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.migration_history
            .push((self.version, target, timestamp));
        self.version = target;

        Ok(())
    }

    /// Check if state can be migrated to target version
    pub fn can_migrate_to(&self, target: &StateVersion) -> bool {
        self.version.is_compatible(target)
    }

    /// Get migration history
    pub fn get_migration_history(&self) -> &[(StateVersion, StateVersion, u64)] {
        &self.migration_history
    }
}
