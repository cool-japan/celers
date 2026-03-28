use crate::{Condition, Signature};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Error handling strategy for workflows
///
/// Determines how the workflow should behave when a task fails.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum ErrorStrategy {
    /// Stop the workflow on first error (default)
    #[default]
    StopOnError,

    /// Continue even if some tasks fail
    ContinueOnError,

    /// Retry the failed task a number of times before failing
    RetryOnError {
        /// Maximum number of retries
        max_retries: u32,
        /// Delay between retries in seconds
        delay: Option<u64>,
    },

    /// Execute a fallback task on error
    Fallback {
        /// Fallback task to execute
        fallback: Signature,
    },

    /// Execute an error handler task that receives the error info
    ErrorHandler {
        /// Error handler task name
        handler: Signature,
    },
}

impl ErrorStrategy {
    /// Create a stop-on-error strategy
    pub fn stop() -> Self {
        Self::StopOnError
    }

    /// Create a continue-on-error strategy
    pub fn continue_on_error() -> Self {
        Self::ContinueOnError
    }

    /// Create a retry strategy
    pub fn retry(max_retries: u32) -> Self {
        Self::RetryOnError {
            max_retries,
            delay: None,
        }
    }

    /// Create a retry strategy with delay
    pub fn retry_with_delay(max_retries: u32, delay: u64) -> Self {
        Self::RetryOnError {
            max_retries,
            delay: Some(delay),
        }
    }

    /// Create a fallback strategy
    pub fn fallback(task: Signature) -> Self {
        Self::Fallback { fallback: task }
    }

    /// Create an error handler strategy
    pub fn error_handler(handler: Signature) -> Self {
        Self::ErrorHandler { handler }
    }

    /// Check if this strategy allows continuing on error
    pub fn allows_continue(&self) -> bool {
        !matches!(self, Self::StopOnError)
    }
}

impl std::fmt::Display for ErrorStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StopOnError => write!(f, "StopOnError"),
            Self::ContinueOnError => write!(f, "ContinueOnError"),
            Self::RetryOnError { max_retries, delay } => {
                if let Some(d) = delay {
                    write!(f, "RetryOnError({} times, {}s delay)", max_retries, d)
                } else {
                    write!(f, "RetryOnError({} times)", max_retries)
                }
            }
            Self::Fallback { fallback } => write!(f, "Fallback({})", fallback.task),
            Self::ErrorHandler { handler } => write!(f, "ErrorHandler({})", handler.task),
        }
    }
}

// ============================================================================
// Workflow Cancellation
// ============================================================================

/// Cancellation token for workflow cancellation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancellationToken {
    /// Workflow ID
    pub workflow_id: Uuid,
    /// Cancellation reason
    pub reason: Option<String>,
    /// Whether to cancel entire tree (including sub-workflows)
    pub cancel_tree: bool,
    /// Whether to cancel only specific branch
    pub branch_id: Option<Uuid>,
}

impl CancellationToken {
    /// Create a new cancellation token for a workflow
    pub fn new(workflow_id: Uuid) -> Self {
        Self {
            workflow_id,
            reason: None,
            cancel_tree: false,
            branch_id: None,
        }
    }

    /// Set cancellation reason
    pub fn with_reason(mut self, reason: String) -> Self {
        self.reason = Some(reason);
        self
    }

    /// Cancel entire workflow tree
    pub fn cancel_tree(mut self) -> Self {
        self.cancel_tree = true;
        self
    }

    /// Cancel only specific branch
    pub fn cancel_branch(mut self, branch_id: Uuid) -> Self {
        self.branch_id = Some(branch_id);
        self
    }
}

impl std::fmt::Display for CancellationToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CancellationToken[workflow={}]", self.workflow_id)?;
        if let Some(ref reason) = self.reason {
            write!(f, " reason={}", reason)?;
        }
        if self.cancel_tree {
            write!(f, " (tree)")?;
        }
        if let Some(branch) = self.branch_id {
            write!(f, " branch={}", branch)?;
        }
        Ok(())
    }
}

// ============================================================================
// Workflow Retry Policies
// ============================================================================

/// Workflow-level retry policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowRetryPolicy {
    /// Maximum number of retries for entire workflow
    pub max_retries: u32,
    /// Retry only failed branches
    pub retry_failed_only: bool,
    /// Exponential backoff factor
    pub backoff_factor: Option<f64>,
    /// Maximum backoff delay in seconds
    pub max_backoff: Option<u64>,
    /// Initial retry delay in seconds
    pub initial_delay: Option<u64>,
}

impl WorkflowRetryPolicy {
    /// Create a new retry policy
    pub fn new(max_retries: u32) -> Self {
        Self {
            max_retries,
            retry_failed_only: false,
            backoff_factor: None,
            max_backoff: None,
            initial_delay: None,
        }
    }

    /// Retry only failed branches
    pub fn failed_only(mut self) -> Self {
        self.retry_failed_only = true;
        self
    }

    /// Set exponential backoff
    pub fn with_backoff(mut self, factor: f64, max_delay: u64) -> Self {
        self.backoff_factor = Some(factor);
        self.max_backoff = Some(max_delay);
        self
    }

    /// Set initial delay
    pub fn with_initial_delay(mut self, delay: u64) -> Self {
        self.initial_delay = Some(delay);
        self
    }

    /// Calculate delay for retry attempt
    pub fn calculate_delay(&self, attempt: u32) -> u64 {
        let base_delay = self.initial_delay.unwrap_or(1);

        if let Some(factor) = self.backoff_factor {
            let delay = (base_delay as f64) * factor.powi(attempt as i32);
            let max = self.max_backoff.unwrap_or(300);
            delay.min(max as f64) as u64
        } else {
            base_delay
        }
    }
}

impl std::fmt::Display for WorkflowRetryPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WorkflowRetryPolicy[max_retries={}]", self.max_retries)?;
        if self.retry_failed_only {
            write!(f, " (failed_only)")?;
        }
        if let Some(factor) = self.backoff_factor {
            write!(f, " backoff={}", factor)?;
        }
        Ok(())
    }
}

// ============================================================================
// Workflow Timeout
// ============================================================================

/// Workflow-level timeout configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowTimeout {
    /// Global workflow timeout in seconds
    pub total_timeout: Option<u64>,
    /// Per-stage timeout in seconds
    pub stage_timeout: Option<u64>,
    /// Timeout escalation - what to do on timeout
    pub escalation: TimeoutEscalation,
}

/// Timeout escalation strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TimeoutEscalation {
    /// Cancel the workflow
    Cancel,
    /// Fail the workflow
    Fail,
    /// Continue with partial results
    ContinuePartial,
}

impl WorkflowTimeout {
    /// Create a new timeout configuration
    pub fn new(total_timeout: u64) -> Self {
        Self {
            total_timeout: Some(total_timeout),
            stage_timeout: None,
            escalation: TimeoutEscalation::Cancel,
        }
    }

    /// Set per-stage timeout
    pub fn with_stage_timeout(mut self, timeout: u64) -> Self {
        self.stage_timeout = Some(timeout);
        self
    }

    /// Set escalation strategy
    pub fn with_escalation(mut self, escalation: TimeoutEscalation) -> Self {
        self.escalation = escalation;
        self
    }
}

impl std::fmt::Display for WorkflowTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WorkflowTimeout[")?;
        if let Some(total) = self.total_timeout {
            write!(f, "total={}s", total)?;
        }
        if let Some(stage) = self.stage_timeout {
            write!(f, " stage={}s", stage)?;
        }
        write!(f, " escalation={:?}]", self.escalation)
    }
}

// ============================================================================
// Workflow Loops
// ============================================================================

/// For-each loop over a collection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForEach {
    /// Task to execute for each item
    pub task: Signature,
    /// Collection to iterate over
    pub items: Vec<serde_json::Value>,
    /// Maximum parallel execution
    pub concurrency: Option<usize>,
}

impl ForEach {
    /// Create a new for-each loop
    pub fn new(task: Signature, items: Vec<serde_json::Value>) -> Self {
        Self {
            task,
            items,
            concurrency: None,
        }
    }

    /// Set maximum concurrent executions
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = Some(concurrency);
        self
    }

    /// Check if loop is empty
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Get number of iterations
    pub fn len(&self) -> usize {
        self.items.len()
    }
}

impl std::fmt::Display for ForEach {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ForEach[task={}, {} items]", self.task.task, self.len())?;
        if let Some(conc) = self.concurrency {
            write!(f, " concurrency={}", conc)?;
        }
        Ok(())
    }
}

/// While loop with condition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhileLoop {
    /// Condition to evaluate
    pub condition: Condition,
    /// Task to execute while condition is true
    pub body: Signature,
    /// Maximum iterations (safety limit)
    pub max_iterations: Option<u32>,
}

impl WhileLoop {
    /// Create a new while loop
    pub fn new(condition: Condition, body: Signature) -> Self {
        Self {
            condition,
            body,
            max_iterations: Some(1000), // Default safety limit
        }
    }

    /// Set maximum iterations
    pub fn with_max_iterations(mut self, max: u32) -> Self {
        self.max_iterations = Some(max);
        self
    }

    /// Remove iteration limit (use with caution!)
    pub fn unlimited(mut self) -> Self {
        self.max_iterations = None;
        self
    }
}

impl std::fmt::Display for WhileLoop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "While[{} -> {}]", self.condition, self.body.task)?;
        if let Some(max) = self.max_iterations {
            write!(f, " max={}", max)?;
        }
        Ok(())
    }
}

// ============================================================================
// Workflow State Tracking
// ============================================================================

/// Workflow execution state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowState {
    /// Workflow ID
    pub workflow_id: Uuid,
    /// Current status
    pub status: WorkflowStatus,
    /// Total tasks in workflow
    pub total_tasks: usize,
    /// Completed tasks
    pub completed_tasks: usize,
    /// Failed tasks
    pub failed_tasks: usize,
    /// Start time (Unix timestamp)
    pub start_time: Option<u64>,
    /// End time (Unix timestamp)
    pub end_time: Option<u64>,
    /// Current stage
    pub current_stage: Option<String>,
    /// Intermediate results
    pub intermediate_results: HashMap<String, serde_json::Value>,
}

/// Workflow status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WorkflowStatus {
    /// Workflow is pending
    Pending,
    /// Workflow is running
    Running,
    /// Workflow completed successfully
    Success,
    /// Workflow failed
    Failed,
    /// Workflow was cancelled
    Cancelled,
    /// Workflow is paused
    Paused,
}

impl WorkflowState {
    /// Create a new workflow state
    pub fn new(workflow_id: Uuid, total_tasks: usize) -> Self {
        Self {
            workflow_id,
            status: WorkflowStatus::Pending,
            total_tasks,
            completed_tasks: 0,
            failed_tasks: 0,
            start_time: None,
            end_time: None,
            current_stage: None,
            intermediate_results: HashMap::new(),
        }
    }

    /// Calculate progress percentage (0-100)
    pub fn progress(&self) -> f64 {
        if self.total_tasks == 0 {
            return 100.0;
        }
        (self.completed_tasks as f64 / self.total_tasks as f64) * 100.0
    }

    /// Check if workflow is complete
    pub fn is_complete(&self) -> bool {
        matches!(
            self.status,
            WorkflowStatus::Success | WorkflowStatus::Failed | WorkflowStatus::Cancelled
        )
    }

    /// Mark task as completed
    pub fn mark_completed(&mut self) {
        self.completed_tasks += 1;
    }

    /// Mark task as failed
    pub fn mark_failed(&mut self) {
        self.failed_tasks += 1;
    }

    /// Set intermediate result
    pub fn set_result(&mut self, key: String, value: serde_json::Value) {
        self.intermediate_results.insert(key, value);
    }

    /// Get intermediate result
    pub fn get_result(&self, key: &str) -> Option<&serde_json::Value> {
        self.intermediate_results.get(key)
    }
}

impl std::fmt::Display for WorkflowState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WorkflowState[id={}, status={:?}, progress={:.1}%]",
            self.workflow_id,
            self.status,
            self.progress()
        )?;
        if self.failed_tasks > 0 {
            write!(f, " failed={}", self.failed_tasks)?;
        }
        Ok(())
    }
}
