use crate::{Chain, Chord, CompensationWorkflow, Group, Signature};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub struct Saga {
    /// Compensation workflow
    pub workflow: CompensationWorkflow,
    /// Isolation level
    pub isolation: SagaIsolation,
}

/// Saga isolation level
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SagaIsolation {
    /// Read uncommitted (no isolation)
    ReadUncommitted,
    /// Read committed (default)
    ReadCommitted,
    /// Serializable (full isolation)
    Serializable,
}

impl Saga {
    /// Create a new saga
    pub fn new(workflow: CompensationWorkflow) -> Self {
        Self {
            workflow,
            isolation: SagaIsolation::ReadCommitted,
        }
    }

    /// Set isolation level
    pub fn with_isolation(mut self, isolation: SagaIsolation) -> Self {
        self.isolation = isolation;
        self
    }
}

impl std::fmt::Display for Saga {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Saga[{} steps, isolation={:?}]",
            self.workflow.len(),
            self.isolation
        )
    }
}

// ============================================================================
// Advanced Workflow Patterns
// ============================================================================

/// Scatter-gather pattern: distribute work, collect results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScatterGather {
    /// Scatter task (distributes work)
    pub scatter: Signature,
    /// Worker tasks (process items)
    pub workers: Vec<Signature>,
    /// Gather task (collects results)
    pub gather: Signature,
    /// Timeout for gathering
    pub timeout: Option<u64>,
}

impl ScatterGather {
    /// Create a new scatter-gather pattern
    pub fn new(scatter: Signature, workers: Vec<Signature>, gather: Signature) -> Self {
        Self {
            scatter,
            workers,
            gather,
            timeout: None,
        }
    }

    /// Set gather timeout
    pub fn with_timeout(mut self, timeout: u64) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

impl std::fmt::Display for ScatterGather {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ScatterGather[scatter={}, {} workers, gather={}]",
            self.scatter.task,
            self.workers.len(),
            self.gather.task
        )
    }
}

/// Pipeline pattern: streaming data through stages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pipeline {
    /// Pipeline stages
    pub stages: Vec<Signature>,
    /// Buffer size between stages
    pub buffer_size: Option<usize>,
}

impl Pipeline {
    /// Create a new pipeline
    pub fn new() -> Self {
        Self {
            stages: Vec::new(),
            buffer_size: None,
        }
    }

    /// Add a stage
    pub fn stage(mut self, stage: Signature) -> Self {
        self.stages.push(stage);
        self
    }

    /// Set buffer size
    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = Some(size);
        self
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.stages.is_empty()
    }

    /// Get number of stages
    pub fn len(&self) -> usize {
        self.stages.len()
    }
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for Pipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Pipeline[{} stages]", self.stages.len())?;
        if let Some(buf) = self.buffer_size {
            write!(f, " buffer={}", buf)?;
        }
        Ok(())
    }
}

/// Fan-out pattern: broadcast to multiple consumers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FanOut {
    /// Source task
    pub source: Signature,
    /// Consumer tasks
    pub consumers: Vec<Signature>,
}

impl FanOut {
    /// Create a new fan-out pattern
    pub fn new(source: Signature) -> Self {
        Self {
            source,
            consumers: Vec::new(),
        }
    }

    /// Add a consumer
    pub fn consumer(mut self, consumer: Signature) -> Self {
        self.consumers.push(consumer);
        self
    }

    /// Get number of consumers
    pub fn len(&self) -> usize {
        self.consumers.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.consumers.is_empty()
    }
}

impl std::fmt::Display for FanOut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FanOut[source={}, {} consumers]",
            self.source.task,
            self.consumers.len()
        )
    }
}

/// Fan-in pattern: collect from multiple sources
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FanIn {
    /// Source tasks
    pub sources: Vec<Signature>,
    /// Aggregator task
    pub aggregator: Signature,
}

impl FanIn {
    /// Create a new fan-in pattern
    pub fn new(aggregator: Signature) -> Self {
        Self {
            sources: Vec::new(),
            aggregator,
        }
    }

    /// Add a source
    pub fn source(mut self, source: Signature) -> Self {
        self.sources.push(source);
        self
    }

    /// Get number of sources
    pub fn len(&self) -> usize {
        self.sources.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }
}

impl std::fmt::Display for FanIn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FanIn[{} sources, aggregator={}]",
            self.sources.len(),
            self.aggregator.task
        )
    }
}

// ============================================================================
// Workflow Validation and Dry-Run
// ============================================================================

/// Workflow validation result
#[derive(Debug, Clone)]
pub struct ValidationResult {
    /// Whether workflow is valid
    pub valid: bool,
    /// Validation errors
    pub errors: Vec<String>,
    /// Validation warnings
    pub warnings: Vec<String>,
}

impl ValidationResult {
    /// Create a valid result
    pub fn valid() -> Self {
        Self {
            valid: true,
            errors: Vec::new(),
            warnings: Vec::new(),
        }
    }

    /// Create an invalid result
    pub fn invalid(error: impl Into<String>) -> Self {
        Self {
            valid: false,
            errors: vec![error.into()],
            warnings: Vec::new(),
        }
    }

    /// Add an error
    pub fn add_error(&mut self, error: impl Into<String>) {
        self.errors.push(error.into());
        self.valid = false;
    }

    /// Add a warning
    pub fn add_warning(&mut self, warning: impl Into<String>) {
        self.warnings.push(warning.into());
    }
}

impl std::fmt::Display for ValidationResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.valid {
            write!(f, "Valid")?;
            if !self.warnings.is_empty() {
                write!(f, " ({} warnings)", self.warnings.len())?;
            }
        } else {
            write!(f, "Invalid ({} errors)", self.errors.len())?;
        }
        Ok(())
    }
}

/// Workflow validator trait
pub trait WorkflowValidator {
    /// Validate workflow structure
    fn validate(&self) -> ValidationResult;
}

impl WorkflowValidator for Chain {
    fn validate(&self) -> ValidationResult {
        let mut result = ValidationResult::valid();

        if self.is_empty() {
            result.add_error("Chain cannot be empty");
        }

        if self.len() > 100 {
            result.add_warning(format!(
                "Chain has {} tasks, which may be inefficient",
                self.len()
            ));
        }

        result
    }
}

impl WorkflowValidator for Group {
    fn validate(&self) -> ValidationResult {
        let mut result = ValidationResult::valid();

        if self.is_empty() {
            result.add_error("Group cannot be empty");
        }

        if self.len() > 1000 {
            result.add_warning(format!(
                "Group has {} tasks, which may overwhelm workers",
                self.len()
            ));
        }

        result
    }
}

impl WorkflowValidator for Chord {
    fn validate(&self) -> ValidationResult {
        let mut result = ValidationResult::valid();

        if self.header.is_empty() {
            result.add_error("Chord header cannot be empty");
        }

        result
    }
}

// ============================================================================
// Loop Control
// ============================================================================

/// Loop control for break/continue operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LoopControl {
    /// Continue to next iteration
    Continue,
    /// Break out of loop
    Break,
    /// Break with result value
    BreakWith { value: serde_json::Value },
}

impl LoopControl {
    /// Create a continue control
    pub fn continue_loop() -> Self {
        Self::Continue
    }

    /// Create a break control
    pub fn break_loop() -> Self {
        Self::Break
    }

    /// Create a break with value
    pub fn break_with(value: serde_json::Value) -> Self {
        Self::BreakWith { value }
    }
}

impl std::fmt::Display for LoopControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Continue => write!(f, "Continue"),
            Self::Break => write!(f, "Break"),
            Self::BreakWith { .. } => write!(f, "BreakWith"),
        }
    }
}

// ============================================================================
// Error Propagation Control
// ============================================================================

/// Error propagation mode for workflows
///
/// Controls how errors are handled and propagated in workflow execution.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub enum ErrorPropagationMode {
    /// Stop on first error (default)
    #[default]
    StopOnFirstError,

    /// Continue execution, collect all errors
    ContinueOnError,

    /// Partial failure handling - continue if threshold not exceeded
    PartialFailure {
        /// Maximum number of failed tasks before stopping
        max_failures: usize,
        /// Maximum failure percentage (0.0-1.0) before stopping
        max_failure_rate: Option<f64>,
    },
}

impl ErrorPropagationMode {
    /// Create a partial failure mode
    pub fn partial_failure(max_failures: usize) -> Self {
        Self::PartialFailure {
            max_failures,
            max_failure_rate: None,
        }
    }

    /// Create a partial failure mode with rate threshold
    pub fn partial_failure_with_rate(max_failures: usize, max_rate: f64) -> Self {
        Self::PartialFailure {
            max_failures,
            max_failure_rate: Some(max_rate),
        }
    }

    /// Check if mode allows continuing after error
    pub fn allows_continue(&self) -> bool {
        !matches!(self, Self::StopOnFirstError)
    }
}

impl std::fmt::Display for ErrorPropagationMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StopOnFirstError => write!(f, "StopOnFirstError"),
            Self::ContinueOnError => write!(f, "ContinueOnError"),
            Self::PartialFailure {
                max_failures,
                max_failure_rate,
            } => {
                write!(f, "PartialFailure(max={})", max_failures)?;
                if let Some(rate) = max_failure_rate {
                    write!(f, " rate={:.1}%", rate * 100.0)?;
                }
                Ok(())
            }
        }
    }
}

/// Tracks partial failure information for workflows
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartialFailureTracker {
    /// Total number of tasks
    pub total_tasks: usize,
    /// Number of successful tasks
    pub successful_tasks: usize,
    /// Number of failed tasks
    pub failed_tasks: usize,
    /// Task IDs that succeeded
    pub successful_task_ids: Vec<Uuid>,
    /// Task IDs that failed with error messages
    pub failed_task_ids: Vec<(Uuid, String)>,
}

impl PartialFailureTracker {
    /// Create a new partial failure tracker
    pub fn new(total_tasks: usize) -> Self {
        Self {
            total_tasks,
            successful_tasks: 0,
            failed_tasks: 0,
            successful_task_ids: Vec::new(),
            failed_task_ids: Vec::new(),
        }
    }

    /// Record a successful task
    pub fn record_success(&mut self, task_id: Uuid) {
        self.successful_tasks += 1;
        self.successful_task_ids.push(task_id);
    }

    /// Record a failed task
    pub fn record_failure(&mut self, task_id: Uuid, error: String) {
        self.failed_tasks += 1;
        self.failed_task_ids.push((task_id, error));
    }

    /// Calculate failure rate (0.0-1.0)
    pub fn failure_rate(&self) -> f64 {
        if self.total_tasks == 0 {
            return 0.0;
        }
        self.failed_tasks as f64 / self.total_tasks as f64
    }

    /// Calculate success rate (0.0-1.0)
    pub fn success_rate(&self) -> f64 {
        if self.total_tasks == 0 {
            return 1.0;
        }
        self.successful_tasks as f64 / self.total_tasks as f64
    }

    /// Check if failure threshold exceeded
    pub fn exceeds_threshold(&self, mode: &ErrorPropagationMode) -> bool {
        match mode {
            ErrorPropagationMode::StopOnFirstError => self.failed_tasks > 0,
            ErrorPropagationMode::ContinueOnError => false,
            ErrorPropagationMode::PartialFailure {
                max_failures,
                max_failure_rate,
            } => {
                if self.failed_tasks >= *max_failures {
                    return true;
                }
                if let Some(rate) = max_failure_rate {
                    if self.failure_rate() > *rate {
                        return true;
                    }
                }
                false
            }
        }
    }

    /// Check if workflow should continue
    pub fn should_continue(&self, mode: &ErrorPropagationMode) -> bool {
        !self.exceeds_threshold(mode)
    }
}

impl std::fmt::Display for PartialFailureTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PartialFailureTracker[success={}/{}, failed={}, rate={:.1}%]",
            self.successful_tasks,
            self.total_tasks,
            self.failed_tasks,
            self.failure_rate() * 100.0
        )
    }
}

// ============================================================================
// Sub-Workflow Isolation
// ============================================================================

/// Isolation level for sub-workflows
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub enum IsolationLevel {
    /// No isolation - sub-workflow shares parent context
    #[default]
    None,

    /// Resource isolation - separate resource limits
    Resource {
        /// Maximum memory in MB
        max_memory_mb: Option<u64>,
        /// Maximum CPU percentage
        max_cpu_percent: Option<u8>,
    },

    /// Error isolation - errors don't propagate to parent
    Error,

    /// Full isolation - separate context, resources, and errors
    Full {
        /// Maximum memory in MB
        max_memory_mb: Option<u64>,
        /// Maximum CPU percentage
        max_cpu_percent: Option<u8>,
    },
}

impl IsolationLevel {
    /// Create resource isolation
    pub fn resource(max_memory_mb: u64) -> Self {
        Self::Resource {
            max_memory_mb: Some(max_memory_mb),
            max_cpu_percent: None,
        }
    }

    /// Create full isolation
    pub fn full(max_memory_mb: u64) -> Self {
        Self::Full {
            max_memory_mb: Some(max_memory_mb),
            max_cpu_percent: None,
        }
    }

    /// Check if isolation includes resource limits
    pub fn has_resource_limits(&self) -> bool {
        matches!(self, Self::Resource { .. } | Self::Full { .. })
    }

    /// Check if isolation includes error boundaries
    pub fn has_error_isolation(&self) -> bool {
        matches!(self, Self::Error | Self::Full { .. })
    }
}

impl std::fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "None"),
            Self::Resource {
                max_memory_mb,
                max_cpu_percent,
            } => {
                write!(f, "Resource(")?;
                if let Some(mem) = max_memory_mb {
                    write!(f, "mem={}MB", mem)?;
                }
                if let Some(cpu) = max_cpu_percent {
                    write!(f, " cpu={}%", cpu)?;
                }
                write!(f, ")")
            }
            Self::Error => write!(f, "Error"),
            Self::Full {
                max_memory_mb,
                max_cpu_percent,
            } => {
                write!(f, "Full(")?;
                if let Some(mem) = max_memory_mb {
                    write!(f, "mem={}MB", mem)?;
                }
                if let Some(cpu) = max_cpu_percent {
                    write!(f, " cpu={}%", cpu)?;
                }
                write!(f, ")")
            }
        }
    }
}

/// Sub-workflow isolation context
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubWorkflowIsolation {
    /// Sub-workflow ID
    pub workflow_id: Uuid,
    /// Parent workflow ID
    pub parent_workflow_id: Option<Uuid>,
    /// Isolation level
    pub isolation_level: IsolationLevel,
    /// Whether errors should propagate to parent
    pub propagate_errors: bool,
    /// Whether cancellation should propagate to parent
    pub propagate_cancellation: bool,
}

impl SubWorkflowIsolation {
    /// Create a new sub-workflow isolation context
    pub fn new(workflow_id: Uuid, isolation_level: IsolationLevel) -> Self {
        Self {
            workflow_id,
            parent_workflow_id: None,
            isolation_level,
            propagate_errors: true,
            propagate_cancellation: true,
        }
    }

    /// Set parent workflow ID
    pub fn with_parent(mut self, parent_id: Uuid) -> Self {
        self.parent_workflow_id = Some(parent_id);
        self
    }

    /// Disable error propagation
    pub fn no_error_propagation(mut self) -> Self {
        self.propagate_errors = false;
        self
    }

    /// Disable cancellation propagation
    pub fn no_cancellation_propagation(mut self) -> Self {
        self.propagate_cancellation = false;
        self
    }
}

impl std::fmt::Display for SubWorkflowIsolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SubWorkflowIsolation[id={}, level={}, errors={}, cancel={}]",
            self.workflow_id,
            self.isolation_level,
            self.propagate_errors,
            self.propagate_cancellation
        )
    }
}
