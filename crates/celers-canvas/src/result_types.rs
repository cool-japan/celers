use crate::{Condition, Signature};
use serde::{Deserialize, Serialize};

pub struct NamedOutput {
    /// Output name
    pub name: String,
    /// Output value
    pub value: serde_json::Value,
    /// Source task
    pub source: Option<String>,
}

impl NamedOutput {
    /// Create a new named output
    pub fn new(name: impl Into<String>, value: serde_json::Value) -> Self {
        Self {
            name: name.into(),
            value,
            source: None,
        }
    }

    /// Set source task
    pub fn with_source(mut self, source: impl Into<String>) -> Self {
        self.source = Some(source.into());
        self
    }
}

/// Result transformation function
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResultTransform {
    /// Extract a field from result
    Extract { field: String },
    /// Map result through a task
    Map { task: Box<Signature> },
    /// Filter result based on condition
    Filter { condition: Condition },
    /// Aggregate multiple results
    Aggregate { strategy: AggregationStrategy },
}

/// Aggregation strategy for combining results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggregationStrategy {
    /// Sum numeric results
    Sum,
    /// Average numeric results
    Average,
    /// Concatenate arrays
    Concat,
    /// Merge objects
    Merge,
    /// Take first non-null result
    Coalesce,
    /// Custom aggregation task
    Custom { task: Box<Signature> },
}

impl std::fmt::Display for ResultTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Extract { field } => write!(f, "Extract[{}]", field),
            Self::Map { task } => write!(f, "Map[{}]", task.task),
            Self::Filter { condition } => write!(f, "Filter[{}]", condition),
            Self::Aggregate { strategy } => write!(f, "Aggregate[{:?}]", strategy),
        }
    }
}

// ============================================================================
// Result Caching
// ============================================================================

/// Result cache configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultCache {
    /// Cache key
    pub key: String,
    /// Cache policy
    pub policy: CachePolicy,
    /// Time-to-live in seconds
    pub ttl: Option<u64>,
}

/// Cache policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CachePolicy {
    /// Always cache results
    Always,
    /// Cache only successful results
    OnSuccess,
    /// Cache based on custom condition
    Conditional { condition: Condition },
    /// Never cache (useful for overriding)
    Never,
}

impl ResultCache {
    /// Create a new cache configuration
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            policy: CachePolicy::OnSuccess,
            ttl: None,
        }
    }

    /// Set cache policy
    pub fn with_policy(mut self, policy: CachePolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Set TTL in seconds
    pub fn with_ttl(mut self, ttl: u64) -> Self {
        self.ttl = Some(ttl);
        self
    }
}

impl std::fmt::Display for ResultCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Cache[key={}]", self.key)?;
        if let Some(ttl) = self.ttl {
            write!(f, " ttl={}s", ttl)?;
        }
        Ok(())
    }
}

// ============================================================================
// Workflow Error Handlers
// ============================================================================

/// Workflow-level error handler
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowErrorHandler {
    /// Error handler task
    pub handler: Signature,
    /// Error types to handle (empty = handle all)
    pub error_types: Vec<String>,
    /// Whether to suppress the error after handling
    pub suppress: bool,
}

impl WorkflowErrorHandler {
    /// Create a new error handler
    pub fn new(handler: Signature) -> Self {
        Self {
            handler,
            error_types: Vec::new(),
            suppress: false,
        }
    }

    /// Handle specific error types
    pub fn for_errors(mut self, error_types: Vec<String>) -> Self {
        self.error_types = error_types;
        self
    }

    /// Suppress error after handling
    pub fn suppress_error(mut self) -> Self {
        self.suppress = true;
        self
    }
}

impl std::fmt::Display for WorkflowErrorHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ErrorHandler[{}]", self.handler.task)?;
        if self.suppress {
            write!(f, " (suppress)")?;
        }
        Ok(())
    }
}

// ============================================================================
// Compensation Workflows (Saga Pattern)
// ============================================================================

/// Compensation workflow for rollback
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompensationWorkflow {
    /// Forward actions
    pub forward: Vec<Signature>,
    /// Compensation actions (run in reverse order on failure)
    pub compensations: Vec<Signature>,
}

impl CompensationWorkflow {
    /// Create a new compensation workflow
    pub fn new() -> Self {
        Self {
            forward: Vec::new(),
            compensations: Vec::new(),
        }
    }

    /// Add a step with compensation
    pub fn step(mut self, forward: Signature, compensation: Signature) -> Self {
        self.forward.push(forward);
        self.compensations.push(compensation);
        self
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.forward.is_empty()
    }

    /// Get number of steps
    pub fn len(&self) -> usize {
        self.forward.len()
    }
}

impl Default for CompensationWorkflow {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for CompensationWorkflow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Compensation[{} steps, {} compensations]",
            self.forward.len(),
            self.compensations.len()
        )
    }
}
