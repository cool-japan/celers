use crate::{Chain, Condition, Signature};
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
///
/// The executable form is a [`Chain`]: see [`to_chain`](Self::to_chain), which
/// is what turns the two parallel vectors below into something a worker runs.
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

    /// Lower the workflow into the [`Chain`] a worker actually executes.
    ///
    /// The forward actions become the chain, in declaration order. Step *k* is
    /// given the compensations of steps *k-1 … 0* — newest first — as its
    /// **sequential error route**
    /// ([`TaskOptions::link_errors`](crate::TaskOptions::link_errors), which
    /// [`crate::dispatch::error_route`] writes into the task payload): when
    /// step *k* fails for good the worker enqueues `compensate(k-1)`, and only
    /// once *that* has finished `compensate(k-2)`, down to `compensate(0)`.
    /// Nothing runs when the chain succeeds.
    ///
    /// # Which compensations run
    ///
    /// Only those of steps that **completed successfully**. The step that
    /// failed is not compensated: it never reported success, so undoing it is
    /// the step's own responsibility. The first step therefore carries no error
    /// route at all — if it fails, nothing has happened yet.
    ///
    /// # Routes the steps already declare are preserved
    ///
    /// A forward signature may carry its own
    /// [`link_error`](crate::TaskOptions::link_error)/`link_errors` (an alert,
    /// a dead-letter hop). Those are kept and appended **after** the rollback,
    /// so the compensations run first and the step's own handler last. Both
    /// fields are folded into `link_errors` so that order is exactly what
    /// [`all_link_errors`](crate::TaskOptions::all_link_errors) reports.
    ///
    /// # A failing compensation truncates the rollback
    ///
    /// The rollback is itself a chain, so if `compensate(k-1)` fails for good
    /// the compensations behind it do not run. Write compensations to be
    /// idempotent and give them a retry budget.
    ///
    /// # Extra or missing compensations
    ///
    /// [`step`](Self::step) keeps the two vectors aligned, but they are public:
    /// a forward step with no compensation at its index simply contributes
    /// nothing to the rollback, and compensations past the end of `forward` are
    /// unreachable and ignored.
    ///
    /// # Example
    ///
    /// ```
    /// use celers_canvas::{CompensationWorkflow, Signature};
    ///
    /// let saga = CompensationWorkflow::new()
    ///     .step(
    ///         Signature::new("reserve_inventory".to_string()),
    ///         Signature::new("release_inventory".to_string()),
    ///     )
    ///     .step(
    ///         Signature::new("charge_payment".to_string()),
    ///         Signature::new("refund_payment".to_string()),
    ///     );
    ///
    /// let chain = saga.to_chain();
    /// assert_eq!(chain.len(), 2);
    /// // Nothing to roll back if the very first step fails.
    /// assert!(chain.tasks[0].options.all_link_errors().is_empty());
    /// // A failed payment releases the inventory that was already reserved.
    /// let rollback: Vec<&str> = chain.tasks[1]
    ///     .options
    ///     .all_link_errors()
    ///     .iter()
    ///     .map(|sig| sig.task.as_str())
    ///     .collect();
    /// assert_eq!(rollback, vec!["release_inventory"]);
    /// ```
    #[must_use]
    pub fn to_chain(&self) -> Chain {
        let mut chain = Chain::with_capacity(self.forward.len());
        // Compensations of the steps already added, most recent first: exactly
        // the rollback order a failure at the next step needs.
        let mut rollback: Vec<Signature> = Vec::with_capacity(self.forward.len());

        for (index, forward) in self.forward.iter().enumerate() {
            let mut step = forward.clone();

            let mut route = rollback.clone();
            route.extend(step.options.all_link_errors().into_iter().cloned());
            // Both declaration forms are folded into `link_errors` so the
            // rollback keeps its position: `all_link_errors` always reports the
            // single `link_error` first, which would otherwise jump ahead of
            // the compensations.
            step.options.link_error = None;
            step.options.link_errors = route;

            chain = chain.then_signature(step);

            if let Some(compensation) = self.compensations.get(index) {
                rollback.insert(0, compensation.clone());
            }
        }

        chain
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
