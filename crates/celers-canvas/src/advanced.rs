use crate::{CanvasError, Chain, Chord, CompensationWorkflow, Group, Signature};
use celers_core::Broker;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(feature = "backend-redis")]
use crate::dispatch;
#[cfg(feature = "backend-redis")]
use celers_backend_redis::{ChordState, ResultBackend};

/// Saga: forward steps, each with a compensating action that undoes it.
///
/// A saga is not a runtime primitive of its own: it **lowers** onto a
/// [`Chain`] whose steps carry the preceding steps' compensations as a
/// sequential error route, which the worker runs — newest compensation first —
/// when a step fails for good. [`to_chain`](Self::to_chain) produces that
/// chain and [`apply`](Self::apply) dispatches it; see
/// [`CompensationWorkflow::to_chain`] for the exact shape and its caveats.
///
/// The `celers` facade builds the same rollback shape straight from tuples with
/// `celers::advanced_patterns::create_saga_workflow`. That helper *replaces*
/// each step's failure route with the rollback, so it is equivalent to this
/// lowering exactly for the input it accepts — bare task names and arguments,
/// with no per-step handlers of their own. Only [`to_chain`](Self::to_chain)
/// keeps a handler a step already declared (see
/// [`CompensationWorkflow::to_chain`]).
pub struct Saga {
    /// Compensation workflow
    pub workflow: CompensationWorkflow,
    /// Isolation level
    pub isolation: SagaIsolation,
}

/// Saga isolation level
///
/// # This is advisory metadata, not an enforced guarantee
///
/// The level is carried with the saga and reported by its [`Display`] impl for
/// the caller's own bookkeeping; it does **not** take part in
/// [`Saga::to_chain`]'s lowering. Isolating concurrent sagas from one another
/// means holding a lock across several independently-scheduled tasks, which
/// needs a transaction manager CeleRS deliberately does not have: a task queue
/// has no rollback segment to read a pre-image from. Enforce the level in the
/// steps themselves (a row lock, an optimistic-concurrency version column, an
/// idempotency key) — a value set here changes nothing about what is
/// dispatched.
///
/// [`Display`]: std::fmt::Display
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

    /// Lower the saga into the [`Chain`] a worker executes.
    ///
    /// Exactly [`CompensationWorkflow::to_chain`] — step *k* carries the
    /// compensations of steps *k-1 … 0* as its error route, so a failure at
    /// step *k* rolls back everything that already succeeded, newest first.
    /// [`SagaIsolation`] takes no part in it (see that enum's documentation).
    ///
    /// # Example
    ///
    /// ```
    /// use celers_canvas::{CompensationWorkflow, Saga, Signature};
    ///
    /// let saga = Saga::new(
    ///     CompensationWorkflow::new()
    ///         .step(
    ///             Signature::new("reserve_inventory".to_string()),
    ///             Signature::new("release_inventory".to_string()),
    ///         )
    ///         .step(
    ///             Signature::new("charge_payment".to_string()),
    ///             Signature::new("refund_payment".to_string()),
    ///         )
    ///         .step(
    ///             Signature::new("ship_order".to_string()),
    ///             Signature::new("cancel_shipment".to_string()),
    ///         ),
    /// );
    ///
    /// let chain = saga.to_chain();
    /// assert_eq!(chain.len(), 3);
    ///
    /// // A failed shipment refunds the payment first, then releases the stock.
    /// let rollback: Vec<&str> = chain.tasks[2]
    ///     .options
    ///     .all_link_errors()
    ///     .iter()
    ///     .map(|sig| sig.task.as_str())
    ///     .collect();
    /// assert_eq!(rollback, vec!["refund_payment", "release_inventory"]);
    /// ```
    #[must_use]
    pub fn to_chain(&self) -> Chain {
        self.workflow.to_chain()
    }

    /// Dispatch the saga: enqueue the first forward step with the rest of the
    /// chain — and every step's rollback route — attached to it.
    ///
    /// Returns the id of the first step's task.
    ///
    /// # Errors
    ///
    /// Returns [`CanvasError::Invalid`] for a saga with no steps, and
    /// propagates broker errors.
    pub async fn apply<B: Broker>(&self, broker: &B) -> Result<Uuid, CanvasError> {
        if self.workflow.is_empty() {
            return Err(CanvasError::Invalid(
                "Saga has no steps: there is nothing to run and nothing to compensate".to_string(),
            ));
        }

        self.to_chain().apply(broker).await
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

/// Message used when [`ScatterGather::apply`] is asked to dispatch the scatter
/// step together with the workers it precedes.
const SCATTER_NOT_SEQUENCEABLE: &str =
    "A ScatterGather's scatter step has to *complete* before its workers may start, and \"run a \
     fan-out once a single task has finished\" is a completion barrier the chain-link mechanism \
     cannot express: a chain step carries one successor, not a group. Dispatch the scatter step \
     yourself (as a plain Signature or a Chain) and call `apply_after_scatter` from it once it has \
     produced the work — or, when the workers' arguments are known up front and the scatter step \
     only exists to seed them, drop it and dispatch `to_chord()` directly.";

/// Message used when [`FanOut::apply`] is asked to dispatch the source task
/// together with the consumers it precedes.
const FANOUT_SOURCE_NOT_SEQUENCEABLE: &str =
    "A FanOut's source has to *complete* before its consumers may start, and \"run a fan-out once \
     a single task has finished\" is a completion barrier the chain-link mechanism cannot express: \
     a chain step carries one successor, not a group. Dispatch the source yourself and call \
     `apply_after_source` from it with its result — or, if the consumers only have to run \
     alongside the source rather than after it, dispatch `to_group()` and the source \
     independently.";

/// Scatter-gather pattern: distribute work, collect results
///
/// Lowers onto a [`Chord`]: the workers are its header (they run in parallel)
/// and the gather task is its body (it runs once, after every worker has
/// finished, with their results). See [`to_chord`](Self::to_chord).
///
/// The `scatter` step is **not** part of that chord — see
/// [`apply_after_scatter`](Self::apply_after_scatter) for why and for what to
/// do instead. When the workers' inputs are known up front, so no scatter step
/// is needed at all, `celers::advanced_patterns::create_parallel_chains` builds
/// the same shape from the facade — parallel branches plus an aggregate — with
/// a dispatch method that establishes the barrier for you.
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

    /// The worker tasks as a parallel [`Group`].
    ///
    /// The header half of [`to_chord`](Self::to_chord), on its own: dispatching
    /// it runs the workers concurrently with no barrier and no gather step.
    #[must_use]
    pub fn to_group(&self) -> Group {
        Group::new().extend(self.workers.iter().cloned())
    }

    /// Lower the pattern into the [`Chord`] a worker executes: the workers in
    /// parallel, then the gather task with all of their results.
    ///
    /// # Example
    ///
    /// ```
    /// use celers_canvas::{ScatterGather, Signature};
    ///
    /// let pattern = ScatterGather::new(
    ///     Signature::new("split_batch".to_string()),
    ///     vec![
    ///         Signature::new("process_shard".to_string()),
    ///         Signature::new("process_shard".to_string()),
    ///     ],
    ///     Signature::new("merge_shards".to_string()),
    /// );
    ///
    /// let chord = pattern.to_chord();
    /// assert_eq!(chord.header.len(), 2);
    /// assert_eq!(chord.body.task, "merge_shards");
    /// ```
    #[must_use]
    pub fn to_chord(&self) -> Chord {
        Chord::new(self.to_group(), self.gather.clone())
    }

    /// Dispatch the workers and register the gather barrier, for a scatter step
    /// that has **already completed**.
    ///
    /// This is the executable half of the pattern: the worker group is
    /// registered as a chord against a freshly minted chord id — with
    /// [`timeout`](Self::timeout) recorded on the barrier — and only then
    /// enqueued, so no worker can finish against a barrier that does not exist
    /// yet. The gather task is not enqueued here; the worker enqueues it once
    /// the completion counter reaches the worker count, with the list of their
    /// results as its first argument.
    ///
    /// Call it from inside the scatter task (or from the client, once the
    /// scatter task's result is in hand): sequencing a fan-out *after* a single
    /// task is a completion barrier that the chain-link mechanism cannot carry,
    /// which is why [`apply`](Self::apply) refuses rather than dispatching the
    /// two halves in the wrong order.
    ///
    /// # The timeout
    ///
    /// [`with_timeout`](Self::with_timeout) is recorded on the
    /// [`ChordState`] so the barrier knows when it has gone stale — it is what
    /// lets a result backend reclaim a chord whose workers never all completed.
    /// Nothing enqueues the gather task on expiry: a timed-out chord has no
    /// results to gather.
    ///
    /// Returns the chord id.
    ///
    /// # Errors
    ///
    /// Returns [`CanvasError::Invalid`] when there are no workers, and
    /// propagates backend and broker errors.
    #[cfg(feature = "backend-redis")]
    pub async fn apply_after_scatter<B: Broker, R: ResultBackend>(
        &self,
        broker: &B,
        backend: &mut R,
    ) -> Result<Uuid, CanvasError> {
        if self.workers.is_empty() {
            return Err(CanvasError::Invalid(
                "ScatterGather has no workers: there is nothing to scatter to and nothing to gather"
                    .to_string(),
            ));
        }

        let chord_id = Uuid::new_v4();

        // Build first so the ids are known before the barrier is written; the
        // group id doubles as the chord id so the fan-out is trackable as a
        // group too. Same ordering as `Chord::apply`, plus the timeout.
        let built = dispatch::build_fanout(&self.workers, Some(chord_id), Some(chord_id))?;
        let task_ids: Vec<Uuid> = built.iter().map(|(task, _)| task.metadata.id).collect();

        let mut state = ChordState::new(chord_id, task_ids.len(), task_ids)
            .with_callback(self.gather.task.clone());
        if let Some(timeout) = self.timeout {
            state = state.with_timeout(std::time::Duration::from_secs(timeout));
        }

        backend
            .chord_init(state)
            .await
            .map_err(|e| CanvasError::Broker(format!("Failed to initialize chord: {}", e)))?;

        dispatch::dispatch_all(broker, built).await?;

        Ok(chord_id)
    }

    /// Dispatch the workers and register the gather barrier, for a scatter step
    /// that has **already completed**.
    ///
    /// Without the `backend-redis` feature there is no barrier store to count
    /// against, so this always fails rather than degrading into a plain group
    /// and dropping the gather step while reporting success — exactly as
    /// [`Chord::apply`] does.
    ///
    /// # Errors
    ///
    /// Always returns [`CanvasError::Invalid`].
    #[cfg(not(feature = "backend-redis"))]
    pub async fn apply_after_scatter<B: Broker>(&self, broker: &B) -> Result<Uuid, CanvasError> {
        self.to_chord().apply(broker).await
    }

    /// Dispatch the whole pattern, scatter step included — which is not
    /// something the runtime can express, so this always fails.
    ///
    /// The scatter step must *complete* before the workers start; a chain step
    /// carries exactly one successor, so "then fan out" has nowhere to live.
    /// The error names the two honest alternatives:
    /// [`apply_after_scatter`](Self::apply_after_scatter) called from the
    /// scatter task, or dropping the scatter step when the workers' arguments
    /// are already known.
    ///
    /// It fails loudly rather than silently dispatching the halves in parallel
    /// (which would run the workers on data the scatter step has not produced
    /// yet) or silently dropping the scatter step.
    ///
    /// # Errors
    ///
    /// Always returns [`CanvasError::Invalid`].
    pub async fn apply<B: Broker>(&self, _broker: &B) -> Result<Uuid, CanvasError> {
        Err(CanvasError::Invalid(SCATTER_NOT_SEQUENCEABLE.to_string()))
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
///
/// Lowers onto a [`Chain`] — see [`to_chain`](Self::to_chain): each stage runs
/// only after its predecessor has completed, receiving that predecessor's
/// return value as its first argument.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pipeline {
    /// Pipeline stages
    pub stages: Vec<Signature>,
    /// Buffer size between stages
    ///
    /// # Advisory only
    ///
    /// A lowered pipeline is a chain, and a chain has no buffer to size: stage
    /// *k+1* is enqueued by the worker that just finished stage *k*, so exactly
    /// one message per pipeline instance is ever in flight. The value is
    /// carried for the caller's own bookkeeping (and reported by [`Display`])
    /// and takes no part in [`to_chain`](Self::to_chain). To process a batch
    /// with real concurrency, dispatch one pipeline per item — a
    /// [`Group`] of chains — rather than sizing a buffer here.
    ///
    /// [`Display`]: std::fmt::Display
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

    /// Lower the pipeline into the [`Chain`] a worker executes.
    ///
    /// The stages become the chain in declaration order, so stage *k+1* starts
    /// only once stage *k* has completed and receives its result as its first
    /// positional argument (unless the stage is
    /// [`immutable`](crate::Signature::immutable)).
    /// [`buffer_size`](Self::buffer_size) takes no part in the lowering.
    ///
    /// # Example
    ///
    /// ```
    /// use celers_canvas::{Pipeline, Signature};
    ///
    /// let pipeline = Pipeline::new()
    ///     .stage(Signature::new("extract".to_string()))
    ///     .stage(Signature::new("transform".to_string()))
    ///     .stage(Signature::new("load".to_string()));
    ///
    /// let chain = pipeline.to_chain();
    /// let stages: Vec<&str> = chain.iter().map(|sig| sig.task.as_str()).collect();
    /// assert_eq!(stages, vec!["extract", "transform", "load"]);
    /// ```
    #[must_use]
    pub fn to_chain(&self) -> Chain {
        Chain::with_capacity(self.stages.len()).extend(self.stages.iter().cloned())
    }

    /// Dispatch the pipeline: enqueue the first stage with the remaining stages
    /// attached to it.
    ///
    /// Returns the id of the first stage's task.
    ///
    /// # Errors
    ///
    /// Returns [`CanvasError::Invalid`] for a pipeline with no stages, and
    /// propagates broker errors.
    pub async fn apply<B: Broker>(&self, broker: &B) -> Result<Uuid, CanvasError> {
        if self.stages.is_empty() {
            return Err(CanvasError::Invalid(
                "Pipeline has no stages: there is nothing to run".to_string(),
            ));
        }

        self.to_chain().apply(broker).await
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
///
/// Lowers onto a [`Group`] of the consumers — see [`to_group`](Self::to_group).
/// The `source` is **not** part of that group: see
/// [`apply_after_source`](Self::apply_after_source).
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

    /// Lower the fan-out into the [`Group`] a worker executes: every consumer,
    /// dispatched in parallel.
    ///
    /// # Example
    ///
    /// ```
    /// use celers_canvas::{FanOut, Signature};
    ///
    /// let fan_out = FanOut::new(Signature::new("publish_event".to_string()))
    ///     .consumer(Signature::new("update_search_index".to_string()))
    ///     .consumer(Signature::new("send_webhook".to_string()));
    ///
    /// let group = fan_out.to_group();
    /// assert_eq!(group.task_names(), vec!["update_search_index", "send_webhook"]);
    /// ```
    #[must_use]
    pub fn to_group(&self) -> Group {
        Group::new().extend(self.consumers.iter().cloned())
    }

    /// Dispatch the consumers in parallel, for a source that has **already
    /// completed**.
    ///
    /// This is the executable half of the pattern. Call it from inside the
    /// source task, giving each consumer whatever the source produced (with
    /// [`Signature::with_args`](crate::Signature::with_args)): broadcasting to
    /// a group once a single task has finished is a completion barrier that a
    /// chain step — which carries exactly one successor — cannot express, which
    /// is why [`apply`](Self::apply) refuses.
    ///
    /// Returns the group id shared by the consumers.
    ///
    /// # Errors
    ///
    /// Returns [`CanvasError::Invalid`] when there are no consumers, and
    /// propagates broker errors.
    pub async fn apply_after_source<B: Broker>(&self, broker: &B) -> Result<Uuid, CanvasError> {
        if self.consumers.is_empty() {
            return Err(CanvasError::Invalid(
                "FanOut has no consumers: there is nothing to broadcast to".to_string(),
            ));
        }

        self.to_group().apply(broker).await
    }

    /// Dispatch the whole pattern, source included — which is not something the
    /// runtime can express, so this always fails.
    ///
    /// Enqueuing the source alongside the consumers would run them in parallel
    /// with it, on data it has not produced yet; dropping it would silently
    /// discard a task the caller declared. The error names the alternative:
    /// [`apply_after_source`](Self::apply_after_source), called from the source
    /// task itself.
    ///
    /// # Errors
    ///
    /// Always returns [`CanvasError::Invalid`].
    pub async fn apply<B: Broker>(&self, _broker: &B) -> Result<Uuid, CanvasError> {
        Err(CanvasError::Invalid(
            FANOUT_SOURCE_NOT_SEQUENCEABLE.to_string(),
        ))
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
///
/// Lowers onto a [`Chord`] — see [`to_chord`](Self::to_chord): the sources are
/// its header and the aggregator its body, so the aggregator runs exactly once,
/// after every source has finished, with the list of their results. Unlike
/// [`FanOut`] and [`ScatterGather`] the pattern maps onto the runtime whole,
/// so [`apply`](Self::apply) really dispatches it.
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

    /// Lower the fan-in into the [`Chord`] a worker executes: the sources in
    /// parallel, then the aggregator with all of their results.
    ///
    /// # Example
    ///
    /// ```
    /// use celers_canvas::{FanIn, Signature};
    ///
    /// let fan_in = FanIn::new(Signature::new("merge_reports".to_string()))
    ///     .source(Signature::new("fetch_sales".to_string()))
    ///     .source(Signature::new("fetch_returns".to_string()));
    ///
    /// let chord = fan_in.to_chord();
    /// assert_eq!(chord.header.task_names(), vec!["fetch_sales", "fetch_returns"]);
    /// assert_eq!(chord.body.task, "merge_reports");
    /// ```
    #[must_use]
    pub fn to_chord(&self) -> Chord {
        Chord::new(
            Group::new().extend(self.sources.iter().cloned()),
            self.aggregator.clone(),
        )
    }

    /// Dispatch the fan-in: register the aggregation barrier, then enqueue the
    /// sources.
    ///
    /// The barrier is written **before** anything is enqueued, so no source can
    /// finish against a barrier that does not exist yet. The aggregator is not
    /// enqueued here — the worker enqueues it once every source has completed,
    /// with the list of their results as its first argument.
    ///
    /// Returns the chord id.
    ///
    /// # Errors
    ///
    /// Returns [`CanvasError::Invalid`] when there are no sources, and
    /// propagates backend and broker errors.
    #[cfg(feature = "backend-redis")]
    pub async fn apply<B: Broker, R: ResultBackend>(
        &self,
        broker: &B,
        backend: &mut R,
    ) -> Result<Uuid, CanvasError> {
        self.to_chord().apply(broker, backend).await
    }

    /// Dispatch the fan-in.
    ///
    /// Without the `backend-redis` feature there is no barrier store to count
    /// against, so this always fails rather than degrading into a plain group
    /// and dropping the aggregator while reporting success — exactly as
    /// [`Chord::apply`] does. Use
    /// [`apply_sources_only`](Self::apply_sources_only) when dispatching just
    /// the sources, with the aggregation coordinated by hand, is what you want.
    ///
    /// # Errors
    ///
    /// Always returns [`CanvasError::Invalid`].
    #[cfg(not(feature = "backend-redis"))]
    pub async fn apply<B: Broker>(&self, broker: &B) -> Result<Uuid, CanvasError> {
        self.to_chord().apply(broker).await
    }

    /// Dispatch only the sources, with no barrier and no aggregator.
    ///
    /// The honest form of "a fan-in without a result backend": it returns the
    /// sources' group id, not a chord id, and never pretends the aggregator was
    /// scheduled.
    ///
    /// # Errors
    ///
    /// Returns [`CanvasError::Invalid`] when there are no sources, and
    /// propagates broker errors.
    pub async fn apply_sources_only<B: Broker>(&self, broker: &B) -> Result<Uuid, CanvasError> {
        self.to_chord().apply_header_only(broker).await
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
