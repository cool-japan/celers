//! Higher-level workflow shapes built on the canvas primitives.
//!
//! Every helper here returns a workflow whose **runtime behaviour matches its
//! name**. That is worth stating explicitly because it used not to be true:
//! these functions accepted a failure task, a set of compensations or an
//! aggregate and then chained everything unconditionally, so a "conditional"
//! payment workflow charged the card whether or not the balance check passed,
//! and a "saga" had no rollback at all. The structures below are executed by
//! the worker as written — a [`Branch`] really branches, a compensation really
//! only runs on failure — see [`celers_worker::workflows`] (the success path)
//! and [`celers_worker::error_links`] (the failure path).
//!
//! # Worker features these shapes need
//!
//! Three of the four shapes work on **any** worker, because everything they
//! need travels as plain JSON in the task payload: a saga's compensations
//! ([`create_saga_workflow`](crate::advanced_patterns::create_saga_workflow)),
//! a dynamic workflow
//! ([`create_dynamic_workflow`](crate::advanced_patterns::create_dynamic_workflow))
//! and parallel chains without an aggregate
//! ([`create_parallel_chains`](crate::advanced_patterns::create_parallel_chains)
//! with `None`). The two that do not are worth knowing before you deploy:
//!
//! * **Conditionals**
//!   ([`create_conditional_workflow`](crate::advanced_patterns::create_conditional_workflow),
//!   [`create_conditional_workflow_with`](crate::advanced_patterns::create_conditional_workflow_with))
//!   need the worker built with
//!   `celers-worker`'s `canvas` feature. It is **off by default**, and the
//!   `celers` facade has no feature that turns it on, so a worker built from
//!   the facade's defaults cannot evaluate a condition: it runs the condition
//!   task, logs a warning that it cannot decode the branch, and **ends the
//!   chain** — neither arm runs. That is deliberate fail-closed behaviour (far
//!   better than guessing an arm and charging a card), but it means a
//!   conditional workflow silently does nothing past its first step unless the
//!   worker binary enables `celers-worker/canvas`.
//! * **Aggregates** (`ParallelChains::apply_with_backend`) additionally need
//!   `celers-worker`'s `workflows` feature *and* a worker given a barrier store
//!   with `Worker::with_chord_backend`; without both, the header chains run and
//!   the aggregate never fires.
//!
//! Until the facade forwards those features, build the worker binary against
//! `celers-worker` directly with `features = ["canvas"]` (or `["workflows"]`)
//! when you use either shape.

use crate::{
    Branch, CanvasElement, CanvasError, Chain, Condition, NestedChain, NestedGroup, Signature,
};
use celers_core::Broker;
use serde_json::Value;
use uuid::Uuid;

/// Creates a conditional workflow that runs **either** the success task or the
/// failure task, never both.
///
/// The workflow is `condition_task -> Branch(truthy(result) ? success : failure)`.
/// The condition task runs first; the worker evaluates its return value and
/// enqueues exactly one of the two arms, passing the condition's result to it
/// as the first positional argument.
///
/// # The condition task must return a truthy/falsy value
///
/// The branch is taken on the *truthiness* of the whole returned value, using
/// JSON semantics: `false`, `null`, `0`, `""`, `[]` and `{}` are falsy;
/// everything else is truthy. A condition task that returns a status **object**
/// such as `{"sufficient": false}` is therefore truthy — a non-empty object —
/// and would take the success arm. Return a bare boolean, or use
/// [`create_conditional_workflow_with`] to test a field.
///
/// # The worker must be built with `celers-worker/canvas`
///
/// That feature is off by default and the `celers` facade does not turn it on.
/// A worker without it cannot decode the branch: it runs the condition task,
/// warns, and ends the chain — so **neither** arm runs. See the [module
/// documentation](self#worker-features-these-shapes-need).
///
/// # Arguments
///
/// * `condition_task` - Task whose return value decides the branch
/// * `condition_args` - Arguments for the condition task
/// * `success_task` - Task to execute when the condition is truthy
/// * `success_args` - Arguments for the success task
/// * `failure_task` - Task to execute when the condition is falsy
/// * `failure_args` - Arguments for the failure task
///
/// # Example
///
/// ```
/// use celers::advanced_patterns::create_conditional_workflow;
/// use serde_json::json;
///
/// // `check_balance` returns `true` or `false`.
/// let workflow = create_conditional_workflow(
///     "check_balance",
///     vec![json!({"account_id": 123})],
///     "process_payment",
///     vec![json!({"amount": 100})],
///     "send_insufficient_funds_notice",
///     vec![json!({"account_id": 123})],
/// );
///
/// // Two elements: the condition, then the branch that picks one arm.
/// assert_eq!(workflow.len(), 2);
/// assert!(workflow.validate().is_ok());
/// ```
pub fn create_conditional_workflow(
    condition_task: &str,
    condition_args: Vec<Value>,
    success_task: &str,
    success_args: Vec<Value>,
    failure_task: &str,
    failure_args: Vec<Value>,
) -> NestedChain {
    create_conditional_workflow_with(
        condition_task,
        condition_args,
        Condition::truthy(),
        success_task,
        success_args,
        failure_task,
        failure_args,
    )
}

/// Creates a conditional workflow with an explicit [`Condition`].
///
/// The general form of [`create_conditional_workflow`]: the condition is
/// evaluated against the condition task's return value, so a task returning a
/// structured status can be tested field by field.
///
/// Like [`create_conditional_workflow`], this needs a worker built with
/// `celers-worker`'s `canvas` feature; without it the chain ends after the
/// condition task and neither arm runs.
///
/// # Example
///
/// ```
/// use celers::advanced_patterns::create_conditional_workflow_with;
/// use celers::Condition;
/// use serde_json::json;
///
/// // `check_balance` returns `{"sufficient": true, "balance": 250}`.
/// let workflow = create_conditional_workflow_with(
///     "check_balance",
///     vec![json!({"account_id": 123})],
///     Condition::field_truthy("sufficient"),
///     "process_payment",
///     vec![json!({"amount": 100})],
///     "send_insufficient_funds_notice",
///     vec![json!({"account_id": 123})],
/// );
///
/// assert!(workflow.validate().is_ok());
/// ```
pub fn create_conditional_workflow_with(
    condition_task: &str,
    condition_args: Vec<Value>,
    condition: Condition,
    success_task: &str,
    success_args: Vec<Value>,
    failure_task: &str,
    failure_args: Vec<Value>,
) -> NestedChain {
    let branch = Branch::new(
        condition,
        Signature::new(success_task.to_string()).with_args(success_args),
    )
    .otherwise(Signature::new(failure_task.to_string()).with_args(failure_args));

    NestedChain::new()
        .then_signature(Signature::new(condition_task.to_string()).with_args(condition_args))
        .then_branch(branch)
}

/// Creates a dynamic workflow where tasks are generated at runtime
///
/// This pattern allows for workflows where the number and type of tasks
/// are determined dynamically based on input data: the generator's return value
/// is passed to the executor as its first argument.
///
/// # Arguments
///
/// * `generator_task` - Task that generates the list of tasks to execute
/// * `generator_args` - Arguments for the generator task
/// * `executor_task` - Task that executes the generated tasks
///
/// # Example
///
/// ```
/// use celers::advanced_patterns::create_dynamic_workflow;
/// use serde_json::json;
///
/// let workflow = create_dynamic_workflow(
///     "generate_tasks",
///     vec![json!({"rules": "config.json"})],
///     "execute_task",
/// );
/// assert_eq!(workflow.len(), 2);
/// ```
pub fn create_dynamic_workflow(
    generator_task: &str,
    generator_args: Vec<Value>,
    executor_task: &str,
) -> Chain {
    Chain::new()
        .then(generator_task, generator_args)
        .then(executor_task, vec![])
}

/// Independent task chains that run concurrently, optionally followed by an
/// aggregate that receives all of their results.
///
/// Returned by [`create_parallel_chains`]. It is a distinct type rather than a
/// bare [`NestedGroup`] because "run these chains, then aggregate" is a
/// **chord**, and a chord needs a result backend to establish its completion
/// barrier — a fact the caller has to be able to see. [`apply`](Self::apply)
/// dispatches the chains; [`apply_with_backend`](Self::apply_with_backend)
/// (feature `backend-redis`) is what an aggregate requires.
#[derive(Debug, Clone)]
pub struct ParallelChains {
    /// The concurrent branches, one [`CanvasElement::Chain`] per input chain.
    group: NestedGroup,
    /// The aggregate callback, when one was requested.
    aggregate: Option<Signature>,
    /// The chains in flattened form, kept so the chord path can tag each
    /// chain's final step without re-deriving it.
    chains: Vec<Vec<Signature>>,
}

impl ParallelChains {
    /// The concurrent branches as a canvas group.
    #[must_use]
    pub fn group(&self) -> &NestedGroup {
        &self.group
    }

    /// The aggregate callback, if one was requested.
    #[must_use]
    pub fn aggregate(&self) -> Option<&Signature> {
        self.aggregate.as_ref()
    }

    /// How many parallel chains there are.
    #[must_use]
    pub fn len(&self) -> usize {
        self.chains.len()
    }

    /// Whether there are no chains at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.chains.is_empty()
    }

    /// The tasks of the `index`-th chain, in order.
    #[must_use]
    pub fn chain(&self, index: usize) -> Option<&[Signature]> {
        self.chains.get(index).map(Vec::as_slice)
    }

    /// Dispatch every chain concurrently.
    ///
    /// Each chain runs its own steps in sequence — step 2 starts only after
    /// step 1 has *completed* — while the chains run alongside one another.
    ///
    /// # Errors
    ///
    /// Returns [`CanvasError::Invalid`] when an aggregate was requested: the
    /// aggregate must run exactly once, after every chain has finished, which
    /// is a barrier and needs a result backend. Use
    /// [`apply_with_backend`](Self::apply_with_backend), or build without an
    /// aggregate. Also propagates broker errors.
    pub async fn apply<B: Broker>(&self, broker: &B) -> Result<Uuid, CanvasError> {
        if self.aggregate.is_some() {
            return Err(CanvasError::Invalid(AGGREGATE_REQUIRES_BACKEND.to_string()));
        }
        self.group.apply(broker).await
    }
}

/// Message used when an aggregate is requested without a result backend.
const AGGREGATE_REQUIRES_BACKEND: &str =
    "Aggregating over parallel chains is a chord: the aggregate task must run once, after every \
     chain has completed, with all of their results. That barrier needs shared state a worker can \
     count against, which is what the result backend provides. Call `apply_with_backend` (the \
     `backend-redis` feature) instead of `apply`, or build the workflow without an aggregate.";

/// Creates a workflow with parallel sub-chains.
///
/// Each entry of `chains` becomes a **complete** [`Chain`] inside the group, so
/// every task of every chain runs, in order. When `aggregate_task` is `Some`,
/// the result is a chord: the aggregate runs once, after all chains have
/// finished, receiving the list of their final results.
///
/// Without an aggregate this runs on any worker. *With* one, the aggregate is a
/// barrier: dispatch it through `ParallelChains::apply_with_backend` (feature
/// `backend-redis`), and run a worker built with `celers-worker`'s `workflows`
/// feature and given the same barrier store via `Worker::with_chord_backend` —
/// otherwise the chains run and the aggregate never fires.
///
/// # Arguments
///
/// * `chains` - List of `(chain_name, tasks)` tuples; `chain_name` is a label
///   for the caller's benefit and does not become a task.
/// * `aggregate_task` - Optional task to aggregate results from all chains
///
/// # Example
///
/// ```
/// use celers::advanced_patterns::create_parallel_chains;
///
/// let chains = vec![
///     ("process_images", vec![("resize", vec![]), ("optimize", vec![])]),
///     ("process_videos", vec![("transcode", vec![]), ("thumbnail", vec![])]),
/// ];
///
/// let workflow = create_parallel_chains(chains, Some("finalize"));
///
/// assert_eq!(workflow.len(), 2);
/// // Every step of every chain is kept, not just the first.
/// assert_eq!(workflow.chain(0).map(<[_]>::len), Some(2));
/// assert_eq!(workflow.chain(1).map(<[_]>::len), Some(2));
/// assert_eq!(workflow.aggregate().map(|sig| sig.task.as_str()), Some("finalize"));
/// ```
#[allow(clippy::type_complexity)]
pub fn create_parallel_chains(
    chains: Vec<(&str, Vec<(&str, Vec<Value>)>)>,
    aggregate_task: Option<&str>,
) -> ParallelChains {
    let mut group = NestedGroup::new();
    let mut flattened = Vec::with_capacity(chains.len());

    for (_chain_name, tasks) in chains {
        if tasks.is_empty() {
            continue;
        }

        let signatures: Vec<Signature> = tasks
            .into_iter()
            .map(|(task, args)| Signature::new(task.to_string()).with_args(args))
            .collect();

        let mut chain = Chain::new();
        for signature in &signatures {
            chain = chain.then_signature(signature.clone());
        }

        group = group.add_element(CanvasElement::Chain(chain));
        flattened.push(signatures);
    }

    ParallelChains {
        group,
        aggregate: aggregate_task.map(|task| Signature::new(task.to_string())),
        chains: flattened,
    }
}

/// Creates a saga workflow: forward steps in order, with each completed step's
/// compensation registered to run **in reverse** if a later step fails.
///
/// Step *k* is dispatched with the compensations of steps *k-1 … 0* attached as
/// its error route, so when it fails for good the worker enqueues
/// `compensate(k-1)`, and only after that one finishes `compensate(k-2)`, and
/// so on down to `compensate(0)`. Nothing runs when the saga succeeds.
///
/// # Which compensations run
///
/// Only those of steps that **completed successfully**. A step that fails is
/// not compensated by this workflow: it never reported success, so undoing it
/// is the step's own responsibility (make the forward action atomic, or its
/// compensation idempotent and self-triggered). The first step therefore
/// carries no error route at all — if it fails, nothing has happened yet.
///
/// # A failing compensation truncates the rollback
///
/// The rollback is itself a chain, so if `compensate(k-1)` fails for good, the
/// compensations behind it (`k-2 … 0`) do not run and the saga is left half
/// undone. That is inherent to running compensations as tasks: there is no
/// meaningful way to "compensate a compensation". Write compensations to be
/// idempotent and retry-safe — give them a retry budget with
/// [`with_exponential_backoff`](crate::error_recovery::with_exponential_backoff),
/// or attach your own alerting handler with
/// [`with_dlq`](crate::error_recovery::with_dlq) — so a transient failure does
/// not strand the rest of the rollback.
///
/// # Arguments
///
/// * `steps` - List of `(forward_task, forward_args, compensate_task, compensate_args)`
///
/// # Example
///
/// ```
/// use celers::advanced_patterns::create_saga_workflow;
/// use serde_json::json;
///
/// let steps = vec![
///     ("reserve_inventory", vec![json!(1)], "release_inventory", vec![json!(1)]),
///     ("charge_payment", vec![json!(2)], "refund_payment", vec![json!(2)]),
///     ("ship_order", vec![json!(3)], "cancel_shipment", vec![json!(3)]),
/// ];
///
/// let workflow = create_saga_workflow(steps);
///
/// // Nothing to roll back if the very first step fails.
/// assert!(workflow.tasks[0].options.all_link_errors().is_empty());
/// // `ship_order` rolls back payment first, then inventory.
/// let rollback: Vec<&str> = workflow.tasks[2]
///     .options
///     .all_link_errors()
///     .iter()
///     .map(|sig| sig.task.as_str())
///     .collect();
/// assert_eq!(rollback, vec!["refund_payment", "release_inventory"]);
/// ```
pub fn create_saga_workflow(steps: Vec<(&str, Vec<Value>, &str, Vec<Value>)>) -> Chain {
    let mut chain = Chain::new();
    // Compensations for the steps already added, most recent first: exactly the
    // rollback order a failure at the next step needs.
    let mut rollback: Vec<Signature> = Vec::with_capacity(steps.len());

    for (forward_task, forward_args, compensate_task, compensate_args) in steps {
        let mut forward = Signature::new(forward_task.to_string()).with_args(forward_args);
        // A compensation is a plain task; it receives the failure descriptor as
        // its first argument (see `celers_worker::error_links::TaskFailure`)
        // unless it declares otherwise.
        forward.options.link_errors = rollback.clone();
        chain = chain.then_signature(forward);

        rollback.insert(
            0,
            Signature::new(compensate_task.to_string()).with_args(compensate_args),
        );
    }

    chain
}

// The chord path needs the result-backend types, which the facade only has when
// its `backend-redis` feature is on.
#[cfg(feature = "backend-redis")]
mod chord_dispatch {
    use super::{ParallelChains, AGGREGATE_REQUIRES_BACKEND};
    use celers_backend_redis::{ChordState, ResultBackend};
    use celers_canvas::dispatch::{self, ChainStep};
    use celers_canvas::{CanvasError, Signature};
    use celers_core::Broker;
    use uuid::Uuid;

    impl ParallelChains {
        /// Dispatch every chain concurrently and, when an aggregate was
        /// requested, register the chord barrier that triggers it.
        ///
        /// The barrier counts **chains, not tasks**: each chain's *final* step
        /// is pre-assigned an id and stamped with the chord id, the barrier is
        /// registered against those ids before anything is enqueued, and the
        /// worker enqueues the aggregate once every chain's last step has
        /// completed — with the list of their results as its first argument.
        ///
        /// Registering before dispatching is what makes the barrier safe: a
        /// chain cannot finish against a barrier that does not exist yet.
        ///
        /// Without an aggregate this is exactly [`apply`](Self::apply).
        ///
        /// # Errors
        ///
        /// Propagates serialization and broker errors, and returns
        /// [`CanvasError::Invalid`] if the workflow has no chains at all.
        pub async fn apply_with_backend<B: Broker, R: ResultBackend>(
            &self,
            broker: &B,
            backend: &mut R,
        ) -> Result<Uuid, CanvasError> {
            let Some(aggregate) = self.aggregate.as_ref() else {
                return self.group.apply(broker).await;
            };

            if self.chains.is_empty() {
                return Err(CanvasError::Invalid(format!(
                    "{AGGREGATE_REQUIRES_BACKEND} (there are also no chains to aggregate over)"
                )));
            }

            let chord_id = Uuid::new_v4();

            // Stamp each chain's final step: a pre-assigned id so the barrier
            // can be registered against it, and the chord id so completing it
            // counts. Intermediate steps carry neither, so a three-task chain
            // still counts once.
            let mut member_ids = Vec::with_capacity(self.chains.len());
            let mut heads: Vec<(Signature, Vec<ChainStep>)> = Vec::with_capacity(self.chains.len());

            for chain in &self.chains {
                let mut steps = chain.clone();
                let Some(last) = steps.last_mut() else {
                    continue;
                };
                let member_id = last.options.task_id.unwrap_or_else(Uuid::new_v4);
                last.options.task_id = Some(member_id);
                last.options.chord_id = Some(chord_id);
                member_ids.push(member_id);

                let tail: Vec<ChainStep> =
                    steps.iter().skip(1).cloned().map(ChainStep::Task).collect();
                let head = steps.remove(0);
                heads.push((head, tail));
            }

            let state = ChordState::new(chord_id, member_ids.len(), member_ids)
                .with_callback(aggregate.task.clone());
            backend
                .chord_init(state)
                .await
                .map_err(|e| CanvasError::Broker(format!("Failed to initialize chord: {e}")))?;

            let mut built = Vec::with_capacity(heads.len());
            for (head, tail) in &heads {
                let mut task = dispatch::build_task(head, tail)?;
                task.metadata.group_id = Some(chord_id);
                built.push((task, dispatch::Schedule::from_options(&head.options)));
            }

            dispatch::dispatch_all(broker, built).await?;

            Ok(chord_id)
        }
    }
}
