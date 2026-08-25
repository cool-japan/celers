//! Shared dispatch machinery for the canvas primitives.
//!
//! Every canvas primitive ([`Chain`](crate::Chain), [`Group`](crate::Group),
//! [`Chord`](crate::Chord), [`Map`](crate::Map), …) ultimately has to turn a
//! [`Signature`] into a [`SerializedTask`] and hand it to a
//! [`Broker`]. Doing that in each primitive independently is how
//! `options.countdown`, `options.link` and friends came to be silently dropped
//! on the floor, so all of them funnel through this module instead.
//!
//! # Wire format
//!
//! The payload of a canvas-dispatched task is a JSON object:
//!
//! ```json
//! {
//!   "args":    [ ... ],
//!   "kwargs":  { ... },
//!   "chain":   [ <ChainStep>, <ChainStep>, ... ],
//!   "errback": [ <ChainStep>, <ChainStep>, ... ],
//!   "ignore_errors": true,
//!   "retry_policy": {"delay": 2, "backoff": 2.0, "max_delay": 3600}
//! }
//! ```
//!
//! `args`/`kwargs` are the task's own arguments. `chain` is present only when
//! the task is a non-final step of a chain and carries the **entire remaining
//! tail** as serialized [`ChainStep`]s. The last three keys are the failure
//! half of the same contract — the task's error route, its failure-suppression
//! flag and its own retry backoff — and are present only when declared; see
//! [`ERROR_ROUTE_KEY`], [`IGNORE_ERRORS_KEY`] and [`RETRY_POLICY_KEY`].
//!
//! Carrying the whole tail is deliberate:
//! [`TaskMetadata::on_success_link`](celers_core::TaskMetadata::on_success_link)
//! is only an `Option<String>` — a task *name* — so it cannot express the
//! successor's args, kwargs, priority, countdown or its own successor. The name
//! is still written into `on_success_link` for plain task steps (so monitoring
//! and Celery-style tooling see the link), while the tail in the payload carries
//! everything the name cannot.
//!
//! The worker pops the head of `chain` on successful completion, applies the
//! predecessor's result to it according to its
//! [`CallbackArgMode`](crate::CallbackArgMode), and re-dispatches it with the
//! remainder of the tail still attached — so an N-task chain runs all N steps.
//! A [`ChainStep::Branch`]/[`ChainStep::Switch`] step is *evaluated* against
//! that result instead, and the selected arm becomes the next task.

use crate::{Branch, CanvasError, Signature, Switch, TaskOptions};
use celers_core::{Broker, SerializedTask};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// JSON key under which the remaining tail of a chain travels in the payload.
///
/// The worker (`celers-worker`'s `workflows` module) reads the same key; the
/// two constants are the contract between producer and consumer.
pub const CHAIN_TAIL_KEY: &str = "chain";

/// JSON key under which a task's **failure** route travels in the payload.
///
/// The value has exactly the same shape as [`CHAIN_TAIL_KEY`] — a JSON array of
/// [`ChainStep`] — and is consumed by the worker's terminal-failure path
/// instead of its success path. The head runs when the task fails for good
/// (retries exhausted, or no retry budget at all), receiving a JSON error
/// descriptor as its argument, and the remaining entries ride along as *its*
/// chain tail so a multi-step error route runs in order.
///
/// A name-only field on the metadata (the shape
/// [`TaskMetadata::on_success_link`](celers_core::TaskMetadata::on_success_link)
/// uses) is not enough here for the same reason it was not enough for the
/// success path: a fallback handler needs its own arguments, and a saga's
/// rollback is a *sequence* of compensations, not one task.
pub const ERROR_ROUTE_KEY: &str = "errback";

/// JSON key carrying [`TaskOptions::ignore_errors`] to the worker.
pub const IGNORE_ERRORS_KEY: &str = "ignore_errors";

/// JSON key carrying a task's own retry backoff policy to the worker.
///
/// Without it the worker can only apply *its* configured [`RetryConfig`]
/// (`celers_worker::RetryConfig`) to every task alike, so a signature built
/// with [`Signature::with_retry_delay`]/[`Signature::with_retry_backoff`] would
/// have those values dropped on the floor. The value is an object:
///
/// ```json
/// {"delay": 2, "backoff": 2.0, "max_delay": 3600, "jitter": true}
/// ```
///
/// with `delay` in seconds; every field is optional.
pub const RETRY_POLICY_KEY: &str = "retry_policy";

/// Upper bound (in seconds) for a generated dispatch countdown: 30 days.
///
/// Countdown-producing helpers ([`Chain::with_staggered_countdown`](crate::Chain::with_staggered_countdown),
/// [`Group::skew`](crate::Group::skew), [`Group::jitter`](crate::Group::jitter))
/// take unvalidated `u64` inputs from public APIs. Clamping keeps their
/// arithmetic free of overflow panics and keeps the values within a range a
/// broker's delayed-delivery support can plausibly honour.
pub const MAX_COUNTDOWN_SECS: u64 = 30 * 24 * 60 * 60;

/// Maximum number of tasks handed to a single [`Broker::enqueue_batch`] call
/// from [`dispatch_all`].
///
/// A very large fan-out (a `Group` with tens of thousands of members) would
/// otherwise become one oversized batch, risking a broker's message-size or
/// pipeline limits. [`dispatch_all`] splits anything larger into chunks of at
/// most this size — see its doc comment for the one case (a chord header)
/// where chunking is deliberately skipped.
pub const MAX_ENQUEUE_BATCH: usize = 500;

/// One step of a chain as it travels in a task payload.
///
/// A chain is not just a list of tasks: a conditional step has to be resolved
/// against the *result* of the step before it, which can only happen at
/// runtime, in the worker. Modelling the tail as `ChainStep` rather than
/// `Vec<Signature>` is what makes
/// [`CanvasElement::Branch`](crate::CanvasElement::Branch) and
/// [`CanvasElement::Switch`](crate::CanvasElement::Switch) executable inside a
/// nested workflow instead of being rejected at dispatch time.
///
/// The representation is internally tagged (`"step_type"`), so a step is
/// self-describing on the wire:
///
/// ```json
/// {"step_type": "task",   "task": "send_email", "args": [], "kwargs": {}}
/// {"step_type": "branch", "condition": {...}, "then_branch": {...}}
/// {"step_type": "switch", "cases": [...], "default": {...}}
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "step_type", rename_all = "snake_case")]
pub enum ChainStep {
    /// An ordinary task step.
    Task(Signature),

    /// A two-way conditional evaluated against the previous step's result.
    Branch(Branch),

    /// A multi-way conditional evaluated against the previous step's result.
    Switch(Switch),
}

impl ChainStep {
    /// The task name this step will dispatch, when that is known statically.
    ///
    /// Conditional steps return `None`: which task runs is only decided once
    /// the predecessor's result exists.
    #[must_use]
    pub fn static_task_name(&self) -> Option<&str> {
        match self {
            Self::Task(sig) => Some(sig.task.as_str()),
            Self::Branch(_) | Self::Switch(_) => None,
        }
    }

    /// Whether this step needs the predecessor's result to decide what to run.
    #[must_use]
    pub const fn is_conditional(&self) -> bool {
        matches!(self, Self::Branch(_) | Self::Switch(_))
    }
}

impl From<Signature> for ChainStep {
    fn from(sig: Signature) -> Self {
        Self::Task(sig)
    }
}

impl From<Branch> for ChainStep {
    fn from(branch: Branch) -> Self {
        Self::Branch(branch)
    }
}

impl From<Switch> for ChainStep {
    fn from(switch: Switch) -> Self {
        Self::Switch(switch)
    }
}

/// When a task should be handed to the broker.
///
/// Derived from a [`Signature`]'s [`TaskOptions`]; selects which of the
/// [`Broker`] enqueue variants is used so that `countdown`/`eta` actually have
/// a runtime effect instead of being decorative.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Schedule {
    /// Enqueue right away via [`Broker::enqueue`].
    #[default]
    Immediate,

    /// Enqueue with a relative delay (seconds) via [`Broker::enqueue_after`].
    After(u64),

    /// Enqueue at an absolute Unix timestamp (seconds) via
    /// [`Broker::enqueue_at`].
    At(i64),
}

impl Schedule {
    /// Derive the schedule from a signature's options.
    ///
    /// An explicit `eta` wins over a `countdown`; a zero countdown is treated
    /// as "immediate" so it does not force a needless scheduling round trip.
    #[must_use]
    pub fn from_options(options: &TaskOptions) -> Self {
        if let Some(eta) = options.eta {
            Self::At(eta)
        } else if let Some(countdown) = options.countdown {
            if countdown > 0 {
                Self::After(countdown)
            } else {
                Self::Immediate
            }
        } else {
            Self::Immediate
        }
    }

    /// Whether this schedule defers the task at all.
    #[must_use]
    pub const fn is_deferred(&self) -> bool {
        !matches!(self, Self::Immediate)
    }
}

/// The failure route declared by a signature, as chain steps.
///
/// [`TaskOptions::link_error`] (the single-callback field) comes first,
/// followed by [`TaskOptions::link_errors`] in declaration order. In CeleRS the
/// entries form a **sequential** error route rather than N independent
/// callbacks: each one receives its predecessor's result, which is what makes a
/// saga's rollback — compensate step 3, then 2, then 1 — expressible at all.
#[must_use]
pub fn error_route(sig: &Signature) -> Vec<ChainStep> {
    sig.options
        .all_link_errors()
        .into_iter()
        .cloned()
        .map(ChainStep::Task)
        .collect()
}

/// The per-task retry policy a signature declares, as a JSON object, or `None`
/// when it declares none.
fn retry_policy_value(options: &TaskOptions) -> Option<serde_json::Value> {
    let TaskOptions {
        retry_delay,
        retry_backoff,
        retry_backoff_max,
        retry_jitter,
        ..
    } = options;

    if retry_delay.is_none()
        && retry_backoff.is_none()
        && retry_backoff_max.is_none()
        && retry_jitter.is_none()
    {
        return None;
    }

    let mut policy = serde_json::Map::with_capacity(4);
    if let Some(delay) = retry_delay {
        policy.insert("delay".to_string(), serde_json::Value::from(*delay));
    }
    if let Some(backoff) = retry_backoff {
        policy.insert("backoff".to_string(), serde_json::Value::from(*backoff));
    }
    if let Some(max_delay) = retry_backoff_max {
        policy.insert("max_delay".to_string(), serde_json::Value::from(*max_delay));
    }
    if let Some(jitter) = retry_jitter {
        policy.insert("jitter".to_string(), serde_json::Value::from(*jitter));
    }

    Some(serde_json::Value::Object(policy))
}

/// Build the JSON payload envelope for `sig`, embedding `chain_tail` when the
/// signature is a non-final chain step.
///
/// Besides `args`/`kwargs`/[`CHAIN_TAIL_KEY`] the envelope carries everything
/// the worker needs that [`celers_core::TaskMetadata`] has no field for: the
/// failure route ([`ERROR_ROUTE_KEY`]), the failure-suppression flag
/// ([`IGNORE_ERRORS_KEY`]) and the task's own retry backoff policy
/// ([`RETRY_POLICY_KEY`]).
fn build_payload(sig: &Signature, chain_tail: &[ChainStep]) -> Result<Vec<u8>, CanvasError> {
    let mut envelope = serde_json::Map::with_capacity(6);
    envelope.insert(
        "args".to_string(),
        serde_json::Value::Array(sig.args.clone()),
    );
    envelope.insert(
        "kwargs".to_string(),
        serde_json::to_value(&sig.kwargs).map_err(|e| CanvasError::Serialization(e.to_string()))?,
    );

    if !chain_tail.is_empty() {
        envelope.insert(
            CHAIN_TAIL_KEY.to_string(),
            serde_json::to_value(chain_tail)
                .map_err(|e| CanvasError::Serialization(e.to_string()))?,
        );
    }

    let errbacks = error_route(sig);
    if !errbacks.is_empty() {
        envelope.insert(
            ERROR_ROUTE_KEY.to_string(),
            serde_json::to_value(&errbacks)
                .map_err(|e| CanvasError::Serialization(e.to_string()))?,
        );
    }

    if sig.options.ignore_errors {
        envelope.insert(IGNORE_ERRORS_KEY.to_string(), serde_json::Value::Bool(true));
    }

    if let Some(policy) = retry_policy_value(&sig.options) {
        envelope.insert(RETRY_POLICY_KEY.to_string(), policy);
    }

    serde_json::to_vec(&serde_json::Value::Object(envelope))
        .map_err(|e| CanvasError::Serialization(e.to_string()))
}

/// Turn a [`Signature`] into a [`SerializedTask`], carrying across every option
/// the transport is able to express.
///
/// * `args`/`kwargs` become the payload envelope.
/// * `chain_tail` (may be empty) rides along in the payload and its head's name
///   is written into `metadata.on_success_link`.
/// * `priority`, `time_limit`/`soft_time_limit` and `max_retries` are copied
///   onto the metadata.
/// * an explicit [`TaskOptions::task_id`] becomes the message's id, so a caller
///   that pre-assigned one (a chord barrier registering its members before they
///   are dispatched) can actually correlate the result.
/// * [`TaskOptions::chord_id`] becomes the message's `chord_id`.
///
/// Scheduling (`countdown`/`eta`) is *not* applied here — it selects the
/// enqueue variant and is handled by [`dispatch`] / [`dispatch_all`].
pub fn build_task(
    sig: &Signature,
    chain_tail: &[ChainStep],
) -> Result<SerializedTask, CanvasError> {
    let payload = build_payload(sig, chain_tail)?;
    let mut task = SerializedTask::new(sig.task.clone(), payload);

    // A pre-assigned id is the only way a caller can know a task's identity
    // *before* it is enqueued; a chord registers its barrier against exactly
    // those ids. Ignoring `with_task_id` (as this used to) left the barrier
    // watching ids that no message would ever carry.
    if let Some(task_id) = sig.options.task_id {
        task.metadata.id = task_id;
    }

    if let Some(chord_id) = sig.options.chord_id {
        task.metadata.chord_id = Some(chord_id);
    }

    if let Some(priority) = sig.options.priority {
        task = task.with_priority(priority.into());
    }

    // The link is only a name on the metadata; the payload's `chain` entry
    // carries the successor's arguments and its own successors. A conditional
    // successor has no statically known name, so the field stays unset and the
    // payload tail is the authoritative continuation record.
    if let Some(next_name) = chain_tail.first().and_then(ChainStep::static_task_name) {
        task.metadata.on_success_link = Some(next_name.to_string());
    }

    if let Some(limit) = sig.options.time_limit.or(sig.options.soft_time_limit) {
        task.metadata.timeout_secs = Some(limit);
    }

    if let Some(max_retries) = sig.options.max_retries {
        task.metadata.max_retries = max_retries;
    }

    Ok(task)
}

/// Hand a single already-built task to the broker using the enqueue variant the
/// `schedule` calls for.
///
/// Returns the task's id. Brokers without native scheduling fall back to
/// immediate enqueue through the [`Broker`] trait's default implementations, so
/// this is always safe to call.
pub async fn dispatch<B: Broker>(
    broker: &B,
    task: SerializedTask,
    schedule: Schedule,
) -> Result<Uuid, CanvasError> {
    let task_id = task.metadata.id;

    let enqueued = match schedule {
        Schedule::Immediate => broker.enqueue(task).await,
        Schedule::After(delay_secs) => broker.enqueue_after(task, delay_secs).await,
        Schedule::At(execute_at) => broker.enqueue_at(task, execute_at).await,
    };

    enqueued.map_err(|e| CanvasError::Broker(e.to_string()))?;

    Ok(task_id)
}

/// Build and dispatch a single signature in one step.
pub async fn dispatch_signature<B: Broker>(
    broker: &B,
    sig: &Signature,
    chain_tail: &[ChainStep],
) -> Result<Uuid, CanvasError> {
    let task = build_task(sig, chain_tail)?;
    dispatch(broker, task, Schedule::from_options(&sig.options)).await
}

/// Dispatch a batch of tasks, preserving the caller's order in the returned ids.
///
/// Tasks that are due immediately are handed to [`Broker::enqueue_batch`] —
/// real brokers implement it as a pipeline (Redis), a single transaction
/// (Postgres) or a batch publish (SQS), which turns N round trips into one.
/// Deferred tasks cannot participate in the batch (they need the scheduling
/// enqueue variants) and are dispatched individually afterwards.
///
/// # Chunking
///
/// An immediate batch larger than [`MAX_ENQUEUE_BATCH`] is split into chunks
/// of at most that size, each its own `enqueue_batch` call, so one oversized
/// fan-out cannot exceed a broker's message-size or pipeline limits. Order is
/// preserved across chunks, and the ids returned to the caller are unaffected
/// either way — they are captured up front, before the immediate/deferred
/// split.
///
/// **Chunking is skipped when any task in the batch carries a `chord_id`.** A
/// chord header is dispatched only after
/// [`ChordState`](celers_backend_redis::ChordState) has already been written
/// with `total` set to the *full* header count (see
/// [`crate::Chord::register_and_dispatch`]), so the barrier is live before the
/// first task goes out. If the header were split across chunks and a later
/// chunk failed, the earlier chunks would already be enqueued against a
/// barrier that can now never reach `total`: the callback would never fire and
/// the chord would hang instead of failing loudly. Sending the whole header in
/// one `enqueue_batch` call keeps it atomic-or-nothing, matching what the
/// barrier was registered to expect. A plain (non-chord) fan-out has no such
/// barrier, so chunking it is safe: a failed chunk simply surfaces a
/// [`CanvasError`] to the caller with a partially-dispatched group, exactly as
/// a failed single-call `enqueue_batch` would today.
pub async fn dispatch_all<B: Broker>(
    broker: &B,
    tasks: Vec<(SerializedTask, Schedule)>,
) -> Result<Vec<Uuid>, CanvasError> {
    if tasks.is_empty() {
        return Ok(Vec::new());
    }

    let mut task_ids = Vec::with_capacity(tasks.len());
    let mut immediate = Vec::with_capacity(tasks.len());
    let mut deferred = Vec::new();

    for (task, schedule) in tasks {
        task_ids.push(task.metadata.id);
        if schedule.is_deferred() {
            deferred.push((task, schedule));
        } else {
            immediate.push(task);
        }
    }

    if !immediate.is_empty() {
        // A chord header's barrier is registered for the full count before
        // any task is enqueued (see `Chord::register_and_dispatch`); splitting
        // it across chunks could leave that barrier permanently short of
        // `total` if a later chunk failed. Keep it as one call.
        let is_chord_header = immediate
            .iter()
            .any(|task| task.metadata.chord_id.is_some());

        if is_chord_header || immediate.len() <= MAX_ENQUEUE_BATCH {
            broker
                .enqueue_batch(immediate)
                .await
                .map_err(|e| CanvasError::Broker(e.to_string()))?;
        } else {
            for chunk in chunk_owned(immediate, MAX_ENQUEUE_BATCH) {
                broker
                    .enqueue_batch(chunk)
                    .await
                    .map_err(|e| CanvasError::Broker(e.to_string()))?;
            }
        }
    }

    for (task, schedule) in deferred {
        dispatch(broker, task, schedule).await?;
    }

    Ok(task_ids)
}

/// Split `items` into chunks of at most `chunk_size` (minimum 1), preserving
/// order and moving elements out of the original allocation rather than
/// cloning them.
fn chunk_owned<T>(mut items: Vec<T>, chunk_size: usize) -> Vec<Vec<T>> {
    let chunk_size = chunk_size.max(1);
    let mut chunks = Vec::with_capacity(items.len().div_ceil(chunk_size));

    while !items.is_empty() {
        let remainder = if items.len() > chunk_size {
            items.split_off(chunk_size)
        } else {
            Vec::new()
        };
        chunks.push(std::mem::replace(&mut items, remainder));
    }

    chunks
}

/// Build the tasks for a parallel fan-out of `signatures`, stamping every one
/// with `group_id` and (optionally) `chord_id`.
///
/// Returns the tasks paired with their schedules, in the order the signatures
/// were declared — chord barriers rely on that order because the callback
/// receives the header results zipped against the recorded task ids.
pub fn build_fanout(
    signatures: &[Signature],
    group_id: Option<Uuid>,
    chord_id: Option<Uuid>,
) -> Result<Vec<(SerializedTask, Schedule)>, CanvasError> {
    let mut built = Vec::with_capacity(signatures.len());

    for sig in signatures {
        let mut task = build_task(sig, &[])?;
        task.metadata.group_id = group_id;
        // `build_task` may already have stamped a chord id from the signature's
        // own options; the fan-out's chord id wins when there is one, but a
        // `None` here must not erase it.
        if chord_id.is_some() {
            task.metadata.chord_id = chord_id;
        }
        built.push((task, Schedule::from_options(&sig.options)));
    }

    Ok(built)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Signature;

    fn payload_json(task: &SerializedTask) -> serde_json::Value {
        serde_json::from_slice(&task.payload).expect("canvas payload must be valid JSON")
    }

    #[test]
    fn build_task_without_tail_has_no_chain_key_and_no_link() {
        let sig = Signature::new("solo".to_string()).with_args(vec![serde_json::json!(1)]);
        let task = build_task(&sig, &[]).expect("build");

        assert_eq!(task.metadata.name, "solo");
        assert!(task.metadata.on_success_link.is_none());

        let payload = payload_json(&task);
        assert_eq!(payload["args"], serde_json::json!([1]));
        assert!(
            payload.get(CHAIN_TAIL_KEY).is_none(),
            "a task with no successors must not carry a chain tail"
        );
    }

    #[test]
    fn build_task_with_tail_sets_link_and_embeds_full_tail() {
        let head = Signature::new("first".to_string());
        let tail = vec![
            ChainStep::Task(
                Signature::new("second".to_string()).with_args(vec![serde_json::json!("x")]),
            ),
            ChainStep::Task(Signature::new("third".to_string())),
        ];

        let task = build_task(&head, &tail).expect("build");

        assert_eq!(
            task.metadata.on_success_link.as_deref(),
            Some("second"),
            "the link name must be the immediate successor"
        );

        let payload = payload_json(&task);
        let embedded = payload[CHAIN_TAIL_KEY]
            .as_array()
            .expect("chain tail must be a JSON array");
        assert_eq!(embedded.len(), 2, "the whole remaining tail travels along");
        assert_eq!(embedded[0]["step_type"], "task");
        assert_eq!(embedded[0]["task"], "second");
        assert_eq!(embedded[0]["args"], serde_json::json!(["x"]));
        assert_eq!(embedded[1]["task"], "third");
    }

    #[test]
    fn chain_step_roundtrips_through_json_for_every_variant() {
        use crate::{Branch, Condition, Switch};

        let steps = vec![
            ChainStep::Task(Signature::new("plain".to_string())),
            ChainStep::Branch(
                Branch::new(
                    Condition::field_greater_than("count", 10.0),
                    Signature::new("big".to_string()),
                )
                .otherwise(Signature::new("small".to_string())),
            ),
            ChainStep::Switch(
                Switch::new()
                    .case(
                        Condition::field_equals("status", serde_json::json!("ok")),
                        Signature::new("ok_path".to_string()),
                    )
                    .default(Signature::new("fallback".to_string())),
            ),
        ];

        let json = serde_json::to_string(&steps).expect("serialize chain steps");
        let restored: Vec<ChainStep> =
            serde_json::from_str(&json).expect("deserialize chain steps");

        assert_eq!(restored.len(), 3);
        assert_eq!(restored[0].static_task_name(), Some("plain"));
        assert!(!restored[0].is_conditional());
        assert!(restored[1].is_conditional());
        assert!(restored[2].is_conditional());
        assert_eq!(
            restored[1].static_task_name(),
            None,
            "a conditional step has no statically known successor"
        );
    }

    #[test]
    fn conditional_head_leaves_on_success_link_unset() {
        use crate::{Branch, Condition};

        let head = Signature::new("decide".to_string());
        let tail = vec![ChainStep::Branch(Branch::new(
            Condition::truthy(),
            Signature::new("yes".to_string()),
        ))];

        let task = build_task(&head, &tail).expect("build");

        assert!(
            task.metadata.on_success_link.is_none(),
            "a conditional successor must not masquerade as a fixed link"
        );
        let payload = payload_json(&task);
        assert_eq!(payload[CHAIN_TAIL_KEY][0]["step_type"], "branch");
    }

    #[test]
    fn build_task_carries_priority_limits_and_retries() {
        let sig = Signature::new("configured".to_string())
            .with_priority(7)
            .with_time_limit(42);
        let mut sig = sig;
        sig.options.max_retries = Some(9);

        let task = build_task(&sig, &[]).expect("build");

        assert_eq!(task.metadata.priority, 7);
        assert_eq!(task.metadata.timeout_secs, Some(42));
        assert_eq!(task.metadata.max_retries, 9);
    }

    #[test]
    fn schedule_prefers_eta_over_countdown_and_ignores_zero() {
        let mut options = TaskOptions::default();
        assert_eq!(Schedule::from_options(&options), Schedule::Immediate);

        options.countdown = Some(0);
        assert_eq!(
            Schedule::from_options(&options),
            Schedule::Immediate,
            "a zero countdown must not force a scheduling round trip"
        );

        options.countdown = Some(30);
        assert_eq!(Schedule::from_options(&options), Schedule::After(30));

        options.eta = Some(1_700_000_000);
        assert_eq!(
            Schedule::from_options(&options),
            Schedule::At(1_700_000_000),
            "an explicit ETA wins over a countdown"
        );
    }

    #[test]
    fn build_fanout_stamps_group_and_chord_ids_in_declaration_order() {
        let group_id = Uuid::new_v4();
        let chord_id = Uuid::new_v4();
        let sigs = vec![
            Signature::new("a".to_string()),
            Signature::new("b".to_string()),
            Signature::new("c".to_string()),
        ];

        let built = build_fanout(&sigs, Some(group_id), Some(chord_id)).expect("build");

        assert_eq!(built.len(), 3);
        let names: Vec<&str> = built
            .iter()
            .map(|(task, _)| task.metadata.name.as_str())
            .collect();
        assert_eq!(names, vec!["a", "b", "c"], "declaration order is preserved");

        for (task, schedule) in &built {
            assert_eq!(task.metadata.group_id, Some(group_id));
            assert_eq!(task.metadata.chord_id, Some(chord_id));
            assert_eq!(*schedule, Schedule::Immediate);
        }
    }

    #[test]
    fn chunk_owned_splits_at_the_boundary_without_reordering() {
        let items: Vec<u32> = (0..1201).collect();
        let chunks = chunk_owned(items, 500);

        assert_eq!(
            chunks.len(),
            3,
            "1201 items at 500/chunk must yield 3 chunks"
        );
        assert_eq!(chunks[0].len(), 500);
        assert_eq!(chunks[1].len(), 500);
        assert_eq!(chunks[2].len(), 201, "the remainder forms its own chunk");

        let flattened: Vec<u32> = chunks.into_iter().flatten().collect();
        let expected: Vec<u32> = (0..1201).collect();
        assert_eq!(
            flattened, expected,
            "chunking must not reorder, drop, or duplicate items"
        );
    }

    #[test]
    fn chunk_owned_exactly_at_the_boundary_is_a_single_chunk() {
        let items: Vec<u32> = (0..500).collect();
        let chunks = chunk_owned(items, 500);

        assert_eq!(
            chunks.len(),
            1,
            "exactly `chunk_size` items must not spill into a second chunk"
        );
        assert_eq!(chunks[0].len(), 500);
    }

    #[test]
    fn chunk_owned_treats_a_zero_chunk_size_as_one() {
        let chunks = chunk_owned(vec![1, 2, 3], 0);
        assert_eq!(
            chunks,
            vec![vec![1], vec![2], vec![3]],
            "a zero chunk size must not loop forever or panic"
        );
    }

    #[test]
    fn chunk_owned_empty_input_yields_no_chunks() {
        let chunks: Vec<Vec<u32>> = chunk_owned(Vec::new(), 500);
        assert!(chunks.is_empty());
    }

    /// Broker that only records the size of every `enqueue_batch`/`enqueue`
    /// call it receives, so `dispatch_all`'s chunking can be verified without
    /// a real broker.
    #[derive(Default)]
    struct BatchSizeRecordingBroker {
        batch_sizes: std::sync::Mutex<Vec<usize>>,
    }

    impl BatchSizeRecordingBroker {
        fn batch_sizes(&self) -> Vec<usize> {
            self.batch_sizes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }
    }

    #[async_trait::async_trait]
    impl Broker for BatchSizeRecordingBroker {
        async fn enqueue(&self, task: SerializedTask) -> celers_core::Result<celers_core::TaskId> {
            self.batch_sizes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(1);
            Ok(task.metadata.id)
        }

        async fn enqueue_batch(
            &self,
            tasks: Vec<SerializedTask>,
        ) -> celers_core::Result<Vec<celers_core::TaskId>> {
            self.batch_sizes
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(tasks.len());
            Ok(tasks.into_iter().map(|task| task.metadata.id).collect())
        }

        async fn dequeue(&self) -> celers_core::Result<Option<celers_core::BrokerMessage>> {
            Ok(None)
        }

        async fn ack(
            &self,
            _task_id: &celers_core::TaskId,
            _receipt_handle: Option<&str>,
        ) -> celers_core::Result<()> {
            Ok(())
        }

        async fn reject(
            &self,
            _task_id: &celers_core::TaskId,
            _receipt_handle: Option<&str>,
            _requeue: bool,
        ) -> celers_core::Result<()> {
            Ok(())
        }

        async fn queue_size(&self) -> celers_core::Result<usize> {
            Ok(0)
        }

        async fn cancel(&self, _task_id: &celers_core::TaskId) -> celers_core::Result<bool> {
            Ok(false)
        }
    }

    /// Build `count` immediate tasks, all sharing `chord_id` when given.
    fn immediate_tasks(count: usize, chord_id: Option<Uuid>) -> Vec<(SerializedTask, Schedule)> {
        (0..count)
            .map(|i| {
                let mut task = SerializedTask::new(format!("t{i}"), Vec::new());
                task.metadata.chord_id = chord_id;
                (task, Schedule::Immediate)
            })
            .collect()
    }

    #[tokio::test]
    async fn dispatch_all_sends_exactly_the_boundary_count_as_one_batch() {
        let broker = BatchSizeRecordingBroker::default();
        let tasks = immediate_tasks(MAX_ENQUEUE_BATCH, None);

        let ids = dispatch_all(&broker, tasks).await.expect("dispatch");

        assert_eq!(ids.len(), MAX_ENQUEUE_BATCH);
        assert_eq!(
            broker.batch_sizes(),
            vec![MAX_ENQUEUE_BATCH],
            "exactly MAX_ENQUEUE_BATCH tasks must fit in a single enqueue_batch call"
        );
    }

    #[tokio::test]
    async fn dispatch_all_chunks_one_task_past_the_boundary() {
        let broker = BatchSizeRecordingBroker::default();
        let tasks = immediate_tasks(MAX_ENQUEUE_BATCH + 1, None);

        let ids = dispatch_all(&broker, tasks).await.expect("dispatch");

        assert_eq!(ids.len(), MAX_ENQUEUE_BATCH + 1);
        assert_eq!(
            broker.batch_sizes(),
            vec![MAX_ENQUEUE_BATCH, 1],
            "one task past the boundary must spill into its own second call"
        );
    }

    #[tokio::test]
    async fn dispatch_all_never_chunks_a_chord_header_even_when_oversized() {
        let broker = BatchSizeRecordingBroker::default();
        let chord_id = Uuid::new_v4();
        let tasks = immediate_tasks(MAX_ENQUEUE_BATCH + 200, Some(chord_id));

        let ids = dispatch_all(&broker, tasks).await.expect("dispatch");

        assert_eq!(ids.len(), MAX_ENQUEUE_BATCH + 200);
        assert_eq!(
            broker.batch_sizes(),
            vec![MAX_ENQUEUE_BATCH + 200],
            "a chord header must go out in one call so its barrier (registered \
             for the full count before dispatch) can never be short an entry"
        );
    }

    #[tokio::test]
    async fn dispatch_all_preserves_order_across_chunk_boundaries() {
        let broker = BatchSizeRecordingBroker::default();
        let tasks: Vec<(SerializedTask, Schedule)> = (0..(MAX_ENQUEUE_BATCH + 50))
            .map(|i| {
                (
                    SerializedTask::new(format!("t{i}"), Vec::new()),
                    Schedule::Immediate,
                )
            })
            .collect();
        let expected_ids: Vec<Uuid> = tasks.iter().map(|(task, _)| task.metadata.id).collect();

        let ids = dispatch_all(&broker, tasks).await.expect("dispatch");

        assert_eq!(
            ids, expected_ids,
            "returned ids must preserve the caller's original order across chunks"
        );
    }
    #[test]
    fn build_task_embeds_the_error_route_as_chain_steps() {
        // A failure route is *not* a chain tail: it must travel in its own key,
        // or a fallback handler becomes a following step and runs on success.
        let sig = Signature::new("primary".to_string())
            .with_link_error(Signature::new("fallback".to_string()))
            .add_link_error(Signature::new("notify".to_string()));

        let task = build_task(&sig, &[]).expect("build");
        let payload = payload_json(&task);

        assert!(
            payload.get(CHAIN_TAIL_KEY).is_none(),
            "an error handler must never appear in the success tail"
        );
        let route = payload[ERROR_ROUTE_KEY]
            .as_array()
            .expect("the error route is a JSON array");
        assert_eq!(route.len(), 2);
        assert_eq!(route[0]["step_type"], "task");
        assert_eq!(route[0]["task"], "fallback");
        assert_eq!(
            route[1]["task"], "notify",
            "`link_error` comes first, then `link_errors` in declaration order"
        );
        assert!(
            task.metadata.on_success_link.is_none(),
            "an error handler is not a success link"
        );
    }

    #[test]
    fn build_task_omits_the_failure_keys_when_nothing_is_declared() {
        let task = build_task(&Signature::new("plain".to_string()), &[]).expect("build");
        let payload = payload_json(&task);

        assert!(payload.get(ERROR_ROUTE_KEY).is_none());
        assert!(payload.get(IGNORE_ERRORS_KEY).is_none());
        assert!(payload.get(RETRY_POLICY_KEY).is_none());
    }

    #[test]
    fn build_task_carries_ignore_errors_and_the_retry_policy() {
        let sig = Signature::new("flaky".to_string())
            .ignoring_errors()
            .with_retry_delay(2)
            .with_retry_backoff(2.0)
            .with_retry_backoff_max(60);

        let payload = payload_json(&build_task(&sig, &[]).expect("build"));

        assert_eq!(payload[IGNORE_ERRORS_KEY], serde_json::json!(true));
        assert_eq!(payload[RETRY_POLICY_KEY]["delay"], serde_json::json!(2));
        assert_eq!(payload[RETRY_POLICY_KEY]["backoff"], serde_json::json!(2.0));
        assert_eq!(
            payload[RETRY_POLICY_KEY]["max_delay"],
            serde_json::json!(60)
        );
    }

    #[test]
    fn build_task_honours_a_pre_assigned_id_and_chord_id() {
        // A barrier registered before dispatch can only recognise its members
        // if the ids it recorded are the ones the messages carry.
        let task_id = Uuid::new_v4();
        let chord_id = Uuid::new_v4();
        let sig = Signature::new("member".to_string())
            .with_task_id(task_id)
            .with_chord_id(chord_id);

        let task = build_task(&sig, &[]).expect("build");

        assert_eq!(task.metadata.id, task_id);
        assert_eq!(task.metadata.chord_id, Some(chord_id));
    }

    #[test]
    fn build_fanout_does_not_erase_a_signature_chord_id() {
        let chord_id = Uuid::new_v4();
        let signatures = vec![Signature::new("member".to_string()).with_chord_id(chord_id)];

        let built = build_fanout(&signatures, Some(Uuid::new_v4()), None).expect("build");

        assert_eq!(
            built[0].0.metadata.chord_id,
            Some(chord_id),
            "a plain fan-out must not clear a chord id the signature set itself"
        );
    }
}
