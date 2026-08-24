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
//!   "args":   [ ... ],
//!   "kwargs": { ... },
//!   "chain":  [ <ChainStep>, <ChainStep>, ... ]
//! }
//! ```
//!
//! `args`/`kwargs` are the task's own arguments. `chain` is present only when
//! the task is a non-final step of a chain and carries the **entire remaining
//! tail** as serialized [`ChainStep`]s.
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

/// Upper bound (in seconds) for a generated dispatch countdown: 30 days.
///
/// Countdown-producing helpers ([`Chain::with_staggered_countdown`](crate::Chain::with_staggered_countdown),
/// [`Group::skew`](crate::Group::skew), [`Group::jitter`](crate::Group::jitter))
/// take unvalidated `u64` inputs from public APIs. Clamping keeps their
/// arithmetic free of overflow panics and keeps the values within a range a
/// broker's delayed-delivery support can plausibly honour.
pub const MAX_COUNTDOWN_SECS: u64 = 30 * 24 * 60 * 60;

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

/// Build the JSON payload envelope for `sig`, embedding `chain_tail` when the
/// signature is a non-final chain step.
fn build_payload(sig: &Signature, chain_tail: &[ChainStep]) -> Result<Vec<u8>, CanvasError> {
    let mut envelope = serde_json::Map::with_capacity(3);
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
///
/// Scheduling (`countdown`/`eta`) is *not* applied here — it selects the
/// enqueue variant and is handled by [`dispatch`] / [`dispatch_all`].
pub fn build_task(
    sig: &Signature,
    chain_tail: &[ChainStep],
) -> Result<SerializedTask, CanvasError> {
    let payload = build_payload(sig, chain_tail)?;
    let mut task = SerializedTask::new(sig.task.clone(), payload);

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
/// Tasks that are due immediately are handed to [`Broker::enqueue_batch`] in one
/// call — real brokers implement it as a pipeline (Redis), a single transaction
/// (Postgres) or a batch publish (SQS), which turns N round trips into one.
/// Deferred tasks cannot participate in the batch (they need the scheduling
/// enqueue variants) and are dispatched individually afterwards.
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
        broker
            .enqueue_batch(immediate)
            .await
            .map_err(|e| CanvasError::Broker(e.to_string()))?;
    }

    for (task, schedule) in deferred {
        dispatch(broker, task, schedule).await?;
    }

    Ok(task_ids)
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
        task.metadata.chord_id = chord_id;
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
}
