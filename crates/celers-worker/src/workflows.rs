//! Workflow support for Canvas primitives
//!
//! This module provides worker integration for Chain and Chord workflows.
//!
//! # Chain continuation wire format
//!
//! `celers-canvas` dispatches a chain by enqueuing only its head task and
//! attaching the **entire remaining tail** to that task's payload:
//!
//! ```json
//! {
//!   "args":   [ ... ],
//!   "kwargs": { ... },
//!   "chain":  [ {"step_type": "task",   "task": "step2", ...},
//!               {"step_type": "branch", "condition": {...}, ...} ]
//! }
//! ```
//!
//! The tail has to live in the payload because
//! [`TaskMetadata::on_success_link`](celers_core::TaskMetadata::on_success_link)
//! is only an `Option<String>`: a task *name*, with nowhere to put the
//! successor's args, kwargs, priority, countdown — or its own successor. With
//! only the name available, a three-task chain would run its first task and
//! silently drop the rest.
//!
//! On successful completion this module pops the head of that tail, applies the
//! finished task's result to it, and re-enqueues it with the remainder still
//! attached, so an N-step chain runs all N steps in order. A `branch`/`switch`
//! step is instead *evaluated* against the result and the selected arm becomes
//! the next task.
//!
//! Tasks that carry an `on_success_link` but no `chain` tail (anything built
//! straight from [`SerializedTask::with_on_success_link`]) still take the
//! original path: the named task is enqueued with the raw result bytes.

use crate::security::SignatureVerification;
use celers_core::{Broker, SerializedTask};
// `warn!` is fully qualified at its call sites: every one of them lives behind a
// feature gate, and importing it would make the import itself unused in some
// feature combinations.
use tracing::{debug, info};

#[cfg(feature = "workflows")]
use celers_backend_redis::ResultBackend;

#[cfg(not(feature = "workflows"))]
pub trait ResultBackend {}

/// End-to-end execution of the advanced canvas patterns (saga rollback,
/// pipeline, fan-out, fan-in, scatter-gather), driven through a real broker by
/// this module and [`crate::error_links`].
#[cfg(all(test, feature = "workflows"))]
mod patterns_e2e;

/// JSON key under which `celers-canvas` embeds the remaining chain tail.
///
/// Mirrors `celers_canvas::CHAIN_TAIL_KEY`; the two constants are the contract
/// between the canvas producer and this consumer. It is duplicated rather than
/// imported because this module is compiled even when the optional `canvas`
/// dependency is not enabled.
pub const CHAIN_TAIL_KEY: &str = "chain";

/// Handle task completion for workflow primitives.
///
/// Equivalent to [`handle_workflow_completion_signed`] with no signer: use it
/// when the worker does not authenticate messages. If it does, the continuations
/// this enqueues would be unsigned and rejected on their own delivery.
#[cfg(feature = "workflows")]
pub async fn handle_workflow_completion<B: Broker>(
    task: &SerializedTask,
    result: &[u8],
    broker: &B,
    backend: Option<&mut (dyn ResultBackend + 'static)>,
) -> Result<(), WorkflowError> {
    handle_workflow_completion_signed(task, result, broker, backend, None).await
}

/// Handle task completion for workflow primitives.
///
/// Equivalent to [`handle_workflow_completion_signed`] with no signer: use it
/// when the worker does not authenticate messages. If it does, the continuations
/// this enqueues would be unsigned and rejected on their own delivery.
#[cfg(not(feature = "workflows"))]
pub async fn handle_workflow_completion<B: Broker>(
    task: &SerializedTask,
    result: &[u8],
    broker: &B,
) -> Result<(), WorkflowError> {
    handle_workflow_completion_signed(task, result, broker, None).await
}

/// Sign a continuation this worker is about to enqueue.
///
/// A chain successor, an `on_success_link` target and a chord callback are all
/// *newly constructed* messages: they carry no producer signature. A worker
/// that verifies signatures would enqueue one and then reject its own delivery
/// of it, ending every workflow at its first hop. Signing here — with the same
/// key the worker verifies against, and a fresh `signed_at`/nonce — is what
/// makes workflows and message authentication compose.
///
/// A no-op (a single `Option` check) when the worker does not verify.
pub(crate) fn sign_continuation(
    task: &mut SerializedTask,
    signature: Option<&SignatureVerification>,
) {
    if let Some(verification) = signature {
        verification.sign(task);
    }
}

/// Handle task completion for workflow primitives, signing every continuation.
///
/// This function should be called after a task completes successfully.
/// It checks for workflow metadata (chain links, chord membership) and
/// triggers appropriate workflow actions.
///
/// `signature` is the worker's
/// [`SignatureVerification`](crate::SignatureVerification) when it authenticates
/// messages, and `None` otherwise. See [`sign_continuation`] for why it has to
/// be threaded through here rather than left to the producer.
/// [`handle_workflow_completion`] is the `None` case.
///
/// # Chain Callback Execution
///
/// If the task has an `on_success_link` in its metadata, enqueue the next
/// task in the chain with the current task's result bytes as the payload.
///
/// # Chord Barrier Synchronization
///
/// If the task has a `chord_id` in its metadata, increment the completion
/// counter in the result backend. When all tasks complete, trigger the callback.
pub async fn handle_workflow_completion_signed<B: Broker>(
    task: &SerializedTask,
    _result: &[u8],
    broker: &B,
    #[cfg(feature = "workflows")] backend: Option<&mut (dyn ResultBackend + 'static)>,
    signature: Option<&SignatureVerification>,
) -> Result<(), WorkflowError> {
    let task_id = task.metadata.id;

    // Handle chord completion (barrier synchronization)
    #[cfg(feature = "workflows")]
    if let Some(chord_id) = task.metadata.chord_id {
        debug!("Task {} is part of chord {}", task_id, chord_id);

        if let Some(backend) = backend {
            // Record this member's result *before* counting it. The callback is
            // enqueued the moment the counter reaches `total`, and it is handed
            // `chord_get_partial_results`; a result written after the increment
            // would race the callback and arrive as a `null` in the aggregate.
            // Nothing else writes into the barrier store, so without this every
            // chord callback received a list of nulls.
            let mut meta = celers_backend_redis::TaskMeta::new(task_id, task.metadata.name.clone());
            meta.result = celers_backend_redis::TaskResult::Success(result_to_json(_result));
            meta.completed_at = Some(chrono::Utc::now());
            if let Err(e) = backend.store_result(task_id, &meta).await {
                tracing::warn!(
                    "Failed to record chord member {}'s result for chord {}: {}; \
                     the callback will see a null in its place",
                    task_id,
                    chord_id,
                    e
                );
            }

            // Atomically increment the completion counter
            let count = backend.chord_complete_task(chord_id).await.map_err(|e| {
                WorkflowError::Backend(format!("Failed to increment chord counter: {}", e))
            })?;

            // Get chord state to check if all tasks are complete
            let state = backend
                .chord_get_state(chord_id)
                .await
                .map_err(|e| WorkflowError::Backend(format!("Failed to get chord state: {}", e)))?;

            if let Some(state) = state {
                if count >= state.total {
                    info!(
                        "Chord {} complete ({}/{}) - ready to trigger callback",
                        chord_id, count, state.total
                    );

                    // Enqueue callback task if specified
                    if let Some(callback_name) = state.callback {
                        info!("Enqueuing chord callback task: {}", callback_name);

                        // Collect the individual header-task results so the chord
                        // callback receives them as its first positional argument,
                        // matching Celery semantics. Results are returned in the
                        // chord's `task_ids` order by the backend.
                        let partial_results = backend
                            .chord_get_partial_results(chord_id)
                            .await
                            .map_err(|e| {
                                WorkflowError::Backend(format!(
                                    "Failed to collect chord results: {}",
                                    e
                                ))
                            })?;

                        // Convert each task's result into its return value. Missing,
                        // pending, or failed results are represented as JSON `null`
                        // so a single absent result does not abort the callback.
                        let result_values: Vec<serde_json::Value> = partial_results
                            .into_iter()
                            .map(|(task_id, meta)| match meta {
                                Some(meta) => match meta.result.success_value() {
                                    Some(value) => value.clone(),
                                    None => {
                                        debug!(
                                            "Chord {} task {} has no success value ({}); using null",
                                            chord_id, task_id, meta.result
                                        );
                                        serde_json::Value::Null
                                    }
                                },
                                None => {
                                    debug!(
                                        "Chord {} task {} result missing; using null",
                                        chord_id, task_id
                                    );
                                    serde_json::Value::Null
                                }
                            })
                            .collect();

                        // Create callback task with aggregated results: the list of
                        // header-task results becomes the first positional argument.
                        let callback_args = serde_json::json!({
                            "args": [serde_json::Value::Array(result_values)],
                            "kwargs": {}
                        });
                        let args_bytes = serde_json::to_vec(&callback_args)
                            .map_err(|e| WorkflowError::Serialization(e.to_string()))?;

                        let mut callback_task =
                            celers_core::SerializedTask::new(callback_name, args_bytes);
                        sign_continuation(&mut callback_task, signature);

                        broker
                            .enqueue(callback_task)
                            .await
                            .map_err(|e| WorkflowError::Broker(e.to_string()))?;

                        info!("Chord callback enqueued successfully");
                    }
                } else {
                    debug!(
                        "Chord {} progress: {}/{} tasks complete",
                        chord_id, count, state.total
                    );
                }
            } else {
                tracing::warn!("Chord state not found for chord_id {}", chord_id);
            }
        } else {
            tracing::warn!("Task has chord_id but no backend configured - cannot track completion");
        }
    }

    // Handle chain continuation.
    //
    // Preferred path: the canvas chain tail embedded in this task's payload,
    // which carries the successor's full signature (args, kwargs, options) plus
    // every step after it. Fallback: a bare `on_success_link` name, for tasks
    // built directly with `SerializedTask::with_on_success_link`.
    let tail = parse_chain_tail(&task.payload);

    if tail.is_empty() {
        if let Some(ref link_name) = task.metadata.on_success_link {
            debug!(
                "Task {} has on_success_link to '{}' with no chain tail, \
                 enqueuing the link with the raw result payload",
                task_id, link_name
            );

            // Build the next task; its payload is this task's result bytes
            let mut next_task =
                celers_core::SerializedTask::new(link_name.clone(), _result.to_vec());
            sign_continuation(&mut next_task, signature);

            broker.enqueue(next_task).await.map_err(|e| {
                WorkflowError::Broker(format!(
                    "Failed to enqueue chain link '{}': {}",
                    link_name, e
                ))
            })?;

            info!("Chain link '{}' enqueued from task {}", link_name, task_id);
        }

        return Ok(());
    }

    let result_value = result_to_json(_result);
    let (head, remaining) = tail.split_at(1);

    let Some(resolved) = resolve_next_step(&head[0], &result_value)? else {
        // A conditional whose arms all evaluated false (and which has no
        // default) legitimately ends the chain here.
        debug!(
            "Task {} chain tail resolved to no successor; chain ends",
            task_id
        );
        return Ok(());
    };

    let (mut next_task, schedule) = build_next_task(&resolved, remaining, &result_value)?;
    sign_continuation(&mut next_task, signature);
    let next_name = next_task.metadata.name.clone();

    dispatch_next(broker, next_task, schedule).await?;

    info!(
        "Chain step '{}' enqueued from task {} ({} step(s) remaining)",
        next_name,
        task_id,
        remaining.len()
    );

    Ok(())
}

/// When the continuation task should be handed to the broker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum Schedule {
    /// Enqueue right away.
    #[default]
    Immediate,
    /// Enqueue with a relative delay in seconds.
    After(u64),
    /// Enqueue at an absolute Unix timestamp in seconds.
    At(i64),
}

/// Extract the canvas chain tail from a task payload.
///
/// Returns an empty vector for payloads that are not JSON, are not an object,
/// or carry no `chain` key — i.e. everything that is not a canvas chain step.
pub(crate) fn parse_chain_tail(payload: &[u8]) -> Vec<serde_json::Value> {
    let Ok(envelope) = serde_json::from_slice::<serde_json::Value>(payload) else {
        return Vec::new();
    };

    match envelope
        .get(CHAIN_TAIL_KEY)
        .and_then(|tail| tail.as_array())
    {
        Some(steps) => steps.clone(),
        None => Vec::new(),
    }
}

/// Interpret a task's raw result bytes as JSON.
///
/// Chain steps receive their predecessor's return value as an argument, which
/// means it has to become a `serde_json::Value`. Results that are already JSON
/// are used as-is; anything else degrades gracefully (UTF-8 text becomes a JSON
/// string, arbitrary bytes become an array of byte values) rather than aborting
/// the chain.
pub(crate) fn result_to_json(result: &[u8]) -> serde_json::Value {
    if result.is_empty() {
        return serde_json::Value::Null;
    }

    match serde_json::from_slice::<serde_json::Value>(result) {
        Ok(value) => value,
        Err(_) => match std::str::from_utf8(result) {
            Ok(text) => serde_json::Value::String(text.to_string()),
            Err(_) => serde_json::Value::Array(
                result
                    .iter()
                    .map(|byte| serde_json::Value::from(*byte))
                    .collect(),
            ),
        },
    }
}

/// A chain step resolved against the predecessor's result.
pub(crate) struct ResolvedStep {
    /// The signature to run, as JSON.
    pub(crate) signature: serde_json::Value,
    /// Whether the predecessor's result has already been folded into the
    /// signature's arguments (conditional steps do this while evaluating).
    pub(crate) result_applied: bool,
}

/// Resolve one chain-tail entry into the signature to run next.
///
/// Plain `task` steps resolve to themselves. `branch`/`switch` steps are
/// evaluated against `result`; they resolve to the selected arm, or to `None`
/// when no arm matches and no default is configured (which ends the chain).
pub(crate) fn resolve_next_step(
    step: &serde_json::Value,
    result: &serde_json::Value,
) -> Result<Option<ResolvedStep>, WorkflowError> {
    let step_type = step
        .get("step_type")
        .and_then(serde_json::Value::as_str)
        // A bare signature object (no discriminator) is a task step.
        .unwrap_or("task");

    match step_type {
        "task" => {
            if step
                .get("task")
                .and_then(serde_json::Value::as_str)
                .is_none()
            {
                return Err(WorkflowError::Serialization(
                    "chain step is missing its `task` name".to_string(),
                ));
            }
            Ok(Some(ResolvedStep {
                signature: step.clone(),
                result_applied: false,
            }))
        }
        "branch" | "switch" => evaluate_conditional_step(step_type, step, result),
        other => Err(WorkflowError::Serialization(format!(
            "unknown chain step type '{}'",
            other
        ))),
    }
}

/// Evaluate a `branch`/`switch` chain step against the predecessor's result.
#[cfg(feature = "canvas")]
fn evaluate_conditional_step(
    step_type: &str,
    step: &serde_json::Value,
    result: &serde_json::Value,
) -> Result<Option<ResolvedStep>, WorkflowError> {
    // `Branch`/`Switch` already fold the result into the selected arm's
    // arguments (honouring `pass_result` and the arm's `immutable` flag), so
    // the resolved signature must not have it applied a second time.
    let selected = if step_type == "branch" {
        let branch: celers_canvas::Branch = serde_json::from_value(step.clone())
            .map_err(|e| WorkflowError::Serialization(format!("invalid branch step: {}", e)))?;
        branch.evaluate(result)
    } else {
        let switch: celers_canvas::Switch = serde_json::from_value(step.clone())
            .map_err(|e| WorkflowError::Serialization(format!("invalid switch step: {}", e)))?;
        switch.evaluate(result)
    };

    match selected {
        Some(sig) => {
            let signature = serde_json::to_value(sig).map_err(|e| {
                WorkflowError::Serialization(format!("failed to encode selected arm: {}", e))
            })?;
            Ok(Some(ResolvedStep {
                signature,
                result_applied: true,
            }))
        }
        None => Ok(None),
    }
}

/// Without the `canvas` feature the worker cannot evaluate conditions, so a
/// conditional step ends the chain with a warning instead of guessing an arm.
#[cfg(not(feature = "canvas"))]
fn evaluate_conditional_step(
    step_type: &str,
    _step: &serde_json::Value,
    _result: &serde_json::Value,
) -> Result<Option<ResolvedStep>, WorkflowError> {
    tracing::warn!(
        "Chain contains a '{}' step but this worker was built without the `canvas` feature, \
         so the condition cannot be evaluated; the chain stops here",
        step_type
    );
    Ok(None)
}

/// Build the continuation task from a resolved signature plus the remaining
/// chain tail, folding in the predecessor's result.
pub(crate) fn build_next_task(
    resolved: &ResolvedStep,
    remaining: &[serde_json::Value],
    result: &serde_json::Value,
) -> Result<(SerializedTask, Schedule), WorkflowError> {
    let sig = &resolved.signature;

    let name = sig
        .get("task")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| {
            WorkflowError::Serialization("chain step is missing its `task` name".to_string())
        })?
        .to_string();

    let mut args: Vec<serde_json::Value> = sig
        .get("args")
        .and_then(serde_json::Value::as_array)
        .cloned()
        .unwrap_or_default();

    let mut kwargs: serde_json::Map<String, serde_json::Value> = sig
        .get("kwargs")
        .and_then(serde_json::Value::as_object)
        .cloned()
        .unwrap_or_default();

    let immutable = sig
        .get("immutable")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false);

    let options = sig.get("options");

    if !resolved.result_applied && !immutable {
        apply_result_to_args(options, result, &mut args, &mut kwargs);
    }

    let mut envelope = serde_json::Map::with_capacity(6);
    envelope.insert("args".to_string(), serde_json::Value::Array(args));
    envelope.insert("kwargs".to_string(), serde_json::Value::Object(kwargs));
    if !remaining.is_empty() {
        envelope.insert(
            CHAIN_TAIL_KEY.to_string(),
            serde_json::Value::Array(remaining.to_vec()),
        );
    }

    // The producer put the *whole* signature — including its failure route,
    // its suppression flag and its own retry policy — into the chain tail. If
    // this rebuild dropped them, only the head of a chain would ever have an
    // error link: a saga's second and third steps would fail with nothing to
    // roll back, which is precisely where a saga needs rollback.
    crate::error_links::carry_failure_options(options, &mut envelope);

    let payload = serde_json::to_vec(&serde_json::Value::Object(envelope))
        .map_err(|e| WorkflowError::Serialization(e.to_string()))?;

    let mut next = SerializedTask::new(name, payload);

    // Carry the successor's link name so the chain stays introspectable; a
    // conditional successor has no statically known name.
    if let Some(next_name) = remaining
        .first()
        .filter(|step| {
            step.get("step_type")
                .and_then(serde_json::Value::as_str)
                .unwrap_or("task")
                == "task"
        })
        .and_then(|step| step.get("task"))
        .and_then(serde_json::Value::as_str)
    {
        next.metadata.on_success_link = Some(next_name.to_string());
    }

    if let Some(options) = options {
        if let Some(priority) = options.get("priority").and_then(serde_json::Value::as_i64) {
            next.metadata.priority = i32::try_from(priority).unwrap_or(i32::MAX);
        }
        let limit = options
            .get("time_limit")
            .and_then(serde_json::Value::as_u64)
            .or_else(|| {
                options
                    .get("soft_time_limit")
                    .and_then(serde_json::Value::as_u64)
            });
        if let Some(limit) = limit {
            next.metadata.timeout_secs = Some(limit);
        }
        if let Some(max_retries) = options
            .get("max_retries")
            .and_then(serde_json::Value::as_u64)
        {
            next.metadata.max_retries = u32::try_from(max_retries).unwrap_or(u32::MAX);
        }
        // A pre-assigned id lets a barrier registered before dispatch recognise
        // this very message; a chord id makes this step the one that completes
        // a chord member. Only a *chain's final step* normally carries either,
        // which is what lets a chord member be a whole chain and still count
        // exactly once.
        if let Some(task_id) = options
            .get("task_id")
            .and_then(serde_json::Value::as_str)
            .and_then(|raw| uuid::Uuid::parse_str(raw).ok())
        {
            next.metadata.id = task_id;
        }
        if let Some(chord_id) = options
            .get("chord_id")
            .and_then(serde_json::Value::as_str)
            .and_then(|raw| uuid::Uuid::parse_str(raw).ok())
        {
            next.metadata.chord_id = Some(chord_id);
        }
    }

    Ok((next, schedule_from_options(options)))
}

/// Fold the predecessor's result into the successor's arguments according to
/// the successor's `callback_arg_mode`.
fn apply_result_to_args(
    options: Option<&serde_json::Value>,
    result: &serde_json::Value,
    args: &mut Vec<serde_json::Value>,
    kwargs: &mut serde_json::Map<String, serde_json::Value>,
) {
    let mode = options
        .and_then(|opts| opts.get("callback_arg_mode"))
        .and_then(serde_json::Value::as_str)
        .unwrap_or("Prepend");

    match mode {
        // Celery semantics: the parent's result is the first positional arg.
        "Prepend" => args.insert(0, result.clone()),
        "Append" => args.push(result.clone()),
        "Kwarg" => {
            let key = options
                .and_then(|opts| opts.get("callback_kwarg_key"))
                .and_then(serde_json::Value::as_str)
                .unwrap_or("result")
                .to_string();
            kwargs.insert(key, result.clone());
        }
        // "None" (and anything unrecognised) leaves the arguments untouched.
        _ => {}
    }
}

/// Derive the continuation's schedule from its options.
fn schedule_from_options(options: Option<&serde_json::Value>) -> Schedule {
    let Some(options) = options else {
        return Schedule::Immediate;
    };

    if let Some(eta) = options.get("eta").and_then(serde_json::Value::as_i64) {
        return Schedule::At(eta);
    }

    match options.get("countdown").and_then(serde_json::Value::as_u64) {
        Some(countdown) if countdown > 0 => Schedule::After(countdown),
        _ => Schedule::Immediate,
    }
}

/// Enqueue the continuation task using the scheduling variant it asks for.
pub(crate) async fn dispatch_next<B: Broker>(
    broker: &B,
    task: SerializedTask,
    schedule: Schedule,
) -> Result<(), WorkflowError> {
    let name = task.metadata.name.clone();

    let enqueued = match schedule {
        Schedule::Immediate => broker.enqueue(task).await,
        Schedule::After(delay_secs) => broker.enqueue_after(task, delay_secs).await,
        Schedule::At(execute_at) => broker.enqueue_at(task, execute_at).await,
    };

    enqueued.map_err(|e| {
        WorkflowError::Broker(format!("Failed to enqueue chain step '{}': {}", name, e))
    })?;

    Ok(())
}

/// Workflow handling errors
#[derive(Debug, thiserror::Error)]
pub enum WorkflowError {
    #[error("Backend error: {0}")]
    Backend(String),

    #[error("Broker error: {0}")]
    Broker(String),

    #[error("Serialization error: {0}")]
    Serialization(String),
}

#[cfg(test)]
mod tests {
    // Everything these tests need — `SerializedTask`, `Broker`,
    // `handle_workflow_completion` — comes through this one glob, so the import
    // stays used in every feature combination (the feature-gated submodules
    // below are compiled out in some of them).
    use super::*;

    /// Verify that `on_success_link` roundtrips correctly through
    /// `SerializedTask` serialization/deserialization.
    #[test]
    fn test_workflow_no_link_serialization_roundtrip() {
        let task = SerializedTask::new("my_task".to_string(), b"payload".to_vec());

        // No on_success_link set — field must be absent after serde roundtrip
        let json = serde_json::to_string(&task).expect("serialize");
        let restored: SerializedTask = serde_json::from_str(&json).expect("deserialize");

        assert!(
            restored.metadata.on_success_link.is_none(),
            "on_success_link should be None when never set"
        );
        // The field must be absent from the JSON (skip_serializing_if)
        assert!(
            !json.contains("on_success_link"),
            "on_success_link should be omitted from JSON when None"
        );
    }

    /// Verify that a task with `on_success_link = "next_task"` roundtrips the
    /// field correctly, preserving the link name through serde JSON.
    #[test]
    fn test_workflow_chain_link_serialization_roundtrip() {
        let task = SerializedTask::new("step_a".to_string(), b"result_bytes".to_vec())
            .with_on_success_link("next_task".to_string());

        assert_eq!(
            task.metadata.on_success_link.as_deref(),
            Some("next_task"),
            "on_success_link must be Some(\"next_task\") before serialization"
        );

        let json = serde_json::to_string(&task).expect("serialize");

        // The field must be present in the JSON
        assert!(
            json.contains("on_success_link"),
            "on_success_link must appear in JSON when set"
        );
        assert!(
            json.contains("next_task"),
            "link name must appear in serialized JSON"
        );

        let restored: SerializedTask = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(
            restored.metadata.on_success_link.as_deref(),
            Some("next_task"),
            "on_success_link must survive a serde roundtrip"
        );
        assert_eq!(
            restored.metadata.name, "step_a",
            "task name must survive a serde roundtrip"
        );
    }

    /// Tests for real chord result aggregation in `handle_workflow_completion`.
    ///
    /// These require the `workflows` feature because they exercise the real
    /// [`celers_backend_redis::ResultBackend`] trait.
    #[cfg(feature = "workflows")]
    mod chord_aggregation {
        use super::*;
        use async_trait::async_trait;
        use celers_backend_redis::{
            ChordState, Result as BackendResult, ResultBackend, TaskMeta, TaskResult,
        };
        use celers_core::Broker;
        use std::collections::HashMap;
        use std::sync::{Arc, Mutex};
        use std::time::Duration;
        use uuid::Uuid;

        /// Broker mock that records every enqueued task for later assertions.
        #[derive(Clone, Default)]
        struct RecordingBroker {
            enqueued: Arc<Mutex<Vec<SerializedTask>>>,
        }

        impl RecordingBroker {
            fn enqueued_tasks(&self) -> Vec<SerializedTask> {
                self.enqueued
                    .lock()
                    .expect("recording broker mutex poisoned")
                    .clone()
            }
        }

        #[async_trait]
        impl Broker for RecordingBroker {
            async fn enqueue(
                &self,
                task: SerializedTask,
            ) -> celers_core::Result<celers_core::TaskId> {
                let id = task.metadata.id;
                self.enqueued
                    .lock()
                    .expect("recording broker mutex poisoned")
                    .push(task);
                Ok(id)
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

        /// In-memory `ResultBackend` mock returning canned chord state and
        /// per-task results. The chord is reported as fully complete so the
        /// callback path is exercised.
        struct MockChordBackend {
            state: ChordState,
            /// Per-task-id result metadata; absence means a missing result.
            results: HashMap<Uuid, TaskMeta>,
        }

        #[async_trait]
        impl ResultBackend for MockChordBackend {
            async fn store_result(
                &mut self,
                _task_id: Uuid,
                _meta: &TaskMeta,
            ) -> BackendResult<()> {
                Ok(())
            }

            async fn get_result(&mut self, task_id: Uuid) -> BackendResult<Option<TaskMeta>> {
                Ok(self.results.get(&task_id).cloned())
            }

            async fn delete_result(&mut self, _task_id: Uuid) -> BackendResult<()> {
                Ok(())
            }

            async fn set_expiration(
                &mut self,
                _task_id: Uuid,
                _ttl: Duration,
            ) -> BackendResult<()> {
                Ok(())
            }

            async fn chord_init(&mut self, state: ChordState) -> BackendResult<()> {
                self.state = state;
                Ok(())
            }

            async fn chord_complete_task(&mut self, _chord_id: Uuid) -> BackendResult<usize> {
                // Report the chord as fully complete to trigger the callback.
                Ok(self.state.total)
            }

            async fn chord_get_state(
                &mut self,
                _chord_id: Uuid,
            ) -> BackendResult<Option<ChordState>> {
                Ok(Some(self.state.clone()))
            }
        }

        /// Build a `TaskMeta` whose result is a successful JSON value.
        fn success_meta(task_id: Uuid, value: serde_json::Value) -> TaskMeta {
            let mut meta = TaskMeta::new(task_id, "header_task".to_string());
            meta.result = TaskResult::Success(value);
            meta
        }

        /// A chord with three header tasks (the middle one's result is missing)
        /// must enqueue the callback with the results collected in `task_ids`
        /// order, substituting JSON `null` for the missing result.
        #[tokio::test]
        async fn chord_callback_receives_aggregated_results_in_order() {
            let chord_id = Uuid::new_v4();
            let id_a = Uuid::new_v4();
            let id_b = Uuid::new_v4();
            let id_c = Uuid::new_v4();

            let state = ChordState::new(chord_id, 3, vec![id_a, id_b, id_c])
                .with_callback("aggregate_callback".to_string());

            // Only the first and third tasks have stored results; the middle one
            // is intentionally missing to verify the null-substitution behaviour.
            let mut results = HashMap::new();
            results.insert(id_a, success_meta(id_a, serde_json::json!(10)));
            results.insert(id_c, success_meta(id_c, serde_json::json!("three")));

            let mut backend = MockChordBackend { state, results };

            // The completing task carries the chord_id so the chord branch runs.
            let completing = SerializedTask::new("header_task".to_string(), b"ignored".to_vec())
                .with_chord_id(chord_id);

            let broker = RecordingBroker::default();

            handle_workflow_completion(&completing, b"ignored", &broker, Some(&mut backend))
                .await
                .expect("chord completion handling should succeed");

            let enqueued = broker.enqueued_tasks();
            assert_eq!(
                enqueued.len(),
                1,
                "exactly one chord callback task should be enqueued"
            );

            let callback = &enqueued[0];
            assert_eq!(
                callback.metadata.name, "aggregate_callback",
                "the enqueued task must be the chord callback"
            );

            let payload: serde_json::Value =
                serde_json::from_slice(&callback.payload).expect("callback payload must be JSON");

            // Celery semantics: the callback's first positional argument is the
            // ordered list of header-task results.
            let args = payload
                .get("args")
                .and_then(|a| a.as_array())
                .expect("callback args must be a JSON array");
            assert_eq!(args.len(), 1, "callback receives a single positional arg");

            let results_arg = args[0]
                .as_array()
                .expect("first positional arg must be the results list");
            assert_eq!(
                results_arg,
                &vec![
                    serde_json::json!(10),
                    serde_json::Value::Null,
                    serde_json::json!("three"),
                ],
                "results must be aggregated in task_ids order with null for the missing one"
            );

            // kwargs is present and empty.
            assert_eq!(
                payload.get("kwargs"),
                Some(&serde_json::json!({})),
                "callback kwargs must be an empty object"
            );
        }

        /// A failed header-task result must be aggregated as JSON `null` rather
        /// than aborting the whole callback.
        #[tokio::test]
        async fn chord_callback_substitutes_null_for_failed_result() {
            let chord_id = Uuid::new_v4();
            let id_a = Uuid::new_v4();
            let id_b = Uuid::new_v4();

            let state = ChordState::new(chord_id, 2, vec![id_a, id_b])
                .with_callback("aggregate_callback".to_string());

            let mut results = HashMap::new();
            results.insert(id_a, success_meta(id_a, serde_json::json!({"ok": true})));
            // id_b failed.
            let mut failed = TaskMeta::new(id_b, "header_task".to_string());
            failed.result = TaskResult::Failure("boom".to_string());
            results.insert(id_b, failed);

            let mut backend = MockChordBackend { state, results };

            let completing = SerializedTask::new("header_task".to_string(), b"ignored".to_vec())
                .with_chord_id(chord_id);
            let broker = RecordingBroker::default();

            handle_workflow_completion(&completing, b"ignored", &broker, Some(&mut backend))
                .await
                .expect("chord completion handling should succeed despite a failed task");

            let enqueued = broker.enqueued_tasks();
            assert_eq!(enqueued.len(), 1, "callback must still be enqueued");

            let payload: serde_json::Value =
                serde_json::from_slice(&enqueued[0].payload).expect("payload must be JSON");
            let results_arg = payload["args"][0]
                .as_array()
                .expect("results list expected");
            assert_eq!(
                results_arg,
                &vec![serde_json::json!({"ok": true}), serde_json::Value::Null],
                "failed task result must become null while successes are preserved"
            );
        }
    }

    /// End-to-end chain execution: a canvas-built chain driven through a real
    /// in-memory broker by a simulated worker loop.
    ///
    /// Requires the `workflows` feature only because `handle_workflow_completion`
    /// takes its backend parameter under that gate; no backend is used here.
    #[cfg(feature = "workflows")]
    mod chain_execution {
        use super::*;
        use celers_core::{Broker, InMemoryBroker};

        /// What a "worker" observed while running one task.
        #[derive(Debug, Clone, PartialEq)]
        struct Executed {
            name: String,
            args: Vec<serde_json::Value>,
            kwargs: serde_json::Map<String, serde_json::Value>,
        }

        /// Drain the broker, running every task through `execute` and feeding
        /// each result back into the workflow handler, exactly as a worker
        /// would. Deterministic: it loops until the queue is empty, with a hard
        /// cap so a runaway chain fails the test instead of hanging it.
        async fn run_to_completion<F>(broker: &InMemoryBroker, mut execute: F) -> Vec<Executed>
        where
            F: FnMut(&Executed) -> Vec<u8>,
        {
            let mut observed = Vec::new();

            for _ in 0..64 {
                // `dequeue` blocks until a task arrives; `dequeue_batch` drains
                // what is immediately available and returns straight away, which
                // is what "run until the workflow is finished" needs.
                let Some(message) = broker
                    .dequeue_batch(1)
                    .await
                    .expect("in-memory dequeue never fails")
                    .pop()
                else {
                    break;
                };

                let task = message.task;
                let envelope: serde_json::Value = serde_json::from_slice(&task.payload)
                    .expect("canvas payloads are JSON envelopes");

                let step = Executed {
                    name: task.metadata.name.clone(),
                    args: envelope
                        .get("args")
                        .and_then(|a| a.as_array())
                        .cloned()
                        .unwrap_or_default(),
                    kwargs: envelope
                        .get("kwargs")
                        .and_then(|k| k.as_object())
                        .cloned()
                        .unwrap_or_default(),
                };

                let result = execute(&step);
                observed.push(step);

                broker
                    .ack(&task.metadata.id, message.receipt_handle.as_deref())
                    .await
                    .expect("ack");

                handle_workflow_completion(&task, &result, broker, None)
                    .await
                    .expect("workflow continuation must succeed");
            }

            assert_eq!(
                broker.queue_size().await.expect("queue size"),
                0,
                "the workflow must terminate with an empty queue"
            );

            observed
        }

        /// A four-task chain must execute all four steps, in order, with each
        /// step receiving its predecessor's result as its first argument.
        #[tokio::test]
        async fn four_task_chain_executes_every_step_in_order() {
            let broker = InMemoryBroker::new();

            let chain = celers_canvas::Chain::new()
                .then("step_a", vec![serde_json::json!("seed")])
                .then("step_b", vec![])
                .then("step_c", vec![])
                .then("step_d", vec![]);

            chain.apply(&broker).await.expect("chain dispatches");

            // Each step returns its own name so the hand-off is observable.
            let mut counter = 0u32;
            let observed = run_to_completion(&broker, |step| {
                counter += 1;
                serde_json::to_vec(&serde_json::json!({
                    "from": step.name,
                    "seq": counter,
                }))
                .expect("result encodes")
            })
            .await;

            let names: Vec<&str> = observed.iter().map(|s| s.name.as_str()).collect();
            assert_eq!(
                names,
                vec!["step_a", "step_b", "step_c", "step_d"],
                "every step of an N-task chain must run, in declaration order"
            );

            assert_eq!(
                observed[0].args,
                vec![serde_json::json!("seed")],
                "the head keeps its own arguments"
            );
            assert_eq!(
                observed[1].args,
                vec![serde_json::json!({"from": "step_a", "seq": 1})],
                "step_b receives step_a's result as its first argument"
            );
            assert_eq!(
                observed[3].args,
                vec![serde_json::json!({"from": "step_c", "seq": 3})],
                "the result hand-off survives every hop"
            );
        }

        /// A chain step's own arguments must survive the trip: the parent result
        /// is *prepended*, not substituted.
        #[tokio::test]
        async fn chain_steps_keep_their_own_arguments_and_kwargs() {
            let broker = InMemoryBroker::new();

            let mut kwargs = std::collections::HashMap::new();
            kwargs.insert("mode".to_string(), serde_json::json!("fast"));

            let chain = celers_canvas::Chain::new()
                .then("first", vec![])
                .then_signature(
                    celers_canvas::Signature::new("second".to_string())
                        .with_args(vec![serde_json::json!("own_arg")])
                        .with_kwargs(kwargs),
                );

            chain.apply(&broker).await.expect("chain dispatches");

            let observed = run_to_completion(&broker, |_| {
                serde_json::to_vec(&serde_json::json!(7)).unwrap()
            })
            .await;

            assert_eq!(observed.len(), 2);
            assert_eq!(
                observed[1].args,
                vec![serde_json::json!(7), serde_json::json!("own_arg")],
                "the parent's result is prepended to the step's own args"
            );
            assert_eq!(
                observed[1].kwargs.get("mode"),
                Some(&serde_json::json!("fast")),
                "kwargs must survive the hop"
            );
        }

        /// An immutable signature must not receive the parent's result.
        #[tokio::test]
        async fn immutable_chain_step_does_not_receive_the_parent_result() {
            let broker = InMemoryBroker::new();

            let chain = celers_canvas::Chain::new()
                .then("first", vec![])
                .then_signature(
                    celers_canvas::Signature::new("second".to_string())
                        .with_args(vec![serde_json::json!("only_mine")])
                        .immutable(),
                );

            chain.apply(&broker).await.expect("chain dispatches");

            let observed = run_to_completion(&broker, |_| b"{\"ignored\":true}".to_vec()).await;

            assert_eq!(
                observed[1].args,
                vec![serde_json::json!("only_mine")],
                "an immutable step keeps exactly its own arguments"
            );
        }

        /// A NestedChain of linear elements must sequence every element, not
        /// just dispatch them back to back.
        #[tokio::test]
        async fn nested_chain_sequences_every_element() {
            let broker = InMemoryBroker::new();

            let workflow = celers_canvas::NestedChain::new()
                .then("head", vec![])
                .then_chain(
                    celers_canvas::Chain::new()
                        .then("mid_a", vec![])
                        .then("mid_b", vec![]),
                )
                .then("tail", vec![]);

            workflow.apply(&broker).await.expect("nested chain applies");

            // Only the head is on the queue right now: the rest are gated behind
            // its completion.
            assert_eq!(
                broker.queue_size().await.expect("queue size"),
                1,
                "a sequenced chain exposes one runnable task at a time"
            );

            let observed = run_to_completion(&broker, |_| b"null".to_vec()).await;

            let names: Vec<&str> = observed.iter().map(|s| s.name.as_str()).collect();
            assert_eq!(
                names,
                vec!["head", "mid_a", "mid_b", "tail"],
                "every nested element runs, in order"
            );
        }

        /// A Branch step is evaluated against the predecessor's result and only
        /// the selected arm runs.
        #[tokio::test]
        async fn branch_step_runs_only_the_selected_arm() {
            for (count, expected_arm) in [(500.0, "process_large_batch"), (5.0, "process_small")] {
                let broker = InMemoryBroker::new();

                let branch = celers_canvas::Branch::new(
                    celers_canvas::Condition::field_greater_than("count", 100.0),
                    celers_canvas::Signature::new("process_large_batch".to_string()),
                )
                .otherwise(celers_canvas::Signature::new("process_small".to_string()));

                let workflow = celers_canvas::NestedChain::new()
                    .then("count_rows", vec![])
                    .then_branch(branch);

                workflow.apply(&broker).await.expect("workflow applies");

                let payload =
                    serde_json::to_vec(&serde_json::json!({ "count": count })).expect("encode");
                let observed = run_to_completion(&broker, |_| payload.clone()).await;

                let names: Vec<&str> = observed.iter().map(|s| s.name.as_str()).collect();
                assert_eq!(
                    names,
                    vec!["count_rows", expected_arm],
                    "exactly the selected arm must run for count={}",
                    count
                );
            }
        }

        /// A Switch step selects the first matching case, and falls through to
        /// its default when nothing matches.
        #[tokio::test]
        async fn switch_step_selects_matching_case_then_default() {
            for (status, expected) in [("approved", "ship_it"), ("weird", "escalate")] {
                let broker = InMemoryBroker::new();

                let switch = celers_canvas::Switch::new()
                    .case(
                        celers_canvas::Condition::field_equals(
                            "status",
                            serde_json::json!("approved"),
                        ),
                        celers_canvas::Signature::new("ship_it".to_string()),
                    )
                    .default(celers_canvas::Signature::new("escalate".to_string()));

                let workflow = celers_canvas::NestedChain::new()
                    .then("review", vec![])
                    .then_element(celers_canvas::CanvasElement::switch(switch));

                workflow.apply(&broker).await.expect("workflow applies");

                let payload =
                    serde_json::to_vec(&serde_json::json!({ "status": status })).expect("encode");
                let observed = run_to_completion(&broker, |_| payload.clone()).await;

                let names: Vec<&str> = observed.iter().map(|s| s.name.as_str()).collect();
                assert_eq!(names, vec!["review", expected]);
            }
        }

        /// A conditional with no matching case and no default ends the chain
        /// cleanly rather than erroring or stalling.
        #[tokio::test]
        async fn unmatched_switch_without_default_ends_the_chain() {
            let broker = InMemoryBroker::new();

            let switch = celers_canvas::Switch::new().case(
                celers_canvas::Condition::field_equals("status", serde_json::json!("approved")),
                celers_canvas::Signature::new("ship_it".to_string()),
            );

            let workflow = celers_canvas::NestedChain::new()
                .then("review", vec![])
                .then_element(celers_canvas::CanvasElement::switch(switch));

            workflow.apply(&broker).await.expect("workflow applies");

            let observed =
                run_to_completion(&broker, |_| b"{\"status\":\"rejected\"}".to_vec()).await;

            let names: Vec<&str> = observed.iter().map(|s| s.name.as_str()).collect();
            assert_eq!(names, vec!["review"], "the chain simply stops");
        }

        /// A task carrying only an `on_success_link` (no canvas chain tail)
        /// keeps the original behaviour: the named task is enqueued with the
        /// raw result bytes.
        #[tokio::test]
        async fn bare_on_success_link_still_enqueues_raw_result() {
            let broker = InMemoryBroker::new();

            let task = SerializedTask::new("legacy".to_string(), b"not json at all".to_vec())
                .with_on_success_link("next_up".to_string());

            handle_workflow_completion(&task, b"raw-result-bytes", &broker, None)
                .await
                .expect("legacy path must keep working");

            let message = broker
                .dequeue_batch(1)
                .await
                .expect("dequeue")
                .pop()
                .expect("the link must have been enqueued");
            assert_eq!(message.task.metadata.name, "next_up");
            assert_eq!(message.task.payload, b"raw-result-bytes".to_vec());
        }

        /// One recorded dispatch: the task and the delay it was enqueued with.
        type ScheduledEntry = (SerializedTask, Option<u64>);

        /// Broker that records the enqueue variant each task arrived through.
        #[derive(Clone, Default)]
        struct SchedulingBroker {
            entries: std::sync::Arc<std::sync::Mutex<Vec<ScheduledEntry>>>,
        }

        impl SchedulingBroker {
            fn snapshot(&self) -> Vec<ScheduledEntry> {
                self.entries
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone()
            }

            fn record(
                &self,
                task: SerializedTask,
                delay: Option<u64>,
            ) -> celers_core::Result<celers_core::TaskId> {
                let id = task.metadata.id;
                self.entries
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push((task, delay));
                Ok(id)
            }
        }

        #[async_trait::async_trait]
        impl Broker for SchedulingBroker {
            async fn enqueue(
                &self,
                task: SerializedTask,
            ) -> celers_core::Result<celers_core::TaskId> {
                self.record(task, None)
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
                Ok(self.snapshot().len())
            }

            async fn cancel(&self, _task_id: &celers_core::TaskId) -> celers_core::Result<bool> {
                Ok(false)
            }

            async fn enqueue_after(
                &self,
                task: SerializedTask,
                delay_secs: u64,
            ) -> celers_core::Result<celers_core::TaskId> {
                self.record(task, Some(delay_secs))
            }
        }

        /// A chain step with a countdown must be handed to the delayed-enqueue
        /// path instead of being run immediately.
        #[tokio::test]
        async fn chain_step_countdown_reaches_the_scheduling_path() {
            let chain = celers_canvas::Chain::new()
                .then_signature(celers_canvas::Signature::new("head".to_string()))
                .then_signature(
                    celers_canvas::Signature::new("delayed".to_string()).with_countdown(90),
                );

            let recording = SchedulingBroker::default();
            chain.apply(&recording).await.expect("chain dispatches");

            let head_task = recording.snapshot().pop().expect("head task enqueued").0;

            handle_workflow_completion(&head_task, b"null", &recording, None)
                .await
                .expect("continuation");

            let scheduled: Vec<(String, Option<u64>)> = recording
                .snapshot()
                .into_iter()
                .map(|(task, delay)| (task.metadata.name, delay))
                .collect();
            assert_eq!(
                scheduled,
                vec![
                    ("head".to_string(), None),
                    ("delayed".to_string(), Some(90))
                ],
                "the successor's countdown must select the delayed-enqueue variant"
            );
        }
    }

    /// The chord barrier itself: the callback must fire exactly once, and only
    /// after every header task has completed.
    ///
    /// The [`chord_aggregation`] mock above always reports the chord complete,
    /// which exercises the aggregation code but says nothing about *when* the
    /// callback fires. This module uses a backend with a real counter.
    #[cfg(feature = "workflows")]
    mod chord_barrier {
        use super::*;
        use async_trait::async_trait;
        use celers_backend_redis::{ChordState, Result as BackendResult, ResultBackend, TaskMeta};
        use celers_core::Broker;
        use std::collections::HashMap;
        use std::sync::{Arc, Mutex};
        use std::time::Duration;
        use uuid::Uuid;

        /// Broker recording every enqueued task.
        #[derive(Clone, Default)]
        struct RecordingBroker {
            enqueued: Arc<Mutex<Vec<SerializedTask>>>,
        }

        impl RecordingBroker {
            fn enqueued_tasks(&self) -> Vec<SerializedTask> {
                self.enqueued
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .clone()
            }
        }

        #[async_trait]
        impl Broker for RecordingBroker {
            async fn enqueue(
                &self,
                task: SerializedTask,
            ) -> celers_core::Result<celers_core::TaskId> {
                let id = task.metadata.id;
                self.enqueued
                    .lock()
                    .unwrap_or_else(|poisoned| poisoned.into_inner())
                    .push(task);
                Ok(id)
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
                Ok(self.enqueued_tasks().len())
            }

            async fn cancel(&self, _task_id: &celers_core::TaskId) -> celers_core::Result<bool> {
                Ok(false)
            }
        }

        /// Result backend whose chord counter really counts, so the barrier can
        /// be observed opening (and staying shut before that).
        struct CountingChordBackend {
            state: ChordState,
            results: HashMap<Uuid, TaskMeta>,
            completed: usize,
        }

        impl CountingChordBackend {
            fn new(state: ChordState) -> Self {
                Self {
                    state,
                    results: HashMap::new(),
                    completed: 0,
                }
            }
        }

        #[async_trait]
        impl ResultBackend for CountingChordBackend {
            async fn store_result(&mut self, task_id: Uuid, meta: &TaskMeta) -> BackendResult<()> {
                self.results.insert(task_id, meta.clone());
                Ok(())
            }

            async fn get_result(&mut self, task_id: Uuid) -> BackendResult<Option<TaskMeta>> {
                Ok(self.results.get(&task_id).cloned())
            }

            async fn delete_result(&mut self, task_id: Uuid) -> BackendResult<()> {
                self.results.remove(&task_id);
                Ok(())
            }

            async fn set_expiration(
                &mut self,
                _task_id: Uuid,
                _ttl: Duration,
            ) -> BackendResult<()> {
                Ok(())
            }

            async fn chord_init(&mut self, state: ChordState) -> BackendResult<()> {
                self.state = state;
                Ok(())
            }

            async fn chord_complete_task(&mut self, _chord_id: Uuid) -> BackendResult<usize> {
                self.completed += 1;
                Ok(self.completed)
            }

            async fn chord_get_state(
                &mut self,
                _chord_id: Uuid,
            ) -> BackendResult<Option<ChordState>> {
                Ok(Some(self.state.clone()))
            }
        }

        /// A completing header task carrying the chord id and its own task id.
        fn completing_task(task_id: Uuid, chord_id: Uuid) -> SerializedTask {
            let mut task = SerializedTask::new("header".to_string(), b"{}".to_vec());
            task.metadata.id = task_id;
            task.metadata.chord_id = Some(chord_id);
            task
        }

        /// The heart of a chord: with three header tasks, nothing may be
        /// enqueued after the first or second completion, and exactly one
        /// callback — carrying the results in `task_ids` order — after the
        /// third. Header tasks complete out of order to prove the ordering
        /// comes from the barrier's recorded ids, not from arrival order.
        #[tokio::test]
        async fn callback_fires_exactly_once_after_the_last_header_completes() {
            let chord_id = Uuid::new_v4();
            let id_a = Uuid::new_v4();
            let id_b = Uuid::new_v4();
            let id_c = Uuid::new_v4();

            let state = ChordState::new(chord_id, 3, vec![id_a, id_b, id_c])
                .with_callback("aggregate".to_string());
            let mut backend = CountingChordBackend::new(state);
            let broker = RecordingBroker::default();

            // Completion order: b, c, a. Values are tied to the task, not the
            // order, so a mis-ordered aggregation is visible.
            let completions = [
                (id_b, serde_json::json!("beta")),
                (id_c, serde_json::json!("gamma")),
                (id_a, serde_json::json!("alpha")),
            ];

            for (index, (task_id, value)) in completions.into_iter().enumerate() {
                // The worker is what records a chord member's result: nothing
                // else writes into the barrier store, so a callback would
                // otherwise aggregate a list of nulls.
                let result = serde_json::to_vec(&value).expect("result serializes");

                let task = completing_task(task_id, chord_id);
                handle_workflow_completion(&task, &result, &broker, Some(&mut backend))
                    .await
                    .expect("chord completion handling");

                let enqueued = broker.enqueued_tasks();
                if index < 2 {
                    assert!(
                        enqueued.is_empty(),
                        "the callback must not fire until every header task has completed \
                         (fired after {} of 3)",
                        index + 1
                    );
                } else {
                    assert_eq!(
                        enqueued.len(),
                        1,
                        "exactly one callback must be enqueued when the barrier opens"
                    );
                }
            }

            let enqueued = broker.enqueued_tasks();
            assert_eq!(enqueued.len(), 1, "exactly once, not once per header task");

            let callback = &enqueued[0];
            assert_eq!(callback.metadata.name, "aggregate");

            let payload: serde_json::Value =
                serde_json::from_slice(&callback.payload).expect("callback payload is JSON");
            let results = payload["args"][0]
                .as_array()
                .expect("first positional arg is the results list");
            assert_eq!(
                results,
                &vec![
                    serde_json::json!("alpha"),
                    serde_json::json!("beta"),
                    serde_json::json!("gamma"),
                ],
                "results must follow the chord's recorded task_ids order, \
                 not the order the tasks happened to finish in"
            );
        }

        /// A header task completing while the barrier is still shut must leave
        /// the queue untouched — not even a partial callback.
        #[tokio::test]
        async fn partial_completion_enqueues_nothing() {
            let chord_id = Uuid::new_v4();
            let ids: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();

            let state = ChordState::new(chord_id, ids.len(), ids.clone())
                .with_callback("aggregate".to_string());
            let mut backend = CountingChordBackend::new(state);
            let broker = RecordingBroker::default();

            for task_id in ids.iter().take(4) {
                let task = completing_task(*task_id, chord_id);
                handle_workflow_completion(&task, b"{}", &broker, Some(&mut backend))
                    .await
                    .expect("chord completion handling");
            }

            assert!(
                broker.enqueued_tasks().is_empty(),
                "4 of 5 header tasks is not a complete chord"
            );
        }

        /// A chord with no callback configured completes silently rather than
        /// enqueuing a nameless task.
        #[tokio::test]
        async fn chord_without_a_callback_enqueues_nothing() {
            let chord_id = Uuid::new_v4();
            let id = Uuid::new_v4();

            let state = ChordState::new(chord_id, 1, vec![id]);
            let mut backend = CountingChordBackend::new(state);
            let broker = RecordingBroker::default();

            let task = completing_task(id, chord_id);
            handle_workflow_completion(&task, b"{}", &broker, Some(&mut backend))
                .await
                .expect("chord completion handling");

            assert!(broker.enqueued_tasks().is_empty());
        }
    }
}
