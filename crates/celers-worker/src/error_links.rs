//! The failure half of the canvas wire format: error links, failure
//! suppression and per-task retry backoff.
//!
//! [`workflows`](crate::workflows) drives what happens when a task *succeeds* —
//! the chain tail, branch/switch evaluation, the chord barrier. This module is
//! its counterpart: what happens when a task fails for good, and what a task's
//! own options say about how it should fail.
//!
//! # Wire format
//!
//! `celers-canvas` puts three more keys in the dispatch payload envelope
//! alongside `args`/`kwargs`/`chain`:
//!
//! ```json
//! {
//!   "args":   [ ... ],
//!   "kwargs": { ... },
//!   "errback": [ {"step_type": "task", "task": "compensate_2", ...},
//!                {"step_type": "task", "task": "compensate_1", ...} ],
//!   "ignore_errors": true,
//!   "retry_policy": {"delay": 2, "backoff": 2.0, "max_delay": 3600}
//! }
//! ```
//!
//! * **`errback`** is a *chain* of error handlers, in the shape the success
//!   path already uses. On terminal failure the worker enqueues its head with a
//!   JSON error descriptor as the argument, and the rest of the array travels
//!   with it as an ordinary chain tail — so a multi-step error route (a saga's
//!   rollback: compensate step 3, then 2, then 1) runs in order, each handler
//!   only after the previous one finished. A name-only metadata field could
//!   express none of that.
//! * **`ignore_errors`** turns a failure into a non-event: no retry, no
//!   dead-letter, a
//!   [`TaskResultValue::Ignored`](celers_core::TaskResultValue::Ignored) result,
//!   and the surrounding workflow continues with a `null` result.
//! * **`retry_policy`** is the task's *own* backoff schedule. Without it the
//!   worker can only apply its single configured
//!   [`RetryConfig`](crate::retry::RetryConfig) to every task alike, so
//!   `Signature::with_retry_delay`/`with_retry_backoff` would be decorative.
//!
//! The three constants below are the contract with `celers_canvas::dispatch`'s
//! `ERROR_ROUTE_KEY`/`IGNORE_ERRORS_KEY`/`RETRY_POLICY_KEY`. They are duplicated
//! rather than imported because this module is compiled even when the optional
//! `canvas` dependency is not enabled.

use crate::security::SignatureVerification;
use crate::workflows::{
    build_next_task, dispatch_next, resolve_next_step, sign_continuation, WorkflowError,
};
use celers_core::{Broker, SerializedTask, TaskId};
use std::time::Duration;
use tracing::{debug, info, warn};

/// JSON key under which `celers-canvas` embeds a task's failure route.
///
/// Mirrors `celers_canvas::ERROR_ROUTE_KEY`.
pub const ERROR_ROUTE_KEY: &str = "errback";

/// JSON key under which `celers-canvas` embeds the failure-suppression flag.
///
/// Mirrors `celers_canvas::IGNORE_ERRORS_KEY`.
pub const IGNORE_ERRORS_KEY: &str = "ignore_errors";

/// JSON key under which `celers-canvas` embeds a task's own retry backoff.
///
/// Mirrors `celers_canvas::RETRY_POLICY_KEY`.
pub const RETRY_POLICY_KEY: &str = "retry_policy";

/// Ceiling applied to a per-task retry delay when the policy names none: 1 hour.
///
/// Matches `celers_canvas::TaskOptions::calculate_retry_delay`'s own default so
/// the producer's arithmetic and the worker's agree.
const DEFAULT_MAX_RETRY_DELAY_SECS: u64 = 3_600;

/// Read the JSON envelope out of a task payload, if it has one.
fn envelope(payload: &[u8]) -> Option<serde_json::Value> {
    serde_json::from_slice::<serde_json::Value>(payload)
        .ok()
        .filter(serde_json::Value::is_object)
}

/// Whether this task was dispatched with failure suppression enabled.
///
/// A task whose payload is not a canvas envelope (a hand-built
/// [`SerializedTask`], a Celery-protocol message) never suppresses: the flag has
/// to be asked for.
#[must_use]
pub fn ignores_errors(payload: &[u8]) -> bool {
    envelope(payload)
        .and_then(|envelope| {
            envelope
                .get(IGNORE_ERRORS_KEY)
                .and_then(serde_json::Value::as_bool)
        })
        .unwrap_or(false)
}

/// Whether this task declares a failure route at all.
#[must_use]
pub fn has_error_route(payload: &[u8]) -> bool {
    !parse_error_route(payload).is_empty()
}

/// Extract the failure route from a task payload.
///
/// Returns an empty vector for payloads that are not JSON, are not an object,
/// or carry no `errback` key.
fn parse_error_route(payload: &[u8]) -> Vec<serde_json::Value> {
    match envelope(payload)
        .as_ref()
        .and_then(|envelope| envelope.get(ERROR_ROUTE_KEY))
        .and_then(serde_json::Value::as_array)
    {
        Some(steps) => steps.clone(),
        None => Vec::new(),
    }
}

/// A task's own retry backoff schedule, as declared by its signature.
///
/// Mirrors `celers_canvas::TaskOptions`'
/// `retry_delay`/`retry_backoff`/`retry_backoff_max`/`retry_jitter`, and
/// reproduces its `calculate_retry_delay` arithmetic so a producer computing an
/// expected schedule and the worker applying one never disagree.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TaskRetryPolicy {
    /// Delay before the first retry, in seconds.
    pub base_delay_secs: u64,
    /// Multiplier applied per retry (2.0 doubles each time).
    pub backoff: f64,
    /// Upper bound on any single delay, in seconds.
    pub max_delay_secs: u64,
    /// Whether to spread the delay randomly to avoid a retry thundering herd.
    pub jitter: bool,
}

impl TaskRetryPolicy {
    /// Parse the policy a task payload declares, if any.
    #[must_use]
    pub fn from_payload(payload: &[u8]) -> Option<Self> {
        let envelope = envelope(payload)?;
        let policy = envelope.get(RETRY_POLICY_KEY)?.as_object()?;

        let base_delay_secs = policy.get("delay").and_then(serde_json::Value::as_u64);
        let backoff = policy.get("backoff").and_then(serde_json::Value::as_f64);
        let max_delay_secs = policy.get("max_delay").and_then(serde_json::Value::as_u64);
        let jitter = policy.get("jitter").and_then(serde_json::Value::as_bool);

        if base_delay_secs.is_none() && backoff.is_none() && max_delay_secs.is_none() {
            // A policy naming only `jitter` has no schedule of its own to
            // apply; leave the worker's configured strategy in charge.
            return None;
        }

        Some(Self {
            base_delay_secs: base_delay_secs.unwrap_or(1),
            // A declared delay with no multiplier is a *fixed* delay, not an
            // accidental doubling.
            backoff: backoff.unwrap_or(1.0),
            max_delay_secs: max_delay_secs.unwrap_or(DEFAULT_MAX_RETRY_DELAY_SECS),
            jitter: jitter.unwrap_or(false),
        })
    }

    /// The delay before the retry that follows `retry_count` already-spent
    /// attempts: `base * backoff^retry_count`, capped at `max_delay_secs`.
    ///
    /// The arithmetic is done in `f64` and clamped before conversion, so an
    /// overflowing `powi` (which yields `inf`) or a `NaN` multiplier lands on
    /// the cap rather than wrapping to zero.
    #[must_use]
    pub fn delay(&self, retry_count: u32) -> Duration {
        /// Largest exponent representable as the `i32` `powi` wants.
        const MAX_EXPONENT: u32 = i32::MAX as u32;

        let exponent = retry_count.min(MAX_EXPONENT) as i32;
        let cap = self.max_delay_secs as f64;
        let scaled = self.base_delay_secs as f64 * self.backoff.powi(exponent);
        let secs = if scaled.is_nan() {
            cap
        } else {
            scaled.clamp(0.0, cap)
        };

        let delay = Duration::from_secs_f64(secs);
        if self.jitter {
            apply_jitter(delay)
        } else {
            delay
        }
    }
}

/// Spread a delay over `[50%, 100%]` of its nominal value.
///
/// Full jitter (`[0, delay]`) would let a retry come back immediately, which
/// defeats the point of a backoff; halving the floor keeps the schedule growing
/// while still de-synchronising a fleet.
fn apply_jitter(delay: Duration) -> Duration {
    use rand::RngExt;

    if delay.is_zero() {
        return delay;
    }
    let millis = delay.as_millis().min(u128::from(u64::MAX)) as u64;
    let floor = millis / 2;
    let span = millis - floor;
    let extra = if span == 0 {
        0
    } else {
        rand::rng().random_range(0..=span)
    };
    Duration::from_millis(floor + extra)
}

/// Copy the failure-related options of a chain step's signature into the
/// envelope of the message being built for it.
///
/// `options` is the step's `options` object as it travelled in the chain tail.
/// The three keys reconstructed here are exactly the ones `celers-canvas`'
/// `build_payload` writes for a task it dispatches itself; without this, only a
/// chain's *head* would ever carry them.
pub(crate) fn carry_failure_options(
    options: Option<&serde_json::Value>,
    envelope: &mut serde_json::Map<String, serde_json::Value>,
) {
    let Some(options) = options else {
        return;
    };

    let errbacks = error_route_from_options(options);
    if !errbacks.is_empty() {
        envelope.insert(
            ERROR_ROUTE_KEY.to_string(),
            serde_json::Value::Array(errbacks),
        );
    }

    if options
        .get("ignore_errors")
        .and_then(serde_json::Value::as_bool)
        .unwrap_or(false)
    {
        envelope.insert(IGNORE_ERRORS_KEY.to_string(), serde_json::Value::Bool(true));
    }

    if let Some(policy) = retry_policy_from_options(options) {
        envelope.insert(RETRY_POLICY_KEY.to_string(), policy);
    }
}

/// Build the `errback` array from a signature's `link_error`/`link_errors`
/// options, in the same order `celers_canvas::dispatch::error_route` uses.
fn error_route_from_options(options: &serde_json::Value) -> Vec<serde_json::Value> {
    let mut steps = Vec::new();

    if let Some(single) = options.get("link_error").filter(|v| v.is_object()) {
        steps.push(as_task_step(single));
    }
    if let Some(many) = options.get("link_errors").and_then(|v| v.as_array()) {
        for handler in many.iter().filter(|v| v.is_object()) {
            steps.push(as_task_step(handler));
        }
    }

    steps
}

/// Tag a bare signature object as a `task` chain step.
fn as_task_step(signature: &serde_json::Value) -> serde_json::Value {
    let mut step = signature.clone();
    if let Some(object) = step.as_object_mut() {
        object.insert(
            "step_type".to_string(),
            serde_json::Value::String("task".to_string()),
        );
    }
    step
}

/// Rebuild the `retry_policy` object from a signature's retry options.
fn retry_policy_from_options(options: &serde_json::Value) -> Option<serde_json::Value> {
    let delay = options
        .get("retry_delay")
        .and_then(serde_json::Value::as_u64);
    let backoff = options
        .get("retry_backoff")
        .and_then(serde_json::Value::as_f64);
    let max_delay = options
        .get("retry_backoff_max")
        .and_then(serde_json::Value::as_u64);
    let jitter = options
        .get("retry_jitter")
        .and_then(serde_json::Value::as_bool);

    if delay.is_none() && backoff.is_none() && max_delay.is_none() && jitter.is_none() {
        return None;
    }

    let mut policy = serde_json::Map::with_capacity(4);
    if let Some(delay) = delay {
        policy.insert("delay".to_string(), serde_json::Value::from(delay));
    }
    if let Some(backoff) = backoff {
        policy.insert("backoff".to_string(), serde_json::Value::from(backoff));
    }
    if let Some(max_delay) = max_delay {
        policy.insert("max_delay".to_string(), serde_json::Value::from(max_delay));
    }
    if let Some(jitter) = jitter {
        policy.insert("jitter".to_string(), serde_json::Value::from(jitter));
    }

    Some(serde_json::Value::Object(policy))
}

/// What a failing task hands its error handlers.
///
/// Serialized as the JSON object below and passed to the first handler exactly
/// the way a predecessor's *result* is passed along a success chain — prepended
/// as the first positional argument unless the handler's `callback_arg_mode`
/// says otherwise — so an error handler is an ordinary task with an ordinary
/// signature:
///
/// ```json
/// {"task_id": "…", "task": "charge_payment",
///  "error": "card declined", "failure_type": "execution_error"}
/// ```
#[derive(Debug, Clone)]
pub struct TaskFailure<'a> {
    /// Id of the task that failed.
    pub task_id: TaskId,
    /// Its registered name.
    pub task_name: &'a str,
    /// Human-readable failure reason.
    pub error: &'a str,
    /// Machine-readable failure class (`execution_error`, `timeout`, …).
    pub failure_type: &'a str,
}

impl TaskFailure<'_> {
    /// The JSON descriptor handed to the error route's first handler.
    #[must_use]
    pub fn descriptor(&self) -> serde_json::Value {
        serde_json::json!({
            "task_id": self.task_id.to_string(),
            "task": self.task_name,
            "error": self.error,
            "failure_type": self.failure_type,
        })
    }
}

/// Run a failed task's error route, if it declares one.
///
/// Returns `Ok(true)` when a handler was enqueued, `Ok(false)` when the task
/// declared no failure route (the overwhelmingly common case, and a single
/// payload parse).
///
/// This is the failure-path twin of
/// [`handle_workflow_completion_signed`](crate::workflows::handle_workflow_completion_signed):
/// the head of the `errback` array is enqueued with the failure descriptor as
/// its argument, and the remaining handlers ride along as its chain tail, so
/// they run one after another rather than all at once. A `branch`/`switch`
/// entry is evaluated against the descriptor, which makes "compensate only when
/// the failure was a timeout" expressible.
///
/// # Errors
///
/// Returns [`WorkflowError`] when the route cannot be decoded or the handler
/// cannot be enqueued. Callers treat that as a logged failure of the *route*,
/// not of the task: the task has already failed and its own disposition
/// (dead-letter, ack) must still happen.
pub async fn run_error_route<B: Broker>(
    task: &SerializedTask,
    failure: &TaskFailure<'_>,
    broker: &B,
    signature: Option<&SignatureVerification>,
) -> Result<bool, WorkflowError> {
    let route = parse_error_route(&task.payload);
    if route.is_empty() {
        return Ok(false);
    }

    let descriptor = failure.descriptor();
    let (head, remaining) = route.split_at(1);

    let Some(resolved) = resolve_next_step(&head[0], &descriptor)? else {
        debug!(
            "Task {} has an error route that selected no handler; nothing to run",
            failure.task_id
        );
        return Ok(false);
    };

    let (mut handler, schedule) = build_next_task(&resolved, remaining, &descriptor)?;
    // An error handler must never inherit the failed task's chord membership or
    // barrier identity: it is a *different* task, and counting it against the
    // chord (or reusing the pre-assigned id) would corrupt the barrier.
    handler.metadata.chord_id = None;
    handler.metadata.group_id = None;
    sign_continuation(&mut handler, signature);

    let handler_name = handler.metadata.name.clone();
    dispatch_next(broker, handler, schedule).await?;

    info!(
        "Error route '{}' enqueued for failed task {} ({} further handler(s))",
        handler_name,
        failure.task_id,
        remaining.len()
    );

    Ok(true)
}

/// Run a failed task's error route, logging rather than propagating a failure
/// of the route itself.
///
/// The task's own disposition — dead-letter entry, broker ack/reject — has to
/// happen whether or not its error handler could be enqueued, so every call
/// site wants this shape rather than a `Result`.
pub(crate) async fn run_error_route_logged<B: Broker>(
    task: &SerializedTask,
    failure: &TaskFailure<'_>,
    broker: &B,
    signature: Option<&SignatureVerification>,
) -> bool {
    match run_error_route(task, failure, broker, signature).await {
        Ok(ran) => ran,
        Err(e) => {
            warn!(
                "Failed to run the error route for task {} ('{}'): {}",
                failure.task_id, failure.task_name, e
            );
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn payload(value: serde_json::Value) -> Vec<u8> {
        serde_json::to_vec(&value).expect("payload must serialize")
    }

    #[test]
    fn ignore_errors_defaults_to_false_for_non_canvas_payloads() {
        assert!(!ignores_errors(b"not json at all"));
        assert!(!ignores_errors(&payload(serde_json::json!([1, 2, 3]))));
        assert!(!ignores_errors(&payload(
            serde_json::json!({"args": [], "kwargs": {}})
        )));
    }

    #[test]
    fn ignore_errors_is_read_from_the_envelope() {
        assert!(ignores_errors(&payload(
            serde_json::json!({"args": [], "kwargs": {}, "ignore_errors": true})
        )));
        assert!(!ignores_errors(&payload(
            serde_json::json!({"args": [], "ignore_errors": false})
        )));
    }

    #[test]
    fn error_route_is_absent_unless_declared() {
        assert!(!has_error_route(&payload(
            serde_json::json!({"args": [], "kwargs": {}})
        )));
        assert!(has_error_route(&payload(serde_json::json!({
            "args": [],
            "errback": [{"step_type": "task", "task": "cleanup"}]
        }))));
    }

    #[test]
    fn retry_policy_grows_geometrically_and_is_capped() {
        let policy = TaskRetryPolicy::from_payload(&payload(serde_json::json!({
            "args": [],
            "retry_policy": {"delay": 2, "backoff": 2.0, "max_delay": 20}
        })))
        .expect("policy must parse");

        assert_eq!(policy.delay(0), Duration::from_secs(2));
        assert_eq!(policy.delay(1), Duration::from_secs(4));
        assert_eq!(policy.delay(2), Duration::from_secs(8));
        assert_eq!(policy.delay(3), Duration::from_secs(16));
        // Capped, not overflowed.
        assert_eq!(policy.delay(4), Duration::from_secs(20));
        assert_eq!(policy.delay(u32::MAX), Duration::from_secs(20));
    }

    #[test]
    fn retry_policy_without_backoff_is_a_fixed_delay() {
        let policy = TaskRetryPolicy::from_payload(&payload(serde_json::json!({
            "retry_policy": {"delay": 5}
        })))
        .expect("policy must parse");

        assert_eq!(policy.delay(0), Duration::from_secs(5));
        assert_eq!(policy.delay(7), Duration::from_secs(5));
    }

    #[test]
    fn retry_policy_absent_when_nothing_is_declared() {
        assert!(TaskRetryPolicy::from_payload(&payload(serde_json::json!({"args": []}))).is_none());
        // Jitter alone describes no schedule of its own.
        assert!(TaskRetryPolicy::from_payload(&payload(
            serde_json::json!({"retry_policy": {"jitter": true}})
        ))
        .is_none());
    }

    #[test]
    fn jittered_delay_stays_within_half_the_nominal_value() {
        let policy = TaskRetryPolicy::from_payload(&payload(serde_json::json!({
            "retry_policy": {"delay": 8, "backoff": 1.0, "jitter": true}
        })))
        .expect("policy must parse");

        for _ in 0..64 {
            let delay = policy.delay(0);
            assert!(
                delay >= Duration::from_secs(4) && delay <= Duration::from_secs(8),
                "jittered delay {delay:?} left the [50%, 100%] band"
            );
        }
    }

    #[test]
    fn failure_options_are_carried_into_a_rebuilt_envelope() {
        let options = serde_json::json!({
            "link_error": {"task": "compensate_2", "args": [], "kwargs": {}},
            "link_errors": [{"task": "compensate_1", "args": [], "kwargs": {}}],
            "ignore_errors": true,
            "retry_delay": 3,
            "retry_backoff": 2.0
        });

        let mut envelope = serde_json::Map::new();
        carry_failure_options(Some(&options), &mut envelope);

        let route = envelope[ERROR_ROUTE_KEY]
            .as_array()
            .expect("errback must be an array");
        assert_eq!(
            route.len(),
            2,
            "single link_error plus one link_errors entry"
        );
        assert_eq!(route[0]["task"], "compensate_2");
        assert_eq!(route[0]["step_type"], "task");
        assert_eq!(route[1]["task"], "compensate_1");

        assert_eq!(envelope[IGNORE_ERRORS_KEY], serde_json::json!(true));
        assert_eq!(envelope[RETRY_POLICY_KEY]["delay"], serde_json::json!(3));
        assert_eq!(
            envelope[RETRY_POLICY_KEY]["backoff"],
            serde_json::json!(2.0)
        );
    }

    #[test]
    fn failure_options_absent_when_the_signature_declares_none() {
        let options = serde_json::json!({"priority": 5});
        let mut envelope = serde_json::Map::new();
        carry_failure_options(Some(&options), &mut envelope);
        assert!(envelope.is_empty());

        let mut envelope = serde_json::Map::new();
        carry_failure_options(None, &mut envelope);
        assert!(envelope.is_empty());
    }
}
