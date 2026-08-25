//! Wiring the security primitives onto a real [`SerializedTask`].
//!
//! [`task_signature`](crate::task_signature), [`sanitize`](crate::sanitize) and
//! [`pii`](crate::pii) are primitive layers: they operate on
//! [`SignedFields`] and [`TaskValue`], not on the message type the brokers move
//! around. This module is the adapter between them and
//! [`SerializedTask`](crate::SerializedTask), so that the worker, a producer
//! and an operator tool all agree on:
//!
//! * **what is signed** — the canonical projection of a task message onto
//!   [`SignedFields`] ([`signed_fields`]), and
//! * **what a redacted copy of a payload looks like** — [`PayloadHygiene`].
//!
//! # What is automatic and what is opt-in
//!
//! Nothing here runs on its own. `celers-core` has no run loop; the pieces
//! below are only active where a *runtime* opts into them:
//!
//! | Piece | Who applies it | Default |
//! |---|---|---|
//! | [`sign_task`] | your producer, explicitly | never automatic |
//! | [`verify_task`] | `celers-worker`, when `WorkerConfig::signature_verification` is set | **off** |
//! | [`PayloadHygiene`] | `celers-worker`, when `WorkerConfig::payload_hygiene` is set | **off** |
//!
//! A worker built with [`WorkerConfig::default()`] verifies nothing and redacts
//! nothing, exactly as before. See the `security_wiring` example in the
//! `celers` facade crate for the full wiring.
//!
//! [`WorkerConfig::default()`]: https://docs.rs/celers-worker
//!
//! # The payload projection
//!
//! A `CeleRS` payload is an opaque `Vec<u8>`: the task handler owns its own
//! serialization. Signing and redaction both need to see it as a *call* —
//! positional arguments plus keyword arguments — so both go through the single
//! projection in [`task_call_from_payload`]. Because producer and consumer use
//! the *same* function, a signature computed over a payload always verifies
//! against the same bytes.
//!
//! The recognized shapes are listed on [`CallShape`]; anything unparseable
//! becomes one opaque [`TaskValue::Bytes`] argument, which still signs and
//! verifies deterministically.
//!
//! # Example: sign, then verify
//!
//! ```rust
//! use celers_core::task_security::{sign_task, verify_task, SignaturePolicy, SigningOptions};
//! use celers_core::task_signature::TaskSigner;
//! use celers_core::SerializedTask;
//!
//! let signer = TaskSigner::new(b"shared-secret-at-least-32-bytes!!");
//! let mut task = SerializedTask::new("tasks.add".to_string(), br#"[2,3]"#.to_vec());
//!
//! sign_task(&signer, &mut task, SigningOptions::default());
//! assert!(verify_task(&signer, &task, &SignaturePolicy::default()).is_ok());
//!
//! // Rewriting an argument invalidates the signature.
//! task.payload = br#"[2,4]"#.to_vec();
//! assert!(verify_task(&signer, &task, &SignaturePolicy::default()).is_err());
//! ```

use crate::pii::{PiiDetector, PiiReport};
use crate::sanitize::{SanitizeError, SanitizeReport, Sanitizer, TaskValue};
use crate::task::SerializedTask;
use crate::task_signature::{
    FreshnessWindow, ReplayGuard, SignatureError, SignedCallback, SignedFields, TaskSignature,
    TaskSigner,
};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

/// The envelope key holding positional arguments.
pub const ARGS_KEY: &str = "args";

/// The envelope key holding keyword arguments.
pub const KWARGS_KEY: &str = "kwargs";

// ===========================================================================
// Payload projection
// ===========================================================================

/// How a payload was recognized by [`task_call_from_payload`].
///
/// Kept alongside the projected call so a redacted rendering can be produced in
/// the *same* shape as the input, instead of silently reshaping an operator's
/// view of the message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallShape {
    /// A JSON object carrying an `args` array and/or a `kwargs` object — the
    /// envelope `celers-worker` builds for chain continuations. Any *other*
    /// top-level key is folded into the keyword arguments.
    Envelope,
    /// A bare JSON array: every element is a positional argument.
    Args,
    /// A bare JSON object that is not an envelope: every entry is a keyword
    /// argument.
    Kwargs,
    /// A JSON scalar (`null`, bool, number, string): one positional argument.
    Scalar,
    /// Not JSON at all (msgpack, a compressed body, ...): one opaque
    /// [`TaskValue::Bytes`] argument.
    Opaque,
}

/// A task payload seen as a call: positional plus keyword arguments.
#[derive(Debug, Clone, PartialEq)]
pub struct TaskCall {
    /// Positional arguments, in order.
    pub args: Vec<TaskValue>,
    /// Keyword arguments, in the order the payload listed them.
    pub kwargs: Vec<(String, TaskValue)>,
    /// The shape the payload was recognized as.
    pub shape: CallShape,
}

impl TaskCall {
    /// The number of positional plus keyword arguments.
    #[must_use]
    pub fn len(&self) -> usize {
        self.args.len().saturating_add(self.kwargs.len())
    }

    /// Whether the call carries no arguments at all.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.args.is_empty() && self.kwargs.is_empty()
    }
}

/// Whether a JSON object should be read as a call *envelope*.
///
/// The rule, pinned by tests: an object is an envelope when its `args` value is
/// an array **or** its `kwargs` value is an object. An object with an `args`
/// key holding anything else (a string, a number) is an ordinary keyword-
/// argument map, so `{"args": "all"}` is one kwarg rather than an empty call.
fn is_envelope(map: &serde_json::Map<String, serde_json::Value>) -> bool {
    map.get(ARGS_KEY).is_some_and(serde_json::Value::is_array)
        || map
            .get(KWARGS_KEY)
            .is_some_and(serde_json::Value::is_object)
}

/// Project a serialized payload onto a call.
///
/// This is the single definition of "what the arguments of this message are",
/// shared by [`signed_fields`] and [`PayloadHygiene`]. It never fails: a
/// payload that is not JSON becomes one opaque [`TaskValue::Bytes`] argument.
#[must_use]
pub fn task_call_from_payload(payload: &[u8]) -> TaskCall {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(payload) else {
        return TaskCall {
            args: vec![TaskValue::Bytes(payload.to_vec())],
            kwargs: Vec::new(),
            shape: CallShape::Opaque,
        };
    };

    match value {
        serde_json::Value::Object(map) if is_envelope(&map) => {
            let mut args = Vec::new();
            let mut kwargs = Vec::new();
            for (key, item) in map {
                match (key.as_str(), item) {
                    (ARGS_KEY, serde_json::Value::Array(items)) => {
                        args = items.into_iter().map(TaskValue::from).collect();
                    }
                    (KWARGS_KEY, serde_json::Value::Object(entries)) => {
                        kwargs.extend(
                            entries
                                .into_iter()
                                .map(|(k, v)| (k, TaskValue::from(v)))
                                .collect::<Vec<_>>(),
                        );
                    }
                    // Anything else at the top level (a chain tail, a custom
                    // marker) is still part of the message and must be covered
                    // by the signature, so it is folded in rather than dropped.
                    (_, other) => kwargs.push((key, TaskValue::from(other))),
                }
            }
            TaskCall {
                args,
                kwargs,
                shape: CallShape::Envelope,
            }
        }
        serde_json::Value::Array(items) => TaskCall {
            args: items.into_iter().map(TaskValue::from).collect(),
            kwargs: Vec::new(),
            shape: CallShape::Args,
        },
        serde_json::Value::Object(map) => TaskCall {
            args: Vec::new(),
            kwargs: map
                .into_iter()
                .map(|(k, v)| (k, TaskValue::from(v)))
                .collect(),
            shape: CallShape::Kwargs,
        },
        scalar => TaskCall {
            args: vec![TaskValue::from(scalar)],
            kwargs: Vec::new(),
            shape: CallShape::Scalar,
        },
    }
}

/// Render a [`TaskValue`] back into JSON for *display*.
///
/// Lossy on purpose, and only ever used for a redacted copy:
///
/// * [`TaskValue::Bytes`] becomes a `<binary, N bytes>` marker — JSON has no
///   byte type, and a redacted preview must not resurrect the blob.
/// * A non-finite float becomes `null`, which is what JSON can represent.
fn task_value_to_json(value: TaskValue) -> serde_json::Value {
    match value {
        TaskValue::Null => serde_json::Value::Null,
        TaskValue::Bool(b) => serde_json::Value::Bool(b),
        TaskValue::Int(i) => serde_json::Value::from(i),
        TaskValue::UInt(u) => serde_json::Value::from(u),
        TaskValue::Float(f) => serde_json::Number::from_f64(f)
            .map_or(serde_json::Value::Null, serde_json::Value::Number),
        TaskValue::String(s) => serde_json::Value::String(s),
        TaskValue::Bytes(b) => serde_json::Value::String(format!("<binary, {} bytes>", b.len())),
        TaskValue::Array(items) => {
            serde_json::Value::Array(items.into_iter().map(task_value_to_json).collect())
        }
        TaskValue::Object(entries) => serde_json::Value::Object(
            entries
                .into_iter()
                .map(|(k, v)| (k, task_value_to_json(v)))
                .collect(),
        ),
    }
}

/// Render a call back into the shape it was projected from.
fn call_to_json(
    args: Vec<TaskValue>,
    kwargs: Vec<(String, TaskValue)>,
    shape: CallShape,
) -> String {
    let json_args = || -> serde_json::Value {
        serde_json::Value::Array(args.clone().into_iter().map(task_value_to_json).collect())
    };
    let json_kwargs = || -> serde_json::Value {
        serde_json::Value::Object(
            kwargs
                .clone()
                .into_iter()
                .map(|(k, v)| (k, task_value_to_json(v)))
                .collect(),
        )
    };

    let value = match shape {
        CallShape::Envelope => {
            let mut map = serde_json::Map::with_capacity(2);
            map.insert(ARGS_KEY.to_string(), json_args());
            map.insert(KWARGS_KEY.to_string(), json_kwargs());
            serde_json::Value::Object(map)
        }
        CallShape::Args | CallShape::Opaque => json_args(),
        CallShape::Kwargs => json_kwargs(),
        CallShape::Scalar => args
            .into_iter()
            .next()
            .map_or(serde_json::Value::Null, task_value_to_json),
    };

    // `serde_json::Value` serialization cannot fail for a value built out of
    // finite numbers and owned strings, but a redacted preview must never be
    // the thing that ends a task, so the fallback is a marker rather than a
    // panic.
    serde_json::to_string(&value).unwrap_or_else(|_| "<unrenderable payload>".to_string())
}

// ===========================================================================
// Signature envelope
// ===========================================================================

/// The signature material a producer attaches to a task message.
///
/// Carried in [`TaskMetadata::signature`](crate::TaskMetadata::signature).
/// `signed_at` and `nonce` live here rather than being recomputed by the
/// verifier because both are *inputs* to the MAC: without them the verifier
/// cannot reconstruct what the producer signed.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SignatureEnvelope {
    /// The algorithm and hex MAC tag.
    pub signature: TaskSignature,

    /// When the producer signed, if it stamped a time (needed for freshness).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub signed_at: Option<DateTime<Utc>>,

    /// Single-use token, if the producer issued one (needed for replay
    /// rejection).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nonce: Option<String>,
}

impl SignatureEnvelope {
    /// An envelope carrying only a MAC tag.
    #[must_use]
    pub const fn new(signature: TaskSignature) -> Self {
        Self {
            signature,
            signed_at: None,
            nonce: None,
        }
    }
}

/// What a producer stamps onto a message besides the MAC itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SigningOptions {
    /// Stamp `signed_at` so a consumer can enforce a [`FreshnessWindow`].
    pub stamp_signed_at: bool,
    /// Issue a random nonce so a consumer can reject a replay.
    pub with_nonce: bool,
}

impl Default for SigningOptions {
    /// Both on: a signature without freshness or replay material is valid
    /// forever, and a captured message stays executable forever with it.
    fn default() -> Self {
        Self {
            stamp_signed_at: true,
            with_nonce: true,
        }
    }
}

impl SigningOptions {
    /// Neither `signed_at` nor a nonce: authenticity only.
    ///
    /// Choose this only when the transport already guarantees freshness.
    #[must_use]
    pub const fn authenticity_only() -> Self {
        Self {
            stamp_signed_at: false,
            with_nonce: false,
        }
    }
}

/// Project a task message onto the fields a signature covers.
///
/// Covered: the task id, the task name, the projected arguments
/// ([`task_call_from_payload`]), the message deadline
/// ([`TaskMetadata::expires_at`](crate::TaskMetadata::expires_at)) and the
/// success link ([`TaskMetadata::on_success_link`](crate::TaskMetadata::on_success_link)),
/// which names a *further task to execute* and is therefore exactly the kind of
/// field an attacker would rewrite.
///
/// **Not covered**, because they are transport or bookkeeping state a broker
/// legitimately rewrites in flight: `state`, `created_at`/`updated_at`,
/// `priority`, `max_retries`, `timeout_secs`, `group_id`, `chord_id` and
/// `dependencies`. A verified signature says nothing about them.
#[must_use]
pub fn signed_fields(task: &SerializedTask) -> SignedFields {
    let call = task_call_from_payload(&task.payload);

    let mut fields = SignedFields::new(task.metadata.id, task.metadata.name.clone())
        .with_args(call.args)
        .with_kwargs(call.kwargs);

    if let Some(expires) = task.metadata.expires_at {
        fields = fields.with_expires(expires);
    }
    if let Some(ref link) = task.metadata.on_success_link {
        fields = fields.with_callback(SignedCallback::new(link.clone()));
    }
    if let Some(ref envelope) = task.metadata.signature {
        if let Some(signed_at) = envelope.signed_at {
            fields = fields.with_signed_at(signed_at);
        }
        if let Some(ref nonce) = envelope.nonce {
            fields = fields.with_nonce(nonce.clone());
        }
    }

    fields
}

/// Sign `task` in place, replacing any signature it already carried.
///
/// Call this on the **producer** side, immediately before enqueueing: the MAC
/// covers the payload bytes as they are at this moment, so any later mutation
/// (a routing layer rewriting `on_success_link`, a re-serialization) invalidates
/// it.
pub fn sign_task(signer: &TaskSigner, task: &mut SerializedTask, options: SigningOptions) {
    // Clear first: a stale envelope would otherwise contribute its old
    // `signed_at`/`nonce` to the fields being signed.
    task.metadata.signature = None;

    let mut fields = signed_fields(task);
    if options.stamp_signed_at {
        fields = fields.signed_now();
    }
    if options.with_nonce {
        fields = fields.with_random_nonce();
    }

    let signature = signer.sign(&fields);
    task.metadata.signature = Some(SignatureEnvelope {
        signature,
        signed_at: fields.signed_at,
        nonce: fields.nonce.clone(),
    });
}

/// How strictly a consumer checks a message's signature.
#[derive(Clone)]
pub struct SignaturePolicy {
    /// Reject a message that carries no signature at all.
    ///
    /// `true` by default. Setting it to `false` is a **migration mode**: signed
    /// messages are still verified and rejected on mismatch, but unsigned ones
    /// are admitted, so a fleet can be rolled over producer-first. It is not a
    /// security posture — flip it back once every producer signs.
    pub require_signature: bool,

    /// Reject an authentic message older than this window (needs the producer
    /// to have stamped `signed_at`).
    pub freshness: Option<FreshnessWindow>,

    /// Reject an authentic, fresh message whose nonce was already accepted.
    ///
    /// The guard enforces its own [`FreshnessWindow`], which takes precedence
    /// over [`Self::freshness`]. It is **per process**: N workers each accept a
    /// replay once. See [`ReplayGuard`].
    pub replay_guard: Option<Arc<ReplayGuard>>,
}

impl Default for SignaturePolicy {
    fn default() -> Self {
        Self {
            require_signature: true,
            freshness: None,
            replay_guard: None,
        }
    }
}

impl std::fmt::Debug for SignaturePolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SignaturePolicy")
            .field("require_signature", &self.require_signature)
            .field("freshness", &self.freshness)
            .field("replay_guard", &self.replay_guard.is_some())
            .finish()
    }
}

impl SignaturePolicy {
    /// Authenticity only: every message must be signed, nothing else checked.
    #[must_use]
    pub fn require_signed() -> Self {
        Self::default()
    }

    /// Admit unsigned messages while still rejecting badly signed ones.
    #[must_use]
    pub fn allow_unsigned(mut self) -> Self {
        self.require_signature = false;
        self
    }

    /// Additionally require the message to be no older than `window`.
    #[must_use]
    pub fn with_freshness(mut self, window: FreshnessWindow) -> Self {
        self.freshness = Some(window);
        self
    }

    /// Additionally reject replays through a shared [`ReplayGuard`].
    #[must_use]
    pub fn with_replay_guard(mut self, guard: Arc<ReplayGuard>) -> Self {
        self.replay_guard = Some(guard);
        self
    }
}

/// Verify a received task message against `policy`.
///
/// The projection is [`signed_fields`], so this succeeds exactly when the
/// message is byte-for-byte (in projected terms) what [`sign_task`] signed.
///
/// # Errors
///
/// * [`SignatureError::MissingSignature`] when the message is unsigned and
///   `policy.require_signature` is set.
/// * [`SignatureError::Mismatch`] when the MAC does not match — tampering, a
///   wrong key, or a producer/consumer disagreement about the payload bytes.
/// * [`SignatureError::Stale`] / [`SignatureError::FutureDated`] /
///   [`SignatureError::MessageExpired`] when a freshness window is configured.
/// * [`SignatureError::MissingNonce`] / [`SignatureError::Replayed`] when a
///   replay guard is configured.
pub fn verify_task(
    signer: &TaskSigner,
    task: &SerializedTask,
    policy: &SignaturePolicy,
) -> Result<(), SignatureError> {
    let Some(envelope) = task.metadata.signature.as_ref() else {
        return if policy.require_signature {
            Err(SignatureError::MissingSignature)
        } else {
            Ok(())
        };
    };

    let fields = signed_fields(task);

    if let Some(ref guard) = policy.replay_guard {
        return guard.verify(signer, &fields, &envelope.signature);
    }
    match policy.freshness {
        Some(ref window) => signer.verify_fresh(&fields, &envelope.signature, window),
        None => signer.verify(&fields, &envelope.signature),
    }
}

// ===========================================================================
// Payload hygiene
// ===========================================================================

/// What a hygiene pass did to one payload copy.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct HygieneReport {
    /// What the sanitizer changed, when a sanitizer is configured.
    pub sanitize: Option<SanitizeReport>,
    /// What the PII detector masked, when a detector is configured.
    pub pii: Option<PiiReport>,
    /// The sanitizer's complaint, when the payload violated a hard limit.
    ///
    /// A hygiene pass is **best effort**: this is recorded and the rendering
    /// falls back to a marker, but it never fails a task. Turning it into a
    /// rejection would build admission control out of a logging feature.
    pub sanitize_error: Option<SanitizeError>,
}

impl HygieneReport {
    /// Whether anything at all was redacted or masked.
    #[must_use]
    pub fn redacted_anything(&self) -> bool {
        self.sanitize_error.is_some()
            || self
                .sanitize
                .as_ref()
                .is_some_and(SanitizeReport::made_changes)
            || self.pii.as_ref().is_some_and(|r| !r.is_empty())
    }

    /// Number of PII matches masked (`0` when no detector is configured).
    #[must_use]
    pub fn pii_masked(&self) -> usize {
        self.pii.as_ref().map_or(0, PiiReport::total)
    }
}

/// A redacted rendering of a payload, plus what was redacted.
#[derive(Debug, Clone, PartialEq)]
pub struct RedactedPayload {
    /// JSON rendering of the redacted call, in the shape the payload had.
    pub text: String,
    /// What the pass changed.
    pub report: HygieneReport,
}

/// Redacts *copies* of task arguments before they are shown or stored.
///
/// # What this does and does not touch
///
/// Every method takes a copy (or produces a new `String`); none of them can
/// reach the bytes a task executes. That is the whole design constraint: the
/// executing payload must stay byte-identical, or the task computes something
/// different from what the caller asked for — and, when the message is signed,
/// stops verifying.
///
/// # Where `celers` applies it
///
/// Only where a runtime is configured to. In `celers-worker`, setting
/// `WorkerConfig::payload_hygiene` applies it to the payload preview that
/// `inspect active` reports and to the worker's own debug logging of arguments.
/// It is **not** applied to dead-letter entries: a DLQ entry is replayable, so
/// its payload is an executing payload.
///
/// There is no automatic application on a result backend: `celers` does not
/// persist task arguments to a result backend at all (no backend's stored
/// metadata carries them). An integration that adds one must call
/// [`PayloadHygiene::redact_payload`] on the copy it is about to write.
#[derive(Debug, Clone, Default)]
pub struct PayloadHygiene {
    sanitizer: Option<Sanitizer>,
    pii: Option<PiiDetector>,
}

impl PayloadHygiene {
    /// A hygiene pass that does nothing until a sanitizer or detector is added.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Both layers with their default configuration: secret-looking keys are
    /// redacted, control characters stripped, oversize values truncated, and
    /// every PII category masked.
    #[must_use]
    pub fn recommended() -> Self {
        Self {
            sanitizer: Some(Sanitizer::default()),
            pii: Some(PiiDetector::new()),
        }
    }

    /// Redact secret-looking keys and enforce the sanitizer's limits.
    #[must_use]
    pub fn with_sanitizer(mut self, sanitizer: Sanitizer) -> Self {
        self.sanitizer = Some(sanitizer);
        self
    }

    /// Mask PII (emails, card numbers, ...) found in string arguments.
    #[must_use]
    pub fn with_pii_detector(mut self, detector: PiiDetector) -> Self {
        self.pii = Some(detector);
        self
    }

    /// Whether either layer is configured. A `PayloadHygiene` with neither is
    /// a pass-through and callers may skip it entirely.
    #[must_use]
    pub const fn is_enabled(&self) -> bool {
        self.sanitizer.is_some() || self.pii.is_some()
    }

    /// The configured sanitizer, if any.
    #[must_use]
    pub const fn sanitizer(&self) -> Option<&Sanitizer> {
        self.sanitizer.as_ref()
    }

    /// The configured PII detector, if any.
    #[must_use]
    pub const fn pii_detector(&self) -> Option<&PiiDetector> {
        self.pii.as_ref()
    }

    /// Redact a call in place.
    ///
    /// The caller owns `args`/`kwargs`, so this only ever mutates a copy the
    /// caller made. Never fails: a sanitizer complaint is recorded in the
    /// returned report.
    pub fn redact_call(
        &self,
        args: &mut [TaskValue],
        kwargs: &mut [(String, TaskValue)],
    ) -> HygieneReport {
        let mut report = HygieneReport::default();

        if let Some(ref sanitizer) = self.sanitizer {
            match sanitizer.sanitize_call(args, kwargs) {
                Ok(sanitize_report) => report.sanitize = Some(sanitize_report),
                Err(e) => report.sanitize_error = Some(e),
            }
        }
        if let Some(ref detector) = self.pii {
            report.pii = Some(detector.mask_call(args, kwargs));
        }

        report
    }

    /// Produce a redacted JSON rendering of a payload.
    ///
    /// The payload itself is never modified: it is projected with
    /// [`task_call_from_payload`], redacted on that copy, and rendered back in
    /// the shape it came in.
    ///
    /// When the sanitizer rejects the payload outright (too many arguments, a
    /// disallowed value kind, an oversize body) the rendering degrades to a
    /// `<redacted: ...>` marker rather than leaking a partially-sanitized copy.
    #[must_use]
    pub fn redact_payload(&self, payload: &[u8]) -> RedactedPayload {
        let call = task_call_from_payload(payload);
        let TaskCall {
            mut args,
            mut kwargs,
            shape,
        } = call;

        let report = self.redact_call(&mut args, &mut kwargs);

        let text = match report.sanitize_error {
            Some(ref e) => format!("<redacted: payload rejected by sanitizer: {e}>"),
            None => call_to_json(args, kwargs, shape),
        };

        RedactedPayload { text, report }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signer() -> TaskSigner {
        TaskSigner::new(b"unit-test-key-unit-test-key-0123")
    }

    #[test]
    fn envelope_shape_splits_args_and_kwargs() {
        let call = task_call_from_payload(br#"{"args":[1,2],"kwargs":{"note":"hi"}}"#);
        assert_eq!(call.shape, CallShape::Envelope);
        assert_eq!(call.args, vec![TaskValue::Int(1), TaskValue::Int(2)]);
        assert_eq!(
            call.kwargs,
            vec![("note".to_string(), TaskValue::String("hi".to_string()))]
        );
    }

    #[test]
    fn envelope_folds_unknown_top_level_keys_into_kwargs() {
        let call = task_call_from_payload(br#"{"args":[1],"__chain__":["next"]}"#);
        assert_eq!(call.shape, CallShape::Envelope);
        assert_eq!(call.args, vec![TaskValue::Int(1)]);
        assert_eq!(call.kwargs.len(), 1);
        assert_eq!(call.kwargs[0].0, "__chain__");
    }

    #[test]
    fn object_with_non_array_args_key_is_kwargs_not_envelope() {
        let call = task_call_from_payload(br#"{"args":"all"}"#);
        assert_eq!(call.shape, CallShape::Kwargs);
        assert!(call.args.is_empty());
        assert_eq!(
            call.kwargs,
            vec![("args".to_string(), TaskValue::String("all".to_string()))]
        );
    }

    #[test]
    fn bare_array_is_positional_args() {
        let call = task_call_from_payload(br#"[1,"two"]"#);
        assert_eq!(call.shape, CallShape::Args);
        assert_eq!(
            call.args,
            vec![TaskValue::Int(1), TaskValue::String("two".to_string())]
        );
        assert!(call.kwargs.is_empty());
    }

    #[test]
    fn scalar_is_one_positional_arg() {
        let call = task_call_from_payload(br#""hello""#);
        assert_eq!(call.shape, CallShape::Scalar);
        assert_eq!(call.args, vec![TaskValue::String("hello".to_string())]);
    }

    #[test]
    fn non_json_is_one_opaque_arg() {
        let call = task_call_from_payload(&[0x82, 0xa1, 0x61]);
        assert_eq!(call.shape, CallShape::Opaque);
        assert_eq!(call.args, vec![TaskValue::Bytes(vec![0x82, 0xa1, 0x61])]);
        assert!(call.kwargs.is_empty());
    }

    #[test]
    fn sign_then_verify_round_trips_for_every_shape() {
        let signer = signer();
        for payload in [
            br#"{"args":[1],"kwargs":{"a":2}}"#.to_vec(),
            br#"[1,2,3]"#.to_vec(),
            br#"{"to":"a@b.c"}"#.to_vec(),
            br#"42"#.to_vec(),
            vec![0x00, 0x01, 0x02],
        ] {
            let mut task = SerializedTask::new("tasks.demo".to_string(), payload);
            sign_task(&signer, &mut task, SigningOptions::default());
            assert!(
                verify_task(&signer, &task, &SignaturePolicy::default()).is_ok(),
                "round trip failed for {:?}",
                task.payload
            );
        }
    }

    #[test]
    fn a_signature_survives_the_json_wire_every_broker_uses() {
        // Every broker in this workspace enqueues with
        // `serde_json::to_string(&task)` and decodes the same way, so the
        // envelope has to survive a plain serde round trip or verification
        // fails for every real deployment while passing in-process tests.
        let signer = signer();
        let mut task = SerializedTask::new("tasks.add".to_string(), br#"{"args":[2,3]}"#.to_vec());
        task.metadata.on_success_link = Some("tasks.notify".to_string());
        task.metadata.expires_at = Some(Utc::now() + chrono::Duration::hours(1));
        sign_task(&signer, &mut task, SigningOptions::default());

        let wire = serde_json::to_string(&task).expect("serializes");
        let decoded: SerializedTask = serde_json::from_str(&wire).expect("deserializes");

        assert_eq!(decoded.metadata.signature, task.metadata.signature);
        assert!(verify_task(&signer, &decoded, &SignaturePolicy::default()).is_ok());
    }

    #[test]
    fn an_unsigned_message_stays_absent_on_the_wire() {
        // `skip_serializing_if` keeps the field off the wire entirely, so a
        // producer that never signs emits exactly the bytes it always did and
        // an older consumer still parses them.
        let task = SerializedTask::new("tasks.add".to_string(), br#"[1]"#.to_vec());
        let wire = serde_json::to_string(&task).expect("serializes");
        assert!(!wire.contains("signature"), "unexpected field in {wire}");

        let decoded: SerializedTask = serde_json::from_str(&wire).expect("deserializes");
        assert!(decoded.metadata.signature.is_none());
    }

    #[test]
    fn tampered_payload_is_rejected() {
        let signer = signer();
        let mut task = SerializedTask::new("tasks.add".to_string(), br#"[2,3]"#.to_vec());
        sign_task(&signer, &mut task, SigningOptions::default());
        task.payload = br#"[2,4]"#.to_vec();
        assert_eq!(
            verify_task(&signer, &task, &SignaturePolicy::default()),
            Err(SignatureError::Mismatch)
        );
    }

    #[test]
    fn rewritten_success_link_is_rejected() {
        let signer = signer();
        let mut task = SerializedTask::new("tasks.charge".to_string(), br#"[1]"#.to_vec());
        task.metadata.on_success_link = Some("tasks.receipt".to_string());
        sign_task(&signer, &mut task, SigningOptions::default());
        task.metadata.on_success_link = Some("attacker.exfiltrate".to_string());
        assert_eq!(
            verify_task(&signer, &task, &SignaturePolicy::default()),
            Err(SignatureError::Mismatch)
        );
    }

    #[test]
    fn wrong_key_is_rejected() {
        let mut task = SerializedTask::new("tasks.add".to_string(), br#"[2,3]"#.to_vec());
        sign_task(&signer(), &mut task, SigningOptions::default());
        let other = TaskSigner::new(b"a-completely-different-secret-key");
        assert_eq!(
            verify_task(&other, &task, &SignaturePolicy::default()),
            Err(SignatureError::Mismatch)
        );
    }

    #[test]
    fn unsigned_message_respects_require_signature() {
        let task = SerializedTask::new("tasks.add".to_string(), br#"[2,3]"#.to_vec());
        assert_eq!(
            verify_task(&signer(), &task, &SignaturePolicy::default()),
            Err(SignatureError::MissingSignature)
        );
        assert!(verify_task(
            &signer(),
            &task,
            &SignaturePolicy::default().allow_unsigned()
        )
        .is_ok());
    }

    #[test]
    fn replay_guard_rejects_the_second_delivery() {
        let signer = signer();
        let mut task = SerializedTask::new("tasks.add".to_string(), br#"[2,3]"#.to_vec());
        sign_task(&signer, &mut task, SigningOptions::default());

        let guard = Arc::new(ReplayGuard::new(FreshnessWindow::new(
            std::time::Duration::from_secs(300),
        )));
        let policy = SignaturePolicy::default().with_replay_guard(guard);

        assert!(verify_task(&signer, &task, &policy).is_ok());
        assert!(matches!(
            verify_task(&signer, &task, &policy),
            Err(SignatureError::Replayed(_))
        ));
    }

    #[test]
    fn resigning_replaces_the_previous_envelope() {
        let signer = signer();
        let mut task = SerializedTask::new("tasks.add".to_string(), br#"[2,3]"#.to_vec());
        sign_task(&signer, &mut task, SigningOptions::default());
        let first = task.metadata.signature.clone();

        task.payload = br#"[2,4]"#.to_vec();
        sign_task(&signer, &mut task, SigningOptions::default());

        assert_ne!(first, task.metadata.signature);
        assert!(verify_task(&signer, &task, &SignaturePolicy::default()).is_ok());
    }

    #[test]
    fn authenticity_only_stamps_neither_time_nor_nonce() {
        let mut task = SerializedTask::new("tasks.add".to_string(), br#"[2,3]"#.to_vec());
        sign_task(&signer(), &mut task, SigningOptions::authenticity_only());
        let envelope = task.metadata.signature.expect("signed");
        assert!(envelope.signed_at.is_none());
        assert!(envelope.nonce.is_none());
    }

    #[test]
    fn hygiene_masks_pii_and_redacts_secret_keys() {
        let hygiene = PayloadHygiene::recommended();
        let redacted =
            hygiene.redact_payload(br#"{"email":"alice@example.com","api_key":"sk-live-1"}"#);

        assert!(!redacted.text.contains("alice@example.com"));
        assert!(!redacted.text.contains("sk-live-1"));
        assert!(redacted.report.redacted_anything());
        assert_eq!(redacted.report.pii_masked(), 1);
    }

    #[test]
    fn hygiene_preserves_the_payload_shape() {
        let hygiene = PayloadHygiene::recommended();
        assert!(hygiene.redact_payload(br#"[1,2]"#).text.starts_with('['));
        assert!(hygiene.redact_payload(br#"{"a":1}"#).text.starts_with('{'));
        let envelope = hygiene.redact_payload(br#"{"args":[1],"kwargs":{}}"#).text;
        assert!(envelope.contains("\"args\""));
        assert!(envelope.contains("\"kwargs\""));
    }

    #[test]
    fn hygiene_never_touches_the_source_payload() {
        let payload = br#"{"email":"alice@example.com"}"#.to_vec();
        let before = payload.clone();
        let _ = PayloadHygiene::recommended().redact_payload(&payload);
        assert_eq!(payload, before);
    }

    #[test]
    fn sanitizer_rejection_degrades_to_a_marker_instead_of_failing() {
        // Bytes are a disallowed kind by default, so an opaque payload trips a
        // hard limit — which must still produce a rendering, not an error.
        let redacted = PayloadHygiene::recommended().redact_payload(&[0xff, 0xfe, 0xfd]);
        assert!(redacted.text.starts_with("<redacted:"));
        assert!(redacted.report.sanitize_error.is_some());
        assert!(redacted.report.redacted_anything());
    }

    #[test]
    fn disabled_hygiene_is_a_pass_through() {
        let hygiene = PayloadHygiene::new();
        assert!(!hygiene.is_enabled());
        let redacted = hygiene.redact_payload(br#"{"email":"alice@example.com"}"#);
        assert!(redacted.text.contains("alice@example.com"));
        assert!(!redacted.report.redacted_anything());
    }
}
