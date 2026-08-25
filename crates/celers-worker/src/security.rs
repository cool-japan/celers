//! Message authentication and payload hygiene for the worker receive path.
//!
//! Both controls here are **opt-in and off by default**. A worker built from
//! [`WorkerConfig::default()`](crate::WorkerConfig) verifies no signatures and
//! redacts nothing, which is exactly what it did before this module existed.
//!
//! # Signature verification
//!
//! Set [`WorkerConfig::signature_verification`](crate::WorkerConfig::signature_verification)
//! to a [`SignatureVerification`] and every dequeued message is checked
//! **before dispatch** — before the revocation registry, the poison-pill
//! strike table, routing, or any other admission decision, so an unauthenticated
//! message can never seed worker-local state keyed on its own task id or name.
//!
//! A message that fails is not executed and not requeued:
//!
//! * with a dead-letter queue configured, it is recorded there with
//!   `failure_type = "signature_verification"` and rejected without requeue;
//! * without one, it is rejected without requeue — dropped.
//!
//! Either way a `task-rejected` event is emitted and
//! [`WorkerStats::signature_rejected`](crate::WorkerStats::signature_rejected)
//! counts it.
//!
//! The producer side is [`celers_core::task_security::sign_task`]; the two share
//! the projection in [`celers_core::task_security::signed_fields`], so what the
//! producer signed is exactly what the worker checks.
//!
//! ## Everything that enqueues must hold the key
//!
//! Turning verification on makes the signing key a *deployment-wide*
//! requirement: every component that builds a **new** message must sign it, or
//! that message is dead-lettered on arrival.
//!
//! * **Your application code**, wherever it enqueues — call
//!   [`sign_task`](celers_core::task_security::sign_task) on the task
//!   immediately before handing it to the broker. This is the one that matters.
//! * **`celers-beat`** does *not* enqueue: it is a schedule engine with no
//!   broker handle at all, so whatever your integration enqueues when a
//!   schedule fires is application code and follows the rule above.
//! * **`celers-cli`** moves existing messages rather than minting new ones —
//!   `task retry` and `dlq replay` re-enqueue the stored bytes, so a signed
//!   message stays signed and stays valid (`state` and `updated_at` are not
//!   covered by the MAC). The exception is `loadtest`, which mints synthetic
//!   unsigned tasks: a verifying worker will reject them.
//!
//! The worker signs its **own** enqueues automatically — retry attempts and
//! workflow continuations (chain successors, `on_success_link` targets, chord
//! callbacks) are freshly constructed messages, and it re-signs each one with
//! [`SignatureVerification::sign`]. Without that, switching verification on
//! would kill every retry and every chain at the first hop.
//!
//! ## Freshness, replay and redelivery
//!
//! [`SignatureVerification::with_freshness`] and
//! [`SignatureVerification::with_replay_guard`] both reason about a message
//! being seen *once*. CeleRS is an at-least-once system, so several paths hand
//! the **same bytes** — same `signed_at`, same nonce — back to the broker:
//!
//! * an admission deferral (routing, affinity, feature flags, rate limiting)
//!   requeues the delivery;
//! * a graceful-shutdown drain requeues whatever was still in flight;
//! * the broker redelivers after a lost worker or an expired visibility
//!   timeout.
//!
//! On the next delivery a replay guard reports
//! [`SignatureError::Replayed`] and a narrow freshness window reports
//! [`SignatureError::Stale`], and the message is dead-lettered rather than run.
//! **Retries are not affected** — the worker re-signs those with a fresh nonce.
//!
//! So: enable a replay guard only where those paths do not occur (no admission
//! deferrals configured, an ack-on-delivery broker), or accept that a deferred
//! or redelivered message lands in the DLQ. For everything else, prefer
//! [`with_freshness`](SignatureVerification::with_freshness) with a window
//! comfortably longer than your longest deferral and shutdown drain.
//!
//! # Payload hygiene
//!
//! Set [`WorkerConfig::payload_hygiene`](crate::WorkerConfig::payload_hygiene)
//! to a [`PayloadHygiene`] and the worker redacts secret-looking keys and masks
//! PII in the payload **copies** it shows to operators:
//!
//! * the bounded payload preview `inspect active` reports, and
//! * the worker's own `debug!` rendering of a task's arguments.
//!
//! It never touches the payload a task executes, and it is deliberately **not**
//! applied to dead-letter entries: a DLQ entry is replayable, so its payload is
//! an executing payload. `celers` does not persist task arguments to a result
//! backend at all, so there is nothing to redact there either — an integration
//! that adds one must call
//! [`PayloadHygiene::redact_payload`](celers_core::task_security::PayloadHygiene::redact_payload)
//! itself.
//!
//! # Example
//!
//! ```no_run
//! use celers_core::task_security::PayloadHygiene;
//! use celers_core::task_signature::TaskSigner;
//! use celers_worker::{SignatureVerification, WorkerConfig};
//!
//! let config = WorkerConfig::builder()
//!     .signature_verification(SignatureVerification::new(TaskSigner::new(
//!         std::env::var("CELERS_TASK_SIGNING_KEY").unwrap_or_default(),
//!     )))
//!     .payload_hygiene(PayloadHygiene::recommended())
//!     .build_unchecked();
//! # let _ = config;
//! ```

use celers_core::task_security::{sign_task, verify_task, SignaturePolicy, SigningOptions};
use celers_core::task_signature::{FreshnessWindow, ReplayGuard, SignatureError, TaskSigner};
use celers_core::SerializedTask;

use std::sync::Arc;

/// The worker's message-authentication configuration: a key plus a policy.
///
/// Cloning is cheap in the sense that matters — the [`ReplayGuard`], when one
/// is configured, is shared behind an [`Arc`], so every clone rejects the same
/// replays. The signing key itself is copied but never printed (its [`Debug`]
/// reports only its length).
#[derive(Clone)]
pub struct SignatureVerification {
    signer: TaskSigner,
    policy: SignaturePolicy,
}

impl std::fmt::Debug for SignatureVerification {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SignatureVerification")
            .field("signer", &self.signer)
            .field("policy", &self.policy)
            .finish()
    }
}

impl SignatureVerification {
    /// Require every message to carry a valid signature made with `signer`'s
    /// key.
    ///
    /// Neither freshness nor replay protection is applied until you add them:
    /// an authentic message stays acceptable forever, so a captured one can be
    /// re-delivered. See [`Self::with_freshness`] and
    /// [`Self::with_replay_guard`].
    #[must_use]
    pub fn new(signer: TaskSigner) -> Self {
        Self {
            signer,
            policy: SignaturePolicy::default(),
        }
    }

    /// Build from an explicit [`SignaturePolicy`].
    #[must_use]
    pub const fn with_policy(signer: TaskSigner, policy: SignaturePolicy) -> Self {
        Self { signer, policy }
    }

    /// **Migration mode**: verify signed messages, admit unsigned ones.
    ///
    /// This lets a fleet roll over producer-first — a producer that already
    /// signs is checked, one that does not yet is still served. It is not a
    /// security posture: while it is on, an attacker only has to omit the
    /// signature. Turn it off once every producer signs.
    #[must_use]
    pub fn allow_unsigned(mut self) -> Self {
        self.policy.require_signature = false;
        self
    }

    /// Additionally reject an authentic message older than `window`.
    ///
    /// Requires the producer to stamp `signed_at`
    /// ([`SigningOptions::stamp_signed_at`](celers_core::task_security::SigningOptions),
    /// on by default); a message without one is rejected as
    /// [`SignatureError::MissingSignedAt`].
    #[must_use]
    pub fn with_freshness(mut self, window: FreshnessWindow) -> Self {
        self.policy.freshness = Some(window);
        self
    }

    /// Additionally reject a message whose nonce this process already accepted.
    ///
    /// The guard enforces its own freshness window, which supersedes
    /// [`Self::with_freshness`]. It is **per process**: N worker processes each
    /// accept a given replay once. Global single-use semantics need shared
    /// storage behind the nonce (a Redis `SET NX PX`, a unique index).
    #[must_use]
    pub fn with_replay_guard(mut self, guard: Arc<ReplayGuard>) -> Self {
        self.policy.replay_guard = Some(guard);
        self
    }

    /// The policy this verifier applies.
    #[must_use]
    pub const fn policy(&self) -> &SignaturePolicy {
        &self.policy
    }

    /// Sign a task **this worker is enqueueing itself**.
    ///
    /// A verifying worker is also a key holder, and it is a producer: it
    /// enqueues retry attempts and workflow continuations (chain successors,
    /// `on_success_link` targets, chord callbacks). Those are freshly
    /// constructed messages — they carry no signature of their own — so without
    /// this the worker would enqueue a message and then reject its own
    /// delivery of it, silently killing every retry and every workflow the
    /// moment verification was switched on.
    ///
    /// A fresh `signed_at` and nonce are stamped each time
    /// ([`SigningOptions::default`](celers_core::task_security::SigningOptions)),
    /// so a re-signed retry is a *new* message as far as a
    /// [`FreshnessWindow`] or a [`ReplayGuard`] is concerned rather than a
    /// replay of the attempt it descends from.
    ///
    /// The worker calls this for itself; you only need it when you enqueue
    /// tasks from your own code.
    pub fn sign(&self, task: &mut SerializedTask) {
        sign_task(&self.signer, task, SigningOptions::default());
    }

    /// Verify one received message.
    ///
    /// # Errors
    ///
    /// Any [`SignatureError`] the policy produces: a missing signature, a MAC
    /// mismatch, a stale/future-dated/expired message, or a replay.
    pub fn verify(&self, task: &SerializedTask) -> Result<(), SignatureError> {
        verify_task(&self.signer, task, &self.policy)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use celers_core::task_security::{sign_task, SigningOptions};

    fn signed(payload: &[u8]) -> (TaskSigner, SerializedTask) {
        let signer = TaskSigner::new(b"worker-unit-test-key-worker-unit!");
        let mut task = SerializedTask::new("tasks.demo".to_string(), payload.to_vec());
        sign_task(&signer, &mut task, SigningOptions::default());
        (signer, task)
    }

    #[test]
    fn a_valid_signature_is_accepted() {
        let (signer, task) = signed(br#"[1,2]"#);
        assert!(SignatureVerification::new(signer).verify(&task).is_ok());
    }

    #[test]
    fn a_tampered_payload_is_rejected() {
        let (signer, mut task) = signed(br#"[1,2]"#);
        task.payload = br#"[1,3]"#.to_vec();
        assert_eq!(
            SignatureVerification::new(signer).verify(&task),
            Err(SignatureError::Mismatch)
        );
    }

    #[test]
    fn an_unsigned_message_is_rejected_unless_migration_mode_is_on() {
        let signer = TaskSigner::new(b"worker-unit-test-key-worker-unit!");
        let task = SerializedTask::new("tasks.demo".to_string(), br#"[1]"#.to_vec());

        assert_eq!(
            SignatureVerification::new(signer.clone()).verify(&task),
            Err(SignatureError::MissingSignature)
        );
        assert!(SignatureVerification::new(signer)
            .allow_unsigned()
            .verify(&task)
            .is_ok());
    }

    #[test]
    fn a_replay_guard_rejects_the_second_delivery() {
        let (signer, task) = signed(br#"[1,2]"#);
        let verification = SignatureVerification::new(signer).with_replay_guard(Arc::new(
            ReplayGuard::new(FreshnessWindow::new(std::time::Duration::from_secs(300))),
        ));

        assert!(verification.verify(&task).is_ok());
        assert!(matches!(
            verification.verify(&task),
            Err(SignatureError::Replayed(_))
        ));
    }

    #[test]
    fn a_clone_shares_the_replay_guard() {
        let (signer, task) = signed(br#"[1,2]"#);
        let verification = SignatureVerification::new(signer).with_replay_guard(Arc::new(
            ReplayGuard::new(FreshnessWindow::new(std::time::Duration::from_secs(300))),
        ));
        let clone = verification.clone();

        assert!(verification.verify(&task).is_ok());
        assert!(
            clone.verify(&task).is_err(),
            "a cloned verifier must not forget what the original accepted"
        );
    }

    #[test]
    fn debug_never_prints_the_key() {
        let signer = TaskSigner::new(b"super-secret-signing-key-material");
        let rendered = format!("{:?}", SignatureVerification::new(signer));
        assert!(!rendered.contains("super-secret"));
    }
}
