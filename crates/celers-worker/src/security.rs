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

use celers_core::task_security::{verify_task, SignaturePolicy};
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
