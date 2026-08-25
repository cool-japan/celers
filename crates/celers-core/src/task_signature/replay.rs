//! Freshness and replay rejection for signed task messages.
//!
//! Split out of [`task_signature`](super); both types are re-exported from
//! there, which is the path the rest of the workspace uses.
//!
//! [`FreshnessWindow`] bounds how old (or how future-dated) an authenticated
//! message may be. [`ReplayGuard`] adds single-use nonce rejection on top of
//! it — within one process. See the [`ReplayGuard`] docs for what that scope
//! does and does not buy, and what a cluster-wide guard would require.

use super::{SignatureError, SignedFields, TaskSignature, TaskSigner};
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

// ===========================================================================
// Freshness & replay
// ===========================================================================

/// How old an authenticated message may be before it is rejected.
///
/// Both bounds are caller-supplied on purpose: the right window depends on the
/// deployment (a few seconds for an RPC-like queue, minutes for a batch
/// pipeline), and no default can be correct for all of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FreshnessWindow {
    /// Maximum age of `signed_at` relative to now.
    pub max_age: Duration,
    /// Tolerance for a `signed_at` in the future (producer clock ahead).
    pub max_clock_skew: Duration,
}

impl FreshnessWindow {
    /// Default tolerance for a producer clock running ahead of the consumer.
    pub const DEFAULT_CLOCK_SKEW: Duration = Duration::from_secs(60);

    /// A window of `max_age` with [`Self::DEFAULT_CLOCK_SKEW`].
    #[must_use]
    pub const fn new(max_age: Duration) -> Self {
        Self {
            max_age,
            max_clock_skew: Self::DEFAULT_CLOCK_SKEW,
        }
    }

    /// Override the permitted clock skew.
    #[must_use]
    pub const fn with_max_clock_skew(mut self, max_clock_skew: Duration) -> Self {
        self.max_clock_skew = max_clock_skew;
        self
    }

    /// Check the freshness of already-authenticated fields against `now`.
    ///
    /// Exposed so a caller that verified the MAC separately (or that batches
    /// verification) can still apply the same policy.
    ///
    /// # Errors
    ///
    /// [`SignatureError::MissingSignedAt`], [`SignatureError::Stale`],
    /// [`SignatureError::FutureDated`] or [`SignatureError::MessageExpired`].
    pub fn check_at(
        &self,
        fields: &SignedFields,
        now: DateTime<Utc>,
    ) -> Result<(), SignatureError> {
        let signed_at = fields.signed_at.ok_or(SignatureError::MissingSignedAt)?;
        let age = now - signed_at;

        if age.num_seconds() < 0 {
            let skew_secs = -age.num_seconds();
            let max_skew_secs = self.max_clock_skew.as_secs();
            if skew_secs > i64::try_from(max_skew_secs).unwrap_or(i64::MAX) {
                return Err(SignatureError::FutureDated {
                    skew_secs,
                    max_skew_secs,
                });
            }
        } else {
            let max_age_secs = self.max_age.as_secs();
            if age.num_seconds() > i64::try_from(max_age_secs).unwrap_or(i64::MAX) {
                return Err(SignatureError::Stale {
                    age_secs: age.num_seconds(),
                    max_age_secs,
                });
            }
        }

        if let Some(expires) = fields.expires {
            if now > expires {
                return Err(SignatureError::MessageExpired {
                    expired_secs_ago: (now - expires).num_seconds(),
                });
            }
        }

        Ok(())
    }
}

/// Rejects a signed message whose nonce has already been accepted.
///
/// # Scope
///
/// This cache lives in **one process**. It stops the same message being
/// executed twice by the same worker, which is what a local `verify` loop can
/// guarantee on its own; it does not coordinate between workers, so N worker
/// processes will each accept a replayed message once. A deployment that needs
/// global single-use semantics must back the check with shared storage (a Redis
/// `SET key NX PX`, a unique index on the nonce) — this type is the local half
/// of that design, not a substitute for it.
///
/// Entries are pruned lazily: a nonce older than the freshness window can be
/// dropped because such a message is rejected by the freshness check anyway.
///
/// # Backing it with shared storage
///
/// There is deliberately no pluggable nonce-store trait here yet, because the
/// only correct implementation needs a dependency celers-core does not have.
/// The design a backend crate should follow, so the two halves agree:
///
/// 1. Verify the MAC and the freshness window **first** — exactly as
///    [`ReplayGuard::verify_at`] does — so a forged or stale message can never
///    reach the shared store and burn a nonce the genuine message needs.
/// 2. Claim the nonce atomically. In Redis that is one round trip:
///    `SET celers:nonce:{nonce} 1 NX PX {retain_ms}` — `NX` makes the claim
///    atomic across every worker, and a reply of `nil` (the key existed) is
///    [`SignatureError::Replayed`]. In SQL it is an `INSERT` against a unique
///    index on the nonce, with a unique-violation mapping to the same error.
/// 3. Set `retain_ms` to `max_age + max_clock_skew` (see
///    [`FreshnessWindow`]) and let the store expire entries itself. A nonce
///    older than that window is already refused by step 1, so remembering it
///    longer buys nothing and grows without bound.
/// 4. Fail **closed**: if the shared store is unreachable, reject the message
///    rather than falling back to a local check. Falling back silently returns
///    the deployment to per-process semantics at exactly the moment an attacker
///    would want it to.
///
/// Until that exists, a fleet configured with this guard gets "each worker
/// process accepts a given replay at most once" — strictly better than no
/// guard, and strictly weaker than single-use.
#[derive(Debug)]
pub struct ReplayGuard {
    window: FreshnessWindow,
    seen: Mutex<HashMap<String, DateTime<Utc>>>,
}

impl ReplayGuard {
    /// Create a guard enforcing `window` plus single-use nonces.
    #[must_use]
    pub fn new(window: FreshnessWindow) -> Self {
        Self {
            window,
            seen: Mutex::new(HashMap::new()),
        }
    }

    /// The freshness window this guard enforces.
    #[must_use]
    pub const fn window(&self) -> &FreshnessWindow {
        &self.window
    }

    /// Number of nonces currently remembered.
    #[must_use]
    pub fn remembered(&self) -> usize {
        self.lock_seen().len()
    }

    /// Full check against the current clock: MAC, freshness, and single-use
    /// nonce.
    ///
    /// # Errors
    ///
    /// See [`ReplayGuard::verify_at`].
    pub fn verify(
        &self,
        signer: &TaskSigner,
        fields: &SignedFields,
        signature: &TaskSignature,
    ) -> Result<(), SignatureError> {
        self.verify_at(signer, fields, signature, Utc::now())
    }

    /// Full check against an explicit `now`.
    ///
    /// The nonce is recorded only after the signature and the freshness window
    /// have both passed, so a forged or stale message cannot poison the cache
    /// and lock out the genuine one.
    ///
    /// # Errors
    ///
    /// Anything [`TaskSigner::verify_fresh_at`] returns, plus
    /// [`SignatureError::MissingNonce`] when the message carries no nonce and
    /// [`SignatureError::Replayed`] when the nonce was already accepted.
    pub fn verify_at(
        &self,
        signer: &TaskSigner,
        fields: &SignedFields,
        signature: &TaskSignature,
        now: DateTime<Utc>,
    ) -> Result<(), SignatureError> {
        signer.verify_fresh_at(fields, signature, &self.window, now)?;

        let nonce = fields.nonce.as_ref().ok_or(SignatureError::MissingNonce)?;

        let mut seen = self.lock_seen();
        Self::prune(&mut seen, &self.window, now);
        if seen.contains_key(nonce) {
            return Err(SignatureError::Replayed(nonce.clone()));
        }
        seen.insert(nonce.clone(), now);
        Ok(())
    }

    /// Drop remembered nonces that can no longer be replayed within the
    /// freshness window.
    pub fn prune_at(&self, now: DateTime<Utc>) {
        let mut seen = self.lock_seen();
        Self::prune(&mut seen, &self.window, now);
    }

    fn prune(
        seen: &mut HashMap<String, DateTime<Utc>>,
        window: &FreshnessWindow,
        now: DateTime<Utc>,
    ) {
        let horizon = i64::try_from(
            window
                .max_age
                .as_secs()
                .saturating_add(window.max_clock_skew.as_secs()),
        )
        .unwrap_or(i64::MAX);
        seen.retain(|_, accepted_at| (now - *accepted_at).num_seconds() <= horizon);
    }

    /// Lock the cache, recovering from poisoning rather than panicking: a
    /// poisoned mutex here only means some other thread panicked mid-check, and
    /// the remembered set is still sound to use.
    fn lock_seen(&self) -> std::sync::MutexGuard<'_, HashMap<String, DateTime<Utc>>> {
        self.seen.lock().unwrap_or_else(|e| e.into_inner())
    }
}
