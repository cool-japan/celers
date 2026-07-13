//! Per-fire dispatch locking (duplicate-execution prevention)
//!
//! When several beat instances run against the same schedule store — for
//! redundancy or during a rolling deploy — they will all independently decide
//! that the same entry is due at the same instant. Without coordination each
//! instance would dispatch the task, producing duplicate fires.
//!
//! This module closes that gap by acquiring a short-lived distributed lock
//! *before* an entry is dispatched and releasing it afterwards. The lock key is
//! derived from the entry name together with the precise scheduled fire instant
//! (see [`dispatch_lock_key`]), so the mutual exclusion is scoped to a single
//! fire: exactly one instance wins the race for a given `(entry, instant)` and
//! the losers skip that fire. Once the winner releases the lock and marks the
//! entry as run, its *next* fire is a different instant — hence a different key —
//! and is free to be acquired again.
//!
//! The lock operations are delegated to the configured
//! [`celers_core::lock::DistributedLockBackend`] when one is set (Redis, a
//! database, or [`crate::lock::InMemoryLockBackend`] for tests/single-process),
//! and fall back to the in-process [`crate::lock::LockManager`] otherwise. This
//! mirrors the backend-selection logic already used by Phase 9 leader election.

use crate::config::ScheduleError;
use crate::scheduler::BeatScheduler;
use chrono::{DateTime, Utc};

/// Default time-to-live, in seconds, for a per-fire dispatch lock.
///
/// The lock only needs to cover the brief window between deciding to dispatch an
/// entry and actually handing it off, so the TTL is short. It still acts as a
/// safety valve: if a holder crashes mid-dispatch the lock expires and another
/// instance can recover the fire.
pub const DEFAULT_DISPATCH_LOCK_TTL_SECS: u64 = 30;

/// Build the deterministic lock key for a single scheduled fire.
///
/// The key combines the entry name and the scheduled fire instant (rendered as
/// RFC 3339, which is timezone-explicit and stable) so that:
///
/// * two instances evaluating the *same* entry for the *same* instant produce
///   the identical key and therefore contend for one lock; and
/// * a later fire of the same entry (a different instant) produces a distinct
///   key and never contends with the earlier fire.
///
/// # Examples
/// ```
/// use celers_beat::dispatch_lock::dispatch_lock_key;
/// use chrono::{TimeZone, Utc};
///
/// let instant = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
/// let key = dispatch_lock_key("send_report", instant);
/// assert_eq!(key, "celers-beat:dispatch:send_report@2026-01-01T00:00:00+00:00");
/// ```
pub fn dispatch_lock_key(entry_name: &str, scheduled_instant: DateTime<Utc>) -> String {
    format!(
        "celers-beat:dispatch:{}@{}",
        entry_name,
        scheduled_instant.to_rfc3339()
    )
}

impl BeatScheduler {
    /// Try to acquire the per-fire dispatch lock for `(entry_name, instant)`.
    ///
    /// Returns `Ok(true)` if this instance now holds the lock and may dispatch
    /// the fire, or `Ok(false)` if another instance already holds it. Uses the
    /// distributed lock backend when configured, otherwise the local
    /// [`crate::lock::LockManager`].
    ///
    /// The lock is keyed via [`dispatch_lock_key`]; callers must release it with
    /// [`BeatScheduler::release_dispatch_lock`] using the same arguments.
    pub async fn try_acquire_dispatch_lock(
        &mut self,
        entry_name: &str,
        scheduled_instant: DateTime<Utc>,
        ttl_secs: u64,
    ) -> Result<bool, ScheduleError> {
        let key = dispatch_lock_key(entry_name, scheduled_instant);
        self.try_acquire_distributed_lock(&key, ttl_secs).await
    }

    /// Release the per-fire dispatch lock for `(entry_name, instant)`.
    ///
    /// Returns `Ok(true)` if the lock was held by this instance and released.
    /// Uses the distributed lock backend when configured, otherwise the local
    /// [`crate::lock::LockManager`].
    pub async fn release_dispatch_lock(
        &mut self,
        entry_name: &str,
        scheduled_instant: DateTime<Utc>,
    ) -> Result<bool, ScheduleError> {
        let key = dispatch_lock_key(entry_name, scheduled_instant);
        self.release_distributed_lock(&key).await
    }

    /// Perform a scheduler tick that guards each dispatch with a per-fire lock.
    ///
    /// This is the duplicate-safe counterpart of [`BeatScheduler::tick`]. The
    /// flow is:
    ///
    /// 1. Tick the heartbeat (leader election / lease renewal). Standby
    ///    instances return immediately with an empty list.
    /// 2. Collect the entries that are due, each paired with its precise
    ///    scheduled fire instant (jitter included, matching
    ///    [`crate::task::ScheduledTask::next_run_time`]).
    /// 3. For every due entry, attempt to acquire the
    ///    [`dispatch_lock_key`]-scoped lock. If acquisition succeeds the entry
    ///    is marked as run and added to the returned list. If another instance
    ///    already holds the lock the entry is skipped — preventing a duplicate
    ///    fire of that exact instant.
    /// 4. Update heartbeat info and persist state if configured.
    ///
    /// The returned vector contains the names of the entries this instance
    /// actually dispatched. Across cooperating instances sharing a lock backend,
    /// each due fire is dispatched by exactly one instance.
    ///
    /// # Lock lifetime
    ///
    /// The per-fire lock is intentionally **held for its TTL rather than
    /// released as soon as the entry is marked as run**. The lock is the durable
    /// claim that "this exact fire has been taken"; releasing it immediately
    /// would let a sibling instance that ticks a moment later re-acquire the
    /// same key and dispatch the same fire again. Holding the claim until the
    /// (short) TTL expires guarantees single-fire semantics even when sibling
    /// ticks do not overlap in time. By the time the TTL lapses the winning
    /// instance has advanced the entry's `last_run_at`, so its *next* fire is a
    /// different instant — a different key — and contends afresh. (Callers that
    /// genuinely want to relinquish a claim early can still do so explicitly via
    /// [`BeatScheduler::release_dispatch_lock`].)
    ///
    /// # Arguments
    /// * `lock_ttl_secs` - TTL for each per-fire lock. Pass
    ///   [`DEFAULT_DISPATCH_LOCK_TTL_SECS`] for the standard short-lived lock.
    pub async fn tick_with_locks(
        &mut self,
        lock_ttl_secs: u64,
    ) -> Result<Vec<String>, ScheduleError> {
        // Step 1: Heartbeat tick + leadership gate (mirrors `tick`).
        let role = self.heartbeat_tick().await?;
        let is_leader = match role {
            Some(crate::heartbeat::BeatRole::Leader) => true,
            Some(_) => false,
            None => true, // No heartbeat configured => single instance.
        };

        if !is_leader {
            self.update_heartbeat_info().await;
            return Ok(Vec::new());
        }

        // Step 2: Snapshot due entries with their scheduled fire instants.
        // We resolve the instant up front (immutable borrow) so the subsequent
        // mutable lock/mark operations don't fight the borrow checker.
        let due_fires: Vec<(String, DateTime<Utc>)> = self
            .get_due_tasks_by_priority()
            .iter()
            .filter_map(|task| {
                // The fire instant is the (jitter-aware) next run time. If it
                // cannot be computed the entry is skipped rather than dispatched
                // blindly.
                task.next_run_time()
                    .ok()
                    .map(|instant| (task.name.clone(), instant))
            })
            .collect();

        // Step 3: Lock-guarded dispatch.
        let mut dispatched = Vec::new();
        for (name, instant) in due_fires {
            let acquired = self
                .try_acquire_dispatch_lock(&name, instant, lock_ttl_secs)
                .await?;

            if !acquired {
                // Another instance owns this exact fire; skip to avoid a
                // duplicate dispatch.
                continue;
            }

            // We own the fire: mark it as run and record it as dispatched.
            // `mark_task_run` advances `last_run_at`, so the entry's *next*
            // fire is a fresh instant (and thus a fresh lock key).
            //
            // The per-fire lock is deliberately NOT released here; it is the
            // durable claim on this instant and is allowed to lapse by its TTL
            // (see the method docs). Early release would permit a sibling
            // instance ticking slightly later to re-dispatch the same fire.
            let _ = self.mark_task_run(&name);
            dispatched.push(name.clone());
        }

        // Step 4: Refresh heartbeat metadata and persist.
        self.update_heartbeat_info().await;
        let _ = self.save_state();

        Ok(dispatched)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn key_is_stable_for_same_entry_and_instant() {
        let instant = Utc
            .with_ymd_and_hms(2026, 6, 13, 9, 30, 0)
            .single()
            .expect("valid instant");
        let a = dispatch_lock_key("report", instant);
        let b = dispatch_lock_key("report", instant);
        assert_eq!(a, b);
    }

    #[test]
    fn key_differs_for_different_instants() {
        let first = Utc
            .with_ymd_and_hms(2026, 6, 13, 9, 30, 0)
            .single()
            .expect("valid instant");
        let second = first + chrono::Duration::seconds(60);
        assert_ne!(
            dispatch_lock_key("report", first),
            dispatch_lock_key("report", second)
        );
    }

    #[test]
    fn key_differs_for_different_entries() {
        let instant = Utc
            .with_ymd_and_hms(2026, 6, 13, 9, 30, 0)
            .single()
            .expect("valid instant");
        assert_ne!(
            dispatch_lock_key("entry_a", instant),
            dispatch_lock_key("entry_b", instant)
        );
    }
}
