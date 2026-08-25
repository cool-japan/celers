//! Pluggable backend for scheduler state persistence.
//!
//! [`BeatScheduler`](crate::BeatScheduler) used to have exactly one place its
//! state could live: a local JSON file, written by the atomic-write machinery
//! in `scheduler_persistence.rs`. That is a genuinely well-built mechanism
//! for a single host, but it has no multi-instance story — in a container
//! with an ephemeral filesystem, or across a Kubernetes `Deployment` with
//! more than one replica, `last_run_at` and the rest of the schedule catalog
//! do not survive a restart and are not shared between replicas.
//!
//! [`ScheduleStore`] is the seam that fixes that: a small, byte-oriented
//! trait ([`load`](ScheduleStore::load)/[`save`](ScheduleStore::save)/
//! [`remove`](ScheduleStore::remove)) that [`BeatScheduler`](crate::BeatScheduler)
//! persists its serialized state through instead of talking to the
//! filesystem directly. [`FileScheduleStore`] is the existing file-based
//! behaviour wrapped behind the trait — nothing changes for a caller who
//! never touches this module — and, behind the off-by-default `redis-store`
//! feature, [`RedisScheduleStore`] gives multiple beat instances a shared,
//! durable place for that same state to live.
//!
//! # Leader election — what this module does and does not solve
//!
//! A shared [`ScheduleStore`] answers "where does the schedule catalog
//! live", not "which instance is allowed to dispatch". Those are different
//! problems, already solved differently in this crate:
//!
//! * **Per-fire duplicate-dispatch prevention** is [`crate::dispatch_lock`]'s
//!   job: it acquires a short-lived distributed lock (via
//!   [`celers_core::lock::DistributedLockBackend`], which has Redis-backed
//!   implementations elsewhere in the workspace) scoped to one `(entry,
//!   fire instant)` pair before dispatching, so two instances racing on the
//!   same due entry do not both fire it. This works today, independently of
//!   [`ScheduleStore`], and is what you want wired up before running more
//!   than one beat instance against the same schedule store.
//! * **Writing the schedule catalog itself** (`save_state_async`, called
//!   after every tick) is **not** leader-elected by this module: if two beat
//!   instances both hold a [`ScheduleStore`] and both call `save`, the store
//!   sees two independent writes with **last-write-wins** semantics — a
//!   `RedisScheduleStore` `SET` has no compare-and-swap, and neither does
//!   `FileScheduleStore`'s atomic rename. This is safe (a write is always a
//!   consistent, complete snapshot; a torn write is not possible), but it
//!   means the last instance to finish a tick decides what the next restart
//!   sees — the two instances' local views of `last_run_at` can still
//!   diverge between saves. Run a single active writer (the leader from
//!   [`crate::heartbeat`], or an external orchestrator's singleton
//!   guarantee) if that divergence matters for your deployment; the
//!   dispatch lock alone is sufficient to prevent duplicate *task
//!   execution*, which is usually the actual correctness requirement.

use async_trait::async_trait;

use crate::config::ScheduleError;

/// A place [`BeatScheduler`](crate::BeatScheduler)'s serialized state can
/// durably live.
///
/// Deliberately byte-oriented rather than typed on `BeatScheduler` itself:
/// the scheduler already persists by serializing itself wholesale
/// (`serde_json::to_vec(&self)`, see `scheduler_persistence.rs`), so a store
/// that moves opaque bytes is both a faithful match for how persistence
/// actually works today and reusable by anything else that wants a durable,
/// swappable blob store without depending on this crate's schema.
#[async_trait]
pub trait ScheduleStore: Send + Sync + std::fmt::Debug {
    /// Load the most recently saved state, or `Ok(None)` if nothing has been
    /// saved yet (a fresh deployment, or a store that was explicitly
    /// [`remove`](Self::remove)d).
    ///
    /// # Errors
    ///
    /// Returns [`ScheduleError::Persistence`] if the store is reachable but
    /// the read failed, or if what was read is present but unusable (e.g. a
    /// corrupt file with no readable backup). A store that is simply *empty*
    /// is `Ok(None)`, not an error.
    async fn load(&self) -> Result<Option<Vec<u8>>, ScheduleError>;

    /// Durably persist `bytes`, replacing whatever was previously saved.
    ///
    /// # Errors
    ///
    /// Returns [`ScheduleError::Persistence`] if the write could not be
    /// completed. A failed `save` must not leave a previously-good save
    /// corrupted — implementations are expected to fail atomically (the old
    /// value survives) rather than partially.
    async fn save(&self, bytes: &[u8]) -> Result<(), ScheduleError>;

    /// Remove any saved state, so a subsequent [`load`](Self::load) returns
    /// `Ok(None)`.
    ///
    /// # Errors
    ///
    /// Returns [`ScheduleError::Persistence`] if the removal could not be
    /// completed. Removing a store that already has nothing saved is a
    /// no-op, not an error.
    async fn remove(&self) -> Result<(), ScheduleError>;
}

mod file_store;
pub use file_store::FileScheduleStore;

#[cfg(feature = "redis-store")]
mod redis_store;
#[cfg(feature = "redis-store")]
pub use redis_store::RedisScheduleStore;
