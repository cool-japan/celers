//! Task Revocation
//!
//! This module provides enhanced task revocation capabilities:
//!
//! - **Revoke by ID**: Revoke a specific task by its ID
//! - **Revoke by Pattern**: Revoke tasks matching a name pattern (glob or regex)
//! - **Bulk Revocation**: Revoke multiple tasks at once
//! - **Persistent Revocation**: Revocations that survive worker restarts
//!
//! # Example
//!
//! ```rust
//! use celers_core::revocation::{RevocationManager, RevocationMode};
//! use uuid::Uuid;
//!
//! let mut manager = RevocationManager::new();
//! let task_id = Uuid::new_v4();
//!
//! // Revoke a single task
//! manager.revoke(task_id, RevocationMode::Terminate);
//!
//! // Revoke all tasks matching a pattern
//! manager.revoke_by_pattern("email.*", RevocationMode::Ignore);
//!
//! // Check if a task is revoked
//! assert!(manager.is_revoked(task_id));
//! ```

use crate::router::PatternMatcher;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// Default upper bound on the number of revoked task ids retained.
///
/// Matches Celery's `worker_state.revoked` bound (`REVOKES_MAX`): revocations
/// created without an explicit expiry never expire, so without a cap the set
/// grows for the lifetime of the process.
pub const DEFAULT_MAX_REVOKED_IDS: usize = 50_000;

/// Default upper bound on the number of terminated task ids retained.
pub const DEFAULT_MAX_TERMINATED_IDS: usize = 50_000;

/// Default retention window for terminated task ids.
///
/// A terminated id only needs to be remembered long enough for in-flight
/// bookkeeping to observe it; a day is generous.
pub const DEFAULT_TERMINATED_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// Owned identity of a [`PatternMatcher`], used to deduplicate pattern
/// revocations (`PatternMatcher` itself is not `PartialEq`).
fn matcher_identity(matcher: &PatternMatcher) -> (u8, String) {
    match matcher {
        PatternMatcher::Exact(s) => (0, s.clone()),
        PatternMatcher::Glob(g) => (1, g.pattern().to_string()),
        PatternMatcher::Regex(r) => (2, r.pattern().to_string()),
        PatternMatcher::All => (3, "*".to_string()),
    }
}

/// Revocation mode for how to handle a revoked task
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum RevocationMode {
    /// Terminate the task if already running
    #[default]
    Terminate,
    /// Ignore the task (don't execute but don't terminate if running)
    Ignore,
}

/// A request to revoke a task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RevocationRequest {
    /// Task ID to revoke
    pub task_id: Uuid,
    /// Revocation mode
    pub mode: RevocationMode,
    /// When the revocation was issued (Unix timestamp)
    pub timestamp: f64,
    /// Optional expiration time (Unix timestamp)
    pub expires: Option<f64>,
    /// Reason for revocation
    pub reason: Option<String>,
    /// Signal to send (for terminate mode)
    pub signal: Option<String>,
}

impl RevocationRequest {
    /// Create a new revocation request
    #[must_use]
    pub fn new(task_id: Uuid, mode: RevocationMode) -> Self {
        Self {
            task_id,
            mode,
            timestamp: current_timestamp(),
            expires: None,
            reason: None,
            signal: None,
        }
    }

    /// Set expiration time
    #[must_use]
    pub fn with_expiration(mut self, expires_in: Duration) -> Self {
        self.expires = Some(current_timestamp() + expires_in.as_secs_f64());
        self
    }

    /// Set reason for revocation
    #[must_use]
    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    /// Set signal to send (for terminate mode)
    #[must_use]
    pub fn with_signal(mut self, signal: impl Into<String>) -> Self {
        self.signal = Some(signal.into());
        self
    }

    /// Check if this revocation has expired
    #[inline]
    #[must_use]
    pub fn is_expired(&self) -> bool {
        if let Some(expires) = self.expires {
            current_timestamp() > expires
        } else {
            false
        }
    }
}

/// A pattern-based revocation rule
#[derive(Debug, Clone)]
pub struct PatternRevocation {
    /// Pattern matcher for task names
    pub pattern: PatternMatcher,
    /// Revocation mode
    pub mode: RevocationMode,
    /// When the revocation was issued
    pub timestamp: f64,
    /// Optional expiration time
    pub expires: Option<f64>,
    /// Reason for revocation
    pub reason: Option<String>,
}

impl PatternRevocation {
    /// Create a new pattern revocation
    #[must_use]
    pub fn new(pattern: PatternMatcher, mode: RevocationMode) -> Self {
        Self {
            pattern,
            mode,
            timestamp: current_timestamp(),
            expires: None,
            reason: None,
        }
    }

    /// Set expiration time
    #[must_use]
    pub fn with_expiration(mut self, expires_in: Duration) -> Self {
        self.expires = Some(current_timestamp() + expires_in.as_secs_f64());
        self
    }

    /// Set reason for revocation
    #[must_use]
    pub fn with_reason(mut self, reason: impl Into<String>) -> Self {
        self.reason = Some(reason.into());
        self
    }

    /// Check if this revocation has expired
    #[inline]
    #[must_use]
    pub fn is_expired(&self) -> bool {
        if let Some(expires) = self.expires {
            current_timestamp() > expires
        } else {
            false
        }
    }

    /// Check if a task name matches this pattern
    #[inline]
    #[must_use]
    pub fn matches(&self, task_name: &str) -> bool {
        self.pattern.matches(task_name)
    }
}

/// Result of a revocation check
#[derive(Debug, Clone)]
pub struct RevocationResult {
    /// Whether the task is revoked
    pub revoked: bool,
    /// Revocation mode
    pub mode: RevocationMode,
    /// Reason for revocation
    pub reason: Option<String>,
    /// Signal to send (for terminate mode)
    pub signal: Option<String>,
}

impl RevocationResult {
    /// Create a result indicating task is not revoked
    #[must_use]
    pub fn not_revoked() -> Self {
        Self {
            revoked: false,
            mode: RevocationMode::Ignore,
            reason: None,
            signal: None,
        }
    }

    /// Create a result indicating task is revoked
    #[must_use]
    pub fn revoked(mode: RevocationMode, reason: Option<String>, signal: Option<String>) -> Self {
        Self {
            revoked: true,
            mode,
            reason,
            signal,
        }
    }
}

/// Serializable revocation state for persistence
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct RevocationState {
    /// Revoked task IDs (`task_id` -> request)
    pub revoked_tasks: HashMap<String, RevocationRequest>,
    /// Pattern-based revocations (serializable form)
    pub pattern_revocations: Vec<SerializablePatternRevocation>,
}

/// Which [`PatternMatcher`] variant produced a persisted pattern string.
///
/// Without this discriminant every persisted pattern was rebuilt as a glob, which
/// silently destroyed regex revocations across a restart: `glob_to_regex` escapes
/// `^ $ ( ) | \` and `.` as literals, so `^app\.tasks\.(payment|refund)$` reloaded
/// as a pattern that matches nothing at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum PatternKind {
    /// Exact string equality.
    Exact,
    /// Glob pattern (`*` and `?`). The historical default.
    #[default]
    Glob,
    /// Regular expression.
    Regex,
    /// Matches every task name.
    All,
}

/// Serializable form of `PatternRevocation`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializablePatternRevocation {
    /// Pattern string, interpreted according to [`Self::kind`]
    pub pattern: String,
    /// Which matcher variant the pattern string belongs to.
    ///
    /// Defaults to [`PatternKind::Glob`] so state persisted by older versions
    /// (which had no discriminant and always meant "glob") loads unchanged.
    #[serde(default)]
    pub kind: PatternKind,
    /// Revocation mode
    pub mode: RevocationMode,
    /// When the revocation was issued
    pub timestamp: f64,
    /// Optional expiration time
    pub expires: Option<f64>,
    /// Reason for revocation
    pub reason: Option<String>,
}

impl From<&PatternRevocation> for SerializablePatternRevocation {
    fn from(rev: &PatternRevocation) -> Self {
        let (kind, pattern) = match &rev.pattern {
            PatternMatcher::Exact(s) => (PatternKind::Exact, s.clone()),
            PatternMatcher::Glob(g) => (PatternKind::Glob, g.pattern().to_string()),
            PatternMatcher::Regex(r) => (PatternKind::Regex, r.pattern().to_string()),
            PatternMatcher::All => (PatternKind::All, "*".to_string()),
        };
        Self {
            pattern,
            kind,
            mode: rev.mode,
            timestamp: rev.timestamp,
            expires: rev.expires,
            reason: rev.reason.clone(),
        }
    }
}

impl SerializablePatternRevocation {
    /// Convert to `PatternRevocation`, rebuilding the original matcher variant.
    ///
    /// # Errors
    ///
    /// Returns the underlying [`regex::Error`] if a persisted
    /// [`PatternKind::Regex`] pattern no longer compiles. Revocation is a
    /// control-plane safety mechanism, so a pattern that cannot be restored is
    /// reported rather than silently downgraded to something that matches nothing.
    pub fn try_into_pattern_revocation(self) -> Result<PatternRevocation, regex::Error> {
        let pattern = match self.kind {
            PatternKind::Exact => PatternMatcher::exact(self.pattern),
            PatternKind::Glob => PatternMatcher::glob(self.pattern),
            PatternKind::Regex => PatternMatcher::regex(&self.pattern)?,
            PatternKind::All => PatternMatcher::all(),
        };
        Ok(PatternRevocation {
            pattern,
            mode: self.mode,
            timestamp: self.timestamp,
            expires: self.expires,
            reason: self.reason,
        })
    }

    /// Convert to `PatternRevocation`, falling back to an exact matcher when a
    /// persisted regex no longer compiles.
    ///
    /// The fallback is logged at `warn` level: an exact matcher can only ever
    /// match fewer names than intended, never more, so it fails closed for the
    /// tasks it does cover while making the problem visible.
    #[must_use]
    pub fn into_pattern_revocation(self) -> PatternRevocation {
        let kind = self.kind;
        let mode = self.mode;
        let timestamp = self.timestamp;
        let expires = self.expires;
        let reason = self.reason.clone();
        let pattern_text = self.pattern.clone();
        match self.try_into_pattern_revocation() {
            Ok(revocation) => revocation,
            Err(error) => {
                tracing::warn!(
                    pattern = %pattern_text,
                    ?kind,
                    %error,
                    "failed to restore persisted revocation pattern; falling back to exact match"
                );
                PatternRevocation {
                    pattern: PatternMatcher::exact(pattern_text),
                    mode,
                    timestamp,
                    expires,
                    reason,
                }
            }
        }
    }
}

/// Revocation manager for tracking revoked tasks
///
/// All three registries are bounded. Revoked and terminated task ids are capped
/// with oldest-first eviction ([`DEFAULT_MAX_REVOKED_IDS`] /
/// [`DEFAULT_MAX_TERMINATED_IDS`]), terminated ids additionally expire after
/// [`DEFAULT_TERMINATED_TTL`], and pattern revocations are deduplicated on
/// insert so a repeated `revoke_by_pattern("email.*")` cannot grow the list that
/// `check_revocation` scans linearly.
#[derive(Debug)]
pub struct RevocationManager {
    /// Revoked task IDs
    revoked_ids: HashMap<Uuid, RevocationRequest>,
    /// Insertion order of `revoked_ids`, used for oldest-first eviction.
    ///
    /// May transiently contain ids no longer present in `revoked_ids`; those are
    /// skipped on eviction and pruned by `cleanup_expired`.
    revoked_order: VecDeque<Uuid>,
    /// Pattern-based revocations
    pattern_revocations: Vec<PatternRevocation>,
    /// Terminated task IDs, with the time each was marked (Unix seconds)
    terminated: HashMap<Uuid, f64>,
    /// Insertion order of `terminated`, used for oldest-first eviction.
    terminated_order: VecDeque<Uuid>,
    /// Maximum number of revoked task ids retained.
    max_revoked_ids: usize,
    /// Maximum number of terminated task ids retained.
    max_terminated_ids: usize,
    /// How long a terminated task id is retained, or `None` to keep it until
    /// evicted by the capacity bound.
    terminated_ttl: Option<Duration>,
}

impl Default for RevocationManager {
    fn default() -> Self {
        Self {
            revoked_ids: HashMap::new(),
            revoked_order: VecDeque::new(),
            pattern_revocations: Vec::new(),
            terminated: HashMap::new(),
            terminated_order: VecDeque::new(),
            max_revoked_ids: DEFAULT_MAX_REVOKED_IDS,
            max_terminated_ids: DEFAULT_MAX_TERMINATED_IDS,
            terminated_ttl: Some(DEFAULT_TERMINATED_TTL),
        }
    }
}

impl RevocationManager {
    /// Create a new revocation manager
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the maximum number of revoked task ids retained.
    ///
    /// A value of `0` is treated as `1`.
    #[must_use]
    pub fn with_max_revoked_ids(mut self, max: usize) -> Self {
        self.max_revoked_ids = max.max(1);
        self.evict_revoked_overflow();
        self
    }

    /// Set the maximum number of terminated task ids retained.
    ///
    /// A value of `0` is treated as `1`.
    #[must_use]
    pub fn with_max_terminated_ids(mut self, max: usize) -> Self {
        self.max_terminated_ids = max.max(1);
        self.evict_terminated_overflow();
        self
    }

    /// Set how long terminated task ids are retained.
    ///
    /// `None` keeps them until the capacity bound evicts them.
    #[must_use]
    pub fn with_terminated_ttl(mut self, ttl: Option<Duration>) -> Self {
        self.terminated_ttl = ttl;
        self
    }

    /// Maximum number of revoked task ids retained.
    #[inline]
    #[must_use]
    pub fn max_revoked_ids(&self) -> usize {
        self.max_revoked_ids
    }

    /// Maximum number of terminated task ids retained.
    #[inline]
    #[must_use]
    pub fn max_terminated_ids(&self) -> usize {
        self.max_terminated_ids
    }

    /// Number of terminated task ids currently retained.
    #[inline]
    #[must_use]
    pub fn terminated_count(&self) -> usize {
        self.terminated.len()
    }

    /// Number of distinct pattern revocations currently registered.
    #[inline]
    #[must_use]
    pub fn pattern_revocation_count(&self) -> usize {
        self.pattern_revocations.len()
    }

    /// Drop the oldest revoked ids until the capacity bound is respected.
    fn evict_revoked_overflow(&mut self) {
        while self.revoked_ids.len() > self.max_revoked_ids {
            let Some(oldest) = self.revoked_order.pop_front() else {
                break;
            };
            self.revoked_ids.remove(&oldest);
        }
    }

    /// Drop the oldest terminated ids until the capacity bound is respected.
    fn evict_terminated_overflow(&mut self) {
        while self.terminated.len() > self.max_terminated_ids {
            let Some(oldest) = self.terminated_order.pop_front() else {
                break;
            };
            self.terminated.remove(&oldest);
        }
    }

    /// Insert (or replace) a revocation request, maintaining the eviction order.
    fn insert_revoked(&mut self, request: RevocationRequest) {
        let task_id = request.task_id;
        if self.revoked_ids.insert(task_id, request).is_none() {
            self.revoked_order.push_back(task_id);
        }
        self.evict_revoked_overflow();
    }

    /// Revoke a task by ID
    pub fn revoke(&mut self, task_id: Uuid, mode: RevocationMode) {
        self.insert_revoked(RevocationRequest::new(task_id, mode));
    }

    /// Revoke a task with a full request
    pub fn revoke_with_request(&mut self, request: RevocationRequest) {
        self.insert_revoked(request);
    }

    /// Revoke all tasks matching a pattern
    pub fn revoke_by_pattern(&mut self, pattern: &str, mode: RevocationMode) {
        let pattern_rev = PatternRevocation::new(PatternMatcher::glob(pattern), mode);
        self.revoke_with_pattern(pattern_rev);
    }

    /// Revoke by pattern with full configuration
    ///
    /// An existing revocation with the same matcher and mode is replaced rather
    /// than duplicated, so repeated calls with the same pattern keep the scanned
    /// list at a single entry.
    pub fn revoke_with_pattern(&mut self, revocation: PatternRevocation) {
        let identity = matcher_identity(&revocation.pattern);
        if let Some(existing) = self
            .pattern_revocations
            .iter_mut()
            .find(|r| r.mode == revocation.mode && matcher_identity(&r.pattern) == identity)
        {
            *existing = revocation;
            return;
        }
        self.pattern_revocations.push(revocation);
    }

    /// Bulk revoke multiple tasks
    pub fn bulk_revoke(&mut self, task_ids: &[Uuid], mode: RevocationMode) {
        for &task_id in task_ids {
            self.revoke(task_id, mode);
        }
    }

    /// Check if a task is revoked (by ID)
    #[inline]
    #[must_use]
    pub fn is_revoked(&self, task_id: Uuid) -> bool {
        if let Some(request) = self.revoked_ids.get(&task_id) {
            !request.is_expired()
        } else {
            false
        }
    }

    /// Check if a task should be revoked (by ID or pattern)
    #[must_use]
    pub fn check_revocation(&self, task_id: Uuid, task_name: &str) -> RevocationResult {
        // Check by ID first
        if let Some(request) = self.revoked_ids.get(&task_id) {
            if !request.is_expired() {
                return RevocationResult::revoked(
                    request.mode,
                    request.reason.clone(),
                    request.signal.clone(),
                );
            }
        }

        // Check by pattern
        for pattern_rev in &self.pattern_revocations {
            if !pattern_rev.is_expired() && pattern_rev.matches(task_name) {
                return RevocationResult::revoked(
                    pattern_rev.mode,
                    pattern_rev.reason.clone(),
                    None,
                );
            }
        }

        RevocationResult::not_revoked()
    }

    /// Mark a task as terminated
    pub fn mark_terminated(&mut self, task_id: Uuid) {
        if self
            .terminated
            .insert(task_id, current_timestamp())
            .is_none()
        {
            self.terminated_order.push_back(task_id);
        }
        self.evict_terminated_overflow();
    }

    /// Check if a task has been terminated
    #[inline]
    #[must_use]
    pub fn is_terminated(&self, task_id: Uuid) -> bool {
        self.terminated.contains_key(&task_id)
    }

    /// Remove revocation for a task ID
    pub fn unrevoke(&mut self, task_id: Uuid) {
        self.revoked_ids.remove(&task_id);
    }

    /// Remove pattern-based revocations matching a pattern string
    pub fn remove_pattern(&mut self, pattern: &str) {
        self.pattern_revocations.retain(|p| {
            if let PatternMatcher::Glob(g) = &p.pattern {
                g.pattern() != pattern
            } else {
                true
            }
        });
    }

    /// Clean up expired revocations
    ///
    /// Also drops terminated task ids older than the configured retention window
    /// and prunes the eviction-order queues of ids no longer present.
    pub fn cleanup_expired(&mut self) {
        self.revoked_ids.retain(|_, request| !request.is_expired());
        self.pattern_revocations.retain(|rev| !rev.is_expired());

        if let Some(ttl) = self.terminated_ttl {
            let cutoff = current_timestamp() - ttl.as_secs_f64();
            self.terminated.retain(|_, marked_at| *marked_at > cutoff);
        }

        let revoked = &self.revoked_ids;
        self.revoked_order.retain(|id| revoked.contains_key(id));
        let terminated = &self.terminated;
        self.terminated_order
            .retain(|id| terminated.contains_key(id));
    }

    /// Get all revoked task IDs
    #[must_use]
    pub fn revoked_ids(&self) -> Vec<Uuid> {
        self.revoked_ids
            .iter()
            .filter(|(_, request)| !request.is_expired())
            .map(|(id, _)| *id)
            .collect()
    }

    /// Get count of revoked tasks
    #[inline]
    #[must_use]
    pub fn revoked_count(&self) -> usize {
        self.revoked_ids
            .values()
            .filter(|request| !request.is_expired())
            .count()
    }

    /// Clear all revocations
    pub fn clear(&mut self) {
        self.revoked_ids.clear();
        self.revoked_order.clear();
        self.pattern_revocations.clear();
        self.terminated.clear();
        self.terminated_order.clear();
    }

    /// Export state for persistence
    pub fn export_state(&self) -> RevocationState {
        let revoked_tasks = self
            .revoked_ids
            .iter()
            .filter(|(_, req)| !req.is_expired())
            .map(|(id, req)| (id.to_string(), req.clone()))
            .collect();

        let pattern_revocations = self
            .pattern_revocations
            .iter()
            .filter(|rev| !rev.is_expired())
            .map(SerializablePatternRevocation::from)
            .collect();

        RevocationState {
            revoked_tasks,
            pattern_revocations,
        }
    }

    /// Import state from persistence
    pub fn import_state(&mut self, state: RevocationState) {
        for (id_str, request) in state.revoked_tasks {
            if !request.is_expired() {
                if let Ok(id) = Uuid::parse_str(&id_str) {
                    // The map key is authoritative: keep the request consistent
                    // with the id it was persisted under.
                    let mut request = request;
                    request.task_id = id;
                    self.insert_revoked(request);
                }
            }
        }

        for ser_pattern in state.pattern_revocations {
            let pattern_rev = ser_pattern.into_pattern_revocation();
            if !pattern_rev.is_expired() {
                self.revoke_with_pattern(pattern_rev);
            }
        }
    }
}

/// Thread-safe revocation manager for workers
///
/// Revocation is a control-plane safety mechanism, so a poisoned lock must never
/// make it fail open: a panic elsewhere in the process would otherwise silently
/// re-enable execution of tasks an operator has revoked. Every accessor therefore
/// recovers the guard with [`PoisonError::into_inner`] and keeps enforcing.
#[derive(Debug, Clone, Default)]
pub struct WorkerRevocationManager {
    inner: Arc<RwLock<RevocationManager>>,
}

impl WorkerRevocationManager {
    /// Create a new worker revocation manager
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Read the registry, recovering from a poisoned lock.
    ///
    /// The guarded state is a plain set of registries: a panic in an unrelated
    /// part of the process cannot leave it logically inconsistent, so continuing
    /// to enforce revocations is strictly safer than ignoring them.
    fn read_inner(&self) -> RwLockReadGuard<'_, RevocationManager> {
        self.inner.read().unwrap_or_else(PoisonError::into_inner)
    }

    /// Write to the registry, recovering from a poisoned lock.
    fn write_inner(&self) -> RwLockWriteGuard<'_, RevocationManager> {
        self.inner.write().unwrap_or_else(PoisonError::into_inner)
    }

    /// Revoke a task by ID
    pub fn revoke(&self, task_id: Uuid, mode: RevocationMode) {
        self.write_inner().revoke(task_id, mode);
    }

    /// Revoke a task with a full request
    pub fn revoke_with_request(&self, request: RevocationRequest) {
        self.write_inner().revoke_with_request(request);
    }

    /// Revoke by pattern
    pub fn revoke_by_pattern(&self, pattern: &str, mode: RevocationMode) {
        self.write_inner().revoke_by_pattern(pattern, mode);
    }

    /// Revoke by pattern with full configuration
    pub fn revoke_with_pattern(&self, revocation: PatternRevocation) {
        self.write_inner().revoke_with_pattern(revocation);
    }

    /// Bulk revoke multiple tasks
    pub fn bulk_revoke(&self, task_ids: &[Uuid], mode: RevocationMode) {
        self.write_inner().bulk_revoke(task_ids, mode);
    }

    /// Check if a task is revoked by ID
    #[must_use]
    pub fn is_revoked(&self, task_id: Uuid) -> bool {
        self.read_inner().is_revoked(task_id)
    }

    /// Check revocation status (by ID and pattern)
    #[must_use]
    pub fn check_revocation(&self, task_id: Uuid, task_name: &str) -> RevocationResult {
        self.read_inner().check_revocation(task_id, task_name)
    }

    /// Mark a task as terminated
    pub fn mark_terminated(&self, task_id: Uuid) {
        self.write_inner().mark_terminated(task_id);
    }

    /// Check if a task has been terminated
    #[must_use]
    pub fn is_terminated(&self, task_id: Uuid) -> bool {
        self.read_inner().is_terminated(task_id)
    }

    /// Remove revocation for a task ID
    pub fn unrevoke(&self, task_id: Uuid) {
        self.write_inner().unrevoke(task_id);
    }

    /// Clean up expired revocations
    pub fn cleanup_expired(&self) {
        self.write_inner().cleanup_expired();
    }

    /// Get all revoked task IDs
    #[must_use]
    pub fn revoked_ids(&self) -> Vec<Uuid> {
        self.read_inner().revoked_ids()
    }

    /// Get count of revoked tasks
    #[must_use]
    pub fn revoked_count(&self) -> usize {
        self.read_inner().revoked_count()
    }

    /// Get count of terminated task IDs currently retained
    #[must_use]
    pub fn terminated_count(&self) -> usize {
        self.read_inner().terminated_count()
    }

    /// Export state for persistence
    #[must_use]
    pub fn export_state(&self) -> RevocationState {
        self.read_inner().export_state()
    }

    /// Import state from persistence
    pub fn import_state(&self, state: RevocationState) {
        self.write_inner().import_state(state);
    }

    /// Clear all revocations
    pub fn clear(&self) {
        self.write_inner().clear();
    }
}

/// Get current timestamp as f64
fn current_timestamp() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_revocation_request() {
        let task_id = Uuid::new_v4();
        let request = RevocationRequest::new(task_id, RevocationMode::Terminate);

        assert_eq!(request.task_id, task_id);
        assert_eq!(request.mode, RevocationMode::Terminate);
        assert!(!request.is_expired());
    }

    #[test]
    fn test_revocation_request_with_expiration() {
        let task_id = Uuid::new_v4();
        let request = RevocationRequest::new(task_id, RevocationMode::Terminate)
            .with_expiration(Duration::from_secs(0));

        // Immediately expired
        std::thread::sleep(Duration::from_millis(10));
        assert!(request.is_expired());
    }

    #[test]
    fn test_revocation_manager_basic() {
        let mut manager = RevocationManager::new();
        let task_id = Uuid::new_v4();

        manager.revoke(task_id, RevocationMode::Terminate);
        assert!(manager.is_revoked(task_id));

        let other_id = Uuid::new_v4();
        assert!(!manager.is_revoked(other_id));
    }

    #[test]
    fn test_revocation_by_pattern() {
        let mut manager = RevocationManager::new();

        manager.revoke_by_pattern("email.*", RevocationMode::Ignore);

        let result = manager.check_revocation(Uuid::new_v4(), "email.send");
        assert!(result.revoked);
        assert_eq!(result.mode, RevocationMode::Ignore);

        let result = manager.check_revocation(Uuid::new_v4(), "sms.send");
        assert!(!result.revoked);
    }

    #[test]
    fn test_bulk_revoke() {
        let mut manager = RevocationManager::new();
        let ids: Vec<Uuid> = (0..5).map(|_| Uuid::new_v4()).collect();

        manager.bulk_revoke(&ids, RevocationMode::Terminate);

        for id in &ids {
            assert!(manager.is_revoked(*id));
        }
    }

    #[test]
    fn test_unrevoke() {
        let mut manager = RevocationManager::new();
        let task_id = Uuid::new_v4();

        manager.revoke(task_id, RevocationMode::Terminate);
        assert!(manager.is_revoked(task_id));

        manager.unrevoke(task_id);
        assert!(!manager.is_revoked(task_id));
    }

    #[test]
    fn test_cleanup_expired() {
        let mut manager = RevocationManager::new();
        let task_id = Uuid::new_v4();

        // Add an expired revocation
        let request = RevocationRequest::new(task_id, RevocationMode::Terminate)
            .with_expiration(Duration::from_secs(0));
        std::thread::sleep(Duration::from_millis(10));
        manager.revoke_with_request(request);

        // Add a non-expired revocation
        let other_id = Uuid::new_v4();
        manager.revoke(other_id, RevocationMode::Terminate);

        manager.cleanup_expired();

        assert!(!manager.is_revoked(task_id)); // Expired
        assert!(manager.is_revoked(other_id)); // Not expired
    }

    #[test]
    fn test_export_import_state() {
        let mut manager = RevocationManager::new();
        let task_id = Uuid::new_v4();

        manager.revoke(task_id, RevocationMode::Terminate);
        manager.revoke_by_pattern("email.*", RevocationMode::Ignore);

        let state = manager.export_state();

        let mut new_manager = RevocationManager::new();
        new_manager.import_state(state);

        assert!(new_manager.is_revoked(task_id));
        let result = new_manager.check_revocation(Uuid::new_v4(), "email.send");
        assert!(result.revoked);
    }

    #[test]
    fn test_worker_revocation_manager() {
        let manager = WorkerRevocationManager::new();
        let task_id = Uuid::new_v4();

        manager.revoke(task_id, RevocationMode::Terminate);
        assert!(manager.is_revoked(task_id));

        manager.mark_terminated(task_id);
        assert!(manager.is_terminated(task_id));
    }

    #[test]
    fn test_revocation_state_serialization() {
        let mut manager = RevocationManager::new();
        let task_id = Uuid::new_v4();

        manager.revoke(task_id, RevocationMode::Terminate);
        manager.revoke_by_pattern("tasks.*", RevocationMode::Ignore);

        let state = manager.export_state();
        let json = serde_json::to_string(&state).unwrap();
        let parsed: RevocationState = serde_json::from_str(&json).unwrap();

        assert!(!parsed.revoked_tasks.is_empty());
        assert!(!parsed.pattern_revocations.is_empty());
    }

    #[test]
    fn test_revocation_with_reason() {
        let mut manager = RevocationManager::new();
        let task_id = Uuid::new_v4();

        let request = RevocationRequest::new(task_id, RevocationMode::Terminate)
            .with_reason("Manual cancellation by user");
        manager.revoke_with_request(request);

        let result = manager.check_revocation(task_id, "any.task");
        assert!(result.revoked);
        assert_eq!(
            result.reason,
            Some("Manual cancellation by user".to_string())
        );
    }

    #[test]
    fn test_revoked_count() {
        let mut manager = RevocationManager::new();

        for _ in 0..5 {
            manager.revoke(Uuid::new_v4(), RevocationMode::Terminate);
        }

        assert_eq!(manager.revoked_count(), 5);
        assert_eq!(manager.revoked_ids().len(), 5);
    }

    #[test]
    fn test_clear() {
        let mut manager = RevocationManager::new();

        manager.revoke(Uuid::new_v4(), RevocationMode::Terminate);
        manager.revoke_by_pattern("*", RevocationMode::Ignore);
        manager.mark_terminated(Uuid::new_v4());

        manager.clear();

        assert_eq!(manager.revoked_count(), 0);
    }

    /// Regression: every persisted pattern was rebuilt as a glob, so a regex
    /// revocation silently stopped matching anything after a restart.
    #[test]
    fn test_pattern_revocation_round_trip_preserves_variant() {
        let cases: Vec<(PatternMatcher, PatternKind, &str, &str)> = vec![
            (
                PatternMatcher::exact("app.tasks.payment"),
                PatternKind::Exact,
                "app.tasks.payment",
                "app.tasks.payments",
            ),
            (
                PatternMatcher::glob("email.*"),
                PatternKind::Glob,
                "email.send",
                "sms.send",
            ),
            (
                PatternMatcher::regex(r"^app\.tasks\.(payment|refund)_.*$")
                    .expect("regex should compile"),
                PatternKind::Regex,
                "app.tasks.payment_capture",
                "app.tasks.invoice_create",
            ),
            (PatternMatcher::all(), PatternKind::All, "anything", ""),
        ];

        for (matcher, expected_kind, should_match, should_not_match) in cases {
            let original = PatternRevocation::new(matcher, RevocationMode::Ignore);
            assert!(original.matches(should_match));

            let serializable = SerializablePatternRevocation::from(&original);
            assert_eq!(serializable.kind, expected_kind);

            let json = serde_json::to_string(&serializable).expect("serialize");
            let parsed: SerializablePatternRevocation =
                serde_json::from_str(&json).expect("deserialize");
            let restored = parsed
                .try_into_pattern_revocation()
                .expect("pattern should be restorable");

            assert!(
                restored.matches(should_match),
                "{expected_kind:?}: restored pattern must still match {should_match}"
            );
            if expected_kind != PatternKind::All {
                assert!(
                    !restored.matches(should_not_match),
                    "{expected_kind:?}: restored pattern must not match {should_not_match}"
                );
            }
            assert_eq!(restored.mode, RevocationMode::Ignore);
        }
    }

    #[test]
    fn test_regex_revocation_survives_manager_export_import() {
        let mut manager = RevocationManager::new();
        manager.revoke_with_pattern(PatternRevocation::new(
            PatternMatcher::regex(r"^app\.tasks\.(payment|refund)_.*$")
                .expect("regex should compile"),
            RevocationMode::Terminate,
        ));
        assert!(
            manager
                .check_revocation(Uuid::new_v4(), "app.tasks.refund_issue")
                .revoked
        );

        let state = manager.export_state();
        let mut restarted = RevocationManager::new();
        restarted.import_state(state);

        assert!(
            restarted
                .check_revocation(Uuid::new_v4(), "app.tasks.refund_issue")
                .revoked,
            "regex revocation must still apply after a restart"
        );
        assert!(
            !restarted
                .check_revocation(Uuid::new_v4(), "app.tasks.invoice_create")
                .revoked
        );
    }

    #[test]
    fn test_legacy_state_without_kind_defaults_to_glob() {
        // State persisted before the `kind` discriminant existed.
        let json =
            r#"{"pattern":"email.*","mode":"ignore","timestamp":0.0,"expires":null,"reason":null}"#;
        let parsed: SerializablePatternRevocation =
            serde_json::from_str(json).expect("legacy state should deserialize");
        assert_eq!(parsed.kind, PatternKind::Glob);
        let restored = parsed
            .try_into_pattern_revocation()
            .expect("glob should restore");
        assert!(restored.matches("email.send"));
        assert!(!restored.matches("sms.send"));
    }

    #[test]
    fn test_invalid_persisted_regex_falls_back_without_panicking() {
        let broken = SerializablePatternRevocation {
            pattern: "([unclosed".to_string(),
            kind: PatternKind::Regex,
            mode: RevocationMode::Ignore,
            timestamp: 1.0,
            expires: None,
            reason: Some("audit".to_string()),
        };
        assert!(broken.clone().try_into_pattern_revocation().is_err());

        let restored = broken.into_pattern_revocation();
        assert_eq!(restored.mode, RevocationMode::Ignore);
        assert_eq!(restored.reason.as_deref(), Some("audit"));
        // Falls back to an exact match: never matches more than intended.
        assert!(restored.matches("([unclosed"));
        assert!(!restored.matches("anything.else"));
    }

    /// Regression: `revoked_ids` retained every non-expiring revocation forever.
    #[test]
    fn test_revoked_ids_are_capacity_bounded() {
        let mut manager = RevocationManager::new().with_max_revoked_ids(4);
        assert_eq!(manager.max_revoked_ids(), 4);

        let ids: Vec<Uuid> = (0..10).map(|_| Uuid::new_v4()).collect();
        for id in &ids {
            manager.revoke(*id, RevocationMode::Terminate);
        }

        assert_eq!(manager.revoked_count(), 4);
        // Oldest-first eviction: the last four inserted survive.
        for id in &ids[6..] {
            assert!(
                manager.is_revoked(*id),
                "recent revocation must be retained"
            );
        }
        for id in &ids[..6] {
            assert!(
                !manager.is_revoked(*id),
                "oldest revocation must be evicted"
            );
        }
    }

    /// Regression: `terminated` accumulated one entry per terminated task for the
    /// process lifetime and `cleanup_expired` never touched it.
    #[test]
    fn test_terminated_ids_are_capacity_bounded_and_expire() {
        let mut manager = RevocationManager::new().with_max_terminated_ids(3);
        let ids: Vec<Uuid> = (0..6).map(|_| Uuid::new_v4()).collect();
        for id in &ids {
            manager.mark_terminated(*id);
        }
        assert_eq!(manager.terminated_count(), 3);
        assert!(manager.is_terminated(ids[5]));
        assert!(!manager.is_terminated(ids[0]));

        // A zero retention window drops everything on the next cleanup.
        let mut manager = RevocationManager::new().with_terminated_ttl(Some(Duration::ZERO));
        manager.mark_terminated(ids[0]);
        assert_eq!(manager.terminated_count(), 1);
        manager.cleanup_expired();
        assert_eq!(manager.terminated_count(), 0);
        assert!(!manager.is_terminated(ids[0]));
    }

    /// Regression: repeated `revoke_by_pattern` calls pushed a duplicate entry
    /// onto the list scanned linearly by every `check_revocation`.
    #[test]
    fn test_pattern_revocations_are_deduplicated() {
        let mut manager = RevocationManager::new();
        for _ in 0..25 {
            manager.revoke_by_pattern("email.*", RevocationMode::Ignore);
        }
        assert_eq!(manager.pattern_revocation_count(), 1);

        // A different mode is a genuinely different rule.
        manager.revoke_by_pattern("email.*", RevocationMode::Terminate);
        assert_eq!(manager.pattern_revocation_count(), 2);

        // As is a different pattern.
        manager.revoke_by_pattern("sms.*", RevocationMode::Ignore);
        assert_eq!(manager.pattern_revocation_count(), 3);

        // Re-importing exported state must not duplicate either.
        let state = manager.export_state();
        manager.import_state(state);
        assert_eq!(manager.pattern_revocation_count(), 3);
    }

    /// Regression: a poisoned lock made revocation checks fail open, silently
    /// letting revoked tasks execute.
    #[test]
    fn test_poisoned_lock_keeps_enforcing_revocation() {
        let manager = WorkerRevocationManager::new();
        let task_id = Uuid::new_v4();
        manager.revoke(task_id, RevocationMode::Terminate);
        manager.revoke_by_pattern("email.*", RevocationMode::Ignore);
        manager.mark_terminated(task_id);

        // Poison the inner lock by panicking while the write guard is held.
        let poisoner = manager.clone();
        let handle = std::thread::spawn(move || {
            let _guard = poisoner
                .inner
                .write()
                .expect("lock should not be poisoned yet");
            panic!("intentional panic to poison the revocation lock");
        });
        assert!(handle.join().is_err(), "helper thread should have panicked");
        assert!(
            manager.inner.read().is_err(),
            "the lock should now be poisoned"
        );

        assert!(
            manager.is_revoked(task_id),
            "revocation must survive a poisoned lock"
        );
        assert!(manager.check_revocation(task_id, "email.send").revoked);
        assert!(
            manager
                .check_revocation(Uuid::new_v4(), "email.send")
                .revoked,
            "pattern revocation must survive a poisoned lock"
        );
        assert!(manager.is_terminated(task_id));
        assert!(manager.revoked_count() >= 1);
    }
}
