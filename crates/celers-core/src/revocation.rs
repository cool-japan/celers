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
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

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

/// Serializable form of `PatternRevocation`
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializablePatternRevocation {
    /// Pattern string (glob format)
    pub pattern: String,
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
        // Extract pattern string (simplified - assumes glob pattern)
        let pattern = match &rev.pattern {
            PatternMatcher::Exact(s) => s.clone(),
            PatternMatcher::Glob(g) => g.pattern().to_string(),
            PatternMatcher::Regex(r) => r.pattern().to_string(),
            PatternMatcher::All => "*".to_string(),
        };
        Self {
            pattern,
            mode: rev.mode,
            timestamp: rev.timestamp,
            expires: rev.expires,
            reason: rev.reason.clone(),
        }
    }
}

impl SerializablePatternRevocation {
    /// Convert to `PatternRevocation`
    #[must_use]
    pub fn into_pattern_revocation(self) -> PatternRevocation {
        PatternRevocation {
            pattern: PatternMatcher::glob(&self.pattern),
            mode: self.mode,
            timestamp: self.timestamp,
            expires: self.expires,
            reason: self.reason,
        }
    }
}

/// Revocation manager for tracking revoked tasks
#[derive(Debug, Default)]
pub struct RevocationManager {
    /// Revoked task IDs
    revoked_ids: HashMap<Uuid, RevocationRequest>,
    /// Pattern-based revocations
    pattern_revocations: Vec<PatternRevocation>,
    /// Set of currently terminated task IDs
    terminated: HashSet<Uuid>,
}

impl RevocationManager {
    /// Create a new revocation manager
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Revoke a task by ID
    pub fn revoke(&mut self, task_id: Uuid, mode: RevocationMode) {
        let request = RevocationRequest::new(task_id, mode);
        self.revoked_ids.insert(task_id, request);
    }

    /// Revoke a task with a full request
    pub fn revoke_with_request(&mut self, request: RevocationRequest) {
        self.revoked_ids.insert(request.task_id, request);
    }

    /// Revoke all tasks matching a pattern
    pub fn revoke_by_pattern(&mut self, pattern: &str, mode: RevocationMode) {
        let pattern_rev = PatternRevocation::new(PatternMatcher::glob(pattern), mode);
        self.pattern_revocations.push(pattern_rev);
    }

    /// Revoke by pattern with full configuration
    pub fn revoke_with_pattern(&mut self, revocation: PatternRevocation) {
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
        self.terminated.insert(task_id);
    }

    /// Check if a task has been terminated
    #[inline]
    #[must_use]
    pub fn is_terminated(&self, task_id: Uuid) -> bool {
        self.terminated.contains(&task_id)
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
    pub fn cleanup_expired(&mut self) {
        self.revoked_ids.retain(|_, request| !request.is_expired());
        self.pattern_revocations.retain(|rev| !rev.is_expired());
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
        self.pattern_revocations.clear();
        self.terminated.clear();
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
                    self.revoked_ids.insert(id, request);
                }
            }
        }

        for ser_pattern in state.pattern_revocations {
            let pattern_rev = ser_pattern.into_pattern_revocation();
            if !pattern_rev.is_expired() {
                self.pattern_revocations.push(pattern_rev);
            }
        }
    }
}

/// Thread-safe revocation manager for workers
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

    /// Revoke a task by ID
    pub fn revoke(&self, task_id: Uuid, mode: RevocationMode) {
        if let Ok(mut guard) = self.inner.write() {
            guard.revoke(task_id, mode);
        }
    }

    /// Revoke a task with a full request
    pub fn revoke_with_request(&self, request: RevocationRequest) {
        if let Ok(mut guard) = self.inner.write() {
            guard.revoke_with_request(request);
        }
    }

    /// Revoke by pattern
    pub fn revoke_by_pattern(&self, pattern: &str, mode: RevocationMode) {
        if let Ok(mut guard) = self.inner.write() {
            guard.revoke_by_pattern(pattern, mode);
        }
    }

    /// Bulk revoke multiple tasks
    pub fn bulk_revoke(&self, task_ids: &[Uuid], mode: RevocationMode) {
        if let Ok(mut guard) = self.inner.write() {
            guard.bulk_revoke(task_ids, mode);
        }
    }

    /// Check if a task is revoked by ID
    #[must_use]
    pub fn is_revoked(&self, task_id: Uuid) -> bool {
        if let Ok(guard) = self.inner.read() {
            guard.is_revoked(task_id)
        } else {
            false
        }
    }

    /// Check revocation status (by ID and pattern)
    #[must_use]
    pub fn check_revocation(&self, task_id: Uuid, task_name: &str) -> RevocationResult {
        if let Ok(guard) = self.inner.read() {
            guard.check_revocation(task_id, task_name)
        } else {
            RevocationResult::not_revoked()
        }
    }

    /// Mark a task as terminated
    pub fn mark_terminated(&self, task_id: Uuid) {
        if let Ok(mut guard) = self.inner.write() {
            guard.mark_terminated(task_id);
        }
    }

    /// Check if a task has been terminated
    #[must_use]
    pub fn is_terminated(&self, task_id: Uuid) -> bool {
        if let Ok(guard) = self.inner.read() {
            guard.is_terminated(task_id)
        } else {
            false
        }
    }

    /// Remove revocation for a task ID
    pub fn unrevoke(&self, task_id: Uuid) {
        if let Ok(mut guard) = self.inner.write() {
            guard.unrevoke(task_id);
        }
    }

    /// Clean up expired revocations
    pub fn cleanup_expired(&self) {
        if let Ok(mut guard) = self.inner.write() {
            guard.cleanup_expired();
        }
    }

    /// Get all revoked task IDs
    #[must_use]
    pub fn revoked_ids(&self) -> Vec<Uuid> {
        if let Ok(guard) = self.inner.read() {
            guard.revoked_ids()
        } else {
            Vec::new()
        }
    }

    /// Get count of revoked tasks
    #[must_use]
    pub fn revoked_count(&self) -> usize {
        if let Ok(guard) = self.inner.read() {
            guard.revoked_count()
        } else {
            0
        }
    }

    /// Export state for persistence
    #[must_use]
    pub fn export_state(&self) -> RevocationState {
        if let Ok(guard) = self.inner.read() {
            guard.export_state()
        } else {
            RevocationState::default()
        }
    }

    /// Import state from persistence
    pub fn import_state(&self, state: RevocationState) {
        if let Ok(mut guard) = self.inner.write() {
            guard.import_state(state);
        }
    }

    /// Clear all revocations
    pub fn clear(&self) {
        if let Ok(mut guard) = self.inner.write() {
            guard.clear();
        }
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
}
