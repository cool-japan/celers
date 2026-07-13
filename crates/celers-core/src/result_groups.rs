//! Grouped result handling for Celery `group` and `chord` primitives.
//!
//! A [`ResultGroup`] aggregates the result metadata of a set of child tasks
//! launched together (a `group`, or the header of a `chord`). It answers the
//! questions a caller typically asks about a group of results:
//!
//! * Are **all** children ready (terminal)?
//! * Did the group **succeed** (all children succeeded) or **fail** (at least
//!   one child failed/was revoked/rejected)?
//! * What are the **ordered collected values**, in the order the children were
//!   registered?
//!
//! Everything here is a pure, synchronous, in-memory data structure built on
//! top of the existing [`crate::result::TaskResultValue`] and [`crate::TaskId`]
//! types. It performs no I/O — callers populate the child metas (e.g. from a
//! [`crate::result::ResultStore`]) and then query the rollups.
//!
//! # Example
//!
//! ```rust
//! use celers_core::result::TaskResultValue;
//! use celers_core::result_groups::{ResultGroup, GroupStatus};
//! use serde_json::json;
//! use uuid::Uuid;
//!
//! let group_id = Uuid::new_v4();
//! let a = Uuid::new_v4();
//! let b = Uuid::new_v4();
//!
//! let mut group = ResultGroup::new(group_id, vec![a, b]);
//! group.set_result(a, TaskResultValue::Success(json!(1)));
//! assert!(!group.ready());            // b not done yet
//! assert_eq!(group.status(), GroupStatus::Pending);
//!
//! group.set_result(b, TaskResultValue::Success(json!(2)));
//! assert!(group.ready());
//! assert!(group.successful());
//! assert_eq!(group.status(), GroupStatus::Succeeded);
//!
//! // Ordered collected values match the registration order [a, b].
//! let values = group.collected_values().unwrap();
//! assert_eq!(values, vec![json!(1), json!(2)]);
//! ```

use crate::result::TaskResultValue;
use crate::TaskId;
use serde_json::Value;
use std::collections::HashMap;

/// Per-child slot inside a [`ResultGroup`].
///
/// Records the child's `task_id` and its latest known result value (if any).
#[derive(Debug, Clone)]
pub struct GroupChild {
    /// The child task identifier.
    pub task_id: TaskId,

    /// The latest known result for this child, or `None` if not yet reported.
    pub result: Option<TaskResultValue>,
}

impl GroupChild {
    /// Create a new, unreported child slot.
    #[must_use]
    pub const fn new(task_id: TaskId) -> Self {
        Self {
            task_id,
            result: None,
        }
    }

    /// Create a child slot pre-populated with a result.
    #[must_use]
    pub const fn with_result(task_id: TaskId, result: TaskResultValue) -> Self {
        Self {
            task_id,
            result: Some(result),
        }
    }

    /// Returns `true` if the child has reported a terminal result.
    #[inline]
    #[must_use]
    pub fn is_ready(&self) -> bool {
        self.result.as_ref().is_some_and(TaskResultValue::is_ready)
    }

    /// Returns `true` if the child reported a successful result.
    #[inline]
    #[must_use]
    pub fn is_successful(&self) -> bool {
        self.result
            .as_ref()
            .is_some_and(TaskResultValue::is_successful)
    }

    /// Returns `true` if the child reported a failed/rejected result.
    #[inline]
    #[must_use]
    pub fn is_failed(&self) -> bool {
        self.result.as_ref().is_some_and(TaskResultValue::is_failed)
    }

    /// Returns `true` if the child reported a revoked result.
    #[inline]
    #[must_use]
    pub fn is_revoked(&self) -> bool {
        matches!(self.result, Some(TaskResultValue::Revoked))
    }
}

/// Aggregate readiness/outcome status of a [`ResultGroup`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupStatus {
    /// At least one child is still not in a terminal state.
    Pending,

    /// All children are terminal and every child succeeded.
    Succeeded,

    /// All children are terminal but at least one did not succeed
    /// (failed, revoked, or rejected).
    Failed,
}

impl GroupStatus {
    /// Returns `true` if the group has finished (every child is terminal).
    #[inline]
    #[must_use]
    pub const fn is_ready(&self) -> bool {
        matches!(self, GroupStatus::Succeeded | GroupStatus::Failed)
    }

    /// Returns `true` if the group finished successfully.
    #[inline]
    #[must_use]
    pub const fn is_successful(&self) -> bool {
        matches!(self, GroupStatus::Succeeded)
    }

    /// Returns `true` if the group finished with at least one non-success.
    #[inline]
    #[must_use]
    pub const fn is_failed(&self) -> bool {
        matches!(self, GroupStatus::Failed)
    }
}

/// An aggregated set of child task results for a `group` or `chord` header.
///
/// The group preserves child registration order so that
/// [`ResultGroup::collected_values`] and [`ResultGroup::results`] return
/// values in a deterministic, caller-meaningful order.
#[derive(Debug, Clone)]
pub struct ResultGroup {
    /// Identifier of the group / chord this aggregate represents.
    group_id: TaskId,

    /// Child task ids in registration order. This is the canonical ordering.
    order: Vec<TaskId>,

    /// Latest known result per child (sparse until children report).
    results: HashMap<TaskId, TaskResultValue>,
}

impl ResultGroup {
    /// Create a new result group for `group_id` with the given child task ids,
    /// in order. Duplicate ids are de-duplicated while preserving the first
    /// occurrence's position.
    #[must_use]
    pub fn new(group_id: TaskId, children: Vec<TaskId>) -> Self {
        let mut order = Vec::with_capacity(children.len());
        for child in children {
            if !order.contains(&child) {
                order.push(child);
            }
        }
        Self {
            group_id,
            order,
            results: HashMap::new(),
        }
    }

    /// Create an empty result group with no children yet registered.
    #[must_use]
    pub fn empty(group_id: TaskId) -> Self {
        Self {
            group_id,
            order: Vec::new(),
            results: HashMap::new(),
        }
    }

    /// The group / chord identifier.
    #[inline]
    #[must_use]
    pub fn group_id(&self) -> TaskId {
        self.group_id
    }

    /// The child task ids in registration order.
    #[inline]
    #[must_use]
    pub fn child_ids(&self) -> &[TaskId] {
        &self.order
    }

    /// Number of registered children.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.order.len()
    }

    /// Returns `true` if no children are registered.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.order.is_empty()
    }

    /// Register an additional child task id, appended to the end of the order.
    ///
    /// No-op if the child is already registered (order is preserved).
    pub fn add_child(&mut self, task_id: TaskId) {
        if !self.order.contains(&task_id) {
            self.order.push(task_id);
        }
    }

    /// Register an additional child together with its result.
    pub fn add_child_with_result(&mut self, task_id: TaskId, result: TaskResultValue) {
        self.add_child(task_id);
        self.results.insert(task_id, result);
    }

    /// Set (or replace) the latest result for a child.
    ///
    /// If the child was not previously registered it is appended to the
    /// ordering so its value still participates in collection.
    pub fn set_result(&mut self, task_id: TaskId, result: TaskResultValue) {
        self.add_child(task_id);
        self.results.insert(task_id, result);
    }

    /// Borrow the latest result for a specific child, if reported.
    #[must_use]
    pub fn result_for(&self, task_id: TaskId) -> Option<&TaskResultValue> {
        self.results.get(&task_id)
    }

    /// Build a snapshot of all children (with their results) in order.
    #[must_use]
    pub fn children(&self) -> Vec<GroupChild> {
        self.order
            .iter()
            .map(|id| GroupChild {
                task_id: *id,
                result: self.results.get(id).cloned(),
            })
            .collect()
    }

    /// Number of children that have reported any result (terminal or not).
    #[must_use]
    pub fn reported_count(&self) -> usize {
        self.results.len()
    }

    /// Number of children that have reached a terminal/ready state.
    #[must_use]
    pub fn completed_count(&self) -> usize {
        self.order
            .iter()
            .filter(|id| self.results.get(id).is_some_and(TaskResultValue::is_ready))
            .count()
    }

    /// Number of children that succeeded.
    #[must_use]
    pub fn successful_count(&self) -> usize {
        self.order
            .iter()
            .filter(|id| {
                self.results
                    .get(id)
                    .is_some_and(TaskResultValue::is_successful)
            })
            .count()
    }

    /// Number of children that failed or were rejected.
    #[must_use]
    pub fn failed_count(&self) -> usize {
        self.order
            .iter()
            .filter(|id| self.results.get(id).is_some_and(TaskResultValue::is_failed))
            .count()
    }

    /// Number of children that were revoked.
    #[must_use]
    pub fn revoked_count(&self) -> usize {
        self.order
            .iter()
            .filter(|id| matches!(self.results.get(id), Some(TaskResultValue::Revoked)))
            .count()
    }

    /// Fraction of children that are complete, in `[0.0, 1.0]`.
    ///
    /// An empty group is considered fully complete (`1.0`).
    #[must_use]
    pub fn completion_ratio(&self) -> f64 {
        if self.order.is_empty() {
            return 1.0;
        }
        self.completed_count() as f64 / self.order.len() as f64
    }

    /// Returns `true` if **all** registered children are terminal (ready).
    ///
    /// An empty group is trivially ready.
    #[must_use]
    pub fn ready(&self) -> bool {
        self.order
            .iter()
            .all(|id| self.results.get(id).is_some_and(TaskResultValue::is_ready))
    }

    /// Returns `true` if the group is ready and every child succeeded.
    #[must_use]
    pub fn successful(&self) -> bool {
        self.ready()
            && self.order.iter().all(|id| {
                self.results
                    .get(id)
                    .is_some_and(TaskResultValue::is_successful)
            })
    }

    /// Returns `true` if the group is ready and at least one child did not
    /// succeed (failed, revoked, or rejected).
    #[must_use]
    pub fn failed(&self) -> bool {
        self.ready() && !self.successful()
    }

    /// Compute the aggregate [`GroupStatus`] (readiness + success/failure
    /// rollup).
    #[must_use]
    pub fn status(&self) -> GroupStatus {
        if !self.ready() {
            GroupStatus::Pending
        } else if self.successful() {
            GroupStatus::Succeeded
        } else {
            GroupStatus::Failed
        }
    }

    /// The ordered list of latest results, one entry per registered child in
    /// registration order. Children that have not reported are `None`.
    #[must_use]
    pub fn results(&self) -> Vec<Option<TaskResultValue>> {
        self.order
            .iter()
            .map(|id| self.results.get(id).cloned())
            .collect()
    }

    /// The ordered collected success values.
    ///
    /// * Returns `Some(values)` (in registration order) only if the group is
    ///   ready *and* every child succeeded.
    /// * Returns `None` if the group is not yet ready or any child did not
    ///   produce a success value — mirroring the semantics of a `chord`
    ///   header, which only fires when all members succeed.
    #[must_use]
    pub fn collected_values(&self) -> Option<Vec<Value>> {
        if !self.ready() {
            return None;
        }
        let mut values = Vec::with_capacity(self.order.len());
        for id in &self.order {
            match self
                .results
                .get(id)
                .and_then(TaskResultValue::success_value)
            {
                Some(value) => values.push(value.clone()),
                None => return None,
            }
        }
        Some(values)
    }

    /// Collect the success values of the children that *did* succeed, ignoring
    /// the rest, in registration order. Unlike [`Self::collected_values`] this
    /// never returns `None` and is useful for partial / best-effort rollups.
    #[must_use]
    pub fn successful_values(&self) -> Vec<Value> {
        self.order
            .iter()
            .filter_map(|id| {
                self.results
                    .get(id)
                    .and_then(TaskResultValue::success_value)
                    .cloned()
            })
            .collect()
    }

    /// Collect the error messages of all non-successful children, in
    /// registration order.
    #[must_use]
    pub fn error_messages(&self) -> Vec<String> {
        self.order
            .iter()
            .filter_map(|id| {
                self.results
                    .get(id)
                    .and_then(TaskResultValue::error_message)
                    .map(String::from)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use uuid::Uuid;

    fn ids(n: usize) -> Vec<TaskId> {
        (0..n).map(|_| Uuid::new_v4()).collect()
    }

    #[test]
    fn empty_group_is_ready_and_successful() {
        let group = ResultGroup::empty(Uuid::new_v4());
        assert!(group.is_empty());
        assert!(group.ready());
        assert!(group.successful());
        assert!(!group.failed());
        assert_eq!(group.status(), GroupStatus::Succeeded);
        assert_eq!(group.collected_values(), Some(vec![]));
        assert_eq!(group.completion_ratio(), 1.0);
    }

    #[test]
    fn readiness_requires_all_children() {
        let v = ids(3);
        let mut group = ResultGroup::new(Uuid::new_v4(), v.clone());
        assert!(!group.ready());
        assert_eq!(group.status(), GroupStatus::Pending);

        group.set_result(v[0], TaskResultValue::Success(json!(10)));
        group.set_result(v[1], TaskResultValue::Success(json!(20)));
        assert!(!group.ready());
        assert_eq!(group.completed_count(), 2);
        assert_eq!(group.completion_ratio(), 2.0 / 3.0);

        group.set_result(v[2], TaskResultValue::Success(json!(30)));
        assert!(group.ready());
        assert_eq!(group.completed_count(), 3);
    }

    #[test]
    fn non_terminal_result_does_not_count_as_ready() {
        let v = ids(2);
        let mut group = ResultGroup::new(Uuid::new_v4(), v.clone());
        group.set_result(v[0], TaskResultValue::Success(json!(1)));
        // Started is non-terminal.
        group.set_result(v[1], TaskResultValue::Started);
        assert!(!group.ready());
        assert_eq!(group.completed_count(), 1);
        assert_eq!(group.reported_count(), 2);
    }

    #[test]
    fn success_rollup_and_ordered_values() {
        let v = ids(3);
        let mut group = ResultGroup::new(Uuid::new_v4(), v.clone());
        // Report out of order; ordering must follow registration order.
        group.set_result(v[2], TaskResultValue::Success(json!("c")));
        group.set_result(v[0], TaskResultValue::Success(json!("a")));
        group.set_result(v[1], TaskResultValue::Success(json!("b")));

        assert!(group.successful());
        assert_eq!(group.status(), GroupStatus::Succeeded);
        assert_eq!(group.successful_count(), 3);

        let values = group.collected_values().unwrap();
        assert_eq!(values, vec![json!("a"), json!("b"), json!("c")]);
    }

    #[test]
    fn failure_rollup() {
        let v = ids(3);
        let mut group = ResultGroup::new(Uuid::new_v4(), v.clone());
        group.set_result(v[0], TaskResultValue::Success(json!(1)));
        group.set_result(
            v[1],
            TaskResultValue::Failure {
                error: "boom".to_string(),
                traceback: None,
            },
        );
        group.set_result(v[2], TaskResultValue::Success(json!(3)));

        assert!(group.ready());
        assert!(!group.successful());
        assert!(group.failed());
        assert_eq!(group.status(), GroupStatus::Failed);
        assert_eq!(group.failed_count(), 1);
        assert_eq!(group.successful_count(), 2);

        // collected_values returns None because not all succeeded.
        assert!(group.collected_values().is_none());

        // Partial collection still yields the two successes in order.
        assert_eq!(group.successful_values(), vec![json!(1), json!(3)]);
        assert_eq!(group.error_messages(), vec!["boom".to_string()]);
    }

    #[test]
    fn revoked_child_marks_failure() {
        let v = ids(2);
        let mut group = ResultGroup::new(Uuid::new_v4(), v.clone());
        group.set_result(v[0], TaskResultValue::Success(json!(1)));
        group.set_result(v[1], TaskResultValue::Revoked);

        assert!(group.ready());
        assert!(group.failed());
        assert_eq!(group.status(), GroupStatus::Failed);
        assert_eq!(group.revoked_count(), 1);
        assert!(group.collected_values().is_none());
    }

    #[test]
    fn rejected_child_marks_failure() {
        let v = ids(2);
        let mut group = ResultGroup::new(Uuid::new_v4(), v.clone());
        group.set_result(v[0], TaskResultValue::Success(json!(1)));
        group.set_result(
            v[1],
            TaskResultValue::Rejected {
                reason: "bad input".to_string(),
            },
        );
        assert!(group.failed());
        assert_eq!(group.failed_count(), 1);
        assert_eq!(group.error_messages(), vec!["bad input".to_string()]);
    }

    #[test]
    fn duplicate_children_are_deduped() {
        let id = Uuid::new_v4();
        let group = ResultGroup::new(Uuid::new_v4(), vec![id, id, id]);
        assert_eq!(group.len(), 1);
        assert_eq!(group.child_ids(), &[id]);
    }

    #[test]
    fn set_result_for_unknown_child_registers_it() {
        let mut group = ResultGroup::empty(Uuid::new_v4());
        let id = Uuid::new_v4();
        group.set_result(id, TaskResultValue::Success(json!(42)));
        assert_eq!(group.len(), 1);
        assert!(group.ready());
        assert_eq!(group.collected_values(), Some(vec![json!(42)]));
    }

    #[test]
    fn children_snapshot_matches_order() {
        let v = ids(2);
        let mut group = ResultGroup::new(Uuid::new_v4(), v.clone());
        group.set_result(v[0], TaskResultValue::Success(json!(1)));
        let children = group.children();
        assert_eq!(children.len(), 2);
        assert_eq!(children[0].task_id, v[0]);
        assert!(children[0].is_ready());
        assert!(children[0].is_successful());
        assert_eq!(children[1].task_id, v[1]);
        assert!(!children[1].is_ready());
        assert!(children[1].result.is_none());
    }

    #[test]
    fn group_status_predicates() {
        assert!(GroupStatus::Succeeded.is_ready());
        assert!(GroupStatus::Succeeded.is_successful());
        assert!(!GroupStatus::Succeeded.is_failed());
        assert!(GroupStatus::Failed.is_ready());
        assert!(GroupStatus::Failed.is_failed());
        assert!(!GroupStatus::Pending.is_ready());
    }

    #[test]
    fn add_child_with_result_and_result_for() {
        let mut group = ResultGroup::empty(Uuid::new_v4());
        let id = Uuid::new_v4();
        group.add_child_with_result(id, TaskResultValue::Success(json!(7)));
        assert_eq!(
            group
                .result_for(id)
                .and_then(TaskResultValue::success_value),
            Some(&json!(7))
        );
        assert!(group.result_for(Uuid::new_v4()).is_none());
    }
}
