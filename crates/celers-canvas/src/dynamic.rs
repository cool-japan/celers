//! Dynamic workflow modification.
//!
//! Builder methods to add, insert, and remove steps in a [`Chain`] (and members
//! in a [`Group`]) at runtime, each returning the modified workflow after
//! validation. Unlike the existing consuming builder methods (`Chain::then`,
//! `Group::add`, …) which always succeed, the methods here validate their
//! arguments — rejecting out-of-bounds indices and empty task names — and report
//! problems as [`CanvasError::Invalid`].
//!
//! All methods take and return `self` by value so they compose fluently with
//! the rest of the canvas builder API while still surfacing a `Result`:
//!
//! ```
//! use celers_canvas::{Chain, Signature};
//!
//! let chain = Chain::new().then("a", vec![]).then("c", vec![]);
//! // Insert "b" between "a" and "c".
//! let chain = chain
//!     .insert_step(1, Signature::new("b".to_string()))
//!     .expect("index in range");
//! assert_eq!(chain.task_names(), vec!["a", "b", "c"]);
//! ```

use crate::{CanvasError, Chain, Group, Signature};

/// Reject a signature with an empty task name (an invalid workflow step).
fn validate_signature(sig: &Signature) -> Result<(), CanvasError> {
    if sig.task.trim().is_empty() {
        Err(CanvasError::Invalid(
            "task name cannot be empty".to_string(),
        ))
    } else {
        Ok(())
    }
}

impl Chain {
    /// Append a step to the end of the chain after validating it.
    ///
    /// Returns [`CanvasError::Invalid`] if the task name is empty.
    pub fn add_step(mut self, sig: Signature) -> Result<Self, CanvasError> {
        validate_signature(&sig)?;
        self.tasks.push(sig);
        Ok(self)
    }

    /// Insert a step at `index`, shifting later steps to the right.
    ///
    /// `index` may equal the current length (equivalent to appending). Returns
    /// [`CanvasError::Invalid`] for an out-of-range index or an empty task name.
    pub fn insert_step(mut self, index: usize, sig: Signature) -> Result<Self, CanvasError> {
        validate_signature(&sig)?;
        if index > self.tasks.len() {
            return Err(CanvasError::Invalid(format!(
                "insert index {} out of range for chain of length {}",
                index,
                self.tasks.len()
            )));
        }
        self.tasks.insert(index, sig);
        Ok(self)
    }

    /// Remove the step at `index`, returning the modified chain.
    ///
    /// Returns [`CanvasError::Invalid`] for an out-of-range index.
    pub fn remove_step(mut self, index: usize) -> Result<Self, CanvasError> {
        if index >= self.tasks.len() {
            return Err(CanvasError::Invalid(format!(
                "remove index {} out of range for chain of length {}",
                index,
                self.tasks.len()
            )));
        }
        self.tasks.remove(index);
        Ok(self)
    }

    /// Remove the first step whose task name equals `task_name`.
    ///
    /// Returns [`CanvasError::Invalid`] if no such step exists.
    pub fn remove_step_by_name(mut self, task_name: &str) -> Result<Self, CanvasError> {
        match self.tasks.iter().position(|t| t.task == task_name) {
            Some(index) => {
                self.tasks.remove(index);
                Ok(self)
            }
            None => Err(CanvasError::Invalid(format!(
                "no step named '{}' in chain",
                task_name
            ))),
        }
    }

    /// Replace the step at `index` with `sig`, returning the modified chain.
    ///
    /// Returns [`CanvasError::Invalid`] for an out-of-range index or an empty
    /// task name.
    pub fn replace_step(mut self, index: usize, sig: Signature) -> Result<Self, CanvasError> {
        validate_signature(&sig)?;
        if index >= self.tasks.len() {
            return Err(CanvasError::Invalid(format!(
                "replace index {} out of range for chain of length {}",
                index,
                self.tasks.len()
            )));
        }
        self.tasks[index] = sig;
        Ok(self)
    }

    /// Move the step at `from` to `to`, shifting intervening steps.
    ///
    /// Both indices must be in range. A no-op move (`from == to`) is allowed.
    pub fn move_step(mut self, from: usize, to: usize) -> Result<Self, CanvasError> {
        let len = self.tasks.len();
        if from >= len || to >= len {
            return Err(CanvasError::Invalid(format!(
                "move indices ({from}, {to}) out of range for chain of length {len}"
            )));
        }
        let sig = self.tasks.remove(from);
        self.tasks.insert(to, sig);
        Ok(self)
    }
}

impl Group {
    /// Add a member to the group after validating it.
    ///
    /// Returns [`CanvasError::Invalid`] if the task name is empty.
    pub fn add_member(mut self, sig: Signature) -> Result<Self, CanvasError> {
        validate_signature(&sig)?;
        self.tasks.push(sig);
        Ok(self)
    }

    /// Insert a member at `index`, shifting later members to the right.
    ///
    /// `index` may equal the current length. Returns [`CanvasError::Invalid`]
    /// for an out-of-range index or an empty task name.
    pub fn insert_member(mut self, index: usize, sig: Signature) -> Result<Self, CanvasError> {
        validate_signature(&sig)?;
        if index > self.tasks.len() {
            return Err(CanvasError::Invalid(format!(
                "insert index {} out of range for group of size {}",
                index,
                self.tasks.len()
            )));
        }
        self.tasks.insert(index, sig);
        Ok(self)
    }

    /// Remove the member at `index`, returning the modified group.
    ///
    /// Returns [`CanvasError::Invalid`] for an out-of-range index.
    pub fn remove_member(mut self, index: usize) -> Result<Self, CanvasError> {
        if index >= self.tasks.len() {
            return Err(CanvasError::Invalid(format!(
                "remove index {} out of range for group of size {}",
                index,
                self.tasks.len()
            )));
        }
        self.tasks.remove(index);
        Ok(self)
    }

    /// Remove the first member whose task name equals `task_name`.
    ///
    /// Returns [`CanvasError::Invalid`] if no such member exists.
    pub fn remove_member_by_name(mut self, task_name: &str) -> Result<Self, CanvasError> {
        match self.tasks.iter().position(|t| t.task == task_name) {
            Some(index) => {
                self.tasks.remove(index);
                Ok(self)
            }
            None => Err(CanvasError::Invalid(format!(
                "no member named '{}' in group",
                task_name
            ))),
        }
    }

    /// Replace the member at `index` with `sig`, returning the modified group.
    ///
    /// Returns [`CanvasError::Invalid`] for an out-of-range index or an empty
    /// task name.
    pub fn replace_member(mut self, index: usize, sig: Signature) -> Result<Self, CanvasError> {
        validate_signature(&sig)?;
        if index >= self.tasks.len() {
            return Err(CanvasError::Invalid(format!(
                "replace index {} out of range for group of size {}",
                index,
                self.tasks.len()
            )));
        }
        self.tasks[index] = sig;
        Ok(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sig(name: &str) -> Signature {
        Signature::new(name.to_string())
    }

    // ---- Chain ----------------------------------------------------------

    #[test]
    fn chain_add_step_appends() {
        let chain = Chain::new()
            .add_step(sig("a"))
            .expect("a")
            .add_step(sig("b"))
            .expect("b");
        assert_eq!(chain.task_names(), vec!["a", "b"]);
    }

    #[test]
    fn chain_add_empty_name_errors() {
        let err = Chain::new().add_step(sig("  ")).expect_err("empty name");
        assert!(err.is_invalid());
    }

    #[test]
    fn chain_insert_in_middle() {
        let chain = Chain::new().then("a", vec![]).then("c", vec![]);
        let chain = chain.insert_step(1, sig("b")).expect("insert");
        assert_eq!(chain.task_names(), vec!["a", "b", "c"]);
    }

    #[test]
    fn chain_insert_at_end_allowed() {
        let chain = Chain::new().then("a", vec![]);
        let chain = chain.insert_step(1, sig("b")).expect("insert at len");
        assert_eq!(chain.task_names(), vec!["a", "b"]);
    }

    #[test]
    fn chain_insert_out_of_range_errors() {
        let chain = Chain::new().then("a", vec![]);
        let err = chain.insert_step(5, sig("x")).expect_err("oob");
        assert!(err.is_invalid());
    }

    #[test]
    fn chain_remove_by_index() {
        let chain = Chain::new()
            .then("a", vec![])
            .then("b", vec![])
            .then("c", vec![]);
        let chain = chain.remove_step(1).expect("remove");
        assert_eq!(chain.task_names(), vec!["a", "c"]);
    }

    #[test]
    fn chain_remove_out_of_range_errors() {
        let chain = Chain::new().then("a", vec![]);
        let err = chain.remove_step(3).expect_err("oob");
        assert!(err.is_invalid());
    }

    #[test]
    fn chain_remove_by_name() {
        let chain = Chain::new()
            .then("a", vec![])
            .then("b", vec![])
            .then("c", vec![]);
        let chain = chain.remove_step_by_name("b").expect("remove b");
        assert_eq!(chain.task_names(), vec!["a", "c"]);
    }

    #[test]
    fn chain_remove_missing_name_errors() {
        let chain = Chain::new().then("a", vec![]);
        let err = chain.remove_step_by_name("zzz").expect_err("missing");
        assert!(err.is_invalid());
    }

    #[test]
    fn chain_replace_step() {
        let chain = Chain::new().then("a", vec![]).then("b", vec![]);
        let chain = chain.replace_step(1, sig("B")).expect("replace");
        assert_eq!(chain.task_names(), vec!["a", "B"]);
    }

    #[test]
    fn chain_replace_out_of_range_errors() {
        let chain = Chain::new().then("a", vec![]);
        assert!(chain.replace_step(9, sig("x")).is_err());
    }

    #[test]
    fn chain_move_step() {
        let chain = Chain::new()
            .then("a", vec![])
            .then("b", vec![])
            .then("c", vec![]);
        // Move "a" to the end.
        let chain = chain.move_step(0, 2).expect("move");
        assert_eq!(chain.task_names(), vec!["b", "c", "a"]);
    }

    #[test]
    fn chain_move_out_of_range_errors() {
        let chain = Chain::new().then("a", vec![]).then("b", vec![]);
        assert!(chain.move_step(0, 9).is_err());
    }

    // ---- Group ----------------------------------------------------------

    #[test]
    fn group_add_member() {
        let group = Group::new()
            .add_member(sig("p1"))
            .expect("p1")
            .add_member(sig("p2"))
            .expect("p2");
        assert_eq!(group.len(), 2);
        assert!(group.contains_task("p1"));
        assert!(group.contains_task("p2"));
    }

    #[test]
    fn group_add_empty_name_errors() {
        assert!(Group::new().add_member(sig("")).is_err());
    }

    #[test]
    fn group_insert_member() {
        let group = Group::new().add("a", vec![]).add("c", vec![]);
        let group = group.insert_member(1, sig("b")).expect("insert");
        assert_eq!(group.len(), 3);
        // Order is preserved within the group's task vector.
        assert_eq!(group.get(1).expect("idx 1").task, "b");
    }

    #[test]
    fn group_insert_out_of_range_errors() {
        let group = Group::new().add("a", vec![]);
        assert!(group.insert_member(9, sig("x")).is_err());
    }

    #[test]
    fn group_remove_member() {
        let group = Group::new()
            .add("a", vec![])
            .add("b", vec![])
            .add("c", vec![]);
        let group = group.remove_member(0).expect("remove");
        assert_eq!(group.len(), 2);
        assert!(!group.contains_task("a"));
    }

    #[test]
    fn group_remove_out_of_range_errors() {
        let group = Group::new().add("a", vec![]);
        assert!(group.remove_member(7).is_err());
    }

    #[test]
    fn group_remove_member_by_name() {
        let group = Group::new().add("a", vec![]).add("b", vec![]);
        let group = group.remove_member_by_name("a").expect("remove a");
        assert_eq!(group.len(), 1);
        assert!(group.contains_task("b"));
    }

    #[test]
    fn group_remove_missing_name_errors() {
        let group = Group::new().add("a", vec![]);
        assert!(group.remove_member_by_name("nope").is_err());
    }

    #[test]
    fn group_replace_member() {
        let group = Group::new().add("a", vec![]).add("b", vec![]);
        let group = group.replace_member(0, sig("A")).expect("replace");
        assert!(group.contains_task("A"));
        assert!(!group.contains_task("a"));
    }

    #[test]
    fn group_replace_out_of_range_errors() {
        let group = Group::new().add("a", vec![]);
        assert!(group.replace_member(4, sig("x")).is_err());
    }

    #[test]
    fn group_id_preserved_through_modifications() {
        let group = Group::new().add("a", vec![]);
        let id = group.group_id;
        let group = group
            .add_member(sig("b"))
            .expect("add")
            .remove_member(0)
            .expect("remove");
        assert_eq!(group.group_id, id, "group id must survive mutation");
    }
}
