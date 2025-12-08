//! Directed Acyclic Graph (DAG) support for task dependencies
//!
//! This module provides functionality for managing task dependencies and validating
//! that task graphs are acyclic.
//!
//! # Example
//!
//! ```
//! use celers_core::dag::{TaskDag, DagNode};
//! use uuid::Uuid;
//!
//! let mut dag = TaskDag::new();
//! let task1 = Uuid::new_v4();
//! let task2 = Uuid::new_v4();
//! let task3 = Uuid::new_v4();
//!
//! // Create a simple DAG: task1 -> task2 -> task3
//! dag.add_node(task1, "task1");
//! dag.add_node(task2, "task2");
//! dag.add_node(task3, "task3");
//!
//! dag.add_dependency(task2, task1).unwrap();
//! dag.add_dependency(task3, task2).unwrap();
//!
//! // Validate the DAG (no cycles)
//! assert!(dag.validate().is_ok());
//!
//! // Get execution order
//! let order = dag.topological_sort().unwrap();
//! assert_eq!(order, vec![task1, task2, task3]);
//! ```

use crate::{CelersError, Result, TaskId};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};

/// A node in the task DAG
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DagNode {
    /// Task ID
    pub task_id: TaskId,

    /// Task name for debugging
    pub task_name: String,

    /// Tasks that this task depends on (must complete before this task)
    pub dependencies: HashSet<TaskId>,

    /// Tasks that depend on this task (will execute after this task)
    pub dependents: HashSet<TaskId>,
}

impl DagNode {
    /// Create a new DAG node
    pub fn new(task_id: TaskId, task_name: impl Into<String>) -> Self {
        Self {
            task_id,
            task_name: task_name.into(),
            dependencies: HashSet::new(),
            dependents: HashSet::new(),
        }
    }

    /// Check if this node has any dependencies
    pub fn has_dependencies(&self) -> bool {
        !self.dependencies.is_empty()
    }

    /// Check if this node has any dependents
    pub fn has_dependents(&self) -> bool {
        !self.dependents.is_empty()
    }

    /// Check if this is a root node (no dependencies)
    pub fn is_root(&self) -> bool {
        self.dependencies.is_empty()
    }

    /// Check if this is a leaf node (no dependents)
    pub fn is_leaf(&self) -> bool {
        self.dependents.is_empty()
    }
}

/// Directed Acyclic Graph for task dependencies
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskDag {
    /// All nodes in the DAG
    nodes: HashMap<TaskId, DagNode>,
}

impl TaskDag {
    /// Create a new empty DAG
    pub fn new() -> Self {
        Self {
            nodes: HashMap::new(),
        }
    }

    /// Add a node to the DAG
    pub fn add_node(&mut self, task_id: TaskId, task_name: impl Into<String>) {
        self.nodes
            .entry(task_id)
            .or_insert_with(|| DagNode::new(task_id, task_name));
    }

    /// Add a dependency relationship: `task_id` depends on `depends_on`
    ///
    /// This means `depends_on` must complete before `task_id` can execute.
    pub fn add_dependency(&mut self, task_id: TaskId, depends_on: TaskId) -> Result<()> {
        // Ensure both nodes exist
        if !self.nodes.contains_key(&task_id) {
            return Err(CelersError::Configuration(format!(
                "Task {} not found in DAG",
                task_id
            )));
        }
        if !self.nodes.contains_key(&depends_on) {
            return Err(CelersError::Configuration(format!(
                "Dependency task {} not found in DAG",
                depends_on
            )));
        }

        // Add the dependency
        if let Some(node) = self.nodes.get_mut(&task_id) {
            node.dependencies.insert(depends_on);
        }

        // Add the dependent
        if let Some(node) = self.nodes.get_mut(&depends_on) {
            node.dependents.insert(task_id);
        }

        // Validate no cycles were introduced
        self.validate()?;

        Ok(())
    }

    /// Remove a dependency relationship
    pub fn remove_dependency(&mut self, task_id: TaskId, depends_on: TaskId) {
        if let Some(node) = self.nodes.get_mut(&task_id) {
            node.dependencies.remove(&depends_on);
        }
        if let Some(node) = self.nodes.get_mut(&depends_on) {
            node.dependents.remove(&task_id);
        }
    }

    /// Get a node by task ID
    pub fn get_node(&self, task_id: &TaskId) -> Option<&DagNode> {
        self.nodes.get(task_id)
    }

    /// Get all root nodes (nodes with no dependencies)
    pub fn get_roots(&self) -> Vec<TaskId> {
        self.nodes
            .values()
            .filter(|node| node.is_root())
            .map(|node| node.task_id)
            .collect()
    }

    /// Get all leaf nodes (nodes with no dependents)
    pub fn get_leaves(&self) -> Vec<TaskId> {
        self.nodes
            .values()
            .filter(|node| node.is_leaf())
            .map(|node| node.task_id)
            .collect()
    }

    /// Get the dependencies of a task
    pub fn get_dependencies(&self, task_id: &TaskId) -> Option<Vec<TaskId>> {
        self.nodes
            .get(task_id)
            .map(|node| node.dependencies.iter().copied().collect())
    }

    /// Get the dependents of a task
    pub fn get_dependents(&self, task_id: &TaskId) -> Option<Vec<TaskId>> {
        self.nodes
            .get(task_id)
            .map(|node| node.dependents.iter().copied().collect())
    }

    /// Check if the DAG contains a cycle
    fn has_cycle(&self) -> bool {
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();

        for node_id in self.nodes.keys() {
            if self.has_cycle_util(*node_id, &mut visited, &mut rec_stack) {
                return true;
            }
        }

        false
    }

    /// Helper function for cycle detection using DFS
    fn has_cycle_util(
        &self,
        node_id: TaskId,
        visited: &mut HashSet<TaskId>,
        rec_stack: &mut HashSet<TaskId>,
    ) -> bool {
        if rec_stack.contains(&node_id) {
            return true; // Cycle detected
        }

        if visited.contains(&node_id) {
            return false; // Already visited this path
        }

        visited.insert(node_id);
        rec_stack.insert(node_id);

        if let Some(node) = self.nodes.get(&node_id) {
            for &dep_id in &node.dependencies {
                if self.has_cycle_util(dep_id, visited, rec_stack) {
                    return true;
                }
            }
        }

        rec_stack.remove(&node_id);
        false
    }

    /// Validate the DAG structure (check for cycles)
    pub fn validate(&self) -> Result<()> {
        if self.has_cycle() {
            return Err(CelersError::Configuration(
                "Task DAG contains a cycle".to_string(),
            ));
        }
        Ok(())
    }

    /// Perform topological sort on the DAG
    ///
    /// Returns tasks in execution order (dependencies before dependents).
    /// Returns an error if the DAG contains a cycle.
    pub fn topological_sort(&self) -> Result<Vec<TaskId>> {
        self.validate()?;

        let mut in_degree: HashMap<TaskId, usize> = HashMap::new();
        let mut result = Vec::new();
        let mut queue = VecDeque::new();

        // Calculate in-degrees
        for node in self.nodes.values() {
            in_degree.insert(node.task_id, node.dependencies.len());
            if node.is_root() {
                queue.push_back(node.task_id);
            }
        }

        // Process nodes in topological order
        while let Some(task_id) = queue.pop_front() {
            result.push(task_id);

            if let Some(node) = self.nodes.get(&task_id) {
                for &dependent_id in &node.dependents {
                    if let Some(degree) = in_degree.get_mut(&dependent_id) {
                        *degree -= 1;
                        if *degree == 0 {
                            queue.push_back(dependent_id);
                        }
                    }
                }
            }
        }

        // If not all nodes were processed, there's a cycle
        if result.len() != self.nodes.len() {
            return Err(CelersError::Configuration(
                "Task DAG contains a cycle".to_string(),
            ));
        }

        Ok(result)
    }

    /// Get the number of nodes in the DAG
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Get the number of edges in the DAG
    pub fn edge_count(&self) -> usize {
        self.nodes
            .values()
            .map(|node| node.dependencies.len())
            .sum()
    }

    /// Check if the DAG is empty
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// Clear all nodes and edges
    pub fn clear(&mut self) {
        self.nodes.clear();
    }
}

impl Default for TaskDag {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dag_basic() {
        let mut dag = TaskDag::new();
        let task1 = TaskId::new_v4();
        let task2 = TaskId::new_v4();

        dag.add_node(task1, "task1");
        dag.add_node(task2, "task2");

        assert_eq!(dag.node_count(), 2);
        assert_eq!(dag.edge_count(), 0);
    }

    #[test]
    fn test_dag_dependencies() {
        let mut dag = TaskDag::new();
        let task1 = TaskId::new_v4();
        let task2 = TaskId::new_v4();

        dag.add_node(task1, "task1");
        dag.add_node(task2, "task2");
        dag.add_dependency(task2, task1).unwrap();

        assert_eq!(dag.edge_count(), 1);

        let deps = dag.get_dependencies(&task2).unwrap();
        assert_eq!(deps.len(), 1);
        assert!(deps.contains(&task1));

        let dependents = dag.get_dependents(&task1).unwrap();
        assert_eq!(dependents.len(), 1);
        assert!(dependents.contains(&task2));
    }

    #[test]
    fn test_dag_cycle_detection() {
        let mut dag = TaskDag::new();
        let task1 = TaskId::new_v4();
        let task2 = TaskId::new_v4();
        let task3 = TaskId::new_v4();

        dag.add_node(task1, "task1");
        dag.add_node(task2, "task2");
        dag.add_node(task3, "task3");

        dag.add_dependency(task2, task1).unwrap();
        dag.add_dependency(task3, task2).unwrap();

        // Try to create a cycle
        let result = dag.add_dependency(task1, task3);
        assert!(result.is_err());
    }

    #[test]
    fn test_dag_topological_sort() {
        let mut dag = TaskDag::new();
        let task1 = TaskId::new_v4();
        let task2 = TaskId::new_v4();
        let task3 = TaskId::new_v4();

        dag.add_node(task1, "task1");
        dag.add_node(task2, "task2");
        dag.add_node(task3, "task3");

        dag.add_dependency(task2, task1).unwrap();
        dag.add_dependency(task3, task2).unwrap();

        let order = dag.topological_sort().unwrap();
        assert_eq!(order.len(), 3);

        // task1 should come before task2
        let pos1 = order.iter().position(|&t| t == task1).unwrap();
        let pos2 = order.iter().position(|&t| t == task2).unwrap();
        let pos3 = order.iter().position(|&t| t == task3).unwrap();

        assert!(pos1 < pos2);
        assert!(pos2 < pos3);
    }

    #[test]
    fn test_dag_roots_and_leaves() {
        let mut dag = TaskDag::new();
        let task1 = TaskId::new_v4();
        let task2 = TaskId::new_v4();
        let task3 = TaskId::new_v4();

        dag.add_node(task1, "task1");
        dag.add_node(task2, "task2");
        dag.add_node(task3, "task3");

        dag.add_dependency(task2, task1).unwrap();
        dag.add_dependency(task3, task2).unwrap();

        let roots = dag.get_roots();
        assert_eq!(roots.len(), 1);
        assert!(roots.contains(&task1));

        let leaves = dag.get_leaves();
        assert_eq!(leaves.len(), 1);
        assert!(leaves.contains(&task3));
    }

    #[test]
    fn test_dag_remove_dependency() {
        let mut dag = TaskDag::new();
        let task1 = TaskId::new_v4();
        let task2 = TaskId::new_v4();

        dag.add_node(task1, "task1");
        dag.add_node(task2, "task2");
        dag.add_dependency(task2, task1).unwrap();

        assert_eq!(dag.edge_count(), 1);

        dag.remove_dependency(task2, task1);
        assert_eq!(dag.edge_count(), 0);
    }

    #[test]
    fn test_dag_complex() {
        let mut dag = TaskDag::new();
        let task1 = TaskId::new_v4();
        let task2 = TaskId::new_v4();
        let task3 = TaskId::new_v4();
        let task4 = TaskId::new_v4();

        dag.add_node(task1, "task1");
        dag.add_node(task2, "task2");
        dag.add_node(task3, "task3");
        dag.add_node(task4, "task4");

        // task1 and task2 are independent roots
        // task3 depends on both task1 and task2
        // task4 depends on task3
        dag.add_dependency(task3, task1).unwrap();
        dag.add_dependency(task3, task2).unwrap();
        dag.add_dependency(task4, task3).unwrap();

        let order = dag.topological_sort().unwrap();
        assert_eq!(order.len(), 4);

        // task3 must come after both task1 and task2
        let pos1 = order.iter().position(|&t| t == task1).unwrap();
        let pos2 = order.iter().position(|&t| t == task2).unwrap();
        let pos3 = order.iter().position(|&t| t == task3).unwrap();
        let pos4 = order.iter().position(|&t| t == task4).unwrap();

        assert!(pos1 < pos3);
        assert!(pos2 < pos3);
        assert!(pos3 < pos4);
    }

    mod proptests {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            #[test]
            fn test_dag_node_count_matches_added_nodes(count in 1usize..20) {
                let mut dag = TaskDag::new();
                let ids: Vec<_> = (0..count).map(|_| TaskId::new_v4()).collect();

                for (i, id) in ids.iter().enumerate() {
                    dag.add_node(*id, &format!("task_{}", i));
                }

                prop_assert_eq!(dag.node_count(), count);
            }

            #[test]
            fn test_dag_linear_chain_sorts_correctly(count in 2usize..15) {
                let mut dag = TaskDag::new();
                let ids: Vec<_> = (0..count).map(|_| TaskId::new_v4()).collect();

                // Create linear chain: ids[0] -> ids[1] -> ids[2] -> ...
                for (i, id) in ids.iter().enumerate() {
                    dag.add_node(*id, &format!("task_{}", i));
                }

                for i in 1..ids.len() {
                    dag.add_dependency(ids[i], ids[i - 1]).unwrap();
                }

                let sorted = dag.topological_sort().unwrap();
                prop_assert_eq!(sorted.len(), count);

                // Verify order: each node must appear after all its dependencies
                for i in 1..ids.len() {
                    let pos_parent = sorted.iter().position(|&t| t == ids[i - 1]).unwrap();
                    let pos_child = sorted.iter().position(|&t| t == ids[i]).unwrap();
                    prop_assert!(pos_parent < pos_child);
                }
            }

            #[test]
            fn test_dag_validate_always_succeeds_for_acyclic(count in 2usize..10) {
                let mut dag = TaskDag::new();
                let ids: Vec<_> = (0..count).map(|_| TaskId::new_v4()).collect();

                for (i, id) in ids.iter().enumerate() {
                    dag.add_node(*id, &format!("task_{}", i));
                }

                // Create valid DAG structure
                for i in 1..ids.len() {
                    dag.add_dependency(ids[i], ids[i - 1]).unwrap();
                }

                prop_assert!(dag.validate().is_ok());
            }

            #[test]
            fn test_dag_roots_have_no_dependencies(count in 2usize..10) {
                let mut dag = TaskDag::new();
                let ids: Vec<_> = (0..count).map(|_| TaskId::new_v4()).collect();

                for (i, id) in ids.iter().enumerate() {
                    dag.add_node(*id, &format!("task_{}", i));
                }

                // Create linear chain
                for i in 1..ids.len() {
                    dag.add_dependency(ids[i], ids[i - 1]).unwrap();
                }

                let roots = dag.get_roots();

                // Each root should have no dependencies
                for root in roots {
                    let deps = dag.get_dependencies(&root).unwrap();
                    prop_assert_eq!(deps.len(), 0);
                }
            }

            #[test]
            fn test_dag_leaves_have_no_dependents(count in 2usize..10) {
                let mut dag = TaskDag::new();
                let ids: Vec<_> = (0..count).map(|_| TaskId::new_v4()).collect();

                for (i, id) in ids.iter().enumerate() {
                    dag.add_node(*id, &format!("task_{}", i));
                }

                // Create linear chain
                for i in 1..ids.len() {
                    dag.add_dependency(ids[i], ids[i - 1]).unwrap();
                }

                let leaves = dag.get_leaves();

                // Each leaf should have no dependents
                for leaf in leaves {
                    let dependents = dag.get_dependents(&leaf).unwrap();
                    prop_assert_eq!(dependents.len(), 0);
                }
            }

            #[test]
            fn test_dag_edge_count_matches_added_dependencies(node_count in 2usize..10) {
                let mut dag = TaskDag::new();
                let ids: Vec<_> = (0..node_count).map(|_| TaskId::new_v4()).collect();

                for (i, id) in ids.iter().enumerate() {
                    dag.add_node(*id, &format!("task_{}", i));
                }

                // Add edges in a linear chain
                let edge_count = node_count - 1;
                for i in 1..ids.len() {
                    dag.add_dependency(ids[i], ids[i - 1]).unwrap();
                }

                prop_assert_eq!(dag.edge_count(), edge_count);
            }

            #[test]
            fn test_dag_remove_dependency_decreases_edge_count(node_count in 2usize..10) {
                let mut dag = TaskDag::new();
                let ids: Vec<_> = (0..node_count).map(|_| TaskId::new_v4()).collect();

                for (i, id) in ids.iter().enumerate() {
                    dag.add_node(*id, &format!("task_{}", i));
                }

                // Add all edges
                for i in 1..ids.len() {
                    dag.add_dependency(ids[i], ids[i - 1]).unwrap();
                }

                let initial_count = dag.edge_count();

                // Remove one dependency
                dag.remove_dependency(ids[1], ids[0]);

                prop_assert_eq!(dag.edge_count(), initial_count - 1);
            }
        }
    }
}
