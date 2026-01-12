//! Task dependencies and execution ordering system
//!
//! This module provides functionality for managing task dependencies and execution
//! ordering. Tasks can declare dependencies on other tasks, and the system ensures
//! they are executed in the correct order.
//!
//! # Features
//!
//! - **Dependency Declaration**: Tasks can declare dependencies using task IDs
//! - **Dependency Resolution**: Automatic resolution of dependency chains
//! - **Cycle Detection**: Prevents circular dependencies
//! - **Parallel Execution**: Independent tasks can execute in parallel
//! - **Status Tracking**: Monitor dependency status and completion
//!
//! # Example
//!
//! ```rust
//! use celers_worker::dependencies::{DependencyGraph, TaskDependency};
//! use uuid::Uuid;
//!
//! # async fn example() {
//! let mut graph = DependencyGraph::new();
//! let task1 = Uuid::new_v4();
//! let task2 = Uuid::new_v4();
//!
//! // Task 2 depends on Task 1
//! graph.add_dependency(task2, task1).await.unwrap();
//!
//! assert!(graph.has_dependency(task2, task1).await);
//! # }
//! ```

use celers_core::{CelersError, Result, TaskId};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Dependency status for a task
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DependencyStatus {
    /// All dependencies are satisfied
    Ready,
    /// Waiting for dependencies to complete
    Waiting,
    /// One or more dependencies failed
    Blocked,
    /// Task is currently running
    Running,
    /// Task completed successfully
    Completed,
    /// Task failed
    Failed,
}

impl DependencyStatus {
    /// Check if the task is ready to execute
    pub fn is_ready(&self) -> bool {
        matches!(self, DependencyStatus::Ready)
    }

    /// Check if the task is waiting
    pub fn is_waiting(&self) -> bool {
        matches!(self, DependencyStatus::Waiting)
    }

    /// Check if the task is blocked
    pub fn is_blocked(&self) -> bool {
        matches!(self, DependencyStatus::Blocked)
    }

    /// Check if the task is running
    pub fn is_running(&self) -> bool {
        matches!(self, DependencyStatus::Running)
    }

    /// Check if the task is completed
    pub fn is_completed(&self) -> bool {
        matches!(self, DependencyStatus::Completed)
    }

    /// Check if the task failed
    pub fn is_failed(&self) -> bool {
        matches!(self, DependencyStatus::Failed)
    }

    /// Check if the task is in a terminal state (completed or failed)
    pub fn is_terminal(&self) -> bool {
        matches!(self, DependencyStatus::Completed | DependencyStatus::Failed)
    }
}

impl std::fmt::Display for DependencyStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DependencyStatus::Ready => write!(f, "Ready"),
            DependencyStatus::Waiting => write!(f, "Waiting"),
            DependencyStatus::Blocked => write!(f, "Blocked"),
            DependencyStatus::Running => write!(f, "Running"),
            DependencyStatus::Completed => write!(f, "Completed"),
            DependencyStatus::Failed => write!(f, "Failed"),
        }
    }
}

/// A task dependency relationship
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskDependency {
    /// Task ID that has the dependency
    pub dependent_task: TaskId,

    /// Task ID that must complete first
    pub dependency_task: TaskId,

    /// Whether this dependency is optional (task can proceed if dependency fails)
    pub optional: bool,
}

impl TaskDependency {
    /// Create a new task dependency
    pub fn new(dependent_task: TaskId, dependency_task: TaskId) -> Self {
        Self {
            dependent_task,
            dependency_task,
            optional: false,
        }
    }

    /// Mark this dependency as optional
    pub fn optional(mut self) -> Self {
        self.optional = true;
        self
    }

    /// Check if this dependency is required
    pub fn is_required(&self) -> bool {
        !self.optional
    }
}

/// Dependency graph for managing task dependencies
pub struct DependencyGraph {
    /// Map of task ID to its dependencies
    dependencies: Arc<RwLock<HashMap<TaskId, HashSet<TaskId>>>>,

    /// Map of task ID to tasks that depend on it
    dependents: Arc<RwLock<HashMap<TaskId, HashSet<TaskId>>>>,

    /// Map of task ID to its current status
    statuses: Arc<RwLock<HashMap<TaskId, DependencyStatus>>>,

    /// Map of task ID to optional dependencies
    optional_dependencies: Arc<RwLock<HashMap<TaskId, HashSet<TaskId>>>>,
}

impl DependencyGraph {
    /// Create a new dependency graph
    pub fn new() -> Self {
        Self {
            dependencies: Arc::new(RwLock::new(HashMap::new())),
            dependents: Arc::new(RwLock::new(HashMap::new())),
            statuses: Arc::new(RwLock::new(HashMap::new())),
            optional_dependencies: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Add a dependency relationship
    pub async fn add_dependency(&self, dependent: TaskId, dependency: TaskId) -> Result<()> {
        // Check for self-dependency
        if dependent == dependency {
            return Err(CelersError::Other(
                "Task cannot depend on itself".to_string(),
            ));
        }

        // Check for circular dependencies
        if self.would_create_cycle(dependent, dependency).await {
            return Err(CelersError::Other(format!(
                "Adding dependency would create a cycle: {} -> {}",
                dependent, dependency
            )));
        }

        // Add to dependencies map
        let mut dependencies = self.dependencies.write().await;
        dependencies
            .entry(dependent)
            .or_default()
            .insert(dependency);
        drop(dependencies);

        // Add to dependents map
        let mut dependents = self.dependents.write().await;
        dependents.entry(dependency).or_default().insert(dependent);
        drop(dependents);

        // Initialize statuses if not present
        let mut statuses = self.statuses.write().await;
        statuses
            .entry(dependent)
            .or_insert(DependencyStatus::Waiting);
        statuses
            .entry(dependency)
            .or_insert(DependencyStatus::Waiting);

        debug!("Added dependency: {} depends on {}", dependent, dependency);
        Ok(())
    }

    /// Add an optional dependency
    pub async fn add_optional_dependency(
        &self,
        dependent: TaskId,
        dependency: TaskId,
    ) -> Result<()> {
        self.add_dependency(dependent, dependency).await?;

        let mut optional_deps = self.optional_dependencies.write().await;
        optional_deps
            .entry(dependent)
            .or_default()
            .insert(dependency);

        debug!(
            "Added optional dependency: {} optionally depends on {}",
            dependent, dependency
        );
        Ok(())
    }

    /// Check if adding a dependency would create a cycle
    async fn would_create_cycle(&self, dependent: TaskId, dependency: TaskId) -> bool {
        // Use BFS to check if dependency already depends on dependent
        let dependencies = self.dependencies.read().await;
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();

        queue.push_back(dependency);

        while let Some(current) = queue.pop_front() {
            if current == dependent {
                return true; // Cycle detected
            }

            if visited.contains(&current) {
                continue;
            }
            visited.insert(current);

            if let Some(deps) = dependencies.get(&current) {
                for dep in deps {
                    queue.push_back(*dep);
                }
            }
        }

        false
    }

    /// Remove a dependency
    pub async fn remove_dependency(&self, dependent: TaskId, dependency: TaskId) -> Result<()> {
        let mut dependencies = self.dependencies.write().await;
        if let Some(deps) = dependencies.get_mut(&dependent) {
            deps.remove(&dependency);
        }
        drop(dependencies);

        let mut dependents = self.dependents.write().await;
        if let Some(deps) = dependents.get_mut(&dependency) {
            deps.remove(&dependent);
        }
        drop(dependents);

        let mut optional_deps = self.optional_dependencies.write().await;
        if let Some(opts) = optional_deps.get_mut(&dependent) {
            opts.remove(&dependency);
        }

        debug!(
            "Removed dependency: {} no longer depends on {}",
            dependent, dependency
        );
        Ok(())
    }

    /// Check if a task has a dependency
    pub async fn has_dependency(&self, dependent: TaskId, dependency: TaskId) -> bool {
        let dependencies = self.dependencies.read().await;
        dependencies
            .get(&dependent)
            .map(|deps| deps.contains(&dependency))
            .unwrap_or(false)
    }

    /// Get all dependencies for a task
    pub async fn get_dependencies(&self, task_id: TaskId) -> Vec<TaskId> {
        let dependencies = self.dependencies.read().await;
        dependencies
            .get(&task_id)
            .map(|deps| deps.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Get all tasks that depend on a given task
    pub async fn get_dependents(&self, task_id: TaskId) -> Vec<TaskId> {
        let dependents = self.dependents.read().await;
        dependents
            .get(&task_id)
            .map(|deps| deps.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Update the status of a task
    pub async fn set_status(&self, task_id: TaskId, status: DependencyStatus) {
        let mut statuses = self.statuses.write().await;
        statuses.insert(task_id, status);
        drop(statuses);

        debug!("Task {} status updated to {}", task_id, status);
    }

    /// Update the statuses of tasks that depend on completed/failed tasks
    /// This should be called after a task completes or fails
    pub async fn update_dependent_statuses(&self, task_id: TaskId) {
        let dependents = self.get_dependents(task_id).await;
        let task_status = self.get_status(task_id).await;

        for dependent in dependents {
            // Check if all dependencies are satisfied
            if self.are_dependencies_satisfied(dependent).await {
                let mut statuses = self.statuses.write().await;
                statuses.insert(dependent, DependencyStatus::Ready);
            } else if task_status == Some(DependencyStatus::Failed)
                && !self.is_optional_dependency(dependent, task_id).await
            {
                // If a required dependency failed, block the dependent task
                let mut statuses = self.statuses.write().await;
                statuses.insert(dependent, DependencyStatus::Blocked);
            }
        }
    }

    /// Get the status of a task
    pub async fn get_status(&self, task_id: TaskId) -> Option<DependencyStatus> {
        let statuses = self.statuses.read().await;
        statuses.get(&task_id).copied()
    }

    /// Check if all dependencies for a task are satisfied
    async fn are_dependencies_satisfied(&self, task_id: TaskId) -> bool {
        let dependencies = self.get_dependencies(task_id).await;
        let statuses = self.statuses.read().await;
        let optional_deps = self.optional_dependencies.read().await;
        let optional_set = optional_deps.get(&task_id);

        for dep in dependencies {
            let dep_status = statuses.get(&dep);
            let is_optional = optional_set.map(|set| set.contains(&dep)).unwrap_or(false);

            match dep_status {
                Some(DependencyStatus::Completed) => continue,
                Some(DependencyStatus::Failed) if is_optional => continue,
                _ => return false,
            }
        }

        true
    }

    /// Check if a dependency is optional
    async fn is_optional_dependency(&self, dependent: TaskId, dependency: TaskId) -> bool {
        let optional_deps = self.optional_dependencies.read().await;
        optional_deps
            .get(&dependent)
            .map(|deps| deps.contains(&dependency))
            .unwrap_or(false)
    }

    /// Get all tasks that are ready to execute
    pub async fn get_ready_tasks(&self) -> Vec<TaskId> {
        let statuses = self.statuses.read().await;
        statuses
            .iter()
            .filter(|(_, status)| status.is_ready())
            .map(|(id, _)| *id)
            .collect()
    }

    /// Get all tasks in waiting status
    pub async fn get_waiting_tasks(&self) -> Vec<TaskId> {
        let statuses = self.statuses.read().await;
        statuses
            .iter()
            .filter(|(_, status)| status.is_waiting())
            .map(|(id, _)| *id)
            .collect()
    }

    /// Get all blocked tasks
    pub async fn get_blocked_tasks(&self) -> Vec<TaskId> {
        let statuses = self.statuses.read().await;
        statuses
            .iter()
            .filter(|(_, status)| status.is_blocked())
            .map(|(id, _)| *id)
            .collect()
    }

    /// Get topologically sorted task order
    pub async fn topological_sort(&self) -> Result<Vec<TaskId>> {
        // First, calculate in-degrees in two passes to avoid overwriting
        let dependencies = self.dependencies.read().await;
        let mut in_degree: HashMap<TaskId, usize> = HashMap::new();

        // Pass 1: Set in-degrees for all tasks that have dependencies
        for (task, deps) in dependencies.iter() {
            in_degree.insert(*task, deps.len());
        }

        // Pass 2: Ensure all dependency tasks are in the map (with 0 if not already set)
        for (_task, deps) in dependencies.iter() {
            for dep in deps {
                in_degree.entry(*dep).or_insert(0);
            }
        }
        drop(dependencies);

        // Add tasks with no dependencies to queue
        let mut queue = VecDeque::new();
        for (task, degree) in &in_degree {
            if *degree == 0 {
                queue.push_back(*task);
            }
        }

        // Process tasks
        let mut result = Vec::new();
        while let Some(task) = queue.pop_front() {
            result.push(task);

            // Get all tasks that depend on this task
            let dependents = self.dependents.read().await;
            let deps_vec: Vec<TaskId> = dependents
                .get(&task)
                .map(|deps| deps.iter().copied().collect())
                .unwrap_or_default();
            drop(dependents);

            for dependent in deps_vec {
                if let Some(degree) = in_degree.get_mut(&dependent) {
                    if *degree > 0 {
                        *degree -= 1;
                        if *degree == 0 {
                            queue.push_back(dependent);
                        }
                    }
                }
            }
        }

        // Check if all tasks were processed (no cycles)
        if result.len() != in_degree.len() {
            return Err(CelersError::Other(
                "Cycle detected in dependency graph".to_string(),
            ));
        }

        Ok(result)
    }

    /// Clear all dependencies for a task
    pub async fn clear_task(&self, task_id: TaskId) {
        // Remove from dependencies map
        let mut dependencies = self.dependencies.write().await;
        dependencies.remove(&task_id);
        // Also remove task_id from other tasks' dependency lists
        for deps in dependencies.values_mut() {
            deps.remove(&task_id);
        }
        drop(dependencies);

        // Remove from dependents map
        let mut dependents = self.dependents.write().await;
        dependents.remove(&task_id);
        // Also remove task_id from other tasks' dependent lists
        for deps in dependents.values_mut() {
            deps.remove(&task_id);
        }
        drop(dependents);

        // Remove from statuses
        let mut statuses = self.statuses.write().await;
        statuses.remove(&task_id);
        drop(statuses);

        // Remove from optional dependencies
        let mut optional_deps = self.optional_dependencies.write().await;
        optional_deps.remove(&task_id);
        for deps in optional_deps.values_mut() {
            deps.remove(&task_id);
        }

        info!("Cleared all dependencies for task {}", task_id);
    }

    /// Get statistics about the dependency graph
    pub async fn get_stats(&self) -> DependencyStats {
        let dependencies = self.dependencies.read().await;
        let statuses = self.statuses.read().await;

        let total_tasks = statuses.len();
        let total_dependencies: usize = dependencies.values().map(|deps| deps.len()).sum();

        let ready_count = statuses.values().filter(|s| s.is_ready()).count();
        let waiting_count = statuses.values().filter(|s| s.is_waiting()).count();
        let blocked_count = statuses.values().filter(|s| s.is_blocked()).count();
        let running_count = statuses.values().filter(|s| s.is_running()).count();
        let completed_count = statuses.values().filter(|s| s.is_completed()).count();
        let failed_count = statuses.values().filter(|s| s.is_failed()).count();

        DependencyStats {
            total_tasks,
            total_dependencies,
            ready_count,
            waiting_count,
            blocked_count,
            running_count,
            completed_count,
            failed_count,
        }
    }
}

impl Default for DependencyGraph {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about the dependency graph
#[derive(Debug, Clone, Default)]
pub struct DependencyStats {
    /// Total number of tasks in the graph
    pub total_tasks: usize,

    /// Total number of dependency relationships
    pub total_dependencies: usize,

    /// Number of ready tasks
    pub ready_count: usize,

    /// Number of waiting tasks
    pub waiting_count: usize,

    /// Number of blocked tasks
    pub blocked_count: usize,

    /// Number of running tasks
    pub running_count: usize,

    /// Number of completed tasks
    pub completed_count: usize,

    /// Number of failed tasks
    pub failed_count: usize,
}

impl DependencyStats {
    /// Check if all tasks are complete
    pub fn is_all_complete(&self) -> bool {
        self.total_tasks == self.completed_count + self.failed_count
    }

    /// Get the completion percentage
    pub fn completion_percentage(&self) -> f64 {
        if self.total_tasks == 0 {
            0.0
        } else {
            (self.completed_count as f64 / self.total_tasks as f64) * 100.0
        }
    }

    /// Get the failure rate
    pub fn failure_rate(&self) -> f64 {
        if self.total_tasks == 0 {
            0.0
        } else {
            (self.failed_count as f64 / self.total_tasks as f64) * 100.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_dependency_status_predicates() {
        assert!(DependencyStatus::Ready.is_ready());
        assert!(DependencyStatus::Waiting.is_waiting());
        assert!(DependencyStatus::Blocked.is_blocked());
        assert!(DependencyStatus::Running.is_running());
        assert!(DependencyStatus::Completed.is_completed());
        assert!(DependencyStatus::Failed.is_failed());

        assert!(DependencyStatus::Completed.is_terminal());
        assert!(DependencyStatus::Failed.is_terminal());
        assert!(!DependencyStatus::Ready.is_terminal());
    }

    #[test]
    fn test_dependency_status_display() {
        assert_eq!(DependencyStatus::Ready.to_string(), "Ready");
        assert_eq!(DependencyStatus::Waiting.to_string(), "Waiting");
        assert_eq!(DependencyStatus::Blocked.to_string(), "Blocked");
        assert_eq!(DependencyStatus::Running.to_string(), "Running");
        assert_eq!(DependencyStatus::Completed.to_string(), "Completed");
        assert_eq!(DependencyStatus::Failed.to_string(), "Failed");
    }

    #[test]
    fn test_task_dependency_creation() {
        let dep_task = uuid::Uuid::new_v4();
        let dependent_task = uuid::Uuid::new_v4();

        let dep = TaskDependency::new(dependent_task, dep_task);
        assert_eq!(dep.dependent_task, dependent_task);
        assert_eq!(dep.dependency_task, dep_task);
        assert!(!dep.optional);
        assert!(dep.is_required());
    }

    #[test]
    fn test_task_dependency_optional() {
        let dep_task = uuid::Uuid::new_v4();
        let dependent_task = uuid::Uuid::new_v4();

        let dep = TaskDependency::new(dependent_task, dep_task).optional();
        assert!(dep.optional);
        assert!(!dep.is_required());
    }

    #[tokio::test]
    async fn test_dependency_graph_add_dependency() {
        let graph = DependencyGraph::new();
        let task1 = uuid::Uuid::new_v4();
        let task2 = uuid::Uuid::new_v4();

        graph.add_dependency(task2, task1).await.unwrap();
        assert!(graph.has_dependency(task2, task1).await);
        assert!(!graph.has_dependency(task1, task2).await);
    }

    #[tokio::test]
    async fn test_dependency_graph_self_dependency() {
        let graph = DependencyGraph::new();
        let task = uuid::Uuid::new_v4();

        let result = graph.add_dependency(task, task).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_dependency_graph_circular_dependency() {
        let graph = DependencyGraph::new();
        let task1 = uuid::Uuid::new_v4();
        let task2 = uuid::Uuid::new_v4();
        let task3 = uuid::Uuid::new_v4();

        graph.add_dependency(task2, task1).await.unwrap();
        graph.add_dependency(task3, task2).await.unwrap();

        // This would create a cycle
        let result = graph.add_dependency(task1, task3).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_dependency_graph_get_dependencies() {
        let graph = DependencyGraph::new();
        let task1 = uuid::Uuid::new_v4();
        let task2 = uuid::Uuid::new_v4();
        let task3 = uuid::Uuid::new_v4();

        graph.add_dependency(task3, task1).await.unwrap();
        graph.add_dependency(task3, task2).await.unwrap();

        let deps = graph.get_dependencies(task3).await;
        assert_eq!(deps.len(), 2);
        assert!(deps.contains(&task1));
        assert!(deps.contains(&task2));
    }

    #[tokio::test]
    async fn test_dependency_graph_get_dependents() {
        let graph = DependencyGraph::new();
        let task1 = uuid::Uuid::new_v4();
        let task2 = uuid::Uuid::new_v4();
        let task3 = uuid::Uuid::new_v4();

        graph.add_dependency(task2, task1).await.unwrap();
        graph.add_dependency(task3, task1).await.unwrap();

        let dependents = graph.get_dependents(task1).await;
        assert_eq!(dependents.len(), 2);
        assert!(dependents.contains(&task2));
        assert!(dependents.contains(&task3));
    }

    #[tokio::test]
    async fn test_dependency_graph_status_update() {
        let graph = DependencyGraph::new();
        let task1 = uuid::Uuid::new_v4();
        let task2 = uuid::Uuid::new_v4();

        graph.add_dependency(task2, task1).await.unwrap();

        // Initially, tasks should be in Waiting status
        assert_eq!(
            graph.get_status(task1).await,
            Some(DependencyStatus::Waiting)
        );
        assert_eq!(
            graph.get_status(task2).await,
            Some(DependencyStatus::Waiting)
        );

        // Mark task1 as completed and update dependents
        graph.set_status(task1, DependencyStatus::Completed).await;
        graph.update_dependent_statuses(task1).await;

        // task2 should now be ready
        assert_eq!(graph.get_status(task2).await, Some(DependencyStatus::Ready));
    }

    #[tokio::test]
    async fn test_dependency_graph_optional_dependency() {
        let graph = DependencyGraph::new();
        let task1 = uuid::Uuid::new_v4();
        let task2 = uuid::Uuid::new_v4();

        graph.add_optional_dependency(task2, task1).await.unwrap();

        // Mark task1 as failed and update dependents
        graph.set_status(task1, DependencyStatus::Failed).await;
        graph.update_dependent_statuses(task1).await;

        // task2 should still be ready because the dependency is optional
        assert_eq!(graph.get_status(task2).await, Some(DependencyStatus::Ready));
    }

    #[tokio::test]
    async fn test_dependency_graph_topological_sort() {
        let graph = DependencyGraph::new();
        let task1 = uuid::Uuid::new_v4();
        let task2 = uuid::Uuid::new_v4();
        let task3 = uuid::Uuid::new_v4();

        graph.add_dependency(task2, task1).await.unwrap();
        graph.add_dependency(task3, task2).await.unwrap();

        let sorted = graph.topological_sort().await.unwrap();
        assert_eq!(sorted.len(), 3);

        // task1 should come before task2, and task2 before task3
        let pos1 = sorted.iter().position(|&t| t == task1).unwrap();
        let pos2 = sorted.iter().position(|&t| t == task2).unwrap();
        let pos3 = sorted.iter().position(|&t| t == task3).unwrap();

        assert!(pos1 < pos2);
        assert!(pos2 < pos3);
    }

    #[tokio::test]
    async fn test_dependency_graph_clear_task() {
        let graph = DependencyGraph::new();
        let task1 = uuid::Uuid::new_v4();
        let task2 = uuid::Uuid::new_v4();

        graph.add_dependency(task2, task1).await.unwrap();
        graph.set_status(task1, DependencyStatus::Running).await;

        graph.clear_task(task1).await;

        assert!(!graph.has_dependency(task2, task1).await);
        assert_eq!(graph.get_status(task1).await, None);
    }

    #[tokio::test]
    async fn test_dependency_stats() {
        let graph = DependencyGraph::new();
        let task1 = uuid::Uuid::new_v4();
        let task2 = uuid::Uuid::new_v4();
        let task3 = uuid::Uuid::new_v4();

        graph.add_dependency(task2, task1).await.unwrap();
        graph.add_dependency(task3, task2).await.unwrap();

        graph.set_status(task1, DependencyStatus::Completed).await;

        let stats = graph.get_stats().await;
        assert_eq!(stats.total_tasks, 3);
        assert_eq!(stats.total_dependencies, 2);
        assert_eq!(stats.completed_count, 1);
    }

    #[test]
    fn test_dependency_stats_predicates() {
        let mut stats = DependencyStats {
            total_tasks: 10,
            completed_count: 5,
            failed_count: 2,
            ..Default::default()
        };

        assert!(!stats.is_all_complete());
        assert_eq!(stats.completion_percentage(), 50.0);
        assert_eq!(stats.failure_rate(), 20.0);

        stats.completed_count = 7;
        stats.failed_count = 3;
        assert!(stats.is_all_complete());
    }
}
