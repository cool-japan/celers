//! Workflow and task chain utilities
//!
//! This module provides helpers for building and managing task workflows,
//! chains, and directed acyclic graphs (DAGs) of tasks.

use crate::builder::{BuilderResult, MessageBuilder};
use crate::Message;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use uuid::Uuid;

/// Header key under which the full list of upstream dependency task IDs is
/// recorded, in addition to the single `parent_id` header.
///
/// The Celery message envelope has room for only one `parent_id`, so when a
/// task depends on more than one predecessor (a fan-in / join), the first
/// dependency is still used as `parent_id` (for ordinary chain-style
/// consumers), but the *complete* dependency list is mirrored here so
/// fan-in information is never silently dropped.
pub const DEPENDENCIES_HEADER: &str = "dependencies";

/// A task in a workflow
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowTask {
    /// Unique task identifier
    pub id: Uuid,
    /// Task name
    pub task_name: String,
    /// Task arguments (JSON)
    pub args: Vec<serde_json::Value>,
    /// Task keyword arguments (JSON)
    pub kwargs: HashMap<String, serde_json::Value>,
    /// Dependencies (task IDs that must complete first)
    pub dependencies: Vec<Uuid>,
}

impl WorkflowTask {
    /// Create a new workflow task
    pub fn new(task_name: impl Into<String>) -> Self {
        Self {
            id: Uuid::new_v4(),
            task_name: task_name.into(),
            args: Vec::new(),
            kwargs: HashMap::new(),
            dependencies: Vec::new(),
        }
    }

    /// Set task arguments
    #[must_use]
    pub fn with_args(mut self, args: Vec<serde_json::Value>) -> Self {
        self.args = args;
        self
    }

    /// Set task keyword arguments
    #[must_use]
    pub fn with_kwargs(mut self, kwargs: HashMap<String, serde_json::Value>) -> Self {
        self.kwargs = kwargs;
        self
    }

    /// Add a dependency on another task
    #[must_use]
    pub fn depends_on(mut self, task_id: Uuid) -> Self {
        self.dependencies.push(task_id);
        self
    }

    /// Add multiple dependencies
    #[must_use]
    pub fn depends_on_many(mut self, task_ids: Vec<Uuid>) -> Self {
        self.dependencies.extend(task_ids);
        self
    }

    /// Convert to a Message with workflow metadata
    ///
    /// # Errors
    ///
    /// Returns [`crate::builder::BuilderError`] if the underlying
    /// [`MessageBuilder`] fails to build the message (for example, if body
    /// serialization fails).
    pub fn to_message(
        &self,
        root_id: Option<Uuid>,
        parent_id: Option<Uuid>,
    ) -> BuilderResult<Message> {
        let mut builder = MessageBuilder::new(&self.task_name)
            .id(self.id)
            .args(self.args.clone())
            .kwargs(self.kwargs.clone());

        if let Some(root) = root_id {
            builder = builder.root(root);
        }

        if let Some(parent) = parent_id {
            builder = builder.parent(parent);
        }

        // A task with more than one dependency loses all but the first once
        // it is squeezed into the single `parent_id` header; mirror the full
        // list into a dedicated header so fan-in information survives.
        if !self.dependencies.is_empty() {
            let deps: Vec<serde_json::Value> = self
                .dependencies
                .iter()
                .map(|id| serde_json::Value::String(id.to_string()))
                .collect();
            builder = builder.header(DEPENDENCIES_HEADER, serde_json::Value::Array(deps));
        }

        builder.build()
    }
}

/// A workflow of tasks with dependencies
#[derive(Debug, Clone)]
pub struct Workflow {
    /// All tasks in the workflow
    tasks: HashMap<Uuid, WorkflowTask>,
    /// Root task ID (entry point)
    root_id: Option<Uuid>,
    /// Workflow name
    name: String,
}

impl Workflow {
    /// Create a new workflow
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            tasks: HashMap::new(),
            root_id: None,
            name: name.into(),
        }
    }

    /// Add a task to the workflow
    pub fn add_task(&mut self, task: WorkflowTask) -> Uuid {
        let id = task.id;
        if self.root_id.is_none() && task.dependencies.is_empty() {
            self.root_id = Some(id);
        }
        self.tasks.insert(id, task);
        id
    }

    /// Get a task by ID
    pub fn get_task(&self, id: &Uuid) -> Option<&WorkflowTask> {
        self.tasks.get(id)
    }

    /// Set the root task
    pub fn set_root(&mut self, task_id: Uuid) {
        if self.tasks.contains_key(&task_id) {
            self.root_id = Some(task_id);
        }
    }

    /// Get all tasks with no dependencies (entry points)
    pub fn get_entry_tasks(&self) -> Vec<&WorkflowTask> {
        self.tasks
            .values()
            .filter(|task| task.dependencies.is_empty())
            .collect()
    }

    /// Get tasks that depend on a specific task
    pub fn get_dependent_tasks(&self, task_id: &Uuid) -> Vec<&WorkflowTask> {
        self.tasks
            .values()
            .filter(|task| task.dependencies.contains(task_id))
            .collect()
    }

    /// Check if the workflow has cycles (invalid)
    pub fn has_cycles(&self) -> bool {
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();

        for task_id in self.tasks.keys() {
            if self.has_cycle_dfs(task_id, &mut visited, &mut rec_stack) {
                return true;
            }
        }

        false
    }

    fn has_cycle_dfs(
        &self,
        task_id: &Uuid,
        visited: &mut HashSet<Uuid>,
        rec_stack: &mut HashSet<Uuid>,
    ) -> bool {
        if rec_stack.contains(task_id) {
            return true;
        }

        if visited.contains(task_id) {
            return false;
        }

        visited.insert(*task_id);
        rec_stack.insert(*task_id);

        if let Some(task) = self.tasks.get(task_id) {
            for dep_id in &task.dependencies {
                if self.has_cycle_dfs(dep_id, visited, rec_stack) {
                    return true;
                }
            }
        }

        rec_stack.remove(task_id);
        false
    }

    /// Get tasks in topological order (execution order)
    pub fn topological_sort(&self) -> Result<Vec<Uuid>, String> {
        if self.has_cycles() {
            return Err("Workflow contains cycles".to_string());
        }

        let mut in_degree: HashMap<Uuid, usize> = HashMap::new();
        let mut adj_list: HashMap<Uuid, Vec<Uuid>> = HashMap::new();

        // Initialize all tasks
        for id in self.tasks.keys() {
            in_degree.insert(*id, 0);
            adj_list.insert(*id, Vec::new());
        }

        // Build adjacency list and count in-degrees
        for (id, task) in &self.tasks {
            for &dep_id in &task.dependencies {
                // dep_id -> id (dep_id points to id)
                adj_list.entry(dep_id).or_default().push(*id);
                *in_degree.entry(*id).or_insert(0) += 1;
            }
        }

        // Find all tasks with no dependencies
        let mut queue: VecDeque<Uuid> = in_degree
            .iter()
            .filter(|(_, &degree)| degree == 0)
            .map(|(&id, _)| id)
            .collect();

        let mut sorted = Vec::new();

        while let Some(task_id) = queue.pop_front() {
            sorted.push(task_id);

            // For all tasks that depend on this task
            if let Some(dependents) = adj_list.get(&task_id) {
                for &dependent_id in dependents {
                    if let Some(degree) = in_degree.get_mut(&dependent_id) {
                        *degree -= 1;
                        if *degree == 0 {
                            queue.push_back(dependent_id);
                        }
                    }
                }
            }
        }

        if sorted.len() != self.tasks.len() {
            Err("Could not complete topological sort".to_string())
        } else {
            Ok(sorted)
        }
    }

    /// Convert workflow to messages in execution order
    pub fn to_messages(&self) -> Result<Vec<Message>, String> {
        let order = self.topological_sort()?;
        if order.is_empty() {
            // An empty workflow has no root task; indexing `order[0]` below
            // would otherwise panic.
            return Ok(Vec::new());
        }
        let root_id = self.root_id.unwrap_or_else(|| order[0]);

        order
            .into_iter()
            .filter_map(|task_id| self.tasks.get(&task_id))
            .map(|task| {
                let parent_id = task.dependencies.first().copied();
                task.to_message(Some(root_id), parent_id).map_err(|e| {
                    format!(
                        "Failed to build message for task '{}': {}",
                        task.task_name, e
                    )
                })
            })
            .collect()
    }

    /// Get the workflow name
    #[inline]
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the number of tasks in the workflow
    #[inline]
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    /// Check if the workflow is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }
}

/// Builder for creating task chains (linear workflows)
#[derive(Debug, Clone)]
pub struct ChainBuilder {
    tasks: Vec<WorkflowTask>,
    name: String,
}

impl ChainBuilder {
    /// Create a new chain builder
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            tasks: Vec::new(),
            name: name.into(),
        }
    }

    /// Add a task to the chain
    #[must_use]
    pub fn then(mut self, task_name: impl Into<String>) -> Self {
        let task = WorkflowTask::new(task_name);
        self.tasks.push(task);
        self
    }

    /// Add a task with arguments
    #[must_use]
    pub fn then_with_args(
        mut self,
        task_name: impl Into<String>,
        args: Vec<serde_json::Value>,
    ) -> Self {
        let task = WorkflowTask::new(task_name).with_args(args);
        self.tasks.push(task);
        self
    }

    /// Build the chain as a workflow
    pub fn build(self) -> Workflow {
        let mut workflow = Workflow::new(self.name);

        let mut prev_id: Option<Uuid> = None;

        for mut task in self.tasks {
            if let Some(prev) = prev_id {
                task = task.depends_on(prev);
            }
            prev_id = Some(task.id);
            workflow.add_task(task);
        }

        workflow
    }

    /// Build and convert to messages
    pub fn build_messages(self) -> Result<Vec<Message>, String> {
        self.build().to_messages()
    }
}

/// Group multiple tasks to run in parallel
#[derive(Debug, Clone)]
pub struct Group {
    tasks: Vec<WorkflowTask>,
    group_id: Uuid,
}

impl Group {
    /// Create a new task group
    pub fn new() -> Self {
        Self {
            tasks: Vec::new(),
            group_id: Uuid::new_v4(),
        }
    }

    /// Add a task to the group
    #[must_use]
    pub fn with_task(mut self, task: WorkflowTask) -> Self {
        self.tasks.push(task);
        self
    }

    /// Add a simple task by name
    #[must_use]
    pub fn add_task(mut self, task_name: impl Into<String>) -> Self {
        self.tasks.push(WorkflowTask::new(task_name));
        self
    }

    /// Convert to messages with group ID
    ///
    /// # Errors
    ///
    /// Returns an error message if any task fails to build (see
    /// [`WorkflowTask::to_message`]).
    pub fn to_messages(&self) -> Result<Vec<Message>, String> {
        self.tasks
            .iter()
            .map(|task| {
                let mut msg = task.to_message(None, None).map_err(|e| {
                    format!(
                        "Failed to build message for task '{}': {}",
                        task.task_name, e
                    )
                })?;
                msg.headers.group = Some(self.group_id);
                Ok(msg)
            })
            .collect()
    }

    /// Get the group ID
    #[inline]
    pub fn id(&self) -> Uuid {
        self.group_id
    }

    /// Get the number of tasks in the group
    #[inline]
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    /// Check if the group is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }
}

impl Default for Group {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workflow_task_creation() {
        let task = WorkflowTask::new("tasks.add")
            .with_args(vec![serde_json::json!(1), serde_json::json!(2)]);

        assert_eq!(task.task_name, "tasks.add");
        assert_eq!(task.args.len(), 2);
        assert!(task.dependencies.is_empty());
    }

    #[test]
    fn test_workflow_task_dependencies() {
        let task1_id = Uuid::new_v4();
        let task2_id = Uuid::new_v4();

        let task = WorkflowTask::new("task3")
            .depends_on(task1_id)
            .depends_on(task2_id);

        assert_eq!(task.dependencies.len(), 2);
        assert!(task.dependencies.contains(&task1_id));
        assert!(task.dependencies.contains(&task2_id));
    }

    #[test]
    fn test_workflow_add_task() {
        let mut workflow = Workflow::new("test_workflow");
        let task = WorkflowTask::new("tasks.test");
        let task_id = workflow.add_task(task);

        assert_eq!(workflow.len(), 1);
        assert!(workflow.get_task(&task_id).is_some());
    }

    #[test]
    fn test_workflow_entry_tasks() {
        let mut workflow = Workflow::new("test");

        let task1 = WorkflowTask::new("task1");
        let task2 = WorkflowTask::new("task2");
        let task1_id = task1.id;

        workflow.add_task(task1);
        workflow.add_task(task2);

        let task3 = WorkflowTask::new("task3").depends_on(task1_id);
        workflow.add_task(task3);

        let entry_tasks = workflow.get_entry_tasks();
        assert_eq!(entry_tasks.len(), 2); // task1 and task2 have no dependencies
    }

    #[test]
    fn test_workflow_dependent_tasks() {
        let mut workflow = Workflow::new("test");

        let task1 = WorkflowTask::new("task1");
        let task1_id = task1.id;
        workflow.add_task(task1);

        let task2 = WorkflowTask::new("task2").depends_on(task1_id);
        let task3 = WorkflowTask::new("task3").depends_on(task1_id);

        workflow.add_task(task2);
        workflow.add_task(task3);

        let dependents = workflow.get_dependent_tasks(&task1_id);
        assert_eq!(dependents.len(), 2);
    }

    #[test]
    fn test_workflow_no_cycles() {
        let mut workflow = Workflow::new("test");

        let task1 = WorkflowTask::new("task1");
        let task1_id = task1.id;
        workflow.add_task(task1);

        let task2 = WorkflowTask::new("task2").depends_on(task1_id);
        workflow.add_task(task2);

        assert!(!workflow.has_cycles());
    }

    #[test]
    fn test_workflow_topological_sort() {
        let mut workflow = Workflow::new("test");

        let task1 = WorkflowTask::new("task1");
        let task1_id = task1.id;
        workflow.add_task(task1);

        let task2 = WorkflowTask::new("task2").depends_on(task1_id);
        let task2_id = task2.id;
        workflow.add_task(task2);

        let task3 = WorkflowTask::new("task3").depends_on(task2_id);
        workflow.add_task(task3);

        let sorted = workflow.topological_sort().unwrap();
        assert_eq!(sorted.len(), 3);
        assert_eq!(sorted[0], task1_id);
        assert_eq!(sorted[1], task2_id);
    }

    #[test]
    fn test_workflow_to_messages() {
        let mut workflow = Workflow::new("test");

        let task1 = WorkflowTask::new("task1");
        let task1_id = task1.id;
        workflow.add_task(task1);

        let task2 = WorkflowTask::new("task2").depends_on(task1_id);
        workflow.add_task(task2);

        let messages = workflow.to_messages().unwrap();
        assert_eq!(messages.len(), 2);
    }

    #[test]
    fn test_chain_builder() {
        let chain = ChainBuilder::new("my_chain")
            .then("task1")
            .then("task2")
            .then("task3")
            .build();

        assert_eq!(chain.len(), 3);

        let sorted = chain.topological_sort().unwrap();
        assert_eq!(sorted.len(), 3);
    }

    #[test]
    fn test_chain_builder_with_args() {
        let chain = ChainBuilder::new("my_chain")
            .then_with_args("task1", vec![serde_json::json!(42)])
            .then("task2")
            .build();

        assert_eq!(chain.len(), 2);
    }

    #[test]
    fn test_chain_to_messages() {
        let messages = ChainBuilder::new("my_chain")
            .then("task1")
            .then("task2")
            .build_messages()
            .unwrap();

        assert_eq!(messages.len(), 2);
        assert!(messages[0].has_root());
    }

    #[test]
    fn test_group_creation() {
        let group = Group::new()
            .add_task("task1")
            .add_task("task2")
            .add_task("task3");

        assert_eq!(group.len(), 3);
    }

    #[test]
    fn test_group_to_messages() {
        let group = Group::new().add_task("task1").add_task("task2");

        let messages = group.to_messages().unwrap();
        assert_eq!(messages.len(), 2);

        // All messages should have the same group ID
        let group_id = messages[0].headers.group.unwrap();
        assert_eq!(messages[1].headers.group.unwrap(), group_id);
    }

    #[test]
    fn test_workflow_task_to_message() {
        let task = WorkflowTask::new("tasks.test").with_args(vec![serde_json::json!(1)]);

        let root_id = Uuid::new_v4();
        let parent_id = Uuid::new_v4();

        let message = task.to_message(Some(root_id), Some(parent_id)).unwrap();

        assert_eq!(message.headers.task, "tasks.test");
        assert_eq!(message.headers.root_id, Some(root_id));
        assert_eq!(message.headers.parent_id, Some(parent_id));
    }

    #[test]
    fn test_empty_workflow_to_messages_does_not_panic() {
        // Regression: an empty workflow used to panic (`order[0]` on an
        // empty Vec) instead of returning an empty message list.
        let workflow = Workflow::new("empty");
        assert!(workflow.is_empty());

        let messages = workflow.to_messages().unwrap();
        assert!(messages.is_empty());
    }

    #[test]
    fn test_workflow_multi_dependency_keeps_first_as_parent_and_records_all_in_header() {
        let mut workflow = Workflow::new("fan-in");

        let task1 = WorkflowTask::new("task1");
        let task1_id = task1.id;
        let task2 = WorkflowTask::new("task2");
        let task2_id = task2.id;
        let task3 = WorkflowTask::new("task3");
        let task3_id = task3.id;
        workflow.add_task(task1);
        workflow.add_task(task2);
        workflow.add_task(task3);

        // task4 fans in from all three predecessors.
        let task4 = WorkflowTask::new("task4").depends_on_many(vec![task1_id, task2_id, task3_id]);
        let task4_id = task4.id;
        workflow.add_task(task4);

        let messages = workflow.to_messages().unwrap();
        let msg4 = messages
            .iter()
            .find(|m| m.headers.id == task4_id)
            .expect("task4 message should be present");

        // parent_id keeps only the first dependency...
        assert_eq!(msg4.headers.parent_id, Some(task1_id));

        // ...but the dedicated header preserves every dependency, so nothing
        // is silently lost.
        let deps = msg4
            .headers
            .extra
            .get(DEPENDENCIES_HEADER)
            .and_then(|v| v.as_array())
            .expect("dependencies header should be a JSON array");
        let dep_ids: Vec<Uuid> = deps
            .iter()
            .map(|v| v.as_str().unwrap().parse().unwrap())
            .collect();
        assert_eq!(
            dep_ids,
            vec![task1_id, task2_id, task3_id],
            "dependencies header must list every dependency in order"
        );
    }

    #[test]
    fn test_workflow_task_without_dependencies_omits_dependencies_header() {
        let task = WorkflowTask::new("tasks.root");
        let message = task.to_message(None, None).unwrap();
        assert!(!message.headers.extra.contains_key(DEPENDENCIES_HEADER));
    }
}
