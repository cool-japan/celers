use std::collections::{HashMap, HashSet};

/// Task dependency graph
pub struct DependencyGraph {
    /// Dependencies: task_id -> set of task_ids it depends on
    dependencies: HashMap<String, HashSet<String>>,
    /// Reverse dependencies: task_id -> set of task_ids that depend on it
    dependents: HashMap<String, HashSet<String>>,
}

impl DependencyGraph {
    /// Creates a new dependency graph
    pub fn new() -> Self {
        Self {
            dependencies: HashMap::new(),
            dependents: HashMap::new(),
        }
    }

    /// Adds a task to the graph
    pub fn add_task(&mut self, task_id: impl Into<String>) {
        let task_id = task_id.into();
        self.dependencies.entry(task_id.clone()).or_default();
        self.dependents.entry(task_id).or_default();
    }

    /// Adds a dependency: task depends on dependency
    pub fn add_dependency(&mut self, task_id: impl Into<String>, dependency_id: impl Into<String>) {
        let task_id = task_id.into();
        let dependency_id = dependency_id.into();

        self.dependencies
            .entry(task_id.clone())
            .or_default()
            .insert(dependency_id.clone());

        self.dependents
            .entry(dependency_id)
            .or_default()
            .insert(task_id);
    }

    /// Gets direct dependencies of a task
    pub fn get_dependencies(&self, task_id: &str) -> Vec<String> {
        self.dependencies
            .get(task_id)
            .map(|deps| deps.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Gets all tasks that depend on this task
    pub fn get_dependents(&self, task_id: &str) -> Vec<String> {
        self.dependents
            .get(task_id)
            .map(|deps| deps.iter().cloned().collect())
            .unwrap_or_default()
    }

    /// Checks if there are circular dependencies
    pub fn has_circular_dependencies(&self) -> bool {
        let mut visited = HashSet::new();
        let mut rec_stack = HashSet::new();

        for task_id in self.dependencies.keys() {
            if self.has_cycle(task_id, &mut visited, &mut rec_stack) {
                return true;
            }
        }
        false
    }

    fn has_cycle(
        &self,
        task_id: &str,
        visited: &mut HashSet<String>,
        rec_stack: &mut HashSet<String>,
    ) -> bool {
        if rec_stack.contains(task_id) {
            return true;
        }
        if visited.contains(task_id) {
            return false;
        }

        visited.insert(task_id.to_string());
        rec_stack.insert(task_id.to_string());

        if let Some(deps) = self.dependencies.get(task_id) {
            for dep in deps {
                if self.has_cycle(dep, visited, rec_stack) {
                    return true;
                }
            }
        }

        rec_stack.remove(task_id);
        false
    }

    /// Gets tasks in topological order (tasks with no dependencies first)
    pub fn topological_sort(&self) -> Result<Vec<String>, String> {
        if self.has_circular_dependencies() {
            return Err("Circular dependencies detected".to_string());
        }

        let mut result = Vec::new();
        let mut visited = HashSet::new();
        let mut temp_mark = HashSet::new();

        for task_id in self.dependencies.keys() {
            if !visited.contains(task_id) {
                self.visit(task_id, &mut visited, &mut temp_mark, &mut result)?;
            }
        }

        Ok(result)
    }

    fn visit(
        &self,
        task_id: &str,
        visited: &mut HashSet<String>,
        temp_mark: &mut HashSet<String>,
        result: &mut Vec<String>,
    ) -> Result<(), String> {
        if temp_mark.contains(task_id) {
            return Err("Circular dependency detected".to_string());
        }
        if visited.contains(task_id) {
            return Ok(());
        }

        temp_mark.insert(task_id.to_string());

        if let Some(deps) = self.dependencies.get(task_id) {
            for dep in deps {
                self.visit(dep, visited, temp_mark, result)?;
            }
        }

        temp_mark.remove(task_id);
        visited.insert(task_id.to_string());
        result.push(task_id.to_string());

        Ok(())
    }

    /// Gets tasks that are ready to execute (all dependencies satisfied)
    pub fn get_ready_tasks(&self, completed_tasks: &HashSet<String>) -> Vec<String> {
        self.dependencies
            .iter()
            .filter(|(task_id, deps)| {
                !completed_tasks.contains(*task_id)
                    && deps.iter().all(|dep| completed_tasks.contains(dep))
            })
            .map(|(task_id, _)| task_id.clone())
            .collect()
    }
}

impl Default for DependencyGraph {
    fn default() -> Self {
        Self::new()
    }
}
