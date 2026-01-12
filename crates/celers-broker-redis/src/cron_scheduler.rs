//! Cron-based task scheduling for recurring tasks
//!
//! This module provides cron-like scheduling for tasks that need to run
//! on a regular schedule.
//!
//! # Example
//!
//! ```rust,no_run
//! use celers_broker_redis::cron_scheduler::{CronScheduler, CronExpression};
//! use celers_core::SerializedTask;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let scheduler = CronScheduler::new();
//!
//!     // Schedule a task to run every hour
//!     let task = SerializedTask::new("cleanup".to_string(), vec![]);
//!     scheduler.schedule("cleanup_job", "0 * * * *", task)?;
//!
//!     // Get tasks that are due to run
//!     let due_tasks = scheduler.get_due_tasks()?;
//!
//!     Ok(())
//! }
//! ```

use celers_core::{CelersError, Result, SerializedTask};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// A cron expression for scheduling
#[derive(Debug, Clone)]
pub struct CronExpression {
    /// The cron expression string (e.g., "0 * * * *" for every hour)
    expression: String,
}

impl CronExpression {
    /// Create a new cron expression
    pub fn new(expression: impl Into<String>) -> Result<Self> {
        let expression = expression.into();
        Self::validate(&expression)?;
        Ok(Self { expression })
    }

    /// Validate a cron expression format
    fn validate(expr: &str) -> Result<()> {
        let parts: Vec<&str> = expr.split_whitespace().collect();
        if parts.len() != 5 {
            return Err(CelersError::Other(format!(
                "Invalid cron expression: expected 5 fields, got {}",
                parts.len()
            )));
        }
        Ok(())
    }

    /// Get the expression string
    pub fn as_str(&self) -> &str {
        &self.expression
    }

    /// Parse common cron expressions
    pub fn every_minute() -> Self {
        Self {
            expression: "* * * * *".to_string(),
        }
    }

    /// Every hour at minute 0
    pub fn hourly() -> Self {
        Self {
            expression: "0 * * * *".to_string(),
        }
    }

    /// Every day at midnight
    pub fn daily() -> Self {
        Self {
            expression: "0 0 * * *".to_string(),
        }
    }

    /// Every week on Sunday at midnight
    pub fn weekly() -> Self {
        Self {
            expression: "0 0 * * 0".to_string(),
        }
    }

    /// Every month on the 1st at midnight
    pub fn monthly() -> Self {
        Self {
            expression: "0 0 1 * *".to_string(),
        }
    }
}

/// A scheduled task entry
#[derive(Debug, Clone)]
pub struct ScheduledTask {
    /// Unique identifier for this scheduled task
    pub id: String,
    /// The cron expression
    pub cron: CronExpression,
    /// The task template to execute
    pub task_template: SerializedTask,
    /// Last execution time (Unix timestamp)
    pub last_run: Option<i64>,
    /// Next scheduled execution time (Unix timestamp)
    pub next_run: i64,
    /// Whether the task is enabled
    pub enabled: bool,
}

impl ScheduledTask {
    /// Create a new scheduled task
    pub fn new(id: String, cron: CronExpression, task_template: SerializedTask) -> Self {
        let next_run = Self::calculate_next_run(&cron, None);
        Self {
            id,
            cron,
            task_template,
            last_run: None,
            next_run,
            enabled: true,
        }
    }

    /// Calculate the next run time based on the cron expression
    fn calculate_next_run(cron: &CronExpression, from: Option<i64>) -> i64 {
        let now = from.unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64
        });

        // Simplified calculation: for now, just add a fixed interval based on the pattern
        // In a real implementation, you would use a cron parser library
        let interval = match cron.as_str() {
            "* * * * *" => 60,      // every minute
            "0 * * * *" => 3600,    // every hour
            "0 0 * * *" => 86400,   // daily
            "0 0 * * 0" => 604800,  // weekly
            "0 0 1 * *" => 2592000, // monthly (approx)
            _ => 3600,              // default to hourly
        };

        now + interval
    }

    /// Update the next run time after execution
    pub fn update_after_run(&mut self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        self.last_run = Some(now);
        self.next_run = Self::calculate_next_run(&self.cron, Some(now));
    }

    /// Check if this task is due to run
    pub fn is_due(&self) -> bool {
        if !self.enabled {
            return false;
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;

        now >= self.next_run
    }
}

/// Cron-based task scheduler
pub struct CronScheduler {
    tasks: Arc<RwLock<HashMap<String, ScheduledTask>>>,
}

impl CronScheduler {
    /// Create a new cron scheduler
    pub fn new() -> Self {
        Self {
            tasks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Schedule a new task
    pub fn schedule(
        &self,
        id: impl Into<String>,
        cron_expr: impl Into<String>,
        task_template: SerializedTask,
    ) -> Result<()> {
        let id = id.into();
        let cron = CronExpression::new(cron_expr)?;
        let scheduled_task = ScheduledTask::new(id.clone(), cron, task_template);

        let mut tasks = self
            .tasks
            .write()
            .map_err(|e| CelersError::Other(format!("Failed to acquire write lock: {}", e)))?;

        tasks.insert(id, scheduled_task);
        Ok(())
    }

    /// Remove a scheduled task
    pub fn unschedule(&self, id: &str) -> Result<bool> {
        let mut tasks = self
            .tasks
            .write()
            .map_err(|e| CelersError::Other(format!("Failed to acquire write lock: {}", e)))?;

        Ok(tasks.remove(id).is_some())
    }

    /// Enable a scheduled task
    pub fn enable(&self, id: &str) -> Result<()> {
        let mut tasks = self
            .tasks
            .write()
            .map_err(|e| CelersError::Other(format!("Failed to acquire write lock: {}", e)))?;

        if let Some(task) = tasks.get_mut(id) {
            task.enabled = true;
            Ok(())
        } else {
            Err(CelersError::Other(format!("Task not found: {}", id)))
        }
    }

    /// Disable a scheduled task
    pub fn disable(&self, id: &str) -> Result<()> {
        let mut tasks = self
            .tasks
            .write()
            .map_err(|e| CelersError::Other(format!("Failed to acquire write lock: {}", e)))?;

        if let Some(task) = tasks.get_mut(id) {
            task.enabled = false;
            Ok(())
        } else {
            Err(CelersError::Other(format!("Task not found: {}", id)))
        }
    }

    /// Get all tasks that are due to run
    pub fn get_due_tasks(&self) -> Result<Vec<SerializedTask>> {
        let mut tasks = self
            .tasks
            .write()
            .map_err(|e| CelersError::Other(format!("Failed to acquire write lock: {}", e)))?;

        let mut due_tasks = Vec::new();

        for task in tasks.values_mut() {
            if task.is_due() {
                due_tasks.push(task.task_template.clone());
                task.update_after_run();
            }
        }

        Ok(due_tasks)
    }

    /// Get all scheduled tasks
    pub fn list_all(&self) -> Result<Vec<ScheduledTask>> {
        let tasks = self
            .tasks
            .read()
            .map_err(|e| CelersError::Other(format!("Failed to acquire read lock: {}", e)))?;

        Ok(tasks.values().cloned().collect())
    }

    /// Get a specific scheduled task
    pub fn get(&self, id: &str) -> Result<Option<ScheduledTask>> {
        let tasks = self
            .tasks
            .read()
            .map_err(|e| CelersError::Other(format!("Failed to acquire read lock: {}", e)))?;

        Ok(tasks.get(id).cloned())
    }

    /// Get the number of scheduled tasks
    pub fn count(&self) -> Result<usize> {
        let tasks = self
            .tasks
            .read()
            .map_err(|e| CelersError::Other(format!("Failed to acquire read lock: {}", e)))?;

        Ok(tasks.len())
    }

    /// Clear all scheduled tasks
    pub fn clear(&self) -> Result<()> {
        let mut tasks = self
            .tasks
            .write()
            .map_err(|e| CelersError::Other(format!("Failed to acquire write lock: {}", e)))?;

        tasks.clear();
        Ok(())
    }
}

impl Default for CronScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for CronScheduler {
    fn clone(&self) -> Self {
        Self {
            tasks: Arc::clone(&self.tasks),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use celers_core::TaskMetadata;

    fn create_test_task() -> SerializedTask {
        SerializedTask {
            metadata: TaskMetadata::new("test_task".to_string()),
            payload: vec![],
        }
    }

    #[test]
    fn test_cron_expression_validation() {
        assert!(CronExpression::new("* * * * *").is_ok());
        assert!(CronExpression::new("0 * * * *").is_ok());
        assert!(CronExpression::new("invalid").is_err());
        assert!(CronExpression::new("* * *").is_err());
    }

    #[test]
    fn test_cron_expression_presets() {
        assert_eq!(CronExpression::every_minute().as_str(), "* * * * *");
        assert_eq!(CronExpression::hourly().as_str(), "0 * * * *");
        assert_eq!(CronExpression::daily().as_str(), "0 0 * * *");
        assert_eq!(CronExpression::weekly().as_str(), "0 0 * * 0");
        assert_eq!(CronExpression::monthly().as_str(), "0 0 1 * *");
    }

    #[test]
    fn test_scheduled_task_creation() {
        let cron = CronExpression::hourly();
        let task = create_test_task();
        let scheduled = ScheduledTask::new("test".to_string(), cron, task);

        assert_eq!(scheduled.id, "test");
        assert!(scheduled.enabled);
        assert!(scheduled.last_run.is_none());
        assert!(scheduled.next_run > 0);
    }

    #[test]
    fn test_cron_scheduler_schedule() {
        let scheduler = CronScheduler::new();
        let task = create_test_task();

        assert!(scheduler.schedule("job1", "* * * * *", task).is_ok());
        assert_eq!(scheduler.count().unwrap(), 1);
    }

    #[test]
    fn test_cron_scheduler_unschedule() {
        let scheduler = CronScheduler::new();
        let task = create_test_task();

        scheduler.schedule("job1", "* * * * *", task).unwrap();
        assert_eq!(scheduler.count().unwrap(), 1);

        assert!(scheduler.unschedule("job1").unwrap());
        assert_eq!(scheduler.count().unwrap(), 0);
    }

    #[test]
    fn test_cron_scheduler_enable_disable() {
        let scheduler = CronScheduler::new();
        let task = create_test_task();

        scheduler.schedule("job1", "* * * * *", task).unwrap();

        assert!(scheduler.disable("job1").is_ok());
        let scheduled = scheduler.get("job1").unwrap().unwrap();
        assert!(!scheduled.enabled);

        assert!(scheduler.enable("job1").is_ok());
        let scheduled = scheduler.get("job1").unwrap().unwrap();
        assert!(scheduled.enabled);
    }

    #[test]
    fn test_cron_scheduler_list_all() {
        let scheduler = CronScheduler::new();
        let task1 = create_test_task();
        let task2 = create_test_task();

        scheduler.schedule("job1", "* * * * *", task1).unwrap();
        scheduler.schedule("job2", "0 * * * *", task2).unwrap();

        let all_tasks = scheduler.list_all().unwrap();
        assert_eq!(all_tasks.len(), 2);
    }

    #[test]
    fn test_cron_scheduler_clear() {
        let scheduler = CronScheduler::new();
        let task = create_test_task();

        scheduler
            .schedule("job1", "* * * * *", task.clone())
            .unwrap();
        scheduler.schedule("job2", "0 * * * *", task).unwrap();
        assert_eq!(scheduler.count().unwrap(), 2);

        scheduler.clear().unwrap();
        assert_eq!(scheduler.count().unwrap(), 0);
    }

    #[test]
    fn test_cron_scheduler_clone() {
        let scheduler = CronScheduler::new();
        let task = create_test_task();

        scheduler.schedule("job1", "* * * * *", task).unwrap();

        let cloned = scheduler.clone();
        assert_eq!(cloned.count().unwrap(), 1);
    }
}
