use serde_json::Value;
use std::sync::Arc;

/// Hook execution result
pub type HookResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

/// Pre-execution hook
///
/// Runs before task execution. Can modify task arguments or abort execution.
pub trait PreExecutionHook: Send + Sync {
    /// Executes before task runs
    ///
    /// # Arguments
    ///
    /// * `task_name` - Name of the task
    /// * `task_id` - Unique identifier for the task instance
    /// * `args` - Task arguments (can be modified)
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Continue with task execution
    /// * `Err(e)` - Abort task execution with error
    fn before_execute(&self, task_name: &str, task_id: &str, args: &mut Vec<Value>) -> HookResult;
}

/// Post-execution hook
///
/// Runs after task execution (both success and failure).
pub trait PostExecutionHook: Send + Sync {
    /// Executes after task completes
    ///
    /// # Arguments
    ///
    /// * `task_name` - Name of the task
    /// * `task_id` - Unique identifier for the task instance
    /// * `result` - Task execution result
    /// * `duration_ms` - Execution duration in milliseconds
    fn after_execute(
        &self,
        task_name: &str,
        task_id: &str,
        result: &Result<Value, String>,
        duration_ms: u128,
    ) -> HookResult;
}

/// Hook registry for managing task lifecycle hooks
pub struct HookRegistry {
    pre_hooks: Vec<Arc<dyn PreExecutionHook>>,
    post_hooks: Vec<Arc<dyn PostExecutionHook>>,
}

impl HookRegistry {
    /// Creates a new hook registry
    pub fn new() -> Self {
        Self {
            pre_hooks: Vec::new(),
            post_hooks: Vec::new(),
        }
    }

    /// Registers a pre-execution hook
    pub fn register_pre_hook<H>(&mut self, hook: H)
    where
        H: PreExecutionHook + 'static,
    {
        self.pre_hooks.push(Arc::new(hook));
    }

    /// Registers a post-execution hook
    pub fn register_post_hook<H>(&mut self, hook: H)
    where
        H: PostExecutionHook + 'static,
    {
        self.post_hooks.push(Arc::new(hook));
    }

    /// Runs all pre-execution hooks
    pub fn run_pre_hooks(
        &self,
        task_name: &str,
        task_id: &str,
        args: &mut Vec<Value>,
    ) -> HookResult {
        for hook in &self.pre_hooks {
            hook.before_execute(task_name, task_id, args)?;
        }
        Ok(())
    }

    /// Runs all post-execution hooks
    pub fn run_post_hooks(
        &self,
        task_name: &str,
        task_id: &str,
        result: &Result<Value, String>,
        duration_ms: u128,
    ) -> HookResult {
        for hook in &self.post_hooks {
            hook.after_execute(task_name, task_id, result, duration_ms)?;
        }
        Ok(())
    }

    /// Gets the number of registered pre-execution hooks
    pub fn pre_hook_count(&self) -> usize {
        self.pre_hooks.len()
    }

    /// Gets the number of registered post-execution hooks
    pub fn post_hook_count(&self) -> usize {
        self.post_hooks.len()
    }
}

impl Default for HookRegistry {
    fn default() -> Self {
        Self::new()
    }
}

/// Logging hook for task execution
pub struct LoggingHook {
    log_args: bool,
    log_results: bool,
}

impl LoggingHook {
    /// Creates a new logging hook
    pub fn new(log_args: bool, log_results: bool) -> Self {
        Self {
            log_args,
            log_results,
        }
    }
}

impl PreExecutionHook for LoggingHook {
    fn before_execute(&self, task_name: &str, task_id: &str, args: &mut Vec<Value>) -> HookResult {
        if self.log_args {
            println!("[TASK] Starting {} ({}): {:?}", task_name, task_id, args);
        } else {
            println!("[TASK] Starting {} ({})", task_name, task_id);
        }
        Ok(())
    }
}

impl PostExecutionHook for LoggingHook {
    fn after_execute(
        &self,
        task_name: &str,
        task_id: &str,
        result: &Result<Value, String>,
        duration_ms: u128,
    ) -> HookResult {
        match result {
            Ok(value) => {
                if self.log_results {
                    println!(
                        "[TASK] Completed {} ({}) in {}ms: {:?}",
                        task_name, task_id, duration_ms, value
                    );
                } else {
                    println!(
                        "[TASK] Completed {} ({}) in {}ms",
                        task_name, task_id, duration_ms
                    );
                }
            }
            Err(e) => {
                println!(
                    "[TASK] Failed {} ({}) in {}ms: {}",
                    task_name, task_id, duration_ms, e
                );
            }
        }
        Ok(())
    }
}

/// Validation hook for checking task arguments
pub struct ValidationHook<F>
where
    F: Fn(&str, &Vec<Value>) -> HookResult + Send + Sync,
{
    validator: F,
}

impl<F> ValidationHook<F>
where
    F: Fn(&str, &Vec<Value>) -> HookResult + Send + Sync,
{
    /// Creates a new validation hook
    pub fn new(validator: F) -> Self {
        Self { validator }
    }
}

impl<F> PreExecutionHook for ValidationHook<F>
where
    F: Fn(&str, &Vec<Value>) -> HookResult + Send + Sync,
{
    fn before_execute(&self, task_name: &str, _task_id: &str, args: &mut Vec<Value>) -> HookResult {
        (self.validator)(task_name, args)
    }
}
