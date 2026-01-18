#![allow(clippy::missing_errors_doc)]
use crate::{Result, SerializedTask, Task};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Type-erased task handler
type TaskHandler = Arc<
    dyn Fn(Vec<u8>) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<Vec<u8>>> + Send>>
        + Send
        + Sync,
>;

/// Registry for mapping task names to their implementations
pub struct TaskRegistry {
    handlers: Arc<RwLock<HashMap<String, TaskHandler>>>,
}

impl TaskRegistry {
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self {
            handlers: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Register a task type with the registry
    pub async fn register<T>(&self, task: T)
    where
        T: Task + 'static,
    {
        let task_name = task.name().to_string();
        let task = Arc::new(task);

        let handler: TaskHandler = Arc::new(move |payload: Vec<u8>| {
            let task = Arc::clone(&task);
            Box::pin(async move {
                // Deserialize input
                let input: T::Input = serde_json::from_slice(&payload)
                    .map_err(|e| crate::CelersError::Deserialization(e.to_string()))?;

                // Execute task
                let output = task.execute(input).await?;

                // Serialize output
                let output_bytes = serde_json::to_vec(&output)
                    .map_err(|e| crate::CelersError::Serialization(e.to_string()))?;

                Ok(output_bytes)
            })
        });

        self.handlers.write().await.insert(task_name, handler);
    }

    /// Execute a task by name
    pub async fn execute(&self, task: &SerializedTask) -> Result<Vec<u8>> {
        let handlers = self.handlers.read().await;

        let handler = handlers.get(&task.metadata.name).ok_or_else(|| {
            crate::CelersError::TaskExecution(format!(
                "Task not found in registry: {}",
                task.metadata.name
            ))
        })?;

        handler(task.payload.clone()).await
    }

    /// Check if a task is registered
    pub async fn has_task(&self, name: &str) -> bool {
        self.handlers.read().await.contains_key(name)
    }

    /// List all registered task names
    pub async fn list_tasks(&self) -> Vec<String> {
        self.handlers.read().await.keys().cloned().collect()
    }
}

impl Default for TaskRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Task;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize)]
    struct AddInput {
        a: i32,
        b: i32,
    }

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct AddOutput {
        result: i32,
    }

    struct AddTask;

    #[async_trait::async_trait]
    impl Task for AddTask {
        type Input = AddInput;
        type Output = AddOutput;

        async fn execute(&self, input: Self::Input) -> Result<Self::Output> {
            Ok(AddOutput {
                result: input.a + input.b,
            })
        }

        fn name(&self) -> &'static str {
            "add"
        }
    }

    #[tokio::test]
    async fn test_registry() {
        let registry = TaskRegistry::new();

        // Register task
        registry.register(AddTask).await;

        // Check registration
        assert!(registry.has_task("add").await);

        // Create task
        let task = SerializedTask {
            metadata: crate::TaskMetadata::new("add".to_string()),
            payload: serde_json::to_vec(&AddInput { a: 2, b: 3 }).unwrap(),
        };

        // Execute
        let result = registry.execute(&task).await.unwrap();
        let output: AddOutput = serde_json::from_slice(&result).unwrap();

        assert_eq!(output.result, 5);
    }
}
