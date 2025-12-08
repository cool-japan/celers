//! Derive macro example
//!
//! This example demonstrates how to use the #[derive(Task)] macro
//! to implement the Task trait with custom types.
//!
//! Run with: cargo run --example derive_task

use celers_macros::Task;
use serde::{Deserialize, Serialize};

// Mock celers_core for the example
mod celers_core {
    use serde::{Deserialize, Serialize};

    #[derive(Debug)]
    pub struct CelersError(pub String);

    impl std::fmt::Display for CelersError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl std::error::Error for CelersError {}

    impl From<String> for CelersError {
        fn from(s: String) -> Self {
            CelersError(s)
        }
    }

    pub type Result<T> = std::result::Result<T, CelersError>;

    #[async_trait::async_trait]
    pub trait Task: Send + Sync {
        type Input: Serialize + for<'de> Deserialize<'de> + Send;
        type Output: Serialize + for<'de> Deserialize<'de> + Send;

        async fn execute(&self, input: Self::Input) -> Result<Self::Output>;
        fn name(&self) -> &str;
    }
}

use celers_core::Task as CelersTask;

// Define custom input/output types
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProcessInput {
    data: String,
    multiplier: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProcessOutput {
    result: String,
    count: i32,
}

// Example 1: Task using derive with custom types
#[derive(Task)]
#[task(
    input = "ProcessInput",
    output = "ProcessOutput",
    name = "process_data"
)]
struct ProcessTask {
    prefix: String,
}

impl ProcessTask {
    async fn execute_impl(&self, input: ProcessInput) -> celers_core::Result<ProcessOutput> {
        let result = format!(
            "{}: {}",
            self.prefix,
            input.data.repeat(input.multiplier as usize)
        );
        let count = result.len() as i32;
        Ok(ProcessOutput { result, count })
    }
}

// Example 2: Task with automatic name conversion
#[derive(Task)]
#[task(input = "String", output = "String")]
struct UpperCaseTask;

impl UpperCaseTask {
    async fn execute_impl(&self, input: String) -> celers_core::Result<String> {
        Ok(input.to_uppercase())
    }
}

// Example 3: Task with JSON values (default)
#[derive(Task)]
struct JsonProcessorTask {
    transform_key: String,
}

impl JsonProcessorTask {
    async fn execute_impl(
        &self,
        input: serde_json::Value,
    ) -> celers_core::Result<serde_json::Value> {
        if let serde_json::Value::Object(mut map) = input {
            map.insert(self.transform_key.clone(), serde_json::json!("processed"));
            Ok(serde_json::Value::Object(map))
        } else {
            Ok(input)
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== CeleRS Derive Macro Examples ===\n");

    // Example 1: Custom types with explicit name
    println!("1. Task with Custom Types:");
    let task1 = ProcessTask {
        prefix: "Result".to_string(),
    };
    println!("   Task name: {}", task1.name());

    let input1 = ProcessInput {
        data: "Hello".to_string(),
        multiplier: 3,
    };
    let result1 = task1.execute(input1).await?;
    println!("   Output: {:?}\n", result1);

    // Example 2: Automatic name conversion (UpperCaseTask -> upper_case_task)
    println!("2. Task with Automatic Name:");
    let task2 = UpperCaseTask;
    println!("   Task name: {}", task2.name());
    let result2 = task2.execute("hello world".to_string()).await?;
    println!("   Result: {}\n", result2);

    // Example 3: JSON processing
    println!("3. JSON Processing Task:");
    let task3 = JsonProcessorTask {
        transform_key: "status".to_string(),
    };
    println!("   Task name: {}", task3.name());

    let input3 = serde_json::json!({
        "id": 123,
        "data": "test"
    });
    let result3 = task3.execute(input3).await?;
    println!("   Result: {}\n", result3);

    println!("=== All derive examples completed successfully! ===");

    Ok(())
}
