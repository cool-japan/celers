//! Basic task example
//!
//! This example demonstrates how to use the #[task] macro to define simple tasks.
//!
//! Run with: cargo run --example basic_task

use celers_macros::task;

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

use celers_core::Task;

// Example 1: Simple addition task
#[task]
async fn add_numbers(a: i32, b: i32) -> celers_core::Result<i32> {
    println!("Adding {} + {}", a, b);
    Ok(a + b)
}

// Example 2: Task with configuration
#[task(name = "tasks.multiply", timeout = 30, priority = 10, max_retries = 3)]
async fn multiply_numbers(x: i32, y: i32) -> celers_core::Result<i32> {
    println!("Multiplying {} * {}", x, y);
    Ok(x * y)
}

// Example 3: Task with optional parameters
#[task]
async fn greet_user(
    name: String,
    title: Option<String>,
    formal: Option<bool>,
) -> celers_core::Result<String> {
    let greeting = if formal.unwrap_or(false) {
        format!(
            "Good day, {}{}",
            title.as_deref().unwrap_or(""),
            if title.is_some() { " " } else { "" }
        )
    } else {
        "Hey ".to_string()
    };

    Ok(format!("{}{}", greeting, name))
}

// Example 4: Task with error handling
#[task]
async fn divide_numbers(numerator: i32, denominator: i32) -> celers_core::Result<f64> {
    if denominator == 0 {
        return Err(celers_core::CelersError(
            "Cannot divide by zero".to_string(),
        ));
    }
    Ok(numerator as f64 / denominator as f64)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== CeleRS Macro Examples ===\n");

    // Example 1: Basic addition
    println!("1. Basic Addition:");
    let task1 = AddNumbersTask;
    let input1 = AddNumbersTaskInput { a: 5, b: 3 };
    let result1 = task1.execute(input1).await?;
    println!("   Result: {}\n", result1);

    // Example 2: Multiplication with configuration
    println!("2. Multiplication with Configuration:");
    let task2 = MultiplyNumbersTask;
    println!("   Task name: {}", task2.name());
    println!("   Timeout: {:?}", task2.timeout());
    println!("   Priority: {:?}", task2.priority());
    println!("   Max retries: {:?}", task2.max_retries());
    let input2 = MultiplyNumbersTaskInput { x: 4, y: 7 };
    let result2 = task2.execute(input2).await?;
    println!("   Result: {}\n", result2);

    // Example 3: Optional parameters
    println!("3. Greeting with Optional Parameters:");
    let task3 = GreetUserTask;

    let input3a = GreetUserTaskInput {
        name: "Alice".to_string(),
        title: Some("Dr.".to_string()),
        formal: Some(true),
    };
    let result3a = task3.execute(input3a).await?;
    println!("   Formal: {}", result3a);

    let input3b = GreetUserTaskInput {
        name: "Bob".to_string(),
        title: None,
        formal: Some(false),
    };
    let result3b = task3.execute(input3b).await?;
    println!("   Informal: {}\n", result3b);

    // Example 4: Error handling
    println!("4. Error Handling:");
    let task4 = DivideNumbersTask;

    let input4a = DivideNumbersTaskInput {
        numerator: 10,
        denominator: 2,
    };
    match task4.execute(input4a).await {
        Ok(result) => println!("   10 / 2 = {}", result),
        Err(e) => println!("   Error: {}", e),
    }

    let input4b = DivideNumbersTaskInput {
        numerator: 10,
        denominator: 0,
    };
    match task4.execute(input4b).await {
        Ok(result) => println!("   10 / 0 = {}", result),
        Err(e) => println!("   Error: {}\n", e),
    }

    println!("=== All examples completed successfully! ===");

    Ok(())
}
