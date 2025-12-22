//! Advanced Retry Policies Example
//!
//! Demonstrates sophisticated retry strategies for task execution.
//! This is useful for:
//! - Handling transient failures gracefully
//! - Implementing backoff strategies
//! - Preventing thundering herd problems
//! - Customizing retry behavior per task type

use celers_broker_sql::{MysqlBroker, PoolConfig, RetryPolicy, RetryStrategy};
use celers_core::{Broker, SerializedTask};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("=== Advanced Retry Policies Example ===\n");

    // Database URL from environment or default
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://root:password@localhost/celers_dev".to_string());

    println!("Connecting to MySQL: {}", database_url);

    // Configure connection pool
    let pool_config = PoolConfig {
        max_connections: 10,
        min_connections: 2,
        acquire_timeout_secs: 5,
        max_lifetime_secs: Some(1800),
        idle_timeout_secs: Some(600),
    };

    // Create broker
    let broker = MysqlBroker::with_config(&database_url, "retry_demo", pool_config).await?;

    println!("Running migrations...");
    broker.migrate().await?;

    // Example 1: Fixed delay retry
    println!("\n=== Example 1: Fixed Delay Retry ===");
    demonstrate_fixed_retry(&broker).await?;

    // Example 2: Linear backoff retry
    println!("\n=== Example 2: Linear Backoff Retry ===");
    demonstrate_linear_retry(&broker).await?;

    // Example 3: Exponential backoff retry
    println!("\n=== Example 3: Exponential Backoff Retry ===");
    demonstrate_exponential_retry(&broker).await?;

    // Example 4: Exponential with jitter retry
    println!("\n=== Example 4: Exponential with Jitter Retry ===");
    demonstrate_exponential_jitter_retry(&broker).await?;

    // Example 5: Compare retry strategies
    println!("\n=== Example 5: Retry Strategy Comparison ===");
    compare_retry_strategies();

    // Example 6: Reject with retry policy
    println!("\n=== Example 6: Reject with Retry Policy ===");
    demonstrate_reject_with_policy(&broker).await?;

    // Example 7: View retry statistics
    println!("\n=== Example 7: Retry Statistics ===");
    view_retry_statistics(&broker).await?;

    println!("\n=== Advanced Retry Policies Example Complete ===");
    Ok(())
}

async fn demonstrate_fixed_retry(broker: &MysqlBroker) -> Result<(), Box<dyn std::error::Error>> {
    let task = SerializedTask::new(
        "api_call_fixed".to_string(),
        serde_json::to_vec(&serde_json::json!({
            "endpoint": "https://api.example.com/data",
            "retry_type": "fixed"
        }))?,
    )
    .with_priority(5);

    // Fixed retry: Always wait 10 seconds between retries
    let retry_policy = RetryPolicy {
        strategy: RetryStrategy::Fixed(10),
        max_retries: 5,
    };

    let task_id = broker.enqueue_with_retry_policy(task, retry_policy).await?;

    println!("Enqueued task with Fixed retry policy: {}", task_id);
    println!("  Strategy: Wait 10 seconds between each retry");
    println!("  Max retries: 5");
    println!("  Retry schedule: 10s, 10s, 10s, 10s, 10s");
    println!("  Total max delay: ~50 seconds");

    Ok(())
}

async fn demonstrate_linear_retry(broker: &MysqlBroker) -> Result<(), Box<dyn std::error::Error>> {
    let task = SerializedTask::new(
        "database_sync_linear".to_string(),
        serde_json::to_vec(&serde_json::json!({
            "table": "users",
            "retry_type": "linear"
        }))?,
    )
    .with_priority(7);

    // Linear retry: delay = base_delay * retry_count
    // 1st retry: 5s, 2nd: 10s, 3rd: 15s, 4th: 20s, 5th: 25s
    let retry_policy = RetryPolicy {
        strategy: RetryStrategy::Linear { base_delay_secs: 5 },
        max_retries: 5,
    };

    let task_id = broker.enqueue_with_retry_policy(task, retry_policy).await?;

    println!("Enqueued task with Linear retry policy: {}", task_id);
    println!("  Strategy: Delay increases linearly (base_delay * retry_count)");
    println!("  Base delay: 5 seconds");
    println!("  Max retries: 5");
    println!("  Retry schedule: 5s, 10s, 15s, 20s, 25s");
    println!("  Total max delay: ~75 seconds");

    Ok(())
}

async fn demonstrate_exponential_retry(
    broker: &MysqlBroker,
) -> Result<(), Box<dyn std::error::Error>> {
    let task = SerializedTask::new(
        "external_api_exponential".to_string(),
        serde_json::to_vec(&serde_json::json!({
            "service": "payment_gateway",
            "retry_type": "exponential"
        }))?,
    )
    .with_priority(9);

    // Exponential retry: delay = base_delay * (multiplier ^ retry_count)
    // 1st: 2s, 2nd: 4s, 3rd: 8s, 4th: 16s, 5th: 32s, 6th: 60s (capped at max)
    let retry_policy = RetryPolicy {
        strategy: RetryStrategy::Exponential {
            base_delay_secs: 2,
            multiplier: 2.0,
            max_delay_secs: 60,
        },
        max_retries: 6,
    };

    let task_id = broker.enqueue_with_retry_policy(task, retry_policy).await?;

    println!("Enqueued task with Exponential retry policy: {}", task_id);
    println!("  Strategy: Delay increases exponentially (base * multiplier^retry)");
    println!("  Base delay: 2 seconds");
    println!("  Multiplier: 2.0");
    println!("  Max delay: 60 seconds");
    println!("  Max retries: 6");
    println!("  Retry schedule: 2s, 4s, 8s, 16s, 32s, 60s");
    println!("  Total max delay: ~122 seconds");

    Ok(())
}

async fn demonstrate_exponential_jitter_retry(
    broker: &MysqlBroker,
) -> Result<(), Box<dyn std::error::Error>> {
    let task = SerializedTask::new(
        "queue_processor_jitter".to_string(),
        serde_json::to_vec(&serde_json::json!({
            "queue": "high_volume",
            "retry_type": "exponential_jitter"
        }))?,
    )
    .with_priority(8);

    // Exponential with jitter: Adds randomness to prevent thundering herd
    // Base schedule: 2s, 4s, 8s, 16s, 32s, 60s
    // Actual: Random between 0 and calculated delay
    let retry_policy = RetryPolicy {
        strategy: RetryStrategy::ExponentialWithJitter {
            base_delay_secs: 2,
            multiplier: 2.0,
            max_delay_secs: 60,
        },
        max_retries: 6,
    };

    let task_id = broker.enqueue_with_retry_policy(task, retry_policy).await?;

    println!(
        "Enqueued task with Exponential+Jitter retry policy: {}",
        task_id
    );
    println!("  Strategy: Exponential backoff with randomized jitter");
    println!("  Base delay: 2 seconds");
    println!("  Multiplier: 2.0");
    println!("  Max delay: 60 seconds");
    println!("  Max retries: 6");
    println!("  Retry schedule (approximate): 0-2s, 0-4s, 0-8s, 0-16s, 0-32s, 0-60s");
    println!("  Benefit: Prevents thundering herd when many tasks fail simultaneously");

    Ok(())
}

#[allow(dead_code)]
fn compare_retry_strategies() {
    println!("Comparison of retry strategies:\n");

    println!("1. FIXED (10s base, 5 retries):");
    println!("   Delays: 10s, 10s, 10s, 10s, 10s");
    println!("   Total: ~50s");
    println!("   Use case: Consistent delays, simple rate limiting\n");

    println!("2. LINEAR (5s base, 5 retries):");
    println!("   Delays: 5s, 10s, 15s, 20s, 25s");
    println!("   Total: ~75s");
    println!("   Use case: Gradual backoff, moderate load increase\n");

    println!("3. EXPONENTIAL (2s base, 2x mult, 60s max, 6 retries):");
    println!("   Delays: 2s, 4s, 8s, 16s, 32s, 60s");
    println!("   Total: ~122s");
    println!("   Use case: External APIs, cascading failures, rapid backoff\n");

    println!("4. EXPONENTIAL WITH JITTER (2s base, 2x mult, 60s max, 6 retries):");
    println!("   Delays: 0-2s, 0-4s, 0-8s, 0-16s, 0-32s, 0-60s (random)");
    println!("   Total: varies");
    println!("   Use case: High-volume systems, preventing thundering herd\n");

    println!("Recommendations:");
    println!("  - Use FIXED for: Simple retries, predictable delays");
    println!("  - Use LINEAR for: Database operations, gradual recovery");
    println!("  - Use EXPONENTIAL for: External APIs, network calls, rate-limited services");
    println!("  - Use EXPONENTIAL+JITTER for: High-concurrency, distributed systems");
}

async fn demonstrate_reject_with_policy(
    broker: &MysqlBroker,
) -> Result<(), Box<dyn std::error::Error>> {
    // First enqueue a task
    let task = SerializedTask::new(
        "test_reject_policy".to_string(),
        serde_json::to_vec(&serde_json::json!({
            "test": "reject_with_policy"
        }))?,
    )
    .with_priority(5)
    .with_max_retries(3);

    let retry_policy = RetryPolicy {
        strategy: RetryStrategy::Exponential {
            base_delay_secs: 3,
            multiplier: 2.0,
            max_delay_secs: 30,
        },
        max_retries: 3,
    };

    let task_id = broker.enqueue_with_retry_policy(task, retry_policy).await?;
    println!("Enqueued task for reject demo: {}", task_id);

    // Dequeue it
    if let Some(msg) = broker.dequeue().await? {
        let task_id = msg.task.metadata.id;
        println!("Dequeued task: {}", task_id);

        // Simulate failure and reject with retry policy
        println!("Simulating task failure...");
        let rejected = broker
            .reject_with_retry_policy(&task_id, Some("Simulated error for demo".to_string()), true)
            .await?;

        if rejected {
            println!("✓ Task rejected and scheduled for retry");
            println!("  Next retry will use exponential backoff");

            // Check if task was rescheduled
            let task_info = broker.get_task(&task_id).await?;
            if let Some(info) = task_info {
                println!("  Current retry count: {}", info.retry_count);
                println!("  Max retries: {}", info.max_retries);
                println!("  Scheduled at: {}", info.scheduled_at);
            }
        }
    }

    Ok(())
}

async fn view_retry_statistics(broker: &MysqlBroker) -> Result<(), Box<dyn std::error::Error>> {
    let retry_stats = broker.get_retry_statistics().await?;

    println!("Retry statistics by task type:\n");

    if retry_stats.is_empty() {
        println!("No retry statistics available yet");
        return Ok(());
    }

    for stat in retry_stats {
        println!("Task: {}", stat.task_name);
        println!("  Unique tasks: {}", stat.unique_tasks);
        println!("  Total retries: {}", stat.total_retries);
        println!(
            "  Average retries per task: {:.2}",
            stat.avg_retries_per_task
        );
        println!("  Max retries observed: {}", stat.max_retries_observed);
        println!();
    }

    Ok(())
}
