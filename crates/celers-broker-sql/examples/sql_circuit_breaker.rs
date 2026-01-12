//! Circuit Breaker Example
//!
//! Demonstrates the circuit breaker pattern for resilient database operations.
//! The circuit breaker protects against cascading failures by:
//! - Opening when failure rate exceeds threshold
//! - Temporarily blocking operations when open
//! - Gradually testing recovery with half-open state
//! - Automatically closing when operations succeed

use celers_broker_sql::{CircuitBreakerConfig, CircuitBreakerState, MysqlBroker, PoolConfig};
use celers_core::{Broker, SerializedTask};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    println!("=== Circuit Breaker Example ===\n");

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

    // Configure circuit breaker
    // - Opens after 3 consecutive failures
    // - Stays open for 5 seconds before attempting recovery
    // - Requires 2 successful operations to fully close
    let circuit_breaker_config = CircuitBreakerConfig {
        failure_threshold: 3,
        timeout_secs: 5,
        success_threshold: 2,
    };

    println!("\nCircuit Breaker Configuration:");
    println!(
        "  Failure threshold: {}",
        circuit_breaker_config.failure_threshold
    );
    println!("  Timeout: {} seconds", circuit_breaker_config.timeout_secs);
    println!(
        "  Success threshold: {}",
        circuit_breaker_config.success_threshold
    );

    // Create broker with circuit breaker configuration
    let broker = MysqlBroker::with_circuit_breaker_config(
        &database_url,
        "circuit_breaker_demo",
        pool_config,
        circuit_breaker_config.clone(),
    )
    .await?;

    println!("\nRunning migrations...");
    broker.migrate().await?;

    // Example 1: Normal operations with circuit breaker protection
    println!("\n=== Example 1: Normal Operations ===");
    demonstrate_normal_operations(&broker).await?;

    // Example 2: Circuit breaker stats
    println!("\n=== Example 2: Circuit Breaker Statistics ===");
    display_circuit_breaker_stats(&broker).await?;

    // Example 3: Simulating failures (requires manual intervention)
    println!("\n=== Example 3: Failure Simulation ===");
    println!("To simulate circuit breaker opening:");
    println!("  1. Stop MySQL: sudo systemctl stop mysql");
    println!("  2. Try enqueueing tasks (will fail)");
    println!(
        "  3. Circuit breaker will open after {} failures",
        circuit_breaker_config.failure_threshold
    );
    println!("  4. Start MySQL: sudo systemctl start mysql");
    println!("  5. Circuit breaker will automatically recover");
    println!("\nSkipping automated failure simulation...");

    // Example 4: Manual circuit breaker reset
    println!("\n=== Example 4: Manual Circuit Breaker Reset ===");
    demonstrate_manual_reset(&broker).await?;

    // Example 5: Protected operations
    println!("\n=== Example 5: Protected Operations ===");
    demonstrate_protected_operations(&broker).await?;

    println!("\n=== Circuit Breaker Example Complete ===");
    Ok(())
}

async fn demonstrate_normal_operations(
    broker: &MysqlBroker,
) -> Result<(), Box<dyn std::error::Error>> {
    // Create some sample tasks
    let task = SerializedTask::new(
        "process_data".to_string(),
        serde_json::to_vec(&serde_json::json!({
            "id": 1,
            "data": "sample data"
        }))?,
    )
    .with_priority(5)
    .with_max_retries(3);

    // Enqueue task (protected by circuit breaker)
    let task_id = broker.enqueue(task.clone()).await?;
    println!("Enqueued task: {}", task_id);

    // Check circuit breaker stats
    let stats = broker.get_circuit_breaker_stats();
    println!("Circuit breaker state: {:?}", stats.state);
    println!("Failure count: {}", stats.failure_count);
    println!("Success count: {}", stats.success_count);

    Ok(())
}

async fn display_circuit_breaker_stats(
    broker: &MysqlBroker,
) -> Result<(), Box<dyn std::error::Error>> {
    let stats = broker.get_circuit_breaker_stats();

    println!("Current State: {:?}", stats.state);
    println!("Failure Count: {}", stats.failure_count);
    println!("Success Count: {}", stats.success_count);
    println!("Last State Change: {}", stats.last_state_change);

    if let Some(last_failure) = stats.last_failure_time {
        println!("Last Failure: {}", last_failure);
    }

    match stats.state {
        CircuitBreakerState::Closed => {
            println!("\n✓ Circuit is CLOSED - All operations allowed");
        }
        CircuitBreakerState::Open => {
            println!("\n✗ Circuit is OPEN - Operations blocked");
            println!("  Waiting for timeout to enter HalfOpen state");
        }
        CircuitBreakerState::HalfOpen => {
            println!("\n⚠ Circuit is HALF-OPEN - Testing recovery");
            println!("  Success count: {}", stats.success_count);
        }
    }

    Ok(())
}

async fn demonstrate_manual_reset(broker: &MysqlBroker) -> Result<(), Box<dyn std::error::Error>> {
    println!("Resetting circuit breaker to Closed state...");
    broker.reset_circuit_breaker();

    let stats = broker.get_circuit_breaker_stats();
    println!("State after reset: {:?}", stats.state);
    println!("Failure count reset to: {}", stats.failure_count);

    Ok(())
}

async fn demonstrate_protected_operations(
    broker: &MysqlBroker,
) -> Result<(), Box<dyn std::error::Error>> {
    println!("Executing database operations with circuit breaker protection...");

    // Example: Protected queue size check
    let result = broker
        .with_circuit_breaker(|| async { broker.queue_size().await })
        .await;

    match result {
        Ok(size) => println!("Queue size: {}", size),
        Err(e) => println!("Operation failed (circuit breaker may be open): {}", e),
    }

    // Example: Protected statistics query
    let result = broker
        .with_circuit_breaker(|| async { broker.get_statistics().await })
        .await;

    match result {
        Ok(stats) => {
            println!("\nQueue Statistics:");
            println!("  Pending: {}", stats.pending);
            println!("  Processing: {}", stats.processing);
            println!("  Completed: {}", stats.completed);
            println!("  Failed: {}", stats.failed);
            println!("  DLQ: {}", stats.dlq);
        }
        Err(e) => println!("Statistics query failed: {}", e),
    }

    Ok(())
}
