//! Idempotency Keys Example
//!
//! This example demonstrates how to use idempotency keys to prevent duplicate task execution
//! in distributed systems. Idempotency is critical for:
//!
//! - **Financial Transactions**: Prevent duplicate payments or charges
//! - **Payment Processing**: Ensure payment requests are processed exactly once
//! - **Email/Notification Sending**: Avoid sending duplicate messages
//! - **External API Calls**: Prevent duplicate API requests
//! - **Data Mutations**: Ensure operations that modify data are executed only once
//!
//! The idempotency feature provides:
//! - Unique deduplication keys per task type
//! - Configurable TTL (time-to-live) for automatic cleanup
//! - Transaction-safe duplicate detection
//! - Statistics and monitoring capabilities
//!
//! # Prerequisites
//!
//! Make sure you have:
//! 1. MySQL 8.0+ running
//! 2. Created the database: `CREATE DATABASE celers_test;`
//! 3. Run migrations including 006_idempotency.sql
//!
//! # Running this example
//!
//! ```bash
//! cargo run --example idempotency_keys
//! ```

use celers_broker_sql::MysqlBroker;
use celers_core::SerializedTask;
use serde_json::json;
use std::time::Duration;
use tokio::time::sleep;
use uuid::Uuid;

#[tokio::main]
#[allow(dead_code)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    println!("=== CeleRS MySQL Broker: Idempotency Keys Demo ===\n");

    // Connect to MySQL broker
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://root:password@localhost:3306/celers_test".to_string());

    let broker = MysqlBroker::new(&database_url).await?;
    println!("✓ Connected to MySQL broker\n");

    // Run migrations to ensure idempotency table exists
    broker.migrate().await?;
    println!("✓ Migrations applied\n");

    // Clean up any existing data
    broker.purge_all().await?;
    broker.cleanup_expired_idempotency_keys().await?;

    // Demo 1: Basic idempotency - prevent duplicate payment processing
    println!("--- Demo 1: Payment Processing with Idempotency ---");
    demo_payment_processing(&broker).await?;

    // Demo 2: Notification sending with idempotency
    println!("\n--- Demo 2: Notification Sending ---");
    demo_notification_sending(&broker).await?;

    // Demo 3: Idempotency with TTL expiration
    println!("\n--- Demo 3: TTL and Expiration ---");
    demo_ttl_expiration(&broker).await?;

    // Demo 4: Idempotency statistics and monitoring
    println!("\n--- Demo 4: Statistics and Monitoring ---");
    demo_statistics(&broker).await?;

    // Demo 5: Cleanup of expired keys
    println!("\n--- Demo 5: Cleanup Expired Keys ---");
    demo_cleanup(&broker).await?;

    println!("\n=== Demo Complete ===");

    Ok(())
}

/// Demo 1: Payment processing with idempotency keys
///
/// This demonstrates how to prevent duplicate payment charges when the same
/// payment request is submitted multiple times (e.g., due to network retries,
/// button double-clicks, or API retries).
async fn demo_payment_processing(broker: &MysqlBroker) -> Result<(), Box<dyn std::error::Error>> {
    let payment_id = Uuid::new_v4().to_string();
    let idempotency_key = format!("payment-{}", payment_id);

    // Create a payment task payload
    let payment_payload = json!({
        "amount": 99.99,
        "currency": "USD",
        "user_id": "user123",
        "payment_method": "card_****1234",
        "payment_id": payment_id,
        "transaction_type": "purchase",
        "merchant_id": "merchant_abc"
    });
    let payment_task = SerializedTask::new(
        "process_payment".to_string(),
        serde_json::to_vec(&payment_payload)?,
    );

    // First submission - should create new task
    println!("  1. Submitting payment request (first time)...");
    let task_id1 = broker
        .enqueue_with_idempotency(
            payment_task.clone(),
            &idempotency_key,
            3600, // TTL: 1 hour
            Some(json!({"client_ip": "192.168.1.100", "request_id": "req-001"})),
        )
        .await?;
    println!("     ✓ Task created: {}", task_id1);

    // Second submission with same idempotency key - should return existing task
    println!("  2. Submitting same payment request (retry)...");
    let task_id2 = broker
        .enqueue_with_idempotency(
            payment_task.clone(),
            &idempotency_key,
            3600,
            Some(json!({"client_ip": "192.168.1.100", "request_id": "req-002"})),
        )
        .await?;
    println!("     ✓ Returned existing task: {}", task_id2);

    // Verify that both submissions returned the same task ID
    assert_eq!(
        task_id1, task_id2,
        "Idempotency check failed: different task IDs returned"
    );
    println!(
        "     ✓ Idempotency verified: Same task ID returned ({} == {})",
        task_id1, task_id2
    );

    // Retrieve the idempotency record
    if let Some(record) = broker
        .get_idempotency_record(&idempotency_key, "process_payment")
        .await?
    {
        println!("\n  Idempotency Record:");
        println!("    - Key: {}", record.idempotency_key);
        println!("    - Task ID: {}", record.task_id);
        println!("    - Task Name: {}", record.task_name);
        println!("    - Created: {}", record.created_at);
        println!("    - Expires: {}", record.expires_at);
        if let Some(meta) = record.metadata {
            println!("    - Metadata: {}", meta);
        }
    }

    Ok(())
}

/// Demo 2: Notification sending with idempotency
///
/// Prevents sending duplicate email notifications when API calls are retried
/// or when the same event triggers multiple notification requests.
async fn demo_notification_sending(broker: &MysqlBroker) -> Result<(), Box<dyn std::error::Error>> {
    let user_id = "user456";
    let notification_type = "order_confirmation";
    let order_id = "order-789";

    // Create idempotency key based on user, notification type, and order
    let idempotency_key = format!("{}-{}-{}", user_id, notification_type, order_id);

    let notification_payload = json!({
        "to": "user@example.com",
        "subject": "Your Order Confirmation",
        "template": "order_confirmation",
        "user_id": user_id,
        "order_id": order_id,
        "notification_type": notification_type
    });
    let notification_task = SerializedTask::new(
        "send_email".to_string(),
        serde_json::to_vec(&notification_payload)?,
    );

    println!("  1. Sending order confirmation email...");
    let task_id1 = broker
        .enqueue_with_idempotency(
            notification_task.clone(),
            &idempotency_key,
            7200, // TTL: 2 hours
            None,
        )
        .await?;
    println!("     ✓ Email task created: {}", task_id1);

    // Simulate retry due to timeout or network error
    println!("  2. Retrying email send (simulated network retry)...");
    let task_id2 = broker
        .enqueue_with_idempotency(notification_task, &idempotency_key, 7200, None)
        .await?;
    println!("     ✓ Returned existing task: {}", task_id2);

    assert_eq!(task_id1, task_id2);
    println!("     ✓ Duplicate notification prevented!");

    Ok(())
}

/// Demo 3: TTL and expiration
///
/// Demonstrates how idempotency keys expire after their TTL, allowing the
/// same operation to be submitted again after the expiration period.
async fn demo_ttl_expiration(broker: &MysqlBroker) -> Result<(), Box<dyn std::error::Error>> {
    let idempotency_key = "api-call-xyz-123";

    let api_payload = json!({
        "endpoint": "/api/v1/resource",
        "method": "POST",
        "payload": {"data": "example"}
    });
    let api_task = SerializedTask::new(
        "call_external_api".to_string(),
        serde_json::to_vec(&api_payload)?,
    );

    // First submission with short TTL (3 seconds)
    println!("  1. Submitting API call with 3-second TTL...");
    let task_id1 = broker
        .enqueue_with_idempotency(api_task.clone(), idempotency_key, 3, None)
        .await?;
    println!("     ✓ Task created: {}", task_id1);

    // Try to submit again immediately - should return same task
    println!("  2. Trying to submit again (before expiration)...");
    let task_id2 = broker
        .enqueue_with_idempotency(api_task.clone(), idempotency_key, 3, None)
        .await?;
    println!("     ✓ Returned existing task: {}", task_id2);
    assert_eq!(task_id1, task_id2);

    // Wait for TTL to expire
    println!("  3. Waiting for TTL to expire (3 seconds)...");
    sleep(Duration::from_secs(4)).await;

    // Clean up expired keys
    let cleaned = broker.cleanup_expired_idempotency_keys().await?;
    println!("     ✓ Cleaned up {} expired keys", cleaned);

    // Now we can submit again with the same key
    println!("  4. Submitting again after expiration...");
    let task_id3 = broker
        .enqueue_with_idempotency(api_task, idempotency_key, 3, None)
        .await?;
    println!("     ✓ New task created: {}", task_id3);

    // Verify it's a different task
    assert_ne!(
        task_id1, task_id3,
        "Expected different task ID after expiration"
    );
    println!(
        "     ✓ New task allowed after expiration ({} != {})",
        task_id1, task_id3
    );

    Ok(())
}

/// Demo 4: Statistics and monitoring
///
/// Shows how to monitor idempotency key usage and effectiveness across
/// different task types.
async fn demo_statistics(broker: &MysqlBroker) -> Result<(), Box<dyn std::error::Error>> {
    // Create several tasks with idempotency keys
    println!("  1. Creating tasks with various idempotency keys...");

    for i in 0..5 {
        let payload = json!({"batch_id": i});
        let task =
            SerializedTask::new("batch_operation".to_string(), serde_json::to_vec(&payload)?);

        broker
            .enqueue_with_idempotency(
                task,
                &format!("batch-op-{}", i),
                3600,
                Some(json!({"batch_number": i})),
            )
            .await?;
    }
    println!("     ✓ Created 5 tasks with unique idempotency keys");

    // Try to create duplicates
    println!("  2. Attempting to create duplicates...");
    for i in 0..5 {
        let payload = json!({"batch_id": i});
        let task =
            SerializedTask::new("batch_operation".to_string(), serde_json::to_vec(&payload)?);

        broker
            .enqueue_with_idempotency(task, &format!("batch-op-{}", i), 3600, None)
            .await?;
    }
    println!("     ✓ Duplicate submissions handled (returned existing tasks)");

    // Get statistics
    println!("\n  Idempotency Statistics:");
    let stats = broker.get_idempotency_statistics().await?;

    for stat in stats {
        println!("    Task: {}", stat.task_name);
        println!("      - Total keys: {}", stat.total_keys);
        println!("      - Unique keys: {}", stat.unique_keys);
        println!("      - Active keys: {}", stat.active_keys);
        println!("      - Expired keys: {}", stat.expired_keys);
        if let Some(oldest) = stat.oldest_key {
            println!("      - Oldest key: {}", oldest);
        }
        if let Some(newest) = stat.newest_key {
            println!("      - Newest key: {}", newest);
        }
    }

    Ok(())
}

/// Demo 5: Cleanup of expired keys
///
/// Demonstrates automatic cleanup of expired idempotency keys to prevent
/// table bloat in production systems.
async fn demo_cleanup(broker: &MysqlBroker) -> Result<(), Box<dyn std::error::Error>> {
    // Create tasks with very short TTL
    println!("  1. Creating tasks with 2-second TTL...");

    for i in 0..10 {
        let payload = json!({"op_id": i});
        let task = SerializedTask::new(
            "temporary_operation".to_string(),
            serde_json::to_vec(&payload)?,
        );

        broker
            .enqueue_with_idempotency(task, &format!("temp-op-{}", i), 2, None)
            .await?;
    }
    println!("     ✓ Created 10 tasks");

    // Check statistics before expiration
    let stats_before = broker.get_idempotency_statistics().await?;
    let active_before = stats_before
        .iter()
        .find(|s| s.task_name == "temporary_operation")
        .map(|s| s.active_keys)
        .unwrap_or(0);
    println!("  2. Active keys before expiration: {}", active_before);

    // Wait for expiration
    println!("  3. Waiting for keys to expire (3 seconds)...");
    sleep(Duration::from_secs(3)).await;

    // Cleanup
    println!("  4. Running cleanup...");
    let cleaned = broker.cleanup_expired_idempotency_keys().await?;
    println!("     ✓ Cleaned up {} expired keys", cleaned);

    // Check statistics after cleanup
    let stats_after = broker.get_idempotency_statistics().await?;
    let active_after = stats_after
        .iter()
        .find(|s| s.task_name == "temporary_operation")
        .map(|s| s.active_keys)
        .unwrap_or(0);
    println!("  5. Active keys after cleanup: {}", active_after);

    println!("\n  💡 Tip: In production, run cleanup_expired_idempotency_keys()");
    println!("     periodically (e.g., via cron job or scheduled task) to prevent");
    println!("     table bloat and maintain optimal performance.");

    Ok(())
}
