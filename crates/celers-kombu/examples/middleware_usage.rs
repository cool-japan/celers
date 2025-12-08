//! Middleware usage example
//!
//! This example demonstrates how to use middleware for message transformation,
//! validation, logging, and metrics collection.
//!
//! Run with: cargo run --example middleware_usage

use celers_kombu::*;
use celers_protocol::Message;
use uuid::Uuid;

#[tokio::main]
async fn main() -> celers_kombu::Result<()> {
    println!("🔌 Middleware Usage Example\n");

    // Create a middleware chain with multiple middlewares
    println!("📦 Building Middleware Chain");
    let metrics = std::sync::Arc::new(std::sync::Mutex::new(BrokerMetrics::default()));
    let chain = MiddlewareChain::new()
        .with_middleware(Box::new(
            ValidationMiddleware::new().with_max_body_size(1024),
        ))
        .with_middleware(Box::new(
            LoggingMiddleware::new("[BROKER]").with_body_logging(),
        ))
        .with_middleware(Box::new(MetricsMiddleware::new(metrics)))
        .with_middleware(Box::new(RetryLimitMiddleware::new(3)))
        .with_middleware(Box::new(RateLimitingMiddleware::new(10.0)))
        .with_middleware(Box::new(DeduplicationMiddleware::new(1000)));

    println!("   Middlewares: {}", chain.len());
    println!("   Chain configured successfully!\n");

    // Create a test message
    let task_id = Uuid::new_v4();
    let mut message = Message::new(
        "example_task".to_string(),
        task_id,
        b"Hello, Middleware!".to_vec(),
    );

    println!("📤 Processing Message Before Publish");
    println!("   Task: {}", message.task_name());
    println!("   ID: {}", message.task_id());
    println!("   Body size: {} bytes\n", message.body.len());

    // Process through middleware chain before publishing
    match chain.process_before_publish(&mut message).await {
        Ok(_) => println!("✅ Message passed all middleware checks\n"),
        Err(e) => {
            println!("❌ Middleware rejected message: {}\n", e);
            return Err(e);
        }
    }

    println!("📥 Processing Message After Consume");
    // Process through middleware chain after consuming
    match chain.process_after_consume(&mut message).await {
        Ok(_) => println!("✅ Message processed successfully after consume\n"),
        Err(e) => {
            println!("❌ Error processing message: {}\n", e);
            return Err(e);
        }
    }

    // Demonstrate individual middlewares
    println!("🔍 Individual Middleware Examples\n");

    // 1. Validation Middleware
    println!("1️⃣  Validation Middleware");
    let validator = ValidationMiddleware::new()
        .with_max_body_size(10)
        .with_require_task_name(true);

    let mut large_message = Message::new(
        "large_task".to_string(),
        Uuid::new_v4(),
        vec![0u8; 100], // 100 bytes
    );

    match validator.before_publish(&mut large_message).await {
        Ok(_) => println!("   ✅ Message validated"),
        Err(e) => println!("   ❌ Validation failed: {}", e),
    }

    // 2. Retry Limit Middleware
    println!("\n2️⃣  Retry Limit Middleware");
    let retry_limiter = RetryLimitMiddleware::new(3);

    println!("   Max retries configured: 3");
    println!("   Middleware will reject messages that exceed retry limit");
    println!("   Note: Retry count tracking is implementation-specific");

    // 3. Rate Limiting Middleware
    println!("\n3️⃣  Rate Limiting Middleware");
    let rate_limiter = RateLimitingMiddleware::new(2.0); // 2 messages per second

    for i in 1..=3 {
        let mut msg = Message::new(format!("rate_limited_task_{}", i), Uuid::new_v4(), vec![]);
        match rate_limiter.before_publish(&mut msg).await {
            Ok(_) => println!("   ✅ Message {}: Published", i),
            Err(e) => println!("   ❌ Message {}: {}", i, e),
        }
    }

    // 4. Deduplication Middleware
    println!("\n4️⃣  Deduplication Middleware");
    let dedup = DeduplicationMiddleware::new(100);
    let dup_id = Uuid::new_v4();

    for i in 1..=3 {
        let mut msg = Message::new(
            "duplicate_task".to_string(),
            dup_id, // Same ID
            vec![],
        );
        match dedup.after_consume(&mut msg).await {
            Ok(_) => println!("   ✅ Attempt {}: New message", i),
            Err(e) => println!("   ❌ Attempt {}: {}", i, e),
        }
    }

    // 5. Message Options
    println!("\n5️⃣  Message Options");
    let options = MessageOptions::new()
        .with_priority(Priority::High)
        .with_ttl(std::time::Duration::from_secs(300))
        .with_correlation_id("correlation-123".to_string())
        .with_reply_to("reply_queue".to_string());

    println!("   Priority: {:?}", options.priority);
    println!("   TTL: {:?}", options.ttl);
    println!("   Correlation ID: {:?}", options.correlation_id);
    println!("   Reply To: {:?}", options.reply_to);

    println!("\n✨ Middleware example completed successfully!");
    Ok(())
}
