//! Resilience features example
//!
//! Demonstrates:
//! - Circuit breaker protection
//! - Rate limiting
//! - Task deduplication
//! - Retry mechanisms
//! - Graceful degradation
//! - Connection pooling
//!
//! Run with: cargo run --example resilience_features
//! Requires: Redis running on localhost:6379

use celers_broker_redis::{
    BackoffStrategy, CircuitBreaker, CircuitBreakerConfig, DedupStrategy, QueueRateLimitConfig,
    RedisBroker, RetryConfig, RetryExecutor, TokenBucketLimiter,
};
use celers_core::{SerializedTask, TaskMetadata};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("=== Resilience Features Example ===\n");

    // Example 1: Circuit Breaker
    println!("1. Circuit Breaker Example");
    circuit_breaker_example().await?;

    // Example 2: Rate Limiting
    println!("\n2. Rate Limiting Example");
    rate_limiting_example().await?;

    // Example 3: Task Deduplication
    println!("\n3. Task Deduplication Example");
    deduplication_example().await?;

    // Example 4: Retry Mechanism
    println!("\n4. Retry Mechanism Example");
    retry_example().await?;

    println!("\n=== All resilience examples completed successfully! ===");
    Ok(())
}

async fn circuit_breaker_example() -> Result<(), Box<dyn std::error::Error>> {
    let config = CircuitBreakerConfig::new()
        .with_failure_threshold(3)
        .with_recovery_timeout(Duration::from_secs(5))
        .with_success_threshold(2);

    let breaker = CircuitBreaker::new(config);

    println!("  Initial state: {:?}", breaker.state());

    // Simulate failures
    println!("  Simulating 3 failures to open circuit...");
    for i in 1..=3 {
        breaker.record_failure();
        println!("    Failure {} - State: {:?}", i, breaker.state());
    }

    // Circuit should be open now
    if breaker.allow_request() {
        println!("  Request allowed (unexpected)");
    } else {
        println!("  Circuit is OPEN - request blocked");
    }

    // Get statistics
    let stats = breaker.stats();
    println!("  Circuit Breaker Stats:");
    println!("    Total failures: {}", stats.total_failures);
    println!("    Total successes: {}", stats.total_successes);
    println!("    Consecutive failures: {}", stats.consecutive_failures);

    // Manual reset
    println!("  Manually closing circuit...");
    breaker.force_close();
    println!("  State after reset: {:?}", breaker.state());

    Ok(())
}

async fn rate_limiting_example() -> Result<(), Box<dyn std::error::Error>> {
    // Create rate limiter: 5 operations per second, burst of 10
    let config = QueueRateLimitConfig::new(5.0)
        .with_burst(10)
        .with_window(Duration::from_secs(1));

    let limiter = TokenBucketLimiter::new(config);

    println!("  Rate limiter: 5 ops/sec, burst: 10");
    println!("  Available permits: {}", limiter.available_permits());

    // Try acquiring permits one at a time
    println!("  Attempting to acquire 8 permits...");
    let mut acquired = 0;
    for _ in 0..8 {
        if limiter.try_acquire() {
            acquired += 1;
        } else {
            break;
        }
    }
    println!("    Acquired {} permits", acquired);
    println!("    Remaining permits: {}", limiter.available_permits());

    // Try acquiring more (should fail)
    println!("  Attempting to acquire 5 more permits...");
    let mut more_acquired = 0;
    for _ in 0..5 {
        if limiter.try_acquire() {
            more_acquired += 1;
        } else {
            break;
        }
    }
    if more_acquired < 5 {
        println!(
            "    Rate limit exceeded! Only acquired {} more",
            more_acquired
        );
        println!(
            "    Time until next permit: {:?}",
            limiter.time_until_available()
        );
    }

    // Reset limiter
    limiter.reset();
    println!(
        "  Limiter reset. Available permits: {}",
        limiter.available_permits()
    );

    Ok(())
}

async fn deduplication_example() -> Result<(), Box<dyn std::error::Error>> {
    let broker = RedisBroker::new("redis://localhost:6379", "dedup_example")?;
    let deduplicator = broker.deduplicator();

    println!("  Testing task deduplication strategies...\n");

    // Strategy 1: By Task ID
    println!("  Strategy 1: By Task ID");
    let dedup_by_id = broker
        .deduplicator()
        .with_strategy(DedupStrategy::ById)
        .with_ttl(Duration::from_secs(60));

    let task1 = create_task("duplicate_test", 5, vec![1, 2, 3]);

    let result1 = dedup_by_id.check_and_mark(&task1).await?;
    println!("    First attempt (new): {}", result1);

    let result2 = dedup_by_id.check_and_mark(&task1).await?;
    println!("    Second attempt (duplicate): {}", result2);

    // Strategy 2: By Content
    println!("\n  Strategy 2: By Content Hash");
    let dedup_by_content = broker
        .deduplicator()
        .with_strategy(DedupStrategy::ByContent)
        .with_ttl(Duration::from_secs(60));

    let task2 = create_task("content_test", 5, vec![4, 5, 6]);
    let task3 = create_task("content_test", 5, vec![4, 5, 6]); // Same content, different ID

    let result3 = dedup_by_content.check_and_mark(&task2).await?;
    println!("    First task (new): {}", result3);

    let result4 = dedup_by_content.check_and_mark(&task3).await?;
    println!("    Duplicate content (duplicate): {}", result4);

    // Check dedup count
    let count = deduplicator.count().await?;
    println!("\n  Total dedup entries: {}", count);

    // Cleanup
    deduplicator.clear().await?;
    broker.purge_all_queues().await?;

    Ok(())
}

async fn retry_example() -> Result<(), Box<dyn std::error::Error>> {
    println!("  Testing retry strategies...\n");

    // Exponential backoff
    println!("  Exponential Backoff:");
    let strategy = BackoffStrategy::exponential(Duration::from_millis(100), Duration::from_secs(5));
    for attempt in 1..=5 {
        let delay = strategy.delay_for_attempt(attempt, None);
        println!("    Attempt {}: delay = {:?}", attempt, delay);
    }

    // Exponential with jitter
    println!("\n  Exponential with Jitter:");
    let strategy = BackoffStrategy::exponential_with_jitter(
        Duration::from_millis(100),
        Duration::from_secs(5),
    );
    for attempt in 1..=5 {
        let delay = strategy.delay_for_attempt(attempt, None);
        println!("    Attempt {}: delay = {:?}", attempt, delay);
    }

    // Linear backoff
    println!("\n  Linear Backoff:");
    let strategy = BackoffStrategy::linear(Duration::from_millis(500), Duration::from_secs(5));
    for attempt in 1..=5 {
        let delay = strategy.delay_for_attempt(attempt, None);
        println!("    Attempt {}: delay = {:?}", attempt, delay);
    }

    // Retry executor with aggressive config
    println!("\n  Retry Executor (Aggressive Config):");
    let config = RetryConfig::aggressive();
    let executor = RetryExecutor::new(config);

    use std::sync::atomic::{AtomicU32, Ordering};
    use std::sync::Arc;

    let attempt_count = Arc::new(AtomicU32::new(0));
    let attempt_count_clone = attempt_count.clone();

    let operation = move || {
        let attempt_count = attempt_count_clone.clone();
        async move {
            let count = attempt_count.fetch_add(1, Ordering::SeqCst) + 1;
            println!("    Operation attempt: {}", count);
            if count < 3 {
                Err(std::io::Error::new(
                    std::io::ErrorKind::ConnectionRefused,
                    "Connection failed",
                ))
            } else {
                Ok("Success!")
            }
        }
    };

    let retry_result = executor.execute(operation).await;
    println!("    Total attempts: {}", retry_result.attempts);
    println!("    Total duration: {:?}", retry_result.total_duration);

    match retry_result.result {
        Ok(value) => {
            println!("    Result: {}", value);
        }
        Err(e) => println!("    Failed after retries: {}", e),
    }

    Ok(())
}

fn create_task(name: &str, priority: i32, payload: Vec<u8>) -> SerializedTask {
    let mut metadata = TaskMetadata::new(name.to_string());
    metadata.priority = priority;
    SerializedTask { metadata, payload }
}
