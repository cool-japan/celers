//! Example demonstrating distributed rate limiting across workers
//!
//! This example shows how to use distributed rate limiting to coordinate
//! rate limits across multiple worker instances using Redis.

use celers_worker::distributed_rate_limit::{
    DistributedRateLimitConfig, DistributedRateLimiterTrait, InMemoryDistributedRateLimiter,
};
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("=== Distributed Rate Limiting Examples ===\n");

    // Example 1: In-memory rate limiter (for testing without Redis)
    println!("1. In-Memory Distributed Rate Limiter");
    in_memory_example().await?;

    // Example 2: Redis-based distributed rate limiter
    #[cfg(feature = "redis")]
    {
        println!("\n2. Redis-Based Distributed Rate Limiter");
        redis_example().await?;
    }

    println!("\n=== Examples completed ===");
    Ok(())
}

async fn in_memory_example() -> Result<(), Box<dyn std::error::Error>> {
    let config = DistributedRateLimitConfig::default()
        .with_capacity(10)
        .with_refill_rate(2.0); // 2 tokens per second

    let limiter = InMemoryDistributedRateLimiter::new(config);

    println!("  Configuration:");
    println!("    - Capacity: 10 tokens");
    println!("    - Refill rate: 2 tokens/second");

    // Consume all tokens
    println!("\n  Consuming all 10 tokens:");
    for i in 1..=10 {
        let acquired = limiter.try_acquire("test_task", 1).await?;
        println!("    Token {}: {}", i, if acquired { "✓" } else { "✗" });
    }

    // Try to acquire when exhausted
    println!("\n  Trying to acquire when exhausted:");
    let acquired = limiter.try_acquire("test_task", 1).await?;
    println!(
        "    Result: {}",
        if acquired {
            "✓ Allowed"
        } else {
            "✗ Rejected"
        }
    );

    // Check available tokens
    let available = limiter.available_tokens("test_task").await?;
    println!("\n  Available tokens: {}", available);

    // Wait for refill
    println!("\n  Waiting 3 seconds for refill (should get ~6 tokens)...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    let available = limiter.available_tokens("test_task").await?;
    println!("  Available tokens after refill: {}", available);

    // Try acquiring again
    let acquired = limiter.try_acquire("test_task", 5).await?;
    println!("  Acquired 5 tokens: {}", if acquired { "✓" } else { "✗" });

    // Get statistics
    let stats = limiter.get_stats("test_task").await?;
    println!("\n  Statistics:");
    println!("    - Total requests: {}", stats.total_requests);
    println!("    - Allowed: {}", stats.allowed_requests);
    println!("    - Rejected: {}", stats.rejected_requests);
    println!("    - Rejection rate: {:.2}%", stats.rejection_rate * 100.0);
    println!("    - Available tokens: {}", stats.available_tokens);

    Ok(())
}

#[cfg(feature = "redis")]
async fn redis_example() -> Result<(), Box<dyn std::error::Error>> {
    use celers_worker::distributed_rate_limit::DistributedRateLimiter;

    let redis_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://127.0.0.1:6379".to_string());

    let config = DistributedRateLimitConfig::new(&redis_url)
        .with_capacity(20)
        .with_refill_rate(5.0) // 5 tokens per second
        .with_key_prefix("example:rate_limit");

    match DistributedRateLimiter::new(config).await {
        Ok(limiter) => {
            println!("  Connected to Redis at {}", redis_url);
            println!("\n  Configuration:");
            println!("    - Capacity: 20 tokens");
            println!("    - Refill rate: 5 tokens/second");

            // Reset to start fresh
            limiter.reset("test_task").await?;

            // Simulate multiple workers
            println!("\n  Simulating multiple workers acquiring tokens:");

            // Worker 1 acquires 10 tokens
            let acquired = limiter.try_acquire("test_task", 10).await?;
            println!(
                "    Worker 1: Acquired 10 tokens - {}",
                if acquired { "✓" } else { "✗" }
            );

            // Worker 2 tries to acquire 10 tokens (should have 10 remaining)
            let acquired = limiter.try_acquire("test_task", 10).await?;
            println!(
                "    Worker 2: Acquired 10 tokens - {}",
                if acquired { "✓" } else { "✗" }
            );

            // Worker 3 tries to acquire 5 tokens (should be rejected, exhausted)
            let acquired = limiter.try_acquire("test_task", 5).await?;
            println!(
                "    Worker 3: Acquired 5 tokens - {}",
                if acquired { "✓" } else { "✗ Rejected" }
            );

            // Check available tokens
            let available = limiter.available_tokens("test_task").await?;
            println!("\n  Available tokens: {}", available);

            // Wait for refill
            println!("\n  Waiting 2 seconds for refill (should get ~10 tokens)...");
            tokio::time::sleep(Duration::from_secs(2)).await;

            let available = limiter.available_tokens("test_task").await?;
            println!("  Available tokens after refill: {}", available);

            // Worker 4 can now acquire
            let acquired = limiter.try_acquire("test_task", 8).await?;
            println!(
                "  Worker 4: Acquired 8 tokens - {}",
                if acquired { "✓" } else { "✗" }
            );

            // Get statistics
            let stats = limiter.get_stats("test_task").await?;
            println!("\n  Statistics:");
            println!("    - Total requests: {}", stats.total_requests);
            println!("    - Allowed: {}", stats.allowed_requests);
            println!("    - Rejected: {}", stats.rejected_requests);
            println!("    - Rejection rate: {:.2}%", stats.rejection_rate * 100.0);
            println!("    - Available tokens: {}", stats.available_tokens);

            // Clean up
            limiter.reset("test_task").await?;
            println!("\n  Cleaned up Redis keys");
        }
        Err(e) => {
            println!("  Warning: Could not connect to Redis: {}", e);
            println!("  Skipping Redis example. Start Redis server to run this example.");
            println!("  Example: docker run -d -p 6379:6379 redis:latest");
        }
    }

    Ok(())
}
