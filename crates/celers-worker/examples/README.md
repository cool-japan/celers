# CeleRS Worker Examples

This directory contains examples demonstrating various worker features and configurations.

## Configuration Examples

### Basic Configuration

```rust
use celers_worker::WorkerConfig;

let config = WorkerConfig::for_production();
// High concurrency, low poll interval, production-ready settings
```

### Environment-Based Configuration

```rust
// Auto-detect environment from CELERS_ENV env var
let config = WorkerConfig::from_env();

// Or use specific presets
let dev_config = WorkerConfig::for_development();
let staging_config = WorkerConfig::for_staging();
let prod_config = WorkerConfig::for_production();
```

### Advanced Features

```rust
use celers_worker::{
    CircuitBreakerConfig, DlqConfig, RateLimitConfig,
    WorkerConfig, WorkerTags
};

let config = WorkerConfig {
    // High throughput
    concurrency: 50,
    enable_batch_dequeue: true,
    batch_size: 20,

    // Circuit breaker
    enable_circuit_breaker: true,

    // Dead letter queue
    enable_dlq: true,

    // Worker routing
    enable_routing: true,
    worker_tags: WorkerTags::new()
        .with_tag("gpu-enabled")
        .with_capability("region", "us-west-2"),

    ..Default::default()
};
```

## Feature Showcases

### Health Checks

```rust
use celers_worker::health::HealthChecker;

let checker = HealthChecker::new();
checker.record_success();
let health = checker.get_health();
println!("Status: {}", health.status);
```

### Performance Metrics

```rust
use celers_worker::{PerformanceConfig, PerformanceTracker};

let config = PerformanceConfig {
    sample_window_secs: 60,
    percentile_window_size: 1000,
};
let tracker = PerformanceTracker::new(config);

// Track task execution
tracker.record_task_start().await;
// ... task execution ...
tracker.record_task_completion(duration, "task-type", true).await;

// Get statistics
let stats = tracker.get_stats().await;
println!("Throughput: {:.2} tasks/sec", stats.tasks_per_second);
```

### Retry Strategies

```rust
use celers_worker::{RetryConfig, RetryStrategy};
use std::time::Duration;

// Exponential backoff
let exponential = RetryConfig {
    max_retries: 5,
    strategy: RetryStrategy::Exponential {
        base_delay: Duration::from_millis(1000),
        max_delay: Duration::from_millis(60000),
        multiplier: 2.0,
    },
    jitter: true,
    jitter_fraction: 0.1,
};

// Linear backoff
let linear = RetryConfig {
    max_retries: 3,
    strategy: RetryStrategy::Linear {
        initial_delay: Duration::from_secs(2),
        increment: Duration::from_secs(3),
        max_delay: Duration::from_secs(15),
    },
    jitter: false,
    jitter_fraction: 0.0,
};
```

### Worker Metadata

```rust
use celers_worker::WorkerMetadata;

let metadata = WorkerMetadata::builder()
    .version("1.2.3")
    .build_info("git-abc123")
    .environment("production")
    .region("us-west-2")
    .add_label("team", "platform")
    .build();
```

## Running Examples

For full working examples, see the integration tests in `tests/` directory.

To run examples with a real broker:

```bash
# Start Redis
docker run -d -p 6379:6379 redis:latest

# Run worker with Redis broker
cargo run --features redis --example worker_with_broker
```
