# CeleRS Redis Broker Examples

This directory contains example code demonstrating key features of celers-broker-redis.

## Running Examples

All examples require a running Redis instance:

```bash
# Start Redis using Docker
docker run -d -p 6379:6379 --name redis-example redis:7-alpine

# Run an example (when available)
cargo run --example example_name

# Stop Redis
docker stop redis-example && docker rm redis-example
```

## Example Topics

### Basic Usage
- Creating a Redis broker
- Enqueuing and dequeuing tasks
- Using FIFO vs Priority queues
- Batch operations

### Security Features
- TLS/SSL configuration
- ACL token authentication
- Authorization policies
- User permissions
- Command restrictions

### High Availability
- Redis Sentinel configuration
- Automatic master discovery
- Failover handling
- Health monitoring

### Advanced Queues
- Weighted queue selection
- Priority aging
- Starvation prevention
- Queue partitioning
- Rate limiting

## Documentation

For comprehensive guides, see:
- Redis Configuration Guide (in module documentation)
- Scaling Recommendations (in module documentation)

## Code Snippets

### Basic Broker Setup

```rust
use celers_broker_redis::{RedisBroker, QueueMode};

// FIFO queue
let broker = RedisBroker::new("redis://localhost:6379", "my_queue")?;

// Priority queue
let broker = RedisBroker::with_mode(
    "redis://localhost:6379",
    "priority_queue",
    QueueMode::Priority
)?;
```

### TLS Configuration

```rust
use celers_broker_redis::{RedisConfig, TlsConfig};

let tls = TlsConfig::new()
    .enabled(true)
    .ca_cert("/path/to/ca.crt")
    .client_cert("/path/to/client.crt", "/path/to/client.key")
    .cipher_suites("TLS_AES_256_GCM_SHA384")
    .min_tls_version("1.3");

let config = RedisConfig::new()
    .url("rediss://redis.example.com:6380")
    .tls(tls)
    .acl_token("my-secure-token");
```

### Authorization

```rust
use celers_broker_redis::AuthorizationPolicy;

// Preset policies
let read_only = AuthorizationPolicy::read_only();
let queue_only = AuthorizationPolicy::queue_only();
let restricted = AuthorizationPolicy::restricted();

// Custom policy
let custom = AuthorizationPolicy::new()
    .allow_commands(&["GET", "SET", "LPUSH"])
    .deny_commands(&["FLUSHDB"])
    .key_namespace("app:prod:")
    .max_key_length(256)
    .max_value_size(10 * 1024 * 1024);
```

### High Availability with Sentinel

```rust
use celers_broker_redis::{SentinelClient, SentinelConfig, RedisBroker, QueueMode};

let sentinel_config = SentinelConfig::new("mymaster")
    .sentinels(vec![
        "redis://sentinel1:26379".to_string(),
        "redis://sentinel2:26379".to_string(),
        "redis://sentinel3:26379".to_string(),
    ]);

let sentinel = SentinelClient::new(sentinel_config).await?;
let broker = RedisBroker::from_sentinel(
    &sentinel,
    "ha_queue",
    QueueMode::Fifo
).await?;
```

### Queue Partitioning

```rust
use celers_broker_redis::PartitionStrategy;

let broker = RedisBroker::new("redis://localhost:6379", "base_queue")?;

let partition_manager = broker.partition_manager(
    8,  // 8 partitions
    PartitionStrategy::ByTaskId,
);

// Get partition for a task
let partition_id = partition_manager.get_partition_id(&task_id);
let queue_name = partition_manager.get_queue_name(partition_id);
```

### Rate Limiting

```rust
use celers_broker_redis::{TokenBucketLimiter, QueueRateLimitConfig};

let config = QueueRateLimitConfig::new(100.0)  // 100 ops/sec
    .with_burst(200)
    .with_window(Duration::from_secs(60));

let limiter = TokenBucketLimiter::new(config);

if limiter.try_acquire(10).await {
    // Proceed with operation
}
```

## Further Reading

- [CeleRS Core Documentation](../../celers-core/README.md)
- [TODO.md](../TODO.md) - Feature status and roadmap
