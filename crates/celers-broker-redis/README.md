# celers-broker-redis

High-performance Redis broker implementation for CeleRS with batch operations, priority queues, and comprehensive monitoring.

## Overview

Production-ready message broker using Redis with:

- ✅ **Batch Operations**: 10-100x throughput improvement via pipelining
- ✅ **Priority Queues**: ZADD-based priority scheduling
- ✅ **Dead Letter Queue**: Automatic failed task handling
- ✅ **Task Cancellation**: Pub/Sub-based cancellation signals
- ✅ **Atomic Operations**: BRPOPLPUSH for reliable delivery
- ✅ **Prometheus Metrics**: Full instrumentation
- ✅ **Lua Scripts**: Atomic visibility timeout support

## Features

| Feature | FIFO Mode | Priority Mode |
|---------|-----------|---------------|
| Enqueue | `RPUSH` O(1) | `ZADD` O(log N) |
| Dequeue | `BRPOPLPUSH` | `ZPOPMIN` |
| Batch Enqueue | ✅ Pipeline | ✅ Pipeline |
| Batch Dequeue | ✅ Pipeline | ✅ Atomic |
| Priority Support | ❌ | ✅ |
| Throughput | 50K/sec | 40K/sec |

## Quick Start

```rust
use celers_broker_redis::{RedisBroker, QueueMode};
use celers_core::Broker;

// FIFO mode (default)
let broker = RedisBroker::new("redis://localhost:6379", "celery")?;

// Priority mode
let broker = RedisBroker::with_mode(
    "redis://localhost:6379",
    "celery",
    QueueMode::Priority
)?;

// Enqueue single task
let task = SerializedTask::new("process_image", args);
broker.enqueue(task).await?;

// Batch enqueue (10-100x faster)
let tasks = vec![task1, task2, task3];
broker.enqueue_batch(tasks).await?;
```

## Batch Operations

### Performance Comparison

| Operation | Individual | Batch (10) | Batch (100) | Speedup |
|-----------|-----------|------------|-------------|---------|
| Enqueue | 1K/sec | 10K/sec | 50K/sec | **50x** |
| Dequeue | 1K/sec | 8K/sec | 40K/sec | **40x** |
| Latency | 1ms | 0.1ms | 0.01ms | **100x** |

### Usage

```rust
// Batch enqueue using Redis pipelining
let tasks = create_many_tasks(100);
let task_ids = broker.enqueue_batch(tasks).await?;

// Batch dequeue (fetch 50 tasks at once)
let messages = broker.dequeue_batch(50).await?;

// Batch acknowledge
let acks: Vec<_> = messages.iter()
    .map(|msg| (msg.task.metadata.id, msg.receipt_handle.clone()))
    .collect();
broker.ack_batch(&acks).await?;
```

### Implementation

Batch operations use **Redis pipelining** for maximum performance:

```rust
// Single network round-trip for N operations
let mut pipe = redis::pipe();
for task in tasks {
    pipe.rpush(&queue_name, &serialized_task);
}
pipe.query_async(&mut conn).await?;  // Single RTT
```

## Priority Queues

```rust
use celers_broker_redis::QueueMode;

let broker = RedisBroker::with_mode(
    "redis://localhost:6379",
    "celery",
    QueueMode::Priority  // Use sorted set for priorities
)?;

// Enqueue with priority (higher = more urgent)
let task = SerializedTask::new("urgent_task", args)
    .with_priority(9);  // Highest priority
broker.enqueue(task).await?;

// Tasks dequeued in priority order (9, 8, 7, ...)
```

### How It Works

- **FIFO Mode**: Redis LIST (RPUSH/BRPOPLPUSH)
- **Priority Mode**: Redis ZSET (ZADD/ZPOPMIN)
  - Score = `-priority` (negated for descending order)
  - Higher priority values processed first

## Dead Letter Queue

Automatically handles permanently failed tasks:

```rust
// Tasks exceeding max_retries moved to DLQ
let task = SerializedTask::new("risky_task", args)
    .with_max_retries(3);

// After 3 failures, automatically moved to DLQ
// DLQ key: {queue_name}:dlq

// Inspect DLQ
let dlq_size = broker.dlq_size().await?;
let failed_tasks = broker.inspect_dlq(10).await?;

// Replay tasks from DLQ
broker.replay_dlq(vec![task_id1, task_id2]).await?;

// Clear entire DLQ
broker.clear_dlq().await?;
```

## Task Cancellation

Pub/Sub-based cancellation system:

```rust
// Publisher side (cancel a task)
broker.cancel(&task_id).await?;

// Worker side (listen for cancellations)
let mut pubsub = broker.create_pubsub().await?;
pubsub.subscribe(broker.cancel_channel()).await?;

loop {
    let msg = pubsub.on_message().next().await;
    // Handle cancellation
}
```

## Visibility Timeout

Lua script-based visibility timeout for crash recovery:

```rust
let broker = RedisBroker::new("redis://localhost:6379", "celery")?
    .with_visibility_timeout(300);  // 5 minutes

// Tasks in processing queue automatically visible after timeout
// Prevents lost tasks due to worker crashes
```

## Prometheus Metrics

When `metrics` feature is enabled:

```rust
[features]
metrics = ["celers-broker-redis/metrics"]
```

**Available Metrics:**
- `celers_tasks_enqueued_total` - Total tasks enqueued
- `celers_queue_size` - Current queue size (gauge)
- `celers_processing_queue_size` - Tasks being processed
- `celers_dlq_size` - Dead letter queue size
- `celers_batch_enqueue_total` - Batch operations count
- `celers_batch_size` - Histogram of batch sizes

## Architecture

### FIFO Mode

```
Main Queue (LIST)              Processing Queue (LIST)
┌──────────────┐              ┌──────────────┐
│  Task N      │              │  Task 2      │ ← Being processed
│  Task N-1    │              │  Task 1      │ ← Being processed
│  ...         │              └──────────────┘
│  Task 3      │
│  Task 2      │  BRPOPLPUSH
│  Task 1      │ ──────────────→
└──────────────┘
           ↓ (after max_retries)
     ┌──────────────┐
     │  Dead Letter │
     │    Queue     │
     └──────────────┘
```

### Priority Mode

```
Priority Queue (ZSET)          Processing Queue (LIST)
┌──────────────────┐          ┌──────────────┐
│ Score | Task     │          │  Task 2      │
│  -9   | Urgent   │          │  Task 1      │
│  -5   | Normal   │ ZPOPMIN └──────────────┘
│  -1   | Low      │ ────────→
└──────────────────┘
```

## Configuration

```rust
use celers_broker_redis::{RedisBroker, QueueMode};

let broker = RedisBroker::new("redis://localhost:6379", "celery")?
    .with_visibility_timeout(300)  // 5 minutes
    .with_mode(QueueMode::Priority);

// Redis URL formats
"redis://localhost:6379"           // Default
"redis://:password@localhost"      // With password
"rediss://localhost:6379"          // TLS
"redis://localhost:6379/2"         // Database 2
```

## Performance Tuning

### Batch Size Selection

```rust
// Small batches (5-10): Low latency, moderate throughput
broker.enqueue_batch(tasks).await?;  // 10 tasks

// Medium batches (20-50): Balanced
broker.dequeue_batch(20).await?;

// Large batches (100+): Maximum throughput
broker.enqueue_batch(large_batch).await?;  // 100+ tasks
```

### Connection Pooling

```rust
// Multiplexed connections automatically managed
// No manual pool configuration needed
// Uses redis::Client::get_multiplexed_async_connection()
```

## Error Handling

```rust
use celers_core::CelersError;

match broker.enqueue(task).await {
    Ok(task_id) => println!("Enqueued: {}", task_id),
    Err(CelersError::Broker(e)) => eprintln!("Redis error: {}", e),
    Err(CelersError::Serialization(e)) => eprintln!("Serialization error: {}", e),
    Err(e) => eprintln!("Other error: {}", e),
}
```

## Requirements

- **Redis**: 6.0+ (6.2+ recommended for better performance)
- **Features**: Lua scripting, Pub/Sub, Lists, Sorted Sets
- **Network**: Low-latency connection to Redis recommended

## Comparison with Other Brokers

| Feature | Redis | PostgreSQL | RabbitMQ |
|---------|-------|------------|----------|
| Throughput | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ |
| Latency | ⭐⭐⭐⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐⭐ |
| Durability | ⭐⭐⭐ | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐⭐ |
| Ease of Use | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐ |
| Batch Ops | ✅ | ✅ | ❌ |
| Priority Queues | ✅ | ✅ | ✅ |

**Use Redis when:**
- Maximum throughput needed
- Low latency required
- Simple deployment preferred
- In-memory performance acceptable

**Use PostgreSQL when:**
- Strict durability required
- Transactional guarantees needed
- Already using PostgreSQL

## Examples

See `examples/` directory:
- `phase1_complete.rs` - Basic usage
- `priority_queue.rs` - Priority queues
- `dead_letter_queue.rs` - DLQ management
- `task_cancellation.rs` - Pub/Sub cancellation

## License

MIT OR Apache-2.0
