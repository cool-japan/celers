# celers-backend-redis

Redis-based result backend for CeleRS task result storage and workflow state management. Provides atomic operations for Chord barrier synchronization.

## Overview

Production-ready result backend with:

- ✅ **Task Result Storage**: Store and retrieve task results
- ✅ **Chord State Management**: Atomic barrier synchronization for map-reduce
- ✅ **Result Expiration**: TTL support for automatic cleanup
- ✅ **Atomic Operations**: Redis INCR for thread-safe counters
- ✅ **Task Metadata**: Complete task lifecycle tracking
- ✅ **Multiple States**: Pending, Started, Success, Failure, Retry, Revoked
- ✅ **Multiplexed Connections**: Efficient async connection pooling

## Quick Start

```rust
use celers_backend_redis::{RedisResultBackend, ResultBackend, TaskMeta, TaskResult};
use uuid::Uuid;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create backend
    let mut backend = RedisResultBackend::new("redis://localhost:6379")?;

    // Store task result
    let task_id = Uuid::new_v4();
    let mut meta = TaskMeta::new(task_id, "my_task".to_string());
    meta.result = TaskResult::Success(serde_json::json!({"value": 42}));

    backend.store_result(task_id, &meta).await?;

    // Retrieve result
    if let Some(stored_meta) = backend.get_result(task_id).await? {
        println!("Task result: {:?}", stored_meta.result);
    }

    // Set expiration (results auto-delete after 1 hour)
    backend.set_expiration(task_id, Duration::from_secs(3600)).await?;

    Ok(())
}
```

## Task Result States

### Available States

```rust
use celers_backend_redis::TaskResult;

// Task is pending execution
let pending = TaskResult::Pending;

// Task is currently running
let started = TaskResult::Started;

// Task completed successfully
let success = TaskResult::Success(serde_json::json!({
    "result": "data",
    "count": 100
}));

// Task failed with error
let failure = TaskResult::Failure("Division by zero".to_string());

// Task was cancelled/revoked
let revoked = TaskResult::Revoked;

// Task retry scheduled (retry count = 3)
let retry = TaskResult::Retry(3);
```

### State Transitions

```
Pending ──> Started ──> Success ✓
                   └──> Failure ✗
                   └──> Retry ↻ ──> Started (again)
                   └──> Revoked ✗

✓ = Final state (success)
✗ = Final state (error)
↻ = Retry loop
```

## Task Metadata

### Structure

```rust
use celers_backend_redis::TaskMeta;
use chrono::Utc;

let task_id = Uuid::new_v4();
let mut meta = TaskMeta::new(task_id, "process_data".to_string());

// Update metadata throughout lifecycle
meta.started_at = Some(Utc::now());
meta.worker = Some("worker-1".to_string());
meta.result = TaskResult::Started;

// On completion
meta.completed_at = Some(Utc::now());
meta.result = TaskResult::Success(serde_json::json!({"processed": 1000}));
```

### Fields

| Field | Type | Description |
|-------|------|-------------|
| `task_id` | Uuid | Unique task identifier |
| `task_name` | String | Task name (e.g., "process_image") |
| `result` | TaskResult | Current task state/result |
| `created_at` | DateTime<Utc> | When task was created |
| `started_at` | Option<DateTime<Utc>> | When task started executing |
| `completed_at` | Option<DateTime<Utc>> | When task completed |
| `worker` | Option<String> | Worker that executed the task |

## Basic Operations

### Store Result

```rust
use celers_backend_redis::{RedisResultBackend, TaskMeta, TaskResult};

let mut backend = RedisResultBackend::new("redis://localhost:6379")?;

let task_id = Uuid::new_v4();
let mut meta = TaskMeta::new(task_id, "my_task".to_string());
meta.result = TaskResult::Success(serde_json::json!("result data"));

backend.store_result(task_id, &meta).await?;
```

**Redis key**: `celery-task-meta-{task_id}`

### Get Result

```rust
match backend.get_result(task_id).await? {
    Some(meta) => {
        match meta.result {
            TaskResult::Success(value) => println!("Success: {:?}", value),
            TaskResult::Failure(err) => eprintln!("Failed: {}", err),
            TaskResult::Pending => println!("Still pending..."),
            _ => println!("Other state: {:?}", meta.result),
        }
    }
    None => println!("Result not found"),
}
```

### Delete Result

```rust
backend.delete_result(task_id).await?;
```

### Set Expiration (TTL)

```rust
use std::time::Duration;

// Result expires after 1 hour
backend.set_expiration(task_id, Duration::from_secs(3600)).await?;

// Result expires after 24 hours
backend.set_expiration(task_id, Duration::from_secs(86400)).await?;
```

**Redis command**: `EXPIRE celery-task-meta-{task_id} {seconds}`

## Chord Barrier Synchronization

The backend provides atomic operations for Chord (map-reduce) patterns.

### How Chord Works

```
Header Tasks (parallel):
┌─────────┐  ┌─────────┐  ┌─────────┐
│ Task 1  │  │ Task 2  │  │ Task 3  │
└────┬────┘  └────┬────┘  └────┬────┘
     │            │            │
     │    ┌───────▼────────┐   │
     │    │  Redis Counter │   │  (Atomic INCR)
     │    │    0 → 1 → 2   │   │
     │    └───────┬────────┘   │
     │            │            │
     └────────────┼────────────┘
                  │
                  ▼ (When count == 3)
          ┌──────────────┐
          │ Callback Task│  (Aggregate results)
          └──────────────┘
```

### Initialize Chord

```rust
use celers_backend_redis::{ChordState, RedisResultBackend};
use uuid::Uuid;

let mut backend = RedisResultBackend::new("redis://localhost:6379")?;

let chord_id = Uuid::new_v4();
let state = ChordState {
    chord_id,
    total: 3,                          // 3 tasks in header
    completed: 0,                      // None completed yet
    callback: Some("aggregate".to_string()),  // Callback task
    task_ids: vec![],                  // Optional task ID tracking
};

backend.chord_init(state).await?;
```

**Redis keys created:**
- `celery-chord-{chord_id}`: Chord state (JSON)
- `celery-chord-counter-{chord_id}`: Atomic counter (integer, initialized to 0)

### Complete Task (Increment Counter)

```rust
// Called by worker when task completes
let count = backend.chord_complete_task(chord_id).await?;
println!("Tasks completed: {}", count);

// When count == state.total, enqueue callback
if count >= state.total {
    // Trigger callback task
    println!("Chord complete! Enqueuing callback...");
}
```

**Redis command**: `INCR celery-chord-counter-{chord_id}` (atomic)

**Thread-safety**: Multiple workers can complete tasks simultaneously without race conditions.

### Get Chord State

```rust
if let Some(state) = backend.chord_get_state(chord_id).await? {
    println!("Total tasks: {}", state.total);
    println!("Callback: {:?}", state.callback);
}
```

## Chord State Structure

```rust
pub struct ChordState {
    /// Chord ID (group ID)
    pub chord_id: Uuid,

    /// Total number of tasks in chord
    pub total: usize,

    /// Number of completed tasks
    pub completed: usize,

    /// Callback task to execute when chord completes
    pub callback: Option<String>,

    /// Task IDs in the chord
    pub task_ids: Vec<Uuid>,
}
```

## Configuration

### Custom Key Prefix

```rust
let backend = RedisResultBackend::new("redis://localhost:6379")?
    .with_prefix("myapp-task-".to_string());

// Results stored at: myapp-task-{task_id}
```

### Redis URL Formats

```rust
// Basic
let backend = RedisResultBackend::new("redis://localhost:6379")?;

// With password
let backend = RedisResultBackend::new("redis://:password@localhost:6379")?;

// TLS
let backend = RedisResultBackend::new("rediss://localhost:6379")?;

// Specific database
let backend = RedisResultBackend::new("redis://localhost:6379/2")?;

// Unix socket
let backend = RedisResultBackend::new("redis+unix:///tmp/redis.sock")?;
```

## Use Cases

### 1. Task Result Polling

```rust
use tokio::time::{sleep, Duration};

async fn wait_for_result(
    backend: &mut RedisResultBackend,
    task_id: Uuid,
) -> Result<serde_json::Value, String> {
    loop {
        if let Some(meta) = backend.get_result(task_id).await? {
            match meta.result {
                TaskResult::Success(value) => return Ok(value),
                TaskResult::Failure(err) => return Err(err),
                TaskResult::Pending | TaskResult::Started => {
                    // Keep polling
                }
                TaskResult::Retry(count) => {
                    println!("Task retrying (attempt {})", count);
                }
                TaskResult::Revoked => return Err("Task was cancelled".to_string()),
            }
        }
        sleep(Duration::from_millis(500)).await;
    }
}
```

### 2. Task Lifecycle Tracking

```rust
async fn track_task_lifecycle(task_id: Uuid) {
    let mut backend = RedisResultBackend::new("redis://localhost:6379").unwrap();

    // Create task
    let mut meta = TaskMeta::new(task_id, "long_running_task".to_string());
    backend.store_result(task_id, &meta).await.unwrap();

    // Mark started
    meta.started_at = Some(Utc::now());
    meta.worker = Some("worker-01".to_string());
    meta.result = TaskResult::Started;
    backend.store_result(task_id, &meta).await.unwrap();

    // Mark completed
    meta.completed_at = Some(Utc::now());
    meta.result = TaskResult::Success(serde_json::json!({"count": 1000}));
    backend.store_result(task_id, &meta).await.unwrap();

    // Set expiration
    backend.set_expiration(task_id, Duration::from_secs(3600)).await.unwrap();
}
```

### 3. Chord Map-Reduce

```rust
use celers_backend_redis::{RedisResultBackend, ChordState};
use celers_core::Broker;

async fn map_reduce_workflow<B: Broker>(
    broker: &B,
    backend: &mut RedisResultBackend,
) -> Result<(), Box<dyn std::error::Error>> {
    // 1. Initialize chord
    let chord_id = Uuid::new_v4();
    let state = ChordState {
        chord_id,
        total: 3,
        completed: 0,
        callback: Some("aggregate_results".to_string()),
        task_ids: vec![],
    };
    backend.chord_init(state).await?;

    // 2. Enqueue header tasks (parallel)
    for i in 0..3 {
        let mut task = celers_core::SerializedTask::new(
            "compute_partial".to_string(),
            serde_json::to_vec(&serde_json::json!({"chunk": i}))?,
        );
        task.metadata.chord_id = Some(chord_id);
        broker.enqueue(task).await?;
    }

    // 3. Workers complete tasks and call chord_complete_task()
    // 4. When count == 3, callback is enqueued

    Ok(())
}
```

### 4. Result Expiration Strategy

```rust
use std::time::Duration;

async fn smart_expiration(
    backend: &mut RedisResultBackend,
    task_id: Uuid,
    meta: &TaskMeta,
) {
    match meta.result {
        // Keep successful results for 24 hours
        TaskResult::Success(_) => {
            backend.set_expiration(task_id, Duration::from_secs(86400)).await.unwrap();
        }
        // Keep failures for 7 days (for debugging)
        TaskResult::Failure(_) => {
            backend.set_expiration(task_id, Duration::from_secs(604800)).await.unwrap();
        }
        // Temporary states expire quickly
        TaskResult::Pending | TaskResult::Started => {
            backend.set_expiration(task_id, Duration::from_secs(3600)).await.unwrap();
        }
        _ => {}
    }
}
```

## Error Handling

```rust
use celers_backend_redis::{RedisResultBackend, BackendError};

match backend.store_result(task_id, &meta).await {
    Ok(()) => println!("Stored successfully"),
    Err(BackendError::Redis(e)) => eprintln!("Redis error: {}", e),
    Err(BackendError::Serialization(e)) => eprintln!("Serialization error: {}", e),
    Err(BackendError::NotFound(id)) => eprintln!("Task {} not found", id),
    Err(BackendError::Connection(e)) => eprintln!("Connection error: {}", e),
}
```

**Error Types:**
- `Redis`: Underlying Redis client errors
- `Serialization`: JSON encoding/decoding errors
- `NotFound`: Task result doesn't exist
- `Connection`: Failed to connect to Redis

## Performance

### Connection Pooling

- Uses multiplexed async connections via `redis::Client::get_multiplexed_async_connection()`
- Connections automatically reused
- No manual pool management required

### Atomic Operations

- **Chord counter**: `INCR` command (O(1), atomic)
- **Store/Get**: `SET`/`GET` commands (O(1))
- **Delete**: `DEL` command (O(1))
- **Expiration**: `EXPIRE` command (O(1))

### Throughput

| Operation | Latency | Notes |
|-----------|---------|-------|
| Store result | <1ms | Depends on network RTT |
| Get result | <1ms | Depends on network RTT |
| Chord increment | <1ms | Atomic operation |
| Set expiration | <1ms | Same as SET |

**Optimization tips:**
- Use pipelining for batch operations
- Set appropriate TTLs to prevent memory bloat
- Co-locate Redis and workers for low latency

## Redis Memory Usage

### Per Task

| Component | Size | Notes |
|-----------|------|-------|
| Task metadata | ~500B - 2KB | Depends on result size |
| Chord state | ~200B | Per chord, not per task |
| Chord counter | ~16B | Integer value |

### Example Calculation

**1 million tasks:**
- Average task metadata: 1KB
- Total: ~1GB
- With 24h TTL: ~42K tasks/hour = 42MB/hour steady state

**1000 active chords:**
- Chord state: 200B × 1000 = 200KB
- Chord counters: 16B × 1000 = 16KB
- Total: ~220KB

## Best Practices

### 1. Always Set TTL

```rust
// Store result
backend.store_result(task_id, &meta).await?;

// Set expiration (required!)
backend.set_expiration(task_id, Duration::from_secs(86400)).await?;
```

**Why:** Prevents unbounded memory growth in Redis.

### 2. Handle Missing Results

```rust
match backend.get_result(task_id).await? {
    Some(meta) => { /* process result */ }
    None => {
        // Result expired or never stored
        eprintln!("Result not available for task {}", task_id);
    }
}
```

### 3. Chord Cleanup

```rust
// After chord completes, clean up state
backend.delete_result(chord_id).await?;

// Or set TTL on chord state
backend.set_expiration(chord_id, Duration::from_secs(3600)).await?;
```

### 4. Error Recovery

```rust
// Retry on transient errors
let mut retries = 3;
loop {
    match backend.store_result(task_id, &meta).await {
        Ok(()) => break,
        Err(BackendError::Redis(e)) if retries > 0 => {
            retries -= 1;
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        Err(e) => return Err(e),
    }
}
```

## Celery Compatibility

### Key Naming

Compatible with Celery's default key naming:
- Task results: `celery-task-meta-{task_id}`
- Chord state: `celery-chord-{chord_id}`
- Chord counter: `celery-chord-counter-{chord_id}`

### Result Format

Task metadata format matches Celery's backend structure:
```json
{
  "task_id": "550e8400-e29b-41d4-a716-446655440000",
  "task_name": "myapp.tasks.process_data",
  "result": {
    "Success": {"value": 42}
  },
  "created_at": "2023-01-01T12:00:00Z",
  "started_at": "2023-01-01T12:00:01Z",
  "completed_at": "2023-01-01T12:00:05Z",
  "worker": "worker-01"
}
```

## Testing

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_store_and_get_result() {
        let mut backend = RedisResultBackend::new("redis://localhost:6379").unwrap();
        let task_id = Uuid::new_v4();
        let mut meta = TaskMeta::new(task_id, "test".to_string());
        meta.result = TaskResult::Success(serde_json::json!(42));

        backend.store_result(task_id, &meta).await.unwrap();
        let retrieved = backend.get_result(task_id).await.unwrap().unwrap();

        assert_eq!(retrieved.task_id, task_id);
        assert!(matches!(retrieved.result, TaskResult::Success(_)));
    }

    #[tokio::test]
    async fn test_chord_barrier() {
        let mut backend = RedisResultBackend::new("redis://localhost:6379").unwrap();
        let chord_id = Uuid::new_v4();

        let state = ChordState {
            chord_id,
            total: 3,
            completed: 0,
            callback: Some("callback".to_string()),
            task_ids: vec![],
        };
        backend.chord_init(state).await.unwrap();

        assert_eq!(backend.chord_complete_task(chord_id).await.unwrap(), 1);
        assert_eq!(backend.chord_complete_task(chord_id).await.unwrap(), 2);
        assert_eq!(backend.chord_complete_task(chord_id).await.unwrap(), 3);
    }
}
```

## Troubleshooting

### Results disappearing

**Cause:** TTL expired
**Solution:** Increase TTL or retrieve results faster

### Chord callback not triggered

**Cause:** Counter state lost (Redis restart)
**Solution:** Enable Redis persistence (AOF or RDB)

### Slow result retrieval

**Cause:** Network latency
**Solution:** Co-locate Redis with workers, use connection pooling

### Memory usage growing

**Cause:** Missing TTL on results
**Solution:** Always call `set_expiration()` after storing results

## See Also

- **Canvas**: `celers-canvas` - Chord workflow primitives
- **Worker**: `celers-worker` - Worker integration with result backend
- **Core**: `celers-core` - Task types and traits

## Requirements

- **Redis**: 6.0+ (6.2+ recommended)
- **Features**: INCR, SET, GET, EXPIRE commands
- **Persistence**: Recommended (AOF or RDB) for chord state durability

## License

MIT OR Apache-2.0
