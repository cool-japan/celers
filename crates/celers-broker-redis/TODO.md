# celers-broker-redis TODO

> Redis-based broker implementation for CeleRS

## Status: ✅ FEATURE COMPLETE

Full-featured Redis broker with FIFO/priority queues, DLQ, and task cancellation.

## Completed Features

### Queue Modes ✅
- [x] FIFO mode using Redis lists
- [x] Priority queue mode using Redis sorted sets
- [x] `QueueMode` enum for configuration
- [x] Atomic BRPOPLPUSH for FIFO dequeue
- [x] ZPOPMIN for priority-based dequeue
- [x] QueueMode utility methods
  - [x] `is_fifo()`, `is_priority()` - Queue mode checks
  - [x] `Display` implementation for human-readable output

### Core Operations ✅
- [x] `enqueue()` - Add tasks to queue
- [x] `dequeue()` - Fetch tasks atomically
- [x] `ack()` - Mark tasks as completed
- [x] `reject()` - Handle failed tasks
- [x] `queue_size()` - Get pending task count
- [x] `cancel()` - Cancel running tasks

### Dead Letter Queue ✅
- [x] Automatic DLQ for max-retry failures
- [x] `dlq_size()` - Get DLQ task count
- [x] `inspect_dlq()` - View failed tasks
- [x] `replay_from_dlq()` - Retry individual tasks
- [x] `clear_dlq()` - Bulk remove DLQ tasks

### Task Cancellation ✅
- [x] Redis Pub/Sub for cancellation signals
- [x] `cancel()` - Publish cancellation message
- [x] `create_pubsub()` - Create PubSub connection
- [x] `cancel_channel()` - Get channel name

### Observability ✅
- [x] Prometheus metrics (optional feature)
- [x] Tasks enqueued counter
- [x] Queue size gauges
- [x] Processing queue tracking
- [x] DLQ size monitoring

### Delayed Task Execution ✅
- [x] `enqueue_at(task, timestamp)` - Schedule for specific Unix timestamp
- [x] `enqueue_after(task, delay_secs)` - Schedule after delay in seconds
- [x] Delayed queue using Redis sorted set (timestamp as score)
- [x] Automatic processing of ready tasks in dequeue()
- [x] Atomic move from delayed queue to main queue
- [x] Support for both FIFO and Priority queue modes
- [x] Metrics tracking for delayed tasks

## Configuration

### Connection ✅
- [x] Redis URL connection string
- [x] Queue name configuration
- [x] Multiplexed async connections
- [x] Automatic reconnection

### Queue Settings ✅
- [x] Queue mode selection (FIFO/Priority)
- [x] Processing queue naming
- [x] DLQ naming convention
- [x] Cancellation channel naming

## Future Enhancements

### Performance
- [ ] Connection pooling for high throughput
  - [ ] Configurable pool size
  - [ ] Connection reuse strategies
  - [ ] Pool health monitoring
- [x] Batch enqueue operations ✅ (with Redis pipelining)
- [x] Batch dequeue operations ✅ (with Redis pipelining)
- [x] Pipeline support for multiple operations ✅ (implemented)
- [ ] Lua script optimization
  - [ ] Script caching (SCRIPT LOAD)
  - [ ] Script versioning
  - [ ] Script performance profiling
- [ ] Memory optimization
  - [ ] Compression for large payloads
  - [ ] TTL-based auto-cleanup
  - [ ] Memory usage monitoring

### Advanced Features
- [x] Task scheduling/delayed execution ✅ (enqueue_at, enqueue_after)
- [ ] Queue partitioning for scaling
  - [ ] Consistent hashing
  - [ ] Key-based sharding
  - [ ] Dynamic partition rebalancing
- [ ] Redis Cluster support
  - [ ] Cluster-aware key distribution
  - [ ] Cross-slot multi-key operations
  - [ ] Cluster failover handling
- [ ] Redis Sentinel support for HA
  - [ ] Automatic master discovery
  - [ ] Failover detection
  - [ ] Read replica support
- [ ] Redis Streams integration
  - [ ] Stream-based task queues
  - [ ] Consumer groups
  - [ ] Stream trimming strategies
- [ ] Geo-distribution
  - [ ] Multi-region replication
  - [ ] Regional read routing
  - [ ] Conflict resolution

### Monitoring
- [ ] Per-queue metrics
  - [ ] Queue depth by name
  - [ ] Enqueue/dequeue rates
  - [ ] Task age histogram
- [ ] Operation latency tracking
  - [ ] Command-level latency
  - [ ] P50/P95/P99 percentiles
  - [ ] Slow operation logging
- [ ] Connection pool metrics
  - [ ] Active connections
  - [ ] Connection wait time
  - [ ] Connection errors
- [ ] Redis server health checks
  - [ ] Ping/PONG checks
  - [ ] Memory usage alerts
  - [ ] Keyspace statistics
  - [ ] Replication lag monitoring

### Reliability & Error Handling
- [ ] Automatic retry with backoff
  - [ ] Exponential backoff
  - [ ] Jittered retry
  - [ ] Max retry limits
- [ ] Circuit breaker for Redis
  - [ ] Failure threshold
  - [ ] Recovery timeout
  - [ ] Half-open state testing
- [ ] Graceful degradation
  - [ ] Fallback to local queue
  - [ ] Read-only mode
  - [ ] Queuing during outages
- [ ] Data integrity
  - [ ] Checksum validation
  - [ ] Duplicate detection
  - [ ] Ordered delivery guarantees

### Security
- [ ] TLS/SSL connections
  - [ ] Certificate validation
  - [ ] Mutual TLS
  - [ ] Cipher suite configuration
- [ ] Authentication
  - [ ] ACL support (Redis 6+)
  - [ ] Password authentication
  - [ ] Token-based auth
- [ ] Authorization
  - [ ] Command restrictions
  - [ ] Key namespace isolation
  - [ ] User permissions

### Advanced Queue Features
- [ ] Queue priorities within Redis
  - [ ] Weighted queue selection
  - [ ] Priority aging
  - [ ] Starvation prevention
- [ ] Queue rate limiting
  - [ ] Token bucket per queue
  - [ ] Sliding window limits
  - [ ] Distributed rate limiting
- [ ] Queue pausing/resuming
  - [ ] Temporary queue suspension
  - [ ] Drain mode
  - [ ] Emergency stop
- [ ] Task deduplication
  - [ ] Content-based dedup
  - [ ] Time-window dedup
  - [ ] Dedup key strategies

## Testing Status

- [ ] Unit tests (mocked Redis)
- [ ] Integration tests with real Redis
- [ ] Load testing
- [ ] Concurrency testing
- [ ] Failover testing

## Documentation

- [x] Module-level documentation
- [x] Queue mode documentation
- [x] DLQ operation examples
- [ ] Redis configuration guide
- [ ] Scaling recommendations
- [ ] Troubleshooting guide

## Dependencies

- `celers-core`: Core traits
- `celers-metrics`: Metrics (optional)
- `redis`: Redis client with tokio support
- `serde_json`: Task serialization
- `tracing`: Logging

## Redis Configuration

Recommended settings:
```conf
maxmemory-policy noeviction
tcp-keepalive 300
timeout 0
appendonly no  # or yes for durability
save ""  # disable snapshots for performance
```

## Notes

- Uses Redis lists for FIFO mode (BRPOPLPUSH)
- Uses Redis sorted sets for priority mode (ZADD/ZPOPMIN)
- DLQ uses Redis lists
- Cancellation uses Redis Pub/Sub
- All operations are atomic
- Supports task priorities 0-255
