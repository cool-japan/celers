# celers-broker-redis TODO

> Redis-based broker implementation for CeleRS

## Status: ✅ FEATURE COMPLETE

Full-featured Redis broker with FIFO/priority queues, DLQ, task cancellation, health checks, queue control, task deduplication, circuit breaker, rate limiting, and automatic retry.

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

### Health Checks ✅ (NEW)
- [x] `HealthChecker` - Redis health monitoring
- [x] `ping()` - Ping Redis and measure latency
- [x] `check_health()` - Comprehensive health status
- [x] `get_queue_stats()` - Queue statistics (pending, processing, DLQ, delayed)
- [x] `get_memory_info()` - Redis memory statistics
- [x] `RedisHealthStatus` - Structured health status
  - [x] Connection status, latency
  - [x] Memory usage (used, max, percentage)
  - [x] Connected clients count
  - [x] Redis version, role (master/slave)
  - [x] Memory critical threshold checks
- [x] `QueueStats` - Queue depth tracking

### Queue Control ✅ (NEW)
- [x] `QueueController` - Queue state management
- [x] `pause()` - Pause queue (no enqueue, no dequeue)
- [x] `resume()` - Resume normal operation
- [x] `drain()` - Drain mode (no enqueue, dequeue allowed)
- [x] `emergency_stop()` - Local emergency stop flag
- [x] `get_state()` - Get current queue state
- [x] `can_enqueue()`, `can_dequeue()` - Permission checks
- [x] `QueueState` enum (Active, Paused, Draining)
- [x] Emergency stop flag sharing for async coordination

### Task Deduplication ✅ (NEW)
- [x] `Deduplicator` - Prevent duplicate task processing
- [x] `DedupStrategy` - Multiple deduplication strategies
  - [x] `ById` - Deduplicate by task ID
  - [x] `ByContent` - Deduplicate by task name + payload hash
  - [x] `ByKey` - Custom deduplication key
- [x] `check()` - Check if task is duplicate
- [x] `mark()` - Mark task as seen
- [x] `check_and_mark()` - Atomic check and mark
- [x] `unmark()` - Remove deduplication entry
- [x] `clear()` - Clear all deduplication entries
- [x] `count()` - Count tracked entries
- [x] TTL-based auto-expiry
- [x] `content_hash()` - Generate content hash for tasks

### Circuit Breaker ✅ (NEW)
- [x] `CircuitBreaker` - Protect against cascading failures
- [x] `CircuitBreakerConfig` - Configurable thresholds and timeouts
- [x] `CircuitState` enum (Closed, Open, HalfOpen)
- [x] `allow_request()` - Check if request should be allowed
- [x] `record_success()`, `record_failure()` - Track operation results
- [x] `force_open()`, `force_close()`, `reset()` - Manual control
- [x] `stats()` - Get breaker statistics
- [x] `time_until_half_open()` - Time until recovery attempt
- [x] Configurable failure threshold
- [x] Configurable recovery timeout
- [x] Success threshold for closing from HalfOpen
- [x] Half-open max requests limit

### Rate Limiting ✅ (NEW)
- [x] `TokenBucketLimiter` - Local rate limiting with burst capacity
- [x] `DistributedRateLimiter` - Redis-backed distributed rate limiting
- [x] `QueueRateLimiter` - Unified API for local/distributed
- [x] `TrackedRateLimiter` - Rate limiter with statistics
- [x] `QueueRateLimitConfig` - Configurable rate, burst, window
- [x] `try_acquire()`, `try_acquire_n()` - Permit acquisition
- [x] `time_until_available()` - Time until permit available
- [x] `available_permits()` - Current available permits
- [x] Sliding window algorithm (distributed)
- [x] Lua script for atomic distributed limiting

### Retry Mechanism ✅ (NEW)
- [x] `RetryExecutor` - Execute operations with automatic retry
- [x] `RetryConfig` - Configurable retry behavior
- [x] `BackoffStrategy` - Multiple backoff algorithms
  - [x] Fixed delay
  - [x] Linear backoff
  - [x] Exponential backoff
  - [x] Exponential with jitter
  - [x] Decorrelated jitter (AWS recommended)
- [x] `ErrorKind` - Error classification for retry decisions
- [x] `RetryResult` - Result with retry metadata
- [x] `execute()` - Generic retry execution
- [x] `execute_with_classification()` - Redis-specific retry
- [x] Preset configs: `aggressive()`, `conservative()`, `no_retry()`

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
- [x] Per-queue metrics ✅
  - [x] Queue depth by name (QueueStats)
  - [ ] Enqueue/dequeue rates
  - [ ] Task age histogram
- [x] Operation latency tracking ✅
  - [x] Command-level latency (ping latency)
  - [ ] P50/P95/P99 percentiles
  - [ ] Slow operation logging
- [ ] Connection pool metrics
  - [ ] Active connections
  - [ ] Connection wait time
  - [ ] Connection errors
- [x] Redis server health checks ✅
  - [x] Ping/PONG checks
  - [x] Memory usage alerts (memory critical checks)
  - [ ] Keyspace statistics
  - [ ] Replication lag monitoring

### Reliability & Error Handling
- [x] Automatic retry with backoff ✅
  - [x] Exponential backoff
  - [x] Jittered retry (exponential with jitter, decorrelated jitter)
  - [x] Max retry limits
  - [x] Linear and fixed backoff
  - [x] Error classification for retry decisions
- [x] Circuit breaker for Redis ✅
  - [x] Failure threshold
  - [x] Recovery timeout
  - [x] Half-open state testing
  - [x] Success threshold for closing
  - [x] Statistics and monitoring
- [ ] Graceful degradation
  - [ ] Fallback to local queue
  - [ ] Read-only mode
  - [ ] Queuing during outages
- [ ] Data integrity
  - [ ] Checksum validation
  - [x] Duplicate detection ✅ (via Deduplicator)
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
- [x] Queue rate limiting ✅
  - [x] Token bucket per queue (TokenBucketLimiter)
  - [x] Sliding window limits (DistributedRateLimiter)
  - [x] Distributed rate limiting (Redis-backed)
- [x] Queue pausing/resuming ✅
  - [x] Temporary queue suspension (pause/resume)
  - [x] Drain mode (process remaining, block new)
  - [x] Emergency stop (local instant effect)
- [x] Task deduplication ✅
  - [x] Content-based dedup (ByContent strategy)
  - [x] Time-window dedup (TTL-based expiry)
  - [x] Dedup key strategies (ById, ByContent, ByKey)

## Testing Status

- [x] Unit tests ✅ (62 tests passing)
  - [x] QueueMode tests
  - [x] Broker construction and accessor tests
  - [x] Health check tests
  - [x] Queue control tests
  - [x] Deduplication tests
  - [x] Lua script tests
  - [x] Visibility manager tests
  - [x] Circuit breaker tests
  - [x] Rate limiter tests
  - [x] Retry mechanism tests
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
