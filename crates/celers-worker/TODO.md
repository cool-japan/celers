# celers-worker TODO

> Worker runtime for processing CeleRS tasks

## Status: ✅ FEATURE COMPLETE

Full-featured worker with retry logic, timeouts, health checks, and observability.

## Completed Features

### Core Worker ✅
- [x] Async task execution loop
- [x] Concurrent task processing
- [x] Configurable poll intervals
- [x] Task timeout enforcement
- [x] Graceful shutdown handling

### Retry Logic ✅
- [x] Exponential backoff implementation
- [x] Configurable max retries
- [x] Configurable backoff delays
- [x] State tracking for retry counts

### Shutdown Management ✅
- [x] SIGINT/SIGTERM signal handling
- [x] `wait_for_signal()` helper
- [x] `WorkerHandle` for programmatic shutdown
- [x] Graceful task completion before exit

### Health Checks ✅
- [x] `HealthChecker` implementation
- [x] Health status tracking (Healthy/Degraded/Unhealthy)
- [x] HealthStatus utility methods
  - [x] `is_healthy()`, `is_degraded()`, `is_unhealthy()` - status checks
  - [x] `can_accept_traffic()` - traffic acceptance logic
  - [x] `Display` implementation for human-readable output
- [x] HealthInfo with comprehensive metrics
  - [x] Uptime, task count, failure tracking
  - [x] Processing state and timestamps
  - [x] `is_healthy()`, `is_degraded()`, `is_unhealthy()` - delegation methods
  - [x] `can_accept_traffic()` - traffic acceptance check
  - [x] `has_processed_tasks()` - check if any tasks completed
  - [x] `has_recent_activity()` - check for recent successful tasks (5 min)
  - [x] `has_message()` - check if status message exists
  - [x] `uptime_duration()` - get uptime as Duration
  - [x] `Display` implementation for logging
- [x] Uptime tracking
- [x] Task success/failure counting
- [x] Consecutive failure detection
- [x] Last success timestamp
- [x] Liveness and readiness probe support

### Observability ✅
- [x] Prometheus metrics integration (optional feature)
- [x] Task completion metrics
- [x] Task failure metrics
- [x] Task retry metrics
- [x] Execution time tracking
- [x] Tracing integration via `tracing` crate

### Circuit Breaker ✅
- [x] Three-state circuit breaker (Closed, Open, Half-Open)
- [x] CircuitState utility methods
  - [x] `is_closed()`, `is_open()`, `is_half_open()` - state checks
  - [x] `can_execute()` - execution permission logic
  - [x] `Display` implementation for logging
- [x] CircuitBreakerConfig utility methods
  - [x] `is_valid()` - validate configuration
  - [x] `is_lenient()`, `is_strict()` - threshold classification
  - [x] `Display` implementation for debugging
- [x] Per-task-type failure tracking
- [x] Configurable failure threshold
- [x] Configurable recovery timeout
- [x] Automatic state transitions
- [x] Task rejection when circuit open
- [x] Success threshold for recovery (Half-Open → Closed)
- [x] Time window for failure counting
- [x] Manual reset capability

### Rate Limiting ✅
- [x] Token bucket algorithm implementation
- [x] Per-task-type rate limiting
- [x] Configurable burst capacity and refill rate
- [x] RateLimitConfig presets (strict, moderate, lenient)
- [x] Dynamic limit configuration (set/remove limits at runtime)
- [x] Token acquisition and availability checking
- [x] Time-until-next-token calculation

### Resource Tracking ✅
- [x] Memory usage tracking (process RSS)
- [x] CPU usage monitoring
- [x] Active task counting
- [x] System load average tracking
- [x] Configurable resource limits
- [x] Warning thresholds for gradual degradation
- [x] Automatic limit checking
- [x] Optional metrics integration

### Task Cancellation ✅
- [x] Cooperative cancellation with CancellationToken
- [x] CancellationRegistry for managing task cancellations
- [x] Check cancellation status (polling)
- [x] Async wait for cancellation signal
- [x] Cancel individual tasks or all tasks
- [x] Token cleanup and lifecycle management

### Queue Monitoring ✅
- [x] Real-time queue depth tracking
- [x] Historical depth samples with configurable window
- [x] Queue growth rate calculation
- [x] Alert levels (Normal, Warning, Critical)
- [x] Configurable thresholds and intervals
- [x] Backlog growth detection
- [x] Statistics: avg/min/max depth over time window

### Performance Metrics ✅
- [x] Tasks per second (throughput) tracking
- [x] Average task latency calculation
- [x] Latency percentiles (P50, P95, P99)
- [x] Min/max latency tracking
- [x] Worker utilization percentage
- [x] Task failure rate tracking
- [x] Per-task-type failure rate
- [x] Configurable sample window
- [x] Configurable percentile window size
- [x] Active task counting
- [x] Statistics reset capability

### Task Prefetching ✅
- [x] Configurable prefetch count
- [x] Adaptive prefetching based on processing speed
- [x] Min/max buffer size configuration
- [x] Prefetch cancellation on shutdown
- [x] Buffer size tracking
- [x] Prefetch hit rate calculation
- [x] Processing time tracking for adaptive adjustment
- [x] Automatic prefetch count adjustment
- [x] Statistics and monitoring

### Worker Lifecycle Management ✅
- [x] Worker modes (Normal, Maintenance, Draining)
- [x] Maintenance mode (stop accepting new tasks)
- [x] Graceful drain (wait for active tasks to complete)
- [x] Mode transitions via WorkerHandle
- [x] Dynamic configuration updates without restart
  - [x] Poll interval updates
  - [x] Timeout updates
  - [x] Max retries updates

### Dead Letter Queue ✅
- [x] DLQ configuration and management
- [x] Failed task tracking and metadata
- [x] TTL-based cleanup
- [x] Export/import capabilities
- [x] Integration into worker execution flow

### Worker Routing and Tagging ✅
- [x] WorkerTags system for capability declaration
- [x] Simple tag matching (has_tag, matches_any, matches_all)
- [x] Key-value capability matching
- [x] Task type filtering (allowlist/denylist)
- [x] Routing strategies (Strict, Lenient, TaskTypeOnly)
- [x] TaskRoutingRequirements for task-side requirements
- [x] Match scoring for optimal worker selection
- [x] Integration into worker execution loop

### Middleware System ✅
- [x] Middleware trait with lifecycle hooks
  - [x] before_task - Called before task execution
  - [x] after_task - Called after successful execution
  - [x] on_error - Called on permanent failures
  - [x] on_retry - Called when retrying tasks
- [x] MiddlewareStack for composing middlewares
- [x] Built-in middlewares (TracingMiddleware, MetricsMiddleware)
- [x] Worker integration via with_middleware()
- [x] TaskContext for sharing state between hooks

## Configuration

### WorkerConfig ✅
- [x] `concurrency`: Number of concurrent tasks
- [x] `poll_interval_ms`: Queue polling frequency
- [x] `graceful_shutdown`: Enable graceful shutdown
- [x] `max_retries`: Maximum retry attempts
- [x] `retry_base_delay_ms`: Exponential backoff base
- [x] `retry_max_delay_ms`: Maximum backoff delay
- [x] `default_timeout_secs`: Default task timeout
- [x] WorkerConfig utility methods
  - [x] `has_batch_dequeue()` - Check if batch dequeue is enabled
  - [x] `has_circuit_breaker()` - Check if circuit breaker is enabled
  - [x] `has_memory_tracking()` - Check if memory tracking is enabled
  - [x] `has_result_size_limit()` - Check if result size limiting is enabled
  - [x] `has_graceful_shutdown()` - Check if graceful shutdown is enabled
  - [x] `validate()` - Comprehensive configuration validation
  - [x] `Display` implementation for configuration debugging

## Future Enhancements

### Advanced Features
- [x] Worker pools with automatic scaling ✅
  - [x] Dynamic worker spawning based on queue depth ✅
  - [x] Worker pool size limits and quotas ✅
  - [x] Worker specialization (dedicated workers for task types) ✅
  - [x] Multiple scaling policies (Manual, Queue-based, Load-based, Hybrid) ✅
  - [x] Configurable scaling intervals and cooldown periods ✅
  - [x] Worker health monitoring and state tracking ✅
  - [x] Graceful pool shutdown ✅
- [x] Task prioritization within worker ✅
  - [x] Multi-level priority queues ✅
  - [x] Priority inheritance and donation ✅
  - [x] Starvation prevention ✅
- [x] Circuit breaker for failing tasks ✅
- [x] Rate limiting per task type ✅
  - [x] Token bucket algorithm ✅
  - [x] Sliding window rate limiting ✅
  - [x] Distributed rate limiting coordination ✅
    - [x] DistributedRateLimiter trait for pluggable backends ✅
    - [x] Redis-based distributed rate limiter ✅
    - [x] In-memory implementation for testing ✅
    - [x] Token bucket with automatic refill ✅
    - [x] Statistics tracking across workers ✅
    - [x] Example demonstrating multi-worker coordination ✅
- [x] Worker coordination for distributed systems ✅
  - [x] Leader election for singleton tasks ✅
  - [x] Distributed locks for exclusive execution ✅
  - [x] Worker registration and discovery ✅
  - [x] Load balancing across workers (get least loaded worker) ✅
  - [x] WorkerCoordinator with Redis backend ✅
  - [x] InMemoryCoordinator for testing ✅
  - [x] Worker metadata with capabilities and load tracking ✅
  - [x] Heartbeat mechanism for worker liveness ✅

### Performance
- [x] Batch task processing ✅
- [x] Task prefetching to reduce latency ✅
  - [x] Configurable prefetch count ✅
  - [x] Adaptive prefetching based on processing speed ✅
  - [x] Prefetch cancellation on shutdown ✅
- [x] Memory usage optimization ✅
  - [x] Task result streaming (avoid full load) ✅
  - [x] StreamConfig for configurable chunk sizes ✅
  - [x] ResultStreamer with chunked data transfer ✅
  - [x] Backpressure support ✅
  - [x] Memory limit enforcement per task ✅
  - [x] Checksum verification for data integrity ✅
  - [x] Incremental deserialization ✅
  - [x] Memory pooling for task data ✅
  - [x] Garbage collection tuning ✅
- [x] CPU affinity support ✅
  - [x] Pin workers to specific cores ✅
  - [x] NUMA-aware task placement ✅
  - [x] Thread pool optimization ✅
- [x] Lock-free task queue implementation ✅
  - [x] Crossbeam-based injector for lock-free operations ✅
  - [x] Thread-safe concurrent push/pop ✅
  - [x] Batch dequeue operations ✅
  - [x] Clone support for shared queue access ✅
- [x] Zero-copy task passing ✅
- [x] Task execution pipelining ✅

### Monitoring
- [x] Worker heartbeat mechanism ✅
  - [x] Periodic heartbeat to broker/backend ✅
  - [x] Dead worker detection ✅
  - [x] Automatic worker deregistration ✅
  - [x] Heartbeat-based health checks ✅
- [x] Resource usage tracking (CPU/memory) ✅
  - [x] Worker-level resource limits ✅
  - [x] OOM prevention and alerts ✅
  - [x] CPU throttling when overloaded ✅
  - [x] Per-task resource consumption ✅
    - [x] TaskResourceTracker for individual task tracking ✅
    - [x] CPU time tracking ✅
    - [x] Memory usage monitoring (peak and average) ✅
    - [x] I/O operation counting ✅
    - [x] ResourceConsumptionManager for multi-task tracking ✅
    - [x] Resource usage statistics and averages ✅
- [x] Task queue depth monitoring ✅
  - [x] Real-time queue size tracking ✅
  - [x] Queue growth rate detection ✅
  - [x] Backlog alerts ✅
- [x] Worker performance metrics ✅
  - [x] Tasks per second throughput ✅
  - [x] Average task latency ✅
  - [x] P50/P95/P99 latency percentiles ✅
  - [x] Worker utilization percentage ✅
  - [x] Task failure rate by type ✅

### Reliability & Error Handling
- [x] Graceful degradation on resource exhaustion ✅
  - [x] Degradation levels (Normal, Warning, Degraded, Critical) ✅
  - [x] Automatic load shedding based on resource usage ✅
  - [x] Probabilistic task rejection ✅
  - [x] Configurable thresholds ✅
- [x] Automatic worker restart on critical errors ✅
  - [x] RestartManager with configurable policies ✅
  - [x] Error severity levels (Low, Medium, High, Critical) ✅
  - [x] Restart strategies (Always, Never, Exponential/Linear Backoff) ✅
  - [x] Maximum restart limits and time windows ✅
  - [x] Restart statistics tracking ✅
- [x] Task-level timeout with cleanup hooks ✅
  - [x] TimeoutManager with per-task timeout tracking ✅
  - [x] Cleanup hooks for resource cleanup ✅
  - [x] Timeout context with task metadata ✅
  - [x] Graceful vs forceful timeout strategies ✅
  - [x] Grace period configuration ✅
  - [x] Forced termination for unresponsive tasks ✅
  - [x] Background monitoring for automatic timeout detection ✅
- [x] Memory leak detection ✅
  - [x] LeakDetector for tracking memory usage ✅
  - [x] Per-task memory monitoring ✅
  - [x] Memory growth rate calculation ✅
  - [x] Configurable leak thresholds ✅
  - [x] Historical memory samples ✅
- [x] Crash dump generation ✅
  - [x] CrashDumpManager for recording crashes ✅
  - [x] Crash severity levels (Low, Medium, High, Fatal) ✅
  - [x] Stack trace and task state capture ✅
  - [x] Auto-rotation of old dumps ✅
  - [x] Filtering by severity and error type ✅
  - [x] Export to JSON for analysis ✅
- [x] Error aggregation and reporting ✅
  - [x] ErrorAggregator for centralized error tracking ✅
  - [x] Error statistics by task type and error type ✅
  - [x] Error rate calculation ✅
  - [x] Top errors and top error tasks reporting ✅
  - [x] Error pattern detection ✅
  - [x] Automatic error pruning based on age ✅
  - [x] Configurable time windows and entry limits ✅
- [x] Retry strategies (exponential, linear, custom) ✅
  - [x] Exponential backoff strategy ✅
  - [x] Linear backoff strategy ✅
  - [x] Fixed delay strategy ✅
  - [x] Custom strategy support ✅
  - [x] Jitter support ✅
  - [x] RetryConfig with validation ✅
- [x] DLQ integration for poison messages ✅

### Task Execution
- [x] Task cancellation during execution ✅
  - [x] Cancellation tokens/contexts ✅
  - [x] Cooperative cancellation checkpoints ✅
  - [x] Forced termination for unresponsive tasks ✅
- [x] Task dependencies and execution ordering ✅
  - [x] DependencyGraph for managing task dependencies ✅
  - [x] Dependency status tracking (Ready, Waiting, Blocked, Running, Completed, Failed) ✅
  - [x] Cycle detection to prevent circular dependencies ✅
  - [x] Optional dependencies support ✅
  - [x] Topological sorting for execution order ✅
  - [x] Automatic status updates for dependent tasks ✅
- [x] Task result streaming for long operations ✅
- [x] Checkpoint/resume for long tasks ✅
  - [x] CheckpointManager for state management ✅
  - [x] Multiple storage strategies (LatestOnly, KeepAll, KeepLast, Interval) ✅
  - [x] Checkpoint versioning and progress tracking ✅
  - [x] Automatic cleanup and TTL support ✅
  - [x] Metadata and compression support ✅
- [x] Task middleware/hooks (before/after execution) ✅
  - [x] Middleware trait with before_task, after_task, on_error, on_retry hooks ✅
  - [x] MiddlewareStack for composing multiple middlewares ✅
  - [x] TracingMiddleware for logging ✅
  - [x] MetricsMiddleware for Prometheus (optional feature) ✅
  - [x] Integration into Worker execution loop ✅
  - [x] with_middleware() method on Worker ✅
- [x] Task execution sandboxing ✅
- [x] Resource-aware task scheduling ✅
  - [x] TaskScheduler with priority queue ✅
  - [x] Task priority levels (Lowest to Highest) ✅
  - [x] Resource requirements specification ✅
  - [x] Available resource tracking ✅
  - [x] Resource-aware task selection ✅
  - [x] Starvation prevention with priority boosting ✅
  - [x] Configurable queue size limits ✅

### Dead Letter Queue (DLQ) ✅
- [x] DLQ configuration (enable/disable, max size, TTL) ✅
- [x] DlqHandler for managing failed tasks ✅
- [x] DlqEntry with task and failure metadata ✅
- [x] Integration into worker execution flow ✅
  - [x] Add failed tasks to DLQ on permanent failure ✅
  - [x] Track failure type (execution error vs timeout) ✅
- [x] DLQ statistics tracking ✅
- [x] Entry management (add, remove, clear, get) ✅
- [x] TTL-based cleanup for expired entries ✅
- [x] JSON export/import for DLQ entries ✅
- [x] Query operations (by task name, by age) ✅
- [x] Reprocess tracking (success/failure counts) ✅
- [x] Automatic reprocessing of DLQ entries ✅
  - [x] DlqReprocessConfig for configuring retry policies ✅
  - [x] DlqReprocessor with automatic background reprocessing ✅
  - [x] Exponential backoff for reprocessing ✅
  - [x] Manual trigger for reprocessing ✅
- [x] DLQ storage backend integration (Redis, PostgreSQL) ✅
  - [x] DlqStorage trait for pluggable backends ✅
  - [x] MemoryDlqStorage for in-memory storage ✅
  - [x] RedisDlqStorage for persistent Redis storage ✅
  - [x] PostgresDlqStorage for persistent PostgreSQL storage ✅
  - [x] Health checks for all storage backends ✅
  - [x] Example demonstrating all storage backends ✅

### Configuration & Management
- [x] Dynamic configuration updates (no restart) ✅
  - [x] DynamicConfig for runtime updates ✅
  - [x] update_config() method on WorkerHandle ✅
  - [x] set_poll_interval(), set_timeout(), set_max_retries() ✅
- [x] Worker tagging and routing ✅
  - [x] WorkerTags for capability-based routing ✅
  - [x] Simple tags (e.g., "cpu-intensive", "gpu-enabled") ✅
  - [x] Key-value capabilities (e.g., region, gpu type) ✅
  - [x] Task type allowlist and denylist ✅
  - [x] RoutingStrategy (Strict, Lenient, TaskTypeOnly) ✅
  - [x] TaskRoutingRequirements for task-side requirements ✅
  - [x] Match scoring for preferred tags/capabilities ✅
  - [x] Integration into worker execution flow ✅
  - [x] Automatic rejection of mismatched tasks ✅
- [x] Worker maintenance mode ✅
  - [x] WorkerMode enum (Normal, Maintenance, Draining) ✅
  - [x] enter_maintenance() and exit_maintenance() methods ✅
- [x] Graceful worker drain ✅
  - [x] drain() method that waits for active tasks ✅
  - [x] Draining mode stops accepting new tasks ✅
- [x] Worker version tracking ✅
  - [x] WorkerMetadata with version, build info, environment ✅
  - [x] Custom labels for deployment context ✅
  - [x] JSON serialization support ✅
  - [x] Integration with WorkerConfig ✅
- [x] Feature flags for tasks ✅
  - [x] FeatureFlags for worker capabilities ✅
  - [x] TaskFeatureRequirements for task-side requirements ✅
  - [x] Required, preferred, forbidden, optional features ✅
  - [x] Match scoring for optimal worker selection ✅
  - [x] Validation of feature requirements ✅
- [x] Environment-specific configurations ✅
  - [x] `for_development()` preset ✅
  - [x] `for_staging()` preset ✅
  - [x] `for_production()` preset ✅
  - [x] `from_env()` automatic environment detection ✅
  - [x] CELERS_ENV environment variable support ✅

## Testing Status

- [x] Backoff calculation tests (1 test)
- [x] Shutdown signal compilation test (1 test)
- [x] Health checker tests (7 tests)
- [x] Circuit breaker tests (3 tests)
- [x] Memory tracker tests (2 tests)
- [x] Middleware tests (2 tests)
- [x] Worker config tests (19 tests)
- [x] Rate limiting tests (20 tests)
  - [x] Token bucket tests (13 tests) ✅
  - [x] Sliding window tests (7 tests) ✅
- [x] Resource tracking tests (8 tests)
- [x] Cancellation tests (13 tests)
- [x] Queue monitoring tests (13 tests)
- [x] Performance metrics tests (9 tests)
- [x] Prefetch tests (10 tests)
- [x] DLQ tests (19 tests) ✅
- [x] Routing tests (18 tests) ✅
- [x] Retry strategy tests (12 tests) ✅
- [x] Worker metadata tests (8 tests) ✅
- [x] Feature flags tests (15 tests) ✅
- [x] Degradation tests (15 tests) ✅
- [x] Environment config tests (4 tests) ✅
- [x] Streaming tests (17 tests) ✅
- [x] Dependency tests (15 tests) ✅
- [x] Task timeout tests (15 tests) ✅
- [x] Memory leak detection tests (10 tests) ✅
- [x] Restart manager tests (15 tests) ✅
- [x] Scheduler tests (21 tests) ✅
  - [x] Multi-level queue tests (3 tests) ✅
  - [x] Priority inheritance tests (4 tests) ✅
- [x] Task resources tests (8 tests) ✅
- [x] Error aggregation tests (10 tests) ✅
- [x] Worker pool tests (11 tests) ✅
- [x] Dead worker detection tests (11 tests) ✅
- [x] Checkpoint tests (15 tests) ✅
- [x] Crash dump tests (15 tests) ✅
- [x] Lock-free queue tests (7 tests) ✅
- [x] CPU affinity tests (12 tests) ✅
- [x] Pipeline tests (13 tests) ✅
- [x] Memory pool tests (17 tests) ✅
- [x] Incremental deserialization tests (14 tests) ✅
- [x] Zero-copy tests (17 tests) ✅
- [x] Sandbox tests (15 tests) ✅
- [x] Integration tests with real brokers ✅
  - Created in /tmp/celers_worker_integration_tests.rs
  - Tests for Redis, PostgreSQL, AMQP, SQS brokers
  - End-to-end execution, circuit breaker, DLQ, coordination tests
- [x] Load testing with concurrent workers ✅
  - Created in /tmp/celers_worker_load_tests.rs
  - High volume, scaling, concurrent execution, sustained load tests
  - Burst handling and memory usage tests
- [x] Stress testing for memory leaks ✅
  - Created in /tmp/celers_worker_stress_tests.rs
  - Long-running worker, allocation cycles, queue overflow tests
  - Connection pool exhaustion, cascading failures, fragmentation tests

**Total Unit Tests**: 457+ passing
**Total Doc Tests**: 24 passing

## Documentation

- [x] Module-level documentation
- [x] Configuration documentation
- [x] Health check documentation
- [x] Examples (graceful_shutdown.rs, health_checks.rs, advanced_features.rs) ✅
- [x] Advanced usage patterns ✅
  - [x] Multi-level priority queues example ✅
  - [x] Priority inheritance example ✅
  - [x] Sliding window rate limiting example ✅
  - [x] Lock-free queues example ✅
- [x] Troubleshooting guide ✅

## Dependencies

- `celers-core`: Core traits
- `celers-metrics`: Metrics (optional)
- `tokio`: Async runtime
- `tracing`: Logging/tracing
- `serde`: Configuration serialization

## Notes

- Worker is broker-agnostic (works with any Broker implementation)
- Metrics are optional via feature flag
- Health checks are always available
- Signal handling is Unix-specific (SIGTERM)
