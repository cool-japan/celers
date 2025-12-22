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
- [ ] Worker pools with automatic scaling
  - [ ] Dynamic worker spawning based on queue depth
  - [ ] Worker pool size limits and quotas
  - [ ] Worker specialization (dedicated workers for task types)
- [ ] Task prioritization within worker
  - [ ] Multi-level priority queues
  - [ ] Priority inheritance and donation
  - [ ] Starvation prevention
- [x] Circuit breaker for failing tasks ✅
- [x] Rate limiting per task type ✅
  - [x] Token bucket algorithm ✅
  - [ ] Sliding window rate limiting
  - [ ] Distributed rate limiting coordination
- [ ] Worker coordination for distributed systems
  - [ ] Leader election for singleton tasks
  - [ ] Distributed locks for exclusive execution
  - [ ] Worker registration and discovery
  - [ ] Load balancing across workers

### Performance
- [x] Batch task processing ✅
- [x] Task prefetching to reduce latency ✅
  - [x] Configurable prefetch count ✅
  - [x] Adaptive prefetching based on processing speed ✅
  - [x] Prefetch cancellation on shutdown ✅
- [ ] Memory usage optimization
  - [ ] Task result streaming (avoid full load)
  - [ ] Incremental deserialization
  - [ ] Memory pooling for task data
  - [ ] Garbage collection tuning
- [ ] CPU affinity support
  - [ ] Pin workers to specific cores
  - [ ] NUMA-aware task placement
  - [ ] Thread pool optimization
- [ ] Lock-free task queue implementation
- [ ] Zero-copy task passing
- [ ] Task execution pipelining

### Monitoring
- [x] Worker heartbeat mechanism ✅
  - [x] Periodic heartbeat to broker/backend ✅
  - [ ] Dead worker detection
  - [ ] Automatic worker deregistration
  - [x] Heartbeat-based health checks ✅
- [x] Resource usage tracking (CPU/memory) ✅
  - [x] Worker-level resource limits ✅
  - [x] OOM prevention and alerts ✅
  - [x] CPU throttling when overloaded ✅
  - [ ] Per-task resource consumption
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
- [ ] Automatic worker restart on critical errors
- [ ] Task-level timeout with cleanup hooks
- [ ] Memory leak detection
- [ ] Crash dump generation
- [ ] Error aggregation and reporting
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
  - [ ] Forced termination for unresponsive tasks
- [ ] Task dependencies and execution ordering
- [ ] Task result streaming for long operations
- [ ] Checkpoint/resume for long tasks
- [x] Task middleware/hooks (before/after execution) ✅
  - [x] Middleware trait with before_task, after_task, on_error, on_retry hooks ✅
  - [x] MiddlewareStack for composing multiple middlewares ✅
  - [x] TracingMiddleware for logging ✅
  - [x] MetricsMiddleware for Prometheus (optional feature) ✅
  - [x] Integration into Worker execution loop ✅
  - [x] with_middleware() method on Worker ✅
- [ ] Task execution sandboxing
- [ ] Resource-aware task scheduling

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
- [ ] Automatic reprocessing of DLQ entries
- [ ] DLQ storage backend integration (Redis, PostgreSQL)

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
- [x] Rate limiting tests (13 tests)
- [x] Resource tracking tests (8 tests)
- [x] Cancellation tests (13 tests)
- [x] Queue monitoring tests (13 tests)
- [x] Performance metrics tests (9 tests)
- [x] Prefetch tests (10 tests)
- [x] DLQ tests (14 tests) ✅
- [x] Routing tests (18 tests) ✅
- [x] Retry strategy tests (12 tests) ✅
- [x] Worker metadata tests (8 tests) ✅
- [x] Feature flags tests (15 tests) ✅
- [x] Degradation tests (15 tests) ✅
- [x] Environment config tests (4 tests) ✅
- [ ] Integration tests with real brokers
- [ ] Load testing with concurrent workers
- [ ] Stress testing for memory leaks

**Total Unit Tests**: 187 passing
**Total Doc Tests**: 17 passing

## Documentation

- [x] Module-level documentation
- [x] Configuration documentation
- [x] Health check documentation
- [x] Examples (graceful_shutdown.rs, health_checks.rs)
- [ ] Advanced usage patterns
- [ ] Troubleshooting guide

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
