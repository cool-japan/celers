# celers-kombu TODO

> Broker abstraction layer (Kombu-style)

## Status: ✅ ENTERPRISE-GRADE + PRODUCTION-READY

All core abstractions implemented with advanced production features: DLQ, transactions, scheduling, consumer groups, message replay, quota management, comprehensive middleware, flow control, poison message detection, utilities, benchmarks, and examples.

### Latest Enhancements (v0.4.5 - 2025-12-20)
- ✅ **Utilities Showcase Example**: **NEW comprehensive example** (NEW)
  - Demonstrates practical usage of 37 utility functions
  - 13 sections covering all major utility categories:
    - Batch optimization (calculate_optimal_batch_size, calculate_optimal_workers)
    - Routing pattern matching (match_routing_pattern, match_direct_routing, match_fanout_routing)
    - Performance analysis (analyze_broker_performance, calculate_throughput, calculate_avg_latency)
    - Queue health monitoring (analyze_queue_health, estimate_drain_time, estimate_queue_memory)
    - Load balancing (calculate_load_distribution)
    - Circuit breaker analysis (analyze_circuit_breaker)
    - Connection pool analysis (analyze_pool_health)
    - Exponential backoff (calculate_backoff_delay)
    - Message size estimation (estimate_message_size)
    - Deduplication ID generation (generate_deduplication_id)
    - Priority scoring (calculate_priority_score)
    - Stale message detection (identify_stale_messages)
    - Batch grouping (suggest_batch_groups)
  - **Total: 9 comprehensive examples** (up from 8)
- ✅ **Expanded Utility Benchmarks**: **15 additional utility benchmarks** (NEW)
  - Added benchmarks for: calculate_optimal_workers, analyze_broker_performance, calculate_throughput
  - Queue health: analyze_queue_health, estimate_drain_time
  - Load balancing: calculate_load_distribution
  - Resilience: analyze_circuit_breaker, analyze_pool_health, calculate_backoff_delay
  - Message management: calculate_priority_score, identify_stale_messages, suggest_batch_groups
  - Prediction: predict_throughput, calculate_efficiency, calculate_health_score
  - **Total: 36 comprehensive benchmarks** (up from 21)
  - **Near-complete coverage** of all 37 utility functions
- ✅ **Zero Warnings Policy**: Clean build with no warnings or clippy issues maintained

### Previous Enhancements (v0.4.4 - 2025-12-13)
- ✅ **Enhanced Benchmarks**: **12 additional benchmarks** (NEW)
  - **3 new middleware benchmarks**: TracingMiddleware, BatchingMiddleware, AuditMiddleware
  - **6 new utility benchmarks**: calculate_optimal_batch_size, calculate_queue_capacity, suggest_partition_count, analyze_message_patterns, match_routing_pattern, suggest_retry_policy
  - **3 config builder benchmarks**: BackpressureConfig, CircuitBreakerConfig, PoolConfig
  - **Total: 21 comprehensive benchmarks** (up from 9)
- ✅ **Code Quality Improvements**:
  - Fixed clippy warning in examples (useless_vec)
  - Fixed clippy warning in tests (assert_eq with literal bool)
  - Maintained zero warnings policy across all code
- ✅ **Zero Warnings Policy**: Clean build with no warnings or clippy issues maintained

### Previous Enhancements (v0.4.3 - 2025-12-13)
- ✅ **Advanced Utilities Module**: **10 new utility functions** (NEW)
  - `calculate_queue_capacity`: Calculate required queue capacity for given workload
  - `suggest_partition_count`: Optimal partition count for distributed queues
  - `calculate_cost_estimate`: Operational cost estimation based on volume
  - `analyze_message_patterns`: Message size/frequency pattern analysis with statistics
  - `calculate_buffer_size`: Optimal buffer size for message batching
  - `estimate_memory_footprint`: Total memory footprint estimation
  - `suggest_ttl`: Appropriate TTL based on processing patterns
  - `calculate_replication_lag`: Acceptable replication lag for distributed queues
  - `calculate_bandwidth_requirement`: Network bandwidth calculation
  - `suggest_retry_policy`: Retry policy recommendations based on error patterns
  - **Total: 37 utility functions** (up from 27)
  - **77 doc tests for utilities and types** (up from 64)
- ✅ **Production Middleware**: **3 new middleware types** (NEW)
  - **TracingMiddleware**: Distributed tracing support
    - Automatic trace ID injection and propagation
    - Service name tagging
    - Span ID generation for operation tracking
    - Timestamp injection for latency analysis
    - 4 unit tests + 1 doc test
  - **BatchingMiddleware**: Automatic message batching hints
    - Configurable batch size and timeout
    - Metadata injection for batch-aware consumers
    - Default settings (100 messages, 5 sec timeout)
    - 3 unit tests + 1 doc test
  - **AuditMiddleware**: Comprehensive audit logging
    - Configurable body logging (with/without PII)
    - Unique audit ID generation
    - Timestamp and operation tracking
    - Compliance-ready audit trail
    - 5 unit tests + 1 doc test
  - **Total: 16 middleware types** (up from 13)
- ✅ **Enhanced Testing**: **228 total tests** (151 unit + 77 doc)
  - Added 12 new unit tests for new middleware
  - Added 13 new doc tests for utilities and middleware
  - All tests passing with zero warnings
- ✅ **Zero Warnings Policy**: Clean build with no warnings or clippy issues maintained
- ✅ **Clippy Improvements**: Fixed manual_div_ceil and manual_clamp warnings

### Previous Enhancements (v0.4.2 - 2025-12-10)
- ✅ **Enhanced Utilities Module**: **8 new utility functions** (NEW)
  - `calculate_priority_score`: Score messages for priority queue ordering
  - `suggest_batch_groups`: Optimal message batching within size constraints
  - `calculate_health_score`: Aggregate health scoring from multiple metrics
  - `identify_stale_messages`: Find messages exceeding age threshold
  - `predict_throughput`: Linear regression-based throughput prediction
  - `calculate_rebalancing`: Queue load balancing recommendations
  - `estimate_completion_time`: Time estimation for message processing
  - `calculate_efficiency`: Processing efficiency ratio calculation
  - **Total: 27 utility functions** (up from 19)
  - **64 doc tests for utilities** (up from 54)
- ✅ **Additional Middleware**: **2 new middleware types** (NEW)
  - **SamplingMiddleware**: Statistical message sampling for monitoring/testing
    - Configurable sample rate (0.0 to 1.0)
    - Deterministic sampling based on message counter
    - 2 unit tests + 1 doc test
  - **TransformationMiddleware**: Message content transformation
    - Custom transformation functions
    - Applies on both publish and consume
    - 2 unit tests + 1 doc test
  - **Total: 13 middleware types** (up from 11)
- ✅ **Enhanced Testing**: **203 total tests** (139 unit + 64 doc)
  - Added 4 new unit tests for new middleware
  - Added 10 new doc tests for utilities and middleware
  - All tests passing with zero warnings
- ✅ **Zero Warnings Policy**: Clean build with no warnings or clippy issues maintained

### Previous Enhancements (v0.4.1 - 2025-12-10)
- ✅ **Comprehensive Examples**: **8 total examples** covering all features
  - **4 new advanced examples** for v0.3.0 and v0.4.0 features
  - advanced_features.rs: Scheduling, consumer groups, replay, and quota management
  - flow_control.rs: Backpressure, poison detection, timeout, and filter middleware
  - circuit_breaker.rs: Circuit breaker, connection pooling, and health checks
  - transactions.rs: Message transactions with ACID guarantees and isolation levels
  - basic_broker.rs: Complete broker implementation example
  - middleware_usage.rs: Middleware chain and individual middleware examples
  - dlq_usage.rs: DLQ configuration and best practices
  - batch_operations.rs: Batch operations and performance guidance
- ✅ **Zero Warnings Policy**: Clean build with no warnings or clippy issues maintained

### Previous Enhancements (v0.4.0 - 2025-12-09)
- ✅ **Utilities Module**: Comprehensive utility functions for broker operations
  - **19 utility functions** for batch optimization, routing, performance analysis
  - Pattern matching for AMQP-style topic routing (topic, direct, fanout)
  - Performance metrics analysis helpers
  - Queue health monitoring and memory estimation
  - Message size estimation and throughput calculators
  - Circuit breaker analysis and recommendations
  - Exponential backoff with jitter calculation
  - Load distribution across multiple queues
  - Connection pool health analysis
  - Optimal worker calculation for queue draining
  - Message deduplication ID generation
  - 19 comprehensive doc tests
  - All utilities fully tested and documented
- ✅ **TimeoutMiddleware**: Message processing timeout enforcement (NEW)
  - Configurable timeout duration
  - Timeout metadata injection into message headers
  - 2 comprehensive tests (1 unit + 1 async)
  - 1 doc test
- ✅ **FilterMiddleware**: Selective message processing (NEW)
  - Custom predicate-based filtering
  - Flexible message matching with closures
  - Reject non-matching messages automatically
  - 2 comprehensive tests (1 unit + 1 async)
  - 1 doc test
- ✅ **BackpressureConfig**: Flow control configuration (NEW)
  - Configurable pending message limits
  - High/low watermark thresholds
  - Queue capacity management
  - Automatic backpressure detection
  - 4 comprehensive unit tests
  - 1 doc test
- ✅ **PoisonMessageDetector**: Automatic poison message detection (NEW)
  - Configurable failure threshold
  - Time-window-based tracking
  - Individual and bulk failure clearing
  - Prevents infinite retry loops
  - 5 comprehensive unit tests
  - 1 doc test
- ✅ **Enhanced Testing**: **189 total tests** (135 unit + 54 doc with all features)
  - Added 13 new unit tests for advanced middleware and flow control
  - Added 4 new doc tests (TimeoutMiddleware, FilterMiddleware, BackpressureConfig, PoisonMessageDetector)
  - Added 19 new doc tests for comprehensive utilities module
  - All tests passing with zero warnings
- ✅ **Zero Warnings Policy**: Clean build with no warnings or clippy issues maintained

### Recent Enhancements (v0.3.0 - 2025-12-05)
- ✅ **Message Scheduling**: Delayed message delivery (NEW)
  - ScheduleConfig with delay and absolute timestamp support
  - MessageScheduler trait for scheduling operations
  - ScheduledMessage information tracking
  - 4 comprehensive unit tests + 1 doc test
- ✅ **Consumer Groups**: Load-balanced consumption (NEW)
  - ConsumerGroupConfig with heartbeat and rebalance settings
  - ConsumerGroup trait for group management
  - Support for distributed consumption with automatic load balancing
  - 2 comprehensive unit tests + 1 doc test
- ✅ **Message Replay**: Debugging and recovery (NEW)
  - ReplayConfig for time-based replay
  - MessageReplay trait for replay sessions
  - ReplayProgress tracking with completion percentage
  - Support for replay speed control
  - 5 comprehensive unit tests + 1 doc test
- ✅ **Quota Management**: Resource limits (NEW)
  - QuotaConfig with message, byte, and rate limits
  - QuotaManager trait for quota operations
  - QuotaUsage statistics with percentage tracking
  - QuotaEnforcement policies (Reject, Throttle, Warn)
  - 8 comprehensive unit tests + 1 doc test
- ✅ **Enhanced Testing**: **155 total tests** (122 unit + 33 doc with all features)
  - Added 19 new unit tests for advanced features
  - Added 4 new doc tests (ScheduleConfig, ReplayConfig, ConsumerGroupConfig, QuotaConfig)
  - All tests passing with zero warnings
- ✅ **Zero Warnings Policy**: Clean build with no warnings or clippy issues maintained

### Recent Enhancements (v0.2.0 - 2025-12-05)
- ✅ **Dead Letter Queue (DLQ) Support**: Complete DLQ implementation (NEW)
  - DlqConfig with builder pattern (max_retries, ttl, metadata)
  - DeadLetterQueue trait for handling failed messages
  - DlqStats for monitoring failed messages with age tracking
  - 6 comprehensive unit tests for DLQ functionality
  - Doc test for DlqConfig
- ✅ **Message Transaction Support**: ACID guarantees for message operations (NEW)
  - MessageTransaction trait for atomic operations
  - IsolationLevel enum (ReadUncommitted, ReadCommitted, RepeatableRead, Serializable)
  - TransactionState enum (Active, Committed, RolledBack)
  - 3 comprehensive unit tests for transaction types
- ✅ **Performance Benchmarks**: Criterion-based benchmarks (UPDATED - v0.4.5)
  - **Core Operations** (9 benchmarks):
    - Message creation benchmarks
    - Envelope creation benchmarks
    - Queue config builder benchmarks
    - Retry policy delay benchmarks
    - Middleware chain benchmarks (3 middlewares)
    - Message options benchmarks
    - DLQ config benchmarks
    - Batch publish result benchmarks (10, 100, 1000 messages)
    - Broker metrics increment benchmarks
  - **Middleware** (3 benchmarks - v0.4.4):
    - TracingMiddleware benchmark
    - BatchingMiddleware benchmark
    - AuditMiddleware benchmark
  - **Utility Functions** (21 benchmarks - EXPANDED v0.4.5):
    - Batch optimization: calculate_optimal_batch_size, calculate_optimal_workers
    - Routing: match_routing_pattern
    - Performance: analyze_broker_performance, calculate_throughput
    - Queue health: analyze_queue_health, estimate_drain_time
    - Load balancing: calculate_load_distribution
    - Resilience: analyze_circuit_breaker, analyze_pool_health, calculate_backoff_delay
    - Message management: calculate_priority_score, identify_stale_messages, suggest_batch_groups
    - Capacity planning: calculate_queue_capacity, suggest_partition_count, analyze_message_patterns
    - Prediction: predict_throughput, calculate_efficiency, calculate_health_score, suggest_retry_policy
  - **Config Builders** (3 benchmarks - v0.4.4):
    - BackpressureConfig benchmark
    - CircuitBreakerConfig benchmark
    - PoolConfig benchmark
  - **Total: 36 comprehensive benchmarks** covering all critical code paths and nearly all utility functions
- ✅ **Usage Examples**: **9 comprehensive examples** (UPDATED - v0.4.5)
  - basic_broker.rs: Complete broker implementation example
  - middleware_usage.rs: Middleware chain and individual middleware examples
  - dlq_usage.rs: DLQ configuration and best practices
  - batch_operations.rs: Batch operations and performance guidance
  - **advanced_features.rs: Scheduling, consumer groups, replay, quotas (v0.4.1)**
  - **flow_control.rs: Backpressure, poison detection, timeout, filter (v0.4.1)**
  - **circuit_breaker.rs: Circuit breaker, connection pooling, health checks (v0.4.1)**
  - **transactions.rs: Message transactions with ACID guarantees (v0.4.1)**
  - **utilities_showcase.rs: Practical usage of 37 utility functions (NEW - v0.4.5)**
- ✅ **Enhanced Testing**: **203 total tests** (139 unit + 64 doc with all features)
  - Comprehensive coverage of all middleware types
  - Comprehensive coverage of all utility functions
  - All tests passing with zero warnings
- ✅ **Zero Warnings Policy**: Clean build with no warnings or clippy issues maintained

### Recent Enhancements (v0.1.0 - 2025-12-04)
- ✅ **Doc Tests**: 28 comprehensive doc tests for API validation (UPDATED - 2025-12-04)
  - Core types: QueueMode, QueueConfig, Envelope, BrokerError
  - Reliability: RetryPolicy, HealthStatus, HealthCheckResponse
  - Messaging: Priority, MessageOptions, BatchPublishResult
  - Resilience: CircuitState, CircuitBreakerConfig, PoolConfig
  - Topology: ExchangeType, ExchangeConfig, BindingConfig
  - Connection: ConnectionState
  - Metrics: BrokerMetrics
  - Middleware: MiddlewareChain, ValidationMiddleware, LoggingMiddleware, MetricsMiddleware, RetryLimitMiddleware
  - Advanced Middleware: RateLimitingMiddleware, DeduplicationMiddleware
  - Feature-gated Middleware: CompressionMiddleware, SigningMiddleware, EncryptionMiddleware
- ✅ **Advanced Middleware**: RateLimitingMiddleware, DeduplicationMiddleware
- ✅ **Protocol-Integrated Middleware**: CompressionMiddleware, SigningMiddleware, EncryptionMiddleware
- ✅ **Feature Flags**: Optional middleware with feature gates (`compression`, `signing`, `encryption`, `full`)
- ✅ **Enhanced Testing**: 94 unit tests + 28 doc tests = 122 total tests
- ✅ **Zero Warnings**: Clean build with no warnings or clippy issues
- ✅ **Comprehensive Documentation**: Complete README with middleware examples and usage patterns

## Completed Features

### Message Scheduling ✅ (NEW - v0.3.0)
- [x] ScheduleConfig - Delayed delivery configuration
  - [x] `delay()`, `at()` - Schedule by duration or timestamp
  - [x] `with_window()` - Set execution window
  - [x] `is_ready()`, `delivery_time()` - Schedule status methods
- [x] MessageScheduler trait - Message scheduling operations
  - [x] `schedule_message()` - Schedule message for delivery
  - [x] `cancel_scheduled()` - Cancel scheduled message
  - [x] `list_scheduled()`, `scheduled_count()` - Query scheduled messages
- [x] ScheduledMessage - Scheduled message information

### Consumer Groups ✅ (NEW - v0.3.0)
- [x] ConsumerGroupConfig - Consumer group configuration
  - [x] `new()`, `with_max_consumers()` - Configuration methods
  - [x] `with_rebalance_timeout()`, `with_heartbeat_interval()` - Timing settings
- [x] ConsumerGroup trait - Load-balanced consumption
  - [x] `join_group()`, `leave_group()` - Group membership
  - [x] `heartbeat()` - Maintain membership
  - [x] `group_members()` - Get group members
  - [x] `consume_from_group()` - Consume with load balancing

### Message Replay ✅ (NEW - v0.3.0)
- [x] ReplayConfig - Replay configuration
  - [x] `from_duration()`, `from_timestamp()` - Replay time range
  - [x] `until()`, `with_max_messages()`, `with_speed()` - Replay control
  - [x] `start_timestamp()` - Calculate start time
- [x] MessageReplay trait - Message replay operations
  - [x] `begin_replay()`, `stop_replay()` - Replay session control
  - [x] `replay_next()` - Get next replay message
  - [x] `replay_progress()` - Track replay progress
- [x] ReplayProgress - Replay progress tracking
  - [x] `completion_percent()` - Progress percentage

### Quota Management ✅ (NEW - v0.3.0)
- [x] QuotaConfig - Resource quota configuration
  - [x] `new()`, `with_max_messages()`, `with_max_bytes()`, `with_max_rate()` - Quota limits
  - [x] `with_max_per_consumer()`, `with_enforcement()` - Additional controls
- [x] QuotaEnforcement enum - Enforcement policies (Reject, Throttle, Warn)
- [x] QuotaUsage - Quota usage statistics
  - [x] `is_*_quota_exceeded()` - Check quota violations
  - [x] `usage_percent()` - Get usage percentage
- [x] QuotaManager trait - Quota management operations
  - [x] `set_quota()`, `get_quota()` - Configure quotas
  - [x] `quota_usage()`, `reset_quota()` - Monitor and reset
  - [x] `check_quota()` - Validate operations

### Dead Letter Queue (DLQ) ✅ (NEW - v0.2.0)
- [x] DlqConfig - DLQ configuration with builder pattern
  - [x] `new()`, `with_max_retries()`, `without_retry_limit()` - Configuration methods
  - [x] `with_ttl()`, `with_metadata()` - Additional settings
- [x] DeadLetterQueue trait - Failed message handling
  - [x] `send_to_dlq()` - Send failed message to DLQ
  - [x] `get_from_dlq()` - Retrieve messages from DLQ
  - [x] `retry_from_dlq()` - Retry message from DLQ
  - [x] `purge_dlq()` - Clear DLQ
  - [x] `dlq_stats()` - Get DLQ statistics
- [x] DlqStats - DLQ monitoring
  - [x] `is_empty()`, `oldest_message_age_secs()` - Statistics methods
  - [x] Message count and failure reason tracking

### Message Transactions ✅ (NEW - v0.2.0)
- [x] MessageTransaction trait - ACID message operations
  - [x] `begin_transaction()` - Start transaction with isolation level
  - [x] `publish_transactional()` - Publish within transaction
  - [x] `consume_transactional()` - Consume within transaction
  - [x] `commit_transaction()` - Commit transaction
  - [x] `rollback_transaction()` - Rollback transaction
  - [x] `transaction_state()` - Get transaction state
- [x] IsolationLevel enum - Transaction isolation levels
- [x] TransactionState enum - Transaction state tracking

### Performance & Documentation ✅ (NEW - v0.2.0)
- [x] **Benchmarks** - Criterion-based performance testing
  - [x] 9 comprehensive benchmarks covering all core operations
- [x] **Examples** - Practical usage examples
  - [x] 4 complete examples with detailed comments
  - [x] Best practices and performance guidance

### Core Traits ✅
- [x] Transport trait - Connection management
- [x] Producer trait - Message publishing
- [x] Consumer trait - Message consumption
- [x] Broker trait - Full broker interface
- [x] ExtendedProducer trait - Producer with message options

### Types ✅
- [x] QueueMode (FIFO, Priority)
  - [x] `is_fifo()`, `is_priority()` - mode checks
  - [x] `Display` implementation for logging
- [x] Envelope (message + metadata)
  - [x] `new()` constructor
  - [x] `is_redelivered()`, `task_id()`, `task_name()` - utility methods
  - [x] `Display` implementation
- [x] QueueConfig - Queue configuration
  - [x] Builder pattern with `with_mode()`, `with_ttl()`, `with_durable()`, `with_auto_delete()`, `with_max_message_size()`
- [x] BrokerError - Comprehensive error types
  - [x] `is_*()` methods for error type checking
  - [x] `is_retryable()` - Check if error should trigger retry
  - [x] `category()` - Get error category as string

### Operations ✅
- [x] Basic publish/consume
- [x] Routing support (exchange/routing_key)
- [x] Message acknowledgment
- [x] Message rejection with requeue
- [x] Queue size queries
- [x] Queue management (create, delete, purge, list)

### Batch Operations ✅
- [x] BatchProducer trait - Batch message publishing
  - [x] `publish_batch()` - Publish multiple messages to queue
  - [x] `publish_batch_with_routing()` - Batch publish with routing
- [x] BatchConsumer trait - Batch message consuming
  - [x] `consume_batch()` - Consume multiple messages
  - [x] `ack_batch()` - Acknowledge multiple messages
  - [x] `reject_batch()` - Reject multiple messages
- [x] BatchPublishResult - Result tracking for batch operations
  - [x] `success()`, `is_complete_success()`, `total()` methods

### Reliability ✅
- [x] RetryPolicy - Connection retry policies
  - [x] Exponential backoff with configurable multiplier
  - [x] Maximum delay capping
  - [x] Jitter support
  - [x] `delay_for_attempt()`, `should_retry()` methods
  - [x] Presets: `no_retry()`, `infinite()`, `fixed_delay()`
- [x] ConnectionState - Connection state tracking
- [x] ConnectionEvent - Connection event types
- [x] ConnectionObserver trait - State change callbacks

### Connection Pooling ✅
- [x] PoolConfig - Connection pool configuration
  - [x] `min_connections`, `max_connections`
  - [x] `idle_timeout`, `acquire_timeout`, `max_lifetime`
  - [x] Builder pattern with `with_*()` methods
- [x] PoolStats - Pool statistics
  - [x] `connections_created`, `connections_closed`
  - [x] `active_connections`, `idle_connections`
  - [x] `acquire_requests`, `acquire_timeouts`
- [x] ConnectionPool trait - Pool management interface

### Circuit Breaker ✅
- [x] CircuitState enum (Closed, Open, HalfOpen)
- [x] CircuitBreakerConfig - Configuration
  - [x] `failure_threshold`, `success_threshold`
  - [x] `open_duration`, `failure_window`
  - [x] Builder pattern with `with_*()` methods
- [x] CircuitBreakerStats - Statistics with `success_rate()`
- [x] CircuitBreaker trait - Circuit breaker interface
  - [x] `state()`, `is_allowed()`, `reset()`
  - [x] `record_success()`, `record_failure()`

### Health Check ✅
- [x] HealthCheck trait - Health monitoring interface
  - [x] `health_check()` - Full health check
  - [x] `ping()` - Simple connectivity check
- [x] HealthStatus enum (Healthy, Degraded, Unhealthy)
  - [x] `is_healthy()`, `is_operational()` methods
- [x] HealthCheckResponse - Detailed health information
  - [x] Status, broker type, connection, latency, details

### Metrics ✅
- [x] BrokerMetrics - Comprehensive metrics collection
  - [x] Messages: published, consumed, acknowledged, rejected
  - [x] Errors: publish_errors, consume_errors
  - [x] Connections: active_connections, connection_attempts, connection_failures
- [x] MetricsProvider trait
  - [x] `get_metrics()` - Get current metrics snapshot
  - [x] `reset_metrics()` - Reset all metrics

### Message Options ✅
- [x] Priority enum (Lowest, Low, Normal, High, Highest)
  - [x] `as_u8()`, `from_u8()` - Numeric conversion
  - [x] Implements `Ord` for comparison
- [x] MessageOptions - Message-level options
  - [x] `priority`, `ttl`, `expires_at`, `delay`
  - [x] `correlation_id`, `reply_to` for RPC patterns
  - [x] `headers` for custom metadata
  - [x] `is_expired()`, `should_delay()` methods
  - [x] `sign`, `signing_key` - Message authentication (HMAC)
  - [x] `encrypt`, `encryption_key` - Message encryption (AES-256-GCM)
  - [x] `compress` - Compression hint
  - [x] `with_signing()`, `with_encryption()`, `with_compression()` builders
  - [x] `should_sign()`, `should_encrypt()`, `should_compress()` checks

### Middleware Support ✅ (NEW)
- [x] MessageMiddleware trait - Message transformation interface
  - [x] `before_publish()` - Pre-publish processing hook
  - [x] `after_consume()` - Post-consume processing hook
  - [x] `name()` - Middleware identification
- [x] MiddlewareChain - Middleware pipeline
  - [x] `with_middleware()` - Builder pattern for adding middleware
  - [x] `process_before_publish()` / `process_after_consume()` - Pipeline execution
  - [x] `len()`, `is_empty()` - Chain inspection
- [x] MiddlewareProducer trait - Producer with middleware support
  - [x] `publish_with_middleware()` - Publish with transformation
- [x] MiddlewareConsumer trait - Consumer with middleware support
  - [x] `consume_with_middleware()` - Consume with transformation
- [x] Built-in Middleware Implementations ✅
  - [x] **ValidationMiddleware** - Message structure validation
    - [x] `with_max_body_size()`, `without_body_size_limit()` - Body size control
    - [x] `with_require_task_name()` - Task name requirement
  - [x] **LoggingMiddleware** - Message event logging
    - [x] `with_body_logging()` - Enable detailed body logging
  - [x] **MetricsMiddleware** - Message statistics collection
    - [x] `get_metrics()` - Get metrics snapshot
  - [x] **RetryLimitMiddleware** - Retry count enforcement
    - [x] Configurable max retries
  - [x] **RateLimitingMiddleware** - Rate limiting (NEW) ✅
    - [x] Token bucket algorithm
    - [x] Configurable rate per second
    - [x] Automatic token refill
  - [x] **DeduplicationMiddleware** - Duplicate message prevention (NEW) ✅
    - [x] Message ID tracking
    - [x] Configurable cache size
    - [x] `with_default_cache()` - 10K message cache
  - [x] **CompressionMiddleware** - Message compression (NEW, feature-gated) ✅
    - [x] Gzip support via celers-protocol
    - [x] `with_min_size()` - Minimum compression threshold
    - [x] `with_level()` - Compression level control
  - [x] **SigningMiddleware** - Message signing (NEW, feature-gated) ✅
    - [x] HMAC-SHA256 via celers-protocol
    - [x] Message integrity verification
  - [x] **EncryptionMiddleware** - Message encryption (NEW, feature-gated) ✅
    - [x] AES-256-GCM via celers-protocol
    - [x] Automatic nonce handling
  - [x] **TimeoutMiddleware** - Processing timeout enforcement (NEW - v0.4.0) ✅
    - [x] Configurable timeout duration
    - [x] Timeout metadata in message headers
  - [x] **FilterMiddleware** - Selective message processing (NEW - v0.4.0) ✅
    - [x] Custom predicate-based filtering
    - [x] Flexible message matching
  - [x] **SamplingMiddleware** - Statistical message sampling (NEW - v0.4.2) ✅
    - [x] Configurable sample rate (0.0 to 1.0)
    - [x] Deterministic sampling based on counter
  - [x] **TransformationMiddleware** - Message content transformation (NEW - v0.4.2) ✅
    - [x] Custom transformation functions
    - [x] Applies on both publish and consume
  - [x] **TracingMiddleware** - Distributed tracing support (NEW - v0.4.3) ✅
    - [x] Automatic trace ID injection and propagation
    - [x] Service name tagging
    - [x] Span ID generation for operation tracking
    - [x] Timestamp injection for latency analysis
  - [x] **BatchingMiddleware** - Automatic message batching hints (NEW - v0.4.3) ✅
    - [x] Configurable batch size and timeout
    - [x] Metadata injection for batch-aware consumers
    - [x] Default settings (100 messages, 5 sec timeout)
  - [x] **AuditMiddleware** - Comprehensive audit logging (NEW - v0.4.3) ✅
    - [x] Configurable body logging (with/without PII)
    - [x] Unique audit ID generation
    - [x] Timestamp and operation tracking
    - [x] Compliance-ready audit trail

### Flow Control & Resilience ✅ (NEW - v0.4.0)
- [x] BackpressureConfig - Flow control configuration
  - [x] `with_max_pending()`, `with_max_queue_size()` - Capacity limits
  - [x] `with_high_watermark()`, `with_low_watermark()` - Threshold configuration
  - [x] `should_apply_backpressure()`, `should_release_backpressure()` - Backpressure detection
  - [x] `is_at_capacity()` - Capacity check
- [x] PoisonMessageDetector - Poison message detection
  - [x] `with_max_failures()`, `with_failure_window()` - Configuration methods
  - [x] `record_failure()` - Track message failures
  - [x] `is_poison()`, `failure_count()` - Poison detection
  - [x] `clear_failures()`, `clear_all()` - Failure history management

### Utilities Module ✅ (NEW - v0.4.0)
- [x] **Batch Optimization** (2 functions)
  - [x] `calculate_optimal_batch_size()` - Calculate optimal batch size based on message size and throughput
  - [x] `calculate_optimal_workers()` - Calculate optimal number of consumer workers
- [x] **Routing Utilities** (3 functions)
  - [x] `match_routing_pattern()` - AMQP-style topic pattern matching with wildcards (* and #)
  - [x] `match_direct_routing()` - Direct exchange exact matching
  - [x] `match_fanout_routing()` - Fanout exchange matching (always true)
- [x] **Performance Analysis** (4 functions)
  - [x] `analyze_broker_performance()` - Calculate success/error/ack rates from metrics
  - [x] `calculate_throughput()` - Calculate messages per second
  - [x] `calculate_avg_latency()` - Calculate average message latency
  - [x] `analyze_circuit_breaker()` - Analyze circuit breaker state and recommend action
- [x] **Queue Management** (4 functions)
  - [x] `analyze_queue_health()` - Analyze queue health based on size thresholds
  - [x] `estimate_drain_time()` - Estimate time to drain queue at given consumption rate
  - [x] `estimate_queue_memory()` - Estimate memory usage for a queue
  - [x] `calculate_load_distribution()` - Distribute workers across multiple queues
- [x] **Connection & Resilience** (2 functions)
  - [x] `analyze_pool_health()` - Analyze connection pool health and efficiency
  - [x] `calculate_backoff_delay()` - Calculate exponential backoff with jitter
- [x] **Message Utilities** (2 functions)
  - [x] `estimate_message_size()` - Estimate serialized message size for planning
  - [x] `generate_deduplication_id()` - Generate stable message ID for deduplication
- [x] **Capacity Planning & Optimization** (10 functions - NEW v0.4.3)
  - [x] `calculate_queue_capacity()` - Calculate required queue capacity for workload
  - [x] `suggest_partition_count()` - Optimal partition count for distributed queues
  - [x] `calculate_cost_estimate()` - Operational cost estimation
  - [x] `analyze_message_patterns()` - Message size/frequency pattern analysis
  - [x] `calculate_buffer_size()` - Optimal buffer size for batching
  - [x] `estimate_memory_footprint()` - Total memory footprint estimation
  - [x] `suggest_ttl()` - Appropriate TTL based on patterns
  - [x] `calculate_replication_lag()` - Acceptable replication lag calculation
  - [x] `calculate_bandwidth_requirement()` - Network bandwidth calculation
  - [x] `suggest_retry_policy()` - Retry policy recommendations

### Admin & Topology ✅
- [x] Admin trait - Broker administration
  - [x] `declare_exchange()`, `delete_exchange()`, `list_exchanges()`
  - [x] `bind_queue()`, `unbind_queue()`, `list_bindings()`
  - [x] `queue_info()` - Get detailed queue information
- [x] ExchangeType enum (Direct, Fanout, Topic, Headers)
- [x] ExchangeConfig - Exchange configuration with builders
- [x] BindingConfig - Queue binding configuration
- [x] QueueInfo - Queue information structure

### Mock Implementation ✅
- [x] MockBroker - Full mock broker for testing
  - [x] Implements all traits: Transport, Producer, Consumer, Broker
  - [x] Implements batch traits: BatchProducer, BatchConsumer
  - [x] Implements health: HealthCheck
  - [x] Implements metrics: MetricsProvider
  - [x] In-memory message storage
  - [x] Delivery tag tracking for ack/reject
  - [x] Requeue support

## Testing ✅

- [x] Unit tests for queue config (5 tests)
- [x] Unit tests for BrokerError (7 tests)
- [x] Unit tests for QueueMode (2 tests)
- [x] Unit tests for Envelope (3 tests)
- [x] Unit tests for RetryPolicy (7 tests)
- [x] Unit tests for HealthStatus/HealthCheckResponse (5 tests)
- [x] Unit tests for BrokerMetrics (2 tests)
- [x] Unit tests for ExchangeType/ExchangeConfig/BindingConfig (3 tests)
- [x] Unit tests for ConnectionState (1 test)
- [x] Unit tests for BatchPublishResult (2 tests)
- [x] Async tests for MockBroker (10 tests)
- [x] Unit tests for PoolConfig/PoolStats (4 tests)
- [x] Unit tests for CircuitState/CircuitBreakerConfig/Stats (4 tests)
- [x] Unit tests for Priority (5 tests)
- [x] Unit tests for MessageOptions (9 tests) - includes security features
- [x] Unit tests for Middleware (7 tests)
- [x] Unit tests for Built-in Middleware (12 tests) - validation, logging, metrics, retry limit
- [x] Unit tests for Advanced Middleware (7 tests) - rate limiting, deduplication
- [x] Unit tests for DLQ (6 tests) - config, stats, age tracking (NEW - v0.2.0)
- [x] Unit tests for Transactions (3 tests) - isolation levels, states (NEW - v0.2.0)
- [x] Unit tests for Scheduling (4 tests) - delay, timestamp, window, readiness (NEW - v0.3.0)
- [x] Unit tests for Consumer Groups (2 tests) - config, builders (NEW - v0.3.0)
- [x] Unit tests for Replay (5 tests) - duration, timestamp, progress (NEW - v0.3.0)
- [x] Unit tests for Quota (8 tests) - config, usage, enforcement (NEW - v0.3.0)
- [x] Unit tests for Timeout Middleware (2 tests) - creation, header injection (NEW - v0.4.0)
- [x] Unit tests for Filter Middleware (2 tests) - creation, filtering (NEW - v0.4.0)
- [x] Unit tests for Backpressure (4 tests) - config, watermarks, capacity (NEW - v0.4.0)
- [x] Unit tests for Poison Message Detector (5 tests) - tracking, clearing, defaults (NEW - v0.4.0)
- [x] **Unit Tests: 135 tests passing** (up from 63 → 75 → 87 → 94 → 103 → 122 → 135) ✅
- [x] **Doc Tests: 54 tests passing with all features** (up from 27 → 28 → 29 → 33 → 35 → 37 → 45 → 54) ✅
  - [x] Core types: QueueMode, QueueConfig, Envelope, BrokerError
  - [x] Reliability: RetryPolicy, HealthStatus, HealthCheckResponse
  - [x] Messaging: Priority, MessageOptions, BatchPublishResult
  - [x] Resilience: CircuitState, CircuitBreakerConfig, PoolConfig
  - [x] Topology: ExchangeType, ExchangeConfig, BindingConfig
  - [x] Connection: ConnectionState
  - [x] Metrics: BrokerMetrics
  - [x] Middleware: MiddlewareChain, ValidationMiddleware, LoggingMiddleware, MetricsMiddleware, RetryLimitMiddleware
  - [x] Advanced Middleware: RateLimitingMiddleware, DeduplicationMiddleware
  - [x] Feature-gated Middleware: CompressionMiddleware, SigningMiddleware, EncryptionMiddleware
  - [x] DLQ: DlqConfig (NEW - v0.2.0)
  - [x] Scheduling: ScheduleConfig (NEW - v0.3.0)
  - [x] Consumer Groups: ConsumerGroupConfig (NEW - v0.3.0)
  - [x] Replay: ReplayConfig (NEW - v0.3.0)
  - [x] Quota: QuotaConfig (NEW - v0.3.0)
  - [x] Flow Control: TimeoutMiddleware, FilterMiddleware, BackpressureConfig, PoisonMessageDetector (NEW - v0.4.0)
  - [x] Utilities (19 functions): calculate_optimal_batch_size, calculate_optimal_workers, match_routing_pattern, match_direct_routing, match_fanout_routing, analyze_broker_performance, calculate_throughput, calculate_avg_latency, analyze_circuit_breaker, analyze_queue_health, estimate_drain_time, estimate_queue_memory, calculate_load_distribution, analyze_pool_health, calculate_backoff_delay, estimate_message_size, generate_deduplication_id (NEW - v0.4.0)
- [x] **Total: 189 tests passing** (135 unit + 54 doc with all features) ✅

## Documentation

- [x] Comprehensive README
- [x] Trait documentation
- [x] Example usage
- [x] Implementation guide (covered in README Quick Start)
- [x] Best practices guide (covered in README Best Practices section)
- [x] **Middleware documentation** ✅ (NEW)
  - [x] Built-in middleware examples (Validation, Logging, Metrics, Retry Limit, Rate Limiting, Deduplication)
  - [x] Feature-gated middleware examples (Compression, Signing, Encryption)
  - [x] Middleware chain composition guide
  - [x] Complete middleware pipeline example

## Dependencies

- `celers-protocol` - Message types
- `async-trait` - Async trait support
- `thiserror` - Error types
- `serde` - Serialization support
- `uuid` - UUID generation

## Future Enhancements

### Protocol Integration
- [x] Middleware trait integration from celers-protocol ✅
  - [x] Pre-publish middleware hooks ✅
  - [x] Post-consume middleware hooks ✅
  - [x] Built-in validation/signing/encryption middleware implementations ✅
    - [x] ValidationMiddleware (body size, task name validation) ✅
    - [x] LoggingMiddleware (message event logging) ✅
    - [x] MetricsMiddleware (statistics collection) ✅
    - [x] RetryLimitMiddleware (retry count enforcement) ✅
- [x] Enhanced MessageOptions for celers-protocol features ✅
  - [x] Authentication headers (HMAC signing) ✅
  - [x] Encryption metadata ✅
  - [x] Compression hints ✅

### Advanced Features
- [x] Message transformation pipeline ✅
- [x] Built-in retry middleware ✅
- [x] Rate limiting middleware ✅ (NEW)
  - [x] Token bucket algorithm ✅
  - [x] Configurable rate per second ✅
  - [x] Automatic token refill ✅
- [x] Message deduplication middleware ✅ (NEW)
  - [x] Message ID tracking ✅
  - [x] Configurable cache size ✅
  - [x] Automatic cache eviction ✅
- [x] Compression middleware (integration with celers-protocol) ✅ (NEW)
  - [x] Gzip compression support ✅
  - [x] Configurable minimum size threshold ✅
  - [x] Configurable compression level ✅
- [x] Signing middleware (integration with celers-protocol auth) ✅ (NEW)
  - [x] HMAC-SHA256 signing ✅
  - [x] Message integrity verification ✅
- [x] Encryption middleware (integration with celers-protocol crypto) ✅ (NEW)
  - [x] AES-256-GCM encryption ✅
  - [x] Automatic nonce generation ✅
  - [x] Secure message decryption ✅
- [x] Timeout middleware ✅ (NEW - v0.4.0)
  - [x] Configurable timeout duration ✅
  - [x] Timeout metadata injection ✅
- [x] Filter middleware ✅ (NEW - v0.4.0)
  - [x] Custom predicate-based filtering ✅
  - [x] Flexible message matching ✅
- [x] Backpressure configuration ✅ (NEW - v0.4.0)
  - [x] Configurable watermarks ✅
  - [x] Capacity management ✅
  - [x] Automatic backpressure detection ✅
- [x] Poison message detection ✅ (NEW - v0.4.0)
  - [x] Failure tracking ✅
  - [x] Time-window-based detection ✅
  - [x] Infinite retry loop prevention ✅

## Notes

- This crate provides traits AND a mock implementation for testing
- Real implementations in celers-broker-* crates
- Inspired by Python Kombu library
- All traits require Send + Sync for async usage
- Mock broker useful for unit testing without external dependencies
- Circuit breaker pattern for resilient broker connections
- Connection pooling support for high-throughput scenarios
- Middleware support for message transformation pipelines
- Built-in middleware: Validation, Logging, Metrics, Retry Limit, Rate Limiting, Deduplication, Timeout, Filter, Sampling, Transformation, Tracing, Batching, Audit
- Optional middleware (feature-gated): Compression, Signing, Encryption
- Full integration with celers-protocol security features (signing, encryption, compression)
- **Dead Letter Queue (DLQ) support** for handling failed messages with retry tracking ✅ (v0.2.0)
- **Message Transaction support** for ACID guarantees on message operations ✅ (v0.2.0)
- **Message Scheduling** for delayed delivery with flexible timing ✅ (v0.3.0)
- **Consumer Groups** for load-balanced distributed consumption ✅ (v0.3.0)
- **Message Replay** for debugging and recovery with progress tracking ✅ (v0.3.0)
- **Quota Management** for resource limits with enforcement policies ✅ (v0.3.0)
- **Backpressure Configuration** for flow control with watermark-based detection ✅ (v0.4.0)
- **Poison Message Detection** for preventing infinite retry loops ✅ (v0.4.0)
- **Timeout Middleware** for message processing time limits ✅ (v0.4.0)
- **Filter Middleware** for selective message processing ✅ (v0.4.0)
- **Utilities Module** with 37 helper functions for optimization and analysis ✅ (v0.4.0, v0.4.2, v0.4.3)
- **Performance Benchmarks** - 36 comprehensive Criterion-based benchmarks covering all critical code paths and nearly all utilities ✅ (v0.2.0, v0.4.4, v0.4.5)
- **9 Comprehensive Examples** covering all features: basics, middleware, DLQ, batch, advanced features, flow control, circuit breaker, transactions, utilities ✅ (v0.2.0, v0.4.1, v0.4.5)
- **228 total tests** (151 unit tests + 77 doc tests with all features), 0 warnings, 0 clippy warnings ✅
- **Doc tests validate API examples** ensuring documentation is always correct and compilable ✅
- **77 doc tests cover all major public types, middleware, and utilities** for comprehensive API validation ✅
- **Complete middleware documentation** with examples for all 16 built-in and feature-gated middleware ✅
- Feature flags: `compression`, `signing`, `encryption`, `full` (enables all)
