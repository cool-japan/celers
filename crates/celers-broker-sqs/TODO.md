# celers-broker-sqs TODO

> AWS SQS broker implementation for CeleRS

## Status: ✅ FEATURE COMPLETE + ADVANCED PRODUCTION UTILITIES + INTELLIGENT AUTO-TUNING

Full AWS SQS broker implementation with long polling, visibility timeout management, FIFO queues, DLQ, SSE, IAM authentication, circuit breaker pattern, real-time cost tracking, advanced batch optimization, distributed tracing, quota management, multi-queue routing, performance profiling, message replay, SLA monitoring, **unified metrics aggregation**, **workload-specific presets**, and **self-adaptive auto-tuning**.

## Completed Features

### Core Operations ✅
- [x] `connect()` - Initialize AWS SDK client and get queue URL
- [x] `disconnect()` - Clean up AWS client
- [x] `publish()` - Send messages to SQS queue
- [x] `consume()` - Receive messages with long polling
- [x] `ack()` - Delete message from queue (acknowledge)
- [x] `reject()` - Reject message with optional requeue
- [x] `queue_size()` - Get approximate number of messages

### Queue Management ✅
- [x] `create_queue()` - Create SQS queue with attributes
- [x] `delete_queue()` - Delete SQS queue
- [x] `purge()` - Remove all messages from queue
- [x] `list_queues()` - List all available queues

### AWS SQS Features ✅
- [x] Long polling (up to 20 seconds wait time)
- [x] Visibility timeout configuration
- [x] Message attributes for priority and correlation ID
- [x] Receive count tracking (redelivered flag)
- [x] IAM role authentication via AWS SDK
- [x] Queue URL caching for performance

### Configuration ✅
- [x] Builder pattern for configuration
- [x] `with_visibility_timeout()` - Set visibility timeout
- [x] `with_wait_time()` - Set long polling wait time
- [x] `with_max_messages()` - Set max messages per poll
- [x] `with_message_retention()` - Set message retention period
- [x] `with_delay_seconds()` - Set default message delay
- [x] Automatic AWS credential detection

### Batch Operations ✅
- [x] `publish_batch()` - Send up to 10 messages in a single API call
- [x] `consume_batch()` - Receive up to 10 messages at once
- [x] `ack_batch()` - Delete up to 10 messages in a single call
- [x] 10x cost reduction vs individual operations
- [x] Automatic handling of partial failures

### FIFO Queue Support ✅
- [x] `FifoConfig` struct for FIFO-specific settings
- [x] `with_fifo()` - Configure FIFO queue settings
- [x] `publish_fifo()` - Publish with message group ID and deduplication
- [x] `publish_fifo_batch()` - Batch publish for FIFO queues
- [x] Content-based deduplication support
- [x] High throughput mode (3000 TPS per message group)
- [x] Automatic deduplication ID generation

### Dead Letter Queue (DLQ) ✅
- [x] `DlqConfig` struct for DLQ configuration
- [x] `with_dlq()` - Configure DLQ at queue creation
- [x] `set_redrive_policy()` - Set DLQ for existing queues
- [x] Configurable max receive count (1-1000)

### Server-Side Encryption (SSE) ✅
- [x] `SseConfig` struct for encryption settings
- [x] `with_sse()` - Configure encryption
- [x] SQS-managed encryption support
- [x] KMS encryption with custom key ID
- [x] Configurable data key reuse period

### Queue Monitoring ✅
- [x] `get_queue_stats()` - Get detailed queue statistics
- [x] `QueueStats` struct with message counts
- [x] Message retention and visibility timeout info
- [x] FIFO queue detection
- [x] Creation and modification timestamps
- [x] `get_queue_arn()` - Get queue ARN
- [x] `extend_visibility()` - Extend message visibility timeout
- [x] `extend_visibility_batch()` - Batch visibility extension

### Advanced Operations ✅
- [x] `publish_with_delay()` - Publish with per-message delay (0-900 seconds)
- [x] `update_queue_attributes()` - Update visibility timeout, retention, delay
- [x] `remove_redrive_policy()` - Remove DLQ configuration
- [x] `get_redrive_policy()` - Get current DLQ configuration
- [x] `tag_queue()` - Add metadata tags to queue
- [x] `get_queue_tags()` - Get queue tags
- [x] `untag_queue()` - Remove tags from queue
- [x] `health_check()` - Verify broker can access queue (for K8s probes/monitoring)

### Configuration Presets ✅
- [x] `production()` - Production-optimized settings (long polling, batching, 14-day retention)
- [x] `development()` - Development-optimized settings (quick feedback, short retention)
- [x] `cost_optimized()` - Cost-optimized settings (90% cost reduction with adaptive polling)
- [x] Quick start without manual configuration

### Utility Methods ✅
- [x] `validate_message_size()` - Validate message against 256 KB SQS limit
- [x] `calculate_batch_size()` - Calculate total size of message batch
- [x] `publish_batch_chunked()` - Auto-chunk large batches into groups of 10
- [x] Prevent API errors from oversized messages

### Enhanced Caching & Auto-creation ✅
- [x] Multi-queue URL caching with HashMap (improves multi-queue performance)
- [x] `with_auto_create_queue()` - Automatically create queue if it doesn't exist
- [x] `clear_queue_url_cache()` - Clear queue URL cache for troubleshooting
- [x] Improved cache management in connect/disconnect operations

### Message Compression ✅
- [x] `with_compression()` - Enable compression for messages larger than threshold
- [x] Gzip compression support for large messages
- [x] Automatic decompression on receive
- [x] Base64 encoding for compressed payloads
- [x] Compression marker for backward compatibility

### Retry & Resilience ✅
- [x] `with_retry_config()` - Configure retry behavior for AWS API calls
- [x] Exponential backoff retry mechanism
- [x] Configurable max retries and base delay
- [x] Automatic retry for transient AWS failures

### DLQ Monitoring & Management ✅
- [x] `get_dlq_messages()` - Retrieve messages from Dead Letter Queue
- [x] `redrive_dlq_message()` - Move message from DLQ back to main queue
- [x] `get_dlq_stats()` - Get DLQ statistics
- [x] Simplified DLQ management and troubleshooting

## AWS SQS Specifics

### Message Attributes
- **priority**: Stored as Number attribute (SQS doesn't natively support priority)
- **correlation_id**: Stored as String attribute for request-response patterns

### Long Polling
- Default wait time: 20 seconds (maximum allowed by SQS)
- Reduces empty receives and costs
- Configurable via `with_wait_time()`

### Visibility Timeout
- Default: 30 seconds
- Time window for message processing before redelivery
- Configurable via `with_visibility_timeout()`

### Queue URL Caching
- Queue URLs are fetched once and cached
- Reduces API calls to AWS
- Automatically refreshed on connection

## Implementation Details

### Message Format
- Messages serialized to JSON
- Compatible with Celery protocol via celers-protocol
- Message attributes used for metadata (priority, correlation_id)

### Error Handling
- AWS SDK errors mapped to BrokerError
- Automatic retry with AWS SDK defaults
- Connection errors properly propagated

### Authentication
- Uses AWS SDK credential chain:
  1. Environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
  2. IAM role (recommended for EC2/ECS)
  3. AWS credentials file (~/.aws/credentials)
  4. ECS container credentials
  5. EC2 instance metadata

## Future Enhancements

### Monitoring (CloudWatch Integration) ✅
- [x] CloudWatch metrics integration
- [x] `CloudWatchConfig` struct for configuration
- [x] `with_cloudwatch()` - Configure CloudWatch metrics
- [x] `publish_metrics()` - Publish queue stats to CloudWatch
- [x] Custom dimensions support
- [x] Automatic metric publishing for queue depth
- [x] Message age tracking (`ApproximateAgeOfOldestMessage`)
- [x] Oldest message age in CloudWatch metrics (useful for backlog detection)
- [x] CloudWatch Alarms support
- [x] `AlarmConfig` struct for alarm configuration
- [x] `create_alarm()` - Create CloudWatch Alarms programmatically
- [x] `delete_alarm()` - Delete CloudWatch Alarms
- [x] `list_alarms()` - List alarms for a queue
- [x] Helper methods for common alarms (queue depth, message age)

### Performance ✅
- [x] Adaptive polling strategies
- [x] `PollingStrategy` enum (Fixed, ExponentialBackoff, Adaptive)
- [x] `AdaptivePollingConfig` for configuration
- [x] `with_adaptive_polling()` - Configure adaptive polling
- [x] Automatic wait time adjustment based on queue activity
- [x] Cost optimization through smart polling
- [x] Parallel message processing
- [x] `consume_parallel()` - Process multiple messages concurrently
- [x] Automatic error handling and requeue on failure
- [x] Improved throughput for I/O-bound and CPU-intensive tasks
- [x] Connection pooling (not needed - AWS SDK handles this internally via hyper HTTP client)

## Production Utilities ✅

### Monitoring Module ✅
- [x] `monitoring.rs` - SQS-specific monitoring utilities
- [x] `ConsumerLagAnalysis` - Consumer lag analysis with autoscaling recommendations
- [x] `ScalingRecommendation` - Worker scaling recommendations (ScaleUp, Optimal, ScaleDown)
- [x] `MessageVelocity` - Message velocity and queue growth trend analysis
- [x] `QueueTrend` - Queue growth trend classification (RapidGrowth, SlowGrowth, Stable, etc.)
- [x] `WorkerScalingSuggestion` - Worker scaling suggestions based on queue metrics
- [x] `MessageAgeDistribution` - Message age distribution for SLA monitoring (P50, P95, P99)
- [x] `ProcessingCapacity` - Processing capacity estimation and time-to-drain calculations
- [x] `QueueHealthAssessment` - Queue health assessment (Healthy, Warning, Critical)
- [x] `SqsPerformanceMetrics` - SQS-specific performance metrics tracking
- [x] Helper functions: `analyze_sqs_consumer_lag()`, `calculate_sqs_message_velocity()`, `suggest_sqs_worker_scaling()`
- [x] Helper functions: `calculate_sqs_message_age_distribution()`, `estimate_sqs_processing_capacity()`, `assess_sqs_queue_health()`
- [x] 8 comprehensive unit tests

### Utilities Module ✅
- [x] `utilities.rs` - SQS-specific utility functions
- [x] `calculate_optimal_sqs_batch_size()` - Optimize batch size based on message size and latency
- [x] `estimate_sqs_monthly_cost()` - Estimate monthly SQS costs with/without batching and FIFO
- [x] `calculate_optimal_sqs_wait_time()` - Optimize long polling wait time for cost vs latency
- [x] `calculate_optimal_sqs_visibility_timeout()` - Calculate visibility timeout based on processing time
- [x] `estimate_sqs_drain_time()` - Estimate time to drain queue with current capacity
- [x] `calculate_sqs_api_efficiency()` - Calculate messages per API request efficiency
- [x] `calculate_optimal_sqs_retention()` - Calculate optimal message retention period
- [x] `should_use_sqs_fifo()` - Decide between FIFO and Standard queue
- [x] `calculate_sqs_dlq_threshold()` - Calculate DLQ max receive count threshold
- [x] `calculate_optimal_sqs_concurrent_receives()` - Optimize concurrent receive operations
- [x] `estimate_sqs_throughput_capacity()` - Estimate throughput capacity with/without batching

### Production Resilience Module ✅ (NEW)
- [x] `circuit_breaker.rs` - Circuit breaker pattern for AWS API resilience
- [x] `CircuitBreaker` - Protect against cascading AWS API failures
- [x] `CircuitState` - Closed, Open, HalfOpen states
- [x] `CircuitBreakerStats` - Success/failure/rejection statistics
- [x] Configurable failure threshold and timeout
- [x] Automatic recovery testing (half-open state)
- [x] 7 comprehensive unit tests

### Real-Time Cost Tracking Module ✅ (NEW)
- [x] `cost_tracker.rs` - Real-time AWS SQS cost tracking
- [x] `CostTracker` - Track all SQS operations
- [x] `CostSummary` - Detailed cost analysis with breakdown
- [x] `CostBreakdown` - Per-operation cost analysis
- [x] Free tier tracking (1M requests/month)
- [x] Batch vs individual cost comparison
- [x] Monthly cost projection
- [x] FIFO vs Standard queue pricing
- [x] 9 comprehensive unit tests

### Advanced Batch Optimizer Module ✅ (NEW)
- [x] `batch_optimizer.rs` - Dynamic batch size optimization
- [x] `BatchOptimizer` - Intelligent batch sizing
- [x] `OptimizerGoal` - MinimizeCost, LowLatency, Balanced, MaxThroughput
- [x] `BatchOptimizationResult` - Detailed recommendations with reasoning
- [x] Dynamic batch sizing based on queue metrics
- [x] Latency-aware optimization
- [x] Cost-aware optimization
- [x] Throughput-aware optimization (multi-worker)
- [x] Automatic strategy recommendation
- [x] 8 comprehensive unit tests

### Distributed Tracing Module ✅ (NEW)
- [x] `tracing_util.rs` - Distributed tracing utilities
- [x] `TraceContext` - Correlation IDs and trace context
- [x] `generate_correlation_id()` - Unique correlation ID generation
- [x] AWS X-Ray trace ID support
- [x] OpenTelemetry trace ID support
- [x] Parent-child span relationships
- [x] Message flow tracking across queues
- [x] `MessageFlowTracker` - Track messages through workflows
- [x] Trace context serialization for SQS attributes
- [x] 10 comprehensive unit tests

### Quota/Budget Management Module ✅ (NEW)
- [x] `quota_manager.rs` - API usage quota and budget control
- [x] `QuotaManager` - Real-time quota tracking and enforcement
- [x] `QuotaConfig` - Configurable quotas (per second/minute/hour/day)
- [x] Daily and monthly budget limits (USD)
- [x] Alert thresholds for approaching limits
- [x] Automatic quota reset at intervals
- [x] Sliding window rate limiting
- [x] Cost tracking integration
- [x] 11 comprehensive unit tests

### Multi-Queue Router Module ✅ (NEW)
- [x] `router.rs` - Message routing to multiple queues
- [x] `MessageRouter` - Rule-based message distribution
- [x] `RoutingRule` - Flexible routing rules
- [x] `RoutingStrategy` - Priority, TaskPattern, MetadataMatch, etc.
- [x] Priority queue simulation (using multiple SQS queues)
- [x] Content-based routing
- [x] Glob pattern matching for task names
- [x] Round-robin load balancing support
- [x] Weighted routing for A/B testing
- [x] `PriorityQueueRouter` - Easy 3-level priority routing
- [x] 12 comprehensive unit tests

### Performance Profiler Module ✅ (NEW)
- [x] `profiler.rs` - Performance profiling utilities
- [x] `PerformanceProfiler` - Latency tracking for all operations
- [x] `OperationStats` - Detailed statistics (P50, P95, P99)
- [x] Percentile calculations for latency analysis
- [x] Throughput measurement (ops/sec)
- [x] Bottleneck detection
- [x] Performance summary formatting
- [x] Support for all operation types (publish, consume, ack, batch, etc.)
- [x] Sliding window with configurable sample size (10,000 samples)
- [x] 11 comprehensive unit tests

### Optimization Module ✅
- [x] `optimization.rs` - High-level auto-tuning and optimization utilities
- [x] `WorkloadProfile` enum - Define workload characteristics (HighThroughput, LowLatency, CostOptimized, Balanced)
- [x] `OptimizedConfig` struct - Complete optimized SQS configuration
- [x] `optimize_for_workload()` - Auto-generate optimized config for workload profile
- [x] `auto_scale_recommendation()` - Get worker scaling recommendations based on queue metrics
- [x] `analyze_queue_health_with_recommendations()` - Queue health analysis with optimization suggestions
- [x] Production-ready profiles for common use cases (e-commerce, notifications, batch processing, microservices)
- [x] Automatic cost estimation and throughput capacity calculations
- [x] Smart compression and batching recommendations

### Message Deduplication Module ✅ (NEW)
- [x] `dedup.rs` - Message deduplication utilities
- [x] `DeduplicationCache` - In-memory cache with time-based expiration
- [x] `DeduplicationStrategy` - Multiple deduplication strategies (MessageId, ContentHash, CorrelationId, TaskSignature)
- [x] Content-based deduplication for standard queues
- [x] Time-window based deduplication (configurable)
- [x] Cache size management with LRU eviction
- [x] `generate_content_hash()` - Deterministic content hashing
- [x] `generate_task_signature()` - Task signature generation for deduplication
- [x] Duplicate detection statistics
- [x] 10 comprehensive unit tests

### Advanced DLQ Analytics Module ✅ (NEW)
- [x] `dlq_analytics.rs` - Sophisticated DLQ message analysis
- [x] `DlqAnalyzer` - Error pattern detection and classification
- [x] `ErrorPattern` - Classify errors (Transient, Permanent, Resource, Dependency)
- [x] `ErrorSeverity` - Severity levels (Low, Medium, High, Critical)
- [x] `RetryRecommendation` - Intelligent retry recommendations
- [x] Automatic error classification from error messages
- [x] Confidence-based retry strategies
- [x] DLQ statistics and aggregations
- [x] Top error patterns and failing tasks analysis
- [x] Retryable vs non-retryable message separation
- [x] 12 comprehensive unit tests

### Observability Hooks Module ✅ (NEW)
- [x] `hooks.rs` - Custom observability integration
- [x] `ObservabilityHooks` - Extensible event hooks system
- [x] `OperationEvent` - Rich event data with metadata
- [x] `EventType` - All operation types (Publish, Consume, Ack, Reject, etc.)
- [x] Event-specific hooks (on_publish, on_consume, on_ack, on_error)
- [x] `MetricsCollector` - Prometheus-style metrics collection
- [x] Thread-safe hook execution
- [x] Integration with custom monitoring systems
- [x] Support for Prometheus, Datadog, custom logging
- [x] 13 comprehensive unit tests

### Backpressure Management Module ✅ (NEW)
- [x] `backpressure.rs` - Automatic throttling and backpressure control
- [x] `BackpressureManager` - Prevent system overload
- [x] `BackpressureConfig` - Configurable thresholds
- [x] `BackpressureMetrics` - Real-time metrics tracking
- [x] `BackpressureState` - Normal, Throttling, AtCapacity states
- [x] Automatic throttling based on in-flight message count
- [x] Configurable max in-flight messages and processing time
- [x] Adaptive throttling with exponential backoff
- [x] Slow message detection
- [x] Processing time statistics (average, P95)
- [x] 10 comprehensive unit tests

### Poison Message Detection Module ✅ (NEW)
- [x] `poison_detector.rs` - Detect and isolate repeatedly failing messages
- [x] `PoisonDetector` - Track message failures over time
- [x] `PoisonConfig` - Configurable failure thresholds
- [x] `FailureInfo` - Detailed failure information
- [x] Automatic detection of repeatedly failing messages
- [x] Configurable failure window (time-based)
- [x] Automatic isolation to prevent resource consumption
- [x] Error pattern analysis and classification
- [x] Failure history tracking
- [x] 14 comprehensive unit tests

### Cost Alert System Module ✅ (NEW)
- [x] `cost_alerts.rs` - Real-time cost monitoring and alerting
- [x] `CostAlertSystem` - Automatic budget monitoring
- [x] `CostAlertConfig` - Configurable thresholds
- [x] `AlertLevel` - Warning and Critical levels
- [x] `AlertHistory` - Alert tracking and statistics
- [x] Configurable daily and monthly budgets
- [x] Alert callbacks for custom integrations
- [x] Alert deduplication to prevent spam
- [x] Budget compliance checking
- [x] 12 comprehensive unit tests

### Lambda Integration Helpers Module ✅ (NEW)
- [x] `lambda_helpers.rs` - AWS Lambda SQS event processing
- [x] `LambdaEventProcessor` - Process Lambda SQS events
- [x] `SqsEventRecord` - Lambda event record structure
- [x] `BatchProcessingResults` - Batch processing results
- [x] `BatchItemFailures` - Partial batch response support
- [x] Automatic retry logic for transient errors
- [x] Message attribute extraction helpers
- [x] FIFO queue detection
- [x] Queue name extraction from ARN
- [x] 10 comprehensive unit tests

### Message Replay Module ✅ (NEW)
- [x] `replay.rs` - DLQ message replay and disaster recovery
- [x] `ReplayManager` - Batch message replay with rate limiting
- [x] `ReplayConfig` - Configurable replay settings
- [x] `ReplayFilter` - Selective replay (time range, task pattern, error pattern)
- [x] `ReplayableMessage` - Message structure for replay
- [x] `ReplayResult` - Detailed replay results
- [x] `ReplayProgress` - Progress tracking and reporting
- [x] Rate limiting for controlled replay
- [x] Automatic retry with backoff
- [x] Message transformation during replay
- [x] Glob pattern matching for task names
- [x] 12 comprehensive unit tests

### SLA Monitor Module ✅ (NEW)
- [x] `sla_monitor.rs` - Service Level Agreement monitoring
- [x] `SlaMonitor` - Track SLA compliance in real-time
- [x] `SlaTarget` - Configurable SLA targets
- [x] `SlaReport` - Comprehensive compliance reporting
- [x] `SlaViolation` - Violation detection and details
- [x] Latency tracking (P50, P95, P99)
- [x] Throughput monitoring (messages/sec)
- [x] Error rate tracking
- [x] Compliance status (Compliant, Warning, Violation)
- [x] Production and critical presets
- [x] 10 comprehensive unit tests

### Unified Metrics Aggregator Module ✅ (NEWEST)
- [x] `metrics_aggregator.rs` - Unified metrics dashboard
- [x] `MetricsAggregator` - Combine metrics from all monitoring modules
- [x] `AggregatorConfig` - Configurable collection and retention
- [x] `MetricsSnapshot` - Complete system health snapshot
- [x] Overall health score calculation (0-100)
- [x] Alert generation based on combined metrics
- [x] Export to JSON and Prometheus formats
- [x] Historical metrics tracking with retention
- [x] Comprehensive summary reports
- [x] Configurable health thresholds
- [x] 11 comprehensive unit tests

### Workload-Specific Presets Module ✅ (NEWEST)
- [x] `workload_presets.rs` - Production-tested configuration presets
- [x] 10 workload-specific presets (e-commerce, IoT, ETL, etc.)
- [x] E-commerce transaction processing preset
- [x] Real-time notification system preset
- [x] Batch ETL pipeline preset
- [x] Event-driven microservices preset
- [x] IoT data ingestion preset
- [x] Log aggregation preset
- [x] Video processing pipeline preset
- [x] Financial trading system preset
- [x] Machine learning inference preset
- [x] API webhook delivery preset
- [x] Side-by-side preset comparison
- [x] Detailed reasoning and cost notes for each preset
- [x] 10 comprehensive unit tests

### Auto-Tuning System Module ✅ (NEWEST)
- [x] `auto_tuner.rs` - Self-adaptive configuration tuning
- [x] `AutoTuner` - ML-inspired auto-tuning system
- [x] `TuningGoal` - MinimizeCost, MaximizePerformance, MaximizeReliability, Balanced
- [x] `RuntimeMetrics` - Real-time workload metrics
- [x] `TuningRecommendations` - Adaptive configuration suggestions
- [x] Adaptive visibility timeout based on processing time
- [x] Dynamic batch size optimization
- [x] Intelligent polling strategy adjustment
- [x] Worker scaling recommendations
- [x] Cost-aware tuning algorithms
- [x] Performance-aware tuning algorithms
- [x] Historical metrics tracking for learning
- [x] Confidence score calculation
- [x] Configurable learning rate
- [x] 13 comprehensive unit tests

## Testing Status

- [x] Compilation tests
- [x] Unit tests (broker creation, builder pattern)
- [x] Configuration tests (FIFO, DLQ, SSE)
- [x] CloudWatch configuration tests (5 tests)
- [x] Adaptive polling tests (10 tests)
- [x] Clamping tests for all configuration values
- [x] Health check test
- [x] CloudWatch Alarms tests (6 tests)
- [x] Configuration presets tests (3 tests)
- [x] Message size validation tests (3 tests)
- [x] Monitoring module tests (9 tests)
- [x] Utilities module tests (11 tests)
- [x] Optimization module tests (8 tests)
- [x] Circuit breaker module tests (8 tests)
- [x] Cost tracker module tests (9 tests)
- [x] Batch optimizer module tests (10 tests)
- [x] Distributed tracing module tests (11 tests)
- [x] Quota manager module tests (12 tests)
- [x] Multi-queue router module tests (13 tests)
- [x] Performance profiler module tests (12 tests)
- [x] Message deduplication module tests (10 tests) ✨ NEW
- [x] Advanced DLQ analytics module tests (13 tests) ✨ NEW
- [x] Observability hooks module tests (14 tests) ✨ NEW
- [x] Backpressure management module tests (11 tests) ✨ NEW
- [x] Poison message detection module tests (15 tests) ✨ NEW
- [x] Cost alert system module tests (12 tests) ✨ NEW
- [x] Lambda integration helpers module tests (10 tests) ✨ NEW
- [x] Message replay module tests (12 tests) ✨ NEW
- [x] SLA monitor module tests (10 tests) ✨ NEW
- [x] Metrics aggregator module tests (11 tests) ✨ NEWEST
- [x] Workload presets module tests (10 tests) ✨ NEWEST
- [x] Auto-tuner module tests (13 tests) ✨ NEWEST
- [x] **285 total unit tests passing** (all production enhancements + 3 newest modules)
- [x] **57 doc tests** (39 passed, 18 ignored for AWS credentials)
- [x] **Zero warnings** (cargo test, cargo clippy --all-targets -- -D warnings)
- [x] Integration tests with LocalStack (12 tests)
- [x] Integration tests with real AWS SQS (same tests work with real credentials)
- [x] Performance benchmarks (8 scenarios)
- [x] Cost analysis

## Documentation

- [x] Module-level documentation (enhanced with IAM examples, cost optimization, batch examples)
- [x] API documentation
- [x] Feature documentation
- [x] AWS IAM policy examples (in lib.rs module docs)
- [x] Deployment guide (covers EC2/ECS/Lambda)
- [x] Cost optimization guide (90% cost reduction strategies)
- [x] Monitoring setup guide (CloudWatch + 6 alarms)
- [x] Testing guide (complete testing instructions)
- [x] Monitoring and utilities example (`examples/monitoring_utilities.rs` - comprehensive demo of all monitoring and utility functions)
- [x] Production optimization example (`examples/production_optimization.rs` - real-world optimization scenarios with auto-tuning)

## Dependencies

- `celers-protocol`: Message protocol
- `celers-kombu`: Broker traits
- `aws-config`: AWS configuration loading
- `aws-sdk-sqs`: SQS client library
- `aws-sdk-cloudwatch`: CloudWatch metrics publishing
- `serde_json`: Message serialization
- `tracing`: Logging
- `flate2`: Gzip compression support
- `base64`: Base64 encoding for compressed messages

## AWS IAM Requirements

Minimum IAM permissions needed:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "sqs:SendMessage",
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:ChangeMessageVisibility",
        "sqs:GetQueueUrl",
        "sqs:GetQueueAttributes",
        "sqs:ListQueues",
        "sqs:CreateQueue",
        "sqs:DeleteQueue",
        "sqs:PurgeQueue"
      ],
      "Resource": "arn:aws:sqs:*:*:*"
    },
    {
      "Effect": "Allow",
      "Action": [
        "cloudwatch:PutMetricData"
      ],
      "Resource": "*",
      "Condition": {
        "StringEquals": {
          "cloudwatch:namespace": "CeleRS/SQS"
        }
      }
    }
  ]
}
```

For production, restrict `Resource` to specific queue ARNs.

**Note**: CloudWatch permissions are only needed if you enable CloudWatch metrics via `with_cloudwatch()`.

## SQS Limits

- Message size: 256 KB maximum
- Message retention: 1 minute to 14 days (default 4 days)
- Visibility timeout: 0 seconds to 12 hours
- Long polling wait time: 0 to 20 seconds
- Batch size: Up to 10 messages
- Queue name: Alphanumeric, hyphens, underscores (max 80 chars)
- FIFO queue throughput: 300 TPS (can increase with batching)
- Standard queue throughput: Nearly unlimited

## Cost Considerations

### Standard Queues
- $0.40 per million requests (first 1 million free per month)
- Requests include: SendMessage, ReceiveMessage, DeleteMessage, etc.

### FIFO Queues
- $0.50 per million requests (first 1 million free per month)

### Cost Optimization Tips
1. Use long polling to reduce empty receives
2. Batch operations when possible (10x cheaper)
3. Adjust visibility timeout to avoid duplicate processing
4. Use IAM roles instead of access keys (free, more secure)
5. Consider message size (large messages cost the same as small)

## Notes

- SQS is a managed service - no infrastructure to manage
- Messages delivered at-least-once (standard queues)
- Messages delivered exactly-once (FIFO queues)
- Priority queues not natively supported (use message attributes)
- No broker topology (exchanges/bindings) like AMQP
- Ideal for cloud-native, serverless architectures
- Automatic scaling and redundancy

## Usage Examples

### CloudWatch Metrics Integration

```rust
use celers_broker_sqs::{SqsBroker, CloudWatchConfig};

// Enable CloudWatch metrics publishing
let cw_config = CloudWatchConfig::new("CeleRS/SQS")
    .with_dimension("Environment", "production")
    .with_dimension("Application", "my-app");

let mut broker = SqsBroker::new("my-queue")
    .await?
    .with_cloudwatch(cw_config);

broker.connect().await?;

// Manually publish metrics (or call periodically)
broker.publish_metrics("my-queue").await?;
```

The following metrics are published to CloudWatch:
- `ApproximateNumberOfMessages` - Messages available in the queue
- `ApproximateNumberOfMessagesNotVisible` - Messages being processed
- `ApproximateNumberOfMessagesDelayed` - Delayed messages
- `ApproximateAgeOfOldestMessage` - Age of oldest message in seconds (useful for backlog detection)

### Adaptive Polling Strategies

```rust
use celers_broker_sqs::{SqsBroker, AdaptivePollingConfig, PollingStrategy};

// Use exponential backoff when queue is empty
let adaptive_config = AdaptivePollingConfig::new(PollingStrategy::ExponentialBackoff)
    .with_min_wait_time(1)
    .with_max_wait_time(20)
    .with_backoff_multiplier(2.0);

let mut broker = SqsBroker::new("my-queue")
    .await?
    .with_adaptive_polling(adaptive_config);

broker.connect().await?;

// Polling will automatically adjust based on queue activity
// - When messages are available: decreases wait time (more responsive)
// - When queue is empty: increases wait time (saves costs)
loop {
    if let Some(envelope) = broker.consume("my-queue", Duration::from_secs(20)).await? {
        // Process message
        broker.ack(&envelope.delivery_tag).await?;
    }
}
```

**Polling Strategies:**
- `Fixed` - Uses configured wait time (default behavior)
- `ExponentialBackoff` - Increases wait time exponentially when queue is empty, resets when messages arrive
- `Adaptive` - Decreases wait time when busy, increases after 3+ consecutive empty receives

### Parallel Message Processing

```rust
use celers_broker_sqs::SqsBroker;
use std::time::Duration;

let mut broker = SqsBroker::new("my-queue").await?;
broker.connect().await?;

// Process up to 10 messages in parallel
let processed = broker.consume_parallel(
    "my-queue",
    10,
    Duration::from_secs(20),
    |envelope| async move {
        // Your async processing logic here
        println!("Processing: {:?}", envelope.message);

        // Simulate some async work
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Return Ok(()) to acknowledge, Err to requeue
        Ok(())
    }
).await?;

println!("Successfully processed {} messages", processed);
```

**Benefits:**
- Concurrent processing of multiple messages
- Automatic acknowledgment on success
- Automatic requeue on failure
- Improved throughput for I/O-bound tasks
- Uses tokio's task spawning for true parallelism

### Health Check for Monitoring

```rust
use celers_broker_sqs::SqsBroker;

let mut broker = SqsBroker::new("my-queue").await?;
broker.connect().await?;

// Check if queue is accessible (useful for K8s liveness/readiness probes)
match broker.health_check("my-queue").await {
    Ok(true) => println!("Queue is healthy and accessible"),
    Ok(false) => println!("Queue is not accessible"),
    Err(e) => println!("Health check failed: {}", e),
}
```

**Use Cases:**
- Kubernetes readiness and liveness probes
- Application startup validation
- Monitoring systems (Prometheus, Datadog, etc.)
- Circuit breaker patterns

### CloudWatch Alarms Setup

```rust
use celers_broker_sqs::{SqsBroker, AlarmConfig};

let mut broker = SqsBroker::new("my-queue").await?;
broker.connect().await?;

// Create alarm for high queue depth
let queue_depth_alarm = AlarmConfig::queue_depth_alarm(
    "HighQueueDepth",  // Alarm name
    "my-queue",        // Queue name
    1000.0             // Threshold (alert when > 1000 messages)
).with_alarm_action("arn:aws:sns:us-east-1:123456789:alerts");

broker.create_alarm(queue_depth_alarm).await?;

// Create alarm for old messages (backlog detection)
let message_age_alarm = AlarmConfig::message_age_alarm(
    "OldMessages",
    "my-queue",
    600.0  // Threshold (alert when oldest message > 10 minutes)
).with_alarm_action("arn:aws:sns:us-east-1:123456789:alerts");

broker.create_alarm(message_age_alarm).await?;

// List all alarms for the queue
let alarms = broker.list_alarms("my-queue").await?;
println!("Alarms: {:?}", alarms);

// Delete an alarm
broker.delete_alarm("HighQueueDepth").await?;
```

**Alarm Features:**
- Programmatic alarm creation and management
- Pre-configured helpers for common scenarios (queue depth, message age)
- SNS integration for notifications (email, SMS, Lambda, etc.)
- Customizable thresholds, evaluation periods, and statistics
- Support for all CloudWatch comparison operators

**Common Alarm Scenarios:**
1. **Queue Depth Alarm** - Alert when queue has too many messages
2. **Message Age Alarm** - Alert when messages are too old (backlog)
3. **Low Throughput Alarm** - Alert when queue is empty for too long
4. **High In-Flight Messages** - Alert when too many messages are being processed

## Comparison with Other Brokers

### vs RabbitMQ (AMQP)
- **Pros**: Fully managed, no infrastructure, unlimited scale, pay-per-use
- **Cons**: No advanced routing, higher latency, no priority queues

### vs Redis
- **Pros**: Durable by design, managed service, better for async patterns
- **Cons**: Higher latency, no sub-millisecond performance, costs

### vs PostgreSQL/MySQL
- **Pros**: Managed service, no database maintenance, better scalability
- **Cons**: Cannot query message history, no SQL-based analytics
