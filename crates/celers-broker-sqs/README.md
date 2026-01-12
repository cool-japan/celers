# celers-broker-sqs

Production-ready AWS SQS broker implementation for CeleRS with batch operations, FIFO queues, CloudWatch integration, and comprehensive cost optimization.

## Overview

Cloud-native message broker using AWS SQS with:

- ✅ **Batch Operations**: 10x cost reduction via batch API calls
- ✅ **FIFO Queues**: Guaranteed ordering with content-based deduplication
- ✅ **Dead Letter Queue**: Automatic failed message handling
- ✅ **CloudWatch Integration**: Metrics and alarms for monitoring
- ✅ **Adaptive Polling**: Smart polling strategies for cost optimization
- ✅ **Server-Side Encryption**: SSE-SQS and KMS encryption support
- ✅ **Parallel Processing**: Concurrent message processing with tokio
- ✅ **Cost Optimization**: 90%+ cost reduction with best practices
- ✅ **Circuit Breaker**: Resilience pattern for AWS API failures
- ✅ **Cost Tracking**: Real-time cost monitoring and budgets
- ✅ **Batch Optimizer**: Dynamic batch size optimization
- ✅ **Distributed Tracing**: Correlation IDs and trace context
- ✅ **Quota Management**: API usage limits and cost budgets
- ✅ **Multi-Queue Router**: Message routing and prioritization
- ✅ **Performance Profiler**: Latency tracking and bottleneck detection
- ✅ **Backpressure Management**: Automatic throttling to prevent overload ✨ NEW
- ✅ **Poison Message Detection**: Isolate repeatedly failing messages ✨ NEW
- ✅ **Cost Alert System**: Real-time budget monitoring with alerts ✨ NEW
- ✅ **Lambda Integration**: Helpers for AWS Lambda SQS event processing ✨ NEW
- ✅ **Message Replay**: DLQ replay with selective filtering and rate limiting ✨ NEW
- ✅ **SLA Monitoring**: Real-time SLA compliance tracking and reporting ✨ NEW

## Features

| Feature | Standard Queue | FIFO Queue |
|---------|---------------|------------|
| Throughput | Nearly Unlimited | 300-3000 TPS |
| Ordering | Best-effort | Strict FIFO |
| Deduplication | ❌ | ✅ (5 min window) |
| Latency | ~10-100ms | ~10-100ms |
| Cost (per 1M) | $0.40 | $0.50 |
| Use Case | High throughput | Ordered processing |

## Quick Start

```rust
use celers_broker_sqs::SqsBroker;
use celers_kombu::{Broker, Consumer, Producer, Transport};
use celers_protocol::builder::MessageBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create and connect
    let mut broker = SqsBroker::new("my-queue").await?;
    broker.connect().await?;

    // Publish a message
    let message = MessageBuilder::new("tasks.process_data")
        .kwarg("user_id", serde_json::json!(123))
        .build()?;
    broker.publish("my-queue", message).await?;

    // Consume messages
    if let Some(envelope) = broker
        .consume("my-queue", std::time::Duration::from_secs(20))
        .await?
    {
        println!("Received: {:?}", envelope.message.task_name());
        broker.ack(&envelope.delivery_tag).await?;
    }

    Ok(())
}
```

## Cost Optimization

### Without Optimization (10M messages/month)
- 10M SendMessage = $4.00
- 10M ReceiveMessage (5 empty receives per message) = $20.00
- 10M DeleteMessage = $4.00
- **Total: $28.00/month**

### With Optimization (10M messages/month)
- 1M SendMessageBatch = $0.40
- 1.2M ReceiveMessage (adaptive polling + long polling) = $0.48
- 1M DeleteMessageBatch = $0.40
- **Total: $1.28/month (95% savings!)**

### Best Practices

```rust
// Use cost-optimized preset for production
let mut broker = SqsBroker::cost_optimized("my-queue").await?;
broker.connect().await?;

// Or configure manually
let broker = SqsBroker::new("my-queue")
    .await?
    .with_wait_time(20)              // Enable long polling (20s max)
    .with_max_messages(10)            // Receive up to 10 messages at once
    .with_adaptive_polling(
        AdaptivePollingConfig::new(PollingStrategy::ExponentialBackoff)
    );
```

## Batch Operations

### Performance Comparison

| Operation | Individual | Batch (10) | Cost Reduction |
|-----------|-----------|------------|----------------|
| Publish | 10 API calls | 1 API call | **90%** |
| Consume | 10 API calls | 1 API call | **90%** |
| Acknowledge | 10 API calls | 1 API call | **90%** |

### Usage

```rust
// Batch publish (up to 10 messages per call)
let messages: Vec<_> = (1..=10)
    .map(|i| MessageBuilder::new("task").kwarg("id", serde_json::json!(i)).build())
    .collect::<Result<Vec<_>, _>>()?;
broker.publish_batch("my-queue", messages).await?;

// Batch consume (up to 10 messages per call)
let envelopes = broker
    .consume_batch("my-queue", 10, Duration::from_secs(20))
    .await?;

// Batch acknowledge
let tags: Vec<String> = envelopes.iter().map(|e| e.delivery_tag.clone()).collect();
broker.ack_batch("my-queue", tags).await?;

// Auto-chunking for large batches
let large_batch = create_many_messages(25);
broker.publish_batch_chunked("my-queue", large_batch).await?;  // Automatically chunks into 10+10+5
```

## FIFO Queues

```rust
use celers_broker_sqs::FifoConfig;

// Create FIFO queue with content-based deduplication
let fifo_config = FifoConfig::new()
    .with_content_based_deduplication(true)
    .with_high_throughput(true);  // 3000 TPS per message group

let mut broker = SqsBroker::new("my-queue.fifo")
    .await?
    .with_fifo(fifo_config);

broker.connect().await?;

// Publish to FIFO queue
let message = MessageBuilder::new("tasks.ordered_processing")
    .kwarg("sequence", serde_json::json!(1))
    .build()?;

broker
    .publish_fifo("my-queue.fifo", message, "group-1", None)
    .await?;
```

## Dead Letter Queue (DLQ)

```rust
// Get DLQ ARN
let dlq_arn = broker.get_queue_arn("my-dlq").await?;

// Configure DLQ for main queue (move to DLQ after 3 failed attempts)
broker.set_redrive_policy("my-queue", &dlq_arn, 3).await?;

// Monitor DLQ
let dlq_messages = broker.get_dlq_messages(10).await?;
println!("DLQ has {} messages", dlq_messages.len());

// Redrive message back to main queue
for envelope in dlq_messages {
    broker.redrive_dlq_message(&envelope.delivery_tag).await?;
}
```

## CloudWatch Integration

### Metrics

```rust
use celers_broker_sqs::CloudWatchConfig;

// Enable CloudWatch metrics
let cw_config = CloudWatchConfig::new("CeleRS/SQS")
    .with_dimension("Environment", "production")
    .with_dimension("Application", "my-app");

let mut broker = SqsBroker::new("my-queue")
    .await?
    .with_cloudwatch(cw_config);

broker.connect().await?;

// Publish metrics to CloudWatch
broker.publish_metrics("my-queue").await?;
```

Publishes these metrics:
- `ApproximateNumberOfMessages` - Messages in queue
- `ApproximateNumberOfMessagesNotVisible` - Messages being processed
- `ApproximateNumberOfMessagesDelayed` - Delayed messages
- `ApproximateAgeOfOldestMessage` - Age of oldest message (seconds)

### Alarms

```rust
use celers_broker_sqs::AlarmConfig;

// Create alarm for high queue depth
let alarm = AlarmConfig::queue_depth_alarm(
    "HighQueueDepth",
    "my-queue",
    1000.0  // Alert when > 1000 messages
).with_alarm_action("arn:aws:sns:us-east-1:123456789012:alerts");

broker.create_alarm(alarm).await?;

// Create alarm for old messages (backlog detection)
let alarm = AlarmConfig::message_age_alarm(
    "OldMessages",
    "my-queue",
    600.0  // Alert when oldest message > 10 minutes
).with_alarm_action("arn:aws:sns:us-east-1:123456789012:alerts");

broker.create_alarm(alarm).await?;

// List all alarms
let alarms = broker.list_alarms("my-queue").await?;
```

## Adaptive Polling

```rust
use celers_broker_sqs::{AdaptivePollingConfig, PollingStrategy};

// Exponential backoff: saves costs when queue is idle
let config = AdaptivePollingConfig::new(PollingStrategy::ExponentialBackoff)
    .with_min_wait_time(1)
    .with_max_wait_time(20)
    .with_backoff_multiplier(2.0);

let mut broker = SqsBroker::new("my-queue")
    .await?
    .with_adaptive_polling(config);

broker.connect().await?;
```

**Polling Strategies:**
- `Fixed` - Uses configured wait time (default)
- `ExponentialBackoff` - Increases wait time when queue is empty, resets when messages arrive
- `Adaptive` - Decreases wait time when busy, increases after 3+ consecutive empty receives

## Parallel Processing

```rust
use std::time::Duration;

// Process up to 10 messages concurrently
let processed = broker
    .consume_parallel(
        "my-queue",
        10,
        Duration::from_secs(20),
        |envelope| async move {
            // Your async processing logic
            process_message(&envelope.message).await?;
            Ok(())
        },
    )
    .await?;

println!("Processed {} messages in parallel", processed);
```

## Message Compression

```rust
// Enable compression for messages > 10 KB
let mut broker = SqsBroker::new("my-queue")
    .await?
    .with_compression(10 * 1024);

broker.connect().await?;

// Large messages are automatically compressed/decompressed
let large_payload = "x".repeat(50_000);  // 50 KB
let message = MessageBuilder::new("tasks.process_data")
    .kwarg("data", serde_json::json!(large_payload))
    .build()?;

broker.publish("my-queue", message).await?;  // Automatically compressed
```

## Configuration Presets

```rust
// Production: optimized for reliability and performance
let broker = SqsBroker::production("my-queue").await?;
// - Long polling: 20s
// - Max messages: 10 (batch receiving)
// - Message retention: 14 days
// - Visibility timeout: 30s

// Development: quick feedback, short retention
let broker = SqsBroker::development("my-queue").await?;
// - Wait time: 5s
// - Message retention: 1 hour
// - Visibility timeout: 30s

// Cost-optimized: 90%+ cost reduction
let broker = SqsBroker::cost_optimized("my-queue").await?;
// - Adaptive polling (exponential backoff)
// - Max messages: 10 (batch receiving)
// - Long polling: 20s
```

## Server-Side Encryption

```rust
use celers_broker_sqs::SseConfig;

// SQS-managed encryption (easiest)
let sse_config = SseConfig::sqs_managed();
let mut broker = SqsBroker::new("my-queue")
    .await?
    .with_sse(sse_config);

// KMS encryption (more control)
let kms_config = SseConfig::kms("alias/my-key")
    .with_data_key_reuse_period(300);  // 5 minutes
let mut broker = SqsBroker::new("my-queue")
    .await?
    .with_sse(kms_config);
```

## Queue Management

```rust
// Get queue size
let size = broker.queue_size("my-queue").await?;
println!("Queue has {} messages", size);

// Get detailed statistics
let stats = broker.get_queue_stats("my-queue").await?;
println!("Messages: {}", stats.approximate_message_count);
println!("In flight: {}", stats.approximate_not_visible_count);
if let Some(age) = stats.approximate_age_of_oldest_message {
    println!("Oldest message: {}s", age);
}

// List all queues
let queues = broker.list_queues().await?;

// Purge queue (delete all messages)
broker.purge("my-queue").await?;

// Health check (for Kubernetes probes)
let is_healthy = broker.health_check("my-queue").await?;
```

## AWS IAM Requirements

### Minimum Permissions

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "sqs:SendMessage",
        "sqs:SendMessageBatch",
        "sqs:ReceiveMessage",
        "sqs:DeleteMessage",
        "sqs:DeleteMessageBatch",
        "sqs:ChangeMessageVisibility",
        "sqs:GetQueueUrl",
        "sqs:GetQueueAttributes"
      ],
      "Resource": "arn:aws:sqs:*:*:my-queue*"
    }
  ]
}
```

### Additional Permissions

For queue management:
```json
{
  "Action": [
    "sqs:CreateQueue",
    "sqs:DeleteQueue",
    "sqs:PurgeQueue",
    "sqs:ListQueues",
    "sqs:SetQueueAttributes",
    "sqs:TagQueue",
    "sqs:UntagQueue"
  ]
}
```

For CloudWatch:
```json
{
  "Action": ["cloudwatch:PutMetricData"],
  "Resource": "*",
  "Condition": {
    "StringEquals": {"cloudwatch:namespace": "CeleRS/SQS"}
  }
}
```

For CloudWatch Alarms:
```json
{
  "Action": [
    "cloudwatch:PutMetricAlarm",
    "cloudwatch:DeleteAlarms",
    "cloudwatch:DescribeAlarms"
  ],
  "Resource": "arn:aws:cloudwatch:*:*:alarm:*"
}
```

## Authentication

The broker uses AWS SDK's credential chain:
1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
2. IAM role (recommended for EC2/ECS/Lambda)
3. AWS credentials file (`~/.aws/credentials`)
4. ECS container credentials
5. EC2 instance metadata

**Recommendation**: Use IAM roles in production for enhanced security.

## Production Features

### Circuit Breaker

Protect against cascading AWS API failures:

```rust
use celers_broker_sqs::circuit_breaker::CircuitBreaker;

let circuit_breaker = CircuitBreaker::new(5, Duration::from_secs(30));

// Execute operations with circuit breaker protection
let result = circuit_breaker.call(|| async {
    broker.publish("my-queue", message).await
}).await;

// Check circuit state
let stats = circuit_breaker.stats();
println!("Circuit state: {:?}", stats.state);
```

### Real-Time Cost Tracking

Monitor and control AWS SQS costs:

```rust
use celers_broker_sqs::cost_tracker::CostTracker;

let mut tracker = CostTracker::new();

// Track operations
tracker.track_publish(false, 1);  // Standard queue, 1 message
tracker.track_consume(10);         // 10 messages consumed

// Get cost summary
let summary = tracker.summary();
println!("Total cost: ${:.4}", summary.total_cost_usd);
println!("Monthly projection: ${:.2}", summary.monthly_projection_usd);

// Check batch savings
println!("Batch savings: ${:.4}", summary.batch_savings_usd);
```

### Batch Optimizer

Dynamically optimize batch sizes:

```rust
use celers_broker_sqs::batch_optimizer::{BatchOptimizer, OptimizerGoal};

let optimizer = BatchOptimizer::new(OptimizerGoal::MinimizeCost);

// Get recommendation based on queue metrics
let recommendation = optimizer.recommend(
    1000,    // Queue depth
    50,      // Avg message size (KB)
    100,     // Avg processing time (ms)
    4        // Number of workers
);

println!("Recommended batch size: {}", recommendation.recommended_batch_size);
println!("Reason: {}", recommendation.reasoning);
```

### Distributed Tracing

Track messages across services:

```rust
use celers_broker_sqs::tracing_util::{TraceContext, generate_correlation_id};

// Create trace context
let trace_ctx = TraceContext::new()
    .with_correlation_id(generate_correlation_id())
    .with_xray_trace_id()
    .with_parent_span_id("parent-123");

// Attach to message attributes
let attrs = trace_ctx.to_message_attributes();

// Track message flow
let mut flow_tracker = MessageFlowTracker::new();
flow_tracker.record_publish(&trace_ctx, "queue-1");
flow_tracker.record_consume(&trace_ctx, "queue-1");
```

### Quota Management

Control API usage and budgets:

```rust
use celers_broker_sqs::quota_manager::{QuotaManager, QuotaConfig};

let config = QuotaConfig::new()
    .with_daily_budget(10.0)      // $10/day max
    .with_monthly_budget(200.0)   // $200/month max
    .with_per_second_limit(100);  // 100 requests/sec max

let mut quota = QuotaManager::new(config);

// Check before operations
if quota.check_request(0.0004).await? {
    broker.publish("my-queue", message).await?;
} else {
    println!("Quota exceeded!");
}

// Get quota status
let status = quota.status();
println!("Daily spend: ${:.2}", status.daily_spend_usd);
```

### Multi-Queue Router

Route messages to multiple queues:

```rust
use celers_broker_sqs::router::{MessageRouter, RoutingRule, RoutingStrategy};

let mut router = MessageRouter::new();

// Priority-based routing
router.add_rule(RoutingRule::priority_based(
    "high-priority-queue",
    RoutingStrategy::PriorityThreshold(8)
));

// Pattern-based routing
router.add_rule(RoutingRule::task_pattern(
    "analytics-queue",
    "tasks.analytics.*"
));

// Route message
let queue = router.route(&message)?;
broker.publish(queue, message).await?;
```

### Performance Profiler

Track and analyze operation latencies:

```rust
use celers_broker_sqs::profiler::PerformanceProfiler;

let mut profiler = PerformanceProfiler::new();

// Record operations
let start = Instant::now();
broker.publish("my-queue", message).await?;
profiler.record_publish(start.elapsed());

// Get statistics
let stats = profiler.summary();
for (op, stat) in stats {
    println!("{}: P50={:.2}ms, P95={:.2}ms, P99={:.2}ms",
        op, stat.p50_ms, stat.p95_ms, stat.p99_ms);
}

// Detect bottlenecks
let bottlenecks = profiler.detect_bottlenecks(100); // 100ms threshold
if !bottlenecks.is_empty() {
    println!("Bottlenecks: {}", bottlenecks.join(", "));
}
```

### Backpressure Management

Prevent system overload with automatic throttling:

```rust
use celers_broker_sqs::backpressure::{BackpressureManager, BackpressureConfig};
use std::time::Duration;

let config = BackpressureConfig::new()
    .with_max_in_flight_messages(100)
    .with_max_processing_time(Duration::from_secs(30))
    .with_throttle_threshold(0.8)  // Start throttling at 80%
    .with_stop_threshold(0.95);     // Stop at 95%

let mut manager = BackpressureManager::new(config);

// Check before consuming
if manager.should_consume() {
    // Safe to process more messages
    manager.track_message_start("msg-1");
    // ... process message ...
    manager.track_message_complete("msg-1");
}

// Get metrics
let metrics = manager.metrics();
println!("Utilization: {:.1}%", metrics.utilization * 100.0);
```

### Poison Message Detection

Automatically detect and isolate repeatedly failing messages:

```rust
use celers_broker_sqs::poison_detector::{PoisonDetector, PoisonConfig};
use std::time::Duration;

let config = PoisonConfig::new()
    .with_max_failures(3)
    .with_failure_window(Duration::from_secs(300))  // 5 minutes
    .with_auto_isolate(true);

let mut detector = PoisonDetector::new(config);

// Track failures
if detector.track_failure("msg-123", "Database timeout") {
    println!("Poison message detected!");
}

// Get statistics
let stats = detector.statistics();
println!("Poison messages: {}", stats.poison_message_count);

// Analyze error patterns
for pattern in detector.analyze_error_patterns() {
    println!("{}: {} occurrences", pattern.pattern, pattern.count);
}
```

### Cost Alert System

Monitor costs with automatic alerts:

```rust
use celers_broker_sqs::cost_alerts::{CostAlertSystem, CostAlertConfig};
use std::sync::Arc;

let config = CostAlertConfig::new()
    .with_daily_warning_threshold(5.0)
    .with_daily_critical_threshold(10.0)
    .with_monthly_warning_threshold(100.0);

let mut alert_system = CostAlertSystem::new(config);

// Register callback
alert_system.register_callback(Arc::new(|alert| {
    println!("COST ALERT: {} - ${:.2}", alert.level, alert.amount_usd);
}));

// Track costs
alert_system.track_cost(0.0004);  // Per-request cost

// Check budget
if !alert_system.is_within_budget() {
    println!("Budget exceeded!");
}
```

### Message Replay

Replay messages from DLQ with selective filtering:

```rust
use celers_broker_sqs::replay::{ReplayManager, ReplayConfig, ReplayFilter};
use std::time::Duration;

// Configure replay
let config = ReplayConfig::new()
    .with_batch_size(10)
    .with_rate_limit(100)  // 100 messages/sec max
    .with_retry_failed(true);

let manager = ReplayManager::new(config);

// Create selective filter
let filter = ReplayFilter::new()
    .with_time_range_hours(1)
    .with_task_pattern("tasks.payment.*")
    .with_min_failure_count(3);

// Messages matching the filter can be replayed
```

### SLA Monitoring

Track SLA compliance in real-time:

```rust
use celers_broker_sqs::sla_monitor::{SlaMonitor, SlaTarget};
use std::time::Duration;

// Define SLA targets
let targets = SlaTarget::production(); // or SlaTarget::critical()

let mut monitor = SlaMonitor::new(targets);

// Record message processing
monitor.record_message(Duration::from_millis(150), true);

// Generate compliance report
let report = monitor.generate_report();
println!("SLA Compliance: {:.2}%", report.overall_compliance * 100.0);
println!("P99 Latency: {:?}", report.current_p99);

// Check for violations
for violation in report.violations {
    println!("Violation: {:?}", violation.violation_type);
}
```

### Lambda Integration Helpers

Process SQS events in AWS Lambda functions:

```rust
use celers_broker_sqs::lambda_helpers::LambdaEventProcessor;

let processor = LambdaEventProcessor::new()
    .with_max_retries(2);

// Process Lambda SQS event
let results = processor.process_event(event_json, |record| {
    // Your message processing logic
    println!("Processing: {}", record.message_id);
    Ok(())
})?;

// Return partial batch response for failed messages
if results.has_failures() {
    let response = results.to_batch_item_failures();
    return serde_json::to_string(&response);
}
```

## AWS SQS Limits

- **Message size**: 256 KB maximum
- **Message retention**: 1 minute to 14 days (default 4 days)
- **Visibility timeout**: 0 seconds to 12 hours
- **Long polling wait time**: 0 to 20 seconds
- **Batch size**: Up to 10 messages
- **Queue name**: Alphanumeric, hyphens, underscores (max 80 chars)
- **FIFO throughput**: 300-3000 TPS per message group
- **Standard throughput**: Nearly unlimited

## Examples

See the `examples/` directory for comprehensive examples:

### Core Features
- **`basic_usage.rs`** - Basic operations (publish, consume, batch, queue management)
- **`advanced_features.rs`** - FIFO, DLQ, SSE, CloudWatch, adaptive polling, parallel processing
- **`cost_optimization.rs`** - Cost reduction strategies and comparisons
- **`monitoring_alarms.rs`** - CloudWatch metrics and alarms setup

### Production Features
- **`monitoring_utilities.rs`** - Monitoring utilities (lag analysis, velocity, health assessment)
- **`production_optimization.rs`** - Auto-tuning and optimization for different workloads
- **`production_suite.rs`** - Complete production setup with all features
- **`quota_management.rs`** - API quota and budget management
- **`routing_patterns.rs`** - Multi-queue routing and message distribution
- **`distributed_tracing.rs`** - Distributed tracing with correlation IDs
- **`advanced_production_features.rs`** - Backpressure, poison detection, cost alerts integration ✨ NEW
- **`lambda_sqs_handler.rs`** - AWS Lambda SQS event processing example ✨ NEW
- **`replay_and_sla.rs`** - Message replay and SLA monitoring examples ✨ NEW

Run examples:
```bash
# Core features
cargo run --example basic_usage
cargo run --example advanced_features
cargo run --example cost_optimization
cargo run --example monitoring_alarms

# Production features
cargo run --example monitoring_utilities
cargo run --example production_optimization
cargo run --example production_suite
cargo run --example quota_management
cargo run --example routing_patterns
cargo run --example distributed_tracing
cargo run --example advanced_production_features  # NEW
cargo run --example lambda_sqs_handler  # NEW
cargo run --example replay_and_sla  # NEW
```

## Benchmarks

See `benches/sqs_benchmarks.rs` for performance benchmarks:

```bash
cargo bench --bench sqs_benchmarks
```

Benchmarks include:
- Individual vs batch operations
- Compression overhead
- Polling strategies
- Parallel vs sequential processing

## Testing

```bash
# Run unit tests
cargo test

# Run with AWS credentials for integration tests
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export AWS_REGION=us-east-1
cargo test --features integration

# Check for warnings
cargo clippy --all-targets -- -D warnings
```

## Comparison with Other Brokers

### vs RabbitMQ (AMQP)
- **Pros**: Fully managed, no infrastructure, unlimited scale, pay-per-use
- **Cons**: Higher latency (~10-100ms), no advanced routing, limited priority queues

### vs Redis
- **Pros**: Durable by design, managed service, better for async patterns, no data loss
- **Cons**: Higher latency, no sub-millisecond performance, costs

### vs PostgreSQL/MySQL
- **Pros**: Managed service, no database maintenance, better scalability
- **Cons**: Cannot query message history, no SQL-based analytics

## License

MIT OR Apache-2.0

## Contributing

Contributions are welcome! Please see the main CeleRS repository for contribution guidelines.
