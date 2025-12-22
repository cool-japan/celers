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

- **`basic_usage.rs`** - Basic operations (publish, consume, batch, queue management)
- **`advanced_features.rs`** - FIFO, DLQ, SSE, CloudWatch, adaptive polling, parallel processing
- **`cost_optimization.rs`** - Cost reduction strategies and comparisons
- **`monitoring_alarms.rs`** - CloudWatch metrics and alarms setup

Run examples:
```bash
cargo run --example basic_usage
cargo run --example advanced_features
cargo run --example cost_optimization
cargo run --example monitoring_alarms
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
