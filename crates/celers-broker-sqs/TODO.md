# celers-broker-sqs TODO

> AWS SQS broker implementation for CeleRS

## Status: ✅ FEATURE COMPLETE

Full AWS SQS broker implementation with long polling, visibility timeout management, FIFO queues, DLQ, SSE, and IAM authentication.

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
- [ ] Queue depth alerts (requires CloudWatch Alarms setup)
- [ ] Receive count statistics via CloudWatch

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
- [ ] Connection pooling (if needed)

## Testing Status

- [x] Compilation tests
- [x] Unit tests (broker creation, builder pattern)
- [x] Configuration tests (FIFO, DLQ, SSE)
- [x] CloudWatch configuration tests (5 tests)
- [x] Adaptive polling tests (10 tests)
- [x] Clamping tests for all configuration values
- [x] 35 total unit tests passing
- [ ] Integration tests with LocalStack
- [ ] Integration tests with real AWS SQS
- [ ] Performance benchmarks
- [ ] Cost analysis

## Documentation

- [x] Module-level documentation
- [x] API documentation
- [x] Feature documentation
- [ ] AWS IAM policy examples
- [ ] Deployment guide
- [ ] Cost optimization guide
- [ ] Monitoring setup guide

## Dependencies

- `celers-protocol`: Message protocol
- `celers-kombu`: Broker traits
- `aws-config`: AWS configuration loading
- `aws-sdk-sqs`: SQS client library
- `aws-sdk-cloudwatch`: CloudWatch metrics publishing
- `serde_json`: Message serialization
- `tracing`: Logging

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
