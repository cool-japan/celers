# celers-kombu

Broker abstraction layer for CeleRS, inspired by Python's Kombu library. Provides unified traits for message broker implementations.

## Overview

Production-ready broker abstraction with:

- ✅ **Transport Abstraction**: Unified interface for Redis, AMQP, SQS, PostgreSQL
- ✅ **Producer/Consumer**: Separate publish/consume interfaces
- ✅ **Queue Management**: Create, delete, purge queues
- ✅ **FIFO & Priority Modes**: Flexible queue ordering
- ✅ **Acknowledgments**: Message delivery guarantees
- ✅ **Requeue Support**: Retry failed messages
- ✅ **Message Envelope**: Delivery metadata tracking
- ✅ **Error Handling**: Comprehensive error types
- ✅ **Dead Letter Queue (DLQ)**: Failed message handling with retry tracking
- ✅ **Message Transactions**: ACID guarantees with isolation levels
- ✅ **Message Scheduling**: Delayed delivery with absolute/relative timing
- ✅ **Consumer Groups**: Load-balanced distributed consumption
- ✅ **Message Replay**: Debugging and recovery with progress tracking
- ✅ **Quota Management**: Resource limits with enforcement policies
- ✅ **Flow Control**: Backpressure detection and poison message handling
- ✅ **Middleware System**: 21 middleware types for transformation, validation, security, and reliability
  - **Built-in** (18): Validation, Logging, Metrics, Retry Limit, Rate Limiting, Deduplication, Timeout, Filter, Sampling, Transformation, Tracing, Batching, Audit, Deadline, ContentType, RoutingKey, Idempotency, Backoff
  - **Feature-gated** (3): Compression (Gzip), Signing (HMAC), Encryption (AES-256-GCM)
- ✅ **Utilities Module**: 47 helper functions for optimization, monitoring, and operational excellence

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                        Broker Trait                      │
│               (Full producer + consumer)                 │
└─────────────────────────────────────────────────────────┘
                        │
        ┌───────────────┴───────────────┐
        │                               │
┌───────▼────────┐            ┌─────────▼────────┐
│    Producer    │            │     Consumer     │
│  (Publishing)  │            │   (Consuming)    │
└───────┬────────┘            └─────────┬────────┘
        │                               │
        └───────────────┬───────────────┘
                        │
                ┌───────▼────────┐
                │   Transport    │
                │  (Connection)  │
                └────────────────┘
                        │
        ┌───────────────┼───────────────┐
        │               │               │
┌───────▼──┐    ┌──────▼───┐    ┌──────▼───┐
│  Redis   │    │   AMQP   │    │   SQS    │
└──────────┘    └──────────┘    └──────────┘
```

## Quick Start

### Implementing a Broker

```rust
use celers_kombu::{Transport, Producer, Consumer, Broker, Message, Envelope, Result};
use async_trait::async_trait;

struct MyBroker {
    connected: bool,
}

#[async_trait]
impl Transport for MyBroker {
    async fn connect(&mut self) -> Result<()> {
        self.connected = true;
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        self.connected = false;
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.connected
    }

    fn name(&self) -> &str {
        "my-broker"
    }
}

#[async_trait]
impl Producer for MyBroker {
    async fn publish(&mut self, queue: &str, message: Message) -> Result<()> {
        // Implement message publishing
        Ok(())
    }

    async fn publish_with_routing(
        &mut self,
        exchange: &str,
        routing_key: &str,
        message: Message,
    ) -> Result<()> {
        // Implement routing
        Ok(())
    }
}

#[async_trait]
impl Consumer for MyBroker {
    async fn consume(&mut self, queue: &str, timeout: Duration) -> Result<Option<Envelope>> {
        // Implement message consumption
        Ok(None)
    }

    async fn ack(&mut self, delivery_tag: &str) -> Result<()> {
        // Implement acknowledgment
        Ok(())
    }

    async fn reject(&mut self, delivery_tag: &str, requeue: bool) -> Result<()> {
        // Implement rejection
        Ok(())
    }

    async fn queue_size(&mut self, queue: &str) -> Result<usize> {
        // Implement queue size check
        Ok(0)
    }
}

#[async_trait]
impl Broker for MyBroker {
    async fn purge(&mut self, queue: &str) -> Result<usize> {
        // Implement queue purge
        Ok(0)
    }

    async fn create_queue(&mut self, queue: &str, mode: QueueMode) -> Result<()> {
        // Implement queue creation
        Ok(())
    }

    async fn delete_queue(&mut self, queue: &str) -> Result<()> {
        // Implement queue deletion
        Ok(())
    }

    async fn list_queues(&mut self) -> Result<Vec<String>> {
        // Implement queue listing
        Ok(vec![])
    }
}
```

## Traits

### Transport

Low-level connection management:

```rust
#[async_trait]
pub trait Transport: Send + Sync {
    /// Connect to the broker
    async fn connect(&mut self) -> Result<()>;

    /// Disconnect from the broker
    async fn disconnect(&mut self) -> Result<()>;

    /// Check if connected
    fn is_connected(&self) -> bool;

    /// Get transport name ("redis", "amqp", "sqs")
    fn name(&self) -> &str;
}
```

**Usage:**
```rust
let mut transport = MyBroker::new();
transport.connect().await?;

assert!(transport.is_connected());
println!("Connected to: {}", transport.name());

transport.disconnect().await?;
```

### Producer

Message publishing interface:

```rust
#[async_trait]
pub trait Producer: Transport {
    /// Publish a message to a queue
    async fn publish(&mut self, queue: &str, message: Message) -> Result<()>;

    /// Publish a message with routing key (AMQP-style)
    async fn publish_with_routing(
        &mut self,
        exchange: &str,
        routing_key: &str,
        message: Message,
    ) -> Result<()>;
}
```

**Usage:**
```rust
use celers_protocol::Message;
use uuid::Uuid;

let message = Message::new("task".to_string(), Uuid::new_v4(), vec![]);

// Simple publish
producer.publish("celery", message.clone()).await?;

// Routing (AMQP-style)
producer.publish_with_routing("tasks", "high_priority", message).await?;
```

### Consumer

Message consumption interface:

```rust
#[async_trait]
pub trait Consumer: Transport {
    /// Consume a message from a queue (blocking with timeout)
    async fn consume(&mut self, queue: &str, timeout: Duration) -> Result<Option<Envelope>>;

    /// Acknowledge a message
    async fn ack(&mut self, delivery_tag: &str) -> Result<()>;

    /// Reject a message (requeue or send to DLQ)
    async fn reject(&mut self, delivery_tag: &str, requeue: bool) -> Result<()>;

    /// Get queue size
    async fn queue_size(&mut self, queue: &str) -> Result<usize>;
}
```

**Usage:**
```rust
use std::time::Duration;

// Consume with timeout
if let Some(envelope) = consumer.consume("celery", Duration::from_secs(5)).await? {
    println!("Received: {:?}", envelope.message);

    // Process message
    match process_message(&envelope.message) {
        Ok(_) => {
            // Acknowledge success
            consumer.ack(&envelope.delivery_tag).await?;
        }
        Err(_) => {
            // Reject and requeue
            consumer.reject(&envelope.delivery_tag, true).await?;
        }
    }
}

// Check queue size
let size = consumer.queue_size("celery").await?;
println!("Queue has {} messages", size);
```

### Broker

Full broker interface combining producer and consumer:

```rust
#[async_trait]
pub trait Broker: Producer + Consumer + Transport {
    /// Purge a queue (remove all messages)
    async fn purge(&mut self, queue: &str) -> Result<usize>;

    /// Create a queue
    async fn create_queue(&mut self, queue: &str, mode: QueueMode) -> Result<()>;

    /// Delete a queue
    async fn delete_queue(&mut self, queue: &str) -> Result<()>;

    /// List all queues
    async fn list_queues(&mut self) -> Result<Vec<String>>;
}
```

**Usage:**
```rust
// Create queues
broker.create_queue("celery", QueueMode::Fifo).await?;
broker.create_queue("priority", QueueMode::Priority).await?;

// List queues
let queues = broker.list_queues().await?;
println!("Queues: {:?}", queues);

// Purge queue
let removed = broker.purge("celery").await?;
println!("Removed {} messages", removed);

// Delete queue
broker.delete_queue("old_queue").await?;
```

## Types

### Message Envelope

```rust
pub struct Envelope {
    /// The actual message
    pub message: Message,

    /// Delivery tag (for acknowledgment)
    pub delivery_tag: String,

    /// Redelivery flag
    pub redelivered: bool,
}
```

**Usage:**
```rust
if let Some(envelope) = consumer.consume("celery", timeout).await? {
    // Access message
    let task_name = &envelope.message.headers.task;

    // Check if redelivered (retry)
    if envelope.redelivered {
        println!("This is a retry");
    }

    // Acknowledge using delivery tag
    consumer.ack(&envelope.delivery_tag).await?;
}
```

### Queue Mode

```rust
pub enum QueueMode {
    /// First-In-First-Out
    Fifo,
    /// Priority-based
    Priority,
}
```

**FIFO Mode:**
- Tasks processed in order received
- Simpler implementation
- Higher throughput

**Priority Mode:**
- Tasks with higher priority processed first
- Requires sorted structure (e.g., Redis ZSET)
- Slightly lower throughput

**Usage:**
```rust
// FIFO queue (default)
broker.create_queue("celery", QueueMode::Fifo).await?;

// Priority queue
broker.create_queue("priority", QueueMode::Priority).await?;
```

### Queue Configuration

```rust
pub struct QueueConfig {
    /// Queue name
    pub name: String,

    /// Queue mode (FIFO or Priority)
    pub mode: QueueMode,

    /// Durable (survive broker restart)
    pub durable: bool,

    /// Auto-delete (delete when no consumers)
    pub auto_delete: bool,

    /// Maximum message size
    pub max_message_size: Option<usize>,

    /// Message TTL (time-to-live)
    pub message_ttl: Option<Duration>,
}
```

**Builder pattern:**
```rust
use std::time::Duration;

let config = QueueConfig::new("celery".to_string())
    .with_mode(QueueMode::Priority)
    .with_ttl(Duration::from_secs(3600));

assert_eq!(config.name, "celery");
assert_eq!(config.mode, QueueMode::Priority);
assert!(config.durable);
```

## Error Types

```rust
pub enum BrokerError {
    /// Connection error
    Connection(String),

    /// Serialization error
    Serialization(String),

    /// Queue not found
    QueueNotFound(String),

    /// Message not found
    MessageNotFound(Uuid),

    /// Timeout waiting for message
    Timeout,

    /// Invalid configuration
    Configuration(String),

    /// Broker operation failed
    OperationFailed(String),
}
```

**Usage:**
```rust
use celers_kombu::BrokerError;

match consumer.consume("celery", timeout).await {
    Ok(Some(envelope)) => { /* process */ }
    Ok(None) => println!("No messages"),
    Err(BrokerError::Timeout) => println!("Timed out"),
    Err(BrokerError::QueueNotFound(q)) => eprintln!("Queue {} not found", q),
    Err(BrokerError::Connection(e)) => eprintln!("Connection error: {}", e),
    Err(e) => eprintln!("Error: {}", e),
}
```

## Broker Implementations

### Available Implementations

| Broker | Crate | Transport Type | Features |
|--------|-------|----------------|----------|
| Redis | `celers-broker-redis` | In-memory | Fast, simple, FIFO/Priority |
| PostgreSQL | `celers-broker-postgres` | Database | Durable, transactional |
| RabbitMQ | `celers-broker-amqp` | Message broker | Advanced routing, exchanges |
| AWS SQS | `celers-broker-sqs` | Cloud queue | Managed, scalable |
| SQL | `celers-broker-sql` | Database | Generic SQL support |

### Example: Using Redis Broker

```rust
use celers_broker_redis::RedisBroker;
use celers_kombu::{Broker, Producer, Consumer};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut broker = RedisBroker::new("redis://localhost:6379", "celery")?;
    broker.connect().await?;

    // Publish message
    let message = Message::new("task".to_string(), Uuid::new_v4(), vec![]);
    broker.publish("celery", message).await?;

    // Consume message
    if let Some(envelope) = broker.consume("celery", Duration::from_secs(5)).await? {
        println!("Received: {:?}", envelope.message);
        broker.ack(&envelope.delivery_tag).await?;
    }

    Ok(())
}
```

## Producer/Consumer Pattern

### Producer (Publisher)

```rust
use celers_kombu::Producer;

async fn enqueue_tasks(producer: &mut impl Producer) -> Result<()> {
    for i in 0..10 {
        let message = Message::new(
            "process_data".to_string(),
            Uuid::new_v4(),
            serde_json::to_vec(&i)?,
        );
        producer.publish("celery", message).await?;
    }
    Ok(())
}
```

### Consumer (Worker)

```rust
use celers_kombu::Consumer;
use std::time::Duration;

async fn worker_loop(consumer: &mut impl Consumer) -> Result<()> {
    loop {
        match consumer.consume("celery", Duration::from_secs(5)).await? {
            Some(envelope) => {
                // Process message
                println!("Processing: {:?}", envelope.message);

                // Acknowledge
                consumer.ack(&envelope.delivery_tag).await?;
            }
            None => {
                // No messages, continue polling
            }
        }
    }
}
```

## Message Acknowledgment Patterns

### Automatic Acknowledgment

```rust
if let Some(envelope) = consumer.consume("celery", timeout).await? {
    consumer.ack(&envelope.delivery_tag).await?;
    // Process after ack (at-most-once delivery)
    process_message(&envelope.message);
}
```

**Pros:** Lower latency, simpler
**Cons:** Message loss if processing fails

### Manual Acknowledgment (Recommended)

```rust
if let Some(envelope) = consumer.consume("celery", timeout).await? {
    // Process first
    match process_message(&envelope.message) {
        Ok(_) => {
            // Acknowledge only on success (at-least-once delivery)
            consumer.ack(&envelope.delivery_tag).await?;
        }
        Err(e) => {
            // Reject and requeue on failure
            consumer.reject(&envelope.delivery_tag, true).await?;
        }
    }
}
```

**Pros:** No message loss
**Cons:** Possible duplicates, higher latency

### Dead Letter Queue Pattern

```rust
const MAX_RETRIES: u32 = 3;

if let Some(envelope) = consumer.consume("celery", timeout).await? {
    let retry_count = envelope.message.headers.retries.unwrap_or(0);

    match process_message(&envelope.message) {
        Ok(_) => {
            consumer.ack(&envelope.delivery_tag).await?;
        }
        Err(_) if retry_count < MAX_RETRIES => {
            // Requeue for retry
            consumer.reject(&envelope.delivery_tag, true).await?;
        }
        Err(_) => {
            // Max retries reached, don't requeue (send to DLQ)
            consumer.reject(&envelope.delivery_tag, false).await?;
        }
    }
}
```

## Routing Patterns

### Direct Routing

```rust
// Simple queue name routing
producer.publish("celery", message).await?;
```

### Topic Routing (AMQP-style)

```rust
// Routing key pattern: "tasks.high_priority"
producer.publish_with_routing("tasks", "high_priority", message).await?;

// Routing key pattern: "logs.error"
producer.publish_with_routing("logs", "error", message).await?;
```

### Fanout (Broadcast)

```rust
// Publish to exchange, all queues receive
producer.publish_with_routing("broadcast", "*", message).await?;
```

## Middleware

The crate provides a powerful middleware system for message transformation, validation, security, and reliability.

### Middleware Chain

```rust
use celers_kombu::{MiddlewareChain, ValidationMiddleware, LoggingMiddleware};

let chain = MiddlewareChain::new()
    .with_middleware(Box::new(ValidationMiddleware::new()))
    .with_middleware(Box::new(LoggingMiddleware::new("MyApp")));

// Use with producer
producer.publish_with_middleware("celery", message, &chain).await?;

// Use with consumer
if let Some(envelope) = consumer.consume_with_middleware("celery", timeout, &chain).await? {
    // Process validated and logged message
}
```

### Built-in Middleware

#### ValidationMiddleware

Validates message structure and size limits:

```rust
use celers_kombu::ValidationMiddleware;

let validator = ValidationMiddleware::new()
    .with_max_body_size(10 * 1024 * 1024)  // 10MB limit
    .with_require_task_name(true);

let chain = MiddlewareChain::new()
    .with_middleware(Box::new(validator));
```

#### LoggingMiddleware

Logs message events for debugging:

```rust
use celers_kombu::LoggingMiddleware;

let logger = LoggingMiddleware::new("MyApp")
    .with_body_logging();  // Enable detailed logging

let chain = MiddlewareChain::new()
    .with_middleware(Box::new(logger));
```

#### MetricsMiddleware

Collects message statistics:

```rust
use celers_kombu::MetricsMiddleware;
use std::sync::{Arc, Mutex};

let metrics = Arc::new(Mutex::new(BrokerMetrics::new()));
let metrics_mw = MetricsMiddleware::new(metrics.clone());

let chain = MiddlewareChain::new()
    .with_middleware(Box::new(metrics_mw));

// Later, get metrics snapshot
let snapshot = metrics.lock().unwrap().clone();
println!("Published: {}, Consumed: {}",
    snapshot.messages_published,
    snapshot.messages_consumed);
```

#### RetryLimitMiddleware

Enforces maximum retry count:

```rust
use celers_kombu::RetryLimitMiddleware;

let retry_limiter = RetryLimitMiddleware::new(3);  // Max 3 retries

let chain = MiddlewareChain::new()
    .with_middleware(Box::new(retry_limiter));
```

#### RateLimitingMiddleware

Controls message publishing rate using token bucket algorithm:

```rust
use celers_kombu::RateLimitingMiddleware;

let rate_limiter = RateLimitingMiddleware::new(100.0);  // 100 messages/sec

let chain = MiddlewareChain::new()
    .with_middleware(Box::new(rate_limiter));

// Automatically enforces rate limit on publish
producer.publish_with_middleware("celery", message, &chain).await?;
```

#### DeduplicationMiddleware

Prevents duplicate message processing:

```rust
use celers_kombu::DeduplicationMiddleware;

let dedup = DeduplicationMiddleware::new(10_000);  // Track 10K message IDs
// Or use default: let dedup = DeduplicationMiddleware::with_default_cache();

let chain = MiddlewareChain::new()
    .with_middleware(Box::new(dedup));

// Rejects duplicate messages based on task_id
consumer.consume_with_middleware("celery", timeout, &chain).await?;
```

#### TimeoutMiddleware

Enforces message processing timeouts:

```rust
use celers_kombu::TimeoutMiddleware;
use std::time::Duration;

let timeout = TimeoutMiddleware::new(Duration::from_secs(30));

let chain = MiddlewareChain::new()
    .with_middleware(Box::new(timeout));

// Timeout metadata injected into message headers
```

#### FilterMiddleware

Selectively processes messages based on custom predicates:

```rust
use celers_kombu::FilterMiddleware;

let filter = FilterMiddleware::new(|msg| {
    msg.headers.task.starts_with("high_priority")
});

let chain = MiddlewareChain::new()
    .with_middleware(Box::new(filter));

// Only processes messages matching the predicate
```

#### SamplingMiddleware

Statistical message sampling for monitoring/testing:

```rust
use celers_kombu::SamplingMiddleware;

let sampler = SamplingMiddleware::new(0.1);  // Sample 10% of messages

let chain = MiddlewareChain::new()
    .with_middleware(Box::new(sampler));
```

#### TransformationMiddleware

Custom message content transformation:

```rust
use celers_kombu::TransformationMiddleware;

let transformer = TransformationMiddleware::new(|msg| {
    // Transform message body
    msg.body = transform_body(&msg.body);
    Ok(())
});

let chain = MiddlewareChain::new()
    .with_middleware(Box::new(transformer));
```

#### TracingMiddleware

Distributed tracing support with automatic trace ID propagation:

```rust
use celers_kombu::TracingMiddleware;

let tracer = TracingMiddleware::new("my-service");

let chain = MiddlewareChain::new()
    .with_middleware(Box::new(tracer));

// Injects trace IDs, span IDs, and timestamps for latency analysis
```

#### BatchingMiddleware

Automatic message batching hints for batch-aware consumers:

```rust
use celers_kombu::BatchingMiddleware;
use std::time::Duration;

let batcher = BatchingMiddleware::new(100, Duration::from_secs(5));

let chain = MiddlewareChain::new()
    .with_middleware(Box::new(batcher));

// Suggests batching metadata (size: 100, timeout: 5s)
```

#### AuditMiddleware

Comprehensive audit logging for compliance:

```rust
use celers_kombu::AuditMiddleware;

let auditor = AuditMiddleware::new("audit-system")
    .with_body_logging();  // Include message body in audit trail

let chain = MiddlewareChain::new()
    .with_middleware(Box::new(auditor));

// Generates unique audit IDs and tracks all operations
```

#### DeadlineMiddleware

Hard deadline enforcement (absolute time-based):

```rust
use celers_kombu::DeadlineMiddleware;
use std::time::Duration;

let deadline = DeadlineMiddleware::new(Duration::from_secs(300));  // 5 min deadline

let chain = MiddlewareChain::new()
    .with_middleware(Box::new(deadline));

// Rejects messages that exceed their absolute deadline
```

#### ContentTypeMiddleware

Content type validation and conversion:

```rust
use celers_kombu::ContentTypeMiddleware;

let validator = ContentTypeMiddleware::new()
    .with_allowed_types(vec!["application/json", "application/msgpack"])
    .with_default_type("application/json");

let chain = MiddlewareChain::new()
    .with_middleware(Box::new(validator));

// Validates and enforces content types
```

#### RoutingKeyMiddleware

Dynamic routing key assignment:

```rust
use celers_kombu::RoutingKeyMiddleware;

// From task name
let router = RoutingKeyMiddleware::from_task_name();

// From task and priority
let router = RoutingKeyMiddleware::from_task_and_priority();

// Custom routing logic
let router = RoutingKeyMiddleware::new(|msg| {
    format!("tasks.{}.{}", msg.headers.task, msg.headers.priority)
});

let chain = MiddlewareChain::new()
    .with_middleware(Box::new(router));
```

#### IdempotencyMiddleware (NEW - v0.4.7)

Exactly-once message processing guarantee:

```rust
use celers_kombu::IdempotencyMiddleware;

let idempotency = IdempotencyMiddleware::new(10_000);  // Track 10K message IDs
// Or use default: IdempotencyMiddleware::with_default_cache();

let chain = MiddlewareChain::new()
    .with_middleware(Box::new(idempotency));

// Tracks processed messages to prevent duplicate processing on retries
// Sets x-already-processed header to true for duplicates
```

#### BackoffMiddleware (NEW - v0.4.7)

Automatic retry backoff calculation with jitter:

```rust
use celers_kombu::BackoffMiddleware;
use std::time::Duration;

let backoff = BackoffMiddleware::new(
    Duration::from_secs(1),   // Initial delay
    Duration::from_secs(300), // Max delay (5 min)
    2.0,                      // Multiplier
);
// Or use defaults: BackoffMiddleware::with_defaults();

let chain = MiddlewareChain::new()
    .with_middleware(Box::new(backoff));

// Calculates exponential backoff with 0-25% jitter
// Injects x-backoff-delay and x-next-retry headers
```

### Feature-Gated Middleware

The following middleware require enabling feature flags in `Cargo.toml`:

#### CompressionMiddleware

Compresses message bodies (requires `compression` feature):

```toml
[dependencies]
celers-kombu = { version = "0.1", features = ["compression"] }
```

```rust
#[cfg(feature = "compression")]
use celers_kombu::CompressionMiddleware;
#[cfg(feature = "compression")]
use celers_protocol::compression::CompressionType;

#[cfg(feature = "compression")]
{
    let compressor = CompressionMiddleware::new(CompressionType::Gzip)
        .with_min_size(1024)    // Only compress messages > 1KB
        .with_level(6);         // Compression level 1-9

    let chain = MiddlewareChain::new()
        .with_middleware(Box::new(compressor));
}
```

#### SigningMiddleware

Signs messages with HMAC-SHA256 (requires `signing` feature):

```toml
[dependencies]
celers-kombu = { version = "0.1", features = ["signing"] }
```

```rust
#[cfg(feature = "signing")]
use celers_kombu::SigningMiddleware;

#[cfg(feature = "signing")]
{
    let secret_key = b"your-secret-key-min-32-bytes-long!!!";
    let signer = SigningMiddleware::new(secret_key);

    let chain = MiddlewareChain::new()
        .with_middleware(Box::new(signer));
}
```

#### EncryptionMiddleware

Encrypts messages with AES-256-GCM (requires `encryption` feature):

```toml
[dependencies]
celers-kombu = { version = "0.1", features = ["encryption"] }
```

```rust
#[cfg(feature = "encryption")]
use celers_kombu::EncryptionMiddleware;

#[cfg(feature = "encryption")]
{
    let encryption_key = b"32-byte-secret-key-for-aes-256!!";  // Must be 32 bytes
    let encryptor = EncryptionMiddleware::new(encryption_key)?;

    let chain = MiddlewareChain::new()
        .with_middleware(Box::new(encryptor));
}
```

### Enable All Features

```toml
[dependencies]
celers-kombu = { version = "0.1", features = ["full"] }
```

### Combining Middleware

Create a complete middleware pipeline:

```rust
use celers_kombu::*;
use std::sync::{Arc, Mutex};

let metrics = Arc::new(Mutex::new(BrokerMetrics::new()));

let chain = MiddlewareChain::new()
    // Validation first
    .with_middleware(Box::new(ValidationMiddleware::new()))
    // Rate limiting
    .with_middleware(Box::new(RateLimitingMiddleware::new(100.0)))
    // Deduplication
    .with_middleware(Box::new(DeduplicationMiddleware::with_default_cache()))
    // Logging
    .with_middleware(Box::new(LoggingMiddleware::new("MyApp")))
    // Metrics collection
    .with_middleware(Box::new(MetricsMiddleware::new(metrics.clone())))
    // Retry limit
    .with_middleware(Box::new(RetryLimitMiddleware::new(3)));

// Optional: Add compression, signing, encryption (if features enabled)
#[cfg(feature = "compression")]
let chain = chain.with_middleware(Box::new(
    CompressionMiddleware::new(CompressionType::Gzip)
));

#[cfg(feature = "signing")]
let chain = chain.with_middleware(Box::new(
    SigningMiddleware::new(b"secret-key")
));

#[cfg(feature = "encryption")]
let chain = chain.with_middleware(Box::new(
    EncryptionMiddleware::new(b"32-byte-encryption-key-here!!!!!")?
));
```

## Dead Letter Queue (DLQ)

Handle failed messages with automatic retry tracking:

```rust
use celers_kombu::{DlqConfig, DeadLetterQueue};

// Configure DLQ
let dlq_config = DlqConfig::new("my-queue-dlq")
    .with_max_retries(3)
    .with_ttl(Duration::from_secs(86400))  // 24 hours
    .with_metadata("reason", "processing_failed");

// Send to DLQ
broker.send_to_dlq(&message, &dlq_config).await?;

// Retrieve from DLQ
if let Some(msg) = broker.get_from_dlq("my-queue-dlq", None).await? {
    println!("Found failed message: {:?}", msg);
}

// Retry from DLQ
broker.retry_from_dlq("my-queue-dlq", &message_id, "my-queue").await?;

// Get DLQ statistics
let stats = broker.dlq_stats("my-queue-dlq").await?;
println!("DLQ has {} messages, oldest: {}s",
    stats.message_count,
    stats.oldest_message_age_secs().unwrap_or(0));

// Purge DLQ
broker.purge_dlq("my-queue-dlq").await?;
```

## Message Transactions

ACID guarantees for message operations:

```rust
use celers_kombu::{MessageTransaction, IsolationLevel};

// Begin transaction with isolation level
let tx_id = broker.begin_transaction(IsolationLevel::ReadCommitted).await?;

// Publish within transaction
broker.publish_transactional(&tx_id, "queue", message1).await?;
broker.publish_transactional(&tx_id, "queue", message2).await?;

// Consume within transaction
if let Some(env) = broker.consume_transactional(&tx_id, "queue", timeout).await? {
    // Process message
}

// Commit or rollback
if success {
    broker.commit_transaction(&tx_id).await?;
} else {
    broker.rollback_transaction(&tx_id).await?;
}

// Check transaction state
let state = broker.transaction_state(&tx_id).await?;
```

**Isolation Levels:**
- `ReadUncommitted`: Dirty reads allowed
- `ReadCommitted`: Only committed data visible
- `RepeatableRead`: Consistent snapshot
- `Serializable`: Full isolation

## Message Scheduling

Delay message delivery with flexible timing:

```rust
use celers_kombu::{ScheduleConfig, MessageScheduler};

// Schedule with delay
let schedule = ScheduleConfig::delay(Duration::from_secs(3600));  // 1 hour delay

// Schedule at absolute time
let schedule = ScheduleConfig::at(SystemTime::now() + Duration::from_secs(7200));

// Schedule with execution window
let schedule = ScheduleConfig::delay(Duration::from_secs(60))
    .with_window(Duration::from_secs(30));  // Allow ±30s variance

// Schedule message
let scheduled_id = broker.schedule_message("queue", message, &schedule).await?;

// Cancel scheduled message
broker.cancel_scheduled(&scheduled_id).await?;

// List scheduled messages
let scheduled = broker.list_scheduled("queue").await?;
for msg in scheduled {
    println!("Message {} scheduled for {:?}", msg.message_id, msg.scheduled_time);
}

// Check if ready for delivery
if schedule.is_ready() {
    println!("Message is ready for delivery");
}
```

## Consumer Groups

Load-balanced distributed consumption:

```rust
use celers_kombu::{ConsumerGroup, ConsumerGroupConfig};

// Configure consumer group
let config = ConsumerGroupConfig::new("my-group")
    .with_max_consumers(10)
    .with_heartbeat_interval(Duration::from_secs(30))
    .with_rebalance_timeout(Duration::from_secs(60));

// Join group
let consumer_id = broker.join_group(&config).await?;
println!("Joined group as: {}", consumer_id);

// Send heartbeat (keep membership alive)
broker.heartbeat(&consumer_id).await?;

// Consume with automatic load balancing
if let Some(envelope) = broker.consume_from_group("queue", &consumer_id, timeout).await? {
    println!("Received: {:?}", envelope);
}

// Get group members
let members = broker.group_members("my-group").await?;
println!("Group has {} consumers", members.len());

// Leave group
broker.leave_group(&consumer_id).await?;
```

## Message Replay

Debug and recover with historical message replay:

```rust
use celers_kombu::{ReplayConfig, MessageReplay};

// Replay last hour
let config = ReplayConfig::from_duration(Duration::from_secs(3600));

// Replay from specific timestamp
let config = ReplayConfig::from_timestamp(SystemTime::now() - Duration::from_secs(7200));

// Replay with limits
let config = ReplayConfig::from_duration(Duration::from_secs(3600))
    .with_max_messages(1000)
    .with_speed(2.0);  // 2x speed

// Begin replay session
let session_id = broker.begin_replay("queue", &config).await?;

// Replay messages
loop {
    match broker.replay_next(&session_id).await? {
        Some(envelope) => {
            println!("Replaying: {:?}", envelope);
        }
        None => break,
    }
}

// Track progress
let progress = broker.replay_progress(&session_id).await?;
println!("Replay {}% complete", progress.completion_percent());

// Stop replay
broker.stop_replay(&session_id).await?;
```

## Quota Management

Resource limits with flexible enforcement:

```rust
use celers_kombu::{QuotaConfig, QuotaManager, QuotaEnforcement};

// Configure quotas
let quota = QuotaConfig::new()
    .with_max_messages(10_000)
    .with_max_bytes(100 * 1024 * 1024)  // 100 MB
    .with_max_rate(100.0)  // 100 messages/sec
    .with_max_per_consumer(50)
    .with_enforcement(QuotaEnforcement::Throttle);

// Set quota
broker.set_quota("queue", quota).await?;

// Check quota before operation
match broker.check_quota("queue", message_size).await? {
    Ok(_) => {
        broker.publish("queue", message).await?;
    }
    Err(e) => println!("Quota exceeded: {}", e),
}

// Get quota usage
let usage = broker.quota_usage("queue").await?;
println!("Message quota: {}%", usage.message_usage_percent());
println!("Byte quota: {}%", usage.byte_usage_percent());
println!("Rate quota: {}%", usage.rate_usage_percent());

if usage.is_message_quota_exceeded() {
    println!("WARNING: Message quota exceeded!");
}

// Reset quota
broker.reset_quota("queue").await?;
```

**Enforcement Policies:**
- `Reject`: Reject operations exceeding quota
- `Throttle`: Slow down operations
- `Warn`: Log warnings but allow

## Flow Control

### Backpressure Detection

Automatic flow control based on queue metrics:

```rust
use celers_kombu::BackpressureConfig;

let backpressure = BackpressureConfig::new()
    .with_max_pending(1000)
    .with_max_queue_size(10_000)
    .with_high_watermark(0.8)  // 80%
    .with_low_watermark(0.2);  // 20%

// Check if backpressure should be applied
if backpressure.should_apply_backpressure(pending, queue_size) {
    println!("Applying backpressure - queue is full");
    tokio::time::sleep(Duration::from_millis(100)).await;
}

// Check if backpressure can be released
if backpressure.should_release_backpressure(pending, queue_size) {
    println!("Releasing backpressure - queue drained");
}
```

### Poison Message Detection

Prevent infinite retry loops:

```rust
use celers_kombu::PoisonMessageDetector;

let detector = PoisonMessageDetector::new()
    .with_max_failures(5)
    .with_failure_window(Duration::from_secs(300));  // 5 min window

// Record failure
detector.record_failure(&message_id);

// Check if poison
if detector.is_poison(&message_id) {
    println!("Poison message detected! Sending to DLQ...");
    broker.send_to_dlq(&message, &dlq_config).await?;
    detector.clear_failures(&message_id);
} else {
    let failures = detector.failure_count(&message_id);
    println!("Message has {} failures", failures);
}

// Clear all tracking
detector.clear_all();
```

## Utilities Module

47 helper functions for broker operations, optimization, and monitoring:

### Batch Optimization

```rust
use celers_kombu::utils;

// Calculate optimal batch size
let batch_size = utils::calculate_optimal_batch_size(
    1024,      // avg_message_size
    10_000,    // target_throughput
    100,       // processing_time_ms
);

// Calculate optimal workers
let workers = utils::calculate_optimal_workers(
    10_000,    // queue_size
    100,       // processing_rate per worker
    3600,      // target_drain_time_secs
);
```

### Performance Analysis

```rust
// Analyze broker performance
let (success_rate, error_rate, ack_rate) = utils::analyze_broker_performance(&metrics);
println!("Success: {:.1}%, Errors: {:.1}%", success_rate, error_rate);

// Calculate throughput
let throughput = utils::calculate_throughput(messages_count, duration_secs);
println!("Throughput: {:.1} msgs/sec", throughput);

// Analyze circuit breaker
let action = utils::analyze_circuit_breaker(&stats, &config);
println!("Recommended action: {:?}", action);
```

### Queue Health Monitoring

```rust
// Analyze queue health
let health = utils::analyze_queue_health(queue_size, high_watermark, low_watermark);
println!("Queue health: {:?}", health);  // Healthy, Warning, Critical

// Estimate drain time
let drain_time = utils::estimate_drain_time(queue_size, consumption_rate);
println!("Queue will drain in {:.1} seconds", drain_time);

// Estimate memory usage
let memory_mb = utils::estimate_queue_memory(queue_size, avg_message_size);
println!("Queue using ~{:.1} MB", memory_mb);
```

### Operational Excellence (NEW - v0.4.7)

```rust
// Anomaly detection
let (is_anomaly, severity, description) = utils::detect_anomalies(
    &current_rates,
    &baseline_rates,
    2.0,  // threshold_multiplier
);

// SLA compliance
let (compliance_pct, violations, avg_time) = utils::calculate_sla_compliance(
    &processing_times,
    target_ms,
);

// Error budget tracking
let (budget_remaining, errors_allowed, hours_to_exhaustion) = utils::calculate_error_budget(
    99.9,              // sla_target
    total_requests,
    failed_requests,
    requests_per_hour,
);

// Cost estimation
let monthly_cost = utils::estimate_infrastructure_cost(
    messages_per_day,
    cost_per_million,
    30,
);

// Queue saturation prediction
let (hours_to_saturation, growth_rate) = utils::predict_queue_saturation(
    &queue_sizes,
    max_capacity,
    hours_per_sample,
);
```

See examples for comprehensive usage: `cargo run --example monitoring` and `cargo run --example operational_excellence`.

## Best Practices

### 1. Always Handle Timeouts

```rust
use std::time::Duration;

let timeout = Duration::from_secs(5);
match consumer.consume("celery", timeout).await {
    Ok(Some(envelope)) => { /* process */ }
    Ok(None) => println!("No messages, continuing..."),
    Err(BrokerError::Timeout) => println!("Timed out, retrying..."),
    Err(e) => return Err(e),
}
```

### 2. Implement Graceful Shutdown

```rust
use tokio::signal;

async fn worker_with_shutdown(consumer: &mut impl Consumer) -> Result<()> {
    loop {
        tokio::select! {
            result = consumer.consume("celery", Duration::from_secs(5)) => {
                if let Some(envelope) = result? {
                    process_message(&envelope.message)?;
                    consumer.ack(&envelope.delivery_tag).await?;
                }
            }
            _ = signal::ctrl_c() => {
                println!("Shutting down gracefully...");
                break;
            }
        }
    }
    Ok(())
}
```

### 3. Use Connection Pooling

```rust
// Create connection pool
let pool = create_broker_pool(config)?;

// Acquire from pool
let mut broker = pool.acquire().await?;
broker.publish("celery", message).await?;

// Return to pool (automatic)
drop(broker);
```

### 4. Monitor Queue Sizes

```rust
async fn monitor_queues(consumer: &mut impl Consumer) -> Result<()> {
    let size = consumer.queue_size("celery").await?;

    if size > 1000 {
        println!("WARNING: Queue backlog detected ({} messages)", size);
        // Trigger alert or scale workers
    }

    Ok(())
}
```

### 5. Separate Queues by Priority

```rust
// High-priority queue
broker.create_queue("high_priority", QueueMode::Fifo).await?;

// Normal queue
broker.create_queue("celery", QueueMode::Fifo).await?;

// Low-priority queue
broker.create_queue("low_priority", QueueMode::Fifo).await?;

// Publish to appropriate queue
if is_urgent {
    producer.publish("high_priority", message).await?;
} else {
    producer.publish("celery", message).await?;
}
```

## Testing

```rust
#[cfg(test)]
mod tests {
    use super::*;

    struct MockBroker {
        connected: bool,
        messages: Vec<Message>,
    }

    #[async_trait]
    impl Transport for MockBroker {
        async fn connect(&mut self) -> Result<()> {
            self.connected = true;
            Ok(())
        }

        async fn disconnect(&mut self) -> Result<()> {
            self.connected = false;
            Ok(())
        }

        fn is_connected(&self) -> bool {
            self.connected
        }

        fn name(&self) -> &str {
            "mock"
        }
    }

    #[tokio::test]
    async fn test_connection() {
        let mut broker = MockBroker {
            connected: false,
            messages: vec![],
        };

        assert!(!broker.is_connected());
        broker.connect().await.unwrap();
        assert!(broker.is_connected());
    }
}
```

## Examples

The crate includes 11 comprehensive examples demonstrating all features:

### Basic Usage
```bash
# Complete broker implementation
cargo run --example basic_broker

# Middleware usage patterns
cargo run --example middleware_usage

# Batch operations
cargo run --example batch_operations
```

### Advanced Features
```bash
# Dead Letter Queue (DLQ)
cargo run --example dlq_usage

# Message transactions with ACID guarantees
cargo run --example transactions

# Scheduling, consumer groups, replay, quotas
cargo run --example advanced_features
```

### Flow Control & Resilience
```bash
# Backpressure, poison detection, timeout, filter
cargo run --example flow_control

# Circuit breaker, connection pooling, health checks
cargo run --example circuit_breaker
```

### Monitoring & Operational Excellence
```bash
# 47 utility functions showcase
cargo run --example utilities_showcase

# Production monitoring and observability
cargo run --example monitoring

# Idempotency, backoff, anomaly detection, SLA tracking, error budgets
cargo run --example operational_excellence
```

Each example includes detailed comments and demonstrates best practices for production use.

## See Also

- **Protocol**: `celers-protocol` - Message format definitions
- **Brokers**: `celers-broker-redis`, `celers-broker-postgres`, etc.
- **Core**: `celers-core` - Task execution

## License

MIT OR Apache-2.0
