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
- ✅ **Middleware System**: Message transformation pipeline
  - **Built-in**: Validation, Logging, Metrics, Retry Limit, Rate Limiting, Deduplication
  - **Feature-gated**: Compression (Gzip), Signing (HMAC), Encryption (AES-256-GCM)

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

## See Also

- **Protocol**: `celers-protocol` - Message format definitions
- **Brokers**: `celers-broker-redis`, `celers-broker-postgres`, etc.
- **Core**: `celers-core` - Task execution

## License

MIT OR Apache-2.0
