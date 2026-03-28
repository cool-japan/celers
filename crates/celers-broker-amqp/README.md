# celers-broker-amqp

RabbitMQ/AMQP broker implementation for CeleRS, providing a full-featured message broker with exchange/queue topology management, publisher confirms, and advanced features like priority queues, dead letter exchanges, and transactions.

**Version: 0.2.0 | Status: [Stable] | Tests: 244 | Updated: 2026-03-27**

## Features

- **Full AMQP Protocol Support** - Complete implementation via RabbitMQ
- **Exchange/Queue Topology** - Direct, Fanout, Topic, and Headers exchanges
- **Publisher Confirms** - Reliable message delivery with automatic confirmation
- **Batch & Pipeline Publishing** - High-throughput message publishing
- **Consumer Streaming** - Async message consumption with `start_consumer()`
- **Priority Queues** - Message prioritization (0-9 priority levels)
- **Dead Letter Exchange (DLX)** - Automatic handling of failed messages
- **Message TTL** - Time-to-live for messages and queues
- **Transactions** - AMQP transaction support (commit/rollback)
- **Connection Recovery** - Automatic reconnection with configurable retry
- **Health Monitoring** - Connection health status and metrics tracking
- **Message Deduplication** - Prevent duplicate message processing
- **Connection & Channel Pooling** - Efficient resource management

### Production Features (v7)
- **Rate Limiting** - Token bucket and leaky bucket algorithms for controlling message rates
- **Bulkhead Pattern** - Resource isolation to prevent cascading failures
- **Message Scheduling** - Delayed message delivery for scheduled tasks and retries
- **Metrics Export** - Export metrics to Prometheus, StatsD, and JSON formats

### Enterprise Production Features (v9)
- **Backpressure Management** - Intelligent flow control to prevent overwhelming the broker or consumers
- **Poison Message Detection** - Identify and handle messages that repeatedly fail processing
- **Advanced Routing** - Sophisticated routing strategies beyond basic AMQP exchange types
- **Performance Optimization** - Advanced optimization strategies for connection tuning and resource management

### v0.2.0 Features
- **AMQP Event Transport** - AmqpEventEmitter/Receiver for fanout exchange event broadcasting
- **Topic Routing** - TopicRouter with AmqpRoutingConfig for pattern-based task routing
- **Unified Compression** - CompressionType aligned across protocol, broker-redis, broker-amqp

### Advanced Production Features (v8)
- **Lifecycle Hooks** - Extensible hooks for message interception and validation
- **DLX Analytics** - Comprehensive Dead Letter Exchange analytics and insights
- **Adaptive Batching** - Dynamic batch size optimization based on system load
- **Performance Profiling** - Detailed performance profiling with percentile analysis

### Enhanced Features (v6)
- **Circuit Breaker Pattern** - Prevent cascading failures with automatic recovery
- **Advanced Retry Strategies** - Exponential backoff with jitter (Full, Equal, Decorrelated)
- **Message Compression** - Reduce network overhead with Gzip/Zstd compression
- **Topology Validation** - Validate AMQP topology before deployment
- **Message Tracing** - Track message lifecycle and analyze flow patterns
- **Consumer Groups** - Load balancing with RoundRobin, LeastConnections, Priority, and Random strategies

## Installation

Add to your `Cargo.toml`:

```toml
[dependencies]
celers-broker-amqp = "0.2"
celers-protocol = "0.2"
celers-kombu = "0.2"
```

## Quick Start

```rust
use celers_broker_amqp::{AmqpBroker, AmqpConfig};
use celers_kombu::{Transport, Producer, Consumer};
use celers_protocol::builder::MessageBuilder;
use std::time::Duration;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create and connect to broker
    let mut broker = AmqpBroker::new("amqp://localhost:5672", "my_queue").await?;
    broker.connect().await?;

    // Publish a message
    let message = MessageBuilder::new("tasks.process")
        .args(vec![serde_json::json!({"data": "hello"})])
        .build()?;
    broker.publish("my_queue", message).await?;

    // Consume messages
    if let Ok(Some(envelope)) = broker.consume("my_queue", Duration::from_secs(5)).await {
        println!("Received: {:?}", envelope.message);
        broker.ack(&envelope.delivery_tag).await?;
    }

    broker.disconnect().await?;
    Ok(())
}
```

## Examples

The `examples/` directory contains 16 comprehensive examples demonstrating various features:

### Basic Examples

```bash
# Basic publish/consume workflow
cargo run --example basic_publish_consume

# High-throughput batch operations
cargo run --example batch_publish

# Priority-based message processing
cargo run --example priority_queue

# Dead Letter Exchange configuration
cargo run --example dead_letter_exchange

# AMQP transaction support
cargo run --example transaction

# Async streaming consumer pattern
cargo run --example streaming_consumer
```

### Advanced Examples

```bash
# RabbitMQ Management API usage
cargo run --example management_api

# Modern queue features (quorum, stream, lazy mode)
cargo run --example modern_queue_features

# Advanced monitoring & batch consumption (v4)
cargo run --example advanced_monitoring

# Monitoring & utility functions demo (v5)
cargo run --example monitoring_utilities

# v6 features: circuit breaker, retry, compression, topology, tracing, consumer groups
cargo run --example v6_features_demo

# Production patterns: complete integration of all v6 features
cargo run --example production_patterns

# v7 features: rate limiting, bulkhead, scheduling, metrics export
cargo run --example v7_features_demo

# v8 features: hooks, DLX analytics, adaptive batching, profiling
cargo run --example v8_features_demo

# v9 features: backpressure, poison detection, routing, optimization
cargo run --example v9_features_demo

# v9 production integration: complete production-ready integration of all v9 features (RECOMMENDED)
cargo run --example v9_production_integration
```

**Note**: Most examples require a running RabbitMQ instance. See the setup guide below.

## RabbitMQ Setup Guide

### Local Development with Docker

The easiest way to get started is using Docker:

```bash
# Start RabbitMQ with management plugin
docker run -d --name rabbitmq \
  -p 5672:5672 \
  -p 15672:15672 \
  rabbitmq:3-management

# Access management UI at http://localhost:15672
# Default credentials: guest/guest
```

### Production Installation

#### Ubuntu/Debian

```bash
# Add RabbitMQ repository
curl -s https://packagecloud.io/install/repositories/rabbitmq/rabbitmq-server/script.deb.sh | sudo bash

# Install RabbitMQ
sudo apt-get install rabbitmq-server

# Enable and start service
sudo systemctl enable rabbitmq-server
sudo systemctl start rabbitmq-server

# Enable management plugin
sudo rabbitmq-plugins enable rabbitmq_management
```

#### macOS

```bash
# Install via Homebrew
brew install rabbitmq

# Start service
brew services start rabbitmq
```

### Recommended RabbitMQ Configuration

Create `/etc/rabbitmq/rabbitmq.conf`:

```conf
# Memory threshold (60% of available memory)
vm_memory_high_watermark.relative = 0.6

# Disk free space threshold (50GB)
disk_free_limit.absolute = 50GB

# Heartbeat timeout
heartbeat = 60

# Maximum number of channels
channel_max = 2047

# Enable lazy queues for better performance with large queues
queue_master_locator = min-masters

# Log level
log.console.level = info
```

### Virtual Hosts

Create isolated environments for different applications:

```bash
# Create virtual host
sudo rabbitmqctl add_vhost production

# Create user
sudo rabbitmqctl add_user myapp secretpassword

# Set permissions
sudo rabbitmqctl set_permissions -p production myapp ".*" ".*" ".*"

# Use in your application
let broker = AmqpBroker::with_config(
    "amqp://myapp:secretpassword@localhost:5672",
    "my_queue",
    AmqpConfig::default().with_vhost("production")
).await?;
```

## Topology Design Patterns

### 1. Work Queue Pattern (Direct Exchange)

Best for distributing tasks among multiple workers with load balancing.

```rust
use celers_broker_amqp::{AmqpBroker, AmqpConfig, QueueConfig};
use celers_kombu::{Transport, Producer};

async fn setup_work_queue() -> Result<(), Box<dyn std::error::Error>> {
    let mut broker = AmqpBroker::new("amqp://localhost:5672", "tasks").await?;
    broker.connect().await?;

    // Configure queue with prefetch for fair dispatch
    let config = AmqpConfig::default()
        .with_prefetch(1)  // One message per worker at a time
        .with_exchange("tasks")
        .with_exchange_type(celers_broker_amqp::AmqpExchangeType::Direct);

    // Workers will automatically round-robin messages
    Ok(())
}
```

### 2. Pub/Sub Pattern (Fanout Exchange)

Broadcast messages to all consumers.

```rust
use celers_broker_amqp::{AmqpBroker, AmqpExchangeType};

async fn setup_pubsub() -> Result<(), Box<dyn std::error::Error>> {
    let mut broker = AmqpBroker::new("amqp://localhost:5672", "notifications").await?;
    broker.connect().await?;

    // Declare fanout exchange
    broker.declare_exchange("notifications", AmqpExchangeType::Fanout).await?;

    // Each consumer gets its own queue
    broker.declare_queue_with_config("email_notifications", &QueueConfig::new()).await?;
    broker.declare_queue_with_config("sms_notifications", &QueueConfig::new()).await?;

    // Bind queues to exchange
    broker.bind_queue("email_notifications", "notifications", "").await?;
    broker.bind_queue("sms_notifications", "notifications", "").await?;

    Ok(())
}
```

### 3. Routing Pattern (Topic Exchange)

Route messages based on routing key patterns.

```rust
use celers_broker_amqp::{AmqpBroker, AmqpExchangeType};

async fn setup_routing() -> Result<(), Box<dyn std::error::Error>> {
    let mut broker = AmqpBroker::new("amqp://localhost:5672", "logs").await?;
    broker.connect().await?;

    // Declare topic exchange
    broker.declare_exchange("logs", AmqpExchangeType::Topic).await?;

    // Bind with patterns
    broker.bind_queue("error_logs", "logs", "*.error").await?;
    broker.bind_queue("all_logs", "logs", "*").await?;
    broker.bind_queue("kernel_logs", "logs", "kernel.*").await?;

    Ok(())
}
```

### 4. Priority Queue Pattern

Process high-priority messages first.

```rust
use celers_broker_amqp::{AmqpBroker, QueueConfig};
use celers_protocol::builder::MessageBuilder;
use celers_kombu::Producer;

async fn setup_priority_queue() -> Result<(), Box<dyn std::error::Error>> {
    let mut broker = AmqpBroker::new("amqp://localhost:5672", "priority_tasks").await?;
    broker.connect().await?;

    // Declare priority queue (max priority: 10)
    let config = QueueConfig::new().with_max_priority(10);
    broker.declare_queue_with_config("priority_tasks", &config).await?;

    // Publish with priority
    let urgent_msg = MessageBuilder::new("urgent.task")
        .priority(9)  // High priority
        .build()?;

    let normal_msg = MessageBuilder::new("normal.task")
        .priority(5)  // Normal priority
        .build()?;

    broker.publish("priority_tasks", urgent_msg).await?;
    broker.publish("priority_tasks", normal_msg).await?;

    Ok(())
}
```

### 5. Dead Letter Exchange (DLX) Pattern

Handle failed messages automatically.

```rust
use celers_broker_amqp::{AmqpBroker, QueueConfig, DlxConfig};

async fn setup_dlx() -> Result<(), Box<dyn std::error::Error>> {
    let mut broker = AmqpBroker::new("amqp://localhost:5672", "main_queue").await?;
    broker.connect().await?;

    // Setup dead letter exchange and queue
    broker.declare_dlx("failed_exchange", "failed_queue").await?;

    // Configure main queue with DLX
    let dlx = DlxConfig::new("failed_exchange").with_routing_key("failed_queue");
    let config = QueueConfig::new()
        .with_dlx(dlx)
        .with_message_ttl(60000);  // 60 second TTL

    broker.declare_queue_with_config("main_queue", &config).await?;

    // Failed/expired messages will automatically go to failed_queue
    Ok(())
}
```

### 6. Delayed Task Pattern

Schedule tasks for future execution.

```rust
use celers_protocol::builder::MessageBuilder;
use celers_kombu::Producer;
use std::time::Duration;

async fn schedule_delayed_task(broker: &mut AmqpBroker) -> Result<(), Box<dyn std::error::Error>> {
    // Schedule task for 5 minutes from now
    let message = MessageBuilder::new("delayed.task")
        .countdown(300)  // 300 seconds
        .build()?;

    broker.publish("delayed_queue", message).await?;
    Ok(())
}
```

## Advanced Features

### Batch Publishing

Publish multiple messages efficiently:

```rust
use celers_protocol::builder::MessageBuilder;

async fn batch_publish(broker: &mut AmqpBroker) -> Result<(), Box<dyn std::error::Error>> {
    let messages: Vec<_> = (0..100)
        .map(|i| {
            MessageBuilder::new("batch.task")
                .args(vec![serde_json::json!(i)])
                .build()
                .unwrap()
        })
        .collect();

    // Publish all at once with confirms
    let count = broker.publish_batch("my_queue", messages).await?;
    println!("Published {} messages", count);

    Ok(())
}
```

### Pipeline Publishing

Control throughput with pipeline depth:

```rust
async fn pipeline_publish(broker: &mut AmqpBroker) -> Result<(), Box<dyn std::error::Error>> {
    let messages = vec![/* ... */];

    // Send 50 messages before waiting for confirms
    let count = broker.publish_pipeline("my_queue", messages, 50).await?;

    Ok(())
}
```

### Consumer Streaming

High-throughput async consumption:

```rust
use futures_lite::StreamExt;

async fn stream_consume(broker: &mut AmqpBroker) -> Result<(), Box<dyn std::error::Error>> {
    let mut consumer = broker.start_consumer("my_queue", "consumer-1").await?;

    while let Some(delivery) = consumer.next().await {
        let delivery = delivery?;

        // Process message
        println!("Received: {:?}", delivery.data);

        // Acknowledge
        delivery.ack(lapin::options::BasicAckOptions::default()).await?;
    }

    Ok(())
}
```

### Transactions

Ensure atomic operations:

```rust
async fn transactional_publish(broker: &mut AmqpBroker) -> Result<(), Box<dyn std::error::Error>> {
    // Start transaction
    broker.start_transaction().await?;

    // Publish multiple messages
    for i in 0..5 {
        let msg = MessageBuilder::new("task").args(vec![serde_json::json!(i)]).build()?;
        broker.publish("my_queue", msg).await?;
    }

    // Commit all or rollback
    if some_condition {
        broker.commit_transaction().await?;
    } else {
        broker.rollback_transaction().await?;
    }

    Ok(())
}
```

### Health Monitoring

Monitor broker connection health:

```rust
async fn monitor_health(broker: &AmqpBroker) {
    let status = broker.health_status();

    if !status.is_healthy() {
        println!("Broker unhealthy!");
        println!("Connected: {}", status.connected);
        println!("Channel open: {}", status.channel_open);
    }

    // Get metrics
    let metrics = broker.channel_metrics();
    println!("Published: {}", metrics.messages_published);
    println!("Consumed: {}", metrics.messages_consumed);
    println!("Errors: {}", metrics.publish_errors);

    // Publisher confirm stats
    let confirm_stats = broker.publisher_confirm_stats();
    println!("Avg latency: {}μs", confirm_stats.avg_confirm_latency_us);
}
```

### Connection Pooling

Improve concurrency with connection pooling:

```rust
let config = AmqpConfig::default()
    .with_connection_pool_size(10)  // 10 connections
    .with_channel_pool_size(100);   // 100 channels per connection

let broker = AmqpBroker::with_config(
    "amqp://localhost:5672",
    "my_queue",
    config
).await?;
```

### Message Deduplication

Prevent duplicate processing:

```rust
let config = AmqpConfig::default()
    .with_deduplication(true)
    .with_deduplication_config(10000, Duration::from_secs(3600));  // Cache 10k IDs for 1 hour

let mut broker = AmqpBroker::with_config(
    "amqp://localhost:5672",
    "my_queue",
    config
).await?;

// Duplicate messages with same ID will be automatically skipped
```

## Enhanced Features (v6)

### Circuit Breaker Pattern

Protect your system from cascading failures with automatic circuit breaking:

```rust
use celers_broker_amqp::circuit_breaker::{CircuitBreaker, CircuitBreakerConfig};
use std::time::Duration;

async fn with_circuit_breaker() -> Result<(), Box<dyn std::error::Error>> {
    let config = CircuitBreakerConfig {
        failure_threshold: 5,      // Open after 5 failures
        success_threshold: 2,      // Close after 2 successes
        timeout: Duration::from_secs(60),  // Try again after 60s
        half_open_max_calls: 3,    // Test with 3 calls in half-open state
    };

    let mut circuit = CircuitBreaker::new(config);

    // Execute operation with circuit breaker protection
    match circuit.call(|| async {
        // Your operation here
        Ok(())
    }).await {
        Ok(result) => println!("Success: {:?}", result),
        Err(e) => println!("Circuit open or operation failed: {:?}", e),
    }

    // Monitor circuit state
    let metrics = circuit.metrics();
    println!("State: {:?}", metrics.state);
    println!("Failures: {}", metrics.failure_count);

    Ok(())
}
```

### Advanced Retry Strategies

Implement sophisticated retry logic with exponential backoff and jitter:

```rust
use celers_broker_amqp::retry::{
    ExponentialBackoff, Jitter, RetryStrategy, RetryExecutor
};
use std::time::Duration;

async fn with_retry() -> Result<(), Box<dyn std::error::Error>> {
    // Create exponential backoff strategy
    let strategy = ExponentialBackoff::new(Duration::from_millis(100))
        .with_max_delay(Duration::from_secs(30))
        .with_max_retries(5)
        .with_jitter(Jitter::Full);  // Full jitter to prevent thundering herd

    // Execute with retry
    let executor = RetryExecutor::new(strategy);
    let result = executor.execute(|| async {
        // Your operation that might fail
        publish_message().await
    }).await?;

    Ok(())
}

async fn publish_message() -> Result<(), Box<dyn std::error::Error>> {
    // Simulated operation
    Ok(())
}
```

Available jitter strategies:
- `Jitter::None` - No randomization
- `Jitter::Full` - Randomize between 0 and calculated delay
- `Jitter::Equal` - Half deterministic, half random
- `Jitter::Decorrelated` - Based on previous delay

### Message Compression

Reduce network overhead with built-in compression:

```rust
use celers_broker_amqp::compression::{
    compress_message, decompress_message, CompressionCodec,
    should_compress_message, CompressionStats
};

fn compression_example() -> Result<(), Box<dyn std::error::Error>> {
    let data = b"Your message data here...".repeat(100);

    // Check if compression is beneficial
    if should_compress_message(&data, 1024) {
        // Compress with gzip
        let compressed = compress_message(&data, CompressionCodec::Gzip)?;
        println!("Original: {} bytes", data.len());
        println!("Compressed: {} bytes", compressed.len());

        // Or use zstd for better compression ratios
        let zstd_compressed = compress_message(&data, CompressionCodec::Zstd)?;
        println!("Zstd compressed: {} bytes", zstd_compressed.len());

        // Decompress
        let decompressed = decompress_message(&compressed, CompressionCodec::Gzip)?;
        assert_eq!(data, decompressed.as_slice());
    }

    Ok(())
}
```

### Topology Validation

Validate your AMQP topology before deployment:

```rust
use celers_broker_amqp::topology::{
    TopologyValidator, ExchangeDefinition, QueueDefinition, BindingDefinition,
    validate_queue_naming, calculate_topology_complexity, analyze_topology_issues
};
use celers_broker_amqp::AmqpExchangeType;

fn validate_topology() -> Result<(), Box<dyn std::error::Error>> {
    let mut validator = TopologyValidator::new();

    // Define exchanges
    let exchange = ExchangeDefinition {
        name: "tasks".to_string(),
        exchange_type: AmqpExchangeType::Topic,
        durable: true,
        auto_delete: false,
    };
    validator.add_exchange(exchange)?;

    // Define queues
    let queue = QueueDefinition {
        name: "tasks.high".to_string(),
        durable: true,
        auto_delete: false,
        arguments: Default::default(),
    };
    validator.add_queue(queue)?;

    // Define bindings
    let binding = BindingDefinition {
        source: "tasks".to_string(),
        destination: "tasks.high".to_string(),
        routing_key: "tasks.high.*".to_string(),
        arguments: Default::default(),
    };
    validator.add_binding(binding)?;

    // Validate topology
    let issues = validator.validate()?;
    if !issues.is_empty() {
        println!("Topology issues found:");
        for issue in issues {
            println!("  - {}", issue);
        }
    }

    // Analyze complexity
    let summary = validator.summary();
    let complexity = calculate_topology_complexity(
        summary.exchanges,
        summary.queues,
        summary.bindings
    );
    println!("Topology complexity score: {:.1}", complexity);

    Ok(())
}
```

### Message Tracing & Observability

Track message flow and analyze patterns:

```rust
use celers_broker_amqp::tracing_util::{
    TraceRecorder, MessageTrace, TraceEvent, MessageFlowAnalyzer
};
use uuid::Uuid;

async fn message_tracing() -> Result<(), Box<dyn std::error::Error>> {
    let mut recorder = TraceRecorder::new(10000);  // Track up to 10k messages

    // Record message lifecycle
    let msg_id = Uuid::new_v4().to_string();
    recorder.record_event(&msg_id, TraceEvent::Published);
    recorder.record_event(&msg_id, TraceEvent::Consumed);
    recorder.record_event(&msg_id, TraceEvent::Acknowledged);

    // Analyze message flow
    let analyzer = MessageFlowAnalyzer::new(recorder);
    let insights = analyzer.analyze();

    println!("Total messages: {}", insights.total_messages);
    println!("Success rate: {:.2}%", insights.success_rate * 100.0);
    println!("Rejection rate: {:.2}%", insights.rejection_rate * 100.0);
    println!("Avg processing time: {:.2}ms", insights.avg_processing_time_ms);
    println!("Health status: {:?}", insights.health_status);

    Ok(())
}
```

### Consumer Group Management

Coordinate multiple consumers with load balancing:

```rust
use celers_broker_amqp::consumer_groups::{
    ConsumerGroup, ConsumerInfo, LoadBalancingStrategy
};

fn consumer_groups_example() -> Result<(), Box<dyn std::error::Error>> {
    // Create consumer group with round-robin strategy
    let mut group = ConsumerGroup::new(
        "my-consumer-group".to_string(),
        LoadBalancingStrategy::RoundRobin
    );

    // Add consumers
    for i in 0..5 {
        let consumer = ConsumerInfo::new(
            format!("consumer-{}", i),
            "my_queue".to_string()
        );
        group.add_consumer(consumer);
    }

    // Select next consumer for message delivery
    if let Some(consumer_id) = group.next_consumer() {
        println!("Routing to consumer: {}", consumer_id);

        // Track message processing
        group.mark_processing_started(&consumer_id);
        // ... process message ...
        group.mark_processing_completed(&consumer_id, true);
    }

    // Get group statistics
    let stats = group.get_statistics();
    println!("Active consumers: {}", stats.active_consumers);
    println!("Total processed: {}", stats.total_messages_processed);
    println!("Avg utilization: {:.2}%", stats.avg_utilization * 100.0);

    Ok(())
}
```

Available load balancing strategies:
- `LoadBalancingStrategy::RoundRobin` - Distribute evenly across consumers
- `LoadBalancingStrategy::LeastConnections` - Route to least busy consumer
- `LoadBalancingStrategy::Priority` - Route to highest priority available consumer
- `LoadBalancingStrategy::Random` - Random consumer selection

## Troubleshooting Guide

### Connection Issues

#### Problem: "Failed to connect: Connection refused"

**Cause**: RabbitMQ is not running or not accessible.

**Solutions**:
```bash
# Check if RabbitMQ is running
sudo systemctl status rabbitmq-server

# Check if port is open
telnet localhost 5672

# Check RabbitMQ logs
sudo tail -f /var/log/rabbitmq/rabbit@hostname.log

# Restart RabbitMQ
sudo systemctl restart rabbitmq-server
```

#### Problem: "Authentication failed"

**Cause**: Invalid credentials or permissions.

**Solutions**:
```bash
# List users
sudo rabbitmqctl list_users

# Add user
sudo rabbitmqctl add_user myuser mypassword

# Set permissions
sudo rabbitmqctl set_permissions -p / myuser ".*" ".*" ".*"

# Set admin tag
sudo rabbitmqctl set_user_tags myuser administrator
```

#### Problem: "Connection lost during operation"

**Cause**: Network issues or RabbitMQ restart.

**Solution**: Enable auto-reconnection:
```rust
let config = AmqpConfig::default()
    .with_auto_reconnect(true)
    .with_auto_reconnect_config(5, Duration::from_secs(2));  // 5 retries, 2s delay

let broker = AmqpBroker::with_config(url, queue, config).await?;
```

### Performance Issues

#### Problem: Slow message consumption

**Causes & Solutions**:

1. **Low prefetch count**:
```rust
let config = AmqpConfig::default()
    .with_prefetch(50);  // Increase prefetch
```

2. **Message acknowledgment bottleneck**:
```rust
// Use manual ack in batches
let mut count = 0;
while let Ok(Some(envelope)) = broker.consume(queue, timeout).await {
    // Process message
    count += 1;

    // Ack every 10 messages
    if count % 10 == 0 {
        broker.ack(&envelope.delivery_tag).await?;
    }
}
```

3. **Single consumer limitation**:
```rust
// Use multiple consumers with streaming
for i in 0..num_workers {
    let mut consumer = broker.start_consumer(queue, &format!("worker-{}", i)).await?;
    tokio::spawn(async move {
        // Process messages
    });
}
```

#### Problem: High memory usage

**Causes & Solutions**:

1. **Large queue backlogs**:
```bash
# Check queue sizes
sudo rabbitmqctl list_queues name messages

# Purge if needed
sudo rabbitmqctl purge_queue queue_name
```

2. **Enable lazy queues** in RabbitMQ config:
```conf
queue_mode = lazy
```

3. **Set queue length limits**:
```rust
let config = QueueConfig::new()
    .with_max_length(10000)
    .with_max_length_bytes(1_000_000_000);  // 1GB
```

#### Problem: Publisher confirm timeouts

**Cause**: High load or slow disk I/O.

**Solutions**:
```rust
// Use pipeline publishing for better throughput
broker.publish_pipeline(queue, messages, 100).await?;

// Or batch publishing
broker.publish_batch(queue, messages).await?;
```

### Message Issues

#### Problem: Messages not being consumed

**Checks**:
```rust
// 1. Check queue size
let size = broker.queue_size(queue).await?;
println!("Queue has {} messages", size);

// 2. Check consumer count
// Use RabbitMQ management API or CLI:
// sudo rabbitmqctl list_queues name consumers

// 3. Verify queue binding
broker.bind_queue(queue, exchange, routing_key).await?;
```

#### Problem: Messages disappearing

**Possible causes**:

1. **Message TTL expired**:
```rust
// Increase or remove TTL
let config = QueueConfig::new()
    .with_message_ttl(600000);  // 10 minutes
```

2. **Queue length limit reached**:
```rust
// Increase limit or use DLX
let dlx = DlxConfig::new("overflow_exchange");
let config = QueueConfig::new()
    .with_max_length(50000)
    .with_dlx(dlx);
```

3. **Auto-delete queue**:
```rust
// Make queue persistent
let config = QueueConfig::new()
    .durable(true)
    .auto_delete(false);
```

#### Problem: Duplicate messages

**Solution**: Enable deduplication:
```rust
let config = AmqpConfig::default()
    .with_deduplication(true);

let broker = AmqpBroker::with_config(url, queue, config).await?;
```

### Error Messages

#### "Channel closed (406 PRECONDITION_FAILED)"

**Cause**: Queue configuration mismatch.

**Solution**: Delete and recreate queue:
```bash
sudo rabbitmqctl delete_queue queue_name
```

#### "Channel closed (405 RESOURCE_LOCKED)"

**Cause**: Queue is used exclusively by another connection.

**Solution**: Close the exclusive connection or wait for it to disconnect.

#### "Connection blocked (311)"

**Cause**: RabbitMQ is running out of resources (memory/disk).

**Solutions**:
```bash
# Check alarms
sudo rabbitmqctl eval 'rabbit_alarm:get_alarms().'

# Check memory
free -h

# Increase memory threshold in config
vm_memory_high_watermark.relative = 0.7
```

### Monitoring and Debugging

Enable detailed logging:

```rust
// Set RUST_LOG environment variable
// RUST_LOG=debug cargo run

use tracing_subscriber;

tracing_subscriber::fmt()
    .with_max_level(tracing::Level::DEBUG)
    .init();
```

Use RabbitMQ Management UI:
- Access at `http://localhost:15672`
- Monitor queues, connections, channels
- View message rates and statistics

## Testing

Run unit tests:
```bash
cargo test
```

Run integration tests (requires RabbitMQ):
```bash
# Start RabbitMQ
docker run -d --name rabbitmq -p 5672:5672 rabbitmq:3

# Run integration tests
cargo test --ignored
```

## Performance Benchmarks

Typical performance on modest hardware (4 CPU cores, 8GB RAM):

- **Publishing**: 10,000+ messages/sec (batch mode)
- **Consumption**: 8,000+ messages/sec (streaming mode)
- **Latency**: < 5ms average (with publisher confirms)
- **Memory**: ~50MB base + ~100 bytes per queued message

## Celery Compatibility

This implementation is 100% compatible with Python Celery:

- Uses same exchange ("celery") and routing patterns
- Compatible message format (JSON serialization)
- Supports priority queues (x-max-priority)
- Follows Celery's queue naming conventions

## Known Limitations

- `list_queues()` requires RabbitMQ Management API (not available via AMQP protocol)
- Connection and channel pools require explicit configuration
- Maximum message size limited by RabbitMQ (default: 128MB)

## Resources

- [RabbitMQ Documentation](https://www.rabbitmq.com/documentation.html)
- [AMQP 0-9-1 Specification](https://www.rabbitmq.com/resources/specs/amqp0-9-1.pdf)
- [Celery Documentation](https://docs.celeryproject.org/)
- [CeleRS GitHub](https://github.com/yourusername/celers)

## License

Apache-2.0
