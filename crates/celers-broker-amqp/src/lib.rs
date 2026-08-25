//! RabbitMQ/AMQP broker implementation for CeleRS
//!
//! This crate provides AMQP protocol support via RabbitMQ,
//! compatible with Python Celery's AMQP backend.
//!
//! # Features
//!
//! - Exchange/Queue/Binding topology management
//! - Publisher Confirm for reliability (`confirm.select` on every channel; a
//!   broker `nack` or an unroutable return is reported as a publish failure)
//! - **Batch Publishing** for improved throughput (publish multiple messages before waiting for confirms)
//! - Consumer QoS (Prefetch) control
//! - **Consumer Streaming** via `start_consumer()` for high-throughput message processing
//! - Dead Letter Exchange (DLX) support
//! - Message TTL support
//! - Exchange types (Direct, Fanout, Topic, Headers)
//! - Connection recovery and retry
//! - Virtual host support
//! - **Health Monitoring** with `health_status()` and `is_healthy()`
//! - **Transaction Support** with `start_transaction()`, `commit_transaction()`, `rollback_transaction()`
//! - **Management API Integration** for queue listing, statistics, and server monitoring
//! - **Monitoring Utilities** for autoscaling, capacity planning, and SLA monitoring (see [`monitoring`] module)
//! - **Utility Functions** for performance tuning and optimization (see [`utilities`] module)
//!
//! # Enhanced Features (v6)
//!
//! - **Circuit Breaker Pattern** - Prevent cascading failures with automatic recovery (see [`circuit_breaker`] module)
//! - **Advanced Retry Strategies** - Exponential backoff with jitter support (see [`retry`] module)
//! - **Message Compression** - Reduce network overhead with Gzip/Zstd (see [`compression`] module)
//! - **Topology Validation** - Validate AMQP topology before deployment (see [`topology`] module)
//! - **Message Tracing** - Track message lifecycle and flow analysis (see [`tracing_util`] module)
//! - **Consumer Groups** - Load balancing with multiple strategies (see [`consumer_groups`] module)
//!
//! # Production Features (v7)
//!
//! - **Rate Limiting** - Token bucket and leaky bucket algorithms for controlling message rates (see [`rate_limiter`] module)
//! - **Bulkhead Pattern** - Resource isolation to prevent cascading failures (see [`bulkhead`] module)
//! - **Message Scheduling** - Delayed message delivery for scheduled tasks and retries (see [`scheduler`] module)
//! - **Metrics Export** - Export metrics to Prometheus, StatsD, and JSON formats (see [`metrics_export`] module)
//!
//! # Advanced Production Features (v8)
//!
//! - **Lifecycle Hooks** - Extensible hooks for intercepting publish, consume, and acknowledgment events (see [`hooks`] module)
//! - **DLX Analytics** - Comprehensive Dead Letter Exchange analytics and insights (see [`dlx_analytics`] module)
//! - **Adaptive Batching** - Dynamic batch size optimization based on system load and latency (see [`batch_optimizer`] module)
//! - **Performance Profiling** - Detailed performance profiling with percentile analysis (see [`profiler`] module)
//!
//! # Enterprise Production Features (v9)
//!
//! - **Backpressure Management** - Intelligent flow control to prevent overwhelming the broker or consumers (see [`backpressure`] module)
//! - **Poison Message Detection** - Identify and handle messages that repeatedly fail processing (see [`poison_detector`] module)
//! - **Advanced Routing** - Sophisticated routing strategies beyond basic AMQP exchange types (see [`router`] module)
//! - **Performance Optimization** - Advanced optimization strategies for connection tuning and resource management (see [`optimization`] module)
//!
//! # Example
//!
//! ```no_run
//! use celers_broker_amqp::{AmqpBroker, AmqpConfig};
//! use celers_kombu::{Broker, Transport, Producer, Consumer};
//! use celers_protocol::builder::MessageBuilder;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Basic usage
//! let mut broker = AmqpBroker::new("amqp://localhost:5672", "celery").await?;
//!
//! // With configuration
//! let config = AmqpConfig::default()
//!     .with_prefetch(10)
//!     .with_retry(3, std::time::Duration::from_secs(1));
//! let mut broker = AmqpBroker::with_config("amqp://localhost:5672", "celery", config).await?;
//!
//! // Publish a message
//! let message = MessageBuilder::new("tasks.add").build()?;
//! broker.publish("my_queue", message).await?;
//!
//! // Consume messages
//! let _envelope = broker.consume("my_queue", std::time::Duration::from_secs(5)).await?;
//!
//! // With Management API for monitoring
//! let config = AmqpConfig::default()
//!     .with_management_api("http://localhost:15672", "guest", "guest");
//! let mut broker = AmqpBroker::with_config("amqp://localhost:5672", "celery", config).await?;
//!
//! // List all queues
//! let queues = broker.list_queues().await?;
//! for queue in queues {
//!     println!("{}: {} messages", queue.name, queue.messages);
//! }
//!
//! // Get detailed queue statistics
//! let stats = broker.get_queue_stats("my_queue").await?;
//! println!("Ready: {}, Unacked: {}", stats.messages_ready, stats.messages_unacknowledged);
//!
//! // Get server overview
//! let overview = broker.get_server_overview().await?;
//! println!("RabbitMQ version: {}", overview.rabbitmq_version);
//!
//! // Monitor connections and channels
//! let connections = broker.list_connections().await?;
//! println!("Active connections: {}", connections.len());
//!
//! let channels = broker.list_channels().await?;
//! println!("Active channels: {}", channels.len());
//!
//! // List exchanges
//! let exchanges = broker.list_exchanges(None).await?;
//! for exchange in exchanges {
//!     println!("{}: {} ({})", exchange.name, exchange.exchange_type, exchange.vhost);
//! }
//!
//! // Inspect queue bindings
//! let bindings = broker.list_queue_bindings("my_queue").await?;
//! for binding in bindings {
//!     println!("{} -> {} via '{}'", binding.source, binding.destination, binding.routing_key);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Running a worker against RabbitMQ
//!
//! [`AmqpBroker`] itself implements `celers_kombu`'s lower-level
//! [`Producer`](celers_kombu::Producer)/[`Consumer`](celers_kombu::Consumer)/
//! [`Transport`](celers_kombu::Transport)/[`Broker`](celers_kombu::Broker)
//! traits — a message-transport abstraction (`publish`/`consume`/`purge`/
//! `create_queue`/...) over [`celers_protocol::Message`]. A
//! `celers_worker::Worker` consumes the *task-queue* abstraction
//! [`celers_core::Broker`] (`enqueue`/`dequeue`/`ack`/`reject`/`defer`/...)
//! instead, so the two do not meet on their own.
//!
//! [`AmqpBroker::into_core_broker`] bridges them, yielding an
//! [`AmqpCoreBroker`]:
//!
//! ```no_run
//! use celers_broker_amqp::AmqpBroker;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let broker = AmqpBroker::new("amqp://localhost:5672", "celery")
//!     .await?
//!     .into_core_broker("celery");
//! // ... hand `broker` to `celers_worker::Worker::new`.
//! # Ok(())
//! # }
//! ```
//!
//! See the [`core_broker`] module for what the adapter maps onto what
//! (batching, `defer`, delayed publishing) and for the trade-offs it inherits.
//! It is behind the `core-broker` feature, which is **on by default**.
//!
//! This does not make the queue readable by a Python Celery worker: the
//! envelope is a JSON-serialized [`celers_protocol::Message`] carrying the
//! task's own metadata, not a Celery v2 task message. See
//! `docs/CELERY_COMPATIBILITY.md`.
//!
//! # Revocation
//!
//! `celers-broker-postgres`, `celers-broker-sql` and `celers-broker-redis` each
//! implement
//! [`celers_core::Broker::revoke`]/[`is_revoked`](celers_core::Broker::is_revoked)/
//! [`subscribe_revocations`](celers_core::Broker::subscribe_revocations).
//! [`AmqpCoreBroker`] keeps the trait's inert defaults, and
//! [`cancel`](celers_core::Broker::cancel) always reports `false`: AMQP hands a
//! consumer whatever is next in the queue and has no operation that addresses
//! one *queued* message by id, so there is no pending copy to withdraw and no
//! server-side revoked set to consult (as a SQL table or a Redis key would be).
//!
//! An AMQP-native revocation channel is now buildable rather than meaningless —
//! a fanout exchange every worker binds an auto-delete queue to for the
//! *already-running-task* notice, backed by a durable table of the kind
//! `celers-broker-postgres` uses for the *queued-task* guarantee — but it is not
//! implemented here, and claiming a revocation was recorded when nothing
//! recorded it would be worse than the honest `false`.

// --- Internal modules (split from original lib.rs) ---
mod batch_ops;
mod broker_core;
mod confirm;
mod connect;
mod management;
mod pool;
mod queue_ops;
mod types;

// --- Public feature modules ---
pub mod backpressure;
pub mod batch_optimizer;
pub mod bulkhead;
pub mod circuit_breaker;
pub mod compression;
pub mod consumer_groups;
pub mod dlx_analytics;
pub mod hooks;
pub mod metrics_export;
pub mod monitoring;
pub mod optimization;
pub mod poison_detector;
pub mod profiler;
pub mod rate_limiter;
pub mod retry;
pub mod router;
pub mod scheduler;
pub mod topic_routing;
pub mod topology;
pub mod tracing_util;
pub mod utilities;

#[cfg(feature = "amqp-events")]
pub mod event_transport;

/// `celers_core::Broker` over this crate's transport, so a worker can consume
/// from RabbitMQ.
///
/// Requires the `core-broker` feature (on by default).
#[cfg(feature = "core-broker")]
pub mod core_broker;

// --- Re-exports: types ---
pub use types::{
    AmqpConfig, AmqpExchangeType, ChannelMetrics, ChannelPoolMetrics, ConnectionPoolMetrics,
    ConsumerConfig, DlxConfig, ExchangeConfig, HealthStatus, MessagePropertiesBuilder,
    PublisherConfirmStats, QueueConfig, QueueLazyMode, QueueOverflowBehavior, QueueType,
    ReconnectionStats, TransactionState,
};

// --- Re-exports: management API data types ---
pub use management::{
    BindingInfo, ChannelInfo, ConnectionDetails, ConnectionInfo, ExchangeInfo, MessageStats,
    ObjectTotals, QueueInfo, QueueStats, QueueTotals, RateDetails, ServerOverview,
};

// --- Re-exports: core broker ---
pub use broker_core::AmqpBroker;

// --- Re-exports: task-queue adapter ---
#[cfg(feature = "core-broker")]
pub use core_broker::AmqpCoreBroker;

// --- Re-exports: Pure-Rust TLS provider installation ---
pub use connect::install_pure_tls_provider;

// --- Tests ---
#[cfg(test)]
#[path = "tests.rs"]
mod tests;

#[cfg(test)]
#[path = "tests_hardening.rs"]
mod tests_hardening;
