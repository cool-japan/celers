//! MySQL broker implementation for CeleRS
//!
//! This broker uses MySQL with `FOR UPDATE SKIP LOCKED` for reliable,
//! distributed task queue processing. It supports:
//! - Priority queues
//! - Dead Letter Queue (DLQ) for permanently failed tasks
//! - Delayed task execution (enqueue_at, enqueue_after)
//! - Prometheus metrics (optional `metrics` feature)
//! - Batch enqueue/dequeue/ack operations
//! - Transaction safety
//! - Distributed workers without contention
//! - Queue pause/resume functionality
//! - DLQ inspection and requeue
//! - Task status inspection
//! - Database health checks
//! - Automatic task archiving

// Core type definitions
pub mod types;
pub use types::*;

// Circuit breaker and idempotency types
pub mod circuit_breaker;
pub use circuit_breaker::*;

// Workflow, hooks, and builder types
pub mod workflow;
pub use workflow::*;

// Distributed tracing context
pub mod tracing;
pub use tracing::*;

// Statistics and diagnostics types
pub mod stats_types;
pub use stats_types::*;

// MysqlBroker struct and core implementation
pub mod broker_core;
pub use broker_core::MysqlBroker;

// Distributed tracing and lifecycle hooks
mod broker_hooks;

// Enhanced broker operations
mod broker_enhanced;

// Broker trait implementation
mod broker_trait;

// Task chain and batch reject operations
mod broker_chain;

// Advanced operations (metrics, retention, rate limiting)
mod broker_advanced;

// Resilience features (retry policies, recurring tasks, circuit breaker, idempotency)
mod broker_resilience;

// Batch operations, worker management, and task groups
pub mod broker_batch;
pub use broker_batch::*;

// Diagnostics, profiling, and statistics
mod broker_diagnostics;

// Monitoring utilities
pub mod monitoring;
pub mod utilities;

// Tests
#[cfg(test)]
mod tests;
