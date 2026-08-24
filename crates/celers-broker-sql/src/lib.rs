//! MySQL broker implementation for CeleRS
//!
//! This broker uses MySQL with `FOR UPDATE SKIP LOCKED` for reliable,
//! distributed task queue processing.
//!
//! # Minimum server version
//!
//! `FOR UPDATE ... SKIP LOCKED` — which every dequeue path depends on — was
//! introduced in **MySQL 8.0.1** and **MariaDB 10.6**. It does not exist in
//! MySQL 5.7 or in any earlier MariaDB release, where every dequeue would
//! fail with a parse error.
//!
//! The version is probed once at connect time
//! ([`MysqlBroker::new`] / [`MysqlBroker::with_queue`] /
//! [`MysqlBroker::with_config`] / [`MysqlBroker::with_circuit_breaker_config`])
//! and an unsupported server is rejected with a
//! [`celers_core::CelersError::Configuration`] naming the required version.
//! A version string the probe cannot parse (proxies, forks) is logged and
//! accepted — see [`server_version`].
//!
//! It supports:
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

// Row-mapping helpers for the OxiSQL-backed MySQL connection (panic-free
// `row.get::<T, _>()` replacement) — see its module doc for details.
mod row_ext;

// URL-driven TLS mode selection for the OxiSQL-backed MySQL connection.
mod tls_mode;

// Canonical SQL text for the claim/dequeue spine (single source of truth,
// unit-testable without a server).
mod sql_text;

// Claimed-row -> BrokerMessage mapping and receipt-handle helpers.
mod task_row;

// Overflow-safe retry backoff.
mod backoff;

// Server capability gating for FOR UPDATE ... SKIP LOCKED.
pub mod server_version;
pub use server_version::{ServerFlavor, ServerVersion};

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
pub mod broker_enhanced;
pub use broker_enhanced::TransactionFuture;

// Broker trait implementation
mod broker_trait;

// Worker-attributed and batch claim paths (split out of broker_core.rs)
mod broker_dequeue;

// Task result storage (split out of broker_core.rs)
mod broker_results;

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

// Regression tests for the hardened claim/ack/reject spine.
#[cfg(test)]
mod tests_hardening;
