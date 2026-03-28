//! Redis result backend for CeleRS
//!
//! This crate provides Redis-based storage for task results, workflow state,
//! and real-time event transport.
//!
//! # Features
//!
//! - Task result storage
//! - **Task progress tracking** for long-running tasks
//! - Chord state management (barrier synchronization)
//! - Result expiration (TTL)
//! - Atomic operations for counter-based workflows
//! - Batch operations for high throughput
//! - **Real-time event transport** via Redis pub/sub
//!
//! # Progress Tracking Example
//!
//! ```no_run
//! use celers_backend_redis::{RedisResultBackend, ResultBackend, ProgressInfo};
//! use uuid::Uuid;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut backend = RedisResultBackend::new("redis://localhost")?;
//! let task_id = Uuid::new_v4();
//!
//! // Report progress during task execution
//! let progress = ProgressInfo::new(50, 100)
//!     .with_message("Processing items...".to_string());
//! backend.set_progress(task_id, progress).await?;
//!
//! // Query progress from client
//! if let Some(progress) = backend.get_progress(task_id).await? {
//!     println!("Task {}% complete", progress.percent);
//! }
//! # Ok(())
//! # }
//! ```
//!
//! # Event Transport Example
//!
//! ```no_run
//! use celers_backend_redis::event_transport::{RedisEventEmitter, RedisEventReceiver};
//! use celers_core::event::{EventEmitter, WorkerEventBuilder};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Publishing events
//! let emitter = RedisEventEmitter::new("redis://localhost")?;
//! let event = WorkerEventBuilder::new("worker-1").online();
//! emitter.emit(event).await?;
//!
//! // Receiving events
//! let receiver = RedisEventReceiver::new("redis://localhost")?;
//! // Subscribe and process events...
//! # Ok(())
//! # }
//! ```

// ── Core modules (extracted from monolithic lib.rs) ──────────────────
pub mod backend;
pub mod query;
pub mod result_backend_trait;
pub mod stats;
pub mod trait_impl;
pub mod types;

// ── Feature modules ──────────────────────────────────────────────────
pub mod batch_stream;
pub mod cache;
pub mod chunking;
pub mod compression;
pub mod encryption;
pub mod event_transport;
#[cfg(feature = "distributed-locks")]
pub mod lock;
pub mod metrics;
pub mod monitoring;
pub mod pipeline;
pub mod profiler;
pub mod result_store;
pub mod retry;
pub mod telemetry;
pub mod utilities;

// ── Tests ────────────────────────────────────────────────────────────
#[cfg(test)]
#[path = "tests.rs"]
mod tests;

// ── Re-exports: preserve public API ──────────────────────────────────

// types
pub use types::{
    BackendError, ChordState, ProgressInfo, Result, TaskMeta, TaskResult, TaskTtlConfig,
};

// trait + helpers
pub use result_backend_trait::{LazyTaskResult, ResultBackend, ResultStream};

// backend struct
pub use backend::RedisResultBackend;

// query
pub use query::TaskQuery;

// stats & constants
pub use stats::{
    batch_size, ttl, BackendStats, BatchOperationResult, PoolStats, StateCount, StatePercentages,
    TaskSummary,
};
