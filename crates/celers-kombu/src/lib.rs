//! Broker abstraction layer (Kombu-style)
//!
//! This crate provides the abstract interface for message brokers, inspired by
//! Python's Kombu library. It defines the core traits that all broker implementations
//! must follow.
//!
//! # Architecture
//!
//! - **Transport**: Low-level broker connection (Redis, AMQP, SQS, Database)
//! - **Producer**: Message publishing interface
//! - **Consumer**: Message consuming interface
//! - **Queue**: Queue abstraction with routing
//! - **BatchProducer**: Batch message publishing interface
//! - **BatchConsumer**: Batch message consuming interface
//! - **HealthCheck**: Health monitoring interface
//! - **Metrics**: Metrics collection interface
//! - **Admin**: Broker administration interface

mod backpressure;
mod circuit_breaker;
mod connection;
mod dlq;
mod error;
mod exchange;
mod health;
mod middleware;
mod middleware_advanced;
mod middleware_basic;
mod middleware_extended;
mod middleware_monitoring;
mod mock;
mod priority;
mod queue_mode;
mod quota;
mod retry;
mod scheduler;
mod types;

pub mod utils;

// Re-export everything from submodules to preserve the public API
pub use backpressure::*;
pub use circuit_breaker::*;
pub use connection::*;
pub use dlq::*;
pub use error::*;
pub use exchange::*;
pub use health::*;
pub use middleware::*;
pub use middleware_advanced::*;
pub use middleware_basic::*;
pub use middleware_extended::*;
pub use middleware_monitoring::*;
pub use mock::*;
pub use priority::*;
pub use queue_mode::*;
pub use quota::*;
pub use retry::*;
pub use scheduler::*;
pub use types::*;

#[cfg(test)]
mod tests;
#[cfg(test)]
mod tests_advanced;
#[cfg(test)]
mod tests_middleware;
