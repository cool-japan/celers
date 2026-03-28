//! Celery Protocol v2/v5 implementation
//!
//! This crate provides the core protocol definitions for Celery message format,
//! ensuring compatibility with Python Celery workers.
//!
//! # Protocol Compatibility
//!
//! - Celery Protocol v2 (Celery 4.x+)
//! - Celery Protocol v5 (Celery 5.x+)
//!
//! # Message Format
//!
//! Messages consist of:
//! - **Headers**: Task metadata (task name, ID, parent/root IDs, etc.)
//! - **Properties**: AMQP properties (`correlation_id`, `reply_to`, `delivery_mode`)
//! - **Body**: Serialized task arguments
//! - **Content-Type**: Serialization format ("application/json", "application/x-msgpack")
//! - **Content-Encoding**: Encoding format ("utf-8", "binary")
//!
//! # Modules
//!
//! - [`compat`] - Python Celery compatibility verification
//! - [`serializer`] - Pluggable serialization framework
//! - [`result`] - Task result message format
//! - [`event`] - Celery event message format
//! - [`compression`] - Message body compression
//! - [`embed`] - Embedded body format (args, kwargs, embed)
//! - [`negotiation`] - Protocol version negotiation
//! - [`security`] - Security utilities and content-type whitelist
//! - [`builder`] - Fluent message builder API
//! - [`auth`] - Message authentication and signing (HMAC)
//! - [`crypto`] - Message encryption (AES-256-GCM)
//! - [`extensions`] - Message extensions and utility helpers
//! - [`migration`] - Protocol version migration helpers
//! - [`middleware`] - Message transformation middleware
//! - [`zerocopy`] - Zero-copy deserialization for performance
//! - [`lazy`] - Lazy deserialization for large messages
//! - [`pool`] - Message pooling for memory efficiency
//! - [`extension_api`] - Custom protocol extensions API
//! - [`utils`] - Message utility helpers
//! - [`batch`] - Batch message processing utilities
//! - [`routing`] - Message routing helpers
//! - [`retry`] - Retry strategy utilities
//! - [`dedup`] - Message deduplication utilities
//! - [`priority_queue`] - Priority-based message queues
//! - [`workflow`] - Workflow and task chain utilities

pub mod auth;
pub mod batch;
pub mod builder;
pub mod compat;
pub mod compression;
pub mod crypto;
pub mod dedup;
pub mod embed;
pub mod event;
pub mod extension_api;
pub mod extensions;
pub mod lazy;
mod message;
pub mod middleware;
pub mod migration;
pub mod negotiation;
pub mod pool;
pub mod priority_queue;
pub mod result;
pub mod retry;
pub mod routing;
pub mod security;
pub mod serializer;
mod types;
pub mod utils;
pub mod workflow;
pub mod zerocopy;

#[cfg(test)]
mod tests;

pub use types::*;

// Re-export pub(crate) constants so they remain accessible at `crate::` path
pub(crate) use types::{CONTENT_TYPE_JSON, DEFAULT_LANG, ENCODING_UTF8};
