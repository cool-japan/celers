//! Prometheus metrics for `CeleRS`
//!
//! This module provides Prometheus metrics integration for monitoring task queue performance.
#![allow(clippy::must_use_candidate)]
#![allow(clippy::return_self_not_must_use)]
#![allow(clippy::cast_precision_loss)]
#![allow(clippy::cast_possible_truncation)]
#![allow(clippy::cast_sign_loss)]
#![allow(clippy::similar_names)]
#![allow(clippy::non_std_lazy_statics)]

pub mod aggregation;
pub mod alerts;
pub mod backends;
pub mod health;
pub mod history;
pub mod prometheus_metrics;
pub mod slo;
pub mod tooling;

#[cfg(test)]
mod tests_advanced;
#[cfg(test)]
mod tests_core;

// Re-export everything for backward compatibility
pub use aggregation::*;
pub use alerts::*;
pub use backends::*;
pub use health::*;
pub use history::*;
pub use prometheus_metrics::*;
pub use slo::*;
pub use tooling::*;
