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
pub mod anomaly;
pub mod audit;
pub mod backends;
pub mod exposition;
pub mod health;
pub mod history;
pub mod native_histogram;
pub mod prometheus_metrics;
pub mod slo;
pub mod slo_tracker;
pub mod statsd;
pub mod summary;
pub mod tooling;

#[cfg(test)]
mod tests_advanced;
#[cfg(test)]
mod tests_core;
#[cfg(test)]
mod tests_core_extra;
#[cfg(test)]
mod tests_enhanced;

// Re-export everything for backward compatibility
pub use aggregation::*;
pub use alerts::*;
pub use anomaly::*;
pub use audit::*;
pub use backends::*;
pub use exposition::*;
pub use health::*;
pub use history::*;
pub use native_histogram::*;
pub use prometheus_metrics::*;
pub use slo::*;
pub use slo_tracker::*;
pub use statsd::*;
pub use summary::*;
pub use tooling::*;
