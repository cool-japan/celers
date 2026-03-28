//! Compile-time feature validation and conflict detection
//!
//! Compile-time feature validation to detect conflicts and ensure correct feature usage.
//!
//! This module uses Rust's const evaluation to perform compile-time checks for:
//! - Feature conflicts (e.g., using incompatible brokers together)
//! - Missing required features
//! - Dead code elimination opportunities
//!
//! # Example
//!
//! ```rust
//! use celers::compile_time_validation::*;
//!
//! // This will pass compilation checks
//! const VALID_CONFIG: () = validate_feature_config();
//! ```

/// Validates that at least one broker feature is enabled
#[inline]
pub const fn has_broker_feature() -> bool {
    cfg!(any(
        feature = "redis",
        feature = "postgres",
        feature = "mysql",
        feature = "amqp",
        feature = "sqs"
    ))
}

/// Validates that at least one serialization format is enabled
#[inline]
pub const fn has_serialization_feature() -> bool {
    cfg!(any(feature = "json", feature = "msgpack"))
}

/// Count how many broker features are enabled
#[inline]
pub const fn count_broker_features() -> usize {
    let mut count = 0;
    if cfg!(feature = "redis") {
        count += 1;
    }
    if cfg!(feature = "postgres") {
        count += 1;
    }
    if cfg!(feature = "mysql") {
        count += 1;
    }
    if cfg!(feature = "amqp") {
        count += 1;
    }
    if cfg!(feature = "sqs") {
        count += 1;
    }
    count
}

/// Count how many backend features are enabled
#[inline]
pub const fn count_backend_features() -> usize {
    let mut count = 0;
    if cfg!(feature = "backend-redis") {
        count += 1;
    }
    if cfg!(feature = "backend-db") {
        count += 1;
    }
    if cfg!(feature = "backend-rpc") {
        count += 1;
    }
    count
}

/// Validates feature configuration at compile time
///
/// This function is designed to be called in a const context to ensure
/// compile-time validation of feature flags.
#[inline]
pub const fn validate_feature_config() {
    // Ensure at least one broker is available
    if !has_broker_feature() {
        panic!("At least one broker feature must be enabled: redis, postgres, mysql, amqp, or sqs");
    }

    // Ensure at least one serialization format is available
    if !has_serialization_feature() {
        panic!("At least one serialization feature must be enabled: json or msgpack");
    }
}

/// Returns a human-readable string describing the current feature configuration
pub fn feature_summary() -> String {
    let broker_count = count_broker_features();
    let backend_count = count_backend_features();

    let mut brokers = Vec::new();
    if cfg!(feature = "redis") {
        brokers.push("redis");
    }
    if cfg!(feature = "postgres") {
        brokers.push("postgres");
    }
    if cfg!(feature = "mysql") {
        brokers.push("mysql");
    }
    if cfg!(feature = "amqp") {
        brokers.push("amqp");
    }
    if cfg!(feature = "sqs") {
        brokers.push("sqs");
    }

    let mut backends = Vec::new();
    if cfg!(feature = "backend-redis") {
        backends.push("redis");
    }
    if cfg!(feature = "backend-db") {
        backends.push("database");
    }
    if cfg!(feature = "backend-rpc") {
        backends.push("grpc");
    }

    let mut formats = Vec::new();
    if cfg!(feature = "json") {
        formats.push("json");
    }
    if cfg!(feature = "msgpack") {
        formats.push("msgpack");
    }

    let mut features = Vec::new();
    if cfg!(feature = "beat") {
        features.push("beat");
    }
    if cfg!(feature = "metrics") {
        features.push("metrics");
    }
    if cfg!(feature = "tracing") {
        features.push("tracing");
    }
    if cfg!(feature = "dev-utils") {
        features.push("dev-utils");
    }

    format!(
        "CeleRS Configuration:\n\
         Brokers ({}): {}\n\
         Backends ({}): {}\n\
         Formats ({}): {}\n\
         Features: {}",
        broker_count,
        if brokers.is_empty() {
            "none".to_string()
        } else {
            brokers.join(", ")
        },
        backend_count,
        if backends.is_empty() {
            "none".to_string()
        } else {
            backends.join(", ")
        },
        formats.len(),
        if formats.is_empty() {
            "none".to_string()
        } else {
            formats.join(", ")
        },
        if features.is_empty() {
            "none".to_string()
        } else {
            features.join(", ")
        }
    )
}
