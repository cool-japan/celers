//! Client configuration for [`crate::GrpcResultBackend`].
//!
//! Centralizes the hardening knobs a production gRPC client needs: a
//! per-call deadline, a connection-establishment timeout, a decode/encode
//! message-size ceiling, an optional bearer token for authentication, and
//! the retry policy applied to transient failures.

use celers_backend_redis::retry::RetryStrategy;
use std::time::Duration;

/// Default per-request deadline applied to every RPC unless overridden.
pub const DEFAULT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

/// Default timeout for establishing the underlying gRPC connection.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(10);

/// Default maximum protobuf message size (16 MiB) applied to both encoding
/// and decoding, on the client and the reference server. Replaces tonic's
/// silent, undocumented 4 MiB decode default with an explicit, generous
/// limit that still fails fast (as a typed [`celers_backend_redis::BackendError`]
/// rather than an opaque transport error) on runaway payloads.
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Configuration for a [`crate::GrpcResultBackend`] connection.
///
/// Controls per-call deadlines, connection timeouts, message size limits,
/// bearer-token authentication, and the retry policy applied to transient
/// gRPC failures (`Unavailable` / `DeadlineExceeded`).
///
/// # TLS is intentionally not configured here
///
/// Wiring tonic's built-in TLS support requires enabling one of its
/// `tls-*` Cargo features, every one of which pulls in `tokio-rustls` with
/// either the `ring` or `aws-lc-rs` cryptography backend (tonic 0.14 does
/// not expose a feature to select a pure-Rust backend such as
/// `rustls-rustcrypto`). Both `ring` (hand-written assembly/C) and
/// `aws-lc-rs` (an `aws-lc-sys` C library via FFI) violate this
/// workspace's Pure-Rust policy, so this crate cannot turn TLS on via
/// Cargo features without regressing that policy for every consumer that
/// enables default features.
///
/// Callers who need TLS should build their own `tonic::transport::Channel`
/// (for example with a pure-Rust-compliant stack) and hand it to
/// [`crate::GrpcResultBackend::from_channel_with_config`], which still
/// applies the deadline, message-size, and auth-token hardening below on
/// top of a caller-supplied channel. See the crate-level docs for
/// details.
#[derive(Debug, Clone)]
pub struct GrpcConfig {
    /// Deadline applied to every individual RPC call. Sent to the server
    /// as a `grpc-timeout` header (so it can abort early too) and also
    /// enforced client-side.
    pub request_timeout: Duration,

    /// Timeout for establishing the underlying connection in
    /// [`crate::GrpcResultBackend::connect_with_config`]. Has no effect
    /// when constructing via
    /// [`crate::GrpcResultBackend::from_channel_with_config`], since the
    /// channel is already connected (or lazily connects on first use) by
    /// the time it is handed over.
    pub connect_timeout: Duration,

    /// Maximum size, in bytes, of an encoded or decoded protobuf message.
    pub max_message_size: usize,

    /// Optional bearer token sent as an `authorization: Bearer <token>`
    /// gRPC metadata header on every request.
    pub auth_token: Option<String>,

    /// Retry policy applied to `Unavailable` / `DeadlineExceeded` gRPC
    /// failures. Non-retryable failures (e.g. `InvalidArgument`,
    /// `NotFound`) are always returned immediately regardless of this
    /// policy.
    pub retry: RetryStrategy,
}

impl Default for GrpcConfig {
    fn default() -> Self {
        Self {
            request_timeout: DEFAULT_REQUEST_TIMEOUT,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            max_message_size: DEFAULT_MAX_MESSAGE_SIZE,
            auth_token: None,
            retry: RetryStrategy::default(),
        }
    }
}

impl GrpcConfig {
    /// Start from the defaults (30s request timeout, 10s connect timeout,
    /// 16 MiB message cap, no auth, the standard exponential-backoff
    /// retry policy).
    pub fn new() -> Self {
        Self::default()
    }

    /// Override the per-request deadline.
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// Override the connect timeout.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Override the maximum protobuf message size (applies to both
    /// encoding and decoding).
    pub fn with_max_message_size(mut self, size: usize) -> Self {
        self.max_message_size = size;
        self
    }

    /// Attach a bearer token sent on every request.
    pub fn with_auth_token(mut self, token: impl Into<String>) -> Self {
        self.auth_token = Some(token.into());
        self
    }

    /// Override the retry policy for transient gRPC failures.
    pub fn with_retry(mut self, retry: RetryStrategy) -> Self {
        self.retry = retry;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config_values() {
        let config = GrpcConfig::default();
        assert_eq!(config.request_timeout, DEFAULT_REQUEST_TIMEOUT);
        assert_eq!(config.connect_timeout, DEFAULT_CONNECT_TIMEOUT);
        assert_eq!(config.max_message_size, DEFAULT_MAX_MESSAGE_SIZE);
        assert!(config.auth_token.is_none());
        assert_eq!(
            config.retry.max_attempts,
            RetryStrategy::default().max_attempts
        );
    }

    #[test]
    fn test_builder_overrides() {
        let config = GrpcConfig::new()
            .with_request_timeout(Duration::from_secs(5))
            .with_connect_timeout(Duration::from_secs(2))
            .with_max_message_size(1024)
            .with_auth_token("secret-token")
            .with_retry(RetryStrategy::new().with_max_attempts(7));

        assert_eq!(config.request_timeout, Duration::from_secs(5));
        assert_eq!(config.connect_timeout, Duration::from_secs(2));
        assert_eq!(config.max_message_size, 1024);
        assert_eq!(config.auth_token.as_deref(), Some("secret-token"));
        assert_eq!(config.retry.max_attempts, 7);
    }
}
