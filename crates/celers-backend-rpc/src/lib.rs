//! gRPC/RPC result backend for CeleRS
//!
//! This crate provides gRPC-based result storage for distributed microservices architectures.
//! It ships **both halves** of the RPC boundary:
//!
//! - [`GrpcResultBackend`] — a client implementing [`ResultBackend`] by calling out to a
//!   remote service.
//! - [`server::RpcBackendServer`] — a reference server that implements the generated
//!   `ResultBackendService` gRPC trait by delegating to any local [`ResultBackend`]
//!   (e.g. `RedisResultBackend`, a SQL-backed one, or an in-memory implementation).
//!
//! A client with nothing implementing the service on the other end is not useful on its
//! own, so [`server::RpcBackendServer`] exists specifically so `GrpcResultBackend::connect(...)`
//! has something to talk to without every user having to hand-roll the eight RPCs (including
//! the chord-completion counter's atomicity) themselves.
//!
//! # Features
//!
//! - gRPC client ([`GrpcResultBackend`]) and reference server ([`server::RpcBackendServer`])
//! - Per-call deadlines, connect timeouts, and message-size limits ([`GrpcConfig`])
//! - Optional bearer-token authentication
//! - Automatic retry with exponential backoff for transient (`Unavailable` /
//!   `DeadlineExceeded`) failures
//! - Client-side Prometheus-compatible metrics ([`RpcMetrics`])
//! - Service mesh compatible
//!
//! TLS is deliberately **not** wired up via tonic's built-in `tls-*` Cargo features — see
//! the [`GrpcConfig`] docs for why, and how to bring your own TLS-enabled channel via
//! [`GrpcResultBackend::from_channel_with_config`].
//!
//! # Example
//!
//! ```no_run
//! use celers_backend_rpc::GrpcResultBackend;
//! use celers_backend_redis::{ResultBackend, TaskMeta};
//! use uuid::Uuid;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut backend = GrpcResultBackend::connect("http://localhost:50051").await?;
//!
//! // Store result
//! let task_id = Uuid::new_v4();
//! let meta = TaskMeta::new(task_id, "my_task".to_string());
//! backend.store_result(task_id, &meta).await?;
//! # Ok(())
//! # }
//! ```
//!
//! See [`server`] for how to run the other end of that connection.

#![deny(clippy::unwrap_used, clippy::expect_used)]

pub mod compression;
pub mod config;
pub mod metrics;
pub mod result_store;
pub mod server;

mod codec;
mod task_meta_extra;

pub use compression::CompressionConfig;
pub use config::GrpcConfig;
pub use metrics::{OperationStats, RpcMetrics, RpcMetricsSnapshot, RpcOperation};
pub use server::RpcBackendServer;

use async_trait::async_trait;
pub use celers_backend_redis::{
    retry::RetryStrategy, BackendError, ChordState, Result, ResultBackend, TaskMeta, TaskResult,
};
use std::sync::Arc;
use std::time::Duration;
use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use uuid::Uuid;

// Include generated protobuf code
pub mod proto {
    tonic::include_proto!("result_backend");
}

use proto::{
    result_backend_service_client::ResultBackendServiceClient, ChordCompleteTaskRequest,
    ChordGetStateRequest, ChordInitRequest, ChordUpdateStateRequest, DeleteResultRequest,
    GetResultRequest, SetExpirationRequest, StoreResultRequest,
};

/// gRPC result backend client
#[derive(Clone)]
pub struct GrpcResultBackend {
    client: ResultBackendServiceClient<Channel>,
    metrics: Arc<RpcMetrics>,
    config: GrpcConfig,
    compression: CompressionConfig,
}

impl GrpcResultBackend {
    /// Connect to a gRPC result backend service using the default
    /// [`GrpcConfig`] (30s request timeout, 10s connect timeout, 16 MiB
    /// message cap, no auth, standard retry policy).
    ///
    /// # Arguments
    /// * `endpoint` - gRPC server endpoint (e.g., "http://localhost:50051")
    pub async fn connect(endpoint: &str) -> Result<Self> {
        Self::connect_with_config(endpoint, GrpcConfig::default()).await
    }

    /// Connect to a gRPC result backend service with an explicit
    /// [`GrpcConfig`], applying its connect timeout and request timeout to
    /// the underlying [`tonic::transport::Endpoint`] before dialing.
    pub async fn connect_with_config(endpoint: &str, config: GrpcConfig) -> Result<Self> {
        let channel_endpoint = tonic::transport::Endpoint::from_shared(endpoint.to_string())
            .map_err(|e| {
                BackendError::Connection(format!("Invalid gRPC endpoint {endpoint:?}: {e}"))
            })?
            .connect_timeout(config.connect_timeout)
            .timeout(config.request_timeout);

        let channel = channel_endpoint.connect().await.map_err(|e| {
            BackendError::Connection(format!("Failed to connect to gRPC server: {}", e))
        })?;

        Ok(Self::from_channel_with_config(channel, config))
    }

    /// Connect with a custom channel (e.g. one configured with your own
    /// TLS setup) and the default [`GrpcConfig`].
    pub fn from_channel(channel: Channel) -> Self {
        Self::from_channel_with_config(channel, GrpcConfig::default())
    }

    /// Connect with a custom channel and an explicit [`GrpcConfig`].
    ///
    /// This is the integration point for callers who need TLS: build a
    /// `tonic::transport::Channel` however you like (including with your
    /// own TLS stack) and hand it here — the deadline, message-size, and
    /// auth-token hardening in `config` still applies to every request.
    pub fn from_channel_with_config(channel: Channel, config: GrpcConfig) -> Self {
        let client = ResultBackendServiceClient::new(channel)
            .max_decoding_message_size(config.max_message_size)
            .max_encoding_message_size(config.max_message_size);

        Self {
            client,
            metrics: Arc::new(RpcMetrics::new()),
            config,
            compression: CompressionConfig::disabled(),
        }
    }

    /// The configuration this client was constructed with.
    pub fn config(&self) -> &GrpcConfig {
        &self.config
    }

    /// Configure compression for `result_data` on the wire.
    ///
    /// Disabled by default — see [`CompressionConfig::disabled`]. Decoding
    /// a compressed *response* is unconditional regardless of this
    /// setting, so turning compression off here never breaks reading a
    /// result some other, compression-enabled client stored.
    pub fn with_compression(mut self, config: CompressionConfig) -> Self {
        self.compression = config;
        self
    }

    /// Get the compression configuration.
    pub fn compression_config(&self) -> &CompressionConfig {
        &self.compression
    }

    /// Return a point-in-time snapshot of all RPC metrics.
    pub fn metrics(&self) -> RpcMetricsSnapshot {
        self.metrics.snapshot()
    }

    /// Reset all metric counters and latency samples.
    pub fn reset_metrics(&self) {
        self.metrics.reset();
    }

    /// Return a clone of the internal `Arc<RpcMetrics>` for sharing with other
    /// components (e.g. a Prometheus exporter running in a separate task).
    pub fn metrics_handle(&self) -> Arc<RpcMetrics> {
        Arc::clone(&self.metrics)
    }

    /// Build a request carrying `message`, applying the configured
    /// per-call deadline (as a `grpc-timeout` header, so the server can
    /// honor it too) and, if configured, a bearer-token `authorization`
    /// header. Used by every RPC method below so hardening lives in one
    /// place instead of being copy-pasted eight times.
    fn prepare_request<T>(&self, message: T) -> Result<tonic::Request<T>> {
        let mut request = tonic::Request::new(message);
        request.set_timeout(self.config.request_timeout);

        if let Some(token) = &self.config.auth_token {
            let value = MetadataValue::try_from(format!("Bearer {token}"))
                .map_err(|e| BackendError::Connection(format!("invalid auth token: {e}")))?;
            request.metadata_mut().insert("authorization", value);
        }

        Ok(request)
    }
}

/// Whether a gRPC status code represents a transient failure worth
/// retrying, as opposed to e.g. `InvalidArgument` or `NotFound`, which
/// will fail identically on every attempt.
fn is_retryable_code(code: tonic::Code) -> bool {
    matches!(
        code,
        tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
    )
}

/// Outcome of evaluating whether a failed RPC attempt should be retried.
#[derive(Debug)]
enum RetryDecision {
    /// Wait this long, then try again.
    Retry(Duration),
    /// Stop and surface this error to the caller.
    GiveUp(BackendError),
}

/// Decide whether the attempt that just failed with `status` (1-based
/// `attempt` count, i.e. the count *including* the attempt that just
/// failed) should be retried under `retry`.
///
/// Pure and synchronous by design: the actual backoff sleep happens in
/// the caller, so this decision is fully unit-testable without an async
/// runtime or a real gRPC server.
fn decide_retry(retry: &RetryStrategy, attempt: u32, status: &tonic::Status) -> RetryDecision {
    if attempt >= retry.max_attempts || !is_retryable_code(status.code()) {
        RetryDecision::GiveUp(BackendError::Connection(format!("gRPC error: {status}")))
    } else {
        RetryDecision::Retry(retry.backoff_duration(attempt - 1))
    }
}

/// Sleep for `backoff` before the next retry attempt, logging why.
async fn wait_before_retry(
    operation: &'static str,
    attempt: u32,
    backoff: Duration,
    code: tonic::Code,
) {
    tracing::debug!(
        operation,
        attempt,
        backoff_ms = backoff.as_millis(),
        ?code,
        "retrying gRPC call after backoff"
    );
    tokio::time::sleep(backoff).await;
}

#[async_trait]
impl ResultBackend for GrpcResultBackend {
    async fn store_result(&mut self, task_id: Uuid, meta: &TaskMeta) -> Result<()> {
        let proto_meta = codec::to_proto_meta_with_compression(meta, &self.compression)?;
        let message = StoreResultRequest {
            task_id: task_id.to_string(),
            meta: Some(proto_meta),
        };

        let start = std::time::Instant::now();
        let mut attempt = 0u32;
        let result = loop {
            let request = self.prepare_request(message.clone())?;
            match self.client.store_result(request).await {
                Ok(resp) => break Ok(resp),
                Err(status) => {
                    attempt += 1;
                    match decide_retry(&self.config.retry, attempt, &status) {
                        RetryDecision::GiveUp(err) => break Err(err),
                        RetryDecision::Retry(backoff) => {
                            wait_before_retry("store_result", attempt, backoff, status.code())
                                .await;
                        }
                    }
                }
            }
        };
        let elapsed = start.elapsed();
        self.metrics
            .record(RpcOperation::StoreResult, elapsed, result.is_err());

        result.map(|_| ())
    }

    async fn get_result(&mut self, task_id: Uuid) -> Result<Option<TaskMeta>> {
        let message = GetResultRequest {
            task_id: task_id.to_string(),
        };

        let start = std::time::Instant::now();
        let mut attempt = 0u32;
        let result = loop {
            let request = self.prepare_request(message.clone())?;
            match self.client.get_result(request).await {
                Ok(resp) => break Ok(resp),
                Err(status) => {
                    attempt += 1;
                    match decide_retry(&self.config.retry, attempt, &status) {
                        RetryDecision::GiveUp(err) => break Err(err),
                        RetryDecision::Retry(backoff) => {
                            wait_before_retry("get_result", attempt, backoff, status.code()).await;
                        }
                    }
                }
            }
        };
        let elapsed = start.elapsed();
        self.metrics
            .record(RpcOperation::GetResult, elapsed, result.is_err());

        match result?.into_inner().meta {
            Some(proto_meta) => Ok(Some(codec::from_proto_meta(proto_meta)?)),
            None => Ok(None),
        }
    }

    async fn delete_result(&mut self, task_id: Uuid) -> Result<()> {
        let message = DeleteResultRequest {
            task_id: task_id.to_string(),
        };

        let start = std::time::Instant::now();
        let mut attempt = 0u32;
        let result = loop {
            let request = self.prepare_request(message.clone())?;
            match self.client.delete_result(request).await {
                Ok(resp) => break Ok(resp),
                Err(status) => {
                    attempt += 1;
                    match decide_retry(&self.config.retry, attempt, &status) {
                        RetryDecision::GiveUp(err) => break Err(err),
                        RetryDecision::Retry(backoff) => {
                            wait_before_retry("delete_result", attempt, backoff, status.code())
                                .await;
                        }
                    }
                }
            }
        };
        let elapsed = start.elapsed();
        self.metrics
            .record(RpcOperation::DeleteResult, elapsed, result.is_err());

        result.map(|_| ())
    }

    async fn set_expiration(&mut self, task_id: Uuid, ttl: Duration) -> Result<()> {
        let message = SetExpirationRequest {
            task_id: task_id.to_string(),
            ttl_seconds: ttl.as_secs(),
        };

        let start = std::time::Instant::now();
        let mut attempt = 0u32;
        let result = loop {
            let request = self.prepare_request(message.clone())?;
            match self.client.set_expiration(request).await {
                Ok(resp) => break Ok(resp),
                Err(status) => {
                    attempt += 1;
                    match decide_retry(&self.config.retry, attempt, &status) {
                        RetryDecision::GiveUp(err) => break Err(err),
                        RetryDecision::Retry(backoff) => {
                            wait_before_retry("set_expiration", attempt, backoff, status.code())
                                .await;
                        }
                    }
                }
            }
        };
        let elapsed = start.elapsed();
        self.metrics
            .record(RpcOperation::SetExpiration, elapsed, result.is_err());

        result.map(|_| ())
    }

    async fn chord_init(&mut self, state: ChordState) -> Result<()> {
        let message = ChordInitRequest {
            state: Some(codec::to_proto_chord(&state)),
        };

        let start = std::time::Instant::now();
        let mut attempt = 0u32;
        let result = loop {
            let request = self.prepare_request(message.clone())?;
            match self.client.chord_init(request).await {
                Ok(resp) => break Ok(resp),
                Err(status) => {
                    attempt += 1;
                    match decide_retry(&self.config.retry, attempt, &status) {
                        RetryDecision::GiveUp(err) => break Err(err),
                        RetryDecision::Retry(backoff) => {
                            wait_before_retry("chord_init", attempt, backoff, status.code()).await;
                        }
                    }
                }
            }
        };
        let elapsed = start.elapsed();
        self.metrics
            .record(RpcOperation::ChordInit, elapsed, result.is_err());

        result.map(|_| ())
    }

    /// Persist a mutated chord state (cancellation, callback change,
    /// timeout update, ...) **without** resetting the completion counter.
    ///
    /// Overriding this is what makes the trait's default
    /// [`ResultBackend::chord_cancel`] correct over gRPC: without it, that
    /// default falls through to [`Self::chord_init`] — a create-or-reset
    /// primitive — and cancelling a chord would silently wipe out every
    /// task that had already completed.
    ///
    /// # Version skew
    ///
    /// `ChordUpdateState` is a new RPC: a client built with this method
    /// against an older [`server::RpcBackendServer`] that predates it gets
    /// back gRPC `Unimplemented`, which `is_retryable_code` correctly
    /// does not retry, so [`ResultBackend::chord_cancel`] (and any other
    /// caller of this method) fails outright during a rolling deploy where
    /// servers lag clients. That is a loud failure, and strictly better
    /// than the silent completion-counter reset it replaces — but roll out
    /// servers before clients to avoid hitting it.
    async fn chord_update_state(&mut self, state: ChordState) -> Result<()> {
        let message = ChordUpdateStateRequest {
            state: Some(codec::to_proto_chord(&state)),
        };

        let start = std::time::Instant::now();
        let mut attempt = 0u32;
        let result = loop {
            let request = self.prepare_request(message.clone())?;
            match self.client.chord_update_state(request).await {
                Ok(resp) => break Ok(resp),
                Err(status) => {
                    attempt += 1;
                    match decide_retry(&self.config.retry, attempt, &status) {
                        RetryDecision::GiveUp(err) => break Err(err),
                        RetryDecision::Retry(backoff) => {
                            wait_before_retry(
                                "chord_update_state",
                                attempt,
                                backoff,
                                status.code(),
                            )
                            .await;
                        }
                    }
                }
            }
        };
        let elapsed = start.elapsed();
        self.metrics
            .record(RpcOperation::ChordUpdateState, elapsed, result.is_err());

        result.map(|_| ())
    }

    async fn chord_complete_task(&mut self, chord_id: Uuid) -> Result<usize> {
        let message = ChordCompleteTaskRequest {
            chord_id: chord_id.to_string(),
        };

        let start = std::time::Instant::now();
        let mut attempt = 0u32;
        let result = loop {
            let request = self.prepare_request(message.clone())?;
            match self.client.chord_complete_task(request).await {
                Ok(resp) => break Ok(resp),
                Err(status) => {
                    attempt += 1;
                    match decide_retry(&self.config.retry, attempt, &status) {
                        RetryDecision::GiveUp(err) => break Err(err),
                        RetryDecision::Retry(backoff) => {
                            wait_before_retry(
                                "chord_complete_task",
                                attempt,
                                backoff,
                                status.code(),
                            )
                            .await;
                        }
                    }
                }
            }
        };
        let elapsed = start.elapsed();
        self.metrics
            .record(RpcOperation::ChordCompleteTask, elapsed, result.is_err());

        Ok(result?.into_inner().completed_count as usize)
    }

    async fn chord_get_state(&mut self, chord_id: Uuid) -> Result<Option<ChordState>> {
        let message = ChordGetStateRequest {
            chord_id: chord_id.to_string(),
        };

        let start = std::time::Instant::now();
        let mut attempt = 0u32;
        let result = loop {
            let request = self.prepare_request(message.clone())?;
            match self.client.chord_get_state(request).await {
                Ok(resp) => break Ok(resp),
                Err(status) => {
                    attempt += 1;
                    match decide_retry(&self.config.retry, attempt, &status) {
                        RetryDecision::GiveUp(err) => break Err(err),
                        RetryDecision::Retry(backoff) => {
                            wait_before_retry("chord_get_state", attempt, backoff, status.code())
                                .await;
                        }
                    }
                }
            }
        };
        let elapsed = start.elapsed();
        self.metrics
            .record(RpcOperation::ChordGetState, elapsed, result.is_err());

        match result?.into_inner().state {
            Some(proto_state) => Ok(Some(codec::from_proto_chord(proto_state)?)),
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use chrono::Utc;
    use uuid::Uuid;

    fn create_test_meta() -> TaskMeta {
        TaskMeta {
            task_id: Uuid::new_v4(),
            task_name: "test_task".to_string(),
            result: TaskResult::Pending,
            created_at: Utc::now(),
            started_at: None,
            completed_at: None,
            worker: None,
            progress: None,
            version: 0,
            tags: Vec::new(),
            metadata: std::collections::HashMap::new(),
            worker_hostname: None,
            runtime_ms: None,
            memory_bytes: None,
            retries: None,
            queue: None,
            ignored_error: None,
        }
    }

    // Helper to create a dummy backend for testing config/plumbing without
    // a live server.
    // Note: `connect_lazy()` does not actually connect to a server.
    fn create_dummy_backend() -> GrpcResultBackend {
        let channel =
            tonic::transport::Endpoint::from_static("http://localhost:50051").connect_lazy();
        GrpcResultBackend::from_channel(channel)
    }

    fn create_dummy_backend_with_config(config: GrpcConfig) -> GrpcResultBackend {
        let channel =
            tonic::transport::Endpoint::from_static("http://localhost:50051").connect_lazy();
        GrpcResultBackend::from_channel_with_config(channel, config)
    }

    // The four tests below construct a `GrpcResultBackend` via
    // `connect_lazy()`, which (through hyper-util's executor) requires an
    // active Tokio runtime even though nothing here performs I/O — hence
    // `#[tokio::test]` rather than plain `#[test]`.

    #[tokio::test]
    async fn test_default_backend_uses_default_config() {
        let backend = create_dummy_backend();
        assert_eq!(
            backend.config().request_timeout,
            config::DEFAULT_REQUEST_TIMEOUT
        );
        assert_eq!(
            backend.config().max_message_size,
            config::DEFAULT_MAX_MESSAGE_SIZE
        );
        assert!(backend.config().auth_token.is_none());
    }

    #[tokio::test]
    async fn test_from_channel_with_config_applies_config() {
        let config = GrpcConfig::new()
            .with_request_timeout(Duration::from_secs(3))
            .with_max_message_size(2048)
            .with_auth_token("tok");
        let backend = create_dummy_backend_with_config(config);

        assert_eq!(backend.config().request_timeout, Duration::from_secs(3));
        assert_eq!(backend.config().max_message_size, 2048);
        assert_eq!(backend.config().auth_token.as_deref(), Some("tok"));
    }

    #[tokio::test]
    async fn test_prepare_request_sets_grpc_timeout_header() {
        let backend = create_dummy_backend_with_config(
            GrpcConfig::new().with_request_timeout(Duration::from_secs(7)),
        );

        let request = backend
            .prepare_request(GetResultRequest {
                task_id: "x".to_string(),
            })
            .unwrap();

        // Matches tonic's own documented encoding for `Request::set_timeout`:
        // microseconds with a `u` suffix.
        assert_eq!(request.metadata().get("grpc-timeout").unwrap(), "7000000u");
    }

    #[tokio::test]
    async fn test_prepare_request_sets_auth_header_when_configured() {
        let backend = create_dummy_backend_with_config(GrpcConfig::new().with_auth_token("s3cr3t"));

        let request = backend
            .prepare_request(GetResultRequest {
                task_id: "x".to_string(),
            })
            .unwrap();

        assert_eq!(
            request
                .metadata()
                .get("authorization")
                .unwrap()
                .to_str()
                .unwrap(),
            "Bearer s3cr3t"
        );
    }

    #[tokio::test]
    async fn test_prepare_request_omits_auth_header_by_default() {
        let backend = create_dummy_backend();
        let request = backend
            .prepare_request(GetResultRequest {
                task_id: "x".to_string(),
            })
            .unwrap();
        assert!(request.metadata().get("authorization").is_none());
    }

    #[test]
    fn test_decide_retry_backs_off_on_unavailable() {
        let retry = RetryStrategy::new()
            .with_max_attempts(3)
            .with_initial_backoff(Duration::from_millis(50))
            .with_multiplier(2.0)
            .with_jitter(false);
        let status = tonic::Status::unavailable("server down");

        match decide_retry(&retry, 1, &status) {
            RetryDecision::Retry(backoff) => assert_eq!(backoff, Duration::from_millis(50)),
            RetryDecision::GiveUp(e) => panic!("expected a retry on attempt 1 of 3, got {e:?}"),
        }
    }

    #[test]
    fn test_decide_retry_retries_on_deadline_exceeded() {
        let retry = RetryStrategy::new().with_max_attempts(5);
        let status = tonic::Status::deadline_exceeded("too slow");
        match decide_retry(&retry, 1, &status) {
            RetryDecision::Retry(_) => {}
            RetryDecision::GiveUp(e) => panic!("DeadlineExceeded should be retried, got {e:?}"),
        }
    }

    #[test]
    fn test_decide_retry_gives_up_after_max_attempts() {
        let retry = RetryStrategy::new().with_max_attempts(2);
        let status = tonic::Status::unavailable("still down");

        match decide_retry(&retry, 2, &status) {
            RetryDecision::GiveUp(BackendError::Connection(msg)) => {
                assert!(msg.contains("gRPC error"));
            }
            other => panic!("expected GiveUp at attempt == max_attempts, got {other:?}"),
        }
    }

    #[test]
    fn test_decide_retry_never_retries_non_retryable_status() {
        let retry = RetryStrategy::new().with_max_attempts(10);
        let status = tonic::Status::invalid_argument("bad request");

        match decide_retry(&retry, 1, &status) {
            RetryDecision::GiveUp(_) => {}
            RetryDecision::Retry(_) => panic!("InvalidArgument must never be retried"),
        }
    }

    #[tokio::test]
    async fn test_proto_meta_pending_conversion() {
        let meta = create_test_meta();
        let task_id = meta.task_id;

        let proto_meta = codec::to_proto_meta(&meta).unwrap();
        assert_eq!(proto_meta.task_id, task_id.to_string());
        assert_eq!(proto_meta.task_name, "test_task");

        let converted = codec::from_proto_meta(proto_meta).unwrap();
        assert_eq!(converted.task_id, task_id);
        assert_eq!(converted.task_name, "test_task");
        assert!(matches!(converted.result, TaskResult::Pending));
    }

    /// Connecting to an address nothing is serving must fail, loudly.
    ///
    /// This used to be `#[ignore]`d "requires gRPC server running" and its
    /// whole body was `let _ = GrpcResultBackend::connect(..).await;` — it
    /// asserted nothing at all, in either direction, and could not have
    /// failed if `connect` had started returning `Ok` for an unreachable
    /// endpoint. The success path is covered for real (over a tonic wire,
    /// against an in-process server) by
    /// `server::tests::test_client_server_round_trip`; what was missing was
    /// the other half, which needs no server and therefore no `#[ignore]`.
    ///
    /// Port 1 on loopback is used deliberately: it is privileged, so nothing
    /// in a test environment binds it, and the connection is refused rather
    /// than left hanging.
    #[tokio::test]
    async fn test_grpc_backend_connect_fails_without_a_server() {
        let backend = GrpcResultBackend::connect("http://127.0.0.1:1").await;

        assert!(
            backend.is_err(),
            "connecting to an endpoint with no gRPC server must be an error, \
             not a handle that fails later on first use"
        );
    }
}
