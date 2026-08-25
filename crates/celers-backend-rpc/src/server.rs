//! Reference gRPC server for [`ResultBackendService`].
//!
//! Before this module existed, `celers-backend-rpc` shipped a client
//! (`GrpcResultBackend`) and generated code for a server trait
//! (`result_backend_service_server::ResultBackendService`) that nothing in
//! the workspace implemented — `GrpcResultBackend::connect(...)` had
//! nothing to connect to. [`RpcBackendServer`] closes that gap: it wraps
//! any [`ResultBackend`] implementation (e.g. `RedisResultBackend`, a
//! Postgres/MySQL backend, or an in-memory backend for tests) and exposes
//! it over the network using the schema in `proto/result_backend.proto`.
//!
//! # Concurrency and chord atomicity
//!
//! The [`ResultBackend`] trait requires `&mut self` for every operation,
//! so a single backend instance cannot be shared across concurrent gRPC
//! calls without external synchronization. [`RpcBackendServer`] wraps the
//! backend in a `tokio::sync::Mutex`, serializing all eight RPCs through
//! one critical section per server instance. This is intentionally
//! coarse — every request, even on unrelated task/chord ids, waits for
//! the previous one to finish — but it is also *trivially correct*:
//! `chord_complete_task`'s read-increment-write sequence can never race
//! with itself or with any other operation on the same backend, which is
//! exactly the atomicity the chord barrier depends on. A deployment that
//! needs more throughput can run multiple `RpcBackendServer`s behind a
//! load balancer (each still individually correct), or point them all at
//! a backend whose own storage provides atomic read-increment-write (as
//! `RedisResultBackend` does via `INCR`) — that's a scaling optimization,
//! not a correctness requirement for this reference implementation.
//!
//! # Example
//!
//! ```no_run
//! use celers_backend_rpc::server::RpcBackendServer;
//! use celers_backend_redis::RedisResultBackend;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let backend = RedisResultBackend::new("redis://127.0.0.1/")?;
//! let addr = "0.0.0.0:50051".parse()?;
//! RpcBackendServer::serve(addr, backend).await?;
//! # Ok(())
//! # }
//! ```

use crate::codec;
use crate::config::DEFAULT_MAX_MESSAGE_SIZE;
use crate::proto::{
    self,
    result_backend_service_server::{ResultBackendService, ResultBackendServiceServer},
};
use async_trait::async_trait;
use celers_backend_redis::{BackendError, ResultBackend};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex as AsyncMutex;
use tonic::{Request, Response, Status};
use uuid::Uuid;

/// gRPC server implementing [`ResultBackendService`], backed by any
/// [`ResultBackend`] implementation.
///
/// See the [module docs](self) for the concurrency model.
pub struct RpcBackendServer<B: ResultBackend + 'static> {
    backend: Arc<AsyncMutex<B>>,
}

impl<B: ResultBackend + 'static> RpcBackendServer<B> {
    /// Wrap `backend` for serving over gRPC.
    pub fn new(backend: B) -> Self {
        Self {
            backend: Arc::new(AsyncMutex::new(backend)),
        }
    }

    /// Build the tonic service, ready to be registered on a
    /// `tonic::transport::Server` via `.add_service(...)`.
    ///
    /// Applies the same message-size limits as [`crate::GrpcConfig::default`]
    /// so a server built this way never silently falls back to tonic's
    /// undocumented 4 MiB decode default.
    pub fn into_service(self) -> ResultBackendServiceServer<Self> {
        ResultBackendServiceServer::new(self)
            .max_decoding_message_size(DEFAULT_MAX_MESSAGE_SIZE)
            .max_encoding_message_size(DEFAULT_MAX_MESSAGE_SIZE)
    }

    /// Serve `backend` at `addr` until the process is terminated.
    pub async fn serve(addr: SocketAddr, backend: B) -> Result<(), tonic::transport::Error> {
        Self::serve_with_shutdown(addr, backend, std::future::pending()).await
    }

    /// Serve `backend` at `addr` until `shutdown` resolves, then finish
    /// in-flight requests and return.
    pub async fn serve_with_shutdown<F>(
        addr: SocketAddr,
        backend: B,
        shutdown: F,
    ) -> Result<(), tonic::transport::Error>
    where
        F: std::future::Future<Output = ()>,
    {
        let server = Self::new(backend);
        tonic::transport::Server::builder()
            .add_service(server.into_service())
            .serve_with_shutdown(addr, shutdown)
            .await
    }
}

/// Parse a UUID path/id field, mapping a bad value to `InvalidArgument`
/// rather than an internal error.
fn parse_uuid(field: &str, s: &str) -> Result<Uuid, Status> {
    Uuid::parse_str(s).map_err(|e| Status::invalid_argument(format!("invalid {field}: {e}")))
}

/// Map a domain [`BackendError`] onto the gRPC status code that best
/// describes it, instead of collapsing every failure into `internal`.
fn to_status(err: BackendError) -> Status {
    match &err {
        BackendError::NotFound(_) => Status::not_found(err.to_string()),
        BackendError::Serialization(_) => Status::invalid_argument(err.to_string()),
        BackendError::Connection(_) => Status::unavailable(err.to_string()),
        BackendError::Redis(_) => Status::internal(err.to_string()),
    }
}

#[async_trait]
impl<B: ResultBackend + 'static> ResultBackendService for RpcBackendServer<B> {
    async fn store_result(
        &self,
        request: Request<proto::StoreResultRequest>,
    ) -> Result<Response<proto::StoreResultResponse>, Status> {
        let req = request.into_inner();
        let task_id = parse_uuid("task_id", &req.task_id)?;
        let proto_meta = req
            .meta
            .ok_or_else(|| Status::invalid_argument("meta is required"))?;
        let meta = codec::from_proto_meta(proto_meta).map_err(to_status)?;

        let mut backend = self.backend.lock().await;
        backend
            .store_result(task_id, &meta)
            .await
            .map_err(to_status)?;
        Ok(Response::new(proto::StoreResultResponse { success: true }))
    }

    async fn get_result(
        &self,
        request: Request<proto::GetResultRequest>,
    ) -> Result<Response<proto::GetResultResponse>, Status> {
        let req = request.into_inner();
        let task_id = parse_uuid("task_id", &req.task_id)?;

        let mut backend = self.backend.lock().await;
        let meta = backend.get_result(task_id).await.map_err(to_status)?;
        drop(backend);
        let meta = meta
            .map(|m| codec::to_proto_meta(&m))
            .transpose()
            .map_err(to_status)?;
        Ok(Response::new(proto::GetResultResponse { meta }))
    }

    async fn delete_result(
        &self,
        request: Request<proto::DeleteResultRequest>,
    ) -> Result<Response<proto::DeleteResultResponse>, Status> {
        let req = request.into_inner();
        let task_id = parse_uuid("task_id", &req.task_id)?;

        let mut backend = self.backend.lock().await;
        backend.delete_result(task_id).await.map_err(to_status)?;
        Ok(Response::new(proto::DeleteResultResponse { success: true }))
    }

    async fn set_expiration(
        &self,
        request: Request<proto::SetExpirationRequest>,
    ) -> Result<Response<proto::SetExpirationResponse>, Status> {
        let req = request.into_inner();
        let task_id = parse_uuid("task_id", &req.task_id)?;
        let ttl = Duration::from_secs(req.ttl_seconds);

        let mut backend = self.backend.lock().await;
        backend
            .set_expiration(task_id, ttl)
            .await
            .map_err(to_status)?;
        Ok(Response::new(proto::SetExpirationResponse {
            success: true,
        }))
    }

    async fn chord_init(
        &self,
        request: Request<proto::ChordInitRequest>,
    ) -> Result<Response<proto::ChordInitResponse>, Status> {
        let req = request.into_inner();
        let proto_state = req
            .state
            .ok_or_else(|| Status::invalid_argument("state is required"))?;
        let state = codec::from_proto_chord(proto_state).map_err(to_status)?;

        let mut backend = self.backend.lock().await;
        backend.chord_init(state).await.map_err(to_status)?;
        Ok(Response::new(proto::ChordInitResponse { success: true }))
    }

    async fn chord_update_state(
        &self,
        request: Request<proto::ChordUpdateStateRequest>,
    ) -> Result<Response<proto::ChordUpdateStateResponse>, Status> {
        let req = request.into_inner();
        let proto_state = req
            .state
            .ok_or_else(|| Status::invalid_argument("state is required"))?;
        let state = codec::from_proto_chord(proto_state).map_err(to_status)?;

        let mut backend = self.backend.lock().await;
        backend.chord_update_state(state).await.map_err(to_status)?;
        Ok(Response::new(proto::ChordUpdateStateResponse {
            success: true,
        }))
    }

    async fn chord_complete_task(
        &self,
        request: Request<proto::ChordCompleteTaskRequest>,
    ) -> Result<Response<proto::ChordCompleteTaskResponse>, Status> {
        let req = request.into_inner();
        let chord_id = parse_uuid("chord_id", &req.chord_id)?;

        let mut backend = self.backend.lock().await;
        let completed = backend
            .chord_complete_task(chord_id)
            .await
            .map_err(to_status)?;
        Ok(Response::new(proto::ChordCompleteTaskResponse {
            completed_count: u32::try_from(completed).unwrap_or(u32::MAX),
        }))
    }

    async fn chord_get_state(
        &self,
        request: Request<proto::ChordGetStateRequest>,
    ) -> Result<Response<proto::ChordGetStateResponse>, Status> {
        let req = request.into_inner();
        let chord_id = parse_uuid("chord_id", &req.chord_id)?;

        let mut backend = self.backend.lock().await;
        let state = backend.chord_get_state(chord_id).await.map_err(to_status)?;
        drop(backend);
        Ok(Response::new(proto::ChordGetStateResponse {
            state: state.map(|s| codec::to_proto_chord(&s)),
        }))
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    // `ResultBackend` is already in scope via `use super::*` (server.rs
    // imports it from `celers_backend_redis` at the top of this file); it
    // must be named (not glob-only) here because the test below invokes
    // its methods through fully-qualified `<GrpcResultBackend as
    // ResultBackend>::...` syntax.
    use crate::{ChordState, GrpcResultBackend, TaskMeta, TaskResult};
    use std::collections::HashMap;
    use tonic::transport::server::TcpIncoming;

    /// A minimal in-memory [`ResultBackend`], used only to exercise
    /// [`RpcBackendServer`] end-to-end without a real Redis/Postgres/MySQL
    /// instance. Not part of the crate's public API.
    #[derive(Default)]
    struct InMemoryBackend {
        results: HashMap<Uuid, TaskMeta>,
        chords: HashMap<Uuid, ChordState>,
    }

    #[async_trait]
    impl ResultBackend for InMemoryBackend {
        async fn store_result(
            &mut self,
            task_id: Uuid,
            meta: &TaskMeta,
        ) -> celers_backend_redis::Result<()> {
            self.results.insert(task_id, meta.clone());
            Ok(())
        }

        async fn get_result(
            &mut self,
            task_id: Uuid,
        ) -> celers_backend_redis::Result<Option<TaskMeta>> {
            Ok(self.results.get(&task_id).cloned())
        }

        async fn delete_result(&mut self, task_id: Uuid) -> celers_backend_redis::Result<()> {
            self.results.remove(&task_id);
            Ok(())
        }

        async fn set_expiration(
            &mut self,
            _task_id: Uuid,
            _ttl: Duration,
        ) -> celers_backend_redis::Result<()> {
            // No TTL eviction in this in-memory test double; accepted and
            // ignored, matching a backend where expiration is a no-op.
            Ok(())
        }

        async fn chord_init(&mut self, mut state: ChordState) -> celers_backend_redis::Result<()> {
            // Mirror `RedisResultBackend::chord_init`'s documented
            // create-or-reset contract (see `ResultBackend::chord_update_state`'s
            // doc comment): this always zeroes the completion counter,
            // regardless of what the caller's `state.completed` says. This
            // is deliberate, not an oversight — it is what makes
            // `test_chord_cancel_over_rpc_preserves_completed_count` below a
            // real regression test instead of a vacuous one: without it, a
            // buggy `chord_update_state` that silently falls back to
            // `chord_init` would look indistinguishable from a correct one.
            state.completed = 0;
            self.chords.insert(state.chord_id, state);
            Ok(())
        }

        async fn chord_update_state(
            &mut self,
            state: ChordState,
        ) -> celers_backend_redis::Result<()> {
            // Persists a state mutation (cancellation, callback change, ...)
            // WITHOUT resetting the completion counter, unlike `chord_init`.
            self.chords.insert(state.chord_id, state);
            Ok(())
        }

        async fn chord_complete_task(
            &mut self,
            chord_id: Uuid,
        ) -> celers_backend_redis::Result<usize> {
            let state = self
                .chords
                .get_mut(&chord_id)
                .ok_or(BackendError::NotFound(chord_id))?;
            state.completed += 1;
            Ok(state.completed)
        }

        async fn chord_get_state(
            &mut self,
            chord_id: Uuid,
        ) -> celers_backend_redis::Result<Option<ChordState>> {
            Ok(self.chords.get(&chord_id).cloned())
        }
    }

    /// Bind a loopback listener on an OS-assigned port synchronously
    /// (`TcpIncoming::bind` performs the actual `bind(2)`/`listen(2)`
    /// syscalls before returning, not lazily inside an async task), so a
    /// client can connect immediately after the server task is spawned
    /// with no risk of a bind-vs-connect race and no sleep needed.
    fn bind_loopback() -> (TcpIncoming, SocketAddr) {
        let incoming =
            TcpIncoming::bind("127.0.0.1:0".parse().unwrap()).expect("failed to bind test port");
        let addr = incoming.local_addr().expect("bound listener has an addr");
        (incoming, addr)
    }

    #[tokio::test]
    async fn test_client_server_round_trip() {
        let (incoming, addr) = bind_loopback();
        let server = RpcBackendServer::new(InMemoryBackend::default());
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        let server_task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(server.into_service())
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        let mut client = GrpcResultBackend::connect(&format!("http://{addr}"))
            .await
            .expect("client failed to connect to in-process server");

        let task_id = Uuid::new_v4();
        let mut meta = TaskMeta::new(task_id, "round_trip_task".to_string());
        meta.result = TaskResult::Success(serde_json::json!({"answer": 42}));
        meta.started_at = Some(utc_now_with_nanos());
        meta.completed_at = Some(utc_now_with_nanos());
        // Populate every extended field too: `store_result`/`get_result`
        // here go over a real tonic-encoded wire (unlike the in-process
        // `codec::to_proto_meta`/`from_proto_meta` unit tests), so this is
        // what actually proves `extra_json` (proto field 14) round-trips
        // through prost end to end, not just through the Rust functions
        // that build and parse it.
        meta.worker = Some("worker-9".to_string());
        meta.progress =
            Some(celers_backend_redis::ProgressInfo::new(1, 2).with_message("halfway".to_string()));
        meta.version = 3;
        meta.tags = vec!["wire".to_string(), "test".to_string()];
        meta.metadata
            .insert("k".to_string(), serde_json::json!("v"));
        meta.worker_hostname = Some("host-wire".to_string());
        meta.runtime_ms = Some(777);
        meta.memory_bytes = Some(2048);
        meta.retries = Some(1);
        meta.queue = Some("default".to_string());
        meta.ignored_error = Some("suppressed on the wire".to_string());

        // Nothing stored yet.
        assert!(
            <GrpcResultBackend as ResultBackend>::get_result(&mut client, task_id)
                .await
                .unwrap()
                .is_none()
        );

        // Store, then read back byte-for-byte (including sub-second
        // timestamps, the JSON payload, and every extended field) across
        // the real gRPC wire.
        <GrpcResultBackend as ResultBackend>::store_result(&mut client, task_id, &meta)
            .await
            .expect("store_result failed");

        let fetched = <GrpcResultBackend as ResultBackend>::get_result(&mut client, task_id)
            .await
            .expect("get_result failed")
            .expect("expected a stored result");
        assert_eq!(
            fetched, meta,
            "TaskMeta must round-trip byte-for-byte over the wire, extended fields included"
        );
        match &fetched.result {
            TaskResult::Success(v) => assert_eq!(v["answer"], 42),
            other => panic!("expected Success, got {other:?}"),
        }

        // Delete round-trips to None.
        <GrpcResultBackend as ResultBackend>::delete_result(&mut client, task_id)
            .await
            .expect("delete_result failed");
        assert!(
            <GrpcResultBackend as ResultBackend>::get_result(&mut client, task_id)
                .await
                .unwrap()
                .is_none()
        );

        // Chord lifecycle: init, complete twice, get state.
        let chord_id = Uuid::new_v4();
        let chord_state = ChordState::new(chord_id, 2, vec![Uuid::new_v4(), Uuid::new_v4()]);
        <GrpcResultBackend as ResultBackend>::chord_init(&mut client, chord_state)
            .await
            .expect("chord_init failed");

        let completed_1 =
            <GrpcResultBackend as ResultBackend>::chord_complete_task(&mut client, chord_id)
                .await
                .expect("chord_complete_task failed");
        assert_eq!(completed_1, 1);
        let completed_2 =
            <GrpcResultBackend as ResultBackend>::chord_complete_task(&mut client, chord_id)
                .await
                .expect("chord_complete_task failed");
        assert_eq!(completed_2, 2);

        let fetched_chord =
            <GrpcResultBackend as ResultBackend>::chord_get_state(&mut client, chord_id)
                .await
                .expect("chord_get_state failed")
                .expect("expected a stored chord state");
        assert_eq!(fetched_chord.chord_id, chord_id);
        assert_eq!(fetched_chord.total, 2);
        assert_eq!(fetched_chord.completed, 2);

        // A nonexistent chord id decodes to None, not an error.
        assert!(
            <GrpcResultBackend as ResultBackend>::chord_get_state(&mut client, Uuid::new_v4())
                .await
                .unwrap()
                .is_none()
        );

        let _ = shutdown_tx.send(());
        server_task
            .await
            .expect("server task panicked")
            .expect("server returned an error");
    }

    /// A compressed `result_data` must round-trip over the *real* gRPC
    /// wire, not just through the in-process `codec` unit tests: this
    /// proves prost actually encodes/decodes the new `bytes`/`string`
    /// optional fields correctly, and that the reference server's
    /// `from_proto_meta` decode (config-free, unconditional) correctly
    /// decompresses a request a compression-enabled client sent.
    #[tokio::test]
    async fn test_client_server_round_trip_with_compression_enabled() {
        let (incoming, addr) = bind_loopback();
        let server = RpcBackendServer::new(InMemoryBackend::default());
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        let server_task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(server.into_service())
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        let mut client = GrpcResultBackend::connect(&format!("http://{addr}"))
            .await
            .expect("client failed to connect to in-process server")
            .with_compression(crate::compression::CompressionConfig::new(16, "zstd"));

        let task_id = Uuid::new_v4();
        let large_items: Vec<serde_json::Value> = (0..512)
            .map(
                |i| serde_json::json!({"seq": i, "note": "same shape every time, compresses well"}),
            )
            .collect();
        let original = serde_json::json!({ "items": large_items });
        let mut meta = TaskMeta::new(task_id, "compressed_wire_round_trip".to_string());
        meta.result = TaskResult::Success(original.clone());

        <GrpcResultBackend as ResultBackend>::store_result(&mut client, task_id, &meta)
            .await
            .expect("store_result failed");

        let fetched = <GrpcResultBackend as ResultBackend>::get_result(&mut client, task_id)
            .await
            .expect("get_result failed")
            .expect("expected a stored result");
        match fetched.result {
            TaskResult::Success(v) => assert_eq!(
                v, original,
                "the large payload must decode back to the exact original value"
            ),
            other => panic!("expected Success, got {other:?}"),
        }

        let _ = shutdown_tx.send(());
        server_task
            .await
            .expect("server task panicked")
            .expect("server returned an error");
    }

    /// `TaskResultValue::Ignored` must survive the *real* gRPC wire through
    /// the same `celers_core::result::ResultStore` interface application
    /// code actually calls -- not just the pure `to_task_result`/
    /// `ignored_error`/`from_task_result` unit tests in `result_store.rs`,
    /// and not just the raw `TaskMeta::ignored_error` field covered by
    /// `test_client_server_round_trip` above. This exercises both in one
    /// pass: the `Ignored` <-> `Success(null)` + `ignored_error` marker
    /// projection, *and* prost actually carrying that marker through
    /// `extra_json` (proto field 14) end to end.
    #[tokio::test]
    async fn test_client_server_round_trip_ignored_result_via_result_store() {
        use celers_core::result::{ResultStore, TaskResultValue};

        let (incoming, addr) = bind_loopback();
        let server = RpcBackendServer::new(InMemoryBackend::default());
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        let server_task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(server.into_service())
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        let client = GrpcResultBackend::connect(&format!("http://{addr}"))
            .await
            .expect("client failed to connect to in-process server");

        let task_id = Uuid::new_v4();
        let ignored = TaskResultValue::Ignored {
            error: "task failed but ignore_errors was set".to_string(),
        };

        <GrpcResultBackend as ResultStore>::store_result(&client, task_id, ignored)
            .await
            .expect("store_result failed");

        let fetched = <GrpcResultBackend as ResultStore>::get_result(&client, task_id)
            .await
            .expect("get_result failed")
            .expect("expected a stored result");
        match fetched {
            TaskResultValue::Ignored { error } => assert_eq!(
                error, "task failed but ignore_errors was set",
                "the suppressed error text must survive the round trip through \
                 ResultStore -> TaskMeta -> gRPC wire -> TaskMeta -> ResultStore"
            ),
            other => panic!("expected Ignored, got {other:?}"),
        }

        // The wire-level projection this is built on: the low-level
        // `ResultBackend::get_result` (what a caller inspecting the raw
        // `TaskMeta` sees) must show the `Success(null)` + `ignored_error`
        // pair `to_task_result`/`ignored_error` write together -- not some
        // other encoding of "ignored".
        let mut raw_client = GrpcResultBackend::connect(&format!("http://{addr}"))
            .await
            .expect("second client failed to connect");
        let raw_meta = <GrpcResultBackend as ResultBackend>::get_result(&mut raw_client, task_id)
            .await
            .expect("get_result failed")
            .expect("expected a stored result");
        assert!(
            matches!(
                raw_meta.result,
                TaskResult::Success(serde_json::Value::Null)
            ),
            "an Ignored result's core `result` field must still project onto Success(null): {:?}",
            raw_meta.result
        );
        assert_eq!(
            raw_meta.ignored_error.as_deref(),
            Some("task failed but ignore_errors was set"),
            "the suppressed error must be carried in TaskMeta::ignored_error over the wire"
        );

        let _ = shutdown_tx.send(());
        server_task
            .await
            .expect("server task panicked")
            .expect("server returned an error");
    }

    /// Regression test: `ResultBackend::chord_cancel`'s default
    /// implementation reads the current state, mutates it locally
    /// (`cancelled = true`), then persists via `chord_update_state` — a
    /// method that must *not* reset the completion counter. Before
    /// `GrpcResultBackend` overrode `chord_update_state`, it fell through
    /// to the trait's own default, which delegates to `chord_init` — a
    /// create-or-reset primitive. Over the wire that meant cancelling a
    /// chord silently wiped out every task that had already completed.
    ///
    /// This only reproduces the bug because `InMemoryBackend::chord_init`
    /// (above) faithfully mirrors `RedisResultBackend`'s real "always
    /// zeroes the counter" behavior instead of a no-op passthrough — see
    /// its doc comment.
    #[tokio::test]
    async fn test_chord_cancel_over_rpc_preserves_completed_count() {
        let (incoming, addr) = bind_loopback();
        let server = RpcBackendServer::new(InMemoryBackend::default());
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        let server_task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(server.into_service())
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        let mut client = GrpcResultBackend::connect(&format!("http://{addr}"))
            .await
            .expect("client failed to connect to in-process server");

        let chord_id = Uuid::new_v4();
        let task_ids = vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        let state = ChordState::new(chord_id, 3, task_ids);
        <GrpcResultBackend as ResultBackend>::chord_init(&mut client, state)
            .await
            .expect("chord_init failed");

        // Two of the three tasks finish before anyone cancels.
        <GrpcResultBackend as ResultBackend>::chord_complete_task(&mut client, chord_id)
            .await
            .expect("chord_complete_task failed");
        let completed_before_cancel =
            <GrpcResultBackend as ResultBackend>::chord_complete_task(&mut client, chord_id)
                .await
                .expect("chord_complete_task failed");
        assert_eq!(completed_before_cancel, 2);

        // Cancelling must preserve the two already-completed tasks, not
        // reset the counter back to zero.
        <GrpcResultBackend as ResultBackend>::chord_cancel(
            &mut client,
            chord_id,
            Some("operator requested".to_string()),
        )
        .await
        .expect("chord_cancel failed");

        let fetched = <GrpcResultBackend as ResultBackend>::chord_get_state(&mut client, chord_id)
            .await
            .expect("chord_get_state failed")
            .expect("expected a stored chord state");
        assert!(fetched.cancelled, "chord must be marked cancelled");
        assert_eq!(
            fetched.cancellation_reason.as_deref(),
            Some("operator requested")
        );
        assert_eq!(
            fetched.completed, 2,
            "cancelling a chord must not reset already-completed tasks to 0"
        );

        let _ = shutdown_tx.send(());
        server_task
            .await
            .expect("server task panicked")
            .expect("server returned an error");
    }

    #[tokio::test]
    async fn test_invalid_task_id_maps_to_invalid_argument_status() {
        let (incoming, addr) = bind_loopback();
        let server = RpcBackendServer::new(InMemoryBackend::default());
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();

        let server_task = tokio::spawn(async move {
            tonic::transport::Server::builder()
                .add_service(server.into_service())
                .serve_with_incoming_shutdown(incoming, async {
                    let _ = shutdown_rx.await;
                })
                .await
        });

        let mut raw_client =
            proto::result_backend_service_client::ResultBackendServiceClient::connect(format!(
                "http://{addr}"
            ))
            .await
            .expect("failed to connect raw client");

        let status = raw_client
            .get_result(proto::GetResultRequest {
                task_id: "not-a-uuid".to_string(),
            })
            .await
            .expect_err("server must reject a non-UUID task_id");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);

        let _ = shutdown_tx.send(());
        server_task
            .await
            .expect("server task panicked")
            .expect("server returned an error");
    }

    /// The current second, pinned to a fixed non-zero nanosecond
    /// component so the test actually exercises sub-second round-tripping
    /// instead of only occasionally landing on `:00` by chance.
    fn utc_now_with_nanos() -> chrono::DateTime<chrono::Utc> {
        use chrono::TimeZone;
        chrono::Utc
            .timestamp_opt(chrono::Utc::now().timestamp(), 123_456_789)
            .unwrap()
    }
}
