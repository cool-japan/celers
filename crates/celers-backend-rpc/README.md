# celers-backend-rpc

**Version: 0.3.1 | Status: [Alpha] | Tests: 49 (`--all-features`, excluding `#[ignore]`d) + 2 doctests | Updated: 2026-08-26**

gRPC/RPC result backend for CeleRS. Enables remote task result storage and retrieval over gRPC, suitable for distributed microservices architectures and service mesh deployments.

This crate ships **both halves** of the RPC boundary: a client (`GrpcResultBackend`) and a
reference server (`RpcBackendServer`) that implements the generated `ResultBackendService` gRPC
trait by delegating to any local `ResultBackend` (e.g. `RedisResultBackend`). You do not need to
write your own server to use this crate — see [Server](#server) below.

## Features

- gRPC client (`GrpcResultBackend`) and reference server (`RpcBackendServer`)
- Protobuf-based wire format for efficient serialization, with sub-second timestamp precision
- Full `ResultBackend` trait implementation (store, get, delete, expire)
- Chord barrier synchronization over gRPC, atomic on the reference server (single-mutex-per-server
  serialization — see the `server` module docs for the concurrency model and how to scale it)
- Per-call deadlines, connect timeouts, and message-size limits, all configurable via `GrpcConfig`
- Optional bearer-token authentication
- Automatic retry with exponential backoff for transient (`Unavailable` / `DeadlineExceeded`) failures
- Client-side metrics: per-operation request/error counts and p50/p95/p99 latency (`RpcMetrics`, new in v0.3.0)
- Service mesh and load balancer compatible
- Lazy and eager connection modes

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
celers-backend-rpc = "0.3"
```

### Connect and Store Results

```rust
use celers_backend_rpc::GrpcResultBackend;
use celers_backend_redis::{ResultBackend, TaskMeta, TaskResult};
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut backend = GrpcResultBackend::connect("http://localhost:50051").await?;

    let task_id = Uuid::new_v4();
    let mut meta = TaskMeta::new(task_id, "my_task".to_string());
    meta.result = TaskResult::Success(serde_json::json!({"value": 42}));

    backend.store_result(task_id, &meta).await?;

    if let Some(result) = backend.get_result(task_id).await? {
        println!("Task result: {:?}", result.result);
    }

    Ok(())
}
```

### Custom Channel

```rust
use celers_backend_rpc::GrpcResultBackend;
use tonic::transport::Endpoint;

let channel = Endpoint::from_static("http://localhost:50051").connect_lazy();
let backend = GrpcResultBackend::from_channel(channel);
```

### Client Hardening: Deadlines, Message Limits, Auth, Retries

`GrpcResultBackend::connect` uses sane defaults (30s request timeout, 10s connect timeout, 16 MiB
message cap, 3-attempt exponential-backoff retry on `Unavailable` / `DeadlineExceeded`, no auth).
Override any of them with `GrpcConfig`:

```rust
use celers_backend_rpc::{GrpcConfig, GrpcResultBackend, RetryStrategy};
use std::time::Duration;

let config = GrpcConfig::new()
    .with_request_timeout(Duration::from_secs(5))
    .with_max_message_size(4 * 1024 * 1024)
    .with_auth_token("my-bearer-token")
    .with_retry(RetryStrategy::new().with_max_attempts(5));

let backend = GrpcResultBackend::connect_with_config("http://localhost:50051", config).await?;
```

TLS is deliberately not wired up via tonic's built-in `tls-*` Cargo features (they pull in `ring`
or `aws-lc-rs`, both of which violate this workspace's Pure-Rust policy). To use TLS, build your
own `tonic::transport::Channel` and pass it to `GrpcResultBackend::from_channel_with_config`,
which still applies the deadline/message-size/auth hardening above on top of it. See the
`GrpcConfig` docs for details.

### Client-Side Metrics

Every `GrpcResultBackend` tracks per-operation request/error counts and latency percentiles
(p50/p95/p99, computed from a 1,000-sample ring buffer per operation):

```rust
let backend = GrpcResultBackend::connect("http://localhost:50051").await?;

// ... perform some store_result / get_result / chord_* calls ...

let snapshot = backend.metrics();
println!("total requests: {}", snapshot.total_requests);
println!("total errors: {}", snapshot.total_errors);

// Share a handle with e.g. a Prometheus exporter task, and reset counters if needed.
let handle = backend.metrics_handle();
backend.reset_metrics();
```

## Server

`RpcBackendServer` wraps any `ResultBackend` implementation and serves it over gRPC:

```rust
use celers_backend_rpc::RpcBackendServer;
use celers_backend_redis::RedisResultBackend;

let backend = RedisResultBackend::new("redis://127.0.0.1/")?;
let addr = "0.0.0.0:50051".parse()?;

// Runs until the process is killed.
RpcBackendServer::serve(addr, backend).await?;

// Or, with graceful shutdown:
// RpcBackendServer::serve_with_shutdown(addr, backend, shutdown_signal).await?;
```

The backend is wrapped in a `tokio::sync::Mutex`, so every RPC on a given server instance is
serialized — this is what makes `chord_complete_task`'s read-increment-write sequence atomic. See
the `server` module docs for the full concurrency model and how to scale beyond one server
instance.

## Supported Operations

| Operation | Method | Description |
|-----------|--------|-------------|
| Store | `store_result` | Persist task result to remote server |
| Get | `get_result` | Retrieve task result by ID |
| Delete | `delete_result` | Remove a stored result |
| Expire | `set_expiration` | Set TTL on a result |
| Chord Init | `chord_init` | Initialize chord barrier state |
| Chord Complete | `chord_complete_task` | Increment chord completion counter |
| Chord State | `chord_get_state` | Query current chord state |

## Part of CeleRS

This crate is part of the [CeleRS](https://github.com/cool-japan/celers) project, a Celery-compatible distributed task queue for Rust.

## Testing

**38 tests passing** (`cargo nextest run`; type conversions including sub-second timestamp
round-tripping and corrupt-payload rejection, chord operations, connection modes, retry-decision
logic, request hardening (deadlines/auth headers), the metrics ring-buffer/percentile logic, and
an in-process client-server round trip over a real local TCP listener exercising every RPC), **1
skipped** (marked `#[ignore]`, requires a *live external* gRPC server rather than the in-process
one already covered above).

## License

Apache-2.0

Copyright (c) COOLJAPAN OU (Team Kitasan)
