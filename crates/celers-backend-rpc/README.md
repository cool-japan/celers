# celers-backend-rpc

**Version: 0.3.1 | Status: [Alpha] | Tests: 18 passing, 1 skipped (requires a live gRPC server) | Updated: 2026-07-13**

gRPC/RPC result backend for CeleRS. Enables remote task result storage and retrieval over gRPC, suitable for distributed microservices architectures and service mesh deployments.

## Features

- gRPC client for remote result storage via Tonic
- Protobuf-based wire format for efficient serialization
- Full `ResultBackend` trait implementation (store, get, delete, expire)
- Chord barrier synchronization over gRPC
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

**18 tests passing** (`cargo nextest run`; type conversions, chord operations, connection modes,
and the metrics ring-buffer/percentile logic), **1 skipped** (marked `#[ignore]`, requires a live
gRPC server).

## License

Apache-2.0

Copyright (c) COOLJAPAN OU (Team Kitasan)
