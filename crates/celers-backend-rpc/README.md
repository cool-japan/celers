# celers-backend-rpc

**Version: 0.2.0 | Status: [Alpha] | Updated: 2026-03-27**

gRPC/RPC result backend for CeleRS. Enables remote task result storage and retrieval over gRPC, suitable for distributed microservices architectures and service mesh deployments.

## Features

- gRPC client for remote result storage via Tonic
- Protobuf-based wire format for efficient serialization
- Full `ResultBackend` trait implementation (store, get, delete, expire)
- Chord barrier synchronization over gRPC
- Service mesh and load balancer compatible
- Lazy and eager connection modes

## Usage

Add to your `Cargo.toml`:

```toml
[dependencies]
celers-backend-rpc = "0.2"
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

**8 tests passing** (unit tests; integration tests require a gRPC server)

## License

Apache-2.0

Copyright (c) COOLJAPAN OU (Team Kitasan)
