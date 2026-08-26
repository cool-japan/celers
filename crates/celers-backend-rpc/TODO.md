# celers-backend-rpc TODO

> gRPC/RPC result backend for CeleRS

**Version: 0.3.1 | Status: [Alpha] | Updated: 2026-08-26 | Tests: 63**

## Status: ✅ FEATURE COMPLETE — CLIENT + REFERENCE SERVER

Full gRPC result backend **client and reference server** for distributed microservices
architectures and service mesh deployments. `RpcBackendServer` (`src/server.rs`) closes what was
previously a client-only crate: it implements the generated `ResultBackendService` trait, wrapping
any `ResultBackend` (Redis, SQL, in-memory, ...) so `GrpcResultBackend::connect(...)` has something
to talk to without every user hand-rolling a server (including the chord counter's atomicity)
themselves. The client also gained per-call deadlines, connect timeouts, message-size limits,
bearer-token auth, and retry-with-backoff on transient failures (`src/config.rs`), and the wire
codec (`src/codec.rs`) now preserves sub-second timestamp precision and turns a corrupt
`result_data` payload into a decode error instead of a silent `Success(null)`.

## Completed Features

### Core gRPC Client ✅
- [x] `connect()` / `connect_with_config()` - Connect to gRPC result backend service
- [x] `from_channel()` / `from_channel_with_config()` - Create from existing gRPC channel
- [x] `GrpcConfig` (`src/config.rs`) - per-call deadline, connect timeout, message-size cap,
      bearer-token auth, retry policy — with sane defaults (30s/10s/16 MiB/none/3 attempts)
- [x] Automatic retry with exponential backoff on `Unavailable` / `DeadlineExceeded`
      (`decide_retry`, unit-tested independent of any network I/O)
- [x] Protocol buffer schema definition
- [x] Automatic code generation via `tonic-prost-build` (`build.rs`)
- [x] Type conversion (Rust ↔ Protobuf), factored into `src/codec.rs` and shared by the client and
      the reference server so they can't drift apart on wire semantics

### ResultBackend Implementation ✅
- [x] `store_result()` - Store task results via gRPC
- [x] `get_result()` - Retrieve task results via gRPC
- [x] `delete_result()` - Delete task results via gRPC
- [x] `set_expiration()` - Set TTL via gRPC
- [x] Error handling and mapping (tonic → BackendError)

### Batch Operations ✅
- [x] `store_results_batch()` - Batch store via default trait implementation
- [x] `get_results_batch()` - Batch get via default trait implementation
- [x] `delete_results_batch()` - Batch delete via default trait implementation
- [x] HTTP/2 multiplexing for concurrent requests

### Chord Synchronization ✅
- [x] `chord_init()` - Initialize chord state via gRPC
- [x] `chord_complete_task()` - Increment counter via gRPC
- [x] `chord_get_state()` - Retrieve chord state via gRPC
- [x] Proto message definitions for chord operations

### Protocol Buffers ✅
- [x] `result_backend.proto` - Full service definition
- [x] TaskMeta message with all fields
- [x] ChordState message with task list
- [x] Request/Response messages for all operations
- [x] TaskResultState enum (Pending/Started/Success/Failure/Revoked/Retry)

### Type Conversions ✅
- [x] `to_proto_meta()` - TaskMeta → proto::TaskMeta
- [x] `from_proto_meta()` - proto::TaskMeta → TaskMeta
- [x] `to_proto_chord()` - ChordState → proto::ChordState
- [x] `from_proto_chord()` - proto::ChordState → ChordState
- [x] UUID ↔ String conversion
- [x] DateTime ↔ Unix timestamp conversion
- [x] JSON ↔ String conversion for result data

## Protocol Buffer Schema

### Service Definition
```protobuf
service ResultBackendService {
  rpc StoreResult(StoreResultRequest) returns (StoreResultResponse);
  rpc GetResult(GetResultRequest) returns (GetResultResponse);
  rpc DeleteResult(DeleteResultRequest) returns (DeleteResultResponse);
  rpc SetExpiration(SetExpirationRequest) returns (SetExpirationResponse);
  rpc ChordInit(ChordInitRequest) returns (ChordInitResponse);
  rpc ChordCompleteTask(ChordCompleteTaskRequest) returns (ChordCompleteTaskResponse);
  rpc ChordGetState(ChordGetStateRequest) returns (ChordGetStateResponse);
}
```

### Message Types
- **TaskMeta:** Task metadata with result, timestamps, worker info
- **ChordState:** Chord synchronization state with task list
- **TaskResultState:** Enum for task states (6 variants)

### Data Type Mappings
| Rust Type | Protobuf Type | Notes |
|-----------|---------------|-------|
| Uuid | string | UUID as hyphenated string |
| DateTime<Utc> | int64 + uint32 | Unix timestamp (seconds) + nanosecond component, so sub-second precision survives the round trip instead of truncating to `:00` |
| serde_json::Value | string | JSON serialized to string; a present-but-corrupt string is a decode error, not a silent `Success(null)` |
| Option<T> | optional T | Proto3 optional fields |

## Usage Examples

### Basic Client Connection
```rust
use celers_backend_rpc::GrpcResultBackend;
use celers_backend_redis::{ResultBackend, TaskMeta, TaskResult};

// Connect to gRPC server
let mut backend = GrpcResultBackend::connect("http://localhost:50051").await?;

// Store result
let mut meta = TaskMeta::new(task_id, "my_task".to_string());
meta.result = TaskResult::Success(json!({"value": 42}));
backend.store_result(task_id, &meta).await?;

// Get result
let result = backend.get_result(task_id).await?;
println!("Result: {:?}", result);

// Set expiration (1 hour)
backend.set_expiration(task_id, Duration::from_secs(3600)).await?;
```

### With Custom gRPC Channel
```rust
use tonic::transport::Channel;

// Create custom channel with options
let channel = Channel::from_static("http://localhost:50051")
    .timeout(Duration::from_secs(5))
    .connect()
    .await?;

let mut backend = GrpcResultBackend::from_channel(channel);
```

### Chord Synchronization
```rust
use celers_backend_redis::ChordState;

// Initialize chord
let chord_state = ChordState {
    chord_id,
    total: 10,
    completed: 0,
    callback: Some("finalize_task".to_string()),
    task_ids: vec![task1_id, task2_id, ...],
};
backend.chord_init(chord_state).await?;

// When each task completes
let completed = backend.chord_complete_task(chord_id).await?;
if completed == 10 {
    // Trigger callback
}

// Check state
let state = backend.chord_get_state(chord_id).await?;
```

## Server Implementation ✅

`RpcBackendServer` (`src/server.rs`) implements the generated `ResultBackendService` trait,
delegating every RPC to any `ResultBackend` implementation (e.g. `RedisResultBackend`) wrapped in
a `tokio::sync::Mutex`:

```rust
use celers_backend_rpc::RpcBackendServer;
use celers_backend_redis::RedisResultBackend;

let backend = RedisResultBackend::new("redis://127.0.0.1/")?;
RpcBackendServer::serve("0.0.0.0:50051".parse()?, backend).await?;
```

- [x] `RpcBackendServer::new` / `into_service` — wrap a `ResultBackend`, build the tonic service
- [x] `RpcBackendServer::serve` / `serve_with_shutdown` — bind and run, with graceful shutdown
- [x] All seven RPCs implemented, delegating to the wrapped backend
- [x] gRPC status-code mapping (`NotFound` / `InvalidArgument` / `Unavailable` / `Internal`)
      instead of collapsing every failure to `internal`
- [x] Message-size limits applied on the server side too (not just the client)
- [x] In-process client-server round-trip test over a real local TCP listener
      (`server::tests::test_client_server_round_trip`), covering every RPC including the chord
      lifecycle — no external gRPC server needed to exercise this crate's own correctness
- [ ] `celers-rpc-server` standalone binary (CLI + config-file loading) — the library API above
      covers embedding a server in your own process; a standalone binary is a natural follow-up
      but isn't required to use this crate

## Architecture Use Cases

### 1. Microservices Architecture
```
[Worker 1] ──┐
[Worker 2] ──┼──> [gRPC Backend Service] ──> [Storage Backend]
[Worker 3] ──┘
```

Benefits:
- Centralized result storage
- Language-agnostic (any gRPC client)
- Load balancing across workers
- Service discovery integration

### 2. Service Mesh Deployment
```
[Worker] ──> [Envoy Sidecar] ──> [Backend Sidecar] ──> [Storage]
              ↓ TLS, Metrics, Tracing
```

Benefits:
- mTLS encryption
- Automatic retries
- Circuit breaking
- Observability (metrics, tracing)

### 3. Multi-Region Deployment
```
[Region A Workers] ──> [Regional gRPC Service A] ──┐
[Region B Workers] ──> [Regional gRPC Service B] ──┼──> [Global Storage]
[Region C Workers] ──> [Regional gRPC Service C] ──┘
```

Benefits:
- Low latency per region
- Geo-distributed result storage
- Fault isolation
- Regional caching

### 4. Hybrid Cloud
```
[On-Prem Workers] ──> [Cloud gRPC Service] ──> [Cloud Storage]
```

Benefits:
- Gradual cloud migration
- Data residency compliance
- Secure API gateway
- Bandwidth optimization

## gRPC Features

### Connection Management
- HTTP/2 multiplexing
- Connection pooling
- Automatic reconnection
- Keep-alive pings

### Performance
- Binary protocol (smaller than JSON)
- Streaming support (future)
- Header compression
- Flow control

### Security
- [x] Token-based auth (`GrpcConfig::with_auth_token`, sent as an `authorization: Bearer <token>`
      metadata header on every request)
- [ ] TLS/SSL encryption — not wired up via tonic's `tls-*` Cargo features, which pull in `ring`
      or `aws-lc-rs` (both violate this workspace's Pure-Rust policy; tonic 0.14 has no feature to
      select a pure-Rust crypto backend instead). Bring your own TLS-enabled `Channel` via
      `GrpcResultBackend::from_channel_with_config` in the meantime.
- [ ] mTLS authentication (blocked on the same TLS gap above)
- [ ] Channel credentials beyond the bearer-token metadata header

## Future Enhancements

### Streaming Support
- [ ] Server streaming for result watching
  ```protobuf
  rpc WatchResult(WatchResultRequest) returns (stream TaskMeta);
  ```
- [ ] Client streaming for batch operations
  ```protobuf
  rpc StoreBatch(stream StoreResultRequest) returns (StoreBatchResponse);
  ```
- [ ] Bidirectional streaming for real-time sync

### Advanced Features
- [ ] Result pagination (list all results)
- [ ] Query/filter results by criteria
- [ ] Result compression (Protocol Buffers encoding)
- [ ] Custom metadata propagation
- [ ] Deadline/timeout propagation

### Authentication
- [x] Bearer-token authentication (`GrpcConfig::with_auth_token`)
- [ ] JWT token authentication (validation/refresh logic — today's bearer token is opaque)
- [ ] OAuth2 integration
- [ ] mTLS client certificates (blocked on the TLS gap noted under Security above)

### Monitoring
- [x] Client-side metrics — `RpcMetrics` in `src/metrics.rs`: per-operation (`RpcOperation`, one of
      the 7 RPC methods) request/error counts and mean/p50/p95/p99 latency, exposed via
      `GrpcResultBackend::metrics()` (snapshot), `metrics_handle()` (shared `Arc` for e.g. a
      Prometheus exporter task), and `reset_metrics()`
- [ ] OpenTelemetry tracing
- [ ] Health check endpoint
- [ ] gRPC reflection for debugging

### Load Balancing
- [ ] Client-side load balancing
- [ ] Connection affinity
- [x] Retry policies — exponential backoff with jitter on `Unavailable` / `DeadlineExceeded`
      (`GrpcConfig::retry`, reusing `celers_backend_redis::retry::RetryStrategy`); non-retryable
      statuses (e.g. `InvalidArgument`, `NotFound`) fail immediately on the first attempt
- [ ] Circuit breaker integration

## Testing Status

- [x] Compilation tests
- [x] Unit tests (63 passing via `cargo nextest run --all-features`: type conversions including
      sub-second timestamp round-tripping and corrupt-payload rejection, chord operations,
      connection modes, retry-decision logic, request hardening, and the `RpcMetrics`
      ring-buffer/percentile logic), 1 skipped (`#[ignore]`d, requires a *live external* server)
- [x] Integration tests with mock server (`InMemoryBackend` in `server::tests`, exercised over a
      real local TCP listener via `RpcBackendServer` — not literally mocked at the gRPC layer)
- [x] Integration tests with a full client-server round trip
      (`server::tests::test_client_server_round_trip`: store/get/delete plus the full chord
      lifecycle, all over the network stack, not in-process function calls)
- [ ] Integration tests against a real external gRPC server on the default port (the `#[ignore]`d
      `test_grpc_backend_connection`; run manually with `cargo test -- --ignored` once one is up)
- [ ] Load testing
- [ ] Latency benchmarks

## Documentation

- [x] Module-level documentation
- [x] API documentation
- [x] Protocol buffer schema
- [x] Usage examples
- [ ] Server implementation guide
- [ ] Deployment guide (Kubernetes)
- [ ] Service mesh integration guide
- [ ] Performance tuning guide

## Dependencies

- `celers-backend-redis`: Trait definitions and types
- `tonic` / `tonic-prost`: gRPC client library
- `prost`: Protocol buffer serialization
- `prost-types`: Well-known protobuf types
- `tonic-prost-build`: Protobuf compilation (build-time, via `build.rs`)

## Comparison with Other Backends

| Feature | Redis | Database | gRPC |
|---------|-------|----------|------|
| **Latency** | ⚡ Ultra-fast | ⚠️ Moderate | ⚠️ Network dependent |
| **Durability** | ⚠️ Optional | ✅ High | ✅ Backend dependent |
| **Scalability** | ✅ Horizontal | ⚠️ Vertical | ✅ Horizontal |
| **Flexibility** | ❌ Limited | ✅ SQL | ✅ Full control |
| **Deployment** | ❌ Self-hosted | ❌ Self-hosted | ✅ Microservices |
| **Language** | ⚠️ Rust only | ⚠️ Rust only | ✅ Any gRPC client |
| **Service Mesh** | ❌ No | ❌ No | ✅ Native |
| **Authentication** | ⚠️ Basic | ⚠️ Database | ✅ Advanced |

## When to Use gRPC Backend

### ✅ Good Fit
- Microservices architecture
- Multi-language environment
- Service mesh deployment
- Kubernetes/cloud-native
- Need advanced auth/security
- Centralized result service
- Cross-region deployment

### ❌ Not Recommended
- Monolithic applications
- Single-process workers
- Need lowest latency possible
- Simple deployments
- No network infrastructure

## Example Deployment

### Kubernetes
```yaml
apiVersion: v1
kind: Service
metadata:
  name: result-backend
spec:
  selector:
    app: result-backend
  ports:
  - port: 50051
    targetPort: 50051
    name: grpc

---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: result-backend
spec:
  replicas: 3
  selector:
    matchLabels:
      app: result-backend
  template:
    metadata:
      labels:
        app: result-backend
    spec:
      containers:
      - name: backend
        image: my-result-backend:latest
        ports:
        - containerPort: 50051
```

### Envoy Configuration (Service Mesh)
```yaml
clusters:
- name: result_backend
  type: STRICT_DNS
  lb_policy: ROUND_ROBIN
  http2_protocol_options: {}
  load_assignment:
    cluster_name: result_backend
    endpoints:
    - lb_endpoints:
      - endpoint:
          address:
            socket_address:
              address: result-backend
              port_value: 50051
```

## Notes

- gRPC uses HTTP/2 (requires compatible infrastructure)
- Binary protocol is more efficient than JSON
- Reference server included (`RpcBackendServer`) — wraps any local `ResultBackend`, so you don't
  need to hand-roll the seven RPCs (or the chord counter's atomicity) yourself; see [Server
  Implementation](#server-implementation-) above
- Great for distributed/cloud deployments
- Can wrap any backend (Redis, DB, S3, etc.) behind `RpcBackendServer`
- Protocol Buffers ensure forward/backward compatibility (the new timestamp-nanos fields are
  additive: older peers that don't set them still decode correctly, at whole-second precision)
- Consider network latency in performance calculations
