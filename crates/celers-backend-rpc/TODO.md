# celers-backend-rpc TODO

> gRPC/RPC result backend for CeleRS

## Status: ✅ FEATURE COMPLETE

Full gRPC result backend client implementation for distributed microservices architectures and service mesh deployments.

## Completed Features

### Core gRPC Client ✅
- [x] `connect()` - Connect to gRPC result backend service
- [x] `from_channel()` - Create from existing gRPC channel
- [x] Protocol buffer schema definition
- [x] Automatic code generation via tonic-build
- [x] Type conversion (Rust ↔ Protobuf)

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
| DateTime<Utc> | int64 | Unix timestamp (seconds) |
| serde_json::Value | string | JSON serialized to string |
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

## Server Implementation (Future)

The server-side implementation is not included in this crate. To use this client, you need to implement a gRPC server that:

1. Implements the `ResultBackendService` from `result_backend.proto`
2. Stores results in your backend of choice (Redis, database, etc.)
3. Handles all RPC methods defined in the proto file

Example server skeleton (using tonic):
```rust
use tonic::{transport::Server, Request, Response, Status};

struct MyResultBackendService {
    // Your storage backend (Redis, DB, etc.)
}

#[tonic::async_trait]
impl ResultBackendService for MyResultBackendService {
    async fn store_result(
        &self,
        request: Request<StoreResultRequest>,
    ) -> Result<Response<StoreResultResponse>, Status> {
        // Implement storage logic
        Ok(Response::new(StoreResultResponse { success: true }))
    }

    // ... implement other methods
}
```

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
- TLS/SSL encryption
- mTLS authentication
- Token-based auth (metadata)
- Channel credentials

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
- [ ] JWT token authentication
- [ ] OAuth2 integration
- [ ] API key authentication
- [ ] mTLS client certificates

### Monitoring
- [ ] Prometheus metrics (request count, latency)
- [ ] OpenTelemetry tracing
- [ ] Health check endpoint
- [ ] gRPC reflection for debugging

### Load Balancing
- [ ] Client-side load balancing
- [ ] Connection affinity
- [ ] Retry policies
- [ ] Circuit breaker integration

## Testing Status

- [x] Compilation tests
- [x] Unit test structure
- [ ] Integration tests with mock server
- [ ] Integration tests with real server
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
- `tonic`: gRPC client library
- `prost`: Protocol buffer serialization
- `prost-types`: Well-known protobuf types
- `tonic-build`: Protobuf compilation (build-time)

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
- Server implementation required (not included)
- Great for distributed/cloud deployments
- Can wrap any backend (Redis, DB, S3, etc.)
- Protocol Buffers ensure forward/backward compatibility
- Consider network latency in performance calculations
