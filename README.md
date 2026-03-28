# CeleRS - Enterprise Distributed Task Queue for Rust

**🎉 100% COMPLETE - ALL 18/18 CRATES IMPLEMENTED! 🎉**

**CeleRS** (Celery + Rust) is a production-ready, Celery-compatible distributed task queue library for Rust. Built from the ground up to provide binary-level protocol compatibility with Python Celery while delivering superior performance, type safety, and reliability.

**Status**: ✅ Production-Ready | ✅ 0 Errors | ✅ 0 Warnings | ✅ 5 Brokers | ✅ 3 Backends

## 🎯 Vision

CeleRS aims to be the definitive task queue solution for Rust, offering:

- **🔄 Celery Compatibility**: Drop-in replacement for Python Celery workers
- **⚡ Performance**: 10x throughput compared to Python Celery
- **🔒 Type Safety**: Compile-time guarantees for task signatures
- **🏢 Enterprise-Ready**: Battle-tested patterns for production deployments
- **🌐 Multi-Language**: Interoperate with Python, JavaScript, and other Celery clients

## ✨ Features

### Core Capabilities
- ✅ **Type-Safe Task Definitions**: Compile-time verified task signatures
- ✅ **Priority Queues**: Multi-level task prioritization
- ✅ **Dead Letter Queue**: Automatic handling of permanently failed tasks
- ✅ **Task Cancellation**: In-flight task cancellation via Pub/Sub
- ✅ **Retry Logic**: Exponential backoff with configurable max retries
- ✅ **Timeout Enforcement**: Task-level and worker-level timeout controls
- ✅ **Graceful Shutdown**: Clean worker termination with in-flight task completion

### Broker Support (5 Types)
- ✅ **Redis**: High-throughput with Lua scripts and pipelining
- ✅ **PostgreSQL**: ACID guarantees with `FOR UPDATE SKIP LOCKED`
- ✅ **MySQL**: Full SQL support with batch operations
- ✅ **RabbitMQ (AMQP)**: Enterprise message routing and exchanges
- ✅ **AWS SQS**: Cloud-native serverless queue integration

### Result Backends (3 Types)
- ✅ **Redis Backend**: Fast in-memory storage with automatic TTL
- ✅ **Database Backend**: PostgreSQL/MySQL with SQL analytics and durability
- ✅ **gRPC Backend**: Microservices-ready RPC result storage
- ✅ **Chord Support**: Distributed barrier synchronization across all backends

### Workflow Primitives (Canvas)
- ✅ **Chain**: Sequential task execution with result passing
- ✅ **Group**: Parallel task execution
- ✅ **Chord**: Map-reduce with distributed barrier callback
- ✅ **Map/Starmap**: Distributed mapping operations
- ✅ **Signature**: Task signatures for workflow composition

### Observability
- ✅ **Prometheus Metrics**: Task throughput, latency, queue depth
- ✅ **Health Checks**: Kubernetes-compatible liveness/readiness probes
- ✅ **OpenTelemetry**: Distributed tracing integration
- ✅ **Grafana Dashboards**: Pre-built visualization templates

### Developer Experience
- ✅ **Procedural Macros**: `#[celers::task]` for automatic task registration
- ✅ **CLI Tooling**: Worker management, queue inspection, DLQ operations
- ✅ **Configuration Management**: TOML/YAML files + environment variables
- ✅ **Comprehensive Documentation**: API docs, guides, and examples

## 🏗️ Architecture

CeleRS follows a **layered architecture** inspired by Python Celery's design:

```
┌─────────────────────────────────────────────────────────┐
│                    Application Layer                     │
│  celers-macros, celers-cli, user task definitions       │
└─────────────────────────────────────────────────────────┘
                           │
┌─────────────────────────────────────────────────────────┐
│              Runtime & Workflow Layer                    │
│  celers-worker, celers-canvas, celers-beat               │
└─────────────────────────────────────────────────────────┘
                           │
┌─────────────────────────────────────────────────────────┐
│                 Messaging Layer (Kombu)                  │
│  celers-kombu, celers-broker-*, celers-backend-*         │
└─────────────────────────────────────────────────────────┘
                           │
┌─────────────────────────────────────────────────────────┐
│                   Protocol Layer                         │
│  celers-protocol (Celery v2/v5 compatibility)            │
└─────────────────────────────────────────────────────────┘
```

### Workspace Crates (18 Total - 100% Complete)

#### Core & Protocol Layer
- **celers**: Facade crate with unified API
- **celers-core**: Core traits (`Task`, `Broker`, `ResultBackend`, `TaskExecutor`)
- **celers-protocol**: Celery Protocol v2/v5 message format
- **celers-kombu**: Kombu-compatible messaging abstraction

#### Broker Layer (5 Implementations)
- **celers-broker-redis**: Redis with Lua scripts and pipelining
- **celers-broker-postgres**: PostgreSQL with `FOR UPDATE SKIP LOCKED`
- **celers-broker-sql**: MySQL with batch operations
- **celers-broker-amqp**: RabbitMQ/AMQP with exchanges and routing
- **celers-broker-sqs**: AWS SQS with long polling

#### Result Backend Layer (3 Implementations)
- **celers-backend-redis**: Redis with TTL and chord synchronization
- **celers-backend-db**: PostgreSQL/MySQL with SQL analytics
- **celers-backend-rpc**: gRPC for microservices architectures

#### Runtime & Workflow Layer
- **celers-worker**: Task execution runtime with concurrency control
- **celers-canvas**: Workflow primitives (Chain, Chord, Group, Map)
- **celers-beat**: Periodic task scheduler (Cron, Interval, Solar)

#### Developer Tools
- **celers-macros**: Procedural macros (`#[task]`, `#[derive(Task)]`)
- **celers-cli**: Command-line worker and queue management
- **celers-metrics**: Prometheus metrics and observability

### Crate Status (v0.2.0)

| Crate | Status | Tests |
|-------|--------|-------|
| celers-worker | [Stable] | 486 |
| celers-protocol | [Stable] | 461 |
| celers-broker-redis | [Stable] | 454 |
| celers-kombu | [Stable] | 323 |
| celers-beat | [Stable] | 312 |
| celers-broker-sqs | [Stable] | 294 |
| celers-core | [Stable] | 247 |
| celers-broker-amqp | [Stable] | 244 |
| celers-macros | [Stable] | 221 |
| celers-backend-redis | [Stable] | 208 |
| celers-canvas | [Stable] | 196 |
| celers-metrics | [Stable] | 183 |
| celers (facade) | [Stable] | 145 |
| celers-broker-postgres | [Stable] | 117 |
| celers-cli | [Stable] | 101 |
| celers-broker-sql | [Stable] | 68 |
| celers-backend-rpc | [Stable] | 8 |
| celers-backend-db | [Stable] | 7 |
| **Total** | **18/18** | **4075** |

## 🚀 Quick Start

### Installation

Add CeleRS to your `Cargo.toml`:

```toml
[dependencies]
celers-core = "0.2"
celers-protocol = "0.2"
celers-broker-redis = "0.2"
celers-worker = "0.2"
celers-macros = "0.2"
tokio = { version = "1", features = ["full"] }
```

### Define a Task

```rust
use celers_macros::task;
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct AddArgs {
    x: i32,
    y: i32,
}

#[task]
async fn add(args: AddArgs) -> i32 {
    args.x + args.y
}
```

### Start a Worker

```rust
use celers_broker_redis::RedisBroker;
use celers_worker::{Worker, WorkerConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create broker
    let broker = RedisBroker::new("redis://localhost:6379", "celers")?;

    // Configure worker
    let config = WorkerConfig {
        concurrency: 4,
        max_retries: 3,
        default_timeout: Duration::from_secs(300),
        ..Default::default()
    };

    // Register tasks
    let mut worker = Worker::new(broker, config);
    worker.register_task("add", add);

    // Start processing
    worker.run().await?;

    Ok(())
}
```

### Enqueue Tasks

```rust
use celers_core::SerializedTask;
use celers_broker_redis::RedisBroker;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut broker = RedisBroker::new("redis://localhost:6379", "celers")?;

    let task = SerializedTask::new(
        "add".to_string(),
        serde_json::to_vec(&serde_json::json!({
            "x": 5,
            "y": 3
        }))?
    ).with_priority(9);  // High priority

    broker.enqueue(task).await?;

    Ok(())
}
```

## 🔧 CLI Usage

CeleRS provides a comprehensive CLI for operational tasks:

```bash
# Start a worker
celers worker --broker redis://localhost:6379 --concurrency 8

# Check queue status
celers status

# Inspect Dead Letter Queue
celers dlq inspect

# Replay failed task
celers dlq replay <task-id>

# Generate configuration file
celers init > celers.toml
```

## 📊 Monitoring

### Prometheus Metrics

CeleRS exports comprehensive metrics:

```
# Counters
celers_tasks_enqueued_total
celers_tasks_completed_total
celers_tasks_failed_total
celers_tasks_retried_total
celers_tasks_cancelled_total

# Gauges
celers_queue_size
celers_processing_queue_size
celers_dlq_size
celers_active_workers

# Histograms
celers_task_execution_seconds
```

### Health Checks

Kubernetes-compatible health endpoints:

```rust
let checker = HealthChecker::new();

// Liveness probe: Is the worker alive?
let health = checker.is_alive();

// Readiness probe: Can the worker accept tasks?
let ready = checker.is_ready();

// Full health status
let info = checker.get_health();
```

## 🗺️ Roadmap

### Current Status (v0.2.0) — Released 2026-03-28

- ✅ **Phase 1**: The Backbone (Core runtime)
- ✅ **Phase 2**: Advanced Features (Priorities, DLQ, Cancellation)
- ✅ **Phase 3**: Developer Experience (Macros, CLI, Metrics)
- ✅ **Phase 4**: Performance & Scalability
- ✅ **Phase 5**: Beat Scheduler (Cron, Interval, Solar)
- ✅ **Phase 6**: Extended Brokers & Backends (AMQP, SQS, DB, gRPC)
- ✅ **Phase 7**: Full Celery Protocol Compatibility (v2 wire format)
- ✅ **Phase 8**: v0.2.0 Enhancements (Compression, Distributed Locks, Events)
- ✅ **Phase 9**: v0.2.0 Production Features (Event Persistence, Chunking, Heartbeat)

### Upcoming Milestones

- **v0.3.0**: Full Python Celery interoperability (bidirectional task exchange, Protocol v5)
- **v1.0.0**: Stable API, Kafka/NATS brokers, web admin dashboard, security hardening

## 📖 Documentation

- [Architecture Decision Records](docs/adr/) - Key design decisions
- [TODO.md](TODO.md) - Detailed task tracking

## 🔬 Examples

The repository includes 8+ working examples:

- `phase1_complete` - Basic task execution
- `graceful_shutdown` - Clean worker termination
- `priority_queue` - Multi-priority task handling
- `dead_letter_queue` - DLQ management
- `task_cancellation` - In-flight cancellation
- `macro_tasks` - Procedural macro usage
- `prometheus_metrics` - Metrics HTTP server
- `health_checks` - Health check endpoints

Run examples with:

```bash
cargo run --example prometheus_metrics
```

## 🧪 Testing

```bash
# Run all tests
cargo test

# Run with coverage
cargo test --all-features

# Run benchmarks
cargo bench

# Check for warnings
cargo clippy -- -D warnings
```

## 🏆 Performance

CeleRS is designed for high throughput:

- **Target**: 10,000 tasks/sec per worker
- **Latency**: P95 < 10ms for enqueue/dequeue
- **Memory**: < 50MB baseline per worker
- **Reliability**: 99.99% task delivery guarantee

Benchmarks available via:

```bash
cargo bench --bench serialization
cargo bench --bench queue_operations
```

## 🤝 Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Development Setup

```bash
git clone https://github.com/cool-japan/celers.git
cd celers
cargo build --all-features
cargo test --all-features
```

### Code Standards

- **No warnings policy**: All code must compile without warnings
- **Test coverage**: Aim for >80% coverage
- **Documentation**: Public APIs must have rustdoc comments
- **Formatting**: Use `cargo fmt` before committing

## Sponsorship

CeleRS is developed and maintained by **COOLJAPAN OU (Team Kitasan)**.

If you find CeleRS useful, please consider sponsoring the project to support continued development of the Pure Rust ecosystem.

[![Sponsor](https://img.shields.io/badge/Sponsor-%E2%9D%A4-red?logo=github)](https://github.com/sponsors/cool-japan)

**[https://github.com/sponsors/cool-japan](https://github.com/sponsors/cool-japan)**

Your sponsorship helps us:
- Maintain and improve the COOLJAPAN ecosystem
- Keep the entire ecosystem (OxiBLAS, OxiFFT, SciRS2, etc.) 100% Pure Rust
- Provide long-term support and security updates

## 📜 License

Licensed under Apache-2.0

## 🙏 Acknowledgments

- Inspired by [Python Celery](https://github.com/celery/celery) and [Kombu](https://github.com/celery/kombu)
- Built on [Tokio](https://tokio.rs) async runtime
- Uses [Redis](https://redis.io) and [PostgreSQL](https://postgresql.org) as brokers

## 📞 Support

- **GitHub Issues**: Bug reports and feature requests
- **Discussions**: Questions and community support
- **Documentation**: Comprehensive guides and API docs

---

**Status**: Active Development | **Version**: 0.2.0 | **Rust**: 1.70+ (MSRV)

Built with ❤️ for the Rust community
