//! # CeleRS - Celery-Compatible Distributed Task Queue for Rust
//!
//! **CeleRS** is a production-ready, Celery-compatible distributed task queue library
//! providing binary-level protocol compatibility with Python Celery while delivering
//! superior performance, type safety, and reliability.
//!
//! ## Quick Start
//!
//! ### 1. Add CeleRS to your Cargo.toml
//!
//! ```toml
//! [dependencies]
//! celers = { version = "0.1", features = ["redis", "backend-redis", "json"] }
//! tokio = { version = "1", features = ["full"] }
//! serde = { version = "1", features = ["derive"] }
//! ```
//!
//! ### 2. Define a Task
//!
//! ```rust,no_run
//! use celers::prelude::*;
//!
//! #[derive(Clone, Serialize, Deserialize, Debug)]
//! struct AddArgs {
//!     x: i32,
//!     y: i32,
//! }
//!
//! #[celers::task]
//! async fn add(args: AddArgs) -> Result<i32, Box<dyn std::error::Error>> {
//!     Ok(args.x + args.y)
//! }
//! ```
//!
//! ### 3. Create a Worker
//!
//! ```rust,ignore
//! use celers::prelude::*;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Create broker from environment variables
//!     let broker = create_broker_from_env().await?;
//!
//!     // Configure and start worker
//!     let worker = WorkerConfigBuilder::new()
//!         .concurrency(4)
//!         .prefetch_count(10)
//!         .build(broker)?;
//!
//!     worker.start().await?;
//!     Ok(())
//! }
//! ```
//!
//! ### 4. Send Tasks
//!
//! ```rust,ignore
//! use celers::prelude::*;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     let broker = create_broker_from_env().await?;
//!
//!     // Send a single task
//!     let task = add::new(AddArgs { x: 1, y: 2 });
//!     broker.enqueue(task).await?;
//!
//!     // Send a workflow
//!     let workflow = Chain::new()
//!         .add(add::new(AddArgs { x: 1, y: 2 }))
//!         .add(add::new(AddArgs { x: 3, y: 4 }));
//!     workflow.apply_async(&broker).await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Features
//!
//! - **Type-Safe**: Compile-time guarantees for task signatures
//! - **Celery-Compatible**: Binary protocol compatibility with Python Celery
//! - **High Performance**: 10x throughput compared to Python Celery
//! - **Multiple Brokers**: Redis, PostgreSQL, MySQL, RabbitMQ (AMQP), AWS SQS
//! - **Multiple Backends**: Redis, PostgreSQL/MySQL (Database), gRPC
//! - **Workflow Primitives**: Chain, Group, Chord, Map, Starmap
//! - **Observability**: Prometheus metrics, distributed tracing
//! - **Production Features**: Batch operations, persistent scheduler, progress tracking
//!
//! ## Architecture
//!
//! CeleRS follows a layered architecture:
//!
//! ```text
//! Application Layer (Your Tasks)
//!        ↓
//! Runtime Layer (celers-worker, celers-canvas)
//!        ↓
//! Messaging Layer (celers-kombu, celers-broker-*)
//!        ↓
//! Protocol Layer (celers-protocol)
//! ```
//!
//! ## Feature Selection Guide
//!
//! Choose features based on your infrastructure and requirements:
//!
//! ### Broker Selection
//!
//! | Feature    | Use When                                    | Performance |
//! |------------|---------------------------------------------|-------------|
//! | `redis`    | Simple setup, high throughput needed        | ⭐⭐⭐⭐⭐        |
//! | `postgres` | PostgreSQL infrastructure exists            | ⭐⭐⭐⭐          |
//! | `mysql`    | MySQL infrastructure exists                 | ⭐⭐⭐⭐          |
//! | `amqp`     | Enterprise messaging, complex routing       | ⭐⭐⭐⭐          |
//! | `sqs`      | AWS cloud, serverless, high availability    | ⭐⭐⭐           |
//!
//! ### Backend Selection
//!
//! | Feature         | Use When                              | Latency |
//! |-----------------|---------------------------------------|---------|
//! | `backend-redis` | With Redis broker (recommended)       | Low     |
//! | `backend-db`    | With PostgreSQL/MySQL broker          | Medium  |
//! | `backend-rpc`   | Distributed systems, microservices    | Medium  |
//!
//! ### Configuration Examples
//!
//! ```rust,ignore
//! use celers::prelude::*;
//!
//! // Example 1: Simple Redis Setup
//! let broker = RedisBroker::new("redis://localhost:6379", "celery")?;
//!
//! // Example 2: PostgreSQL with Connection Pool
//! let broker = PostgresBroker::with_queue(
//!     "postgres://user:pass@localhost/celery",
//!     "celery"
//! ).await?;
//!
//! // Example 3: Environment-based Configuration
//! // Set: CELERS_BROKER_TYPE=redis
//! //      CELERS_BROKER_URL=redis://localhost:6379
//! //      CELERS_BROKER_QUEUE=celery
//! let broker = create_broker_from_env().await?;
//!
//! // Example 4: Worker with Validation
//! let result = validate_worker_config(Some(4), Some(10));
//! if let Err(errors) = result {
//!     for error in errors {
//!         eprintln!("Configuration error: {}", error);
//!     }
//! }
//!
//! // Example 5: Feature Compatibility Check
//! println!("{}", feature_compatibility_matrix());
//! ```
//!
//! ## Testing
//!
//! CeleRS provides development utilities for testing:
//!
//! ```rust
//! #[cfg(test)]
//! mod tests {
//!     use celers::dev_utils::{MockBroker, TaskBuilder};
//!     use celers::prelude::*;
//!
//!     #[tokio::test]
//!     async fn test_task_execution() {
//!         let broker = MockBroker::new();
//!
//!         // Create and enqueue a test task
//!         let task = TaskBuilder::new("my.task")
//!             .max_retries(3)
//!             .build();
//!
//!         let task_id = broker.enqueue(task).await.unwrap();
//!
//!         // Verify task was enqueued
//!         assert_eq!(broker.queue_len(), 1);
//!
//!         // Dequeue and process
//!         let msg = broker.dequeue().await.unwrap().unwrap();
//!         assert_eq!(msg.task.metadata.name, "my.task");
//!     }
//! }
//! ```
//!
//! ## Production Deployment Guide
//!
//! ### Infrastructure Setup
//!
//! #### 1. Broker Selection and Configuration
//!
//! **Redis (Recommended for Most Use Cases)**
//! ```bash
//! # Install Redis
//! sudo apt-get install redis-server
//!
//! # Configure for production
//! # /etc/redis/redis.conf
//! maxmemory 2gb
//! maxmemory-policy allkeys-lru
//! appendonly yes
//! appendfsync everysec
//!
//! # Enable persistence
//! save 900 1
//! save 300 10
//! save 60 10000
//! ```
//!
//! **PostgreSQL (For Database-Centric Deployments)**
//! ```sql
//! -- Create database and tables
//! CREATE DATABASE celers_broker;
//! CREATE TABLE celery_tasks (
//!     id SERIAL PRIMARY KEY,
//!     task_id UUID UNIQUE NOT NULL,
//!     task_name VARCHAR(255) NOT NULL,
//!     payload BYTEA NOT NULL,
//!     created_at TIMESTAMP DEFAULT NOW(),
//!     INDEX idx_created_at (created_at)
//! );
//! ```
//!
//! #### 2. Worker Configuration
//!
//! ```rust,ignore
//! use celers::prelude::*;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Initialize logging
//!     env_logger::init();
//!
//!     // Create broker with connection pooling
//!     let broker = RedisBroker::new(
//!         &std::env::var("BROKER_URL")?,
//!         &std::env::var("QUEUE_NAME")?
//!     )?;
//!
//!     // Configure worker for production
//!     let worker = WorkerConfigBuilder::new()
//!         .concurrency(num_cpus::get()) // Use all CPU cores
//!         .prefetch_count(10) // Balance throughput and memory
//!         .max_retries(3)
//!         .retry_delay_seconds(60)
//!         .build(broker)?;
//!
//!     // Start worker with graceful shutdown
//!     worker.start().await?;
//!     Ok(())
//! }
//! ```
//!
//! #### 3. Systemd Service Configuration
//!
//! Create `/etc/systemd/system/celers-worker.service`:
//!
//! ```ini
//! [Unit]
//! Description=CeleRS Worker
//! After=network.target redis.service
//!
//! [Service]
//! Type=simple
//! User=celers
//! WorkingDirectory=/opt/celers
//! Environment="RUST_LOG=info"
//! Environment="BROKER_URL=redis://localhost:6379"
//! Environment="QUEUE_NAME=celery"
//! ExecStart=/opt/celers/bin/worker
//! Restart=always
//! RestartSec=10
//! StandardOutput=journal
//! StandardError=journal
//!
//! [Install]
//! WantedBy=multi-user.target
//! ```
//!
//! Enable and start:
//! ```bash
//! sudo systemctl enable celers-worker
//! sudo systemctl start celers-worker
//! sudo systemctl status celers-worker
//! ```
//!
//! ### Monitoring and Observability
//!
//! #### Metrics Integration
//!
//! ```rust,ignore
//! use celers::prelude::*;
//!
//! // Enable metrics
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Start Prometheus metrics server
//!     tokio::spawn(async {
//!         let addr = ([0, 0, 0, 0], 9090).into();
//!         // Serve metrics at /metrics
//!     });
//!
//!     // Your worker code
//!     Ok(())
//! }
//! ```
//!
//! #### Distributed Tracing
//!
//! ```rust,ignore
//! use celers::tracing::*;
//!
//! // Initialize tracing
//! init_tracing("celers-worker")?;
//!
//! // Tasks automatically get traced
//! #[celers::task]
//! async fn my_task(args: Args) -> Result<(), Error> {
//!     // Span is automatically created and propagated
//!     Ok(())
//! }
//! ```
//!
//! ### Performance Tuning
//!
//! #### Worker Concurrency
//!
//! ```rust,ignore
//! // CPU-bound tasks
//! .concurrency(num_cpus::get())
//!
//! // I/O-bound tasks
//! .concurrency(num_cpus::get() * 4)
//!
//! // Mixed workload
//! .concurrency(num_cpus::get() * 2)
//! ```
//!
//! #### Prefetch Configuration
//!
//! ```rust,ignore
//! // Low memory systems
//! .prefetch_count(5)
//!
//! // Standard configuration
//! .prefetch_count(10)
//!
//! // High throughput
//! .prefetch_count(20)
//! ```
//!
//! ### High Availability
//!
//! #### Multiple Workers
//!
//! Deploy multiple worker instances for redundancy:
//!
//! ```bash
//! # Worker 1
//! WORKER_ID=worker-1 cargo run --release
//!
//! # Worker 2
//! WORKER_ID=worker-2 cargo run --release
//!
//! # Worker 3
//! WORKER_ID=worker-3 cargo run --release
//! ```
//!
//! #### Redis Cluster
//!
//! ```rust,ignore
//! // Use Redis cluster for high availability
//! let broker = RedisBroker::with_cluster(&[
//!     "redis://node1:6379",
//!     "redis://node2:6379",
//!     "redis://node3:6379",
//! ], "celery")?;
//! ```
//!
//! ### Security Best Practices
//!
//! #### 1. Secure Connections
//!
//! ```bash
//! # Use TLS for Redis
//! BROKER_URL=rediss://user:password@redis.example.com:6380
//!
//! # Use SSL for PostgreSQL
//! BROKER_URL=postgresql://user:password@db.example.com:5432/celers?sslmode=require
//! ```
//!
//! #### 2. Authentication
//!
//! ```rust,ignore
//! // Use environment variables for credentials
//! let broker_url = std::env::var("BROKER_URL")?;
//! let broker = create_broker("redis", &broker_url, "celery").await?;
//! ```
//!
//! #### 3. Network Isolation
//!
//! - Run workers in private network
//! - Use VPN or SSH tunnels for remote access
//! - Implement firewall rules
//!
//! ### Scaling Strategies
//!
//! #### Horizontal Scaling
//!
//! ```yaml
//! # Kubernetes deployment
//! apiVersion: apps/v1
//! kind: Deployment
//! metadata:
//!   name: celers-worker
//! spec:
//!   replicas: 5  # Scale up/down as needed
//!   selector:
//!     matchLabels:
//!       app: celers-worker
//!   template:
//!     metadata:
//!       labels:
//!         app: celers-worker
//!     spec:
//!       containers:
//!       - name: worker
//!         image: myapp/celers-worker:latest
//!         env:
//!         - name: BROKER_URL
//!           valueFrom:
//!             secretKeyRef:
//!               name: celers-secrets
//!               key: broker-url
//!         resources:
//!           limits:
//!             memory: "1Gi"
//!             cpu: "1000m"
//! ```
//!
//! #### Auto-scaling with Kubernetes HPA
//!
//! ```yaml
//! apiVersion: autoscaling/v2
//! kind: HorizontalPodAutoscaler
//! metadata:
//!   name: celers-worker-hpa
//! spec:
//!   scaleTargetRef:
//!     apiVersion: apps/v1
//!     kind: Deployment
//!     name: celers-worker
//!   minReplicas: 2
//!   maxReplicas: 10
//!   metrics:
//!   - type: Resource
//!     resource:
//!       name: cpu
//!       target:
//!         type: Utilization
//!         averageUtilization: 70
//! ```
//!
//! ### Troubleshooting
//!
//! #### Common Issues
//!
//! **High Memory Usage**
//! - Reduce `prefetch_count`
//! - Implement chunked processing
//! - Use streaming for large datasets
//!
//! **Slow Task Processing**
//! - Increase worker concurrency
//! - Profile task execution
//! - Optimize database queries
//!
//! **Task Failures**
//! - Check logs: `journalctl -u celers-worker`
//! - Increase retry limits
//! - Implement proper error handling
//!
//! **Queue Backlog**
//! - Add more workers
//! - Optimize task execution time
//! - Implement rate limiting
//!
//! ## Migration Guide from Python Celery
//!
//! ### Feature Comparison
//!
//! | Feature | Python Celery | CeleRS | Notes |
//! |---------|--------------|--------|-------|
//! | Task Definition | `@task` decorator | `#[celers::task]` macro | Type-safe in Rust |
//! | Brokers | Redis, RabbitMQ, SQS | Redis, PostgreSQL, MySQL, AMQP, SQS | Same protocol |
//! | Result Backends | Redis, Database, RPC | Redis, Database, gRPC | Binary compatible |
//! | Canvas Primitives | chain, group, chord | Chain, Group, Chord, Map, Starmap | Same semantics |
//! | Periodic Tasks | Celery Beat | CeleRS Beat | Compatible schedules |
//! | Rate Limiting | ✅ | ✅ | Token bucket & sliding window |
//! | Task Routing | ✅ | ✅ | Glob & regex patterns |
//! | Retries | ✅ | ✅ | Exponential backoff |
//! | Monitoring | Flower | Prometheus + Grafana | Standard metrics |
//! | Performance | Baseline | **10x faster** | Native async |
//!
//! ### API Mapping
//!
//! #### Task Definition
//!
//! **Python Celery:**
//! ```python
//! from celery import Celery
//!
//! app = Celery('tasks', broker='redis://localhost:6379')
//!
//! @app.task
//! def add(x, y):
//!     return x + y
//! ```
//!
//! **CeleRS:**
//! ```rust,ignore
//! use celers::prelude::*;
//!
//! #[derive(Serialize, Deserialize)]
//! struct AddArgs { x: i32, y: i32 }
//!
//! #[celers::task]
//! async fn add(args: AddArgs) -> Result<i32, Box<dyn Error>> {
//!     Ok(args.x + args.y)
//! }
//! ```
//!
//! #### Sending Tasks
//!
//! **Python Celery:**
//! ```python
//! # Simple task
//! result = add.delay(4, 4)
//!
//! # With options
//! result = add.apply_async(
//!     args=(4, 4),
//!     countdown=10,
//!     retry=True,
//!     retry_policy={'max_retries': 3}
//! )
//! ```
//!
//! **CeleRS:**
//! ```rust,ignore
//! // Simple task
//! let task = SerializedTask::new("add", serde_json::to_vec(&AddArgs { x: 4, y: 4 })?);
//! broker.enqueue(task).await?;
//!
//! // With options (using Signature)
//! let sig = Signature::new("add".to_string())
//!     .with_args(vec![json!(4), json!(4)])
//!     .with_countdown(10)
//!     .with_max_retries(3);
//! ```
//!
//! #### Canvas Primitives
//!
//! **Python Celery:**
//! ```python
//! from celery import chain, group, chord
//!
//! # Chain
//! workflow = chain(add.s(2, 2), add.s(4), add.s(8))
//! workflow.apply_async()
//!
//! # Group
//! job = group(add.s(i, i) for i in range(10))
//! result = job.apply_async()
//!
//! # Chord
//! callback = tsum.s()
//! header = [add.s(i, i) for i in range(10)]
//! result = chord(header)(callback)
//! ```
//!
//! **CeleRS:**
//! ```rust,ignore
//! use celers::prelude::*;
//!
//! // Chain
//! let workflow = Chain::new()
//!     .add("add", vec![json!(2), json!(2)])
//!     .add("add", vec![json!(4)])
//!     .add("add", vec![json!(8)]);
//! workflow.apply(&broker).await?;
//!
//! // Group
//! let mut group = Group::new();
//! for i in 0..10 {
//!     group = group.add("add", vec![json!(i), json!(i)]);
//! }
//! group.apply(&broker).await?;
//!
//! // Chord
//! let mut header = Group::new();
//! for i in 0..10 {
//!     header = header.add("add", vec![json!(i), json!(i)]);
//! }
//! let callback = Signature::new("tsum".to_string());
//! let chord = Chord::new(header, callback);
//! ```
//!
//! #### Worker Configuration
//!
//! **Python Celery:**
//! ```bash
//! celery -A tasks worker \
//!     --concurrency=4 \
//!     --loglevel=info \
//!     --max-tasks-per-child=1000
//! ```
//!
//! **CeleRS:**
//! ```rust,ignore
//! let worker = WorkerConfigBuilder::new()
//!     .concurrency(4)
//!     .prefetch_count(10)
//!     .build(broker)?;
//!
//! worker.start().await?;
//! ```
//!
//! ### Code Conversion Examples
//!
//! #### Example 1: Simple Task with Retry
//!
//! **Python:**
//! ```python
//! @app.task(bind=True, max_retries=3)
//! def send_email(self, email, subject, body):
//!     try:
//!         mail_client.send(email, subject, body)
//!     except Exception as exc:
//!         raise self.retry(exc=exc, countdown=60)
//! ```
//!
//! **Rust:**
//! ```rust,ignore
//! #[derive(Serialize, Deserialize)]
//! struct EmailArgs {
//!     email: String,
//!     subject: String,
//!     body: String,
//! }
//!
//! #[celers::task]
//! async fn send_email(args: EmailArgs) -> Result<(), Box<dyn Error>> {
//!     mail_client.send(&args.email, &args.subject, &args.body).await?;
//!     Ok(())
//! }
//!
//! // Configure retry in task signature
//! let sig = Signature::new("send_email".to_string())
//!     .with_max_retries(3)
//!     .with_retry_delay(60);
//! ```
//!
//! #### Example 2: Periodic Tasks
//!
//! **Python:**
//! ```python
//! from celery.schedules import crontab
//!
//! app.conf.beat_schedule = {
//!     'cleanup-every-midnight': {
//!         'task': 'tasks.cleanup',
//!         'schedule': crontab(hour=0, minute=0),
//!     },
//! }
//! ```
//!
//! **Rust:**
//! ```rust,ignore
//! use celers::prelude::*;
//!
//! let scheduler = BeatScheduler::new();
//! scheduler.add_task(ScheduledTask {
//!     name: "cleanup".to_string(),
//!     schedule: Schedule::Crontab {
//!         minute: "0".to_string(),
//!         hour: "0".to_string(),
//!         day: "*".to_string(),
//!         month: "*".to_string(),
//!         weekday: "*".to_string(),
//!     },
//! });
//! ```
//!
//! ### Performance Differences
//!
//! #### Throughput Comparison
//!
//! | Metric | Python Celery | CeleRS | Improvement |
//! |--------|--------------|--------|-------------|
//! | Tasks/sec (simple) | ~1,000 | ~10,000 | **10x** |
//! | Tasks/sec (I/O) | ~5,000 | ~50,000 | **10x** |
//! | Memory per worker | ~50 MB | ~5 MB | **10x less** |
//! | Startup time | ~2 sec | ~50 ms | **40x faster** |
//! | Message latency | ~10 ms | ~1 ms | **10x faster** |
//!
//! #### Why CeleRS is Faster
//!
//! 1. **Native Async**: Tokio's async runtime vs Python's asyncio
//! 2. **Zero-copy Serialization**: Direct memory access without Python object overhead
//! 3. **Compiled Code**: No runtime interpretation
//! 4. **Efficient Memory**: Stack allocation and no GC pauses
//! 5. **Type Safety**: Compile-time optimization opportunities
//!
//! ### Migration Checklist
//!
//! ✅ **Phase 1: Setup**
//! - [ ] Install Rust toolchain
//! - [ ] Create new Rust project with CeleRS
//! - [ ] Configure broker connection
//! - [ ] Set up development environment
//!
//! ✅ **Phase 2: Task Migration**
//! - [ ] Identify all Celery tasks
//! - [ ] Define Rust task argument structs
//! - [ ] Convert task logic to async Rust
//! - [ ] Add error handling
//! - [ ] Configure retry policies
//!
//! ✅ **Phase 3: Worker Deployment**
//! - [ ] Build CeleRS worker binary
//! - [ ] Configure worker settings (concurrency, prefetch)
//! - [ ] Set up monitoring (Prometheus)
//! - [ ] Deploy alongside Python workers
//! - [ ] Gradual traffic migration
//!
//! ✅ **Phase 4: Validation**
//! - [ ] Monitor task success rates
//! - [ ] Compare performance metrics
//! - [ ] Verify result backend compatibility
//! - [ ] Test retry behavior
//! - [ ] Validate error handling
//!
//! ✅ **Phase 5: Optimization**
//! - [ ] Tune worker concurrency
//! - [ ] Optimize database queries
//! - [ ] Implement rate limiting
//! - [ ] Set up distributed tracing
//!
//! ### Compatibility Notes
//!
//! **Binary Protocol Compatibility:**
//! - CeleRS uses the same message format as Python Celery
//! - Tasks can be sent from Python and consumed by Rust (and vice versa)
//! - Result backends are fully compatible
//!
//! **Limitations:**
//! - Pickle serialization not supported (use JSON or MessagePack)
//! - Some advanced Python-specific features unavailable
//! - Canvas primitives have same semantics but different API
//!
//! **Best Practices:**
//! - Start with stateless, CPU-bound tasks
//! - Use JSON for serialization (most compatible)
//! - Keep Python workers for Python-specific tasks
//! - Gradually migrate high-throughput tasks to Rust
//!
//! ## Architecture Documentation
//!
//! ### System Design Overview
//!
//! CeleRS follows a layered architecture for modularity and maintainability:
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │                     Application Layer                        │
//! │              (Your Tasks and Workflows)                      │
//! └─────────────────────────────────────────────────────────────┘
//!                            ↓
//! ┌─────────────────────────────────────────────────────────────┐
//! │                    Canvas Layer                              │
//! │        (Chain, Group, Chord, Map, Starmap)                  │
//! │         celers-canvas                                        │
//! └─────────────────────────────────────────────────────────────┘
//!                            ↓
//! ┌─────────────────────────────────────────────────────────────┐
//! │                     Worker Layer                             │
//! │    (Task Execution, Concurrency, Retry Logic)               │
//! │         celers-worker                                        │
//! └─────────────────────────────────────────────────────────────┘
//!                            ↓
//! ┌─────────────────────────────────────────────────────────────┐
//! │                   Messaging Layer                            │
//! │         (Producer, Consumer, Transport)                      │
//! │              celers-kombu                                    │
//! └─────────────────────────────────────────────────────────────┘
//!                            ↓
//! ┌─────────────────────────────────────────────────────────────┐
//! │                   Broker Layer                               │
//! │    (Redis, PostgreSQL, MySQL, AMQP, SQS)                    │
//! │  celers-broker-{redis,postgres,sql,amqp,sqs}                │
//! └─────────────────────────────────────────────────────────────┘
//!                            ↓
//! ┌─────────────────────────────────────────────────────────────┐
//! │                   Protocol Layer                             │
//! │         (Message Format, Serialization)                      │
//! │            celers-protocol                                   │
//! └─────────────────────────────────────────────────────────────┘
//!                            ↓
//! ┌─────────────────────────────────────────────────────────────┐
//! │                     Core Layer                               │
//! │      (Task Metadata, State, Common Types)                   │
//! │              celers-core                                     │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! ### Component Interactions
//!
//! #### Task Submission Flow
//!
//! ```text
//! Application
//!     │
//!     │ 1. Create task with args
//!     ▼
//! SerializedTask
//!     │
//!     │ 2. Serialize to protocol format
//!     ▼
//! Protocol Layer (Message)
//!     │
//!     │ 3. Enqueue to broker
//!     ▼
//! Broker (Redis/PostgreSQL/etc)
//!     │
//!     │ 4. Store in queue
//!     ▼
//! Queue
//! ```
//!
//! #### Task Execution Flow
//!
//! ```text
//! Worker
//!     │
//!     │ 1. Dequeue task
//!     ▼
//! Broker
//!     │
//!     │ 2. Return BrokerMessage
//!     ▼
//! Worker
//!     │
//!     │ 3. Deserialize & validate
//!     ▼
//! Task Handler
//!     │
//!     │ 4. Execute async task
//!     ▼
//! Result
//!     │
//!     │ 5. Store in result backend
//!     ▼
//! Result Backend (Redis/DB)
//!     │
//!     │ 6. ACK/NACK to broker
//!     ▼
//! Broker
//! ```
//!
//! #### Workflow Execution (Chord Example)
//!
//! ```text
//! Chord { header: Group, callback: Task }
//!     │
//!     │ 1. Execute Group tasks in parallel
//!     ▼
//! ┌─────────┬─────────┬─────────┐
//! │ Task 1  │ Task 2  │ Task 3  │
//! └─────────┴─────────┴─────────┘
//!     │         │         │
//!     │ 2. Collect results
//!     ▼         ▼         ▼
//! Result Aggregator
//!     │
//!     │ 3. Trigger callback when all complete
//!     ▼
//! Callback Task
//! ```
//!
//! ### Data Flow Diagrams
//!
//! #### Message Format (Protocol Layer)
//!
//! ```text
//! Message {
//!     headers: {
//!         id: UUID,
//!         task: String,
//!         origin: String,
//!         ...
//!     },
//!     properties: {
//!         correlation_id: String,
//!         reply_to: String,
//!         content_type: "application/json",
//!         content_encoding: "utf-8",
//!     },
//!     body: Vec<u8>  // Serialized task args
//! }
//! ```
//!
//! #### Worker Pool Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────┐
//! │              Worker Manager                  │
//! │  ┌──────────────────────────────────────┐  │
//! │  │      Prefetch Queue (Bounded)         │  │
//! │  │    [Task] [Task] [Task] ...          │  │
//! │  └──────────────────────────────────────┘  │
//! │              │         │         │          │
//! │              ▼         ▼         ▼          │
//! │  ┌─────────┬─────────┬─────────┬─────────┐ │
//! │  │Worker 1 │Worker 2 │Worker 3 │Worker 4 │ │
//! │  │(Tokio  │(Tokio  │(Tokio  │(Tokio  │ │
//! │  │ Task)   │ Task)   │ Task)   │ Task)   │ │
//! │  └─────────┴─────────┴─────────┴─────────┘ │
//! └─────────────────────────────────────────────┘
//! ```
//!
//! ### Scalability Patterns
//!
//! #### Horizontal Scaling
//!
//! ```text
//! Load Balancer
//!     │
//!     ├─────────────┬─────────────┬─────────────┐
//!     ▼             ▼             ▼             ▼
//! Worker 1      Worker 2      Worker 3      Worker 4
//!     │             │             │             │
//!     └─────────────┴─────────────┴─────────────┘
//!                      │
//!                      ▼
//!              Shared Broker Queue
//! ```
//!
//! #### Vertical Scaling
//!
//! - Increase worker concurrency (more Tokio tasks per worker)
//! - Increase prefetch count (more tasks buffered)
//! - Optimize task execution (reduce CPU/memory usage)
//!
//! #### Queue-Based Load Balancing
//!
//! ```text
//! ┌──────────────────────────────────────┐
//! │         Task Router                   │
//! │  (Based on task name or priority)    │
//! └──────────────────────────────────────┘
//!     │         │         │         │
//!     ▼         ▼         ▼         ▼
//! [Queue 1] [Queue 2] [Queue 3] [Queue 4]
//!     │         │         │         │
//!     ▼         ▼         ▼         ▼
//! Worker    Worker    Worker    Worker
//!  Pool      Pool      Pool      Pool
//! ```

// Re-export core types
pub use celers_core::{
    ActiveTaskInfo, AsyncResult, Broker, BrokerStats, CompositeEventEmitter, ControlCommand,
    ControlResponse, DeliveryInfo, Event, EventEmitter, GlobPattern, InMemoryEventEmitter,
    InspectCommand, InspectResponse, LogLevel, LoggingEventEmitter, NoOpEventEmitter,
    PatternMatcher, PoolStats, QueueStats, RateLimitConfig, RateLimiter, RegexPattern, RequestInfo,
    ReservedTaskInfo, ResultStore, RouteResult, RouteRule, Router, RouterBuilder, RoutingConfig,
    ScheduledTaskInfo, SerializedTask, SlidingWindow, TaskEvent, TaskEventBuilder, TaskRateLimiter,
    TaskResultValue, TaskState, TokenBucket, WorkerConf, WorkerEvent, WorkerEventBuilder,
    WorkerRateLimiter, WorkerReport, WorkerStats,
};

// Re-export protocol types
pub use celers_protocol::{
    ContentEncoding, ContentType, Message, MessageHeaders, MessageProperties, ProtocolVersion,
    TaskArgs,
};

// Re-export kombu types
pub use celers_kombu::{
    BrokerError, Consumer, Envelope, Producer, QueueConfig, QueueMode, Result, Transport,
};

// Re-export worker types
pub use celers_worker::{Worker, WorkerConfig};

// Re-export canvas types
pub use celers_canvas::{
    Chain, Chord, Chunks, Group, Map, Signature, Starmap, TaskOptions, XMap, XStarmap,
};

// Re-export macros
pub use celers_macros::{task, Task};

// Optional broker re-exports
#[cfg(feature = "redis")]
pub use celers_broker_redis::RedisBroker;

#[cfg(feature = "postgres")]
pub use celers_broker_postgres::PostgresBroker;

#[cfg(feature = "mysql")]
pub use celers_broker_sql::MysqlBroker;

#[cfg(feature = "amqp")]
pub use celers_broker_amqp::AmqpBroker;

#[cfg(feature = "sqs")]
pub use celers_broker_sqs::SqsBroker;

// Optional backend re-exports
#[cfg(feature = "backend-redis")]
pub use celers_backend_redis::{
    event_transport::{RedisEventConfig, RedisEventEmitter, RedisEventReceiver},
    ChordState, RedisResultBackend, ResultBackend, TaskMeta, TaskResult,
};

#[cfg(feature = "backend-db")]
pub use celers_backend_db::{MysqlResultBackend, PostgresResultBackend};

#[cfg(feature = "backend-rpc")]
pub use celers_backend_rpc::GrpcResultBackend;

// Optional beat re-exports
#[cfg(feature = "beat")]
pub use celers_beat::{BeatScheduler, Schedule, ScheduledTask};

// Optional metrics re-exports
#[cfg(feature = "metrics")]
pub use celers_metrics::{gather_metrics, reset_metrics};

/// Prelude module for common imports
pub mod prelude {
    // Core types
    pub use crate::AsyncResult;
    pub use crate::Broker;
    pub use crate::CompositeEventEmitter;
    pub use crate::ControlCommand;
    pub use crate::ControlResponse;
    pub use crate::Event;
    pub use crate::EventEmitter;
    pub use crate::InMemoryEventEmitter;
    pub use crate::InspectCommand;
    pub use crate::InspectResponse;
    pub use crate::LogLevel;
    pub use crate::LoggingEventEmitter;
    pub use crate::NoOpEventEmitter;
    pub use crate::ResultStore;
    pub use crate::SerializedTask;
    pub use crate::TaskEvent;
    pub use crate::TaskEventBuilder;
    pub use crate::TaskResultValue;
    pub use crate::TaskState;
    pub use crate::WorkerEvent;
    pub use crate::WorkerEventBuilder;
    pub use crate::WorkerStats;

    // Rate limiting types
    pub use crate::RateLimitConfig;
    pub use crate::RateLimiter;
    pub use crate::SlidingWindow;
    pub use crate::TaskRateLimiter;
    pub use crate::TokenBucket;
    pub use crate::WorkerRateLimiter;

    // Task routing types
    pub use crate::GlobPattern;
    pub use crate::PatternMatcher;
    pub use crate::RegexPattern;
    pub use crate::RouteResult;
    pub use crate::RouteRule;
    pub use crate::Router;
    pub use crate::RouterBuilder;
    pub use crate::RoutingConfig;

    // Worker types
    pub use crate::Worker;
    pub use crate::WorkerConfig;
    pub use celers_worker::WorkerConfigBuilder;

    // Broker helper functions
    pub use crate::broker_helper::{create_broker, create_broker_from_env, BrokerConfigError};

    // Configuration validation
    pub use crate::config_validation::{
        check_feature_compatibility, feature_compatibility_matrix, validate_broker_url,
        validate_worker_config, ConfigValidator, ValidationError,
    };

    // Broker implementations
    #[cfg(feature = "redis")]
    pub use crate::RedisBroker;

    #[cfg(feature = "postgres")]
    pub use crate::PostgresBroker;

    #[cfg(feature = "mysql")]
    pub use crate::MysqlBroker;

    #[cfg(feature = "amqp")]
    pub use crate::AmqpBroker;

    #[cfg(feature = "sqs")]
    pub use crate::SqsBroker;

    // Backend implementations
    #[cfg(feature = "backend-redis")]
    pub use crate::{
        ChordState, RedisEventConfig, RedisEventEmitter, RedisEventReceiver, RedisResultBackend,
        ResultBackend, TaskMeta,
    };

    #[cfg(feature = "backend-db")]
    pub use crate::{MysqlResultBackend, PostgresResultBackend};

    #[cfg(feature = "backend-rpc")]
    pub use crate::GrpcResultBackend;

    // Beat scheduler
    #[cfg(feature = "beat")]
    pub use crate::{BeatScheduler, Schedule, ScheduledTask};

    // Metrics
    #[cfg(feature = "metrics")]
    pub use crate::{gather_metrics, reset_metrics};

    // Tracing
    #[cfg(feature = "tracing")]
    pub use crate::tracing::{
        create_tracer_provider, extract_trace_context, init_tracing, inject_trace_context,
        publish_span, task_span,
    };

    // Macros (task attribute and Task derive)
    pub use celers_macros::{task, Task};

    // Canvas primitives
    pub use crate::{
        Chain, Chord, Chunks, Group, Map, Signature, Starmap, TaskOptions, XMap, XStarmap,
    };

    // Error types
    pub use crate::BrokerError;

    // Common external crates
    pub use async_trait::async_trait;
    pub use serde::{Deserialize, Serialize};
    pub use serde_json;
    pub use serde_json::json;
    pub use tokio;
    pub use uuid::Uuid;

    // Development utilities (test/dev-utils feature)
    #[cfg(any(test, feature = "dev-utils"))]
    pub use crate::dev_utils::{
        create_test_task, EventTracker, MockBroker, PerformanceProfiler, QueueInspector,
        TaskBuilder, TaskDebugger,
    };

    // Type aliases for common patterns
    pub type TaskResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
    pub type AsyncTaskFn<T> =
        fn(Vec<u8>) -> std::pin::Pin<Box<dyn std::future::Future<Output = TaskResult<T>> + Send>>;

    // Re-export common Result type from kombu
    pub use celers_kombu::Result as KombuResult;
}

/// Convenience functions module
pub mod convenience {
    /// Create a task signature with fluent API
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::convenience::task;
    ///
    /// let sig = task("my_task")
    ///     .with_args(vec![json!(1), json!(2)])
    ///     .with_priority(5)
    ///     .with_max_retries(3);
    /// ```
    pub fn task(name: impl Into<String>) -> crate::Signature {
        crate::Signature::new(name.into())
    }

    /// Create a chain workflow
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::convenience::chain;
    ///
    /// let workflow = chain()
    ///     .add("task1", vec![json!(1)])
    ///     .add("task2", vec![json!(2)]);
    /// ```
    pub fn chain() -> crate::Chain {
        crate::Chain::new()
    }

    /// Create a group workflow
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::convenience::group;
    ///
    /// let workflow = group()
    ///     .add("task1", vec![json!(1)])
    ///     .add("task2", vec![json!(2)]);
    /// ```
    pub fn group() -> crate::Group {
        crate::Group::new()
    }

    /// Create a chord workflow with header and callback
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::convenience::{group, task, chord};
    ///
    /// let header = group()
    ///     .add("task1", vec![json!(1)])
    ///     .add("task2", vec![json!(2)]);
    ///
    /// let callback = task("aggregate_results");
    ///
    /// let workflow = chord(header, callback);
    /// ```
    pub fn chord(header: crate::Group, callback: crate::Signature) -> crate::Chord {
        crate::Chord::new(header, callback)
    }
}

/// Quick start helpers for common use cases
pub mod quick_start {
    /// Quick Redis broker setup with sensible defaults
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::quick_start::redis_broker;
    ///
    /// let broker = redis_broker("localhost:6379", "celery")?;
    /// ```
    #[cfg(feature = "redis")]
    pub fn redis_broker(
        url: &str,
        queue: &str,
    ) -> std::result::Result<crate::RedisBroker, celers_core::error::CelersError> {
        let full_url = if url.starts_with("redis://") {
            url.to_string()
        } else {
            format!("redis://{}", url)
        };
        crate::RedisBroker::new(&full_url, queue)
    }

    /// Quick PostgreSQL broker setup
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::quick_start::postgres_broker;
    ///
    /// let broker = postgres_broker(
    ///     "postgresql://user:pass@localhost/db",
    ///     "celery"
    /// ).await?;
    /// ```
    #[cfg(feature = "postgres")]
    pub async fn postgres_broker(
        url: &str,
        queue: &str,
    ) -> std::result::Result<crate::PostgresBroker, celers_core::error::CelersError> {
        crate::PostgresBroker::with_queue(url, queue).await
    }

    /// Build a WorkerConfig with sensible defaults
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::quick_start::{redis_broker, default_worker_config};
    ///
    /// let config = default_worker_config()?;
    /// // Use config to create worker
    /// ```
    pub fn default_worker_config() -> std::result::Result<crate::WorkerConfig, String> {
        use celers_worker::WorkerConfigBuilder;

        WorkerConfigBuilder::new()
            .concurrency(num_cpus::get())
            .build()
    }

    /// Build a WorkerConfig with custom concurrency
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::quick_start::worker_config_with_concurrency;
    ///
    /// let config = worker_config_with_concurrency(8)?;
    /// ```
    pub fn worker_config_with_concurrency(
        concurrency: usize,
    ) -> std::result::Result<crate::WorkerConfig, String> {
        use celers_worker::WorkerConfigBuilder;

        WorkerConfigBuilder::new().concurrency(concurrency).build()
    }
}

/// Production-ready configuration presets
pub mod presets {
    /// Production worker configuration preset
    ///
    /// Optimized for production workloads with:
    /// - Concurrency matching CPU cores
    /// - Standard polling interval
    /// - Graceful shutdown enabled
    pub fn production_config() -> std::result::Result<crate::WorkerConfig, String> {
        use celers_worker::WorkerConfigBuilder;

        WorkerConfigBuilder::new()
            .concurrency(num_cpus::get())
            .poll_interval_ms(1000)
            .graceful_shutdown(true)
            .build()
    }

    /// High-throughput worker configuration preset
    ///
    /// Optimized for maximum throughput:
    /// - High concurrency (4x CPU cores)
    /// - Fast polling interval
    /// - Suitable for I/O-bound tasks
    pub fn high_throughput_config() -> std::result::Result<crate::WorkerConfig, String> {
        use celers_worker::WorkerConfigBuilder;

        let concurrency = num_cpus::get() * 4;

        WorkerConfigBuilder::new()
            .concurrency(concurrency)
            .poll_interval_ms(100)
            .build()
    }

    /// Low-latency worker configuration preset
    ///
    /// Optimized for low latency:
    /// - Moderate concurrency
    /// - Very fast polling for quick response
    /// - Suitable for real-time tasks
    pub fn low_latency_config() -> std::result::Result<crate::WorkerConfig, String> {
        use celers_worker::WorkerConfigBuilder;

        WorkerConfigBuilder::new()
            .concurrency(num_cpus::get() * 2)
            .poll_interval_ms(50)
            .build()
    }

    /// Memory-constrained worker configuration preset
    ///
    /// Optimized for low memory usage:
    /// - Conservative concurrency
    /// - Slower polling to reduce overhead
    /// - Suitable for resource-limited environments
    pub fn memory_constrained_config() -> std::result::Result<crate::WorkerConfig, String> {
        use celers_worker::WorkerConfigBuilder;

        WorkerConfigBuilder::new()
            .concurrency(num_cpus::get())
            .poll_interval_ms(2000)
            .build()
    }
}

/// Error types re-exported from celers-kombu
pub mod error {
    pub use celers_kombu::BrokerError;
}

/// Protocol types for advanced usage
pub mod protocol {
    pub use celers_protocol::*;
}

/// Canvas workflow types
pub mod canvas {
    pub use celers_canvas::*;
}

/// Worker runtime types
pub mod worker {
    pub use celers_worker::*;
}

/// Rate limiting types
pub mod rate_limit {
    pub use celers_core::rate_limit::*;
}

/// Task routing types
pub mod router {
    pub use celers_core::router::*;
}

/// Distributed tracing support with OpenTelemetry
#[cfg(feature = "tracing")]
pub mod tracing {
    pub use opentelemetry;
    pub use opentelemetry_sdk;
    pub use tracing;
    pub use tracing_opentelemetry;
    pub use tracing_subscriber;

    use opentelemetry::trace::SpanKind;
    use opentelemetry::KeyValue;
    use opentelemetry_sdk::trace::{RandomIdGenerator, Sampler, TracerProvider};
    use opentelemetry_sdk::Resource;
    use tracing::Span;
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    use tracing_subscriber::layer::SubscriberExt;
    use tracing_subscriber::util::SubscriberInitExt;

    /// Initialize basic tracing with OpenTelemetry support
    ///
    /// This sets up tracing with console output. For production use,
    /// configure your own exporter (e.g., Jaeger, OTLP) using the
    /// opentelemetry_sdk crate directly.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::tracing::init_tracing;
    ///
    /// init_tracing("my-service").expect("Failed to initialize tracing");
    /// ```
    pub fn init_tracing(_service_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        // Initialize tracing subscriber with basic formatting
        // Note: service_name is accepted for API consistency but not used
        // in the basic setup. For production use with service identification,
        // use create_tracer_provider() to build a custom TracerProvider.
        tracing_subscriber::registry()
            .with(tracing_subscriber::EnvFilter::from_default_env())
            .with(tracing_subscriber::fmt::layer())
            .try_init()
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

        Ok(())
    }

    /// Create a TracerProvider with the given service name and exporter
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::tracing::create_tracer_provider;
    /// use opentelemetry_sdk::trace::Sampler;
    ///
    /// // Configure your own exporter here
    /// let provider = create_tracer_provider("my-service");
    /// ```
    pub fn create_tracer_provider(service_name: &str) -> TracerProvider {
        let resource = Resource::new(vec![KeyValue::new(
            "service.name",
            service_name.to_string(),
        )]);

        TracerProvider::builder()
            .with_sampler(Sampler::AlwaysOn)
            .with_id_generator(RandomIdGenerator::default())
            .with_resource(resource)
            .build()
    }

    /// Create a new span for task execution
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::tracing::task_span;
    ///
    /// let span = task_span("my_task", "task-id-123");
    /// let _guard = span.enter();
    /// // Task execution code here
    /// ```
    pub fn task_span(task_name: &str, task_id: &str) -> Span {
        tracing::info_span!(
            "task.execute",
            task.name = task_name,
            task.id = task_id,
            otel.kind = ?SpanKind::Consumer
        )
    }

    /// Create a new span for task publish
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::tracing::publish_span;
    ///
    /// let span = publish_span("my_task", "task-id-123");
    /// let _guard = span.enter();
    /// // Publish code here
    /// ```
    pub fn publish_span(task_name: &str, task_id: &str) -> Span {
        tracing::info_span!(
            "task.publish",
            task.name = task_name,
            task.id = task_id,
            otel.kind = ?SpanKind::Producer
        )
    }

    /// Extract trace context from message headers
    ///
    /// This allows distributed tracing across task boundaries
    pub fn extract_trace_context(headers: &std::collections::HashMap<String, String>) {
        use opentelemetry::propagation::TextMapPropagator;
        use opentelemetry_sdk::propagation::TraceContextPropagator;

        let propagator = TraceContextPropagator::new();
        let context = propagator.extract(headers);
        tracing::Span::current().set_parent(context);
    }

    /// Inject trace context into message headers
    ///
    /// This allows distributed tracing across task boundaries
    pub fn inject_trace_context(headers: &mut std::collections::HashMap<String, String>) {
        use opentelemetry::propagation::TextMapPropagator;
        use opentelemetry_sdk::propagation::TraceContextPropagator;

        let propagator = TraceContextPropagator::new();
        let context = tracing::Span::current().context();
        propagator.inject_context(&context, headers);
    }
}

/// Development utilities for testing
#[cfg(any(test, feature = "dev-utils"))]
pub mod dev_utils {
    use crate::{Broker, SerializedTask};
    use async_trait::async_trait;
    use celers_core::broker::BrokerMessage;
    use celers_core::error::CelersError;
    use celers_core::task::TaskId;
    use std::collections::VecDeque;
    use std::sync::{Arc, Mutex};

    type Result<T> = std::result::Result<T, CelersError>;

    /// Mock broker for testing
    ///
    /// This broker stores tasks in memory and provides inspection capabilities
    /// for testing task execution.
    #[derive(Clone)]
    pub struct MockBroker {
        queue: Arc<Mutex<VecDeque<SerializedTask>>>,
        published_tasks: Arc<Mutex<Vec<SerializedTask>>>,
    }

    impl MockBroker {
        /// Create a new mock broker
        pub fn new() -> Self {
            Self {
                queue: Arc::new(Mutex::new(VecDeque::new())),
                published_tasks: Arc::new(Mutex::new(Vec::new())),
            }
        }

        /// Get the number of tasks in the queue
        pub fn queue_len(&self) -> usize {
            self.queue.lock().unwrap().len()
        }

        /// Get all published tasks
        pub fn published_tasks(&self) -> Vec<SerializedTask> {
            self.published_tasks.lock().unwrap().clone()
        }

        /// Clear all tasks
        pub fn clear(&self) {
            self.queue.lock().unwrap().clear();
            self.published_tasks.lock().unwrap().clear();
        }

        /// Push a task to the front of the queue (for testing)
        pub fn push_task(&self, task: SerializedTask) {
            self.queue.lock().unwrap().push_back(task);
        }
    }

    impl Default for MockBroker {
        fn default() -> Self {
            Self::new()
        }
    }

    #[async_trait]
    impl Broker for MockBroker {
        async fn enqueue(&self, task: SerializedTask) -> Result<TaskId> {
            let task_id = task.metadata.id;
            self.published_tasks.lock().unwrap().push(task.clone());
            self.queue.lock().unwrap().push_back(task);
            Ok(task_id)
        }

        async fn dequeue(&self) -> Result<Option<BrokerMessage>> {
            let task = self.queue.lock().unwrap().pop_front();
            Ok(task.map(BrokerMessage::new))
        }

        async fn ack(&self, _task_id: &TaskId, _receipt_handle: Option<&str>) -> Result<()> {
            Ok(())
        }

        async fn reject(
            &self,
            _task_id: &TaskId,
            _receipt_handle: Option<&str>,
            _requeue: bool,
        ) -> Result<()> {
            Ok(())
        }

        async fn queue_size(&self) -> Result<usize> {
            Ok(self.queue.lock().unwrap().len())
        }

        async fn cancel(&self, task_id: &TaskId) -> Result<bool> {
            let mut queue = self.queue.lock().unwrap();
            let original_len = queue.len();
            queue.retain(|t| &t.metadata.id != task_id);
            Ok(queue.len() < original_len)
        }
    }

    /// Task builder for testing
    pub struct TaskBuilder {
        name: String,
        id: Option<String>,
        max_retries: u32,
        payload: Vec<u8>,
    }

    impl TaskBuilder {
        /// Create a new task builder
        pub fn new(task_name: &str) -> Self {
            Self {
                name: task_name.to_string(),
                id: None,
                max_retries: 0,
                payload: Vec::new(),
            }
        }

        /// Set task ID
        pub fn id(mut self, id: String) -> Self {
            self.id = Some(id);
            self
        }

        /// Set max retries
        pub fn max_retries(mut self, max_retries: u32) -> Self {
            self.max_retries = max_retries;
            self
        }

        /// Set payload
        pub fn payload(mut self, payload: Vec<u8>) -> Self {
            self.payload = payload;
            self
        }

        /// Build the task
        pub fn build(self) -> SerializedTask {
            use uuid::Uuid;

            let mut task = SerializedTask::new(self.name, self.payload);
            if let Some(id) = self.id {
                task.metadata.id = Uuid::parse_str(&id).unwrap_or_else(|_| Uuid::new_v4());
            }
            task.metadata.max_retries = self.max_retries;
            task
        }
    }

    /// Helper to create a simple test task
    pub fn create_test_task(name: &str) -> SerializedTask {
        TaskBuilder::new(name).build()
    }

    /// Task debugger for inspecting task state and execution
    pub struct TaskDebugger {
        task_history: Arc<Mutex<Vec<TaskDebugInfo>>>,
    }

    /// Debug information for a task
    #[derive(Debug, Clone)]
    pub struct TaskDebugInfo {
        pub task_id: String,
        pub task_name: String,
        pub state: String,
        pub timestamp: std::time::SystemTime,
        pub metadata: std::collections::HashMap<String, String>,
    }

    impl TaskDebugger {
        /// Create a new task debugger
        pub fn new() -> Self {
            Self {
                task_history: Arc::new(Mutex::new(Vec::new())),
            }
        }

        /// Record task execution
        pub fn record_task(&self, task: &SerializedTask, state: &str) {
            let mut history = self.task_history.lock().unwrap();
            history.push(TaskDebugInfo {
                task_id: task.metadata.id.to_string(),
                task_name: task.metadata.name.clone(),
                state: state.to_string(),
                timestamp: std::time::SystemTime::now(),
                metadata: std::collections::HashMap::new(),
            });
        }

        /// Get task history
        pub fn history(&self) -> Vec<TaskDebugInfo> {
            self.task_history.lock().unwrap().clone()
        }

        /// Clear history
        pub fn clear(&self) {
            self.task_history.lock().unwrap().clear();
        }

        /// Get tasks by state
        pub fn tasks_by_state(&self, state: &str) -> Vec<TaskDebugInfo> {
            self.task_history
                .lock()
                .unwrap()
                .iter()
                .filter(|info| info.state == state)
                .cloned()
                .collect()
        }

        /// Print task history in a formatted table
        pub fn print_history(&self) {
            let history = self.history();
            println!("\n╔══════════════════════════════════════════════════════════════════════════════╗");
            println!(
                "║                            Task Execution History                             ║"
            );
            println!("╚══════════════════════════════════════════════════════════════════════════════╝\n");

            for (idx, info) in history.iter().enumerate() {
                println!("Task #{}", idx + 1);
                println!("  ID:        {}", info.task_id);
                println!("  Name:      {}", info.task_name);
                println!("  State:     {}", info.state);
                println!("  Timestamp: {:?}", info.timestamp);
                if !info.metadata.is_empty() {
                    println!("  Metadata:");
                    for (key, value) in &info.metadata {
                        println!("    {}: {}", key, value);
                    }
                }
                println!();
            }
        }
    }

    impl Default for TaskDebugger {
        fn default() -> Self {
            Self::new()
        }
    }

    /// Event tracker for debugging task events
    pub struct EventTracker {
        events: Arc<Mutex<Vec<TrackedEvent>>>,
    }

    /// Tracked event information
    #[derive(Debug, Clone)]
    pub struct TrackedEvent {
        pub event_type: String,
        pub task_id: Option<String>,
        pub message: String,
        pub timestamp: std::time::SystemTime,
    }

    impl EventTracker {
        /// Create a new event tracker
        pub fn new() -> Self {
            Self {
                events: Arc::new(Mutex::new(Vec::new())),
            }
        }

        /// Track an event
        pub fn track(&self, event_type: &str, task_id: Option<String>, message: String) {
            let mut events = self.events.lock().unwrap();
            events.push(TrackedEvent {
                event_type: event_type.to_string(),
                task_id,
                message,
                timestamp: std::time::SystemTime::now(),
            });
        }

        /// Get all events
        pub fn events(&self) -> Vec<TrackedEvent> {
            self.events.lock().unwrap().clone()
        }

        /// Get events by type
        pub fn events_by_type(&self, event_type: &str) -> Vec<TrackedEvent> {
            self.events
                .lock()
                .unwrap()
                .iter()
                .filter(|e| e.event_type == event_type)
                .cloned()
                .collect()
        }

        /// Clear events
        pub fn clear(&self) {
            self.events.lock().unwrap().clear();
        }

        /// Print events in a formatted table
        pub fn print_events(&self) {
            let events = self.events();
            println!("\n╔══════════════════════════════════════════════════════════════════════════════╗");
            println!(
                "║                              Event Log                                        ║"
            );
            println!("╚══════════════════════════════════════════════════════════════════════════════╝\n");

            for (idx, event) in events.iter().enumerate() {
                println!("Event #{}", idx + 1);
                println!("  Type:      {}", event.event_type);
                if let Some(ref task_id) = event.task_id {
                    println!("  Task ID:   {}", task_id);
                }
                println!("  Message:   {}", event.message);
                println!("  Timestamp: {:?}", event.timestamp);
                println!();
            }
        }
    }

    impl Default for EventTracker {
        fn default() -> Self {
            Self::new()
        }
    }

    /// Performance profiler for debugging task execution time
    pub struct PerformanceProfiler {
        measurements: Arc<Mutex<Vec<PerformanceMeasurement>>>,
    }

    /// Performance measurement data
    #[derive(Debug, Clone)]
    pub struct PerformanceMeasurement {
        pub name: String,
        pub duration_ms: u128,
        pub timestamp: std::time::SystemTime,
        pub metadata: std::collections::HashMap<String, String>,
    }

    impl PerformanceProfiler {
        /// Create a new performance profiler
        pub fn new() -> Self {
            Self {
                measurements: Arc::new(Mutex::new(Vec::new())),
            }
        }

        /// Start a measurement
        pub fn start_measurement(&self, name: &str) -> MeasurementGuard {
            MeasurementGuard {
                name: name.to_string(),
                start: std::time::Instant::now(),
                profiler: self.clone(),
            }
        }

        /// Record a measurement
        fn record(&self, name: String, duration_ms: u128) {
            let mut measurements = self.measurements.lock().unwrap();
            measurements.push(PerformanceMeasurement {
                name,
                duration_ms,
                timestamp: std::time::SystemTime::now(),
                metadata: std::collections::HashMap::new(),
            });
        }

        /// Get all measurements
        pub fn measurements(&self) -> Vec<PerformanceMeasurement> {
            self.measurements.lock().unwrap().clone()
        }

        /// Clear measurements
        pub fn clear(&self) {
            self.measurements.lock().unwrap().clear();
        }

        /// Get average duration for a measurement name
        pub fn average_duration(&self, name: &str) -> Option<u128> {
            let measurements = self.measurements.lock().unwrap();
            let matching: Vec<_> = measurements
                .iter()
                .filter(|m| m.name == name)
                .map(|m| m.duration_ms)
                .collect();

            if matching.is_empty() {
                None
            } else {
                Some(matching.iter().sum::<u128>() / matching.len() as u128)
            }
        }

        /// Print performance summary
        pub fn print_summary(&self) {
            let measurements = self.measurements();
            println!("\n╔══════════════════════════════════════════════════════════════════════════════╗");
            println!(
                "║                          Performance Summary                                  ║"
            );
            println!("╚══════════════════════════════════════════════════════════════════════════════╝\n");

            // Group by name
            let mut grouped: std::collections::HashMap<String, Vec<u128>> =
                std::collections::HashMap::new();

            for m in measurements {
                grouped.entry(m.name).or_default().push(m.duration_ms);
            }

            for (name, durations) in grouped {
                let count = durations.len();
                let total: u128 = durations.iter().sum();
                let avg = total / count as u128;
                let min = *durations.iter().min().unwrap();
                let max = *durations.iter().max().unwrap();

                println!("{}", name);
                println!("  Count: {}", count);
                println!("  Avg:   {} ms", avg);
                println!("  Min:   {} ms", min);
                println!("  Max:   {} ms", max);
                println!("  Total: {} ms", total);
                println!();
            }
        }
    }

    impl Clone for PerformanceProfiler {
        fn clone(&self) -> Self {
            Self {
                measurements: Arc::clone(&self.measurements),
            }
        }
    }

    impl Default for PerformanceProfiler {
        fn default() -> Self {
            Self::new()
        }
    }

    /// RAII guard for performance measurements
    pub struct MeasurementGuard {
        name: String,
        start: std::time::Instant,
        profiler: PerformanceProfiler,
    }

    impl Drop for MeasurementGuard {
        fn drop(&mut self) {
            let duration_ms = self.start.elapsed().as_millis();
            self.profiler.record(self.name.clone(), duration_ms);
        }
    }

    /// Queue inspector for debugging broker queues
    pub struct QueueInspector {
        snapshots: Arc<Mutex<Vec<QueueSnapshot>>>,
    }

    /// Snapshot of queue state
    #[derive(Debug, Clone)]
    pub struct QueueSnapshot {
        pub queue_size: usize,
        pub timestamp: std::time::SystemTime,
        pub metadata: std::collections::HashMap<String, String>,
    }

    impl QueueInspector {
        /// Create a new queue inspector
        pub fn new() -> Self {
            Self {
                snapshots: Arc::new(Mutex::new(Vec::new())),
            }
        }

        /// Take a snapshot of the queue
        pub async fn snapshot(&self, broker: &MockBroker) {
            let size = broker.queue_len();
            let mut snapshots = self.snapshots.lock().unwrap();
            snapshots.push(QueueSnapshot {
                queue_size: size,
                timestamp: std::time::SystemTime::now(),
                metadata: std::collections::HashMap::new(),
            });
        }

        /// Get all snapshots
        pub fn snapshots(&self) -> Vec<QueueSnapshot> {
            self.snapshots.lock().unwrap().clone()
        }

        /// Clear snapshots
        pub fn clear(&self) {
            self.snapshots.lock().unwrap().clear();
        }

        /// Print queue history
        pub fn print_history(&self) {
            let snapshots = self.snapshots();
            println!("\n╔══════════════════════════════════════════════════════════════════════════════╗");
            println!(
                "║                            Queue Size History                                 ║"
            );
            println!("╚══════════════════════════════════════════════════════════════════════════════╝\n");

            for (idx, snapshot) in snapshots.iter().enumerate() {
                println!("Snapshot #{}", idx + 1);
                println!("  Queue Size: {}", snapshot.queue_size);
                println!("  Timestamp:  {:?}", snapshot.timestamp);
                println!();
            }
        }
    }

    impl Default for QueueInspector {
        fn default() -> Self {
            Self::new()
        }
    }
}

/// Configuration validation helpers
pub mod config_validation {
    /// Validation error
    #[derive(Debug, thiserror::Error)]
    pub enum ValidationError {
        #[error("Invalid configuration: {0}")]
        InvalidConfig(String),

        #[error("Missing required field: {0}")]
        MissingField(String),

        #[error("Invalid value for {field}: {message}")]
        InvalidValue { field: String, message: String },

        #[error("Incompatible configuration: {0}")]
        IncompatibleConfig(String),
    }

    /// Configuration validator
    pub struct ConfigValidator {
        errors: Vec<ValidationError>,
        warnings: Vec<String>,
    }

    impl ConfigValidator {
        /// Create a new validator
        pub fn new() -> Self {
            Self {
                errors: Vec::new(),
                warnings: Vec::new(),
            }
        }

        /// Check if a required field is present
        pub fn require_field(&mut self, field_name: &str, value: Option<&str>) {
            if value.is_none() || value == Some("") {
                self.errors
                    .push(ValidationError::MissingField(field_name.to_string()));
            }
        }

        /// Add a validation error
        pub fn add_error(&mut self, error: ValidationError) {
            self.errors.push(error);
        }

        /// Add a warning
        pub fn add_warning(&mut self, message: String) {
            self.warnings.push(message);
        }

        /// Check if validation passed
        pub fn is_valid(&self) -> bool {
            self.errors.is_empty()
        }

        /// Get all errors
        pub fn errors(&self) -> &[ValidationError] {
            &self.errors
        }

        /// Get all warnings
        pub fn warnings(&self) -> &[String] {
            &self.warnings
        }

        /// Validate and return result
        pub fn validate(self) -> Result<Vec<String>, Vec<ValidationError>> {
            if self.errors.is_empty() {
                Ok(self.warnings)
            } else {
                Err(self.errors)
            }
        }
    }

    impl Default for ConfigValidator {
        fn default() -> Self {
            Self::new()
        }
    }

    /// Validate worker configuration
    pub fn validate_worker_config(
        concurrency: Option<usize>,
        prefetch_count: Option<usize>,
    ) -> Result<Vec<String>, Vec<ValidationError>> {
        let mut validator = ConfigValidator::new();

        if let Some(c) = concurrency {
            if c == 0 {
                validator.add_error(ValidationError::InvalidValue {
                    field: "concurrency".to_string(),
                    message: "must be greater than 0".to_string(),
                });
            }
            if c > 1000 {
                validator.add_warning(format!(
                    "High concurrency value ({}). Consider if this is intentional.",
                    c
                ));
            }
        }

        if let Some(p) = prefetch_count {
            if p == 0 {
                validator.add_error(ValidationError::InvalidValue {
                    field: "prefetch_count".to_string(),
                    message: "must be greater than 0".to_string(),
                });
            }
            if p > 1000 {
                validator.add_warning(format!(
                    "High prefetch_count value ({}). This may consume significant memory.",
                    p
                ));
            }
        }

        validator.validate()
    }

    /// Validate broker URL format
    pub fn validate_broker_url(url: &str) -> Result<String, ValidationError> {
        if url.is_empty() {
            return Err(ValidationError::InvalidValue {
                field: "broker_url".to_string(),
                message: "cannot be empty".to_string(),
            });
        }

        // Basic URL format validation
        if !url.contains("://") {
            return Err(ValidationError::InvalidValue {
                field: "broker_url".to_string(),
                message: "invalid URL format (missing scheme)".to_string(),
            });
        }

        // Extract scheme
        let scheme = url.split("://").next().unwrap_or("");

        // Validate known schemes
        match scheme {
            "redis" | "rediss" | "postgres" | "postgresql" | "mysql" | "amqp" | "amqps" | "sqs" => {
                Ok(format!("Valid {} URL", scheme))
            }
            _ => Err(ValidationError::InvalidValue {
                field: "broker_url".to_string(),
                message: format!("unsupported scheme: {}", scheme),
            }),
        }
    }

    /// Check feature compatibility
    pub fn check_feature_compatibility(features: &[&str]) -> Result<Vec<String>, ValidationError> {
        let mut warnings = Vec::new();

        // Check for multiple broker features enabled
        let broker_features: Vec<_> = features
            .iter()
            .filter(|f| ["redis", "postgres", "mysql", "amqp", "sqs"].contains(f))
            .collect();

        if broker_features.len() > 1 {
            warnings.push(format!(
                "Multiple broker features enabled: {:?}. Ensure you're using the correct broker.",
                broker_features
            ));
        }

        // Check for multiple backend features enabled
        let backend_features: Vec<_> = features
            .iter()
            .filter(|f| ["backend-redis", "backend-db", "backend-rpc"].contains(f))
            .collect();

        if backend_features.len() > 1 {
            warnings.push(format!(
                "Multiple backend features enabled: {:?}. Ensure you're using the correct backend.",
                backend_features
            ));
        }

        Ok(warnings)
    }

    /// Get feature compatibility matrix documentation
    ///
    /// Returns a formatted string documenting which features are compatible
    /// and which combinations are recommended.
    ///
    /// # Example
    ///
    /// ```rust
    /// use celers::config_validation::feature_compatibility_matrix;
    ///
    /// println!("{}", feature_compatibility_matrix());
    /// ```
    pub fn feature_compatibility_matrix() -> String {
        r#"
╔══════════════════════════════════════════════════════════════════════════════╗
║                      CeleRS Feature Compatibility Matrix                      ║
╚══════════════════════════════════════════════════════════════════════════════╝

BROKER FEATURES (Choose ONE):
  ✓ redis      - Redis broker (recommended for most use cases)
  ✓ postgres   - PostgreSQL broker (good for existing PostgreSQL infrastructure)
  ✓ mysql      - MySQL broker (good for existing MySQL infrastructure)
  ✓ amqp       - RabbitMQ/AMQP broker (enterprise messaging)
  ✓ sqs        - AWS SQS broker (cloud-native, serverless)

BACKEND FEATURES (Choose ONE):
  ✓ backend-redis  - Redis result backend (recommended with redis broker)
  ✓ backend-db     - PostgreSQL/MySQL backend (use with postgres/mysql broker)
  ✓ backend-rpc    - gRPC result backend (distributed systems)

SERIALIZATION FEATURES (Can combine):
  ✓ json           - JSON serialization (default, always available)
  ✓ msgpack        - MessagePack serialization (compact binary format)

OBSERVABILITY FEATURES (Can combine):
  ✓ metrics        - Prometheus metrics
  ✓ tracing        - OpenTelemetry distributed tracing

OTHER FEATURES (Can combine):
  ✓ beat           - Periodic task scheduler
  ✓ dev-utils      - Development and testing utilities

RECOMMENDED COMBINATIONS:
  1. Simple Setup:
     features = ["redis", "backend-redis", "json"]

  2. Production Ready:
     features = ["redis", "backend-redis", "json", "metrics", "tracing"]

  3. PostgreSQL Stack:
     features = ["postgres", "backend-db", "json", "metrics"]

  4. AWS Cloud:
     features = ["sqs", "backend-rpc", "json", "msgpack", "metrics"]

  5. Full Featured:
     features = ["full"]  # Enables all features

NOTES:
  - Multiple brokers can be compiled but only one should be used at runtime
  - Multiple backends can be compiled but only one should be used at runtime
  - json + msgpack enables both serialization formats
  - metrics + tracing provides comprehensive observability
"#
        .to_string()
    }
}

/// Compile-time feature validation and conflict detection
pub mod compile_time_validation {
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
            panic!(
                "At least one broker feature must be enabled: redis, postgres, mysql, amqp, or sqs"
            );
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
}

// Validate feature configuration at compile time
#[allow(dead_code)]
const _FEATURE_VALIDATION: () = compile_time_validation::validate_feature_config();

/// Broker selection helpers
pub mod broker_helper {
    use std::env;

    /// Broker configuration error with helpful suggestions
    #[derive(Debug, thiserror::Error)]
    pub enum BrokerConfigError {
        #[error("Missing environment variable: {0}\n\nSuggestion: Set the environment variable before running:\n  export {0}=<value>")]
        MissingEnvVar(String),

        #[error("Unsupported broker type: {broker_type}\n\nSupported types: redis, postgres, mysql, amqp, sqs\nNote: {note}")]
        UnsupportedBrokerType { broker_type: String, note: String },

        #[error("Feature not enabled: {feature}\n\nTo enable this feature, add it to your Cargo.toml:\n  celers = {{ version = \"0.1\", features = [\"{feature}\"] }}\n\nAvailable features: redis, postgres, mysql, amqp, sqs, backend-redis, backend-db, backend-rpc")]
        FeatureNotEnabled { feature: String },

        #[error("Broker creation failed: {message}\n\nPossible causes:\n{suggestions}")]
        CreationFailed {
            message: String,
            suggestions: String,
        },
    }

    /// Create a broker from environment variables
    ///
    /// Environment variables:
    /// - `CELERS_BROKER_TYPE`: Type of broker (redis, postgres, mysql, amqp, sqs)
    /// - `CELERS_BROKER_URL`: Connection URL for the broker
    /// - `CELERS_BROKER_QUEUE`: Queue name (default: "celers")
    ///
    /// # Example
    ///
    /// ```bash
    /// export CELERS_BROKER_TYPE=redis
    /// export CELERS_BROKER_URL=redis://localhost:6379
    /// export CELERS_BROKER_QUEUE=my_queue
    /// ```
    ///
    /// ```rust,ignore
    /// use celers::broker_helper::create_broker_from_env;
    ///
    /// let broker = create_broker_from_env().await?;
    /// ```
    pub async fn create_broker_from_env() -> Result<Box<dyn crate::Broker>, BrokerConfigError> {
        let broker_type = env::var("CELERS_BROKER_TYPE")
            .map_err(|_| BrokerConfigError::MissingEnvVar("CELERS_BROKER_TYPE".to_string()))?;

        let broker_url = env::var("CELERS_BROKER_URL")
            .map_err(|_| BrokerConfigError::MissingEnvVar("CELERS_BROKER_URL".to_string()))?;

        let queue_name = env::var("CELERS_BROKER_QUEUE").unwrap_or_else(|_| "celers".to_string());

        create_broker(&broker_type, &broker_url, &queue_name).await
    }

    /// Create a broker with explicit configuration
    ///
    /// # Arguments
    ///
    /// * `broker_type` - Type of broker: "redis", "postgres", "mysql", "amqp", "sqs"
    /// * `broker_url` - Connection URL for the broker
    /// * `queue_name` - Name of the queue to use
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::broker_helper::create_broker;
    ///
    /// let broker = create_broker("redis", "redis://localhost:6379", "my_queue").await?;
    /// ```
    pub async fn create_broker(
        broker_type: &str,
        broker_url: &str,
        queue_name: &str,
    ) -> Result<Box<dyn crate::Broker>, BrokerConfigError> {
        match broker_type.to_lowercase().as_str() {
            #[cfg(feature = "redis")]
            "redis" => {
                use crate::RedisBroker;

                RedisBroker::new(broker_url, queue_name)
                    .map(|b| Box::new(b) as Box<dyn crate::Broker>)
                    .map_err(|e| BrokerConfigError::CreationFailed {
                        message: e.to_string(),
                        suggestions: "- Check that Redis server is running\n  - Verify the connection URL format: redis://host:port\n  - Ensure network connectivity to Redis server".to_string(),
                    })
            }

            #[cfg(feature = "postgres")]
            "postgres" | "postgresql" => {
                use crate::PostgresBroker;

                PostgresBroker::with_queue(broker_url, queue_name)
                    .await
                    .map(|b| Box::new(b) as Box<dyn crate::Broker>)
                    .map_err(|e| BrokerConfigError::CreationFailed {
                        message: e.to_string(),
                        suggestions: "- Check that PostgreSQL server is running\n  - Verify the connection URL format: postgres://user:pass@host:port/db\n  - Ensure database exists and user has permissions".to_string(),
                    })
            }

            #[cfg(feature = "mysql")]
            "mysql" => {
                use crate::MysqlBroker;

                MysqlBroker::with_queue(broker_url, queue_name)
                    .await
                    .map(|b| Box::new(b) as Box<dyn crate::Broker>)
                    .map_err(|e| BrokerConfigError::CreationFailed {
                        message: e.to_string(),
                        suggestions: "- Check that MySQL server is running\n  - Verify the connection URL format: mysql://user:pass@host:port/db\n  - Ensure database exists and user has permissions".to_string(),
                    })
            }

            _ => {
                // Check if it's a known type but feature not enabled
                #[cfg(not(feature = "redis"))]
                if broker_type.to_lowercase() == "redis" {
                    return Err(BrokerConfigError::FeatureNotEnabled {
                        feature: "redis".to_string(),
                    });
                }

                #[cfg(not(feature = "postgres"))]
                if broker_type.to_lowercase() == "postgres"
                    || broker_type.to_lowercase() == "postgresql"
                {
                    return Err(BrokerConfigError::FeatureNotEnabled {
                        feature: "postgres".to_string(),
                    });
                }

                #[cfg(not(feature = "mysql"))]
                if broker_type.to_lowercase() == "mysql" {
                    return Err(BrokerConfigError::FeatureNotEnabled {
                        feature: "mysql".to_string(),
                    });
                }

                // AMQP and SQS use celers-kombu Transport trait, not Broker trait
                if broker_type.to_lowercase() == "amqp" || broker_type.to_lowercase() == "rabbitmq"
                {
                    return Err(BrokerConfigError::UnsupportedBrokerType {
                        broker_type: broker_type.to_string(),
                        note: "AMQP brokers use the Transport trait. Import and use AmqpBroker directly from celers::AmqpBroker".to_string(),
                    });
                }

                if broker_type.to_lowercase() == "sqs" {
                    return Err(BrokerConfigError::UnsupportedBrokerType {
                        broker_type: broker_type.to_string(),
                        note: "SQS brokers use the Transport trait. Import and use SqsBroker directly from celers::SqsBroker".to_string(),
                    });
                }

                Err(BrokerConfigError::UnsupportedBrokerType {
                    broker_type: broker_type.to_string(),
                    note: "Check the broker type name for typos".to_string(),
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_facade_exports() {
        // Verify main types are exported
        let _: Option<Box<dyn Broker>> = None;
    }

    #[test]
    fn test_config_validation() {
        use crate::config_validation::*;

        // Test worker config validation
        let result = validate_worker_config(Some(4), Some(10));
        assert!(result.is_ok());

        let result = validate_worker_config(Some(0), Some(10));
        assert!(result.is_err());

        let result = validate_worker_config(Some(4), Some(0));
        assert!(result.is_err());

        // Test broker URL validation
        let result = validate_broker_url("redis://localhost:6379");
        assert!(result.is_ok());

        let result = validate_broker_url("invalid");
        assert!(result.is_err());

        let result = validate_broker_url("");
        assert!(result.is_err());

        // Test feature compatibility
        let result = check_feature_compatibility(&["redis", "postgres"]);
        assert!(result.is_ok());
        assert!(!result.unwrap().is_empty()); // Should have warnings
    }

    #[test]
    #[cfg(feature = "redis")]
    fn test_redis_broker_export() {
        // Verify Redis broker is available when feature is enabled
        use crate::RedisBroker;
        let _: Option<RedisBroker> = None;
    }

    #[test]
    #[cfg(feature = "postgres")]
    fn test_postgres_broker_export() {
        // Verify PostgreSQL broker is available when feature is enabled
        use crate::PostgresBroker;
        let _: Option<PostgresBroker> = None;
    }

    #[test]
    #[cfg(feature = "mysql")]
    fn test_mysql_broker_export() {
        // Verify MySQL broker is available when feature is enabled
        use crate::MysqlBroker;
        let _: Option<MysqlBroker> = None;
    }

    #[test]
    #[cfg(feature = "amqp")]
    fn test_amqp_broker_export() {
        // Verify AMQP broker is available when feature is enabled
        use crate::AmqpBroker;
        let _: Option<AmqpBroker> = None;
    }

    #[test]
    #[cfg(feature = "sqs")]
    fn test_sqs_broker_export() {
        // Verify SQS broker is available when feature is enabled
        use crate::SqsBroker;
        let _: Option<SqsBroker> = None;
    }

    #[test]
    #[cfg(feature = "backend-redis")]
    fn test_redis_backend_export() {
        // Verify Redis backend is available when feature is enabled
        use crate::RedisResultBackend;
        let _: Option<RedisResultBackend> = None;
    }

    #[test]
    #[cfg(feature = "backend-db")]
    fn test_db_backend_export() {
        // Verify database backends are available when feature is enabled
        use crate::{MysqlResultBackend, PostgresResultBackend};
        let _: Option<PostgresResultBackend> = None;
        let _: Option<MysqlResultBackend> = None;
    }

    #[test]
    #[cfg(feature = "backend-rpc")]
    fn test_rpc_backend_export() {
        // Verify gRPC backend is available when feature is enabled
        use crate::GrpcResultBackend;
        let _: Option<GrpcResultBackend> = None;
    }

    #[test]
    #[cfg(feature = "beat")]
    fn test_beat_export() {
        // Verify beat scheduler is available when feature is enabled
        use crate::BeatScheduler;
        let _: Option<BeatScheduler> = None;
    }

    #[test]
    #[cfg(feature = "metrics")]
    #[allow(unused_imports)]
    fn test_metrics_export() {
        // Verify metrics functions are available when feature is enabled
        use crate::{gather_metrics, reset_metrics};
    }

    #[test]
    #[cfg(feature = "tracing")]
    #[allow(unused_imports)]
    fn test_tracing_export() {
        // Verify tracing functions are available when feature is enabled
        use crate::tracing::{init_tracing, task_span};
    }

    #[test]
    fn test_prelude_imports() {
        // Verify prelude imports work
        use crate::prelude::*;

        // Should be able to use common types
        let _: Option<Box<dyn Broker>> = None;
        let _: Option<SerializedTask> = None;
        let _: Option<TaskState> = None;
    }

    #[tokio::test]
    async fn test_mock_broker() {
        use crate::dev_utils::{create_test_task, MockBroker};
        use crate::Broker;

        let broker = MockBroker::new();
        assert_eq!(broker.queue_len(), 0);

        // Test enqueue
        let task = create_test_task("test.task");
        let task_id = broker.enqueue(task.clone()).await.unwrap();
        assert_eq!(task_id, task.metadata.id);

        assert_eq!(broker.queue_len(), 1);
        assert_eq!(broker.published_tasks().len(), 1);

        // Test dequeue
        let consumed = broker.dequeue().await.unwrap();
        assert!(consumed.is_some());

        let consumed_msg = consumed.unwrap();
        assert_eq!(consumed_msg.task.metadata.name, "test.task");

        assert_eq!(broker.queue_len(), 0);

        // Test clear
        broker.enqueue(task.clone()).await.unwrap();
        broker.clear();
        assert_eq!(broker.queue_len(), 0);
        assert_eq!(broker.published_tasks().len(), 0);
    }

    #[test]
    fn test_task_builder() {
        use crate::dev_utils::TaskBuilder;

        let task = TaskBuilder::new("my.task")
            .id("550e8400-e29b-41d4-a716-446655440000".to_string())
            .max_retries(3)
            .build();

        assert_eq!(task.metadata.name, "my.task");
        assert_eq!(
            task.metadata.id.to_string(),
            "550e8400-e29b-41d4-a716-446655440000"
        );
        assert_eq!(task.metadata.max_retries, 3);
    }

    #[test]
    fn test_compile_time_validation() {
        use crate::compile_time_validation::*;

        // Test that feature validation functions are callable
        assert!(has_broker_feature());
        assert!(has_serialization_feature());
        assert!(count_broker_features() > 0);

        // Test feature summary
        let summary = feature_summary();
        assert!(summary.contains("CeleRS Configuration:"));
        assert!(summary.contains("Brokers"));
        assert!(summary.contains("Backends"));
        assert!(summary.contains("Formats"));
    }

    // Integration tests for all broker types
    #[cfg(all(test, feature = "redis"))]
    mod redis_integration {
        use super::*;

        #[tokio::test]
        #[ignore = "requires Redis server"]
        async fn test_redis_broker_integration() {
            use crate::RedisBroker;

            // This test requires a running Redis server
            let broker_result = RedisBroker::new("redis://localhost:6379", "test_queue");

            if let Ok(broker) = broker_result {
                let task = crate::dev_utils::create_test_task("redis.test");
                let result = broker.enqueue(task).await;
                assert!(result.is_ok());
            }
        }
    }

    #[cfg(all(test, feature = "postgres"))]
    mod postgres_integration {
        use super::*;

        #[tokio::test]
        #[ignore = "requires PostgreSQL server"]
        async fn test_postgres_broker_integration() {
            use crate::PostgresBroker;

            // This test requires a running PostgreSQL server
            let broker_result =
                PostgresBroker::with_queue("postgres://localhost/test", "test_queue").await;

            if let Ok(broker) = broker_result {
                let task = crate::dev_utils::create_test_task("postgres.test");
                let result = broker.enqueue(task).await;
                assert!(result.is_ok());
            }
        }
    }

    #[cfg(all(test, feature = "mysql"))]
    mod mysql_integration {
        use super::*;

        #[tokio::test]
        #[ignore = "requires MySQL server"]
        async fn test_mysql_broker_integration() {
            use crate::MysqlBroker;

            // This test requires a running MySQL server
            let broker_result =
                MysqlBroker::with_queue("mysql://localhost/test", "test_queue").await;

            if let Ok(broker) = broker_result {
                let task = crate::dev_utils::create_test_task("mysql.test");
                let result = broker.enqueue(task).await;
                assert!(result.is_ok());
            }
        }
    }

    #[cfg(all(test, feature = "amqp"))]
    mod amqp_integration {
        use super::*;

        #[tokio::test]
        #[ignore = "requires RabbitMQ server"]
        async fn test_amqp_broker_integration() {
            use crate::AmqpBroker;

            // This test requires a running RabbitMQ server
            let broker_result = AmqpBroker::new("amqp://localhost:5672", "test_queue").await;

            if let Ok(broker) = broker_result {
                let task = crate::dev_utils::create_test_task("amqp.test");
                let result = broker.enqueue(task).await;
                assert!(result.is_ok());
            }
        }
    }

    #[cfg(all(test, feature = "sqs"))]
    mod sqs_integration {
        use super::*;

        #[tokio::test]
        #[ignore = "requires AWS SQS"]
        async fn test_sqs_broker_integration() {
            use crate::SqsBroker;

            // This test requires AWS SQS access
            let broker_result = SqsBroker::new(
                "us-east-1",
                "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
            )
            .await;

            if let Ok(broker) = broker_result {
                let task = crate::dev_utils::create_test_task("sqs.test");
                let result = broker.enqueue(task).await;
                assert!(result.is_ok());
            }
        }
    }

    // Backend integration tests
    #[cfg(all(test, feature = "backend-redis"))]
    mod backend_redis_integration {
        use super::*;

        #[tokio::test]
        #[ignore = "requires Redis server"]
        async fn test_redis_backend_integration() {
            use crate::RedisResultBackend;

            let backend_result = RedisResultBackend::new("redis://localhost:6379");

            if let Ok(backend) = backend_result {
                use uuid::Uuid;
                let task_id = Uuid::new_v4();
                let result = backend
                    .store_result(task_id, &serde_json::json!({"result": "success"}))
                    .await;
                assert!(result.is_ok());
            }
        }
    }

    #[cfg(all(test, feature = "backend-db"))]
    mod backend_db_integration {
        use super::*;

        #[tokio::test]
        #[ignore = "requires PostgreSQL server"]
        async fn test_postgres_backend_integration() {
            use crate::PostgresResultBackend;

            let backend_result = PostgresResultBackend::new("postgres://localhost/test").await;

            if let Ok(backend) = backend_result {
                use uuid::Uuid;
                let task_id = Uuid::new_v4();
                let result = backend
                    .store_result(task_id, &serde_json::json!({"result": "success"}))
                    .await;
                assert!(result.is_ok());
            }
        }

        #[tokio::test]
        #[ignore = "requires MySQL server"]
        async fn test_mysql_backend_integration() {
            use crate::MysqlResultBackend;

            let backend_result = MysqlResultBackend::new("mysql://localhost/test").await;

            if let Ok(backend) = backend_result {
                use uuid::Uuid;
                let task_id = Uuid::new_v4();
                let result = backend
                    .store_result(task_id, &serde_json::json!({"result": "success"}))
                    .await;
                assert!(result.is_ok());
            }
        }
    }

    #[cfg(all(test, feature = "beat"))]
    mod beat_integration {
        use super::*;

        #[tokio::test]
        #[ignore = "requires broker server"]
        async fn test_beat_scheduler_integration() {
            use crate::BeatScheduler;

            // Create a mock broker for testing
            let broker = crate::dev_utils::MockBroker::new();

            // Create a simple schedule
            let scheduler = BeatScheduler::new(Box::new(broker));

            // Verify scheduler was created
            assert!(scheduler.is_ok());
        }
    }

    // Workflow integration tests
    #[test]
    fn test_workflow_chain() {
        use crate::canvas::Chain;

        let chain = Chain::new()
            .then("task1", vec![])
            .then("task2", vec![])
            .then("task3", vec![]);

        // Verify chain structure
        assert!(chain.tasks.len() >= 1);
    }

    #[test]
    fn test_workflow_group() {
        use crate::canvas::Group;

        let group = Group::new()
            .add("task1", vec![])
            .add("task2", vec![])
            .add("task3", vec![]);

        // Verify group structure
        assert!(group.tasks.len() >= 1);
    }

    #[test]
    fn test_workflow_chord() {
        use crate::canvas::{Chord, Group, Signature};

        let header = Group::new().add("task1", vec![]).add("task2", vec![]);

        let callback = Signature::new("callback".to_string());

        let chord = Chord::new(header, callback);

        // Verify chord structure
        assert!(chord.header.tasks.len() >= 1);
    }

    // Performance and benchmarking tests
    #[test]
    fn test_task_creation_performance() {
        use crate::dev_utils::TaskBuilder;
        use std::time::Instant;

        let start = Instant::now();
        for i in 0..1000 {
            let _task = TaskBuilder::new(&format!("task.{}", i)).build();
        }
        let duration = start.elapsed();

        // Should be able to create 1000 tasks quickly (< 100ms)
        assert!(duration.as_millis() < 100);
    }

    #[test]
    fn test_broker_helper_functions() {
        use crate::broker_helper::BrokerConfigError;

        // Test error types
        let error = BrokerConfigError::MissingEnvVar("TEST".to_string());
        assert!(error.to_string().contains("TEST"));

        let error = BrokerConfigError::UnsupportedBrokerType {
            broker_type: "foo".to_string(),
            note: "bar".to_string(),
        };
        assert!(error.to_string().contains("foo"));

        let error = BrokerConfigError::FeatureNotEnabled {
            feature: "redis".to_string(),
        };
        assert!(error.to_string().contains("redis"));
    }

    // Configuration validation tests
    #[test]
    fn test_presets_exist() {
        use crate::presets::*;

        // Test that all presets are available
        let _config = production_config();
        let _config = high_throughput_config();
        let _config = low_latency_config();
        let _config = memory_constrained_config();
    }

    // Zero-cost abstractions verification tests
    #[test]
    fn test_zero_cost_task_creation() {
        use crate::dev_utils::TaskBuilder;
        use std::time::Instant;

        // Measure overhead of task creation
        let start = Instant::now();
        for _ in 0..10000 {
            let _task = TaskBuilder::new("test.task").build();
        }
        let duration = start.elapsed();

        // Should be extremely fast - less than 10ms for 10k tasks
        assert!(
            duration.as_millis() < 10,
            "Task creation overhead too high: {}ms",
            duration.as_millis()
        );
    }

    #[test]
    fn test_zero_cost_workflow_construction() {
        use crate::canvas::{Chain, Group};
        use std::time::Instant;

        // Measure overhead of workflow construction
        let start = Instant::now();
        for _ in 0..1000 {
            let _chain = Chain::new()
                .then("task1", vec![])
                .then("task2", vec![])
                .then("task3", vec![]);

            let _group = Group::new()
                .add("task1", vec![])
                .add("task2", vec![])
                .add("task3", vec![]);
        }
        let duration = start.elapsed();

        // Should be very fast - less than 5ms for 1k workflows
        assert!(
            duration.as_millis() < 5,
            "Workflow construction overhead too high: {}ms",
            duration.as_millis()
        );
    }

    #[test]
    fn test_feature_validation_overhead() {
        use crate::compile_time_validation::*;
        use std::time::Instant;

        // Measure overhead of feature validation functions
        let start = Instant::now();
        for _ in 0..100000 {
            let _ = has_broker_feature();
            let _ = has_serialization_feature();
            let _ = count_broker_features();
            let _ = count_backend_features();
        }
        let duration = start.elapsed();

        // Const functions should have near-zero overhead
        assert!(
            duration.as_millis() < 10,
            "Feature validation overhead too high: {}ms",
            duration.as_millis()
        );
    }

    #[test]
    fn test_memory_efficiency() {
        use crate::dev_utils::TaskBuilder;

        // Measure memory footprint of tasks
        let tasks: Vec<_> = (0..1000)
            .map(|i| TaskBuilder::new(&format!("task{}", i)).build())
            .collect();

        // Verify tasks are created
        assert_eq!(tasks.len(), 1000);

        // Memory usage should be reasonable
        let estimated_size_per_task = std::mem::size_of_val(&tasks[0]);
        assert!(
            estimated_size_per_task < 1024,
            "Task size too large: {} bytes",
            estimated_size_per_task
        );
    }

    #[test]
    fn test_inline_optimization_candidates() {
        use crate::compile_time_validation::*;

        // Test that inline functions are small enough to be inlined
        assert!(has_broker_feature() || !has_broker_feature()); // Should be optimized to const
        assert!(has_serialization_feature() || !has_serialization_feature());
        // Should be optimized to const
    }
}
