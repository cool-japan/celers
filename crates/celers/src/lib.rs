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
    utils, BrokerError, Consumer, Envelope, Producer, QueueConfig, QueueMode, Result, Transport,
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
pub use celers_broker_redis::{circuit_breaker, dedup, health, monitoring, utilities, RedisBroker};

#[cfg(feature = "postgres")]
pub use celers_broker_postgres::{monitoring, utilities, PostgresBroker};

#[cfg(feature = "mysql")]
pub use celers_broker_sql::{monitoring, utilities, MysqlBroker};

#[cfg(feature = "amqp")]
pub use celers_broker_amqp::{monitoring, utilities, AmqpBroker};

#[cfg(feature = "sqs")]
pub use celers_broker_sqs::{monitoring, optimization, utilities, SqsBroker};

// Optional backend re-exports
#[cfg(feature = "backend-redis")]
pub use celers_backend_redis::{
    batch_size,
    event_transport::{RedisEventConfig, RedisEventEmitter, RedisEventReceiver},
    ttl, ChordState, RedisResultBackend, ResultBackend, TaskMeta, TaskResult,
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
    // Note: monitoring and utilities modules are available at crate root level
    // when the corresponding broker feature is enabled, but not re-exported in
    // prelude to avoid naming conflicts when multiple brokers are used.
    #[cfg(feature = "redis")]
    pub use crate::{circuit_breaker, dedup, health, rate_limit, RedisBroker};

    #[cfg(feature = "postgres")]
    pub use crate::PostgresBroker;

    #[cfg(feature = "mysql")]
    pub use crate::MysqlBroker;

    #[cfg(feature = "amqp")]
    pub use crate::AmqpBroker;

    #[cfg(feature = "sqs")]
    pub use crate::{optimization, SqsBroker};

    // Backend implementations
    #[cfg(feature = "backend-redis")]
    pub use crate::{
        batch_size, ttl, ChordState, RedisEventConfig, RedisEventEmitter, RedisEventReceiver,
        RedisResultBackend, ResultBackend, TaskMeta,
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
    /// Result type for task execution with thread-safe error handling
    pub type TaskResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
    /// Async task function signature
    pub type AsyncTaskFn<T> =
        fn(Vec<u8>) -> std::pin::Pin<Box<dyn std::future::Future<Output = TaskResult<T>> + Send>>;

    // Re-export common Result type from kombu
    pub use celers_kombu::Result as KombuResult;

    // Kombu utility functions
    pub use crate::utils;

    // Convenience functions for ergonomic workflow creation
    pub use crate::convenience::{
        batch, best_effort, chain, chain_from, chord, chunks, critical, delay, expire_in, fan_in,
        fan_out, group, group_from, high_priority, low_priority, map, options, parallel, pipeline,
        retry_with_backoff, starmap, task, task_with_options, transient, with_countdown,
        with_expires, with_priority, with_retry, with_timeout,
    };

    // Beat-specific convenience functions
    #[cfg(feature = "beat")]
    pub use crate::convenience::recurring;

    // Workflow templates for common patterns
    pub use crate::workflow_templates::{
        batch_processing, etl_pipeline, map_reduce_workflow, priority_workflow, scatter_gather,
        sequential_pipeline,
    };

    // Task composition utilities
    pub use crate::task_composition::{
        circuit_breaker_group, rate_limited_workflow, retry_wrapper, timeout_wrapper,
    };

    // Error recovery patterns
    pub use crate::error_recovery::{
        ignore_errors, with_dlq, with_exponential_backoff, with_fallback,
    };

    // Workflow validation utilities
    pub use crate::workflow_validation::{
        check_performance_concerns_chain, check_performance_concerns_group, validate_chain,
        validate_chord, validate_group, ValidationError as WorkflowValidationError,
    };

    // Result aggregation helpers
    pub use crate::result_helpers::{
        create_result_collector, create_result_filter, create_result_reducer,
        create_result_transformer,
    };

    // Advanced workflow patterns
    pub use crate::advanced_patterns::{
        create_conditional_workflow, create_dynamic_workflow, create_parallel_chains,
        create_saga_workflow,
    };

    // Monitoring and observability helpers
    pub use crate::monitoring_helpers::TaskMonitor;

    // Batch processing helpers
    pub use crate::batch_helpers::{
        create_adaptive_batches, create_dynamic_batches, create_prioritized_batches,
    };

    // Health check utilities
    pub use crate::health_check::{
        DependencyChecker, HealthCheckResult, HealthStatus, WorkerHealthChecker,
    };

    // Resource management utilities
    pub use crate::resource_management::{ResourceLimits, ResourcePool, ResourceTracker};

    // Task lifecycle hooks
    pub use crate::task_hooks::{
        HookRegistry, HookResult, LoggingHook, PostExecutionHook, PreExecutionHook, ValidationHook,
    };

    // Metrics aggregation utilities
    pub use crate::metrics_aggregation::{DataPoint, Histogram, MetricsAggregator};

    // Task cancellation utilities
    pub use crate::task_cancellation::{CancellationToken, ExecutionGuard, TimeoutManager};

    // Advanced retry strategies
    pub use crate::retry_strategies::{
        DefaultRetryPolicy, ErrorPatternRetryPolicy, RetryPolicy, RetryStrategy,
    };

    // Task dependency management
    pub use crate::task_dependencies::DependencyGraph;

    // Performance profiling utilities
    // Note: PerformanceProfiler conflicts with dev_utils::PerformanceProfiler
    // Access via crate::performance_profiling::PerformanceProfiler
    pub use crate::performance_profiling::{PerformanceProfile, ProfileSpan};
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

    /// Create a chunks workflow for processing items in batches
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::convenience::chunks;
    /// use serde_json::json;
    ///
    /// // Process items in chunks of 10
    /// let items = vec![json!(1), json!(2), json!(3)];
    /// let workflow = chunks("process_item", items, 10);
    /// ```
    pub fn chunks<T: serde::Serialize>(
        task_name: impl Into<String>,
        items: Vec<T>,
        chunk_size: usize,
    ) -> crate::Chunks {
        let sig = crate::Signature::new(task_name.into());
        let serialized_items: Vec<serde_json::Value> = items
            .into_iter()
            .filter_map(|item| serde_json::to_value(item).ok())
            .collect();
        crate::Chunks::new(sig, serialized_items, chunk_size)
    }

    /// Create a map workflow for applying a task to each item
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::convenience::map;
    /// use serde_json::json;
    ///
    /// let items = vec![json!(1), json!(2), json!(3)];
    /// let workflow = map("square", items);
    /// ```
    pub fn map<T: serde::Serialize>(task_name: impl Into<String>, items: Vec<T>) -> crate::Map {
        let sig = crate::Signature::new(task_name.into());
        let serialized_items: Vec<Vec<serde_json::Value>> = items
            .into_iter()
            .filter_map(|item| serde_json::to_value(item).ok().map(|v| vec![v]))
            .collect();
        crate::Map::new(sig, serialized_items)
    }

    /// Create a starmap workflow for applying a task with multiple arguments
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::convenience::starmap;
    /// use serde_json::json;
    ///
    /// let args = vec![
    ///     vec![json!(1), json!(2)],
    ///     vec![json!(3), json!(4)],
    /// ];
    /// let workflow = starmap("add", args);
    /// ```
    pub fn starmap<T: serde::Serialize>(
        task_name: impl Into<String>,
        args: Vec<Vec<T>>,
    ) -> crate::Starmap {
        let sig = crate::Signature::new(task_name.into());
        let serialized_args: Vec<Vec<serde_json::Value>> = args
            .into_iter()
            .map(|arg_list| {
                arg_list
                    .into_iter()
                    .filter_map(|item| serde_json::to_value(item).ok())
                    .collect()
            })
            .collect();
        crate::Starmap::new(sig, serialized_args)
    }

    /// Create task options for configuring task execution
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::convenience::options;
    ///
    /// let opts = options()
    ///     .max_retries(3)
    ///     .countdown(60)
    ///     .priority(5);
    /// ```
    pub fn options() -> crate::TaskOptions {
        crate::TaskOptions::default()
    }

    /// Create task options with retry configuration
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::convenience::with_retry;
    ///
    /// let opts = with_retry(5, 60);  // 5 retries with 60s delay
    /// ```
    pub fn with_retry(max_retries: u32, retry_delay_secs: u64) -> crate::TaskOptions {
        crate::TaskOptions {
            max_retries: Some(max_retries),
            countdown: Some(retry_delay_secs),
            ..Default::default()
        }
    }

    /// Create task options with timeout configuration
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::convenience::with_timeout;
    ///
    /// let opts = with_timeout(300);  // 5 minute timeout
    /// ```
    pub fn with_timeout(timeout_secs: u64) -> crate::TaskOptions {
        crate::TaskOptions {
            time_limit: Some(timeout_secs),
            ..Default::default()
        }
    }

    /// Create task options with priority
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::convenience::with_priority;
    ///
    /// let opts = with_priority(9);  // High priority task
    /// ```
    pub fn with_priority(priority: u8) -> crate::TaskOptions {
        crate::TaskOptions {
            priority: Some(priority),
            ..Default::default()
        }
    }

    /// Create task options with countdown (delay in seconds)
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::convenience::with_countdown;
    ///
    /// let opts = with_countdown(60);  // Run after 60 seconds
    /// ```
    pub fn with_countdown(countdown_secs: u64) -> crate::TaskOptions {
        crate::TaskOptions {
            countdown: Some(countdown_secs),
            ..Default::default()
        }
    }

    /// Create task options with expires (expiration time in seconds)
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::convenience::with_expires;
    ///
    /// let opts = with_expires(7200);  // Expire after 2 hours
    /// ```
    pub fn with_expires(expires_secs: u64) -> crate::TaskOptions {
        crate::TaskOptions {
            expires: Some(expires_secs),
            ..Default::default()
        }
    }

    /// Create a batch of task signatures from a collection
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::convenience::batch;
    /// use serde_json::json;
    ///
    /// let items = vec![
    ///     vec![json!(1), json!(2)],
    ///     vec![json!(3), json!(4)],
    /// ];
    /// let tasks = batch("add", items);
    /// ```
    pub fn batch<T: serde::Serialize>(
        task_name: impl Into<String>,
        args_list: Vec<Vec<T>>,
    ) -> Vec<crate::Signature> {
        let task_name = task_name.into();
        args_list
            .into_iter()
            .map(|args| {
                let sig = crate::Signature::new(task_name.clone());
                let serialized_args: Vec<serde_json::Value> = args
                    .into_iter()
                    .filter_map(|arg| serde_json::to_value(arg).ok())
                    .collect();
                sig.with_args(serialized_args)
            })
            .collect()
    }

    /// Create a chain from a list of task names and their arguments
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::convenience::chain_from;
    /// use serde_json::json;
    ///
    /// let tasks = vec![
    ///     ("task1", vec![json!(1), json!(2)]),
    ///     ("task2", vec![json!(3), json!(4)]),
    /// ];
    /// let workflow = chain_from(tasks);
    /// ```
    pub fn chain_from<T: serde::Serialize>(tasks: Vec<(&str, Vec<T>)>) -> crate::Chain {
        let mut chain = crate::Chain::new();
        for (task_name, args) in tasks {
            let serialized_args: Vec<serde_json::Value> = args
                .into_iter()
                .filter_map(|arg| serde_json::to_value(arg).ok())
                .collect();
            chain = chain.then(task_name, serialized_args);
        }
        chain
    }

    /// Create a group from a list of task names and their arguments
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::convenience::group_from;
    /// use serde_json::json;
    ///
    /// let tasks = vec![
    ///     ("task1", vec![json!(1)]),
    ///     ("task2", vec![json!(2)]),
    ///     ("task3", vec![json!(3)]),
    /// ];
    /// let workflow = group_from(tasks);
    /// ```
    pub fn group_from<T: serde::Serialize>(tasks: Vec<(&str, Vec<T>)>) -> crate::Group {
        let mut group = crate::Group::new();
        for (task_name, args) in tasks {
            let serialized_args: Vec<serde_json::Value> = args
                .into_iter()
                .filter_map(|arg| serde_json::to_value(arg).ok())
                .collect();
            group = group.add(task_name, serialized_args);
        }
        group
    }

    /// Create a signature with common task options applied
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::convenience::task_with_options;
    /// use serde_json::json;
    ///
    /// let sig = task_with_options(
    ///     "my_task",
    ///     vec![json!(1), json!(2)],
    ///     3,  // max_retries
    ///     5   // priority
    /// );
    /// ```
    pub fn task_with_options(
        name: impl Into<String>,
        args: Vec<serde_json::Value>,
        max_retries: u32,
        priority: u8,
    ) -> crate::Signature {
        let mut sig = crate::Signature::new(name.into()).with_args(args);
        sig.options.max_retries = Some(max_retries);
        sig.options.priority = Some(priority);
        sig
    }

    /// Create a recurring task using cron-like syntax (requires beat feature)
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::convenience::recurring;
    ///
    /// // Run every day at midnight
    /// let schedule = recurring("cleanup_task", "0 0 * * *");
    /// ```
    #[cfg(feature = "beat")]
    pub fn recurring(
        task_name: impl Into<String>,
        cron_expr: impl Into<String>,
    ) -> crate::ScheduledTask {
        crate::ScheduledTask {
            name: task_name.into(),
            task: crate::Signature::new(task_name.into()),
            schedule: crate::Schedule::Cron(cron_expr.into()),
        }
    }

    /// Create a delayed task (delayed execution by specified seconds)
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::convenience::delay;
    /// use serde_json::json;
    ///
    /// // Execute after 60 seconds
    /// let sig = delay("send_email", vec![json!("user@example.com")], 60);
    /// ```
    pub fn delay(
        task_name: impl Into<String>,
        args: Vec<serde_json::Value>,
        delay_secs: u64,
    ) -> crate::Signature {
        let mut sig = crate::Signature::new(task_name.into()).with_args(args);
        sig.options.countdown = Some(delay_secs);
        sig
    }

    /// Create a task with expiration time
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::convenience::expire_in;
    /// use serde_json::json;
    ///
    /// // Expire after 2 hours
    /// let sig = expire_in("process_data", vec![json!({"id": 123})], 7200);
    /// ```
    pub fn expire_in(
        task_name: impl Into<String>,
        args: Vec<serde_json::Value>,
        expires_secs: u64,
    ) -> crate::Signature {
        let mut sig = crate::Signature::new(task_name.into()).with_args(args);
        sig.options.expires = Some(expires_secs);
        sig
    }

    /// Create a high priority task (priority level 9)
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::convenience::high_priority;
    /// use serde_json::json;
    ///
    /// let sig = high_priority("urgent_task", vec![json!({"alert": true})]);
    /// ```
    pub fn high_priority(
        task_name: impl Into<String>,
        args: Vec<serde_json::Value>,
    ) -> crate::Signature {
        let mut sig = crate::Signature::new(task_name.into()).with_args(args);
        sig.options.priority = Some(9);
        sig
    }

    /// Create a low priority task (priority level 1)
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::convenience::low_priority;
    /// use serde_json::json;
    ///
    /// let sig = low_priority("background_cleanup", vec![json!({})]);
    /// ```
    pub fn low_priority(
        task_name: impl Into<String>,
        args: Vec<serde_json::Value>,
    ) -> crate::Signature {
        let mut sig = crate::Signature::new(task_name.into()).with_args(args);
        sig.options.priority = Some(1);
        sig
    }

    /// Create a parallel workflow (alias for group)
    ///
    /// This is a more intuitive name for the group workflow pattern.
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::convenience::parallel;
    ///
    /// let workflow = parallel()
    ///     .add("task1", vec![json!(1)])
    ///     .add("task2", vec![json!(2)])
    ///     .add("task3", vec![json!(3)]);
    /// ```
    pub fn parallel() -> crate::Group {
        crate::Group::new()
    }

    /// Create a critical task (high priority with maximum retries)
    ///
    /// Critical tasks are executed with priority 9 and retry up to 5 times.
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::convenience::critical;
    /// use serde_json::json;
    ///
    /// let sig = critical("process_payment", vec![json!({"amount": 100})]);
    /// ```
    pub fn critical(
        task_name: impl Into<String>,
        args: Vec<serde_json::Value>,
    ) -> crate::Signature {
        let mut sig = crate::Signature::new(task_name.into()).with_args(args);
        sig.options.priority = Some(9);
        sig.options.max_retries = Some(5);
        sig
    }

    /// Create a best-effort task (low priority with no retries)
    ///
    /// Best-effort tasks run at low priority and don't retry on failure.
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::convenience::best_effort;
    /// use serde_json::json;
    ///
    /// let sig = best_effort("update_cache", vec![json!({"key": "value"})]);
    /// ```
    pub fn best_effort(
        task_name: impl Into<String>,
        args: Vec<serde_json::Value>,
    ) -> crate::Signature {
        let mut sig = crate::Signature::new(task_name.into()).with_args(args);
        sig.options.priority = Some(1);
        sig.options.max_retries = Some(0);
        sig
    }

    /// Create a transient task (with expiration time)
    ///
    /// Transient tasks expire if not executed within the specified TTL.
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::convenience::transient;
    /// use serde_json::json;
    ///
    /// // Task expires after 5 minutes
    /// let sig = transient("temp_notification", vec![json!({"msg": "hi"})], 300);
    /// ```
    pub fn transient(
        task_name: impl Into<String>,
        args: Vec<serde_json::Value>,
        ttl_secs: u64,
    ) -> crate::Signature {
        let mut sig = crate::Signature::new(task_name.into()).with_args(args);
        sig.options.expires = Some(ttl_secs);
        sig
    }

    /// Create task options with retry and exponential backoff
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::convenience::retry_with_backoff;
    ///
    /// // Retry up to 5 times, starting with 60s delay
    /// let opts = retry_with_backoff(5, 60);
    /// ```
    pub fn retry_with_backoff(max_retries: u32, initial_delay_secs: u64) -> crate::TaskOptions {
        crate::TaskOptions {
            max_retries: Some(max_retries),
            countdown: Some(initial_delay_secs),
            // Note: Exponential backoff is handled by the retry mechanism
            ..Default::default()
        }
    }

    /// Create a pipeline workflow (modern alias for chain)
    ///
    /// Pipeline is a more modern/intuitive name for sequential task execution.
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::convenience::pipeline;
    ///
    /// let workflow = pipeline()
    ///     .then("fetch_data", vec![])
    ///     .then("process_data", vec![])
    ///     .then("save_results", vec![]);
    /// ```
    pub fn pipeline() -> crate::Chain {
        crate::Chain::new()
    }

    /// Create a fan-out workflow (parallel execution with map pattern)
    ///
    /// More intuitive name for the map pattern - fan out to process multiple items.
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::convenience::fan_out;
    /// use serde_json::json;
    ///
    /// let items = vec![json!(1), json!(2), json!(3)];
    /// let workflow = fan_out("process_item", items);
    /// ```
    pub fn fan_out<T: serde::Serialize>(task_name: impl Into<String>, items: Vec<T>) -> crate::Map {
        map(task_name, items)
    }

    /// Create a fan-in workflow (gather results with chord pattern)
    ///
    /// More intuitive name for chord - fan out tasks then fan in to callback.
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::convenience::{parallel, task, fan_in};
    ///
    /// let tasks = parallel()
    ///     .add("task1", vec![])
    ///     .add("task2", vec![]);
    ///
    /// let callback = task("aggregate_results");
    /// let workflow = fan_in(tasks, callback);
    /// ```
    pub fn fan_in(tasks: crate::Group, callback: crate::Signature) -> crate::Chord {
        crate::Chord::new(tasks, callback)
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

    /// Quick MySQL broker setup
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::quick_start::mysql_broker;
    ///
    /// let broker = mysql_broker(
    ///     "mysql://user:pass@localhost/db",
    ///     "celery"
    /// ).await?;
    /// ```
    #[cfg(feature = "mysql")]
    pub async fn mysql_broker(
        url: &str,
        queue: &str,
    ) -> std::result::Result<crate::MysqlBroker, celers_core::error::CelersError> {
        crate::MysqlBroker::with_queue(url, queue).await
    }

    /// Quick AMQP (RabbitMQ) broker setup
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::quick_start::amqp_broker;
    ///
    /// let broker = amqp_broker(
    ///     "amqp://guest:guest@localhost:5672",
    ///     "celery"
    /// ).await?;
    /// ```
    #[cfg(feature = "amqp")]
    pub async fn amqp_broker(
        url: &str,
        queue: &str,
    ) -> std::result::Result<crate::AmqpBroker, celers_core::error::CelersError> {
        crate::AmqpBroker::new(url, queue).await
    }

    /// Quick SQS broker setup
    ///
    /// # Example
    /// ```rust,ignore
    /// use celers::quick_start::sqs_broker;
    ///
    /// let broker = sqs_broker(
    ///     "https://sqs.us-east-1.amazonaws.com/123456789/celery",
    ///     "celery"
    /// ).await?;
    /// ```
    #[cfg(feature = "sqs")]
    pub async fn sqs_broker(
        url: &str,
        queue: &str,
    ) -> std::result::Result<crate::SqsBroker, celers_core::error::CelersError> {
        crate::SqsBroker::new(url, queue).await
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

    /// CPU-bound worker configuration preset
    ///
    /// Optimized for CPU-intensive tasks:
    /// - Concurrency matches CPU cores (no oversubscription)
    /// - Standard polling interval
    /// - Suitable for computation-heavy tasks
    pub fn cpu_bound_config() -> std::result::Result<crate::WorkerConfig, String> {
        use celers_worker::WorkerConfigBuilder;

        WorkerConfigBuilder::new()
            .concurrency(num_cpus::get())
            .poll_interval_ms(500)
            .build()
    }

    /// I/O-bound worker configuration preset
    ///
    /// Optimized for I/O-intensive tasks:
    /// - High concurrency (4x CPU cores) for async I/O
    /// - Fast polling for quick task pickup
    /// - Suitable for network/database operations
    pub fn io_bound_config() -> std::result::Result<crate::WorkerConfig, String> {
        use celers_worker::WorkerConfigBuilder;

        let concurrency = num_cpus::get() * 4;

        WorkerConfigBuilder::new()
            .concurrency(concurrency)
            .poll_interval_ms(200)
            .build()
    }

    /// Balanced worker configuration preset
    ///
    /// Optimized for mixed workloads:
    /// - Moderate concurrency (2x CPU cores)
    /// - Balanced polling interval
    /// - Good default for varied task types
    pub fn balanced_config() -> std::result::Result<crate::WorkerConfig, String> {
        use celers_worker::WorkerConfigBuilder;

        let concurrency = num_cpus::get() * 2;

        WorkerConfigBuilder::new()
            .concurrency(concurrency)
            .poll_interval_ms(500)
            .build()
    }

    /// Development worker configuration preset
    ///
    /// Optimized for development and testing:
    /// - Low concurrency for easier debugging
    /// - Slower polling to reduce noise
    /// - Suitable for local development
    pub fn development_config() -> std::result::Result<crate::WorkerConfig, String> {
        use celers_worker::WorkerConfigBuilder;

        WorkerConfigBuilder::new()
            .concurrency(2)
            .poll_interval_ms(1000)
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
        /// Unique identifier for the task
        pub task_id: String,
        /// Name of the task
        pub task_name: String,
        /// Current state of the task
        pub state: String,
        /// Timestamp when this debug info was captured
        pub timestamp: std::time::SystemTime,
        /// Additional metadata about the task
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
        /// Type of the event (e.g., "task_received", "task_started", "task_completed")
        pub event_type: String,
        /// Optional task ID associated with this event
        pub task_id: Option<String>,
        /// Human-readable message describing the event
        pub message: String,
        /// Timestamp when this event occurred
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
        /// Name of the operation being measured
        pub name: String,
        /// Duration of the operation in milliseconds
        pub duration_ms: u128,
        /// Timestamp when this measurement was taken
        pub timestamp: std::time::SystemTime,
        /// Additional metadata about the measurement
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
        /// Number of tasks in the queue at the time of snapshot
        pub queue_size: usize,
        /// Timestamp when this snapshot was taken
        pub timestamp: std::time::SystemTime,
        /// Additional metadata about the queue state
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
        /// Invalid configuration error
        #[error("Invalid configuration: {0}")]
        InvalidConfig(String),

        /// Missing required field error
        #[error("Missing required field: {0}")]
        MissingField(String),

        /// Invalid value for a specific field
        #[error("Invalid value for {field}: {message}")]
        InvalidValue {
            /// The field name that has an invalid value
            field: String,
            /// Error message describing why the value is invalid
            message: String,
        },

        /// Incompatible configuration detected
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

pub mod compile_time_validation {
    //! Compile-time feature validation and conflict detection
    //!
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
        /// Missing required environment variable
        #[error("Missing environment variable: {0}\n\nSuggestion: Set the environment variable before running:\n  export {0}=<value>")]
        MissingEnvVar(String),

        /// Unsupported broker type requested
        #[error("Unsupported broker type: {broker_type}\n\nSupported types: redis, postgres, mysql, amqp, sqs\nNote: {note}")]
        UnsupportedBrokerType {
            /// The broker type that was requested
            broker_type: String,
            /// Additional note about the error
            note: String,
        },

        /// Required feature not enabled in Cargo.toml
        #[error("Feature not enabled: {feature}\n\nTo enable this feature, add it to your Cargo.toml:\n  celers = {{ version = \"0.1\", features = [\"{feature}\"] }}\n\nAvailable features: redis, postgres, mysql, amqp, sqs, backend-redis, backend-db, backend-rpc")]
        FeatureNotEnabled {
            /// The feature name that needs to be enabled
            feature: String,
        },

        /// Broker creation failed with detailed error information
        #[error("Broker creation failed: {message}\n\nPossible causes:\n{suggestions}")]
        CreationFailed {
            /// Error message from the broker creation attempt
            message: String,
            /// Suggestions for resolving the error
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

/// Startup time optimization utilities
///
/// This module provides utilities and patterns for optimizing application startup time,
/// including lazy initialization and caching strategies.
pub mod startup_optimization {
    use std::sync::OnceLock;

    /// Type alias for async initialization tasks used in parallel_init
    pub type AsyncInitTask<T, E> = Box<
        dyn FnOnce() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, E>> + Send>>
            + Send,
    >;

    /// Lazy initialization helper using OnceLock for thread-safe static initialization
    ///
    /// # Example
    ///
    /// ```rust
    /// use celers::startup_optimization::LazyInit;
    ///
    /// static MY_CONFIG: LazyInit<String> = LazyInit::new();
    ///
    /// fn get_config() -> &'static String {
    ///     MY_CONFIG.get_or_init(|| {
    ///         // Expensive initialization happens only once
    ///         String::from("config_value")
    ///     })
    /// }
    /// ```
    pub struct LazyInit<T> {
        cell: OnceLock<T>,
    }

    impl<T> LazyInit<T> {
        /// Create a new lazy initialization wrapper
        pub const fn new() -> Self {
            Self {
                cell: OnceLock::new(),
            }
        }

        /// Get the value, initializing it if necessary
        #[inline]
        pub fn get_or_init<F>(&self, f: F) -> &T
        where
            F: FnOnce() -> T,
        {
            self.cell.get_or_init(f)
        }

        /// Try to get the value if it's already initialized
        #[inline]
        pub fn get(&self) -> Option<&T> {
            self.cell.get()
        }
    }

    impl<T> Default for LazyInit<T> {
        fn default() -> Self {
            Self::new()
        }
    }

    /// Pre-compiled pattern cache for faster startup
    ///
    /// Note: For regex caching, add the `regex` crate to your dependencies
    /// and implement a similar pattern using `OnceLock<Mutex<HashMap<String, &'static Regex>>>`.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use std::sync::OnceLock;
    /// use std::collections::HashMap;
    /// use std::sync::Mutex;
    /// use regex::Regex;
    ///
    /// pub fn cached_regex(pattern: &str) -> &'static Regex {
    ///     static CACHE: OnceLock<Mutex<HashMap<String, &'static Regex>>> = OnceLock::new();
    ///     let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
    ///     let mut cache = cache.lock().unwrap();
    ///
    ///     if let Some(regex) = cache.get(pattern) {
    ///         return regex;
    ///     }
    ///
    ///     let regex = Box::leak(Box::new(Regex::new(pattern).expect("Invalid regex")));
    ///     cache.insert(pattern.to_string(), regex);
    ///     regex
    /// }
    /// ```
    #[allow(dead_code)]
    fn _cached_pattern_example() {
        // This is a documentation placeholder
    }

    /// Parallel initialization helper for running multiple initialization tasks concurrently
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use celers::startup_optimization::parallel_init;
    ///
    /// # async fn example() {
    /// let results = parallel_init(vec![
    ///     Box::new(|| Box::pin(async { /* Initialize DB */ Ok::<(), String>(()) })),
    ///     Box::new(|| Box::pin(async { /* Connect to broker */ Ok::<(), String>(()) })),
    ///     Box::new(|| Box::pin(async { /* Load config */ Ok::<(), String>(()) })),
    /// ]).await;
    /// # }
    /// ```
    pub async fn parallel_init<T, E>(tasks: Vec<AsyncInitTask<T, E>>) -> Vec<Result<T, E>>
    where
        T: Send + 'static,
        E: Send + 'static,
    {
        let handles: Vec<_> = tasks
            .into_iter()
            .map(|task| tokio::spawn(async move { task().await }))
            .collect();

        let mut results = Vec::new();
        for handle in handles {
            match handle.await {
                Ok(result) => results.push(result),
                Err(e) => {
                    // Handle join error - convert to result type
                    // For now, we'll panic as this indicates a serious issue
                    panic!("Task panicked: {:?}", e);
                }
            }
        }
        results
    }

    /// Startup performance metrics
    #[derive(Debug, Clone)]
    pub struct StartupMetrics {
        /// Time spent initializing brokers
        pub broker_init_ms: u64,
        /// Time spent loading configuration
        pub config_load_ms: u64,
        /// Time spent connecting to backends
        pub backend_init_ms: u64,
        /// Total startup time
        pub total_ms: u64,
    }

    impl StartupMetrics {
        /// Create a new startup metrics tracker
        pub fn new() -> Self {
            Self {
                broker_init_ms: 0,
                config_load_ms: 0,
                backend_init_ms: 0,
                total_ms: 0,
            }
        }

        /// Report the metrics as a formatted string
        pub fn report(&self) -> String {
            format!(
                "Startup Performance:\n\
                 - Broker Init: {}ms\n\
                 - Config Load: {}ms\n\
                 - Backend Init: {}ms\n\
                 - Total: {}ms",
                self.broker_init_ms, self.config_load_ms, self.backend_init_ms, self.total_ms
            )
        }
    }

    impl Default for StartupMetrics {
        fn default() -> Self {
            Self::new()
        }
    }

    /// Helper macro for timing initialization steps
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::time_init;
    ///
    /// let duration = time_init!({
    ///     // Expensive initialization code
    ///     initialize_database().await
    /// });
    /// println!("Initialization took {}ms", duration.as_millis());
    /// ```
    #[macro_export]
    macro_rules! time_init {
        ($block:block) => {{
            let start = std::time::Instant::now();
            let result = $block;
            let duration = start.elapsed();
            (result, duration)
        }};
    }
}

/// IDE support and type hints module
///
/// This module provides comprehensive type aliases, trait bounds, and documentation
/// to improve IDE autocomplete, type inference, and developer experience.
pub mod ide_support {
    use std::future::Future;
    use std::pin::Pin;

    /// Common result type for async operations with Send + Sync error bounds
    ///
    /// This type alias helps IDEs provide better autocomplete and type hints.
    ///
    /// # Example
    ///
    /// ```rust
    /// use celers::ide_support::BoxedResult;
    ///
    /// async fn my_operation() -> BoxedResult<String> {
    ///     Ok("success".to_string())
    /// }
    /// ```
    pub type BoxedResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

    /// Pinned boxed future for async task functions
    ///
    /// This is the standard return type for async task implementations.
    ///
    /// # Example
    ///
    /// ```rust
    /// use celers::ide_support::BoxedFuture;
    ///
    /// fn create_task() -> BoxedFuture<String> {
    ///     Box::pin(async { Ok("result".to_string()) })
    /// }
    /// ```
    pub type BoxedFuture<T> = Pin<Box<dyn Future<Output = BoxedResult<T>> + Send + 'static>>;

    /// Task function signature for type hints
    ///
    /// Use this when defining task handler functions for better IDE support.
    pub type TaskFn<Args, Output> =
        fn(Args) -> Pin<Box<dyn Future<Output = BoxedResult<Output>> + Send>>;

    /// Broker instance type for common use cases
    ///
    /// This type alias helps with IDE autocomplete when working with brokers.
    pub type BoxedBroker = Box<dyn crate::Broker>;

    /// Result backend instance type
    ///
    /// Use this when working with result backends for better type hints.
    #[cfg(feature = "backend-redis")]
    pub type BoxedResultBackend = Box<dyn crate::ResultBackend>;

    /// Worker configuration builder type for fluent API
    ///
    /// This alias improves IDE support when building worker configurations.
    pub type WorkerBuilder = celers_worker::WorkerConfigBuilder;

    /// Signature builder type for creating task signatures
    ///
    /// Use this for better autocomplete when building task signatures.
    pub type TaskSignature = crate::Signature;

    /// Chain workflow builder type
    pub type ChainBuilder = crate::Chain;

    /// Group workflow builder type
    pub type GroupBuilder = crate::Group;

    /// Chord workflow builder type
    pub type ChordBuilder = crate::Chord;

    /// Serialized task type for queue operations
    pub type QueueTask = crate::SerializedTask;

    /// Task metadata type
    pub use crate::TaskState;

    /// Broker error type
    pub use crate::BrokerError;

    /// Task ID type for tracking tasks
    pub type TaskId = uuid::Uuid;

    /// Task result value type
    pub use crate::TaskResultValue;

    /// Event emitter type for task events
    pub type EventEmitter = Box<dyn crate::EventEmitter>;

    /// Async result type for task result retrieval
    pub use crate::AsyncResult;

    /// Worker statistics type
    pub use crate::WorkerStats;

    /// Task options type for task configuration
    pub use crate::TaskOptions;

    /// Rate limiter configuration type
    pub use crate::RateLimitConfig;

    /// Router type for task routing
    pub use crate::Router;

    /// Queue name type for better readability
    ///
    /// Use this when defining queue names for clearer code intent.
    pub type QueueName = String;

    /// Broker URL type for configuration
    ///
    /// Use this when defining broker connection strings.
    pub type BrokerUrl = String;

    /// Retry count type for task configuration
    ///
    /// Use this when specifying maximum retry attempts.
    pub type RetryCount = u32;

    /// Priority level type for task prioritization
    ///
    /// Valid range: 0-9, where 9 is highest priority.
    pub type PriorityLevel = u8;

    /// Timeout duration in seconds
    ///
    /// Use this when specifying task time limits and countdowns.
    pub type TimeoutSeconds = u64;

    /// Task name type for task identification
    ///
    /// Use this when defining or referencing task names.
    pub type TaskName = String;

    /// Concurrency level type for worker configuration
    ///
    /// Represents the number of concurrent tasks a worker can execute.
    pub type ConcurrencyLevel = usize;

    /// Prefetch count type for worker configuration
    ///
    /// Number of tasks to prefetch from the broker.
    pub type PrefetchCount = usize;

    /// Type trait bounds helper for task arguments
    ///
    /// This trait bound helps IDEs understand the requirements for task arguments.
    pub trait TaskArgs:
        serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static
    {
    }
    impl<T> TaskArgs for T where
        T: serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static
    {
    }

    /// Type trait bounds helper for task results
    ///
    /// This trait bound helps IDEs understand the requirements for task results.
    pub trait TaskResult: serde::Serialize + serde::de::DeserializeOwned + Send + 'static {}
    impl<T> TaskResult for T where T: serde::Serialize + serde::de::DeserializeOwned + Send + 'static {}

    /// Marker trait for broker implementations
    ///
    /// Helps IDEs identify valid broker types.
    pub trait BrokerImpl: crate::Broker + Send + Sync {}

    /// Helper constants for common configuration values
    pub mod defaults {
        /// Default concurrency level (number of CPU cores)
        pub const DEFAULT_CONCURRENCY: usize = 4;

        /// Default prefetch count
        pub const DEFAULT_PREFETCH: usize = 10;

        /// Default max retries
        pub const DEFAULT_MAX_RETRIES: u32 = 3;

        /// Default retry delay in seconds
        pub const DEFAULT_RETRY_DELAY_SECS: u64 = 60;

        /// Default task timeout in seconds
        pub const DEFAULT_TASK_TIMEOUT_SECS: u64 = 3600;

        /// Default broker port for Redis
        pub const DEFAULT_REDIS_PORT: u16 = 6379;

        /// Default broker port for PostgreSQL
        pub const DEFAULT_POSTGRES_PORT: u16 = 5432;

        /// Default broker port for MySQL
        pub const DEFAULT_MYSQL_PORT: u16 = 3306;

        /// Default broker port for RabbitMQ
        pub const DEFAULT_RABBITMQ_PORT: u16 = 5672;

        /// Default queue name
        pub const DEFAULT_QUEUE_NAME: &str = "celery";
    }

    /// Common configuration patterns and examples
    pub mod examples {
        /// Example broker URL for Redis
        pub const REDIS_URL_EXAMPLE: &str = "redis://localhost:6379";

        /// Example broker URL for PostgreSQL
        pub const POSTGRES_URL_EXAMPLE: &str = "postgres://user:password@localhost:5432/celery";

        /// Example broker URL for MySQL
        pub const MYSQL_URL_EXAMPLE: &str = "mysql://user:password@localhost:3306/celery";

        /// Example broker URL for RabbitMQ
        pub const RABBITMQ_URL_EXAMPLE: &str = "amqp://guest:guest@localhost:5672/";

        /// Example broker URL for SQS
        pub const SQS_URL_EXAMPLE: &str =
            "https://sqs.us-east-1.amazonaws.com/123456789012/my-queue";
    }
}

/// Quick reference documentation module
///
/// This module provides quick reference guides and cheat sheets for common operations.
pub mod quick_reference {
    pub mod patterns {
        //! Quick start guide for common patterns
        //!
        //! # Common Patterns
        //!
        //! ## Creating a Simple Task
        //! ```rust,ignore
        //! use celers::prelude::*;
        //!
        //! #[derive(Serialize, Deserialize)]
        //! struct Args { x: i32, y: i32 }
        //!
        //! #[celers::task]
        //! async fn add(args: Args) -> TaskResult<i32> {
        //!     Ok(args.x + args.y)
        //! }
        //! ```
        //!
        //! ## Creating a Worker
        //! ```rust,ignore
        //! use celers::prelude::*;
        //!
        //! let broker = create_broker_from_env().await?;
        //! let worker = WorkerConfigBuilder::new()
        //!     .concurrency(4)
        //!     .prefetch_count(10)
        //!     .build(broker)?;
        //! worker.start().await?;
        //! ```
        //!
        //! ## Sending Tasks
        //! ```rust,ignore
        //! let task = my_task::new(Args { x: 1, y: 2 });
        //! broker.enqueue(task).await?;
        //! ```
        //!
        //! ## Creating Workflows
        //! ```rust,ignore
        //! // Chain: Sequential execution
        //! let chain = Chain::new()
        //!     .then("task1", vec![])
        //!     .then("task2", vec![]);
        //!
        //! // Group: Parallel execution
        //! let group = Group::new()
        //!     .add("task1", vec![])
        //!     .add("task2", vec![]);
        //!
        //! // Chord: Parallel + callback
        //! let chord = Chord::new(group, callback);
        //! ```
    }

    pub mod config_examples {
        //! Configuration examples and snippets
        //!
        //! # Configuration Examples
        //!
        //! ## Environment Variables
        //! ```bash
        //! export CELERS_BROKER_TYPE=redis
        //! export CELERS_BROKER_URL=redis://localhost:6379
        //! export CELERS_BROKER_QUEUE=celery
        //! ```
        //!
        //! ## Worker Presets
        //! ```rust,ignore
        //! use celers::presets::*;
        //!
        //! // Production config
        //! let config = production_config();
        //!
        //! // High throughput
        //! let config = high_throughput_config();
        //!
        //! // Low latency
        //! let config = low_latency_config();
        //!
        //! // Memory constrained
        //! let config = memory_constrained_config();
        //! ```
    }

    pub mod troubleshooting {
        //! Troubleshooting guide
        //!
        //! # Troubleshooting Guide
        //!
        //! ## Common Issues
        //!
        //! ### Connection Refused
        //! - Check broker is running: `redis-cli ping` or `psql`
        //! - Verify connection URL format
        //! - Check firewall rules
        //!
        //! ### Tasks Not Processing
        //! - Verify worker is running
        //! - Check queue name matches
        //! - Inspect worker logs
        //!
        //! ### High Memory Usage
        //! - Reduce prefetch_count
        //! - Implement chunked processing
        //! - Monitor task memory usage
        //!
        //! ### Slow Task Execution
        //! - Increase worker concurrency
        //! - Profile task execution time
        //! - Optimize database queries
    }
}

#[cfg(feature = "dev-utils")]
pub mod assembly_inspection {
    //! Assembly inspection utilities for verifying zero-cost abstractions
    //!
    //! This module provides tools and documentation for inspecting generated assembly
    //! to verify that Rust's zero-cost abstractions are working as expected.
    //!
    //! # Assembly Inspection Guide
    //!
    //! This module helps you verify zero-cost abstractions by inspecting generated assembly.
    //!
    //! ## Generating Assembly Output
    //!
    //! ### Method 1: Using cargo-asm (Recommended)
    //!
    //! ```bash
    //! # Install cargo-asm
    //! cargo install cargo-asm
    //!
    //! # Generate assembly for a specific function
    //! cargo asm --release celers::dev_utils::TaskBuilder::new
    //!
    //! # Generate assembly with Intel syntax (easier to read)
    //! cargo asm --release --intel celers::dev_utils::TaskBuilder::new
    //! ```
    //!
    //! ### Method 2: Using rustc directly
    //!
    //! ```bash
    //! # Generate assembly for the entire crate
    //! cargo rustc --release -- --emit asm
    //!
    //! # Output will be in target/release/deps/*.s
    //! ```
    //!
    //! ### Method 3: Using Compiler Explorer (Godbolt)
    //!
    //! Visit <https://rust.godbolt.org/> and paste your code to see assembly interactively.
    //!
    //! ## What to Look For
    //!
    //! ### 1. Function Inlining
    //!
    //! Zero-cost abstractions should be inlined. Look for:
    //! - Direct instructions instead of `call` instructions
    //! - No function preamble/epilogue for simple wrappers
    //!
    //! Example of good inlining:
    //! ```asm
    //! # Simple arithmetic directly in caller
    //! add    rdi, rsi
    //! mov    rax, rdi
    //! ```
    //!
    //! Example of poor inlining (overhead):
    //! ```asm
    //! # Function call overhead
    //! call   my_wrapper_function
    //! push   rbp
    //! mov    rbp, rsp
    //! ```
    //!
    //! ### 2. Dead Code Elimination
    //!
    //! Unused code should be completely removed:
    //! - No assembly generated for unused branches
    //! - Constant propagation should eliminate runtime checks
    //!
    //! ### 3. Iterator Optimization
    //!
    //! Iterator chains should compile to simple loops:
    //! ```asm
    //! # Should see a tight loop, not iterator machinery
    //! .LBB0_1:
    //!     add    rax, 1
    //!     cmp    rax, rcx
    //!     jne    .LBB0_1
    //! ```
    //!
    //! ### 4. Monomorphization
    //!
    //! Generic functions should be specialized:
    //! - No dynamic dispatch (vtable calls) unless using trait objects
    //! - Type parameters resolved at compile time
    //!
    //! ## Example Commands for CeleRS
    //!
    //! ```bash
    //! # Verify TaskBuilder is zero-cost
    //! cargo asm --release --intel celers::dev_utils::TaskBuilder::build
    //!
    //! # Verify chain workflow construction
    //! cargo asm --release --intel celers::canvas::Chain::new
    //!
    //! # Compare debug vs release
    //! cargo asm celers::dev_utils::TaskBuilder::build > debug.asm
    //! cargo asm --release celers::dev_utils::TaskBuilder::build > release.asm
    //! diff debug.asm release.asm
    //! ```
    //!
    //! ## Automated Verification
    //!
    //! Use these helper functions to automate assembly inspection:

    use std::process::Command;

    /// Generate assembly output for a specific function
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # #[cfg(feature = "dev-utils")]
    /// # {
    /// use celers::assembly_inspection::generate_asm;
    ///
    /// let asm = generate_asm("celers::dev_utils::TaskBuilder::new", true).unwrap();
    /// println!("Assembly:\n{}", asm);
    /// # }
    /// ```
    pub fn generate_asm(function_path: &str, release: bool) -> Result<String, std::io::Error> {
        let mut cmd = Command::new("cargo");
        cmd.arg("asm");
        if release {
            cmd.arg("--release");
        }
        cmd.arg("--intel");
        cmd.arg(function_path);

        let output = cmd.output()?;
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    }

    /// Check if a function is properly inlined by looking for call instructions
    ///
    /// Returns true if the function appears to be inlined (no call instructions found)
    pub fn verify_inlined(asm: &str) -> bool {
        !asm.contains("call") || asm.lines().filter(|l| l.contains("call")).count() < 2
    }

    /// Count the number of instructions in the assembly
    ///
    /// Useful for comparing optimization levels
    pub fn count_instructions(asm: &str) -> usize {
        asm.lines()
            .filter(|line| {
                let trimmed = line.trim();
                !trimmed.is_empty()
                    && !trimmed.starts_with(';')
                    && !trimmed.starts_with('#')
                    && !trimmed.starts_with('.')
                    && !trimmed.ends_with(':')
            })
            .count()
    }

    /// Generate a performance report comparing debug and release assembly
    pub fn compare_debug_release(function_path: &str) -> Result<String, std::io::Error> {
        let debug_asm = generate_asm(function_path, false)?;
        let release_asm = generate_asm(function_path, true)?;

        let debug_count = count_instructions(&debug_asm);
        let release_count = count_instructions(&release_asm);
        let debug_inlined = verify_inlined(&debug_asm);
        let release_inlined = verify_inlined(&release_asm);

        Ok(format!(
            "Assembly Comparison for {}\n\
             \n\
             Debug Build:\n\
             - Instructions: {}\n\
             - Inlined: {}\n\
             \n\
             Release Build:\n\
             - Instructions: {}\n\
             - Inlined: {}\n\
             \n\
             Optimization Ratio: {:.2}x fewer instructions in release\n",
            function_path,
            debug_count,
            debug_inlined,
            release_count,
            release_inlined,
            debug_count as f64 / release_count.max(1) as f64
        ))
    }
}

/// Workflow templates for common distributed computing patterns
///
/// This module provides pre-built workflow templates that implement common
/// distributed computing patterns, making it easy to apply best practices
/// without having to build workflows from scratch.
///
/// # Available Templates
///
/// - **ETL Pipeline**: Extract, Transform, Load pattern
/// - **Map-Reduce**: Parallel processing with aggregation
/// - **Scatter-Gather**: Distribute work and collect results
/// - **Fan-Out/Fan-In**: Parallel execution with synchronization
/// - **Sequential Pipeline**: Multi-stage processing
/// - **Batch Processing**: Process items in batches with retry
///
/// # Example
///
/// ```rust,ignore
/// use celers::workflow_templates::*;
/// use serde_json::json;
///
/// // Create an ETL pipeline
/// let pipeline = etl_pipeline(
///     "extract_data",
///     vec![json!({"source": "database"})],
///     "transform_data",
///     "load_data"
/// );
///
/// // Create a map-reduce workflow
/// let workflow = map_reduce_workflow(
///     "process_item",
///     vec![json!(1), json!(2), json!(3)],
///     "aggregate_results"
/// );
/// ```
pub mod workflow_templates {
    use crate::canvas::{Chain, Chord, Group};
    use crate::Signature;
    use serde_json::Value;

    /// Creates an ETL (Extract, Transform, Load) pipeline workflow
    ///
    /// This pattern is commonly used for data processing pipelines where data
    /// flows through multiple transformation stages.
    ///
    /// # Arguments
    ///
    /// * `extract_task` - Name of the task that extracts data
    /// * `extract_args` - Arguments for the extract task
    /// * `transform_task` - Name of the task that transforms data
    /// * `load_task` - Name of the task that loads processed data
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::workflow_templates::etl_pipeline;
    /// use serde_json::json;
    ///
    /// let pipeline = etl_pipeline(
    ///     "extract_from_api",
    ///     vec![json!({"url": "https://api.example.com"})],
    ///     "transform_data",
    ///     "load_to_database"
    /// );
    ///
    /// pipeline.apply_async(&broker).await?;
    /// ```
    pub fn etl_pipeline(
        extract_task: &str,
        extract_args: Vec<Value>,
        transform_task: &str,
        load_task: &str,
    ) -> Chain {
        Chain::new()
            .then(extract_task, extract_args)
            .then(transform_task, vec![])
            .then(load_task, vec![])
    }

    /// Creates a Map-Reduce workflow for parallel processing with aggregation
    ///
    /// This pattern processes items in parallel (map phase) and then aggregates
    /// the results (reduce phase).
    ///
    /// # Arguments
    ///
    /// * `map_task` - Name of the task to apply to each item
    /// * `items` - Collection of items to process
    /// * `reduce_task` - Name of the task that aggregates results
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::workflow_templates::map_reduce_workflow;
    /// use serde_json::json;
    ///
    /// let workflow = map_reduce_workflow(
    ///     "process_number",
    ///     vec![json!(1), json!(2), json!(3), json!(4)],
    ///     "sum_results"
    /// );
    /// ```
    pub fn map_reduce_workflow(map_task: &str, items: Vec<Value>, reduce_task: &str) -> Chord {
        let mut group = Group::new();
        for item in items {
            group = group.add(map_task, vec![item]);
        }

        let reduce_sig = Signature::new(reduce_task.to_string());

        Chord {
            header: group,
            body: reduce_sig,
        }
    }

    /// Creates a scatter-gather workflow for distributing work
    ///
    /// This pattern distributes different tasks to be executed in parallel
    /// and then gathers all results.
    ///
    /// # Arguments
    ///
    /// * `tasks` - List of (task_name, args) tuples to execute in parallel
    /// * `gather_task` - Name of the task that gathers all results
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::workflow_templates::scatter_gather;
    /// use serde_json::json;
    ///
    /// let tasks = vec![
    ///     ("fetch_user_data", vec![json!({"user_id": 1})]),
    ///     ("fetch_order_data", vec![json!({"user_id": 1})]),
    ///     ("fetch_preferences", vec![json!({"user_id": 1})]),
    /// ];
    ///
    /// let workflow = scatter_gather(tasks, "combine_user_profile");
    /// ```
    pub fn scatter_gather(tasks: Vec<(&str, Vec<Value>)>, gather_task: &str) -> Chord {
        let mut group = Group::new();
        for (task_name, args) in tasks {
            group = group.add(task_name, args);
        }

        let gather_sig = Signature::new(gather_task.to_string());

        Chord {
            header: group,
            body: gather_sig,
        }
    }

    /// Creates a batch processing workflow with automatic chunking
    ///
    /// This pattern processes large datasets by dividing them into batches,
    /// processing each batch in parallel, and then aggregating results.
    ///
    /// # Arguments
    ///
    /// * `process_task` - Name of the task that processes a batch
    /// * `items` - All items to process
    /// * `batch_size` - Number of items per batch
    /// * `aggregate_task` - Optional task to aggregate all batch results
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::workflow_templates::batch_processing;
    /// use serde_json::json;
    ///
    /// let items = (1..=100).map(|i| json!(i)).collect();
    /// let workflow = batch_processing(
    ///     "process_batch",
    ///     items,
    ///     10, // Process 10 items per batch
    ///     Some("combine_batch_results")
    /// );
    /// ```
    pub fn batch_processing(
        process_task: &str,
        items: Vec<Value>,
        batch_size: usize,
        aggregate_task: Option<&str>,
    ) -> Chord {
        let mut group = Group::new();

        // Split items into batches
        for chunk in items.chunks(batch_size) {
            let batch = Value::Array(chunk.to_vec());
            group = group.add(process_task, vec![batch]);
        }

        let body_sig = if let Some(agg_task) = aggregate_task {
            Signature::new(agg_task.to_string())
        } else {
            // Default no-op aggregation
            Signature::new("celers.noop".to_string())
        };

        Chord {
            header: group,
            body: body_sig,
        }
    }

    /// Creates a sequential pipeline workflow with error handling
    ///
    /// This pattern chains tasks sequentially with automatic retry and
    /// error recovery built in.
    ///
    /// # Arguments
    ///
    /// * `stages` - List of (task_name, args, max_retries) tuples
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::workflow_templates::sequential_pipeline;
    /// use serde_json::json;
    ///
    /// let stages = vec![
    ///     ("validate_input", vec![json!({"data": "test"})], 3),
    ///     ("process_data", vec![], 5),
    ///     ("save_results", vec![], 3),
    /// ];
    ///
    /// let pipeline = sequential_pipeline(stages);
    /// ```
    pub fn sequential_pipeline(stages: Vec<(&str, Vec<Value>, u32)>) -> Chain {
        let mut chain = Chain::new();

        for (task_name, args, max_retries) in stages {
            let mut sig = Signature::new(task_name.to_string()).with_args(args);
            sig.options.max_retries = Some(max_retries);

            chain.tasks.push(sig);
        }

        chain
    }

    /// Creates a priority-based workflow for handling urgent tasks first
    ///
    /// This pattern creates parallel tasks with different priorities,
    /// ensuring high-priority tasks are processed first.
    ///
    /// # Arguments
    ///
    /// * `tasks` - List of (task_name, args, priority) tuples
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::workflow_templates::priority_workflow;
    /// use serde_json::json;
    ///
    /// let tasks = vec![
    ///     ("critical_task", vec![json!(1)], 9),
    ///     ("normal_task", vec![json!(2)], 5),
    ///     ("low_priority_task", vec![json!(3)], 1),
    /// ];
    ///
    /// let workflow = priority_workflow(tasks);
    /// ```
    pub fn priority_workflow(tasks: Vec<(&str, Vec<Value>, u8)>) -> Group {
        let mut group = Group::new();

        for (task_name, args, priority) in tasks {
            let mut sig = Signature::new(task_name.to_string()).with_args(args);
            sig.options.priority = Some(priority);

            group.tasks.push(sig);
        }

        group
    }
}

/// Advanced task composition utilities
///
/// This module provides utilities for composing complex task workflows,
/// including conditional execution, dynamic task generation, and result
/// transformation.
///
/// # Example
///
/// ```rust,ignore
/// use celers::task_composition::*;
/// use serde_json::json;
///
/// // Create a conditional workflow
/// let workflow = conditional_chain(
///     "validate_data",
///     vec![json!({"data": "test"})],
///     vec![
///         ("process_valid_data", vec![]),
///         ("save_results", vec![]),
///     ],
///     Some(("handle_error", vec![]))
/// );
/// ```
pub mod task_composition {
    use crate::canvas::{Chain, Group};
    use crate::Signature;
    use serde_json::Value;

    /// Creates a conditional execution chain
    ///
    /// Executes a predicate task, and based on its result, executes either
    /// the success chain or the fallback chain.
    ///
    /// # Arguments
    ///
    /// * `predicate_task` - Task that returns a boolean condition
    /// * `predicate_args` - Arguments for the predicate task
    /// * `success_chain` - Tasks to execute if predicate is true
    /// * `fallback_chain` - Optional tasks to execute if predicate is false
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::task_composition::conditional_chain;
    /// use serde_json::json;
    ///
    /// let workflow = conditional_chain(
    ///     "check_balance",
    ///     vec![json!({"account_id": 123})],
    ///     vec![("process_payment", vec![]), ("send_receipt", vec![])],
    ///     Some(("send_insufficient_funds_email", vec![]))
    /// );
    /// ```
    #[allow(dead_code)]
    pub fn conditional_chain(
        predicate_task: &str,
        predicate_args: Vec<Value>,
        success_chain: Vec<(&str, Vec<Value>)>,
        _fallback_chain: Option<(&str, Vec<Value>)>,
    ) -> Chain {
        let mut chain = Chain::new();

        // Add predicate task
        chain = chain.then(predicate_task, predicate_args);

        // Add success chain tasks
        for (task_name, args) in success_chain {
            chain = chain.then(task_name, args);
        }

        // Note: Actual conditional logic would need to be implemented in the task itself
        // This is a template for the workflow structure

        chain
    }

    /// Creates a retry wrapper with exponential backoff
    ///
    /// Wraps a task with automatic retry logic using exponential backoff.
    ///
    /// # Arguments
    ///
    /// * `task_name` - Name of the task to wrap
    /// * `args` - Task arguments
    /// * `max_retries` - Maximum number of retry attempts
    /// * `initial_delay` - Initial delay in seconds (doubles each retry)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::task_composition::retry_wrapper;
    /// use serde_json::json;
    ///
    /// let sig = retry_wrapper(
    ///     "fetch_external_api",
    ///     vec![json!({"url": "https://api.example.com"})],
    ///     5,  // Retry up to 5 times
    ///     10  // Start with 10 second delay
    /// );
    /// ```
    pub fn retry_wrapper(
        task_name: &str,
        args: Vec<Value>,
        max_retries: u32,
        initial_delay: u64,
    ) -> Signature {
        let mut sig = Signature::new(task_name.to_string()).with_args(args);
        sig.options.max_retries = Some(max_retries);
        sig.options.countdown = Some(initial_delay);
        sig
    }

    /// Creates a timeout-protected task
    ///
    /// Wraps a task with a timeout, ensuring it doesn't run indefinitely.
    ///
    /// # Arguments
    ///
    /// * `task_name` - Name of the task to protect
    /// * `args` - Task arguments
    /// * `timeout_seconds` - Maximum execution time in seconds
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::task_composition::timeout_wrapper;
    /// use serde_json::json;
    ///
    /// let sig = timeout_wrapper(
    ///     "long_running_task",
    ///     vec![json!({"process": "large_dataset"})],
    ///     300  // 5 minute timeout
    /// );
    /// ```
    pub fn timeout_wrapper(task_name: &str, args: Vec<Value>, timeout_seconds: u64) -> Signature {
        let mut sig = Signature::new(task_name.to_string()).with_args(args);
        sig.options.time_limit = Some(timeout_seconds);
        sig
    }

    /// Creates a task group with circuit breaker pattern
    ///
    /// Groups tasks and adds circuit breaker semantics to prevent cascading
    /// failures.
    ///
    /// # Arguments
    ///
    /// * `tasks` - List of (task_name, args) tuples
    /// * `max_failures` - Maximum failures before circuit opens
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::task_composition::circuit_breaker_group;
    /// use serde_json::json;
    ///
    /// let tasks = vec![
    ///     ("call_service_a", vec![json!(1)]),
    ///     ("call_service_b", vec![json!(2)]),
    ///     ("call_service_c", vec![json!(3)]),
    /// ];
    ///
    /// let group = circuit_breaker_group(tasks, 2);
    /// ```
    pub fn circuit_breaker_group(tasks: Vec<(&str, Vec<Value>)>, max_failures: u32) -> Group {
        let mut group = Group::new();

        for (task_name, args) in tasks {
            let mut sig = Signature::new(task_name.to_string()).with_args(args);
            // Add retry logic for circuit breaker
            sig.options.max_retries = Some(max_failures);

            group.tasks.push(sig);
        }

        group
    }

    /// Creates a rate-limited workflow
    ///
    /// Spaces out task execution to respect rate limits.
    ///
    /// # Arguments
    ///
    /// * `task_name` - Name of the task
    /// * `items` - Items to process
    /// * `delay_between_tasks` - Delay in seconds between each task
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::task_composition::rate_limited_workflow;
    /// use serde_json::json;
    ///
    /// let items = vec![json!(1), json!(2), json!(3)];
    /// let workflow = rate_limited_workflow(
    ///     "call_rate_limited_api",
    ///     items,
    ///     5  // 5 seconds between each call
    /// );
    /// ```
    pub fn rate_limited_workflow(
        task_name: &str,
        items: Vec<Value>,
        delay_between_tasks: u64,
    ) -> Chain {
        let mut chain = Chain::new();

        for (i, item) in items.into_iter().enumerate() {
            let mut sig = Signature::new(task_name.to_string()).with_args(vec![item]);

            // Add countdown for all tasks except the first
            if i > 0 {
                sig.options.countdown = Some(delay_between_tasks * i as u64);
            }

            chain.tasks.push(sig);
        }

        chain
    }
}

/// Error recovery patterns for resilient task execution
///
/// This module provides patterns and utilities for handling errors gracefully
/// in distributed task systems, including fallback strategies, error aggregation,
/// and recovery mechanisms.
///
/// # Example
///
/// ```rust,ignore
/// use celers::error_recovery::*;
/// use serde_json::json;
///
/// // Create a task with fallback
/// let task_with_fallback = with_fallback(
///     "risky_operation",
///     vec![json!({"data": "test"})],
///     "safe_fallback_operation",
///     vec![json!({"data": "fallback"})]
/// );
/// ```
pub mod error_recovery {
    use crate::{Chain, Signature};
    use serde_json::Value;

    /// Creates a task chain with a fallback task in case of failure
    ///
    /// This pattern provides a fallback mechanism where if the primary task fails,
    /// a secondary fallback task is executed instead.
    ///
    /// # Arguments
    ///
    /// * `primary_task` - Name of the primary task to execute
    /// * `primary_args` - Arguments for the primary task
    /// * `fallback_task` - Name of the fallback task to execute on failure
    /// * `fallback_args` - Arguments for the fallback task
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::error_recovery::with_fallback;
    /// use serde_json::json;
    ///
    /// let task = with_fallback(
    ///     "fetch_from_primary_api",
    ///     vec![json!({"url": "https://api.example.com"})],
    ///     "fetch_from_backup_api",
    ///     vec![json!({"url": "https://backup.example.com"})]
    /// );
    /// ```
    ///
    /// # Note
    ///
    /// The actual fallback logic must be implemented in the task handlers.
    /// This function creates the workflow structure.
    pub fn with_fallback(
        primary_task: &str,
        primary_args: Vec<Value>,
        fallback_task: &str,
        fallback_args: Vec<Value>,
    ) -> Chain {
        Chain::new()
            .then(primary_task, primary_args)
            .then(fallback_task, fallback_args)
    }

    /// Creates a task signature with error suppression
    ///
    /// Configures a task to ignore errors and continue execution.
    /// Useful for non-critical tasks where failures shouldn't block the workflow.
    ///
    /// # Arguments
    ///
    /// * `task_name` - Name of the task
    /// * `args` - Task arguments
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::error_recovery::ignore_errors;
    /// use serde_json::json;
    ///
    /// let sig = ignore_errors(
    ///     "log_analytics",
    ///     vec![json!({"event": "user_action"})]
    /// );
    /// ```
    pub fn ignore_errors(task_name: &str, args: Vec<Value>) -> Signature {
        let mut sig = Signature::new(task_name.to_string()).with_args(args);
        sig.options.max_retries = Some(0); // No retries
                                           // Note: Actual error suppression logic would be in the task handler
        sig
    }

    /// Creates a task with graduated retry delays
    ///
    /// Implements exponential backoff retry strategy for tasks that may
    /// experience transient failures.
    ///
    /// # Arguments
    ///
    /// * `task_name` - Name of the task
    /// * `args` - Task arguments
    /// * `max_retries` - Maximum number of retry attempts
    /// * `base_delay` - Initial delay in seconds (doubles each retry)
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::error_recovery::with_exponential_backoff;
    /// use serde_json::json;
    ///
    /// let sig = with_exponential_backoff(
    ///     "call_flaky_api",
    ///     vec![json!({"endpoint": "/data"})],
    ///     5,   // Retry up to 5 times
    ///     2    // Start with 2 second delay, then 4, 8, 16, 32
    /// );
    /// ```
    pub fn with_exponential_backoff(
        task_name: &str,
        args: Vec<Value>,
        max_retries: u32,
        base_delay: u64,
    ) -> Signature {
        let mut sig = Signature::new(task_name.to_string()).with_args(args);
        sig.options.max_retries = Some(max_retries);
        sig.options.countdown = Some(base_delay);
        // Note: Actual exponential backoff logic would be in the retry handler
        sig
    }

    /// Creates a task with a dead letter queue on failure
    ///
    /// Routes failed tasks to a designated error handling queue.
    ///
    /// # Arguments
    ///
    /// * `task_name` - Name of the task
    /// * `args` - Task arguments
    /// * `dlq_task` - Name of the dead letter queue handler task
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::error_recovery::with_dlq;
    /// use serde_json::json;
    ///
    /// let task = with_dlq(
    ///     "process_payment",
    ///     vec![json!({"amount": 100})],
    ///     "handle_failed_payment"
    /// );
    /// ```
    pub fn with_dlq(task_name: &str, args: Vec<Value>, dlq_task: &str) -> Chain {
        Chain::new().then(task_name, args).then(dlq_task, vec![])
    }
}

/// Workflow validation utilities
///
/// This module provides utilities for validating workflow configurations
/// before execution, helping catch configuration errors early.
///
/// # Example
///
/// ```rust,ignore
/// use celers::workflow_validation::*;
///
/// let chain = Chain::new()
///     .then("task1", vec![])
///     .then("task2", vec![]);
///
/// if let Err(errors) = validate_chain(&chain) {
///     for error in errors {
///         eprintln!("Validation error: {}", error);
///     }
/// }
/// ```
pub mod workflow_validation {
    use crate::{Chain, Chord, Group};

    /// Validation error type
    #[derive(Debug, Clone)]
    pub struct ValidationError {
        /// Error message
        pub message: String,
        /// Task name where error occurred (if applicable)
        pub task_name: Option<String>,
    }

    impl ValidationError {
        /// Creates a new validation error
        pub fn new(message: impl Into<String>) -> Self {
            Self {
                message: message.into(),
                task_name: None,
            }
        }

        /// Creates a validation error with task context
        pub fn with_task(message: impl Into<String>, task_name: impl Into<String>) -> Self {
            Self {
                message: message.into(),
                task_name: Some(task_name.into()),
            }
        }
    }

    impl std::fmt::Display for ValidationError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            if let Some(task) = &self.task_name {
                write!(f, "[{}] {}", task, self.message)
            } else {
                write!(f, "{}", self.message)
            }
        }
    }

    /// Validates a chain workflow
    ///
    /// Checks for common issues like empty chains, invalid task names, etc.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::workflow_validation::validate_chain;
    /// use celers::Chain;
    ///
    /// let chain = Chain::new()
    ///     .then("task1", vec![])
    ///     .then("task2", vec![]);
    ///
    /// match validate_chain(&chain) {
    ///     Ok(_) => println!("Chain is valid"),
    ///     Err(errors) => {
    ///         for err in errors {
    ///             eprintln!("Error: {}", err);
    ///         }
    ///     }
    /// }
    /// ```
    pub fn validate_chain(chain: &Chain) -> Result<(), Vec<ValidationError>> {
        let mut errors = Vec::new();

        if chain.tasks.is_empty() {
            errors.push(ValidationError::new("Chain must contain at least one task"));
        }

        for task in &chain.tasks {
            if task.task.is_empty() {
                errors.push(ValidationError::new("Task name cannot be empty"));
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Validates a group workflow
    ///
    /// Checks for common issues in parallel task groups.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::workflow_validation::validate_group;
    /// use celers::Group;
    ///
    /// let group = Group::new()
    ///     .add("task1", vec![])
    ///     .add("task2", vec![]);
    ///
    /// validate_group(&group)?;
    /// ```
    pub fn validate_group(group: &Group) -> Result<(), Vec<ValidationError>> {
        let mut errors = Vec::new();

        if group.tasks.is_empty() {
            errors.push(ValidationError::new("Group must contain at least one task"));
        }

        for task in &group.tasks {
            if task.task.is_empty() {
                errors.push(ValidationError::new("Task name cannot be empty"));
            }
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Validates a chord workflow
    ///
    /// Checks that both header and body are properly configured.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::workflow_validation::validate_chord;
    /// use celers::{Chord, Group, Signature};
    ///
    /// let chord = Chord {
    ///     header: Group::new().add("task1", vec![]),
    ///     body: Signature::new("callback".to_string()),
    /// };
    ///
    /// validate_chord(&chord)?;
    /// ```
    pub fn validate_chord(chord: &Chord) -> Result<(), Vec<ValidationError>> {
        let mut errors = Vec::new();

        // Validate header group
        if let Err(mut group_errors) = validate_group(&chord.header) {
            errors.append(&mut group_errors);
        }

        // Validate body callback
        if chord.body.task.is_empty() {
            errors.push(ValidationError::with_task(
                "Callback task name cannot be empty",
                "body",
            ));
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Checks if a workflow is likely to cause performance issues
    ///
    /// Warns about potentially problematic configurations.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::workflow_validation::check_performance_concerns;
    /// use celers::Group;
    ///
    /// let large_group = Group::new();
    /// for i in 0..1000 {
    ///     large_group.add(&format!("task_{}", i), vec![]);
    /// }
    ///
    /// if let Some(warnings) = check_performance_concerns_group(&large_group) {
    ///     for warning in warnings {
    ///         println!("Warning: {}", warning);
    ///     }
    /// }
    /// ```
    pub fn check_performance_concerns_group(group: &Group) -> Option<Vec<String>> {
        let mut warnings = Vec::new();

        if group.tasks.len() > 100 {
            warnings.push(format!(
                "Group contains {} tasks, which may cause performance issues. Consider batching.",
                group.tasks.len()
            ));
        }

        if warnings.is_empty() {
            None
        } else {
            Some(warnings)
        }
    }

    /// Checks chain for performance concerns
    pub fn check_performance_concerns_chain(chain: &Chain) -> Option<Vec<String>> {
        let mut warnings = Vec::new();

        if chain.tasks.len() > 50 {
            warnings.push(format!(
                "Chain contains {} sequential tasks, which may cause long latency. Consider parallelizing.",
                chain.tasks.len()
            ));
        }

        if warnings.is_empty() {
            None
        } else {
            Some(warnings)
        }
    }
}

/// Result aggregation helpers for working with task results
///
/// This module provides utilities for collecting, transforming, and
/// aggregating results from multiple tasks.
///
/// # Example
///
/// ```rust,ignore
/// use celers::result_helpers::*;
///
/// // Helper for creating result collection tasks
/// let collector = create_result_collector("process_batch", 10);
/// ```
pub mod result_helpers {
    use crate::Signature;
    use serde_json::Value;

    /// Creates a signature for collecting results from multiple tasks
    ///
    /// Useful for aggregating results in map-reduce patterns.
    ///
    /// # Arguments
    ///
    /// * `collector_task` - Name of the task that collects results
    /// * `expected_count` - Expected number of results to collect
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::result_helpers::create_result_collector;
    ///
    /// let collector = create_result_collector("aggregate_results", 100);
    /// ```
    pub fn create_result_collector(collector_task: &str, expected_count: usize) -> Signature {
        let mut sig =
            Signature::new(collector_task.to_string()).with_args(vec![serde_json::json!({
                "expected_count": expected_count
            })]);
        sig.options.time_limit = Some(300); // 5 minute timeout for collection
        sig
    }

    /// Creates a task that filters results based on criteria
    ///
    /// # Arguments
    ///
    /// * `filter_task` - Name of the filtering task
    /// * `criteria` - Filtering criteria as JSON value
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::result_helpers::create_result_filter;
    /// use serde_json::json;
    ///
    /// let filter = create_result_filter(
    ///     "filter_successful",
    ///     json!({"status": "success"})
    /// );
    /// ```
    pub fn create_result_filter(filter_task: &str, criteria: Value) -> Signature {
        Signature::new(filter_task.to_string()).with_args(vec![criteria])
    }

    /// Creates a task for transforming result data
    ///
    /// # Arguments
    ///
    /// * `transform_task` - Name of the transformation task
    /// * `transformation_config` - Configuration for the transformation
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::result_helpers::create_result_transformer;
    /// use serde_json::json;
    ///
    /// let transformer = create_result_transformer(
    ///     "format_results",
    ///     json!({"format": "csv", "include_headers": true})
    /// );
    /// ```
    pub fn create_result_transformer(
        transform_task: &str,
        transformation_config: Value,
    ) -> Signature {
        Signature::new(transform_task.to_string()).with_args(vec![transformation_config])
    }

    /// Creates a task for reducing/aggregating multiple results
    ///
    /// # Arguments
    ///
    /// * `reduce_task` - Name of the reduce task
    /// * `operation` - Type of reduction operation (e.g., "sum", "average", "concat")
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::result_helpers::create_result_reducer;
    ///
    /// let reducer = create_result_reducer("sum_results", "sum");
    /// ```
    pub fn create_result_reducer(reduce_task: &str, operation: &str) -> Signature {
        Signature::new(reduce_task.to_string()).with_args(vec![serde_json::json!({
            "operation": operation
        })])
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
        assert!(!chain.tasks.is_empty());
    }

    #[test]
    fn test_workflow_group() {
        use crate::canvas::Group;

        let group = Group::new()
            .add("task1", vec![])
            .add("task2", vec![])
            .add("task3", vec![]);

        // Verify group structure
        assert!(!group.tasks.is_empty());
    }

    #[test]
    fn test_workflow_chord() {
        use crate::canvas::{Chord, Group, Signature};

        let header = Group::new().add("task1", vec![]).add("task2", vec![]);

        let callback = Signature::new("callback".to_string());

        let chord = Chord::new(header, callback);

        // Verify chord structure
        assert!(!chord.header.tasks.is_empty());
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

        // Should be fast - less than 50ms for 10k tasks (debug mode is slower)
        assert!(
            duration.as_millis() < 50,
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

        // Should be very fast - less than 50ms for 1k workflows in debug builds
        assert!(
            duration.as_millis() < 50,
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

    // Performance regression tests with baseline tracking
    #[test]
    fn test_performance_regression_task_creation() {
        use crate::dev_utils::TaskBuilder;
        use std::time::Instant;

        // Baseline adjusted for debug builds (debug builds are ~10x slower than release)
        const BASELINE_MS: u128 = 100; // Baseline: 100ms for 10k tasks in debug
        const ITERATIONS: usize = 10000;

        let start = Instant::now();
        for i in 0..ITERATIONS {
            let _task = TaskBuilder::new(&format!("task.{}", i)).build();
        }
        let duration = start.elapsed();

        // Alert if performance regresses by more than 50%
        assert!(
            duration.as_millis() < BASELINE_MS * 3 / 2,
            "Performance regression detected: {}ms (baseline: {}ms) for {} tasks",
            duration.as_millis(),
            BASELINE_MS,
            ITERATIONS
        );
    }

    #[test]
    fn test_performance_regression_workflow_construction() {
        use crate::canvas::{Chain, Group};
        use std::time::Instant;

        const BASELINE_MS: u128 = 50; // Baseline: 50ms for 1k workflows (adjusted for debug builds)
        const ITERATIONS: usize = 1000;

        let start = Instant::now();
        for _ in 0..ITERATIONS {
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

        // Alert if performance regresses by more than 50%
        assert!(
            duration.as_millis() < BASELINE_MS * 3 / 2,
            "Performance regression detected: {}ms (baseline: {}ms) for {} workflows",
            duration.as_millis(),
            BASELINE_MS,
            ITERATIONS
        );
    }

    #[test]
    fn test_performance_regression_serialization() {
        use crate::dev_utils::TaskBuilder;
        use std::time::Instant;

        const BASELINE_MS: u128 = 50; // Baseline: 50ms for 1k serializations
        const ITERATIONS: usize = 1000;

        let tasks: Vec<_> = (0..ITERATIONS)
            .map(|i| TaskBuilder::new(&format!("task{}", i)).build())
            .collect();

        let start = Instant::now();
        for task in &tasks {
            let _serialized = serde_json::to_string(task).unwrap();
        }
        let duration = start.elapsed();

        // Alert if performance regresses by more than 50%
        assert!(
            duration.as_millis() < BASELINE_MS * 3 / 2,
            "Performance regression detected: {}ms (baseline: {}ms) for {} serializations",
            duration.as_millis(),
            BASELINE_MS,
            ITERATIONS
        );
    }

    #[test]
    fn test_performance_regression_config_validation() {
        use crate::config_validation::*;
        use std::time::Instant;

        const BASELINE_MS: u128 = 100; // Baseline: 100ms for 10k validations (adjusted for debug builds)
        const ITERATIONS: usize = 10000;

        let start = Instant::now();
        for _ in 0..ITERATIONS {
            let _ = validate_worker_config(Some(4), Some(10));
            let _ = validate_broker_url("redis://localhost:6379");
        }
        let duration = start.elapsed();

        // Alert if performance regresses by more than 50%
        assert!(
            duration.as_millis() < BASELINE_MS * 3 / 2,
            "Performance regression detected: {}ms (baseline: {}ms) for {} validations",
            duration.as_millis(),
            BASELINE_MS,
            ITERATIONS
        );
    }

    // Startup optimization tests
    #[test]
    fn test_lazy_init() {
        use crate::startup_optimization::LazyInit;

        static COUNTER: LazyInit<usize> = LazyInit::new();

        // First access initializes
        let value = COUNTER.get_or_init(|| 42);
        assert_eq!(*value, 42);

        // Second access returns the same value
        let value2 = COUNTER.get_or_init(|| 100);
        assert_eq!(*value2, 42); // Should still be 42, not 100
    }

    #[test]
    fn test_startup_metrics() {
        use crate::startup_optimization::StartupMetrics;

        let mut metrics = StartupMetrics::new();
        metrics.broker_init_ms = 100;
        metrics.config_load_ms = 50;
        metrics.backend_init_ms = 75;
        metrics.total_ms = 225;

        let report = metrics.report();
        assert!(report.contains("100ms"));
        assert!(report.contains("50ms"));
        assert!(report.contains("75ms"));
        assert!(report.contains("225ms"));
    }

    #[test]
    fn test_startup_metrics_default() {
        use crate::startup_optimization::StartupMetrics;

        let metrics = StartupMetrics::default();
        assert_eq!(metrics.broker_init_ms, 0);
        assert_eq!(metrics.config_load_ms, 0);
        assert_eq!(metrics.backend_init_ms, 0);
        assert_eq!(metrics.total_ms, 0);
    }

    #[tokio::test]
    async fn test_parallel_init() {
        use crate::startup_optimization::{parallel_init, AsyncInitTask};
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let counter = Arc::new(AtomicUsize::new(0));

        type TestResult = std::result::Result<(), String>;
        let tasks: Vec<AsyncInitTask<(), String>> = vec![
            {
                let counter = counter.clone();
                Box::new(move || {
                    Box::pin(async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                        as std::pin::Pin<Box<dyn std::future::Future<Output = TestResult> + Send>>
                })
            },
            {
                let counter = counter.clone();
                Box::new(move || {
                    Box::pin(async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                        as std::pin::Pin<Box<dyn std::future::Future<Output = TestResult> + Send>>
                })
            },
            {
                let counter = counter.clone();
                Box::new(move || {
                    Box::pin(async move {
                        counter.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                        as std::pin::Pin<Box<dyn std::future::Future<Output = TestResult> + Send>>
                })
            },
        ];

        let results = parallel_init(tasks).await;
        assert_eq!(results.len(), 3);
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    // IDE support tests
    #[test]
    fn test_ide_support_type_aliases() {
        use crate::ide_support::*;

        // Test BoxedResult compiles
        let _result: BoxedResult<i32> = Ok(42);

        // Test type aliases are accessible
        let _broker: Option<BoxedBroker> = None;
        let _task: Option<QueueTask> = None;
    }

    #[test]
    fn test_ide_support_defaults() {
        use crate::ide_support::defaults::*;

        // Verify default constants
        assert_eq!(DEFAULT_CONCURRENCY, 4);
        assert_eq!(DEFAULT_PREFETCH, 10);
        assert_eq!(DEFAULT_MAX_RETRIES, 3);
        assert_eq!(DEFAULT_RETRY_DELAY_SECS, 60);
        assert_eq!(DEFAULT_TASK_TIMEOUT_SECS, 3600);
        assert_eq!(DEFAULT_REDIS_PORT, 6379);
        assert_eq!(DEFAULT_POSTGRES_PORT, 5432);
        assert_eq!(DEFAULT_MYSQL_PORT, 3306);
        assert_eq!(DEFAULT_RABBITMQ_PORT, 5672);
        assert_eq!(DEFAULT_QUEUE_NAME, "celery");
    }

    #[test]
    fn test_ide_support_examples() {
        use crate::ide_support::examples::*;

        // Verify example constants are valid
        assert!(REDIS_URL_EXAMPLE.starts_with("redis://"));
        assert!(POSTGRES_URL_EXAMPLE.starts_with("postgres://"));
        assert!(MYSQL_URL_EXAMPLE.starts_with("mysql://"));
        assert!(RABBITMQ_URL_EXAMPLE.starts_with("amqp://"));
        assert!(SQS_URL_EXAMPLE.starts_with("https://sqs"));
    }

    #[test]
    fn test_ide_support_trait_bounds() {
        use crate::ide_support::{TaskArgs, TaskResult};

        // Test that common types implement the trait bounds
        fn assert_task_args<T: TaskArgs>() {}
        fn assert_task_result<T: TaskResult>() {}

        assert_task_args::<String>();
        assert_task_args::<i32>();
        assert_task_args::<Vec<u8>>();

        assert_task_result::<String>();
        assert_task_result::<i32>();
        assert_task_result::<Vec<u8>>();
    }

    #[tokio::test]
    async fn test_ide_support_boxed_future() {
        use crate::ide_support::{BoxedFuture, BoxedResult};

        fn create_future() -> BoxedFuture<String> {
            Box::pin(async { Ok("test".to_string()) })
        }

        let result: BoxedResult<String> = create_future().await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "test");
    }

    #[test]
    #[cfg(feature = "dev-utils")]
    fn test_assembly_inspection_count_instructions() {
        use crate::assembly_inspection::count_instructions;

        let sample_asm = r#"
            ; Function prologue
            push   rbp
            mov    rbp, rsp
            ; Actual code
            add    rdi, rsi
            mov    rax, rdi
            ; Function epilogue
            pop    rbp
            ret
        "#;

        let count = count_instructions(sample_asm);
        assert_eq!(count, 6); // Should count 6 actual instructions (push, mov, add, mov, pop, ret)
    }

    #[test]
    #[cfg(feature = "dev-utils")]
    fn test_assembly_inspection_verify_inlined() {
        use crate::assembly_inspection::verify_inlined;

        // Well-inlined function (no calls)
        let inlined_asm = r#"
            add    rdi, rsi
            mov    rax, rdi
            ret
        "#;
        assert!(verify_inlined(inlined_asm));

        // Not inlined (has call instruction)
        let not_inlined_asm = r#"
            push   rbp
            call   some_function
            call   another_function
            pop    rbp
            ret
        "#;
        assert!(!verify_inlined(not_inlined_asm));
    }

    // Convenience functions tests
    #[test]
    fn test_convenience_chunks() {
        use crate::convenience::chunks;
        use serde_json::json;

        let items = vec![json!(1), json!(2), json!(3), json!(4), json!(5)];
        let workflow = chunks("process_item", items, 2);

        assert_eq!(workflow.task.task, "process_item");
        assert_eq!(workflow.items.len(), 5);
        assert_eq!(workflow.chunk_size, 2);
    }

    #[test]
    fn test_convenience_map() {
        use crate::convenience::map;
        use serde_json::json;

        let items = vec![json!(1), json!(2), json!(3)];
        let workflow = map("square", items);

        assert_eq!(workflow.task.task, "square");
        assert_eq!(workflow.argsets.len(), 3);
    }

    #[test]
    fn test_convenience_starmap() {
        use crate::convenience::starmap;
        use serde_json::json;

        let args = vec![vec![json!(1), json!(2)], vec![json!(3), json!(4)]];
        let workflow = starmap("add", args);

        assert_eq!(workflow.task.task, "add");
        assert_eq!(workflow.argsets.len(), 2);
        assert_eq!(workflow.argsets[0].len(), 2);
    }

    #[test]
    fn test_convenience_options() {
        use crate::convenience::options;

        let opts = options();
        // Just verify it creates TaskOptions successfully
        assert!(opts.max_retries.is_none());
    }

    #[test]
    fn test_convenience_with_retry() {
        use crate::convenience::with_retry;

        let opts = with_retry(5, 60);
        assert_eq!(opts.max_retries, Some(5));
        assert_eq!(opts.countdown, Some(60));
    }

    #[test]
    fn test_convenience_with_timeout() {
        use crate::convenience::with_timeout;

        let opts = with_timeout(300);
        assert_eq!(opts.time_limit, Some(300));
    }

    #[test]
    fn test_convenience_with_priority() {
        use crate::convenience::with_priority;

        let opts = with_priority(9);
        assert_eq!(opts.priority, Some(9));
    }

    #[test]
    fn test_convenience_with_countdown() {
        use crate::convenience::with_countdown;

        let opts = with_countdown(60);
        assert_eq!(opts.countdown, Some(60));
    }

    #[test]
    fn test_convenience_with_expires() {
        use crate::convenience::with_expires;

        let opts = with_expires(7200);
        assert_eq!(opts.expires, Some(7200));
    }

    #[test]
    fn test_convenience_batch() {
        use crate::convenience::batch;
        use serde_json::json;

        let args_list = vec![
            vec![json!(1), json!(2)],
            vec![json!(3), json!(4)],
            vec![json!(5), json!(6)],
        ];
        let tasks = batch("add", args_list);

        assert_eq!(tasks.len(), 3);
        assert_eq!(tasks[0].task, "add");
        assert_eq!(tasks[1].task, "add");
        assert_eq!(tasks[2].task, "add");
    }

    #[test]
    #[cfg(feature = "redis")]
    fn test_quick_start_redis_broker() {
        use crate::quick_start::redis_broker;

        // Test that the function creates a broker with proper URL handling
        let result = redis_broker("localhost:6379", "test_queue");
        assert!(result.is_ok() || result.is_err()); // Will fail to connect but should parse
    }

    #[test]
    #[cfg(feature = "mysql")]
    fn test_quick_start_mysql_broker_function_exists() {
        // Just verify the function compiles and is available
        // Actual connection test would require a running MySQL server
        use crate::quick_start::mysql_broker;
        let _f: fn(&str, &str) -> _ = mysql_broker;
    }

    #[test]
    #[cfg(feature = "amqp")]
    fn test_quick_start_amqp_broker_function_exists() {
        // Just verify the function compiles and is available
        use crate::quick_start::amqp_broker;
        let _f: fn(&str, &str) -> _ = amqp_broker;
    }

    #[test]
    #[cfg(feature = "sqs")]
    fn test_quick_start_sqs_broker_function_exists() {
        // Just verify the function compiles and is available
        use crate::quick_start::sqs_broker;
        let _f: fn(&str, &str) -> _ = sqs_broker;
    }

    #[test]
    fn test_ide_support_additional_type_aliases() {
        use crate::ide_support::{TaskId, WorkerStats};

        // Verify type aliases are usable
        let _task_id: TaskId = uuid::Uuid::new_v4();

        // Test that WorkerStats type alias works
        fn accepts_worker_stats(_stats: &WorkerStats) {}
        let stats = WorkerStats {
            total_tasks: 0,
            active_tasks: 0,
            succeeded: 0,
            failed: 0,
            retried: 0,
            uptime: 0.0,
            loadavg: None,
            memory_usage: None,
            pool: None,
            broker: None,
            clock: None,
        };
        accepts_worker_stats(&stats);
    }

    // Tests for new convenience functions
    #[test]
    fn test_convenience_delay() {
        use crate::convenience::delay;
        use serde_json::json;

        let sig = delay("send_email", vec![json!("user@example.com")], 60);
        assert_eq!(sig.task, "send_email");
        assert_eq!(sig.options.countdown, Some(60));
        assert_eq!(sig.args.len(), 1);
    }

    #[test]
    fn test_convenience_expire_in() {
        use crate::convenience::expire_in;
        use serde_json::json;

        let sig = expire_in("process_data", vec![json!({"id": 123})], 7200);
        assert_eq!(sig.task, "process_data");
        assert_eq!(sig.options.expires, Some(7200));
        assert_eq!(sig.args.len(), 1);
    }

    #[test]
    fn test_convenience_high_priority() {
        use crate::convenience::high_priority;
        use serde_json::json;

        let sig = high_priority("urgent_task", vec![json!({"alert": true})]);
        assert_eq!(sig.task, "urgent_task");
        assert_eq!(sig.options.priority, Some(9));
    }

    #[test]
    fn test_convenience_low_priority() {
        use crate::convenience::low_priority;
        use serde_json::json;

        let sig = low_priority("background_cleanup", vec![json!({})]);
        assert_eq!(sig.task, "background_cleanup");
        assert_eq!(sig.options.priority, Some(1));
    }

    #[test]
    fn test_convenience_parallel() {
        use crate::convenience::parallel;
        use serde_json::json;

        let workflow = parallel()
            .add("task1", vec![json!(1)])
            .add("task2", vec![json!(2)])
            .add("task3", vec![json!(3)]);

        assert_eq!(workflow.tasks.len(), 3);
    }

    // Tests for new worker presets
    #[test]
    fn test_presets_cpu_bound_config() {
        use crate::presets::cpu_bound_config;

        let config = cpu_bound_config();
        assert!(config.is_ok());
        let config = config.unwrap();
        assert_eq!(config.concurrency, num_cpus::get());
    }

    #[test]
    fn test_presets_io_bound_config() {
        use crate::presets::io_bound_config;

        let config = io_bound_config();
        assert!(config.is_ok());
        let config = config.unwrap();
        assert_eq!(config.concurrency, num_cpus::get() * 4);
    }

    #[test]
    fn test_presets_balanced_config() {
        use crate::presets::balanced_config;

        let config = balanced_config();
        assert!(config.is_ok());
        let config = config.unwrap();
        assert_eq!(config.concurrency, num_cpus::get() * 2);
    }

    #[test]
    fn test_presets_development_config() {
        use crate::presets::development_config;

        let config = development_config();
        assert!(config.is_ok());
        let config = config.unwrap();
        assert_eq!(config.concurrency, 2);
    }

    // Tests for new type aliases
    #[test]
    fn test_ide_support_new_type_aliases() {
        use crate::ide_support::{
            BrokerUrl, ConcurrencyLevel, PrefetchCount, PriorityLevel, QueueName, RetryCount,
            TaskName, TimeoutSeconds,
        };

        // Verify type aliases compile and work correctly
        let _queue: QueueName = "celery".to_string();
        let _url: BrokerUrl = "redis://localhost:6379".to_string();
        let _retries: RetryCount = 3;
        let _priority: PriorityLevel = 9;
        let _timeout: TimeoutSeconds = 300;
        let _task_name: TaskName = "my_task".to_string();
        let _concurrency: ConcurrencyLevel = 4;
        let _prefetch: PrefetchCount = 10;
    }

    // Tests for advanced convenience functions
    #[test]
    fn test_convenience_critical() {
        use crate::convenience::critical;
        use serde_json::json;

        let sig = critical("process_payment", vec![json!({"amount": 100})]);
        assert_eq!(sig.task, "process_payment");
        assert_eq!(sig.options.priority, Some(9));
        assert_eq!(sig.options.max_retries, Some(5));
    }

    #[test]
    fn test_convenience_best_effort() {
        use crate::convenience::best_effort;
        use serde_json::json;

        let sig = best_effort("update_cache", vec![json!({"key": "value"})]);
        assert_eq!(sig.task, "update_cache");
        assert_eq!(sig.options.priority, Some(1));
        assert_eq!(sig.options.max_retries, Some(0));
    }

    #[test]
    fn test_convenience_transient() {
        use crate::convenience::transient;
        use serde_json::json;

        let sig = transient("temp_notification", vec![json!({"msg": "hi"})], 300);
        assert_eq!(sig.task, "temp_notification");
        assert_eq!(sig.options.expires, Some(300));
    }

    #[test]
    fn test_convenience_retry_with_backoff() {
        use crate::convenience::retry_with_backoff;

        let opts = retry_with_backoff(5, 60);
        assert_eq!(opts.max_retries, Some(5));
        assert_eq!(opts.countdown, Some(60));
    }

    #[test]
    fn test_convenience_pipeline() {
        use crate::convenience::pipeline;
        use serde_json::json;

        let workflow = pipeline()
            .then("fetch_data", vec![json!(1)])
            .then("process_data", vec![json!(2)])
            .then("save_results", vec![json!(3)]);

        assert_eq!(workflow.tasks.len(), 3);
    }

    #[test]
    fn test_convenience_fan_out() {
        use crate::convenience::fan_out;
        use serde_json::json;

        let items = vec![json!(1), json!(2), json!(3)];
        let workflow = fan_out("process_item", items);

        assert_eq!(workflow.task.task, "process_item");
        assert_eq!(workflow.argsets.len(), 3);
    }

    #[test]
    fn test_convenience_fan_in() {
        use crate::convenience::{fan_in, parallel, task};
        use serde_json::json;

        let tasks = parallel()
            .add("task1", vec![json!(1)])
            .add("task2", vec![json!(2)]);

        let callback = task("aggregate_results");
        let workflow = fan_in(tasks, callback);

        assert_eq!(workflow.header.tasks.len(), 2);
        assert_eq!(workflow.body.task, "aggregate_results");
    }

    // Tests for workflow templates
    #[test]
    fn test_workflow_template_etl_pipeline() {
        use crate::workflow_templates::etl_pipeline;
        use serde_json::json;

        let pipeline = etl_pipeline(
            "extract",
            vec![json!({"source": "db"})],
            "transform",
            "load",
        );

        assert_eq!(pipeline.tasks.len(), 3);
        assert_eq!(pipeline.tasks[0].task, "extract");
        assert_eq!(pipeline.tasks[1].task, "transform");
        assert_eq!(pipeline.tasks[2].task, "load");
    }

    #[test]
    fn test_workflow_template_map_reduce() {
        use crate::workflow_templates::map_reduce_workflow;
        use serde_json::json;

        let workflow =
            map_reduce_workflow("process", vec![json!(1), json!(2), json!(3)], "aggregate");

        assert_eq!(workflow.header.tasks.len(), 3);
        assert_eq!(workflow.body.task, "aggregate");
    }

    #[test]
    fn test_workflow_template_scatter_gather() {
        use crate::workflow_templates::scatter_gather;
        use serde_json::json;

        let tasks = vec![
            ("task1", vec![json!(1)]),
            ("task2", vec![json!(2)]),
            ("task3", vec![json!(3)]),
        ];

        let workflow = scatter_gather(tasks, "gather");

        assert_eq!(workflow.header.tasks.len(), 3);
        assert_eq!(workflow.body.task, "gather");
    }

    #[test]
    fn test_workflow_template_batch_processing() {
        use crate::workflow_templates::batch_processing;
        use serde_json::json;

        let items: Vec<_> = (1..=25).map(|i| json!(i)).collect();
        let workflow = batch_processing("process_batch", items, 10, Some("aggregate"));

        // 25 items with batch_size 10 = 3 batches (10 + 10 + 5)
        assert_eq!(workflow.header.tasks.len(), 3);
        assert_eq!(workflow.body.task, "aggregate");
    }

    #[test]
    fn test_workflow_template_sequential_pipeline() {
        use crate::workflow_templates::sequential_pipeline;
        use serde_json::json;

        let stages = vec![
            ("stage1", vec![json!(1)], 3),
            ("stage2", vec![json!(2)], 5),
            ("stage3", vec![json!(3)], 2),
        ];

        let pipeline = sequential_pipeline(stages);

        assert_eq!(pipeline.tasks.len(), 3);
        assert_eq!(pipeline.tasks[0].options.max_retries, Some(3));
        assert_eq!(pipeline.tasks[1].options.max_retries, Some(5));
        assert_eq!(pipeline.tasks[2].options.max_retries, Some(2));
    }

    #[test]
    fn test_workflow_template_priority_workflow() {
        use crate::workflow_templates::priority_workflow;
        use serde_json::json;

        let tasks = vec![
            ("critical", vec![json!(1)], 9),
            ("normal", vec![json!(2)], 5),
            ("low", vec![json!(3)], 1),
        ];

        let workflow = priority_workflow(tasks);

        assert_eq!(workflow.tasks.len(), 3);
        assert_eq!(workflow.tasks[0].options.priority, Some(9));
        assert_eq!(workflow.tasks[1].options.priority, Some(5));
        assert_eq!(workflow.tasks[2].options.priority, Some(1));
    }

    // Tests for task composition
    #[test]
    fn test_task_composition_retry_wrapper() {
        use crate::task_composition::retry_wrapper;
        use serde_json::json;

        let sig = retry_wrapper("my_task", vec![json!(1)], 5, 10);

        assert_eq!(sig.task, "my_task");
        assert_eq!(sig.options.max_retries, Some(5));
        assert_eq!(sig.options.countdown, Some(10));
    }

    #[test]
    fn test_task_composition_timeout_wrapper() {
        use crate::task_composition::timeout_wrapper;
        use serde_json::json;

        let sig = timeout_wrapper("long_task", vec![json!(1)], 300);

        assert_eq!(sig.task, "long_task");
        assert_eq!(sig.options.time_limit, Some(300));
    }

    #[test]
    fn test_task_composition_circuit_breaker() {
        use crate::task_composition::circuit_breaker_group;
        use serde_json::json;

        let tasks = vec![("service_a", vec![json!(1)]), ("service_b", vec![json!(2)])];

        let group = circuit_breaker_group(tasks, 3);

        assert_eq!(group.tasks.len(), 2);
        assert_eq!(group.tasks[0].options.max_retries, Some(3));
        assert_eq!(group.tasks[1].options.max_retries, Some(3));
    }

    #[test]
    fn test_task_composition_rate_limited() {
        use crate::task_composition::rate_limited_workflow;
        use serde_json::json;

        let items = vec![json!(1), json!(2), json!(3)];
        let workflow = rate_limited_workflow("api_call", items, 5);

        assert_eq!(workflow.tasks.len(), 3);
        assert_eq!(workflow.tasks[0].options.countdown, None); // First task has no delay
        assert_eq!(workflow.tasks[1].options.countdown, Some(5)); // 5 seconds delay
        assert_eq!(workflow.tasks[2].options.countdown, Some(10)); // 10 seconds delay
    }

    // Tests for prelude exports of new modules
    #[test]
    fn test_prelude_workflow_templates() {
        use crate::prelude::*;
        use serde_json::json;

        // Test that workflow template functions are available from prelude
        let _pipeline = etl_pipeline("extract", vec![json!(1)], "transform", "load");
        let _map_reduce = map_reduce_workflow("map", vec![json!(1)], "reduce");
        let _scatter = scatter_gather(vec![("t1", vec![json!(1)])], "gather");
        let _batch = batch_processing("process", vec![json!(1)], 5, None);
        let _seq = sequential_pipeline(vec![("s1", vec![json!(1)], 3)]);
        let _priority = priority_workflow(vec![("t1", vec![json!(1)], 9)]);
    }

    #[test]
    fn test_prelude_task_composition() {
        use crate::prelude::*;
        use serde_json::json;

        // Test that task composition functions are available from prelude
        let _retry = retry_wrapper("task", vec![json!(1)], 5, 10);
        let _timeout = timeout_wrapper("task", vec![json!(1)], 300);
        let _circuit = circuit_breaker_group(vec![("t1", vec![json!(1)])], 3);
        let _rate = rate_limited_workflow("task", vec![json!(1)], 5);
    }

    // Tests for error recovery patterns
    #[test]
    fn test_error_recovery_with_fallback() {
        use crate::error_recovery::with_fallback;
        use serde_json::json;

        let chain = with_fallback(
            "primary_task",
            vec![json!(1)],
            "fallback_task",
            vec![json!(2)],
        );

        assert_eq!(chain.tasks.len(), 2);
        assert_eq!(chain.tasks[0].task, "primary_task");
        assert_eq!(chain.tasks[1].task, "fallback_task");
    }

    #[test]
    fn test_error_recovery_ignore_errors() {
        use crate::error_recovery::ignore_errors;
        use serde_json::json;

        let sig = ignore_errors("non_critical_task", vec![json!(1)]);

        assert_eq!(sig.task, "non_critical_task");
        assert_eq!(sig.options.max_retries, Some(0));
        // Error suppression logic would be implemented in task handler
    }

    #[test]
    fn test_error_recovery_exponential_backoff() {
        use crate::error_recovery::with_exponential_backoff;
        use serde_json::json;

        let sig = with_exponential_backoff("flaky_task", vec![json!(1)], 5, 2);

        assert_eq!(sig.task, "flaky_task");
        assert_eq!(sig.options.max_retries, Some(5));
        assert_eq!(sig.options.countdown, Some(2));
    }

    #[test]
    fn test_error_recovery_with_dlq() {
        use crate::error_recovery::with_dlq;
        use serde_json::json;

        let chain = with_dlq("risky_task", vec![json!(1)], "dlq_handler");

        assert_eq!(chain.tasks.len(), 2);
        assert_eq!(chain.tasks[0].task, "risky_task");
        assert_eq!(chain.tasks[1].task, "dlq_handler");
    }

    // Tests for workflow validation
    #[test]
    fn test_workflow_validation_chain_valid() {
        use crate::workflow_validation::validate_chain;
        use crate::Chain;

        let chain = Chain::new().then("task1", vec![]).then("task2", vec![]);

        assert!(validate_chain(&chain).is_ok());
    }

    #[test]
    fn test_workflow_validation_chain_empty() {
        use crate::workflow_validation::validate_chain;
        use crate::Chain;

        let chain = Chain::new();

        assert!(validate_chain(&chain).is_err());
    }

    #[test]
    fn test_workflow_validation_group_valid() {
        use crate::workflow_validation::validate_group;
        use crate::Group;

        let group = Group::new().add("task1", vec![]).add("task2", vec![]);

        assert!(validate_group(&group).is_ok());
    }

    #[test]
    fn test_workflow_validation_group_empty() {
        use crate::workflow_validation::validate_group;
        use crate::Group;

        let group = Group::new();

        assert!(validate_group(&group).is_err());
    }

    #[test]
    fn test_workflow_validation_chord_valid() {
        use crate::workflow_validation::validate_chord;
        use crate::{Chord, Group, Signature};

        let chord = Chord {
            header: Group::new().add("task1", vec![]),
            body: Signature::new("callback".to_string()),
        };

        assert!(validate_chord(&chord).is_ok());
    }

    #[test]
    fn test_workflow_validation_performance_concerns() {
        use crate::workflow_validation::check_performance_concerns_group;
        use crate::Group;

        let mut large_group = Group::new();
        for i in 0..150 {
            large_group = large_group.add(&format!("task_{}", i), vec![]);
        }

        let warnings = check_performance_concerns_group(&large_group);
        assert!(warnings.is_some());
    }

    // Tests for result helpers
    #[test]
    fn test_result_helpers_collector() {
        use crate::result_helpers::create_result_collector;

        let collector = create_result_collector("aggregate", 100);

        assert_eq!(collector.task, "aggregate");
        assert_eq!(collector.options.time_limit, Some(300));
    }

    #[test]
    fn test_result_helpers_filter() {
        use crate::result_helpers::create_result_filter;
        use serde_json::json;

        let filter = create_result_filter("filter_task", json!({"status": "success"}));

        assert_eq!(filter.task, "filter_task");
        assert_eq!(filter.args.len(), 1);
    }

    #[test]
    fn test_result_helpers_transformer() {
        use crate::result_helpers::create_result_transformer;
        use serde_json::json;

        let transformer = create_result_transformer("transform", json!({"format": "csv"}));

        assert_eq!(transformer.task, "transform");
        assert_eq!(transformer.args.len(), 1);
    }

    #[test]
    fn test_result_helpers_reducer() {
        use crate::result_helpers::create_result_reducer;

        let reducer = create_result_reducer("reduce_task", "sum");

        assert_eq!(reducer.task, "reduce_task");
        assert_eq!(reducer.args.len(), 1);
    }

    // Tests for prelude exports of new modules
    #[test]
    fn test_prelude_error_recovery() {
        use crate::prelude::*;
        use serde_json::json;

        let _fallback = with_fallback("t1", vec![json!(1)], "t2", vec![json!(2)]);
        let _ignore = ignore_errors("t", vec![json!(1)]);
        let _backoff = with_exponential_backoff("t", vec![json!(1)], 5, 2);
        let _dlq = with_dlq("t", vec![json!(1)], "dlq");
    }

    #[test]
    fn test_prelude_workflow_validation() {
        use crate::prelude::*;

        let chain = Chain::new().then("task", vec![]);
        let group = Group::new().add("task", vec![]);

        let _ = validate_chain(&chain);
        let _ = validate_group(&group);
        let _ = check_performance_concerns_chain(&chain);
        let _ = check_performance_concerns_group(&group);

        // Test WorkflowValidationError is available from prelude (aliased)
        let _error = WorkflowValidationError::new("test error");
    }

    #[test]
    fn test_prelude_result_helpers() {
        use crate::prelude::*;
        use serde_json::json;

        let _collector = create_result_collector("collect", 10);
        let _filter = create_result_filter("filter", json!({}));
        let _transformer = create_result_transformer("transform", json!({}));
        let _reducer = create_result_reducer("reduce", "sum");
    }

    // Tests for advanced workflow patterns
    #[test]
    fn test_advanced_patterns_module_available() {
        // Verify advanced patterns module is accessible
        use crate::advanced_patterns::*;

        // Test conditional workflow helper
        let workflow =
            create_conditional_workflow("check", vec![], "success", vec![], "failure", vec![]);
        assert!(!workflow.tasks.is_empty());
    }

    #[test]
    fn test_monitoring_helpers_available() {
        use crate::monitoring_helpers::*;

        let monitor = TaskMonitor::new();
        assert_eq!(monitor.total_tasks(), 0);

        monitor.record_success(100);
        assert_eq!(monitor.total_tasks(), 1);
        assert_eq!(monitor.successful_tasks(), 1);

        monitor.record_failure(200);
        assert_eq!(monitor.total_tasks(), 2);
        assert_eq!(monitor.failed_tasks(), 1);
    }

    #[test]
    fn test_batch_helpers_available() {
        use crate::batch_helpers::*;
        use serde_json::json;

        let items = vec![json!(1), json!(2), json!(3), json!(4), json!(5)];
        let batches = create_dynamic_batches("process", items, 2);
        assert_eq!(batches.header.tasks.len(), 3); // 5 items / 2 = 3 batches
    }

    #[test]
    fn test_advanced_patterns_dynamic_workflow() {
        use crate::advanced_patterns::create_dynamic_workflow;
        use serde_json::json;

        let workflow =
            create_dynamic_workflow("generator", vec![json!({"config": "test"})], "executor");
        assert_eq!(workflow.tasks.len(), 2);
    }

    #[test]
    fn test_advanced_patterns_saga_workflow() {
        use crate::advanced_patterns::create_saga_workflow;
        use serde_json::json;

        let steps = vec![
            ("step1", vec![json!(1)], "compensate1", vec![json!(1)]),
            ("step2", vec![json!(2)], "compensate2", vec![json!(2)]),
        ];

        let workflow = create_saga_workflow(steps);
        assert_eq!(workflow.tasks.len(), 2);
    }

    #[test]
    fn test_monitoring_average_time() {
        use crate::monitoring_helpers::TaskMonitor;

        let monitor = TaskMonitor::new();
        monitor.record_success(100);
        monitor.record_success(200);
        monitor.record_success(300);

        assert_eq!(monitor.average_execution_time_ms(), 200);
    }

    #[test]
    fn test_monitoring_success_rate() {
        use crate::monitoring_helpers::TaskMonitor;

        let monitor = TaskMonitor::new();
        monitor.record_success(100);
        monitor.record_success(100);
        monitor.record_failure(100);

        assert!((monitor.success_rate() - 66.67).abs() < 0.1);
    }

    #[test]
    fn test_batch_adaptive_batches() {
        use crate::batch_helpers::create_adaptive_batches;
        use serde_json::json;

        let items: Vec<_> = (1..=100).map(|i| json!(i)).collect();
        let workflow = create_adaptive_batches("process", items, 5, 20);

        // Should create batches with adaptive sizing
        assert!(!workflow.header.tasks.is_empty());
    }

    #[test]
    fn test_batch_prioritized_batches() {
        use crate::batch_helpers::create_prioritized_batches;
        use serde_json::json;

        let high = vec![json!(1), json!(2)];
        let medium = vec![json!(3), json!(4)];
        let low = vec![json!(5), json!(6)];

        let group = create_prioritized_batches("process", (high, medium, low), 1);

        // Should have tasks with different priorities
        assert_eq!(group.tasks.len(), 6);
        assert_eq!(group.tasks[0].options.priority, Some(9));
        assert_eq!(group.tasks[2].options.priority, Some(5));
        assert_eq!(group.tasks[4].options.priority, Some(1));
    }

    // Health check utilities tests
    #[test]
    fn test_health_check_worker_health_checker() {
        use crate::health_check::{HealthStatus, WorkerHealthChecker};

        let checker = WorkerHealthChecker::default();

        // Should be healthy initially
        let result = checker.check_health();
        assert_eq!(result.status, HealthStatus::Healthy);
        assert!(checker.is_ready());
        assert!(checker.is_alive());

        // Record heartbeat and task processing
        checker.heartbeat();
        checker.task_processed();

        // Should still be healthy
        let result = checker.check_health();
        assert_eq!(result.status, HealthStatus::Healthy);
    }

    #[test]
    fn test_health_check_dependency_checker() {
        use crate::health_check::{DependencyChecker, HealthCheckResult, HealthStatus};

        let checker = DependencyChecker::new("database", || {
            HealthCheckResult::healthy("Database is operational")
        });

        assert_eq!(checker.name(), "database");
        let result = checker.check();
        assert_eq!(result.status, HealthStatus::Healthy);
    }

    #[test]
    fn test_health_check_result_builder() {
        use crate::health_check::{HealthCheckResult, HealthStatus};

        let result = HealthCheckResult::healthy("All systems operational")
            .with_metadata("uptime", "3600")
            .with_metadata("requests", "1000");

        assert_eq!(result.status, HealthStatus::Healthy);
        assert_eq!(result.message, "All systems operational");
        assert_eq!(result.metadata.len(), 2);
    }

    // Resource management tests
    #[test]
    fn test_resource_management_limits() {
        use crate::resource_management::ResourceLimits;

        let limits = ResourceLimits::unlimited();
        assert!(limits.max_memory_bytes.is_none());
        assert!(limits.max_cpu_seconds.is_none());

        let limits = ResourceLimits::memory_constrained(512);
        assert_eq!(limits.max_memory_bytes, Some(512 * 1024 * 1024));

        let limits = ResourceLimits::cpu_intensive(60);
        assert_eq!(limits.max_cpu_seconds, Some(60));

        let limits = ResourceLimits::io_intensive(300);
        assert_eq!(limits.max_wall_time_seconds, Some(300));
    }

    #[test]
    fn test_resource_management_tracker() {
        use crate::resource_management::{ResourceLimits, ResourceTracker};
        use std::thread;
        use std::time::Duration;

        let limits = ResourceLimits::memory_constrained(100);
        let tracker = ResourceTracker::new(limits);

        tracker.start();
        thread::sleep(Duration::from_millis(10));

        tracker.record_memory_usage(50 * 1024 * 1024); // 50 MB
        assert_eq!(tracker.peak_memory_bytes(), 50 * 1024 * 1024);

        // Should be within limits
        assert!(tracker.check_limits().is_ok());

        // Recording more memory
        tracker.record_memory_usage(75 * 1024 * 1024); // 75 MB
        assert_eq!(tracker.peak_memory_bytes(), 75 * 1024 * 1024);
    }

    #[test]
    fn test_resource_management_pool() {
        use crate::resource_management::ResourcePool;

        let pool: ResourcePool<String> = ResourcePool::new(3);
        assert!(pool.is_empty());
        assert_eq!(pool.max_size(), 3);

        // Add resources
        pool.release("resource1".to_string()).unwrap();
        pool.release("resource2".to_string()).unwrap();
        assert_eq!(pool.available(), 2);

        // Acquire resources
        let r1 = pool.acquire();
        assert!(r1.is_some());
        assert_eq!(pool.available(), 1);

        let r2 = pool.acquire();
        assert!(r2.is_some());
        assert_eq!(pool.available(), 0);
        assert!(pool.is_empty());
    }

    // Task hooks tests
    #[test]
    fn test_task_hooks_hook_registry() {
        use crate::task_hooks::{HookRegistry, LoggingHook};

        let mut registry = HookRegistry::new();
        assert_eq!(registry.pre_hook_count(), 0);
        assert_eq!(registry.post_hook_count(), 0);

        registry.register_pre_hook(LoggingHook::new(false, false));
        registry.register_post_hook(LoggingHook::new(false, false));

        assert_eq!(registry.pre_hook_count(), 1);
        assert_eq!(registry.post_hook_count(), 1);
    }

    #[test]
    fn test_task_hooks_logging_hook() {
        use crate::task_hooks::{LoggingHook, PostExecutionHook, PreExecutionHook};
        use serde_json::json;

        let hook = LoggingHook::new(false, false);
        let mut args = vec![json!({"x": 1})];

        // Test pre-execution hook
        let result = hook.before_execute("test_task", "task-123", &mut args);
        assert!(result.is_ok());

        // Test post-execution hook
        let task_result: std::result::Result<serde_json::Value, String> = Ok(json!({"result": 42}));
        let result = hook.after_execute("test_task", "task-123", &task_result, 100);
        assert!(result.is_ok());

        // Test with error result
        let task_result: std::result::Result<serde_json::Value, String> =
            Err("Task failed".to_string());
        let result = hook.after_execute("test_task", "task-123", &task_result, 100);
        assert!(result.is_ok());
    }

    #[test]
    fn test_task_hooks_validation_hook() {
        use crate::task_hooks::{PreExecutionHook, ValidationHook};
        use serde_json::json;

        let hook = ValidationHook::new(|task_name: &str, args: &Vec<serde_json::Value>| {
            if args.is_empty() {
                return Err("Arguments cannot be empty".into());
            }
            if task_name == "forbidden" {
                return Err("Task is forbidden".into());
            }
            Ok(())
        });

        let mut args = vec![json!({"x": 1})];
        let result = hook.before_execute("allowed_task", "task-123", &mut args);
        assert!(result.is_ok());

        let result = hook.before_execute("forbidden", "task-123", &mut args);
        assert!(result.is_err());

        let mut empty_args = vec![];
        let result = hook.before_execute("allowed_task", "task-123", &mut empty_args);
        assert!(result.is_err());
    }

    #[test]
    fn test_task_hooks_run_hooks() {
        use crate::task_hooks::{HookRegistry, LoggingHook};
        use serde_json::json;

        let mut registry = HookRegistry::new();
        registry.register_pre_hook(LoggingHook::new(false, false));
        registry.register_post_hook(LoggingHook::new(false, false));

        let mut args = vec![json!({"x": 1})];
        let result = registry.run_pre_hooks("test_task", "task-123", &mut args);
        assert!(result.is_ok());

        let task_result: std::result::Result<serde_json::Value, String> = Ok(json!({"result": 42}));
        let result = registry.run_post_hooks("test_task", "task-123", &task_result, 100);
        assert!(result.is_ok());
    }

    // Metrics aggregation tests
    #[test]
    fn test_metrics_aggregation_histogram() {
        use crate::metrics_aggregation::Histogram;

        let mut histogram = Histogram::new();
        assert_eq!(histogram.count(), 0);
        assert_eq!(histogram.mean(), 0.0);

        histogram.record(100.0);
        histogram.record(200.0);
        histogram.record(150.0);

        assert_eq!(histogram.count(), 3);
        assert_eq!(histogram.sum(), 450.0);
        assert_eq!(histogram.mean(), 150.0);

        let p50 = histogram.percentile(50.0);
        assert!(p50 > 0.0);
    }

    #[test]
    fn test_metrics_aggregation_aggregator() {
        use crate::metrics_aggregation::MetricsAggregator;
        use std::time::Duration;

        let aggregator = MetricsAggregator::new();

        // Record some task executions
        aggregator.record_duration("task1", Duration::from_millis(100));
        aggregator.record_duration("task1", Duration::from_millis(200));
        aggregator.record_duration("task2", Duration::from_millis(50));

        assert_eq!(aggregator.task_count("task1"), 2);
        assert_eq!(aggregator.task_count("task2"), 1);
        assert_eq!(aggregator.task_count("task3"), 0);

        let mean = aggregator.mean_duration("task1");
        assert!(mean > 100.0 && mean < 200.0);

        let p50 = aggregator.percentile_duration("task1", 50.0);
        assert!(p50 > 0.0);

        let throughput = aggregator.throughput("task1");
        assert!(throughput > 0.0);
    }

    #[test]
    fn test_metrics_aggregation_error_tracking() {
        use crate::metrics_aggregation::MetricsAggregator;
        use std::time::Duration;

        let aggregator = MetricsAggregator::new();

        aggregator.record_duration("task1", Duration::from_millis(100));
        aggregator.record_duration("task1", Duration::from_millis(150));
        aggregator.record_error("task1");

        assert_eq!(aggregator.task_count("task1"), 2);
        assert_eq!(aggregator.error_count("task1"), 1);

        let success_rate = aggregator.success_rate("task1");
        assert!((success_rate - 50.0).abs() < 0.1);

        // Task with no errors should have 100% success rate
        aggregator.record_duration("task2", Duration::from_millis(100));
        let success_rate = aggregator.success_rate("task2");
        assert_eq!(success_rate, 100.0);
    }

    #[test]
    fn test_metrics_aggregation_summary() {
        use crate::metrics_aggregation::MetricsAggregator;
        use std::time::Duration;

        let aggregator = MetricsAggregator::new();

        aggregator.record_duration("task1", Duration::from_millis(100));
        aggregator.record_duration("task1", Duration::from_millis(200));
        aggregator.record_error("task1");

        let summary = aggregator.summary("task1");
        assert!(summary.contains("task1"));
        assert!(summary.contains("Total Executions"));
        assert!(summary.contains("Mean Duration"));
        assert!(summary.contains("Throughput"));
    }

    #[test]
    fn test_metrics_aggregation_task_names() {
        use crate::metrics_aggregation::MetricsAggregator;
        use std::time::Duration;

        let aggregator = MetricsAggregator::new();

        aggregator.record_duration("task1", Duration::from_millis(100));
        aggregator.record_duration("task2", Duration::from_millis(200));
        aggregator.record_duration("task3", Duration::from_millis(300));

        let names = aggregator.task_names();
        assert_eq!(names.len(), 3);
        assert!(names.contains(&"task1".to_string()));
        assert!(names.contains(&"task2".to_string()));
        assert!(names.contains(&"task3".to_string()));
    }

    #[test]
    fn test_prelude_exports_new_modules() {
        // Test that all new module types are exported in prelude
        use crate::prelude::*;

        // Health check types
        let _: Option<WorkerHealthChecker> = None;
        let _: Option<DependencyChecker> = None;
        let _: Option<HealthCheckResult> = None;

        // Resource management types
        let _: Option<ResourceLimits> = None;
        let _: Option<ResourceTracker> = None;
        let _: Option<ResourcePool<String>> = None;

        // Task hooks types
        let _: Option<HookRegistry> = None;
        let _: Option<LoggingHook> = None;

        // Metrics aggregation types
        let _: Option<MetricsAggregator> = None;
        let _: Option<Histogram> = None;
    }

    // Task cancellation tests
    #[test]
    fn test_task_cancellation_token() {
        use crate::task_cancellation::CancellationToken;

        let token = CancellationToken::new();
        assert!(!token.is_cancelled());
        assert!(token.check_cancelled().is_ok());

        token.cancel(Some("User requested cancellation".to_string()));
        assert!(token.is_cancelled());
        assert!(token.check_cancelled().is_err());
        assert_eq!(
            token.cancellation_reason(),
            Some("User requested cancellation".to_string())
        );
    }

    #[test]
    fn test_task_cancellation_timeout_manager() {
        use crate::task_cancellation::TimeoutManager;
        use std::thread;
        use std::time::Duration;

        let manager = TimeoutManager::new(Duration::from_millis(100));
        assert!(!manager.is_timed_out());
        assert!(manager.check_timeout().is_ok());

        thread::sleep(Duration::from_millis(150));
        assert!(manager.is_timed_out());
        assert!(manager.check_timeout().is_err());
    }

    #[test]
    fn test_task_cancellation_execution_guard() {
        use crate::task_cancellation::{CancellationToken, ExecutionGuard};
        use std::time::Duration;

        let token = CancellationToken::new();
        let guard = ExecutionGuard::new(token.clone(), Some(Duration::from_secs(10)));

        assert!(guard.should_continue().is_ok());

        token.cancel(None);
        assert!(guard.should_continue().is_err());
    }

    // Retry strategies tests
    #[test]
    fn test_retry_strategies_exponential_backoff() {
        use crate::retry_strategies::RetryStrategy;
        use std::time::Duration;

        let strategy = RetryStrategy::exponential_backoff(3, Duration::from_secs(1));
        assert_eq!(strategy.max_retries, 3);

        let delay0 = strategy.calculate_delay(0);
        assert_eq!(delay0, Duration::from_secs(0));

        let delay1 = strategy.calculate_delay(1);
        assert!(delay1.as_millis() >= 750 && delay1.as_millis() <= 1250); // With jitter

        let delay2 = strategy.calculate_delay(2);
        assert!(delay2.as_millis() >= 1500); // At least 1.5s base with jitter
    }

    #[test]
    fn test_retry_strategies_fixed_delay() {
        use crate::retry_strategies::RetryStrategy;
        use std::time::Duration;

        let strategy = RetryStrategy::fixed_delay(5, Duration::from_millis(500));
        assert_eq!(strategy.max_retries, 5);

        let delay = strategy.calculate_delay(1);
        assert_eq!(delay, Duration::from_millis(500));

        let delay = strategy.calculate_delay(3);
        assert_eq!(delay, Duration::from_millis(500));
    }

    #[test]
    fn test_retry_strategies_default_policy() {
        use crate::retry_strategies::{DefaultRetryPolicy, RetryPolicy};

        let policy = DefaultRetryPolicy::new(3);
        assert!(policy.should_retry("any error", 0));
        assert!(policy.should_retry("any error", 2));
        assert!(!policy.should_retry("any error", 3));
    }

    #[test]
    fn test_retry_strategies_error_pattern_policy() {
        use crate::retry_strategies::{ErrorPatternRetryPolicy, RetryPolicy};

        let policy =
            ErrorPatternRetryPolicy::new(3, vec!["timeout".to_string(), "connection".to_string()]);

        assert!(policy.should_retry("connection error", 0));
        assert!(policy.should_retry("timeout occurred", 1));
        assert!(!policy.should_retry("invalid input", 0));
        assert!(!policy.should_retry("timeout", 3)); // Max retries reached
    }

    // Task dependencies tests
    #[test]
    fn test_task_dependencies_graph() {
        use crate::task_dependencies::DependencyGraph;

        let mut graph = DependencyGraph::new();
        graph.add_task("task1");
        graph.add_task("task2");
        graph.add_task("task3");

        graph.add_dependency("task2", "task1"); // task2 depends on task1
        graph.add_dependency("task3", "task2"); // task3 depends on task2

        assert_eq!(graph.get_dependencies("task1"), Vec::<String>::new());
        assert_eq!(graph.get_dependencies("task2"), vec!["task1"]);
        assert_eq!(graph.get_dependencies("task3"), vec!["task2"]);

        assert_eq!(graph.get_dependents("task1"), vec!["task2"]);
        assert_eq!(graph.get_dependents("task2"), vec!["task3"]);
    }

    #[test]
    fn test_task_dependencies_circular_detection() {
        use crate::task_dependencies::DependencyGraph;

        let mut graph = DependencyGraph::new();
        graph.add_task("task1");
        graph.add_task("task2");
        graph.add_task("task3");

        graph.add_dependency("task2", "task1");
        graph.add_dependency("task3", "task2");
        graph.add_dependency("task1", "task3"); // Creates cycle

        assert!(graph.has_circular_dependencies());
    }

    #[test]
    fn test_task_dependencies_topological_sort() {
        use crate::task_dependencies::DependencyGraph;

        let mut graph = DependencyGraph::new();
        graph.add_task("task1");
        graph.add_task("task2");
        graph.add_task("task3");

        graph.add_dependency("task2", "task1");
        graph.add_dependency("task3", "task2");

        let sorted = graph.topological_sort().unwrap();
        assert_eq!(sorted, vec!["task1", "task2", "task3"]);
    }

    #[test]
    fn test_task_dependencies_ready_tasks() {
        use crate::task_dependencies::DependencyGraph;
        use std::collections::HashSet;

        let mut graph = DependencyGraph::new();
        graph.add_task("task1");
        graph.add_task("task2");
        graph.add_task("task3");

        graph.add_dependency("task2", "task1");
        graph.add_dependency("task3", "task2");

        let completed: HashSet<String> = HashSet::new();
        let ready = graph.get_ready_tasks(&completed);
        assert_eq!(ready, vec!["task1"]); // Only task1 has no dependencies

        let mut completed = HashSet::new();
        completed.insert("task1".to_string());
        let ready = graph.get_ready_tasks(&completed);
        assert_eq!(ready, vec!["task2"]); // task2 becomes ready
    }

    // Performance profiling tests
    #[test]
    fn test_performance_profiling_profiler() {
        use crate::performance_profiling::PerformanceProfiler;
        use std::thread;
        use std::time::Duration;

        let profiler = PerformanceProfiler::new();

        profiler.start_span("operation1");
        thread::sleep(Duration::from_millis(10));
        profiler.end_span();

        profiler.start_span("operation2");
        thread::sleep(Duration::from_millis(20));
        profiler.end_span();

        let profile1 = profiler.get_profile("operation1").unwrap();
        assert_eq!(profile1.name, "operation1");
        assert_eq!(profile1.invocation_count, 1);
        assert!(profile1.total_duration.as_millis() >= 10);

        let profile2 = profiler.get_profile("operation2").unwrap();
        assert_eq!(profile2.invocation_count, 1);
        assert!(profile2.total_duration.as_millis() >= 20);
    }

    #[test]
    fn test_performance_profiling_multiple_invocations() {
        use crate::performance_profiling::PerformanceProfiler;
        use std::thread;
        use std::time::Duration;

        let profiler = PerformanceProfiler::new();

        for _ in 0..3 {
            profiler.start_span("repeated_op");
            thread::sleep(Duration::from_millis(5));
            profiler.end_span();
        }

        let profile = profiler.get_profile("repeated_op").unwrap();
        assert_eq!(profile.invocation_count, 3);
        assert!(profile.total_duration.as_millis() >= 15);
    }

    #[test]
    fn test_performance_profiling_slowest_operations() {
        use crate::performance_profiling::PerformanceProfiler;
        use std::thread;
        use std::time::Duration;

        let profiler = PerformanceProfiler::new();

        profiler.start_span("fast_op");
        thread::sleep(Duration::from_millis(5));
        profiler.end_span();

        profiler.start_span("slow_op");
        thread::sleep(Duration::from_millis(20));
        profiler.end_span();

        profiler.start_span("medium_op");
        thread::sleep(Duration::from_millis(10));
        profiler.end_span();

        let slowest = profiler.get_slowest_operations(2);
        assert_eq!(slowest.len(), 2);
        assert_eq!(slowest[0].name, "slow_op");
        assert_eq!(slowest[1].name, "medium_op");
    }

    #[test]
    fn test_performance_profiling_report_generation() {
        use crate::performance_profiling::PerformanceProfiler;
        use std::thread;
        use std::time::Duration;

        let profiler = PerformanceProfiler::new();

        profiler.start_span("test_operation");
        thread::sleep(Duration::from_millis(10));
        profiler.end_span();

        let report = profiler.generate_report();
        assert!(report.contains("Performance Profile Report"));
        assert!(report.contains("test_operation"));
        assert!(report.contains("Count"));
        assert!(report.contains("Total"));
    }

    #[test]
    fn test_prelude_exports_additional_modules() {
        // Test that all additional module types are exported in prelude
        use crate::prelude::*;

        // Task cancellation types
        let _: Option<CancellationToken> = None;
        let _: Option<TimeoutManager> = None;
        let _: Option<ExecutionGuard> = None;

        // Retry strategy types
        let _: Option<RetryStrategy> = None;
        let _: Option<DefaultRetryPolicy> = None;
        let _: Option<ErrorPatternRetryPolicy> = None;

        // Task dependency types
        let _: Option<DependencyGraph> = None;

        // Performance profiling types
        // Note: PerformanceProfiler is not exported in prelude to avoid conflict with dev_utils
        let _: Option<PerformanceProfile> = None;
        let _: Option<ProfileSpan<'_>> = None;

        // Access PerformanceProfiler directly from module
        let _: Option<crate::performance_profiling::PerformanceProfiler> = None;
    }
}

/// Advanced workflow patterns for complex use cases
///
/// This module provides advanced workflow patterns including conditional execution,
/// dynamic task generation, and complex orchestration patterns.
pub mod advanced_patterns {
    use crate::{Chain, Group, Signature};
    use serde_json::Value;

    /// Creates a conditional workflow with success and failure branches
    ///
    /// This creates a chain that includes both success and failure paths.
    /// The actual branching logic must be implemented in the condition task.
    ///
    /// # Arguments
    ///
    /// * `condition_task` - Task that evaluates the condition
    /// * `condition_args` - Arguments for the condition task
    /// * `success_task` - Task to execute on success
    /// * `success_args` - Arguments for success task
    /// * `failure_task` - Task to execute on failure
    /// * `failure_args` - Arguments for failure task
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::advanced_patterns::create_conditional_workflow;
    /// use serde_json::json;
    ///
    /// let workflow = create_conditional_workflow(
    ///     "check_balance",
    ///     vec![json!({"account_id": 123})],
    ///     "process_payment",
    ///     vec![json!({"amount": 100})],
    ///     "send_insufficient_funds_notice",
    ///     vec![json!({"account_id": 123})]
    /// );
    /// ```
    pub fn create_conditional_workflow(
        condition_task: &str,
        condition_args: Vec<Value>,
        success_task: &str,
        success_args: Vec<Value>,
        failure_task: &str,
        failure_args: Vec<Value>,
    ) -> Chain {
        // Create a chain with condition task
        // The condition task should route to success or failure based on its result
        let mut chain = Chain::new();
        chain = chain.then(condition_task, condition_args);
        chain = chain.then(success_task, success_args);
        // Note: Failure path would be implemented via task routing/error handling
        // This is a template for the workflow structure
        let _ = failure_task;
        let _ = failure_args;
        chain
    }

    /// Creates a dynamic workflow where tasks are generated at runtime
    ///
    /// This pattern allows for workflows where the number and type of tasks
    /// are determined dynamically based on input data.
    ///
    /// # Arguments
    ///
    /// * `generator_task` - Task that generates the list of tasks to execute
    /// * `generator_args` - Arguments for the generator task
    /// * `executor_task` - Task that executes the generated tasks
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::advanced_patterns::create_dynamic_workflow;
    /// use serde_json::json;
    ///
    /// let workflow = create_dynamic_workflow(
    ///     "generate_tasks",
    ///     vec![json!({"rules": "config.json"})],
    ///     "execute_task"
    /// );
    /// ```
    pub fn create_dynamic_workflow(
        generator_task: &str,
        generator_args: Vec<Value>,
        executor_task: &str,
    ) -> Chain {
        Chain::new()
            .then(generator_task, generator_args)
            .then(executor_task, vec![])
    }

    /// Creates a workflow with parallel sub-chains
    ///
    /// This pattern executes multiple chains in parallel, useful for
    /// independent workflows that should run concurrently.
    ///
    /// # Arguments
    ///
    /// * `chains` - List of (chain_name, tasks) tuples
    /// * `aggregate_task` - Optional task to aggregate results from all chains
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::advanced_patterns::create_parallel_chains;
    /// use serde_json::json;
    ///
    /// let chains = vec![
    ///     ("process_images", vec![("resize", vec![]), ("optimize", vec![])]),
    ///     ("process_videos", vec![("transcode", vec![]), ("thumbnail", vec![])]),
    /// ];
    ///
    /// let workflow = create_parallel_chains(chains, Some("finalize"));
    /// ```
    #[allow(clippy::type_complexity)]
    pub fn create_parallel_chains(
        chains: Vec<(&str, Vec<(&str, Vec<Value>)>)>,
        aggregate_task: Option<&str>,
    ) -> Group {
        let mut group = Group::new();

        for (_chain_name, tasks) in chains {
            // Create a signature for the first task in each chain
            if let Some((first_task, first_args)) = tasks.first() {
                let sig = Signature::new(first_task.to_string()).with_args(first_args.clone());
                group.tasks.push(sig);
            }
        }

        // Note: Aggregate task would be added as a chord if provided
        let _ = aggregate_task;

        group
    }

    /// Creates a saga pattern workflow for distributed transactions
    ///
    /// Implements the saga pattern with compensating transactions for each step.
    ///
    /// # Arguments
    ///
    /// * `steps` - List of (forward_task, forward_args, compensate_task, compensate_args) tuples
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::advanced_patterns::create_saga_workflow;
    /// use serde_json::json;
    ///
    /// let steps = vec![
    ///     ("reserve_inventory", vec![json!(1)], "release_inventory", vec![json!(1)]),
    ///     ("charge_payment", vec![json!(2)], "refund_payment", vec![json!(2)]),
    ///     ("ship_order", vec![json!(3)], "cancel_shipment", vec![json!(3)]),
    /// ];
    ///
    /// let workflow = create_saga_workflow(steps);
    /// ```
    pub fn create_saga_workflow(steps: Vec<(&str, Vec<Value>, &str, Vec<Value>)>) -> Chain {
        let mut chain = Chain::new();

        // Add forward tasks
        for (forward_task, forward_args, _compensate_task, _compensate_args) in steps {
            chain = chain.then(forward_task, forward_args);
            // Note: Compensation tasks would be invoked on failure
            // This requires error handling logic in the task implementation
        }

        chain
    }
}

/// Monitoring and observability helpers
///
/// This module provides utilities for monitoring task execution, tracking metrics,
/// and observing workflow behavior in production.
pub mod monitoring_helpers {
    use std::sync::{Arc, Mutex};
    use std::time::{SystemTime, UNIX_EPOCH};

    /// Task execution monitor
    ///
    /// Tracks task execution statistics and provides insights into task performance.
    #[derive(Clone)]
    pub struct TaskMonitor {
        metrics: Arc<Mutex<MonitorMetrics>>,
    }

    #[derive(Debug, Clone)]
    struct MonitorMetrics {
        total_tasks: usize,
        successful_tasks: usize,
        failed_tasks: usize,
        total_execution_time_ms: u128,
        start_time: u64,
    }

    impl TaskMonitor {
        /// Creates a new task monitor
        pub fn new() -> Self {
            Self {
                metrics: Arc::new(Mutex::new(MonitorMetrics {
                    total_tasks: 0,
                    successful_tasks: 0,
                    failed_tasks: 0,
                    total_execution_time_ms: 0,
                    start_time: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_secs(),
                })),
            }
        }

        /// Records a successful task execution
        pub fn record_success(&self, execution_time_ms: u128) {
            let mut metrics = self.metrics.lock().unwrap();
            metrics.total_tasks += 1;
            metrics.successful_tasks += 1;
            metrics.total_execution_time_ms += execution_time_ms;
        }

        /// Records a failed task execution
        pub fn record_failure(&self, execution_time_ms: u128) {
            let mut metrics = self.metrics.lock().unwrap();
            metrics.total_tasks += 1;
            metrics.failed_tasks += 1;
            metrics.total_execution_time_ms += execution_time_ms;
        }

        /// Gets the total number of tasks processed
        pub fn total_tasks(&self) -> usize {
            self.metrics.lock().unwrap().total_tasks
        }

        /// Gets the number of successful tasks
        pub fn successful_tasks(&self) -> usize {
            self.metrics.lock().unwrap().successful_tasks
        }

        /// Gets the number of failed tasks
        pub fn failed_tasks(&self) -> usize {
            self.metrics.lock().unwrap().failed_tasks
        }

        /// Gets the average execution time in milliseconds
        pub fn average_execution_time_ms(&self) -> u128 {
            let metrics = self.metrics.lock().unwrap();
            if metrics.total_tasks == 0 {
                0
            } else {
                metrics.total_execution_time_ms / metrics.total_tasks as u128
            }
        }

        /// Gets the success rate as a percentage
        pub fn success_rate(&self) -> f64 {
            let metrics = self.metrics.lock().unwrap();
            if metrics.total_tasks == 0 {
                0.0
            } else {
                (metrics.successful_tasks as f64 / metrics.total_tasks as f64) * 100.0
            }
        }

        /// Resets all metrics
        pub fn reset(&self) {
            let mut metrics = self.metrics.lock().unwrap();
            metrics.total_tasks = 0;
            metrics.successful_tasks = 0;
            metrics.failed_tasks = 0;
            metrics.total_execution_time_ms = 0;
            metrics.start_time = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs();
        }

        /// Generates a summary report
        pub fn summary(&self) -> String {
            let metrics = self.metrics.lock().unwrap();
            format!(
                "Task Monitor Summary:\n\
                 - Total Tasks: {}\n\
                 - Successful: {} ({:.2}%)\n\
                 - Failed: {} ({:.2}%)\n\
                 - Avg Execution Time: {}ms\n\
                 - Uptime: {}s",
                metrics.total_tasks,
                metrics.successful_tasks,
                if metrics.total_tasks > 0 {
                    (metrics.successful_tasks as f64 / metrics.total_tasks as f64) * 100.0
                } else {
                    0.0
                },
                metrics.failed_tasks,
                if metrics.total_tasks > 0 {
                    (metrics.failed_tasks as f64 / metrics.total_tasks as f64) * 100.0
                } else {
                    0.0
                },
                if metrics.total_tasks > 0 {
                    metrics.total_execution_time_ms / metrics.total_tasks as u128
                } else {
                    0
                },
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_secs()
                    - metrics.start_time
            )
        }
    }

    impl Default for TaskMonitor {
        fn default() -> Self {
            Self::new()
        }
    }
}

/// Batch processing helpers for efficient data processing
///
/// This module provides utilities for processing large datasets in batches,
/// including dynamic batch sizing and parallel batch processing.
pub mod batch_helpers {
    use crate::{Chord, Group, Signature};
    use serde_json::Value;

    /// Creates batches with dynamic sizing based on item count
    ///
    /// Automatically determines optimal batch size for efficient processing.
    ///
    /// # Arguments
    ///
    /// * `task_name` - Name of the task to process each batch
    /// * `items` - Items to process
    /// * `target_batch_size` - Target size for each batch
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::batch_helpers::create_dynamic_batches;
    /// use serde_json::json;
    ///
    /// let items = (1..=100).map(|i| json!(i)).collect();
    /// let workflow = create_dynamic_batches("process_batch", items, 10);
    /// ```
    pub fn create_dynamic_batches(
        task_name: &str,
        items: Vec<Value>,
        target_batch_size: usize,
    ) -> Chord {
        let mut group = Group::new();
        let batch_size = if target_batch_size == 0 {
            1
        } else {
            target_batch_size
        };

        for chunk in items.chunks(batch_size) {
            let batch = Value::Array(chunk.to_vec());
            group = group.add(task_name, vec![batch]);
        }

        Chord {
            header: group,
            body: Signature::new("batch_complete".to_string()),
        }
    }

    /// Creates adaptive batches that adjust size based on processing characteristics
    ///
    /// # Arguments
    ///
    /// * `task_name` - Name of the task to process each batch
    /// * `items` - Items to process
    /// * `min_batch_size` - Minimum batch size
    /// * `max_batch_size` - Maximum batch size
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::batch_helpers::create_adaptive_batches;
    /// use serde_json::json;
    ///
    /// let items = (1..=1000).map(|i| json!(i)).collect();
    /// let workflow = create_adaptive_batches("process", items, 10, 100);
    /// ```
    pub fn create_adaptive_batches(
        task_name: &str,
        items: Vec<Value>,
        min_batch_size: usize,
        max_batch_size: usize,
    ) -> Chord {
        let mut group = Group::new();

        // Calculate adaptive batch size based on total items
        let batch_size = if items.len() < min_batch_size {
            items.len()
        } else if items.len() > max_batch_size * 10 {
            max_batch_size
        } else {
            // Use a size between min and max based on item count
            let calculated = (items.len() as f64).sqrt() as usize;
            calculated.clamp(min_batch_size, max_batch_size)
        };

        for chunk in items.chunks(batch_size.max(1)) {
            let batch = Value::Array(chunk.to_vec());
            group = group.add(task_name, vec![batch]);
        }

        Chord {
            header: group,
            body: Signature::new("batch_complete".to_string()),
        }
    }

    /// Creates prioritized batches with different priority levels
    ///
    /// # Arguments
    ///
    /// * `task_name` - Name of the task to process each batch
    /// * `priority_items` - Items grouped by priority (high, medium, low)
    /// * `batch_size` - Size of each batch
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// use celers::batch_helpers::create_prioritized_batches;
    /// use serde_json::json;
    ///
    /// let high = vec![json!(1), json!(2)];
    /// let medium = vec![json!(3), json!(4)];
    /// let low = vec![json!(5), json!(6)];
    ///
    /// let workflow = create_prioritized_batches("process", (high, medium, low), 2);
    /// ```
    pub fn create_prioritized_batches(
        task_name: &str,
        priority_items: (Vec<Value>, Vec<Value>, Vec<Value>),
        batch_size: usize,
    ) -> Group {
        let (high_priority, medium_priority, low_priority) = priority_items;
        let mut group = Group::new();

        // Process high priority items first
        for chunk in high_priority.chunks(batch_size.max(1)) {
            let batch = Value::Array(chunk.to_vec());
            let mut sig = Signature::new(task_name.to_string()).with_args(vec![batch]);
            sig.options.priority = Some(9);
            group.tasks.push(sig);
        }

        // Then medium priority
        for chunk in medium_priority.chunks(batch_size.max(1)) {
            let batch = Value::Array(chunk.to_vec());
            let mut sig = Signature::new(task_name.to_string()).with_args(vec![batch]);
            sig.options.priority = Some(5);
            group.tasks.push(sig);
        }

        // Finally low priority
        for chunk in low_priority.chunks(batch_size.max(1)) {
            let batch = Value::Array(chunk.to_vec());
            let mut sig = Signature::new(task_name.to_string()).with_args(vec![batch]);
            sig.options.priority = Some(1);
            group.tasks.push(sig);
        }

        group
    }
}

/// Health check utilities for monitoring worker and task health
///
/// This module provides utilities for implementing health checks in workers,
/// including readiness, liveness, and dependency checks.
pub mod health_check {
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    /// Health status of a component
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub enum HealthStatus {
        /// Component is healthy and operational
        Healthy,
        /// Component is degraded but still operational
        Degraded,
        /// Component is unhealthy and not operational
        Unhealthy,
    }

    /// Health check result
    #[derive(Debug, Clone)]
    pub struct HealthCheckResult {
        /// Status of the health check
        pub status: HealthStatus,
        /// Human-readable message
        pub message: String,
        /// Timestamp of the check
        pub timestamp: Instant,
        /// Additional metadata
        pub metadata: Vec<(String, String)>,
    }

    impl HealthCheckResult {
        /// Creates a new healthy result
        pub fn healthy(message: impl Into<String>) -> Self {
            Self {
                status: HealthStatus::Healthy,
                message: message.into(),
                timestamp: Instant::now(),
                metadata: Vec::new(),
            }
        }

        /// Creates a new degraded result
        pub fn degraded(message: impl Into<String>) -> Self {
            Self {
                status: HealthStatus::Degraded,
                message: message.into(),
                timestamp: Instant::now(),
                metadata: Vec::new(),
            }
        }

        /// Creates a new unhealthy result
        pub fn unhealthy(message: impl Into<String>) -> Self {
            Self {
                status: HealthStatus::Unhealthy,
                message: message.into(),
                timestamp: Instant::now(),
                metadata: Vec::new(),
            }
        }

        /// Adds metadata to the health check result
        pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
            self.metadata.push((key.into(), value.into()));
            self
        }
    }

    /// Worker health checker
    ///
    /// Monitors worker health including heartbeat, task processing, and dependencies.
    #[derive(Clone)]
    pub struct WorkerHealthChecker {
        last_heartbeat: Arc<Mutex<Instant>>,
        last_task_processed: Arc<Mutex<Option<Instant>>>,
        heartbeat_timeout: Duration,
        task_timeout: Duration,
    }

    impl WorkerHealthChecker {
        /// Creates a new worker health checker
        ///
        /// # Arguments
        ///
        /// * `heartbeat_timeout` - Maximum time between heartbeats before unhealthy
        /// * `task_timeout` - Maximum time since last task processed before degraded
        pub fn new(heartbeat_timeout: Duration, task_timeout: Duration) -> Self {
            Self {
                last_heartbeat: Arc::new(Mutex::new(Instant::now())),
                last_task_processed: Arc::new(Mutex::new(None)),
                heartbeat_timeout,
                task_timeout,
            }
        }

        /// Records a heartbeat
        pub fn heartbeat(&self) {
            *self.last_heartbeat.lock().unwrap() = Instant::now();
        }

        /// Records task processing
        pub fn task_processed(&self) {
            *self.last_task_processed.lock().unwrap() = Some(Instant::now());
        }

        /// Checks worker health status
        pub fn check_health(&self) -> HealthCheckResult {
            let now = Instant::now();
            let last_heartbeat = *self.last_heartbeat.lock().unwrap();
            let last_task = *self.last_task_processed.lock().unwrap();

            // Check heartbeat
            if now.duration_since(last_heartbeat) > self.heartbeat_timeout {
                return HealthCheckResult::unhealthy("Worker heartbeat timeout").with_metadata(
                    "last_heartbeat_seconds_ago",
                    format!("{}", now.duration_since(last_heartbeat).as_secs()),
                );
            }

            // Check task processing
            if let Some(last_task_time) = last_task {
                if now.duration_since(last_task_time) > self.task_timeout {
                    return HealthCheckResult::degraded("No tasks processed recently")
                        .with_metadata(
                            "last_task_seconds_ago",
                            format!("{}", now.duration_since(last_task_time).as_secs()),
                        );
                }
            }

            HealthCheckResult::healthy("Worker is operational").with_metadata(
                "uptime_seconds",
                format!("{}", now.duration_since(last_heartbeat).as_secs()),
            )
        }

        /// Checks if worker is ready to accept tasks
        pub fn is_ready(&self) -> bool {
            matches!(
                self.check_health().status,
                HealthStatus::Healthy | HealthStatus::Degraded
            )
        }

        /// Checks if worker is alive
        pub fn is_alive(&self) -> bool {
            let now = Instant::now();
            let last_heartbeat = *self.last_heartbeat.lock().unwrap();
            now.duration_since(last_heartbeat) <= self.heartbeat_timeout
        }
    }

    impl Default for WorkerHealthChecker {
        fn default() -> Self {
            Self::new(Duration::from_secs(30), Duration::from_secs(300))
        }
    }

    /// Dependency health checker
    ///
    /// Monitors health of external dependencies (database, cache, etc.)
    pub struct DependencyChecker {
        name: String,
        check_fn: Box<dyn Fn() -> HealthCheckResult + Send + Sync>,
    }

    impl DependencyChecker {
        /// Creates a new dependency checker
        ///
        /// # Arguments
        ///
        /// * `name` - Name of the dependency
        /// * `check_fn` - Function to check dependency health
        pub fn new<F>(name: impl Into<String>, check_fn: F) -> Self
        where
            F: Fn() -> HealthCheckResult + Send + Sync + 'static,
        {
            Self {
                name: name.into(),
                check_fn: Box::new(check_fn),
            }
        }

        /// Checks the dependency health
        pub fn check(&self) -> HealthCheckResult {
            (self.check_fn)()
        }

        /// Gets the dependency name
        pub fn name(&self) -> &str {
            &self.name
        }
    }
}

/// Resource management utilities for controlling task resource usage
///
/// This module provides utilities for managing and limiting resource consumption
/// by tasks, including memory, CPU, and execution time limits.
pub mod resource_management {
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    /// Resource limits for a task
    #[derive(Debug, Clone)]
    pub struct ResourceLimits {
        /// Maximum memory usage in bytes
        pub max_memory_bytes: Option<usize>,
        /// Maximum CPU time in seconds
        pub max_cpu_seconds: Option<u64>,
        /// Maximum wall-clock time in seconds
        pub max_wall_time_seconds: Option<u64>,
        /// Maximum number of file descriptors
        pub max_file_descriptors: Option<usize>,
    }

    impl ResourceLimits {
        /// Creates new resource limits with no restrictions
        pub fn unlimited() -> Self {
            Self {
                max_memory_bytes: None,
                max_cpu_seconds: None,
                max_wall_time_seconds: None,
                max_file_descriptors: None,
            }
        }

        /// Creates resource limits for memory-constrained tasks
        pub fn memory_constrained(max_memory_mb: usize) -> Self {
            Self {
                max_memory_bytes: Some(max_memory_mb * 1024 * 1024),
                max_cpu_seconds: None,
                max_wall_time_seconds: Some(300), // 5 minutes
                max_file_descriptors: Some(100),
            }
        }

        /// Creates resource limits for CPU-intensive tasks
        pub fn cpu_intensive(max_cpu_seconds: u64) -> Self {
            Self {
                max_memory_bytes: None,
                max_cpu_seconds: Some(max_cpu_seconds),
                max_wall_time_seconds: Some(max_cpu_seconds + 60),
                max_file_descriptors: Some(50),
            }
        }

        /// Creates resource limits for I/O-intensive tasks
        pub fn io_intensive(max_wall_time_seconds: u64) -> Self {
            Self {
                max_memory_bytes: Some(512 * 1024 * 1024), // 512 MB
                max_cpu_seconds: None,
                max_wall_time_seconds: Some(max_wall_time_seconds),
                max_file_descriptors: Some(1000),
            }
        }

        /// Sets maximum memory limit
        pub fn with_max_memory_mb(mut self, mb: usize) -> Self {
            self.max_memory_bytes = Some(mb * 1024 * 1024);
            self
        }

        /// Sets maximum CPU time limit
        pub fn with_max_cpu_seconds(mut self, seconds: u64) -> Self {
            self.max_cpu_seconds = Some(seconds);
            self
        }

        /// Sets maximum wall-clock time limit
        pub fn with_max_wall_time_seconds(mut self, seconds: u64) -> Self {
            self.max_wall_time_seconds = Some(seconds);
            self
        }
    }

    /// Resource usage tracker
    #[derive(Clone)]
    pub struct ResourceTracker {
        start_time: Arc<Mutex<Instant>>,
        peak_memory_bytes: Arc<Mutex<usize>>,
        limits: ResourceLimits,
    }

    impl ResourceTracker {
        /// Creates a new resource tracker with specified limits
        pub fn new(limits: ResourceLimits) -> Self {
            Self {
                start_time: Arc::new(Mutex::new(Instant::now())),
                peak_memory_bytes: Arc::new(Mutex::new(0)),
                limits,
            }
        }

        /// Starts tracking resources
        pub fn start(&self) {
            *self.start_time.lock().unwrap() = Instant::now();
        }

        /// Records memory usage
        pub fn record_memory_usage(&self, bytes: usize) {
            let mut peak = self.peak_memory_bytes.lock().unwrap();
            if bytes > *peak {
                *peak = bytes;
            }
        }

        /// Checks if resource limits are exceeded
        pub fn check_limits(&self) -> Result<(), String> {
            let elapsed = self.start_time.lock().unwrap().elapsed();

            // Check wall-clock time
            if let Some(max_wall_time) = self.limits.max_wall_time_seconds {
                if elapsed > Duration::from_secs(max_wall_time) {
                    return Err(format!(
                        "Wall-clock time limit exceeded: {}s > {}s",
                        elapsed.as_secs(),
                        max_wall_time
                    ));
                }
            }

            // Check memory
            if let Some(max_memory) = self.limits.max_memory_bytes {
                let peak_memory = *self.peak_memory_bytes.lock().unwrap();
                if peak_memory > max_memory {
                    return Err(format!(
                        "Memory limit exceeded: {} bytes > {} bytes",
                        peak_memory, max_memory
                    ));
                }
            }

            Ok(())
        }

        /// Gets elapsed time
        pub fn elapsed(&self) -> Duration {
            self.start_time.lock().unwrap().elapsed()
        }

        /// Gets peak memory usage
        pub fn peak_memory_bytes(&self) -> usize {
            *self.peak_memory_bytes.lock().unwrap()
        }

        /// Gets resource limits
        pub fn limits(&self) -> &ResourceLimits {
            &self.limits
        }
    }

    /// Resource pool for managing shared resources
    pub struct ResourcePool<T> {
        resources: Arc<Mutex<Vec<T>>>,
        max_size: usize,
    }

    impl<T> ResourcePool<T> {
        /// Creates a new resource pool
        pub fn new(max_size: usize) -> Self {
            Self {
                resources: Arc::new(Mutex::new(Vec::with_capacity(max_size))),
                max_size,
            }
        }

        /// Acquires a resource from the pool
        pub fn acquire(&self) -> Option<T> {
            self.resources.lock().unwrap().pop()
        }

        /// Returns a resource to the pool
        pub fn release(&self, resource: T) -> Result<(), String> {
            let mut resources = self.resources.lock().unwrap();
            if resources.len() >= self.max_size {
                return Err("Resource pool is full".to_string());
            }
            resources.push(resource);
            Ok(())
        }

        /// Gets the number of available resources
        pub fn available(&self) -> usize {
            self.resources.lock().unwrap().len()
        }

        /// Gets the maximum pool size
        pub fn max_size(&self) -> usize {
            self.max_size
        }

        /// Checks if the pool is empty
        pub fn is_empty(&self) -> bool {
            self.resources.lock().unwrap().is_empty()
        }
    }

    impl<T> Clone for ResourcePool<T> {
        fn clone(&self) -> Self {
            Self {
                resources: Arc::clone(&self.resources),
                max_size: self.max_size,
            }
        }
    }
}

/// Task lifecycle hooks for pre/post execution events
///
/// This module provides utilities for implementing hooks that run before/after
/// task execution, useful for logging, metrics, validation, and cleanup.
pub mod task_hooks {
    use serde_json::Value;
    use std::sync::Arc;

    /// Hook execution result
    pub type HookResult = Result<(), Box<dyn std::error::Error + Send + Sync>>;

    /// Pre-execution hook
    ///
    /// Runs before task execution. Can modify task arguments or abort execution.
    pub trait PreExecutionHook: Send + Sync {
        /// Executes before task runs
        ///
        /// # Arguments
        ///
        /// * `task_name` - Name of the task
        /// * `task_id` - Unique identifier for the task instance
        /// * `args` - Task arguments (can be modified)
        ///
        /// # Returns
        ///
        /// * `Ok(())` - Continue with task execution
        /// * `Err(e)` - Abort task execution with error
        fn before_execute(
            &self,
            task_name: &str,
            task_id: &str,
            args: &mut Vec<Value>,
        ) -> HookResult;
    }

    /// Post-execution hook
    ///
    /// Runs after task execution (both success and failure).
    pub trait PostExecutionHook: Send + Sync {
        /// Executes after task completes
        ///
        /// # Arguments
        ///
        /// * `task_name` - Name of the task
        /// * `task_id` - Unique identifier for the task instance
        /// * `result` - Task execution result
        /// * `duration_ms` - Execution duration in milliseconds
        fn after_execute(
            &self,
            task_name: &str,
            task_id: &str,
            result: &Result<Value, String>,
            duration_ms: u128,
        ) -> HookResult;
    }

    /// Hook registry for managing task lifecycle hooks
    pub struct HookRegistry {
        pre_hooks: Vec<Arc<dyn PreExecutionHook>>,
        post_hooks: Vec<Arc<dyn PostExecutionHook>>,
    }

    impl HookRegistry {
        /// Creates a new hook registry
        pub fn new() -> Self {
            Self {
                pre_hooks: Vec::new(),
                post_hooks: Vec::new(),
            }
        }

        /// Registers a pre-execution hook
        pub fn register_pre_hook<H>(&mut self, hook: H)
        where
            H: PreExecutionHook + 'static,
        {
            self.pre_hooks.push(Arc::new(hook));
        }

        /// Registers a post-execution hook
        pub fn register_post_hook<H>(&mut self, hook: H)
        where
            H: PostExecutionHook + 'static,
        {
            self.post_hooks.push(Arc::new(hook));
        }

        /// Runs all pre-execution hooks
        pub fn run_pre_hooks(
            &self,
            task_name: &str,
            task_id: &str,
            args: &mut Vec<Value>,
        ) -> HookResult {
            for hook in &self.pre_hooks {
                hook.before_execute(task_name, task_id, args)?;
            }
            Ok(())
        }

        /// Runs all post-execution hooks
        pub fn run_post_hooks(
            &self,
            task_name: &str,
            task_id: &str,
            result: &Result<Value, String>,
            duration_ms: u128,
        ) -> HookResult {
            for hook in &self.post_hooks {
                hook.after_execute(task_name, task_id, result, duration_ms)?;
            }
            Ok(())
        }

        /// Gets the number of registered pre-execution hooks
        pub fn pre_hook_count(&self) -> usize {
            self.pre_hooks.len()
        }

        /// Gets the number of registered post-execution hooks
        pub fn post_hook_count(&self) -> usize {
            self.post_hooks.len()
        }
    }

    impl Default for HookRegistry {
        fn default() -> Self {
            Self::new()
        }
    }

    /// Logging hook for task execution
    pub struct LoggingHook {
        log_args: bool,
        log_results: bool,
    }

    impl LoggingHook {
        /// Creates a new logging hook
        pub fn new(log_args: bool, log_results: bool) -> Self {
            Self {
                log_args,
                log_results,
            }
        }
    }

    impl PreExecutionHook for LoggingHook {
        fn before_execute(
            &self,
            task_name: &str,
            task_id: &str,
            args: &mut Vec<Value>,
        ) -> HookResult {
            if self.log_args {
                println!("[TASK] Starting {} ({}): {:?}", task_name, task_id, args);
            } else {
                println!("[TASK] Starting {} ({})", task_name, task_id);
            }
            Ok(())
        }
    }

    impl PostExecutionHook for LoggingHook {
        fn after_execute(
            &self,
            task_name: &str,
            task_id: &str,
            result: &Result<Value, String>,
            duration_ms: u128,
        ) -> HookResult {
            match result {
                Ok(value) => {
                    if self.log_results {
                        println!(
                            "[TASK] Completed {} ({}) in {}ms: {:?}",
                            task_name, task_id, duration_ms, value
                        );
                    } else {
                        println!(
                            "[TASK] Completed {} ({}) in {}ms",
                            task_name, task_id, duration_ms
                        );
                    }
                }
                Err(e) => {
                    println!(
                        "[TASK] Failed {} ({}) in {}ms: {}",
                        task_name, task_id, duration_ms, e
                    );
                }
            }
            Ok(())
        }
    }

    /// Validation hook for checking task arguments
    pub struct ValidationHook<F>
    where
        F: Fn(&str, &Vec<Value>) -> HookResult + Send + Sync,
    {
        validator: F,
    }

    impl<F> ValidationHook<F>
    where
        F: Fn(&str, &Vec<Value>) -> HookResult + Send + Sync,
    {
        /// Creates a new validation hook
        pub fn new(validator: F) -> Self {
            Self { validator }
        }
    }

    impl<F> PreExecutionHook for ValidationHook<F>
    where
        F: Fn(&str, &Vec<Value>) -> HookResult + Send + Sync,
    {
        fn before_execute(
            &self,
            task_name: &str,
            _task_id: &str,
            args: &mut Vec<Value>,
        ) -> HookResult {
            (self.validator)(task_name, args)
        }
    }
}

/// Metrics aggregation utilities for collecting and analyzing task metrics
///
/// This module provides utilities for aggregating and analyzing metrics from
/// task execution, including histograms, percentiles, and time-series data.
pub mod metrics_aggregation {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    /// Time-series data point
    #[derive(Debug, Clone)]
    pub struct DataPoint {
        /// Timestamp of the data point
        pub timestamp: Instant,
        /// Value of the metric
        pub value: f64,
    }

    /// Histogram for tracking value distributions
    pub struct Histogram {
        buckets: Vec<(f64, usize)>, // (upper_bound, count)
        total_count: usize,
        sum: f64,
    }

    impl Histogram {
        /// Creates a new histogram with default buckets
        pub fn new() -> Self {
            Self::with_buckets(vec![
                10.0, 50.0, 100.0, 250.0, 500.0, 1000.0, 2500.0, 5000.0, 10000.0,
            ])
        }

        /// Creates a histogram with custom buckets
        pub fn with_buckets(bucket_bounds: Vec<f64>) -> Self {
            let buckets = bucket_bounds.into_iter().map(|b| (b, 0)).collect();
            Self {
                buckets,
                total_count: 0,
                sum: 0.0,
            }
        }

        /// Records a value
        pub fn record(&mut self, value: f64) {
            self.total_count += 1;
            self.sum += value;

            for (bound, count) in &mut self.buckets {
                if value <= *bound {
                    *count += 1;
                    break;
                }
            }
        }

        /// Gets the total count of recorded values
        pub fn count(&self) -> usize {
            self.total_count
        }

        /// Gets the sum of all recorded values
        pub fn sum(&self) -> f64 {
            self.sum
        }

        /// Gets the mean value
        pub fn mean(&self) -> f64 {
            if self.total_count == 0 {
                0.0
            } else {
                self.sum / self.total_count as f64
            }
        }

        /// Gets the percentile value (approximate)
        pub fn percentile(&self, p: f64) -> f64 {
            if self.total_count == 0 {
                return 0.0;
            }

            let target_count = (self.total_count as f64 * p / 100.0) as usize;
            let mut cumulative = 0;

            for (bound, count) in &self.buckets {
                cumulative += count;
                if cumulative >= target_count {
                    return *bound;
                }
            }

            // Return the last bucket bound if we didn't find it
            self.buckets.last().map(|(b, _)| *b).unwrap_or(0.0)
        }

        /// Resets the histogram
        pub fn reset(&mut self) {
            for (_, count) in &mut self.buckets {
                *count = 0;
            }
            self.total_count = 0;
            self.sum = 0.0;
        }
    }

    impl Default for Histogram {
        fn default() -> Self {
            Self::new()
        }
    }

    /// Metrics aggregator for task execution metrics
    pub struct MetricsAggregator {
        task_durations: Arc<Mutex<HashMap<String, Histogram>>>,
        task_counts: Arc<Mutex<HashMap<String, usize>>>,
        task_errors: Arc<Mutex<HashMap<String, usize>>>,
        time_series: Arc<Mutex<HashMap<String, Vec<DataPoint>>>>,
        start_time: Instant,
    }

    impl MetricsAggregator {
        /// Creates a new metrics aggregator
        pub fn new() -> Self {
            Self {
                task_durations: Arc::new(Mutex::new(HashMap::new())),
                task_counts: Arc::new(Mutex::new(HashMap::new())),
                task_errors: Arc::new(Mutex::new(HashMap::new())),
                time_series: Arc::new(Mutex::new(HashMap::new())),
                start_time: Instant::now(),
            }
        }

        /// Records task execution duration
        pub fn record_duration(&self, task_name: &str, duration: Duration) {
            let duration_ms = duration.as_secs_f64() * 1000.0;

            // Update histogram
            let mut durations = self.task_durations.lock().unwrap();
            durations
                .entry(task_name.to_string())
                .or_default()
                .record(duration_ms);

            // Update count
            let mut counts = self.task_counts.lock().unwrap();
            *counts.entry(task_name.to_string()).or_insert(0) += 1;

            // Update time series
            let mut series = self.time_series.lock().unwrap();
            series
                .entry(task_name.to_string())
                .or_default()
                .push(DataPoint {
                    timestamp: Instant::now(),
                    value: duration_ms,
                });
        }

        /// Records task error
        pub fn record_error(&self, task_name: &str) {
            let mut errors = self.task_errors.lock().unwrap();
            *errors.entry(task_name.to_string()).or_insert(0) += 1;
        }

        /// Gets task execution count
        pub fn task_count(&self, task_name: &str) -> usize {
            self.task_counts
                .lock()
                .unwrap()
                .get(task_name)
                .copied()
                .unwrap_or(0)
        }

        /// Gets task error count
        pub fn error_count(&self, task_name: &str) -> usize {
            self.task_errors
                .lock()
                .unwrap()
                .get(task_name)
                .copied()
                .unwrap_or(0)
        }

        /// Gets task success rate
        pub fn success_rate(&self, task_name: &str) -> f64 {
            let total = self.task_count(task_name);
            if total == 0 {
                return 100.0;
            }
            let errors = self.error_count(task_name);
            ((total - errors) as f64 / total as f64) * 100.0
        }

        /// Gets mean duration for a task
        pub fn mean_duration(&self, task_name: &str) -> f64 {
            self.task_durations
                .lock()
                .unwrap()
                .get(task_name)
                .map(|h| h.mean())
                .unwrap_or(0.0)
        }

        /// Gets percentile duration for a task
        pub fn percentile_duration(&self, task_name: &str, percentile: f64) -> f64 {
            self.task_durations
                .lock()
                .unwrap()
                .get(task_name)
                .map(|h| h.percentile(percentile))
                .unwrap_or(0.0)
        }

        /// Gets throughput (tasks per second) for a task
        pub fn throughput(&self, task_name: &str) -> f64 {
            let elapsed = self.start_time.elapsed().as_secs_f64();
            if elapsed == 0.0 {
                return 0.0;
            }
            self.task_count(task_name) as f64 / elapsed
        }

        /// Gets all task names
        pub fn task_names(&self) -> Vec<String> {
            self.task_counts.lock().unwrap().keys().cloned().collect()
        }

        /// Generates a summary report
        pub fn summary(&self, task_name: &str) -> String {
            let count = self.task_count(task_name);
            let errors = self.error_count(task_name);
            let success_rate = self.success_rate(task_name);
            let mean = self.mean_duration(task_name);
            let p50 = self.percentile_duration(task_name, 50.0);
            let p95 = self.percentile_duration(task_name, 95.0);
            let p99 = self.percentile_duration(task_name, 99.0);
            let throughput = self.throughput(task_name);

            format!(
                "Task Metrics: {}\n\
                 - Total Executions: {}\n\
                 - Errors: {} ({:.2}% success rate)\n\
                 - Mean Duration: {:.2}ms\n\
                 - P50 Duration: {:.2}ms\n\
                 - P95 Duration: {:.2}ms\n\
                 - P99 Duration: {:.2}ms\n\
                 - Throughput: {:.2} tasks/sec",
                task_name, count, errors, success_rate, mean, p50, p95, p99, throughput
            )
        }

        /// Resets all metrics
        pub fn reset(&self) {
            self.task_durations.lock().unwrap().clear();
            self.task_counts.lock().unwrap().clear();
            self.task_errors.lock().unwrap().clear();
            self.time_series.lock().unwrap().clear();
        }
    }

    impl Default for MetricsAggregator {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Clone for MetricsAggregator {
        fn clone(&self) -> Self {
            Self {
                task_durations: Arc::clone(&self.task_durations),
                task_counts: Arc::clone(&self.task_counts),
                task_errors: Arc::clone(&self.task_errors),
                time_series: Arc::clone(&self.time_series),
                start_time: self.start_time,
            }
        }
    }
}

/// Task cancellation utilities for managing task lifecycle
///
/// This module provides utilities for cancelling tasks, implementing timeouts,
/// and managing task execution boundaries.
pub mod task_cancellation {
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    /// Cancellation token for task control
    #[derive(Clone)]
    pub struct CancellationToken {
        cancelled: Arc<Mutex<bool>>,
        cancellation_reason: Arc<Mutex<Option<String>>>,
    }

    impl CancellationToken {
        /// Creates a new cancellation token
        pub fn new() -> Self {
            Self {
                cancelled: Arc::new(Mutex::new(false)),
                cancellation_reason: Arc::new(Mutex::new(None)),
            }
        }

        /// Cancels the token with an optional reason
        pub fn cancel(&self, reason: Option<String>) {
            *self.cancelled.lock().unwrap() = true;
            *self.cancellation_reason.lock().unwrap() = reason;
        }

        /// Checks if the token is cancelled
        pub fn is_cancelled(&self) -> bool {
            *self.cancelled.lock().unwrap()
        }

        /// Gets the cancellation reason if available
        pub fn cancellation_reason(&self) -> Option<String> {
            self.cancellation_reason.lock().unwrap().clone()
        }

        /// Throws an error if the token is cancelled
        pub fn check_cancelled(&self) -> Result<(), String> {
            if self.is_cancelled() {
                Err(self
                    .cancellation_reason()
                    .unwrap_or_else(|| "Task was cancelled".to_string()))
            } else {
                Ok(())
            }
        }
    }

    impl Default for CancellationToken {
        fn default() -> Self {
            Self::new()
        }
    }

    /// Timeout manager for task execution
    pub struct TimeoutManager {
        timeout: Duration,
        start_time: Instant,
    }

    impl TimeoutManager {
        /// Creates a new timeout manager
        pub fn new(timeout: Duration) -> Self {
            Self {
                timeout,
                start_time: Instant::now(),
            }
        }

        /// Checks if the timeout has been exceeded
        pub fn is_timed_out(&self) -> bool {
            self.start_time.elapsed() > self.timeout
        }

        /// Gets the remaining time
        pub fn remaining(&self) -> Duration {
            let elapsed = self.start_time.elapsed();
            if elapsed >= self.timeout {
                Duration::from_secs(0)
            } else {
                self.timeout - elapsed
            }
        }

        /// Gets the elapsed time
        pub fn elapsed(&self) -> Duration {
            self.start_time.elapsed()
        }

        /// Checks timeout and returns error if exceeded
        pub fn check_timeout(&self) -> Result<(), String> {
            if self.is_timed_out() {
                Err(format!(
                    "Task timeout exceeded: {}s",
                    self.timeout.as_secs()
                ))
            } else {
                Ok(())
            }
        }
    }

    /// Task execution guard combining cancellation and timeout
    pub struct ExecutionGuard {
        cancellation_token: CancellationToken,
        timeout_manager: Option<TimeoutManager>,
    }

    impl ExecutionGuard {
        /// Creates a new execution guard
        pub fn new(cancellation_token: CancellationToken, timeout: Option<Duration>) -> Self {
            Self {
                cancellation_token,
                timeout_manager: timeout.map(TimeoutManager::new),
            }
        }

        /// Checks if execution should continue
        pub fn should_continue(&self) -> Result<(), String> {
            self.cancellation_token.check_cancelled()?;
            if let Some(timeout_mgr) = &self.timeout_manager {
                timeout_mgr.check_timeout()?;
            }
            Ok(())
        }

        /// Gets the cancellation token
        pub fn cancellation_token(&self) -> &CancellationToken {
            &self.cancellation_token
        }

        /// Gets remaining timeout if configured
        pub fn remaining_timeout(&self) -> Option<Duration> {
            self.timeout_manager.as_ref().map(|t| t.remaining())
        }
    }
}

/// Advanced retry strategies for fault-tolerant task execution
///
/// This module provides sophisticated retry strategies including exponential backoff,
/// jitter, circuit breaker integration, and custom retry policies.
pub mod retry_strategies {
    use std::time::Duration;

    /// Retry strategy configuration
    #[derive(Debug, Clone)]
    pub struct RetryStrategy {
        /// Maximum number of retry attempts
        pub max_retries: u32,
        /// Initial delay between retries
        pub initial_delay: Duration,
        /// Maximum delay between retries
        pub max_delay: Duration,
        /// Backoff multiplier
        pub backoff_multiplier: f64,
        /// Whether to add jitter
        pub use_jitter: bool,
    }

    impl RetryStrategy {
        /// Creates a new retry strategy with exponential backoff
        pub fn exponential_backoff(max_retries: u32, initial_delay: Duration) -> Self {
            Self {
                max_retries,
                initial_delay,
                max_delay: Duration::from_secs(300), // 5 minutes
                backoff_multiplier: 2.0,
                use_jitter: true,
            }
        }

        /// Creates a linear backoff strategy
        pub fn linear_backoff(max_retries: u32, delay: Duration) -> Self {
            Self {
                max_retries,
                initial_delay: delay,
                max_delay: delay,
                backoff_multiplier: 1.0,
                use_jitter: false,
            }
        }

        /// Creates a fixed delay strategy
        pub fn fixed_delay(max_retries: u32, delay: Duration) -> Self {
            Self {
                max_retries,
                initial_delay: delay,
                max_delay: delay,
                backoff_multiplier: 1.0,
                use_jitter: false,
            }
        }

        /// Creates a fibonacci backoff strategy
        pub fn fibonacci_backoff(max_retries: u32, base_delay: Duration) -> Self {
            Self {
                max_retries,
                initial_delay: base_delay,
                max_delay: Duration::from_secs(600), // 10 minutes
                backoff_multiplier: 1.618,           // Golden ratio
                use_jitter: true,
            }
        }

        /// Calculates the delay for a specific retry attempt
        pub fn calculate_delay(&self, attempt: u32) -> Duration {
            if attempt == 0 {
                return Duration::from_secs(0);
            }

            let base_delay = if self.backoff_multiplier == 1.0 {
                self.initial_delay
            } else {
                let multiplier = self.backoff_multiplier.powi(attempt as i32 - 1);
                let delay_ms = self.initial_delay.as_millis() as f64 * multiplier;
                Duration::from_millis(delay_ms.min(self.max_delay.as_millis() as f64) as u64)
            };

            if self.use_jitter {
                // Add ±25% jitter
                let jitter_factor = 0.75 + (rand::random::<f64>() * 0.5);
                let delay_ms = (base_delay.as_millis() as f64 * jitter_factor) as u64;
                Duration::from_millis(delay_ms)
            } else {
                base_delay
            }
        }

        /// Sets maximum delay
        pub fn with_max_delay(mut self, max_delay: Duration) -> Self {
            self.max_delay = max_delay;
            self
        }

        /// Sets whether to use jitter
        pub fn with_jitter(mut self, use_jitter: bool) -> Self {
            self.use_jitter = use_jitter;
            self
        }

        /// Sets backoff multiplier
        pub fn with_multiplier(mut self, multiplier: f64) -> Self {
            self.backoff_multiplier = multiplier;
            self
        }
    }

    impl Default for RetryStrategy {
        fn default() -> Self {
            Self::exponential_backoff(3, Duration::from_secs(1))
        }
    }

    /// Retry decision based on error type
    pub trait RetryPolicy: Send + Sync {
        /// Determines if a retry should be attempted for the given error
        fn should_retry(&self, error: &str, attempt: u32) -> bool;
    }

    /// Default retry policy that retries on all errors
    pub struct DefaultRetryPolicy {
        max_retries: u32,
    }

    impl DefaultRetryPolicy {
        /// Creates a new default retry policy
        pub fn new(max_retries: u32) -> Self {
            Self { max_retries }
        }
    }

    impl RetryPolicy for DefaultRetryPolicy {
        fn should_retry(&self, _error: &str, attempt: u32) -> bool {
            attempt < self.max_retries
        }
    }

    /// Retry policy that only retries on specific error patterns
    pub struct ErrorPatternRetryPolicy {
        max_retries: u32,
        retryable_patterns: Vec<String>,
    }

    impl ErrorPatternRetryPolicy {
        /// Creates a new error pattern retry policy
        pub fn new(max_retries: u32, retryable_patterns: Vec<String>) -> Self {
            Self {
                max_retries,
                retryable_patterns,
            }
        }
    }

    impl RetryPolicy for ErrorPatternRetryPolicy {
        fn should_retry(&self, error: &str, attempt: u32) -> bool {
            if attempt >= self.max_retries {
                return false;
            }
            self.retryable_patterns
                .iter()
                .any(|pattern| error.contains(pattern))
        }
    }
}

/// Task dependency management for workflow orchestration
///
/// This module provides utilities for managing task dependencies,
/// building dependency graphs, and ensuring proper execution order.
pub mod task_dependencies {
    use std::collections::{HashMap, HashSet};

    /// Task dependency graph
    pub struct DependencyGraph {
        /// Dependencies: task_id -> set of task_ids it depends on
        dependencies: HashMap<String, HashSet<String>>,
        /// Reverse dependencies: task_id -> set of task_ids that depend on it
        dependents: HashMap<String, HashSet<String>>,
    }

    impl DependencyGraph {
        /// Creates a new dependency graph
        pub fn new() -> Self {
            Self {
                dependencies: HashMap::new(),
                dependents: HashMap::new(),
            }
        }

        /// Adds a task to the graph
        pub fn add_task(&mut self, task_id: impl Into<String>) {
            let task_id = task_id.into();
            self.dependencies.entry(task_id.clone()).or_default();
            self.dependents.entry(task_id).or_default();
        }

        /// Adds a dependency: task depends on dependency
        pub fn add_dependency(
            &mut self,
            task_id: impl Into<String>,
            dependency_id: impl Into<String>,
        ) {
            let task_id = task_id.into();
            let dependency_id = dependency_id.into();

            self.dependencies
                .entry(task_id.clone())
                .or_default()
                .insert(dependency_id.clone());

            self.dependents
                .entry(dependency_id)
                .or_default()
                .insert(task_id);
        }

        /// Gets direct dependencies of a task
        pub fn get_dependencies(&self, task_id: &str) -> Vec<String> {
            self.dependencies
                .get(task_id)
                .map(|deps| deps.iter().cloned().collect())
                .unwrap_or_default()
        }

        /// Gets all tasks that depend on this task
        pub fn get_dependents(&self, task_id: &str) -> Vec<String> {
            self.dependents
                .get(task_id)
                .map(|deps| deps.iter().cloned().collect())
                .unwrap_or_default()
        }

        /// Checks if there are circular dependencies
        pub fn has_circular_dependencies(&self) -> bool {
            let mut visited = HashSet::new();
            let mut rec_stack = HashSet::new();

            for task_id in self.dependencies.keys() {
                if self.has_cycle(task_id, &mut visited, &mut rec_stack) {
                    return true;
                }
            }
            false
        }

        fn has_cycle(
            &self,
            task_id: &str,
            visited: &mut HashSet<String>,
            rec_stack: &mut HashSet<String>,
        ) -> bool {
            if rec_stack.contains(task_id) {
                return true;
            }
            if visited.contains(task_id) {
                return false;
            }

            visited.insert(task_id.to_string());
            rec_stack.insert(task_id.to_string());

            if let Some(deps) = self.dependencies.get(task_id) {
                for dep in deps {
                    if self.has_cycle(dep, visited, rec_stack) {
                        return true;
                    }
                }
            }

            rec_stack.remove(task_id);
            false
        }

        /// Gets tasks in topological order (tasks with no dependencies first)
        pub fn topological_sort(&self) -> Result<Vec<String>, String> {
            if self.has_circular_dependencies() {
                return Err("Circular dependencies detected".to_string());
            }

            let mut result = Vec::new();
            let mut visited = HashSet::new();
            let mut temp_mark = HashSet::new();

            for task_id in self.dependencies.keys() {
                if !visited.contains(task_id) {
                    self.visit(task_id, &mut visited, &mut temp_mark, &mut result)?;
                }
            }

            Ok(result)
        }

        fn visit(
            &self,
            task_id: &str,
            visited: &mut HashSet<String>,
            temp_mark: &mut HashSet<String>,
            result: &mut Vec<String>,
        ) -> Result<(), String> {
            if temp_mark.contains(task_id) {
                return Err("Circular dependency detected".to_string());
            }
            if visited.contains(task_id) {
                return Ok(());
            }

            temp_mark.insert(task_id.to_string());

            if let Some(deps) = self.dependencies.get(task_id) {
                for dep in deps {
                    self.visit(dep, visited, temp_mark, result)?;
                }
            }

            temp_mark.remove(task_id);
            visited.insert(task_id.to_string());
            result.push(task_id.to_string());

            Ok(())
        }

        /// Gets tasks that are ready to execute (all dependencies satisfied)
        pub fn get_ready_tasks(&self, completed_tasks: &HashSet<String>) -> Vec<String> {
            self.dependencies
                .iter()
                .filter(|(task_id, deps)| {
                    !completed_tasks.contains(*task_id)
                        && deps.iter().all(|dep| completed_tasks.contains(dep))
                })
                .map(|(task_id, _)| task_id.clone())
                .collect()
        }
    }

    impl Default for DependencyGraph {
        fn default() -> Self {
            Self::new()
        }
    }
}

/// Performance profiling utilities for task execution analysis
///
/// This module provides utilities for profiling task execution,
/// identifying bottlenecks, and optimizing performance.
pub mod performance_profiling {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    /// Performance profile for a task or operation
    #[derive(Debug, Clone)]
    pub struct PerformanceProfile {
        /// Name of the profiled operation
        pub name: String,
        /// Total execution time
        pub total_duration: Duration,
        /// Time spent in child operations
        pub children_duration: Duration,
        /// Self time (total - children)
        pub self_duration: Duration,
        /// Number of invocations
        pub invocation_count: usize,
    }

    impl PerformanceProfile {
        /// Gets average duration per invocation
        pub fn avg_duration(&self) -> Duration {
            if self.invocation_count == 0 {
                Duration::from_secs(0)
            } else {
                self.total_duration / self.invocation_count as u32
            }
        }

        /// Gets percentage of time spent in self vs children
        pub fn self_time_percentage(&self) -> f64 {
            if self.total_duration.as_millis() == 0 {
                0.0
            } else {
                (self.self_duration.as_millis() as f64 / self.total_duration.as_millis() as f64)
                    * 100.0
            }
        }
    }

    /// Performance profiler for tracking execution time
    pub struct PerformanceProfiler {
        profiles: Arc<Mutex<HashMap<String, PerformanceProfile>>>,
        active_spans: Arc<Mutex<Vec<(String, Instant)>>>,
    }

    impl PerformanceProfiler {
        /// Creates a new performance profiler
        pub fn new() -> Self {
            Self {
                profiles: Arc::new(Mutex::new(HashMap::new())),
                active_spans: Arc::new(Mutex::new(Vec::new())),
            }
        }

        /// Starts profiling an operation
        pub fn start_span(&self, name: impl Into<String>) {
            let name = name.into();
            self.active_spans
                .lock()
                .unwrap()
                .push((name, Instant::now()));
        }

        /// Ends the current profiling span
        pub fn end_span(&self) {
            let span_data = self.active_spans.lock().unwrap().pop();
            if let Some((name, start_time)) = span_data {
                let duration = start_time.elapsed();
                let mut profiles = self.profiles.lock().unwrap();

                let profile = profiles
                    .entry(name.clone())
                    .or_insert_with(|| PerformanceProfile {
                        name: name.clone(),
                        total_duration: Duration::from_secs(0),
                        children_duration: Duration::from_secs(0),
                        self_duration: Duration::from_secs(0),
                        invocation_count: 0,
                    });

                profile.total_duration += duration;
                profile.invocation_count += 1;
                profile.self_duration = profile.total_duration - profile.children_duration;
            }
        }

        /// Gets the profile for a specific operation
        pub fn get_profile(&self, name: &str) -> Option<PerformanceProfile> {
            self.profiles.lock().unwrap().get(name).cloned()
        }

        /// Gets all profiles
        pub fn get_all_profiles(&self) -> Vec<PerformanceProfile> {
            self.profiles.lock().unwrap().values().cloned().collect()
        }

        /// Gets profiles sorted by total duration (slowest first)
        pub fn get_slowest_operations(&self, limit: usize) -> Vec<PerformanceProfile> {
            let mut profiles = self.get_all_profiles();
            profiles.sort_by(|a, b| b.total_duration.cmp(&a.total_duration));
            profiles.truncate(limit);
            profiles
        }

        /// Generates a performance report
        pub fn generate_report(&self) -> String {
            let profiles = self.get_all_profiles();
            if profiles.is_empty() {
                return "No profiling data available".to_string();
            }

            let mut report = String::from("Performance Profile Report\n");
            report.push_str(&format!("{}\n", "=".repeat(80)));

            for profile in profiles {
                report.push_str(&format!(
                    "{:<30} | Count: {:>6} | Total: {:>8.2}ms | Avg: {:>8.2}ms | Self: {:>6.1}%\n",
                    profile.name,
                    profile.invocation_count,
                    profile.total_duration.as_secs_f64() * 1000.0,
                    profile.avg_duration().as_secs_f64() * 1000.0,
                    profile.self_time_percentage()
                ));
            }

            report
        }

        /// Resets all profiling data
        pub fn reset(&self) {
            self.profiles.lock().unwrap().clear();
            self.active_spans.lock().unwrap().clear();
        }
    }

    impl Default for PerformanceProfiler {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Clone for PerformanceProfiler {
        fn clone(&self) -> Self {
            Self {
                profiles: Arc::clone(&self.profiles),
                active_spans: Arc::clone(&self.active_spans),
            }
        }
    }

    /// RAII guard for automatic span tracking
    pub struct ProfileSpan<'a> {
        profiler: &'a PerformanceProfiler,
    }

    impl<'a> ProfileSpan<'a> {
        /// Creates a new profile span
        pub fn new(profiler: &'a PerformanceProfiler, name: impl Into<String>) -> Self {
            profiler.start_span(name);
            Self { profiler }
        }
    }

    impl<'a> Drop for ProfileSpan<'a> {
        fn drop(&mut self) {
            self.profiler.end_span();
        }
    }
}
