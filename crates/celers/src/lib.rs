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
//! celers = { version = "0.2", features = ["redis", "backend-redis", "json"] }
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
pub use celers_broker_redis::{
    circuit_breaker, dedup, health, monitoring as redis_monitoring, utilities as redis_utilities,
    RedisBroker,
};

#[cfg(feature = "postgres")]
pub use celers_broker_postgres::{
    monitoring as postgres_monitoring, utilities as postgres_utilities, PostgresBroker,
};

#[cfg(feature = "mysql")]
pub use celers_broker_sql::{
    monitoring as mysql_monitoring, utilities as mysql_utilities, MysqlBroker,
};

#[cfg(feature = "amqp")]
pub use celers_broker_amqp::{
    monitoring as amqp_monitoring, utilities as amqp_utilities, AmqpBroker,
};

#[cfg(feature = "sqs")]
pub use celers_broker_sqs::{
    monitoring as sqs_monitoring, optimization, utilities as sqs_utilities, SqsBroker,
};

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

// ---- Module declarations (split from original monolithic lib.rs) ----

/// Prelude module for common imports
pub mod prelude;

/// Convenience functions module
pub mod convenience;

/// Quick start helpers for common use cases
pub mod quick_start;

/// Production-ready configuration presets
pub mod presets;

// Re-export sub-crate modules
mod reexport_modules;
pub use reexport_modules::canvas;
pub use reexport_modules::error;
pub use reexport_modules::protocol;
pub use reexport_modules::rate_limit;
pub use reexport_modules::router;
pub use reexport_modules::worker;

/// Distributed tracing support with OpenTelemetry
#[cfg(feature = "tracing")]
#[path = "tracing_support.rs"]
pub mod tracing;

/// Development utilities for testing
#[cfg(any(test, feature = "dev-utils"))]
pub mod dev_utils;

/// Configuration validation helpers
pub mod config_validation;

/// Compile-time feature validation and conflict detection
pub mod compile_time_validation;

// Validate feature configuration at compile time
#[allow(dead_code)]
const _FEATURE_VALIDATION: () = compile_time_validation::validate_feature_config();

/// Broker selection helpers
pub mod broker_helper;

/// Startup time optimization utilities
pub mod startup_optimization;

/// IDE support and documentation helpers
pub mod ide_support;

/// Quick reference documentation
pub mod quick_reference;

/// Assembly inspection utilities
#[cfg(feature = "dev-utils")]
pub mod assembly_inspection;

/// Workflow templates for common patterns
pub mod workflow_templates;

/// Task composition utilities
pub mod task_composition;

/// Error recovery patterns
pub mod error_recovery;

/// Workflow validation utilities
pub mod workflow_validation;

/// Result aggregation helpers
pub mod result_helpers;

/// Advanced workflow patterns for complex use cases
pub mod advanced_patterns;

/// Monitoring and observability helpers
pub mod monitoring_helpers;

/// Batch processing helpers
pub mod batch_helpers;

/// Health check utilities
pub mod health_check;

/// Resource management utilities
pub mod resource_management;

/// Task lifecycle hooks
pub mod task_hooks;

/// Metrics aggregation utilities
pub mod metrics_aggregation;

/// Task cancellation utilities
pub mod task_cancellation;

/// Advanced retry strategies
pub mod retry_strategies;

/// Task dependency management
pub mod task_dependencies;

/// Performance profiling utilities
pub mod performance_profiling;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod tests_extended;
