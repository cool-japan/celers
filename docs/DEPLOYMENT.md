# CeleRS Deployment Guide

This guide covers deploying CeleRS in production environments using Docker, Kubernetes, and cloud platforms.

**Read this first if you only read one section:** [Building a Worker Image](#building-a-worker-image). CeleRS tasks
are compiled-in Rust code, not modules a generic binary loads at runtime the way Python Celery does. The `celers`
CLI binary this repository ships (and the `Dockerfile` at the repo root builds) is an *operational* tool --
`status`, `inspect`, `control`, `queue`, `dlq`, `schedule`, `backup`/`restore`, `doctor`, and friends -- not a task
executor. Running `celers worker` from that image starts a real worker that connects to your broker and joins the
remote-control channel, but by default it registers **zero tasks**, so it can never execute one
(`crates/celers-cli/src/commands/worker.rs` builds the registry with `TaskRegistry::new()` and prints a warning
about it at startup). Passing `--demo-tasks` registers three harmless built-ins (`demo.echo`, `demo.sleep`,
`demo.fail`) so you can smoke-test broker connectivity end-to-end -- enqueue, dequeue, execute, ack/DLQ -- before
any real task code exists; it is a smoke test, not a path to running your own tasks. Every deployment below that
shows `worker` running from the stock image (with or without `--demo-tasks`) is demonstrating broker connectivity,
CLI operations, and worker lifecycle -- not real task execution. To execute your own tasks you build your own
binary that links `celers-worker` and registers them, and deploy *that* image instead.

## Table of Contents

- [Quick Start](#quick-start)
- [Building a Worker Image](#building-a-worker-image)
- [Docker Deployment](#docker-deployment)
- [Kubernetes Deployment](#kubernetes-deployment)
- [Cloud Platforms](#cloud-platforms)
- [Monitoring & Observability](#monitoring--observability)
- [Performance Tuning](#performance-tuning)
- [Running Beat (the periodic scheduler)](#running-beat-the-periodic-scheduler)
- [Security Best Practices](#security-best-practices)
- [Troubleshooting](#troubleshooting)

## Quick Start

### Prerequisites

- Docker 24.0+ or Kubernetes 1.28+
- Redis 7.0+ or PostgreSQL 16+ or RabbitMQ 3.12+
- 2GB+ RAM per worker instance
- Network connectivity between workers and brokers

### Local Development

`docker-compose.yml` at the repo root starts Redis, PostgreSQL, RabbitMQ, Prometheus, Grafana, and a `celers`
worker container running with `--demo-tasks` (smoke test only -- see the note above and
[Building a Worker Image](#building-a-worker-image)):

```bash
# Start the core stack (redis, postgres, rabbitmq, prometheus, grafana, worker)
docker-compose up -d

# Also start MySQL and the SQS emulator (localstack), for the broker suites
# that need them -- see tests/integration/README.md
docker-compose --profile test up -d

# View logs
docker-compose logs -f worker

# Scale the demo worker container (still only demo.echo/demo.sleep/demo.fail
# -- this proves broker fan-out, not your own task throughput)
docker-compose up -d --scale worker=4

# Stop all services
docker-compose down
```

## Building a Worker Image

This is the part the shipped `celers` image cannot do for you. A minimal worker project:

```
my-worker/
├── Cargo.toml
├── Dockerfile
└── src/
    └── main.rs
```

`Cargo.toml`:

```toml
[package]
name = "my-worker"
version = "0.1.0"
edition = "2021"

[dependencies]
celers-macros = "0.3"
celers-core = "0.3"
celers-broker-redis = "0.3"
celers-worker = "0.3"
tokio = { version = "1", features = ["full"] }
anyhow = "1"
tracing-subscriber = "0.3"

# Required by the code `#[task]` generates -- see the root README's
# "Installation" section for why these two are needed even though
# celers-macros re-exports parts of their API.
serde = { version = "1", features = ["derive"] }
async-trait = "0.1"
```

`src/main.rs` (adapted from `crates/celers-examples/examples/macro_tasks.rs` in this repository, trimmed to one
task):

```rust
use celers_broker_redis::RedisBroker;
use celers_core::TaskRegistry;
use celers_macros::task;
use celers_worker::{Worker, WorkerConfig};

/// Define a task with the #[task] macro. This expands to an `AddTask` type
/// implementing `celers_core::Task`, plus an `AddTaskInput { a: i64, b: i64 }`
/// struct generated from the function signature.
#[task]
async fn add(a: i64, b: i64) -> celers_core::Result<i64> {
    Ok(a + b)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let broker_url =
        std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string());
    let broker = RedisBroker::new(&broker_url, "celery")?;

    // This is the step the shipped `celers` CLI binary cannot take for
    // you: register the tasks this binary actually knows how to run.
    let registry = TaskRegistry::new();
    registry.register(AddTask).await;

    let config = WorkerConfig {
        concurrency: 4,
        max_retries: 3,
        default_timeout_secs: 300,
        ..Default::default()
    };

    let worker = Worker::new(broker, registry, config);
    worker.run().await?;

    Ok(())
}
```

`Dockerfile` (same two-stage pattern as this repository's own `Dockerfile`, building your crate instead of
`celers-cli`):

```dockerfile
FROM rust:1.95.0-slim-bookworm AS builder
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
RUN cargo build --release

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*
RUN useradd -m -u 1000 worker
COPY --from=builder /app/target/release/my-worker /usr/local/bin/my-worker
USER worker
ENV RUST_LOG=info
ENTRYPOINT ["/usr/local/bin/my-worker"]
```

Build and run it exactly like any other container:

```bash
docker build -t my-worker:latest .

# --network must be the Compose network the root docker-compose.yml's redis
# service is on. Compose names it `<project>_celers-network`, where
# <project> defaults to the checkout's directory name; confirm yours with
# `docker compose config | grep -A1 '^networks:'`. For a checkout named
# "celers" (the repository's own directory name) that is:
docker run --rm -e REDIS_URL=redis://redis:6379 --network celers_celers-network my-worker:latest
```

This image, unlike the stock `celers` one, actually executes `add` tasks enqueued against the `celery` queue.
Every `docker run`/Compose/Kubernetes/ECS/Cloud Run example later in this guide that shows a worker doing real
work assumes an image built this way, not the stock `celers:latest`.

If you also want this worker reachable from `celers inspect`/`celers control`, join the same remote-control
channel the stock CLI uses -- see `RedisControlTransport` in `celers-broker-redis` and
`crates/celers-cli/src/commands/worker.rs` for the reference wiring. If you want it to expose Prometheus metrics
for the dashboard in [Monitoring & Observability](#monitoring--observability), see
`crates/celers-examples/examples/prometheus_metrics.rs` for a minimal HTTP exporter built on `celers-metrics`.

## Docker Deployment

**No official CeleRS image is published to a public registry as of 0.3.1.** The only workflow that ever built and
pushed one lives disabled at `.github/workflows.disabled/release.yml` (this project's release automation is
manual; see the repository's contribution policy). Build locally instead:

```bash
# From the repository root: builds the operational `celers` CLI image
# (status/inspect/control/queue/dlq/backup/doctor -- NOT a task executor;
# see "Building a Worker Image" above for that)
docker build -t celers:0.3.1 .

# Run it
docker run --rm -e RUST_LOG=info celers:0.3.1 status --broker redis://redis:6379
```

If you publish your own image under your own registry namespace, replace every `celers:0.3.1` below with that
tag, and replace `worker` invocations with your own worker image where the example is meant to execute tasks.

### Single Worker Instance (smoke test)

```bash
docker build -t celers:0.3.1 .

# --broker must resolve from inside the container -- e.g. a Redis reachable
# at that hostname on a shared Docker network (see the same --network note
# under "Building a Worker Image"), or a real hostname/IP if Redis is
# external. A bare `redis://redis:6379` only resolves if you also attach
# this container to the network a host named "redis" is actually on.
# --demo-tasks registers demo.echo/demo.sleep/demo.fail so this proves
# broker connectivity end-to-end; it does not run your own tasks.
docker run -d \
  --name celers-worker \
  --network celers_celers-network \
  -e RUST_LOG=info \
  celers:0.3.1 \
  worker --broker redis://redis:6379 --concurrency 8 --demo-tasks
```

### Multi-Worker with Docker Compose

Create `docker-compose.prod.yml` (swap `image: my-worker:latest` for your own worker image built per
[Building a Worker Image](#building-a-worker-image) to actually execute tasks; using `celers:0.3.1` here, as
below, reproduces the same demo-tasks-only smoke test as the root `docker-compose.yml`):

```yaml
services:
  redis:
    image: redis:7-alpine
    volumes:
      - redis-data:/data
    command: redis-server --appendonly yes
    deploy:
      resources:
        limits:
          memory: 512M

  worker-high-priority:
    image: my-worker:latest
    environment:
      RUST_LOG: info
      REDIS_URL: redis://redis:6379
    command: ["--concurrency", "16", "--queue", "high_priority"]
    deploy:
      replicas: 2
      resources:
        limits:
          memory: 2G
          cpus: '2'

  worker-normal:
    image: my-worker:latest
    environment:
      RUST_LOG: info
      REDIS_URL: redis://redis:6379
    command: ["--concurrency", "8", "--queue", "celery"]
    deploy:
      replicas: 4
      resources:
        limits:
          memory: 1G
          cpus: '1'

volumes:
  redis-data:
```

Deploy:

```bash
docker-compose -f docker-compose.prod.yml up -d
```

## Kubernetes Deployment

### Redis StatefulSet

Create `k8s/redis-statefulset.yaml`:

```yaml
apiVersion: v1
kind: Service
metadata:
  name: redis
spec:
  ports:
    - port: 6379
      targetPort: 6379
  clusterIP: None
  selector:
    app: redis
---
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: redis
spec:
  serviceName: redis
  replicas: 1
  selector:
    matchLabels:
      app: redis
  template:
    metadata:
      labels:
        app: redis
    spec:
      containers:
        - name: redis
          image: redis:7-alpine
          ports:
            - containerPort: 6379
          volumeMounts:
            - name: redis-data
              mountPath: /data
          resources:
            requests:
              memory: "256Mi"
              cpu: "250m"
            limits:
              memory: "512Mi"
              cpu: "500m"
  volumeClaimTemplates:
    - metadata:
        name: redis-data
      spec:
        accessModes: [ "ReadWriteOnce" ]
        resources:
          requests:
            storage: 10Gi
```

### Worker Deployment

Create `k8s/worker-deployment.yaml`. `image` must be your own worker image (see
[Building a Worker Image](#building-a-worker-image)) -- the stock `celers` image will start and pass its
liveness/readiness probes (nothing here is broker-specific) but will never process a task. The probes below also
assume *your* binary starts an HTTP server on 9090 exposing `/health` and `/ready`; `celers-worker::health`
gives you `HealthChecker` to back those handlers, but your binary has to wire the listener itself -- `celers
worker` does not start one:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: celers-worker
  labels:
    app: celers-worker
spec:
  replicas: 4
  selector:
    matchLabels:
      app: celers-worker
  template:
    metadata:
      labels:
        app: celers-worker
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
        prometheus.io/path: "/metrics"
    spec:
      containers:
        - name: worker
          image: my-worker:latest
          env:
            - name: RUST_LOG
              value: "info"
            - name: REDIS_URL
              valueFrom:
                secretKeyRef:
                  name: celers-secrets
                  key: redis-url
          resources:
            requests:
              memory: "1Gi"
              cpu: "1000m"
            limits:
              memory: "2Gi"
              cpu: "2000m"
          livenessProbe:
            httpGet:
              path: /health
              port: 9090
            initialDelaySeconds: 30
            periodSeconds: 10
          readinessProbe:
            httpGet:
              path: /ready
              port: 9090
            initialDelaySeconds: 10
            periodSeconds: 5
---
apiVersion: v1
kind: Secret
metadata:
  name: celers-secrets
type: Opaque
stringData:
  redis-url: "redis://redis:6379"
```

### Horizontal Pod Autoscaler

Create `k8s/hpa.yaml`:

```yaml
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: celers-worker-hpa
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: Deployment
    name: celers-worker
  minReplicas: 2
  maxReplicas: 20
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: 70
    - type: Resource
      resource:
        name: memory
        target:
          type: Utilization
          averageUtilization: 80
    - type: Pods
      pods:
        metric:
          name: queue_size
        target:
          type: AverageValue
          averageValue: "100"
```

The `queue_size` custom metric requires a metrics adapter (e.g. `prometheus-adapter`) wired to your Prometheus
instance -- this file only declares the HPA's intent, not that pipeline.

Deploy to Kubernetes:

```bash
# Create namespace
kubectl create namespace celers

# Apply configurations
kubectl apply -f k8s/ -n celers

# Verify deployment
kubectl get pods -n celers
kubectl get svc -n celers

# Scale workers
kubectl scale deployment celers-worker --replicas=10 -n celers
```

## Cloud Platforms

The examples below all assume `image` points at your own worker image (see
[Building a Worker Image](#building-a-worker-image)), pushed to whatever registry your platform reads from --
substitute your actual registry/tag.

### AWS ECS

Use AWS ECS with Fargate for serverless deployment:

```json
{
  "family": "celers-worker",
  "networkMode": "awsvpc",
  "requiresCompatibilities": ["FARGATE"],
  "cpu": "2048",
  "memory": "4096",
  "containerDefinitions": [
    {
      "name": "worker",
      "image": "<your-account>.dkr.ecr.<region>.amazonaws.com/my-worker:latest",
      "environment": [
        {"name": "RUST_LOG", "value": "info"},
        {"name": "REDIS_URL", "value": "redis://cache.xxxxx.0001.use1.cache.amazonaws.com:6379"}
      ],
      "logConfiguration": {
        "logDriver": "awslogs",
        "options": {
          "awslogs-group": "/ecs/celers-worker",
          "awslogs-region": "us-east-1",
          "awslogs-stream-prefix": "worker"
        }
      }
    }
  ]
}
```

### Google Cloud Run

Deploy to Cloud Run (note: requires HTTP endpoint for health checks -- see the Kubernetes section's note about
your binary needing to start that server itself):

```bash
gcloud run deploy celers-worker \
  --image gcr.io/PROJECT_ID/my-worker:latest \
  --platform managed \
  --region us-central1 \
  --memory 2Gi \
  --cpu 2 \
  --min-instances 2 \
  --max-instances 10 \
  --set-env-vars REDIS_URL=redis://REDIS_IP:6379
```

### Azure Container Instances

```bash
az container create \
  --resource-group celers-rg \
  --name celers-worker \
  --image myregistry.azurecr.io/my-worker:latest \
  --cpu 2 \
  --memory 4 \
  --environment-variables \
    RUST_LOG=info \
    REDIS_URL=redis://cache.redis.cache.windows.net:6380 \
  --restart-policy Always
```

## Monitoring & Observability

### Prometheus Integration

The root `docker-compose.yml` already wires a real, minimal `docs/prometheus.yml` into the `prometheus` service.
It scrapes Prometheus itself out of the box (always up, proves the stack came up); the commented-out job for a
`celers-worker` target is there to uncomment once you build a worker binary that exports metrics -- see
[Building a Worker Image](#building-a-worker-image) and `crates/celers-examples/examples/prometheus_metrics.rs`.
For your own deployment, add a `scrape_configs` job pointing at wherever that binary's exporter listens, e.g.:

```yaml
scrape_configs:
  - job_name: 'celers-workers'
    static_configs:
      - targets: ['worker:9090']
    metrics_path: '/metrics'
    scrape_interval: 15s
```

### Grafana Dashboards

`docker-compose.yml`'s `grafana` service provisions from `docs/grafana/`: a Prometheus datasource
(`docs/grafana/datasources/datasource.yml`) and one real dashboard, **CeleRS Overview**
(`docs/grafana/dashboards/json/celers-overview.json`), built from the metric names `celers-metrics` actually
registers -- task throughput, queue depth, active workers, and p50/p95/p99 execution latency. It stays empty
until something is being scraped; see the Prometheus note above.

### Logging

Configure structured logging via the CLI's own flags (there is no `RUST_LOG_FORMAT` environment variable --
`--log-format`/`--log-sink` are CLI flags, and take precedence over `RUST_LOG` for level only):

```bash
# JSON logging for production
RUST_LOG=info,celers=debug celers worker --broker redis://localhost:6379 --log-format json

# Send logs to a file or a TCP collector instead of stdout
celers worker --broker redis://localhost:6379 --log-sink file:/var/log/celers/worker.log
celers worker --broker redis://localhost:6379 --log-sink tcp:collector.internal:5170
```

Your own worker binary (built per [Building a Worker Image](#building-a-worker-image)) is regular Rust code --
wire `tracing-subscriber` however your logging pipeline expects; it is not tied to the CLI's `--log-*` flags.

## Performance Tuning

### Worker Configuration

`celers worker --help` is the source of truth for what the stock CLI accepts -- it changes between releases, so
treat any flag list here (including this one) as a snapshot, not a promise. As of 0.3.1 it includes `--broker`,
`--queue`, `--mode`, `--concurrency`, `--max-retries`, `--timeout`, `--shutdown-timeout`, `--no-connect-check`,
`--broker-connect-timeout`, `--demo-tasks` (see [Building a Worker Image](#building-a-worker-image)), and
`--config`. There is no `--enable-batch-dequeue`, `--batch-size`, `--poll-interval`, `--enable-circuit-breaker`,
or `--max-result-size` flag on this command.

```bash
# Higher concurrency, longer per-task budget
celers worker --broker redis://localhost:6379 --concurrency 16 --timeout 60

# Tighter retry/timeout budget for latency-sensitive queues
celers worker --broker redis://localhost:6379 --concurrency 8 --max-retries 5 --timeout 10
```

`--config celers.toml` (see `celers init`) exposes one knob the CLI flags don't: `[worker] poll_interval_ms`, the
delay between dequeue attempts when a queue is empty (default 1000ms). Lower it for latency, raise it to reduce
idle broker load:

```toml
[worker]
concurrency = 8
poll_interval_ms = 100
max_retries = 3
default_timeout_secs = 60
```

If you're building your own worker binary, `celers_worker::WorkerConfig` has more fields than either the CLI or
the config file exposes today (retry backoff, defer-delay bounds, graceful-shutdown timing) -- see
`crates/celers-worker/src/types.rs` and set them directly.

### Redis Tuning

Configure Redis for high throughput:

```bash
# redis.conf
maxmemory 4gb
maxmemory-policy allkeys-lru
save 900 1
save 300 10
save 60 10000
appendonly yes
appendfsync everysec
```

### PostgreSQL Tuning

Optimize PostgreSQL for queue operations:

```sql
-- Increase connection pool
ALTER SYSTEM SET max_connections = 200;

-- Tune for writes
ALTER SYSTEM SET shared_buffers = '2GB';
ALTER SYSTEM SET effective_cache_size = '6GB';
ALTER SYSTEM SET maintenance_work_mem = '512MB';
ALTER SYSTEM SET checkpoint_completion_target = 0.9;
```

## Running Beat (the periodic scheduler)

`celers-beat` is a library you embed the same way you embed the worker — build a binary, construct a
`BeatScheduler`, register your `ScheduledTask`s, and run it beside (not inside) your workers.

Two things decide whether it is safe to run more than one replica:

* **Where the schedule catalog lives.** `BeatScheduler` persists through the `ScheduleStore` seam.
  `FileScheduleStore` (the default) is a local file, which does not survive an ephemeral container
  filesystem and is not shared between replicas. `RedisScheduleStore` (feature `redis-store`) gives
  every replica the same durable catalog. Note that `save` is **last-write-wins** — no
  compare-and-swap — so two replicas both writing means the last to finish a tick decides what the
  next restart sees.
* **Which replica may dispatch.** That is a *different* problem, solved by `dispatch_lock`: a
  short-lived distributed lock scoped to one `(entry, fire instant)` pair, taken before dispatching,
  so two instances racing on the same due entry do not both fire it. Wire it up before running more
  than one beat instance against a shared store.

The safe default is a single active beat replica (a `Deployment` with `replicas: 1` and
`strategy.type: Recreate`, or a `StatefulSet` with one pod), with `dispatch_lock` configured anyway so
a rolling restart's overlap window cannot double-fire.

**Do not use solar schedules in 0.3.1** — `Schedule::Solar::next_run` errors for every input; see
[TODO.md](../TODO.md#known-gaps--the-roadmap-after-031).

## Security Best Practices

### Network Security

1. **Use VPCs**: Isolate workers and brokers in private subnets
2. **Enable TLS**: Use `rediss://` and SSL connections
3. **Firewall rules**: Restrict access to broker ports

### Authentication

Configure Redis authentication:

```bash
# redis.conf
requirepass your_strong_password

# Worker configuration
REDIS_URL=redis://:your_strong_password@redis:6379
celers worker --broker "$REDIS_URL"
```

### Message authentication (task signatures)

Redis `requirepass` keeps strangers off the broker; it does nothing about a compromised *producer*
inside your network. `celers-worker` can require every dequeued message to carry a valid HMAC-SHA256
signature, checked **before dispatch** — before the revocation registry, the poison-pill strike table
or routing, so an unauthenticated message never seeds worker-local state keyed on its own id or name.
It is **off by default**.

```rust
use celers_core::TaskSigner;
use celers_worker::{SignatureVerification, WorkerConfig};

let key = std::env::var("CELERS_TASK_SIGNING_KEY")?;   // from your secret manager
let signer = TaskSigner::new(key.as_bytes());

// Require a valid signature on every message. During rollout, append
// `.allow_unsigned()` to verify signed messages while still admitting unsigned ones.
let config = WorkerConfig {
    signature_verification: Some(SignatureVerification::new(signer)),
    ..Default::default()
};
```

Operational notes:

* **Every producer must hold the key.** A message that fails verification is not executed and not
  requeued: with a DLQ configured it is recorded there with
  `failure_type = "signature_verification"`, otherwise it is dropped. Either way a `task-rejected`
  event fires and `WorkerStats::signature_rejected` counts it — alert on that counter.
* Roll it out with `.allow_unsigned()` first (verifies signed messages, still admits unsigned ones),
  then drop that call once every producer is signing. While it is on, an attacker only has to omit
  the signature — it is a migration step, not a posture.
* The worker re-signs the messages it produces itself — retry attempts and workflow continuations —
  so chains and chords keep working under a verifying fleet.
* `celers-cli loadtest` takes `--signing-key`, falling back to `CELERS_TASK_SIGNING_KEY`. Without one
  a verifying fleet dead-letters every synthetic task.
* Freshness windows and `ReplayGuard` are at odds with at-least-once redelivery — the same signed
  bytes legitimately arrive twice — and `ReplayGuard` is per-process. Read its module docs before
  enabling either in production.

`crates/celers/examples/security_wiring.rs` is a runnable end-to-end walkthrough.

### Secrets Management

Use Kubernetes secrets or cloud secret managers:

```bash
# AWS Secrets Manager
aws secretsmanager create-secret \
  --name celers/redis-url \
  --secret-string "redis://password@redis:6379"

# Reference in deployment
kubectl create secret generic celers-secrets \
  --from-literal=redis-url="$(aws secretsmanager get-secret-value --secret-id celers/redis-url --query SecretString --output text)"
```

### Resource Limits

Set appropriate limits to prevent resource exhaustion:

```yaml
resources:
  requests:
    memory: "1Gi"
    cpu: "1000m"
  limits:
    memory: "2Gi"
    cpu: "2000m"
```

## Troubleshooting

### Common Issues

**Workers not processing tasks:**
- Confirm you're running a worker binary that actually registers tasks -- the stock `celers` image never does; see
  [Building a Worker Image](#building-a-worker-image)
- Check Redis connectivity: `redis-cli ping`
- Verify the queue has tasks: `celers status --broker redis://localhost:6379` or
  `celers queue stats --broker redis://localhost:6379`
- Check worker logs: `docker logs celers-worker`

**High memory usage:**
- Reduce concurrency: `--concurrency 4`
- Check for large task payloads/results piling up in the broker/backend (`celers queue stats`,
  `celers db` for a SQL backend)

**Slow task processing:**
- Increase concurrency: `--concurrency 16`
- Lower `[worker] poll_interval_ms` in `celers.toml` if workers are idling between polls
- Check broker latency: `celers status` and the Prometheus histogram in
  [Monitoring & Observability](#monitoring--observability) once your worker exports it

For more help, [open an issue](https://github.com/cool-japan/celers/issues).
