# CeleRS - Enterprise Distributed Task Queue for Rust

**CeleRS** (Celery + Rust) is a Celery-protocol-compatible distributed task queue library for Rust: a
type-safe, Pure-Rust task runtime that speaks Python Celery's message format.

**Status (v0.3.1)**: ✅ 0 errors | ✅ 0 warnings | ✅ `cargo deny check` clean on all four checks (advisories/bans/licenses/sources), `[graph] exclude` empty
| ✅ **7,791 tests + 1,178 doctests passing** (`--all-features`, verified 2026-08-26 — see
[Crate Status](#crate-status-v031))

### What is and is not verified

CeleRS' **protocol layer is interop-verified against a real Python Celery 5.6.3**, both directions, by
[`tests/python-compat/`](tests/python-compat/) — a suite that runs a real Celery client and a real
`celery -A tasks worker` against a real Redis, with no mock anywhere in the loop — and by verbatim Celery
wire captures in [`crates/celers-protocol/tests/fixtures/`](crates/celers-protocol/tests/fixtures/).

What is **not** interoperable: the **broker and the result backend**. `celers-broker-redis` puts its own
`SerializedTask` JSON on the queue and both Redis result backends store a CeleRS-shaped record, so **a
Python Celery worker and a CeleRS worker cannot share a queue today.** Read
[docs/CELERY_COMPATIBILITY.md](docs/CELERY_COMPATIBILITY.md) before planning a migration: every row there
carries the test that proves it, or an honest statement that nothing does.

## 🎯 Vision

CeleRS aims to be the definitive task queue solution for Rust, offering:

- **🔄 Celery protocol compatibility**: exchange tasks and results with Python Celery at the wire level
- **⚡ Performance**: a compiled, `tokio`-based runtime with no GIL and no per-task process fork
- **🔒 Type Safety**: Compile-time guarantees for task signatures
- **🦀 Pure Rust**: no C, C++, Fortran or vendored assembly anywhere in the tree, enforced by `cargo deny`
- **🏢 Production concerns first class**: remote control, durable revocation, message authentication,
  time limits, DLQ, circuit breakers

## ✨ Features

### Core Capabilities
- ✅ **Type-Safe Task Definitions**: Compile-time verified task signatures
- ✅ **Priority Queues**: Multi-level task prioritization
- ✅ **Dead Letter Queue**: Automatic handling of permanently failed tasks
- ✅ **Task Cancellation**: In-flight task cancellation via Pub/Sub
- ✅ **Retry Logic**: Exponential backoff with configurable max retries
- ✅ **Timeout Enforcement**: Task-level and worker-level timeout controls
- ✅ **Graceful Shutdown**: Clean worker termination with in-flight task completion
- ✅ **Soft & Hard Time Limits**: cooperative soft deadline (`execution_context::check_soft_time_limit()`,
  emits `task-soft-time-limit-exceeded`) plus a hard kill, both settable at runtime over the control channel
- ✅ **Local Development Mode**: In-memory broker/backend (`InMemoryBroker`, `InMemoryResultBackend`) — no external services required

### Remote Control & Revocation
- ✅ **Remote control protocol**: `celers inspect` (read-only) and `celers control` (mutating) reach every
  running worker over a Redis pub/sub channel workers join automatically — no daemon to run. Enable on a
  worker with `Worker::with_control_transport`. **CeleRS-native: it does not interoperate with
  `celery -A app inspect`** (no kombu pidbox codec yet)
- ✅ **Broker-fed revocation**: `Broker::revoke` / `is_revoked` / `subscribe_revocations` record a
  revocation durably *and* publish it live, so a task revoked while queued is refused at dequeue even by a
  worker that was offline at the time. Implemented by the Redis, PostgreSQL, MySQL and in-memory brokers;
  opt a worker in with `Worker::with_broker_revocation` (`celers worker` does)
- ✅ **Message authentication**: opt-in HMAC-SHA256 signature verification **before dispatch**, plus
  argument sanitization and PII detection/masking. A failing message is dead-lettered with
  `failure_type = "signature_verification"`, never executed and never requeued. Off by default

### Brokers

A `celers_worker::Worker` consumes from a `celers_core::Broker`. These implement it:

- ✅ **Redis**: High-throughput with Lua scripts and pipelining; `rediss://` TLS
- ✅ **PostgreSQL**: ACID guarantees with `FOR UPDATE SKIP LOCKED`
- ✅ **MySQL**: Full SQL support with batch operations
- ✅ **In-memory**: `InMemoryBroker`, for local development and tests
- ✅ **RabbitMQ (AMQP)**: exchanges, topic routing, publisher confirms, management API — plus, new in
  0.3.1, `celers_core::Broker` via `AmqpBroker::into_core_broker(queue)`
- ✅ **AWS SQS**: long polling, FIFO queues, batch operations, visibility extension — plus, new in
  0.3.1, `celers_core::Broker` via `SqsBroker::into_core_broker(queue)`

RabbitMQ and SQS reach `celers_core::Broker` through `celers_kombu::core_adapter::KombuBrokerAdapter`,
which wraps either transport's existing `publish`/`consume`/`purge` traits (over
`celers_protocol::Message`) rather than replacing them. Both crates enable it **by default** — the
`core-broker` feature, pulling in `celers-kombu/core-adapter` — so no extra Cargo flag is needed:

```rust,ignore
let broker = AmqpBroker::new("amqp://localhost:5672", "celery")
    .await?
    .into_core_broker("celery");   // now a celers_core::Broker; hand it to Worker::new
```

Two things worth knowing before you point a `Worker` at either one:

- **One transport, one lock.** The adapter serializes every operation through a single
  `tokio::sync::Mutex` around the transport. The worker's `dequeue` parks inside it for up to the
  adapter's poll timeout (1s by default) while it waits for a message, during which an `ack` from an
  already-dispatched task briefly waits too. Give each worker its own transport rather than sharing
  one — a shared adapter serializes those workers against each other, not just against themselves.
  SQS's native `dequeue_batch`/`enqueue_batch`/`ack_batch`/`defer` map onto real batch calls
  (`ReceiveMessage`, `SendMessageBatch`, `DeleteMessageBatch`, `ChangeMessageVisibility`) rather than
  one request per message.
- **Still not Celery-wire-compatible.** Both transports carry a JSON-serialized
  `celers_protocol::Message` as the body, not kombu's own AMQP/SQS wire framing, so a real Celery
  worker cannot consume from either — see
  [docs/CELERY_COMPATIBILITY.md](docs/CELERY_COMPATIBILITY.md#brokers). Revocation is also not
  wired here yet: `cancel()`/`revoke()` answer `false`/no-op rather than reaching a queued message,
  because neither transport can address one by id before it is delivered — tracked in
  [TODO.md](TODO.md#known-gaps--the-roadmap-after-031).

### Result Backends (3 Types)
- ✅ **Redis Backend**: Fast in-memory storage with automatic TTL
- ✅ **Database Backend**: PostgreSQL/MySQL with SQL analytics and durability
- ✅ **gRPC Backend**: Microservices-ready RPC result storage
- ✅ All three carry **chord barrier synchronization** through the `ResultStore` adapters

### Workflow Primitives (Canvas)

Every primitive below is executed end to end by the worker, covered by
`crates/celers-worker/tests/workflow_semantics.rs` and `celers_worker::workflows::patterns_e2e`. From the
`celers` facade they need the `workflows` feature (which pulls `canvas` and `backend-redis`) — without it a
chord's callback is compiled out of the worker you build.

- ✅ **Chain**: Sequential task execution with result passing
- ✅ **Group**: Parallel task execution
- ✅ **Chord**: Map-reduce with a distributed barrier callback that receives the ordered header results
- ✅ **Map/Starmap/Chunks**: Distributed mapping operations
- ✅ **Signature**: Task signatures for workflow composition
- ✅ **Saga**: forward steps with compensating actions, rolled back newest-first on failure
- ✅ **Branch/Switch**: conditional routing on a task's outcome
- ✅ **Pipeline / FanIn / FanOut / ScatterGather**: higher-level patterns that lower onto the above

### Observability
- ✅ **Prometheus Metrics**: Task throughput, latency, queue depth (native histograms + P² streaming quantile summaries)
- ✅ **StatsD Backend**: Pure-`std::net::UdpSocket` exporter (counters, gauges, timers, DogStatsD tags) alongside Prometheus
- ✅ **SLA/SLO Tracking & Anomaly Detection**: Error-budget burn alerts plus EWMA-based statistical anomaly detection
- ✅ **Audit Log**: Task lifecycle audit trail (ring-buffer + JSONL file sinks, queryable)
- ✅ **Health Checks**: Kubernetes-compatible liveness/readiness probes
- ✅ **OpenTelemetry**: Distributed tracing integration (facade feature `tracing`)
- ✅ **Celery-compatible event stream**: `celeryev` pub/sub carrying the shape `celery events` and Flower
  parse, with Celery's Lamport clock. **Breaking change in 0.3.1** for anyone who consumed the previous
  CeleRS-shaped events — see [CHANGELOG.md](CHANGELOG.md)
- ✅ **Grafana + Prometheus config**: a scrape config (`docs/prometheus.yml`) and an overview dashboard
  (`docs/grafana/`), both mounted by the `docker-compose.yml` monitoring stack

### Developer Experience
- ✅ **Procedural Macros**: `#[celers::task]` for automatic task registration
- ✅ **CLI Tooling**: Worker management, queue inspection, DLQ operations, dependency-graph visualization (`deps`)
- ✅ **CLI Connection Pooling & Caching**: `ClientPool`/`TtlCache` front queue/worker reads with concurrent lookups; `cache-stats` command and REPL `stats` reporting live hit/reuse ratios
- ✅ **Structured CLI Errors**: Classified `CliError`s with decorated `error[E_CODE]:` output and actionable `suggestion:` lines; `error-codes` reference command
- ✅ **Structured Logging**: `--log-format text|json` plus `--log-sink stdout|file:<path>|tcp:<host:port>` for CLI log output
- ✅ **Smart Defaults**: Broker URL auto-detection (`CELERY_BROKER_URL`/`CELERS_BROKER_URL`/`REDIS_URL`/`AMQP_URL`) and Levenshtein-based "did you mean" suggestions in the `interactive` REPL
- ✅ **User-Defined Aliases**: `alias add/remove/list` for custom command shortcuts, expanded before argument parsing
- ✅ **Incremental Backup/Restore**: `backup --previous <archive>`/`--since <timestamp>` plus `restore --conflict-policy skip|overwrite|merge`
- ✅ **Setup Wizard**: `init --wizard` interactive broker/queue/worker/alerting configuration with live connection testing
- ✅ **Reporting & Profiling**: `report daily/weekly/history/queues/workers` and `analyze profile task/resources/worker`, with `table`/`csv`/`html` output (HTML includes an inline SVG chart)
- ✅ **Configuration Management**: TOML/YAML files + environment variables + runtime `config reload`
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

### Workspace Crates (18 published)

The workspace has two further members that are never published
(`publish = false`): `celers-examples` (the runnable examples and benchmarks)
and `celers-facade-test` (a two-dependency crate that pins the minimum
dependency set `#[celers::task]` needs downstream). Both are in `members` so
`cargo build/clippy/fmt --workspace` covers them.

#### Core & Protocol Layer
- **celers**: Facade crate with unified API
- **celers-core**: Core traits (`Task`, `Broker`, `ResultBackend`, `TaskExecutor`)
- **celers-protocol**: Celery protocol v2 message format (plus a CeleRS-internal "v5" version label
  that nothing in the interop suite exercises — the suite and every capture pin `task_protocol = 2`)
- **celers-kombu**: Kombu-style messaging abstraction (`Producer`/`Consumer`/`Transport`)

#### Broker Layer (5 task-queue brokers)
- **celers-broker-redis**: Redis with Lua scripts and pipelining — implements `celers_core::Broker`
- **celers-broker-postgres**: PostgreSQL with `FOR UPDATE SKIP LOCKED` — implements `celers_core::Broker`
- **celers-broker-sql**: MySQL with batch operations — implements `celers_core::Broker`
- **celers-broker-amqp**: RabbitMQ/AMQP with exchanges and routing — `celers-kombu` transport, plus
  `celers_core::Broker` via `into_core_broker()` (default-on `core-broker` feature); still not
  Celery-wire-compatible (JSON body, not kombu's AMQP framing)
- **celers-broker-sqs**: AWS SQS with long polling — `celers-kombu` transport, plus
  `celers_core::Broker` via `into_core_broker()` (default-on `core-broker` feature); same caveat

#### Result Backend Layer (3 Implementations)
- **celers-backend-redis**: Redis with TTL and chord synchronization
- **celers-backend-db**: PostgreSQL/MySQL with SQL analytics
- **celers-backend-rpc**: gRPC for microservices architectures

#### Runtime & Workflow Layer
- **celers-worker**: Task execution runtime with concurrency control
- **celers-canvas**: Workflow primitives (Chain, Chord, Group, Map, Saga, Branch/Switch)
- **celers-beat**: Periodic task scheduler (Cron, Interval, one-time; the `solar` feature is currently
  **broken** — see [TODO.md](TODO.md#known-gaps--the-roadmap-after-031))

#### Developer Tools
- **celers-macros**: Procedural macros (`#[task]`, `#[derive(Task)]`)
- **celers-cli**: Command-line worker and queue management
- **celers-metrics**: Prometheus metrics and observability

### Pure Rust: no exceptions

Every crate above builds C/C++/Fortran-free — with default features, with
`--all-features`, and with the facade's `full` feature. The banned-crate policy
is enforced mechanically by `cargo deny check bans` against the workspace's
`deny.toml`, whose `[graph] exclude` list is **empty**: no crate is hidden from
the check.

Verify it yourself:

```bash
cargo tree -e features -i aws-lc-sys --all-features   # "did not match any packages"
cargo deny check bans
```

The three transports that need TLS each get there differently, and all three are
Pure Rust as of 0.3.1:

| Feature | Crate | How TLS works |
|---|---|---|
| `sqs` | `celers-broker-sqs` | The AWS SDK's `default-https-client` is **not** enabled: it hard-selects `aws-smithy-http-client/rustls-aws-lc` → `aws-lc-rs` → `aws-lc-sys` (vendored C/C++/assembly built with `cmake` + `cc`), and the SDK's only alternatives (`rustls-ring`, `rustls-aws-lc-fips`, `s2n-tls`) are also C/asm. Instead, `celers_broker_sqs::pure_http` implements the SDK's `HttpClient`/`HttpConnector` over `oxihttp-client` (hyper 1.x + `tokio-rustls` + OxiTLS' `rustls-rustcrypto` provider, webpki roots) and installs it at every `aws_config::defaults(..)`. On by default via the crate's `pure-http` feature. |
| `amqp` | `celers-broker-amqp` | `lapin` is pinned to `default-features = false` + `rustls-webpki-roots-certs`, and `celers_broker_amqp::install_pure_tls_provider()` installs the `rustls-rustcrypto` provider. Deployments behind a private CA should enable `celers-broker-amqp/tls-native-certs` (also Pure Rust) to add the OS trust store. |
| `rediss://` | `celers-broker-redis`, `celers-backend-redis` | The `redis` crate is built with `tokio-rustls-comp` + `tls-rustls-webpki-roots`, neither of which selects a crypto provider, and each crate's `install_pure_tls_provider()` supplies OxiTLS'. Custom CA and client certificates go through `RedisConfig::tls(TlsConfig::new().ca_cert(..).client_cert(.., ..))`, which routes to `redis::Client::build_with_tls`. |

Because no rustls provider *feature* is enabled anywhere in the workspace, a
bare `rustls::ClientConfig::builder()` has no provider to auto-select and would
panic. Every client constructor in these crates therefore calls its
`install_pure_tls_provider()` first. An application that builds rustls clients
through *other* libraries before constructing a CeleRS broker should call one of
them itself, first thing in `main`; whoever installs a default provider first
wins.

The gRPC result backend (`celers-backend-rpc`) ships without built-in TLS for
the same reason: `tonic`'s `tls-ring` / `tls-aws-lc` features pull banned
crypto. Bring your own TLS-enabled `Channel` via
`GrpcResultBackend::from_channel_with_config`.

### Crate Status (v0.3.1)

**Verified 2026-08-26 with `cargo nextest run --workspace --all-features`: 7,791 tests run, 7,791 passed, 0
failed, 104 skipped** (default features: 7,509 run, 7,509 passed, 0 failed, 97 skipped), plus **1,178 passing
doctests** (`cargo test --doc --workspace --all-features`; 138 more are ```` ```ignore ```` and never
compile). A per-crate breakdown is intentionally not reproduced here: with 18 published crates under
active, parallel development, a static table drifts out of date between releases faster than it gets
corrected -- regenerate one locally with `cargo nextest list --workspace --all-features` if you want a
current snapshot, or watch a single crate's count with `cargo nextest list -p <crate> --all-features`.

That 7,791 total is not the whole story on what it verifies. Three categories of test coexist inside it, and
only the first two ran a real assertion:

1. **Ordinary tests** -- ran, asserted, passed.
2. **Env-gated live-service tests** -- ran and asserted for real *only if* the matching `CELERS_TEST_*`
   (or, for `celers-backend-db` and the facade tests that drive it, the differently-named `DATABASE_URL` /
   `MYSQL_URL`) variable was set to a reachable server; otherwise they print a `skipping` line and return,
   **still counted as passing**. The size of this category is measured rather than estimated: `cargo nextest
   run --workspace --all-features --no-capture` with none of those variables exported emits **147 skip
   lines** (re-measured 2026-08-26; it was 109 before this release added the PostgreSQL result-store and
   advisory-lock suites, which account for 90 of the 147 on their own). That is an upper bound on the number
   of such tests -- a test that opens two connections prints two lines (see `celers-broker-sql`'s
   `queues_are_isolated_from_each_other`). See
   [tests/integration/README.md](tests/integration/README.md) for the full variable-to-service table and how
   to tell which happened.
3. **`#[ignore]`d tests** (104, the "skipped" figure above) -- not attempted at all unless the run adds
   `--run-ignored all`.

Category 2 means a green `--all-features` run with no service URLs exported -- the common case on a laptop --
has not exercised live Redis/PostgreSQL/MySQL/RabbitMQ/SQS behavior in that subset, only the code paths that
don't need one. `docker-compose.yml` carries a service for every one of them (`--profile test` adds MySQL
and LocalStack, `--profile python-compat` adds Celery), and **[`scripts/test-integration.sh`](scripts/test-integration.sh)
runs the full matrix in one command**:

```bash
./scripts/test-integration.sh              # every service, every gated suite, PASS/FAIL/SKIP summary
./scripts/test-integration.sh --only mysql # or scope it to one service
```

It brings the services up, waits for a real protocol handshake through the host-forwarded port (restarting a
container that accepts TCP but never answers -- four of five services needed exactly that on a fresh `up -d`
during this release's verification), exports all eight gate variables, and runs each suite with
`--run-ignored all`. What is still missing is *automation*, not tooling: `.github/` holds only
`dependabot.yml`, `FUNDING.yml` and a `workflows.disabled/` directory, so no workflow runs it on push -- it
has to be invoked by hand. Doing so is the only way to know all 7,791 assertions actually fired, and it is
what found (then fixed) real defects in `celers-broker-postgres`/`celers-broker-sql`/`celers-backend-db` that
every category-1/2 run above had been silently passing around -- including a PostgreSQL result store whose
table no migration created, and the MySQL table-name collision between the broker and the result backend that
was tracked as Known gaps #16 and is now closed. See
[CHANGELOG.md](CHANGELOG.md)'s 0.3.1 "Fixed" and "Breaking changes → Database schema" sections.

## 🚀 Quick Start

### Installation

Add CeleRS to your `Cargo.toml`:

```toml
[dependencies]
celers-core = "0.3"
celers-protocol = "0.3"
celers-broker-redis = "0.3"
celers-worker = "0.3"
celers-macros = "0.3"
tokio = { version = "1", features = ["full"] }

# Required by the code `#[task]` generates, not just by your own code:
# the macro expands to an `#[async_trait]` impl whose input struct derives
# `serde::Serialize`/`Deserialize`, and those paths resolve in *your* crate.
serde = { version = "1", features = ["derive"] }
async-trait = "0.1"
```

Or use the `celers` facade, which re-exports the whole API surface. Note that
`serde` is still needed as a direct dependency even then — a derive path is not
satisfied by the facade's re-export:

```toml
[dependencies]
celers = { version = "0.3", features = ["redis"] }
serde = { version = "1", features = ["derive"] }
tokio = { version = "1", features = ["full"] }
```

**If you use Canvas workflows through the facade, ask for them explicitly.** The primitives
themselves are always re-exported, but the half that *finishes* a workflow — the chord barrier and
the chain continuation — lives in the worker behind `celers-worker`'s own gates. Without the
features below you can build and apply a `Chord`, watch its header run, and never see the callback:

```toml
[dependencies]
# `workflows` implies `canvas` and `backend-redis` (the barrier is counted in a result backend).
celers = { version = "0.3", features = ["redis", "workflows"] }
```

`celers/full` includes both. Feature flags for the rest: `postgres`, `mysql`, `amqp`, `sqs`,
`backend-redis`, `backend-db`, `backend-rpc`, `beat` / `beat-cron` / `beat-solar`, `json`,
`msgpack`, `metrics`, `tracing`, `dev-utils`.

**MSRV**: 1.89 for everything except `sqs`, which needs 1.94.1 (its AWS SDK dependencies are not
optional) — and therefore so do `celers/full` and any `--all-features` build.

### Define a Task

```rust
use celers_macros::task;
use celers_core::Result;

#[task]
async fn add(x: i32, y: i32) -> Result<i32> {
    Ok(x + y)
}

// The macro generates `AddTask` (implements the `Task` trait) and an
// `AddTaskInput { x: i32, y: i32 }` struct from the function signature.
```

### Start a Worker

```rust
use celers_broker_redis::RedisBroker;
use celers_core::TaskRegistry;
use celers_worker::{Worker, WorkerConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Create broker
    let broker = RedisBroker::new("redis://localhost:6379", "celers")?;

    // Register tasks (registry is handed to the worker at construction time)
    let registry = TaskRegistry::new();
    registry.register(AddTask).await;

    // Configure worker
    let config = WorkerConfig {
        concurrency: 4,
        max_retries: 3,
        default_timeout_secs: 300,
        ..Default::default()
    };

    // Create and start the worker
    let worker = Worker::new(broker, registry, config);
    worker.run().await?;

    Ok(())
}
```

### Enqueue Tasks

```rust
use celers_core::{Broker, SerializedTask};
use celers_broker_redis::RedisBroker;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let broker = RedisBroker::new("redis://localhost:6379", "celers")?;

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

# Generate configuration file (or launch the interactive setup wizard)
celers init > celers.toml
celers init --wizard

# Visualize task dependencies from a queue export
celers deps --from queue-export.json --format dot

# Manage user-defined command aliases
celers alias add up "worker --broker redis://localhost:6379"

# Connection-pool / cache stats and structured error reference
celers cache-stats
celers error-codes

# Incremental backup, restore with conflict handling
celers backup --previous last-backup.json
celers restore --conflict-policy skip

# Reports and profiling (table/csv/html, HTML embeds an SVG chart)
celers report weekly --format html --output weekly.html
celers analyze profile task --queue default
```

### Remote control and inspection

`celers inspect` (read-only) and `celers control` (mutating) talk to running workers over a Redis pub/sub control
channel every worker joins automatically -- there is no separate daemon to start. Both broadcast a request and
print every reply that arrives before a timeout; "no worker answered" means nothing was listening, not that the
command failed. This is a CeleRS-native protocol -- it does not interoperate with `celery -A app inspect` against
a Python worker, or vice versa.

```bash
# Which workers are alive right now
$ celers inspect ping --broker redis://localhost:6379
✓ 1 worker(s) answered 'ping'
  kitasan pong (clock 1787655038.426)

# What task types a worker knows how to run
$ celers inspect registered --broker redis://localhost:6379
✓ 1 worker(s) answered 'inspect registered'
  kitasan
      demo.echo
      demo.fail
      demo.sleep

# Tasks a worker is executing right now
celers inspect active --broker redis://localhost:6379

# Revoke a task by id (recorded durably, so it also blocks the task if it's
# still queued or a worker for it hasn't started yet -- see --help)
celers control revoke <task-id> --broker redis://localhost:6379

# Ask every listening worker to drain and stop
celers control shutdown --broker redis://localhost:6379
```

`celers inspect --help` / `celers control --help` list every subcommand (11 and 10 respectively as of 0.3.1):
ping/active/scheduled/reserved/revoked/registered/stats/queues/report/conf/circuit-breakers on the read side,
and ping/shutdown/revoke/revoke-pattern/rate-limit/time-limit/add-consumer/cancel-consumer/queue-length/
reset-circuit-breaker on the mutating side.

Two answers are deliberately shaped and worth knowing: `inspect scheduled` and `inspect reserved` always come
back empty, and that is *accurate* rather than a stub — a CeleRS worker keeps no worker-local scheduled or
reserved set (ETA tasks wait in the broker's delayed queue, and a batch dequeue dispatches rather than parks).
`control queue purge/delete/bind/unbind/declare` answer with an error naming the reason: the `Broker` trait has
no such operations, so a worker cannot perform them on your behalf — use `celers queue purge` and friends.

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
use celers_worker::health::HealthChecker;

let checker = HealthChecker::new();

// Liveness probe: Is the worker healthy?
let healthy = checker.is_healthy();

// Readiness probe: Can the worker accept tasks?
let ready = checker.is_ready();

// Full health status
let info = checker.get_health();
```

## 🗺️ Roadmap

### Current Status (v0.3.1) — In Development (v0.3.0 released 2026-07-12)

- ✅ **Phase 1**: The Backbone (Core runtime)
- ✅ **Phase 2**: Advanced Features (Priorities, DLQ, Cancellation)
- ✅ **Phase 3**: Developer Experience (Macros, CLI, Metrics)
- ✅ **Phase 4**: Performance & Scalability
- ✅ **Phase 5**: Beat Scheduler -- Cron, Interval, one-time and solar schedules all work (solar covers
  sunrise/sunset, civil/nautical/astronomical twilight and golden hour, and handles polar day/night)
- ✅ **Phase 6**: Extended Brokers & Backends -- all three result backends are complete; AMQP and SQS are
  now also worker-usable `celers_core::Broker`s via `celers_kombu::core_adapter::KombuBrokerAdapter`
  (see [Brokers](#brokers) above), on by default
- 🚧 **Phase 7**: Full Celery Protocol Compatibility -- the **protocol layer is interop-verified** against a
  live Python Celery 5.6.3, both directions (`tests/python-compat/`). What remains is every broker and
  result backend still carrying CeleRS-shaped payloads instead of Celery's own wire framing -- true for
  Redis/PostgreSQL/MySQL, and, separately, for the now-worker-usable AMQP/SQS brokers too -- so no CeleRS
  broker can yet share a queue with a Python Celery worker. Row-by-row evidence in
  [docs/CELERY_COMPATIBILITY.md](docs/CELERY_COMPATIBILITY.md)
- ✅ **Phase 8**: v0.2.0 Enhancements (Compression, Distributed Locks, Events)
- ✅ **Phase 9**: v0.2.0 Production Features (Event Persistence, Chunking, Heartbeat)

### Upcoming Milestones

- **Next**: route every broker and result backend through `celers-protocol` so a Python Celery worker and a
  CeleRS worker can share a queue; a kombu pidbox codec so `celery -A app inspect` reaches a CeleRS worker.
  The full, evidence-backed list is
  [TODO.md → Known gaps](TODO.md#known-gaps--the-roadmap-after-031)
- **v1.0.0**: Stable API, Kafka/NATS brokers, web admin dashboard

## 📖 Documentation

- [docs/CELERY_COMPATIBILITY.md](docs/CELERY_COMPATIBILITY.md) - What interoperates with Python Celery, and
  which test proves each row
- [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) - Docker, Kubernetes, and building a worker image
- [tests/python-compat/README.md](tests/python-compat/README.md) - The live Celery interoperability suite
- [tests/integration/README.md](tests/integration/README.md) - Gate variable → service → invocation for every
  env-gated suite
- [Architecture Decision Records](docs/adr/) - Key design decisions
- [CHANGELOG.md](CHANGELOG.md) - Release notes, including 0.3.1's breaking wire-format changes
- [TODO.md](TODO.md) - Roadmap and the honest list of known gaps
- [CONTRIBUTING.md](CONTRIBUTING.md) - Dev setup, the gated live-service suites, and code standards

## 🔬 Examples

The repository includes 15 working examples (in `crates/celers-examples/examples/`):

- `phase1_complete` - Basic task execution
- `graceful_shutdown` - Clean worker termination
- `priority_queue` - Multi-priority task handling
- `dead_letter_queue` - DLQ management
- `task_cancellation` - In-flight cancellation
- `macro_tasks` - Procedural macro usage
- `prometheus_metrics` - Metrics HTTP server
- `health_checks` - Health check endpoints
- `async_result` - AsyncResult API usage
- `canvas_workflows` - Chain/Group/Chord workflow composition
- `facade_usage` - Using the `celers` facade crate
- `basic_processing` - Minimal end-to-end processing example
- `postgres_broker_example` - PostgreSQL broker walkthrough
- `web_scraper` - Real-world web scraping workload
- `image_processing` - Real-world image processing workload

Run examples with (the repository root is a virtual workspace with no `[package]` of its own, so `-p
celers-examples` is required -- a bare `cargo run --example <name>` fails to select a package):

```bash
cargo run -p celers-examples --example basic_processing

# prometheus_metrics additionally needs the `metrics` feature (it links
# celers-metrics through celers-worker/celers-broker-redis):
cargo run -p celers-examples --example prometheus_metrics --features metrics
```

## 🧪 Testing

Verified workspace-wide with `cargo nextest run --workspace --all-features`: **7,791 tests passing, 0
failed, 104 skipped**, plus **1,178 passing doctests**. See [Crate Status](#crate-status-v031) above for what
those figures do and do not establish about the env-gated live-service suites, and
[tests/integration/README.md](tests/integration/README.md) to run them against real services.

```bash
# Run all tests (default features)
cargo test

# Run the full suite, including every broker/backend
cargo nextest run --workspace --all-features

# Doctests (nextest does not run these)
cargo test --doc --workspace --all-features

# Bring up the services the gated suites need, then run them for real
docker-compose up -d
docker-compose --profile test up -d          # MySQL + LocalStack
CELERS_TEST_REDIS_URL=redis://127.0.0.1:6379 \
  cargo nextest run --workspace --all-features --run-ignored all

# Live Python Celery interoperability (creates its own venv under $TMPDIR)
CELERS_TEST_REDIS_URL=redis://127.0.0.1:6379 tests/python-compat/run.sh

# Run benchmarks -- see "Benchmarks available via" below; `-p` is required
cargo bench -p celers-examples --bench serialization

# Check for warnings
cargo clippy --workspace --all-features --all-targets -- -D warnings
```

## 🏆 Performance

CeleRS is designed for high throughput. The figures below are **design targets, not measurements** — this
repository publishes no benchmark results, and none of these numbers has been verified against a run here.
Measure your own workload with the criterion suites below before planning capacity around them.

- **Target throughput**: 10,000 tasks/sec per worker
- **Target latency**: P95 < 10 ms for enqueue/dequeue
- **Target memory**: < 50 MB baseline per worker
- **Delivery**: at-least-once, via visibility timeouts and an unacked set that a reaper redelivers from. Not
  exactly-once, and not a numeric guarantee — design your tasks to be idempotent

Benchmarks available via (note: `celers-cli` and `celers-examples` both declare a `serialization` bench, so a bare
`--bench serialization` with no `-p` runs it in *both* crates; add `-p` to run just one):

```bash
cargo bench -p celers-examples --bench serialization
cargo bench -p celers-examples --bench queue_operations
cargo bench -p celers-examples --bench batch_operations
cargo bench -p celers-cli --bench serialization
```

## 🤝 Contributing

We welcome contributions. See [CONTRIBUTING.md](CONTRIBUTING.md) for the full picture — dev setup, the
env-gated live-service suites and how to run each against a real service, and the mechanically-enforced
code standards. The short version is the same six gates CONTRIBUTING.md asks you to run before opening a
PR, since there is currently no CI that runs them for you (`.github/` holds only `dependabot.yml`,
`FUNDING.yml`, and an inactive `workflows.disabled/`):

```bash
cargo build --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo nextest run --workspace --all-features
cargo test --doc --workspace --all-features
cargo fmt --all --check
cargo deny check bans
```

### Development Setup

```bash
git clone https://github.com/cool-japan/celers.git
cd celers
cargo build --all-features
cargo nextest run --all-features
```

### Code Standards

- **No warnings policy**: `cargo clippy --workspace --all-targets --all-features -- -D warnings` must be clean
- **No `unwrap()`/`expect()`** outside test code — a typed `CelersError`, or a documented, genuinely
  unreachable fallback
- **Pure Rust by default**: no C/C++/Fortran/vendored assembly, enforced by `cargo deny check bans`
  against [`deny.toml`](deny.toml)'s `[graph] exclude`, which is empty and must stay that way
- **File size**: keep source files under 2000 lines; split into submodules before crossing it
- **Documentation**: Public APIs must have rustdoc comments
- **Formatting**: `cargo fmt --all --check` must pass

Full detail, including how to run the suites that need Redis/PostgreSQL/MySQL/RabbitMQ/SQS/Python-Celery,
is in [CONTRIBUTING.md](CONTRIBUTING.md).

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

**Status**: Active Development | **Version**: 0.3.1 | **MSRV**: 1.89, declared as `rust-version` in the
workspace manifest (`oxisql`/`oxitls` set that floor) -- except `celers-broker-sqs`, which declares 1.94.1
because its AWS SDK dependencies are not optional, and therefore so do `celers/sqs`, `celers/full` and any
`--all-features` build. Re-derive with `cargo metadata --format-version 1 --all-features` and take the highest
`rust_version` in the resolved graph.

Built with ❤️ for the Rust community
