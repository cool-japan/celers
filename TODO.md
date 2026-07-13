# CeleRS - Development Roadmap

> Distributed task queue system for Rust

## Project Status

- **Phase 1**: The Backbone ✅ **COMPLETE**
- **Phase 2**: Advanced Features ✅ **COMPLETE**
- **Phase 3**: Developer Experience ✅ **COMPLETE**
- **Phase 4**: Performance & Scalability ✅ **COMPLETE**
- **Phase 5**: Beat Scheduler ✅ **COMPLETE**
- **Phase 6**: Extended Brokers & Backends ✅ **COMPLETE**
- **Phase 8**: v0.2.0 Enhancements ✅ **COMPLETE**
- **Phase 9**: v0.2.0 Production Features ✅ **COMPLETE**

**🎉 100% PROJECT COMPLETION - ALL 18/18 CRATES IMPLEMENTED! 🎉**

### v0.3.0 Hardening (in progress — 2026-06-13)

Ongoing stub-elimination / correctness round (branch `0.3.0`). Latest sweep:

- **Canvas/Worker**: real nested chord execution (callback no longer dropped); chord callback now
  receives the aggregated, ordered list of header results.
- **Protocol**: message `created_at` timestamp + age tracking; real v2↔v5 migration (version
  stamping, priority/legacy field mirroring, feature-aware strict compatibility).
- **Kombu**: compression middleware now round-trips (header + decompress on consume — fixed silent
  data corruption); signing middleware now stores + verifies HMAC; health-check middleware real.
- **Beat**: real `reqwest` webhook alert delivery; **fixed** crontab `day_of_week` Unix→Quartz
  off-by-one (`"1-5"` now means Mon–Fri, `"0"` = Sunday accepted).
- **Redis broker**: real cron parsing for `CronScheduler` (arbitrary 5-field expressions);
  adaptive failure-classified DLQ replay; real DLQ error classification/signatures.
- **Feature expansion (round 1)**: Canvas DAG viz export (Mermaid/DOT); Beat holiday calendar +
  business-day calc + schedule conflict detection + missed-task catch-up; Protocol v5 wire builder +
  version negotiation; Core distributed rate limiting + event snapshots/alerting; Metrics native
  histograms + summary quantiles (P²).
- **Feature expansion (rounds 2–3)**: Canvas loops/sub-workflows/templates/dynamic-edit + workflow
  versioning; Beat timezone-aware + dynamic updates + jitter + dispatch-locking; Protocol YAML +
  custom serializers; Core result tombstones/groups/TTL + task security (HMAC sig, sanitization,
  PII); Worker poison-pill + self-healing; CLI layered config; Metrics StatsD backend.
- **Feature expansion (round 4)**: Worker cooperative cancellation + distributed rate-limit
  coordination; Kombu AES-256-GCM encryption middleware; CLI Prometheus metrics/monitor commands;
  Canvas workflow rate-limit integration. (Also hardened flaky facade timing tests.)
- **Feature expansion (rounds 5–6)**: Core in-memory broker/backend + result caching + per-type
  circuit breakers + per-tenant rate limiting; Worker adaptive polling + batching/coalescing + task
  affinity; Metrics SLO tracking + anomaly detection + audit log; CLI task replay + load testing.
- All 18 crates: `cargo build`/`clippy -D warnings` clean, **5073 tests pass** (0 failures).

### v0.3.0 Hardening — QA/release-prep pass (2026-07-11)

`/nagare` QA/hardening sweep (branch `0.3.0`). All fixes independently verified with zero
regressions — workspace builds clean, clippy clean, **5068/5068 tests passing**:

- **Fixed (CLI)**: `celers` CLI `health` and `db test-connection`/`health`/`pool-stats`/`migrate`
  commands reported failure in their printed output but incorrectly exited 0 (would silently mask
  failures in any script checking the exit code) — now exit nonzero correctly.
- **Fixed (Worker)**: the lock-free queue's `try_pop()` treated a spurious `Steal::Retry` signal
  from the underlying `crossbeam_deque::Injector` the same as a genuinely empty queue — found via
  Miri testing, not a data race (Miri confirmed the `unsafe impl Send/Sync` on the queue is sound)
  but a real logic bug that could make a worker see a false-empty queue under contention. Now
  delegates to the already-correct `pop()`.
- **Fixed (Docs)**: 17 rustdoc broken/redundant/private intra-doc links across `celers-metrics`,
  `celers-core`, `celers-canvas`, `celers-beat`, `celers-cli`, `celers-worker` —
  `RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps` now passes clean.
- **Fixed (Dependencies)**: 12 genuinely-unused dependencies removed across 12 crates (verified
  individually, not just from the static-analysis tool's raw output — one flagged candidate,
  `celers-canvas` in `celers-worker`, was confirmed to be a real feature-gate-only usage and
  correctly kept).
- **Fixed (Metadata)**: `readme = "README.md"` metadata field was missing from 17 of 18 crates'
  `Cargo.toml` despite each having a real README.md on disk (crates.io wouldn't have rendered
  them) — added to all 17.
- **Fixed (Workspace)**: 2 `celers-beat` dependency declarations used
  `{ path = "...", version = "..." }` instead of `{ workspace = true }` — corrected for
  consistency (zero semantic change).
- **New tracked finding (not fixed this pass — needs an architectural decision)**: `ring` and
  `aws-lc-sys`/`aws-lc-rs` are pulled transitively into 9 crates via `sqlx`'s explicit
  `tls-rustls-ring` feature plus `reqwest`/`lapin`/`aws-sdk-*`'s default rustls provider
  selection — a Pure-Rust policy violation. `cargo audit` additionally found 3 real RUSTSEC
  vulnerabilities in this exact same dependency chain (`rustls-webpki` 0.101.7: a reachable panic
  in CRL parsing, plus 2 certificate name-constraint validation bypasses), confirming this isn't
  just a purity concern. Fixing this means either finding TLS-provider-selection flags on
  `sqlx`/`reqwest`/`lapin`/`aws-sdk-*`, or migrating onto `oxisql-postgres`/`oxisql-mysql` and
  `oxihttp-client` (both already production-ready per COOLJAPAN governance).
- **New tracked finding**: 3 files exceed the workspace's 2000-line policy
  (`celers-cli/src/main.rs` at 2153, `celers-metrics/src/tests_core.rs` at exactly 2000,
  `celers-macros/tests/integration_test.rs` at 3038) — candidates for a future `splitrs` pass.

### sqlx → oxisql-\* / reqwest → oxihttp-client migration complete (2026-07-11)

Follow-up to this same day's "not fixed this pass" finding above: `sqlx`/`reqwest` removed from
every crate (`celers-broker-postgres`, `celers-broker-sql`, `celers-backend-db`, `celers-cli`,
`celers-worker`, plus the non-member `celers-examples`) and from the workspace root
`[workspace.dependencies]` entirely. Zero `sqlx`/`reqwest` dependency declarations remain
anywhere in the workspace.

- **Fixed (real bugs, not just mechanical port)**: `oxisql-core` has no `ToSqlValue`/`FromValue`
  bridge for `uuid::Uuid`/`serde_json::Value`, only for the raw `Value::Uuid(u128)`/
  `Value::Json(String)` representations — binding a bare `u128` doesn't compile
  (`ToSqlValue` isn't implemented for it); every call site now goes through a `uuid_param`/
  `json_param`/`uuid_from_row`/`json_from_row` helper (`row_ext.rs`, one per migrated crate)
  instead of hand-rolling the conversion.
  `DateTime<Utc>` binding was a genuine, previously-undetected corruption risk on **both**
  backends, each needing a different fix: Postgres's `oxisql-postgres` always sends parameters in
  binary wire format, so a `.timestamp()` (raw `i64`) or bare RFC3339 `String` bind against a
  `TIMESTAMPTZ` column is rejected by the server's binary decoder — fixed via `.to_rfc3339()` text
  bound through an explicit `$n::text::timestamptz` SQL-side cast. MySQL's `mysql_async`/
  `mysql_common` wire layer instead requires its own strict `DATETIME` grammar
  (`YYYY-MM-DD HH:MM:SS.ffffff`, space separator, no `T`, no timezone suffix) — RFC3339 fails
  `mysql_common`'s parser outright, so MySQL binds use `.format("%Y-%m-%d %H:%M:%S%.6f")` instead,
  with a regression test pinned to `mysql_common`'s actual accepted grammar.
- **Fixed (security regression)**: the initial oxisql port hardcoded `TlsMode::Disabled` at every
  `PgConnection`/MySQL connect call site, silently downgrading to plain-text even when the
  caller's URL said `sslmode=require` — a MEDIUM-severity regression (credentials/queries sent in
  the clear while the caller believes they're encrypted). Fixed in all 5 migrated crates via a
  `tls_mode.rs` helper that parses `sslmode` from the connection URL and resolves the matching
  `TlsMode` (Rustls via `oxitls`, or Disabled only when the URL actually says so).
- **Deferred (perf, tracked not fixed)**: `celers-broker-postgres`'s connection changed from
  `sqlx::PgPool` (a real pool, up to `max_connections`) to `oxisql_postgres::PgConnection`, which
  is `Clone` but internally one shared `Arc<Mutex<tokio_postgres::Client>>` — all clones now
  multiplex a single underlying connection instead of drawing from a pool. Documented in-code as
  accepted for now; revisit if this becomes a throughput bottleneck.
- **Deferred (out of scope, no clean pure-Rust path)**: `celers-broker-amqp` (via `lapin`) and
  `celers-broker-sqs` (via the AWS SDK: `aws-config`/`aws-sdk-sqs`/`aws-sdk-cloudwatch`) still pull
  in `ring`/`aws-lc-sys` — confirmed via `cargo tree -i ring`/`-i aws-lc-sys`: every path now goes
  through exactly these two chains and nothing else. Replacing either is a separate, larger effort
  (no drop-in pure-Rust AMQP client or AWS SDK exists yet in the COOLJAPAN ecosystem).
- **Verified**: of the 3 previously-found `rustls-webpki` 0.101.7 RUSTSEC advisories
  (RUSTSEC-2026-0098/0099/0104), `cargo tree -i rustls-webpki@0.101.7` now shows a single root —
  entirely the AWS SDK chain (`aws-smithy-http-client` → old `hyper-rustls`/`tokio-rustls`/
  `rustls` 0.21.12) via `celers-broker-sqs`. `sqlx`'s own independent `tls-rustls-ring` TLS edge
  (a separate contributor pre-migration) is gone along with the crate itself, so this advisory's
  exposure is narrower now, not just relabeled — though not fully eliminated, since the deferred
  AWS SDK chain still carries it. `cargo audit` also surfaces 4 more `rustls-webpki` 0.102.8
  findings and an `rsa` advisory rooted in `rustls-rustcrypto` (a dependency of `oxitls` itself,
  unrelated to this migration) — tracked separately, not actionable from this workspace.
- Workspace-wide: `cargo build`/`clippy -D warnings --all-targets` clean, **5278 tests pass** (up
  from the 5068 pre-migration baseline — each migrated crate's `row_ext.rs`/`tls_mode.rs` added
  its own test module), 1030 doc tests pass, 0 failures.

### v0.3.0 Release-Prep — final `/runall` pipeline pass (2026-07-13)

Final release-readiness sweep before 0.3.0 ships (branch `0.3.0`). Full workspace re-verified
clean: `cargo build --workspace --all-features` and
`cargo clippy --workspace --all-features --all-targets -- -D warnings` both clean (0 errors,
0 warnings), **5676/5676 tests passing with `--all-features`, 5495/5495 with default features**
(0 failures, 0 flaky).

- **Fixed (Docs)**: 3 broken rustdoc private-intra-doc-links in `celers-broker-postgres`.
- **Fixed (Test speed)**: 2 slow (>30s) tests in `celers-cli`'s `depgraph.rs` — shrank the test
  fanout size so both run at normal speed with no loss of coverage.
- **Fixed (Dependencies)**: removed a genuinely-unused `rust_decimal` dependency from
  `celers-broker-sql`; bumped `aws-config`/`aws-sdk-sqs`/`aws-sdk-cloudwatch`/`regex`/`rand` to
  latest compatible versions.
- **Fixed (Workspace)**: consolidated 13 internal crate dependencies (`celers-core`,
  `celers-protocol`, `celers-metrics`, `celers-backend-redis`, `celers-kombu`, `celers-worker`,
  `celers-canvas`, `celers-broker-redis`, `celers-broker-postgres`, `celers-macros`,
  `celers-broker-sqs`, `celers-broker-sql`, `celers-broker-amqp`) into root
  `[workspace.dependencies]`, updating 14 consumer crates to `{ workspace = true }` — zero semantic
  change, consistency only.
- **Cleanup**: deleted a stray empty leftover directory (`crates/broker-redis/`, superseded long
  ago by `crates/celers-broker-redis/`); fixed a stale `VERSION="0.2.0"` in the external publish
  script (`~/work/pub_celers.sh`) to `0.3.0`.
- **New tracked finding (accepted caveat, not fixed this pass)**: `proc-macro-error2` 2.0.1
  (transitively via `tabled_derive` → `tabled` → `celers-cli`) is flagged unmaintained upstream
  (RUSTSEC-2026-0173). No CVE/known exploit; documented in `CHANGELOG.md` as accepted for this
  release rather than swapping `tabled` this close to publish, since that crate's call sites in
  `celers-cli` were already extensively touched this same cycle (report/error-codes/cache-stats
  table rendering).
- **Deferred (open, tracked — do not attempt without a dedicated follow-up session)**: 3 files now
  exceed the workspace's 2000-line refactor policy — `celers-broker-sql/src/broker_core.rs`
  (2051 lines, production code), `celers-macros/tests/integration_test.rs` (3038 lines), and
  `celers-metrics/src/tests_core.rs` (exactly 2000 lines). This supersedes the file list in the
  2026-07-11 entry above: that pass's offender, `celers-cli/src/main.rs` (then 2153 lines), has
  since been split down to 210 lines by the CLI module reorganization documented in
  `CHANGELOG.md`'s 0.3.0 CLI section (`cli/types.rs`, `cli/dispatch.rs`, `commands/*.rs`);
  `broker_core.rs` newly crossed the threshold during the sqlx→oxisql migration above. Splitting
  all 3 is explicitly deferred to a dedicated follow-up session rather than risked during release
  prep — `splitrs` is the recommended tool per project policy.

## Quick Stats

- **Crates**: 18/18 (100% COMPLETE) - core, worker, protocol, kombu, canvas, beat, macros, CLI, metrics, 5 brokers, 3 backends
- **Brokers**: Redis, PostgreSQL, MySQL, RabbitMQ (AMQP), AWS SQS
- **Backends**: Redis, PostgreSQL/MySQL (Database), gRPC - ALL with ResultStore adapters
- **Examples**: 15 working examples (including Canvas workflows, web scraper, image processing, AsyncResult API)
- **Benchmarks**: 3 comprehensive benchmark suites
- **Unit Tests**: 5676 tests passing with `--all-features` / 5495 with default features, 0 failures
  (v0.3.0 hardening + feature expansion + release-prep pass, verified 2026-07-13)
- **Build Status**: ✅ 0 errors, 0 warnings, 0 clippy warnings, 0 doc warnings
- **Documentation**: 1500+ lines of guides + 18 TODO.md files
- **Monitoring**: Full Prometheus + Grafana + OpenTelemetry support
- **Features**: Task queues, priorities, DLQ, cancellation, retries, timeouts, health checks, Canvas workflows (chunks, xmap, xstarmap, group.skew, group.jitter, conditional: Branch/Maybe/Switch), batch operations, memory optimization, chord synchronization, AsyncResult API with ALL backend ResultStore adapters, Protocol v2 compatibility tests, Real-time events (task & worker lifecycle), Event emission from worker, Worker control commands (inspect, ping, shutdown, revoke), Per-task rate limiting (token bucket, sliding window), Task routing by name patterns (glob, regex), Time limits (soft/hard), Enhanced task revocation (bulk, pattern, persistent), Queue control commands

---

## Phase 1: The Backbone ✅ COMPLETE

### Goal
Simple tasks can be enqueued to Redis, and Rust workers can pick them up and execute them.

### Tasks
- [x] `celers-core`: Define `Task` and `Broker` traits
- [x] `celers-core`: Implement `TaskState` state machine
- [x] `celers-core`: Define error types
- [x] `celers-broker-redis`: Implement basic `enqueue` and `dequeue`
- [x] `celers-broker-redis`: Add BRPOPLPUSH atomic operation
- [x] `celers-core`: Add TaskExecutor trait and registry
- [x] `celers-worker`: Implement task execution loop
- [x] `celers-worker`: Add retry logic with exponential backoff
- [x] `celers-worker`: Add graceful shutdown handling
- [x] `celers-worker`: Add task timeout enforcement
- [x] Create end-to-end working example (phase1_complete.rs)
- [x] Create graceful shutdown example (graceful_shutdown.rs)
- [ ] Add integration tests with Redis

## Phase 2: Advanced Features ✅ In Progress

### PostgreSQL Broker ✅ COMPLETE
- [x] `celers-broker-postgres`: Design schema for task queue table
- [x] `celers-broker-postgres`: Implement `FOR UPDATE SKIP LOCKED` pattern
- [x] `celers-broker-postgres`: Add transaction support
- [x] `celers-broker-postgres`: Add migration scripts
- [x] Create comprehensive example (postgres_broker_example.rs)

### Task Priorities ✅ COMPLETE
- [x] `celers-core`: Extend broker trait for priority queues
- [x] `celers-broker-redis`: Implement using ZADD with scores
- [x] `celers-broker-postgres`: Implement using ORDER BY priority
- [x] Add helper methods (with_priority, with_max_retries, with_timeout)
- [x] Create priority queue example (priority_queue.rs)

### Task Cancellation ✅ COMPLETE
- [x] `celers-core`: Add cancellation support to broker trait
- [x] `celers-broker-redis`: Implement Pub/Sub for cancellation signals
- [x] `celers-broker-redis`: Add PubSub connection helper methods
- [x] Create task cancellation example (task_cancellation.rs)
- [x] `celers-worker`: Handle cancellation during execution (cooperative token, broker-signal driven)

### Dead Letter Queue ✅ COMPLETE
- [x] `celers-core`: Define DLQ behavior
- [x] `celers-broker-redis`: Move failed tasks to DLQ after max retries
- [x] `celers-broker-postgres`: Implement DLQ table
- [x] `celers-broker-postgres`: Add automatic DLQ movement on max retries
- [x] `celers-broker-redis`: Add DLQ inspection and replay capabilities
- [x] Create DLQ example (dead_letter_queue.rs)

## Phase 3: Developer Experience

### Procedural Macros ✅ COMPLETE
- [x] `celers-macros`: Implement `#[derive(Task)]` macro
- [x] `celers-macros`: Implement `#[task]` attribute macro
- [x] `celers-macros`: Result type extraction and handling
- [x] Add macro documentation and examples
- [x] Create macro usage example (macro_tasks.rs)

### CLI Tooling ✅ COMPLETE
- [x] `celers-cli`: Implement worker start/stop commands
- [x] `celers-cli`: Add queue inspection commands (status)
- [x] `celers-cli`: Add DLQ management commands (inspect, clear, replay)
- [x] `celers-cli`: Add configuration file support (TOML)
- [x] `celers-cli`: Add colored output and formatted tables
- [x] `celers-cli`: Create comprehensive CLI documentation
- [x] `celers-cli`: Add task cancel command
- [x] `celers-cli`: Add metrics and monitoring commands (`metrics` + `monitor`, Prometheus scrape)

### Monitoring & Observability ✅ COMPLETE
- [x] Add Prometheus metrics exporter
- [x] Add OpenTelemetry tracing integration (via documentation)
- [x] Create Grafana dashboard templates
- [x] Add health check endpoints

## Phase 4: Performance & Scalability ✅ COMPLETE

### Performance Optimization
- [x] Create Criterion.rs benchmarking suite
- [x] Benchmark JSON serialization performance
- [x] Create comprehensive performance optimization guide
- [x] Implement batch enqueue/dequeue operations
- [x] Add connection pooling metrics
- [x] Optimize memory usage in worker
- [x] Create batch operations benchmark

### Horizontal Scaling
- [ ] Test multiple worker instances (ready for testing)
- [x] Implement worker heartbeat mechanism (WorkerStats, heartbeat events)
- [x] Add worker coordination for distributed rate limiting
- [x] Document scaling best practices (included in performance guide)

### Benchmarking Infrastructure
- [x] Set up Criterion.rs for performance testing
- [x] Create serialization benchmarks
- [x] Create queue operations benchmarks
- [x] Document how to run and interpret benchmarks

## Documentation 🚧 IN PROGRESS

### Core Documentation
- [ ] Write comprehensive API documentation (in progress via rustdoc)
- [x] Create user guide with examples (10+ working examples)
- [x] Write deployment guide (Docker, Kubernetes) ✅ (DEPLOYMENT.md created)
- [x] Create performance tuning guide (PERFORMANCE.md created)
- [ ] Add architecture decision records (ADRs) (future)

### Specialized Guides
- [x] Grafana dashboard documentation (GRAFANA.md)
- [x] OpenTelemetry integration guide (OPENTELEMETRY.md)
- [x] CLI usage documentation (celers-cli/README.md)
- [x] Performance optimization guide (PERFORMANCE.md)
- [ ] Migration guide from other task queues (future)

### Examples & Tutorials
- [x] Basic task execution (phase1_complete.rs)
- [x] Graceful shutdown (graceful_shutdown.rs)
- [x] Priority queues (priority_queue.rs)
- [x] Dead letter queue (dead_letter_queue.rs)
- [x] Task cancellation (task_cancellation.rs)
- [x] Procedural macros (macro_tasks.rs)
- [x] Prometheus metrics (prometheus_metrics.rs)
- [x] Health checks (health_checks.rs)
- [x] AsyncResult API (async_result.rs)

## Testing

- [ ] Add unit tests for all core types
- [ ] Add integration tests with real Redis
- [ ] Add integration tests with PostgreSQL
- [ ] Add stress tests and benchmarks
- [ ] Add chaos testing scenarios
- [ ] Achieve >80% code coverage

## Release Preparation

- [x] Set up CI/CD pipeline (GitHub Actions) ✅
- [x] Add automated testing on multiple Rust versions ✅
- [x] Configure cargo-release for versioning (via GitHub Actions) ✅
- [x] Prepare crates.io publication ✅ (All 18 crates have complete metadata)
- [ ] Write migration guide from other task queues
- [x] Create example applications (web scraper, image processing) ✅

## Subcrate TODO Files

Each crate has its own detailed TODO.md with implementation status and future enhancements:

### Core Crates
- **[celers](crates/celers/TODO.md)** - Facade crate and re-exports
- **[celers-core](crates/celers-core/TODO.md)** - Core traits and types
- **[celers-worker](crates/celers-worker/TODO.md)** - Worker runtime and health checks
- **[celers-protocol](crates/celers-protocol/TODO.md)** - Celery protocol implementation
- **[celers-kombu](crates/celers-kombu/TODO.md)** - Kombu-compatible layer

### Brokers (Task Queues)
- **[celers-broker-redis](crates/celers-broker-redis/TODO.md)** - Redis broker (pipelining, Lua scripts)
- **[celers-broker-postgres](crates/celers-broker-postgres/TODO.md)** - PostgreSQL broker (FOR UPDATE SKIP LOCKED)
- **[celers-broker-sql](crates/celers-broker-sql/TODO.md)** - MySQL broker (FOR UPDATE SKIP LOCKED)
- **[celers-broker-amqp](crates/celers-broker-amqp/TODO.md)** - RabbitMQ/AMQP broker
- **[celers-broker-sqs](crates/celers-broker-sqs/TODO.md)** - AWS SQS broker (cloud-native)

### Backends (Result Storage)
- **[celers-backend-redis](crates/celers-backend-redis/TODO.md)** - Redis result backend
- **[celers-backend-db](crates/celers-backend-db/TODO.md)** - PostgreSQL/MySQL result backend
- **[celers-backend-rpc](crates/celers-backend-rpc/TODO.md)** - gRPC result backend

### Features & Tools
- **[celers-canvas](crates/celers-canvas/TODO.md)** - Workflow primitives (chain, group, chord)
- **[celers-beat](crates/celers-beat/TODO.md)** - Periodic task scheduler
- **[celers-macros](crates/celers-macros/TODO.md)** - Procedural macros (#[task])
- **[celers-cli](crates/celers-cli/TODO.md)** - Command-line interface
- **[celers-metrics](crates/celers-metrics/TODO.md)** - Prometheus metrics

---

## Phase 5: Beat Scheduler ✅ COMPLETE

### Periodic Task Scheduling ✅ COMPLETE
- [x] `celers-beat`: Interval scheduling (every N seconds)
- [x] `celers-beat`: Crontab scheduling (with cron crate)
- [x] `celers-beat`: Solar scheduling (sunrise/sunset with sunrise crate)
- [x] `celers-beat`: Task enable/disable support
- [x] `celers-beat`: Last run tracking
- [x] `celers-beat`: Priority support for scheduled tasks
- [x] Documentation and examples

## Phase 6: Extended Brokers & Backends ✅ COMPLETE

### Additional Brokers ✅ COMPLETE
- [x] RabbitMQ/AMQP broker (celers-broker-amqp)
- [x] MySQL broker (celers-broker-sql)
- [x] AWS SQS broker (celers-broker-sqs)

### Additional Backends ✅ COMPLETE
- [x] Database backend - PostgreSQL/MySQL (celers-backend-db)
- [x] gRPC backend for microservices (celers-backend-rpc)

## Phase 7: Full Celery Protocol Compatibility 🚧 IN PROGRESS

**Goal**: Achieve 100% compatibility with Python Celery for seamless interoperability

### Critical: Python Celery Interoperability
- [x] **Full Protocol v2 Wire Compatibility** ✅
  - [x] Verify Celery v2 message format (headers, properties, body)
  - [x] Test serialization/deserialization with Python Celery messages
  - [x] Handle all Celery v2 message types (task, result, event)
  - [x] Support task arguments (args, kwargs, embed) formats
  - [x] Support all content types (JSON) - msgpack, pickle, YAML pending
  - [x] Handle task ETA/countdown correctly
  - [x] Support task expires timestamps
  - [ ] Test with Python Celery 4.x workers (integration tests pending)

- [ ] **Full Protocol v5 Wire Compatibility**
  - [x] Implement Celery v5 message format changes
  - [ ] Test with Python Celery 5.x workers
  - [x] Support protocol version negotiation
  - [x] Handle backward compatibility with v2

- [ ] **Bidirectional Task Exchange**
  - [ ] Rust worker can execute tasks sent from Python Celery
  - [ ] Python Celery worker can execute tasks sent from Rust
  - [ ] Shared task registry between Python and Rust
  - [ ] Result retrieval across languages
  - [ ] Error handling compatibility

- [ ] **Integration Testing Suite**
  - [ ] Create Python Celery + CeleRS integration tests
  - [ ] Test task submission Python → Rust
  - [ ] Test task submission Rust → Python
  - [ ] Test result retrieval across languages
  - [ ] Test Canvas workflows (Chain, Chord, Group) interop
  - [ ] Test Beat scheduler task submission
  - [ ] Performance comparison benchmarks

### Task Routing & Dispatching
- [x] **Advanced Routing** ✅ (Implemented in celers-core/src/router.rs)
  - [x] Task routing by name patterns (glob, regex)
  - [x] Task routing by arguments/kwargs (ArgumentCondition)
  - [x] Queue routing based on task type
  - [x] Topic-based routing (AMQP exchanges)
  - [x] Priority-based routing
  - [x] Custom routing strategies (RouterBuilder, RoutingConfig)

- [x] **Rate Limiting** ✅ (Implemented in celers-core/src/rate_limit.rs)
  - [x] Per-task rate limiting (X tasks per second)
  - [x] Per-worker rate limiting
  - [x] Token bucket algorithm
  - [x] Sliding window rate limiting
  - [x] Distributed rate limiting across workers
  - [x] Rate limit integration with Canvas workflows

### Task Execution & Control
- [x] **Time Limits** ✅ (Implemented in celers-core/src/time_limit.rs)
  - [x] Soft time limit (warning before kill)
  - [x] Hard time limit (force kill)
  - [x] Different limits per task type
  - [x] Graceful cleanup on timeout (TimeLimitStatus enum)
  - [x] Time limit exceeded exception handling (TimeLimitExceeded error)

- [x] **Task Revocation (Enhanced)** ✅ (Implemented in celers-core/src/revocation.rs)
  - [x] Revoke task by ID
  - [x] Revoke all instances of a task
  - [x] Terminate vs ignore revocation modes
  - [x] Persistent revocation (survive worker restart) - RevocationState
  - [x] Revoke by task name pattern (PatternRevocation)
  - [x] Bulk task revocation (bulk_revoke method)

- [x] **Task Linking & Callbacks** ✅ (Implemented in celers-canvas/src/lib.rs)
  - [x] link (on_success callback chain)
  - [x] link_error (on_failure callback)
  - [x] on_retry callbacks
  - [x] Signature linking (task.apply_async(link=...))
  - [x] Immutable signatures
  - [x] Callback argument passing (CallbackArgMode: Prepend, Append, Kwarg, None)

- [x] **Task State Tracking** ✅
  - [x] PENDING state
  - [x] RECEIVED state
  - [x] STARTED state
  - [x] SUCCESS state
  - [x] FAILURE state
  - [x] RETRY state
  - [x] REVOKED state
  - [x] REJECTED state
  - [x] Custom states (TaskState::Custom with name and metadata)
  - [x] State transition events (StateTransition, StateHistory)

### Events & Monitoring
- [ ] **Real-Time Events**
  - [x] Event type definitions (celers-core/src/event.rs)
  - [x] task-sent event type
  - [x] task-received event type
  - [x] task-started event type
  - [x] task-succeeded event type
  - [x] task-failed event type
  - [x] task-retried event type
  - [x] task-revoked event type
  - [x] task-rejected event type
  - [x] worker-online event type
  - [x] worker-offline event type
  - [x] worker-heartbeat event type
  - [x] TaskEventBuilder for easy event creation
  - [x] WorkerEventBuilder for easy event creation
  - [x] EventEmitter trait and implementations (NoOp, InMemory, Logging, Composite)
  - [x] Event emitting from worker (received, started, succeeded, failed, retried, rejected)
  - [x] Worker lifecycle events (online, offline)
  - [x] Redis event transport (pub/sub) - RedisEventEmitter, RedisEventReceiver
  - [x] AMQP event transport (fanout)

- [x] **Event Consumers**
  - [x] Event receiver/dispatcher
  - [x] Event filtering and routing
  - [x] Event persistence (database, file)
  - [ ] Event streaming (WebSocket, SSE)
  - [x] Snapshot events for monitoring
  - [x] Event-based alerting

### Remote Control & Inspection
- [x] **Worker Control Commands** ✅ (Protocol defined in celers-core/src/control.rs)
  - [x] `worker.control.inspect.active()` - List active tasks
  - [x] `worker.control.inspect.scheduled()` - List scheduled tasks
  - [x] `worker.control.inspect.reserved()` - List reserved tasks
  - [x] `worker.control.inspect.stats()` - Worker statistics
  - [x] `worker.control.inspect.registered()` - Registered tasks
  - [x] `worker.control.pool_restart()` - Restart worker pool
  - [x] `worker.control.pool_grow()` - Add worker processes
  - [x] `worker.control.pool_shrink()` - Remove worker processes
  - [x] `worker.control.shutdown()` - Graceful shutdown
  - [x] `worker.control.rate_limit()` - Set task rate limits
  - [x] `worker.control.time_limit()` - Set task time limits
  - [x] `worker.control.revoke()` - Revoke tasks
  - [x] `worker.control.ping()` - Ping workers

- [x] **Queue Control** ✅ (Implemented in celers-core/src/control.rs - QueueCommand enum)
  - [x] `queue.purge()` - Clear queue
  - [x] `queue.length()` - Get queue size
  - [x] `queue.delete()` - Delete queue
  - [x] `queue.bind()` - Bind queue to exchange (AMQP)
  - [x] `queue.unbind()` - Unbind queue
  - [x] `queue.declare()` - Declare new queue

### Canvas Workflow Enhancements
- [x] **Enhanced Workflows** ✅ (Implemented in celers-canvas/src/lib.rs)
  - [x] `chain.apply_async()` with ETA (apply_with_eta, apply_with_countdown, with_staggered_countdown)
  - [x] `group.skew()` - Staggered task execution
  - [x] `group.jitter()` - Random delay distribution
  - [x] `chunks()` - Split iterable into chunks
  - [x] `xmap()` - Map with exception handling
  - [x] `xstarmap()` - Starmap with exception handling
  - [x] Nested workflows (CanvasElement, NestedChain, NestedGroup)
  - [x] Conditional workflows (if/else primitives) ✅ - Condition, Branch, Maybe, Switch
  - [x] Workflow error handling strategies (ErrorStrategy enum)

- [x] **Signature Improvements** ✅
  - [x] Partial signatures (`.partial()`, `.complete()`)
  - [x] Signature cloning (`.clone()` via derive)
  - [x] Signature merging (`.merge()`)
  - [x] Signature serialization/deserialization (`.to_json()`, `.from_json()`)
  - [x] Immutable signatures (`.si()`, `.immutable()`)
  - [x] Signature options inheritance (`.merge()`)
  - [x] Extended TaskOptions (expires, countdown, max_retries, routing_key)

### Result Backend Enhancements
- [ ] **Advanced Result Features**
  - [x] Result metadata (task_id, name, args, kwargs, worker, etc.)
  - [x] Result TTL per task type
  - [x] Result compression (gzip, lz4, zstd)
  - [x] Result chunking for large payloads
  - [ ] Result streaming for long-running tasks
  - [x] Result tombstones (mark deleted results)
  - [x] Result groups (grouped results for Chord)

- [x] **AsyncResult API** ✅
  - [x] `result.get()` - Block until result ready
  - [x] `result.get(timeout=X)` - Wait with timeout
  - [x] `result.ready()` - Check if ready
  - [x] `result.successful()` - Check if succeeded
  - [x] `result.failed()` - Check if failed
  - [x] `result.state` - Get current state
  - [x] `result.info` - Get task info/metadata
  - [x] `result.traceback` - Get exception traceback
  - [x] `result.revoke()` - Revoke task
  - [x] `result.forget()` - Remove result from backend
  - [x] `result.parent` - Get parent result
  - [x] `result.children` - Get child results

### Serialization & Content Types
- [ ] **Full Serializer Support**
  - [ ] JSON (default) ✅
  - [ ] MessagePack ✅
  - [x] YAML serializer
  - [ ] Pickle serializer (Python compat - security warning)
  - [x] Custom serializers
  - [x] Compression support (gzip, brotli, zstd)
  - [x] Serializer auto-detection

### Configuration & Settings
- [ ] **Celery-Compatible Configuration**
  - [ ] Support celeryconfig.py-style configuration
  - [x] Environment variable configuration (CELERY_*)
  - [x] Configuration via CLI arguments ✅ (celers-cli: `CliConfigArgs`, precedence CLI > env > file > defaults)
  - [x] Configuration via YAML/TOML files ✅ (celers-cli: format auto-detected by extension)
  - [x] Dynamic configuration updates ✅ (celers-cli: `config reload` + `ReloadableConfig`/`ConfigDiff`)
  - [x] Configuration validation and defaults

### Beat Scheduler Enhancements
- [ ] **Advanced Scheduling**
  - [ ] Persistent schedule (database-backed)
  - [x] Dynamic schedule updates (add/remove at runtime)
  - [x] Schedule conflict detection
  - [x] Missed task catch-up logic
  - [x] Schedule locking (prevent duplicate execution)
  - [x] Timezone-aware schedules
  - [x] Holiday calendar support
  - [x] Business day calculations

- [x] **Beat Synchronization**
  - [x] Leader election for multi-beat deployments
  - [x] Heartbeat mechanism
  - [x] Failover support
  - [ ] Schedule replication

### Error Handling & Retry Policies
- [x] **Advanced Retry Strategies** ✅ (Implemented in celers-core/src/retry.rs)
  - [x] Exponential backoff
  - [x] Linear backoff
  - [x] Fixed delay
  - [x] Polynomial backoff
  - [x] Fibonacci backoff
  - [x] Decorrelated jitter (AWS recommended)
  - [x] Full jitter and Equal jitter
  - [x] Custom retry strategies (explicit delay sequence)
  - [x] Max retry delays
  - [x] Retry jitter
  - [x] Conditional retries (retry_on/dont_retry_on patterns)
  - [x] RetryPolicy configuration

- [x] **Exception Handling** ✅ (Implemented in celers-core/src/exception.rs)
  - [x] Ignore result on specific exceptions (ExceptionPolicy.ignore_on)
  - [x] Custom exception handlers per task (ExceptionHandler trait)
  - [x] Exception serialization for cross-language (TaskException JSON/Celery format)
  - [x] Traceback preservation (TracebackFrame, traceback_str)
  - [x] Retry on specific exception types (ExceptionPolicy.retry_on)

### Security & Authentication
- [ ] **Broker Security**
  - [ ] TLS/SSL for Redis ✅ (supported by redis crate)
  - [ ] TLS/SSL for PostgreSQL ✅ (supported by sqlx)
  - [ ] TLS/SSL for RabbitMQ ✅ (supported by amqp crate)
  - [ ] Authentication tokens
  - [ ] IP whitelisting

- [ ] **Task Security**
  - [x] Task signature verification
  - [x] Message encryption (at rest and in transit)
  - [x] Task argument sanitization
  - [ ] Secure pickle (prevent arbitrary code execution)

### Developer Experience
- [ ] **Migration Tools**
  - [ ] Celery → CeleRS migration guide
  - [ ] Code generator for Rust tasks from Python
  - [ ] Configuration converter (celeryconfig.py → TOML)
  - [ ] Task registry synchronization tool

- [ ] **Debugging Tools**
  - [ ] Task execution tracing
  - [ ] Step-through debugger integration
  - [x] Task replay (re-execute failed tasks)
  - [ ] Time-travel debugging
  - [ ] Performance profiler

### Documentation
- [ ] **Celery Compatibility Guide**
  - [ ] Feature parity matrix
  - [ ] API compatibility reference
  - [ ] Migration guide from Python Celery
  - [ ] Interoperability examples
  - [ ] Best practices for mixed deployments

## Phase 8: v0.2.0 Enhancements ✅ COMPLETE

### Compression Unification ✅ COMPLETE
- [x] Unified CompressionType across protocol, broker-redis, broker-amqp
- [x] CompressionRegistry for managing available algorithms
- [x] CompressionStats for tracking compression effectiveness
- [x] Zlib compression support added

### Distributed Beat Locks ✅ COMPLETE
- [x] DistributedLockBackend trait in celers-core
- [x] InMemoryLockBackend for single-instance/testing
- [x] RedisLockBackend (SET NX EX + Lua CAS)
- [x] DbLockBackend (PostgreSQL table-based locks)
- [x] BeatScheduler integration with distributed locks

### AMQP Event Transport ✅ COMPLETE
- [x] AmqpEventEmitter (fanout exchange, JSON serialization)
- [x] AmqpEventReceiver (exclusive auto-delete queue)
- [x] Publisher confirms for reliability
- [x] Event transport statistics tracking

### Event Filtering & Routing ✅ COMPLETE
- [x] EventFilter trait with GlobEventFilter
- [x] ExactEventFilter and PrefixEventFilter
- [x] CompositeEventFilter (AND/OR/NOT modes)
- [x] EventRouter with priority-based dispatch
- [x] EventHandler async trait

### Result Backend Enhancements ✅ COMPLETE
- [x] Per-task-type result TTL configuration
- [x] Result metadata enrichment (worker_hostname, runtime_ms, memory_bytes)
- [x] Zstd compression support in Redis backend
- [x] Compression statistics tracking

### Serialization Auto-Detection ✅ COMPLETE
- [x] Magic number detection for JSON, MessagePack, BSON, YAML, Protobuf
- [x] Format negotiation between endpoints
- [x] Available types enumeration based on features

### Beat Crate Refactoring ✅ COMPLETE
- [x] Split 12,515-line lib.rs into 12 focused modules
- [x] All modules under 2000 lines

## Phase 9: v0.2.0 Production Features ✅ COMPLETE

### Event Persistence ✅ COMPLETE
- [x] EventPersister trait (query, count, cleanup, flush)
- [x] FileEventPersister (JSONL with daily/size rotation, retention cleanup)
- [x] DbEventPersister (PostgreSQL batch insert, SQL queries)

### Result Chunking ✅ COMPLETE
- [x] ChunkingConfig (threshold, chunk size, CRC32 checksum)
- [x] ResultChunker (split, reassemble, sentinel markers)
- [x] Wire into RedisResultBackend (chunking_config, chunker fields)

### Beat Heartbeat & Failover ✅ COMPLETE
- [x] BeatRole enum (Leader, Standby, Unknown)
- [x] HeartbeatConfig with configurable intervals
- [x] BeatHeartbeat (leader election, lease renewal, failover detection)
- [x] BeatScheduler integration (is_leader check)

### AMQP Topic Routing ✅ COMPLETE
- [x] TopicRoutingRule (glob patterns, priority)
- [x] TopicRouter (resolve_routing_key, dynamic add/remove)
- [x] AmqpRoutingConfig (exchange, rules, default key)

### Enhanced Celery Config ✅ COMPLETE
- [x] Expanded from_env() with 23+ CELERY_* environment variables
- [x] ConfigValidation (errors, warnings, suggestions)
- [x] validate_detailed() for comprehensive config checking
- [x] to_env_vars() for config export
- [x] dump() for debug output

## Future Enhancements (Post v1.0)

### Infrastructure & Brokers
- [x] Add support for scheduled/delayed tasks ✅ (celers-beat complete)
- [x] Add support for RabbitMQ/AMQP, AWS SQS ✅ (all complete)
- [ ] Add Kafka broker support (high-throughput event streaming)
- [ ] Add NATS broker support (cloud-native messaging)
- [ ] Add Azure Service Bus broker support
- [ ] Add Google Cloud Pub/Sub broker support
- [ ] Redis Cluster support with automatic sharding
- [ ] Redis Sentinel support for high availability
- [ ] Multi-region broker coordination

### Result Backends
- [x] Implement task result backend ✅ (celers-backend-redis complete)
- [x] Add database backend support ✅ (celers-backend-db complete)
- [x] Add gRPC backend support ✅ (celers-backend-rpc complete)
- [ ] Add S3/Object Storage result backend (for large results)
- [ ] Add WebSocket result streaming (real-time updates)
- [ ] Add Cassandra backend (distributed NoSQL)
- [ ] Add MongoDB backend (document-oriented)
- [ ] Result compression and deduplication
- [ ] Result archival and retention policies

### Workflow & Orchestration
- [x] Implement task workflows/DAGs ✅ (celers-canvas complete)
- [x] Add batch operations for brokers ✅ (SQS, AMQP, Worker)
- [x] Add task progress tracking ✅ (Redis backend)
- [x] Dynamic workflow modification (add/remove tasks at runtime)
- [x] Workflow versioning and migration
- [x] Conditional workflow branching (if/else logic) ✅ - Branch, Maybe, Switch primitives
- [x] Workflow loops and iteration
- [x] Sub-workflows and nested composition
- [x] Workflow templates and macros
- [x] Workflow DAG visualization export (GraphViz, Mermaid)

### Scheduling & Beat
- [x] Add persistent state for beat scheduler ✅ (JSON file-based persistence)
- [x] Add leader election for beat scheduler ✅ (implemented in celers-beat/src/heartbeat.rs — BeatHeartbeat with BeatRole enum, Phase 9)
- [x] Timezone-aware cron schedules
- [x] Dynamic schedule updates via API
- [x] Schedule conflict detection
- [x] Missed task catch-up logic
- [x] Schedule jitter to prevent thundering herd
- [x] Holiday calendar support
- [x] Business day calculations

### Monitoring & Observability
- [x] Enhanced Prometheus metrics (percentiles, histograms)
- [x] StatsD metrics backend
- [ ] OpenTelemetry metrics (full integration)
- [ ] CloudWatch metrics integration
- [ ] Datadog APM integration
- [ ] Distributed tracing (span propagation)
- [ ] Structured logging with correlation IDs
- [ ] Real-time event streaming (WebSocket, SSE)
- [x] Audit log for task lifecycle events
- [ ] Performance profiling integration

### Administration & Management
- [ ] Create web-based admin dashboard (React/Vue/Svelte)
- [ ] REST API for task management
- [ ] GraphQL API for flexible queries
- [ ] Task inspection and debugging tools
- [ ] Queue management UI (pause, purge, rebalance)
- [ ] Worker fleet management interface
- [ ] Live task execution monitoring
- [ ] Historical analytics and reporting
- [ ] Rate limiting and quota management
- [ ] Multi-tenancy support

### Performance & Scalability
- [ ] Connection pooling for all brokers
- [ ] Task prefetching and pipelining
- [x] Result caching layer (celers-core `CachingResultBackend<B>`: bounded native LRU + per-entry TTL fronting any `ResultStore`)
- [ ] Lazy task deserialization
- [ ] Zero-copy message passing
- [ ] SIMD optimizations for serialization
- [x] Task batching and coalescing
- [x] Adaptive polling intervals
- [ ] Memory-mapped queue storage
- [ ] Lock-free data structures

### Developer Experience
- [ ] Task testing framework and mocks
- [x] Local development mode (in-memory broker) (celers-core `InMemoryBroker` + `InMemoryResultBackend`, no external services)
- [ ] Task debugging and step-through execution
- [ ] Task replay and time-travel debugging
- [ ] Hot reload for task definitions
- [ ] IDE integrations (LSP, syntax highlighting)
- [ ] Code generation from schemas
- [ ] Migration tools from Celery/Sidekiq/Bull
- [ ] Benchmark suite and profiling tools
- [x] Task simulation and load testing

### Security & Compliance
- [x] Task encryption at rest and in transit
- [ ] mTLS for broker connections
- [ ] RBAC for task execution permissions
- [ ] Secrets management integration (Vault, AWS Secrets)
- [ ] Audit logging for compliance (SOC2, HIPAA)
- [x] Task signature verification
- [x] Rate limiting per user/tenant
- [ ] IP whitelisting for workers
- [ ] Data residency controls
- [x] PII detection and masking

### Resilience & Reliability
- [x] Circuit breaker per task type
- [ ] Automatic retry with backoff strategies
- [ ] Task timeout with graceful degradation
- [x] Poison pill detection and quarantine
- [x] Self-healing worker restarts
- [ ] Automatic queue rebalancing
- [ ] Data corruption detection
- [ ] Split-brain prevention
- [ ] Consensus algorithms for coordination
- [ ] Disaster recovery procedures

### Integration & Ecosystem
- [ ] Python Celery interoperability (task submission)
- [ ] JavaScript/Node.js client library
- [ ] Go client library
- [ ] Java/Kotlin client library
- [ ] Terraform provider for infrastructure
- [ ] Kubernetes operator
- [ ] Helm charts for deployment
- [ ] Docker Compose examples
- [ ] AWS CDK constructs
- [ ] Pulumi resources

### Advanced Features
- [ ] Task dependencies and DAG scheduling
- [ ] Resource allocation and scheduling (CPU, memory)
- [ ] Priority-based task queuing improvements
- [x] Task affinity (worker-to-task matching)
- [ ] Speculative execution for critical tasks
- [ ] Machine learning for task optimization
- [ ] Predictive scaling based on patterns
- [x] Anomaly detection for task failures
- [ ] Cost optimization recommendations
- [x] SLA/SLO tracking and alerting
