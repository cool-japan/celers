# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.1] - Unreleased

0.3.1 is a hardening release, not a feature drop: 475 files changed against 0.3.0. The headline
additions are the ones that make a CeleRS deployment operable and defensible in production — a
remote control protocol with `celers inspect` / `celers control` in front of it, revocations that
survive a worker restart because the *broker* holds them, an event stream a Python Celery monitor
can actually parse, opt-in message authentication on the worker receive path, soft/hard time
limits, and workflow patterns (chord aggregation, saga compensation, conditional branches) that a
worker really executes instead of only building. The whole workspace is now Pure Rust with an empty
`deny.toml` `[graph] exclude`.

Celery compatibility is now **proved rather than asserted — at the protocol layer**: a new
`tests/python-compat/` suite exchanges tasks and results with a real Python Celery 5.6.3 in both
directions, and `crates/celers-protocol/tests/fixtures/` holds verbatim Celery wire captures. The
boundary is stated plainly in the rewritten `docs/CELERY_COMPATIBILITY.md`: the **broker and result
backend still carry CeleRS-shaped payloads**, so a Python Celery worker and a CeleRS worker cannot
share a queue yet.

**Read the breaking-changes section first if you consume CeleRS' event stream, subscribe to a Redis
`<queue>:cancel` channel, `match` exhaustively on `TaskEvent`, or rely on task coalescing.**

### ⚠️ Breaking changes

#### Wire format

- **The event stream now emits the Celery wire shape.** Everything CeleRS publishes on the Redis
  `celeryev*` channels and the AMQP `celeryev` exchange is now the shape a Celery monitor parses —
  `uuid`, `name`, a **float** Unix `timestamp`, `hostname`, `pid`, `clock`, `utcoffset`, plus
  Celery's own names for the payload fields — instead of the previous serde projection of the typed
  `Event` enum (`{"type": "…", "timestamp": "<RFC 3339 string>", "task_id": …}`). A 0.3.0 consumer
  that parsed the old shape **must be updated**; a Python Celery monitor, `celery events` or Flower
  now works without one. The exact bytes are pinned by `celers_core::event::wire`'s
  `wire_json_is_byte_for_byte_stable` test — they are a published interface, so changing them again
  is a breaking change.
  The *receive* direction accepts both: CeleRS' event receivers parse the Celery shape (including
  events published by a real Python worker, whose sparser `task-retried` / `task-rejected` /
  post-`task-received` events fill `task_name`, `retries` and `reason` with documented defaults) and
  still parse the legacy CeleRS projection, so a mixed-version cluster on one channel does not lose
  events during a rolling upgrade
- **The queue cancel channel now carries JSON.** `<queue>:cancel` used to carry a bare task-id
  string; it now carries a `RevocationNotice` document, `{"task_id": "…", "terminate": <bool>}`,
  because a revocation has to say whether a *running* copy must be aborted (Celery's
  `revoke(id, terminate=True)`) or only a queued one refused (`revoke(id)`).
  `RevocationNotice::from_wire` accepts both forms — a bare id reads as `terminate: false`, which is
  the honest reading of what the old payload meant — so an old publisher still reaches a new worker.
  A *reader* that expected a bare id (a hand-rolled monitor, a `redis-cli SUBSCRIBE` script) must be
  updated
- `TaskEvent::Revoked` gained a `hostname` field, so a monitor can tell *which* worker revoked a
  task. `task-revoked` now carries the emitting worker's own hostname rather than falling back to
  the publisher hostname configured on the event emitter, matching Python Celery, which stamps the
  field on every worker-emitted event. Inbound events without `hostname` still parse (the field
  reads as empty) so a rolling upgrade is not lossy
- `celers_protocol::event::EventMessage::get_datetime()` now falls back to the Unix epoch for a
  non-finite or unrepresentable timestamp instead of `Utc::now()`. A corrupt timestamp used to read
  as "just now", which is indistinguishable from a healthy event; it now reads as obviously wrong

#### Source compatibility

- `TaskEvent` gained a `SoftTimeLimitExceeded` variant and **is not `#[non_exhaustive]`**: a
  downstream `match` over `TaskEvent` without a wildcard arm stops compiling until the arm is added
- `celers_core::Broker` gained four methods — `revoke(&TaskId, terminate)`, `is_revoked(&TaskId)`,
  `subscribe_revocations()` and `defer(task_id, receipt_handle, delay)`. All four have trait
  defaults (`revoke` forwards to `cancel`, `is_revoked` answers `false`, `subscribe_revocations`
  answers `None`, `defer` forwards to `reject(requeue = true)`), so an existing `impl Broker` keeps
  compiling — it just gets the inert behaviour until it overrides them

#### Defaults

- **Task coalescing now requires the same task id.** `WorkerConfig::coalesce_require_same_task_id`
  is new and defaults to `true`, which is the only lossless setting: coalescing then collapses
  genuine redeliveries of *one* task and nothing else. The previous behaviour — now reachable only
  by setting the flag to `false` — widened the coalescing key to `(task name, payload hash)`, so two
  *independent* submissions with identical arguments collapsed into one: the dropped submission
  never ran, never produced a result, and left its caller waiting forever on an `AsyncResult` that
  could never resolve. Turn it off only for idempotent, fire-and-forget work where nobody awaits the
  second submission

#### Manifests and features

- `rust-version` is now declared: **1.89** for the workspace (set by the `oxisql` 0.4.1 and `oxitls`
  0.3.0 chains) and **1.94.1** for `celers-broker-sqs`, whose AWS SDK dependencies are not optional
  — so `celers/sqs`, `celers/full` and any `--all-features` build require 1.94.1 too. The root
  manifest documents how to re-derive both numbers
- `celers` facade: new `canvas` and `workflows` features, forwarding `celers-worker/canvas` and
  `celers-worker/workflows`, both now part of `full`. Without them a facade user could build and
  apply a `Chord` but the worker they built had the barrier and chain continuation compiled out, so
  the callback never ran. `workflows` also pulls `backend-redis`, because the barrier is counted in
  a result backend; `backend-redis` additionally forwards `celers-canvas/backend-redis`. The
  README's install snippet — which advertised a `workflows` feature that did not exist and failed to
  build as printed — now matches the manifest
- `celers-broker-sqs`: new **default-on** `pure-http` feature selecting the crate's own
  `oxihttp-client`-backed AWS SDK transport. Building with `default-features = false` and nothing
  else leaves the SDK with no HTTP client at all — the deliberate escape hatch for a deployment that
  wants the stock (non-Pure-Rust) transport, which must then enable
  `aws-config/default-https-client` itself
- `celers-beat`: new off-by-default `redis-store` feature (`RedisScheduleStore`)
- `celers-worker`: the `redis` feature now also enables `oxitls`, mandatory rather than optional —
  no rustls provider *feature* is enabled anywhere in this workspace, so the bare
  `rustls::ClientConfig::builder()` the `redis` crate uses for `rediss://` panics unless a
  process-default provider was installed first
- Workspace `redis` is now built with `tokio-rustls-comp` + `tls-rustls-webpki-roots` (`rediss://`
  support); workspace `lapin` stays pinned to `default-features = false` +
  `rustls-webpki-roots-certs`. Both trust the Mozilla bundle rather than the OS store; a private CA
  is supplied per connection (`RedisConfig::tls(…)`) or via `celers-broker-amqp/tls-native-certs`
- `deny.toml` is committed at the workspace root and its `[graph] exclude` list is **empty** — no
  crate is hidden from `cargo deny check bans` any more

### Added

#### Remote worker control (`celers inspect` / `celers control`)

- A complete remote control protocol: `celers_core::control` defines the command vocabulary
  (`ControlCommand` / `ControlResponse`), `celers_core::control_transport` the broadcast-plus-reply
  wire framing (`ControlClient`, `ControlTransport`), and `celers_worker::control`'s `ControlService`
  the worker-side handler that dispatches each command into the running worker. A worker joins the
  channel with `Worker::with_control_transport`; the run loop starts the subscriber alongside the
  revocation watcher and stops it on shutdown. `celers_broker_redis::RedisControlTransport` is the
  Redis Pub/Sub implementation (RESP3 server pushes, so Redis 6.0+)
- `celers inspect` (read-only; 11 subcommands: `ping`, `active`, `scheduled`, `reserved`, `revoked`,
  `registered`, `stats`, `queues`, `report`, `conf`, `circuit-breakers`) and `celers control`
  (mutating; 10: `ping`, `shutdown`, `revoke`, `revoke-pattern`, `rate-limit`, `time-limit`,
  `add-consumer`, `cancel-consumer`, `queue-length`, `reset-circuit-breaker`). Both broadcast a
  request and print every reply that arrives before a timeout, so "no worker answered" means nothing
  was listening rather than that the command failed. There is no separate daemon to run.
  This is a CeleRS-native protocol: it does **not** interoperate with `celery -A app inspect`
  against a Python worker — there is no kombu pidbox codec yet (see `TODO.md`, "Known gaps")
- `RateLimit` installs a per-task-name token bucket consulted before every dispatch; `TimeLimit`
  sets the soft/hard limits the worker resolves per task; `AddConsumer` / `CancelConsumer` resume and
  suspend consumption of the worker's queue; `ResetCircuitBreaker` closes one or every per-task-type
  breaker. `Inspect(Scheduled)` and `Inspect(Reserved)` deliberately answer empty and say why in
  their module docs rather than inventing a number

#### Broker-fed revocation

- Revocations now live in the broker, not only in the worker that received them.
  `Broker::revoke(task_id, terminate)` records the revocation durably **and** publishes a live
  notice; `Broker::is_revoked` is the dequeue-time lookup; `Broker::subscribe_revocations` is the
  live feed a running task is aborted from. The two halves cover different failures and neither is
  redundant: the persisted set survives a worker restart and refuses a task that was revoked while
  it sat in the queue; the Pub/Sub feed is what makes an in-flight abort prompt
- `celers-broker-redis` implements all three: a durable `<queue>:revoked` sorted set scored by
  expiry, checked inside the dequeue Lua scripts (`POP_TO_UNACKED` / `POP_BATCH_TO_UNACKED` drop a
  revoked message and charge it an attempt), plus the `<queue>:cancel` subscription.
  `celers-broker-postgres` and `celers-broker-sql` persist revocations in new tables (migrations
  `008_revocation.sql` and `011_revocation.sql`). `InMemoryBroker` implements the same contract
  in-process. AMQP and SQS keep the inert trait defaults
- `Worker::with_broker_revocation` opts a worker into the dequeue-time check (one lookup per
  message, which is why it is opt-in); `celers worker` turns it on. `celers control revoke <id>`
  therefore also blocks a task that is still queued, or one whose worker has not started yet
- `RedisBroker::queue_names()` deliberately does not list `<queue>:revoked`, so `purge_all_queues()`
  leaves recorded revocations in place — purging messages must not un-revoke anything

#### Celery-compatible event wire

- `celers_core::event::wire` renders and parses the Celery event shape (`Event::to_wire_value` /
  `to_wire_json` / `from_wire_value` / `from_wire_json`, `EventEnvelope`), over the lossless typed
  `Event` ⇄ `celers_protocol::event::EventMessage` conversion in `celers_core::event::message`
- Celery's Lamport clock on both halves: `forward_event_clock` (one tick per emitted event) and
  `adjust_event_clock` (folding a remote worker's clock into your own), so a monitor can order
  events whose wall-clock timestamps are unreliable
- `utcoffset` is always `0` because CeleRS always timestamps in UTC;
  `EventEnvelope::with_utcoffset` exists for bridges re-publishing events captured from a non-UTC
  producer
- `TaskEvent::SoftTimeLimitExceeded` (`task-soft-time-limit-exceeded`) — a CeleRS extension for the
  moment Celery would raise `SoftTimeLimitExceeded` inside the task

#### Task security, wired into the worker

- Opt-in message authentication on the receive path (`celers_worker::security`): set
  `WorkerConfig::signature_verification` and every dequeued message is verified **before dispatch**
  — before the revocation registry, the poison-pill strike table, routing, or any other admission
  decision — so an unauthenticated message can never seed worker-local state keyed on its own task
  id or name. A message that fails verification is not executed and not requeued: it is recorded in
  the DLQ with `failure_type = "signature_verification"` if one is configured, dropped otherwise;
  either way a `task-rejected` event is emitted and `WorkerStats::signature_rejected` counts it. A
  migration mode admits unsigned messages while still rejecting forged ones
- The producer side is `celers_core::task_security::sign_task`, and the two sides share the field
  projection in `signed_fields`, so what the producer signed is exactly what the worker checks.
  `TaskMetadata` gained `signature: Option<SignatureEnvelope>` (serde-defaulted, so old payloads
  still parse). The worker re-signs its own retry attempts and workflow continuations
- Payload hygiene: `celers inspect active` reports a redacted argument preview rather than raw
  payloads
- All of it is **off by default**: a worker built from `WorkerConfig::default()` verifies no
  signatures and redacts nothing, exactly as before

#### Time limits

- Soft and hard per-task time limits, resolvable per task and settable at runtime over the control
  channel (`celers control time-limit`). The soft limit is cooperative —
  `execution_context::check_soft_time_limit()` / `soft_time_limit_exceeded()` let a task body notice
  it and wind down, the Rust equivalent of Celery raising `SoftTimeLimitExceeded` inside the task —
  and emits `task-soft-time-limit-exceeded`; the hard limit terminates

#### Workflows that actually run

- Chord aggregation, saga compensation and conditional branches are executed end to end by the
  worker, not merely built by the canvas crate. `celers_canvas::Saga`, `Pipeline`, `FanIn`, `FanOut`
  and `ScatterGather` *lower* onto the executable primitives (a chain, a group, a chord, a rollback
  route), and `celers_worker::workflows::patterns_e2e` proves the lowered graph runs: a four-step
  saga rolls its two completed steps back in reverse when the third fails, a chord's aggregator
  fires exactly once with every member's result, a fan-out's consumers all get their message
- `celers_canvas::Branch` / `Switch` conditional routing is executed by the worker's error-route and
  continuation machinery (`celers_worker::error_links`), covered by the new `workflow_semantics`
  integration tests in both `celers-worker` and the `celers` facade: a true condition takes the
  success arm only, a false one the failure arm only, a switch picks exactly one case, and a chord
  over chains runs its aggregate once after every chain finishes
- `SagaIsolation` is documented as advisory metadata, not an enforced guarantee — a task queue has
  no rollback segment, so isolation belongs in the steps themselves

#### Broker and backend hardening

- `Broker::defer(task_id, receipt_handle, delay)` — a retry-neutral way to return a delivered
  message to the queue. `reject(requeue = true)` means "this task ran and failed", and the Redis
  broker implements it that way (it rewrites the payload to `Retrying(n + 1)`); an admission miss
  (wrong worker, unmet affinity, a disabled feature flag, a saturated rate limiter, a half-open
  circuit's spent probe budget, a draining worker) means "this task never ran", and now goes through
  `defer` instead. Previously such a task was charged a retry every time it visited a worker that
  could not serve it, and was eventually dead-lettered without ever executing
- Redis `defer` splits on the delay: under one second the message goes straight back to the ready
  queue (unchanged bytes, so another worker can take it at once), one second or more into the
  delayed sorted set via the new `DEFER_UNACKED` Lua script (`SCRIPT_VERSION` is now `3`).
  `InMemoryBroker` honours the delay through its scheduled set
- `rediss://` support in `celers-broker-redis`, `celers-backend-redis` and `celers-worker`, each
  with an `install_pure_tls_provider()` that installs OxiTLS' Pure-Rust `rustls-rustcrypto` provider
  as the process default before any `redis::Client` is opened. Custom CA and client certificates go
  through `RedisConfig::tls(TlsConfig::new().ca_cert(…).client_cert(…, …))`
- `celers-beat`: `ScheduleStore`, a pluggable byte-oriented backend for scheduler state
  (`load`/`save`/`remove`). `FileScheduleStore` is the previous file-based behaviour behind the
  trait — nothing changes for a caller who never touches the module — and `RedisScheduleStore`
  (feature `redis-store`) gives several beat instances a shared, durable schedule catalog. The
  module documents what it deliberately does *not* solve: `save` is last-write-wins, so per-fire
  duplicate-dispatch prevention remains `dispatch_lock`'s job and a single active writer is still
  the recommended deployment
- `impl From<CancellationError> for CelersError`, mapping onto `CelersError::Cancelled`. A
  cooperative task body can now write `execution_context::check_cancelled()?` directly — which its
  documentation had always promised — and the resulting error is recognisable downstream
  (`is_cancelled()`, not retryable) instead of an opaque string

#### CLI

- `celers worker` performs a real startup connectivity probe: a bounded exponential-backoff retry
  loop that issues an actual Redis `PING` (and a control-channel subscribe) before reporting
  success. `--broker-connect-timeout` sets the budget and `--no-connect-check` skips the probe for
  an operator who has already verified connectivity another way. Previously the command reported a
  successful broker connection after zero network I/O
- `celers worker --demo-tasks` registers three harmless built-ins (`demo.echo`, `demo.sleep`,
  `demo.fail`) so a fresh deployment can be smoke-tested end to end — enqueue, dequeue, execute,
  ack/DLQ — before any application task code exists. Without it the command now prints an explicit
  warning that its registry is empty and points at the README, instead of starting silently and
  letting tasks accumulate unexecuted
- `celers config validate` (and `celers worker` startup) now validate the broker URL — non-empty,
  a real `scheme://` separator, and a scheme among the known broker schemes — instead of passing any
  string clean and failing later, mid-connection

#### Testing and proof

- **`tests/python-compat`**: a live interoperability suite running a real Python Celery client and a
  real `celery -A tasks worker` against a real Redis, with the CeleRS side going through
  `celers-protocol`'s own public API via the new `crates/celers-protocol/examples/celery_bridge.rs`.
  It covers both directions (Python publishes → CeleRS consumes and answers; CeleRS publishes → a
  real worker executes) and skips visibly without `CELERS_TEST_REDIS_URL` / `CELERS_PYTHON`.
  Reachable from cargo as `cargo test -p celers-protocol --test python_interop`
- **Verbatim Celery wire captures** in `crates/celers-protocol/tests/fixtures/`, recorded from
  Celery 5.6.3 / kombu 5.6.2 / Python 3.14.6 by `tests/python-compat/capture_fixtures.py` with only
  four environment-specific substrings normalised. `tests/celery_golden.rs` checks this crate
  against them with no services required. The canonical CeleRS envelope fixture
  (`celers_envelope_accepted_by_celery.json`) is recorded **only after a real Celery worker executed
  it**, so it is no longer generated by the code it validates
- Property-based round-trip tests for `celers-protocol` (`tests/proptest_roundtrip.rs`)
- A `trybuild` compile-fail UI harness for `celers-macros` (`tests/ui_compile_fail.rs`), plus
  validator suites (`validators_ids.rs`, `validators_geo.rs`, `validators_practical.rs`).
  `celers-macros` now dev-depends on `celers-core`, so generated code is checked against the real
  `Task` / `CelersError` types instead of a hand-rolled mirror that could drift
- Byte-exact event-wire golden tests in both directions (`celers-backend-redis`'s
  `tests_event_wire.rs`), broker hardening suites for AMQP, MySQL and PostgreSQL, an in-crate
  LocalStack suite for SQS, and `security_wiring` / `workflow_semantics` integration tests for the
  worker and the facade
- `docker-compose.yml` gained `mysql` and `localstack` behind a `test` profile and `python-celery`
  behind a `python-compat` profile, so every env-gated suite has a service it can run against.
  `tests/integration/README.md` maps each gate variable to its service and its exact invocation, and
  explains how to tell a real run from a silent skip

### Changed

- Dependency upgrades: `oxisql-core` / `-postgres` / `-mysql` 0.3.2→**0.4.1** (which, with `oxitls`,
  sets the declared 1.89 MSRV), `oxitls` 0.2.0→**0.3.0**, `oxicode` 0.2.4→**0.2.6**,
  `oxihttp-client` 0.2.0→**0.2.1** (plus a new direct `oxihttp-core` 0.2.1 for the request-body
  type the SQS transport hands to it), `oxiarc-deflate` / `-zstd` / `-archive` 0.3.5→**0.4.1**,
  `redis` 1.3.0→**1.6.0** (with `tokio-rustls-comp`
  + `tls-rustls-webpki-roots` for `rediss://`), `tokio` 1.52.3→1.53.1, `serde` 1.0.228→1.0.229, and
  the AWS SDK crates (`aws-config` 1.11.0, `aws-sdk-sqs` 1.107.0, `aws-sdk-cloudwatch` 1.126.0), all
  now declared with `default-features = false` so `default-https-client` stays out of the graph.
  `aws-smithy-runtime-api` and `aws-smithy-types` are new workspace dependencies, used by
  `celers-broker-sqs`' Pure-Rust transport
- `celers-kombu` now depends on `tracing` and emits its diagnostics through it instead of writing
  straight to stderr; `celers-beat` no longer mixes `eprintln!` and `tracing` in the same function
- `celers-metrics`' trend and forecast alert tests inject deterministic timestamps through
  `record_batch` and assert the real verdict, instead of calling `should_alert` only to check that
  it does not panic. `tests_core.rs` was split at a `#[test]` boundary into `tests_core_extra.rs` to
  stay under the 2000-line-per-file convention
- The canvas and `celers-backend-redis` chord tests that were named for barrier races but exercised
  `tokio` and `AtomicUsize` (or set the counter by direct field assignment) are renamed for what
  they actually pin, and the increment path is now covered by real contention against the barrier
- `celers-beat`'s scheduler tests replace timing-dependent sleeps with deterministic
  next-occurrence assertions wherever the behaviour allows, and document the cases that genuinely
  need elapsed time
- `celers-worker/src/worker_core.rs` was split (`execution.rs`, `control_wiring.rs`,
  `broker_revocation.rs`, `runtime.rs`, `support.rs`) to stay under the 2000-line-per-file
  convention; `celers-beat`'s `schedule_store` and `celers-broker-redis`'s `defer` are likewise
  their own modules
- The Docker image no longer installs `ca-certificates`: on Debian bookworm it carries a hard
  `Depends: openssl`, which would put OpenSSL into a Pure-Rust image through the OS package manager.
  `celers-cli` resolves TLS through the compiled-in `webpki-roots` bundle and never reads
  `/etc/ssl/certs`

### Fixed

- **Solar schedules now fire at all.** `Schedule::Solar::next_run` previously returned
  `Err(ScheduleError::Invalid)` for *every* input, so a solar beat entry never ran: the branch
  called the deprecated `sunrise::sunrise_sunset`, whose `(i64, i64)` return is a pair of Unix
  **seconds** timestamps, and divided each as "minutes since midnight" — producing an hour count in
  the tens of millions that `NaiveDate::and_hms_opt` rejected on the first loop iteration. The
  branch now resolves events through `sunrise`'s `SolarDay::event_time`, which returns an absolute
  `DateTime<Utc>` and needs no unit conversion. Three further defects went with it:
  civil/nautical/astronomical twilight were approximated as flat ±30/60/90-minute offsets from
  sunrise/sunset and are now true 6°/12°/18° elevation solves; polar day and polar night (where the
  event genuinely does not occur) are skipped rather than treated as errors, so a Svalbard sunrise
  schedule resolves to the first sunrise after the midnight sun ends; and out-of-range coordinates
  now return `ScheduleError::Invalid` instead of panicking inside the `sunrise` crate and taking the
  beat process down. The search also starts a day earlier, because the event for a given *local*
  date routinely falls on a neighbouring UTC date. `test_solar_schedule_{sunrise,sunset}` are
  un-`#[ignore]`d and assert real almanac instants (Tokyo's 2026 solstice sunrise is
  `2026-06-21T19:25:59Z` = 04:25 JST), alongside new tests for negative longitudes, twilight
  ordering, polar day and invalid coordinates

- A task that reports its *own* cancellation — `Err(CelersError::Cancelled)`, which is what
  `check_cancelled()?` now produces — is disposed of as revoked (acked, no DLQ entry, `task-revoked`
  with `terminated: false`) instead of being read as an execution failure and re-dispatched up to
  `max_retries` times. `CelersError::TaskRevoked` returned from a handler is terminal for the same
  reason. Previously the polite form of cancellation was punished while the abrupt form (the
  revocation watcher aborting the future) was already terminal
- Redis `defer` re-adds a message only if it was still in flight, so a deferral that loses a race to
  the reaper, an `ack` or a revocation cannot duplicate the message
- `create_python_celery_message` no longer writes Rust `Debug` formatting into the Celery
  `argsrepr` / `kwargsrepr` headers. Both are now rendered the way `celery.utils.saferepr` renders
  them — Python literals (`True`, `None`, tuple versus list parentheses) with the same third-level
  container elision — checked against a recorded `celery.utils.saferepr` table in both Rust
  (`celery_golden.rs`) and Python (`tests/python-compat/test_reprs.py`)
- The Docker image builds again: the builder stage was pinned to `rust:1.75`, far below the
  dependency MSRV, and its `COPY --from=builder` named `target/release/celers-cli` — a binary the
  build never produces, because `celers-cli`'s `[[bin]]` is named `celers`
- The docker-compose monitoring stack no longer mounts nonexistent paths: `docs/prometheus.yml`,
  `docs/grafana/datasources/` and `docs/grafana/dashboards/` (with a real `celers-overview.json`)
  are committed, and `docs/DEPLOYMENT.md` points at what exists

### Security

- Message authentication is now enforceable end to end: HMAC-SHA256 task signatures are verified on
  the worker receive path before any admission decision, and the worker re-signs the messages it
  produces itself (retries, workflow continuations) — see Added → Task security. Freshness and
  replay-guard settings are documented as being at odds with at-least-once redelivery, and
  `ReplayGuard` remains per-process by construction; a deployment needing global single-use-nonce
  semantics must back it with shared storage
- The whole workspace is now Pure Rust — no C, C++, Fortran or vendored assembly in a default build,
  with `--all-features`, or under `celers/full` — and `deny.toml`'s `[graph] exclude` is empty, so
  `cargo deny check bans` checks every member. The last exception, `celers-broker-sqs`, was closed
  by `celers_broker_sqs::pure_http`: an AWS SDK `HttpClient` implemented over `oxihttp-client`
  (hyper 1.x + `tokio-rustls` + OxiTLS' `rustls-rustcrypto` provider, webpki roots) and installed at
  every `aws_config::defaults(…)`, replacing the SDK's `default-https-client` →
  `aws-smithy-http-client/rustls-aws-lc` → `aws-lc-sys` chain. Removing that dependency also removed
  the ~12 s `rustls_native_certs::load_native_certs()` walk it performed at client construction on
  macOS
- `celers-broker-amqp` is Pure Rust for the same reason at the AMQP layer: `lapin` is pinned to
  `default-features = false` + `rustls-webpki-roots-certs`, and `install_pure_tls_provider()`
  supplies the crypto provider. Deployments behind a private CA can enable the (also Pure Rust)
  `tls-native-certs` feature
- Verify both claims yourself with `cargo tree -e features -i aws-lc-sys --all-features` (must
  report "did not match any packages") and `cargo deny check bans`

## [0.3.0] - 2026-07-12

### Added

#### Canvas & Worker
- Real nested chain/group execution for chords inside Canvas elements — the chord callback is now
  enqueued (with header tasks) instead of being silently dropped when a chord is nested in a chain
  or group
- Chord result aggregation in the worker: the chord callback now receives the ordered list of
  header-task results (Celery semantics) via `ResultBackend::chord_get_partial_results`, with `null`
  substituted for missing/failed results, instead of empty arguments
- Chain continuation: a task carrying `on_success_link` metadata now enqueues the next chain step
  with the completed task's result bytes as payload on success, driven off real `SerializedTask`
  metadata instead of being a documented no-op
- `WorkerPool` executes real submitted work: a `WorkerTaskFn` job channel + `submit_task` (with
  work-stealing across idle workers) replaces the previous simulated `sleep`-only worker loop, and
  `set_queue_depth` / `queue_depth_handle` feed real queue-depth + CPU/memory signal into
  `LoadBased` / `QueueBased` autoscaling decisions (previously discarded as unused parameters)
- Real Linux NUMA topology + `cpulist` parsing (`/sys/devices/system/node`) for CPU affinity,
  replacing a fixed/simplified topology

#### Protocol
- Message creation timestamp (`MessageHeaders.created_at`, serde-backward-compatible) with
  `Message::created_at()` accessor and `MessageExt::get_age_seconds()` for message-age tracking
- Real protocol v2↔v5 migration: version stamping in message headers, AMQP priority mirroring on
  upgrade to v5, non-destructive legacy field mirroring on downgrade to v2, and feature-aware strict
  compatibility checks
- Protocol version negotiation (`negotiate_version`, supported-version advertising, header
  encode/parse) and a native Celery protocol v5 wire-format builder (`build_v5_message`,
  `to_v5_wire`), additive to the existing message API
- YAML serializer (`application/x-yaml`) wired into the content-type registry + auto-detection, and a
  `CustomSerializer` trait + `CustomSerializerRegistry` for user-registered serializers (Pickle
  intentionally omitted as a remote-code-execution risk)

#### Beat Scheduler
- Real webhook alert delivery over HTTP (originally via `reqwest`, migrated to `oxihttp-client` later
  in this same release — see Changed → Pure-Rust Migration), dispatched asynchronously from the sync
  alert callback (guarded by the current Tokio runtime, with custom-header support)
- Holiday calendar (`WorkingCalendar`) and business-day arithmetic (is/next/previous business day,
  add N business days, business-days-between) honoring weekends + holidays
- Schedule conflict detection (same-instant collisions within a lookahead window) and missed-task
  catch-up logic with `CatchupPolicy` (FireAll / FireLatestOnly / Skip), built on a shared
  next-occurrence enumeration primitive
- Timezone-aware schedules (`Schedule::with_timezone`, `next_run_in_tz`) honoring local wall-clock +
  DST, plus runtime dynamic schedule updates via a thread-safe `ScheduleRegistry` (add/remove/update)
- Deterministic per-entry schedule jitter (hash-based thundering-herd mitigation) and per-fire
  dispatch locking over `DistributedLockBackend` to prevent duplicate execution across beat instances

#### Canvas
- Workflow DAG visualization export: `DagVisualize::to_mermaid()` / `to_dot()` for Chain, Group,
  Chord, Map, Starmap, Chunks, Branch, Switch and nested chain/group elements, with deterministic
  node ids, correct fan-out/fan-in/chord edges, and label escaping
- Workflow loops/iteration (`WorkflowLoop`, `WorkflowMap`), sub-workflow composition (`SubWorkflow`,
  `WorkflowComposition`), parameterized templates (`ParamTemplate`), and runtime
  add/insert/remove/replace/move on Chain/Group members
- Workflow versioning + migration: versioned serialization (`to_versioned_json` /
  `from_versioned_json`) with a `MigrationRegistry` that upgrades older payloads on load
- Rate-limit integration for workflows: `Group::with_rate_limit` / `rate_limited_countdowns` derive
  per-member staggered dispatch countdowns from a `RateLimitConfig` (token-bucket spacing)

#### Core
- Distributed rate limiting across workers: `DistributedRateLimitBackend` trait + in-memory backend
  + `DistributedRateLimiter` reusing the token-bucket / sliding-window algorithms (Redis-ready)
- Event snapshots (`EventSnapshot`) and an alerting rule engine (`AlertRule` / `AlertEvaluator` for
  failure-rate, queue-depth, no-heartbeat with hysteresis + cooldown)
- Result tombstones (`ResultExistence` / `TombstoneRegistry`, distinguishing deleted vs absent),
  result groups (`ResultGroup` readiness + success/failure rollup + ordered values), and per-task-type
  result TTL (`ResultTtlConfig`) — added as defaulted `ResultStore` methods (downstream-compatible)
- Task security: HMAC-SHA256 task signature signing/verification (native SHA-256/HMAC, RFC 4231
  vectors), configurable argument sanitization (size/kind limits, control-char stripping, secret
  redaction), and PII detection + masking (email / phone / Luhn-checked card / SSN)
- Local development mode: `InMemoryBroker` + `InMemoryResultBackend` (full trait coverage, no
  external services) for local dev and testing, plus a `CachingResultBackend<B>` LRU wrapper (bounded
  capacity + per-entry TTL) fronting any result store
- Per-task-type circuit breakers (`TaskTypeCircuitBreakers`) and per-tenant rate limiting
  (`TenantRateLimiter`, optional per-tenant quota), composing the existing breaker / token-bucket
  primitives

#### Worker
- Poison-pill detection & quarantine (`PoisonPillDetector` — per-task failure/redelivery threshold
  with decay window) and self-healing worker restarts (`SelfHealingSupervisor` — exponential backoff
  with a max-restart circuit breaker)
- Cooperative task cancellation during execution (a cancellation token threaded into the task
  context, tripped by the broker revocation signal → task transitions to Revoked) and worker-side
  distributed rate-limit coordination gating execution via celers-core's `DistributedRateLimiter`
- Adaptive polling intervals (`AdaptivePoll` — backs off when idle, speeds up under load) and task
  batching + coalescing (`BatchAccumulator`, dedup by coalescing key, acks dropped duplicates)
- Task affinity / worker-to-task matching (`AffinityRegistry` — required / preferred / anti labels
  with deterministic scoring; unservable tasks are deferred)

#### CLI
- Layered configuration: CLI-argument overrides, TOML **and** YAML config-file loading, and runtime
  config reload (`ReloadableConfig`) with precedence args > env > file > defaults, plus a `config`
  subcommand (show / reload)
- `metrics` and `monitor` commands: scrape a Prometheus endpoint over HTTP, parse the exposition
  format natively, and render metrics as colored tables / a live top-style dashboard
- `replay` command: re-enqueue failed / DLQ tasks by id, glob pattern, or all, with `--limit` and
  `--dry-run` (pure, tested selection/planning logic)
- `loadtest` / `simulate` command: generate synthetic task load at a target rate/duration with
  constant / seeded-jittered / poisson arrival patterns (deterministic, tested load planner)
- Connection pooling (`pool.rs`'s `ClientPool<T>` + `PoolStats`, tracking size/reuse/utilization) and
  TTL caching (`cache.rs`'s `TtlCache<K, V>` + `CacheStats`, monotonic-clock expiry) front the
  `queue`/`worker` read paths (list and stats lookups), with per-queue-type, per-worker, and
  per-task-location reads now issued concurrently (`futures::future::join_all` / `tokio::join!`)
  instead of sequentially
- `cache-stats` command prints the pool/cache layer's configured capacity and per-cache TTL/entry
  counts; `celers interactive` (the REPL) additionally gains a `stats`/`cs` command reporting live
  hit/reuse ratios, since those process-lifetime counters only accumulate meaningfully across a
  long-running session
- Structured logging (`logging.rs`): `--log-format text|json` selects human-readable or
  newline-delimited-JSON log output, and `--log-sink stdout|file:<path>|tcp:<host:port>` streams
  formatted log lines to a file or a TCP log collector instead of only stdout
- Smart defaults (`smart_defaults.rs`): broker URL auto-detection now also recognizes the
  `REDIS_URL` and `AMQP_URL` hosting-provider conventions (alongside the existing
  `CELERY_BROKER_URL`/`CELERS_BROKER_URL`) as a fallback in `Config::apply_env_overrides`, and
  `celers interactive` offers Levenshtein-based "did you mean" suggestions for unrecognized REPL
  commands and for `use <queue>` against a nonexistent queue name
- Structured CLI errors (`errors.rs`'s `CliError` enum): every command failure now prints a
  decorated `error[E_CODE]: <message>` line plus an actionable `suggestion:` line, classified from
  the failure via `errors::classify_anyhow`; a new `error-codes` command prints the full
  code/message/suggestion reference table
- User-defined command aliases (`aliases.rs`'s `AliasConfig`): `alias add <name> <expansion>` /
  `alias remove <name>` / `alias list` manage a `[aliases]` config-file table, and every invocation
  now expands a matching alias (`AliasConfig::resolve`) before argument parsing, preserving the
  binary name and any trailing arguments
- Incremental backup (`backup --previous <archive>` or `--since <RFC3339-timestamp>`) writes out
  only the queue/task/schedule entries that are new or changed relative to a prior backup or
  timestamp, and `restore --conflict-policy skip|overwrite|merge` chooses how pre-existing queue
  content at the restore target is resolved (default `skip`, the safest option)
- `deps` command: renders a task dependency graph from a queue-export JSON file as an ASCII tree or
  GraphViz DOT (`--format ascii|dot`), with an `--interactive` exploration session that can start
  from a given task id/name via `--start` (`depgraph.rs`)
- `init --wizard`: an interactive setup wizard (`wizard.rs`, built on `dialoguer` prompts) walks
  through broker selection, a live connection test, queue/worker configuration with validation and
  recommended defaults, auto-scaling/alert setup, and dev/staging/prod profile selection, then
  writes the assembled configuration
- `report daily`/`report weekly` gain `--format table|csv|html --output <path> --template <STRING>`
  output (previously table-only), joined by three new commands — `report history`, `report queues`,
  and `report workers` — covering task-execution history, per-queue, and per-worker metrics; HTML
  output across all five now embeds an inline SVG bar chart above the table
- New `analyze profile` command family: `profile task` and `profile resources` trend execution time
  and queue-depth/worker-count/DLQ-size/broker-memory over a configurable rolling window of days;
  `profile worker` ranks all workers live (or inspects one via `--worker-id`) — all three share
  `report`'s `--format table|csv|html --output <path>` options

#### Metrics
- Native Prometheus histograms (configurable cumulative buckets, `_bucket`/`_sum`/`_count`) and
  summaries with a self-implemented P² streaming quantile estimator (`_bucket`-free quantile output)
- StatsD metrics backend: pure-`std::net::UdpSocket` exporter with per-kind line formatting
  (counter / gauge / delta / timer / histogram / set), sample rates, DogStatsD tags, name
  sanitization, and packet batching
- SLA/SLO tracking & alerting (`SloTracker` — attainment, error-budget burn, breach alerts) and
  statistical anomaly detection (`AnomalyDetector` — online EWMA mean/variance z-score)
- Task lifecycle audit log (`AuditEntry` + ring-buffer and JSONL-file `AuditSink`s with query/filter)
- gRPC result backend client-side metrics: per-operation request/error counts and p50/p95/p99
  latency (`celers-backend-rpc::metrics`), and database analytics helpers (task success/failure
  rates, duration percentiles, per-worker throughput, storage sizing, chord completion rate) for
  both the Postgres and MySQL result backends (`celers-backend-db::analytics`)

#### Dependencies
- New `chacha20poly1305` and `twox-hash` workspace dependencies (see Security and Fixed)

### Changed

- Dependency upgrades: `tokio` 1.50→1.52, `redis` 1.1→1.3, `sqlx` 0.8→0.9 (now on
  `tls-rustls-ring` — `sqlx` itself was removed entirely later in this same release, see
  Pure-Rust Migration below), `lapin` 4.3→4.10, `aws-sdk-sqs`/`aws-sdk-cloudwatch`,
  `clap_mangen` 0.2→0.3, `hmac` 0.12→0.13, `sha2` 0.10→0.11, `aes-gcm` 0.10→0.11, `cron` 0.16→0.17,
  `oxiarc-deflate` / `oxiarc-zstd` / `oxiarc-archive` 0.2.6→0.3.5, `oxicode` 0.2→0.2.4, and
  `rustyline`, `tabled`, `ratatui`, `opentelemetry`/`opentelemetry_sdk`/`tracing-opentelemetry`
  minor bumps
- Continued `unwrap()`-removal sweep across `celers-beat`, `celers-broker-postgres`,
  `celers-broker-redis`, `celers-kombu`, and `celers-metrics`: lock acquisition now recovers from
  poisoned mutexes (`unwrap_or_else(|e| e.into_inner())`) instead of panicking, `SystemTime`
  arithmetic uses `expect("SystemTime should be after UNIX_EPOCH")` for a clearer panic message,
  and NaN-prone float comparisons in percentile calculation fall back to `Ordering::Equal`

#### Pure-Rust Migration
- `reqwest` → `oxihttp-client`: migrated in `celers-beat` (webhook alert delivery — see Beat
  Scheduler above), `celers-broker-amqp` (RabbitMQ management-API HTTP client),
  `celers-broker-redis` (DLQ archival HTTP client), and `celers-cli`
- `sqlx` → `oxisql-postgres` / `oxisql-mysql`: migrated in `celers-cli`, `celers-broker-postgres`,
  `celers-backend-db`, `celers-worker`, `celers-broker-sql`, and `celers-examples`. Zero `sqlx` or
  `reqwest` dependency declarations remain anywhere in the workspace after this migration
- Public API rename following from the `sqlx` migration: `pool()` getters renamed to
  `connection()` across `celers-broker-postgres` (`PostgresBroker::pool()` →
  `PostgresBroker::connection()`), `celers-broker-sql` (`MysqlBroker`'s equivalent getter), and
  `celers-backend-db` (3 separate getters, in `event_persistence.rs`, `lib.rs` — both a
  Postgres-backed and a MySQL-backed getter — and `lock.rs`). The rename reflects that these now
  wrap a single multiplexed connection rather than a real connection pool (see Deferred/Known
  Limitations below), not just a cosmetic change
- Real bugs found and fixed during the port (not just a mechanical swap): `oxisql-core` has no
  `ToSqlValue`/`FromValue` bridge for `uuid::Uuid`/`serde_json::Value` — every migrated call site
  now goes through `uuid_param`/`json_param`/`uuid_from_row`/`json_from_row` helpers (`row_ext.rs`,
  one per migrated crate) instead of a raw bind. `DateTime<Utc>` binding needed two different fixes
  per backend: PostgreSQL (`oxisql-postgres` sends parameters in binary wire format, so
  `.to_rfc3339()` text is bound via an explicit `$n::text::timestamptz` SQL-side cast) vs. MySQL
  (`mysql_async`/`mysql_common`'s strict `DATETIME` grammar requires
  `.format("%Y-%m-%d %H:%M:%S%.6f")`; RFC3339 fails outright there), with a regression test pinned
  to `mysql_common`'s actual accepted grammar
- Security regression caught and fixed during the port itself, before it shipped: the initial
  oxisql port hardcoded `TlsMode::Disabled` at every `PgConnection`/MySQL connect call site,
  silently downgrading to plain-text even when the caller's URL said `sslmode=require`. Fixed in
  all 5 migrated crates via a `tls_mode.rs` helper that parses `sslmode` from the connection URL
  and resolves the matching `TlsMode`

### Fixed

#### Security
- `celers-broker-redis` envelope encryption replaced a mock XOR "cipher" with genuine AES-256-GCM
  and ChaCha20-Poly1305 AEAD (`aes-gcm` / `chacha20poly1305`), wrapping the DEK with the KEK under
  AES-256-GCM, generating IVs from `OsRng`, and cryptographically verifying the authentication tag
  on decrypt (previously any ciphertext "decrypted" without any integrity check)
- SQL-injection hardening in `celers-broker-postgres` / `celers-broker-sql`: table names are now
  validated against `^[A-Za-z_][A-Za-z0-9_]*$` before interpolation (`validate_sql_identifier`),
  `apply_retention_policies` parses the task-state filter through a closed-vocabulary `DbTaskState`
  enum instead of splicing the raw string into the generated `WHERE` clause, `MysqlBroker::explain_query`
  rejects input that doesn't start with `SELECT` or that contains `;`, and the queue metadata filter
  in `queue_ops.rs` now binds both the JSON key and value as parameters instead of interpolating the
  key into the query text

#### Data Integrity
- `HashAlgorithm::XxHash` partitioning in `celers-broker-redis` now uses real xxHash64
  (`twox-hash`) instead of silently falling back to `DefaultHasher` (SipHash 1-3), so partition
  assignment now matches what any other real xxHash64 implementation computes
- `celers-metrics::estimate_costs()` now computes real data-transfer cost from a new
  `TOTAL_PAYLOAD_BYTES_PROCESSED` counter / `record_payload_bytes()` instead of a hardcoded `0.0`
  (so `CostConfig::cost_per_gb` is no longer dead configuration), and `MetricHistory` gained the
  `remove_samples_older_than()` method that retention pruning was already calling

#### Persistence
- PostgreSQL deduplication queries targeted a nonexistent `celers_task_deduplication` table (the
  real table is `celers_deduplication`) — every dedup lookup/insert/cleanup would have failed
  against a real database; added migration `006_deduplication_columns.sql` to reconcile the schema
  with the columns the Rust code already queried, including the unique index required by the
  `ON CONFLICT (idempotency_key, queue_name)` clause

#### Test Reliability
- Replaced flaky wall-clock micro-benchmark assertions in the `celers` facade tests with generous
  catastrophic-regression ceilings (real throughput is tracked by the Criterion benchmark suite);
  the previous tight bounds (e.g. < 50 ms for 10k task builds) failed intermittently under heavy
  concurrent build/CI load

#### Messaging Correctness
- Kombu `CompressionMiddleware` now records the codec in a `content-encoding` header on publish and
  actually decompresses on consume — previously compressed bodies were never restored (silent data
  corruption on the consumer side)
- Kombu `SigningMiddleware` now stores the HMAC signature on publish and verifies it on consume,
  rejecting tampered or unsigned messages
- Kombu `HealthCheckMiddleware` now performs a real (throttled) health evaluation and injects status
  + timestamp headers

#### Scheduling Correctness
- Beat crontab `day_of_week` now honors the documented Unix convention (0–6, 0 = Sunday) by
  translating to the `cron` crate's Quartz numbering (1–7, 1 = Sunday). Previously `"1-5"` fired
  Sun–Thu instead of Mon–Fri and `"0"` (Sunday) was rejected as out of range
- Redis broker `CronScheduler::calculate_next_run` now parses arbitrary 5-field cron expressions via
  the `cron` crate (with the same Unix→Quartz day-of-week translation) instead of matching five
  literal patterns and defaulting everything else to hourly

#### Dead Letter Queue
- Redis broker DLQ replay now uses an adaptive, failure-classified strategy (transient failures
  first, permanent failures skipped) extracted from the real recorded failure reason, instead of a
  single fixed strategy
- Redis broker DLQ analytics now classify errors and build error signatures from the real recorded
  task failure message instead of a simplified placeholder

#### CLI
- A `Config` struct literal missing the new `aliases` field in `tests/proptest_cli.rs` and
  `benches/serialization.rs` failed `cargo build --all-targets` outright (E0063) once the alias
  work above landed; both call sites now populate it. Also closed 14 pre-existing `cargo doc`
  intra-doc-link warnings, unrelated to the alias work, scattered across `cache.rs`, `pool.rs`,
  `cli/mod.rs`, `cli/dispatch.rs`, `commands/wizard.rs`, and the monitoring `profile.rs`/`report.rs`
  modules
- Removed ~1700 lines of duplicate/superseded code with zero behavior change: a stale top-level
  `database.rs` superseded by `commands/database.rs`, a validator-function quintet duplicated in
  `commands/utils.rs`, and non-formatted `report`/`backup` functions superseded by their
  `--format`-aware / policy-based replacements

### Security

- Narrowed (not eliminated) the 3 `rustls-webpki` 0.101.7 RUSTSEC advisories previously found via
  the old `sqlx`/`reqwest` `ring`/`aws-lc-sys` dependency chain (RUSTSEC-2026-0098/0099/0104: a
  reachable panic in CRL parsing plus 2 certificate name-constraint validation bypasses). Following
  the Pure-Rust Migration above, `cargo tree -i ring` / `-i aws-lc-sys` now shows exactly two
  remaining chains: `celers-broker-amqp` (via `lapin`) and `celers-broker-sqs` (via the AWS SDK:
  `aws-config`/`aws-sdk-sqs`/`aws-sdk-cloudwatch`) — `sqlx`'s own independent TLS edge that used to
  also contribute to this advisory is gone entirely along with the crate itself, and the shipped
  `celers-cli` binary built with default features is now fully free of `ring`/`aws-lc-sys`. This
  advisory is still present in the codebase's full dependency tree whenever the non-default
  `celers-broker-amqp`/`celers-broker-sqs` code paths are built — it is scoped narrower, not fixed
- Known caveat (not introduced or fixed by this release): the Pure-Rust replacement crypto
  provider, `rustls-rustcrypto` 0.0.2-alpha — pulled in transitively by `oxitls`/`oxisql-postgres`/
  `oxisql-mysql` as the `CryptoProvider` for the migration above — itself carries its own
  newly-tracked RUSTSEC advisories and is marked alpha/not-production-ready upstream. This is a
  COOLJAPAN-ecosystem gap being tracked separately, not a regression caused by this release's code
- Known caveat (found during this release's pre-publish `cargo audit` pass): the proc-macro helper
  crate, `proc-macro-error2` 2.0.1 — pulled in transitively via `tabled_derive` → `tabled` →
  `celers-cli` (which uses `tabled` for its report/error-codes/cache-stats table rendering) — is
  flagged unmaintained upstream (RUSTSEC-2026-0173). This is a maintenance-status advisory rather
  than an active vulnerability (no CVE, no known exploit), and it is accepted for this release
  rather than swapping `tabled` for an alternative table-rendering crate, since that would touch
  every table-rendering call site in `celers-cli` — recently and heavily modified this same release
  cycle — shortly before publish. Tracked as a candidate for a future dependency swap, similar in
  spirit to how the `rustls-rustcrypto` caveat above is tracked

### Known Limitations

- `lapin` (`celers-broker-amqp`) and the AWS SDK (`celers-broker-sqs`) remain on
  `ring`/`aws-lc-sys` pending upstream work — no drop-in Pure-Rust AMQP client or AWS SDK exists yet
  in the COOLJAPAN ecosystem
- PostgreSQL connection handling changed from a real N-connection pool (`sqlx::PgPool`) to a single
  multiplexed connection (`oxisql_postgres::PgConnection`, `Clone` but internally one shared
  `Arc<Mutex<tokio_postgres::Client>>`) — functional but lower-concurrency, tracked as a perf
  follow-up rather than fixed in this release

Final verified state for the full 0.3.0 release (QA/hardening pass plus the Pure-Rust migration
above): workspace builds and `clippy -D warnings --all-targets` clean, **5278 tests pass** (up from
5068 pre-migration), **1030 doc tests pass**, **0 failures**. These counts predate the celers-cli
work captured above (pooling/caching, structured logging, smart defaults, structured errors,
aliases, incremental backup, `deps`, the setup wizard, and the new report/profile commands) —
celers-cli test count pending final release-check re-verification before publish.

## [0.2.0] - 2026-03-28

### Added

#### Event Persistence
- File-based event storage with JSONL format and automatic log rotation for audit trails
- Database-backed event persistence for durable, queryable event history
- Event filtering and routing system with topic-based subscription and pattern matching
- AMQP event transport using fanout exchange for real-time event broadcasting

#### Result Chunking
- Auto-split large task results across multiple Redis keys to bypass size limits
- CRC32 checksum verification for chunked result integrity
- Transparent reassembly on result retrieval with configurable chunk size thresholds

#### Beat Heartbeat and Failover
- Leader election for beat scheduler instances using distributed locks
- Lease renewal with configurable heartbeat intervals
- Automatic standby failover when the active leader becomes unresponsive
- Distributed beat locks with Redis and database backends for single-leader scheduling

#### AMQP Topic Routing
- Glob pattern matching for task name to routing key mapping
- Wildcard-based topic exchange routing for flexible task distribution
- Configurable routing rules per task type with fallback defaults

#### Enhanced Configuration
- Support for 23+ `CELERY_*` environment variables for runtime configuration
- `validate_detailed()` method for comprehensive configuration validation with diagnostics
- Configuration export to TOML/JSON for reproducible deployments
- Centralized `celers-core::config` module for unified configuration management

#### Protocol and Serialization
- Serialization auto-detection for incoming messages (JSON, MessagePack, YAML, BSON)
- Dedicated `celers-protocol::serializer` module extracted for cleaner separation
- Compression type unification across crates (unified `CompressionType` enum shared by all broker and backend crates)

#### Other Additions
- Per-task TTL configuration and metadata storage in result backends
- Zstd compression support across all broker and backend crates via OxiARC

### Changed

- Massive codebase refactoring: split all source files to under 2000 lines each (552 Rust files, 198K SLoC total)
- Extracted large modules into focused sub-modules across all 18 workspace crates (net reduction of ~115K lines through deduplication and reorganization)
- Replaced compression backends with OxiARC (oxiarc-*) for Pure Rust compliance across celers-protocol, celers-broker-amqp, celers-broker-redis, and celers-backend-redis
- Upgraded dependencies: lapin 4.3, rand 0.10, sha2/hmac version alignment, OxiARC integration
- Moved CLI commands module out of monolithic file into dedicated sub-modules in celers-cli
- Reorganized celers-kombu, celers-canvas, celers-metrics, and celers-macros internals for maintainability
- Test suite expanded to 4075 tests (up from 3979 in 0.1.0)

### Fixed

- Compression round-trip correctness across all broker and backend crates after OxiARC migration
- Result backend key handling for large payloads that previously exceeded single-key Redis limits
- Beat scheduler stability under concurrent leader election scenarios
- Configuration validation edge cases for environment variable overrides

## [0.1.0] - 2026-01-18

### Added

#### Core & Protocol Layer
- Add `celers-core` with core traits (`Task`, `Broker`, `ResultBackend`, `TaskExecutor`)
- Add `celers-protocol` with Celery Protocol v2/v5 message format compatibility
- Add `celers-kombu` for Kombu-compatible messaging abstraction
- Add `celers-macros` with `#[celers::task]` procedural macro for automatic task registration
- Add `celers` facade crate with unified API

#### Broker Layer (5 Implementations)
- Add `celers-broker-redis` with Lua scripts, pipelining, and Redis Streams support
- Add `celers-broker-postgres` with PostgreSQL and `FOR UPDATE SKIP LOCKED` optimization
- Add `celers-broker-sql` with MySQL/SQLite support and batch operations
- Add `celers-broker-amqp` with RabbitMQ/AMQP exchanges and routing
- Add `celers-broker-sqs` with AWS SQS cloud-native integration

#### Result Backend Layer (3 Implementations)
- Add `celers-backend-redis` with fast in-memory storage and automatic TTL
- Add `celers-backend-db` with PostgreSQL/MySQL durability and SQL analytics
- Add `celers-backend-rpc` with gRPC-based result storage for microservices
- Add chord support with distributed barrier synchronization across all backends

#### Runtime & Workflow Layer
- Add `celers-worker` with task execution, graceful shutdown, and concurrency control
- Add `celers-canvas` with workflow primitives (Chain, Group, Chord, Map, Starmap)
- Add `celers-beat` with periodic task scheduler using cron expressions and solar schedules

#### Utilities & Tooling
- Add `celers-cli` with worker management, queue inspection, and DLQ operations
- Add `celers-metrics` with Prometheus metrics integration (throughput, latency, queue depth)

#### Core Features
- Add type-safe task definitions with compile-time signature verification
- Add priority queues with multi-level task prioritization
- Add dead letter queue (DLQ) with automatic handling of permanently failed tasks
- Add task cancellation via Pub/Sub for in-flight tasks
- Add retry logic with exponential backoff and configurable max retries
- Add timeout enforcement at task and worker levels
- Add graceful shutdown with in-flight task completion
- Add health checks for Kubernetes liveness/readiness probes

#### Observability
- Add OpenTelemetry integration for distributed tracing
- Add structured logging with context propagation
- Add performance profiling and resource tracking
- Add Grafana dashboard templates

#### Interoperability
- Add binary-level Celery protocol compatibility
- Add support for interoperation with Python Celery workers
- Add message format negotiation (v2/v5)
- Add compression support (zlib, zstd, gzip)
- Add encryption support for sensitive task payloads

#### Advanced Features
- Add circuit breaker pattern for fault tolerance
- Add bulkhead isolation for resource management
- Add rate limiting with token bucket algorithm
- Add quota management for multi-tenant scenarios
- Add distributed locks with Redis-based implementation
- Add task groups for coordinated execution
- Add backup/restore utilities for broker state
- Add monitoring dashboards with real-time queue metrics

### Changed
- Replace all production `unwrap()` calls with proper error handling using `expect()` (120+ occurrences)
- Update telemetry ID generation to use randomness for uniqueness guarantees
- Improve error messages with descriptive context throughout the codebase

### Fixed
- Fix telemetry span ID and trace ID generation to ensure uniqueness
- Fix workspace configuration to match actual crate structure
- Correct Cargo.toml metadata for all 18 workspace crates

### Security
- Eliminate all `unwrap()` usage in production code following "No unwrap policy"
- Add encryption support for task payloads using AES-GCM
- Add HMAC-based message integrity verification
- Add secure credential handling for broker connections

## Project Information

### Workspace Structure
- Published crates: 18, plus 2 unpublished members (`celers-examples`, `celers-facade-test`)
- Facade: 1 (celers)
- Core/Protocol: 4 (celers-core, celers-protocol, celers-kombu, celers-macros)
- Brokers: 5 crates — 3 implement `celers_core::Broker` and can back a worker (Redis, PostgreSQL,
  SQL/MySQL); 2 are `celers-kombu` transports only, with no `celers_core::Broker` adapter yet
  (AMQP, SQS)
- Result Backends: 3 (Redis, Database, RPC)
- Runtime: 3 (worker, canvas, beat)
- Utilities: 2 (cli, metrics)

### Supported Rust Versions
- Minimum Supported Rust Version (MSRV): **1.89**, declared as `rust-version` in the workspace
  manifest since 0.3.1 — except `celers-broker-sqs`, which declares **1.94.1**, and therefore so do
  `celers/sqs`, `celers/full` and any `--all-features` build
- Edition: 2021

### License
- Apache-2.0

### Authors
- COOLJAPAN OU (Team Kitasan)

### Repository
- https://github.com/cool-japan/celers

[0.3.0]: https://github.com/cool-japan/celers/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/cool-japan/celers/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/cool-japan/celers/releases/tag/v0.1.0
