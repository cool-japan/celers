# CeleRS - Development Roadmap

> Distributed task queue system for Rust

## Project Status

- **Phase 1**: The Backbone ✅ **COMPLETE**
- **Phase 2**: Advanced Features ✅ **COMPLETE**
- **Phase 3**: Developer Experience ✅ **COMPLETE**
- **Phase 4**: Performance & Scalability ✅ **COMPLETE**
- **Phase 5**: Beat Scheduler ✅ **COMPLETE**
- **Phase 6**: Extended Brokers & Backends ✅ **COMPLETE**
- **Phase 7**: Full Celery Protocol Compatibility 🚧 **IN PROGRESS** — the *protocol layer* is
  interop-verified against Celery 5.6.3; the broker and result backend are not on the Celery wire
  yet, so a Python worker and a CeleRS worker still cannot share a queue — true for Redis/PostgreSQL/
  MySQL and, separately, for RabbitMQ/SQS now that a `celers_worker::Worker` can run against them
  (see Known gaps #8): they carry a JSON `celers_protocol::Message` body, not kombu's own AMQP/SQS
  framing. See [docs/CELERY_COMPATIBILITY.md](docs/CELERY_COMPATIBILITY.md) and
  [Known gaps](#known-gaps--the-roadmap-after-031)
- **Phase 8**: v0.2.0 Enhancements ✅ **COMPLETE**
- **Phase 9**: v0.2.0 Production Features ✅ **COMPLETE**

All 18 publishable crates are implemented and shipping. That is a statement about breadth, not
about completeness against Python Celery — Phase 7 above is the honest measure of that, and
[Known gaps](#known-gaps--the-roadmap-after-031) is the list of what is still missing.

### v0.3.1 Hardening campaign ✅ COMPLETE (2026-08-26)

A workspace-wide correctness, security and honesty campaign on branch `0.3.1`. Verified green at the
close of the campaign — `cargo build --workspace --all-features`, `cargo clippy --workspace
--all-targets --all-features -- -D warnings` (and the same pair with default features),
`cargo fmt --all --check`, `cargo deny check` (advisories/bans/licenses/sources), and
**7,791/7,791 tests passing** (`cargo nextest run --workspace --all-features`, 104 `#[ignore]`d;
7,509/7,509 with default features, 97 `#[ignore]`d) plus **1,178 passing doctests**
(`cargo test --doc --workspace --all-features`, 138 `ignore`d) — all reverified 2026-08-26 against
the tree this entry describes.

The final wave of the campaign ran the `celers-broker-postgres`, `celers-broker-sql` and
`celers-backend-db` gated suites against real PostgreSQL/MySQL servers for the first time (see
[CHANGELOG.md](CHANGELOG.md)'s "PostgreSQL and MySQL, verified against real servers for the first
time") and closed what it found: `celers-broker-postgres` was non-functional against a live
database (a bare-`$n` bind defect on every UUID placeholder) and is now **271/271** live; the same
bind-cast defect, a concurrent-migration race, and a MySQL `migrate()` that failed outright on every
call were fixed in `celers-backend-db`; `celers-broker-sql` gained MySQL deadlock retry, a working
`move_to_dlq`, and a fix for a queue-scoping leak across its diagnostics methods — all
live-re-verified the same way. One residual defect found in that wave — a MySQL table-name
collision between `celers-backend-db` and `celers-broker-sql` when both are pointed at the same
database — was closed before release by renaming the broker's table to `celers_broker_results`; see
[Known gaps #16](#known-gaps--the-roadmap-after-031), now resolved.

A final verification pass then ran the whole matrix once more — both feature sets, both clippy
variants, doctests, `fmt`, `cargo deny check`, **every** gated suite against live
Redis/PostgreSQL/MySQL/RabbitMQ/LocalStack (each proved to have actually connected, via a
`--no-capture` run with zero skip lines), the Python Celery interop suite (38 passed, no xfails), a
`docker build` of the shipped image and a throwaway PostgreSQL whose `initdb` loaded every
migration file in the tree. It found three more defects and closed them: an SQS DLQ **dry run left every
message it inspected invisible**, so the real replay that followed moved nothing and reported
success (see [CHANGELOG.md](CHANGELOG.md)'s 0.3.1 "Fixed"); six of the `celers` facade's own
"integration" tests hardcoded `postgres://localhost/test`-style URLs inside `if let Ok(..)`, so they
asserted nothing when no server was up and failed against one that was (they are now env-gated, do a
real round trip through each re-exported broker/backend, and clean up after themselves); and
`celers-broker-sql`'s `recurring_tasks_are_claimed_exactly_once` asserted on a database-wide counter
that also counts other runs' leftover configurations, so it failed on a live MySQL for arithmetic
reasons rather than a claim defect (it now counts the rows its own uniquely-named task produced,
which no neighbour can perturb). Two vestigial `#[ignore]`s that hid zero-assertion tests
(`celers-backend-rpc`'s gRPC connect, the facade's beat-scheduler constructor) were replaced with
real assertions that need no service.

A closing wave then shut the gaps that pass had *disclosed* rather than fixed. On PostgreSQL:
migration `009_broker_results.sql` finally creates the `celers_broker_results` table the crate's
entire result store had always addressed and no migration had ever built (every one of those calls
failed against a live server with `relation does not exist`), and `010_task_updated_at.sql` adds the
`celers_tasks.updated_at` column two analytics entry points read — backfilled from each row's real
last-touch time rather than stamped with `NOW()`, and maintained by an explicit `updated_at = NOW()`
in roughly thirty `UPDATE` statements instead of by a hidden trigger. The advisory-lock API was
replaced with an RAII `AdvisoryLockGuard` that owns the connection its session-scoped lock lives on;
the old trio still works but is `#[deprecated(since = "0.3.1")]`. On MySQL: Known gaps #16 is closed
by renaming the broker's result table (**operator-facing** — see
[CHANGELOG.md](CHANGELOG.md)'s "Breaking changes → Database schema"), the claim was split into a
non-locking candidate scan plus a primary-key-only locking read so one queue's in-flight claim can no
longer starve another through InnoDB next-key locks, and the bulk maintenance statements joined the
task-lifecycle ones in `with_deadlock_retry`. `scripts/test-integration.sh` now runs the whole live
matrix in one command.

Everything above was re-verified 2026-08-26 with the PostgreSQL and MySQL volumes **destroyed and
recreated** (`docker compose down -v`) so each schema was built from nothing by the migrations in
this tree, and then a second time against the now-populated volumes for the replay and upgrade
paths; both passes were identical. `migrate()` was additionally pointed at a PostgreSQL database
`initdb` had never touched, where it built all 11 tables and recorded 9 ledger rows (000 applies
untracked; `003_partitioning.sql` is opt-in and deliberately not applied), after which that database
passed the full suite 271/271.

What landed (full detail in [CHANGELOG.md](CHANGELOG.md)'s 0.3.1 section):

- **Remote worker control**: `celers_core::control` / `control_transport`, `celers_worker::control`,
  `celers_broker_redis::RedisControlTransport`, and the `celers inspect` (11) / `celers control`
  (10) CLI commands in front of them. CeleRS-native — no kombu pidbox codec yet.
- **Broker-fed revocation**: `Broker::revoke` / `is_revoked` / `subscribe_revocations`, durable
  revoked-id storage in Redis / PostgreSQL / MySQL, and dequeue-time refusal via
  `Worker::with_broker_revocation`.
- **Celery-compatible event wire**: the `celeryev` channels now carry the shape a Celery monitor
  parses, with the Lamport clock, over a **topic** exchange on AMQP (was fanout) keyed by event type;
  **breaking for 0.3.0 consumers**.
- **Task security wired into the worker**: signature verification before dispatch, worker-side
  re-signing of retries and workflow continuations, redacted `inspect active` previews — all
  off by default.
- **Soft/hard time limits**, settable at runtime over the control channel.
- **Workflows that run**: chord aggregation, saga compensation, conditional Branch/Switch, and the
  Pipeline/FanIn/FanOut/ScatterGather lowerings, all covered end to end by `workflow_semantics` and
  `patterns_e2e`.
- **RabbitMQ and SQS become worker-usable**: `celers_kombu::core_adapter::KombuBrokerAdapter`
  implements `celers_core::Broker` over either transport; `AmqpBroker::into_core_broker` and
  `SqsBroker::into_core_broker` build one, on by default. Still not Celery-wire-compatible at the
  RabbitMQ/SQS level — see Known gaps #8. The facade's `celers::broker_helper::create_broker(type,
  url, queue)` now also builds one directly for `"amqp"`/`"rabbitmq"`/`"sqs"`, live-verified against
  a real RabbitMQ.
- **PostgreSQL and MySQL, verified against a real server for the first time**: doing so found
  `celers-broker-postgres` non-functional against a live database (fixed, now 271/271 live) plus
  more PostgreSQL-bind, concurrent-migration and MySQL correctness defects across
  `celers-backend-db`/`celers-broker-sql`, all fixed and live-re-verified. The MySQL table-name
  collision between the two crates that this wave found was also fixed before release (the broker's
  result table is `celers_broker_results` now, with an in-place upgrade for existing databases) —
  see [Known gaps #16](#known-gaps--the-roadmap-after-031), now resolved.
- **Protocol wire completeness**: `MessageProperties` now serializes `delivery_tag` / `delivery_info`
  (their absence killed a real kombu consumer's event loop with a `KeyError`), `ResultMessage::children`
  models Celery's actual result-tree shape instead of a bare id list, and `ExceptionInfo::exc_message`
  is Python's `exc.args` list rather than a joined string — all three interop-verified against a real
  Celery worker.
- **Pure Rust, no exceptions**: `deny.toml`'s `[graph] exclude` is empty. The last holdout,
  `celers-broker-sqs`, now goes through `pure_http`, an AWS SDK `HttpClient` over `oxihttp-client`.
- **Celery interop proved, not asserted**: `tests/python-compat/` runs a real Celery client and a
  real `celery` worker against a real Redis, and `crates/celers-protocol/tests/fixtures/` holds
  verbatim Celery 5.6.3 captures.
- **MSRV declared**: 1.89 workspace-wide, 1.94.1 for `celers-broker-sqs` (and therefore `celers/sqs`,
  `celers/full`, `--all-features`).

### v0.3.0 Hardening (2026-06-13)

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

- **Crates**: 18 published + 2 unpublished workspace members (`celers-examples`, `celers-facade-test`)
- **Brokers a `celers_worker::Worker` can consume from** (`celers_core::Broker`): Redis, PostgreSQL,
  MySQL, the in-process `InMemoryBroker`, and — as of 0.3.1, via
  `celers_kombu::core_adapter::KombuBrokerAdapter` — RabbitMQ (AMQP) and AWS SQS, each on by default
  through the owning crate's `core-broker` feature. None of the three network transports is
  Celery-wire-compatible; see [Known gaps](#known-gaps--the-roadmap-after-031)
- **Backends**: Redis, PostgreSQL/MySQL (Database), gRPC - ALL with ResultStore adapters
- **Examples**: 15 working examples (including Canvas workflows, web scraper, image processing, AsyncResult API)
- **Benchmarks**: 3 comprehensive benchmark suites
- **Tests**: 7,791 passing with `--all-features` (0 failures, 104 `#[ignore]`d; 7,509/7,509 with
  default features, 97 `#[ignore]`d), plus 1,178 passing doctests (138 `ignore`d) — verified
  2026-08-26 at the close of the 0.3.1 campaign. Note that the env-gated live-service suites inside
  that count print a skip line and pass without asserting when their `CELERS_TEST_*` variable is
  unset; a `--no-capture` run of the whole workspace with none of those variables exported emitted
  **147 such skip lines** (109 before this release added the PostgreSQL result-store and advisory-lock
  suites) — the size of that category measured rather than estimated, and an upper bound on
  the number of tests in it, since a test that opens two connections prints two lines. See
  [tests/integration/README.md](tests/integration/README.md). The live-service suites
  themselves — run for real against PostgreSQL/MySQL/RabbitMQ/Redis/LocalStack this campaign, not
  merely present — are summarized in [CHANGELOG.md](CHANGELOG.md)'s 0.3.1 "Known Limitations"
- **Build Status**: ✅ 0 errors, 0 warnings, 0 clippy warnings, 0 doc warnings, `cargo deny check` clean
  on all four checks (advisories/bans/licenses/sources)
- **Documentation**: 1500+ lines of guides + 18 TODO.md files
- **Monitoring**: Full Prometheus + Grafana + OpenTelemetry support
- **Features**: Task queues, priorities, DLQ, cancellation, retries, timeouts, health checks, Canvas workflows (chunks, xmap, xstarmap, group.skew, group.jitter, conditional: Branch/Maybe/Switch), batch operations, memory optimization, chord synchronization, AsyncResult API with ALL backend ResultStore adapters, Protocol v2 compatibility tests, Real-time events (task & worker lifecycle), Event emission from worker, Worker control commands (inspect, ping, shutdown, revoke), Per-task rate limiting (token bucket, sliding window), Task routing by name patterns (glob, regex), Time limits (soft/hard), Enhanced task revocation (bulk, pattern, persistent), Queue control commands

## Known gaps — the roadmap after 0.3.1

Everything below was *verified against the code at the close of the 0.3.1 campaign*, not inherited
from an older list. Each entry names where the evidence lives. This is the honest roadmap; the
per-phase checklists further down are history.

### Blocking full Celery interoperability

1. **The broker and result backend are not on the Celery wire.** `celers-broker-redis` (and the
   PostgreSQL / MySQL brokers) enqueue a `SerializedTask` JSON document, not a Celery envelope, and
   both Redis result backends store a CeleRS-shaped record (`{task_id, task_name, result,
   created_at, …}`) under Celery's `celery-task-meta-<uuid>` key rather than Celery's
   `{status, result, traceback, children, date_done, task_id}`. **A Python Celery worker and a
   CeleRS worker cannot share a queue.** Closing this means routing both through `celers-protocol`;
   `tests/python-compat/` is built to prove it when it lands.
   *Evidence:* [docs/CELERY_COMPATIBILITY.md](docs/CELERY_COMPATIBILITY.md) → "What is not
   interoperable".
2. **No kombu pidbox codec.** `celers inspect` / `celers control` are shaped like a pidbox
   (broadcast channel + per-request reply channel) but speak a CeleRS-native encoding, so
   `celery -A app inspect` cannot reach a CeleRS worker and vice versa. A pidbox codec plus a golden
   fixture for the control-command wire is the missing piece.
   *Evidence:* `crates/celers-core/src/control_transport.rs`.
3. ~~**`MessageBuilder` envelopes are not deliverable as-is.**~~ **FIXED.** `MessageProperties` now
   serializes `delivery_tag` (fresh per message, as kombu's producer mints it) and `delivery_info`
   (`{exchange, routing_key}`, the routing key naming the queue `.queue(...)` / `.routing_key(...)`
   chose), so the ordinary producer path no longer kills a Celery worker's consumer loop with the
   `KeyError` that `kombu.transport.virtual.base.Message.__init__` raises on either missing key.
   `build()` also stamps `argsrepr` / `kwargsrepr`, which a monitor displays.
   *Evidence:* `tests/python-compat/test_celers_to_python.py::test_message_builder_envelope_is_deliverable_as_is`
   publishes the builder's envelope **unpatched** and a real worker executes it; the requirement
   itself stays pinned by `::test_kombu_requires_delivery_tag_and_delivery_info`.
4. ~~**`ResultMessage::children` is `Vec<Uuid>`.**~~ **FIXED.** `children` is a tree of
   `ResultChild` nodes modelling `AsyncResult.as_tuple()` (`[[id, parent], group_results]`), so a
   retried, chained or grouped task's record parses and is written back in Celery's own shape; the
   legacy id-list form, Celery's short `[id, nodes]` form and a `children: null` record (what
   `Backend.current_task_children` writes outside a task context) are all accepted.
   *Evidence:* `tests/python-compat/test_celers_to_python.py::test_celers_parses_a_celery_record_that_has_children`,
   whose children come from Celery's own `as_tuple()` and go back through `result_from_tuple`.
5. **The Celery-shaped event stream has not been pointed at a real monitor.** CeleRS emits the
   Celery event shape as of 0.3.1 and pins the bytes with a golden test, but no test has yet run
   `celery events` or Flower against a CeleRS worker. Doing so is the cheapest remaining interop
   win.

### Known defects

6. ~~**Solar schedules never fire.**~~ **FIXED.** The `Schedule::Solar` branch of
   `crates/celers-beat/src/schedule.rs` now resolves events through `sunrise`'s
   `SolarDay::event_time`, which returns an absolute `DateTime<Utc>` and needs no unit conversion;
   the old code divided that crate's Unix-*seconds* return as minutes-since-midnight and errored on
   the first iteration. `test_solar_schedule_{sunrise,sunset}` are un-`#[ignore]`d and now assert
   real almanac instants, joined by tests for negative longitudes, twilight ordering, polar day and
   out-of-range coordinates. `golden_hour_begin`/`_end`, the last remaining approximation, is now
   also a true `SolarEvent::Elevation` solve (`elevation: 0.0` for the morning boundary,
   `elevation: -6°` for the evening one) rather than a flat offset from sunrise/sunset — there is no
   remaining approximation in the solar branch.
7. ~~**`AmqpEventEmitter` publishes every event with one configured routing key** (default empty, so
   fanout).~~ **FIXED.** The default exchange type is now `"topic"` (was `"fanout"`), and
   `AmqpEventConfig::routing_mode` (default `EventRoutingMode::PerEventType`) derives each event's
   routing key from its own wire `type` — `task-started` publishes as `task.started`,
   `worker-heartbeat` as `worker.heartbeat` — matching real Celery's `EventDispatcher`. A consumer
   can now bind `task.#`, `worker.#`, or `#` selectively; `AmqpEventReceiver` binds `#` by default.
   The pre-fix behaviour survives as an explicit opt-in, `EventRoutingMode::Fixed`.
   *Evidence:* `crates/celers-broker-amqp/src/event_transport.rs`.

### Missing adapters and features

8. ~~**No `celers_core::Broker` adapter over the `celers-kombu` transports.**~~ **FIXED.**
   `celers_kombu::core_adapter::KombuBrokerAdapter<T>` implements `celers_core::Broker` over any
   `CoreBrokerTransport`; `AmqpBroker::into_core_broker(queue)` and `SqsBroker::into_core_broker(queue)`
   build one, so **a `celers_worker::Worker` can now run against RabbitMQ and SQS**. Both crates
   enable it by default (`core-broker` feature, pulling `celers-kombu/core-adapter`); SQS additionally
   maps `dequeue_batch`/`enqueue_batch`/`ack_batch`/`defer`/`enqueue_after` onto native
   `ReceiveMessage(MaxNumberOfMessages)`/`SendMessageBatch`/`DeleteMessageBatch`/
   `ChangeMessageVisibility`/`SendMessage(DelaySeconds)` instead of the trait's one-at-a-time
   defaults. One transport, one `tokio::sync::Mutex`: every operation is serialised through it, so
   the worker's `dequeue` poll (parked for up to `DEFAULT_POLL_TIMEOUT`, 1s) briefly blocks `ack`
   calls from tasks it already dispatched — give each worker its own transport rather than sharing
   one. The revocation half of the original gap **remains open on purpose**: `cancel()` always
   answers `Ok(false)` and `revoke`/`is_revoked`/`subscribe_revocations` are not overridden (they
   keep the trait's inert defaults), because neither AMQP nor SQS can address an already-queued
   message by id. A worker still refuses a revoked task it has *dequeued*, via its own
   revocation registry.
   *Evidence:* `crates/celers-kombu/src/core_adapter/adapter.rs`,
   `crates/celers-broker-amqp/src/core_broker.rs`, `crates/celers-broker-sqs/src/core_broker.rs`.
   The facade's `celers::broker_helper::create_broker("amqp"/"rabbitmq"/"sqs", url, queue)` now
   builds one directly too, live-verified against a real RabbitMQ
   (`broker_helper::tests::live_amqp::create_broker_amqp_reaches_a_real_rabbitmq_end_to_end`).
9. **`celers worker` builds an empty `TaskRegistry`.** This is inherent — CeleRS tasks are Rust
   types registered at build time — and 0.3.1 makes it loud (an explicit warning plus
   `--demo-tasks`), but the only way to run application tasks remains linking `celers-worker` into
   your own binary. A configuration-driven registry would need a plugin ABI CeleRS does not have.
10. **`ReplayGuard` is per-process by construction.** A deployment needing global single-use-nonce
    semantics must back it with shared storage (the module documents how). Relatedly,
    `SignatureVerification::with_freshness` and `with_replay_guard` are at odds with at-least-once
    redelivery: several paths hand the same signed bytes to a worker more than once by design.
11. **`PayloadHygiene::redact_payload` has no result-backend caller.** The worker uses it for the
    `inspect active` argument preview, but no result-backend surface currently persists task
    args/kwargs, so there is nothing to redact there. The call becomes necessary the moment one
    does.
12. ~~**`celers-cli` `[dev-dependencies]` still lacks `async-trait`**~~ **FIXED.** `async-trait` is
    now a `celers-cli` **`[dependencies]`** entry (not `[dev-dependencies]`) — promoted rather than
    added there, because `commands::worker`'s built-in demo tasks (`--demo-tasks`) are production
    code, not test-only. `tests/control_redis.rs` now writes `#[async_trait::async_trait] impl Task`
    directly instead of the desugared `Pin<Box<dyn Future>>` form.

### Test and tooling debt

13. **Env-gated suites still pass without asserting** — but there is now one command that runs them
    all. Tests that early-return when their `CELERS_TEST_*` (or `DATABASE_URL`/`MYSQL_URL`) variable
    is unset emitted **147 skip lines** in a
    `cargo nextest run --workspace --all-features --no-capture` with none of them exported — the
    measured size of that category, and an upper bound on its test count, since a test opening two
    connections prints two lines. A further **104** are `#[ignore]`d and not attempted at all without
    `--run-ignored all`. (Both re-measured 2026-08-26. The skip-line count rose from 109 because this
    wave added gated tests rather than because gating got worse: 90 of the 147 are now
    `CELERS_TEST_POSTGRES_URL`, which the new `tests_pg_results.rs` and `tests_pg_locks.rs` account
    for. The ignore count came down from 112 when two vestigial `#[ignore]`s and six hardcoded-URL
    facade "integration" tests were replaced with env-gated ones that really run.)
    `docker-compose.yml` has a service for every one of them (`--profile test` for MySQL and
    LocalStack, `--profile python-compat` for Celery) and
    [tests/integration/README.md](tests/integration/README.md) maps variable → service → invocation.

    ~~Nothing in the repository *runs* the full matrix.~~ **`scripts/test-integration.sh` now
    does** — it brings the stack up, waits for real health through the host-forwarded port
    (restarting a wedged container and retrying, which four of five services needed on a fresh
    `up -d` during this release's verification), provisions the two databases that must not be
    shared, exports all **eight** `CELERS_TEST_*`-family variables, runs every gated suite with
    `--run-ignored all`, and prints a PASS/FAIL/SKIP summary. `--only <service>` scopes it, `--full`
    adds the Python-interop suite and a `docker build` smoke. What remains open is *automation*, not
    tooling: `.github/` still holds only `dependabot.yml`, `FUNDING.yml` and a `workflows.disabled/`
    directory, so the script has to be run by hand. Running the live PostgreSQL/MySQL suites this way
    is exactly what found the defects fixed in [CHANGELOG.md](CHANGELOG.md)'s 0.3.1 "Fixed" section
    and closed Known gaps #16 below — the risk this item describes is not hypothetical.
14. ~~**Three files remain at or over the 2000-line policy limit**~~ **Down to one.**
    `celers-backend-db/src/lib.rs` (was 2334) is now 239 lines, split into `mysql_backend.rs`,
    `postgres_backend.rs` and `result_compression.rs`; `celers-worker/src/worker_core/tests.rs` (was
    2308) is now a `worker_core/tests/` directory of 16 files, none over 600 lines;
    `celers-worker/src/sandbox.rs` (was 2049) is now 600 lines, split into `sandbox/config.rs`,
    `error.rs`, `rlimit_impl.rs`, `seccomp_impl.rs`, `stats.rs` and `tests.rs`. One file is over the
    cap as of this release: `celers-beat/src/tests/tests_schedule.rs` at **2004 lines** (grew past it
    with this wave's solar/golden-hour test additions). `splitrs` is the project's tool for this.
15. **The seccomp filter is type-checked but never executed here.** `sandbox.rs`'s `seccomp_impl` is
    gated on `all(target_os = "linux", feature = "seccomp")`; its BPF jump encoding has a unit test,
    but no test in this repository installs the filter under a Linux kernel.
16. ~~**`celers-backend-db` and `celers-broker-sql` collide on the MySQL table name
    `celers_task_results`.**~~ **Fixed — the broker's table is renamed.** Both crates used to
    declare `CREATE TABLE IF NOT EXISTS celers_task_results` with incompatible column sets, so
    whichever `migrate()` reached a shared database *second* found the table present with the other
    crate's columns and its own follow-up DDL failed (`celers-backend-db`'s
    `CREATE INDEX ... (expires_at)` with `ERROR 1072 (42000): Key column 'expires_at' doesn't exist
    in table`, one way round; `Key column 'status' doesn't exist in table`, the other). It triggered
    exactly when a deployment pointed both the broker and the result backend at one MySQL
    database — a normal, documented topology.

    `celers-backend-db` is CeleRS's result *backend* and keeps the `celers_task_results` name; the
    broker-internal store is now **`celers_broker_results`** (`migrations/010_broker_results.sql`),
    and `celers-broker-postgres` adopted the same name in its new `009_broker_results.sql`, so the
    two engines agree. Existing MySQL databases are upgraded in place by
    `MysqlBroker::rename_legacy_broker_results_table`, an untracked step that runs on every
    `migrate()` — untracked because a database an older build migrated already records version
    `010`, so the tracked migration is skipped there and the rename is the only thing that can
    produce the new table. It acts only when the legacy table carries this crate's `traceback`
    signature column and the new name is free, and never drops or writes to the legacy table under
    any branch. **Operator-facing:** hand-written SQL, dashboards or reporting jobs reading the
    broker's `celers_task_results` must be repointed — see
    [CHANGELOG.md](CHANGELOG.md)'s 0.3.1 "Breaking changes → Database schema".

    *Verified live, on a MySQL volume recreated from scratch with `docker compose down -v`:*
    `celers-broker-sql` **236/236** (parallel and `--test-threads=1`) and then `celers-backend-db`
    **151/151 against that same database** — the configuration that used to produce 7 failures.
    Regression-tested from both directions by
    `celers-broker-sql`'s `broker_and_result_backend_coexist_on_one_database` (migrates both crates
    onto one database and round-trips a result through each) and the new
    `tests_hardening::migration_upgrade` suite, which drives all four branches of the rename against
    a real server — a database seeded with the pre-rename schema, rows, indexes *and* a
    `celers_migrations` row for `010`; a `celers_task_results` that belongs to `celers-backend-db`;
    both names present; and an ambiguous table carrying both crates' signature columns.
    *Evidence:* `crates/celers-broker-sql/src/broker_migrate.rs`,
    `crates/celers-broker-sql/migrations/010_broker_results.sql`,
    `crates/celers-broker-postgres/migrations/009_broker_results.sql`.

---

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
- [x] API documentation via rustdoc — 1,178 passing doctests, `RUSTDOCFLAGS="-D warnings" cargo doc`
      clean
- [x] User guide with examples (15 working examples in `crates/celers-examples/examples/`)
- [x] Deployment guide ([docs/DEPLOYMENT.md](docs/DEPLOYMENT.md))
- [x] Architecture decision records — five, in [docs/adr/](docs/adr/)
- [x] Celery compatibility guide, rewritten in 0.3.1 with a per-row evidence column
      ([docs/CELERY_COMPATIBILITY.md](docs/CELERY_COMPATIBILITY.md))
- [x] Integration-test guide: gate variable → service → invocation
      ([tests/integration/README.md](tests/integration/README.md))
- [x] Python interop guide ([tests/python-compat/README.md](tests/python-compat/README.md))
- [x] `CONTRIBUTING.md` — dev setup, gated live-service suites, code standards, the no-CI-on-push
      reality, and how a release actually gets cut
- [ ] 138 doctests are still ```ignore``d and therefore never compile

### Specialized Guides
- [x] CLI usage documentation (`crates/celers-cli/README.md`)
- [x] Per-crate READMEs for all 18 published crates
- [x] Grafana dashboard + Prometheus scrape config, as **files that exist** and that
      `docker-compose.yml` mounts (`docs/grafana/`, `docs/prometheus.yml`) — the previously claimed
      `GRAFANA.md` and `OPENTELEMETRY.md` never existed and the entries are removed rather than
      re-promised
- [x] Result-backend performance notes (`crates/celers-backend-redis/PERFORMANCE.md`)
- [ ] Workspace-level performance tuning guide (a `PERFORMANCE.md` at the repository root was
      claimed before and never existed)
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

- [x] Unit tests for all core types — 7,791 passing with `--all-features` (7,509 with default features)
- [x] Integration tests against a real Redis (env-gated on `CELERS_TEST_REDIS_URL`)
- [x] Integration tests against a real PostgreSQL / MySQL / RabbitMQ / LocalStack SQS (env-gated;
      services in `docker-compose.yml`, one `--profile test` away) — actually **run** against all
      four this campaign for the first time (not merely present), which is what found and fixed the
      defects in CHANGELOG.md's 0.3.1 "Fixed" section
- [x] Live Python Celery interoperability suite (`tests/python-compat/`)
- [x] Property-based round-trip tests (`celers-protocol`) and a `trybuild` compile-fail UI harness
      (`celers-macros`)
- [x] Benchmarks — 3 criterion suites in `celers-examples`, 1 in `celers-cli`
- [ ] **Something in the repository that actually runs the gated matrix** — see Known gaps #13; the
      suites exist and the services exist, but no script or workflow ties them together, so a green
      local run proves nothing about them
- [ ] Stress / chaos testing scenarios
- [ ] Measure code coverage (no coverage run has ever been recorded here; the ">80%" target was
      never backed by a number)

## Release Preparation

- [ ] **CI/CD pipeline** — `.github/` holds only `dependabot.yml`, `FUNDING.yml` and a
      `workflows.disabled/` directory: **no workflow runs on push today**. The previous ✅ on this
      line was wrong. Project policy allows only `pypi-publish.yml` / `npm-publish.yml` under
      `.github/workflows`, so build/test automation has to live elsewhere (a `scripts/` entry point
      run locally or by an external runner)
- [ ] Automated testing on multiple Rust versions — the MSRV is now *declared* (1.89; 1.94.1 with
      `sqs`), but nothing verifies a build at that floor
- [x] crates.io metadata complete for all 18 published crates (`readme`, `keywords`, `categories`,
      `rust-version`)
- [x] `cargo deny check bans` green with an empty `[graph] exclude`
- [x] Example applications (web scraper, image processing)
- [ ] Migration guide from other task queues

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

**Goal**: interoperate with Python Celery well enough that the two can share a queue.

**Status after 0.3.1**: the *protocol layer* is done and interop-verified against Celery 5.6.3; the
*broker and result backend* are not, so the queue is not shared yet. The row-by-row evidence table
lives in [docs/CELERY_COMPATIBILITY.md](docs/CELERY_COMPATIBILITY.md); what remains is items 1–5 of
[Known gaps](#known-gaps--the-roadmap-after-031).

### Critical: Python Celery Interoperability
- [x] **Protocol v2 wire compatibility** ✅ interop-verified
  - [x] Celery v2 envelope (headers, properties, base64 body) parsed and produced
  - [x] Checked against **verbatim Celery 5.6.3 captures** (`crates/celers-protocol/tests/fixtures/`,
        `tests/celery_golden.rs`) — recorded from a real Celery, not generated by CeleRS
  - [x] args / kwargs / embed, including tuple-vs-list `argsrepr`
  - [x] ETA, countdown→eta resolution, expires (UTC RFC 3339)
  - [x] `argsrepr` / `kwargsrepr` rendered as `celery.utils.saferepr` renders them
  - [x] Celery 5.6-only headers (`shadow`, `stamps`, `replaced_task_nesting`, …) tolerated on parse
  - [x] **Tested against a live Python Celery 5.6.3 worker** (`tests/python-compat/`)
  - [ ] Tested against Celery 4.x (only 5.6.3 is pinned today)
- [x] **Result records** ✅ interop-verified
  - [x] Read a real worker's SUCCESS and FAILURE records (`exc_type`/`exc_module`/`exc_message`
        + `traceback`)
  - [x] Write a record a real `AsyncResult.get()` reads, including the pub/sub publish that wakes it
  - [ ] `children` as Celery's nested result tuples (Known gaps #4)
- [ ] **"Protocol v5"** — note that this is a **CeleRS-internal** version label, not a Celery wire
      protocol: nothing in the interop suite or the recorded captures exercises it (both pin
      `task_protocol = 2`). Version negotiation and the v2↔v5 migration are implemented and
      Rust-tested; there is nothing on the Celery side to test them against.
- [ ] **Bidirectional task exchange over a shared queue** (Known gaps #1)
  - [x] Bidirectional exchange at the **protocol** layer, via `celery_bridge` + a real Celery worker
  - [ ] `celers-broker-*` puts a Celery envelope on the queue instead of a `SerializedTask`
  - [ ] `celers-backend-redis` stores Celery's `{status, result, traceback, children, date_done,
        task_id}` record
  - [ ] A CeleRS worker and a `celery` worker consuming the same queue
  - [ ] `MessageBuilder` emits `delivery_tag` / `delivery_info` (Known gaps #3)
- [x] **Integration testing suite** ✅ `tests/python-compat/`
  - [x] Real Celery client + real `celery` worker + real Redis, no mocks
  - [x] Python → CeleRS and CeleRS → Python, both directions
  - [x] Result retrieval across languages, failures and retries included
  - [x] Chain link and group headers at the protocol level
  - [x] Reachable from cargo (`cargo test -p celers-protocol --test python_interop`) and skips
        visibly without Redis / Python
  - [ ] Canvas workflows executed across the two runtimes (needs the shared queue above)
  - [ ] Beat scheduler task submission into a Celery worker
  - [ ] `celery events` / Flower pointed at a CeleRS worker (Known gaps #5)
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
  - [x] task-soft-time-limit-exceeded event type (0.3.1, CeleRS extension)
  - [x] **Celery-compatible wire shape** on `celeryev` channels (0.3.1) — `uuid`, `name`, float
        `timestamp`, `hostname`, `pid`, `clock`, `utcoffset`; byte-stability pinned by
        `celers_core::event::wire`'s `wire_json_is_byte_for_byte_stable`. **Breaking for 0.3.0
        consumers**
  - [x] Celery Lamport clock (`forward_event_clock` / `adjust_event_clock`) (0.3.1)
  - [x] Parses events published by a real Python Celery worker, filling its two sparser fields with
        documented defaults (0.3.1)
  - [ ] Verified against a real monitor (`celery events`, Flower) — Known gaps #5
  - [ ] AMQP emitter uses a topic exchange keyed by event type instead of one routing key
        (Known gaps #7)

- [x] **Event Consumers**
  - [x] Event receiver/dispatcher
  - [x] Event filtering and routing
  - [x] Event persistence (database, file)
  - [ ] Event streaming (WebSocket, SSE)
  - [x] Snapshot events for monitoring
  - [x] Event-based alerting

### Remote Control & Inspection

**0.3.1 closed the transport half of this.** The command vocabulary lived in
`celers-core/src/control.rs` from earlier phases with nothing carrying it; there is now a wire
(`celers_core::control_transport`), a Redis implementation
(`celers_broker_redis::RedisControlTransport`), a worker-side handler (`celers_worker::control`)
and the `celers inspect` / `celers control` CLI in front of it. Three caveats survive:

- [ ] **No kombu pidbox codec** — the channel, framing and command names are CeleRS-native, so
      `celery -A app inspect/control` cannot reach a CeleRS worker (Known gaps #2)
- [x] `Inspect(Scheduled)` / `Inspect(Reserved)` answer empty **accurately**: a CeleRS worker keeps
      no worker-local scheduled or reserved set (ETA tasks live in the broker's delayed queue; a
      batch dequeue dispatches rather than parks)
- [ ] `Queue(Purge/Delete/Bind/Unbind/Declare)` answer with an error naming the reason: the `Broker`
      trait has no such operations, so a worker cannot perform them. Use `celers queue purge` and
      the other broker-specific commands

- [x] **Worker Control Commands** ✅ (vocabulary in celers-core/src/control.rs; dispatched by
      celers-worker/src/control.rs since 0.3.1)
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
  - [x] TLS for Redis (`rediss://`) — 0.3.1: `tokio-rustls-comp` + `tls-rustls-webpki-roots` with
        OxiTLS' Pure-Rust provider installed by each crate's `install_pure_tls_provider()`; custom
        CA and client certs via `RedisConfig::tls(...)`
  - [x] TLS for RabbitMQ — `lapin` pinned to `rustls-webpki-roots-certs` + the same Pure-Rust
        provider; `celers-broker-amqp/tls-native-certs` adds the OS trust store
  - [x] TLS for AWS SQS — 0.3.1: `celers_broker_sqs::pure_http`, an SDK `HttpClient` over
        `oxihttp-client`
  - [ ] TLS for PostgreSQL/MySQL — inherited from `oxisql-*`; not exercised by a test here
  - [ ] TLS for the gRPC result backend — deliberately not built in (`tonic`'s `tls-ring` /
        `tls-aws-lc` pull banned crypto); bring your own `Channel` via
        `GrpcResultBackend::from_channel_with_config`
  - [ ] Authentication tokens
  - [ ] IP whitelisting

- [x] **Task Security** ✅ (wired into the worker in 0.3.1; all of it off by default)
  - [x] Task signature verification (HMAC-SHA256), enforced **before dispatch** — before revocation,
        poison-pill or routing decisions — with DLQ recording, a `task-rejected` event and a
        `WorkerStats::signature_rejected` counter
  - [x] Worker re-signs the messages it produces (retries, workflow continuations)
  - [x] Migration mode: admit unsigned, still reject forged
  - [x] Message encryption (at rest and in transit)
  - [x] Task argument sanitization + payload redaction in `inspect active`
  - [x] `celers-cli loadtest --signing-key`, falling back to `CELERS_TASK_SIGNING_KEY`, so a
        verifying fleet does not dead-letter every synthetic task
  - [ ] `ReplayGuard` backed by shared storage for global single-use-nonce semantics
        (Known gaps #10)
  - [x] Secure pickle — resolved by **not implementing pickle**: it is an arbitrary-code-execution
        risk and is deliberately absent

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
