# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.1] - Unreleased

0.3.1 is a hardening release, not a feature drop: 557 files changed against 0.3.0. The headline
additions are the ones that make a CeleRS deployment operable and defensible in production — a
remote control protocol with `celers inspect` / `celers control` in front of it, revocations that
survive a worker restart because the *broker* holds them, an event stream a Python Celery monitor
can actually parse, opt-in message authentication on the worker receive path, soft/hard time
limits, workflow patterns (chord aggregation, saga compensation, conditional branches) that a
worker really executes instead of only building, and — new this release — RabbitMQ and SQS joining
Redis/PostgreSQL/MySQL as brokers a `celers_worker::Worker` can actually run against, via
`celers_kombu::core_adapter`. The whole workspace is now Pure Rust with an empty `deny.toml`
`[graph] exclude`.

Celery compatibility is now **proved rather than asserted — at the protocol layer**: a new
`tests/python-compat/` suite exchanges tasks and results with a real Python Celery 5.6.3 in both
directions, and `crates/celers-protocol/tests/fixtures/` holds verbatim Celery wire captures. The
boundary is stated plainly in the rewritten `docs/CELERY_COMPATIBILITY.md`: **every broker and
result backend still carries CeleRS-shaped payloads**, so a Python Celery worker and a CeleRS worker
cannot share a queue yet — true of Redis/PostgreSQL/MySQL as before, and now, separately, of the
newly worker-usable RabbitMQ/SQS brokers too (they gained `celers_core::Broker`, not Celery's own
AMQP/SQS wire framing).

**Read the breaking-changes section first if you consume CeleRS' event stream (including over
AMQP — the exchange type changed), subscribe to a Redis `<queue>:cancel` channel, `match`
exhaustively on `TaskEvent`, pattern-match `celers_protocol::result::ExceptionInfo::exc_message` /
`ResultMessage::children` or `celers_broker_redis::QueueRateLimiter::Distributed`, implement
`celers_core::Broker` yourself, rely on task coalescing, call `PostgresBroker`'s advisory-lock
trio, or read the MySQL broker's `celers_task_results` table directly — that table is renamed
`celers_broker_results` and existing databases are upgraded in place on the next `migrate()`.**

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
- **The AMQP event exchange is now a topic exchange, not a fanout.** `AmqpEventConfig`'s
  `exchange_type` default changed from `"fanout"` to `"topic"`, matching real Celery's
  `celery.events.dispatcher.EventDispatcher`, and the default `EventRoutingMode::PerEventType`
  publishes each event under a key derived from its own wire `type` (`task-started` →
  `task.started`, `worker-heartbeat` → `worker.heartbeat`) instead of one fixed empty key. **This is
  an operational break, not just a behavioural one**: if a `celeryev` exchange from a running 0.3.0
  deployment already exists as `fanout` in RabbitMQ, this emitter's next `exchange_declare` fails
  with a `406 PRECONDITION_FAILED` (exchange type mismatch) until an operator deletes and lets it be
  redeclared. `AmqpEventReceiver` substitutes Celery's own catch-all binding pattern `"#"` for
  itself when left at its default, so it keeps receiving everything without any config change; a
  hand-rolled consumer bound on a specific key must rebind. The pre-fix behaviour is still available
  as an explicit `EventRoutingMode::Fixed`

#### Database schema

- **`celers-broker-sql`'s MySQL result table is renamed `celers_task_results` →
  `celers_broker_results`, and existing databases are upgraded automatically on the next
  `migrate()`.** This is the fix for what shipped as Known gaps #16: `celers-backend-db`'s
  `MysqlResultBackend` auto-migrates a table *also* called `celers_task_results`, with an
  incompatible schema (`result_state`/`result_data`/`extra` there, `status`/`result`/`traceback`
  here), and a normal deployment points the broker and the result backend at one database. Whichever
  migrated second lost: `CREATE TABLE IF NOT EXISTS` silently no-op'd against the other crate's
  table and the follow-up DDL died — `ERROR 1072 (42000): Key column 'status' doesn't exist in
  table` one way, `Key column 'expires_at' doesn't exist in table` the other. The **result
  backend** keeps `celers_task_results`; this broker-internal store moved.

  **What an operator has to do: nothing, but read this if you query the table directly.** The new
  `MysqlBroker::rename_legacy_broker_results_table` step runs on every `migrate()`, is untracked (so
  it also runs on a database whose ledger already records migration `010` under the old name — the
  case that matters, since the tracked migration is skipped there), and issues a `RENAME TABLE`,
  which preserves every row and carries the table's three indexes across under their existing names.
  It is deliberately conservative and never destructive: it acts **only** when the legacy table
  carries this crate's `traceback` signature column and the new name is free. A
  `celers_task_results` carrying `celers-backend-db`'s `result_state` instead is recognised as the
  result backend's live store and left strictly alone; one carrying *both* columns is ambiguous and
  also left alone; with both table names already present the legacy one is left in place and a
  warning names it, because rows in it would otherwise be silently unreachable. The legacy table is
  never dropped and never written to under any branch — on a shared database it is another crate's
  data. **Dashboards, reporting jobs and hand-written SQL that read `celers_task_results` expecting
  the broker's columns must be repointed at `celers_broker_results`.**

  `celers-broker-postgres` had the same collision latent — its `results.rs` addressed a result table
  no migration had ever created — and its new `009_broker_results.sql` adopts the same name, so the
  two backends now agree on both engines: the *broker's* result store is `celers_broker_results`
  everywhere, the *result backend's* is `celers_task_results` everywhere. There is no rename step on
  the PostgreSQL side and none is needed, because that crate never created a result table at all
  (see Fixed).

  Verified live rather than asserted: `celers-broker-sql`'s new
  `tests_hardening::migration_upgrade` suite drives all four branches against a real MySQL 8 —
  including a database seeded with the pre-rename schema, rows, indexes *and* a `celers_migrations`
  row for version `010`, which is exactly what an older build leaves behind — and asserts the rows
  and index names survive, that the result backend's table is untouched, and that a replay changes
  nothing. It needs a database it may drop tables in, so it is gated on its own
  `CELERS_TEST_MYSQL_UPGRADE_URL` (provisioned by `scripts/test-integration.sh`) and refuses to run
  against the database `CELERS_TEST_MYSQL_URL` names. The collision itself is now covered from the
  other side too: `celers-backend-db`'s full suite passes **151/151 against the very database
  `celers-broker-sql` has just migrated**, the configuration that used to produce 7 failures

#### Source compatibility

- **`PostgresBroker::try_advisory_lock` / `advisory_lock` / `release_advisory_lock` are
  `#[deprecated(since = "0.3.1")]`** — a downstream build with `-D warnings` stops compiling until
  it moves to the guard API (see Added). They were not merely renamed, they were *broken*: each
  issued its `pg_advisory_lock` / `pg_advisory_unlock` straight through the pool, which hands out a
  different connection per statement, so an acquire and its release usually landed on different
  backend sessions — the unlock returned `false` without releasing anything and the lock stayed held
  on whichever pooled connection had taken it until the process exited. They keep their signatures
  and now work correctly (the acquire parks the pinned connection in the broker's lock registry and
  the release unlocks on that exact connection), but they stay deprecated because no implementation
  can give them what the guard has: with no RAII, a caller that returns early or panics between the
  two calls pins one of the broker's pool slots for the broker's whole lifetime with nothing to
  notice it
- `TaskEvent` gained a `SoftTimeLimitExceeded` variant and **is not `#[non_exhaustive]`**: a
  downstream `match` over `TaskEvent` without a wildcard arm stops compiling until the arm is added
- `celers_core::Broker` gained five methods — `revoke(&TaskId, terminate)`, `is_revoked(&TaskId)`,
  `subscribe_revocations()`, `defer(task_id, receipt_handle, delay)` and
  `dequeue_is_cancel_safe()`. All five have trait defaults (`revoke` forwards to `cancel`,
  `is_revoked` answers `false`, `subscribe_revocations` answers `None`, `defer` forwards to
  `reject(requeue = true)`, `dequeue_is_cancel_safe` answers `false`), so an existing `impl Broker`
  keeps compiling — it just gets the inert behaviour until it overrides them. See Defaults, below,
  for why `dequeue_is_cancel_safe` exists and what changed alongside it
- `celers_broker_redis::QueueRateLimiter::Distributed`'s payload is now
  `Box<DistributedRateLimiter>` instead of `DistributedRateLimiter`: a `match` that binds the
  variant's field by value, or constructs it directly, stops compiling. Boxed because
  `DistributedRateLimiter` owns a `redis::Client`, whose `ConnectionAddr::TcpTls` variant now carries
  the rustls trust store and client-certificate chain (the `rediss://` work above) — roughly 3× the
  size of the `Local` variant, which an unboxed enum would charge to *every* `QueueRateLimiter`,
  including purely local ones
- `celers_protocol::result::ExceptionInfo::exc_message` is now `Vec<serde_json::Value>` instead of
  `String`: it models Python's `exc.args` — the list `celery.backends.base.Backend
  .exception_to_python` splats into the exception constructor as `*args` — rather than a
  pre-joined display string. `ExceptionInfo::new(exc_type, message)` still takes one string (it
  becomes the sole element); code that read `.exc_message` as a string should read
  `ExceptionInfo::message()` instead, which renders the same `", "`-joined form for display.
  `with_args(Vec<serde_json::Value>)` is the new builder for a multi-argument exception. See Fixed,
  below, for why the old shape lost information on the wire
- `celers_protocol::result::ResultMessage::children` is now `Vec<ResultChild>` instead of
  `Vec<Uuid>`: it models the whole result tree `celery.result.AsyncResult.as_tuple()` writes
  (`[[id, parent], group_results]`), not a flat list of ids, because Celery does not store child
  *ids* on the wire. `ResultMessage::with_children(Vec<Uuid>)` still compiles and still builds a flat
  list of parent-less, non-group children — the common case — but code that inspected `.children`
  elements as bare `Uuid`s must switch to `ResultChild::task_id` (or `.child_ids()` for the old flat
  view). `with_child_results(Vec<ResultChild>)` is the new builder for a whole tree. See Fixed, below

#### Defaults

- **Task coalescing now requires the same task id.** `WorkerConfig::coalesce_require_same_task_id`
  is new and defaults to `true`, which is the only lossless setting: coalescing then collapses
  genuine redeliveries of *one* task and nothing else. The previous behaviour — now reachable only
  by setting the flag to `false` — widened the coalescing key to `(task name, payload hash)`, so two
  *independent* submissions with identical arguments collapsed into one: the dropped submission
  never ran, never produced a result, and left its caller waiting forever on an `AsyncResult` that
  could never resolve. Turn it off only for idempotent, fire-and-forget work where nobody awaits the
  second submission
- **`Broker::try_dequeue`'s default now refuses instead of lying.** It used to poll `dequeue()`
  once and report a still-pending poll as `Ok(None)` ("nothing available") — wrong for any broker
  that does real I/O, since the first poll of a network call is *always* pending, so the default
  reported an empty queue no matter how much work was waiting. Worse, dropping that half-polled
  future abandoned whatever it had already committed to server-side, stranding a message until its
  redelivery window expired on a broker (SQS, most network brokers) that commits the pop before its
  first `.await`. The default now returns `Err(CelersError::Broker(..))` — the same shape of named
  refusal `enqueue_at`'s default already used — so a broker that has not overridden `try_dequeue`
  fails loudly instead of silently under-reporting its queue. Nothing in this crate's own defaults
  calls `try_dequeue` any more (see the next entry), so this only affects a caller that invokes it
  directly against a broker that never overrode it
- **`Broker::dequeue_batch`'s default no longer calls `try_dequeue`.** It now waits for the first
  message with `dequeue()` and then drains further messages by probing `queue_size()` before each
  further `dequeue()` call, stopping as soon as the probe reports zero — never dropping a `dequeue`
  future early, and never parking on an empty queue past the first, documented wait. This is what
  makes the `try_dequeue` change above safe for `dequeue_batch`: a broker that overrides neither
  method keeps working, at the cost of one `queue_size` round trip per extra message in the batch
  instead of one `try_dequeue`. `celers_kombu::core_adapter::KombuBrokerAdapter` (new this release)
  overrides both with real transport primitives regardless
- New `Broker::dequeue_is_cancel_safe()` (default `false`) lets a caller ask, before racing a
  broker's `dequeue()` future against a timer or a shutdown signal, whether the loser's dropped
  future can lose a message. Only `InMemoryBroker` currently overrides it to `true`; every network
  broker keeps the safe default

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
- `celers-broker-amqp` and `celers-broker-sqs`: new **default-on** `core-broker` feature (pulling
  in `celers-core` and `celers-kombu/core-adapter`), which is what makes `into_core_broker(..)`
  available — see Added, below. Building either crate with `default-features = false` drops the
  dependency on `celers-core` and leaves only the `celers-kombu` transport traits, matching the
  pre-0.3.1 surface; a build that already used `default-features = false` and named its features
  explicitly is unaffected either way
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

#### PostgreSQL advisory locks: an RAII guard API

- **`AdvisoryLockGuard`, and three ways to take one.** `PostgresBroker::try_acquire_advisory_lock`
  (non-blocking, `Ok(None)` when another session holds it), `acquire_advisory_lock` (blocks until
  granted) and `acquire_advisory_lock_within(lock_id, deadline)` (bounded wait — a zero deadline
  still makes exactly one attempt, so it degrades to the try form rather than to "never"). The guard
  **owns the pooled connection the lock was taken on**, which is what makes it correct:
  `pg_advisory_lock` is session-scoped, so an acquire and its release have to run on the same
  backend session, and a pool that hands out a different connection per statement cannot promise
  that. Dropping the guard releases the lock and returns the connection; `release()` does it
  explicitly and reports whether the lock was actually held. `DEFAULT_MIGRATION_LOCK_TIMEOUT` (60s)
  is the bound `migrate()` itself uses, so a migration that cannot get the lock now fails with a
  clear timeout instead of hanging on start-up forever. Covered against a real PostgreSQL by the new
  `tests_pg_locks.rs` (11 tests: mutual exclusion across two brokers, the blocking acquire waiting
  for a holder and then succeeding, the bounded acquire failing fast, a dropped guard leaving the
  pool usable, and `migrate()` both giving up when another session holds the migration lock and
  releasing it when it finishes)

#### One entry point for the live-service test matrix

- **`scripts/test-integration.sh`.** Every gated suite in this workspace needs a service and an
  environment variable, and until now the only record of which went with which was prose in
  `tests/integration/README.md`. The script brings the `docker-compose.yml` services up, exports the
  eight `CELERS_TEST_*`-family variables, runs each crate's gated suite the way that README
  documents, and prints a PASS/FAIL/SKIP summary — `--only redis|postgres|mysql|rabbitmq|localstack`
  to scope it, `--full` to add the Python-interop suite and a `docker build` smoke, `--keep-up`,
  `--down-v`. It provisions the two databases that must not be shared (`celers_backend_db_test`,
  `celers_upgrade_test`) with the root credentials, and only exports their variables if creation
  succeeded, so a provisioning failure produces an honest skip rather than a misattributed test
  failure.

  It waits for **real** health through the host-forwarded port rather than for `nc -z` to report the
  port open, and restarts a container that does not answer within ~60s before retrying. That is not
  defensive padding: on a fresh `docker compose up -d` during this release's own verification, four
  of the five services (PostgreSQL, MySQL, RabbitMQ, LocalStack) accepted TCP connections on the
  forwarded port while never completing a protocol handshake, and every one of them was fixed by
  exactly the restart the script performs. Without it the failure presents as a 30-second connect
  timeout in every gated test — indistinguishable, from the test output alone, from a real defect

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

#### RabbitMQ and SQS become worker-usable

- `celers_kombu::core_adapter::KombuBrokerAdapter<T>` implements `celers_core::Broker` over any
  transport that implements the new `CoreBrokerTransport` seam, joining `celers-kombu`'s
  message-transport traits (`publish`/`consume`/`purge`, which those crates already had) to the
  task-queue abstraction `celers_worker::Worker` actually consumes. `AmqpBroker::into_core_broker(queue)`
  and `SqsBroker::into_core_broker(queue)` build one; `queue` becomes both the adapter's queue and the
  underlying transport's own queue name, connecting lazily (no I/O at construction). Both crates
  enable the adapter **by default** — `core-broker`, forwarding to `celers-kombu/core-adapter` — so
  existing `AmqpBroker`/`SqsBroker` users get `.into_core_broker(..)` with no manifest change
- SQS's adapter additionally maps `dequeue_batch` / `enqueue_batch` / `ack_batch` / `defer` /
  `enqueue_after` onto native `ReceiveMessage(MaxNumberOfMessages)` / `SendMessageBatch` /
  `DeleteMessageBatch` / `ChangeMessageVisibility` / `SendMessage(DelaySeconds)` instead of the
  trait's one-request-per-message defaults
- **One transport, one lock.** `celers_core::Broker` takes `&self`; the transport traits take
  `&mut self`, so the adapter owns its transport behind one `tokio::sync::Mutex` and serializes every
  operation through it. The worker's main loop parks inside it while polling `dequeue`
  (`DEFAULT_POLL_TIMEOUT`, 1 second), during which an `ack` from an already-dispatched task on the
  same adapter briefly waits its turn. Sharing one adapter between several workers therefore
  serializes them against each other rather than letting them consume in parallel — give each worker
  its own transport
- Neither transport can address an already-queued message by id, so `cancel()` always answers
  `Ok(false)` and the revocation trait methods (`revoke`/`is_revoked`/`subscribe_revocations`) keep
  their inert `Broker` defaults — broker-fed revocation (above) does not reach RabbitMQ or SQS yet. A
  worker still refuses a *dequeued* revoked task through its own revocation registry
- **Not Celery-wire-compatible**: the body each transport publishes is still a JSON-serialized
  `celers_protocol::Message`, not kombu's own AMQP basic-properties framing or SQS message
  attributes, so a real Celery worker cannot consume from either. This closes the *worker-usability*
  gap, a separate, shallower claim than wire compatibility — see
  [docs/CELERY_COMPATIBILITY.md](docs/CELERY_COMPATIBILITY.md#brokers)
- Fixed alongside: `AmqpBroker::publish` (the `Producer` impl `celers-kombu`'s `publish`/`consume`
  traits use directly, and what the new adapter calls) published to the hardcoded exchange name
  `"celery"` instead of the configured `AmqpConfig::default_exchange`. The two agreed only when a
  caller never called `with_exchange(..)`; otherwise the queue-declare/bind used one exchange and
  `publish` published to another, and RabbitMQ answered with a channel-closing 404 — visible from
  `into_core_broker` as `enqueue` failing while `enqueue_batch` (which already read the configured
  exchange) succeeded
- The facade's URL-based factory, `celers::broker_helper::create_broker(broker_type, url, queue)`,
  gained `"amqp"`/`"rabbitmq"` and `"sqs"` arms wired to the adapter above: it builds an
  `AmqpBroker`/`SqsBroker` and hands back `.into_core_broker(queue)` as a `Box<dyn
  celers_core::Broker>`, so code that only has a broker-type string and a URL (a `celers config`
  style deployment) can now get a worker-usable RabbitMQ or SQS broker the same way it already could
  for `redis`/`postgres`/`mysql`. Previously both arms fell through to the generic case and returned
  `BrokerConfigError::UnsupportedBrokerType` unconditionally — even in a build with the `amqp`/`sqs`
  feature compiled in. `SqsBroker` has no connection URL of its own (the AWS SDK reads its endpoint
  from the environment), so the `sqs` arm's `url` parameter is intentionally unused. A build with the
  feature *off* now reports `BrokerConfigError::FeatureNotEnabled` for these two types instead of the
  same `UnsupportedBrokerType` used for a genuinely unrecognised string — accurate, since the type is
  not unsupported, only not compiled into this build. Live-verified end to end against a real
  RabbitMQ: `broker_helper::tests::live_amqp::create_broker_amqp_reaches_a_real_rabbitmq_end_to_end`
  (gated on `CELERS_TEST_AMQP_URL`) enqueues, dequeues and acks a task through the URL-built broker

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
  their own modules. Also split this release: `celers-backend-db/src/lib.rs` (2334 → 239 lines, into
  `mysql_backend.rs` / `postgres_backend.rs` / `result_compression.rs`);
  `celers-worker/src/worker_core/tests.rs` (2308 lines, into a 16-file `worker_core/tests/`
  directory, none over 600 lines); `celers-worker/src/sandbox.rs` (2049 → 600 lines, into
  `sandbox/config.rs` / `error.rs` / `rlimit_impl.rs` / `seccomp_impl.rs` / `stats.rs` /
  `tests.rs`). One file grew past the limit while this release's own tests were added:
  `celers-beat/src/tests/tests_schedule.rs` is now 2004 lines
- `celers-cli`'s `async-trait` dependency moved from `[dev-dependencies]` to `[dependencies]`: the
  crate's built-in demo tasks (`commands::worker`'s `--demo-tasks`) are production code, not
  test-only, and were the only reason the dependency existed at all
- The Docker image no longer installs `ca-certificates`: on Debian bookworm it carries a hard
  `Depends: openssl`, which would put OpenSSL into a Pure-Rust image through the OS package manager.
  `celers-cli` resolves TLS through the compiled-in `webpki-roots` bundle and never reads
  `/etc/ssl/certs`

### Fixed

- **`celers-broker-postgres`'s entire result store addressed a table no migration created.**
  `results.rs` (`store_result` / `get_result` / `delete_result` / `archive_results` /
  `get_results_batch` / `delete_results_batch`), `analytics.rs`'s `store_results_batch` and
  `db_monitoring.rs`'s `analyze_tables` / `vacuum_tables` all read and wrote a broker-side result
  table, and **no migration in the crate had ever created it** — every one of those calls failed
  against a live server with `ERROR: relation "..." does not exist`. It went unnoticed because none
  of it was reachable from a test that touched a real database. New `009_broker_results.sql` creates
  `celers_broker_results`, with the DDL derived from the binds rather than the other way round:
  `task_id UUID PRIMARY KEY` because every write ends in `ON CONFLICT (task_id) DO UPDATE` and every
  read casts through `$n::text::uuid`; `task_name NOT NULL DEFAULT ''` because `store_results_batch`
  does not bind it while the read side deserialises it into a non-optional `String`; `result JSONB`
  nullable, because the crate deliberately distinguishes a SQL `NULL` from the JSON literal `null`;
  a `CHECK` transcribed from `TaskResultStatus`'s own `FromStr`. Its four indexes are named
  `idx_broker_results_*` rather than `idx_task_results_*` because PostgreSQL index names are
  database-wide and `celers-backend-db` already owns the latter — the table-name collision one level
  down. Covered by the new `tests_pg_results.rs` (24 tests against a real PostgreSQL 16, including
  the `NULL`-vs-`null` distinction, the upsert conflict branch, and batch/archive scoping)

- **Two PostgreSQL analytics entry points read a `celers_tasks.updated_at` column that did not
  exist.** `get_state_transition_history` projects it, windows over it
  (`LAG(..) OVER (PARTITION BY id ORDER BY updated_at)`), filters and orders by it; and
  `detect_abnormal_state_duration("processing", ..)` ages a claimed task through
  `COALESCE(started_at, updated_at)`. Both failed against a live server with
  `column "updated_at" does not exist`. New `010_task_updated_at.sql` adds it, and roughly thirty
  `UPDATE celers_tasks` statements across `sql.rs`, `convenience.rs`, `advanced_ops.rs`,
  `workflows.rs`, `analytics.rs`, `dlq.rs`, `db_monitoring.rs` and `scheduling.rs` now carry an
  explicit `updated_at = NOW()`. Explicit rather than a `BEFORE UPDATE` trigger on purpose: a
  trigger hides the write from `EXPLAIN`, from `pg_stat_statements` and from a plain reading of the
  SQL, and puts the behaviour in the database rather than in the crate under review — and the
  explicit form is what makes the column portable to the sibling brokers.

  The migration adds the column **nullable**, backfills it from the row's real last-touch time
  (`COALESCE(completed_at, started_at, created_at)`) and only then pins `DEFAULT NOW()` / `NOT
  NULL`. Doing it in one `ADD COLUMN ... NOT NULL DEFAULT NOW()` would stamp every pre-existing row
  with the migration's own timestamp, making every historical task look freshly touched and
  under-reporting every genuinely stuck task for a whole threshold window. The `WHERE updated_at IS
  NULL` guard is what makes the file replayable. Both properties were verified against a live
  PostgreSQL on a database put into the pre-migration state by hand — three rows exercising all
  three `COALESCE` branches each received their own true last-touch time rather than `NOW()`, and a
  forced replay of the migration left those values untouched

- **A MySQL claim on one queue starved every other queue.** `dequeue` was a single locking
  `SELECT ... ORDER BY priority DESC, created_at ASC LIMIT n FOR UPDATE SKIP LOCKED`, and under
  InnoDB's default `REPEATABLE READ` that shape locks far more than the rows it returns, in two
  independent ways. *Across queues*: a locking range scan takes next-key locks, which cover the
  index record **after** the scanned range — measured on this crate's own MySQL 8.0 via
  `performance_schema.data_locks`, a claim on queue `zzprobe_a` held `X` on the
  `idx_tasks_queue_dequeue` record belonging to queue `zzprobe_b`, so while that claim was open
  queue B's own `dequeue()` skipped its only pending row and returned `Ok(None)`. `SKIP LOCKED`
  skips a locked *record*, and a next-key lock locks the record. *Within a queue*:
  `ORDER BY priority DESC, created_at ASC` cannot be served in order from the dequeue index once
  `scheduled_at <= NOW()` makes it a range, so MySQL filesorts — and a filesort reads every
  qualifying row before `LIMIT` applies, while a locking read locks every row it reads, so one
  `dequeue()` locked the queue's entire due backlog for the life of its transaction.

  Neither is fixable by rewriting the statement: `READ COMMITTED` cannot be reached through
  `oxisql`'s `Connection::transaction()` (it hardcodes `TxOpts::default()`), and
  `SET TRANSACTION ISOLATION LEVEL` inside an open transaction is `ERROR 1568`. The claim is
  therefore split into a **non-locking** consistent read that picks candidate ids in priority order
  (so its range scan cannot touch a neighbouring queue) followed by a locking read **by primary key
  only** — `WHERE id IN (..)`, `FORCE INDEX (PRIMARY)`, no `ORDER BY`, no `LIMIT` — which takes
  record-only (`REC_NOT_GAP`) locks and, with no filesort, locks nothing the caller does not claim.
  `claim_pending_rows` walks the candidate list in chunks sized to what is still needed, so a row
  another worker locked costs one extra round trip rather than a lost claim. Proved against a live
  MySQL by `a_held_claim_on_one_queue_does_not_block_another_queue` and
  `a_held_batch_claim_does_not_block_a_neighbouring_queue`, which assert the neighbour claims on its
  **first** attempt with a claim held open, and by
  `concurrent_claims_on_one_queue_partition_the_backlog`. The same rewrite removed two further
  defects the three hand-written copies of this SQL had all drifted into: `FOR UPDATE SKIP LOCKED`
  emitted *before* `LIMIT` (a parse error on MySQL, valid only on PostgreSQL, which is how the shape
  reached a MySQL-only crate) and a missing `queue_name` predicate that let brokers on different
  logical queues steal each other's tasks. All dequeue paths now build their statements through one
  pair of builders in `sql_text.rs`

- **MySQL bulk maintenance was not deadlock-retried.** The task-lifecycle statements
  (`dequeue`/`ack`/`reject`/`cancel`) gained `with_deadlock_retry` earlier in this release, but the
  bulk maintenance ones did not — and they are the statements *most* able to deadlock, because each
  takes many row locks in one transaction while ordinary traffic keeps running. `archive_completed_tasks`,
  `recover_stuck_tasks`, `purge_all`, `purge_by_state` and `purge_by_task_name` now go through the
  same bounded, jittered retry loop. Each is idempotent — a row already in its target state matches
  nothing on the retry — so restarting cannot double-apply anything

- **An SQS DLQ dry run silently blinded the replay that followed it.**
  `ReplayManager::replay_from_dlq(.., dry_run = true)` promised that inspected messages are
  "returned to the DLQ", but its only mechanism for returning them was the visibility timeout:
  `get_dlq_messages` issues a real `ReceiveMessage`, which makes every message it reads invisible
  for the queue's visibility timeout (30 seconds by default, up to 12 hours) even though nothing is
  deleted. The documented operator workflow — rehearse, read the count, then run for real — therefore
  moved nothing and reported success, because the real run received an empty DLQ. Messages the
  `ReplayFilter` rejected had the same problem: "skipped and left in the DLQ" meant "invisible for
  the next 30 seconds". Both are now genuinely put back, with a `ChangeMessageVisibility` to zero
  issued against the queue the delivery tag names once the receive loop is over — after the loop, so
  a released message cannot be received and counted twice by the same run, and through `reject_on`
  so an active visibility heartbeat is stopped rather than left to extend the timeout straight back
  out. A release that fails is logged, not raised: the message still returns when its timeout
  expires. Covered end-to-end against LocalStack by
  `celers-broker-sqs`'s `tests/localstack.rs::dlq_redrive_moves_the_message_and_empties_the_dlq`,
  which failed on exactly this before the fix

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
  ordering, polar day and invalid coordinates. `golden_hour_begin`/`_end`, initially left as a flat
  offset from sunrise/sunset, is now also a true `SolarEvent::Elevation` solve (0° for the morning
  boundary, -6° for the evening one) — there is no remaining approximation in the solar branch

- **A `MessageBuilder` envelope killed a real Celery worker's event loop.** `MessageProperties`
  never serialized `delivery_tag` or `delivery_info`, both of which
  `kombu.transport.virtual.base.Message.__init__` indexes directly — no `.get()`, no default — so a
  message built and published through the ordinary producer path raised `KeyError` *inside the
  consumer callback*, which does not just fail one task: it kills the whole worker's event loop.
  Both fields now serialize (`delivery_tag` fresh per message, the way kombu's own producer mints
  one; `delivery_info` as `{exchange, routing_key}`, the routing key naming whichever queue
  `.queue(..)`/`.routing_key(..)` selected), verified by publishing a `MessageBuilder` envelope
  **unpatched** to a queue a real Celery worker consumes and executes
  (`tests/python-compat/test_celers_to_python.py::test_message_builder_envelope_is_deliverable_as_is`)
- **`ResultMessage::children` and `ExceptionInfo::exc_message` did not model what Celery actually
  writes.** `children` was a flat `Vec<Uuid>`, but Celery's `AsyncResult.as_tuple()` writes whole
  result trees (`[[id, parent], group_results]`) — any retried, chained or grouped task's record now
  round-trips through the new `ResultChild` type instead of failing to parse or silently discarding
  the parent chain. `exc_message` joined a multi-argument exception's `exc.args` into one string with
  `", "`, so `raise ValueError("a", "b")` came back as one argument instead of two, and a non-string
  argument (`OSError(2, "no such file")`) lost its type, rendered through `Display` instead of kept
  as JSON. Both are now interop-verified against a real Celery worker: the `children` fix by feeding
  Celery's own `as_tuple()` output back through `result_from_tuple`
  (`test_celers_to_python.py::test_celers_parses_a_celery_record_that_has_children`), the
  `exc_message` fix by letting Celery's `exception_to_python` rebuild a real `OSError(2, ..)` from a
  CeleRS-written record (`::test_a_python_exception_rebuilt_from_a_celers_record_keeps_its_args`).
  See Breaking changes → Source compatibility for the field-type changes this required
- `celers_broker_redis::ResultBackend::store_task_result` now `PUBLISH`es the same bytes on a
  channel named after the result key, in the same pipeline as the `SET`/`SETEX` — matching Celery's
  own `celery.backends.redis.BaseKeyValueStoreBackend._set`. A writer that only `SET`s leaves every
  client blocked in `AsyncResult.get()`'s Pub/Sub wait until its poll-interval fallback, rather than
  waking immediately
- `celers-backend-db`: a task stored as `TaskResultValue::Ignored { error }` now round-trips its
  error text through `meta.ignored_error` instead of losing it, and — the more consequential half —
  a *later* non-`Ignored` store for the same task now unconditionally clears the stale
  `ignored_error` marker instead of leaving it behind, which previously made a task that was once
  ignored and later completed with a real result keep reading back as `Ignored`
- **The partitioning migration could not be applied.** Two functions in
  `crates/celers-broker-postgres/migrations/003_partitioning.sql`
  (`create_tasks_partitions_range`, `maintain_tasks_partitions`) declared a PL/pgSQL variable named
  `current_date` — not merely shadowing the `CURRENT_DATE` SQL-standard constant, but colliding with
  it as a reserved word, which PostgreSQL rejects at parse time: `CREATE OR REPLACE FUNCTION` itself
  fails with `ERROR: syntax error at or near "current_date"` (confirmed against a real PostgreSQL 16
  — the assignment `current_date := ...` is what the parser trips on). The whole migration file
  would abort on this statement before either function was created, taking automatic partition
  creation and maintenance out with it. Renamed to `v_current_date` throughout both functions

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
- `MessageBuilder::build()` and `v5::build_v5_message` now **stamp** `argsrepr` / `kwargsrepr`
  themselves, computed from the same args/kwargs the message carries; previously neither ever set
  them, so every message built through the ordinary producer path showed a monitor no argument
  preview at all (not merely a wrong one). An explicit `.header("argsrepr", ..)` still overrides the
  computed value
- The Docker image builds again: the builder stage was pinned to `rust:1.75`, far below the
  dependency MSRV, and its `COPY --from=builder` named `target/release/celers-cli` — a binary the
  build never produces, because `celers-cli`'s `[[bin]]` is named `celers`
- The docker-compose monitoring stack no longer mounts nonexistent paths: `docs/prometheus.yml`,
  `docs/grafana/datasources/` and `docs/grafana/dashboards/` (with a real `celers-overview.json`)
  are committed, and `docs/DEPLOYMENT.md` points at what exists

#### PostgreSQL and MySQL, verified against real servers for the first time

The gated live-service suites for `celers-broker-postgres`, `celers-broker-sql` and
`celers-backend-db` had never actually been run against a real server before this release — every
integration test that needed one was `#[ignore]`d, so the workspace stayed green while these three
crates were, in places, non-functional. Running them for the first time (see Testing and proof,
above, and [tests/integration/README.md](tests/integration/README.md)) found the defects below, all
now fixed and re-verified live.

- **PostgreSQL rejected almost every UUID- or JSONB-bound statement — a release blocker.**
  `oxisql-postgres` renders a `Value::Uuid`/JSON parameter as *text*, but `tokio-postgres` always
  binds parameters in PostgreSQL's *binary* wire format, so a bare `$n` against a column PostgreSQL
  infers as `uuid` or `jsonb` fails at bind time: `incorrect binary data format in bind parameter n`
  for a UUID, `unsupported jsonb version number 123`/`91`/`110` (the first byte of `{`/`[`/a bare
  number) for JSON text presented where binary `jsonb` belongs. Nothing in-process could ever catch
  this — it only exists on the wire to a real server. `celers-broker-postgres` had 60 such bare-`$n`
  sites (every `celers_tasks.id`/`celers_revoked_tasks.task_id` placeholder, including the generated
  `IN (...)` lists), and it made the crate **non-functional against a live database**: 36 of its 210
  gated tests failed the moment they were first run for real, including the plain enqueue path.
  `celers-backend-db`'s PostgreSQL result backend had the identical defect on `result_data`/`extra`/
  `task_ids` (`postgres_backend.rs`) and on `celers_events.payload` (`event_persistence.rs`) — so
  **storing a task result with a payload, the crate's primary job, also failed outright** against a
  live server. Fixed everywhere by pinning the placeholder's inferred type with an explicit
  `::text::uuid` / `::text::jsonb` cast ahead of the server-side conversion —
  `crate::row_ext::uuid_param`/`json_param`'s doc comments in both crates derive the fix in full, and
  `celers-broker-postgres::sql::uuid_in_clause` centralises the generated `IN (...)` case so no call
  site can omit the cast by hand. Reading a `JSONB` column back needed the mirror fix,
  `SELECT payload::text AS payload` / `metadata::text AS metadata`, because `oxisql-postgres` decodes
  JSON by asking `tokio-postgres` for a `String`, which does not accept a raw `jsonb` OID either. The
  same "the server infers a stricter binary type than the driver can produce" hazard also reached
  `celers-broker-postgres`'s analytics queries, on `EXTRACT(EPOCH FROM ...)` results (need
  `::double precision`/`::BIGINT`) and interval-multiplication parameters (`$n::bigint`). Proven
  live: `celers-broker-postgres` was **230/230 against a real PostgreSQL 16** when this landed
  (`tests_pg.rs` plus 18 new regression tests in `tests_pg_binds.rs`, gated on
  `CELERS_TEST_POSTGRES_URL`; **271/271** by release, once the result-store and advisory-lock suites
  below were added), and
  `celers-backend-db`'s PostgreSQL half passes in full against the same server (`DATABASE_URL`)
- **Concurrent auto-migration raced on both engines.** Every CeleRS component migrates its own
  schema on start-up, and a normal deployment starts many workers at once, so `CREATE TABLE
  IF NOT EXISTS`/`CREATE INDEX IF NOT EXISTS`'s existence-check-then-create is not atomic against a
  second session doing the same thing — the loser failed startup with an opaque `db error`
  (PostgreSQL: `duplicate key value violates unique constraint "pg_type_typname_nsp_index"`).
  `celers-backend-db` now serializes its three PostgreSQL migration call sites
  (`PostgresResultBackend::migrate`, `DbEventPersister`'s migration, `DbLockBackend::ensure_table`)
  on one transaction-scoped `pg_advisory_xact_lock` (new `crate::pg_ddl::advisory_locked_migration`
  — the `_xact_` variant self-releases on commit *or* rollback, so it cannot leak across a pooled
  connection's next use the way the session-scoped lock would). MySQL has no advisory lock, so
  `celers-broker-sql`'s migration runner instead treats the specific errors a *concurrent* migrator
  produces as success and retries the one that means "the other side is still mid-statement":
  `1061 Duplicate key name` (another migrator's index already landed), `1060 Duplicate column name`
  (another migrator's `ALTER TABLE` already landed) and `1213 Deadlock found` (retried through the
  same `with_deadlock_retry` the application statements use, below). Its migrations-tracking insert
  also switched to `ON DUPLICATE KEY UPDATE`, so two migrators applying the same file both succeed
  instead of one hitting `Duplicate entry '...' for key celers_migrations.version`. Before this fix,
  any one of these aborted `migrate()` part-way — including on `009_queue_name.sql`, whose column
  the statistics queries below need, leaving the broker failing every one of them with
  `Unknown column 'queue_name'`
- **`celers-backend-db`'s MySQL `migrate()` failed outright, on every call, against every MySQL
  database.** Its statement splitter fed `001_init_mysql.sql`'s `DELIMITER //`-terminated
  `CREATE PROCEDURE` bodies to the server verbatim; `//` is a client-side directive, not SQL, so the
  server rejected it with `ERROR 1064 ... near '//'` and aborted the whole migration —
  `cleanup_expired_results` and `chord_increment_counter` were never created on **any** MySQL
  database this crate ever migrated. A second defect in the same splitter dropped the leading
  `-- comment` lines that precede a statement, which silently defeated the `information_schema`
  existence guard on the first index of a re-run migration, failing a second `migrate()` call with
  `1061 Duplicate key name`. Both are fixed by a (deliberately narrow — see the new parsing helpers'
  own documentation) statement recogniser that finds `CREATE INDEX`/`CREATE PROCEDURE` regardless of
  a leading comment and checks `information_schema.statistics`/`.routines` before creating either,
  plus routing `1060`/`1061`/`1213` through the same idempotent-retry handling as the previous entry.
  Both defects were invisible before this release because the live tests that would have caught them
  are `#[ignore]`d and had never actually been run against a server
- **`MysqlAnalytics::task_stats` crashed on an empty result set.** `SUM(...)` over zero matching
  rows is SQL `NULL`, not `0`, and the query read each aggregate as a non-optional `i64` — so a
  `task_stats` call over any time window with no matching tasks (an idle queue, a narrow window)
  failed the whole query with `type mismatch: expected I64/F64/Decimal, got Null` instead of
  reporting all-zero counts. Fixed by reading each `SUM` through the existing
  `row_ext::opt_decimal_i64_from_row` and defaulting a `None` to `0`, which is exactly what "no rows
  contributed" means
- **`celers-broker-sql`'s `dequeue`/`ack`/`reject`/`cancel` surfaced MySQL's `1213 Deadlock found`
  as a hard failure.** A claim, an ack and a revocation-driven cancel all take row locks on
  `celers_tasks` that can legitimately cycle under concurrent load — a live parallel run of this
  crate's own gated suite reproduced it — and InnoDB's documented remedy is to restart the losing
  transaction. New `mysql_error::with_deadlock_retry`/`with_deadlock_retry_celers` (a bounded,
  jittered restart loop) now wraps every one of those statements, including the multi-row batch
  cancel; each is idempotent (a row already in its target state simply matches nothing on the
  retry), so restarting cannot double-apply anything. `BeforeDequeue`'s documented behaviour — a
  rejecting hook rolls the claim back rather than losing the message — is preserved across a
  retried claim
- **`celers-broker-sql`'s dead-letter move could not run.** `001_init.sql` defines `move_to_dlq` as
  a stored procedure, but MySQL rejects `CREATE PROCEDURE` over the prepared-statement protocol
  (`1295`) and `oxisql-mysql` has no text-protocol escape hatch to reach for — so the procedure
  never existed on any migrated database, and `CALL move_to_dlq(?)` could only fail: a task that
  exhausted its retries was never actually moved to the dead-letter queue. New `dlq_move.rs` issues
  the procedure's own two statements directly (copy the row into `celers_dead_letter_queue`, then
  delete it) inside the same transaction that gave the procedure its atomicity, for both the
  single-task path (`Broker::reject`) and the batch path (`broker_chain.rs`'s batch DLQ move).
  Live-verified: `tests_hardening::retry_exhaustion_moves_the_task_into_the_dead_letter_queue` and
  `::reject_batch_moves_an_exhausted_task_into_the_dead_letter_queue`
- **Several `celers-broker-sql` read/diagnostic methods reported database-wide numbers on a
  queue-scoped broker.** `count_by_state_quick`, `get_task_age_distribution`,
  `get_retry_statistics`, `list_active_workers`, `get_worker_statistics`/`get_all_worker_statistics`,
  `get_queue_health`/`has_capacity` and `list_scheduled_tasks`/`count_scheduled_tasks` all queried
  `celers_tasks` with no `queue_name` filter — the same class of bug `009_queue_name.sql` was
  written to close for `queue_size`/`get_statistics`, left open in every read method added since.
  Two brokers pointed at different queues in the same database saw each other's tasks in their own
  stats, and a broker's own "pending" count could exceed everything its own `dequeue` would ever
  hand out. Every one of them is now scoped to `WHERE queue_name = ?`, matching `queue_size`. This
  is also what made this crate's own gated suite order-sensitive —
  `queues_are_isolated_from_each_other` and a `test_concurrent_dequeue` count mismatch
  (`left: 50, right: 20`) were two different symptoms of the same missing filter — and is why the
  whole suite is live-verified against a real MySQL 8 — **217/217** when this landed, **236/236** by
  release — both under `nextest`'s default parallel execution and `--test-threads=1`
- Corrected the SPDX license header in 29 `celers-broker-sqs`/`celers-kombu` source files from the
  stale `MIT OR Apache-2.0` to `Apache-2.0`, matching the workspace's actual single-license
  `license = "Apache-2.0"` (`Cargo.toml`, `LICENSE`) — the dual-license form was never accurate for
  this project and is not present anywhere else in the workspace

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

### Known Limitations

- ~~**`celers-backend-db` and `celers-broker-sql` collide on the MySQL table name
  `celers_task_results`.**~~ **Fixed before release.** This shipped as an open limitation through
  most of the campaign and is now closed by renaming the broker's table to `celers_broker_results`,
  with an automatic in-Rust upgrade for existing databases — see **Breaking changes → Database
  schema**, above, for the operator-facing detail. The configuration that used to fail (both crates
  on one MySQL database) is now a covered, passing case: `celers-broker-sql` **236/236** and
  `celers-backend-db` **151/151** against the same `docker-compose` database, in that order, on a
  volume created fresh for the test. The collision is also regression-tested from both directions —
  `broker_and_result_backend_coexist_on_one_database` migrates both crates onto one database and
  round-trips a result through each, and `tests_hardening::migration_upgrade` proves the rename
  leaves the result backend's identically-named table strictly alone

- **The AMQP suite has an intermittent failure under parallel execution.** Across five consecutive
  full runs of `celers-broker-amqp`'s gated suite against one RabbitMQ, one run had a single
  failure (`tests::test_integration_queue_stats`), in the run immediately following a container
  restart; the test passes on its own, and the four other runs — including three back to back —
  were 313/313. The residual is most likely test-to-test interference through the shared broker's
  management API rather than a defect in the crate, but it has not been root-caused, so it is
  recorded here rather than claimed fixed

Final verified state for 0.3.1 (this hardening campaign): workspace builds and
`clippy --all-targets -- -D warnings` clean with both `--all-features` and default features,
`cargo fmt --all --check` clean, and `cargo deny check` clean on all four checks
(advisories/bans/licenses/sources; empty `[graph] exclude`).
`cargo nextest run --workspace --all-features`: **7,791 tests run, 7,791 passed, 0 failed, 104
`#[ignore]`d**; default features: **7,509 run, 7,509 passed, 0 failed, 97 `#[ignore]`d**.
`cargo test --doc --workspace --all-features`: **1,178 passing doctests, 0 failed**, 138 more are
```` ```ignore ```` and never compile.

Live-service gated suites, brought up via the root `docker-compose.yml` (redis, postgres, rabbitmq,
mysql, localstack) and — for PostgreSQL and MySQL — on **volumes destroyed and recreated with
`docker compose down -v` first**, so the schema each suite ran against was built from nothing by the
migrations in this tree, then re-run a second time against the now-populated volumes to exercise the
replay and upgrade paths. Both passes were identical: `celers-broker-redis` + `celers-backend-redis`
+ `celers-worker` + `celers-cli` together (**2,926/2,926**, one shared Redis, via
`scripts/test-integration.sh --only redis`), `celers-broker-amqp` (**313/313** against a real
RabbitMQ — see Known Limitations for one intermittent), `celers-broker-postgres` (**271/271**
against a real PostgreSQL 16), `celers-broker-sql` (**236/236** against a real MySQL 8, under both
`nextest`'s default parallel execution and `--test-threads=1`), `celers-broker-sqs` (**436/436**
against LocalStack), and the `celers` facade with all eight variables exported (**185/185**).
`celers-backend-db` passes **151/151** with `DATABASE_URL` on the PostgreSQL server and `MYSQL_URL`
on a separate MySQL database *and*, now that the table-name collision is fixed, **151/151 on the
very database `celers-broker-sql` had just migrated**. `tests/python-compat` (a real Celery 5.6.3 +
kombu 5.6.2 client and worker): **38/38**.

The PostgreSQL migration set was additionally proved to build its schema from nothing by
`migrate()` itself, not merely by the `docker-entrypoint-initdb.d` mount that a fresh compose volume
uses: against a database `initdb` never touched, `PostgresBroker::migrate()` created all **11**
tables and recorded **9** ledger rows — 000 is applied untracked, and `003_partitioning.sql` is an
opt-in file `migrate()` deliberately does not apply — after which the full suite passed 271/271 on
it. All counts measured 2026-08-26 against the tree this entry describes.

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
