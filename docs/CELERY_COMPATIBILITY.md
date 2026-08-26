# CeleRS / Python Celery Compatibility

This document states, row by row, what CeleRS can and cannot do with a real Python Celery — and
what evidence backs each claim. It is deliberately conservative: a row is only marked
interop-verified when a test in this repository puts CeleRS on one side of the wire and *Celery* on
the other.

**The one-sentence summary:** the **protocol layer** (`celers-protocol`) is interoperable and
proved so against Celery 5.6.3; the **broker and result backend are not**, because they do not yet
speak the Celery wire format. A Python Celery worker and a CeleRS worker therefore **cannot share a
queue today**. Everything below elaborates on that.

## How to read the Status column

| Status | Meaning |
|---|---|
| ✅ **Interop-verified** | A test exchanges this with a real Python Celery — a live round trip in [`tests/python-compat/`](../tests/python-compat/), or a verbatim Celery wire capture in [`crates/celers-protocol/tests/fixtures/`](../crates/celers-protocol/tests/fixtures/) checked by `crates/celers-protocol/tests/celery_golden.rs`. The proving test is named. |
| 🟢 **Implemented** | Implemented and covered by Rust unit/integration tests. **No Celery was on the other end.** Wire-compatible by construction and by review, not by experiment. |
| 🟡 **Partial** | Works with a documented caveat or only on some paths. The caveat is stated. |
| 🔴 **Not interoperable** | Implemented in CeleRS, but the bytes are CeleRS-shaped: Celery cannot read them and CeleRS does not read Celery's. |
| ⛔ **Not implemented** | Absent. |

Live round trips need a Redis and a Python with Celery installed; without them the suite prints a
`SKIPPED:` line and exits 0. See [Running the interop suite](#running-the-interop-suite).

## Quick start

### Python Celery configuration

These are the settings the interop suite itself pins, each because CeleRS depends on it — see
[`tests/python-compat/celeryconfig.py`](../tests/python-compat/celeryconfig.py) for the reasons in
full.

```python
from celery import Celery

app = Celery('myapp', broker='redis://localhost:6379/0')
app.conf.update(
    task_serializer='json',      # the only content type both runtimes speak
    result_serializer='json',
    accept_content=['json'],
    result_accept_content=['json'],

    task_protocol=2,             # v1 puts the task name in the body; CeleRS models v2 only
    timezone='UTC',
    enable_utc=True,             # CeleRS renders every eta/expires as a UTC RFC 3339 instant
)
```

### CeleRS configuration

JSON is the default and no configuration change is needed. What *is* needed is knowing which layer
you are interoperating at — see [What is not interoperable](#what-is-not-interoperable).

## Compatibility matrix

### Protocol

| Protocol | Python Celery | CeleRS | Status | Proof |
|---|---|---|---|---|
| v1 (legacy) | ✅ | ⛔ | ⛔ Not implemented | v1 puts the task name in the body; `celers_protocol::Message` models v2 only |
| v2 (current) | ✅ default | ✅ default | ✅ Interop-verified | `celery_golden.rs::every_captured_celery_envelope_parses_and_validates`, and every round trip in `tests/python-compat/` |
| "v5" | — | ✅ | 🟢 Implemented (CeleRS-only) | `ProtocolVersion::V5` is a **CeleRS-internal** version label ("Celery 5.x+"), not something proved to exist on the Celery wire. Nothing in the interop suite or the captures exercises it: `celeryconfig.py` pins `task_protocol = 2`, and every fixture was recorded from Celery 5.6.3 at protocol 2. Treat it as a CeleRS-to-CeleRS feature until a capture says otherwise |

Celery 5.6.3 headers CeleRS does not model (`shadow`, `replaced_task_nesting`, `stamped_headers`,
`stamps`, `ignore_result`) are **tolerated on parse** — verified by
`celery_golden.rs::celery_5_6_only_headers_are_tolerated`.

### Serialization

| Format | Python Celery | CeleRS | Status | Notes |
|---|---|---|---|---|
| JSON | ✅ default | ✅ default | ✅ Interop-verified | The only format the interop suite exercises, and the only one it asserts byte shapes for |
| MessagePack | ✅ | ✅ `msgpack` feature | 🟢 Implemented | Round-tripped in Rust; never exchanged with Celery in a test |
| YAML | ✅ | ✅ `yaml` feature | 🟡 Partial | `application/x-yaml` is registered and auto-detected; no Celery-side test |
| Pickle | ✅ | ⛔ | ⛔ Not implemented | Deliberate: arbitrary-code-execution risk. Set `task_serializer='json'` on the Python side |

### Task fields (protocol v2 headers and embed)

Every row here is proved by a named test against real Celery. The `→` direction column reads
"Python publishes → CeleRS consumes" (P→C) and "CeleRS publishes → a real `celery` worker executes"
(C→P).

| Field / behaviour | Status | Direction | Proof |
|---|---|---|---|
| `task`, `id`, `lang` | ✅ Interop-verified | both | `test_python_to_celers.py::test_positional_arguments`, `test_celers_to_python.py::test_positional_arguments` |
| Positional args (list *and* tuple `argsrepr`) | ✅ Interop-verified | both | `celery_golden.rs::positional_arguments_survive_the_wire`; `test_python_to_celers.py::test_celers_reads_the_arguments_celery_actually_sent` |
| Keyword args (incl. `True` / `None`) | ✅ Interop-verified | both | `celery_golden.rs::keyword_arguments_survive_the_wire`; `test_celers_to_python.py::test_keyword_arguments` |
| `countdown` → absolute `eta` | ✅ Interop-verified | P→C | `celery_golden.rs::countdown_reaches_the_wire_as_an_eta_header`; `test_python_to_celers.py::test_countdown_arrives_as_an_absolute_eta` |
| `eta` / `expires` (ISO 8601 UTC) | ✅ Interop-verified | both | `celery_golden.rs::eta_and_expires_survive_the_wire`; `test_python_to_celers.py::test_eta_and_expires_arrive_as_utc_instants`; `test_celers_to_python.py::test_eta_delays_execution` (a real worker honours a CeleRS-written `eta`) |
| `root_id` / `parent_id` | ✅ Interop-verified | both | present and asserted in every envelope fixture; `celery_golden.rs::every_captured_celery_envelope_parses_and_validates` |
| `group` / `group_index` | ✅ Interop-verified | P→C | `celery_golden.rs::a_group_member_carries_the_group_id_and_index`; `test_python_to_celers.py::test_group_members_carry_their_group` |
| Chain link (the `chain` embed) | ✅ Interop-verified | P→C | `celery_golden.rs::a_chain_head_carries_the_next_link_in_the_embed_dict`; `test_python_to_celers.py::test_chain_of_two_tasks` |
| `argsrepr` / `kwargsrepr` as Python literals | ✅ Interop-verified | C→P | `celery_golden.rs::argsrepr_matches_celerys_own_saferepr`, `::the_repr_rules_are_the_python_ones_not_the_rust_ones`; `test_reprs.py::test_celers_reproduces_every_recorded_repr`, `::test_no_rust_debug_formatting_reaches_the_wire` |
| `retries` + `retry(countdown=…)` | ✅ Interop-verified | C→P | `test_celers_to_python.py::test_retry_with_countdown` — a Python worker retries a CeleRS-published task |
| Priority (`properties.priority`) | 🟢 Implemented | — | Present in the captures and modelled; no test drives priority ordering across the two runtimes |
| Callbacks (`link`) / error callbacks (`link_error`) | 🟢 Implemented | — | The `callbacks` / `errbacks` embed slots are modelled and round-tripped in Rust; not exercised against Celery |
| Immutable signatures (`.si()`) | 🟢 Implemented | — | Canvas-level; not exercised against Celery |
| `timelimit`, `origin`, `shadow`, `ignore_result` | 🟡 Partial | P→C | Parsed and tolerated inbound; CeleRS emits `timelimit` and `origin` but not the rest |

### Result records

| Aspect | Status | Proof / caveat |
|---|---|---|
| Reading a Celery **success** record | ✅ Interop-verified | `celery_golden.rs::a_real_celery_success_record_parses` against `celery_result_success.json`, a record a real worker wrote |
| Reading a Celery **failure** record (`exc_type` / `exc_module` / `exc_message` + `traceback`) | ✅ Interop-verified | `celery_golden.rs::a_real_celery_failure_record_parses_into_the_typed_exception`; `test_celers_to_python.py::test_failure_carries_the_python_traceback_back_to_celers` |
| Writing a record a real `AsyncResult.get()` reads | ✅ Interop-verified | `test_python_to_celers.py::test_a_result_already_stored_is_read_without_a_publish` and `::test_a_late_result_must_be_published_to_wake_a_waiting_client` (the pub/sub publish is required, and is asserted) |
| Multi-argument exceptions | ✅ Interop-verified | `ExceptionInfo::exc_message` is Python's `exc.args` verbatim — a list of JSON values, not a joined string. `test_celers_to_python.py::test_a_multi_argument_exception_keeps_its_argument_boundaries` and `::test_a_python_exception_rebuilt_from_a_celers_record_keeps_its_args`, which lets Celery's own `exception_to_python` rebuild an `OSError(2, "no such file")` from a CeleRS record |
| `children` on a retried or chained task | ✅ Interop-verified | `ResultMessage::children` is a tree of `ResultChild` nodes modelling `AsyncResult.as_tuple()` (`[[id, parent], group_results]`), read and written in Celery's shape, with the legacy id-list and short forms still accepted. `test_celers_to_python.py::test_celers_parses_a_celery_record_that_has_children` (children built by Celery's own `as_tuple()`, then fed back through `result_from_tuple`), `::test_celers_parses_the_children_celerys_own_backend_writes` (via `Backend._get_result_meta`) and `::test_a_record_stored_outside_a_task_context_has_null_children` |

### Producer paths (which CeleRS API emits a deliverable envelope)

| API | Status | Detail |
|---|---|---|
| `celers_protocol::compat::create_python_celery_message` | ✅ Interop-verified | Its output was published to a queue and **executed by a real Celery worker**; that exact envelope is committed as `celers_envelope_accepted_by_celery.json` and re-checked by `celery_golden.rs::the_canonical_celers_envelope_is_the_one_celery_accepted`. It is a *fixture* builder: every field is a function of the arguments, so `reply_to` and `delivery_tag` are the nil UUID — deterministic by design, and therefore **not** unique per publish the way kombu's tag is. Publish real work through `MessageBuilder` |
| `celers_protocol::builder::MessageBuilder` | ✅ Interop-verified | Its envelope is published **unpatched** to a queue a real Celery worker consumes, and executes: `test_celers_to_python.py::test_message_builder_envelope_is_deliverable_as_is`. `MessageProperties` serializes `delivery_tag` (fresh per message, as kombu's producer mints it) and `delivery_info` (`{exchange, routing_key}`, with the routing key naming the queue `.queue(...)` / `.routing_key(...)` selected) — `kombu.transport.virtual.base.Message.__init__` indexes both directly (no `.get()`, no default), so omitting either made the `KeyError` escape the consumer callback and **kill the worker's event loop**, not just the one message. The requirement itself stays pinned by `::test_kombu_requires_delivery_tag_and_delivery_info`, and `build()` now also stamps `argsrepr` / `kwargsrepr` so a monitor shows the arguments (`::test_message_builder_stamps_the_argument_reprs_a_monitor_displays`) |

| `celers_protocol::v5::build_v5_message` | 🟡 Structurally verified | The same envelope plus the CeleRS-internal v5 header stamps. It carries the kombu properties (`MessageProperties::new()`) and, as of this release, `argsrepr` / `kwargsrepr` (`v5.rs::test_v5_message_carries_python_arg_reprs`), and satisfies the v2 envelope rules (`compat.rs::test_v5_message_satisfies_v2_envelope_rules`) — but nothing in the interop suite publishes one to a live worker, and `V5MessageSpec` has no queue of its own, so the routing key stays `celery` unless the caller sets `properties.with_routing_key(...)` |

### Brokers

Two different traits are in play here, and the difference decides what you can build:

* **`celers_core::Broker`** is the task-queue abstraction (`enqueue`/`dequeue`/`ack`/`reject`/
  `cancel`/`revoke`/`defer`). It is what `celers_worker::Worker` consumes from.
* **`celers_kombu::Broker`/`Producer`/`Consumer`** is the lower-level message-transport abstraction
  (`publish`/`consume`/`purge`/`create_queue`) over `celers_protocol::Message`.

| Broker | Python Celery | `celers_core::Broker` (worker-usable) | Celery interop | Notes |
|---|---|---|---|---|
| Redis | ✅ | ✅ | 🔴 Not interoperable | See [What is not interoperable](#what-is-not-interoperable). Kombu-style *key names* (priority suffix, `celery-task-meta-`), CeleRS-shaped *payloads* |
| PostgreSQL | ⛔ | ✅ | n/a | CeleRS-specific; Celery has no PostgreSQL broker. **Live-verified against a real PostgreSQL 16, 271/271** (`tests_pg.rs`, `tests_pg_binds.rs`, `tests_pg_results.rs`, `tests_pg_locks.rs`, gated on `CELERS_TEST_POSTGRES_URL`) — this release fixed a bare-`$n` UUID/JSONB bind defect that made the broker non-functional against a live server, created the result table (`celers_broker_results`) that the whole result store addressed but no migration had ever built, and added the `celers_tasks.updated_at` column two analytics entry points read; see [CHANGELOG.md](../CHANGELOG.md)'s 0.3.1 "Fixed" section |
| MySQL | ⛔ | ✅ | n/a | CeleRS-specific. **Live-verified against a real MySQL 8, 236/236** (gated on `CELERS_TEST_MYSQL_URL`/`MYSQL_URL`), under both `nextest`'s default parallel execution and `--test-threads=1`, on a volume recreated from scratch — this release fixed missing deadlock retry, an uncreatable `move_to_dlq`, a queue-scoping leak across the diagnostics methods, and a claim whose next-key locks starved neighbouring queues; see CHANGELOG.md. Pointing this broker and `celers-backend-db`'s MySQL result backend at the *same* database is now **supported and tested** — the broker's result table was renamed `celers_broker_results`, which closed Known gaps #16 |
| In-memory (dev) | ⛔ | ✅ | n/a | `celers_core::InMemoryBroker`, for local development and tests |
| RabbitMQ (AMQP) | ✅ | ✅ *(new in 0.3.1)* | 🔴 Not interoperable | `AmqpBroker` still implements the `celers_kombu` transport traits directly; `AmqpBroker::into_core_broker(queue)` now also wraps it in `celers_kombu::core_adapter::KombuBrokerAdapter`, which implements `celers_core::Broker` (default-on `core-broker` feature), so a `celers_worker::Worker` **can** consume from RabbitMQ as of this release. It still publishes a JSON-serialized `celers_protocol::Message` envelope as the AMQP body rather than mapping the v2 headers onto AMQP basic-properties headers the way kombu's AMQP transport does, so this is a worker-usability fix, not a wire-compatibility one — untested against a real Celery AMQP consumer. `cancel()`/`revoke()` are not wired to anything (the adapter cannot address a queued message by id): see [TODO.md → Known gaps #8](../TODO.md#known-gaps--the-roadmap-after-031). **URL-configurable**: `celers::broker_helper::create_broker("amqp"/"rabbitmq", url, queue)` builds a worker-usable broker directly, live-verified against a real RabbitMQ |
| Amazon SQS | ✅ | ✅ *(new in 0.3.1)* | 🔴 Not interoperable | Same shape as AMQP: `SqsBroker::into_core_broker(queue)` wraps the existing transport in the same `KombuBrokerAdapter` (default-on `core-broker` feature), and additionally routes `dequeue_batch`/`enqueue_batch`/`ack_batch`/`defer`/`enqueue_after` onto SQS's native `ReceiveMessage(MaxNumberOfMessages)`/`SendMessageBatch`/`DeleteMessageBatch`/`ChangeMessageVisibility`/`SendMessage(DelaySeconds)` rather than one request per message. SQS also has no message-level priority, so priorities still mean separate queues. [ADR-005](adr/005-sqs-celery-compatibility.md) sketches a Kombu-compatible SQS wire design but is **Proposed and unimplemented** — read it as a plan, not a description; it is a different, deeper claim than worker-usability. **URL-configurable** the same way as RabbitMQ: `celers::broker_helper::create_broker("sqs", url, queue)` (`url` is unused — SQS takes its endpoint from the environment) |

### Result backends

| Backend | Python Celery | CeleRS | Celery interop | Notes |
|---|---|---|---|---|
| Redis | ✅ | ✅ | 🔴 Not interoperable | The key prefix is Celery's (`celery-task-meta-`), the stored document is not — see below |
| PostgreSQL | ✅ (SQLAlchemy) | ✅ | 🔴 Not interoperable | Different schema entirely. **Live-verified against a real PostgreSQL 16** — this release fixed a bare-`$n` JSONB bind defect (`result_data`/`extra`/`task_ids`) that made storing a result with a payload fail outright against a live server; see [CHANGELOG.md](../CHANGELOG.md)'s 0.3.1 "Fixed" section. `celers-backend-db`'s full suite (`DATABASE_URL` set, `MYSQL_URL` on either a shared or a separate database — see the MySQL row below) is **151/151** |
| MySQL | ✅ (SQLAlchemy) | ✅ | 🔴 Not interoperable | Different schema entirely. **Live-verified against a real MySQL 8** — this release fixed a `migrate()` that failed outright on every MySQL database (an uncreatable `DELIMITER //` stored-procedure body) and a `SUM`-over-empty-result crash in `task_stats`. `MYSQL_URL` pointed at the *same* database as `celers-broker-sql`'s MySQL broker used to collide on the shared table name `celers_task_results` and fail 7 tests; the broker's table is now `celers_broker_results`, so both topologies are clean — **151/151** on a shared database and on a separate one alike (Known gaps #16, closed) |
| RPC (gRPC) | ✅ (AMQP RPC) | ✅ (gRPC) | ⛔ Not implemented | CeleRS' "RPC backend" is gRPC; Celery's is AMQP reply queues. Different mechanism, not a compatible one |

### Canvas (workflow primitives)

Every row is 🟢 **Implemented** — executed end to end by `celers-worker` and covered by
`crates/celers-worker/tests/workflow_semantics.rs`, `celers_worker::workflows::patterns_e2e` and
the canvas crate's own suites. None is interop-verified: the only Celery-side coverage of canvas is
at the *protocol* level (the `chain` embed and the `group` / `group_index` headers, both rows in
[Task fields](#task-fields-protocol-v2-headers-and-embed)).

| Primitive | Python Celery | CeleRS | Status |
|---|---|---|---|
| Chain | ✅ | ✅ | 🟢 Implemented (embed link ✅ interop-verified) |
| Group | ✅ | ✅ | 🟢 Implemented (`group` / `group_index` ✅ interop-verified) |
| Chord | ✅ | ✅ | 🟢 Implemented — needs a result backend for the barrier |
| Map / Starmap / Chunks | ✅ | ✅ | 🟢 Implemented |
| Saga (compensation) | ⛔ | ✅ | CeleRS extension — lowers onto a chain with a rollback error route |
| Branch / Switch (conditional) | ⛔ | ✅ | CeleRS extension |
| Pipeline / FanIn / FanOut / ScatterGather | ⛔ | ✅ | CeleRS extensions — lower onto chain/group/chord |

### Beat (scheduler)

| Schedule type | Python Celery | CeleRS | Status |
|---|---|---|---|
| Interval | ✅ | ✅ | 🟢 Implemented |
| Crontab | ✅ | ✅ | 🟢 Implemented — 5-field expressions, Unix `day_of_week` semantics |
| One-time | ✅ | ✅ | 🟢 Implemented |
| Solar | ✅ | ✅ | Behind `celers-beat/solar`. Resolves via `sunrise`'s `SolarDay::event_time` (absolute `DateTime<Utc>`). Supports sunrise/sunset, civil/nautical/astronomical twilight, *and* golden hour, all as true `SolarEvent::Elevation` solves (0° / -6° for golden hour's morning/evening boundary) rather than a flat offset from sunrise/sunset; handles polar day/night by skipping dates with no such event |

### Remote control and monitoring

| Feature | Python Celery | CeleRS | Celery interop | Notes |
|---|---|---|---|---|
| Event stream (`celeryev`) | ✅ | ✅ | 🟢 Implemented | As of 0.3.1 CeleRS emits the Celery event shape (`uuid`, `name`, float `timestamp`, `hostname`, `pid`, `clock`, `utcoffset`) and parses events a Python worker published. Byte-stability is pinned by `celers_core::event::wire`'s `wire_json_is_byte_for_byte_stable`, but **no test has yet pointed `celery events` or Flower at a CeleRS worker** |
| Lamport event clock | ✅ | ✅ | 🟢 Implemented | `forward_event_clock` / `adjust_event_clock` |
| `celery -A app inspect` / `control` (pidbox) | ✅ | ⛔ | ⛔ Not implemented | `celers inspect` / `celers control` are a **CeleRS-native** protocol shaped like a pidbox (broadcast channel + per-request reply channel) but with no kombu pidbox codec. `celery -A app inspect` cannot reach a CeleRS worker and `celers inspect` cannot reach a Python one |
| Task revocation | ✅ | ✅ | 🔴 Not interoperable | CeleRS revocations live in the broker (`<queue>:revoked` sorted set + a `RevocationNotice` JSON notice on `<queue>:cancel`); Celery's ride the pidbox |

## What is not interoperable

This is the section that decides whether CeleRS fits your migration.

**A Python Celery worker and a CeleRS worker cannot share a queue today.** The protocol layer speaks
Celery, but the broker and the result backend are not routed through it:

* `celers-broker-redis` enqueues `serde_json::to_string(&task)`: a `SerializedTask`, CeleRS' own
  message type. It is not a Celery envelope — no `headers` / `properties` / base64 `body` framing,
  no `argsrepr`, nothing kombu can construct a `Message` from. (`celers-broker-postgres` and
  `celers-broker-sql`, the other two brokers a worker can consume from, store tasks as rows in a
  `celers_tasks` table, which Celery has no notion of at all.)
* Both Redis result backends store a **CeleRS-shaped** record. The key prefix matches Celery
  (`celery-task-meta-<uuid>`), but the value is
  `{task_id, task_name, result, created_at, started_at, completed_at, worker, …}` — not Celery's
  `{status, result, traceback, children, date_done, task_id}`. `AsyncResult.get()` on a CeleRS
  record will not find the `status` field it needs.

Closing the gap means routing the broker and the backend through `celers-protocol`. When that lands,
[`tests/python-compat/`](../tests/python-compat/) is where it gets proved — the suite is built for
exactly that. Until then, treat CeleRS as a *Celery-protocol-compatible* library, not as a drop-in
Celery worker.

## Message format

### Task envelope (protocol v2)

This is a **verbatim capture** from Celery 5.6.3 (`celery_task_v2_positional_tuple.json`, recording
`add.apply_async(args=(4, 5))`), not an illustration:

```json
{
  "body": "W1s0LCA1XSwge30sIHsiY2FsbGJhY2tzIjogbnVsbCwgImVycmJhY2tzIjogbnVsbCwgImNoYWluIjogbnVsbCwgImNob3JkIjogbnVsbH1d",
  "content-encoding": "utf-8",
  "content-type": "application/json",
  "headers": {
    "lang": "py",
    "task": "tasks.add",
    "id": "7b1a0d1e-0000-4000-8000-000000000002",
    "shadow": null,
    "eta": null,
    "expires": null,
    "group": null,
    "group_index": null,
    "retries": 0,
    "timelimit": [null, null],
    "root_id": "7b1a0d1e-0000-4000-8000-000000000002",
    "parent_id": null,
    "argsrepr": "(4, 5)",
    "kwargsrepr": "{}",
    "origin": "gen93813@capture-host",
    "ignore_result": false,
    "replaced_task_nesting": 0,
    "stamped_headers": null,
    "stamps": {}
  },
  "properties": {
    "correlation_id": "7b1a0d1e-0000-4000-8000-000000000002",
    "reply_to": "970bdf51-6fa2-3cbd-8882-334a482c0358",
    "delivery_mode": 2,
    "delivery_info": {"exchange": "", "routing_key": "celers-compat-capture"},
    "priority": 0,
    "body_encoding": "base64",
    "delivery_tag": "181ed2eb-268b-41d8-934b-a03258c1b354"
  }
}
```

`properties.delivery_tag` and `properties.delivery_info` are **not optional** for a kombu consumer —
see [Producer paths](#producer-paths-which-celers-api-emits-a-deliverable-envelope).

### Body (the embed triple)

`body` is base64 of a three-element JSON array — `[args, kwargs, embed]`. Decoding the capture
above:

```json
[[4, 5], {}, {"callbacks": null, "errbacks": null, "chain": null, "chord": null}]
```

Celery emits exactly those four embed keys, and they are `null` rather than `[]` when unused. A
chain head carries its next link inside `embed["chain"]`
(`celery_golden.rs::a_chain_head_carries_the_next_link_in_the_embed_dict`).

### Result record

Verbatim, from `celery_result_success.json` and `celery_result_failure.json` — both written by a
real Celery worker:

```json
{
  "status": "SUCCESS",
  "result": 9,
  "traceback": null,
  "children": [],
  "date_done": "2026-08-25T10:52:04.521712+00:00",
  "task_id": "7b1a0d1e-0000-4000-8000-000000000010"
}
```

```json
{
  "status": "FAILURE",
  "result": {
    "exc_type": "ValueError",
    "exc_message": ["fixture failure"],
    "exc_module": "builtins"
  },
  "traceback": "Traceback (most recent call last):\n  …\nValueError: fixture failure\n",
  "children": [],
  "date_done": "2026-08-25T10:52:04.571332+00:00",
  "task_id": "7b1a0d1e-0000-4000-8000-000000000011"
}
```

`AsyncResult.get()` needs more than the key: a **pub/sub publish** on the result key's channel is
what wakes a waiting client (`test_python_to_celers.py::test_a_late_result_must_be_published_to_wake_a_waiting_client`).

## Redis specifics

### Key names CeleRS uses

| Purpose | Key | Celery/Kombu name? |
|---|---|---|
| Default queue | `celery` | ✅ same |
| Priority queue | `<queue>\x06\x16<priority>` | ✅ same separator kombu uses (`lua_scripts::ENQUEUE_WITH_PRIORITY`) |
| Task result | `celery-task-meta-<uuid>` | ✅ same key, ❌ different value shape |
| Chord state / counter | `celery-chord-<group-id>` / `celery-chord-counter-<group-id>` | ✅ same |
| In-flight (visibility) | `<queue>:unacked` (sorted set, scored by deadline) + `<queue>:processing` (list) | ❌ CeleRS-specific — kombu uses an `unacked` hash plus an `unacked_index` sorted set |
| Delayed messages | `<queue>:delayed` (sorted set, scored by due time) | ❌ CeleRS-specific |
| Revoked task ids | `<queue>:revoked` (sorted set, scored by expiry) | ❌ CeleRS-specific |
| Revocation notices | `<queue>:cancel` (pub/sub, `RevocationNotice` JSON) | ❌ CeleRS-specific |

CeleRS does **not** write kombu's `_kombu.binding.<queue>` binding sets. It does not need them (it
does not do kombu's exchange emulation), and their absence is one more reason a Python worker cannot
be pointed at a CeleRS queue.

### Visibility timeout

Dequeue is a single `EVAL` that pops the message and records its deadline atomically
(`lua_scripts::POP_TO_UNACKED`). Note what the script does **not** do: no blocking command is used
inside Lua — `BRPOP` and `BRPOPLPUSH` inside `EVAL` would park the single-threaded server, and
`lua_scripts`' own tests assert no script contains either. The blocking fast path is issued by the
client and staged onto `<queue>:processing`; the reaper adopts anything that never got a deadline.

```lua
-- lua_scripts::POP_TO_UNACKED (excerpt): pop, check the revoked set, record the deadline
local msg = redis.call('RPOP', queue)          -- ZPOPMIN in priority mode
if not msg then return {1, ''} end
local id = string.match(msg, '^{"metadata":{"id":"([^"]+)"')
if id and redis.call('ZSCORE', revoked, id) then ... end   -- revoked: drop, try again
redis.call('LPUSH', processing, msg)
redis.call('ZADD', unacked, deadline, msg)
return {0, msg}
```

`SCRIPT_VERSION` is `3` as of 0.3.1 (the `DEFER_UNACKED` script was added).

**Known limitation:** both in-flight structures key on the message *body*, so two byte-identical
messages collapse into one sorted-set entry and the second copy is recovered by the reaper rather
than acked directly. Task ids are UUIDs, so this only arises if the same `SerializedTask` value is
enqueued twice.

## Migration guidance

Given [what is not interoperable](#what-is-not-interoperable), the honest migration paths are:

1. **Protocol-level integration (supported today).** Use `celers-protocol` to produce and consume
   Celery envelopes and result records — with your own transport code, or with
   `create_python_celery_message` and a Redis client. This is what the interop suite does, and it
   is the only path with test coverage against real Celery.
2. **Queue-at-a-time cutover (supported today).** Move one task type at a time to a *CeleRS-only*
   queue served by CeleRS workers, leaving the rest on Celery. The two systems share Redis but not
   queues. Route with Celery's `task_routes` on the Python side.
3. **Mixed workers on one queue (not yet possible).** Blocked on routing the broker and result
   backend through `celers-protocol`.

Whichever path you take, keep `task_serializer='json'` and `task_protocol=2` on the Python side.

## Running the interop suite

```sh
# Everything, including a real celery worker (creates a venv under $TMPDIR):
CELERS_TEST_REDIS_URL=redis://127.0.0.1:6379 tests/python-compat/run.sh

# The same suite from cargo:
cargo build -p celers-protocol --example celery_bridge
CELERS_TEST_REDIS_URL=redis://127.0.0.1:6379 \
CELERS_PYTHON=/path/to/venv/bin/python \
  cargo test -p celers-protocol --test python_interop

# The fixture-backed half — no services needed at all:
cargo test -p celers-protocol --test celery_golden

# Re-record the captures after a Celery upgrade, then read the diff:
CELERS_TEST_REDIS_URL=redis://127.0.0.1:6379 tests/python-compat/run.sh capture
```

Gating: without `CELERS_TEST_REDIS_URL` or `CELERS_PYTHON` the suite skips visibly and exits 0;
without `CELERS_BRIDGE` the round-trip tests skip while the golden and repr tests still run. Full
details, including how the round trip is wired, are in
[`tests/python-compat/README.md`](../tests/python-compat/README.md).

Pinned versions for the committed captures: **celery 5.6.3, kombu 5.6.2, Python 3.14.6,
`task_protocol=2`, JSON**. See
[`crates/celers-protocol/tests/fixtures/README.md`](../crates/celers-protocol/tests/fixtures/README.md)
for what each fixture records and the four environment-specific substrings that are normalised.

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| Celery worker dies with `KeyError: 'delivery_tag'` | A `MessageBuilder` envelope reached a kombu consumer | Add `delivery_tag` / `delivery_info` to `properties`, or use `create_python_celery_message` |
| Message rejected on the Python side | Pickle serialization | `task_serializer='json'`, `accept_content=['json']` |
| Task never runs after a CeleRS worker was pointed at a Celery queue | The broker layers do not interoperate | See [What is not interoperable](#what-is-not-interoperable) |
| `AsyncResult.get()` hangs though the key exists | The result was written without the pub/sub publish | Publish on the result channel as well; the interop suite asserts this requirement |
| Solar schedule always errors | The `Schedule::Solar` unit bug | Known defect — see the Beat row above |
| Priority ignored | SQS broker | SQS has no priorities; use separate queues ([ADR-005](adr/005-sqs-celery-compatibility.md)) |
| `celery -A app inspect` finds no CeleRS workers | No pidbox codec | Expected; use `celers inspect` |

## References

- [`tests/python-compat/README.md`](../tests/python-compat/README.md) — how the live round trip is wired
- [`crates/celers-protocol/tests/fixtures/README.md`](../crates/celers-protocol/tests/fixtures/README.md) — the captures
- [ADR-004: Celery protocol compatibility](adr/004-celery-protocol-compatibility.md) — Accepted; carries a 0.3.1 amendment correcting its "protocol v5" premise
- [ADR-005: SQS Celery compatibility](adr/005-sqs-celery-compatibility.md) — **Proposed and unimplemented**
- [Celery protocol documentation](https://docs.celeryq.dev/en/stable/internals/protocol.html)
- [Kombu Redis transport](https://github.com/celery/kombu/blob/main/kombu/transport/redis.py)
