# Python Celery interoperability suite

Celery compatibility is the product's headline claim. This directory is where
it is *proved*, against a real Python Celery and a real Redis, rather than
asserted.

```sh
CELERS_TEST_REDIS_URL=redis://127.0.0.1:6379 ./run.sh
```

`run.sh` is idempotent: it creates a virtualenv (under `$TMPDIR`, not in the
source tree), installs a pinned Celery, builds the Rust bridge, and runs the
suite. Without a Redis it prints a `SKIPPED:` line and exits 0.

The same suite is reachable from cargo:

```sh
cargo build -p celers-protocol --example celery_bridge
CELERS_TEST_REDIS_URL=redis://127.0.0.1:6379 \
CELERS_PYTHON=/path/to/venv/bin/python \
  cargo test -p celers-protocol --test python_interop
```

## How a round trip works

There is no mock anywhere in the loop. Python runs Celery; Rust runs
`celers-protocol`; Redis is a real broker.

```
Python -> CeleRS                       CeleRS -> Python
-----------------                      ----------------
celery apply_async()                   celery_bridge encode-task
   -> Redis list                          -> Redis list
celery_bridge decode-task/execute      celery -A tasks worker  (real worker)
   -> celery-task-meta-<id>               -> celery-task-meta-<id>
AsyncResult.get()  (real client)       celery_bridge decode-result
```

The CeleRS side is [`crates/celers-protocol/examples/celery_bridge.rs`][bridge]:
a small binary that reads one JSON document on stdin and writes one on stdout.
It does its work through the crate's ordinary public API, so the suite
exercises shipped protocol code rather than a test-local reimplementation. The
Python side does the Redis I/O, which keeps the bridge free of any broker
dependency.

Both runtimes implement the same task *names* (`tasks.add`, `tasks.greet`,
`tasks.boom`, `tasks.double`, `tasks.flaky`) — in `tasks.py` and in the
bridge's `run_task`. Keep the two in step, or a round trip proves only that
bytes moved.

[bridge]: ../../crates/celers-protocol/examples/celery_bridge.rs

## Files

| file | what it is |
|---|---|
| `celeryconfig.py` | Celery settings the suite depends on, each with the reason |
| `tasks.py` | the Celery tasks a real worker executes |
| `harness.py` | Redis helpers, the worker subprocess, the bridge pipe |
| `conftest.py` | gating, isolation and key cleanup |
| `capture_fixtures.py` | re-records the verbatim wire captures |
| `run.sh` | venv setup, bridge build, suite run |
| `test_python_to_celers.py` | Python publishes, CeleRS consumes and answers |
| `test_celers_to_python.py` | CeleRS publishes, a real worker executes |
| `test_envelope_golden.py` | the committed captures still match this Celery |
| `test_reprs.py` | `argsrepr` / `kwargsrepr` match `celery.utils.saferepr` |

## What is covered

* positional arguments, keyword arguments (including `True` / `None`)
* the two properties kombu indexes without a default — `delivery_tag` and
  `delivery_info` — asserted both as a requirement (drop one and a real worker
  dies with `KeyError`) and as something every CeleRS producer path emits
* `countdown`, which Celery resolves client-side into an absolute `eta`
* explicit `eta` and `expires`, and that a worker honours a CeleRS-written `eta`
* task failure: `exc_type` / `exc_module` / `exc_message`, and the `traceback`
  sibling — asserted by letting Celery re-raise a real `ValueError`, including a
  multi-argument exception whose `exc.args` Celery rebuilds from a CeleRS record
* `children` as the nested result tuples Celery actually writes — built by
  Celery's own `as_tuple()` and `Backend._get_result_meta`, including the
  `children: null` a record stored outside a task context carries
* `retry(countdown=...)`, driven by a Python worker retrying a CeleRS-published
  task
* a two-task chain, including following the embedded next link
* a group, including `group` / `group_index`
* the pub/sub requirement that makes `AsyncResult.get()` return
* `argsrepr` / `kwargsrepr` rendered as Python literals, not Rust `Debug`

## Gating

| variable | effect when absent |
|---|---|
| `CELERS_TEST_REDIS_URL` | the whole suite skips, visibly |
| `CELERS_PYTHON` | the whole suite skips, visibly (needed to spawn workers) |
| `CELERS_BRIDGE` | round-trip tests skip; the golden and repr tests still run |

Optional: `CELERS_COMPAT_REDIS_DB` (default `15`) picks the database, and
`CELERS_COMPAT_VENV` relocates the virtualenv.

## Isolation and cleanup

The suite confines itself to one Redis database and a queue name unique to the
session, and deletes every key it created on the way out. It never calls
`FLUSHDB` — this may be a shared Redis.

## Fixtures

`capture_fixtures.py` records what Celery puts on the wire into
[`crates/celers-protocol/tests/fixtures/`](../../crates/celers-protocol/tests/fixtures/),
where the Rust golden tests read it. Those files are recordings: the only
edits are four environment-specific substrings (paths, hostname), listed in the
fixtures' own README. Re-record with `./run.sh capture` when the pinned Celery
moves, then read the diff before trusting it.

## Scope, honestly

This proves the **protocol** layer, `celers-protocol`, against real Celery.

It does not prove the CeleRS *broker* or *result backend*, because those do not
currently speak this wire format: `celers-broker-redis` puts its own
`SerializedTask` JSON on the queue, and both Redis result backends store a
CeleRS-shaped record rather than Celery's
`{status, result, traceback, children, date_done, task_id}`. A Python Celery
worker and a CeleRS worker therefore cannot yet share a queue. Closing that gap
means routing the broker and backend through `celers-protocol`; when it is
done, this suite is where it gets proved.

The suite carries no `xfail`: every test here passes against the pinned Celery.
Two wire-format gaps used to be pinned that way and are now closed —
`MessageProperties` emits `delivery_tag` and `delivery_info` (without them a
`MessageBuilder` envelope made a Celery worker raise `KeyError`, killing the
consumer loop rather than one message), and `ResultMessage::children` models
Celery's nested result tuples instead of a list of ids. Pin the *next* gap the
same way: an `xfail(strict=True)` naming the file and the type fails the moment
it is fixed, so the test is updated rather than forgotten.
