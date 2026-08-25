"""CeleRS publishes; a real Python Celery worker executes; CeleRS reads back.

The mirror image of ``test_python_to_celers.py``. Here the CeleRS bridge builds
the envelope with ``celers-protocol``, it goes onto a real Redis queue, an
unmodified ``celery -A tasks worker`` picks it up and runs it, and the bridge
parses the result record Celery wrote.

Nothing is stubbed: if the envelope is malformed, the worker either ignores it
or dies, and the test times out.
"""

from __future__ import annotations

import json
import uuid

import pytest

import harness

pytestmark = pytest.mark.usefixtures("bridge")


@pytest.fixture(scope="module")
def module_worker(request, tmp_path_factory):
    """One worker for the whole module -- booting one per test is pure latency."""
    import os

    queue = os.environ["CELERS_COMPAT_QUEUE"]
    log = tmp_path_factory.mktemp("worker") / "celery.log"
    with harness.CeleryWorker(queue, log) as worker:
        yield worker


def publish(client, queue_name, envelope: dict) -> None:
    client.lpush(queue_name, json.dumps(envelope))


def new_id() -> str:
    return str(uuid.uuid4())


# =============================================================================
# The canonical envelope
# =============================================================================


def test_positional_arguments(client, keyring, queue_name, module_worker):
    task_id = new_id()
    envelope = harness.run_bridge(
        "encode-task",
        {"task": "tasks.add", "id": task_id, "args": [4, 5], "kwargs": {}},
    )
    keyring.track_task(task_id)
    publish(client, queue_name, envelope)

    meta = harness.await_result(client, task_id)
    decoded = harness.run_bridge("decode-result", meta)

    assert decoded["status"] == "SUCCESS", module_worker.log_text()[-2000:]
    assert decoded["is_success"] is True
    assert decoded["result"] == 9
    assert decoded["task_id"] == task_id


def test_keyword_arguments(client, keyring, queue_name, module_worker):
    task_id = new_id()
    envelope = harness.run_bridge(
        "encode-task",
        {
            "task": "tasks.greet",
            "id": task_id,
            "args": ["Ada"],
            "kwargs": {"punct": "?", "loud": True},
        },
    )
    keyring.track_task(task_id)
    publish(client, queue_name, envelope)

    decoded = harness.run_bridge("decode-result", harness.await_result(client, task_id))
    # Python read `loud` as a bool and `punct` as a string out of CeleRS' JSON.
    assert decoded["result"] == "HELLO, ADA?", module_worker.log_text()[-2000:]


def test_failure_carries_the_python_traceback_back_to_celers(
    client, keyring, queue_name, module_worker
):
    task_id = new_id()
    envelope = harness.run_bridge(
        "encode-task",
        {"task": "tasks.boom", "id": task_id, "args": ["from python"], "kwargs": {}},
    )
    keyring.track_task(task_id)
    publish(client, queue_name, envelope)

    decoded = harness.run_bridge("decode-result", harness.await_result(client, task_id))

    assert decoded["status"] == "FAILURE"
    assert decoded["is_failure"] is True
    assert decoded["exc_type"] == "ValueError"
    assert decoded["exc_module"] == "builtins"
    # Celery writes `exc_message` as a list -- it is `exc.args`, splatted into
    # the exception constructor on the way back. CeleRS models it as a single
    # String and joins a multi-element list with ", " (documented on
    # `ExceptionInfo::exc_message`), so a one-argument exception arrives whole.
    assert decoded["exc_message"] == "from python"
    assert decoded["traceback"].startswith("Traceback (most recent call last):")
    assert "ValueError: from python" in decoded["traceback"]


def test_a_multi_argument_exception_loses_its_argument_boundaries(
    client, keyring, queue_name, module_worker
):
    """Pin what CeleRS does with `raise ValueError("a", "b")`.

    Python's `exc.args` is a tuple, and Celery preserves it as a JSON list.
    `ExceptionInfo::exc_message` is a `String`, so the two arguments are joined
    into `"a, b"` and re-serialize as the single-element list `["a, b"]` -- a
    Python client rebuilding the exception from CeleRS' record therefore gets
    one argument where it sent two.

    This is documented behaviour rather than a silent surprise, and asserting
    it means a future change to `ExceptionInfo` cannot go unnoticed.
    """
    record = {
        "status": "FAILURE",
        "result": {
            "exc_type": "ValueError",
            "exc_message": ["first", "second"],
            "exc_module": "builtins",
        },
        "traceback": "Traceback (most recent call last):\nValueError: first",
        "children": [],
        "task_id": new_id(),
    }
    decoded = harness.run_bridge("decode-result", record)
    assert decoded["exc_message"] == "first, second"


def test_retry_with_countdown(client, keyring, queue_name, module_worker):
    """A Python task that retries itself must still resolve for CeleRS.

    ``tasks.flaky`` raises ``self.retry(countdown=1)`` on its first attempt.
    The worker re-publishes the message with ``retries`` incremented and an
    ``eta`` one second out; the second attempt succeeds. CeleRS reads only the
    final record, which is what a client sees.
    """
    task_id = new_id()
    marker = f"celers-compat-flaky-{uuid.uuid4().hex[:8]}"
    keyring.track(marker)
    keyring.track_task(task_id)

    # `queue=` is load-bearing here, not tidiness: Celery routes a retry with
    # `self.request.delivery_info`, so an envelope that claimed the default
    # routing key would re-publish the retry to `celery`, which no worker in
    # this suite consumes, and the task would sit at RETRY until the timeout.
    envelope = harness.run_bridge(
        "encode-task",
        {
            "task": "tasks.flaky",
            "id": task_id,
            "args": [marker],
            "kwargs": {"countdown": 1},
            "queue": queue_name,
        },
    )
    publish(client, queue_name, envelope)

    # A retrying task publishes an intermediate RETRY record first, so waiting
    # for "a record" rather than "a terminal record" reads the wrong one.
    meta = harness.await_terminal_result(client, task_id, timeout=60)
    decoded = harness.run_bridge("decode-result", meta)

    assert decoded["status"] == "SUCCESS", module_worker.log_text()[-3000:]
    # The second attempt returned the attempt counter.
    assert decoded["result"] == 2
    # The worker really did retry rather than swallowing the first failure.
    assert int(client.get(marker)) == 2
    assert "Retry" in module_worker.log_text() or "retry" in module_worker.log_text()


@pytest.mark.xfail(
    strict=True,
    reason=(
        "ResultMessage::children in crates/celers-protocol/src/result.rs is "
        "Vec<Uuid>, but Celery serialises children as nested result tuples -- "
        "[[[task_id, parent], None]] -- so from_json() rejects every record a "
        "retried or chained task produces. Un-xfail when result.rs models it."
    ),
)
def test_celers_parses_a_celery_record_that_has_children():
    """Celery's ``children`` is not a list of ids.

    ``celery.result.AsyncResult.as_tuple()`` renders a child as
    ``((task_id, parent_tuple), None)``, and the backend stores that structure
    verbatim. Any retried task, and any task with a chain or group parent,
    carries one -- so this is not an exotic shape.
    """
    record = {
        "status": "SUCCESS",
        "result": 2,
        "traceback": None,
        "children": [[["00b0d7c2-9c80-4dc3-b5a7-671a099b92ea", None], None]],
        "date_done": "2026-01-01T00:00:00+00:00",
        "task_id": "7b1a0d1e-0000-4000-8000-000000000001",
    }
    decoded = harness.run_bridge("decode-result", record)
    assert decoded["status"] == "SUCCESS"


def test_eta_delays_execution(client, keyring, queue_name, module_worker):
    """An `eta` CeleRS wrote must be honoured, not ignored."""
    import datetime
    import time

    task_id = new_id()
    keyring.track_task(task_id)
    eta = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=3)
    envelope = harness.run_bridge(
        "build-task",
        {
            "task": "tasks.add",
            "id": task_id,
            "args": [1, 2],
            "eta": eta.isoformat(),
        },
    )
    envelope = _kombu_deliverable(envelope, queue_name)
    publish(client, queue_name, envelope)

    started = time.monotonic()
    meta = harness.await_result(client, task_id, timeout=60)
    elapsed = time.monotonic() - started

    assert meta["status"] == "SUCCESS", module_worker.log_text()[-2000:]
    assert elapsed >= 2.0, (
        f"the worker ran the task after {elapsed:.1f}s; the eta was ~3s out, "
        "so the header was ignored"
    )


# =============================================================================
# What kombu requires of any producer
# =============================================================================


def _kombu_deliverable(envelope: dict, queue_name: str) -> dict:
    """Add the two properties kombu indexes unconditionally.

    See ``test_kombu_requires_delivery_tag_and_delivery_info``: without these,
    a Celery worker cannot even construct the message. The canonical
    ``encode-task`` envelope already carries them; the ``MessageBuilder`` one
    does not, so tests that exercise the builder patch them in here and the gap
    itself is asserted separately.
    """
    envelope = json.loads(json.dumps(envelope))
    envelope["properties"].setdefault("delivery_tag", str(uuid.uuid4()))
    envelope["properties"].setdefault(
        "delivery_info", {"exchange": "", "routing_key": queue_name}
    )
    return envelope


@pytest.fixture
def doomed_queue(client, keyring):
    """A queue of its own for tests that deliberately kill a worker.

    A malformed message takes down whichever consumer reads it, so publishing
    one onto the shared queue would kill the module-scoped worker and fail
    every test that runs after it.
    """
    name = f"celers-compat-doomed-{uuid.uuid4().hex[:12]}"
    keyring.track(name, f"_kombu.binding.{name}")
    yield name
    client.delete(name)


@pytest.mark.parametrize("missing", ["delivery_tag", "delivery_info"])
def test_kombu_requires_delivery_tag_and_delivery_info(
    client, keyring, doomed_queue, worker_log, missing
):
    """Drop one property and a real worker cannot construct the message.

    ``kombu.transport.virtual.base.Message.__init__`` indexes
    ``properties['delivery_tag']`` and ``properties['delivery_info']['exchange']``
    directly -- no ``.get()``, no default. The resulting ``KeyError`` escapes
    the consumer callback and takes down the worker's event loop, so the damage
    is not one lost message but a dead worker.

    This pins the requirement itself, which is why it does not need revisiting
    when a producer is fixed.
    """
    task_id = new_id()
    keyring.track_task(task_id)
    envelope = harness.run_bridge(
        "encode-task",
        {"task": "tasks.add", "id": task_id, "args": [1, 1], "kwargs": {}},
    )
    assert missing in envelope["properties"], (
        f"the canonical CeleRS envelope should carry {missing}; "
        "this test would otherwise prove nothing"
    )
    del envelope["properties"][missing]

    with harness.CeleryWorker(doomed_queue, worker_log) as worker:
        publish(client, doomed_queue, envelope)
        with pytest.raises(AssertionError):
            harness.await_result(client, task_id, timeout=8)
        log = worker.log_text()

    assert "KeyError" in log, (
        f"expected kombu to fail on the missing {missing}; log tail:\n{log[-2000:]}"
    )


@pytest.mark.xfail(
    strict=True,
    reason=(
        "MessageProperties serialization in crates/celers-protocol/src/types.rs "
        "omits delivery_tag and delivery_info, so a MessageBuilder envelope kills "
        "a Celery worker with KeyError. When that is fixed, delete this xfail and "
        "the _kombu_deliverable() patching above."
    ),
)
def test_message_builder_envelope_is_deliverable_as_is(client, keyring, queue_name):
    """The ordinary CeleRS producer path should need no patching.

    ``MessageBuilder`` is what a CeleRS application uses; ``create_python_celery_message``
    is a fixture helper. Today only the helper emits an envelope kombu can read.
    """
    envelope = harness.run_bridge(
        "build-task",
        {"task": "tasks.add", "id": new_id(), "args": [1, 1], "kwargs": {}},
    )
    assert "delivery_tag" in envelope["properties"]
    assert "delivery_info" in envelope["properties"]


def test_message_builder_envelope_runs_once_the_properties_are_supplied(
    client, keyring, queue_name, module_worker
):
    """With the two properties added, the builder's envelope executes.

    This is what makes the gap above a *missing field* rather than a deeper
    incompatibility: `lang: "rust"`, the absent `argsrepr` and the absent
    `timelimit` are all tolerated by a real worker.
    """
    task_id = new_id()
    keyring.track_task(task_id)
    envelope = harness.run_bridge(
        "build-task",
        {
            "task": "tasks.greet",
            "id": task_id,
            "args": ["Ada"],
            "kwargs": {"loud": True},
        },
    )
    assert envelope["headers"]["lang"] == "rust", "the builder stamps the Rust lang"
    publish(client, queue_name, _kombu_deliverable(envelope, queue_name))

    decoded = harness.run_bridge("decode-result", harness.await_result(client, task_id))
    assert decoded["status"] == "SUCCESS", module_worker.log_text()[-2000:]
    assert decoded["result"] == "HELLO, ADA!"
