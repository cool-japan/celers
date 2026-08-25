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
    # the exception constructor on the way back -- and CeleRS keeps it as one.
    # `exc_message_text` is the joined rendering, for display only.
    assert decoded["exc_message"] == ["from python"]
    assert decoded["exc_message_text"] == "from python"
    assert decoded["traceback"].startswith("Traceback (most recent call last):")
    assert "ValueError: from python" in decoded["traceback"]


def test_a_multi_argument_exception_keeps_its_argument_boundaries(
    client, keyring, queue_name, module_worker
):
    """`raise ValueError("a", "b")` must survive CeleRS with two arguments.

    Python's `exc.args` is a tuple, Celery preserves it as a JSON list, and
    `celery.backends.base.Backend.exception_to_python` splats that list into the
    exception constructor. A CeleRS record that collapsed it would make a Python
    client rebuild a *different* exception than the one that was raised.

    Regression: `ExceptionInfo::exc_message` used to be a `String` that joined
    the list with ", ".
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
    assert decoded["exc_message"] == ["first", "second"]
    # The joined form is still available, as display text rather than as the
    # wire value.
    assert decoded["exc_message_text"] == "first, second"


def test_a_python_exception_rebuilt_from_a_celers_record_keeps_its_args(
    client, keyring, queue_name, module_worker
):
    """The end the argument list exists for: Celery reconstructing the error.

    A CeleRS-written FAILURE record goes through Celery's own
    ``exception_to_python``; the exception that comes out must carry the same
    ``args`` -- including a non-string one -- that the record described.
    """
    import tasks

    task_id = new_id()
    key = keyring.track_task(task_id)
    meta = {
        "status": "FAILURE",
        "result": {
            "exc_type": "OSError",
            "exc_message": [2, "no such file"],
            "exc_module": "builtins",
        },
        "traceback": "Traceback (most recent call last):\nOSError: no such file",
        "children": [],
        "task_id": task_id,
    }
    # Round-trip the record through CeleRS first: what Celery reads is what
    # `ResultMessage` re-serialized, not the literal above.
    decoded = harness.run_bridge("decode-result", meta)
    assert decoded["exc_message"] == [2, "no such file"]

    harness.store_celers_result(client, task_id, meta)
    assert client.exists(key)

    rebuilt = tasks.app.AsyncResult(task_id).result
    assert isinstance(rebuilt, OSError)
    assert rebuilt.args == (2, "no such file"), (
        "Celery splats exc_message into the exception constructor; a collapsed "
        "list rebuilds the wrong exception"
    )


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


def test_celers_parses_a_celery_record_that_has_children(celery_app):
    """Celery's ``children`` is not a list of ids.

    ``celery.result.AsyncResult.as_tuple()`` renders a child as
    ``((task_id, parent_tuple), None)``, and the backend stores that structure
    verbatim. Any retried task, and any task with a chain or group parent,
    carries one -- so this is not an exotic shape.

    The children here are built by *Celery's own* ``as_tuple``, not written out
    by hand, so this cannot drift from what the installed Celery emits.

    Regression: ``ResultMessage::children`` was ``Vec<Uuid>``, so ``from_json``
    rejected every record a retried or chained task produced.
    """
    from celery.result import AsyncResult, GroupResult

    child_id, parent_id, group_id, member_id = (new_id() for _ in range(4))

    chained = AsyncResult(child_id, app=celery_app)
    chained.parent = AsyncResult(parent_id, app=celery_app)
    group = GroupResult(
        id=group_id, results=[AsyncResult(member_id, app=celery_app)], app=celery_app
    )

    record = {
        "status": "SUCCESS",
        "result": 2,
        "traceback": None,
        "children": [chained.as_tuple(), group.as_tuple()],
        "date_done": "2026-01-01T00:00:00+00:00",
        "task_id": new_id(),
    }
    decoded = harness.run_bridge("decode-result", record)

    assert decoded["status"] == "SUCCESS"
    assert [child["task_id"] for child in decoded["children"]] == [child_id, group_id]
    # The parent chain survives...
    assert decoded["children"][0]["parent"]["task_id"] == parent_id
    assert decoded["children"][0]["is_group"] is False
    # ...and so does the group's membership, which is a different slot.
    assert decoded["children"][1]["is_group"] is True
    assert [m["task_id"] for m in decoded["children"][1]["children"]] == [member_id]

    # What CeleRS writes back is what Celery reads: `result_from_tuple` turns it
    # into the same results it started from.
    from celery.result import result_from_tuple

    restored = [
        result_from_tuple(node, app=celery_app) for node in decoded["children_wire"]
    ]
    assert [r.id for r in restored] == [child_id, group_id]
    assert restored[0].parent.id == parent_id
    assert [r.id for r in restored[1].results] == [member_id]


def test_celers_parses_the_children_celerys_own_backend_writes(celery_app):
    """The same shape, produced by Celery's result-backend code path.

    ``Backend._get_result_meta`` is what actually writes a meta record, and its
    ``children`` come from ``current_task_children`` -- so this asserts against
    the function that puts the bytes in Redis rather than against a literal.
    """

    class FakeRequest:
        """The attributes ``_get_result_meta`` reads off a task request."""

        def __init__(self, children):
            self.children = children
            self.group = None
            self.parent_id = None

    from celery.result import AsyncResult

    child_id = new_id()
    task_id = new_id()
    meta = celery_app.backend._get_result_meta(
        result=7,
        state="SUCCESS",
        traceback=None,
        request=FakeRequest([AsyncResult(child_id, app=celery_app)]),
    )
    meta["task_id"] = task_id

    assert meta["children"], "Celery must have recorded the child"
    decoded = harness.run_bridge("decode-result", meta)
    assert decoded["status"] == "SUCCESS"
    assert [child["task_id"] for child in decoded["children"]] == [child_id]


def test_a_record_stored_outside_a_task_context_has_null_children(celery_app):
    """``current_task_children`` returns ``None`` with no request in scope.

    That is what a client-side ``mark_as_failure`` writes, so ``children: null``
    is a real record -- and one CeleRS used to reject outright, because
    ``#[serde(default)]`` covers a *missing* key, not a null one.
    """
    meta = celery_app.backend._get_result_meta(
        result=1, state="SUCCESS", traceback=None, request=None
    )
    assert meta["children"] is None, "Celery still writes a null children slot"

    meta["task_id"] = new_id()
    decoded = harness.run_bridge("decode-result", meta)
    assert decoded["status"] == "SUCCESS"
    assert decoded["children"] == []


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
            "queue": queue_name,
        },
    )
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


def test_message_builder_envelope_is_deliverable_as_is(
    client, keyring, queue_name, module_worker
):
    """The ordinary CeleRS producer path needs no patching.

    ``MessageBuilder`` is what a CeleRS application uses;
    ``create_python_celery_message`` is a fixture helper. Both must emit an
    envelope a real worker can construct a message from and execute.

    Regression: the builder used to omit ``delivery_tag`` and ``delivery_info``,
    which is not a lost message but a dead worker -- see
    ``test_kombu_requires_delivery_tag_and_delivery_info``. Nothing here patches
    the envelope: it is published exactly as CeleRS serialized it.
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
            "queue": queue_name,
        },
    )

    assert "delivery_tag" in envelope["properties"]
    assert envelope["properties"]["delivery_info"] == {
        "exchange": "",
        "routing_key": queue_name,
    }, "the routing key must name the queue the message is actually on"
    assert envelope["headers"]["lang"] == "rust", "the builder stamps the Rust lang"

    publish(client, queue_name, envelope)

    decoded = harness.run_bridge("decode-result", harness.await_result(client, task_id))
    assert decoded["status"] == "SUCCESS", module_worker.log_text()[-2000:]
    assert decoded["result"] == "HELLO, ADA!"


def test_message_builder_stamps_the_argument_reprs_a_monitor_displays(
    client, queue_name
):
    """``argsrepr`` / ``kwargsrepr`` are Python literals, not Rust ``Debug``.

    Flower, ``celery events`` and ``celery inspect`` read these headers rather
    than deserializing the body, so a producer that omits them shows a blank
    argument list for every task. The strings must be what Python's own
    ``celery.utils.saferepr`` would have produced -- ``test_reprs.py`` pins the
    rendering rules; this pins that the *builder* emits them at all.
    """
    from celery.utils.saferepr import saferepr

    envelope = harness.run_bridge(
        "build-task",
        {
            "task": "tasks.greet",
            "id": new_id(),
            "args": ["Ada"],
            "kwargs": {"loud": True},
            "queue": queue_name,
        },
    )

    assert envelope["headers"]["argsrepr"] == saferepr(("Ada",)) == "('Ada',)"
    assert envelope["headers"]["kwargsrepr"] == saferepr({"loud": True})

    # And they describe the body that was actually encoded.
    decoded = harness.run_bridge("decode-task", envelope)
    assert decoded["args"] == ["Ada"]
    assert decoded["kwargs"] == {"loud": True}
    assert decoded["argsrepr"] == envelope["headers"]["argsrepr"]
