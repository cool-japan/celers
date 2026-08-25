"""Python Celery publishes; CeleRS consumes, executes, and answers.

Each test does the whole loop:

1. a real Celery client publishes a task to a real Redis;
2. the CeleRS bridge takes the raw envelope off the queue, parses it with
   ``celers-protocol``, runs the CeleRS-side implementation of the task, and
   renders a Celery result record;
3. that record goes back into Redis, and Python's own ``AsyncResult`` reads it.

Step 3 is the part that cannot be faked: if CeleRS got the envelope, the
arguments, or the result shape wrong, Celery's client either raises or returns
the wrong value.
"""

from __future__ import annotations

import json

import pytest
from celery.exceptions import TimeoutError as CeleryTimeoutError
from celery.result import AsyncResult

import harness

pytestmark = pytest.mark.usefixtures("bridge")


def publish_and_take(client, queue_name, signature_result) -> dict:
    """Take the envelope Celery just published, verbatim, off the queue."""
    raw = client.rpop(queue_name)
    assert raw is not None, (
        f"Celery published nothing to {queue_name}; the client and the test "
        "disagree about the queue name"
    )
    return json.loads(raw)


def round_trip(client, keyring, queue_name, async_result) -> AsyncResult:
    """Run one message through CeleRS and hand the result back to Celery."""
    envelope = publish_and_take(client, queue_name, async_result)
    meta = harness.run_bridge("execute", envelope)
    keyring.track_task(async_result.id)
    harness.store_celers_result(client, async_result.id, meta)
    return async_result


# =============================================================================
# Arguments
# =============================================================================


def test_positional_arguments(client, keyring, queue_name, celery_app):
    import tasks

    pending = tasks.add.apply_async(args=[4, 5], queue=queue_name)
    result = round_trip(client, keyring, queue_name, pending)

    assert result.get(timeout=20) == 9
    assert result.successful()
    assert result.state == "SUCCESS"


def test_keyword_arguments(client, keyring, queue_name, celery_app):
    import tasks

    pending = tasks.greet.apply_async(
        args=("Ada",), kwargs={"punct": "?", "loud": True}, queue=queue_name
    )
    result = round_trip(client, keyring, queue_name, pending)

    # CeleRS had to read `loud=True` out of the JSON body as a real bool and
    # `punct` as a string; getting either wrong changes this value.
    assert result.get(timeout=20) == "HELLO, ADA?"


def test_celers_reads_the_arguments_celery_actually_sent(client, queue_name):
    """The decode step on its own, so a failure points at the field."""
    import tasks

    tasks.greet.apply_async(
        args=("Ada",), kwargs={"punct": "!", "loud": False}, queue=queue_name
    )
    envelope = publish_and_take(client, queue_name, None)
    decoded = harness.run_bridge("decode-task", envelope)

    assert decoded["task"] == "tasks.greet"
    assert decoded["lang"] == "py"
    assert decoded["args"] == ["Ada"]
    assert decoded["kwargs"] == {"punct": "!", "loud": False}
    assert decoded["content_type"] == "application/json"
    assert decoded["delivery_mode"] == 2
    # Celery's own repr headers survive into CeleRS unchanged.
    assert decoded["argsrepr"] == "('Ada',)"
    assert decoded["kwargsrepr"] == "{'punct': '!', 'loud': False}"


# =============================================================================
# Scheduling headers
# =============================================================================


def test_countdown_arrives_as_an_absolute_eta(client, queue_name):
    import tasks

    tasks.add.apply_async(args=[1, 2], countdown=45, queue=queue_name)
    decoded = harness.run_bridge("decode-task", publish_and_take(client, queue_name, None))

    # Celery resolves `countdown` on the client; a consumer that waited for a
    # `countdown` field would never run the task.
    assert decoded["eta"] is not None
    assert decoded["expires"] is None


def test_eta_and_expires_arrive_as_utc_instants(client, queue_name):
    import datetime

    import tasks

    eta = datetime.datetime(2031, 6, 7, 8, 9, 10, tzinfo=datetime.timezone.utc)
    expires = datetime.datetime(2031, 6, 7, 9, 9, 10, tzinfo=datetime.timezone.utc)
    tasks.add.apply_async(
        args=[1, 2], eta=eta, expires=expires, queue=queue_name
    )
    decoded = harness.run_bridge("decode-task", publish_and_take(client, queue_name, None))

    assert decoded["eta"].startswith("2031-06-07T08:09:10")
    assert decoded["expires"].startswith("2031-06-07T09:09:10")


# =============================================================================
# Failure
# =============================================================================


def test_failure_round_trips_with_a_traceback(client, keyring, queue_name, celery_app):
    import tasks

    pending = tasks.boom.apply_async(args=["from celers"], queue=queue_name)
    result = round_trip(client, keyring, queue_name, pending)

    with pytest.raises(ValueError) as raised:
        result.get(timeout=20)
    # Celery rebuilt a real ValueError from the exc_type/exc_module/exc_message
    # dict CeleRS wrote -- that only works if all three are right.
    assert "from celers" in str(raised.value)

    assert result.state == "FAILURE"
    assert result.failed()

    # `traceback` is a sibling string of `result`, not part of the exception
    # dict. Without it a mixed cluster loses every stack trace.
    assert result.traceback is not None
    assert result.traceback.startswith("Traceback (most recent call last):")


def test_a_result_already_stored_is_read_without_a_publish(
    client, keyring, queue_name, celery_app
):
    """A result that is already in Redis needs no pub/sub notification.

    ``AsyncResult.get()`` reads the key once before it starts waiting, so a
    writer that only ``SET``s is sufficient for a client that arrives late.
    The *other* ordering is the one that matters -- see the next test.
    """
    import tasks

    pending = tasks.add.apply_async(args=[2, 3], queue=queue_name)
    envelope = publish_and_take(client, queue_name, pending)
    meta = harness.run_bridge("execute", envelope)
    keyring.track_task(pending.id)

    harness.store_celers_result(client, pending.id, meta, publish=False)
    assert pending.get(timeout=10) == 5


def test_a_late_result_must_be_published_to_wake_a_waiting_client(
    client, keyring, queue_name, celery_app
):
    """The interop requirement a CeleRS result backend has to meet.

    In production the client calls ``get()`` *before* the task finishes. Once
    waiting, Celery's Redis backend is parked on a pub/sub channel named by the
    result key -- it does not re-read the key. Celery's own backend therefore
    writes results with ``SET`` **and** ``PUBLISH``.

    So a CeleRS result backend that only ``SET``s leaves every already-waiting
    Python client blocked until its timeout, even though the value is sitting
    in Redis. Both halves are asserted here so the requirement is evidence
    rather than folklore.
    """
    import threading

    import tasks

    def write_later(task_id: str, value: int, publish: bool) -> None:
        meta = {
            "status": "SUCCESS",
            "result": value,
            "traceback": None,
            "children": [],
            "date_done": "2026-01-01T00:00:00+00:00",
            "task_id": task_id,
        }
        threading.Timer(
            0.5, harness.store_celers_result, (client, task_id, meta, publish)
        ).start()

    # Without a publish: the waiter never learns the value arrived.
    silent = tasks.add.apply_async(args=[1, 1], queue=queue_name)
    client.rpop(queue_name)
    keyring.track_task(silent.id)
    write_later(silent.id, 2, publish=False)
    with pytest.raises(CeleryTimeoutError):
        silent.get(timeout=4)
    # ...though it was in Redis the whole time.
    assert AsyncResult(silent.id, app=celery_app).result == 2

    # With a publish: the same write reaches the same waiter.
    announced = tasks.add.apply_async(args=[1, 1], queue=queue_name)
    client.rpop(queue_name)
    keyring.track_task(announced.id)
    write_later(announced.id, 2, publish=True)
    assert announced.get(timeout=10) == 2


# =============================================================================
# Canvas
# =============================================================================


def test_chain_of_two_tasks(client, keyring, queue_name, celery_app):
    """CeleRS runs the head and follows the embedded chain to the second link.

    Celery does not put a chain in the headers: the remaining links ride inside
    the body's embed dict, each carrying the id Celery pre-assigned to it. A
    consumer that reads only headers runs the first task and silently drops the
    rest.
    """
    from celery import chain

    import tasks

    workflow = chain(tasks.add.s(4, 5), tasks.double.s())
    pending = workflow.apply_async(queue=queue_name)

    head_envelope = publish_and_take(client, queue_name, None)
    decoded = harness.run_bridge("decode-task", head_envelope)
    assert decoded["chain"] == ["tasks.double"], decoded["chain"]
    assert decoded["has_workflow"] is True

    link = decoded["chain_links"][0]
    assert link["task_id"], "Celery pre-assigns the next link's task id"

    # Run the head through CeleRS...
    head_meta = harness.run_bridge("execute", head_envelope)
    keyring.track_task(decoded["id"])
    harness.store_celers_result(client, decoded["id"], head_meta)
    assert head_meta["result"] == 9

    # ...then the link, with the head's return value prepended the way Celery
    # chains a non-immutable signature.
    link_envelope = harness.run_bridge(
        "encode-task",
        {
            "task": link["task"],
            "id": link["task_id"],
            "args": [head_meta["result"], *link["args"]],
            "kwargs": link["kwargs"],
        },
    )
    link_meta = harness.run_bridge("execute", link_envelope)
    keyring.track_task(link["task_id"])
    harness.store_celers_result(client, link["task_id"], link_meta)

    # Celery's own handle on the chain reads the final value.
    assert pending.get(timeout=20) == 18
    assert AsyncResult(decoded["id"], app=celery_app).get(timeout=20) == 9


def test_group_members_carry_their_group(client, keyring, queue_name, celery_app):
    from celery import group

    import tasks

    workflow = group(tasks.add.s(1, 2), tasks.add.s(3, 4))
    pending = workflow.apply_async(queue=queue_name)

    seen_groups = set()
    seen_indexes = set()
    values = []
    for _ in range(2):
        envelope = publish_and_take(client, queue_name, None)
        decoded = harness.run_bridge("decode-task", envelope)
        assert decoded["group"], "a group member names its group"
        seen_groups.add(decoded["group"])
        seen_indexes.add(json.loads(json.dumps(envelope))["headers"]["group_index"])

        meta = harness.run_bridge("execute", envelope)
        keyring.track_task(decoded["id"])
        harness.store_celers_result(client, decoded["id"], meta)
        values.append(meta["result"])

    assert len(seen_groups) == 1, f"both members share one group id: {seen_groups}"
    assert seen_indexes == {0, 1}, f"members are indexed within the group: {seen_indexes}"
    assert sorted(values) == [3, 7]

    # And Celery's GroupResult reads every member back.
    assert sorted(pending.get(timeout=20)) == [3, 7]
