"""The committed captures still describe the Celery that is installed.

The Rust tests in ``crates/celers-protocol/tests/celery_golden.rs`` assert
against files recorded from a particular Celery release. Those files can only
be trusted while they still match what Celery emits *today* -- otherwise CeleRS
would keep passing against a museum piece.

This module is the drift detector. It republishes the same calls the capture
script made and compares the *shape* of what Celery produces against what was
recorded: the header names, the property names, the body framing. It
deliberately does not compare values that are expected to vary (ids, hostnames,
delivery tags, timestamps).

When one of these fails, the fix is usually::

    ./run.sh capture

...followed by reading the diff to see what Celery changed, and only then
re-running the Rust suite.

No Rust is required here.
"""

from __future__ import annotations

import base64
import json

import celery
import pytest

import harness


def fixture(name: str) -> dict:
    return json.loads((harness.fixtures_dir() / name).read_text())


def take(client, queue_name) -> dict:
    raw = client.rpop(queue_name)
    assert raw is not None, f"Celery published nothing to {queue_name}"
    return json.loads(raw)


def body_of(envelope: dict) -> list:
    assert envelope["properties"]["body_encoding"] == "base64"
    return json.loads(base64.b64decode(envelope["body"]))


# =============================================================================


def test_the_fixtures_record_the_installed_celery(client):
    """A version mismatch is a warning, not a failure -- but a loud one."""
    readme = (harness.fixtures_dir() / "README.md").read_text()
    if f"| celery | {celery.__version__} |" not in readme:
        pytest.skip(
            f"fixtures were captured from a different Celery than the installed "
            f"{celery.__version__}; run ./run.sh capture to re-record, then "
            f"re-run the Rust suite"
        )


def test_header_set_is_unchanged(client, queue_name):
    """Celery has not added or removed a protocol v2 header."""
    import tasks

    tasks.add.apply_async(args=(4, 5), queue=queue_name)
    live = take(client, queue_name)
    recorded = fixture("celery_task_v2_positional_tuple.json")

    assert set(live["headers"]) == set(recorded["headers"]), (
        "Celery's protocol v2 header set changed; re-capture the fixtures and "
        "check whether celers-protocol needs a new typed field"
    )
    assert set(live["properties"]) == set(recorded["properties"])
    assert live["content-type"] == recorded["content-type"] == "application/json"
    assert live["content-encoding"] == recorded["content-encoding"] == "utf-8"

    # The values that must not vary at all.
    for key in ("lang", "task", "argsrepr", "kwargsrepr", "retries", "timelimit"):
        assert live["headers"][key] == recorded["headers"][key], key
    assert live["properties"]["delivery_mode"] == 2
    assert live["properties"]["body_encoding"] == "base64"


def test_body_framing_is_unchanged(client, queue_name):
    """The body is still the ``[args, kwargs, embed]`` tuple with four keys."""
    import tasks

    tasks.add.apply_async(args=(4, 5), queue=queue_name)
    live = body_of(take(client, queue_name))
    recorded = body_of(fixture("celery_task_v2_positional_tuple.json"))

    assert len(live) == 3, "protocol v2 frames the body as a 3-tuple"
    assert live == recorded, "the body Celery emits changed"
    assert set(live[2]) == {"callbacks", "errbacks", "chain", "chord"}


def test_kwargs_envelope_shape_is_unchanged(client, queue_name):
    import tasks

    tasks.greet.apply_async(
        args=("Ada",), kwargs={"loud": True, "punct": "?"}, queue=queue_name
    )
    live = take(client, queue_name)
    recorded = fixture("celery_task_v2_kwargs.json")

    assert live["headers"]["kwargsrepr"] == recorded["headers"]["kwargsrepr"]
    assert body_of(live)[1] == body_of(recorded)[1] == {"loud": True, "punct": "?"}


def test_chain_still_travels_in_the_embed_dict(client, queue_name):
    from celery import chain

    import tasks

    chain(tasks.add.s(1, 2), tasks.double.s()).apply_async(queue=queue_name)
    live = body_of(take(client, queue_name))
    recorded = body_of(fixture("celery_task_v2_chain_head.json"))

    assert live[2]["chain"] is not None, "the chain still rides in the body"
    assert [link["task"] for link in live[2]["chain"]] == [
        link["task"] for link in recorded[2]["chain"]
    ]
    # The next link's id is pre-assigned and travels in its options.
    assert "task_id" in live[2]["chain"][0]["options"]
    client.delete(queue_name)


def test_group_headers_are_unchanged(client, queue_name):
    from celery import group

    import tasks

    group(tasks.add.s(1, 2), tasks.add.s(3, 4)).apply_async(queue=queue_name)
    live = take(client, queue_name)
    recorded = fixture("celery_task_v2_group_member.json")

    assert live["headers"]["group"] is not None
    assert live["headers"]["group_index"] == recorded["headers"]["group_index"]
    client.delete(queue_name)


def test_result_record_shape_is_unchanged(client, keyring, queue_name, worker_log):
    """Celery still writes the same result keys, for success and for failure."""
    import tasks

    with harness.CeleryWorker(queue_name, worker_log):
        ok = tasks.add.apply_async(args=[4, 5], queue=queue_name)
        keyring.track_task(ok.id)
        live_ok = harness.await_result(client, ok.id)

        bad = tasks.boom.apply_async(args=["drift check"], queue=queue_name)
        keyring.track_task(bad.id)
        live_bad = harness.await_result(client, bad.id)

    assert set(live_ok) == set(fixture("celery_result_success.json"))
    assert set(live_bad) == set(fixture("celery_result_failure.json"))

    # The parts celers-protocol reads by name.
    assert live_ok["status"] == "SUCCESS"
    assert live_ok["result"] == 9
    assert live_bad["status"] == "FAILURE"
    assert set(live_bad["result"]) == {"exc_type", "exc_message", "exc_module"}
    assert isinstance(live_bad["result"]["exc_message"], list), (
        "exc_message is exc.args; a bare string would change how Celery "
        "reconstructs the exception"
    )
    assert isinstance(live_bad["traceback"], str)
