"""``argsrepr`` / ``kwargsrepr``: CeleRS must render what Celery renders.

Two independent checks:

* the committed repr table still matches ``celery.utils.saferepr`` in the
  installed Celery -- if not, the table is stale and the Rust assertions built
  on it are meaningless;
* the CeleRS bridge reproduces every entry in it, so the header a monitor
  displays is the same string either runtime would have written.

These headers are display-only: no dispatch decision reads them. That is
exactly why they rot unnoticed, and why they get their own test. The bug this
guards against produced ``"[Number(4), Number(5)]"`` -- Rust's ``Debug`` for
``serde_json::Value`` -- in a field Flower renders verbatim.
"""

from __future__ import annotations

import json

import pytest
from celery.utils.saferepr import saferepr

import harness


def table() -> dict:
    return json.loads((harness.fixtures_dir() / "celery_saferepr_table.json").read_text())


def sorted_deep(value):
    """Rebuild ``value`` with every dict in sorted-key order.

    ``serde_json::Map`` is ordered, so CeleRS can only ever render a dict with
    sorted keys. Comparing against an insertion-ordered Python repr would pin a
    difference CeleRS cannot close; the ordering caveat is asserted on its own
    below.
    """
    if isinstance(value, dict):
        return {key: sorted_deep(value[key]) for key in sorted(value)}
    if isinstance(value, list):
        return [sorted_deep(item) for item in value]
    return value


# =============================================================================
# The table is current
# =============================================================================


def test_the_recorded_args_reprs_still_match_installed_celery():
    for case in table()["args"]:
        assert saferepr(tuple(case["value"])) == case["repr"], (
            f"celery.utils.saferepr changed for {case['value']!r}; "
            "re-run ./run.sh capture"
        )


def test_the_recorded_kwargs_reprs_still_match_installed_celery():
    for case in table()["kwargs"]:
        assert saferepr(sorted_deep(case["value"])) == case["repr"], (
            f"celery.utils.saferepr changed for {case['value']!r}; "
            "re-run ./run.sh capture"
        )


def test_saferepr_is_not_the_builtin_repr():
    """The distinction the CeleRS implementation is built on.

    ``saferepr`` always single-quotes a string and escapes an embedded quote as
    ``\\'``; the builtin switches to double quotes instead. Implementing
    ``repr`` would therefore be a *bug*, not a simplification.
    """
    assert saferepr(("it's",)) == "('it\\'s',)"
    assert repr(("it's",)) == '("it\'s",)'
    assert saferepr(("it's",)) != repr(("it's",))


def test_saferepr_elides_containers_opened_at_the_third_level():
    """The ``maxlevels=3`` rule CeleRS reproduces.

    A container is elided to ``{...}`` / ``[...]`` when it is opened with three
    containers already on the stack. The enclosing tuple of ``argsrepr``
    *counts*, so the same value nests one level deeper in ``argsrepr`` than in
    ``kwargsrepr`` -- which is why the two are one level apart below.
    """
    # argsrepr: the tuple is level 1.
    assert saferepr(({"a": {"b": 1}},)) == "({'a': {'b': 1}},)"
    assert saferepr(({"a": {"b": {"c": 1}}},)) == "({'a': {'b': {...}}},)"

    # kwargsrepr: the dict itself is level 1, so one more level fits.
    assert saferepr({"a": {"b": {"c": 1}}}) == "{'a': {'b': {'c': 1}}}"
    assert saferepr({"a": {"b": {"c": [1]}}}) == "{'a': {'b': {'c': [...]}}}"


@pytest.mark.usefixtures("bridge")
def test_celers_reproduces_the_nesting_rule_in_both_contexts():
    """The tuple wrapper's level offset is easy to get wrong by one."""
    cases = [
        ([{"a": {"b": 1}}], {}, "argsrepr", "({'a': {'b': 1}},)"),
        ([{"a": {"b": {"c": 1}}}], {}, "argsrepr", "({'a': {'b': {...}}},)"),
        ([], {"a": {"b": {"c": 1}}}, "kwargsrepr", "{'a': {'b': {'c': 1}}}"),
        ([], {"a": {"b": {"c": [1]}}}, "kwargsrepr", "{'a': {'b': {'c': [...]}}}"),
    ]
    for args, kwargs, header, expected in cases:
        envelope = harness.run_bridge(
            "encode-task",
            {
                "task": "tasks.add",
                "id": "7b1a0d1e-0000-4000-8000-00000000000e",
                "args": args,
                "kwargs": kwargs,
            },
        )
        assert envelope["headers"][header] == expected
        # ...and Celery agrees, so the expectation is not merely self-consistent.
        subject = tuple(args) if header == "argsrepr" else kwargs
        assert saferepr(subject) == expected


# =============================================================================
# CeleRS reproduces it
# =============================================================================


@pytest.mark.usefixtures("bridge")
def test_celers_reproduces_every_recorded_repr():
    """Drive the reprs through the real header-building path.

    ``encode-task`` calls ``compat::create_python_celery_message``, so this
    asserts the header a CeleRS producer actually writes -- not a helper that
    only the test calls.
    """
    entries = table()
    for index, case in enumerate(entries["args"]):
        envelope = harness.run_bridge(
            "encode-task",
            {
                "task": "tasks.add",
                "id": f"7b1a0d1e-0000-4000-8000-{index:012d}",
                "args": case["value"],
                "kwargs": {},
            },
        )
        assert envelope["headers"]["argsrepr"] == case["repr"], (
            f"CeleRS renders {case['value']!r} as "
            f"{envelope['headers']['argsrepr']!r}, Celery as {case['repr']!r}"
        )

    for index, case in enumerate(entries["kwargs"]):
        envelope = harness.run_bridge(
            "encode-task",
            {
                "task": "tasks.add",
                "id": f"7b1a0d1e-0000-4000-8000-{index:012d}",
                "args": [],
                "kwargs": case["value"],
            },
        )
        assert envelope["headers"]["kwargsrepr"] == case["repr"], (
            f"CeleRS renders {case['value']!r} as "
            f"{envelope['headers']['kwargsrepr']!r}, Celery as {case['repr']!r}"
        )


@pytest.mark.usefixtures("bridge")
def test_no_rust_debug_formatting_reaches_the_wire():
    """The specific regression: ``format!("{:?}", args)`` on a JSON value."""
    envelope = harness.run_bridge(
        "encode-task",
        {
            "task": "tasks.add",
            "id": "7b1a0d1e-0000-4000-8000-00000000ffff",
            "args": [4, "x", {"a": 1}, None, True],
            "kwargs": {"flag": False, "n": 2.0},
        },
    )
    rendered = envelope["headers"]["argsrepr"] + envelope["headers"]["kwargsrepr"]
    for leak in ("Number(", "String(", "Object(", "Array(", "Bool(", "Null"):
        assert leak not in rendered, f"Rust Debug leaked into the wire: {rendered}"

    # And it is exactly what Celery would have written.
    assert envelope["headers"]["argsrepr"] == saferepr((4, "x", {"a": 1}, None, True))
    assert envelope["headers"]["kwargsrepr"] == saferepr({"flag": False, "n": 2.0})
