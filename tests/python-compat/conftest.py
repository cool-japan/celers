"""Gating and fixtures for the CeleRS <-> Python Celery interoperability suite.

The suite touches a real Redis and boots a real Celery worker, so it refuses to
run -- visibly -- unless it has been told where both live:

* ``CELERS_TEST_REDIS_URL`` -- the Redis the whole hardening suite uses.
* ``CELERS_PYTHON``          -- the interpreter with ``celery`` installed; the
  suite spawns workers with it, so it cannot be guessed from ``sys.executable``
  without silently testing the wrong environment.
* ``CELERS_BRIDGE``          -- the ``celery_bridge`` binary built from
  ``celers-protocol``. Only the round-trip tests need it; the golden-fixture
  and repr tests run without any Rust at all.

Every missing prerequisite produces a printed ``SKIPPED:`` line, never a silent
pass.

Isolation: the suite uses a dedicated Redis database (``CELERS_COMPAT_REDIS_DB``,
default 15) and a queue name unique to the session, and deletes every key it
created on the way out. It never calls ``FLUSHDB``.
"""

from __future__ import annotations

import os
import pathlib
import sys
import uuid

import pytest

HERE = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))

#: Redis database the suite confines itself to.
DEFAULT_DB = "15"


def _skip(reason: str, hint: str) -> None:
    print(f"SKIPPED: tests/python-compat -- {reason} (set {hint} to run)",
          file=sys.stderr)


def _compose_url(base: str, db: str) -> str:
    """Point ``base`` at database ``db``, replacing any database it names."""
    from urllib.parse import urlsplit, urlunsplit

    parts = urlsplit(base)
    return urlunsplit((parts.scheme, parts.netloc, f"/{db}", parts.query,
                       parts.fragment))


def pytest_configure(config):
    """Resolve the environment once, before collection imports ``tasks``."""
    redis_url = os.environ.get("CELERS_TEST_REDIS_URL")
    python = os.environ.get("CELERS_PYTHON")

    config._celers_skip = None
    if not redis_url:
        _skip("no Redis configured", "CELERS_TEST_REDIS_URL")
        config._celers_skip = "CELERS_TEST_REDIS_URL is not set"
        return
    if not python:
        _skip("no Celery interpreter configured", "CELERS_PYTHON")
        config._celers_skip = "CELERS_PYTHON is not set"
        return
    if not pathlib.Path(python).exists():
        _skip(f"CELERS_PYTHON={python} does not exist", "CELERS_PYTHON")
        config._celers_skip = f"CELERS_PYTHON={python} does not exist"
        return

    db = os.environ.get("CELERS_COMPAT_REDIS_DB", DEFAULT_DB)
    os.environ["CELERS_COMPAT_REDIS_URL"] = _compose_url(redis_url, db)
    os.environ.setdefault(
        "CELERS_COMPAT_QUEUE", f"celers-compat-{uuid.uuid4().hex[:12]}"
    )


def pytest_collection_modifyitems(config, items):
    reason = getattr(config, "_celers_skip", None)
    if reason is None:
        return
    marker = pytest.mark.skip(reason=reason)
    for item in items:
        item.add_marker(marker)


@pytest.fixture(scope="session")
def queue_name() -> str:
    return os.environ["CELERS_COMPAT_QUEUE"]


@pytest.fixture(scope="session")
def client():
    """A redis-py client that cleans up after the whole session."""
    import harness

    connection = harness.redis_client()
    yield connection
    connection.close()


@pytest.fixture(scope="session")
def keyring(client, queue_name):
    """Records every key the suite creates and deletes them on the way out.

    A ``FLUSHDB`` would be simpler and is exactly what a shared machine must
    never do, so the suite tracks its own keys instead.
    """
    created: set[str] = set()

    class Keyring:
        def track(self, *keys: str) -> None:
            created.update(keys)

        def track_task(self, task_id: str) -> str:
            key = f"celery-task-meta-{task_id}"
            created.add(key)
            return key

    yield Keyring()

    # Queue-scoped keys kombu creates on its own, plus everything the tests
    # registered. Deleting a missing key is a no-op, so this is safe to repeat.
    created.update(
        {
            queue_name,
            f"_kombu.binding.{queue_name}",
            # A retry routed with the default routing key lands here; deleting
            # it keeps a failed run from leaving work behind.
            "celery",
            "_kombu.binding.celery",
            "_kombu.binding.celeryev",
            "unacked",
            "unacked_index",
            "unacked_mutex",
        }
    )
    if created:
        client.delete(*sorted(created))


@pytest.fixture(scope="session")
def bridge():
    """The CeleRS-side binary, or a visible skip when it was not built."""
    import harness

    path = harness.bridge_path()
    if not path or not pathlib.Path(path).exists():
        print(
            "SKIPPED: CeleRS round-trip tests -- celery_bridge was not built "
            "(set CELERS_BRIDGE to run; ./run.sh builds it)",
            file=sys.stderr,
        )
        pytest.skip("CELERS_BRIDGE is not set or does not exist")
    return path


@pytest.fixture(scope="session")
def celery_app(queue_name):
    """The Celery app the tests publish through."""
    import tasks

    return tasks.app


@pytest.fixture
def worker_log(tmp_path) -> pathlib.Path:
    return tmp_path / "celery-worker.log"
