"""Shared plumbing for the CeleRS <-> Python Celery interoperability suite.

Nothing here asserts anything; it is the machinery the tests and the fixture
capture script both need:

* :func:`redis_url` / :func:`redis_client` -- the Redis the suite is pointed at.
* :class:`CeleryWorker` -- a real ``celery -A tasks worker`` subprocess, started
  and stopped around a block, so "Python executes it" means a genuine worker.
* :func:`run_bridge` -- pipes JSON through the CeleRS side, which is the
  ``celery_bridge`` example built from ``celers-protocol``. Everything crossing
  the boundary in either direction goes through that binary, so the suite
  exercises production protocol code rather than a test-local reimplementation.
* :func:`fixtures_dir` -- where the verbatim captures live.
"""

from __future__ import annotations

import json
import os
import pathlib
import subprocess
import time

HERE = pathlib.Path(__file__).resolve().parent
REPO_ROOT = HERE.parent.parent

#: Where the verbatim wire captures are committed.
FIXTURES = REPO_ROOT / "crates" / "celers-protocol" / "tests" / "fixtures"

#: Seconds to wait for a worker to announce itself before giving up.
WORKER_BOOT_TIMEOUT = 60.0

#: Seconds to wait for a result key to appear.
RESULT_TIMEOUT = 30.0


def redis_url() -> str:
    """The Redis URL the suite uses, including the database index."""
    return os.environ["CELERS_COMPAT_REDIS_URL"]


def redis_client():
    """A redis-py client for :func:`redis_url`."""
    import redis

    return redis.Redis.from_url(redis_url())


def bridge_path() -> str | None:
    """Path to the CeleRS-side ``celery_bridge`` binary, if it was built."""
    return os.environ.get("CELERS_BRIDGE")


def fixtures_dir() -> pathlib.Path:
    return FIXTURES


def run_bridge(subcommand: str, payload) -> dict:
    """Pipe ``payload`` through the CeleRS bridge and return its JSON reply.

    The bridge reads one JSON document on stdin and writes one on stdout, so a
    failure is always visible as a non-zero exit plus whatever it wrote to
    stderr -- never as a silently empty result.
    """
    binary = bridge_path()
    if binary is None:
        raise RuntimeError("CELERS_BRIDGE is not set; the CeleRS side is unavailable")
    proc = subprocess.run(
        [binary, subcommand],
        input=json.dumps(payload).encode(),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=60,
    )
    if proc.returncode != 0:
        raise AssertionError(
            f"celery_bridge {subcommand} failed ({proc.returncode}):\n"
            f"{proc.stderr.decode(errors='replace')}"
        )
    return json.loads(proc.stdout.decode())


def result_key(task_id: str) -> str:
    """The Redis key Celery's Redis backend stores a task result under."""
    return f"celery-task-meta-{task_id}"


def store_celers_result(client, task_id: str, meta: dict, publish: bool = True) -> str:
    """Write a CeleRS-produced result record where Celery's backend looks.

    Celery's Redis backend writes a result with ``SET`` **and** ``PUBLISH`` on a
    channel named by the key. The publish is not decoration: with
    ``supports_native_join`` the client's ``AsyncResult.get()`` waits on that
    pub/sub channel, so a writer that only ``SET``s leaves every waiter blocked
    until its timeout. ``publish=False`` exists so a test can demonstrate
    exactly that.
    """
    key = result_key(task_id)
    payload = json.dumps(meta).encode()
    pipe = client.pipeline()
    pipe.setex(key, 3600, payload)
    if publish:
        pipe.publish(key, payload)
    pipe.execute()
    return key


def await_result(client, task_id: str, timeout: float = RESULT_TIMEOUT) -> dict:
    """Block until the result key for ``task_id`` exists, then decode it."""
    key = result_key(task_id)
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        raw = client.get(key)
        if raw is not None:
            return json.loads(raw)
        time.sleep(0.05)
    raise AssertionError(f"no result stored at {key} within {timeout}s")


#: States Celery treats as final. A retrying task publishes a ``RETRY`` record
#: first, so a reader that takes the first record it sees gets an intermediate
#: one.
TERMINAL_STATES = frozenset({"SUCCESS", "FAILURE", "REVOKED"})


def await_terminal_result(client, task_id: str, timeout: float = RESULT_TIMEOUT) -> dict:
    """Block until the stored record reaches a terminal state."""
    key = result_key(task_id)
    deadline = time.monotonic() + timeout
    last = None
    while time.monotonic() < deadline:
        raw = client.get(key)
        if raw is not None:
            last = json.loads(raw)
            if last.get("status") in TERMINAL_STATES:
                return last
        time.sleep(0.05)
    raise AssertionError(
        f"{key} did not reach a terminal state within {timeout}s; last saw {last!r}"
    )


class CeleryWorker:
    """A real Celery worker subprocess, scoped to a ``with`` block.

    ``--pool=solo`` is deliberate: the prefork pool forks the interpreter, and
    on macOS with recent CPython that is unsafe enough to turn an unrelated
    crash into what looks like a protocol failure. Solo runs the task in the
    main thread, which is all the suite needs and is far easier to diagnose.
    """

    def __init__(self, queue: str, log_path: pathlib.Path, extra_args=()):
        self.queue = queue
        self.log_path = log_path
        self.extra_args = list(extra_args)
        self.proc: subprocess.Popen | None = None
        self._log = None

    def __enter__(self) -> "CeleryWorker":
        python = os.environ["CELERS_PYTHON"]
        self._log = self.log_path.open("wb")
        self.proc = subprocess.Popen(
            [
                python,
                "-m",
                "celery",
                "-A",
                "tasks",
                "worker",
                "-Q",
                self.queue,
                "--pool=solo",
                "--without-mingle",
                "--without-gossip",
                "--without-heartbeat",
                "-l",
                "info",
                *self.extra_args,
            ],
            cwd=str(HERE),
            stdout=self._log,
            stderr=subprocess.STDOUT,
            env=os.environ.copy(),
        )
        self._wait_for_ready()
        return self

    def _wait_for_ready(self) -> None:
        deadline = time.monotonic() + WORKER_BOOT_TIMEOUT
        while time.monotonic() < deadline:
            if self.proc is not None and self.proc.poll() is not None:
                raise AssertionError(
                    "celery worker exited during start-up:\n" + self.log_text()
                )
            if "ready." in self.log_text():
                return
            time.sleep(0.1)
        raise AssertionError(
            f"celery worker did not become ready within {WORKER_BOOT_TIMEOUT}s:\n"
            + self.log_text()
        )

    def log_text(self) -> str:
        try:
            return self.log_path.read_text(errors="replace")
        except FileNotFoundError:
            return ""

    def __exit__(self, *_exc) -> None:
        if self.proc is not None and self.proc.poll() is None:
            self.proc.terminate()
            try:
                self.proc.wait(timeout=20)
            except subprocess.TimeoutExpired:
                self.proc.kill()
                self.proc.wait(timeout=20)
        if self._log is not None:
            self._log.close()
