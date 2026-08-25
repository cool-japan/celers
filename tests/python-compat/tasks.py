"""Python Celery tasks executed by both runtimes in the interop suite.

The same task *names* are implemented on both sides:

* here, as real Celery tasks a ``celery -A tasks worker`` executes;
* in ``crates/celers-protocol/examples/celery_bridge.rs``, as the CeleRS-side
  interpretation of the decoded protocol v2 body.

That is what makes a round trip meaningful: a message produced by either
runtime names a task the other one can actually run, and both must agree on
what the arguments mean.
"""

import os

from celery import Celery

app = Celery("celers_compat")
app.config_from_object("celeryconfig")

# The queue is unique per test session (see conftest.py) so a stray worker from
# an earlier run can never steal a message from this one.
QUEUE = os.environ.get("CELERS_COMPAT_QUEUE", "celers-compat")
app.conf.task_default_queue = QUEUE


@app.task(name="tasks.add")
def add(x, y):
    """Positional arguments: the simplest possible round trip."""
    return x + y


@app.task(name="tasks.greet")
def greet(name, punct="!", loud=False):
    """Keyword arguments, including a bool default.

    Exercises the ``kwargsrepr`` header as well as the body: Python renders
    ``True``/``False``/``None`` where JSON has ``true``/``false``/``null``.
    """
    text = f"Hello, {name}{punct}"
    return text.upper() if loud else text


@app.task(name="tasks.boom")
def boom(message="boom"):
    """Always fails, so the failure/traceback wire shape can be asserted."""
    raise ValueError(message)


@app.task(bind=True, name="tasks.flaky", max_retries=3)
def flaky(self, marker_key, countdown=1):
    """Fails once, then succeeds -- a real ``retry(countdown=...)`` path.

    The attempt counter lives in Redis rather than in process memory because
    ``self.retry`` re-publishes the task and a ``--pool=solo`` worker may pick
    it up in the same process or a different one.
    """
    import redis

    client = redis.Redis.from_url(os.environ["CELERS_COMPAT_REDIS_URL"])
    attempt = client.incr(marker_key)
    client.expire(marker_key, 300)
    if attempt == 1:
        raise self.retry(exc=RuntimeError("first attempt always fails"),
                         countdown=countdown)
    return attempt


@app.task(name="tasks.double")
def double(x):
    """Second link of the two-task chain: consumes the previous return value."""
    return x * 2
