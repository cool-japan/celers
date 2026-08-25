"""Celery configuration for the CeleRS interoperability suite.

Every setting here exists because CeleRS depends on it being what it is:

* ``task_serializer`` / ``result_serializer`` / ``accept_content`` -- JSON is
  the only content type both runtimes speak, and the suite asserts byte shapes
  that only hold for JSON.
* ``task_protocol = 2`` -- protocol v1 puts the task name in the *body* rather
  than the headers; ``celers_protocol::Message`` only models v2.
* ``enable_utc`` / ``timezone`` -- CeleRS renders every ``eta`` / ``expires``
  as a UTC RFC 3339 instant, so the comparison is only meaningful in UTC.
* ``result_expires`` -- the suite deletes its own keys, but a crashed run must
  not leave result keys behind forever.

The broker and backend URLs come from the environment so the suite can be
pointed at any Redis; see ``conftest.py`` for the gating rules.
"""

import os

_URL = os.environ["CELERS_COMPAT_REDIS_URL"]

broker_url = _URL
result_backend = _URL

task_serializer = "json"
result_serializer = "json"
accept_content = ["json"]
result_accept_content = ["json"]

task_protocol = 2

enable_utc = True
timezone = "UTC"

# Results the suite forgets to delete disappear on their own within the hour.
result_expires = 3600

# One task in flight at a time keeps the round-trip assertions deterministic.
worker_prefetch_multiplier = 1

# The suite never relies on worker-to-worker discovery, and skipping it saves
# more than a second of start-up per worker the tests boot.
worker_send_task_events = False
