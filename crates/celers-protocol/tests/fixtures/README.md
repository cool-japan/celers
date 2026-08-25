# Verbatim Celery wire captures

Recorded by `tests/python-compat/capture_fixtures.py` from a **real** Python
Celery talking to a **real** Redis. Re-record them with:

```sh
CELERS_TEST_REDIS_URL=redis://127.0.0.1:6379 tests/python-compat/run.sh capture
```

| | |
|---|---|
| celery | 5.6.3 |
| kombu | 5.6.2 |
| python | 3.14.6 |
| task_protocol | 2 |
| serializer | json |

## What is in each file

| file | what it records |
|---|---|
| `celery_task_v2_positional_list.json` | `add.apply_async(args=[4, 5])` -- list `argsrepr` |
| `celery_task_v2_positional_tuple.json` | `add.apply_async(args=(4, 5))` -- tuple `argsrepr`, the `.delay()` form CeleRS emits |
| `celery_task_v2_kwargs.json` | keyword arguments, including a bool -- `kwargsrepr` with `True` |
| `celery_task_v2_countdown.json` | `countdown=30`, which Celery resolves to an `eta` header |
| `celery_task_v2_eta_expires.json` | explicit `eta` and `expires`, plus `properties.expiration` |
| `celery_task_v2_chain_head.json` | head of `chain(add.s(1, 2), double.s())` -- the `chain` embed |
| `celery_task_v2_group_member.json` | one member of a `group` -- the `group` and `group_index` headers |
| `celery_result_success.json` | result record a real worker wrote for a success |
| `celery_result_failure.json` | result record a real worker wrote for a raised `ValueError` |
| `celery_saferepr_table.json` | what `celery.utils.saferepr` renders for the values CeleRS must reproduce |
| `celers_envelope_accepted_by_celery.json` | a **CeleRS**-built envelope, recorded only after a real worker executed it |

## Normalization

The bytes are Celery's own, with four environment-specific substrings replaced
so the files are machine-independent:

* the Python environment root -> `<python-env>`
* the repository root -> `<repo>`
* the capturing machine's hostname -> `capture-host` (this reaches the `origin`
  header and the worker's node name)
* the queue is always `celers-compat-capture`, so `delivery_info` is stable

No structural change is made: no re-serialization, no key reordering, no
dropped fields. `reply_to` and `delivery_tag` are generated per process by
kombu and will differ between captures.
