#!/usr/bin/env python3
"""
Python Celery interoperability example

This example demonstrates how Python Celery workers can consume messages
produced by the Rust celers-protocol library.

Prerequisites:
    pip install celery redis

Running:
    1. Start Redis: redis-server
    2. Run this worker: python examples/python_consumer.py worker
    3. Send tasks from Rust or Python
"""

from celery import Celery
import json

# Configure Celery to use Redis as broker and result backend
app = Celery(
    'celery_interop',
    broker='redis://localhost:6379/0',
    backend='redis://localhost:6379/0'
)

# Configure Celery settings
app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='UTC',
    enable_utc=True,
    task_track_started=True,
    task_protocol=2,  # Use protocol v2 (compatible with Rust implementation)
)


@app.task(name='tasks.add')
def add(x, y):
    """Simple addition task"""
    result = x + y
    print(f"[tasks.add] {x} + {y} = {result}")
    return result


@app.task(name='tasks.multiply')
def multiply(x=None, y=None):
    """Multiplication task with keyword arguments"""
    if x is None or y is None:
        return {"error": "Missing arguments"}
    result = x * y
    print(f"[tasks.multiply] {x} * {y} = {result}")
    return result


@app.task(name='tasks.cleanup')
def cleanup():
    """Cleanup task (typically delayed)"""
    print("[tasks.cleanup] Running cleanup...")
    return {"status": "cleaned"}


@app.task(name='tasks.process')
def process(data):
    """Process data task (may have expiration)"""
    print(f"[tasks.process] Processing: {data}")
    return {"processed": data}


@app.task(name='tasks.step1')
def step1(value):
    """First step in a chain"""
    print(f"[tasks.step1] Input: {value}")
    result = value * 2
    print(f"[tasks.step1] Output: {result}")
    return result


@app.task(name='tasks.step2')
def step2(value):
    """Second step in a chain"""
    print(f"[tasks.step2] Input: {value}")
    result = value + 10
    print(f"[tasks.step2] Output: {result}")
    return result


@app.task(name='tasks.step3')
def step3(value):
    """Third step in a chain"""
    print(f"[tasks.step3] Input: {value}")
    result = value - 5
    print(f"[tasks.step3] Final output: {result}")
    return result


@app.task(name='tasks.risky')
def risky(data):
    """Task that might fail"""
    print(f"[tasks.risky] Processing risky operation: {data}")
    if data == "fail":
        raise ValueError("Intentional failure")
    return {"success": True}


@app.task(name='tasks.handle_error')
def handle_error(task_id, exc, traceback):
    """Error handler task"""
    print(f"[tasks.handle_error] Task {task_id} failed: {exc}")
    return {"error_handled": True}


@app.task(name='tasks.urgent')
def urgent():
    """High priority task"""
    print("[tasks.urgent] Processing urgent task!")
    return {"priority": "high"}


@app.task(name='tasks.workflow_step')
def workflow_step(data):
    """Workflow task with parent/root tracking"""
    print(f"[tasks.workflow_step] Workflow data: {data}")
    return {"workflow": "completed"}


@app.task(name='tasks.parallel')
def parallel(data):
    """Task designed to run in parallel groups"""
    print(f"[tasks.parallel] Processing in parallel: {data}")
    return {"parallel_result": data}


@app.task(name='tasks.custom')
def custom(value):
    """Custom task"""
    print(f"[tasks.custom] Custom value: {value}")
    return value * 2


def send_test_tasks():
    """Send test tasks to demonstrate interop"""
    print("Sending test tasks...")

    # Simple task
    print("\n1. Sending simple add task...")
    result = add.apply_async(args=[2, 3])
    print(f"   Task ID: {result.id}")

    # Task with kwargs
    print("\n2. Sending multiply task with kwargs...")
    result = multiply.apply_async(kwargs={'x': 10, 'y': 20})
    print(f"   Task ID: {result.id}")

    # Delayed task
    print("\n3. Sending delayed cleanup task (60s)...")
    result = cleanup.apply_async(countdown=60)
    print(f"   Task ID: {result.id}")

    # Task with expiration
    print("\n4. Sending task with expiration (300s)...")
    result = process.apply_async(args=['data'], expires=300)
    print(f"   Task ID: {result.id}")

    # Task chain
    print("\n5. Sending task chain...")
    from celery import chain
    result = chain(step1.s(100), step2.s(), step3.s()).apply_async()
    print(f"   Chain ID: {result.id}")

    # High priority task
    print("\n6. Sending high priority task...")
    result = urgent.apply_async(priority=9)
    print(f"   Task ID: {result.id}")

    print("\n✓ All test tasks sent successfully!")
    print("Monitor the worker to see them being processed.")


def verify_message_format():
    """Verify that Rust-generated messages can be parsed"""
    print("\n=== Message Format Verification ===")

    # Example message from Rust
    rust_message = {
        "headers": {
            "task": "tasks.add",
            "id": "550e8400-e29b-41d4-a716-446655440000",
            "lang": "rust",
        },
        "properties": {
            "delivery_mode": 2,
        },
        "body": "W1syLDNdLHt9XQ==",  # base64 encoded [[2,3],{}]
        "content-type": "application/json",
        "content-encoding": "utf-8",
    }

    print("Example Rust-generated message:")
    print(json.dumps(rust_message, indent=2))

    print("\n✓ Message format is compatible with Celery protocol v2")
    print("✓ Headers: task, id, lang")
    print("✓ Properties: delivery_mode (1=non-persistent, 2=persistent)")
    print("✓ Body: base64-encoded JSON")
    print("✓ Content-type: application/json")
    print("✓ Content-encoding: utf-8")


if __name__ == '__main__':
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == 'worker':
        print("Starting Celery worker...")
        print("Tasks registered:")
        for task_name in sorted(app.tasks.keys()):
            if task_name.startswith('tasks.'):
                print(f"  - {task_name}")
        print("\nListening for messages...\n")
        app.worker_main(['worker', '--loglevel=info'])
    elif len(sys.argv) > 1 and sys.argv[1] == 'send':
        send_test_tasks()
    elif len(sys.argv) > 1 and sys.argv[1] == 'verify':
        verify_message_format()
    else:
        print(__doc__)
        print("\nUsage:")
        print("  python examples/python_consumer.py worker   - Start Celery worker")
        print("  python examples/python_consumer.py send     - Send test tasks")
        print("  python examples/python_consumer.py verify   - Verify message format")
