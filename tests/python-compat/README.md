# Python-Rust Compatibility Tests

This directory contains tests that verify CeleRS can interoperate with Python Celery workers.

## Setup

```bash
# Create Python virtual environment
python3 -m venv venv
source venv/bin/activate

# Install Celery
pip install celery[redis] pytest

# Install Rust worker
cargo build --release
```

## Test Scenarios

1. **Python→Rust**: Python Celery enqueues task, Rust worker executes
2. **Rust→Python**: Rust enqueues task, Python Celery worker executes
3. **Mixed Deployment**: Both workers consuming from same queue
4. **Chord Protocol**: Verify chord completion across workers
5. **Result Backend**: Shared result storage compatibility

## Running Tests

```bash
# Start Redis
docker run -d -p 6379:6379 redis:latest

# Run Python compatibility tests
pytest test_python_rust_compat.py -v

# Clean up
docker stop $(docker ps -q --filter ancestor=redis)
```

## Files

- `test_python_rust_compat.py` - Python test suite
- `tasks.py` - Sample Python Celery tasks
- `celeryconfig.py` - Celery configuration (JSON serialization enforced)
- `test_rust_python.rs` - Rust integration test

## Important Notes

- **Serialization**: Only JSON is supported for interoperability
- **Protocol**: Tests verify Protocol v2 compatibility
- **Message Format**: Headers, properties, body must match exactly
