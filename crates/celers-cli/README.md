# celers-cli

Command-line interface for managing CeleRS workers and queues.

## Overview

**Status: Basic skeleton implemented**

The CLI tool provides commands for:
- Starting and managing workers
- Inspecting queue status
- Monitoring task execution
- Administrative operations

## Current Commands

### Worker Management

```bash
# Start a worker
celers worker --broker redis://localhost:6379 --queue my_queue --concurrency 4

# Start with custom configuration
celers worker \
  --broker redis://localhost:6379 \
  --queue my_queue \
  --concurrency 8 \
  --poll-interval 500
```

### Queue Inspection

```bash
# Check queue status
celers status --broker redis://localhost:6379 --queue my_queue
```

## Planned Commands

### Task Management

```bash
# List tasks in queue
celers tasks list --broker redis://localhost --queue my_queue

# View task details
celers tasks get <task-id>

# Cancel a task
celers tasks cancel <task-id>

# Retry a failed task
celers tasks retry <task-id>

# Purge queue
celers tasks purge --queue my_queue
```

### Monitoring

```bash
# Show worker statistics
celers stats --broker redis://localhost

# Tail task logs
celers logs --follow --queue my_queue

# Show queue metrics
celers metrics --queue my_queue
```

### Dead Letter Queue

```bash
# List failed tasks
celers dlq list

# Retry all failed tasks
celers dlq retry-all

# Move failed task to queue
celers dlq requeue <task-id>
```

## Configuration File

Support for configuration files (planned):

```toml
# celers.toml
[broker]
type = "redis"
url = "redis://localhost:6379"

[worker]
concurrency = 4
poll_interval_ms = 1000
graceful_shutdown = true

[queues]
default = { max_retries = 3, timeout_secs = 60 }
high_priority = { max_retries = 5, timeout_secs = 120 }
```

## Usage

```bash
# Install
cargo install celers-cli

# Run with config file
celers --config celers.toml worker

# Get help
celers --help
celers worker --help
```

## See Also

- `celers-worker`: Worker runtime
- `celers-core`: Core types and traits
