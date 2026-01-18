# celers-cli

Command-line interface for managing CeleRS workers, queues, and task execution.

## Overview

**Status: ✅ FEATURE COMPLETE**

A comprehensive CLI tool providing full-featured management for CeleRS distributed task queues. Supports multiple brokers (Redis, PostgreSQL), advanced worker management, monitoring, debugging, and operational workflows.

## Installation

```bash
# Install from source
cargo install --path crates/celers-cli

# The binary is named 'celers'
celers --help
```

## Quick Start

```bash
# Initialize configuration
celers init

# Start a worker
celers worker --broker redis://localhost:6379 --queue my_queue --concurrency 4

# Check queue status
celers status --broker redis://localhost:6379 --queue my_queue

# Run health diagnostics
celers health --broker redis://localhost:6379

# Launch interactive dashboard
celers dashboard --broker redis://localhost:6379 --queue my_queue
```

## Core Features

### Worker Management

```bash
# Start worker with configuration
celers worker --broker redis://localhost:6379 --queue my_queue --concurrency 8

# List all running workers
celers worker-mgmt list --broker redis://localhost:6379

# Show worker statistics
celers worker-mgmt stats <worker-id> --broker redis://localhost:6379

# Stop a worker (graceful)
celers worker-mgmt stop <worker-id> --graceful --broker redis://localhost:6379

# Pause/resume worker
celers worker-mgmt pause <worker-id> --broker redis://localhost:6379
celers worker-mgmt resume <worker-id> --broker redis://localhost:6379

# Drain worker (finish current tasks, accept no new ones)
celers worker-mgmt drain <worker-id> --broker redis://localhost:6379

# Stream worker logs
celers worker-mgmt logs <worker-id> --follow --level info --broker redis://localhost:6379

# Scale workers
celers worker-mgmt scale 5 --broker redis://localhost:6379
```

### Queue Management

```bash
# List all queues (Redis)
celers queue list --broker redis://localhost:6379

# Show detailed queue statistics
celers queue stats --queue my_queue --broker redis://localhost:6379

# Pause/resume queue processing
celers queue pause --queue my_queue --broker redis://localhost:6379
celers queue resume --queue my_queue --broker redis://localhost:6379

# Purge all tasks from queue
celers queue purge --queue my_queue --confirm --broker redis://localhost:6379

# Move tasks between queues
celers queue move --from old_queue --to new_queue --confirm --broker redis://localhost:6379

# Export/import queues
celers queue export --queue my_queue --output backup.json --broker redis://localhost:6379
celers queue import --queue my_queue --input backup.json --confirm --broker redis://localhost:6379
```

### Task Operations

```bash
# Inspect task details
celers task inspect <task-id> --broker redis://localhost:6379 --queue my_queue

# Cancel a running task
celers task cancel <task-id> --broker redis://localhost:6379 --queue my_queue

# Retry a failed task
celers task retry <task-id> --broker redis://localhost:6379 --queue my_queue

# Show task result
celers task result <task-id> --backend redis://localhost:6379

# Show task execution logs
celers task logs <task-id> --broker redis://localhost:6379 --limit 100

# Move task to different queue
celers task requeue <task-id> --from queue1 --to queue2 --broker redis://localhost:6379
```

### Dead Letter Queue (DLQ)

```bash
# Inspect failed tasks
celers dlq inspect --broker redis://localhost:6379 --queue my_queue --limit 20

# Clear all DLQ tasks
celers dlq clear --confirm --broker redis://localhost:6379 --queue my_queue

# Replay specific task from DLQ
celers dlq replay <task-id> --broker redis://localhost:6379 --queue my_queue
```

### Scheduling & Beat

```bash
# List scheduled tasks
celers schedule list --broker redis://localhost:6379

# Add new scheduled task
celers schedule add my_daily_task \
  --task process_data \
  --cron "0 0 * * *" \
  --queue my_queue \
  --args '{"param": "value"}' \
  --broker redis://localhost:6379

# Pause/resume schedule
celers schedule pause my_daily_task --broker redis://localhost:6379
celers schedule resume my_daily_task --broker redis://localhost:6379

# Manually trigger scheduled task
celers schedule trigger my_daily_task --broker redis://localhost:6379

# Show execution history
celers schedule history my_daily_task --limit 50 --broker redis://localhost:6379

# Remove schedule
celers schedule remove my_daily_task --confirm --broker redis://localhost:6379
```

### Monitoring & Metrics

```bash
# Display live metrics
celers metrics --format text

# Export metrics to file
celers metrics --format json --output metrics.json
celers metrics --format prometheus --output metrics.prom

# Filter metrics by pattern
celers metrics --pattern "task_*" --format text

# Watch mode (auto-refresh)
celers metrics --watch 5  # Refresh every 5 seconds

# Interactive dashboard (TUI)
celers dashboard --broker redis://localhost:6379 --queue my_queue --refresh 1
```

### Auto-scaling

```bash
# Start auto-scaling service
celers autoscale start --broker redis://localhost:6379 --queue my_queue

# Check auto-scaling status
celers autoscale status --broker redis://localhost:6379
```

### Alerting

```bash
# Start alert monitoring
celers alert start --broker redis://localhost:6379 --queue my_queue

# Test webhook notification
celers alert test --webhook-url https://hooks.example.com/alerts --message "Test alert"
```

### Debugging & Diagnostics

```bash
# Debug task execution
celers debug task <task-id> --broker redis://localhost:6379 --queue my_queue

# Debug worker issues
celers debug worker <worker-id> --broker redis://localhost:6379

# System health check
celers health --broker redis://localhost:6379 --queue my_queue

# Automatic problem detection
celers doctor --broker redis://localhost:6379 --queue my_queue
```

### Reporting & Analytics

```bash
# Daily execution report
celers report daily --broker redis://localhost:6379 --queue my_queue

# Weekly statistics
celers report weekly --broker redis://localhost:6379 --queue my_queue

# Analyze performance bottlenecks
celers analyze bottlenecks --broker redis://localhost:6379 --queue my_queue

# Analyze failure patterns
celers analyze failures --broker redis://localhost:6379 --queue my_queue
```

### Database Operations

```bash
# Test database connection
celers db test-connection --url postgresql://user:pass@localhost/celers

# Run latency benchmark
celers db test-connection --url postgresql://user:pass@localhost/celers --benchmark

# Check database health
celers db health --url postgresql://user:pass@localhost/celers

# Show connection pool statistics
celers db pool-stats --url postgresql://user:pass@localhost/celers

# Apply migrations
celers db migrate --url postgresql://user:pass@localhost/celers --action apply

# Check migration status
celers db migrate --url postgresql://user:pass@localhost/celers --action status
```

## Configuration

### Generate Default Configuration

```bash
celers init --output celers.toml
```

### Configuration File Format

```toml
# celers.toml

[broker]
type = "redis"  # or "postgres"
url = "redis://localhost:6379"
queue = "celers"
mode = "fifo"  # or "priority"

# Broker failover (optional)
failover_urls = [
    "redis://backup1:6379",
    "redis://backup2:6379"
]
failover_retry_attempts = 3
failover_timeout_secs = 5

[worker]
concurrency = 4
poll_interval_ms = 1000
max_retries = 3
default_timeout_secs = 300

# Multiple queues
queues = ["celers", "high_priority", "low_priority"]

[autoscale]
enabled = false
min_workers = 1
max_workers = 10
scale_up_threshold = 80
scale_down_threshold = 20
check_interval_secs = 60

[alerts]
enabled = false
webhook_url = "https://hooks.example.com/alerts"
check_interval_secs = 60
dlq_threshold = 100
failed_tasks_threshold = 50
```

### Environment Variables

Configuration supports environment variable expansion:

```toml
[broker]
url = "${REDIS_URL:redis://localhost:6379}"  # Uses $REDIS_URL or default
queue = "${QUEUE_NAME}"  # Uses $QUEUE_NAME
```

### Profile Support

Use different configurations for different environments:

```bash
# Create profile-specific configs
celers init --output celers-dev.toml
celers init --output celers-prod.toml

# Use specific profile
celers worker --config celers-prod.toml
```

### Configuration Validation

```bash
# Validate configuration file
celers validate --config celers.toml

# Validate and test broker connection
celers validate --config celers.toml --test-connection
```

## Shell Completion

Generate completion scripts for your shell:

```bash
# Bash
celers completions bash > /etc/bash_completion.d/celers

# Zsh
celers completions zsh > /usr/share/zsh/site-functions/_celers

# Fish
celers completions fish > ~/.config/fish/completions/celers.fish

# PowerShell
celers completions powershell > celers.ps1

# Elvish
celers completions elvish > celers.elv
```

## Man Pages

Generate and install man pages:

```bash
# Generate man pages
celers manpages --output ./man

# Install system-wide
sudo cp ./man/celers.1 /usr/share/man/man1/
sudo mandb

# View man page
man celers
```

## Broker Support

### Redis

Full support for all features:
- FIFO and Priority queues
- Real-time monitoring
- Pub/Sub for commands
- DLQ management
- Task scheduling

```bash
celers worker --broker redis://localhost:6379 --queue my_queue
```

### PostgreSQL

Full support for all features:
- FIFO and Priority queues
- Transaction support
- Schema migrations
- Connection pooling

```bash
celers worker --broker postgresql://user:pass@localhost/celers --queue my_queue
```

## Advanced Usage

### Multi-Queue Management

```bash
# Configure multiple queues in config file
[worker]
queues = ["high", "medium", "low"]

# Start worker processing all queues
celers worker --config celers.toml
```

### Production Workflows

```bash
# 1. Health check before deployment
celers doctor --broker redis://prod:6379 --queue production

# 2. Drain workers for maintenance
celers worker-mgmt drain <worker-id> --broker redis://prod:6379

# 3. Export queue for backup
celers queue export --queue production --output backup-$(date +%Y%m%d).json

# 4. Monitor with auto-refresh
celers metrics --watch 10 --format text

# 5. Set up alerting
celers alert start --broker redis://prod:6379 --queue production
```

### Automation & Scripting

The CLI returns appropriate exit codes for scripting:

```bash
#!/bin/bash
# Check if queue is healthy
if celers health --broker redis://localhost:6379 --queue my_queue; then
    echo "Queue is healthy"
else
    echo "Queue has issues"
    celers doctor --broker redis://localhost:6379 --queue my_queue
fi
```

## Command Reference

| Category | Command | Description |
|----------|---------|-------------|
| **Worker** | `worker` | Start worker process |
| | `worker-mgmt list` | List all workers |
| | `worker-mgmt stats` | Worker statistics |
| | `worker-mgmt stop` | Stop worker |
| | `worker-mgmt pause/resume` | Pause/resume worker |
| | `worker-mgmt drain` | Drain worker |
| | `worker-mgmt scale` | Scale workers |
| | `worker-mgmt logs` | Stream worker logs |
| **Queue** | `queue list` | List queues |
| | `queue stats` | Queue statistics |
| | `queue purge` | Clear queue |
| | `queue move` | Move tasks |
| | `queue export/import` | Backup/restore |
| | `queue pause/resume` | Pause/resume queue |
| **Task** | `task inspect` | Task details |
| | `task cancel` | Cancel task |
| | `task retry` | Retry task |
| | `task result` | Show result |
| | `task logs` | Task logs |
| | `task requeue` | Move task |
| **DLQ** | `dlq inspect` | View failed tasks |
| | `dlq clear` | Clear DLQ |
| | `dlq replay` | Retry task |
| **Schedule** | `schedule list` | List schedules |
| | `schedule add` | Add schedule |
| | `schedule remove` | Remove schedule |
| | `schedule pause/resume` | Pause/resume |
| | `schedule trigger` | Manual trigger |
| | `schedule history` | Execution history |
| **Monitoring** | `metrics` | Show metrics |
| | `dashboard` | Interactive TUI |
| | `autoscale start/status` | Auto-scaling |
| | `alert start/test` | Alerting |
| **Diagnostics** | `health` | Health check |
| | `doctor` | Problem detection |
| | `debug task/worker` | Debug tools |
| **Reporting** | `report daily/weekly` | Reports |
| | `analyze bottlenecks` | Performance |
| | `analyze failures` | Failure patterns |
| **Database** | `db test-connection` | Test connection |
| | `db health` | Database health |
| | `db pool-stats` | Pool statistics |
| | `db migrate` | Run migrations |
| **Config** | `init` | Generate config |
| | `validate` | Validate config |
| | `completions` | Shell completion |
| | `manpages` | Generate man pages |
| **Other** | `status` | Queue status |

## Colored Output

The CLI uses colored output for better readability:
- 🟢 Green: Success messages
- 🟡 Yellow: Warnings
- 🔴 Red: Errors
- 🔵 Cyan: Information

Colors are automatically disabled when output is piped or in CI environments.

## Exit Codes

- `0`: Success
- `1`: General error
- `2`: Configuration error
- `3`: Connection error

## Contributing

See the main CeleRS repository for contribution guidelines.

## See Also

- `celers-core`: Core types and traits
- `celers-worker`: Worker runtime
- `celers-broker-redis`: Redis broker implementation
- `celers-broker-postgres`: PostgreSQL broker implementation
- `celers-metrics`: Metrics collection and export

## License

See LICENSE file in the repository root.
