# celers-cli

**Version: 0.3.1 | Status: [Alpha] | Updated: 2026-07-13**

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

# Launch a live auto-refreshing dashboard
celers dashboard --broker redis://localhost:6379 --queue my_queue

# Launch the interactive REPL (multi-command session, tab completion, history)
celers interactive --broker redis://localhost:6379 --queue my_queue
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

# Live auto-refreshing dashboard (clears/redraws on an interval; Ctrl+C to exit)
celers dashboard --broker redis://localhost:6379 --queue my_queue --refresh 1

# Live (top-like) monitor of a Prometheus metrics endpoint
celers monitor --endpoint http://localhost:9090/metrics --interval 5 --focus tasks_total,queue_depth
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

# Export a report as CSV, or self-contained HTML with an inline SVG bar chart
celers report daily --format csv --output daily.csv --broker redis://localhost:6379 --queue my_queue
celers report weekly --format html --output weekly.html --broker redis://localhost:6379 --queue my_queue

# Rolling task-execution history, per-worker stats, and per-queue health exports
celers report history --days 14 --broker redis://localhost:6379 --queue my_queue
celers report workers --broker redis://localhost:6379
celers report queues --broker redis://localhost:6379

# Performance profiling: task duration trend, worker comparison, resource usage
celers analyze profile task --days 7 --broker redis://localhost:6379 --queue my_queue
celers analyze profile worker --broker redis://localhost:6379
celers analyze profile resources --days 7 --broker redis://localhost:6379 --queue my_queue
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

### Interactive Mode

```bash
# Launch the REPL: command history, tab completion, session state
celers interactive --broker redis://localhost:6379 --queue my_queue
```

Recognized REPL commands (`help`/`?` prints this list live):

| Command | Description |
|---------|-------------|
| `status`, `st` | Show queue status |
| `queues`, `ls` | List all queues |
| `workers`, `w` | List all workers |
| `health`, `h` | Run health diagnostics |
| `doctor`, `d` | Automatic problem detection |
| `metrics`, `m` | Display metrics |
| `stats`, `cs` | Connection pool & cache statistics (live hit/reuse ratios) |
| `dlq inspect [limit]` | Inspect DLQ tasks |
| `dlq clear` | Clear all DLQ tasks |
| `use <queue>` | Switch to a different queue (offers "did you mean" on a typo) |
| `broker [url]` | Show/set broker URL |
| `clear`, `cls` | Clear screen |
| `exit`, `quit`, `q` | Exit interactive mode |

An unrecognized command (or an unknown queue passed to `use`) gets a Levenshtein-distance "did you mean" suggestion instead of a bare error.

### Load Testing

```bash
# Generate synthetic load: 1000 tasks at 50/sec, constant arrival
celers loadtest --total 1000 --rate 50 --broker redis://localhost:6379 --queue my_queue

# Jittered or Poisson-like arrival, deterministic given the same --seed
celers loadtest --pattern jittered --seed 42 --rate 20 --duration 30 --broker redis://localhost:6379 --queue my_queue

# Preview the generated schedule without enqueuing anything
celers loadtest --total 500 --rate 100 --dry-run

# `simulate` is a visible alias for `loadtest`
celers simulate --total 100 --rate 10 --broker redis://localhost:6379 --queue my_queue
```

### Backup & Restore

```bash
# Full backup of broker state (queues, DLQ, schedules) to a compressed archive
celers backup --broker redis://localhost:6379 --output celers-backup.tar.gz

# Incremental backup: only entries changed since a prior archive, or an explicit timestamp
celers backup --broker redis://localhost:6379 --output incr.tar.gz --previous celers-backup.tar.gz
celers backup --broker redis://localhost:6379 --output incr.tar.gz --since 2026-07-01T00:00:00Z

# Restore, choosing how to resolve a queue that already has live content at the target
celers restore --broker redis://localhost:6379 --input celers-backup.tar.gz --conflict-policy skip
celers restore --broker redis://localhost:6379 --input celers-backup.tar.gz --conflict-policy overwrite
celers restore --broker redis://localhost:6379 --input celers-backup.tar.gz --conflict-policy merge

# Dry run and selective (per-queue) restore
celers restore --broker redis://localhost:6379 --input celers-backup.tar.gz --dry-run
celers restore --broker redis://localhost:6379 --input celers-backup.tar.gz --queues high_priority,default
```

`--conflict-policy` defaults to `skip` (the safest option: never silently overwrites live data).

### Task Dependency Visualization

```bash
# Render a task dependency graph from a queue-export JSON file (see `celers queue export`)
celers deps --from backup.json --format ascii
celers deps --from backup.json --format dot > graph.dot

# Interactively explore the graph starting from a specific task
celers deps --from backup.json --interactive --start <task-id>
```

### Command Aliases

```bash
# Define a user alias; expansion happens before argument parsing
celers alias add w "worker --concurrency 8"
celers w --broker redis://localhost:6379 --queue my_queue   # expands to `worker --concurrency 8 ...`

celers alias list
celers alias remove w
```

An alias can never shadow a real command name. Built-in short aliases are also available on many top-level commands, e.g. `celers w` (`worker`), `celers q` (`queue`), `celers t` (`task`), `celers wm` (`worker-mgmt`), `celers i` (`interactive`), `celers dash` (`dashboard`), `celers a` (`alias`).

### Error Codes & Troubleshooting

Every command failure prints a structured `error[E_CODE]: <message>` line plus an actionable `suggestion:` line. Print the full code/message/suggestion reference table:

```bash
celers error-codes
```

### Structured Logging

```bash
# JSON logs (one object per event) instead of the default human-readable text
celers worker --broker redis://localhost:6379 --queue my_queue --log-format json

# Send logs to a file or a TCP collector instead of stdout
celers worker --broker redis://localhost:6379 --queue my_queue --log-sink file:/var/log/celers.log
celers worker --broker redis://localhost:6379 --queue my_queue --log-sink tcp:127.0.0.1:9000

# --log-level still takes precedence over RUST_LOG
celers worker --broker redis://localhost:6379 --queue my_queue --log-level debug
```

`--log-format`/`--log-sink`/`--log-level` are global flags, valid on every subcommand.

### Smart Defaults

If `--broker` and the config file's `broker.url` are both unset, the CLI falls back to environment variables, checked in order:

```bash
export REDIS_URL=redis://localhost:6379
celers status --queue my_queue   # broker URL auto-detected from REDIS_URL
```

Fallback order: `REDIS_URL` -> `CELERY_BROKER_URL` -> `AMQP_URL`.

### Performance: Connection Pool & Cache

```bash
# Configured pool capacity, per-cache TTL, and current in-process entry counts
celers cache-stats

# Live hit/reuse ratios (meaningful only in a long-running session)
celers interactive
# then, inside the REPL:
> stats
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

# User-defined command aliases (also managed via `celers alias add|remove|list`)
[aliases]
w = "worker --concurrency 8"
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
| **Worker** | `worker` (`w`) | Start worker process |
| | `worker-mgmt` (`wm`) `list` | List all workers |
| | `worker-mgmt stats` | Worker statistics |
| | `worker-mgmt stop` | Stop worker |
| | `worker-mgmt pause/resume` | Pause/resume worker |
| | `worker-mgmt drain` | Drain worker |
| | `worker-mgmt scale` | Scale workers |
| | `worker-mgmt logs` | Stream worker logs |
| **Queue** | `queue` (`q`) `list` | List queues |
| | `queue stats` | Queue statistics |
| | `queue purge` | Clear queue |
| | `queue move` | Move tasks between queues |
| | `queue export/import` | Backup/restore a queue to/from JSON |
| | `queue pause/resume` | Pause/resume queue |
| **Task** | `task` (`t`) `inspect` | Task details |
| | `task cancel` | Cancel task |
| | `task retry` | Retry task |
| | `task result` | Show result |
| | `task logs` | Task logs |
| | `task requeue` | Move task to a different queue |
| **DLQ / Replay** | `dlq inspect` | View failed tasks |
| | `dlq clear` | Clear DLQ |
| | `dlq replay <id>` | Retry one DLQ task |
| | `replay` | Replay by `--id`/`--pattern`/`--all`, with `--limit`/`--dry-run` |
| **Load Testing** | `loadtest` (`simulate`) | Generate synthetic task load (constant/jittered/poisson) |
| **Schedule** | `schedule list` | List schedules |
| | `schedule add` | Add schedule |
| | `schedule remove` | Remove schedule |
| | `schedule pause/resume` | Pause/resume |
| | `schedule trigger` | Manual trigger |
| | `schedule history` | Execution history |
| **Monitoring** | `metrics` | Show metrics (text/json/prometheus, or scrape `--endpoint`) |
| | `monitor` | Live (top-like) view of a Prometheus endpoint |
| | `dashboard` (`dash`) | Live auto-refreshing terminal dashboard |
| | `interactive` (`i`) | Interactive REPL session |
| | `autoscale start/status` | Auto-scaling |
| | `alert start/test` | Alerting |
| **Diagnostics** | `health` | Health check |
| | `doctor` | Problem detection |
| | `debug task/worker` | Debug tools |
| **Reporting** | `report daily/weekly` | Reports (`--format table\|csv\|html`, `--output`, `--template`) |
| | `report history` | Task execution history over a rolling window |
| | `report workers` | Per-worker statistics export |
| | `report queues` | Per-queue depth/health export |
| | `analyze bottlenecks` | Performance bottleneck detection |
| | `analyze failures` | Failure pattern analysis |
| | `analyze profile task/worker/resources` | Duration trend, worker comparison, resource usage |
| **Backup** | `backup` | Full or incremental (`--previous`/`--since`) broker-state backup |
| | `restore` | Restore, with `--conflict-policy skip\|overwrite\|merge` |
| **Dependencies** | `deps` | ASCII/DOT task dependency graph, optional `--interactive` |
| **Database** | `db test-connection` | Test connection (`--benchmark` for latency) |
| | `db health` | Database health |
| | `db pool-stats` | Connection pool statistics |
| | `db migrate` | Run/inspect migrations |
| **Aliases** | `alias` (`a`) `list/add/remove` | User-defined command aliases |
| **Errors / Perf** | `error-codes` | Print the structured error-code reference table |
| | `cache-stats` | Configured pool/cache capacity & TTL snapshot |
| **Config** | `init` (`--wizard` for interactive setup) | Generate config |
| | `config show/reload` | Print/reload the resolved configuration |
| | `validate` | Validate config |
| | `completions` | Shell completion |
| | `manpages` | Generate man pages |
| **Other** | `status` | Queue status |

Global flags (valid on every subcommand): `--log-level <LEVEL>`, `--log-format text\|json`, `--log-sink stdout\|file:<path>\|tcp:<host:port>`.

## Colored Output

The CLI uses colored output for better readability:
- 🟢 Green: Success messages
- 🟡 Yellow: Warnings
- 🔴 Red: Errors
- 🔵 Cyan: Information

Colors are automatically disabled when output is piped or in CI environments.

## Exit Codes

- `0`: Success (including `--help`/`--version`)
- `1`: Command failure — broker/connection/config/task errors. The `error[E_CODE]: <message>` line printed to stderr disambiguates the cause; run `celers error-codes` for the full code/message/suggestion reference table.
- `2`: CLI usage error (invalid arguments/flags), via `clap`'s own exit path

## Contributing

See the main CeleRS repository for contribution guidelines.

## See Also

- `celers-core`: Core types and traits
- `celers-worker`: Worker runtime
- `celers-broker-redis`: Redis broker implementation
- `celers-broker-postgres`: PostgreSQL broker implementation
- `celers-beat`: Beat scheduler integration (cron-based scheduling, backed up by `backup`)
- `celers-metrics`: Metrics collection and export

## Testing

**757 tests passing** (unit tests across command parsing, configuration layering, backup/restore, connection pooling, TTL caching, structured logging, smart defaults, alias expansion, error classification, task-dependency graphs, and the interactive REPL)

## License

See LICENSE file in the repository root.
