# celers-cli TODO

> Command-line interface for CeleRS task queue management

## Status: ✅ FEATURE COMPLETE

Full-featured CLI for worker management, queue inspection, and DLQ operations.

## Completed Features

### Commands ✅

#### Worker Command
- [x] Start worker with configuration
- [x] Redis broker support
- [x] Queue mode selection (FIFO/Priority)
- [x] Concurrency configuration
- [x] Max retries configuration
- [x] Timeout configuration
- [x] Graceful shutdown handling

#### Status Command
- [x] Display queue statistics
- [x] Show pending task count
- [x] Show DLQ size
- [x] Formatted table output
- [x] Colored warnings for DLQ

#### DLQ Commands
- [x] `dlq inspect` - View failed tasks
- [x] `dlq clear` - Remove all DLQ tasks
- [x] `dlq replay` - Retry specific task
- [x] Confirmation prompts for destructive operations
- [x] Task metadata display

#### Init Command
- [x] Generate default configuration file
- [x] TOML format support
- [x] Commented configuration template

#### Queue Commands
- [x] `queue list` - List all queues (Redis)
- [x] `queue purge` - Clear all tasks from queue
- [x] Confirmation prompts for destructive operations

#### Task Commands
- [x] `task inspect <id>` - View specific task details
- [x] `task cancel <id>` - Cancel running or pending task
- [x] Search across main queue, DLQ, and delayed queue
- [x] Detailed task metadata display
- [x] Cancellation via Redis Pub/Sub

### Configuration ✅
- [x] TOML file support
- [x] Command-line argument override
- [x] Broker configuration (type, URL, queue)
- [x] Worker configuration (concurrency, retries, timeout)
- [x] Multiple queue support

### User Experience ✅
- [x] Colored output (green, yellow, red, cyan)
- [x] Formatted tables using `tabled`
- [x] Clear error messages
- [x] Help text for all commands
- [x] Usage examples in help

### Documentation ✅
- [x] Comprehensive README
- [x] Command examples
- [x] Configuration guide
- [x] Common workflows

### Additional Commands
- [x] `metrics` - Display live metrics ✅
  - [x] Metric filtering by name pattern
  - [x] Export to JSON/Prometheus/text formats
  - [x] Save metrics to file
  - [ ] Real-time metrics refresh with auto-update
- [x] `task cancel <id>` - Cancel running task ✅
- [x] `task retry <id>` - Retry failed task ✅
- [x] `task result <id>` - Show task result ✅
- [ ] `task logs <id>` - Show task execution logs
- [x] `task requeue <id>` - Move task to different queue ✅

### Worker Management
- [ ] `worker list` - Show all running workers
  - [ ] Show worker status (active, idle, draining)
  - [ ] Display task counts and resources
- [ ] `worker stop <id>` - Stop specific worker
  - [ ] Graceful shutdown option
  - [ ] Drain before stop
- [ ] `worker scale <n>` - Scale to N workers
  - [ ] Auto-scaling based on queue depth
- [ ] `worker logs` - Stream worker logs
  - [ ] Filter by log level
  - [ ] Follow mode (tail -f)
- [ ] `worker stats <id>` - Detailed worker statistics
- [ ] `worker pause <id>` - Pause task processing
- [ ] `worker resume <id>` - Resume task processing
- [ ] `worker drain <id>` - Drain worker (no new tasks)

### Queue Management Enhancements
- [ ] `queue pause <name>` - Pause queue processing
- [ ] `queue resume <name>` - Resume queue processing
- [x] `queue stats <name>` - Detailed queue statistics ✅
  - [x] Queue type detection (FIFO/Priority)
  - [x] Pending, processing, DLQ, and delayed task counts
  - [x] Total task count
  - [x] Task type distribution (top 10)
  - [x] Health warnings (DLQ size, stuck workers)
- [x] `queue move <src> <dst>` - Move tasks between queues ✅
  - [x] Bulk move all tasks from source to destination
  - [x] Support for FIFO and Priority queue types
  - [x] Automatic queue type conversion
  - [x] Progress indicator for large batches
  - [x] Confirmation requirement for safety
- [x] `queue export <name>` - Export queue to file ✅
  - [x] Export tasks to JSON file with metadata
  - [x] Queue type and timestamp information
  - [x] File size reporting
- [x] `queue import <file>` - Import queue from file ✅
  - [x] Import tasks from JSON export file
  - [x] Preview import information before confirmation
  - [x] Progress indicator for large imports
  - [x] Queue type conversion support
  - [x] Confirmation requirement for safety

### Scheduling & Beat
- [ ] `schedule list` - List scheduled tasks
- [ ] `schedule add` - Add new scheduled task
- [ ] `schedule remove <name>` - Remove scheduled task
- [ ] `schedule pause <name>` - Pause schedule
- [ ] `schedule trigger <name>` - Manually trigger task
- [ ] `schedule history <name>` - Show execution history

### Configuration
- [ ] Multiple broker support in single config
  - [ ] Broker failover configuration
- [ ] Environment variable expansion
  - [ ] ${VAR} syntax support
  - [ ] Default values
- [x] Config validation command ✅
  - [x] Schema validation (TOML parsing)
  - [x] Broker type validation
  - [x] Queue mode validation
  - [x] Worker configuration validation with warnings
  - [x] Redis connection testing
  - [ ] PostgreSQL/MySQL/AMQP/SQS connection testing
- [ ] Profile support (dev, staging, prod)
  - [ ] Profile inheritance
  - [ ] Environment-specific overrides

### Monitoring Integration
- [ ] Export metrics to file
  - [ ] JSON, CSV, Prometheus formats
- [ ] Live dashboard mode (TUI)
  - [ ] Real-time task flow visualization
  - [ ] Queue depth graphs
  - [ ] Interactive navigation
- [ ] Alert configuration
  - [ ] Threshold-based alerts
  - [ ] Alert rules engine
- [ ] Webhook notifications
  - [ ] Custom webhook URLs
  - [ ] Payload templating

### Database Support
- [ ] PostgreSQL broker support in CLI
  - [ ] Connection string validation
  - [ ] Query execution
- [ ] Database migration commands
  - [ ] Apply/rollback migrations
  - [ ] Migration status
- [ ] Database health checks
  - [ ] Connection pool status
  - [ ] Query performance
- [ ] Connection testing
  - [ ] Latency measurement
  - [ ] Authentication verification

### Debugging & Troubleshooting
- [ ] `debug task <id>` - Debug task execution
- [ ] `debug worker <id>` - Debug worker issues
- [ ] `health check` - System health diagnostics
- [ ] `doctor` - Automatic problem detection

### Reporting & Analytics
- [ ] `report daily` - Daily execution report
- [ ] `report weekly` - Weekly statistics
- [ ] `analyze bottlenecks` - Find performance issues
- [ ] `analyze failures` - Analyze failure patterns

## Testing Status

- [ ] Unit tests for command logic
- [ ] Integration tests with real brokers
- [ ] E2E tests for workflows
- [ ] Configuration parsing tests

## Documentation

- [x] CLI README
- [x] Command usage examples
- [ ] Advanced usage patterns
- [ ] Shell completion scripts
- [ ] Man pages

## Dependencies

- `celers-core`: Core traits
- `celers-worker`: Worker runtime
- `celers-broker-redis`: Redis broker
- `celers-broker-postgres`: PostgreSQL broker
- `clap`: CLI argument parsing
- `toml`: Configuration files
- `tabled`: Table formatting
- `colored`: Terminal colors
- `redis`: Direct Redis operations for queue management
- `uuid`: Task ID parsing
- `serde_json`: Task deserialization

## Binary Output

### Package
- Binary name: `celers`
- Install: `cargo install --path crates/celers-cli`

### Shell Completion

Generate completion scripts:
```bash
celers completions bash > /etc/bash_completion.d/celers
celers completions zsh > /usr/share/zsh/site-functions/_celers
celers completions fish > ~/.config/fish/completions/celers.fish
```

## Configuration File

Default location: `celers.toml`

```toml
[broker]
type = "redis"
url = "redis://localhost:6379"
queue = "celers"
mode = "fifo"

[worker]
concurrency = 4
poll_interval_ms = 1000
max_retries = 3
default_timeout_secs = 300

queues = ["celers", "high_priority", "low_priority"]
```

## Notes

- CLI is designed for operational tasks, not development
- Requires worker to have registered tasks
- Config file is optional (CLI args work standalone)
- Supports both Redis and PostgreSQL brokers
- Colored output automatically disabled in CI/pipes
