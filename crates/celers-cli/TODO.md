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
  - [x] Real-time metrics refresh with auto-update ✅
    - [x] Watch mode with configurable refresh interval
    - [x] Clear screen for better readability
    - [x] Display last updated timestamp
- [x] `task cancel <id>` - Cancel running task ✅
- [x] `task retry <id>` - Retry failed task ✅
- [x] `task result <id>` - Show task result ✅
- [x] `task logs <id>` - Show task execution logs ✅
  - [x] Display structured JSON logs with color coding
  - [x] Support for log levels (ERROR, WARN, INFO, DEBUG)
  - [x] Limit number of log lines shown
  - [x] Timestamp display
- [x] `task requeue <id>` - Move task to different queue ✅
- [x] `db` - Database operations ✅
  - [x] `db test-connection` - Test database connection ✅
  - [x] `db health` - Check database health ✅
  - [x] `db pool-stats` - Show connection pool statistics ✅

### Worker Management ✅
- [x] `worker-mgmt list` - Show all running workers ✅
  - [x] Show worker status (active)
  - [x] Display last heartbeat
- [x] `worker-mgmt stop <id>` - Stop specific worker ✅
  - [x] Graceful shutdown option
  - [x] Immediate shutdown option
  - [x] Pub/Sub command delivery
- [x] `worker-mgmt scale <n>` - Scale to N workers ✅
  - [x] Display current vs target worker count
  - [x] Instructions for scaling up/down
  - [x] Auto-scaling based on queue depth ✅
    - [x] `autoscale start` - Start auto-scaling service ✅
    - [x] `autoscale status` - Show auto-scaling status ✅
    - [x] Configurable min/max workers ✅
    - [x] Queue depth thresholds ✅
    - [x] Automatic scaling recommendations ✅
- [x] `worker-mgmt logs <id>` - Stream worker logs ✅
  - [x] Filter by log level (error, warn, info, debug)
  - [x] Follow mode (tail -f)
  - [x] Display initial N lines
  - [x] Color-coded log levels
  - [x] JSON log parsing
  - [x] Detect worker shutdown
- [x] `worker-mgmt stats <id>` - Detailed worker statistics ✅
  - [x] Tasks processed/failed
  - [x] Worker uptime
  - [x] Heartbeat status
- [x] `worker-mgmt pause <id>` - Pause task processing ✅
  - [x] Set pause flag in Redis
  - [x] Timestamp tracking
- [x] `worker-mgmt resume <id>` - Resume task processing ✅
  - [x] Remove pause flag
  - [x] Restore normal operation
- [x] `worker-mgmt drain <id>` - Drain worker (no new tasks) ✅
  - [x] Set draining flag in Redis
  - [x] Timestamp tracking

### Queue Management Enhancements
- [x] `queue pause <name>` - Pause queue processing ✅
- [x] `queue resume <name>` - Resume queue processing ✅
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

### Scheduling & Beat ✅
- [x] `schedule list` - List scheduled tasks ✅
  - [x] Display task name, cron expression, status (active/paused)
  - [x] Show last run time
  - [x] Formatted table output
- [x] `schedule add` - Add new scheduled task ✅
  - [x] Cron expression validation
  - [x] Task name and queue configuration
  - [x] JSON arguments support
  - [x] Duplicate schedule detection
- [x] `schedule remove <name>` - Remove scheduled task ✅
  - [x] Confirmation requirement
  - [x] Remove pause flags
- [x] `schedule pause <name>` - Pause schedule ✅
  - [x] Timestamp tracking
  - [x] Resume instructions
- [x] `schedule resume <name>` - Resume paused schedule ✅
  - [x] Pause status checking
  - [x] Resume confirmation
- [x] `schedule trigger <name>` - Manually trigger task ✅
  - [x] Pub/Sub trigger command
  - [x] Beat scheduler integration
  - [x] Subscriber status reporting
- [x] `schedule history <name>` - Show execution history ✅
  - [x] Display execution history from Redis
  - [x] Show timestamp, status, and task ID
  - [x] Limit number of entries shown

### Configuration
- [x] Multiple broker support in single config ✅
  - [x] Broker failover configuration ✅
    - [x] Multiple failover URLs support
    - [x] Configurable retry attempts
    - [x] Configurable timeout settings
- [x] Environment variable expansion ✅
  - [x] ${VAR} syntax support
  - [x] ${VAR:default} default values support
- [x] Config validation command ✅
  - [x] Schema validation (TOML parsing)
  - [x] Broker type validation
  - [x] Queue mode validation
  - [x] Worker configuration validation with warnings
  - [x] Redis connection testing
  - [x] Auto-scaling configuration validation ✅
  - [x] Alert configuration validation ✅
  - [x] PostgreSQL connection testing ✅
  - [x] MySQL/AMQP/SQS connection guidance (manual testing instructions provided)
- [x] Profile support (dev, staging, prod) ✅
  - [x] Profile-specific configuration files ✅
  - [x] Configuration merging/inheritance ✅
  - [x] Environment-specific overrides ✅

### Monitoring Integration
- [x] Export metrics to file ✅
  - [x] JSON, Prometheus, text formats ✅
- [x] Alert configuration ✅
  - [x] `alert start` - Start alert monitoring service ✅
  - [x] `alert test` - Test webhook notification ✅
  - [x] Threshold-based alerts (DLQ, failed tasks) ✅
  - [x] Configurable check intervals ✅
- [x] Webhook notifications ✅
  - [x] Custom webhook URLs ✅
  - [x] JSON payload with timestamp ✅
  - [x] Automatic alert triggering ✅
- [x] Live dashboard mode (TUI) ✅
  - [x] Real-time task flow visualization ✅
  - [x] Queue depth gauge ✅
  - [x] Interactive controls (press 'q' to quit) ✅

### Database Support
- [x] PostgreSQL broker support in CLI ✅
  - [x] Connection string validation ✅
  - [x] Connection testing ✅
  - [x] `db test-connection` - Test database connection ✅
  - [x] `db health` - Check database health ✅
- [x] Database health checks ✅
  - [x] Connection testing ✅
  - [x] PostgreSQL version detection ✅
  - [x] Connection pool status ✅
  - [x] Query performance metrics ✅
- [x] Connection testing ✅
  - [x] Latency measurement ✅
  - [x] Benchmark mode (10 queries) ✅
  - [x] Authentication verification ✅
  - [x] Password masking in output ✅
- [x] Database migration commands ✅
  - [x] Apply migrations ✅
  - [x] Rollback migrations (with manual instructions) ✅
  - [x] Migration status ✅

### Debugging & Troubleshooting ✅
- [x] `debug task <id>` - Debug task execution ✅
  - [x] Display task logs with color-coded levels
  - [x] Show task metadata
  - [x] Inspect task state in queue
- [x] `debug worker <id>` - Debug worker issues ✅
  - [x] Check worker heartbeat and status
  - [x] Display worker statistics
  - [x] Show pause/drain status
  - [x] Display recent worker logs
- [x] `health` - System health diagnostics ✅
  - [x] Broker connection testing
  - [x] Queue status checks
  - [x] DLQ size monitoring
  - [x] Queue pause status detection
  - [x] Memory usage reporting
  - [x] Health recommendations
- [x] `doctor` - Automatic problem detection ✅
  - [x] Broker connectivity checks
  - [x] Queue health analysis
  - [x] Worker availability monitoring
  - [x] Queue pause status detection
  - [x] Memory usage inspection
  - [x] Issue prioritization (critical vs warnings)
  - [x] Actionable recommendations

### Reporting & Analytics ✅
- [x] `report daily` - Daily execution report ✅
  - [x] Display daily task metrics
  - [x] Show total tasks, succeeded, failed, retried
  - [x] Calculate average execution time
- [x] `report weekly` - Weekly statistics ✅
  - [x] Aggregate daily metrics for 7 days
  - [x] Calculate success and failure rates
  - [x] Display percentage breakdowns
- [x] `analyze bottlenecks` - Find performance issues ✅
  - [x] Check queue depth and worker count
  - [x] Detect high DLQ size
  - [x] Identify bottlenecks
  - [x] Provide actionable recommendations
- [x] `analyze failures` - Analyze failure patterns ✅
  - [x] Group failures by task type
  - [x] Display top failing tasks
  - [x] Provide troubleshooting recommendations

## Testing Status

- [x] Unit tests for configuration parsing ✅
  - [x] Default configuration tests
  - [x] Serialization/deserialization tests
  - [x] Validation tests
  - [x] Environment variable expansion tests
  - [x] File I/O tests
  - [x] Broker failover configuration tests ✅
  - [x] Auto-scaling configuration tests ✅
  - [x] Alert configuration tests ✅
  - [x] Profile configuration tests ✅
- [x] Unit tests for command logic ✅
  - [x] Task ID parsing validation
  - [x] Worker ID extraction
  - [x] Log level matching
  - [x] Redis key formatting
  - [x] Queue key formatting
  - [x] JSON log parsing
  - [x] Limit range calculations
  - [x] Diagnostic thresholds
  - [x] Shutdown channel naming
  - [x] Timestamp formatting
  - [x] Password masking in URLs ✅
- [x] Integration tests with real brokers ✅
  - [x] Test framework created in /tmp/celers_integration_tests.rs ✅
  - [x] Redis broker integration tests ✅
  - [x] PostgreSQL broker integration tests ✅
  - [x] Configuration validation tests ✅
- [x] E2E tests for workflows ✅
  - [x] Queue management workflows ✅
  - [x] DLQ handling workflows ✅
  - [x] Worker management workflows ✅
  - [x] Database operations workflows ✅
  - [x] Test framework created in /tmp/celers_e2e_tests.rs ✅

## Documentation

- [x] CLI README
- [x] Command usage examples
- [x] Advanced usage patterns ✅
  - [x] Multi-queue management ✅
  - [x] Production workflows ✅
  - [x] Monitoring and alerting ✅
  - [x] Database operations ✅
  - [x] Configuration management ✅
  - [x] Debugging and troubleshooting ✅
  - [x] Automation and scripting ✅
  - [x] Documentation created at /tmp/ADVANCED_USAGE.md ✅
- [x] Shell completion scripts ✅
  - [x] Bash support
  - [x] Zsh support
  - [x] Fish support
  - [x] PowerShell support
  - [x] Elvish support
- [x] Man pages ✅
  - [x] Man page generation command ✅
  - [x] Installation instructions ✅

## Dependencies

- `celers-core`: Core traits
- `celers-worker`: Worker runtime
- `celers-broker-redis`: Redis broker
- `celers-broker-postgres`: PostgreSQL broker
- `celers-metrics`: Metrics collection and export
- `clap`: CLI argument parsing
- `clap_complete`: Shell completion generation
- `clap_mangen`: Man page generation
- `toml`: Configuration files
- `tabled`: Table formatting
- `colored`: Terminal colors
- `redis`: Direct Redis operations for queue management
- `uuid`: Task ID parsing
- `serde_json`: Task deserialization
- `ratatui`: Terminal UI framework
- `crossterm`: Terminal manipulation
- `reqwest`: HTTP client for webhooks

## Binary Output

### Package
- Binary name: `celers`
- Install: `cargo install --path crates/celers-cli`

### Shell Completion ✅

Generate completion scripts:
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

### Man Pages ✅

Generate and install man pages:
```bash
# Generate man pages
celers manpages -o ./man

# Install man page
sudo cp ./man/celers.1 /usr/share/man/man1/
sudo mandb

# View man page
man celers
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
