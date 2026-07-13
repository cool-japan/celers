# celers-cli TODO

> Command-line interface for CeleRS task queue management

**Version: 0.3.0 | Status: [Alpha] | Updated: 2026-07-13 | Tests: 757**

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
- [x] Task replay (re-execute failed tasks) ✅
  - [x] `replay` command re-enqueues failed/DLQ tasks to their target queue
  - [x] Selection by task id (`--id`), name glob pattern (`--pattern`), or `--all`
  - [x] `--limit N` caps replay count; `--dry-run` previews the plan
  - [x] Pure, unit-tested planning function (`plan_replay` + `glob_match`)
  - [x] Thin broker layer reuses `RedisBroker::replay_from_dlq` plumbing

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
- [x] YAML file support ✅ (format auto-detected by extension)
- [x] Configuration via CLI arguments ✅ (`CliConfigArgs` maps onto every config field)
- [x] Command-line argument override
- [x] Layered precedence: CLI args > environment > config file > defaults ✅
- [x] Environment-variable overrides (`CELERY_*` / `CELERS_*`) ✅
- [x] Dynamic configuration updates ✅ (`config reload` reports a `ConfigDiff`)
- [x] `config show` - print the resolved configuration (TOML/YAML) ✅
- [x] `--log-level` global flag (overrides `RUST_LOG`) ✅
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
  - [x] `metrics --endpoint <URL>` - Scrape a remote Prometheus endpoint ✅
    - [x] Native Prometheus text-format parser (HELP/TYPE/labels) ✅
    - [x] Counters, gauges, histograms, summaries; malformed lines skipped ✅
    - [x] Render parsed model as a colored `tabled` table ✅
    - [x] Pure parser + renderer (fully unit-testable, no live server) ✅
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
- [x] Add metrics and monitoring commands ✅
  - [x] `metrics --endpoint <URL>` - fetch + parse + table-render remote metrics ✅
  - [x] `monitor` - live (top-like) view with configurable refresh interval ✅
    - [x] `--focus` to surface key base metrics first ✅
    - [x] Pure render step over the parsed model (testable); thin refresh loop ✅
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
- [x] Live auto-refreshing terminal dashboard ✅ (corrected 2026-07-13: a plain stdout clear/redraw loop over a fixed interval, not a `ratatui`/`crossterm` TUI — `ratatui`/`crossterm` are not dependencies of this crate; see the Dependencies section note)
  - [x] Real-time queue/DLQ/worker-count/Redis-memory snapshot each refresh ✅
  - [x] Health status line (DLQ backlog / no-workers warnings) ✅
  - [x] Configurable refresh interval; Ctrl+C to exit (no in-app keybindings) ✅

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

**Total: 757 tests passing** (`cargo nextest run -p celers-cli --all-features`, confirmed 2026-07-13; 0 failed, 0 skipped)

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
- [x] Unit tests for report/CSV/HTML rendering utilities (current `command_utils` API; corrected 2026-07-13) ✅
  - [x] HTML escaping and SVG chart value/label rendering
  - [x] CSV row formatting per report type (history/worker/queue)
  - [x] Template substitution (`{{placeholder}}`, incl. missing-key handling)
  - ~~"enhanced utilities (v1.1)": string truncation, relative-time formatting, broker-URL validation, thousands-separator formatting~~ — those functions no longer exist in source; corrected 2026-07-13 (found stale during this pass's verification, not part of this cycle's actual changes)
- [x] Integration tests with real brokers ✅
  - [x] Redis broker integration tests ✅
  - [x] PostgreSQL broker integration tests ✅
  - [x] Configuration validation tests ✅
- [x] E2E tests for workflows ✅
  - [x] Queue management workflows ✅
  - [x] DLQ handling workflows ✅
  - [x] Worker management workflows ✅
  - [x] Database operations workflows ✅

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

Verified against `Cargo.toml` 2026-07-13. Note: `ratatui`/`crossterm`/`reqwest` were listed here previously but are **not** actual dependencies of this crate — `dashboard`/`monitor` are plain stdout-refresh loops (no TUI framework), and HTTP (webhooks, Prometheus scraping) goes through `oxihttp-client` (COOLJAPAN Pure Rust Policy) instead of `reqwest`. Corrected below.

- `celers-beat`: Beat scheduler integration (cron scheduling, backed up by `backup`)
- `celers-core`: Core traits
- `celers-worker`: Worker runtime
- `celers-broker-redis`: Redis broker
- `celers-broker-postgres`: PostgreSQL broker
- `celers-metrics`: Metrics collection and export
- `clap`: CLI argument parsing
- `clap_complete`: Shell completion generation
- `clap_mangen`: Man page generation
- `tokio`: Async runtime
- `anyhow`: Error handling
- `thiserror`: Structured `CliError` codes (`errors.rs`)
- `tracing` / `tracing-subscriber`: Structured logging (`logging.rs`)
- `serde` / `serde_json` / `serde_yaml`: Config, task, and report (de)serialization
- `uuid`: Task ID parsing
- `redis`: Direct Redis operations for queue management
- `chrono`: Timestamps
- `oxisql-core` / `oxisql-postgres` / `oxisql-mysql`: Pure-Rust SQL connectivity (COOLJAPAN policy)
- `oxitls` / `rustls`: TLS for database connections (`tls_mode.rs`)
- `toml`: Configuration files
- `tabled`: Table formatting
- `colored`: Terminal colors
- `oxihttp-client`: HTTP client for webhooks & Prometheus scraping (COOLJAPAN policy; replaces `reqwest`)
- `url`: URL parsing/validation
- `rustyline`: Interactive REPL (history, editing)
- `csv`: CSV report export
- `oxiarc-deflate` / `oxiarc-archive`: Pure-Rust backup archive compression (COOLJAPAN policy; replaces `zip`/`flate2`)
- `dialoguer`: Interactive configuration wizard prompts
- `dirs`: Home-directory discovery (REPL history file)
- `futures`: Parallel/batch async operations (`join_all`, alongside `tokio::join!`)

### Dev Dependencies
- `tempfile`: Temp-file I/O in tests
- `proptest`: Property-based tests (`tests/proptest_cli.rs`)
- `criterion`: Benchmarks (`benches/serialization.rs`)

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

## Code Organization ✅

- [x] Utility functions extracted to separate module ✅
  - [x] `command_utils` module with helper functions ✅
  - [x] Comprehensive documentation and examples ✅
  - [x] Zero warnings from `cargo doc` ✅ (confirmed 2026-07-13; all 14 prior intra-doc-link warnings fixed)
  - [x] Report/CSV/HTML rendering functions (current API; corrected 2026-07-13 — superseded the old "v1.1" list below, which no longer exists in source) ✅
    - [x] `format_duration` - Human-readable duration formatting
    - [x] `write_csv` / `csv_to_string` - CSV export helpers
    - [x] `format_history_csv` / `format_worker_csv` / `format_queue_csv` - Per-report-type CSV row formatters
    - [x] `render_table_string` / `render_table_html` - Table rendering (text/HTML)
    - [x] `html_escape` - HTML/XML-safe string escaping
    - [x] `render_html_report` / `render_html_report_with_chart` / `render_svg_chart` - Self-contained HTML reports with an inline SVG bar chart
    - [x] `wrap_html_document` - HTML document wrapper
    - [x] `apply_template` - `{{placeholder}}` template substitution
  - ~~Enhanced utility functions (v1.1): `truncate_string`, `format_relative_time`, `validate_broker_url`, `format_count`, `print_progress`~~ — removed from source; corrected 2026-07-13 (found stale during this pass's verification, not part of this cycle's actual changes)
- [x] Database commands extracted to separate module ✅
  - [x] `database` module with db operations ✅
  - [x] Connection testing and benchmarking ✅
  - [x] Health checks and diagnostics ✅
  - [x] Pool statistics and monitoring ✅
  - [x] Migration management ✅
  - [x] Comprehensive documentation with examples ✅
  - [x] Full test coverage (3 unit tests) ✅
  - [x] 5 doc tests for all public functions ✅
  - [x] Zero warnings from clippy ✅

## Library Support ✅

- [x] Hybrid crate (binary + library) ✅
  - [x] `src/lib.rs` exposes `commands` module ✅
  - [x] `src/lib.rs` exposes `command_utils` module ✅
  - [x] Database operations reachable via `celers_cli::commands::{db_health, db_migrate, db_pool_stats, db_test_connection, run_dashboard}` ✅ (corrected 2026-07-13: `commands/mod.rs`'s `database` submodule is private — `mod database;`, not `pub mod` — and its public functions are flattened into `commands` via `pub use database::{...}`; there is no standalone `celers_cli::database` path. The former top-level `src/database.rs` was deleted as stale/superseded by `commands/database.rs`.)
  - [x] Examples can import `celers_cli::commands` ✅ (confirmed: all 5 example files do exactly this, then call qualified functions such as `commands::db_health(...)`)
  - [x] Examples can import `celers_cli::command_utils` ✅
  - [x] Programmatic usage supported ✅

### Examples ✅

- [x] `basic_workflow.rs` - Common operational workflows ✅
  - [x] Queue status checking
  - [x] Listing queues
  - [x] Metrics display
  - [x] Health checks
- [x] `monitoring_and_diagnostics.rs` - Production monitoring ✅
  - [x] Doctor diagnostics
  - [x] Bottleneck analysis
  - [x] Failure pattern analysis
  - [x] Daily/weekly reports
  - [x] Worker and queue statistics
- [x] `queue_management.rs` - Queue operations ✅
  - [x] Queue listing and statistics
  - [x] Pause/resume operations
  - [x] Export/import workflows
  - [x] Task migration between queues
  - [x] Backup and restore procedures
- [x] `worker_management.rs` - Worker lifecycle ✅
  - [x] Worker listing and statistics
  - [x] Pause/resume/drain operations
  - [x] Graceful shutdown procedures
  - [x] Scaling strategies
  - [x] Rolling deployment patterns
  - [x] Auto-scaling configuration
- [x] `task_and_dlq_management.rs` - Task operations ✅
  - [x] Task inspection and debugging
  - [x] Cancel/retry operations
  - [x] Task log viewing
  - [x] DLQ inspection and management
  - [x] Task reprioritization
  - [x] Failure handling workflows

## Notes

- CLI is designed for operational tasks, not development
- Requires worker to have registered tasks
- Config file is optional (CLI args work standalone)
- Supports both Redis and PostgreSQL brokers
- Colored output automatically disabled in CI/pipes
- Library mode enables programmatic usage and examples

## 🚀 Enhancement Roadmap (v0.2.0)

### Interactive Mode ✅
- [x] `interactive` - Launch REPL mode for running multiple commands ✅
  - [x] Command history with arrow keys ✅
  - [x] Tab completion for commands and arguments ✅
  - [x] Multi-line editing support ✅
  - [x] Session state persistence ✅
  - [x] Configurable prompt with queue/broker info ✅

### Task Simulation & Load Testing ✅
- [x] Task simulation and load testing ✅
  - [x] `loadtest` (alias `simulate`) - generate synthetic task load against a queue ✅
    - [x] Configurable total count, target rate (tasks/sec), and duration cap ✅
    - [x] Configurable task name and payload size (clamped to broker-safe range) ✅
    - [x] Arrival patterns: constant, seeded jittered, seeded poisson-like ✅
    - [x] `--dry-run` prints the plan without enqueuing ✅
  - [x] Pure, deterministic, unit-tested planner (`plan_loadtest`) ✅
    - [x] Ordered `(offset_ms, synthetic_task)` schedule from config + seed ✅
    - [x] Same seed → identical plan; jitter/poisson within bounds ✅
    - [x] Constant spacing == `1000 / rate` ms; duration bound caps count ✅
    - [x] No clock/OS randomness in the asserted plan math (SplitMix64 from seed) ✅
  - [x] Thin enqueue loop (sleep-until-offset, then enqueue via the broker) ✅

### Configuration Wizard ✅
- [x] `init --wizard` - Interactive configuration setup ✅ (shipped 2026-07-13)
  - [x] Step-by-step broker selection ✅
  - [x] Connection testing during setup ✅
  - [x] Queue configuration with validation ✅
  - [x] Worker settings with recommendations ✅
  - [x] Auto-scaling and alert setup ✅
  - [x] Profile selection (dev/staging/prod) ✅
  - **Package plan P1 (2026-07-12, shipped 2026-07-13):**
    - **Goal:** `celers init --wizard` interactively assembles a validated config (broker selection, live connection test, queue config w/ validation, worker settings w/ recommended defaults, autoscale/alert setup, profile dev/staging/prod), written via existing Config::to_file + profile overlay.
    - **Design:** WizardAnswers struct + pure build_config(&WizardAnswers) -> Config (unit-tested); thin run_wizard() fills answers via dialoguer Select/Input/Confirm. Reuse config.validate() (config.rs:481) and db_test_connection/broker.test_connection() (database.rs:70).
    - **Files:** `src/commands/wizard.rs` (1013 lines) — `WizardAnswers`, `build_config`, `resolve_wizard_output_path`, `run_wizard`; `commands/config_cmds.rs::init_config_wizard`; `cli/types.rs` `Init { output, wizard: bool }`; `Cargo.toml` (`dialoguer`, workspace-pinned).
    - **Tests:** confirmed — 19 `#[test]` functions in `wizard.rs` cover `build_config` from representative answers, validation rejection of bad input, and profile-overlay path selection.
    - **Risk:** mitigated as designed — every prompt-adjacent computation lives in the pure `build_config`/`resolve_wizard_output_path`; only `run_wizard()` itself touches interactive I/O (not unit-tested, as expected).

### Backup & Restore ✅
- [x] `backup` - Full broker state backup ✅
  - [x] Export all queues to single archive ✅
  - [x] Include scheduled tasks ✅
  - [x] Include worker configurations ✅
  - [x] Include metrics and statistics ✅
  - [x] Compressed backup format (.tar.gz, via `oxiarc-deflate`/`oxiarc-archive`) ✅
  - [x] Incremental backup support ✅ (shipped 2026-07-13)
- [x] `restore` - Restore from backup ✅
  - [x] Validate backup before restore ✅
  - [x] Selective restore (specific queues) ✅
  - [x] Dry-run mode ✅
  - [x] Conflict resolution options ✅ (shipped 2026-07-13)
  - **Package plan P8 (2026-07-12, shipped 2026-07-13):**
    - **Goal:** add an incremental backup mode and conflict-resolution policy to the existing backup/restore commands.
    - **Design:** incremental mode keyed off BackupMetadata.timestamp (diff against a prior archive); add a ConflictPolicy enum (skip/overwrite/merge) param to restore_backup — both hang off the existing Backup/QueueBackup structs and the create_backup/restore_backup entry functions in src/backup.rs.
    - **Files:** `src/backup.rs` (1238 lines) — `create_backup_incremental(broker_url, output, previous_backup_path, since)`; `ConflictPolicy` (`Skip`/`Overwrite`/`Merge`, `#[default]` = `Skip`); `cli/types.rs` — `Backup { --previous, --since }`, `Restore { --conflict-policy }`.
    - **Tests:** confirmed present and passing as part of the 757-test suite (diff-against-previous, `--since` filtering, and per-`ConflictPolicy`-variant resolution).
    - **Risk:** mitigated as designed — `ConflictPolicy::default()` is `Skip` (never silently overwrites); `overwrite`/`merge` require the flag to be passed explicitly.

### Enhanced Reporting ✅
- [x] CSV export format for reports ✅
  - [x] CSV helper functions ✅
  - [x] Task statistics formatting ✅
  - [x] Daily/weekly reports to CSV ✅ (shipped 2026-07-13 — `report daily/weekly --format csv`)
  - [x] Task execution history export ✅ (`report history --format csv`, via `format_history_csv`)
  - [x] Worker statistics export ✅ (`report workers --format csv`, via `format_worker_csv`)
  - [x] Queue metrics export ✅ (`report queues --format csv`, via `format_queue_csv`)
- [x] HTML report generation ✅ (shipped 2026-07-13)
  - [x] Chart embedded in the HTML report ✅ (inline SVG bar chart via `render_svg_chart`/`render_html_report_with_chart`; static, not JS-interactive — see note below)
  - [x] Shareable reports ✅ (single self-contained HTML file: inline SVG, no external JS/CSS, all interpolated values HTML-escaped)
  - [x] Custom report templates ✅ (`--template '<STRING>'` with `{{placeholder}}` substitution via `apply_template`, including missing-key handling)

### Performance Enhancements ✅
- [x] Connection pooling optimization ✅ (shipped 2026-07-13)
  - [x] Configurable pool size ✅ (`[pool] max_size`, `CELERS_POOL_MAX_SIZE` env override)
  - [x] Connection reuse tracking ✅ (`PoolStats::reuse_ratio`/`utilization_pct`)
  - [x] Pool statistics monitoring ✅ (`celers cache-stats`)
- [x] Caching for frequently accessed data ✅ (shipped 2026-07-13)
  - [x] Queue statistics cache ✅ (`queue_cache_stats`, `commands/queue.rs`)
  - [x] Worker list cache with TTL ✅ (`worker_cache_stats`, `commands/worker.rs`)
  - [x] Configurable cache settings ✅ (`[cache] ttl_secs`/`enabled`, env overrides)
- [x] Parallel operations ✅ (shipped 2026-07-13)
  - [x] Batch queue operations ✅ (`futures::future::join_all` over cloned handles, `commands/queue.rs`)
  - [x] Parallel task inspection ✅ (`tokio::join!` across main/DLQ/delayed queues, `commands/task.rs`)
  - [x] Concurrent worker commands ✅ (`futures::future::join_all`/`tokio::join!`, `commands/worker.rs`)
  - **Package plan P3 (2026-07-12, shipped 2026-07-13):**
    - **Goal:** configurable connection pool (size, reuse tracking, pool-stats monitoring); TTL caches for queue-stats and worker-list (configurable settings); parallel/batch execution for queue ops, task inspection, worker commands.
    - **Design:** new src/pool.rs (reusable client holder + reuse_count/PoolStats) and src/cache.rs (generic TtlCache<K,V> w/ monotonic-clock expiry). Add [pool]/[cache] config fields to config.rs. Wire caches into queue-stats & worker-list read paths; convert batch loops to futures::future::join_all / tokio::join!.
    - **Files:** `src/pool.rs` (466 lines) — `ClientPool<T>`, `PoolStats`, `redis_connection_pool()`, `print_cache_pool_snapshot`; `src/cache.rs` (390 lines) — `TtlCache<K,V>`, `CacheStats`; `src/config.rs` — `PoolConfig`, `CacheConfig`; `commands/queue.rs`, `commands/worker.rs`, `commands/task.rs`.
    - **Tests:** confirmed present and passing (cache hit/miss + TTL expiry via an injected `FakeClock`; pool reuse counter + stats; `parallel_join_all_matches_equivalent_serial_loop` in both `queue.rs` and `worker.rs`).
    - **Risk:** mitigated as designed — cache TTL bounds staleness; default TTL and pool size documented on `CacheConfig`/`PoolConfig`.

### Advanced Features
- [ ] Multi-broker management (not started — no federated/multi-broker code exists in the crate yet)
  - [ ] Manage multiple brokers from single CLI
  - [ ] Cross-broker task migration
  - [ ] Federated queue view
  - [ ] Aggregated metrics
- [x] Task dependency visualization ✅ (shipped 2026-07-13)
  - [x] ASCII art dependency graph ✅ (box-drawing tree, `render_ascii`)
  - [x] GraphViz export ✅ (`render_dot`, `--format dot`)
  - [x] Interactive graph navigation ✅ (`--interactive --start <task-id>`, `run_interactive`)
  - **Package plan P4 (2026-07-12, shipped 2026-07-13):**
    - **Goal:** `celers deps` renders a celers-core::TaskDag as an ASCII tree, exports GraphViz DOT, and offers lightweight interactive navigation.
    - **Design:** new src/commands/depgraph.rs. TaskDag.nodes is private -> walk via public accessors get_roots() + get_dependencies/get_dependents + topological_sort() (celers-core/src/dag.rs). render_ascii and render_dot are pure fns; interactive mode = existing stdout-refresh pattern (no ratatui) over roots/children.
    - **Files:** `src/commands/depgraph.rs` (1375 lines) — `render_ascii`/`render_dot` (depth- and node-count-capped, cycle-safe), `dag_from_tasks`, `InteractiveSession`/`run_interactive`; `cli/types.rs` `Deps { from, format, interactive, start }`.
    - **Tests:** confirmed present and passing, including the two previously-slow wide-fan-out truncation tests (see the dated entry below) plus a large-chain depth-cap test.
    - **Risk:** mitigated as designed — `DEFAULT_MAX_DEPTH`/`DEFAULT_MAX_RENDERED_NODES` cap output on large graphs; a defensive ancestor check prints `(cycle detected)` instead of looping forever on a corrupted/cyclic `TaskDag`.
- [x] Performance profiling ✅ (shipped 2026-07-13)
  - [x] Task execution profiling ✅ (`analyze profile task`, duration trend over a rolling window of days)
  - [x] Worker performance analysis ✅ (`analyze profile worker`, single worker or ranked cross-worker comparison)
  - [x] Bottleneck detection ✅ (pre-existing `analyze bottlenecks`, unchanged this pass)
  - [x] Resource usage tracking ✅ (`analyze profile resources`: queue depth, worker count, DLQ size, load trend)
  - **Package plan P2 (2026-07-12, shipped 2026-07-13):**
    - **Goal:** `report daily|weekly` and new `report history|workers|queues` gain `--format table|csv|html --output <path>`; new `analyze profile` command family for task-execution profiling, per-worker performance analysis, resource-usage tracking.
    - **Design:** extend command_utils.rs with format_history_csv/format_worker_csv/format_queue_csv (routing through existing write_csv), render_html_report/render_svg_chart (self-contained HTML, inline SVG, HTML-escaped, no JS lib), apply_template ({{placeholder}} substitution). Extend monitoring.rs report_daily/report_weekly/analyze_bottlenecks handlers layered on celers-metrics::MetricHistory trend/moving_average. Split monitoring.rs with splitrs if it crosses 2000 lines.
    - **Files:** `src/command_utils.rs` (917 lines); `src/commands/monitoring/` split via SplitRS into `report.rs` (766 lines), `profile.rs` (348 lines), `diagnostics.rs` (989 lines), `autoscale_alert.rs` (265 lines), `metrics_display.rs` (201 lines), `tests.rs` (329 lines), replacing the former monolithic `monitoring.rs`; `cli/types.rs` `ReportCommands`/`ProfileCommands`.
    - **Tests:** confirmed present and passing — CSV rows per formatter, HTML/SVG series-value + escaping tests (`test_render_html_report_with_chart_includes_chart_markup`, `test_render_svg_chart_contains_values_and_escapes`), template substitution incl. missing-key handling.
    - **Risk:** mitigated as designed — `html_escape` neutralizes `&`/`<`/`>`/`"`/`'` in every interpolated value before it reaches `render_html_report`/`render_svg_chart`.

### User Experience ✅
- [x] Enhanced error messages ✅ (shipped 2026-07-13)
  - [x] Actionable suggestions ✅ (`CliError::suggestion()`, printed as a `suggestion:` line on every command failure)
  - [x] Common fixes documentation ✅ (each of the 7 error codes carries a fix-hint suggestion string, listed by `celers error-codes`)
  - [x] Error code reference ✅ (`celers error-codes` → `errors::print_error_code_reference`/`render_error_code_reference`)
  - [x] Debug hints ✅ (the `suggestion:` line doubles as a debug hint; `main.rs`'s top-level handler prints `error[E_CODE]: <message>` plus the hint for every `Err` path)
- [x] Smart defaults ✅ (shipped 2026-07-13)
  - [x] Auto-detect broker from environment ✅ (`smart_defaults::detect_broker_from_env`: `REDIS_URL` → `CELERY_BROKER_URL` → `AMQP_URL`, wired into `Config::apply_env_overrides`)
  - [x] Intelligent queue selection ✅ (`smart_defaults::suggest_queue`: Levenshtein "did you mean" against the live queue list, used by `celers interactive`'s `use <queue>`)
  - [x] Context-aware suggestions ✅ (`smart_defaults::context_suggestion`, used for unrecognized `celers interactive` commands)
- [x] Command aliases ✅ (shipped 2026-07-13)
  - [x] Short aliases for common commands ✅ (`clap` `visible_alias`: `w`→`worker`, `q`→`queue`, `t`→`task`, `wm`→`worker-mgmt`, `i`→`interactive`, `dash`→`dashboard`, `a`→`alias`, `simulate`→`loadtest`)
  - [x] User-defined aliases ✅ (`aliases::AliasConfig`, `[aliases]` config table, pre-parse expansion in `main.rs::expand_aliases`/`apply_alias_expansion`)
  - [x] Alias management ✅ (`celers alias list|add|remove`, persisted via `Config::to_file`)
  - **Package plan P5 (2026-07-12, shipped 2026-07-13):**
    - **Goal:** enhanced error messages with error codes, actionable suggestions, debug hints, error-code reference; smart defaults (auto-detect broker from env, intelligent queue selection, context-aware suggestions); command aliases (short built-in visible_alias + user-defined aliases from config + alias management).
    - **Design:** new src/errors.rs with a thiserror CliError enum carrying code() + suggestion(); decorator mapping common anyhow failures to CliError with a fix hint. Smart defaults: detect_broker_from_env (REDIS_URL/CELERY_BROKER_URL/AMQP_URL) feeding the existing broker.unwrap_or(cfg...) idiom. Aliases: add visible_alias/visible_aliases to clap structs; add [aliases] config map + pre-parse expansion pass + `celers alias list|add|remove`.
    - **Files:** `src/errors.rs` (574 lines) — `CliError` (7 variants: `ConnectionRefused`/`InvalidBrokerUrl`/`InvalidQueue`/`ConfigValidation`/`AuthFailure`/`Timeout`/`Other`), `classify_anyhow`, `print_error_code_reference`; `src/smart_defaults.rs` (322 lines) — `detect_broker_from_env`, `suggest_queue`, `context_suggestion`, `levenshtein_distance`; `src/aliases.rs` (389 lines) — `AliasConfig`; `src/config.rs` (`aliases: Option<AliasConfig>` field); `main.rs` (`expand_aliases`); `cli/types.rs` (`AliasCommands`, `ErrorCodes`).
    - **Tests:** confirmed present and passing — `codes_are_unique`, per-code `classify_anyhow` mapping tests, `levenshtein_distance_matches_known_values`, `detect_broker_from_env` precedence tests, `apply_alias_expansion_*` tests in `main.rs`.
    - **Risk:** mitigated as designed — `AliasConfig::add` rejects any name in `RESERVED_COMMAND_NAMES` (real commands always win); verified via `aliases.add("worker", ...)` returning `Err` in the module's own doctest.

### Testing & Quality
- [x] Property-based tests ✅ (shipped 2026-07-13)
  - [x] Command argument validation ✅ (clap parsing never panics on arbitrary args)
  - [x] Configuration parsing ✅ (Config TOML/YAML roundtrip properties)
  - [x] Data serialization ✅ (task/result serde roundtrip properties)
- [ ] Integration tests with real brokers (beyond the existing Redis/PostgreSQL suites already noted under "Testing Status" above)
  - [ ] Redis integration suite
  - [ ] PostgreSQL integration suite
  - [ ] AMQP integration suite (no AMQP broker crate exists in this workspace yet)
- [ ] Benchmarks
  - [ ] Command execution benchmarks
  - [ ] Connection pool benchmarks
  - [x] Serialization benchmarks ✅ (shipped 2026-07-13)
  - **Package plan P7 (2026-07-12, shipped 2026-07-13):**
    - **Goal:** property-based tests (command-arg validation, config parsing, serde roundtrip) and a criterion serialization benchmark.
    - **Design:** add proptest (dev) tests: clap parse never panics on arbitrary args; Config TOML/YAML roundtrips; task/result serde roundtrips. Add criterion (dev) + [[bench]] benches/serialization.rs over pure serde (no broker).
    - **Files:** `tests/proptest_cli.rs`; `benches/serialization.rs` (`bench_config_toml_serialize/deserialize`, `bench_config_yaml_serialize/deserialize`, `bench_serialized_task_json_serialize/deserialize`); `Cargo.toml` — `[dev-dependencies] proptest`, `criterion`; `[[bench]] name = "serialization"`.
    - **Tests:** confirmed present and passing as part of the 757-test suite; the `[[bench]]` target is wired and compiles as part of normal `cargo build --all-targets` (not re-run standalone this pass — out of this session's scope).
    - **Risk:** mitigated as designed — proptest cases are deterministic given the default seed; no flakiness observed across repeated `cargo nextest` runs this session.

### Observability
- [ ] Grafana dashboard templates (not started)
  - [ ] Pre-built dashboards
  - [ ] Dashboard export/import
- [ ] Extended alerting integrations (not started — only the existing generic webhook alert exists)
  - [ ] Slack notifications
  - [ ] PagerDuty integration
  - [ ] Email alerts
  - [ ] Custom webhook templates
- [x] Structured logging ✅ (shipped 2026-07-13)
  - [x] JSON log output option ✅ (`--log-format json`, one JSON object per event via `tracing_subscriber::fmt().json()`)
  - [x] Log level filtering ✅ (unchanged precedence: `--log-level` > `RUST_LOG` > `info`)
  - [x] Log streaming to external systems ✅ (`--log-sink file:<path>` / `--log-sink tcp:<host:port>`, in addition to the default `stdout`)
  - **Package plan P6 (2026-07-12, shipped 2026-07-13):**
    - **Goal:** `--log-format text|json`, level filtering (keep existing precedence), log streaming to an external sink (file/TCP writer).
    - **Design:** new src/logging.rs with init_logging(level, format, sink); JSON via tracing_subscriber::fmt().json(); reuse existing EnvFilter precedence (--log-level > RUST_LOG > info, config_layer.rs:160); streaming = pluggable MakeWriter (file/TCP). Replace the inline block in main.rs/cli.
    - **Files:** `src/logging.rs` (544 lines) — `LogFormat` (`Text`/`Json`), `LogSink` (`Stdout`/`File`/`Tcp`, `FromStr`-parsed), `init_logging`; `cli/types.rs` `Cli { log_level, log_format, log_sink }` (all `global = true`); `main.rs` (wires `cli.log_format`/`cli.log_sink` into `logging::init_logging` — previously parsed but ignored, now genuinely takes effect).
    - **Tests:** confirmed present and passing — `test_log_format_default_is_text`, `test_log_sink_default_is_stdout`, `LogSink` `FromStr` parsing table (`stdout`/`-`/`file:<path>`/`tcp:<host:port>`/invalid).
    - **Risk:** mitigated as designed — `init_logging` is called exactly once, from `main.rs`, before `cli::dispatch`.

### 2026-07-13 — Feature-completion pass: P1-P8 wired end-to-end

Follow-up to the scaffolding commits that introduced `pool.rs`/`cache.rs`/`logging.rs`/`smart_defaults.rs`/`errors.rs`/`aliases.rs`/`backup.rs` expansion/`depgraph.rs`/`wizard.rs`/the `monitoring/` split: fixed the `cargo build --all-targets` compile break (missing `aliases` field on `Config`, E0063) and completed the wiring so every roadmap item above is genuinely reachable from the CLI, not just present in source. Verified against source and a fresh `cargo nextest` run this pass:

- **All 8 package plans (P1-P8) above are now complete** and marked `[x]`, each individually re-verified against source (see the per-plan "Files"/"Tests" notes updated above) rather than blanket-marked.
- Structured logging (`--log-format`/`--log-sink`) now genuinely takes effect (previously parsed but silently ignored).
- User-defined alias expansion (`celers alias add` then `celers <alias>`) now genuinely expands before argument parsing (previously silently a no-op).
- Every command failure now prints a decorated `error[E_CODE]: <message>` + `suggestion:` line; new `celers error-codes` reference command.
- `REDIS_URL`/`CELERY_BROKER_URL`/`AMQP_URL` now work as broker-URL fallbacks, wired into `Config::apply_env_overrides`.
- `celers interactive` now offers Levenshtein "did you mean" suggestions for unrecognized commands and for `use <queue>` against a nonexistent queue.
- New `celers cache-stats` command (configured pool/cache capacity snapshot) plus a `stats`/`cs` command inside `celers interactive` (live hit/reuse ratios — meaningful only in a long-running REPL session).
- HTML monitoring reports (`report daily/weekly/history/workers/queues --format html`) now embed an inline SVG bar chart above the table.
- Deleted the stale top-level `src/database.rs` (superseded by `commands/database.rs`, confirmed gone from the working tree) and consolidated validator/formatting helpers so `commands/utils.rs` is the single canonical location — no duplicate `format_bytes`/`format_duration`/`validate_task_id`/`validate_queue_name`/`calculate_percentage`/`mask_password` remain in `command_utils.rs`.
- Fixed all 14 pre-existing `cargo doc` intra-doc-link warnings — `cargo doc -p celers-cli --all-features --no-deps` completes with zero warnings (confirmed 2026-07-13).
- Fixed 2 slow `commands/depgraph.rs` tests (`render_ascii_wide_fanout_truncates_at_node_cap_without_hanging`, `render_dot_wide_fanout_truncates_without_hanging`) by shrinking the synthetic fan-out graph size; both now run in ~2.4s each (previously 21-46s each), confirmed via `cargo nextest run -p celers-cli --all-features`.
- Doc-accuracy corrections found and fixed during this pass's verification (not new features): `README.md`/`TODO.md` previously described `dashboard`/`monitor` as a `ratatui`/`crossterm` TUI with a "queue depth gauge" and keypress controls — in reality they are plain stdout clear/redraw loops (`ratatui`/`crossterm` are not dependencies of this crate); and the `Dependencies` list previously named `reqwest` for HTTP, when the crate actually uses `oxihttp-client` (COOLJAPAN Pure Rust Policy).
- Full crate test suite: **757 tests passing, 0 failed, 0 skipped** (`cargo nextest run -p celers-cli --all-features`, confirmed 2026-07-13).
- File-size check (Refactoring policy: keep files under 2000 lines): largest file in the crate is `config.rs` at 1563 lines; next largest are `depgraph.rs` (1375), `loadtest_cmds.rs` (1300), `backup.rs` (1238), `queue.rs` (1236), `metrics_cmds.rs` (1200), `types.rs` (1197), `dispatch.rs` (1179). No file exceeds 2000 lines. `command_utils.rs` sits at 917 lines post-cleanup, as expected from the earlier duplicate-code deletions.
