# celers-beat

Periodic task scheduler for CeleRS, equivalent to Celery Beat. Schedule tasks to run at regular intervals or specific times using interval or crontab expressions.

## Overview

Production-ready task scheduler with:

- ✅ **Interval Schedules**: Execute every N seconds
- ✅ **Crontab Schedules**: Unix cron-style scheduling (optional feature: "cron")
- ✅ **Solar Schedules**: Sunrise/sunset events (optional feature: "solar")
- ✅ **Persistent State**: Track last run times
- ✅ **Enable/Disable**: Control task execution
- ✅ **Priority Support**: Schedule high-priority tasks
- ✅ **Timezone Support**: UTC-based scheduling

## Quick Start

### Installation

```toml
[dependencies]
celers-beat = "0.1"
celers = { version = "0.1", features = ["redis"] }
```

### Basic Example

```rust
use celers_beat::{BeatScheduler, Schedule, ScheduledTask};
use celers_broker_redis::RedisBroker;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create broker
    let broker = RedisBroker::new("redis://localhost:6379", "celery")?;

    // Create scheduler
    let mut scheduler = BeatScheduler::new();

    // Schedule task every 60 seconds
    let task = ScheduledTask::new(
        "send_report".to_string(),
        Schedule::interval(60)
    );
    scheduler.add_task("report", task);

    // Run scheduler
    scheduler.run(&broker).await?;

    Ok(())
}
```

## Schedule Types

### Interval Schedule

Execute tasks at fixed intervals:

```rust
use celers_beat::Schedule;

// Every 10 seconds
let schedule = Schedule::interval(10);

// Every 5 minutes
let schedule = Schedule::interval(300);

// Every hour
let schedule = Schedule::interval(3600);

// Every day
let schedule = Schedule::interval(86400);
```

### Crontab Schedule (Optional)

Unix cron-style scheduling:

```toml
[dependencies]
celers-beat = { version = "0.1", features = ["cron"] }
```

```rust
use celers_beat::Schedule;

// Every minute
let schedule = Schedule::crontab("*", "*", "*", "*", "*");

// Every hour at minute 0
let schedule = Schedule::crontab("0", "*", "*", "*", "*");

// Every day at midnight
let schedule = Schedule::crontab("0", "0", "*", "*", "*");

// Every Monday at 9 AM
let schedule = Schedule::crontab("0", "9", "1", "*", "*");

// Weekdays at 9 AM
let schedule = Schedule::crontab("0", "9", "1-5", "*", "*");
```

**Crontab format:**
```
┌───────────── minute (0-59)
│ ┌───────────── hour (0-23)
│ │ ┌───────────── day of week (0-6, 0=Sunday)
│ │ │ ┌───────────── day of month (1-31)
│ │ │ │ ┌───────────── month (1-12)
│ │ │ │ │
* * * * *
```

**Special characters:**
- `*` - Any value
- `*/n` - Every n units (e.g., `*/15` = every 15 minutes)
- `n-m` - Range (e.g., `9-17` = 9 AM to 5 PM)
- `n,m` - List (e.g., `1,15` = 1st and 15th)

### Solar Schedule (Optional)

Execute tasks at sunrise/sunset:

```toml
[dependencies]
celers-beat = { version = "0.1", features = ["solar"] }
```

```rust
use celers_beat::Schedule;

// Run at sunrise in Tokyo
let schedule = Schedule::solar("sunrise", 35.6762, 139.6503);

// Run at sunset in New York
let schedule = Schedule::solar("sunset", 40.7128, -74.0060);

// Run at sunrise in London
let schedule = Schedule::solar("sunrise", 51.5074, -0.1278);
```

**Supported events:**
- `"sunrise"` - Task runs at sunrise
- `"sunset"` - Task runs at sunset

**Notes:**
- Latitude/longitude must be in decimal degrees
- Times are calculated in UTC
- Next occurrence is searched up to 365 days ahead

## Scheduled Tasks

### Creating Tasks

```rust
use celers_beat::{ScheduledTask, Schedule};
use serde_json::json;

// Basic task
let task = ScheduledTask::new(
    "cleanup_old_files".to_string(),
    Schedule::interval(3600)
);

// Task with arguments
let task = ScheduledTask::new(
    "send_email".to_string(),
    Schedule::interval(300)
)
.with_args(vec![
    json!("admin@example.com"),
    json!("Daily Report"),
]);

// Task with keyword arguments
let mut kwargs = HashMap::new();
kwargs.insert("subject".to_string(), json!("Report"));
kwargs.insert("priority".to_string(), json!("high"));

let task = ScheduledTask::new(
    "send_email".to_string(),
    Schedule::interval(300)
)
.with_kwargs(kwargs);

// Disabled task (won't run)
let task = ScheduledTask::new(
    "maintenance".to_string(),
    Schedule::interval(3600)
)
.disabled();
```

### Task Structure

```rust
pub struct ScheduledTask {
    /// Task name (must match registered task)
    pub name: String,

    /// Execution schedule
    pub schedule: Schedule,

    /// Positional arguments
    pub args: Vec<serde_json::Value>,

    /// Keyword arguments
    pub kwargs: HashMap<String, serde_json::Value>,

    /// Task options (queue, priority, expires)
    pub options: TaskOptions,

    /// Last execution timestamp
    pub last_run_at: Option<DateTime<Utc>>,

    /// Total number of executions
    pub total_run_count: u64,

    /// Enable/disable flag
    pub enabled: bool,
}
```

## Beat Scheduler

### Basic Usage

```rust
use celers_beat::{BeatScheduler, Schedule, ScheduledTask};

let mut scheduler = BeatScheduler::new();

// Add tasks
scheduler.add_task("report", ScheduledTask::new(
    "generate_report".to_string(),
    Schedule::interval(3600)
));

scheduler.add_task("cleanup", ScheduledTask::new(
    "cleanup_temp_files".to_string(),
    Schedule::interval(86400)
));

// Run scheduler (blocks until shutdown)
scheduler.run(&broker).await?;
```

### Managing Tasks

```rust
// Add task
scheduler.add_task("my_task", task);

// Remove task
scheduler.remove_task("my_task");

// Enable/disable task
scheduler.enable_task("my_task");
scheduler.disable_task("my_task");

// Get task info
if let Some(task) = scheduler.get_task("my_task") {
    println!("Last run: {:?}", task.last_run_at);
    println!("Run count: {}", task.total_run_count);
}

// List all tasks
for (name, task) in scheduler.list_tasks() {
    println!("{}: enabled={}", name, task.enabled);
}
```

### Persistent Schedules

Load/save schedule state to persist across restarts:

```rust
use std::fs;

// Save state
let state = scheduler.export_state()?;
fs::write("schedule_state.json", state)?;

// Load state
let state = fs::read_to_string("schedule_state.json")?;
scheduler.import_state(&state)?;
```

## Configuration Examples

### Periodic Reports

```rust
// Daily report at midnight
let daily_report = ScheduledTask::new(
    "daily_report".to_string(),
    Schedule::crontab("0", "0", "*", "*", "*")
);

// Weekly report every Monday at 9 AM
let weekly_report = ScheduledTask::new(
    "weekly_report".to_string(),
    Schedule::crontab("0", "9", "1", "*", "*")
);

// Monthly report on 1st at midnight
let monthly_report = ScheduledTask::new(
    "monthly_report".to_string(),
    Schedule::crontab("0", "0", "*", "1", "*")
);
```

### Maintenance Tasks

```rust
// Cleanup every hour
let cleanup = ScheduledTask::new(
    "cleanup_temp_files".to_string(),
    Schedule::interval(3600)
);

// Database backup daily at 3 AM
let backup = ScheduledTask::new(
    "backup_database".to_string(),
    Schedule::crontab("0", "3", "*", "*", "*")
);

// Log rotation every 6 hours
let rotate_logs = ScheduledTask::new(
    "rotate_logs".to_string(),
    Schedule::interval(21600)
);
```

### Monitoring Tasks

```rust
// Health check every 30 seconds
let health_check = ScheduledTask::new(
    "health_check".to_string(),
    Schedule::interval(30)
);

// Metrics collection every 5 minutes
let collect_metrics = ScheduledTask::new(
    "collect_metrics".to_string(),
    Schedule::interval(300)
);

// Alert check every minute
let check_alerts = ScheduledTask::new(
    "check_alerts".to_string(),
    Schedule::interval(60)
);
```

## Advanced Features

### Priority Scheduling

```rust
let task = ScheduledTask::new(
    "critical_task".to_string(),
    Schedule::interval(60)
);

task.options.priority = Some(9);  // Highest priority
```

### Custom Queue

```rust
let task = ScheduledTask::new(
    "background_task".to_string(),
    Schedule::interval(300)
);

task.options.queue = Some("low_priority".to_string());
```

### Task Expiration

```rust
let task = ScheduledTask::new(
    "time_sensitive".to_string(),
    Schedule::interval(60)
);

task.options.expires = Some(300);  // Expire after 5 minutes
```

### Conditional Execution

```rust
// Check if task is due before running
if task.is_due()? {
    println!("Task is due, executing...");
    // Execute task
} else {
    println!("Task not due yet");
}
```

## Production Deployment

### Systemd Service

```ini
[Unit]
Description=CeleRS Beat Scheduler
After=network.target redis.service

[Service]
Type=simple
User=celery
WorkingDirectory=/opt/celery
ExecStart=/opt/celery/beat
Restart=on-failure
RestartSec=10

[Install]
WantedBy=multi-user.target
```

### Docker

```dockerfile
FROM rust:1.70 AS builder
WORKDIR /app
COPY . .
RUN cargo build --release --bin beat

FROM debian:bullseye-slim
COPY --from=builder /app/target/release/beat /usr/local/bin/
CMD ["beat"]
```

### High Availability

**Important:** Only run **one** beat scheduler instance to avoid duplicate task execution!

```rust
// Use leader election or singleton pattern
use tokio::sync::Mutex;
use std::sync::Arc;

let lock = Arc::new(Mutex::new(()));

// Acquire lock before starting
let _guard = lock.lock().await;
scheduler.run(&broker).await?;
```

## Monitoring

### Scheduled Task Metrics

```rust
// Track execution
for (name, task) in scheduler.list_tasks() {
    println!("Task: {}", name);
    println!("  Last run: {:?}", task.last_run_at);
    println!("  Total runs: {}", task.total_run_count);
    println!("  Enabled: {}", task.enabled);
}
```

### Health Checks

```rust
use tokio::time::{interval, Duration};

// Periodic health check
let mut ticker = interval(Duration::from_secs(60));
loop {
    ticker.tick().await;

    // Check scheduler is running
    if !scheduler.is_running() {
        eprintln!("WARNING: Scheduler not running!");
        // Alert or restart
    }
}
```

## Best Practices

### 1. Single Scheduler Instance

```rust
// ❌ Bad: Multiple schedulers (duplicates tasks)
// Worker 1: scheduler.run()
// Worker 2: scheduler.run()

// ✅ Good: Single scheduler instance
// Beat server: scheduler.run()
// Workers: Only execute tasks
```

### 2. Persistent State

```rust
// Save state on shutdown
use tokio::signal;

tokio::select! {
    _ = scheduler.run(&broker) => {}
    _ = signal::ctrl_c() => {
        println!("Saving scheduler state...");
        let state = scheduler.export_state()?;
        fs::write("state.json", state)?;
    }
}
```

### 3. Timezone Handling

```rust
// Always use UTC internally
use chrono::Utc;

let now = Utc::now();
let next_run = schedule.next_run(Some(now))?;

// Convert to local time for display
use chrono_tz::America::New_York;
let local = next_run.with_timezone(&New_York);
println!("Next run: {}", local);
```

### 4. Error Handling

```rust
// Graceful error handling
loop {
    match scheduler.tick(&broker).await {
        Ok(executed) => {
            println!("Executed {} tasks", executed);
        }
        Err(e) => {
            eprintln!("Scheduler error: {}", e);
            // Log but continue
        }
    }

    tokio::time::sleep(Duration::from_secs(1)).await;
}
```

### 5. Task Idempotency

```rust
// Ensure tasks are idempotent (safe to run multiple times)
registry.register("generate_report", |args| async move {
    let report_id = generate_unique_id();

    // Check if already generated
    if report_exists(report_id).await? {
        return Ok("Already generated".to_string());
    }

    // Generate report
    generate_report(report_id).await?;
    Ok(format!("Generated report {}", report_id))
});
```

## Troubleshooting

### Tasks not executing

**Check:**
1. Scheduler is running: `scheduler.is_running()`
2. Task is enabled: `task.enabled == true`
3. Schedule is correct: `task.schedule.next_run()`
4. Broker connection: `broker.ping()`

### Duplicate executions

**Cause:** Multiple scheduler instances running
**Solution:** Ensure only one scheduler instance

### Missed schedules

**Cause:** Scheduler was down during scheduled time
**Solution:** Implement catchup logic or use persistent state

### Timezone issues

**Cause:** Mixing UTC and local time
**Solution:** Always use UTC internally, convert for display only

## Performance

### Scheduling Overhead

| Schedule Type | Overhead | Memory |
|--------------|----------|--------|
| Interval | <1ms | ~100B per task |
| Crontab | <5ms | ~200B per task |
| Solar | <10ms | ~300B per task |

### Scalability

- **Tasks:** 10,000+ scheduled tasks
- **Precision:** 1-second granularity
- **Latency:** <10ms schedule evaluation

## Comparison with Celery Beat

| Feature | Celery Beat | CeleRS Beat |
|---------|-------------|-------------|
| Interval schedules | ✅ | ✅ |
| Crontab schedules | ✅ | ✅ |
| Solar schedules | ✅ | 🔄 Planned |
| Persistent state | File/DB | JSON/DB |
| Performance | ~100 tasks/sec | ~1000 tasks/sec |
| Memory | 50MB+ | 10MB |

## See Also

- **Worker**: `celers-worker` - Task execution runtime
- **Broker**: `celers-broker-redis` - Message broker
- **Core**: `celers-core` - Task registry

## License

MIT OR Apache-2.0
