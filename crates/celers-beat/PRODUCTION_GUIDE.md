# Production Deployment Guide

Complete guide for deploying celers-beat scheduler in production environments.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Deployment Patterns](#deployment-patterns)
- [Configuration](#configuration)
- [State Management](#state-management)
- [Monitoring & Alerting](#monitoring--alerting)
- [High Availability](#high-availability)
- [Performance Tuning](#performance-tuning)
- [Security](#security)
- [Troubleshooting](#troubleshooting)

## Architecture Overview

### Single Scheduler Architecture

```
┌─────────────────┐
│   Beat Server   │
│   (Scheduler)   │
└────────┬────────┘
         │
         ├─> Redis/AMQP Broker
         │
         ▼
┌─────────────────────────┐
│   Worker Pool (1-N)     │
│  Execute Tasks          │
└─────────────────────────┘
```

**Critical:** Only run ONE beat scheduler instance to avoid duplicate task execution.

### Components

1. **Beat Scheduler** - Schedules tasks and sends them to broker
2. **Message Broker** - Redis, RabbitMQ, SQS, etc.
3. **Workers** - Execute the scheduled tasks
4. **State Store** - JSON file for schedule persistence

## Deployment Patterns

### Pattern 1: Dedicated Beat Server

**Recommended for production.**

```rust
// beat_server.rs
use celers_beat::{BeatScheduler, Schedule, ScheduledTask};
use celers_broker_redis::RedisBroker;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::init();

    // Load scheduler with persistence
    let state_file = "/var/lib/celers/beat_state.json";
    let mut scheduler = BeatScheduler::load_from_file(state_file)
        .unwrap_or_else(|_| BeatScheduler::with_persistence(state_file.to_string()));

    // Set instance ID for distributed deployments
    let instance_id = std::env::var("SCHEDULER_INSTANCE_ID")
        .unwrap_or_else(|_| hostname::get()?.to_string_lossy().to_string());
    scheduler.set_instance_id(instance_id);

    // Connect to broker
    let broker = RedisBroker::new(
        &std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6379".to_string()),
        "celery"
    )?;

    // Setup signal handling
    tokio::select! {
        result = scheduler.run(&broker) => {
            result?;
        }
        _ = tokio::signal::ctrl_c() => {
            log::info!("Shutdown signal received, saving state...");
            scheduler.save_state()?;
        }
    }

    Ok(())
}
```

### Pattern 2: Kubernetes Deployment

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: celers-beat
spec:
  replicas: 1  # MUST be 1
  selector:
    matchLabels:
      app: celers-beat
  template:
    metadata:
      labels:
        app: celers-beat
    spec:
      containers:
      - name: beat
        image: myapp/celers-beat:latest
        env:
        - name: REDIS_URL
          value: "redis://redis-service:6379"
        - name: SCHEDULER_INSTANCE_ID
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        volumeMounts:
        - name: state
          mountPath: /var/lib/celers
        resources:
          requests:
            memory: "128Mi"
            cpu: "100m"
          limits:
            memory: "256Mi"
            cpu: "200m"
      volumes:
      - name: state
        persistentVolumeClaim:
          claimName: beat-state-pvc
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: beat-state-pvc
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 1Gi
```

### Pattern 3: Docker Compose

```yaml
version: '3.8'

services:
  beat:
    build: .
    command: /app/beat_server
    environment:
      - REDIS_URL=redis://redis:6379
      - RUST_LOG=info
    volumes:
      - beat-state:/var/lib/celers
    depends_on:
      - redis
    restart: unless-stopped
    deploy:
      replicas: 1  # CRITICAL: Must be 1

  redis:
    image: redis:7-alpine
    volumes:
      - redis-data:/data

  worker:
    build: .
    command: /app/worker
    environment:
      - REDIS_URL=redis://redis:6379
    depends_on:
      - redis
    deploy:
      replicas: 4  # Scale workers as needed

volumes:
  beat-state:
  redis-data:
```

### Pattern 4: Systemd Service

```ini
# /etc/systemd/system/celers-beat.service
[Unit]
Description=CeleRS Beat Scheduler
After=network.target redis.service
Requires=redis.service

[Service]
Type=simple
User=celery
Group=celery
WorkingDirectory=/opt/celery
Environment="REDIS_URL=redis://localhost:6379"
Environment="RUST_LOG=info"
ExecStart=/opt/celery/bin/beat_server
Restart=on-failure
RestartSec=10
StandardOutput=journal
StandardError=journal

# Security hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/celers

[Install]
WantedBy=multi-user.target
```

Enable and start:
```bash
sudo systemctl enable celers-beat
sudo systemctl start celers-beat
sudo systemctl status celers-beat
```

## Configuration

### Environment Variables

```bash
# Required
REDIS_URL=redis://localhost:6379
# or
AMQP_URL=amqp://localhost:5672

# Optional
SCHEDULER_INSTANCE_ID=beat-01
RUST_LOG=info
STATE_FILE=/var/lib/celers/beat_state.json
TICK_INTERVAL=1  # seconds
```

### Programmatic Configuration

```rust
use celers_beat::{BeatScheduler, Schedule, ScheduledTask, AlertConfig};
use std::env;

fn configure_scheduler() -> Result<BeatScheduler, Box<dyn std::error::Error>> {
    let state_file = env::var("STATE_FILE")
        .unwrap_or_else(|_| "/var/lib/celers/beat_state.json".to_string());

    let mut scheduler = BeatScheduler::load_from_file(&state_file)
        .unwrap_or_else(|_| BeatScheduler::with_persistence(state_file));

    // Configure instance ID
    if let Ok(id) = env::var("SCHEDULER_INSTANCE_ID") {
        scheduler.set_instance_id(id);
    }

    // Register alert callbacks
    scheduler.on_alert(Arc::new(|alert| {
        log::error!(
            "[ALERT][{}] {}: {}",
            alert.level,
            alert.task_name,
            alert.message
        );
        // Send to monitoring system (PagerDuty, Slack, etc.)
    }));

    // Register failure callbacks
    scheduler.on_failure(Arc::new(|task, error| {
        log::error!("Task '{}' failed: {}", task, error);
        // Send to error tracking (Sentry, etc.)
    }));

    Ok(scheduler)
}
```

### Task Configuration Best Practices

```rust
use celers_beat::{ScheduledTask, Schedule, AlertConfig};

// Critical task with strict monitoring
let db_backup = ScheduledTask::new(
    "database_backup".to_string(),
    Schedule::crontab_tz("0", "2", "*", "*", "*", "UTC")
)
.with_alert_config(AlertConfig {
    enabled: true,
    consecutive_failures_threshold: 2,
    failure_rate_threshold: 0.3,
    slow_execution_threshold_ms: Some(300_000), // 5 minutes
    alert_on_missed_schedule: true,
    alert_on_stuck: true,
})
.with_queue("critical".to_string())
.with_priority(9);  // Highest priority

// Normal task with standard monitoring
let send_report = ScheduledTask::new(
    "daily_report".to_string(),
    Schedule::crontab("0", "9", "1-5", "*", "*")
)
.with_alert_config(AlertConfig {
    enabled: true,
    consecutive_failures_threshold: 3,
    failure_rate_threshold: 0.5,
    slow_execution_threshold_ms: Some(120_000), // 2 minutes
    alert_on_missed_schedule: true,
    alert_on_stuck: false,
})
.with_priority(5);

// Low priority cleanup task
let cleanup = ScheduledTask::new(
    "cleanup_temp".to_string(),
    Schedule::interval(7200)
)
.with_priority(2)
.with_expires(3600);  // Can skip if worker busy
```

## State Management

### Persistence Strategy

The scheduler automatically saves state on:
- Task addition
- Task removal
- Task updates
- Shutdown (if graceful)

```rust
// Manual save
scheduler.save_state()?;

// Manual load
let scheduler = BeatScheduler::load_from_file("/path/to/state.json")?;

// Export for backup
let json = scheduler.export_state()?;
std::fs::write("/backup/state.json", json)?;
```

### State File Location

**Recommended locations:**

- **Linux**: `/var/lib/celers/beat_state.json`
- **Docker**: Mounted volume `/data/beat_state.json`
- **Kubernetes**: PersistentVolume `/var/lib/celers/beat_state.json`

**Permissions:**
```bash
sudo mkdir -p /var/lib/celers
sudo chown celery:celery /var/lib/celers
sudo chmod 750 /var/lib/celers
```

### Backup Strategy

```bash
#!/bin/bash
# /opt/celery/scripts/backup_state.sh

STATE_FILE="/var/lib/celers/beat_state.json"
BACKUP_DIR="/var/backups/celers"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

mkdir -p "$BACKUP_DIR"
cp "$STATE_FILE" "$BACKUP_DIR/beat_state_${TIMESTAMP}.json"

# Keep only last 30 backups
ls -t "$BACKUP_DIR"/beat_state_*.json | tail -n +31 | xargs rm -f
```

Add to crontab:
```bash
0 */6 * * * /opt/celery/scripts/backup_state.sh
```

## Monitoring & Alerting

### Health Checks

```rust
use celers_beat::BeatScheduler;
use std::sync::Arc;
use tokio::sync::RwLock;

async fn health_check_endpoint(
    scheduler: Arc<RwLock<BeatScheduler>>
) -> Result<(), String> {
    let scheduler = scheduler.read().await;

    // Check overall health
    let stats = scheduler.get_comprehensive_stats();

    if stats.unhealthy_tasks > 0 {
        return Err(format!("{} unhealthy tasks", stats.unhealthy_tasks));
    }

    if stats.warning_tasks > 5 {
        return Err(format!("{} tasks with warnings", stats.warning_tasks));
    }

    Ok(())
}
```

### Metrics Collection

```rust
use celers_beat::BeatScheduler;
use tokio::time::{interval, Duration};

async fn metrics_collector(scheduler: Arc<RwLock<BeatScheduler>>) {
    let mut ticker = interval(Duration::from_secs(60));

    loop {
        ticker.tick().await;

        let scheduler = scheduler.read().await;
        let stats = scheduler.get_comprehensive_stats();

        // Export to Prometheus
        metrics::gauge!("celers_beat_total_tasks", stats.total_tasks as f64);
        metrics::gauge!("celers_beat_enabled_tasks", stats.enabled_tasks as f64);
        metrics::gauge!("celers_beat_unhealthy_tasks", stats.unhealthy_tasks as f64);
        metrics::counter!("celers_beat_total_executions", stats.total_executions);
        metrics::gauge!("celers_beat_success_rate", stats.success_rate);

        if let Some(avg_duration) = stats.avg_duration_ms {
            metrics::gauge!("celers_beat_avg_duration_ms", avg_duration as f64);
        }
    }
}
```

### Alert Integration

```rust
use celers_beat::{Alert, AlertLevel};

// PagerDuty integration
fn send_to_pagerduty(alert: &Alert) {
    if alert.level == AlertLevel::Critical {
        // Trigger PagerDuty incident
    }
}

// Slack integration
fn send_to_slack(alert: &Alert) {
    let color = match alert.level {
        AlertLevel::Critical => "danger",
        AlertLevel::Warning => "warning",
        AlertLevel::Info => "good",
    };

    // Post to Slack webhook
}

// Register callbacks
scheduler.on_alert(Arc::new(|alert| {
    send_to_pagerduty(alert);
    send_to_slack(alert);
}));
```

## High Availability

### Single Instance Guarantee

**Option 1: Kubernetes Leader Election**

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: beat-lease
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: celers-beat
spec:
  replicas: 2  # For redundancy
  template:
    spec:
      containers:
      - name: beat
        command: ["/app/beat_server", "--leader-elect"]
        env:
        - name: LEASE_NAME
          value: "beat-lease"
```

**Option 2: Consul/etcd Lock**

```rust
// Acquire distributed lock before starting
let lock = consul_client.acquire_lock("celers-beat-lock", 60)?;

if lock.is_acquired() {
    scheduler.run(&broker).await?;
} else {
    log::info!("Another instance is active, standing by...");
}
```

**Option 3: Database Lock**

```sql
-- PostgreSQL advisory lock
SELECT pg_try_advisory_lock(12345);
```

### Failover Strategy

```rust
use std::time::Duration;
use tokio::time::interval;

async fn standby_monitor(
    scheduler: Arc<RwLock<BeatScheduler>>,
    lock_service: Arc<dyn LockService>
) {
    let mut ticker = interval(Duration::from_secs(10));

    loop {
        ticker.tick().await;

        if lock_service.try_acquire_leader().await {
            log::info!("Became leader, starting scheduler");
            let scheduler = scheduler.read().await;
            // Start scheduling
        } else {
            log::debug!("Still standby");
        }
    }
}
```

## Performance Tuning

### Scheduler Performance

```rust
// Use batch operations for bulk task management
let tasks = vec![task1, task2, task3];
scheduler.add_tasks_batch(tasks)?;

// Use schedule indexing for faster lookups (automatic)
// No configuration needed

// Limit execution history to reduce memory
let mut task = ScheduledTask::new("task".to_string(), schedule);
task.max_history_size = 100;  // Keep last 100 executions
```

### Resource Limits

```yaml
# Kubernetes resource limits
resources:
  requests:
    memory: "128Mi"
    cpu: "100m"
  limits:
    memory: "512Mi"
    cpu: "500m"
```

### Tick Interval Tuning

```rust
// Default: 1 second
// For sub-second precision, use shorter intervals
// For resource efficiency, use longer intervals

let tick_interval = Duration::from_millis(500);  // 500ms precision
```

## Security

### File Permissions

```bash
# State file should be readable/writable only by scheduler user
chmod 600 /var/lib/celers/beat_state.json
chown celery:celery /var/lib/celers/beat_state.json
```

### Network Security

```rust
// Use TLS for broker connections
let broker = RedisBroker::new(
    "rediss://redis.example.com:6380",  // TLS
    "celery"
)?;

// Use authentication
let broker = RabbitMQBroker::new(
    "amqps://user:pass@rabbitmq.example.com:5671"
)?;
```

### Secrets Management

```rust
use std::env;

// Never hardcode credentials
let redis_url = env::var("REDIS_URL")
    .expect("REDIS_URL environment variable required");

// Or use a secrets manager
let redis_url = aws_secrets_manager::get_secret("redis-url")?;
```

## Troubleshooting

### Common Issues

#### 1. Tasks Not Executing

**Symptoms:** Tasks scheduled but not running

**Diagnosis:**
```rust
// Check task health
let health = scheduler.check_all_tasks_health();
for result in health {
    if !result.health.is_healthy() {
        println!("Unhealthy: {} - {:?}", result.task_name, result.health);
    }
}

// Check next run times
let preview = scheduler.preview_upcoming_executions(Some(10), None);
for (task, times) in preview {
    println!("{}: {:?}", task, times);
}
```

**Solutions:**
- Verify schedule syntax
- Check broker connection
- Ensure workers are running
- Check task is enabled

#### 2. Duplicate Task Execution

**Symptoms:** Tasks running multiple times

**Cause:** Multiple scheduler instances running

**Solution:**
```bash
# Check for multiple processes
ps aux | grep beat_server

# Ensure only one instance in Kubernetes
kubectl get pods -l app=celers-beat
# Should show only 1 pod
```

#### 3. High Memory Usage

**Symptoms:** Scheduler memory growing over time

**Diagnosis:**
```rust
let stats = scheduler.get_comprehensive_stats();
println!("Total tasks: {}", stats.total_tasks);
println!("Total executions: {}", stats.total_executions);
```

**Solutions:**
- Limit execution history: `task.max_history_size = 100`
- Reduce alert history: Default is 1000
- Check for task leaks

#### 4. State File Corruption

**Symptoms:** Scheduler won't start, state load fails

**Solution:**
```bash
# Restore from backup
cp /var/backups/celers/beat_state_latest.json /var/lib/celers/beat_state.json

# Or start fresh (loses execution history)
rm /var/lib/celers/beat_state.json
```

### Logging

```rust
use env_logger;

// Initialize with environment variable
// RUST_LOG=debug cargo run
env_logger::init();

// Or programmatically
env_logger::Builder::from_default_env()
    .filter_level(log::LevelFilter::Info)
    .init();
```

### Debug Mode

```rust
// Enable debug logging for scheduler
RUST_LOG=celers_beat=debug cargo run

// Check task details
let task = scheduler.get_task("my_task").unwrap();
println!("{}", task);  // Uses Display implementation
```

## See Also

- [README.md](README.md) - Main documentation
- [CRONTAB_GUIDE.md](CRONTAB_GUIDE.md) - Crontab syntax
- [CELERY_MIGRATION.md](CELERY_MIGRATION.md) - Migration guide
- [examples/production_deployment.rs](examples/production_deployment.rs) - Full example
