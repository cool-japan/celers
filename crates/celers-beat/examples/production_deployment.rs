/// Production Deployment Example
///
/// This example demonstrates best practices for deploying celers-beat in production:
/// - Graceful shutdown with signal handling
/// - State persistence across restarts
/// - Health check endpoint pattern
/// - Structured logging
/// - Error recovery
/// - Metrics collection
/// - Alert callbacks
/// - Lock management for distributed deployments
///
/// Run with: cargo run --example production_deployment --features cron
use celers_beat::{AlertConfig, AlertLevel, BeatScheduler, Schedule, ScheduleError, ScheduledTask};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::signal;
use tokio::sync::RwLock;

/// Application state for production deployment
struct AppState {
    scheduler: Arc<RwLock<BeatScheduler>>,
    shutdown: Arc<AtomicBool>,
    health_check_failures: Arc<AtomicBool>,
}

impl AppState {
    fn new() -> Self {
        let state_file = "/tmp/production_scheduler.json";

        // Try to load existing state, create new if doesn't exist
        let scheduler = match BeatScheduler::load_from_file(state_file) {
            Ok(s) => {
                eprintln!(
                    "[STARTUP] Loaded existing scheduler state from {}",
                    state_file
                );
                s
            }
            Err(_) => {
                eprintln!("[STARTUP] Creating new scheduler with persistence");
                BeatScheduler::with_persistence(state_file.to_string())
            }
        };

        Self {
            scheduler: Arc::new(RwLock::new(scheduler)),
            shutdown: Arc::new(AtomicBool::new(false)),
            health_check_failures: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Check if shutdown has been requested
    fn is_shutting_down(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }

    /// Signal shutdown
    fn signal_shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
    }

    /// Mark health check as failed
    fn mark_unhealthy(&self) {
        self.health_check_failures.store(true, Ordering::Relaxed);
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("=== Production Scheduler Deployment ===\n");

    // Initialize application state
    let app_state = Arc::new(AppState::new());

    // Setup graceful shutdown handler
    let shutdown_state = app_state.clone();
    tokio::spawn(async move {
        if let Err(e) = signal::ctrl_c().await {
            eprintln!("[ERROR] Failed to listen for shutdown signal: {}", e);
            return;
        }
        eprintln!("\n[SHUTDOWN] Received shutdown signal, initiating graceful shutdown...");
        shutdown_state.signal_shutdown();
    });

    // Setup alert callbacks for production monitoring
    setup_alert_callbacks(&app_state).await?;

    // Initialize scheduler with production tasks
    initialize_production_tasks(&app_state).await?;

    // Set instance ID for distributed deployment
    {
        let mut scheduler = app_state.scheduler.write().await;
        let instance_id = std::env::var("SCHEDULER_INSTANCE_ID")
            .unwrap_or_else(|_| format!("scheduler-{}", std::process::id()));
        scheduler.set_instance_id(instance_id.clone());
        eprintln!("[STARTUP] Instance ID: {}", instance_id);
    }

    // Start health check background task
    let health_check_state = app_state.clone();
    tokio::spawn(async move {
        health_check_loop(health_check_state).await;
    });

    // Start metrics collection background task
    let metrics_state = app_state.clone();
    tokio::spawn(async move {
        metrics_collection_loop(metrics_state).await;
    });

    // Start alert monitoring background task
    let alert_state = app_state.clone();
    tokio::spawn(async move {
        alert_monitoring_loop(alert_state).await;
    });

    // Main scheduler loop
    eprintln!("[STARTUP] Starting main scheduler loop\n");
    match run_scheduler_loop(&app_state).await {
        Ok(_) => eprintln!("[SHUTDOWN] Scheduler stopped gracefully"),
        Err(e) => eprintln!("[ERROR] Scheduler error: {}", e),
    }

    // Final state save before shutdown
    {
        let scheduler = app_state.scheduler.read().await;
        if let Err(e) = scheduler.save_state() {
            eprintln!("[ERROR] Failed to save final state: {}", e);
        } else {
            eprintln!("[SHUTDOWN] Final state saved successfully");
        }
    }

    eprintln!("[SHUTDOWN] Shutdown complete");
    Ok(())
}

/// Setup alert callbacks for production monitoring
async fn setup_alert_callbacks(app_state: &Arc<AppState>) -> Result<(), ScheduleError> {
    let mut scheduler = app_state.scheduler.write().await;

    // Alert callback for logging and external notification
    scheduler.on_alert(Arc::new(|alert| {
        let level_str = match alert.level {
            AlertLevel::Critical => "CRITICAL",
            AlertLevel::Warning => "WARNING",
            AlertLevel::Info => "INFO",
        };

        eprintln!(
            "[ALERT][{}] Task: {} | Condition: {:?} | Message: {}",
            level_str, alert.task_name, alert.condition, alert.message
        );

        // In production, you would send this to:
        // - PagerDuty, OpsGenie, or other alerting services
        // - Slack/Teams webhooks
        // - Email notifications
        // - Metrics systems (Prometheus, DataDog, etc.)
    }));

    // Failure callback for task execution failures
    scheduler.on_failure(Arc::new(|task_name, error| {
        eprintln!("[FAILURE] Task '{}' failed: {}", task_name, error);
        // Log to external logging system (ELK, Splunk, etc.)
    }));

    Ok(())
}

/// Initialize production tasks with proper configuration
async fn initialize_production_tasks(app_state: &Arc<AppState>) -> Result<(), ScheduleError> {
    let mut scheduler = app_state.scheduler.write().await;

    // Critical: Database backup task (high priority, strict monitoring)
    let mut db_backup = ScheduledTask::new(
        "database_backup".to_string(),
        #[cfg(feature = "cron")]
        Schedule::crontab_tz("0", "2", "*", "*", "*", "UTC"), // 2 AM UTC daily
        #[cfg(not(feature = "cron"))]
        Schedule::interval(86400), // Daily if cron feature not available
    )
    .with_alert_config(AlertConfig {
        enabled: true,
        consecutive_failures_threshold: 2, // Alert after 2 failures
        failure_rate_threshold: 0.3,       // 30% failure rate threshold
        slow_execution_threshold_ms: Some(300_000), // 5 minutes
        alert_on_missed_schedule: true,
        alert_on_stuck: true,
    });
    db_backup.options.priority = Some(9); // Highest priority
    db_backup.options.queue = Some("critical".to_string());

    scheduler.add_task(db_backup)?;
    eprintln!("[INIT] Registered: database_backup (priority: 9, queue: critical)");

    // High Priority: Log rotation (important for disk space)
    let mut log_rotation = ScheduledTask::new(
        "log_rotation".to_string(),
        Schedule::interval(3600), // Every hour
    )
    .with_alert_config(AlertConfig {
        enabled: true,
        consecutive_failures_threshold: 3,
        failure_rate_threshold: 0.5,
        slow_execution_threshold_ms: Some(60_000), // 1 minute
        alert_on_missed_schedule: true,
        alert_on_stuck: true,
    });
    log_rotation.options.priority = Some(7);

    scheduler.add_task(log_rotation)?;
    eprintln!("[INIT] Registered: log_rotation (priority: 7)");

    // Normal Priority: Send reports
    let send_report = ScheduledTask::new(
        "send_daily_report".to_string(),
        #[cfg(feature = "cron")]
        Schedule::crontab_tz("0", "9", "1-5", "*", "*", "America/New_York"), // 9 AM ET weekdays
        #[cfg(not(feature = "cron"))]
        Schedule::interval(86400), // Daily
    )
    .with_alert_config(AlertConfig {
        enabled: true,
        consecutive_failures_threshold: 3,
        failure_rate_threshold: 0.5,
        slow_execution_threshold_ms: Some(120_000), // 2 minutes
        alert_on_missed_schedule: true,
        alert_on_stuck: false,
    });

    scheduler.add_task(send_report)?;
    eprintln!("[INIT] Registered: send_daily_report (priority: 5)");

    // Low Priority: Cleanup tasks
    let mut cleanup = ScheduledTask::new(
        "cleanup_temp_files".to_string(),
        Schedule::interval(7200), // Every 2 hours
    );
    cleanup.options.priority = Some(2);
    cleanup.options.expires = Some(3600); // Expire after 1 hour if not processed

    scheduler.add_task(cleanup)?;
    eprintln!("[INIT] Registered: cleanup_temp_files (priority: 2, expires: 3600s)");

    // Validate all schedules
    let health_results = scheduler.check_all_tasks_health();
    let unhealthy = health_results
        .iter()
        .filter(|r| r.health.is_unhealthy())
        .count();
    if unhealthy > 0 {
        eprintln!(
            "[WARNING] Found {} unhealthy tasks during initialization",
            unhealthy
        );
    } else {
        eprintln!("[INIT] All tasks validated successfully");
    }

    Ok(())
}

/// Main scheduler loop with error recovery
async fn run_scheduler_loop(app_state: &Arc<AppState>) -> Result<(), Box<dyn std::error::Error>> {
    let mut tick_interval = tokio::time::interval(Duration::from_secs(1));
    let mut consecutive_errors = 0;
    const MAX_CONSECUTIVE_ERRORS: u32 = 10;

    loop {
        // Check for shutdown signal
        if app_state.is_shutting_down() {
            eprintln!("[SCHEDULER] Shutdown requested, exiting main loop");
            break;
        }

        tick_interval.tick().await;

        // Get due tasks with starvation prevention
        let due_task_info: Vec<(String, u8)> = {
            let scheduler = app_state.scheduler.read().await;
            scheduler
                .get_due_tasks_with_starvation_prevention(Some(60))
                .into_iter()
                .map(|task| (task.name.clone(), task.options.priority.unwrap_or(5)))
                .collect()
        };

        // Execute due tasks
        for (task_name, priority) in due_task_info {
            eprintln!(
                "[EXEC] Executing task: {} (priority: {})",
                task_name, priority
            );

            // In production, you would:
            // 1. Try to acquire lock (for distributed deployment)
            // 2. Send task to broker/queue
            // 3. Mark task as executed
            // 4. Update execution history

            // Simulate task execution tracking
            let mut scheduler = app_state.scheduler.write().await;
            scheduler.mark_task_run(&task_name)?;

            // Reset error counter on successful execution
            consecutive_errors = 0;
        }

        // Check for errors and implement backoff
        if consecutive_errors >= MAX_CONSECUTIVE_ERRORS {
            eprintln!(
                "[ERROR] Too many consecutive errors ({}), entering recovery mode",
                consecutive_errors
            );
            tokio::time::sleep(Duration::from_secs(10)).await;
            consecutive_errors = 0; // Reset after backoff
        }
    }

    Ok(())
}

/// Health check background task
async fn health_check_loop(app_state: Arc<AppState>) {
    let mut interval = tokio::time::interval(Duration::from_secs(30));

    loop {
        interval.tick().await;

        if app_state.is_shutting_down() {
            break;
        }

        let scheduler = app_state.scheduler.read().await;
        let health_results = scheduler.check_all_tasks_health();

        let unhealthy = health_results
            .iter()
            .filter(|r| r.health.is_unhealthy())
            .count();
        let warnings = health_results
            .iter()
            .filter(|r| r.health.has_warnings())
            .count();

        if unhealthy > 0 {
            eprintln!("[HEALTH] WARNING: {} unhealthy tasks detected", unhealthy);
            app_state.mark_unhealthy();
        } else if warnings > 0 {
            eprintln!("[HEALTH] INFO: {} tasks have warnings", warnings);
        }

        // In production, expose this via HTTP endpoint:
        // GET /health -> 200 OK if healthy, 503 Service Unavailable if unhealthy
    }
}

/// Metrics collection background task
async fn metrics_collection_loop(app_state: Arc<AppState>) {
    let mut interval = tokio::time::interval(Duration::from_secs(60));

    loop {
        interval.tick().await;

        if app_state.is_shutting_down() {
            break;
        }

        let scheduler = app_state.scheduler.read().await;
        let stats = scheduler.get_comprehensive_stats();

        eprintln!("\n[METRICS] ===== Scheduler Statistics =====");
        eprintln!("[METRICS] Total tasks: {}", stats.total_tasks);
        eprintln!("[METRICS] Enabled tasks: {}", stats.enabled_tasks);
        eprintln!("[METRICS] Total executions: {}", stats.total_executions);
        eprintln!("[METRICS] Success rate: {:.2}%", stats.success_rate * 100.0);
        eprintln!(
            "[METRICS] Health: {} healthy, {} warnings, {} unhealthy",
            stats.healthy_tasks, stats.warning_tasks, stats.unhealthy_tasks
        );

        if let Some(avg_duration) = stats.avg_duration_ms {
            eprintln!("[METRICS] Avg execution time: {}ms", avg_duration);
        }

        // In production, export these to:
        // - Prometheus (via /metrics endpoint)
        // - DataDog, New Relic, etc.
        // - CloudWatch, Stackdriver, etc.
    }
}

/// Alert monitoring background task
async fn alert_monitoring_loop(app_state: Arc<AppState>) {
    let mut interval = tokio::time::interval(Duration::from_secs(60));

    loop {
        interval.tick().await;

        if app_state.is_shutting_down() {
            break;
        }

        let mut scheduler = app_state.scheduler.write().await;

        // Check all tasks for alert conditions
        scheduler.check_all_alerts();

        // Check for missed schedules
        let missed_count = scheduler.check_missed_schedules(Some(60));
        if missed_count > 0 {
            eprintln!("[ALERT] Detected {} missed schedules", missed_count);
        }

        // Get critical alerts
        let critical_alerts = scheduler.get_critical_alerts();
        if !critical_alerts.is_empty() {
            eprintln!("[ALERT] {} critical alerts active", critical_alerts.len());
            for alert in critical_alerts.iter().take(5) {
                eprintln!("[ALERT]   - {}: {}", alert.task_name, alert.message);
            }
        }
    }
}
