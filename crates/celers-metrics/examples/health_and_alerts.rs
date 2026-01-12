//! Health checks and alerting example
//!
//! Demonstrates health monitoring and alert management.
//!
//! Run with: cargo run --example health_and_alerts

use celers_metrics::*;

fn main() {
    println!("=== CeleRS Health & Alerts Demo ===\n");

    // Configure health check thresholds
    let health_config = HealthCheckConfig {
        max_queue_size: 1000.0,
        max_dlq_size: 100.0,
        min_active_workers: 2.0,
        slo_target: Some(SloTarget {
            success_rate: 0.99,
            latency_seconds: 5.0,
            throughput: 100.0,
        }),
    };

    // Create alert manager
    let mut alerts = AlertManager::new();

    // Add alert rules
    alerts.add_rule(AlertRule::new(
        "high_error_rate",
        AlertCondition::ErrorRateAbove { threshold: 0.05 },
        AlertSeverity::Critical,
        "Error rate exceeded 5%",
    ));

    alerts.add_rule(AlertRule::new(
        "queue_backlog",
        AlertCondition::GaugeAbove { threshold: 500.0 },
        AlertSeverity::Warning,
        "Queue size exceeded 500 tasks",
    ));

    // Scenario 1: Healthy system
    println!("=== Scenario 1: Healthy System ===");
    reset_metrics();
    TASKS_ENQUEUED_TOTAL.inc_by(1000.0);
    TASKS_COMPLETED_TOTAL.inc_by(990.0);
    TASKS_FAILED_TOTAL.inc_by(10.0);
    QUEUE_SIZE.set(50.0);
    ACTIVE_WORKERS.set(5.0);

    check_system(&health_config, &alerts);

    // Scenario 2: High error rate
    println!("\n=== Scenario 2: High Error Rate ===");
    reset_metrics();
    TASKS_ENQUEUED_TOTAL.inc_by(1000.0);
    TASKS_COMPLETED_TOTAL.inc_by(900.0);
    TASKS_FAILED_TOTAL.inc_by(100.0);
    QUEUE_SIZE.set(100.0);
    ACTIVE_WORKERS.set(5.0);

    check_system(&health_config, &alerts);

    // Scenario 3: Queue backlog
    println!("\n=== Scenario 3: Queue Backlog ===");
    reset_metrics();
    TASKS_ENQUEUED_TOTAL.inc_by(2000.0);
    TASKS_COMPLETED_TOTAL.inc_by(1400.0);
    TASKS_FAILED_TOTAL.inc_by(10.0);
    QUEUE_SIZE.set(600.0);
    ACTIVE_WORKERS.set(5.0);

    check_system(&health_config, &alerts);
}

fn check_system(config: &HealthCheckConfig, alerts: &AlertManager) {
    // Check health
    let health = health_check(config);

    match &health {
        HealthStatus::Healthy => {
            println!("Health: ✓ HEALTHY");
        }
        HealthStatus::Degraded { reasons } => {
            println!("Health: ⚠ DEGRADED");
            for reason in reasons {
                println!("  - {}", reason);
            }
        }
        HealthStatus::Unhealthy { reasons } => {
            println!("Health: ✗ UNHEALTHY");
            for reason in reasons {
                println!("  - {}", reason);
            }
        }
    }

    // Check alerts
    let metrics = CurrentMetrics::capture();
    let fired = alerts.check_alerts(&metrics);

    if !fired.is_empty() {
        println!("\nAlerts ({} fired):", fired.len());
        for alert in &fired {
            println!(
                "  [{:?}] {}: {}",
                alert.severity, alert.name, alert.description
            );
        }
    } else {
        println!("\nAlerts: None fired");
    }

    // Show metrics
    println!("\nMetrics:");
    println!("  Success rate: {:.2}%", metrics.success_rate() * 100.0);
    println!("  Queue size: {}", metrics.queue_size);
    println!("  Workers: {}", metrics.active_workers);
}
