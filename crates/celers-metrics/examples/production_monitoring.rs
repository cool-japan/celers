//! Comprehensive production monitoring example
//!
//! Demonstrates a complete production monitoring setup with:
//! - Real-time metrics tracking
//! - Health checks and alerting
//! - Auto-scaling recommendations
//! - Trend analysis and forecasting
//! - Cost estimation
//! - Performance profiling
//!
//! Run with: cargo run --example production_monitoring

use celers_metrics::*;
use std::thread;
use std::time::Duration;

fn main() {
    println!("=== CeleRS Production Monitoring Demo ===\n");

    // Initialize monitoring infrastructure
    let monitor = ProductionMonitor::new();

    // Simulate production workload
    println!("Starting production workload simulation...\n");

    // Simulate 10 seconds of production traffic
    for iteration in 1..=10 {
        println!("--- Iteration {} ---", iteration);

        // Simulate varying load
        simulate_workload(iteration);

        // Collect metrics and analyze
        monitor.collect_and_analyze();

        thread::sleep(Duration::from_secs(1));
    }

    // Final report
    monitor.generate_final_report();
}

struct ProductionMonitor {
    health_config: HealthCheckConfig,
    alert_manager: AlertManager,
    cost_config: CostConfig,
    autoscaling_config: AutoScalingConfig,
    queue_size_history: MetricHistory,
    error_rate_history: MetricHistory,
}

impl ProductionMonitor {
    fn new() -> Self {
        // Configure health checks
        let health_config = HealthCheckConfig {
            max_queue_size: 1000.0,
            max_dlq_size: 50.0,
            min_active_workers: 2.0,
            slo_target: Some(SloTarget {
                success_rate: 0.99,
                latency_seconds: 5.0,
                throughput: 100.0,
            }),
        };

        // Configure alerts
        let mut alert_manager = AlertManager::new();
        alert_manager.add_rule(AlertRule::new(
            "critical_error_rate",
            AlertCondition::ErrorRateAbove { threshold: 0.05 },
            AlertSeverity::Critical,
            "Error rate exceeded 5% - immediate action required",
        ));
        alert_manager.add_rule(AlertRule::new(
            "queue_backlog",
            AlertCondition::GaugeAbove { threshold: 500.0 },
            AlertSeverity::Warning,
            "Queue backlog detected - consider scaling",
        ));
        alert_manager.add_rule(AlertRule::new(
            "low_workers",
            AlertCondition::GaugeBelow { threshold: 2.0 },
            AlertSeverity::Critical,
            "Worker count below minimum - service degradation likely",
        ));

        // Configure cost tracking
        let cost_config = CostConfig {
            cost_per_worker_hour: 0.10,
            cost_per_million_tasks: 1000.0,
            cost_per_gb: 0.09,
        };

        // Configure auto-scaling
        let autoscaling_config = AutoScalingConfig {
            target_queue_per_worker: 10.0,
            min_workers: 2,
            max_workers: 20,
            scale_up_threshold: 0.8,
            scale_down_threshold: 0.3,
            cooldown_seconds: 60,
        };

        // Initialize metric histories
        let queue_size_history = MetricHistory::new(100);
        let error_rate_history = MetricHistory::new(100);

        Self {
            health_config,
            alert_manager,
            cost_config,
            autoscaling_config,
            queue_size_history,
            error_rate_history,
        }
    }

    fn collect_and_analyze(&self) {
        // Capture current metrics
        let metrics = CurrentMetrics::capture();

        // Record history for trend analysis
        self.queue_size_history.record(metrics.queue_size);
        self.error_rate_history.record(metrics.error_rate());

        // Health check
        let health = health_check(&self.health_config);
        self.print_health_status(&health);

        // Alert check
        let alerts = self.alert_manager.check_alerts(&metrics);
        if !alerts.is_empty() {
            println!("  Alerts fired: {}", alerts.len());
            for alert in alerts {
                println!("    [{:?}] {}", alert.severity, alert.description);
            }
        }

        // Auto-scaling recommendation
        let scaling = recommend_scaling(&self.autoscaling_config);
        self.print_scaling_recommendation(&scaling);

        // Trend analysis
        if self.queue_size_history.len() >= 3 {
            if let Some(trend) = self.queue_size_history.trend() {
                if trend > 5.0 {
                    println!("  Queue growing rapidly: +{:.1}/sec", trend);
                } else if trend < -5.0 {
                    println!("  Queue shrinking: {:.1}/sec", trend);
                }
            }
        }

        // Cost estimation (for 1 hour)
        let cost_estimate = estimate_costs(&self.cost_config, 1.0);
        println!(
            "  Cost estimate: ${:.4}/hr (${:.2}/day)",
            cost_estimate.total_cost,
            cost_estimate.total_cost * 24.0
        );

        println!();
    }

    fn print_health_status(&self, health: &HealthStatus) {
        match health {
            HealthStatus::Healthy => {
                println!("  Health: ✓ HEALTHY");
            }
            HealthStatus::Degraded { reasons } => {
                println!("  Health: ⚠ DEGRADED");
                for reason in reasons {
                    println!("    - {}", reason);
                }
            }
            HealthStatus::Unhealthy { reasons } => {
                println!("  Health: ✗ UNHEALTHY");
                for reason in reasons {
                    println!("    - {}", reason);
                }
            }
        }
    }

    fn print_scaling_recommendation(&self, scaling: &ScalingRecommendation) {
        match scaling {
            ScalingRecommendation::ScaleUp { workers, reason } => {
                println!("  Scaling: ⬆ Scale up by {} workers ({})", workers, reason);
            }
            ScalingRecommendation::ScaleDown { workers, reason } => {
                println!(
                    "  Scaling: ⬇ Scale down by {} workers ({})",
                    workers, reason
                );
            }
            ScalingRecommendation::NoChange => {
                // Don't print anything for no change
            }
        }
    }

    fn generate_final_report(&self) {
        println!("\n=== Final Production Report ===\n");

        // Overall metrics summary
        println!("{}", generate_metric_summary());

        // Historical analysis
        if self.queue_size_history.len() >= 5 {
            println!("\n=== Queue Size Analysis ===");
            let queue_snapshot = self.queue_size_history.snapshot();
            println!("  Min: {:.0}", queue_snapshot.min);
            println!("  Max: {:.0}", queue_snapshot.max);
            println!("  Mean: {:.1}", queue_snapshot.mean);
            println!("  Std Dev: {:.1}", queue_snapshot.std_dev);
            if let Some(trend) = queue_snapshot.trend {
                println!("  Trend: {:.2}/sec", trend);
            }

            // Forecast next 60 seconds
            if let Some(forecast) = forecast_metric(&self.queue_size_history, 60) {
                println!("\n  Forecast (60s):");
                println!("    Predicted queue size: {:.0}", forecast.predicted_value);
                println!("    Confidence: {:.1}%", forecast.confidence * 100.0);
                println!("    Trend: {:?}", forecast.trend);
            }
        }

        // Error rate analysis
        if self.error_rate_history.len() >= 5 {
            println!("\n=== Error Rate Analysis ===");
            let error_snapshot = self.error_rate_history.snapshot();
            println!("  Mean error rate: {:.2}%", error_snapshot.mean * 100.0);
            println!("  Peak error rate: {:.2}%", error_snapshot.max * 100.0);

            // Check if error rate is trending upward
            if let Some(trend) = error_snapshot.trend {
                if trend > 0.01 {
                    println!("  ⚠ Error rate trending upward!");
                }
            }
        }

        // Final health check
        let final_health = health_check(&self.health_config);
        println!("\n=== Final Health Status ===");
        self.print_health_status(&final_health);

        // Cost optimization recommendations
        let cost_opt_config = CostOptimizationConfig::default();
        let optimizations = recommend_cost_optimizations(&cost_opt_config);

        if !optimizations.is_empty() {
            println!("\n=== Cost Optimization Recommendations ===");
            for (i, opt) in optimizations.iter().enumerate() {
                match opt {
                    CostOptimization::ScaleDown {
                        current_workers,
                        recommended_workers,
                        estimated_savings,
                    } => {
                        println!(
                            "  {}. Scale down {} → {} workers (save ${:.2}/month)",
                            i + 1,
                            current_workers,
                            recommended_workers,
                            estimated_savings
                        );
                    }
                    CostOptimization::IncreaseBatching {
                        current_batch_size,
                        recommended_batch_size,
                        cost_reduction_percent,
                    } => {
                        println!(
                            "  {}. Increase batch size {:.1} → {} ({:.1}% cost reduction)",
                            i + 1,
                            current_batch_size,
                            recommended_batch_size,
                            cost_reduction_percent
                        );
                    }
                    CostOptimization::UseSpotInstances {
                        estimated_savings,
                        cost_reduction_percent,
                    } => {
                        println!(
                            "  {}. Use spot instances (save ${:.2}/month, {:.1}% reduction)",
                            i + 1,
                            estimated_savings,
                            cost_reduction_percent
                        );
                    }
                    CostOptimization::OptimizePolling {
                        recommended_poll_interval,
                        cost_reduction_percent,
                    } => {
                        println!(
                            "  {}. Optimize polling to {}s interval ({:.1}% cost reduction)",
                            i + 1,
                            recommended_poll_interval,
                            cost_reduction_percent
                        );
                    }
                    CostOptimization::NoOptimizationNeeded => {
                        // Skip - system is already optimized
                    }
                }
            }
        }

        println!("\n=== Monitoring Complete ===");
    }
}

fn simulate_workload(iteration: usize) {
    // Vary the workload to simulate realistic production traffic
    let base_load = 10;
    let spike_factor = if iteration == 5 || iteration == 6 {
        3 // Simulate a traffic spike
    } else {
        1
    };

    let tasks_this_iteration = base_load * spike_factor;

    // Enqueue tasks
    TASKS_ENQUEUED_TOTAL.inc_by(tasks_this_iteration as f64);

    // Complete most tasks
    let completed = (tasks_this_iteration as f64 * 0.92) as usize;
    TASKS_COMPLETED_TOTAL.inc_by(completed as f64);

    // Some failures
    let failed = tasks_this_iteration - completed;
    TASKS_FAILED_TOTAL.inc_by(failed as f64);

    // Update queue size (accumulates during spike)
    let current_queue = QUEUE_SIZE.get();
    let new_queue = (current_queue + tasks_this_iteration as f64 - completed as f64).max(0.0);
    QUEUE_SIZE.set(new_queue);

    // Processing queue
    PROCESSING_QUEUE_SIZE.set(((tasks_this_iteration as f64) * 0.1).min(10.0));

    // DLQ grows slowly
    if failed > 0 {
        let dlq_increment = (failed as f64 * 0.3).max(1.0);
        let current_dlq = DLQ_SIZE.get();
        DLQ_SIZE.set(current_dlq + dlq_increment);
    }

    // Workers scale based on queue size
    let workers = if new_queue > 100.0 {
        10.0
    } else if new_queue > 50.0 {
        5.0
    } else {
        3.0
    };
    ACTIVE_WORKERS.set(workers);

    // Simulate execution times
    for _ in 0..completed.min(5) {
        TASK_EXECUTION_TIME.observe(0.5 + (iteration as f64 % 3.0) * 0.2);
    }

    // Simulate memory usage
    WORKER_MEMORY_USAGE_BYTES.set(1_000_000.0 * workers);
}
