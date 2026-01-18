//! Comprehensive v9 Production Integration Example
//!
//! This example demonstrates how to integrate all v9 enterprise features
//! into a production-ready message processing system:
//!
//! - Backpressure Management - Prevent broker overload
//! - Poison Message Detection - Identify failing messages
//! - Advanced Routing - Route messages intelligently
//! - Performance Optimization - Monitor and tune performance
//!
//! This simulates a real-world high-throughput message processing pipeline
//! with automatic tuning, error handling, and monitoring.
//!
//! Run with: `cargo run --example v9_production_integration`

use celers_broker_amqp::{
    backpressure::{BackpressureAction, BackpressureConfig, BackpressureManager},
    optimization::{OptimizationConfig, PerformanceOptimizer},
    poison_detector::{PoisonDetector, PoisonDetectorConfig},
    router::{MessageRouter, RouteBuilder, RouteCondition, RoutingStrategy},
};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Production message processor integrating all v9 features
struct ProductionMessageProcessor {
    backpressure: BackpressureManager,
    poison_detector: PoisonDetector,
    router: MessageRouter,
    optimizer: PerformanceOptimizer,
    stats: ProcessingStats,
}

#[derive(Debug, Default)]
struct ProcessingStats {
    total_processed: u64,
    successful: u64,
    failed: u64,
    poisoned: u64,
    backpressure_triggers: u64,
    route_distribution: HashMap<String, u64>,
}

impl ProductionMessageProcessor {
    /// Create a new production message processor with optimal configuration
    fn new() -> Self {
        // Configure backpressure management
        let backpressure_config = BackpressureConfig {
            max_queue_depth: 50_000,
            warning_threshold: 0.6,
            critical_threshold: 0.85,
            check_interval: Duration::from_secs(10),
            min_prefetch: 5,
            max_prefetch: 200,
            prefetch_adjustment_factor: 0.15,
        };

        // Configure poison message detection
        let poison_config = PoisonDetectorConfig {
            max_retries: 5,
            retry_window: Duration::from_secs(600),
            failure_rate_threshold: 0.75,
            min_samples: 10,
        };

        // Configure performance optimization
        let optimization_config = OptimizationConfig {
            target_latency_ms: 50.0,
            target_throughput: 5000.0,
            max_memory_bytes: 2 * 1024 * 1024 * 1024, // 2GB
            sample_window: 200,
            auto_tune: true,
        };

        // Setup message router with intelligent routing
        let mut router = MessageRouter::with_strategy("default_queue", RoutingStrategy::FirstMatch);

        // High priority messages go to priority queue
        let priority_rule = RouteBuilder::new("high_priority")
            .condition(RouteCondition::PriorityRange { min: 8, max: 9 })
            .to_queue("priority_queue")
            .with_weight(1.0)
            .build()
            .expect("Failed to build priority rule");
        router.add_rule(priority_rule);

        // Large messages go to dedicated queue
        let large_rule = RouteBuilder::new("large_messages")
            .condition(RouteCondition::PayloadSize {
                min: Some(10240), // 10KB
                max: None,
            })
            .to_queue("large_message_queue")
            .with_weight(1.0)
            .build()
            .expect("Failed to build large message rule");
        router.add_rule(large_rule);

        // Error/alert messages go to alert queue
        let alert_rule = RouteBuilder::new("alerts")
            .condition(RouteCondition::PayloadContains("ALERT".to_string()))
            .to_queue("alert_queue")
            .with_weight(1.0)
            .build()
            .expect("Failed to build alert rule");
        router.add_rule(alert_rule);

        Self {
            backpressure: BackpressureManager::new(backpressure_config),
            poison_detector: PoisonDetector::new(poison_config),
            router,
            optimizer: PerformanceOptimizer::new(optimization_config),
            stats: ProcessingStats::default(),
        }
    }

    /// Process a single message with full monitoring and control
    fn process_message(
        &mut self,
        message_id: &str,
        payload: &[u8],
        priority: Option<u8>,
        current_queue_depth: usize,
    ) -> Result<ProcessingResult, ProcessingError> {
        let start_time = Instant::now();

        // Update queue depth for backpressure management
        self.backpressure
            .update_queue_depth(current_queue_depth as u64);

        // Check if we should apply backpressure
        if self.backpressure.should_apply_backpressure() {
            self.stats.backpressure_triggers += 1;
            let action = self.backpressure.get_recommended_action();

            match action {
                BackpressureAction::Continue => {
                    // Continue processing but monitor closely
                }
                BackpressureAction::ReduceRate { .. }
                | BackpressureAction::PauseConsumption { .. } => {
                    return Err(ProcessingError::Backpressure {
                        action,
                        queue_depth: current_queue_depth,
                        utilization: self.backpressure.queue_utilization(),
                    });
                }
            }
        }

        // Check if this is a known poison message
        if self.poison_detector.is_poison(message_id) {
            self.stats.poisoned += 1;
            return Err(ProcessingError::PoisonMessage {
                message_id: message_id.to_string(),
            });
        }

        // Route the message to appropriate queue
        let target_queue = self.router.route(payload, priority);
        *self
            .stats
            .route_distribution
            .entry(target_queue.clone())
            .or_insert(0) += 1;

        // Simulate message processing
        let processing_result = self.simulate_processing(message_id, payload, &target_queue);

        // Record the processing latency
        let latency_ms = start_time.elapsed().as_secs_f64() * 1000.0;
        self.optimizer.record_latency(latency_ms);

        // Update stats and poison detector based on result
        match processing_result {
            Ok(_) => {
                self.poison_detector.record_success(message_id);
                self.stats.successful += 1;
                self.stats.total_processed += 1;
                Ok(ProcessingResult::Success {
                    target_queue,
                    latency_ms,
                })
            }
            Err(error) => {
                self.poison_detector
                    .record_failure(message_id, &error.to_string());
                self.stats.failed += 1;
                self.stats.total_processed += 1;
                Err(error)
            }
        }
    }

    /// Simulate message processing (in real code, this would do actual work)
    fn simulate_processing(
        &self,
        message_id: &str,
        payload: &[u8],
        _target_queue: &str,
    ) -> Result<(), ProcessingError> {
        // Simulate occasional failures
        if message_id.ends_with("fail") || payload.is_empty() {
            return Err(ProcessingError::ProcessingFailed {
                reason: "Simulated failure".to_string(),
            });
        }

        // Simulate processing time
        std::thread::sleep(Duration::from_micros(100));

        Ok(())
    }

    /// Get current system health status
    fn health_status(&mut self) -> HealthStatus {
        let backpressure_level = self.backpressure.calculate_backpressure_level();
        let poison_analytics = self.poison_detector.analytics();
        let performance_score = self.optimizer.performance_score();
        let is_meeting_targets = self.optimizer.is_meeting_targets();

        HealthStatus {
            backpressure_level,
            queue_utilization: self.backpressure.queue_utilization(),
            poison_count: poison_analytics.poison_count,
            failure_rate: poison_analytics.failure_rate,
            performance_score,
            meeting_targets: is_meeting_targets,
        }
    }

    /// Get optimization recommendations
    fn get_optimization_recommendations(&self) -> Vec<String> {
        let recommendations = self.optimizer.get_recommendations();
        recommendations
            .iter()
            .map(|rec| {
                format!(
                    "[{:?}] {}: Current={:.1}, Recommended={:.1}, Improvement={:.1}%",
                    rec.category,
                    rec.recommendation,
                    rec.current_value,
                    rec.recommended_value,
                    rec.expected_improvement
                )
            })
            .collect()
    }
}

#[allow(dead_code)]
#[derive(Debug)]
enum ProcessingResult {
    Success {
        target_queue: String,
        latency_ms: f64,
    },
}

#[derive(Debug, thiserror::Error)]
enum ProcessingError {
    #[error("Backpressure triggered: {action:?}, Queue depth: {queue_depth}, Utilization: {utilization:.1}%")]
    Backpressure {
        action: BackpressureAction,
        queue_depth: usize,
        utilization: f64,
    },
    #[error("Poison message detected: {message_id}")]
    PoisonMessage { message_id: String },
    #[error("Processing failed: {reason}")]
    ProcessingFailed { reason: String },
}

#[derive(Debug)]
struct HealthStatus {
    backpressure_level: celers_broker_amqp::backpressure::BackpressureLevel,
    queue_utilization: f64,
    poison_count: u64,
    failure_rate: f64,
    performance_score: f64,
    meeting_targets: bool,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== v9 Production Integration Example ===\n");
    println!("Simulating high-throughput message processing with");
    println!("full monitoring, backpressure, poison detection, and optimization.\n");

    let mut processor = ProductionMessageProcessor::new();

    // Simulate message processing workload
    println!("Processing message workload...\n");

    // Simulate 10,000 messages with varying characteristics
    for batch in 0..10 {
        println!("--- Batch {} ---", batch + 1);

        // Simulate queue depth variation
        let base_queue_depth = 5000 + batch * 3000;

        for i in 0..1000 {
            let message_id = format!("msg-{}-{}", batch, i);
            let mut payload = format!("Message data batch {} item {}", batch, i).into_bytes();

            // Some messages are large
            if i % 100 == 0 {
                payload.extend(vec![0u8; 15000]); // Make it >10KB
            }

            // Some messages have high priority
            let priority = if i % 50 == 0 { Some(9) } else { Some(5) };

            // Some messages are alerts
            if i % 75 == 0 {
                payload = b"ALERT: System anomaly detected".to_vec();
            }

            // Some messages will fail
            let message_id = if i % 500 == 0 {
                format!("{}-fail", message_id)
            } else {
                message_id
            };

            // Simulate increasing queue depth
            let queue_depth = base_queue_depth + (i * 2);

            // Process the message
            match processor.process_message(&message_id, &payload, priority, queue_depth) {
                Ok(_) => {}
                Err(ProcessingError::Backpressure { .. }) => {
                    // In production, would slow down or pause consumption
                }
                Err(ProcessingError::PoisonMessage { .. }) => {
                    // In production, would route to DLQ
                }
                Err(ProcessingError::ProcessingFailed { .. }) => {
                    // Normal failure, will retry
                }
            }

            // Record throughput
            if i % 100 == 0 {
                processor.optimizer.record_throughput(1000.0);
            }
        }

        // Print health status after each batch
        let health = processor.health_status();
        println!("Health Status:");
        println!(
            "  Backpressure: {:?} ({:.1}% utilization)",
            health.backpressure_level,
            health.queue_utilization * 100.0
        );
        println!("  Poison messages: {}", health.poison_count);
        println!("  Failure rate: {:.2}%", health.failure_rate * 100.0);
        println!(
            "  Performance score: {:.1}%",
            health.performance_score * 100.0
        );
        println!(
            "  Meeting targets: {}",
            if health.meeting_targets { "✓" } else { "✗" }
        );
        println!();
    }

    // Final statistics
    println!("\n=== Final Statistics ===");
    println!("Total processed: {}", processor.stats.total_processed);
    println!(
        "Successful: {} ({:.1}%)",
        processor.stats.successful,
        (processor.stats.successful as f64 / processor.stats.total_processed as f64) * 100.0
    );
    println!(
        "Failed: {} ({:.1}%)",
        processor.stats.failed,
        (processor.stats.failed as f64 / processor.stats.total_processed as f64) * 100.0
    );
    println!("Poisoned: {}", processor.stats.poisoned);
    println!(
        "Backpressure triggers: {}",
        processor.stats.backpressure_triggers
    );

    println!("\nRoute Distribution:");
    for (queue, count) in &processor.stats.route_distribution {
        println!(
            "  {}: {} messages ({:.1}%)",
            queue,
            count,
            (*count as f64 / processor.stats.total_processed as f64) * 100.0
        );
    }

    // Performance metrics
    let metrics = processor.optimizer.metrics();
    println!("\nPerformance Metrics:");
    println!(
        "  Avg latency: {:.2}ms (p95: {:.2}ms, p99: {:.2}ms)",
        metrics.avg_latency_ms, metrics.p95_latency_ms, metrics.p99_latency_ms
    );
    println!("  Avg throughput: {:.0} msg/s", metrics.avg_throughput);
    println!("  Peak throughput: {:.0} msg/s", metrics.peak_throughput);

    // Optimization recommendations
    let recommendations = processor.get_optimization_recommendations();
    if !recommendations.is_empty() {
        println!("\nOptimization Recommendations:");
        for rec in recommendations {
            println!("  {}", rec);
        }
    }

    // Optimal settings
    let optimal_prefetch = processor.optimizer.calculate_optimal_prefetch();
    let optimal_batch = processor.optimizer.calculate_optimal_batch_size();

    println!("\nRecommended Settings:");
    println!("  Optimal prefetch: {}", optimal_prefetch);
    println!("  Optimal batch size: {}", optimal_batch);

    println!("\n=== Production Integration Demo Complete ===");

    Ok(())
}
