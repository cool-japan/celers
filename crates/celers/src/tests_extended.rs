#[allow(unused_imports)]
use super::*;

#[test]
fn test_convenience_critical() {
    use crate::convenience::critical;
    use serde_json::json;

    let sig = critical("process_payment", vec![json!({"amount": 100})]);
    assert_eq!(sig.task, "process_payment");
    assert_eq!(sig.options.priority, Some(9));
    assert_eq!(sig.options.max_retries, Some(5));
}

#[test]
fn test_convenience_best_effort() {
    use crate::convenience::best_effort;
    use serde_json::json;

    let sig = best_effort("update_cache", vec![json!({"key": "value"})]);
    assert_eq!(sig.task, "update_cache");
    assert_eq!(sig.options.priority, Some(1));
    assert_eq!(sig.options.max_retries, Some(0));
}

#[test]
fn test_convenience_transient() {
    use crate::convenience::transient;
    use serde_json::json;

    let sig = transient("temp_notification", vec![json!({"msg": "hi"})], 300);
    assert_eq!(sig.task, "temp_notification");
    assert_eq!(sig.options.expires, Some(300));
}

#[test]
fn test_convenience_retry_with_backoff() {
    use crate::convenience::retry_with_backoff;

    let opts = retry_with_backoff(5, 60);
    assert_eq!(opts.max_retries, Some(5));
    assert_eq!(opts.countdown, Some(60));
}

#[test]
fn test_convenience_pipeline() {
    use crate::convenience::pipeline;
    use serde_json::json;

    let workflow = pipeline()
        .then("fetch_data", vec![json!(1)])
        .then("process_data", vec![json!(2)])
        .then("save_results", vec![json!(3)]);

    assert_eq!(workflow.tasks.len(), 3);
}

#[test]
fn test_convenience_fan_out() {
    use crate::convenience::fan_out;
    use serde_json::json;

    let items = vec![json!(1), json!(2), json!(3)];
    let workflow = fan_out("process_item", items);

    assert_eq!(workflow.task.task, "process_item");
    assert_eq!(workflow.argsets.len(), 3);
}

#[test]
fn test_convenience_fan_in() {
    use crate::convenience::{fan_in, parallel, task};
    use serde_json::json;

    let tasks = parallel()
        .add("task1", vec![json!(1)])
        .add("task2", vec![json!(2)]);

    let callback = task("aggregate_results");
    let workflow = fan_in(tasks, callback);

    assert_eq!(workflow.header.tasks.len(), 2);
    assert_eq!(workflow.body.task, "aggregate_results");
}

// Tests for workflow templates
#[test]
fn test_workflow_template_etl_pipeline() {
    use crate::workflow_templates::etl_pipeline;
    use serde_json::json;

    let pipeline = etl_pipeline(
        "extract",
        vec![json!({"source": "db"})],
        "transform",
        "load",
    );

    assert_eq!(pipeline.tasks.len(), 3);
    assert_eq!(pipeline.tasks[0].task, "extract");
    assert_eq!(pipeline.tasks[1].task, "transform");
    assert_eq!(pipeline.tasks[2].task, "load");
}

#[test]
fn test_workflow_template_map_reduce() {
    use crate::workflow_templates::map_reduce_workflow;
    use serde_json::json;

    let workflow = map_reduce_workflow("process", vec![json!(1), json!(2), json!(3)], "aggregate");

    assert_eq!(workflow.header.tasks.len(), 3);
    assert_eq!(workflow.body.task, "aggregate");
}

#[test]
fn test_workflow_template_scatter_gather() {
    use crate::workflow_templates::scatter_gather;
    use serde_json::json;

    let tasks = vec![
        ("task1", vec![json!(1)]),
        ("task2", vec![json!(2)]),
        ("task3", vec![json!(3)]),
    ];

    let workflow = scatter_gather(tasks, "gather");

    assert_eq!(workflow.header.tasks.len(), 3);
    assert_eq!(workflow.body.task, "gather");
}

#[test]
fn test_workflow_template_batch_processing() {
    use crate::workflow_templates::batch_processing;
    use serde_json::json;

    let items: Vec<_> = (1..=25).map(|i| json!(i)).collect();
    let workflow = batch_processing("process_batch", items, 10, Some("aggregate"));

    // 25 items with batch_size 10 = 3 batches (10 + 10 + 5)
    assert_eq!(workflow.header.tasks.len(), 3);
    assert_eq!(workflow.body.task, "aggregate");
}

#[test]
fn test_workflow_template_sequential_pipeline() {
    use crate::workflow_templates::sequential_pipeline;
    use serde_json::json;

    let stages = vec![
        ("stage1", vec![json!(1)], 3),
        ("stage2", vec![json!(2)], 5),
        ("stage3", vec![json!(3)], 2),
    ];

    let pipeline = sequential_pipeline(stages);

    assert_eq!(pipeline.tasks.len(), 3);
    assert_eq!(pipeline.tasks[0].options.max_retries, Some(3));
    assert_eq!(pipeline.tasks[1].options.max_retries, Some(5));
    assert_eq!(pipeline.tasks[2].options.max_retries, Some(2));
}

#[test]
fn test_workflow_template_priority_workflow() {
    use crate::workflow_templates::priority_workflow;
    use serde_json::json;

    let tasks = vec![
        ("critical", vec![json!(1)], 9),
        ("normal", vec![json!(2)], 5),
        ("low", vec![json!(3)], 1),
    ];

    let workflow = priority_workflow(tasks);

    assert_eq!(workflow.tasks.len(), 3);
    assert_eq!(workflow.tasks[0].options.priority, Some(9));
    assert_eq!(workflow.tasks[1].options.priority, Some(5));
    assert_eq!(workflow.tasks[2].options.priority, Some(1));
}

// Tests for task composition
#[test]
fn test_task_composition_retry_wrapper() {
    use crate::task_composition::retry_wrapper;
    use serde_json::json;

    let sig = retry_wrapper("my_task", vec![json!(1)], 5, 10);

    assert_eq!(sig.task, "my_task");
    assert_eq!(sig.options.max_retries, Some(5));
    assert_eq!(sig.options.countdown, Some(10));
}

#[test]
fn test_task_composition_timeout_wrapper() {
    use crate::task_composition::timeout_wrapper;
    use serde_json::json;

    let sig = timeout_wrapper("long_task", vec![json!(1)], 300);

    assert_eq!(sig.task, "long_task");
    assert_eq!(sig.options.time_limit, Some(300));
}

#[test]
fn test_task_composition_circuit_breaker() {
    use crate::task_composition::circuit_breaker_group;
    use serde_json::json;

    let tasks = vec![("service_a", vec![json!(1)]), ("service_b", vec![json!(2)])];

    let group = circuit_breaker_group(tasks, 3);

    assert_eq!(group.tasks.len(), 2);
    assert_eq!(group.tasks[0].options.max_retries, Some(3));
    assert_eq!(group.tasks[1].options.max_retries, Some(3));
}

#[test]
fn test_task_composition_rate_limited() {
    use crate::task_composition::rate_limited_workflow;
    use serde_json::json;

    let items = vec![json!(1), json!(2), json!(3)];
    let workflow = rate_limited_workflow("api_call", items, 5);

    assert_eq!(workflow.tasks.len(), 3);
    assert_eq!(workflow.tasks[0].options.countdown, None); // First task has no delay
    assert_eq!(workflow.tasks[1].options.countdown, Some(5)); // 5 seconds delay
    assert_eq!(workflow.tasks[2].options.countdown, Some(10)); // 10 seconds delay
}

// Tests for prelude exports of new modules
#[test]
fn test_prelude_workflow_templates() {
    use crate::prelude::*;
    use serde_json::json;

    // Test that workflow template functions are available from prelude
    let _pipeline = etl_pipeline("extract", vec![json!(1)], "transform", "load");
    let _map_reduce = map_reduce_workflow("map", vec![json!(1)], "reduce");
    let _scatter = scatter_gather(vec![("t1", vec![json!(1)])], "gather");
    let _batch = batch_processing("process", vec![json!(1)], 5, None);
    let _seq = sequential_pipeline(vec![("s1", vec![json!(1)], 3)]);
    let _priority = priority_workflow(vec![("t1", vec![json!(1)], 9)]);
}

#[test]
fn test_prelude_task_composition() {
    use crate::prelude::*;
    use serde_json::json;

    // Test that task composition functions are available from prelude
    let _retry = retry_wrapper("task", vec![json!(1)], 5, 10);
    let _timeout = timeout_wrapper("task", vec![json!(1)], 300);
    let _circuit = circuit_breaker_group(vec![("t1", vec![json!(1)])], 3);
    let _rate = rate_limited_workflow("task", vec![json!(1)], 5);
}

// Tests for error recovery patterns
#[test]
fn test_error_recovery_with_fallback() {
    use crate::error_recovery::with_fallback;
    use serde_json::json;

    let chain = with_fallback(
        "primary_task",
        vec![json!(1)],
        "fallback_task",
        vec![json!(2)],
    );

    assert_eq!(chain.tasks.len(), 2);
    assert_eq!(chain.tasks[0].task, "primary_task");
    assert_eq!(chain.tasks[1].task, "fallback_task");
}

#[test]
fn test_error_recovery_ignore_errors() {
    use crate::error_recovery::ignore_errors;
    use serde_json::json;

    let sig = ignore_errors("non_critical_task", vec![json!(1)]);

    assert_eq!(sig.task, "non_critical_task");
    assert_eq!(sig.options.max_retries, Some(0));
    // Error suppression logic would be implemented in task handler
}

#[test]
fn test_error_recovery_exponential_backoff() {
    use crate::error_recovery::with_exponential_backoff;
    use serde_json::json;

    let sig = with_exponential_backoff("flaky_task", vec![json!(1)], 5, 2);

    assert_eq!(sig.task, "flaky_task");
    assert_eq!(sig.options.max_retries, Some(5));
    assert_eq!(sig.options.countdown, Some(2));
}

#[test]
fn test_error_recovery_with_dlq() {
    use crate::error_recovery::with_dlq;
    use serde_json::json;

    let chain = with_dlq("risky_task", vec![json!(1)], "dlq_handler");

    assert_eq!(chain.tasks.len(), 2);
    assert_eq!(chain.tasks[0].task, "risky_task");
    assert_eq!(chain.tasks[1].task, "dlq_handler");
}

// Tests for workflow validation
#[test]
fn test_workflow_validation_chain_valid() {
    use crate::workflow_validation::validate_chain;
    use crate::Chain;

    let chain = Chain::new().then("task1", vec![]).then("task2", vec![]);

    assert!(validate_chain(&chain).is_ok());
}

#[test]
fn test_workflow_validation_chain_empty() {
    use crate::workflow_validation::validate_chain;
    use crate::Chain;

    let chain = Chain::new();

    assert!(validate_chain(&chain).is_err());
}

#[test]
fn test_workflow_validation_group_valid() {
    use crate::workflow_validation::validate_group;
    use crate::Group;

    let group = Group::new().add("task1", vec![]).add("task2", vec![]);

    assert!(validate_group(&group).is_ok());
}

#[test]
fn test_workflow_validation_group_empty() {
    use crate::workflow_validation::validate_group;
    use crate::Group;

    let group = Group::new();

    assert!(validate_group(&group).is_err());
}

#[test]
fn test_workflow_validation_chord_valid() {
    use crate::workflow_validation::validate_chord;
    use crate::{Chord, Group, Signature};

    let chord = Chord {
        header: Group::new().add("task1", vec![]),
        body: Signature::new("callback".to_string()),
    };

    assert!(validate_chord(&chord).is_ok());
}

#[test]
fn test_workflow_validation_performance_concerns() {
    use crate::workflow_validation::check_performance_concerns_group;
    use crate::Group;

    let mut large_group = Group::new();
    for i in 0..150 {
        large_group = large_group.add(&format!("task_{}", i), vec![]);
    }

    let warnings = check_performance_concerns_group(&large_group);
    assert!(warnings.is_some());
}

// Tests for result helpers
#[test]
fn test_result_helpers_collector() {
    use crate::result_helpers::create_result_collector;

    let collector = create_result_collector("aggregate", 100);

    assert_eq!(collector.task, "aggregate");
    assert_eq!(collector.options.time_limit, Some(300));
}

#[test]
fn test_result_helpers_filter() {
    use crate::result_helpers::create_result_filter;
    use serde_json::json;

    let filter = create_result_filter("filter_task", json!({"status": "success"}));

    assert_eq!(filter.task, "filter_task");
    assert_eq!(filter.args.len(), 1);
}

#[test]
fn test_result_helpers_transformer() {
    use crate::result_helpers::create_result_transformer;
    use serde_json::json;

    let transformer = create_result_transformer("transform", json!({"format": "csv"}));

    assert_eq!(transformer.task, "transform");
    assert_eq!(transformer.args.len(), 1);
}

#[test]
fn test_result_helpers_reducer() {
    use crate::result_helpers::create_result_reducer;

    let reducer = create_result_reducer("reduce_task", "sum");

    assert_eq!(reducer.task, "reduce_task");
    assert_eq!(reducer.args.len(), 1);
}

// Tests for prelude exports of new modules
#[test]
fn test_prelude_error_recovery() {
    use crate::prelude::*;
    use serde_json::json;

    let _fallback = with_fallback("t1", vec![json!(1)], "t2", vec![json!(2)]);
    let _ignore = ignore_errors("t", vec![json!(1)]);
    let _backoff = with_exponential_backoff("t", vec![json!(1)], 5, 2);
    let _dlq = with_dlq("t", vec![json!(1)], "dlq");
}

#[test]
fn test_prelude_workflow_validation() {
    use crate::prelude::*;

    let chain = Chain::new().then("task", vec![]);
    let group = Group::new().add("task", vec![]);

    let _ = validate_chain(&chain);
    let _ = validate_group(&group);
    let _ = check_performance_concerns_chain(&chain);
    let _ = check_performance_concerns_group(&group);

    // Test WorkflowValidationError is available from prelude (aliased)
    let _error = WorkflowValidationError::new("test error");
}

#[test]
fn test_prelude_result_helpers() {
    use crate::prelude::*;
    use serde_json::json;

    let _collector = create_result_collector("collect", 10);
    let _filter = create_result_filter("filter", json!({}));
    let _transformer = create_result_transformer("transform", json!({}));
    let _reducer = create_result_reducer("reduce", "sum");
}

// Tests for advanced workflow patterns
#[test]
fn test_advanced_patterns_module_available() {
    // Verify advanced patterns module is accessible
    use crate::advanced_patterns::*;

    // Test conditional workflow helper
    let workflow =
        create_conditional_workflow("check", vec![], "success", vec![], "failure", vec![]);
    assert!(!workflow.tasks.is_empty());
}

#[test]
fn test_monitoring_helpers_available() {
    use crate::monitoring_helpers::*;

    let monitor = TaskMonitor::new();
    assert_eq!(monitor.total_tasks(), 0);

    monitor.record_success(100);
    assert_eq!(monitor.total_tasks(), 1);
    assert_eq!(monitor.successful_tasks(), 1);

    monitor.record_failure(200);
    assert_eq!(monitor.total_tasks(), 2);
    assert_eq!(monitor.failed_tasks(), 1);
}

#[test]
fn test_batch_helpers_available() {
    use crate::batch_helpers::*;
    use serde_json::json;

    let items = vec![json!(1), json!(2), json!(3), json!(4), json!(5)];
    let batches = create_dynamic_batches("process", items, 2);
    assert_eq!(batches.header.tasks.len(), 3); // 5 items / 2 = 3 batches
}

#[test]
fn test_advanced_patterns_dynamic_workflow() {
    use crate::advanced_patterns::create_dynamic_workflow;
    use serde_json::json;

    let workflow =
        create_dynamic_workflow("generator", vec![json!({"config": "test"})], "executor");
    assert_eq!(workflow.tasks.len(), 2);
}

#[test]
fn test_advanced_patterns_saga_workflow() {
    use crate::advanced_patterns::create_saga_workflow;
    use serde_json::json;

    let steps = vec![
        ("step1", vec![json!(1)], "compensate1", vec![json!(1)]),
        ("step2", vec![json!(2)], "compensate2", vec![json!(2)]),
    ];

    let workflow = create_saga_workflow(steps);
    assert_eq!(workflow.tasks.len(), 2);
}

#[test]
fn test_monitoring_average_time() {
    use crate::monitoring_helpers::TaskMonitor;

    let monitor = TaskMonitor::new();
    monitor.record_success(100);
    monitor.record_success(200);
    monitor.record_success(300);

    assert_eq!(monitor.average_execution_time_ms(), 200);
}

#[test]
fn test_monitoring_success_rate() {
    use crate::monitoring_helpers::TaskMonitor;

    let monitor = TaskMonitor::new();
    monitor.record_success(100);
    monitor.record_success(100);
    monitor.record_failure(100);

    assert!((monitor.success_rate() - 66.67).abs() < 0.1);
}

#[test]
fn test_batch_adaptive_batches() {
    use crate::batch_helpers::create_adaptive_batches;
    use serde_json::json;

    let items: Vec<_> = (1..=100).map(|i| json!(i)).collect();
    let workflow = create_adaptive_batches("process", items, 5, 20);

    // Should create batches with adaptive sizing
    assert!(!workflow.header.tasks.is_empty());
}

#[test]
fn test_batch_prioritized_batches() {
    use crate::batch_helpers::create_prioritized_batches;
    use serde_json::json;

    let high = vec![json!(1), json!(2)];
    let medium = vec![json!(3), json!(4)];
    let low = vec![json!(5), json!(6)];

    let group = create_prioritized_batches("process", (high, medium, low), 1);

    // Should have tasks with different priorities
    assert_eq!(group.tasks.len(), 6);
    assert_eq!(group.tasks[0].options.priority, Some(9));
    assert_eq!(group.tasks[2].options.priority, Some(5));
    assert_eq!(group.tasks[4].options.priority, Some(1));
}

// Health check utilities tests
#[test]
fn test_health_check_worker_health_checker() {
    use crate::health_check::{HealthStatus, WorkerHealthChecker};

    let checker = WorkerHealthChecker::default();

    // Should be healthy initially
    let result = checker.check_health();
    assert_eq!(result.status, HealthStatus::Healthy);
    assert!(checker.is_ready());
    assert!(checker.is_alive());

    // Record heartbeat and task processing
    checker.heartbeat();
    checker.task_processed();

    // Should still be healthy
    let result = checker.check_health();
    assert_eq!(result.status, HealthStatus::Healthy);
}

#[test]
fn test_health_check_dependency_checker() {
    use crate::health_check::{DependencyChecker, HealthCheckResult, HealthStatus};

    let checker = DependencyChecker::new("database", || {
        HealthCheckResult::healthy("Database is operational")
    });

    assert_eq!(checker.name(), "database");
    let result = checker.check();
    assert_eq!(result.status, HealthStatus::Healthy);
}

#[test]
fn test_health_check_result_builder() {
    use crate::health_check::{HealthCheckResult, HealthStatus};

    let result = HealthCheckResult::healthy("All systems operational")
        .with_metadata("uptime", "3600")
        .with_metadata("requests", "1000");

    assert_eq!(result.status, HealthStatus::Healthy);
    assert_eq!(result.message, "All systems operational");
    assert_eq!(result.metadata.len(), 2);
}

// Resource management tests
#[test]
fn test_resource_management_limits() {
    use crate::resource_management::ResourceLimits;

    let limits = ResourceLimits::unlimited();
    assert!(limits.max_memory_bytes.is_none());
    assert!(limits.max_cpu_seconds.is_none());

    let limits = ResourceLimits::memory_constrained(512);
    assert_eq!(limits.max_memory_bytes, Some(512 * 1024 * 1024));

    let limits = ResourceLimits::cpu_intensive(60);
    assert_eq!(limits.max_cpu_seconds, Some(60));

    let limits = ResourceLimits::io_intensive(300);
    assert_eq!(limits.max_wall_time_seconds, Some(300));
}

#[test]
fn test_resource_management_tracker() {
    use crate::resource_management::{ResourceLimits, ResourceTracker};
    use std::thread;
    use std::time::Duration;

    let limits = ResourceLimits::memory_constrained(100);
    let tracker = ResourceTracker::new(limits);

    tracker.start();
    thread::sleep(Duration::from_millis(10));

    tracker.record_memory_usage(50 * 1024 * 1024); // 50 MB
    assert_eq!(tracker.peak_memory_bytes(), 50 * 1024 * 1024);

    // Should be within limits
    assert!(tracker.check_limits().is_ok());

    // Recording more memory
    tracker.record_memory_usage(75 * 1024 * 1024); // 75 MB
    assert_eq!(tracker.peak_memory_bytes(), 75 * 1024 * 1024);
}

#[test]
fn test_resource_management_pool() {
    use crate::resource_management::ResourcePool;

    let pool: ResourcePool<String> = ResourcePool::new(3);
    assert!(pool.is_empty());
    assert_eq!(pool.max_size(), 3);

    // Add resources
    pool.release("resource1".to_string()).unwrap();
    pool.release("resource2".to_string()).unwrap();
    assert_eq!(pool.available(), 2);

    // Acquire resources
    let r1 = pool.acquire();
    assert!(r1.is_some());
    assert_eq!(pool.available(), 1);

    let r2 = pool.acquire();
    assert!(r2.is_some());
    assert_eq!(pool.available(), 0);
    assert!(pool.is_empty());
}

// Task hooks tests
#[test]
fn test_task_hooks_hook_registry() {
    use crate::task_hooks::{HookRegistry, LoggingHook};

    let mut registry = HookRegistry::new();
    assert_eq!(registry.pre_hook_count(), 0);
    assert_eq!(registry.post_hook_count(), 0);

    registry.register_pre_hook(LoggingHook::new(false, false));
    registry.register_post_hook(LoggingHook::new(false, false));

    assert_eq!(registry.pre_hook_count(), 1);
    assert_eq!(registry.post_hook_count(), 1);
}

#[test]
fn test_task_hooks_logging_hook() {
    use crate::task_hooks::{LoggingHook, PostExecutionHook, PreExecutionHook};
    use serde_json::json;

    let hook = LoggingHook::new(false, false);
    let mut args = vec![json!({"x": 1})];

    // Test pre-execution hook
    let result = hook.before_execute("test_task", "task-123", &mut args);
    assert!(result.is_ok());

    // Test post-execution hook
    let task_result: std::result::Result<serde_json::Value, String> = Ok(json!({"result": 42}));
    let result = hook.after_execute("test_task", "task-123", &task_result, 100);
    assert!(result.is_ok());

    // Test with error result
    let task_result: std::result::Result<serde_json::Value, String> =
        Err("Task failed".to_string());
    let result = hook.after_execute("test_task", "task-123", &task_result, 100);
    assert!(result.is_ok());
}

#[test]
fn test_task_hooks_validation_hook() {
    use crate::task_hooks::{PreExecutionHook, ValidationHook};
    use serde_json::json;

    let hook = ValidationHook::new(|task_name: &str, args: &Vec<serde_json::Value>| {
        if args.is_empty() {
            return Err("Arguments cannot be empty".into());
        }
        if task_name == "forbidden" {
            return Err("Task is forbidden".into());
        }
        Ok(())
    });

    let mut args = vec![json!({"x": 1})];
    let result = hook.before_execute("allowed_task", "task-123", &mut args);
    assert!(result.is_ok());

    let result = hook.before_execute("forbidden", "task-123", &mut args);
    assert!(result.is_err());

    let mut empty_args = vec![];
    let result = hook.before_execute("allowed_task", "task-123", &mut empty_args);
    assert!(result.is_err());
}

#[test]
fn test_task_hooks_run_hooks() {
    use crate::task_hooks::{HookRegistry, LoggingHook};
    use serde_json::json;

    let mut registry = HookRegistry::new();
    registry.register_pre_hook(LoggingHook::new(false, false));
    registry.register_post_hook(LoggingHook::new(false, false));

    let mut args = vec![json!({"x": 1})];
    let result = registry.run_pre_hooks("test_task", "task-123", &mut args);
    assert!(result.is_ok());

    let task_result: std::result::Result<serde_json::Value, String> = Ok(json!({"result": 42}));
    let result = registry.run_post_hooks("test_task", "task-123", &task_result, 100);
    assert!(result.is_ok());
}

// Metrics aggregation tests
#[test]
fn test_metrics_aggregation_histogram() {
    use crate::metrics_aggregation::Histogram;

    let mut histogram = Histogram::new();
    assert_eq!(histogram.count(), 0);
    assert_eq!(histogram.mean(), 0.0);

    histogram.record(100.0);
    histogram.record(200.0);
    histogram.record(150.0);

    assert_eq!(histogram.count(), 3);
    assert_eq!(histogram.sum(), 450.0);
    assert_eq!(histogram.mean(), 150.0);

    let p50 = histogram.percentile(50.0);
    assert!(p50 > 0.0);
}

#[test]
fn test_metrics_aggregation_aggregator() {
    use crate::metrics_aggregation::MetricsAggregator;
    use std::time::Duration;

    let aggregator = MetricsAggregator::new();

    // Record some task executions
    aggregator.record_duration("task1", Duration::from_millis(100));
    aggregator.record_duration("task1", Duration::from_millis(200));
    aggregator.record_duration("task2", Duration::from_millis(50));

    assert_eq!(aggregator.task_count("task1"), 2);
    assert_eq!(aggregator.task_count("task2"), 1);
    assert_eq!(aggregator.task_count("task3"), 0);

    let mean = aggregator.mean_duration("task1");
    assert!(mean > 100.0 && mean < 200.0);

    let p50 = aggregator.percentile_duration("task1", 50.0);
    assert!(p50 > 0.0);

    let throughput = aggregator.throughput("task1");
    assert!(throughput > 0.0);
}

#[test]
fn test_metrics_aggregation_error_tracking() {
    use crate::metrics_aggregation::MetricsAggregator;
    use std::time::Duration;

    let aggregator = MetricsAggregator::new();

    aggregator.record_duration("task1", Duration::from_millis(100));
    aggregator.record_duration("task1", Duration::from_millis(150));
    aggregator.record_error("task1");

    assert_eq!(aggregator.task_count("task1"), 2);
    assert_eq!(aggregator.error_count("task1"), 1);

    let success_rate = aggregator.success_rate("task1");
    assert!((success_rate - 50.0).abs() < 0.1);

    // Task with no errors should have 100% success rate
    aggregator.record_duration("task2", Duration::from_millis(100));
    let success_rate = aggregator.success_rate("task2");
    assert_eq!(success_rate, 100.0);
}

#[test]
fn test_metrics_aggregation_summary() {
    use crate::metrics_aggregation::MetricsAggregator;
    use std::time::Duration;

    let aggregator = MetricsAggregator::new();

    aggregator.record_duration("task1", Duration::from_millis(100));
    aggregator.record_duration("task1", Duration::from_millis(200));
    aggregator.record_error("task1");

    let summary = aggregator.summary("task1");
    assert!(summary.contains("task1"));
    assert!(summary.contains("Total Executions"));
    assert!(summary.contains("Mean Duration"));
    assert!(summary.contains("Throughput"));
}

#[test]
fn test_metrics_aggregation_task_names() {
    use crate::metrics_aggregation::MetricsAggregator;
    use std::time::Duration;

    let aggregator = MetricsAggregator::new();

    aggregator.record_duration("task1", Duration::from_millis(100));
    aggregator.record_duration("task2", Duration::from_millis(200));
    aggregator.record_duration("task3", Duration::from_millis(300));

    let names = aggregator.task_names();
    assert_eq!(names.len(), 3);
    assert!(names.contains(&"task1".to_string()));
    assert!(names.contains(&"task2".to_string()));
    assert!(names.contains(&"task3".to_string()));
}

#[test]
fn test_prelude_exports_new_modules() {
    // Test that all new module types are exported in prelude
    use crate::prelude::*;

    // Health check types
    let _: Option<WorkerHealthChecker> = None;
    let _: Option<DependencyChecker> = None;
    let _: Option<HealthCheckResult> = None;

    // Resource management types
    let _: Option<ResourceLimits> = None;
    let _: Option<ResourceTracker> = None;
    let _: Option<ResourcePool<String>> = None;

    // Task hooks types
    let _: Option<HookRegistry> = None;
    let _: Option<LoggingHook> = None;

    // Metrics aggregation types
    let _: Option<MetricsAggregator> = None;
    let _: Option<Histogram> = None;
}

// Task cancellation tests
#[test]
fn test_task_cancellation_token() {
    use crate::task_cancellation::CancellationToken;

    let token = CancellationToken::new();
    assert!(!token.is_cancelled());
    assert!(token.check_cancelled().is_ok());

    token.cancel(Some("User requested cancellation".to_string()));
    assert!(token.is_cancelled());
    assert!(token.check_cancelled().is_err());
    assert_eq!(
        token.cancellation_reason(),
        Some("User requested cancellation".to_string())
    );
}

#[test]
fn test_task_cancellation_timeout_manager() {
    use crate::task_cancellation::TimeoutManager;
    use std::thread;
    use std::time::Duration;

    let manager = TimeoutManager::new(Duration::from_millis(100));
    assert!(!manager.is_timed_out());
    assert!(manager.check_timeout().is_ok());

    thread::sleep(Duration::from_millis(150));
    assert!(manager.is_timed_out());
    assert!(manager.check_timeout().is_err());
}

#[test]
fn test_task_cancellation_execution_guard() {
    use crate::task_cancellation::{CancellationToken, ExecutionGuard};
    use std::time::Duration;

    let token = CancellationToken::new();
    let guard = ExecutionGuard::new(token.clone(), Some(Duration::from_secs(10)));

    assert!(guard.should_continue().is_ok());

    token.cancel(None);
    assert!(guard.should_continue().is_err());
}

// Retry strategies tests
#[test]
fn test_retry_strategies_exponential_backoff() {
    use crate::retry_strategies::RetryStrategy;
    use std::time::Duration;

    let strategy = RetryStrategy::exponential_backoff(3, Duration::from_secs(1));
    assert_eq!(strategy.max_retries, 3);

    let delay0 = strategy.calculate_delay(0);
    assert_eq!(delay0, Duration::from_secs(0));

    let delay1 = strategy.calculate_delay(1);
    assert!(delay1.as_millis() >= 750 && delay1.as_millis() <= 1250); // With jitter

    let delay2 = strategy.calculate_delay(2);
    assert!(delay2.as_millis() >= 1500); // At least 1.5s base with jitter
}

#[test]
fn test_retry_strategies_fixed_delay() {
    use crate::retry_strategies::RetryStrategy;
    use std::time::Duration;

    let strategy = RetryStrategy::fixed_delay(5, Duration::from_millis(500));
    assert_eq!(strategy.max_retries, 5);

    let delay = strategy.calculate_delay(1);
    assert_eq!(delay, Duration::from_millis(500));

    let delay = strategy.calculate_delay(3);
    assert_eq!(delay, Duration::from_millis(500));
}

#[test]
fn test_retry_strategies_default_policy() {
    use crate::retry_strategies::{DefaultRetryPolicy, RetryPolicy};

    let policy = DefaultRetryPolicy::new(3);
    assert!(policy.should_retry("any error", 0));
    assert!(policy.should_retry("any error", 2));
    assert!(!policy.should_retry("any error", 3));
}

#[test]
fn test_retry_strategies_error_pattern_policy() {
    use crate::retry_strategies::{ErrorPatternRetryPolicy, RetryPolicy};

    let policy =
        ErrorPatternRetryPolicy::new(3, vec!["timeout".to_string(), "connection".to_string()]);

    assert!(policy.should_retry("connection error", 0));
    assert!(policy.should_retry("timeout occurred", 1));
    assert!(!policy.should_retry("invalid input", 0));
    assert!(!policy.should_retry("timeout", 3)); // Max retries reached
}

// Task dependencies tests
#[test]
fn test_task_dependencies_graph() {
    use crate::task_dependencies::DependencyGraph;

    let mut graph = DependencyGraph::new();
    graph.add_task("task1");
    graph.add_task("task2");
    graph.add_task("task3");

    graph.add_dependency("task2", "task1"); // task2 depends on task1
    graph.add_dependency("task3", "task2"); // task3 depends on task2

    assert_eq!(graph.get_dependencies("task1"), Vec::<String>::new());
    assert_eq!(graph.get_dependencies("task2"), vec!["task1"]);
    assert_eq!(graph.get_dependencies("task3"), vec!["task2"]);

    assert_eq!(graph.get_dependents("task1"), vec!["task2"]);
    assert_eq!(graph.get_dependents("task2"), vec!["task3"]);
}

#[test]
fn test_task_dependencies_circular_detection() {
    use crate::task_dependencies::DependencyGraph;

    let mut graph = DependencyGraph::new();
    graph.add_task("task1");
    graph.add_task("task2");
    graph.add_task("task3");

    graph.add_dependency("task2", "task1");
    graph.add_dependency("task3", "task2");
    graph.add_dependency("task1", "task3"); // Creates cycle

    assert!(graph.has_circular_dependencies());
}

#[test]
fn test_task_dependencies_topological_sort() {
    use crate::task_dependencies::DependencyGraph;

    let mut graph = DependencyGraph::new();
    graph.add_task("task1");
    graph.add_task("task2");
    graph.add_task("task3");

    graph.add_dependency("task2", "task1");
    graph.add_dependency("task3", "task2");

    let sorted = graph.topological_sort().unwrap();
    assert_eq!(sorted, vec!["task1", "task2", "task3"]);
}

#[test]
fn test_task_dependencies_ready_tasks() {
    use crate::task_dependencies::DependencyGraph;
    use std::collections::HashSet;

    let mut graph = DependencyGraph::new();
    graph.add_task("task1");
    graph.add_task("task2");
    graph.add_task("task3");

    graph.add_dependency("task2", "task1");
    graph.add_dependency("task3", "task2");

    let completed: HashSet<String> = HashSet::new();
    let ready = graph.get_ready_tasks(&completed);
    assert_eq!(ready, vec!["task1"]); // Only task1 has no dependencies

    let mut completed = HashSet::new();
    completed.insert("task1".to_string());
    let ready = graph.get_ready_tasks(&completed);
    assert_eq!(ready, vec!["task2"]); // task2 becomes ready
}

// Performance profiling tests
#[test]
fn test_performance_profiling_profiler() {
    use crate::performance_profiling::PerformanceProfiler;
    use std::thread;
    use std::time::Duration;

    let profiler = PerformanceProfiler::new();

    profiler.start_span("operation1");
    thread::sleep(Duration::from_millis(10));
    profiler.end_span();

    profiler.start_span("operation2");
    thread::sleep(Duration::from_millis(20));
    profiler.end_span();

    let profile1 = profiler.get_profile("operation1").unwrap();
    assert_eq!(profile1.name, "operation1");
    assert_eq!(profile1.invocation_count, 1);
    assert!(profile1.total_duration.as_millis() >= 10);

    let profile2 = profiler.get_profile("operation2").unwrap();
    assert_eq!(profile2.invocation_count, 1);
    assert!(profile2.total_duration.as_millis() >= 20);
}

#[test]
fn test_performance_profiling_multiple_invocations() {
    use crate::performance_profiling::PerformanceProfiler;
    use std::thread;
    use std::time::Duration;

    let profiler = PerformanceProfiler::new();

    for _ in 0..3 {
        profiler.start_span("repeated_op");
        thread::sleep(Duration::from_millis(5));
        profiler.end_span();
    }

    let profile = profiler.get_profile("repeated_op").unwrap();
    assert_eq!(profile.invocation_count, 3);
    assert!(profile.total_duration.as_millis() >= 15);
}

#[test]
fn test_performance_profiling_slowest_operations() {
    use crate::performance_profiling::PerformanceProfiler;
    use std::thread;
    use std::time::Duration;

    let profiler = PerformanceProfiler::new();

    // Use larger time gaps to avoid flakiness under system load
    profiler.start_span("fast_op");
    thread::sleep(Duration::from_millis(10));
    profiler.end_span();

    profiler.start_span("slow_op");
    thread::sleep(Duration::from_millis(150));
    profiler.end_span();

    profiler.start_span("medium_op");
    thread::sleep(Duration::from_millis(50));
    profiler.end_span();

    let slowest = profiler.get_slowest_operations(2);
    assert_eq!(slowest.len(), 2);
    assert_eq!(slowest[0].name, "slow_op");
    assert_eq!(slowest[1].name, "medium_op");
}

#[test]
fn test_performance_profiling_report_generation() {
    use crate::performance_profiling::PerformanceProfiler;
    use std::thread;
    use std::time::Duration;

    let profiler = PerformanceProfiler::new();

    profiler.start_span("test_operation");
    thread::sleep(Duration::from_millis(10));
    profiler.end_span();

    let report = profiler.generate_report();
    assert!(report.contains("Performance Profile Report"));
    assert!(report.contains("test_operation"));
    assert!(report.contains("Count"));
    assert!(report.contains("Total"));
}

#[test]
fn test_prelude_exports_additional_modules() {
    // Test that all additional module types are exported in prelude
    use crate::prelude::*;

    // Task cancellation types
    let _: Option<CancellationToken> = None;
    let _: Option<TimeoutManager> = None;
    let _: Option<ExecutionGuard> = None;

    // Retry strategy types
    let _: Option<RetryStrategy> = None;
    let _: Option<DefaultRetryPolicy> = None;
    let _: Option<ErrorPatternRetryPolicy> = None;

    // Task dependency types
    let _: Option<DependencyGraph> = None;

    // Performance profiling types
    // Note: PerformanceProfiler is not exported in prelude to avoid conflict with dev_utils
    let _: Option<PerformanceProfile> = None;
    let _: Option<ProfileSpan<'_>> = None;

    // Access PerformanceProfiler directly from module
    let _: Option<crate::performance_profiling::PerformanceProfiler> = None;
}
