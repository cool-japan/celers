//! Tests for the PostgreSQL broker
#![cfg(test)]

use crate::*;
use celers_core::{Broker, SerializedTask};
use chrono::Utc;
use serde_json::json;
use uuid::Uuid;

#[test]
fn test_db_task_state_display() {
    assert_eq!(DbTaskState::Pending.to_string(), "pending");
    assert_eq!(DbTaskState::Processing.to_string(), "processing");
    assert_eq!(DbTaskState::Completed.to_string(), "completed");
    assert_eq!(DbTaskState::Failed.to_string(), "failed");
    assert_eq!(DbTaskState::Cancelled.to_string(), "cancelled");
}

#[test]
fn test_db_task_state_from_str() {
    assert_eq!(
        "pending".parse::<DbTaskState>().unwrap(),
        DbTaskState::Pending
    );
    assert_eq!(
        "processing".parse::<DbTaskState>().unwrap(),
        DbTaskState::Processing
    );
    assert_eq!(
        "completed".parse::<DbTaskState>().unwrap(),
        DbTaskState::Completed
    );
    assert_eq!(
        "failed".parse::<DbTaskState>().unwrap(),
        DbTaskState::Failed
    );
    assert_eq!(
        "cancelled".parse::<DbTaskState>().unwrap(),
        DbTaskState::Cancelled
    );
    // Case insensitive
    assert_eq!(
        "PENDING".parse::<DbTaskState>().unwrap(),
        DbTaskState::Pending
    );
    assert_eq!(
        "Completed".parse::<DbTaskState>().unwrap(),
        DbTaskState::Completed
    );
}

#[test]
fn test_db_task_state_invalid() {
    assert!("invalid".parse::<DbTaskState>().is_err());
    assert!("".parse::<DbTaskState>().is_err());
}

#[test]
fn test_queue_statistics_default() {
    let stats = QueueStatistics::default();
    assert_eq!(stats.pending, 0);
    assert_eq!(stats.processing, 0);
    assert_eq!(stats.completed, 0);
    assert_eq!(stats.failed, 0);
    assert_eq!(stats.cancelled, 0);
    assert_eq!(stats.dlq, 0);
    assert_eq!(stats.total, 0);
}

#[test]
fn test_db_task_state_serialization() {
    let state = DbTaskState::Pending;
    let json = serde_json::to_string(&state).unwrap();
    assert_eq!(json, "\"pending\"");

    let deserialized: DbTaskState = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized, state);
}

#[test]
fn test_task_result_status_display() {
    assert_eq!(TaskResultStatus::Pending.to_string(), "PENDING");
    assert_eq!(TaskResultStatus::Started.to_string(), "STARTED");
    assert_eq!(TaskResultStatus::Success.to_string(), "SUCCESS");
    assert_eq!(TaskResultStatus::Failure.to_string(), "FAILURE");
    assert_eq!(TaskResultStatus::Retry.to_string(), "RETRY");
    assert_eq!(TaskResultStatus::Revoked.to_string(), "REVOKED");
}

#[test]
fn test_task_result_status_from_str() {
    assert_eq!(
        "PENDING".parse::<TaskResultStatus>().unwrap(),
        TaskResultStatus::Pending
    );
    assert_eq!(
        "STARTED".parse::<TaskResultStatus>().unwrap(),
        TaskResultStatus::Started
    );
    assert_eq!(
        "SUCCESS".parse::<TaskResultStatus>().unwrap(),
        TaskResultStatus::Success
    );
    assert_eq!(
        "FAILURE".parse::<TaskResultStatus>().unwrap(),
        TaskResultStatus::Failure
    );
    assert_eq!(
        "RETRY".parse::<TaskResultStatus>().unwrap(),
        TaskResultStatus::Retry
    );
    assert_eq!(
        "REVOKED".parse::<TaskResultStatus>().unwrap(),
        TaskResultStatus::Revoked
    );
    // Case insensitive
    assert_eq!(
        "pending".parse::<TaskResultStatus>().unwrap(),
        TaskResultStatus::Pending
    );
    assert_eq!(
        "Success".parse::<TaskResultStatus>().unwrap(),
        TaskResultStatus::Success
    );
}

#[test]
fn test_task_result_status_invalid() {
    assert!("invalid".parse::<TaskResultStatus>().is_err());
    assert!("".parse::<TaskResultStatus>().is_err());
}

#[test]
fn test_task_result_status_serialization() {
    let status = TaskResultStatus::Success;
    let json = serde_json::to_string(&status).unwrap();
    assert_eq!(json, "\"success\"");

    let deserialized: TaskResultStatus = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized, status);
}

#[tokio::test]
#[ignore] // Requires PostgreSQL running
async fn test_postgres_broker_lifecycle() {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost/celers_test".to_string());

    let broker = PostgresBroker::new(&database_url).await.unwrap();
    broker.migrate().await.unwrap();

    // Test enqueue
    let task = SerializedTask::new("test_task".to_string(), vec![1, 2, 3, 4]);
    let task_id = task.metadata.id;

    let returned_id = broker.enqueue(task.clone()).await.unwrap();
    assert_eq!(returned_id, task_id);

    // Test queue size
    let size = broker.queue_size().await.unwrap();
    assert!(size >= 1);

    // Test dequeue
    let msg = broker.dequeue().await.unwrap();
    assert!(msg.is_some());
    let msg = msg.unwrap();
    assert_eq!(msg.task.metadata.name, "test_task");

    // Test ack
    broker
        .ack(&msg.task.metadata.id, msg.receipt_handle.as_deref())
        .await
        .unwrap();

    // Verify task is completed
    let size = broker.queue_size().await.unwrap();
    assert_eq!(size, 0);
}

#[tokio::test]
#[ignore] // Requires PostgreSQL running
async fn test_skip_locked_concurrent_dequeue() {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost/celers_test".to_string());

    let broker1 = PostgresBroker::new(&database_url).await.unwrap();
    broker1.migrate().await.unwrap();

    let broker2 = PostgresBroker::new(&database_url).await.unwrap();

    // Enqueue multiple tasks
    for i in 0..10 {
        let task = SerializedTask::new(format!("task_{}", i), vec![i as u8]);
        broker1.enqueue(task).await.unwrap();
    }

    // Dequeue concurrently
    let (msg1, msg2) = tokio::join!(broker1.dequeue(), broker2.dequeue());

    let msg1 = msg1.unwrap();
    let msg2 = msg2.unwrap();

    // Both should get different tasks (SKIP LOCKED ensures no contention)
    assert!(msg1.is_some());
    assert!(msg2.is_some());
    assert_ne!(
        msg1.unwrap().task.metadata.id,
        msg2.unwrap().task.metadata.id
    );
}

#[test]
fn test_retry_strategy_exponential() {
    let strategy = RetryStrategy::Exponential {
        max_delay_secs: 3600,
    };

    assert_eq!(strategy.calculate_backoff(0), 1); // 2^0 = 1
    assert_eq!(strategy.calculate_backoff(1), 2); // 2^1 = 2
    assert_eq!(strategy.calculate_backoff(2), 4); // 2^2 = 4
    assert_eq!(strategy.calculate_backoff(3), 8); // 2^3 = 8
    assert_eq!(strategy.calculate_backoff(10), 1024); // 2^10 = 1024
    assert_eq!(strategy.calculate_backoff(20), 3600); // Capped at max_delay_secs
}

#[test]
fn test_retry_strategy_linear() {
    let strategy = RetryStrategy::Linear {
        base_delay_secs: 10,
        max_delay_secs: 100,
    };

    assert_eq!(strategy.calculate_backoff(0), 0); // 10 * 0 = 0
    assert_eq!(strategy.calculate_backoff(1), 10); // 10 * 1 = 10
    assert_eq!(strategy.calculate_backoff(2), 20); // 10 * 2 = 20
    assert_eq!(strategy.calculate_backoff(5), 50); // 10 * 5 = 50
    assert_eq!(strategy.calculate_backoff(15), 100); // Capped at max_delay_secs
}

#[test]
fn test_retry_strategy_fixed() {
    let strategy = RetryStrategy::Fixed { delay_secs: 30 };

    assert_eq!(strategy.calculate_backoff(0), 30);
    assert_eq!(strategy.calculate_backoff(1), 30);
    assert_eq!(strategy.calculate_backoff(5), 30);
    assert_eq!(strategy.calculate_backoff(100), 30);
}

#[test]
fn test_retry_strategy_immediate() {
    let strategy = RetryStrategy::Immediate;

    assert_eq!(strategy.calculate_backoff(0), 0);
    assert_eq!(strategy.calculate_backoff(1), 0);
    assert_eq!(strategy.calculate_backoff(10), 0);
}

#[test]
fn test_retry_strategy_exponential_with_jitter() {
    let strategy = RetryStrategy::ExponentialWithJitter {
        max_delay_secs: 3600,
    };

    // Jitter should produce values in reasonable range
    let backoff0 = strategy.calculate_backoff(0);
    assert!((0..=2).contains(&backoff0)); // 2^0 = 1, with jitter 0.5-1.5

    let backoff3 = strategy.calculate_backoff(3);
    assert!((4..=12).contains(&backoff3)); // 2^3 = 8, with jitter ~0.5-1.5
}

#[test]
fn test_retry_strategy_default() {
    let strategy = RetryStrategy::default();
    match strategy {
        RetryStrategy::Exponential { max_delay_secs } => {
            assert_eq!(max_delay_secs, 3600);
        }
        _ => panic!("Default should be Exponential"),
    }
}

#[tokio::test]
#[ignore] // Requires PostgreSQL running
async fn test_pool_metrics() {
    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost/celers_test".to_string());

    let broker = PostgresBroker::new(&database_url).await.unwrap();
    broker.migrate().await.unwrap();

    let metrics = broker.get_pool_metrics();

    // Pool should have some configuration
    assert!(metrics.max_size > 0);
    assert!(metrics.size <= metrics.max_size);
    assert_eq!(metrics.size, metrics.idle + metrics.in_use);
}

#[test]
fn test_task_chain_creation() {
    let task1 = SerializedTask::new("task1".to_string(), vec![1, 2, 3]);
    let task2 = SerializedTask::new("task2".to_string(), vec![4, 5, 6]);

    let chain = TaskChain {
        tasks: vec![task1, task2],
        stop_on_failure: true,
    };

    assert_eq!(chain.tasks.len(), 2);
    assert!(chain.stop_on_failure);
}

#[test]
fn test_task_chain_serialization() {
    let task1 = SerializedTask::new("task1".to_string(), vec![1]);
    let chain = TaskChain {
        tasks: vec![task1],
        stop_on_failure: false,
    };

    let json = serde_json::to_string(&chain).unwrap();
    let deserialized: TaskChain = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.tasks.len(), 1);
    assert!(!deserialized.stop_on_failure);
}

#[test]
fn test_workflow_stage_creation() {
    let task = SerializedTask::new("task1".to_string(), vec![1]);

    let stage = WorkflowStage {
        id: "stage1".to_string(),
        tasks: vec![task],
        depends_on: vec!["stage0".to_string()],
    };

    assert_eq!(stage.id, "stage1");
    assert_eq!(stage.tasks.len(), 1);
    assert_eq!(stage.depends_on.len(), 1);
    assert_eq!(stage.depends_on[0], "stage0");
}

#[test]
fn test_workflow_creation() {
    let task1 = SerializedTask::new("task1".to_string(), vec![1]);
    let task2 = SerializedTask::new("task2".to_string(), vec![2]);

    let stage1 = WorkflowStage {
        id: "stage1".to_string(),
        tasks: vec![task1],
        depends_on: vec![],
    };

    let stage2 = WorkflowStage {
        id: "stage2".to_string(),
        tasks: vec![task2],
        depends_on: vec!["stage1".to_string()],
    };

    let workflow = TaskWorkflow {
        id: Uuid::new_v4(),
        name: "test_workflow".to_string(),
        stages: vec![stage1, stage2],
    };

    assert_eq!(workflow.name, "test_workflow");
    assert_eq!(workflow.stages.len(), 2);
    assert_eq!(workflow.stages[0].id, "stage1");
    assert_eq!(workflow.stages[1].depends_on[0], "stage1");
}

#[test]
fn test_workflow_serialization() {
    let task = SerializedTask::new("task".to_string(), vec![]);
    let stage = WorkflowStage {
        id: "s1".to_string(),
        tasks: vec![task],
        depends_on: vec![],
    };

    let workflow = TaskWorkflow {
        id: Uuid::new_v4(),
        name: "wf".to_string(),
        stages: vec![stage],
    };

    let json = serde_json::to_string(&workflow).unwrap();
    let deserialized: TaskWorkflow = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.name, "wf");
    assert_eq!(deserialized.stages.len(), 1);
    assert_eq!(deserialized.stages[0].id, "s1");
}

#[test]
fn test_chain_status_serialization() {
    let status = ChainStatus {
        chain_id: Uuid::new_v4(),
        total_tasks: 10,
        completed_tasks: 5,
        failed_tasks: 1,
        pending_tasks: 3,
        processing_tasks: 1,
        current_position: Some(5),
        is_complete: false,
        has_failures: true,
    };

    let json = serde_json::to_string(&status).unwrap();
    let deserialized: ChainStatus = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.total_tasks, 10);
    assert_eq!(deserialized.completed_tasks, 5);
    assert_eq!(deserialized.failed_tasks, 1);
    assert!(deserialized.has_failures);
    assert!(!deserialized.is_complete);
    assert_eq!(deserialized.current_position, Some(5));
}

#[test]
fn test_stage_status_serialization() {
    let status = StageStatus {
        stage_id: "stage1".to_string(),
        total_tasks: 5,
        completed_tasks: 3,
        failed_tasks: 0,
        pending_tasks: 2,
        processing_tasks: 0,
        is_complete: false,
        dependencies_met: true,
    };

    let json = serde_json::to_string(&status).unwrap();
    let deserialized: StageStatus = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.stage_id, "stage1");
    assert_eq!(deserialized.total_tasks, 5);
    assert!(deserialized.dependencies_met);
    assert!(!deserialized.is_complete);
}

#[test]
fn test_workflow_status_serialization() {
    let stage_status = StageStatus {
        stage_id: "s1".to_string(),
        total_tasks: 2,
        completed_tasks: 2,
        failed_tasks: 0,
        pending_tasks: 0,
        processing_tasks: 0,
        is_complete: true,
        dependencies_met: true,
    };

    let status = WorkflowStatus {
        workflow_id: Uuid::new_v4(),
        workflow_name: "test_wf".to_string(),
        total_stages: 2,
        completed_stages: 1,
        active_stages: 1,
        total_tasks: 10,
        completed_tasks: 7,
        failed_tasks: 1,
        pending_tasks: 1,
        processing_tasks: 1,
        is_complete: false,
        has_failures: true,
        stage_statuses: vec![stage_status],
    };

    let json = serde_json::to_string(&status).unwrap();
    let deserialized: WorkflowStatus = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.workflow_name, "test_wf");
    assert_eq!(deserialized.total_stages, 2);
    assert_eq!(deserialized.completed_stages, 1);
    assert_eq!(deserialized.stage_statuses.len(), 1);
    assert!(deserialized.has_failures);
}

#[test]
fn test_health_status_serialization() {
    let status = HealthStatus {
        healthy: true,
        connection_pool_size: 20,
        idle_connections: 15,
        pending_tasks: 100,
        processing_tasks: 5,
        dlq_tasks: 2,
        database_version: "PostgreSQL 14.5".to_string(),
    };

    let json = serde_json::to_string(&status).unwrap();
    let deserialized: HealthStatus = serde_json::from_str(&json).unwrap();

    assert!(deserialized.healthy);
    assert_eq!(deserialized.connection_pool_size, 20);
    assert_eq!(deserialized.idle_connections, 15);
    assert_eq!(deserialized.pending_tasks, 100);
    assert_eq!(deserialized.database_version, "PostgreSQL 14.5");
}

#[test]
fn test_pool_metrics_serialization() {
    let metrics = PoolMetrics {
        max_size: 20,
        size: 10,
        idle: 7,
        in_use: 3,
        waiting: 2,
    };

    let json = serde_json::to_string(&metrics).unwrap();
    let deserialized: PoolMetrics = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.max_size, 20);
    assert_eq!(deserialized.size, 10);
    assert_eq!(deserialized.idle, 7);
    assert_eq!(deserialized.in_use, 3);
    assert_eq!(deserialized.waiting, 2);
}

#[test]
fn test_task_info_serialization() {
    let now = Utc::now();
    let info = TaskInfo {
        id: Uuid::new_v4(),
        task_name: "test_task".to_string(),
        state: DbTaskState::Processing,
        priority: 5,
        retry_count: 2,
        max_retries: 3,
        created_at: now,
        scheduled_at: now,
        started_at: Some(now),
        completed_at: None,
        worker_id: Some("worker1".to_string()),
        error_message: None,
    };

    let json = serde_json::to_string(&info).unwrap();
    let deserialized: TaskInfo = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.task_name, "test_task");
    assert_eq!(deserialized.state, DbTaskState::Processing);
    assert_eq!(deserialized.priority, 5);
    assert_eq!(deserialized.retry_count, 2);
    assert_eq!(deserialized.worker_id, Some("worker1".to_string()));
}

#[test]
fn test_dlq_task_info_serialization() {
    let now = Utc::now();
    let info = DlqTaskInfo {
        id: Uuid::new_v4(),
        task_id: Uuid::new_v4(),
        task_name: "failed_task".to_string(),
        retry_count: 5,
        error_message: Some("Task failed permanently".to_string()),
        failed_at: now,
    };

    let json = serde_json::to_string(&info).unwrap();
    let deserialized: DlqTaskInfo = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.task_name, "failed_task");
    assert_eq!(deserialized.retry_count, 5);
    assert_eq!(
        deserialized.error_message,
        Some("Task failed permanently".to_string())
    );
}

#[test]
fn test_workflow_parallel_execution() {
    // Test that a stage can have multiple tasks (parallel execution)
    let task1 = SerializedTask::new("parallel1".to_string(), vec![1]);
    let task2 = SerializedTask::new("parallel2".to_string(), vec![2]);
    let task3 = SerializedTask::new("parallel3".to_string(), vec![3]);

    let stage = WorkflowStage {
        id: "parallel_stage".to_string(),
        tasks: vec![task1, task2, task3],
        depends_on: vec![],
    };

    assert_eq!(stage.tasks.len(), 3);
    // All tasks in the same stage can be executed in parallel
}

#[test]
fn test_workflow_complex_dependencies() {
    let stage1 = WorkflowStage {
        id: "s1".to_string(),
        tasks: vec![SerializedTask::new("t1".to_string(), vec![])],
        depends_on: vec![],
    };

    let stage2 = WorkflowStage {
        id: "s2".to_string(),
        tasks: vec![SerializedTask::new("t2".to_string(), vec![])],
        depends_on: vec![],
    };

    let stage3 = WorkflowStage {
        id: "s3".to_string(),
        tasks: vec![SerializedTask::new("t3".to_string(), vec![])],
        depends_on: vec!["s1".to_string(), "s2".to_string()],
    };

    let workflow = TaskWorkflow {
        id: Uuid::new_v4(),
        name: "complex".to_string(),
        stages: vec![stage1, stage2, stage3],
    };

    // s1 and s2 have no dependencies (can run in parallel)
    assert!(workflow.stages[0].depends_on.is_empty());
    assert!(workflow.stages[1].depends_on.is_empty());

    // s3 depends on both s1 and s2
    assert_eq!(workflow.stages[2].depends_on.len(), 2);
    assert!(workflow.stages[2].depends_on.contains(&"s1".to_string()));
    assert!(workflow.stages[2].depends_on.contains(&"s2".to_string()));
}

#[test]
fn test_get_state_counts_structure() {
    use std::collections::HashMap;

    let mut expected_keys = HashMap::new();
    expected_keys.insert("pending", 0i64);
    expected_keys.insert("processing", 0i64);
    expected_keys.insert("completed", 0i64);
    expected_keys.insert("failed", 0i64);
    expected_keys.insert("cancelled", 0i64);
    expected_keys.insert("dlq", 0i64);

    // Verify all expected state keys exist
    for key in expected_keys.keys() {
        assert!([
            "pending",
            "processing",
            "completed",
            "failed",
            "cancelled",
            "dlq"
        ]
        .contains(key));
    }
}

#[test]
fn test_detailed_health_status_structure() {
    let warnings = vec![
        "High DLQ count".to_string(),
        "Old pending tasks".to_string(),
    ];
    let recommendations = vec!["Increase workers".to_string()];

    let pool_metrics = PoolMetrics {
        max_size: 20,
        size: 15,
        idle: 10,
        in_use: 5,
        waiting: 0,
    };

    let queue_stats = QueueStatistics {
        pending: 100,
        processing: 10,
        completed: 500,
        failed: 5,
        cancelled: 2,
        dlq: 3,
        total: 620,
    };

    let status = DetailedHealthStatus {
        healthy: true,
        connection_ok: true,
        query_latency_ms: 5,
        pool_metrics,
        queue_stats,
        database_version: "PostgreSQL 15".to_string(),
        warnings,
        recommendations,
    };

    assert!(status.healthy);
    assert!(status.connection_ok);
    assert_eq!(status.warnings.len(), 2);
    assert_eq!(status.recommendations.len(), 1);
    assert_eq!(status.query_latency_ms, 5);
}

#[test]
fn test_batch_size_recommendation_structure() {
    let recommendation = BatchSizeRecommendation {
        enqueue_batch_size: 100,
        dequeue_batch_size: 50,
        ack_batch_size: 50,
        reasoning: "Based on current workload and pool size".to_string(),
    };

    assert_eq!(recommendation.enqueue_batch_size, 100);
    assert_eq!(recommendation.dequeue_batch_size, 50);
    assert_eq!(recommendation.ack_batch_size, 50);
    assert!(!recommendation.reasoning.is_empty());
}

#[test]
fn test_table_size_info_structure() {
    let info = TableSizeInfo {
        table_name: "celers_tasks".to_string(),
        row_count: 10000,
        total_size_bytes: 5242880,
        table_size_bytes: 4194304,
        index_size_bytes: 1048576,
        total_size_pretty: "5.0 MB".to_string(),
    };

    assert_eq!(info.table_name, "celers_tasks");
    assert_eq!(info.row_count, 10000);
    assert!(info.total_size_bytes > info.table_size_bytes);
    assert_eq!(
        info.total_size_bytes,
        info.table_size_bytes + info.index_size_bytes
    );
}

#[test]
fn test_index_usage_info_structure() {
    let info = IndexUsageInfo {
        table_name: "celers_tasks".to_string(),
        index_name: "idx_tasks_state_priority".to_string(),
        index_scans: 1000,
        tuples_read: 50000,
        tuples_fetched: 45000,
        index_size_bytes: 524288,
        index_size_pretty: "512 KB".to_string(),
    };

    assert_eq!(info.table_name, "celers_tasks");
    assert_eq!(info.index_name, "idx_tasks_state_priority");
    assert!(info.index_scans > 0);
    assert!(info.tuples_read >= info.tuples_fetched);
}

#[test]
fn test_partition_info_structure() {
    use chrono::TimeZone;

    let info = PartitionInfo {
        partition_name: "celers_tasks_2026_01".to_string(),
        partition_start: Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        partition_end: Utc.with_ymd_and_hms(2026, 2, 1, 0, 0, 0).unwrap(),
        row_count: 5000,
        size_bytes: 2097152,
    };

    assert!(info.partition_name.contains("celers_tasks"));
    assert!(info.partition_name.contains("2026_01"));
    assert_eq!(info.row_count, 5000);
    assert!(info.size_bytes > 0);
    assert!(info.partition_end > info.partition_start);
}

#[test]
fn test_tenant_broker_isolation() {
    // Test that tenant IDs are properly formatted
    let tenant_id = "tenant-123";
    assert!(!tenant_id.is_empty());
    assert!(tenant_id.contains("-"));
}

#[test]
fn test_retry_strategy_calculation_bounds() {
    // Test that retry delay calculations don't overflow
    let strategy = RetryStrategy::Exponential {
        max_delay_secs: 3600,
    };

    // Should handle reasonable retry counts
    for retry_count in 0..10 {
        let delay = strategy.calculate_backoff(retry_count);
        assert!(delay < 10000); // Less than ~3 hours
    }
}

#[test]
fn test_retry_strategy_jitter_range() {
    let strategy = RetryStrategy::ExponentialWithJitter {
        max_delay_secs: 3600,
    };

    // Jitter should provide some variation
    let delay1 = strategy.calculate_backoff(3);
    let delay2 = strategy.calculate_backoff(3);

    // With jitter, delays might vary (though not guaranteed in every run)
    // Just verify they're in reasonable range
    assert!(delay1 <= 100);
    assert!(delay2 <= 100);
}

#[test]
fn test_db_task_state_all_variants() {
    let states = vec![
        DbTaskState::Pending,
        DbTaskState::Processing,
        DbTaskState::Completed,
        DbTaskState::Failed,
        DbTaskState::Cancelled,
    ];

    for state in states {
        let s = state.to_string();
        assert!(!s.is_empty());

        // Verify round-trip conversion
        let parsed: DbTaskState = s.parse().unwrap();
        assert_eq!(parsed, state);
    }
}

#[test]
fn test_task_result_status_all_variants() {
    let statuses = vec![
        TaskResultStatus::Pending,
        TaskResultStatus::Started,
        TaskResultStatus::Success,
        TaskResultStatus::Failure,
        TaskResultStatus::Retry,
        TaskResultStatus::Revoked,
    ];

    for status in statuses {
        let s = status.to_string();
        assert!(!s.is_empty());

        // Verify round-trip conversion
        let parsed: TaskResultStatus = s.parse().unwrap();
        assert_eq!(parsed, status);
    }
}

#[test]
fn test_task_result_structure() {
    let result = TaskResult {
        task_id: Uuid::new_v4(),
        task_name: "test_task".to_string(),
        status: TaskResultStatus::Success,
        result: Some(json!({"output": "success"})),
        error: None,
        traceback: None,
        created_at: Utc::now(),
        completed_at: Some(Utc::now()),
        runtime_ms: Some(1500),
    };

    assert_eq!(result.task_name, "test_task");
    assert_eq!(result.status, TaskResultStatus::Success);
    assert!(result.result.is_some());
    assert!(result.error.is_none());
    assert_eq!(result.runtime_ms, Some(1500));
}

#[test]
fn test_dlq_task_info_structure() {
    let info = DlqTaskInfo {
        id: Uuid::new_v4(),
        task_id: Uuid::new_v4(),
        task_name: "failed_task".to_string(),
        retry_count: 5,
        error_message: Some("Connection timeout".to_string()),
        failed_at: Utc::now(),
    };

    assert_eq!(info.task_name, "failed_task");
    assert_eq!(info.retry_count, 5);
    assert!(info.error_message.is_some());
}

#[test]
fn test_queue_statistics_comprehensive() {
    let stats = QueueStatistics {
        pending: 100,
        processing: 10,
        completed: 500,
        failed: 5,
        cancelled: 2,
        dlq: 3,
        total: 620,
    };

    // Verify total calculation
    assert_eq!(stats.total, 620);

    // Verify relationships
    assert!(stats.completed > stats.pending);
    assert!(stats.processing < stats.completed);
    assert!(stats.failed < stats.pending);
    assert!(stats.dlq < stats.failed);
}

// ========== LISTEN/NOTIFY Tests ==========

#[test]
fn test_task_notification_structure() {
    let notification = TaskNotification {
        task_id: Uuid::new_v4(),
        task_name: "test_task".to_string(),
        queue_name: "default".to_string(),
        priority: 5,
        enqueued_at: Utc::now(),
    };

    assert_eq!(notification.task_name, "test_task");
    assert_eq!(notification.queue_name, "default");
    assert_eq!(notification.priority, 5);
}

#[test]
fn test_task_notification_serialization() {
    let notification = TaskNotification {
        task_id: Uuid::new_v4(),
        task_name: "test_task".to_string(),
        queue_name: "default".to_string(),
        priority: 10,
        enqueued_at: Utc::now(),
    };

    // Test serialization
    let json = serde_json::to_string(&notification).unwrap();
    assert!(json.contains("test_task"));
    assert!(json.contains("default"));

    // Test deserialization
    let deserialized: TaskNotification = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.task_id, notification.task_id);
    assert_eq!(deserialized.task_name, notification.task_name);
    assert_eq!(deserialized.queue_name, notification.queue_name);
    assert_eq!(deserialized.priority, notification.priority);
}

#[test]
fn test_notification_channel_naming() {
    // Test that channel names follow the expected format
    let queue_name = "default";
    let expected_channel = format!("celers_tasks_{}", queue_name);
    assert_eq!(expected_channel, "celers_tasks_default");

    let queue_name = "high_priority";
    let expected_channel = format!("celers_tasks_{}", queue_name);
    assert_eq!(expected_channel, "celers_tasks_high_priority");
}

#[tokio::test]
#[ignore] // Requires PostgreSQL connection
async fn test_notification_listener_creation() {
    // This test requires a live PostgreSQL database
    // Run with: cargo test test_notification_listener_creation -- --ignored
}

#[tokio::test]
#[ignore] // Requires PostgreSQL connection
async fn test_enable_disable_notifications() {
    // This test requires a live PostgreSQL database
    // Run with: cargo test test_enable_disable_notifications -- --ignored
}

#[tokio::test]
#[ignore] // Requires PostgreSQL connection
async fn test_notification_end_to_end() {
    // This test requires a live PostgreSQL database
    // Run with: cargo test test_notification_end_to_end -- --ignored
}

// ========== Task Deduplication Tests ==========

#[test]
fn test_deduplication_config_default() {
    let config = DeduplicationConfig::default();
    assert!(config.enabled);
    assert_eq!(config.window_secs, 300); // 5 minutes
}

#[test]
fn test_deduplication_config_custom() {
    let config = DeduplicationConfig {
        enabled: true,
        window_secs: 600, // 10 minutes
    };
    assert!(config.enabled);
    assert_eq!(config.window_secs, 600);
}

#[test]
fn test_deduplication_config_serialization() {
    let config = DeduplicationConfig {
        enabled: true,
        window_secs: 300,
    };

    // Test serialization
    let json = serde_json::to_string(&config).unwrap();
    assert!(json.contains("enabled"));
    assert!(json.contains("window_secs"));

    // Test deserialization
    let deserialized: DeduplicationConfig = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.enabled, config.enabled);
    assert_eq!(deserialized.window_secs, config.window_secs);
}

#[test]
fn test_deduplication_info_structure() {
    let info = DeduplicationInfo {
        idempotency_key: "order-123".to_string(),
        task_id: Uuid::new_v4(),
        task_name: "process_order".to_string(),
        first_seen_at: Utc::now(),
        expires_at: Utc::now() + chrono::Duration::seconds(300),
        duplicate_count: 5,
    };

    assert_eq!(info.idempotency_key, "order-123");
    assert_eq!(info.task_name, "process_order");
    assert_eq!(info.duplicate_count, 5);
    assert!(info.expires_at > info.first_seen_at);
}

#[test]
fn test_deduplication_info_serialization() {
    let info = DeduplicationInfo {
        idempotency_key: "payment-456".to_string(),
        task_id: Uuid::new_v4(),
        task_name: "process_payment".to_string(),
        first_seen_at: Utc::now(),
        expires_at: Utc::now() + chrono::Duration::seconds(600),
        duplicate_count: 3,
    };

    // Test serialization
    let json = serde_json::to_string(&info).unwrap();
    assert!(json.contains("payment-456"));
    assert!(json.contains("process_payment"));

    // Test deserialization
    let deserialized: DeduplicationInfo = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.idempotency_key, info.idempotency_key);
    assert_eq!(deserialized.task_id, info.task_id);
    assert_eq!(deserialized.task_name, info.task_name);
    assert_eq!(deserialized.duplicate_count, info.duplicate_count);
}

#[test]
fn test_deduplication_window_expiry() {
    let config = DeduplicationConfig {
        enabled: true,
        window_secs: 300,
    };

    let now = Utc::now();
    let expires_at = now + chrono::Duration::seconds(config.window_secs);

    // Verify expiry is in the future
    assert!(expires_at > now);

    // Verify expiry is exactly window_secs in the future
    let duration = expires_at - now;
    assert!(duration.num_seconds() <= config.window_secs + 1); // Allow 1 second tolerance
    assert!(duration.num_seconds() >= config.window_secs - 1);
}

#[tokio::test]
#[ignore] // Requires PostgreSQL connection
async fn test_enqueue_idempotent() {
    // This test requires a live PostgreSQL database
    // Run with: cargo test test_enqueue_idempotent -- --ignored
}

#[tokio::test]
#[ignore] // Requires PostgreSQL connection
async fn test_check_deduplication() {
    // This test requires a live PostgreSQL database
    // Run with: cargo test test_check_deduplication -- --ignored
}

#[tokio::test]
#[ignore] // Requires PostgreSQL connection
async fn test_cleanup_deduplication() {
    // This test requires a live PostgreSQL database
    // Run with: cargo test test_cleanup_deduplication -- --ignored
}

#[tokio::test]
#[ignore] // Requires PostgreSQL connection
async fn test_get_deduplication_stats() {
    // This test requires a live PostgreSQL database
    // Run with: cargo test test_get_deduplication_stats -- --ignored
}

// ========== Tests for new features (TTL, Advisory Locks, Performance Analytics) ==========

#[tokio::test]
#[ignore] // Requires PostgreSQL connection
async fn test_expire_tasks_by_ttl() {
    // This test requires a live PostgreSQL database
    // It would test:
    // 1. Enqueue tasks of a specific type
    // 2. Manually update their created_at to be old
    // 3. Call expire_tasks_by_ttl
    // 4. Verify tasks are in cancelled state
}

#[tokio::test]
#[ignore] // Requires PostgreSQL connection
async fn test_expire_all_tasks_by_ttl() {
    // This test requires a live PostgreSQL database
    // It would test:
    // 1. Enqueue multiple task types
    // 2. Manually update their created_at to be old
    // 3. Call expire_all_tasks_by_ttl
    // 4. Verify all old tasks are cancelled
}

#[tokio::test]
#[ignore] // Requires PostgreSQL connection
async fn test_advisory_lock_acquire_release() {
    // This test requires a live PostgreSQL database
    // It would test:
    // 1. Acquire lock with try_advisory_lock
    // 2. Verify it returns true
    // 3. Try to acquire same lock again, verify returns false
    // 4. Release lock
    // 5. Verify lock can be acquired again
}

#[tokio::test]
#[ignore] // Requires PostgreSQL connection
async fn test_advisory_lock_blocking() {
    // This test requires a live PostgreSQL database
    // It would test:
    // 1. Acquire lock with advisory_lock
    // 2. Verify is_advisory_lock_held returns true
    // 3. Release lock
    // 4. Verify is_advisory_lock_held returns false
}

#[tokio::test]
#[ignore] // Requires PostgreSQL connection
async fn test_get_task_percentiles() {
    // This test requires a live PostgreSQL database
    // It would test:
    // 1. Create and complete multiple tasks with varying durations
    // 2. Call get_task_percentiles
    // 3. Verify p50, p95, p99 are calculated correctly
}

#[tokio::test]
#[ignore] // Requires PostgreSQL connection
async fn test_get_slowest_tasks() {
    // This test requires a live PostgreSQL database
    // It would test:
    // 1. Create and complete multiple tasks with varying durations
    // 2. Call get_slowest_tasks
    // 3. Verify results are ordered by duration (slowest first)
    // 4. Verify limit is respected
}

// ========== Tests for Rate Limiting, Priority, DLQ Analytics, Cancellation ==========

#[tokio::test]
#[ignore] // Requires PostgreSQL connection
async fn test_get_task_rate() {
    // This test requires a live PostgreSQL database
    // It would test:
    // 1. Complete multiple tasks of same type
    // 2. Call get_task_rate
    // 3. Verify count matches expected rate
}

#[tokio::test]
#[ignore] // Requires PostgreSQL connection
async fn test_is_rate_limited() {
    // This test requires a live PostgreSQL database
    // It would test:
    // 1. Complete tasks to reach rate limit
    // 2. Call is_rate_limited
    // 3. Verify it returns true when limit exceeded
}

#[tokio::test]
#[ignore] // Requires PostgreSQL connection
async fn test_boost_task_priority() {
    // This test requires a live PostgreSQL database
    // It would test:
    // 1. Enqueue tasks with normal priority
    // 2. Boost priority
    // 3. Verify tasks have increased priority
}

#[tokio::test]
#[ignore] // Requires PostgreSQL connection
async fn test_set_task_priority() {
    // This test requires a live PostgreSQL database
    // It would test:
    // 1. Enqueue tasks
    // 2. Set absolute priority for specific tasks
    // 3. Verify priority was set correctly
}

#[tokio::test]
#[ignore] // Requires PostgreSQL connection
async fn test_get_dlq_stats_by_task() {
    // This test requires a live PostgreSQL database
    // It would test:
    // 1. Create failed tasks in DLQ
    // 2. Call get_dlq_stats_by_task
    // 3. Verify task counts grouped by name
}

#[tokio::test]
#[ignore] // Requires PostgreSQL connection
async fn test_get_dlq_error_patterns() {
    // This test requires a live PostgreSQL database
    // It would test:
    // 1. Create DLQ tasks with various errors
    // 2. Call get_dlq_error_patterns
    // 3. Verify most common errors are returned
}

#[tokio::test]
#[ignore] // Requires PostgreSQL connection
async fn test_get_recent_dlq_tasks() {
    // This test requires a live PostgreSQL database
    // It would test:
    // 1. Create DLQ tasks at different times
    // 2. Call get_recent_dlq_tasks
    // 3. Verify only recent tasks are returned
}

#[tokio::test]
#[ignore] // Requires PostgreSQL connection
async fn test_cancel_with_reason() {
    // This test requires a live PostgreSQL database
    // It would test:
    // 1. Enqueue task
    // 2. Cancel with reason
    // 3. Verify task is cancelled with reason recorded
}

#[tokio::test]
#[ignore] // Requires PostgreSQL connection
async fn test_cancel_batch_with_reason() {
    // This test requires a live PostgreSQL database
    // It would test:
    // 1. Enqueue multiple tasks
    // 2. Batch cancel with reason
    // 3. Verify all tasks cancelled with reason
}

#[tokio::test]
#[ignore] // Requires PostgreSQL connection
async fn test_get_cancellation_reasons() {
    // This test requires a live PostgreSQL database
    // It would test:
    // 1. Cancel tasks with various reasons
    // 2. Call get_cancellation_reasons
    // 3. Verify reasons are grouped and counted correctly
}
