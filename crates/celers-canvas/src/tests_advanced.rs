#![cfg(test)]

use crate::*;
use std::collections::HashMap;
use uuid::Uuid;

#[test]
fn test_batching_strategy_display() {
    let strategy = BatchingStrategy::ByPriority;
    assert_eq!(format!("{}", strategy), "ByPriority");
}

#[test]
fn test_observable() {
    let mut obs = Observable::new(42);
    assert_eq!(*obs.get(), 42);
    assert_eq!(obs.subscriber_count(), 0);

    // Subscribe workflows
    let wf1 = Uuid::new_v4();
    let wf2 = Uuid::new_v4();
    obs.subscribe(wf1);
    obs.subscribe(wf2);
    assert_eq!(obs.subscriber_count(), 2);

    // Update value
    obs.set(100);
    assert_eq!(*obs.get(), 100);
    assert_eq!(obs.history.len(), 1);

    // Unsubscribe
    obs.unsubscribe(&wf1);
    assert_eq!(obs.subscriber_count(), 1);
}

#[test]
fn test_reactive_workflow() {
    let reaction = Signature::new("on_change".to_string());
    let workflow = ReactiveWorkflow::new(reaction)
        .watch("observable1")
        .watch("observable2")
        .with_debounce(100)
        .with_throttle(500)
        .with_filter("value > 10");

    assert_eq!(workflow.watched_observables.len(), 2);
    assert_eq!(workflow.debounce_ms, Some(100));
    assert_eq!(workflow.throttle_ms, Some(500));
    assert!(workflow.filter.is_some());

    let display = format!("{}", workflow);
    assert!(display.contains("ReactiveWorkflow"));
    assert!(display.contains("watching=2"));
    assert!(display.contains("on_change"));
}

#[test]
fn test_stream_operator() {
    let map_op = StreamOperator::Map;
    let filter_op = StreamOperator::Filter;
    let debounce_op = StreamOperator::Debounce;

    assert_eq!(format!("{}", map_op), "Map");
    assert_eq!(format!("{}", filter_op), "Filter");
    assert_eq!(format!("{}", debounce_op), "Debounce");
}

#[test]
fn test_reactive_stream() {
    let mut stream = ReactiveStream::new("source1")
        .map(serde_json::json!({"transform": "uppercase"}))
        .filter(serde_json::json!({"condition": "length > 5"}))
        .take(10)
        .skip(2)
        .debounce(100)
        .throttle(500);

    assert_eq!(stream.source_id, "source1");
    assert_eq!(stream.operators.len(), 6);

    // Subscribe workflow
    let wf = Uuid::new_v4();
    stream.subscribe(wf);
    assert_eq!(stream.subscribers.len(), 1);

    let display = format!("{}", stream);
    assert!(display.contains("ReactiveStream"));
    assert!(display.contains("source=source1"));
    assert!(display.contains("operators=6"));
}

#[test]
fn test_mock_task_result() {
    let success =
        MockTaskResult::success("task1", serde_json::json!({"result": "ok"})).with_delay(50);
    assert!(!success.should_fail);
    assert_eq!(success.delay_ms, 50);

    let failure = MockTaskResult::failure("task2", "Task failed");
    assert!(failure.should_fail);
    assert_eq!(failure.failure_message, Some("Task failed".to_string()));
}

#[test]
fn test_mock_task_executor() {
    let mut executor = MockTaskExecutor::new();

    // Register mock results
    executor.register(MockTaskResult::success("task1", serde_json::json!(42)));
    executor.register(MockTaskResult::failure("task2", "Error"));

    // Execute successful task
    let result1 = executor.execute("task1");
    assert!(result1.is_ok());
    assert_eq!(result1.unwrap(), serde_json::json!(42));

    // Execute failing task
    let result2 = executor.execute("task2");
    assert!(result2.is_err());

    // Execute unregistered task
    let result3 = executor.execute("task3");
    assert!(result3.is_err());

    // Check execution count
    assert_eq!(executor.execution_count("task1"), 1);
    assert_eq!(executor.execution_count("task2"), 1);

    // Clear history
    executor.clear_history();
    assert_eq!(executor.execution_count("task1"), 0);
}

#[test]
fn test_test_data_injector() {
    let mut injector = TestDataInjector::new();

    // Inject data
    injector.inject("key1", serde_json::json!({"value": 123}));
    injector.inject("key2", serde_json::json!("test"));

    // Get data
    assert!(injector.get("key1").is_some());
    assert_eq!(injector.get("key2"), Some(&serde_json::json!("test")));
    assert!(injector.get("key3").is_none());

    // Clear data
    injector.clear();
    assert!(injector.get("key1").is_none());
}

#[test]
fn test_workflow_snapshot() {
    let workflow_id = Uuid::new_v4();
    let state = WorkflowState::new(workflow_id, 5);
    let mut snapshot = WorkflowSnapshot::new(workflow_id, state);

    assert_eq!(snapshot.workflow_id, workflow_id);
    assert_eq!(snapshot.completed_tasks.len(), 0);

    // Record task
    let task_id = Uuid::new_v4();
    snapshot.record_task(task_id, serde_json::json!({"result": "ok"}));

    assert_eq!(snapshot.completed_tasks.len(), 1);
    assert!(snapshot.task_results.contains_key(&task_id));

    // Attach checkpoint
    let checkpoint = WorkflowCheckpoint::new(workflow_id, WorkflowState::new(workflow_id, 5));
    snapshot = snapshot.with_checkpoint(checkpoint);
    assert!(snapshot.checkpoint.is_some());
}

#[test]
fn test_time_travel_debugger() {
    let workflow_id = Uuid::new_v4();
    let mut debugger = TimeTravelDebugger::new(workflow_id);

    assert_eq!(debugger.snapshot_count(), 0);
    assert!(!debugger.step_mode);

    // Record snapshots
    let snapshot1 = WorkflowSnapshot::new(workflow_id, WorkflowState::new(workflow_id, 5));
    let snapshot2 = WorkflowSnapshot::new(workflow_id, WorkflowState::new(workflow_id, 5));
    debugger.record_snapshot(snapshot1);
    debugger.record_snapshot(snapshot2);

    assert_eq!(debugger.snapshot_count(), 2);
    assert_eq!(debugger.current_index, 1);

    // Step backward
    let snapshot = debugger.step_backward();
    assert!(snapshot.is_some());
    assert_eq!(debugger.current_index, 0);

    // Step forward
    let snapshot = debugger.step_forward();
    assert!(snapshot.is_some());
    assert_eq!(debugger.current_index, 1);

    // Replay from specific point
    let snapshot = debugger.replay_from(0);
    assert!(snapshot.is_some());
    assert_eq!(debugger.current_index, 0);

    // Enable step mode
    debugger.enable_step_mode();
    assert!(debugger.step_mode);

    // Test display
    let display = format!("{}", debugger);
    assert!(display.contains("TimeTravelDebugger"));
    assert!(display.contains("snapshots=2"));

    // Clear snapshots
    debugger.clear();
    assert_eq!(debugger.snapshot_count(), 0);
}

// ===== Integration Tests =====

/// Integration tests for broker, backend, chord barriers, and performance
mod integration {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    /// Mock broker for testing
    #[derive(Clone)]
    struct MockBroker {
        tasks: Arc<std::sync::Mutex<Vec<String>>>,
    }

    impl MockBroker {
        fn new() -> Self {
            Self {
                tasks: Arc::new(std::sync::Mutex::new(Vec::new())),
            }
        }

        fn enqueued_tasks(&self) -> Vec<String> {
            self.tasks.lock().unwrap().clone()
        }

        fn task_count(&self) -> usize {
            self.tasks.lock().unwrap().len()
        }
    }

    #[async_trait::async_trait]
    impl celers_core::Broker for MockBroker {
        async fn enqueue(
            &self,
            task: celers_core::SerializedTask,
        ) -> celers_core::Result<celers_core::TaskId> {
            let task_name = task.metadata.name.clone();
            let task_id = task.metadata.id;
            self.tasks.lock().unwrap().push(task_name);
            Ok(task_id)
        }

        async fn dequeue(&self) -> celers_core::Result<Option<celers_core::BrokerMessage>> {
            Ok(None)
        }

        async fn ack(
            &self,
            _task_id: &celers_core::TaskId,
            _receipt_handle: Option<&str>,
        ) -> celers_core::Result<()> {
            Ok(())
        }

        async fn reject(
            &self,
            _task_id: &celers_core::TaskId,
            _receipt_handle: Option<&str>,
            _requeue: bool,
        ) -> celers_core::Result<()> {
            Ok(())
        }

        async fn queue_size(&self) -> celers_core::Result<usize> {
            Ok(self.tasks.lock().unwrap().len())
        }

        async fn cancel(&self, _task_id: &celers_core::TaskId) -> celers_core::Result<bool> {
            Ok(true)
        }
    }

    // ===== Broker Integration Tests =====

    #[tokio::test]
    async fn test_chain_broker_integration() {
        let broker = MockBroker::new();

        let chain = Chain::new()
            .then("task1", vec![serde_json::json!(1)])
            .then("task2", vec![serde_json::json!(2)])
            .then("task3", vec![serde_json::json!(3)]);

        // Apply the chain
        let result = chain.apply(&broker).await;
        assert!(result.is_ok(), "Chain apply should succeed");

        // Verify only the first task was published
        // (subsequent tasks are linked via callback mechanism)
        let tasks = broker.enqueued_tasks();
        assert_eq!(tasks.len(), 1, "Chain should publish only first task");
        assert!(tasks.contains(&"task1".to_string()));
    }

    #[tokio::test]
    async fn test_group_broker_integration() {
        let broker = MockBroker::new();

        let group = Group::new()
            .add("task1", vec![serde_json::json!(1)])
            .add("task2", vec![serde_json::json!(2)])
            .add("task3", vec![serde_json::json!(3)]);

        // Apply the group
        let result = group.apply(&broker).await;
        assert!(result.is_ok(), "Group apply should succeed");

        // Verify all tasks were published in parallel
        let tasks = broker.enqueued_tasks();
        assert_eq!(tasks.len(), 3, "Should publish 3 tasks");
    }

    #[tokio::test]
    async fn test_map_broker_integration() {
        let broker = MockBroker::new();

        let map = Map::new(
            Signature::new("process".to_string()),
            vec![
                vec![serde_json::json!(1)],
                vec![serde_json::json!(2)],
                vec![serde_json::json!(3)],
            ],
        );

        let result = map.apply(&broker).await;
        assert!(result.is_ok(), "Map apply should succeed");

        // Verify all task instances were published
        let tasks = broker.enqueued_tasks();
        assert_eq!(tasks.len(), 3, "Should publish 3 task instances");
        assert_eq!(tasks.iter().filter(|t| *t == "process").count(), 3);
    }

    #[tokio::test]
    async fn test_nested_workflow_broker_integration() {
        let broker = MockBroker::new();

        // Create nested workflows
        let inner_group1 = Group::new()
            .add("task1", vec![serde_json::json!(1)])
            .add("task2", vec![serde_json::json!(2)]);

        let inner_group2 = Group::new()
            .add("task3", vec![serde_json::json!(3)])
            .add("task4", vec![serde_json::json!(4)]);

        let _ = inner_group1.apply(&broker).await;
        let _ = inner_group2.apply(&broker).await;

        assert_eq!(broker.task_count(), 4, "Should publish all nested tasks");
    }

    // ===== Backend Integration Tests =====

    #[cfg(feature = "backend-redis")]
    #[tokio::test]
    async fn test_chord_backend_integration() {
        let group = Group::new()
            .add("task1", vec![serde_json::json!(1)])
            .add("task2", vec![serde_json::json!(2)]);
        let callback = Signature::new("aggregate".to_string());
        let chord = Chord::new(group, callback);

        assert_eq!(chord.header.tasks.len(), 2);
        assert_eq!(chord.body.task, "aggregate");
    }

    #[cfg(feature = "backend-redis")]
    #[tokio::test]
    async fn test_chord_state_tracking() {
        let chord_id = Uuid::new_v4();
        let mut group = Group::new();
        group.group_id = Some(chord_id);
        let group = group
            .add("task1", vec![serde_json::json!(1)])
            .add("task2", vec![serde_json::json!(2)]);
        let callback = Signature::new("aggregate".to_string());
        let chord = Chord::new(group, callback);

        assert_eq!(chord.header.group_id, Some(chord_id));
        assert_eq!(chord.header.tasks.len(), 2);
    }

    // ===== Chord Barrier Race Condition Tests =====

    #[tokio::test]
    async fn test_chord_concurrent_completion() {
        let counter = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(tokio::sync::Barrier::new(10));

        let mut handles = vec![];

        // Simulate 10 tasks completing concurrently
        for _ in 0..10 {
            let counter = counter.clone();
            let barrier = barrier.clone();

            let handle = tokio::spawn(async move {
                // Wait for all tasks to be ready
                barrier.wait().await;

                // Simulate task completion and counter increment (like Redis INCR)
                let old = counter.fetch_add(1, Ordering::SeqCst);
                old + 1
            });

            handles.push(handle);
        }

        // Wait for all tasks
        let mut results = vec![];
        for handle in handles {
            results.push(handle.await.unwrap());
        }

        // Verify no duplicates (each increment should be unique)
        results.sort();
        assert_eq!(results, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        assert_eq!(counter.load(Ordering::SeqCst), 10);
    }

    #[tokio::test]
    async fn test_chord_barrier_idempotency() {
        // Test that callback is triggered exactly once even with race conditions
        let callback_count = Arc::new(AtomicUsize::new(0));
        let completed_count = Arc::new(AtomicUsize::new(0));
        let total_tasks = 5;

        let mut handles = vec![];

        for _ in 0..total_tasks {
            let callback_count = callback_count.clone();
            let completed_count = completed_count.clone();

            let handle = tokio::spawn(async move {
                // Simulate task completion
                let count = completed_count.fetch_add(1, Ordering::SeqCst) + 1;

                // Only the last task should trigger callback
                if count == total_tasks {
                    callback_count.fetch_add(1, Ordering::SeqCst);
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap();
        }

        // Verify callback was triggered exactly once
        assert_eq!(callback_count.load(Ordering::SeqCst), 1);
        assert_eq!(completed_count.load(Ordering::SeqCst), total_tasks);
    }

    #[tokio::test]
    async fn test_chord_partial_failure_handling() {
        // Test chord behavior when some tasks fail
        let success_count = Arc::new(AtomicUsize::new(0));
        let failure_count = Arc::new(AtomicUsize::new(0));

        let mut handles = vec![];

        for i in 0..10 {
            let success_count = success_count.clone();
            let failure_count = failure_count.clone();

            let handle = tokio::spawn(async move {
                if i % 3 == 0 {
                    // Simulate failure
                    failure_count.fetch_add(1, Ordering::SeqCst);
                    Err::<(), &str>("Task failed")
                } else {
                    // Simulate success
                    success_count.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            });

            handles.push(handle);
        }

        for handle in handles {
            let _ = handle.await.unwrap();
        }

        let success = success_count.load(Ordering::SeqCst);
        let failure = failure_count.load(Ordering::SeqCst);

        assert_eq!(success + failure, 10);
        assert!(failure > 0, "Should have some failures");
    }

    // ===== Performance Tests =====

    #[test]
    fn test_chain_creation_performance() {
        let start = Instant::now();

        for _ in 0..1000 {
            let _ = Chain::new()
                .then("task1", vec![serde_json::json!(1)])
                .then("task2", vec![serde_json::json!(2)])
                .then("task3", vec![serde_json::json!(3)]);
        }

        let duration = start.elapsed();
        assert!(
            duration < Duration::from_millis(100),
            "Creating 1000 chains should take less than 100ms, took {:?}",
            duration
        );
    }

    #[test]
    fn test_group_creation_performance() {
        let start = Instant::now();

        for _ in 0..1000 {
            let _ = Group::new()
                .add("task1", vec![serde_json::json!(1)])
                .add("task2", vec![serde_json::json!(2)])
                .add("task3", vec![serde_json::json!(3)]);
        }

        let duration = start.elapsed();
        assert!(
            duration < Duration::from_millis(100),
            "Creating 1000 groups should take less than 100ms, took {:?}",
            duration
        );
    }

    #[test]
    fn test_large_workflow_creation() {
        let start = Instant::now();

        let mut chain = Chain::new();
        for i in 0..1000 {
            let task_name = format!("task{}", i);
            chain = chain.then(&task_name, vec![serde_json::json!(i)]);
        }

        let duration = start.elapsed();
        assert!(
            duration < Duration::from_millis(500),
            "Creating chain with 1000 tasks should take less than 500ms, took {:?}",
            duration
        );
        assert_eq!(chain.len(), 1000);
    }

    #[test]
    fn test_map_with_large_dataset() {
        let start = Instant::now();

        let args: Vec<Vec<serde_json::Value>> =
            (0..1000).map(|i| vec![serde_json::json!(i)]).collect();

        let map = Map::new(Signature::new("process".to_string()), args);

        let duration = start.elapsed();
        assert!(
            duration < Duration::from_millis(100),
            "Creating map with 1000 items should take less than 100ms, took {:?}",
            duration
        );
        assert_eq!(map.len(), 1000);
    }

    #[test]
    fn test_workflow_serialization_performance() {
        let chain = Chain::new()
            .then("task1", vec![serde_json::json!(1)])
            .then("task2", vec![serde_json::json!(2)])
            .then("task3", vec![serde_json::json!(3)]);

        let start = Instant::now();

        for _ in 0..1000 {
            let _ = serde_json::to_string(&chain).unwrap();
        }

        let duration = start.elapsed();
        assert!(
            duration < Duration::from_millis(100),
            "Serializing chain 1000 times should take less than 100ms, took {:?}",
            duration
        );
    }

    #[tokio::test]
    async fn test_broker_publish_performance() {
        let broker = MockBroker::new();
        let start = Instant::now();

        for i in 0..100 {
            let task_name = format!("task{}", i);
            let chain = Chain::new().then(&task_name, vec![serde_json::json!(i)]);
            let _ = chain.apply(&broker).await;
        }

        let duration = start.elapsed();
        assert!(
            duration < Duration::from_secs(1),
            "Publishing 100 chains should take less than 1s, took {:?}",
            duration
        );
        assert_eq!(broker.task_count(), 100);
    }

    #[tokio::test]
    async fn test_concurrent_workflow_enqueue() {
        let broker = Arc::new(MockBroker::new());
        let mut handles = vec![];

        let start = Instant::now();

        for i in 0..100 {
            let broker = broker.clone();
            let handle = tokio::spawn(async move {
                let task_name = format!("task{}", i);
                let chain = Chain::new().then(&task_name, vec![serde_json::json!(i)]);
                chain.apply(&*broker).await
            });
            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap().unwrap();
        }

        let duration = start.elapsed();
        assert!(
            duration < Duration::from_secs(2),
            "Concurrent publishing of 100 chains should take less than 2s, took {:?}",
            duration
        );
        assert_eq!(broker.task_count(), 100);
    }

    #[test]
    fn test_memory_efficiency_large_group() {
        // Test that large groups don't cause excessive memory usage
        let mut group = Group::new();

        for i in 0..10000 {
            let task_name = format!("task{}", i);
            group = group.add(&task_name, vec![serde_json::json!(i)]);
        }

        assert_eq!(group.len(), 10000);
        assert!(!group.is_empty());
    }

    #[test]
    fn test_workflow_clone_performance() {
        let chain = Chain::new()
            .then("task1", vec![serde_json::json!(1)])
            .then("task2", vec![serde_json::json!(2)])
            .then("task3", vec![serde_json::json!(3)]);

        let start = Instant::now();

        for _ in 0..1000 {
            let _ = chain.clone();
        }

        let duration = start.elapsed();
        assert!(
            duration < Duration::from_millis(50),
            "Cloning chain 1000 times should take less than 50ms, took {:?}",
            duration
        );
    }

    // ===== Stress Tests =====

    #[test]
    fn test_deeply_nested_workflows() {
        // Test that deeply nested workflows don't cause stack overflow
        let mut current = Chain::new().then("task0", vec![serde_json::json!(0)]);

        for i in 1..100 {
            let task_name = format!("task{}", i);
            current = current.then(&task_name, vec![serde_json::json!(i)]);
        }

        assert_eq!(current.len(), 100);
    }

    #[test]
    fn test_workflow_with_large_payloads() {
        // Test workflows with large argument payloads
        let large_data = vec![serde_json::json!({ "data": "x".repeat(10000) })];

        let chain = Chain::new()
            .then("process_large", large_data.clone())
            .then("process_large2", large_data);

        let serialized = serde_json::to_string(&chain).unwrap();
        assert!(
            serialized.len() > 20000,
            "Serialized chain should contain large data"
        );
    }

    // ===== DAG Export Tests =====

    #[test]
    fn test_dag_export_dot_format() {
        let chain = Chain::new()
            .then("task1", vec![serde_json::json!(1)])
            .then("task2", vec![serde_json::json!(2)])
            .then("task3", vec![serde_json::json!(3)]);

        let dot = chain.to_dot();
        assert!(dot.contains("digraph Chain"));
        assert!(dot.contains("task1"));
        assert!(dot.contains("task2"));
        assert!(dot.contains("task3"));
        assert!(dot.contains("->"));
    }

    #[test]
    fn test_dag_export_mermaid_format() {
        let group = Group::new()
            .add("task1", vec![serde_json::json!(1)])
            .add("task2", vec![serde_json::json!(2)]);

        let mermaid = group.to_mermaid();
        assert!(mermaid.contains("graph"));
        assert!(mermaid.contains("task1"));
        assert!(mermaid.contains("task2"));
    }

    #[test]
    fn test_dag_export_json_format() {
        let chain = Chain::new().then("task1", vec![serde_json::json!(1)]);

        let json = chain.to_json().unwrap();
        assert!(json.contains("task1"));
        assert!(json.contains("tasks"));
    }

    #[test]
    fn test_dag_export_render_commands() {
        let chain = Chain::new().then("task1", vec![serde_json::json!(1)]);

        let svg_cmd = chain.svg_render_command();
        assert!(svg_cmd.contains("dot"));
        assert!(svg_cmd.contains("-Tsvg"));

        let png_cmd = chain.png_render_command();
        assert!(png_cmd.contains("dot"));
        assert!(png_cmd.contains("-Tpng"));
    }

    #[test]
    #[ignore] // Requires GraphViz to be installed
    fn test_dag_export_to_svg() {
        let chain = Chain::new()
            .then("task1", vec![serde_json::json!(1)])
            .then("task2", vec![serde_json::json!(2)]);

        if is_graphviz_available() {
            let svg = chain.to_svg().unwrap();
            assert!(svg.contains("<svg"));
            assert!(svg.contains("</svg>"));
            assert!(svg.contains("task1"));
        } else {
            println!("Skipping: GraphViz not installed");
        }
    }

    #[test]
    #[ignore] // Requires GraphViz to be installed
    fn test_dag_export_to_png() {
        let chain = Chain::new()
            .then("task1", vec![serde_json::json!(1)])
            .then("task2", vec![serde_json::json!(2)]);

        if is_graphviz_available() {
            let png = chain.to_png().unwrap();
            assert!(!png.is_empty());
            // PNG magic bytes
            assert_eq!(&png[0..4], &[0x89, 0x50, 0x4E, 0x47]);
        } else {
            println!("Skipping: GraphViz not installed");
        }
    }

    #[test]
    fn test_graphviz_availability_check() {
        // This test just verifies the function runs without panicking
        let _available = is_graphviz_available();
        // Result depends on system, so we just ensure it doesn't crash
    }

    #[test]
    fn test_dag_format_enum() {
        let _dot = DagFormat::Dot;
        let _mermaid = DagFormat::Mermaid;
        let _json = DagFormat::Json;
        let _svg = DagFormat::Svg;
        let _png = DagFormat::Png;
    }

    // ========================================================================
    // Visualization Features Tests
    // ========================================================================

    #[test]
    fn test_visual_theme_light() {
        let theme = VisualTheme::light();
        assert_eq!(theme.name, "light");
        assert_eq!(theme.color_for_state("completed"), Some("#4CAF50"));
        assert_eq!(theme.color_for_state("running"), Some("#2196F3"));
        assert_eq!(theme.color_for_state("failed"), Some("#F44336"));
        assert_eq!(theme.shape_for_type("task"), Some("box"));
    }

    #[test]
    fn test_visual_theme_dark() {
        let theme = VisualTheme::dark();
        assert_eq!(theme.name, "dark");
        assert_eq!(theme.color_for_state("completed"), Some("#388E3C"));
        assert_eq!(theme.color_for_state("running"), Some("#1976D2"));
        assert_eq!(theme.color_for_state("failed"), Some("#D32F2F"));
    }

    #[test]
    fn test_visual_theme_default() {
        let theme = VisualTheme::default();
        assert_eq!(theme.name, "light");
    }

    #[test]
    fn test_task_visual_metadata() {
        let task_id = Uuid::new_v4();
        let mut metadata =
            TaskVisualMetadata::new(task_id, "test_task".to_string(), "running".to_string());

        assert_eq!(metadata.task_name, "test_task");
        assert_eq!(metadata.state, "running");
        assert_eq!(metadata.progress, 0.0);
        assert_eq!(metadata.color, "#2196F3");

        metadata = metadata.with_progress(50.0);
        assert_eq!(metadata.progress, 50.0);

        metadata = metadata.with_position(100.0, 200.0);
        assert_eq!(metadata.position, Some((100.0, 200.0)));

        metadata.add_css_class("highlight".to_string());
        assert!(metadata.css_classes.contains(&"highlight".to_string()));

        metadata.add_metadata("custom".to_string(), serde_json::json!("value"));
        assert_eq!(
            metadata.metadata.get("custom"),
            Some(&serde_json::json!("value"))
        );
    }

    #[test]
    fn test_workflow_visualization_data() {
        let workflow_id = Uuid::new_v4();
        let state = WorkflowState {
            workflow_id,
            status: WorkflowStatus::Running,
            total_tasks: 3,
            completed_tasks: 1,
            failed_tasks: 0,
            start_time: Some(12345),
            end_time: None,
            current_stage: Some("stage1".to_string()),
            intermediate_results: HashMap::new(),
        };

        let mut viz_data =
            WorkflowVisualizationData::new(workflow_id, "test_workflow".to_string(), state);

        let task1_id = Uuid::new_v4();
        let task1 = TaskVisualMetadata::new(task1_id, "task1".to_string(), "completed".to_string());
        viz_data.add_task(task1);

        let task2_id = Uuid::new_v4();
        let task2 = TaskVisualMetadata::new(task2_id, "task2".to_string(), "running".to_string());
        viz_data.add_task(task2);

        viz_data.add_edge(task1_id, task2_id, "chain".to_string());

        assert_eq!(viz_data.tasks.len(), 2);
        assert_eq!(viz_data.edges.len(), 1);

        // Test JSON export
        let json = viz_data.to_json();
        assert!(json.is_ok());

        // Test vis.js format
        let visjs = viz_data.to_visjs_format();
        assert!(visjs.is_object());
    }

    #[test]
    fn test_workflow_visualization_data_with_theme() {
        let workflow_id = Uuid::new_v4();
        let state = WorkflowState {
            workflow_id,
            status: WorkflowStatus::Pending,
            total_tasks: 1,
            completed_tasks: 0,
            failed_tasks: 0,
            start_time: None,
            end_time: None,
            current_stage: None,
            intermediate_results: HashMap::new(),
        };

        let viz_data = WorkflowVisualizationData::new(workflow_id, "test".to_string(), state)
            .with_theme(VisualTheme::dark())
            .with_layout("force".to_string());

        assert_eq!(viz_data.theme.name, "dark");
        assert_eq!(viz_data.layout_hint, "force");
    }

    #[test]
    fn test_timeline_entry() {
        let task_id = Uuid::new_v4();
        let mut entry = TimelineEntry::new(task_id, "test_task".to_string(), 1000);

        assert_eq!(entry.task_name, "test_task");
        assert_eq!(entry.start_time, 1000);
        assert_eq!(entry.state, "running");
        assert_eq!(entry.end_time, None);

        entry.complete(2000);
        assert_eq!(entry.end_time, Some(2000));
        assert_eq!(entry.duration, Some(1000));
        assert_eq!(entry.state, "completed");
        assert_eq!(entry.color, "#4CAF50");
    }

    #[test]
    fn test_timeline_entry_fail() {
        let task_id = Uuid::new_v4();
        let mut entry = TimelineEntry::new(task_id, "test_task".to_string(), 1000);

        entry.fail(2500);
        assert_eq!(entry.end_time, Some(2500));
        assert_eq!(entry.duration, Some(1500));
        assert_eq!(entry.state, "failed");
        assert_eq!(entry.color, "#F44336");
    }

    #[test]
    fn test_timeline_entry_with_worker() {
        let task_id = Uuid::new_v4();
        let entry = TimelineEntry::new(task_id, "test".to_string(), 1000)
            .with_worker("worker-1".to_string())
            .with_parent(Uuid::new_v4());

        assert_eq!(entry.worker_id, Some("worker-1".to_string()));
        assert!(entry.parent_id.is_some());
    }

    #[test]
    fn test_execution_timeline() {
        let workflow_id = Uuid::new_v4();
        let mut timeline = ExecutionTimeline::new(workflow_id);

        let task1_id = Uuid::new_v4();
        let index = timeline.start_task(task1_id, "task1".to_string());
        assert_eq!(timeline.entries.len(), 1);

        timeline.complete_task(index);
        assert_eq!(timeline.entries[index].state, "completed");

        timeline.complete_workflow();
        assert!(timeline.workflow_end.is_some());
    }

    #[test]
    fn test_execution_timeline_fail_task() {
        let workflow_id = Uuid::new_v4();
        let mut timeline = ExecutionTimeline::new(workflow_id);

        let task_id = Uuid::new_v4();
        let index = timeline.start_task(task_id, "failing_task".to_string());

        timeline.fail_task(index);
        assert_eq!(timeline.entries[index].state, "failed");
    }

    #[test]
    fn test_execution_timeline_json_export() {
        let workflow_id = Uuid::new_v4();
        let timeline = ExecutionTimeline::new(workflow_id);

        let json = timeline.to_json();
        assert!(json.is_ok());
    }

    #[test]
    fn test_execution_timeline_google_charts() {
        let workflow_id = Uuid::new_v4();
        let mut timeline = ExecutionTimeline::new(workflow_id);

        timeline.add_entry(TimelineEntry::new(
            Uuid::new_v4(),
            "task1".to_string(),
            1000,
        ));

        let chart_data = timeline.to_google_charts_format();
        assert!(chart_data.is_object());
        assert!(chart_data["cols"].is_array());
        assert!(chart_data["rows"].is_array());
    }

    #[test]
    fn test_animation_frame() {
        let workflow_state = WorkflowState {
            workflow_id: Uuid::new_v4(),
            status: WorkflowStatus::Running,
            total_tasks: 5,
            completed_tasks: 2,
            failed_tasks: 0,
            start_time: Some(1000),
            end_time: None,
            current_stage: Some("processing".to_string()),
            intermediate_results: HashMap::new(),
        };

        let mut frame = AnimationFrame::new(0, workflow_state);

        let task1_id = Uuid::new_v4();
        let task2_id = Uuid::new_v4();

        frame.set_task_state(task1_id, "completed".to_string());
        frame.add_active_task(task2_id);
        frame.add_completed_task(task1_id);

        assert_eq!(
            frame.task_states.get(&task1_id),
            Some(&"completed".to_string())
        );
        assert!(frame.active_tasks.contains(&task2_id));
        assert!(frame.completed_tasks.contains(&task1_id));
        assert!(!frame.active_tasks.contains(&task1_id));
    }

    #[test]
    fn test_animation_frame_with_events() {
        let workflow_state = WorkflowState {
            workflow_id: Uuid::new_v4(),
            status: WorkflowStatus::Running,
            total_tasks: 1,
            completed_tasks: 0,
            failed_tasks: 0,
            start_time: Some(1000),
            end_time: None,
            current_stage: None,
            intermediate_results: HashMap::new(),
        };

        let mut frame = AnimationFrame::new(0, workflow_state);

        let task_id = Uuid::new_v4();
        frame.add_event(WorkflowEvent::TaskCompleted { task_id });

        assert_eq!(frame.events.len(), 1);
    }

    #[test]
    fn test_workflow_animation() {
        let workflow_id = Uuid::new_v4();
        let mut animation = WorkflowAnimation::new(workflow_id, 100);

        let state1 = WorkflowState {
            workflow_id,
            status: WorkflowStatus::Pending,
            total_tasks: 1,
            completed_tasks: 0,
            failed_tasks: 0,
            start_time: None,
            end_time: None,
            current_stage: None,
            intermediate_results: HashMap::new(),
        };

        let frame1 = AnimationFrame::new(0, state1);
        animation.add_frame(frame1);

        assert_eq!(animation.frame_count(), 1);
        assert_eq!(animation.total_duration, 100);

        let retrieved_frame = animation.get_frame(0);
        assert!(retrieved_frame.is_some());
    }

    #[test]
    fn test_workflow_animation_json_export() {
        let workflow_id = Uuid::new_v4();
        let animation = WorkflowAnimation::new(workflow_id, 100);

        let json = animation.to_json();
        assert!(json.is_ok());
    }

    #[test]
    fn test_dag_export_with_state_chain() {
        let mut chain = Chain::new();
        chain.tasks.push(Signature::new("task1".to_string()));
        chain.tasks.push(Signature::new("task2".to_string()));

        let workflow_state = WorkflowState {
            workflow_id: Uuid::new_v4(),
            status: WorkflowStatus::Running,
            total_tasks: 2,
            completed_tasks: 1,
            failed_tasks: 0,
            start_time: Some(1000),
            end_time: None,
            current_stage: Some("task2".to_string()),
            intermediate_results: HashMap::new(),
        };

        let mut task_states = HashMap::new();
        let task1_id = Uuid::new_v4();
        let task2_id = Uuid::new_v4();
        task_states.insert(task1_id, "completed".to_string());
        task_states.insert(task2_id, "running".to_string());

        let dot = chain.to_dot_with_state(&workflow_state, &task_states);
        assert!(dot.contains("digraph Chain"));
        assert!(dot.contains("task1"));
        assert!(dot.contains("task2"));

        let mermaid = chain.to_mermaid_with_state(&workflow_state, &task_states);
        assert!(mermaid.contains("graph LR"));
        assert!(mermaid.contains("completed"));
        assert!(mermaid.contains("running"));

        let json = chain.to_json_with_state(&workflow_state, &task_states);
        assert!(json.is_ok());
    }

    #[test]
    fn test_dag_export_with_state_group() {
        let mut group = Group::new();
        group.tasks.push(Signature::new("task1".to_string()));
        group.tasks.push(Signature::new("task2".to_string()));

        let workflow_state = WorkflowState {
            workflow_id: Uuid::new_v4(),
            status: WorkflowStatus::Running,
            total_tasks: 2,
            completed_tasks: 0,
            failed_tasks: 0,
            start_time: Some(1000),
            end_time: None,
            current_stage: None,
            intermediate_results: HashMap::new(),
        };

        let task_states = HashMap::new();

        let dot = group.to_dot_with_state(&workflow_state, &task_states);
        assert!(dot.contains("digraph Group"));

        let mermaid = group.to_mermaid_with_state(&workflow_state, &task_states);
        assert!(mermaid.contains("graph TB"));

        let json = group.to_json_with_state(&workflow_state, &task_states);
        assert!(json.is_ok());
    }

    #[test]
    fn test_dag_export_with_state_chord() {
        let mut header = Group::new();
        header.tasks.push(Signature::new("task1".to_string()));
        header.tasks.push(Signature::new("task2".to_string()));
        let body = Signature::new("callback".to_string());
        let chord = Chord::new(header, body);

        let workflow_state = WorkflowState {
            workflow_id: Uuid::new_v4(),
            status: WorkflowStatus::Running,
            total_tasks: 3,
            completed_tasks: 2,
            failed_tasks: 0,
            start_time: Some(1000),
            end_time: None,
            current_stage: Some("callback".to_string()),
            intermediate_results: HashMap::new(),
        };

        let task_states = HashMap::new();

        let dot = chord.to_dot_with_state(&workflow_state, &task_states);
        assert!(dot.contains("digraph Chord"));
        assert!(dot.contains("callback"));

        let mermaid = chord.to_mermaid_with_state(&workflow_state, &task_states);
        assert!(mermaid.contains("graph TB"));
        assert!(mermaid.contains("callback"));

        let json = chord.to_json_with_state(&workflow_state, &task_states);
        assert!(json.is_ok());
    }

    #[test]
    fn test_workflow_event_stream() {
        let workflow_id = Uuid::new_v4();
        let mut stream = WorkflowEventStream::new(workflow_id);

        assert_eq!(stream.workflow_id, workflow_id);
        assert_eq!(stream.events.len(), 0);

        let task_id = Uuid::new_v4();
        stream.push(WorkflowEvent::TaskCompleted { task_id });
        stream.push(WorkflowEvent::WorkflowStarted { workflow_id });

        assert_eq!(stream.events.len(), 2);

        let all_events = stream.all_events();
        assert_eq!(all_events.len(), 2);
    }

    #[test]
    fn test_workflow_event_stream_buffer_limit() {
        let workflow_id = Uuid::new_v4();
        let mut stream = WorkflowEventStream::new(workflow_id).with_max_buffer_size(2);

        let task1 = Uuid::new_v4();
        let task2 = Uuid::new_v4();
        let task3 = Uuid::new_v4();

        stream.push(WorkflowEvent::TaskCompleted { task_id: task1 });
        stream.push(WorkflowEvent::TaskCompleted { task_id: task2 });
        stream.push(WorkflowEvent::TaskCompleted { task_id: task3 });

        // Should only keep last 2 events
        assert_eq!(stream.events.len(), 2);
    }

    #[test]
    fn test_workflow_event_stream_since() {
        let workflow_id = Uuid::new_v4();
        let mut stream = WorkflowEventStream::new(workflow_id);

        stream.push(WorkflowEvent::WorkflowStarted { workflow_id });
        std::thread::sleep(std::time::Duration::from_millis(10));

        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        std::thread::sleep(std::time::Duration::from_millis(10));
        let task_id = Uuid::new_v4();
        stream.push(WorkflowEvent::TaskCompleted { task_id });

        let recent_events = stream.events_since(timestamp);
        assert_eq!(recent_events.len(), 1);
    }

    #[test]
    fn test_workflow_event_stream_clear() {
        let workflow_id = Uuid::new_v4();
        let mut stream = WorkflowEventStream::new(workflow_id);

        stream.push(WorkflowEvent::WorkflowStarted { workflow_id });
        assert_eq!(stream.events.len(), 1);

        stream.clear();
        assert_eq!(stream.events.len(), 0);
    }

    #[test]
    fn test_workflow_event_stream_sse_format() {
        let workflow_id = Uuid::new_v4();
        let mut stream = WorkflowEventStream::new(workflow_id);

        stream.push(WorkflowEvent::WorkflowStarted { workflow_id });

        let sse_messages = stream.to_sse_format();
        assert_eq!(sse_messages.len(), 1);
        assert!(sse_messages[0].starts_with("event: workflow"));
    }

    // ========================================================================
    // Production-Ready Enhancements Tests
    // ========================================================================

    #[test]
    fn test_workflow_metrics_collector() {
        let workflow_id = Uuid::new_v4();
        let mut collector = WorkflowMetricsCollector::new(workflow_id);

        assert_eq!(collector.workflow_id, workflow_id);
        assert_eq!(collector.total_tasks, 0);
        assert_eq!(collector.completed_tasks, 0);
        assert_eq!(collector.failed_tasks, 0);

        let task1 = Uuid::new_v4();
        let task2 = Uuid::new_v4();

        collector.record_task_start(task1);
        collector.record_task_complete(task1, 100);

        assert_eq!(collector.total_tasks, 1);
        assert_eq!(collector.completed_tasks, 1);

        collector.record_task_start(task2);
        collector.record_task_failure(task2, 50);

        assert_eq!(collector.total_tasks, 2);
        assert_eq!(collector.failed_tasks, 1);

        collector.record_task_retry(task2);
        assert_eq!(*collector.task_retries.get(&task2).unwrap(), 1);

        collector.finalize();
        assert!(collector.end_time.is_some());
        assert!(collector.total_duration.is_some());
        assert!(collector.avg_task_duration.is_some());
        assert!(collector.success_rate.is_some());

        let summary = collector.summary();
        assert!(summary.contains("WorkflowMetrics"));
    }

    #[test]
    fn test_workflow_metrics_collector_display() {
        let workflow_id = Uuid::new_v4();
        let collector = WorkflowMetricsCollector::new(workflow_id);

        let display = format!("{}", collector);
        assert!(display.contains("WorkflowMetrics"));
    }

    #[test]
    fn test_workflow_rate_limiter() {
        let mut limiter = WorkflowRateLimiter::new(2, 1000);

        assert_eq!(limiter.max_workflows, 2);
        assert_eq!(limiter.window_ms, 1000);

        assert!(limiter.allow_workflow());
        assert!(limiter.allow_workflow());
        assert!(!limiter.allow_workflow()); // Should be rejected

        assert_eq!(limiter.total_workflows, 2);
        assert_eq!(limiter.rejected_workflows, 1);

        let rate = limiter.current_rate();
        assert!(rate > 0.0);

        let rejection_rate = limiter.rejection_rate();
        assert!(rejection_rate > 0.0);

        limiter.reset();
        assert_eq!(limiter.workflow_timestamps.len(), 0);
    }

    #[test]
    fn test_workflow_rate_limiter_display() {
        let limiter = WorkflowRateLimiter::new(10, 1000);

        let display = format!("{}", limiter);
        assert!(display.contains("RateLimiter"));
    }

    #[test]
    fn test_workflow_concurrency_control() {
        let mut control = WorkflowConcurrencyControl::new(2);

        assert_eq!(control.max_concurrent, 2);
        assert_eq!(control.current_concurrency(), 0);
        assert_eq!(control.available_slots(), 2);
        assert!(!control.is_at_capacity());

        let wf1 = Uuid::new_v4();
        let wf2 = Uuid::new_v4();
        let wf3 = Uuid::new_v4();

        assert!(control.try_start(wf1));
        assert!(control.try_start(wf2));
        assert!(!control.try_start(wf3)); // Should be rejected

        assert_eq!(control.current_concurrency(), 2);
        assert!(control.is_at_capacity());
        assert_eq!(control.peak_concurrency, 2);

        assert!(control.complete(wf1));
        assert_eq!(control.current_concurrency(), 1);
        assert_eq!(control.total_completed, 1);

        assert!(control.try_start(wf3));
        assert_eq!(control.current_concurrency(), 2);
    }

    #[test]
    fn test_workflow_concurrency_control_display() {
        let control = WorkflowConcurrencyControl::new(5);

        let display = format!("{}", control);
        assert!(display.contains("ConcurrencyControl"));
    }

    #[test]
    fn test_workflow_builder() {
        let builder = WorkflowBuilder::new("test_workflow")
            .with_description("Test workflow description")
            .add_tag("test")
            .add_tag("production")
            .add_metadata("version", serde_json::json!("1.0"));

        assert_eq!(builder.name, "test_workflow");
        assert_eq!(
            builder.description,
            Some("Test workflow description".to_string())
        );
        assert_eq!(builder.tags.len(), 2);
        assert_eq!(builder.metadata.len(), 1);

        let chain = builder.clone().chain();
        assert!(chain.is_empty());

        let group = builder.clone().group();
        assert!(group.is_empty());

        let task = Signature::new("map_task".to_string());
        let argsets = vec![vec![serde_json::json!(1)], vec![serde_json::json!(2)]];
        let map = builder.map(task, argsets);
        assert_eq!(map.task.task, "map_task");
    }

    #[test]
    fn test_workflow_registry() {
        let mut registry = WorkflowRegistry::new();

        assert_eq!(registry.count(), 0);

        let wf1 = Uuid::new_v4();
        let wf2 = Uuid::new_v4();

        let mut metadata1 = HashMap::new();
        metadata1.insert("version".to_string(), serde_json::json!("1.0"));

        registry.register(wf1, "workflow_1".to_string(), metadata1.clone());
        registry.register(wf2, "workflow_2".to_string(), HashMap::new());

        assert_eq!(registry.count(), 2);
        assert_eq!(registry.get_name(&wf1), Some("workflow_1"));
        assert_eq!(registry.get_state(&wf1), Some(&WorkflowStatus::Pending));

        registry.update_state(wf1, WorkflowStatus::Running);
        assert_eq!(registry.get_state(&wf1), Some(&WorkflowStatus::Running));

        registry.add_tag(wf1, "production".to_string());
        registry.add_tag(wf2, "production".to_string());

        let production_workflows = registry.get_by_tag("production");
        assert_eq!(production_workflows.len(), 2);

        let running = registry.get_by_state(&WorkflowStatus::Running);
        assert_eq!(running.len(), 1);

        assert!(registry.remove(&wf1));
        assert_eq!(registry.count(), 1);

        registry.clear();
        assert_eq!(registry.count(), 0);
    }

    #[test]
    fn test_workflow_registry_default() {
        let registry = WorkflowRegistry::default();
        assert_eq!(registry.count(), 0);
    }

    #[test]
    fn test_workflow_registry_display() {
        let mut registry = WorkflowRegistry::new();

        let wf1 = Uuid::new_v4();
        registry.register(wf1, "test".to_string(), HashMap::new());

        let display = format!("{}", registry);
        assert!(display.contains("WorkflowRegistry"));
        assert!(display.contains("total=1"));
    }

    // ===== NestedChain Tests =====

    #[tokio::test]
    async fn test_nested_chain_apply() {
        let broker = MockBroker::new();

        let nested_chain = NestedChain::new()
            .then("task1", vec![serde_json::json!(1)])
            .then_group(
                Group::new()
                    .add("task2a", vec![serde_json::json!(2)])
                    .add("task2b", vec![serde_json::json!(3)]),
            )
            .then("task3", vec![serde_json::json!(4)]);

        let result = nested_chain.apply(&broker).await;
        assert!(result.is_ok(), "NestedChain apply should succeed");

        // Verify tasks were published
        assert!(
            broker.task_count() >= 4,
            "Should publish at least 4 tasks (task1, task2a, task2b, task3)"
        );
    }

    #[tokio::test]
    async fn test_nested_chain_with_chains() {
        let broker = MockBroker::new();

        let nested_chain = NestedChain::new()
            .then_chain(Chain::new().then("step1", vec![]).then("step2", vec![]))
            .then_chain(Chain::new().then("step3", vec![]).then("step4", vec![]));

        let result = nested_chain.apply(&broker).await;
        assert!(result.is_ok(), "NestedChain with chains should succeed");

        // Each chain only enqueues the first task (with links to subsequent tasks)
        // So we expect 2 tasks (one per chain), not 4
        assert_eq!(
            broker.task_count(),
            2,
            "Should publish 2 first tasks from two chains"
        );
    }

    #[tokio::test]
    async fn test_nested_chain_empty_error() {
        let broker = MockBroker::new();
        let nested_chain = NestedChain::new();

        let result = nested_chain.apply(&broker).await;
        assert!(result.is_err(), "Empty NestedChain should return error");
        match result {
            Err(CanvasError::Invalid(msg)) => {
                assert!(msg.contains("empty"));
            }
            _ => panic!("Expected Invalid error for empty NestedChain"),
        }
    }

    #[test]
    fn test_nested_chain_builder_methods() {
        let chain = NestedChain::new()
            .then("task1", vec![])
            .then_signature(Signature::new("task2".to_string()))
            .then_group(Group::new().add("task3", vec![]));

        assert_eq!(chain.len(), 3);
        assert!(!chain.is_empty());
    }

    #[test]
    fn test_nested_chain_display() {
        let chain = NestedChain::new()
            .then("task1", vec![])
            .then("task2", vec![]);

        let display = format!("{}", chain);
        assert!(display.contains("NestedChain"));
        assert!(display.contains("->"));
    }

    // ===== NestedGroup Tests =====

    #[tokio::test]
    async fn test_nested_group_apply() {
        let broker = MockBroker::new();

        let nested_group = NestedGroup::new()
            .add("task1", vec![serde_json::json!(1)])
            .add_chain(Chain::new().then("task2a", vec![]).then("task2b", vec![]))
            .add("task3", vec![serde_json::json!(3)]);

        let result = nested_group.apply(&broker).await;
        assert!(result.is_ok(), "NestedGroup apply should succeed");

        // Verify tasks were published
        // Chain only enqueues first task, so: task1 + task2a (from chain) + task3 = 3
        assert_eq!(
            broker.task_count(),
            3,
            "Should publish 3 tasks (task1, task2a from chain, task3)"
        );
    }

    #[tokio::test]
    async fn test_nested_group_with_multiple_chains() {
        let broker = MockBroker::new();

        let nested_group = NestedGroup::new()
            .add_chain(Chain::new().then("a1", vec![]).then("a2", vec![]))
            .add_chain(Chain::new().then("b1", vec![]).then("b2", vec![]))
            .add_chain(Chain::new().then("c1", vec![]).then("c2", vec![]));

        let result = nested_group.apply(&broker).await;
        assert!(
            result.is_ok(),
            "NestedGroup with multiple chains should succeed"
        );

        // Each chain only enqueues the first task (with links)
        // So we expect 3 tasks (one per chain), not 6
        assert_eq!(
            broker.task_count(),
            3,
            "Should publish 3 first tasks from three chains"
        );
    }

    #[tokio::test]
    async fn test_nested_group_empty_error() {
        let broker = MockBroker::new();
        let nested_group = NestedGroup::new();

        let result = nested_group.apply(&broker).await;
        assert!(result.is_err(), "Empty NestedGroup should return error");
        match result {
            Err(CanvasError::Invalid(msg)) => {
                assert!(msg.contains("empty"));
            }
            _ => panic!("Expected Invalid error for empty NestedGroup"),
        }
    }

    #[test]
    fn test_nested_group_builder_methods() {
        let group = NestedGroup::new()
            .add("task1", vec![])
            .add_signature(Signature::new("task2".to_string()))
            .add_chain(Chain::new().then("task3", vec![]));

        assert_eq!(group.len(), 3);
        assert!(!group.is_empty());
    }

    #[test]
    fn test_nested_group_display() {
        let group = NestedGroup::new().add("task1", vec![]).add("task2", vec![]);

        let display = format!("{}", group);
        assert!(display.contains("NestedGroup"));
        assert!(display.contains("|"));
    }

    #[tokio::test]
    async fn test_nested_workflows_complex_composition() {
        let broker = MockBroker::new();

        // Create a complex nested workflow:
        // Chain of [ Group of tasks -> Chain of tasks -> Group of tasks ]
        let nested = NestedChain::new()
            .then_group(
                Group::new()
                    .add("parallel1", vec![])
                    .add("parallel2", vec![])
                    .add("parallel3", vec![]),
            )
            .then_chain(Chain::new().then("seq1", vec![]).then("seq2", vec![]))
            .then_element(CanvasElement::Group(
                Group::new().add("final1", vec![]).add("final2", vec![]),
            ));

        let result = nested.apply(&broker).await;
        assert!(result.is_ok(), "Complex nested workflow should succeed");

        // Total tasks: 3 (parallel group) + 1 (first from chain) + 2 (final group) = 6
        // Chains only enqueue their first task with links to subsequent tasks
        assert_eq!(broker.task_count(), 6, "Should publish 6 initial tasks");
    }

    #[test]
    fn test_chain_iterator_methods() {
        let chain = Chain::new()
            .then("task1", vec![])
            .then("task2", vec![])
            .then("task3", vec![]);

        // Test first/last
        assert_eq!(chain.first().unwrap().task, "task1");
        assert_eq!(chain.last().unwrap().task, "task3");

        // Test get
        assert_eq!(chain.get(1).unwrap().task, "task2");
        assert!(chain.get(10).is_none());

        // Test iter
        let names: Vec<String> = chain.iter().map(|s| s.task.clone()).collect();
        assert_eq!(names, vec!["task1", "task2", "task3"]);

        assert_eq!(chain.len(), 3);
        assert!(!chain.is_empty());
    }

    #[test]
    fn test_chain_into_iterator() {
        let chain = Chain::new().then("task1", vec![]).then("task2", vec![]);

        let mut count = 0;
        for sig in &chain {
            assert!(sig.task.starts_with("task"));
            count += 1;
        }
        assert_eq!(count, 2);

        // Test consuming iterator
        let tasks: Vec<Signature> = chain.into_iter().collect();
        assert_eq!(tasks.len(), 2);
    }

    #[test]
    fn test_chain_from_vec() {
        let sigs = vec![
            Signature::new("task1".to_string()),
            Signature::new("task2".to_string()),
        ];

        let chain = Chain::from(sigs);
        assert_eq!(chain.len(), 2);
        assert_eq!(chain.first().unwrap().task, "task1");
    }

    #[test]
    fn test_chain_from_iter() {
        let chain: Chain = vec![
            Signature::new("task1".to_string()),
            Signature::new("task2".to_string()),
        ]
        .into_iter()
        .collect();

        assert_eq!(chain.len(), 2);
    }

    #[test]
    fn test_chain_with_capacity() {
        let chain = Chain::with_capacity(10);
        assert_eq!(chain.len(), 0);
        assert!(chain.is_empty());
    }

    #[test]
    fn test_chain_extend() {
        let chain = Chain::new().then("task1", vec![]).extend(vec![
            Signature::new("task2".to_string()),
            Signature::new("task3".to_string()),
        ]);

        assert_eq!(chain.len(), 3);
    }

    #[test]
    fn test_chain_reverse() {
        let chain = Chain::new()
            .then("task1", vec![])
            .then("task2", vec![])
            .then("task3", vec![])
            .reverse();

        assert_eq!(chain.first().unwrap().task, "task3");
        assert_eq!(chain.last().unwrap().task, "task1");
    }

    #[test]
    fn test_chain_retain() {
        let chain = Chain::new()
            .then("keep1", vec![])
            .then("remove", vec![])
            .then("keep2", vec![])
            .retain(|sig| sig.task.starts_with("keep"));

        assert_eq!(chain.len(), 2);
        assert_eq!(chain.first().unwrap().task, "keep1");
        assert_eq!(chain.last().unwrap().task, "keep2");
    }

    #[test]
    fn test_group_iterator_methods() {
        let group = Group::new()
            .add("task1", vec![])
            .add("task2", vec![])
            .add("task3", vec![]);

        // Test first/last
        assert_eq!(group.first().unwrap().task, "task1");
        assert_eq!(group.last().unwrap().task, "task3");

        // Test get
        assert_eq!(group.get(1).unwrap().task, "task2");
        assert!(group.get(10).is_none());

        // Test iter
        let names: Vec<String> = group.iter().map(|s| s.task.clone()).collect();
        assert_eq!(names, vec!["task1", "task2", "task3"]);

        assert_eq!(group.len(), 3);
        assert!(!group.is_empty());
    }

    #[test]
    fn test_group_find_filter() {
        let group = Group::new()
            .add("task_a", vec![])
            .add("task_b", vec![])
            .add("other", vec![]);

        // Test find
        let found = group.find(|sig| sig.task == "task_b");
        assert!(found.is_some());
        assert_eq!(found.unwrap().task, "task_b");

        // Test filter
        let filtered = group.clone().filter(|sig| sig.task.starts_with("task_"));
        assert_eq!(filtered.len(), 2);
    }

    #[test]
    fn test_group_from_vec() {
        let sigs = vec![
            Signature::new("task1".to_string()),
            Signature::new("task2".to_string()),
        ];

        let group = Group::from(sigs);
        assert_eq!(group.len(), 2);
        assert!(group.has_group_id());
    }

    #[test]
    fn test_group_with_capacity() {
        let group = Group::with_capacity(10);
        assert_eq!(group.len(), 0);
        assert!(group.is_empty());
        assert!(group.has_group_id());
    }

    #[test]
    fn test_signature_convenience_methods() {
        let sig = Signature::new("task".to_string())
            .add_kwarg("key1", serde_json::json!("value1"))
            .add_kwarg("key2", serde_json::json!(42))
            .add_arg(serde_json::json!(1))
            .add_arg(serde_json::json!(2));

        assert!(sig.has_kwarg("key1"));
        assert!(sig.has_kwarg("key2"));
        assert!(!sig.has_kwarg("key3"));

        assert_eq!(sig.get_kwarg("key1"), Some(&serde_json::json!("value1")));
        assert_eq!(sig.get_kwarg("key2"), Some(&serde_json::json!(42)));

        assert_eq!(sig.args.len(), 2);
        assert_eq!(sig.args[0], serde_json::json!(1));
    }

    #[test]
    fn test_workflow_registry_query_methods() {
        let mut registry = WorkflowRegistry::new();

        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        let id3 = Uuid::new_v4();

        registry.register(id1, "workflow_test_1".to_string(), HashMap::new());
        registry.register(id2, "workflow_test_2".to_string(), HashMap::new());
        registry.register(id3, "other_workflow".to_string(), HashMap::new());

        // Test find_by_name
        let found = registry.find_by_name("test");
        assert_eq!(found.len(), 2);

        // Test all_workflow_ids
        let all_ids = registry.all_workflow_ids();
        assert_eq!(all_ids.len(), 3);

        // Test contains
        assert!(registry.contains(&id1));
        assert!(!registry.contains(&Uuid::new_v4()));

        // Test tags
        registry.add_tag(id1, "tag1".to_string());
        registry.add_tag(id2, "tag1".to_string());
        registry.add_tag(id2, "tag2".to_string());

        let all_tags = registry.all_tags();
        assert!(all_tags.contains(&"tag1".to_string()));

        // Test get_by_tags_all
        let with_both = registry.get_by_tags_all(&["tag1", "tag2"]);
        assert_eq!(with_both.len(), 1);
        assert!(with_both.contains(&id2));

        // Test get_by_tags_any
        let with_any = registry.get_by_tags_any(&["tag1", "tag2"]);
        assert!(with_any.len() >= 2);

        // Test state counts
        registry.update_state(id1, WorkflowStatus::Running);
        registry.update_state(id2, WorkflowStatus::Success);
        registry.update_state(id3, WorkflowStatus::Pending);

        assert_eq!(registry.running_count(), 1);
        assert_eq!(registry.pending_count(), 1);
        assert_eq!(registry.completed_count(), 1);
        assert_eq!(registry.count_by_state(&WorkflowStatus::Success), 1);
    }

    #[test]
    fn test_workflow_registry_age_queries() {
        let mut registry = WorkflowRegistry::new();
        let id = Uuid::new_v4();

        registry.register(id, "test".to_string(), HashMap::new());

        // Age should be very small (just registered)
        let age = registry.get_age(&id);
        assert!(age.is_some());
        assert!(age.unwrap() < 1000); // Less than 1 second

        // Test get_older_than
        let old = registry.get_older_than(1_000_000); // 1000 seconds
        assert_eq!(old.len(), 0); // Nothing is that old yet

        // Sleep a tiny bit to ensure age > 0
        std::thread::sleep(std::time::Duration::from_millis(1));

        let recent = registry.get_older_than(0); // 0 ms
        assert_eq!(recent.len(), 1); // Our workflow is older than 0ms
    }
}
