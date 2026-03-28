#![cfg(test)]

use crate::*;
use std::collections::HashMap;
use uuid::Uuid;

#[test]
fn test_signature_creation() {
    let sig = Signature::new("test_task".to_string())
        .with_args(vec![serde_json::json!(1), serde_json::json!(2)])
        .with_priority(9);

    assert_eq!(sig.task, "test_task");
    assert_eq!(sig.args.len(), 2);
    assert_eq!(sig.options.priority, Some(9));
}

#[test]
fn test_signature_predicates() {
    let sig = Signature::new("task".to_string())
        .with_args(vec![serde_json::json!(1)])
        .with_kwargs({
            let mut map = HashMap::new();
            map.insert("key".to_string(), serde_json::json!("value"));
            map
        })
        .immutable();

    assert!(sig.has_args());
    assert!(sig.has_kwargs());
    assert!(sig.is_immutable());
}

#[test]
fn test_signature_display() {
    let sig = Signature::new("my_task".to_string())
        .with_args(vec![serde_json::json!(1), serde_json::json!(2)])
        .immutable();

    let display = format!("{}", sig);
    assert!(display.contains("Signature[task=my_task]"));
    assert!(display.contains("args=2"));
    assert!(display.contains("(immutable)"));
}

#[test]
fn test_task_options_predicates() {
    let mut opts = TaskOptions::default();
    assert!(!opts.has_priority());
    assert!(!opts.has_queue());
    assert!(!opts.has_task_id());
    assert!(!opts.has_link());
    assert!(!opts.has_link_error());

    opts.priority = Some(5);
    opts.queue = Some("celery".to_string());
    opts.task_id = Some(Uuid::new_v4());
    opts.link = Some(Box::new(Signature::new("link_task".to_string())));
    opts.link_error = Some(Box::new(Signature::new("error_task".to_string())));

    assert!(opts.has_priority());
    assert!(opts.has_queue());
    assert!(opts.has_task_id());
    assert!(opts.has_link());
    assert!(opts.has_link_error());
}

#[test]
fn test_task_options_display() {
    let task_id = Uuid::new_v4();
    let opts = TaskOptions {
        priority: Some(9),
        queue: Some("high_priority".to_string()),
        task_id: Some(task_id),
        link: Some(Box::new(Signature::new("success".to_string()))),
        link_error: Some(Box::new(Signature::new("failure".to_string()))),
        ..Default::default()
    };

    let display = format!("{}", opts);
    assert!(display.contains("TaskOptions"));
    assert!(display.contains("priority=9"));
    assert!(display.contains("queue=high_priority"));
    assert!(display.contains("task_id="));
    assert!(display.contains("link=yes"));
    assert!(display.contains("link_error=yes"));
}

#[test]
fn test_task_options_display_default() {
    let opts = TaskOptions::default();
    let display = format!("{}", opts);
    assert_eq!(display, "TaskOptions[default]");
}

#[test]
fn test_chain_builder() {
    let chain = Chain::new()
        .then("task1", vec![serde_json::json!(1)])
        .then("task2", vec![serde_json::json!(2)])
        .then("task3", vec![serde_json::json!(3)]);

    assert_eq!(chain.tasks.len(), 3);
    assert_eq!(chain.tasks[0].task, "task1");
    assert_eq!(chain.tasks[2].task, "task3");
}

#[test]
fn test_chain_predicates() {
    let chain = Chain::new();
    assert!(chain.is_empty());
    assert_eq!(chain.len(), 0);

    let chain = chain.then("task1", vec![]).then("task2", vec![]);
    assert!(!chain.is_empty());
    assert_eq!(chain.len(), 2);
}

#[test]
fn test_chain_display() {
    let chain = Chain::new()
        .then("first", vec![])
        .then("middle", vec![])
        .then("last", vec![]);

    let display = format!("{}", chain);
    assert!(display.contains("Chain[3 tasks]"));
    assert!(display.contains("first -> ... -> last"));
}

#[test]
fn test_chain_display_empty() {
    let chain = Chain::new();
    let display = format!("{}", chain);
    assert_eq!(display, "Chain[0 tasks]");
}

#[test]
fn test_group_builder() {
    let group = Group::new()
        .add("task1", vec![])
        .add("task2", vec![])
        .add("task3", vec![]);

    assert_eq!(group.tasks.len(), 3);
}

#[test]
fn test_group_predicates() {
    let group = Group::new();
    assert!(group.is_empty());
    assert_eq!(group.len(), 0);
    assert!(group.has_group_id());

    let group = group.add("task1", vec![]).add("task2", vec![]);
    assert!(!group.is_empty());
    assert_eq!(group.len(), 2);
}

#[test]
fn test_group_display() {
    let group = Group::new()
        .add("task1", vec![])
        .add("task2", vec![])
        .add("task3", vec![]);

    let display = format!("{}", group);
    assert!(display.contains("Group[3 tasks]"));
    assert!(display.contains("id="));
}

#[test]
fn test_chord_creation() {
    let header = Group::new().add("task1", vec![]).add("task2", vec![]);

    let body = Signature::new("callback".to_string());

    let chord = Chord::new(header, body);

    assert_eq!(chord.header.tasks.len(), 2);
    assert_eq!(chord.body.task, "callback");
}

#[test]
fn test_chord_display() {
    let header = Group::new()
        .add("task1", vec![])
        .add("task2", vec![])
        .add("task3", vec![]);
    let body = Signature::new("aggregate".to_string());
    let chord = Chord::new(header, body);

    let display = format!("{}", chord);
    assert!(display.contains("Chord[3 tasks] -> callback(aggregate)"));
}

#[test]
fn test_map_creation() {
    let task = Signature::new("process".to_string());
    let argsets = vec![
        vec![serde_json::json!(1)],
        vec![serde_json::json!(2)],
        vec![serde_json::json!(3)],
    ];

    let map = Map::new(task, argsets);

    assert_eq!(map.argsets.len(), 3);
}

#[test]
fn test_map_predicates() {
    let task = Signature::new("process".to_string());
    let empty_map = Map::new(task.clone(), vec![]);
    assert!(empty_map.is_empty());
    assert_eq!(empty_map.len(), 0);

    let map = Map::new(
        task,
        vec![vec![serde_json::json!(1)], vec![serde_json::json!(2)]],
    );
    assert!(!map.is_empty());
    assert_eq!(map.len(), 2);
}

#[test]
fn test_map_display() {
    let task = Signature::new("process".to_string());
    let argsets = vec![
        vec![serde_json::json!(1)],
        vec![serde_json::json!(2)],
        vec![serde_json::json!(3)],
    ];
    let map = Map::new(task, argsets);

    let display = format!("{}", map);
    assert!(display.contains("Map[task=process, 3 argsets]"));
}

#[test]
fn test_starmap_predicates() {
    let task = Signature::new("add".to_string());
    let empty_starmap = Starmap::new(task.clone(), vec![]);
    assert!(empty_starmap.is_empty());
    assert_eq!(empty_starmap.len(), 0);

    let starmap = Starmap::new(
        task,
        vec![
            vec![serde_json::json!(1), serde_json::json!(2)],
            vec![serde_json::json!(3), serde_json::json!(4)],
        ],
    );
    assert!(!starmap.is_empty());
    assert_eq!(starmap.len(), 2);
}

#[test]
fn test_starmap_display() {
    let task = Signature::new("multiply".to_string());
    let argsets = vec![
        vec![serde_json::json!(2), serde_json::json!(3)],
        vec![serde_json::json!(4), serde_json::json!(5)],
    ];
    let starmap = Starmap::new(task, argsets);

    let display = format!("{}", starmap);
    assert!(display.contains("Starmap[task=multiply, 2 argsets]"));
}

#[test]
fn test_canvas_error_predicates() {
    let invalid_err = CanvasError::Invalid("bad workflow".to_string());
    assert!(invalid_err.is_invalid());
    assert!(!invalid_err.is_broker());
    assert!(!invalid_err.is_serialization());
    assert!(!invalid_err.is_retryable());

    let broker_err = CanvasError::Broker("connection failed".to_string());
    assert!(!broker_err.is_invalid());
    assert!(broker_err.is_broker());
    assert!(!broker_err.is_serialization());
    assert!(broker_err.is_retryable());

    let ser_err = CanvasError::Serialization("bad json".to_string());
    assert!(!ser_err.is_invalid());
    assert!(!ser_err.is_broker());
    assert!(ser_err.is_serialization());
    assert!(!ser_err.is_retryable());
}

#[test]
fn test_canvas_error_category() {
    let invalid_err = CanvasError::Invalid("test".to_string());
    assert_eq!(invalid_err.category(), "invalid");

    let broker_err = CanvasError::Broker("test".to_string());
    assert_eq!(broker_err.category(), "broker");

    let ser_err = CanvasError::Serialization("test".to_string());
    assert_eq!(ser_err.category(), "serialization");

    let cancelled_err = CanvasError::Cancelled("test".to_string());
    assert_eq!(cancelled_err.category(), "cancelled");

    let timeout_err = CanvasError::Timeout("test".to_string());
    assert_eq!(timeout_err.category(), "timeout");
}

#[test]
fn test_canvas_error_display() {
    let err = CanvasError::Invalid("empty chain".to_string());
    assert_eq!(err.to_string(), "Invalid workflow: empty chain");

    let err = CanvasError::Broker("timeout".to_string());
    assert_eq!(err.to_string(), "Broker error: timeout");

    let err = CanvasError::Serialization("malformed json".to_string());
    assert_eq!(err.to_string(), "Serialization error: malformed json");

    let err = CanvasError::Cancelled("user requested".to_string());
    assert_eq!(err.to_string(), "Workflow cancelled: user requested");

    let err = CanvasError::Timeout("exceeded 5s".to_string());
    assert_eq!(err.to_string(), "Workflow timeout: exceeded 5s");
}

#[test]
fn test_cancellation_token() {
    let workflow_id = Uuid::new_v4();
    let token = CancellationToken::new(workflow_id)
        .with_reason("user requested".to_string())
        .cancel_tree();

    assert_eq!(token.workflow_id, workflow_id);
    assert_eq!(token.reason, Some("user requested".to_string()));
    assert!(token.cancel_tree);
    assert!(token.branch_id.is_none());

    let display = format!("{}", token);
    assert!(display.contains("CancellationToken"));
    assert!(display.contains("reason=user requested"));
    assert!(display.contains("(tree)"));
}

#[test]
fn test_workflow_retry_policy() {
    let policy = WorkflowRetryPolicy::new(3)
        .failed_only()
        .with_backoff(2.0, 60)
        .with_initial_delay(1);

    assert_eq!(policy.max_retries, 3);
    assert!(policy.retry_failed_only);
    assert_eq!(policy.backoff_factor, Some(2.0));
    assert_eq!(policy.max_backoff, Some(60));
    assert_eq!(policy.initial_delay, Some(1));

    // Test delay calculation
    assert_eq!(policy.calculate_delay(0), 1); // 1 * 2^0 = 1
    assert_eq!(policy.calculate_delay(1), 2); // 1 * 2^1 = 2
    assert_eq!(policy.calculate_delay(2), 4); // 1 * 2^2 = 4
    assert_eq!(policy.calculate_delay(10), 60); // Capped at max_backoff

    let display = format!("{}", policy);
    assert!(display.contains("WorkflowRetryPolicy"));
    assert!(display.contains("max_retries=3"));
    assert!(display.contains("(failed_only)"));
}

#[test]
fn test_workflow_timeout() {
    let timeout = WorkflowTimeout::new(300)
        .with_stage_timeout(60)
        .with_escalation(TimeoutEscalation::ContinuePartial);

    assert_eq!(timeout.total_timeout, Some(300));
    assert_eq!(timeout.stage_timeout, Some(60));
    assert!(matches!(
        timeout.escalation,
        TimeoutEscalation::ContinuePartial
    ));

    let display = format!("{}", timeout);
    assert!(display.contains("WorkflowTimeout"));
    assert!(display.contains("total=300s"));
    assert!(display.contains("stage=60s"));
}

#[test]
fn test_foreach_loop() {
    let task = Signature::new("process".to_string());
    let items = vec![
        serde_json::json!(1),
        serde_json::json!(2),
        serde_json::json!(3),
    ];
    let foreach = ForEach::new(task, items).with_concurrency(2);

    assert_eq!(foreach.len(), 3);
    assert!(!foreach.is_empty());
    assert_eq!(foreach.concurrency, Some(2));

    let display = format!("{}", foreach);
    assert!(display.contains("ForEach"));
    assert!(display.contains("process"));
    assert!(display.contains("3 items"));
    assert!(display.contains("concurrency=2"));

    let empty = ForEach::new(Signature::new("task".to_string()), vec![]);
    assert!(empty.is_empty());
    assert_eq!(empty.len(), 0);
}

#[test]
fn test_while_loop() {
    let condition = Condition::field_equals("status", serde_json::json!("pending"));
    let body = Signature::new("check".to_string());
    let while_loop = WhileLoop::new(condition, body).with_max_iterations(100);

    assert_eq!(while_loop.max_iterations, Some(100));

    let display = format!("{}", while_loop);
    assert!(display.contains("While"));
    assert!(display.contains("check"));
    assert!(display.contains("max=100"));

    let unlimited = WhileLoop::new(
        Condition::field_equals("x", serde_json::json!(0)),
        Signature::new("task".to_string()),
    )
    .unlimited();
    assert!(unlimited.max_iterations.is_none());
}

#[test]
fn test_workflow_state() {
    let workflow_id = Uuid::new_v4();
    let mut state = WorkflowState::new(workflow_id, 10);

    assert_eq!(state.workflow_id, workflow_id);
    assert_eq!(state.status, WorkflowStatus::Pending);
    assert_eq!(state.total_tasks, 10);
    assert_eq!(state.completed_tasks, 0);
    assert_eq!(state.failed_tasks, 0);
    assert_eq!(state.progress(), 0.0);
    assert!(!state.is_complete());

    // Mark some tasks as completed
    state.mark_completed();
    state.mark_completed();
    state.mark_completed();
    assert_eq!(state.completed_tasks, 3);
    assert_eq!(state.progress(), 30.0);

    // Mark a task as failed
    state.mark_failed();
    assert_eq!(state.failed_tasks, 1);

    // Set and get intermediate results
    state.set_result("step1".to_string(), serde_json::json!({"result": 42}));
    assert_eq!(
        state.get_result("step1"),
        Some(&serde_json::json!({"result": 42}))
    );
    assert_eq!(state.get_result("nonexistent"), None);

    // Test completion states
    state.status = WorkflowStatus::Success;
    assert!(state.is_complete());

    state.status = WorkflowStatus::Failed;
    assert!(state.is_complete());

    state.status = WorkflowStatus::Cancelled;
    assert!(state.is_complete());

    state.status = WorkflowStatus::Running;
    assert!(!state.is_complete());

    let display = format!("{}", state);
    assert!(display.contains("WorkflowState"));
    assert!(display.contains("progress=30.0%"));
    assert!(display.contains("failed=1"));
}

#[test]
fn test_dag_export_chain() {
    let chain = Chain::new()
        .then("task1", vec![])
        .then("task2", vec![])
        .then("task3", vec![]);

    // Test DOT export
    let dot = chain.to_dot();
    assert!(dot.contains("digraph Chain"));
    assert!(dot.contains("rankdir=LR"));
    assert!(dot.contains("task1"));
    assert!(dot.contains("task2"));
    assert!(dot.contains("task3"));
    assert!(dot.contains("n0 -> n1"));
    assert!(dot.contains("n1 -> n2"));

    // Test Mermaid export
    let mmd = chain.to_mermaid();
    assert!(mmd.contains("graph LR"));
    assert!(mmd.contains("task1"));
    assert!(mmd.contains("task2"));
    assert!(mmd.contains("task3"));
    assert!(mmd.contains("n0 --> n1"));
    assert!(mmd.contains("n1 --> n2"));

    // Test JSON export
    let json = chain.to_json().unwrap();
    assert!(json.contains("task1"));
    assert!(json.contains("task2"));
    assert!(json.contains("task3"));
}

#[test]
fn test_dag_export_group() {
    let group = Group::new()
        .add("task1", vec![])
        .add("task2", vec![])
        .add("task3", vec![]);

    // Test DOT export
    let dot = group.to_dot();
    assert!(dot.contains("digraph Group"));
    assert!(dot.contains("rankdir=TB"));
    assert!(dot.contains("start"));
    assert!(dot.contains("task1"));
    assert!(dot.contains("task2"));
    assert!(dot.contains("task3"));
    assert!(dot.contains("start -> n0"));
    assert!(dot.contains("start -> n1"));
    assert!(dot.contains("start -> n2"));

    // Test Mermaid export
    let mmd = group.to_mermaid();
    assert!(mmd.contains("graph TB"));
    assert!(mmd.contains("start"));
    assert!(mmd.contains("task1"));
    assert!(mmd.contains("start --> n0"));

    // Test JSON export
    let json = group.to_json().unwrap();
    assert!(json.contains("task1"));
}

#[test]
fn test_dag_export_chord() {
    let header = Group::new().add("task1", vec![]).add("task2", vec![]);
    let body = Signature::new("callback".to_string());
    let chord = Chord::new(header, body);

    // Test DOT export
    let dot = chord.to_dot();
    assert!(dot.contains("digraph Chord"));
    assert!(dot.contains("callback"));
    assert!(dot.contains("task1"));
    assert!(dot.contains("task2"));
    assert!(dot.contains("n0 -> callback"));
    assert!(dot.contains("n1 -> callback"));

    // Test Mermaid export
    let mmd = chord.to_mermaid();
    assert!(mmd.contains("graph TB"));
    assert!(mmd.contains("callback"));
    assert!(mmd.contains("task1"));
    assert!(mmd.contains("n0 --> callback"));

    // Test JSON export
    let json = chord.to_json().unwrap();
    assert!(json.contains("callback"));
    assert!(json.contains("task1"));
}

#[test]
fn test_canvas_error_new_variants() {
    let cancelled = CanvasError::Cancelled("user cancelled".to_string());
    assert!(cancelled.is_cancelled());
    assert!(!cancelled.is_timeout());
    assert!(!cancelled.is_retryable());
    assert_eq!(cancelled.category(), "cancelled");

    let timeout = CanvasError::Timeout("exceeded limit".to_string());
    assert!(timeout.is_timeout());
    assert!(!timeout.is_cancelled());
    assert!(!timeout.is_retryable());
    assert_eq!(timeout.category(), "timeout");
}

#[test]
fn test_named_output() {
    let output = NamedOutput::new("result", serde_json::json!(42)).with_source("task1");

    assert_eq!(output.name, "result");
    assert_eq!(output.value, serde_json::json!(42));
    assert_eq!(output.source, Some("task1".to_string()));
}

#[test]
fn test_result_transform() {
    let extract = ResultTransform::Extract {
        field: "data".to_string(),
    };
    assert!(format!("{}", extract).contains("Extract[data]"));

    let map = ResultTransform::Map {
        task: Box::new(Signature::new("transform".to_string())),
    };
    assert!(format!("{}", map).contains("Map[transform]"));
}

#[test]
fn test_result_cache() {
    let cache = ResultCache::new("task:123")
        .with_policy(CachePolicy::OnSuccess)
        .with_ttl(3600);

    assert_eq!(cache.key, "task:123");
    assert_eq!(cache.ttl, Some(3600));

    let display = format!("{}", cache);
    assert!(display.contains("Cache[key=task:123]"));
    assert!(display.contains("ttl=3600s"));
}

#[test]
fn test_workflow_error_handler() {
    let handler = WorkflowErrorHandler::new(Signature::new("handle_error".to_string()))
        .for_errors(vec!["NetworkError".to_string(), "TimeoutError".to_string()])
        .suppress_error();

    assert_eq!(handler.handler.task, "handle_error");
    assert_eq!(handler.error_types.len(), 2);
    assert!(handler.suppress);

    let display = format!("{}", handler);
    assert!(display.contains("ErrorHandler[handle_error]"));
    assert!(display.contains("(suppress)"));
}

#[test]
fn test_compensation_workflow() {
    let mut workflow = CompensationWorkflow::new();
    assert!(workflow.is_empty());
    assert_eq!(workflow.len(), 0);

    workflow = workflow
        .step(
            Signature::new("create".to_string()),
            Signature::new("delete".to_string()),
        )
        .step(
            Signature::new("update".to_string()),
            Signature::new("rollback".to_string()),
        );

    assert!(!workflow.is_empty());
    assert_eq!(workflow.len(), 2);
    assert_eq!(workflow.forward.len(), 2);
    assert_eq!(workflow.compensations.len(), 2);

    let display = format!("{}", workflow);
    assert!(display.contains("Compensation[2 steps, 2 compensations]"));
}

#[test]
fn test_saga() {
    let workflow = CompensationWorkflow::new()
        .step(
            Signature::new("reserve".to_string()),
            Signature::new("cancel_reservation".to_string()),
        )
        .step(
            Signature::new("charge".to_string()),
            Signature::new("refund".to_string()),
        );

    let saga = Saga::new(workflow).with_isolation(SagaIsolation::Serializable);

    assert_eq!(saga.workflow.len(), 2);
    assert!(matches!(saga.isolation, SagaIsolation::Serializable));

    let display = format!("{}", saga);
    assert!(display.contains("Saga[2 steps"));
    assert!(display.contains("Serializable"));
}

#[test]
fn test_scatter_gather() {
    let scatter = Signature::new("distribute".to_string());
    let workers = vec![
        Signature::new("worker1".to_string()),
        Signature::new("worker2".to_string()),
        Signature::new("worker3".to_string()),
    ];
    let gather = Signature::new("collect".to_string());

    let sg = ScatterGather::new(scatter, workers, gather).with_timeout(30);

    assert_eq!(sg.workers.len(), 3);
    assert_eq!(sg.timeout, Some(30));

    let display = format!("{}", sg);
    assert!(display.contains("ScatterGather"));
    assert!(display.contains("distribute"));
    assert!(display.contains("3 workers"));
    assert!(display.contains("collect"));
}

#[test]
fn test_pipeline() {
    let mut pipeline = Pipeline::new();
    assert!(pipeline.is_empty());
    assert_eq!(pipeline.len(), 0);

    pipeline = pipeline
        .stage(Signature::new("fetch".to_string()))
        .stage(Signature::new("transform".to_string()))
        .stage(Signature::new("load".to_string()))
        .with_buffer_size(100);

    assert!(!pipeline.is_empty());
    assert_eq!(pipeline.len(), 3);
    assert_eq!(pipeline.buffer_size, Some(100));

    let display = format!("{}", pipeline);
    assert!(display.contains("Pipeline[3 stages]"));
    assert!(display.contains("buffer=100"));
}

#[test]
fn test_fanout() {
    let source = Signature::new("broadcast".to_string());
    let fanout = FanOut::new(source)
        .consumer(Signature::new("consumer1".to_string()))
        .consumer(Signature::new("consumer2".to_string()))
        .consumer(Signature::new("consumer3".to_string()));

    assert!(!fanout.is_empty());
    assert_eq!(fanout.len(), 3);

    let display = format!("{}", fanout);
    assert!(display.contains("FanOut"));
    assert!(display.contains("broadcast"));
    assert!(display.contains("3 consumers"));
}

#[test]
fn test_fanin() {
    let aggregator = Signature::new("aggregate".to_string());
    let fanin = FanIn::new(aggregator)
        .source(Signature::new("source1".to_string()))
        .source(Signature::new("source2".to_string()));

    assert!(!fanin.is_empty());
    assert_eq!(fanin.len(), 2);

    let display = format!("{}", fanin);
    assert!(display.contains("FanIn"));
    assert!(display.contains("2 sources"));
    assert!(display.contains("aggregate"));
}

#[test]
fn test_validation_result() {
    let mut result = ValidationResult::valid();
    assert!(result.valid);
    assert!(result.errors.is_empty());
    assert!(result.warnings.is_empty());

    result.add_warning("This is a warning");
    assert!(result.valid);
    assert_eq!(result.warnings.len(), 1);

    result.add_error("This is an error");
    assert!(!result.valid);
    assert_eq!(result.errors.len(), 1);

    let display = format!("{}", result);
    assert!(display.contains("Invalid"));
    assert!(display.contains("1 errors"));
}

#[test]
fn test_workflow_validator_chain() {
    let empty_chain = Chain::new();
    let result = empty_chain.validate();
    assert!(!result.valid);
    assert!(result.errors.iter().any(|e| e.contains("cannot be empty")));

    let valid_chain = Chain::new()
        .then("task1", vec![])
        .then("task2", vec![])
        .then("task3", vec![]);
    let result = valid_chain.validate();
    assert!(result.valid);

    // Create a large chain to test warning
    let mut large_chain = Chain::new();
    for i in 0..150 {
        large_chain = large_chain.then(&format!("task{}", i), vec![]);
    }
    let result = large_chain.validate();
    assert!(result.valid);
    assert!(!result.warnings.is_empty());
}

#[test]
fn test_workflow_validator_group() {
    let empty_group = Group::new();
    let result = empty_group.validate();
    assert!(!result.valid);
    assert!(result.errors.iter().any(|e| e.contains("cannot be empty")));

    let valid_group = Group::new().add("task1", vec![]).add("task2", vec![]);
    let result = valid_group.validate();
    assert!(result.valid);
}

#[test]
fn test_workflow_validator_chord() {
    let empty_header = Group::new();
    let body = Signature::new("callback".to_string());
    let chord = Chord::new(empty_header, body);

    let result = chord.validate();
    assert!(!result.valid);
    assert!(result.errors.iter().any(|e| e.contains("cannot be empty")));

    let valid_header = Group::new().add("task1", vec![]).add("task2", vec![]);
    let body = Signature::new("callback".to_string());
    let chord = Chord::new(valid_header, body);

    let result = chord.validate();
    assert!(result.valid);
}

#[test]
fn test_loop_control() {
    let continue_ctrl = LoopControl::continue_loop();
    assert!(matches!(continue_ctrl, LoopControl::Continue));
    assert_eq!(format!("{}", continue_ctrl), "Continue");

    let break_ctrl = LoopControl::break_loop();
    assert!(matches!(break_ctrl, LoopControl::Break));
    assert_eq!(format!("{}", break_ctrl), "Break");

    let break_with = LoopControl::break_with(serde_json::json!({"result": 42}));
    assert!(matches!(break_with, LoopControl::BreakWith { .. }));
    assert_eq!(format!("{}", break_with), "BreakWith");
}

#[test]
fn test_error_propagation_mode() {
    let stop = ErrorPropagationMode::StopOnFirstError;
    assert!(!stop.allows_continue());
    assert_eq!(format!("{}", stop), "StopOnFirstError");

    let continue_mode = ErrorPropagationMode::ContinueOnError;
    assert!(continue_mode.allows_continue());
    assert_eq!(format!("{}", continue_mode), "ContinueOnError");

    let partial = ErrorPropagationMode::partial_failure(3);
    assert!(partial.allows_continue());
    assert!(format!("{}", partial).contains("PartialFailure"));

    let partial_rate = ErrorPropagationMode::partial_failure_with_rate(5, 0.5);
    assert!(partial_rate.allows_continue());
    let display = format!("{}", partial_rate);
    assert!(display.contains("PartialFailure"));
    assert!(display.contains("50.0%"));
}

#[test]
fn test_partial_failure_tracker() {
    let mut tracker = PartialFailureTracker::new(10);
    assert_eq!(tracker.total_tasks, 10);
    assert_eq!(tracker.successful_tasks, 0);
    assert_eq!(tracker.failed_tasks, 0);

    // Record successes
    tracker.record_success(Uuid::new_v4());
    tracker.record_success(Uuid::new_v4());
    assert_eq!(tracker.successful_tasks, 2);
    assert_eq!(tracker.success_rate(), 0.2);

    // Record failures
    tracker.record_failure(Uuid::new_v4(), "error1".to_string());
    tracker.record_failure(Uuid::new_v4(), "error2".to_string());
    assert_eq!(tracker.failed_tasks, 2);
    assert_eq!(tracker.failure_rate(), 0.2);

    // Test threshold checking
    let stop_mode = ErrorPropagationMode::StopOnFirstError;
    assert!(tracker.exceeds_threshold(&stop_mode));
    assert!(!tracker.should_continue(&stop_mode));

    let continue_mode = ErrorPropagationMode::ContinueOnError;
    assert!(!tracker.exceeds_threshold(&continue_mode));
    assert!(tracker.should_continue(&continue_mode));

    let partial_mode = ErrorPropagationMode::partial_failure(3);
    assert!(!tracker.exceeds_threshold(&partial_mode));
    assert!(tracker.should_continue(&partial_mode));

    // Add more failures to exceed threshold
    tracker.record_failure(Uuid::new_v4(), "error3".to_string());
    assert!(tracker.exceeds_threshold(&partial_mode));
    assert!(!tracker.should_continue(&partial_mode));

    let display = format!("{}", tracker);
    assert!(display.contains("PartialFailureTracker"));
    assert!(display.contains("2/10"));
}

#[test]
fn test_isolation_level() {
    let none = IsolationLevel::None;
    assert!(!none.has_resource_limits());
    assert!(!none.has_error_isolation());
    assert_eq!(format!("{}", none), "None");

    let resource = IsolationLevel::resource(512);
    assert!(resource.has_resource_limits());
    assert!(!resource.has_error_isolation());
    let display = format!("{}", resource);
    assert!(display.contains("Resource"));
    assert!(display.contains("512MB"));

    let error = IsolationLevel::Error;
    assert!(!error.has_resource_limits());
    assert!(error.has_error_isolation());
    assert_eq!(format!("{}", error), "Error");

    let full = IsolationLevel::full(1024);
    assert!(full.has_resource_limits());
    assert!(full.has_error_isolation());
    let display = format!("{}", full);
    assert!(display.contains("Full"));
    assert!(display.contains("1024MB"));
}

#[test]
fn test_sub_workflow_isolation() {
    let workflow_id = Uuid::new_v4();
    let parent_id = Uuid::new_v4();

    let isolation = SubWorkflowIsolation::new(workflow_id, IsolationLevel::Error)
        .with_parent(parent_id)
        .no_error_propagation()
        .no_cancellation_propagation();

    assert_eq!(isolation.workflow_id, workflow_id);
    assert_eq!(isolation.parent_workflow_id, Some(parent_id));
    assert_eq!(isolation.isolation_level, IsolationLevel::Error);
    assert!(!isolation.propagate_errors);
    assert!(!isolation.propagate_cancellation);

    let display = format!("{}", isolation);
    assert!(display.contains("SubWorkflowIsolation"));
    assert!(display.contains(&workflow_id.to_string()));
}

#[test]
fn test_workflow_checkpoint() {
    let workflow_id = Uuid::new_v4();
    let state = WorkflowState::new(workflow_id, 10);
    let mut checkpoint = WorkflowCheckpoint::new(workflow_id, state);

    assert_eq!(checkpoint.workflow_id, workflow_id);
    assert_eq!(checkpoint.version, 1);
    assert_eq!(checkpoint.completed_tasks.len(), 0);

    // Record tasks
    let task1 = Uuid::new_v4();
    let task2 = Uuid::new_v4();
    let task3 = Uuid::new_v4();

    checkpoint.record_in_progress(task1);
    checkpoint.record_completed(task2);
    checkpoint.record_failed(task3, "test error".to_string());

    assert!(checkpoint.is_completed(&task2));
    assert!(checkpoint.is_failed(&task3));
    assert!(!checkpoint.is_completed(&task1));
    assert_eq!(checkpoint.tasks_to_retry().len(), 1);

    // Test serialization
    let json = checkpoint.to_json().unwrap();
    let deserialized = WorkflowCheckpoint::from_json(&json).unwrap();
    assert_eq!(deserialized.workflow_id, workflow_id);
    assert_eq!(deserialized.completed_tasks.len(), 1);
    assert_eq!(deserialized.failed_tasks.len(), 1);

    let display = format!("{}", checkpoint);
    assert!(display.contains("WorkflowCheckpoint"));
    assert!(display.contains("completed=1"));
    assert!(display.contains("failed=1"));
}

#[test]
fn test_workflow_recovery_policy() {
    let auto = WorkflowRecoveryPolicy::auto_recover();
    assert!(auto.auto_recovery);
    assert!(auto.resume_from_checkpoint);
    assert!(auto.replay_failed);
    assert_eq!(auto.max_checkpoint_age, Some(3600));

    let manual = WorkflowRecoveryPolicy::manual();
    assert!(!manual.auto_recovery);
    assert!(manual.resume_from_checkpoint);
    assert!(!manual.replay_failed);
    assert_eq!(manual.max_checkpoint_age, None);

    let custom = WorkflowRecoveryPolicy::auto_recover()
        .with_max_checkpoint_age(7200)
        .with_retry_policy(WorkflowRetryPolicy::new(3));
    assert_eq!(custom.max_checkpoint_age, Some(7200));
    assert!(custom.retry_policy.is_some());

    // Test checkpoint validation
    let workflow_id = Uuid::new_v4();
    let state = WorkflowState::new(workflow_id, 10);
    let checkpoint = WorkflowCheckpoint::new(workflow_id, state);
    assert!(auto.is_checkpoint_valid(&checkpoint));

    let display = format!("{}", auto);
    assert!(display.contains("WorkflowRecoveryPolicy"));
    assert!(display.contains("auto"));
}

#[test]
fn test_optimization_pass() {
    let cse = OptimizationPass::CommonSubexpressionElimination;
    assert_eq!(format!("{}", cse), "CSE");

    let dce = OptimizationPass::DeadCodeElimination;
    assert_eq!(format!("{}", dce), "DCE");

    let fusion = OptimizationPass::TaskFusion;
    assert_eq!(format!("{}", fusion), "TaskFusion");

    let scheduling = OptimizationPass::ParallelScheduling;
    assert_eq!(format!("{}", scheduling), "ParallelScheduling");

    let resource = OptimizationPass::ResourceOptimization;
    assert_eq!(format!("{}", resource), "ResourceOptimization");
}

#[test]
fn test_workflow_compiler() {
    let compiler = WorkflowCompiler::new();
    assert!(!compiler.aggressive);
    assert_eq!(compiler.passes.len(), 2);

    let aggressive = WorkflowCompiler::new().aggressive();
    assert!(aggressive.aggressive);
    assert!(aggressive.passes.len() > 2);

    let custom = WorkflowCompiler::new()
        .add_pass(OptimizationPass::TaskFusion)
        .add_pass(OptimizationPass::ParallelScheduling);
    assert_eq!(custom.passes.len(), 4);

    // Test basic optimization (no change expected)
    let chain = Chain::new().then("task1", vec![]).then("task2", vec![]);
    let optimized = compiler.optimize_chain(&chain);
    assert_eq!(optimized.tasks.len(), chain.tasks.len());

    let group = Group::new().add("task1", vec![]).add("task2", vec![]);
    let optimized_group = compiler.optimize_group(&group);
    assert_eq!(optimized_group.tasks.len(), group.tasks.len());

    let display = format!("{}", compiler);
    assert!(display.contains("WorkflowCompiler"));
    assert!(display.contains("DCE"));
    assert!(display.contains("CSE"));
}

#[test]
fn test_workflow_compiler_cse_chain() {
    use serde_json::json;

    // Test Common Subexpression Elimination for chains
    let compiler = WorkflowCompiler::new().aggressive();
    let chain = Chain::new()
        .then_signature(Signature::new("task1".to_string()).with_args(vec![json!(1)]))
        .then_signature(Signature::new("task2".to_string()).with_args(vec![json!(2)]))
        .then_signature(Signature::new("task1".to_string()).with_args(vec![json!(1)])); // Duplicate

    let optimized = compiler.optimize_chain(&chain);
    // CSE should remove the duplicate task in aggressive mode
    assert_eq!(optimized.tasks.len(), 2);
    assert_eq!(optimized.tasks[0].task, "task1");
    assert_eq!(optimized.tasks[1].task, "task2");
}

#[test]
fn test_workflow_compiler_cse_group() {
    use serde_json::json;

    // Test Common Subexpression Elimination for groups
    let compiler = WorkflowCompiler::new().aggressive();
    let group = Group::new()
        .add_signature(Signature::new("task1".to_string()).with_args(vec![json!(1)]))
        .add_signature(Signature::new("task2".to_string()).with_args(vec![json!(2)]))
        .add_signature(Signature::new("task1".to_string()).with_args(vec![json!(1)])); // Duplicate

    let optimized = compiler.optimize_group(&group);
    // CSE should remove the duplicate task in aggressive mode
    assert_eq!(optimized.tasks.len(), 2);
    assert_eq!(optimized.tasks[0].task, "task1");
    assert_eq!(optimized.tasks[1].task, "task2");
}

#[test]
fn test_workflow_compiler_dce_chain() {
    use serde_json::json;

    // Test Dead Code Elimination for chains
    let compiler = WorkflowCompiler::new();
    let chain = Chain::new()
        .then_signature(Signature::new("task1".to_string()).with_args(vec![json!(1)]))
        .then_signature(Signature::new("".to_string())) // Empty task name (dead code)
        .then_signature(Signature::new("task2".to_string()).with_args(vec![json!(2)]));

    let optimized = compiler.optimize_chain(&chain);
    // DCE should remove the empty task
    assert_eq!(optimized.tasks.len(), 2);
    assert_eq!(optimized.tasks[0].task, "task1");
    assert_eq!(optimized.tasks[1].task, "task2");
}

#[test]
fn test_workflow_compiler_dce_group() {
    use serde_json::json;

    // Test Dead Code Elimination for groups
    let compiler = WorkflowCompiler::new();
    let group = Group::new()
        .add_signature(Signature::new("task1".to_string()).with_args(vec![json!(1)]))
        .add_signature(Signature::new("".to_string())) // Empty task name (dead code)
        .add_signature(Signature::new("task2".to_string()).with_args(vec![json!(2)]));

    let optimized = compiler.optimize_group(&group);
    // DCE should remove the empty task
    assert_eq!(optimized.tasks.len(), 2);
    assert_eq!(optimized.tasks[0].task, "task1");
    assert_eq!(optimized.tasks[1].task, "task2");
}

#[test]
fn test_workflow_compiler_task_fusion() {
    use serde_json::json;

    // Test Task Fusion for chains
    let compiler = WorkflowCompiler::new().aggressive();
    let chain = Chain::new()
        .then_signature(
            Signature::new("process".to_string())
                .with_args(vec![json!(1)])
                .immutable(),
        )
        .then_signature(
            Signature::new("process".to_string())
                .with_args(vec![json!(2)])
                .immutable(),
        )
        .then_signature(Signature::new("finalize".to_string()));

    let optimized = compiler.optimize_chain(&chain);
    // Task fusion should combine the two "process" tasks
    assert_eq!(optimized.tasks.len(), 2);
    assert_eq!(optimized.tasks[0].task, "process");
    assert_eq!(optimized.tasks[0].args.len(), 2); // Fused args
    assert_eq!(optimized.tasks[1].task, "finalize");
}

#[test]
fn test_workflow_compiler_parallel_scheduling() {
    // Test Parallel Scheduling for groups
    let compiler = WorkflowCompiler::new().add_pass(OptimizationPass::ParallelScheduling);

    let group = Group::new()
        .add_signature(Signature::new("task1".to_string()).with_priority(1))
        .add_signature(Signature::new("task2".to_string()).with_priority(5))
        .add_signature(Signature::new("task3".to_string()).with_priority(3));

    let optimized = compiler.optimize_group(&group);
    // Parallel scheduling should sort by priority (highest first)
    assert_eq!(optimized.tasks.len(), 3);
    assert_eq!(optimized.tasks[0].options.priority, Some(5));
    assert_eq!(optimized.tasks[1].options.priority, Some(3));
    assert_eq!(optimized.tasks[2].options.priority, Some(1));
}

#[test]
fn test_workflow_compiler_resource_optimization() {
    // Test Resource Optimization for groups
    let compiler = WorkflowCompiler::new().add_pass(OptimizationPass::ResourceOptimization);

    let group = Group::new()
        .add_signature(Signature::new("task1".to_string()).with_queue("queue_b".to_string()))
        .add_signature(Signature::new("task2".to_string()).with_queue("queue_a".to_string()))
        .add_signature(Signature::new("task3".to_string()).with_queue("queue_a".to_string()));

    let optimized = compiler.optimize_group(&group);
    // Resource optimization should sort by queue
    assert_eq!(optimized.tasks.len(), 3);
    assert_eq!(
        optimized.tasks[0].options.queue.as_ref().unwrap(),
        "queue_a"
    );
    assert_eq!(
        optimized.tasks[1].options.queue.as_ref().unwrap(),
        "queue_a"
    );
    assert_eq!(
        optimized.tasks[2].options.queue.as_ref().unwrap(),
        "queue_b"
    );
}

#[test]
fn test_workflow_compiler_optimize_chord() {
    use serde_json::json;

    // Test chord optimization
    let compiler = WorkflowCompiler::new().aggressive();

    let group = Group::new()
        .add_signature(Signature::new("task1".to_string()).with_args(vec![json!(1)]))
        .add_signature(Signature::new("task1".to_string()).with_args(vec![json!(1)])) // Duplicate
        .add_signature(Signature::new("task2".to_string()).with_args(vec![json!(2)]));

    let chord = Chord::new(group, Signature::new("callback".to_string()));
    let optimized = compiler.optimize_chord(&chord);

    // CSE should remove duplicates from the chord's header group
    assert_eq!(optimized.header.tasks.len(), 2);
    assert_eq!(optimized.body.task, "callback");
}

#[test]
fn test_workflow_compiler_combined_passes() {
    use serde_json::json;

    // Test multiple optimization passes together
    let compiler = WorkflowCompiler::new()
        .aggressive()
        .add_pass(OptimizationPass::ParallelScheduling);

    let group = Group::new()
        .add_signature(
            Signature::new("task1".to_string())
                .with_priority(1)
                .with_args(vec![json!(1)]),
        )
        .add_signature(Signature::new("".to_string())) // Dead code
        .add_signature(
            Signature::new("task2".to_string())
                .with_priority(5)
                .with_args(vec![json!(2)]),
        )
        .add_signature(
            Signature::new("task1".to_string())
                .with_priority(1)
                .with_args(vec![json!(1)]),
        ); // Duplicate

    let optimized = compiler.optimize_group(&group);
    // Should remove dead code, remove duplicate, and sort by priority
    assert_eq!(optimized.tasks.len(), 2);
    assert_eq!(optimized.tasks[0].options.priority, Some(5));
    assert_eq!(optimized.tasks[0].task, "task2");
    assert_eq!(optimized.tasks[1].options.priority, Some(1));
    assert_eq!(optimized.tasks[1].task, "task1");
}

#[test]
fn test_typed_result() {
    let result = TypedResult::new(42i32).with_metadata("source", serde_json::json!("test"));

    assert_eq!(result.value, 42);
    assert_eq!(result.type_name(), "i32");
    assert!(result.metadata.contains_key("source"));

    let display = format!("{}", result);
    assert!(display.contains("TypedResult"));
    assert!(display.contains("i32"));
}

#[test]
fn test_type_validator() {
    let validator = TypeValidator::new("i32");
    assert!(validator.validate("i32"));
    assert!(!validator.validate("String"));

    let compatible = TypeValidator::new("serde_json::Value").allow_compatible();
    assert!(compatible.validate("i32"));
    assert!(compatible.validate("String"));
    assert!(compatible.allow_compatible);

    let display = format!("{}", validator);
    assert!(display.contains("TypeValidator"));
    assert!(display.contains("i32"));
}

#[test]
fn test_task_dependency() {
    let task_id = Uuid::new_v4();
    let dep = TaskDependency::new(task_id)
        .with_output_key("result")
        .optional();

    assert_eq!(dep.task_id, task_id);
    assert_eq!(dep.output_key, Some("result".to_string()));
    assert!(dep.optional);

    let display = format!("{}", dep);
    assert!(display.contains("TaskDependency"));
    assert!(display.contains(&task_id.to_string()));
    assert!(display.contains("result"));
}

#[test]
fn test_dependency_graph() {
    let mut graph = DependencyGraph::new();
    let task1 = Uuid::new_v4();
    let task2 = Uuid::new_v4();
    let task3 = Uuid::new_v4();

    graph.add_dependency(task2, TaskDependency::new(task1));
    graph.add_dependency(task3, TaskDependency::new(task2));

    assert_eq!(graph.get_dependencies(&task2).len(), 1);
    assert_eq!(graph.get_dependents(&task1).len(), 1);
    assert!(!graph.has_circular_dependency());

    let sorted = graph.topological_sort().unwrap();
    assert_eq!(sorted.len(), 3);

    let display = format!("{}", graph);
    assert!(display.contains("DependencyGraph"));
    assert!(display.contains("2 tasks")); // Only task2 and task3 have dependencies
}

#[test]
fn test_circular_dependency() {
    let mut graph = DependencyGraph::new();
    let task1 = Uuid::new_v4();
    let task2 = Uuid::new_v4();

    graph.add_dependency(task1, TaskDependency::new(task2));
    graph.add_dependency(task2, TaskDependency::new(task1));

    assert!(graph.has_circular_dependency());
    assert!(graph.topological_sort().is_err());
}

#[test]
fn test_parallel_reduce() {
    let map_task = Signature::new("map".to_string());
    let reduce_task = Signature::new("reduce".to_string());
    let inputs = vec![
        serde_json::json!(1),
        serde_json::json!(2),
        serde_json::json!(3),
    ];

    let pr = ParallelReduce::new(map_task, reduce_task, inputs)
        .with_parallelism(8)
        .with_initial_value(serde_json::json!(0));

    assert_eq!(pr.parallelism, 8);
    assert_eq!(pr.input_count(), 3);
    assert!(!pr.is_empty());
    assert!(pr.initial_value.is_some());

    let display = format!("{}", pr);
    assert!(display.contains("ParallelReduce"));
    assert!(display.contains("map"));
    assert!(display.contains("reduce"));
    assert!(display.contains("parallelism=8"));
}

#[test]
fn test_template_parameter() {
    let param = TemplateParameter::new("count", "usize")
        .with_default(serde_json::json!(10))
        .with_description("Number of items");

    assert_eq!(param.name, "count");
    assert_eq!(param.param_type, "usize");
    assert!(!param.required);
    assert!(param.default.is_some());
    assert!(param.description.is_some());

    let display = format!("{}", param);
    assert!(display.contains("count:usize"));
    assert!(display.contains("optional"));
}

#[test]
fn test_workflow_template() {
    let param =
        TemplateParameter::new("queue", "String").with_default(serde_json::json!("default"));

    let template = WorkflowTemplate::new("etl_pipeline", "1.0")
        .add_parameter(param)
        .with_description("ETL workflow template")
        .with_chain(Chain::new().then("extract", vec![]));

    assert_eq!(template.name, "etl_pipeline");
    assert_eq!(template.version, "1.0");
    assert_eq!(template.parameters.len(), 1);
    assert!(template.chain.is_some());
    assert!(template.description.is_some());

    // Test instantiation with valid parameters
    let mut params = HashMap::new();
    params.insert("queue".to_string(), serde_json::json!("custom"));
    let instance = template.instantiate(params).unwrap();
    assert_eq!(instance.name, "etl_pipeline");

    let display = format!("{}", template);
    assert!(display.contains("WorkflowTemplate"));
    assert!(display.contains("etl_pipeline@1.0"));
}

#[test]
fn test_workflow_template_validation() {
    let required_param = TemplateParameter::new("api_key", "String");
    let template = WorkflowTemplate::new("api_workflow", "1.0").add_parameter(required_param);

    // Should fail without required parameter
    let result = template.instantiate(HashMap::new());
    assert!(result.is_err());
}

#[test]
fn test_workflow_event() {
    let task_id = Uuid::new_v4();
    let workflow_id = Uuid::new_v4();

    let task_completed = WorkflowEvent::TaskCompleted { task_id };
    assert_eq!(
        format!("{}", task_completed),
        format!("TaskCompleted[{}]", task_id)
    );

    let task_failed = WorkflowEvent::TaskFailed {
        task_id,
        error: "test error".to_string(),
    };
    assert!(format!("{}", task_failed).contains("TaskFailed"));

    let workflow_started = WorkflowEvent::WorkflowStarted { workflow_id };
    assert!(format!("{}", workflow_started).contains("WorkflowStarted"));

    let custom = WorkflowEvent::Custom {
        event_type: "data_updated".to_string(),
        data: "{}".to_string(),
    };
    assert!(format!("{}", custom).contains("Custom"));
    assert!(format!("{}", custom).contains("data_updated"));
}

#[test]
fn test_event_handler() {
    let handler_task = Signature::new("handle_completion".to_string());
    let handler =
        EventHandler::new("TaskCompleted", handler_task).with_filter("task_type == 'important'");

    assert_eq!(handler.event_type, "TaskCompleted");
    assert_eq!(handler.handler_task.task, "handle_completion");
    assert!(handler.filter.is_some());

    let display = format!("{}", handler);
    assert!(display.contains("EventHandler"));
    assert!(display.contains("TaskCompleted"));
    assert!(display.contains("handle_completion"));
}

#[test]
fn test_event_driven_workflow() {
    let handler1 = EventHandler::new("TaskCompleted", Signature::new("on_complete".to_string()));
    let handler2 = EventHandler::new("TaskFailed", Signature::new("on_fail".to_string()));

    let workflow = EventDrivenWorkflow::new()
        .on_event(handler1)
        .on_event(handler2)
        .on_task_completed(Signature::new("notify".to_string()));

    assert!(workflow.active);
    assert_eq!(workflow.handlers.len(), 3);
    assert!(workflow.has_handlers());

    let deactivated = workflow.clone().deactivate();
    assert!(!deactivated.active);

    let display = format!("{}", workflow);
    assert!(display.contains("EventDrivenWorkflow"));
    assert!(display.contains("handlers=3"));
}

#[test]
fn test_state_version() {
    let v1 = StateVersion::new(1, 0, 0);
    let v2 = StateVersion::new(1, 1, 0);
    let v3 = StateVersion::new(2, 0, 0);

    // Same major version is compatible
    assert!(v1.is_compatible(&v2));
    // Different major version is not compatible
    assert!(!v1.is_compatible(&v3));

    // Display
    assert_eq!(format!("{}", v1), "1.0.0");
    assert_eq!(format!("{}", v2), "1.1.0");

    // Current version
    let current = StateVersion::current();
    assert_eq!(current.major, 1);
    assert_eq!(current.minor, 0);
    assert_eq!(current.patch, 0);
}

#[test]
fn test_state_migration_error() {
    let v1 = StateVersion::new(1, 0, 0);
    let v2 = StateVersion::new(2, 0, 0);

    let err = StateMigrationError::IncompatibleVersion { from: v1, to: v2 };
    let display = format!("{}", err);
    assert!(display.contains("Incompatible"));
    assert!(display.contains("1.0.0"));
    assert!(display.contains("2.0.0"));

    let err2 = StateMigrationError::MigrationFailed("test error".to_string());
    assert!(format!("{}", err2).contains("migration failed"));

    let err3 = StateMigrationError::UnsupportedVersion(v1);
    assert!(format!("{}", err3).contains("Unsupported"));
}

#[test]
fn test_versioned_workflow_state() {
    let workflow_id = Uuid::new_v4();
    let state = WorkflowState::new(workflow_id, 5);
    let mut versioned = VersionedWorkflowState::new(state);

    // Check initial version
    assert_eq!(versioned.version, StateVersion::current());
    assert_eq!(versioned.migration_history.len(), 0);

    // Migrate to compatible version
    let target = StateVersion::new(1, 1, 0);
    assert!(versioned.can_migrate_to(&target));
    assert!(versioned.migrate_to(target).is_ok());
    assert_eq!(versioned.version, target);
    assert_eq!(versioned.migration_history.len(), 1);

    // Migrate to same version is OK
    assert!(versioned.migrate_to(target).is_ok());
    assert_eq!(versioned.migration_history.len(), 1);

    // Migrate to incompatible version fails
    let incompatible = StateVersion::new(2, 0, 0);
    assert!(!versioned.can_migrate_to(&incompatible));
    assert!(versioned.migrate_to(incompatible).is_err());
}

#[test]
fn test_task_priority() {
    let low = TaskPriority::Low;
    let normal = TaskPriority::Normal;
    let high = TaskPriority::High;
    let critical = TaskPriority::Critical;

    // Test ordering
    assert!(low < normal);
    assert!(normal < high);
    assert!(high < critical);

    // Test display
    assert_eq!(format!("{}", low), "Low");
    assert_eq!(format!("{}", normal), "Normal");
    assert_eq!(format!("{}", high), "High");
    assert_eq!(format!("{}", critical), "Critical");

    // Test default
    assert_eq!(TaskPriority::default(), TaskPriority::Normal);
}

#[test]
fn test_worker_capacity() {
    let mut worker = WorkerCapacity::new("worker1", 4, 8192);

    assert_eq!(worker.worker_id, "worker1");
    assert_eq!(worker.cpu_cores, 4);
    assert_eq!(worker.memory_mb, 8192);
    assert_eq!(worker.current_load, 0.0);
    assert_eq!(worker.active_tasks, 0);

    // Test capacity checks
    assert!(worker.has_capacity(0.5));
    assert!(worker.has_capacity(1.0));
    assert!(!worker.has_capacity(1.1));

    // Test available capacity
    assert_eq!(worker.available_capacity(), 1.0);
    worker.current_load = 0.3;
    assert_eq!(worker.available_capacity(), 0.7);
}

#[test]
fn test_scheduling_decision() {
    let task_id = Uuid::new_v4();
    let decision =
        SchedulingDecision::new(task_id, "worker1", TaskPriority::High).with_estimated_time(120);

    assert_eq!(decision.task_id, task_id);
    assert_eq!(decision.worker_id, "worker1");
    assert_eq!(decision.priority, TaskPriority::High);
    assert_eq!(decision.estimated_time, Some(120));
}

#[test]
fn test_scheduling_strategy() {
    let rr = SchedulingStrategy::RoundRobin;
    let ll = SchedulingStrategy::LeastLoaded;
    let pb = SchedulingStrategy::PriorityBased;
    let ra = SchedulingStrategy::ResourceAware;

    assert_eq!(format!("{}", rr), "RoundRobin");
    assert_eq!(format!("{}", ll), "LeastLoaded");
    assert_eq!(format!("{}", pb), "PriorityBased");
    assert_eq!(format!("{}", ra), "ResourceAware");

    assert_eq!(
        SchedulingStrategy::default(),
        SchedulingStrategy::LeastLoaded
    );
}

#[test]
fn test_parallel_scheduler_round_robin() {
    let mut scheduler = ParallelScheduler::new(SchedulingStrategy::RoundRobin);

    // Add workers
    scheduler.add_worker(WorkerCapacity::new("worker1", 4, 8192));
    scheduler.add_worker(WorkerCapacity::new("worker2", 4, 8192));

    assert_eq!(scheduler.worker_count(), 2);

    // Schedule tasks
    let task1 = Uuid::new_v4();
    let decision = scheduler.schedule_task(task1, TaskPriority::Normal);
    assert!(decision.is_some());
}

#[test]
fn test_parallel_scheduler_least_loaded() {
    let mut scheduler = ParallelScheduler::new(SchedulingStrategy::LeastLoaded);

    let mut worker1 = WorkerCapacity::new("worker1", 4, 8192);
    worker1.current_load = 0.8;
    let mut worker2 = WorkerCapacity::new("worker2", 4, 8192);
    worker2.current_load = 0.3;

    scheduler.add_worker(worker1);
    scheduler.add_worker(worker2);

    // Should assign to worker2 (lower load)
    let task = Uuid::new_v4();
    let decision = scheduler.schedule_task(task, TaskPriority::Normal);
    assert!(decision.is_some());
    assert_eq!(decision.unwrap().worker_id, "worker2");
}

#[test]
fn test_parallel_scheduler_metrics() {
    let mut scheduler = ParallelScheduler::new(SchedulingStrategy::LeastLoaded);

    let mut worker1 = WorkerCapacity::new("worker1", 4, 8192);
    worker1.current_load = 0.5;
    let mut worker2 = WorkerCapacity::new("worker2", 4, 8192);
    worker2.current_load = 0.3;

    scheduler.add_worker(worker1);
    scheduler.add_worker(worker2);

    // Test metrics
    assert_eq!(scheduler.worker_count(), 2);
    assert_eq!(scheduler.average_load(), 0.4);
    assert_eq!(scheduler.total_capacity(), 1.2);

    let display = format!("{}", scheduler);
    assert!(display.contains("ParallelScheduler"));
    assert!(display.contains("workers=2"));
}

#[test]
fn test_parallel_scheduler_max_tasks() {
    let mut scheduler =
        ParallelScheduler::new(SchedulingStrategy::LeastLoaded).with_max_tasks_per_worker(2);

    let mut worker = WorkerCapacity::new("worker1", 4, 8192);
    worker.active_tasks = 2;
    scheduler.add_worker(worker);

    // Should not schedule (max reached)
    let task = Uuid::new_v4();
    let decision = scheduler.schedule_task(task, TaskPriority::Normal);
    assert!(decision.is_none());
}

#[test]
fn test_workflow_batch() {
    let mut batch = WorkflowBatch::new(5);

    assert!(batch.is_empty());
    assert!(!batch.is_full());
    assert_eq!(batch.size(), 0);
    assert_eq!(batch.max_size, 5);

    // Add workflows
    let wf1 = Uuid::new_v4();
    let wf2 = Uuid::new_v4();
    assert!(batch.add_workflow(wf1));
    assert!(batch.add_workflow(wf2));
    assert_eq!(batch.size(), 2);
    assert!(!batch.is_empty());
    assert!(!batch.is_full());

    // Fill batch
    for _ in 0..3 {
        batch.add_workflow(Uuid::new_v4());
    }
    assert!(batch.is_full());

    // Cannot add more
    assert!(!batch.add_workflow(Uuid::new_v4()));

    // Test display
    let display = format!("{}", batch);
    assert!(display.contains("WorkflowBatch"));
    assert!(display.contains("5/5"));
}

#[test]
fn test_workflow_batch_timeout() {
    let mut batch = WorkflowBatch::new(10).with_timeout(0);

    // With 0 timeout, should be immediately timed out
    std::thread::sleep(std::time::Duration::from_millis(10));
    assert!(batch.is_timed_out());

    // Batch without timeout never times out
    batch.timeout = None;
    assert!(!batch.is_timed_out());
}

#[test]
fn test_batching_strategy() {
    let by_type = BatchingStrategy::ByType;
    let by_priority = BatchingStrategy::ByPriority;
    let by_size = BatchingStrategy::BySize;
    let by_time = BatchingStrategy::ByTimeWindow;

    assert_eq!(format!("{}", by_type), "ByType");
    assert_eq!(format!("{}", by_priority), "ByPriority");
    assert_eq!(format!("{}", by_size), "BySize");
    assert_eq!(format!("{}", by_time), "ByTimeWindow");

    assert_eq!(BatchingStrategy::default(), BatchingStrategy::ByType);
}

#[test]
fn test_workflow_batcher() {
    let mut batcher = WorkflowBatcher::new(BatchingStrategy::ByType)
        .with_batch_size(3)
        .with_timeout(60);

    assert_eq!(batcher.batch_count(), 0);
    assert_eq!(batcher.total_workflow_count(), 0);

    // Add workflows
    let wf1 = Uuid::new_v4();
    let wf2 = Uuid::new_v4();
    let wf3 = Uuid::new_v4();

    batcher.add_workflow(wf1, TaskPriority::Normal);
    assert_eq!(batcher.batch_count(), 1);
    assert_eq!(batcher.total_workflow_count(), 1);

    batcher.add_workflow(wf2, TaskPriority::Normal);
    batcher.add_workflow(wf3, TaskPriority::Normal);
    assert_eq!(batcher.batch_count(), 1);
    assert_eq!(batcher.total_workflow_count(), 3);

    // Batch should be full
    let ready = batcher.get_ready_batches();
    assert_eq!(ready.len(), 1);
    assert!(ready[0].is_full());
}

#[test]
fn test_workflow_batcher_by_priority() {
    let mut batcher = WorkflowBatcher::new(BatchingStrategy::ByPriority).with_batch_size(5);

    // Add workflows with different priorities
    let wf1 = Uuid::new_v4();
    let wf2 = Uuid::new_v4();
    let wf3 = Uuid::new_v4();

    batcher.add_workflow(wf1, TaskPriority::High);
    batcher.add_workflow(wf2, TaskPriority::Low);
    batcher.add_workflow(wf3, TaskPriority::High);

    // Should create 2 batches (one for High, one for Low)
    assert_eq!(batcher.batch_count(), 2);
    assert_eq!(batcher.total_workflow_count(), 3);
}

#[test]
fn test_workflow_batcher_remove_ready() {
    let mut batcher = WorkflowBatcher::new(BatchingStrategy::ByType).with_batch_size(2);

    // Add workflows to fill one batch
    batcher.add_workflow(Uuid::new_v4(), TaskPriority::Normal);
    batcher.add_workflow(Uuid::new_v4(), TaskPriority::Normal);

    // Add one more to create a second batch
    batcher.add_workflow(Uuid::new_v4(), TaskPriority::Normal);

    assert_eq!(batcher.batch_count(), 2);

    // Remove ready batches (the full one)
    let ready = batcher.remove_ready_batches();
    assert_eq!(ready.len(), 1);
    assert_eq!(batcher.batch_count(), 1);

    let display = format!("{}", batcher);
    assert!(display.contains("WorkflowBatcher"));
    assert!(display.contains("batches=1"));
}

#[test]
fn test_streaming_map_reduce() {
    let map_task = Signature::new("map_task".to_string());
    let reduce_task = Signature::new("reduce_task".to_string());

    let stream = StreamingMapReduce::new(map_task, reduce_task)
        .with_chunk_size(50)
        .with_buffer_size(500)
        .with_backpressure(true);

    assert_eq!(stream.chunk_size, 50);
    assert_eq!(stream.buffer_size, 500);
    assert!(stream.backpressure);

    let display = format!("{}", stream);
    assert!(display.contains("StreamingMapReduce"));
    assert!(display.contains("map_task"));
    assert!(display.contains("reduce_task"));
    assert!(display.contains("chunk_size=50"));
    assert!(display.contains("buffer_size=500"));
}

#[test]
fn test_resource_utilization() {
    let util = ResourceUtilization::new(0.8, 0.6, 0.4, 0.2);

    assert_eq!(util.cpu, 0.8);
    assert_eq!(util.memory, 0.6);
    assert_eq!(util.disk_io, 0.4);
    assert_eq!(util.network_io, 0.2);

    // Test overall
    assert!((util.overall() - 0.5).abs() < 0.01);

    // Test overload
    assert!(util.is_overloaded(0.7));
    assert!(!util.is_overloaded(0.9));

    // Test bottleneck
    assert_eq!(util.bottleneck(), "cpu");

    // Test clamping
    let util2 = ResourceUtilization::new(1.5, -0.5, 0.5, 0.5);
    assert_eq!(util2.cpu, 1.0);
    assert_eq!(util2.memory, 0.0);
}

#[test]
fn test_resource_utilization_display() {
    let util = ResourceUtilization::new(0.5, 0.6, 0.7, 0.8);
    let display = format!("{}", util);
    assert!(display.contains("ResourceUtilization"));
    assert!(display.contains("cpu=0.50"));
    assert!(display.contains("mem=0.60"));
}

#[test]
fn test_workflow_resource_monitor() {
    let workflow_id = Uuid::new_v4();
    let mut monitor = WorkflowResourceMonitor::new(workflow_id)
        .with_max_history(100)
        .with_sampling_interval(10);

    assert_eq!(monitor.workflow_id, workflow_id);
    assert_eq!(monitor.max_history, 100);
    assert_eq!(monitor.sampling_interval, 10);
    assert_eq!(monitor.history.len(), 0);

    // Record some utilization
    monitor.record(ResourceUtilization::new(0.5, 0.6, 0.4, 0.3));
    monitor.record(ResourceUtilization::new(0.7, 0.8, 0.6, 0.5));

    assert_eq!(monitor.history.len(), 2);

    // Test peak
    let peak = monitor.peak_utilization().unwrap();
    assert!(peak.overall() > 0.6);

    // Test average
    let avg = monitor.average_utilization(3600).unwrap();
    assert!(avg.cpu > 0.5 && avg.cpu < 0.7);

    // Test clear
    monitor.clear();
    assert_eq!(monitor.history.len(), 0);
}

#[test]
fn test_workflow_resource_monitor_max_history() {
    let workflow_id = Uuid::new_v4();
    let mut monitor = WorkflowResourceMonitor::new(workflow_id).with_max_history(3);

    // Add more than max_history
    for i in 0..5 {
        monitor.record(ResourceUtilization::new(i as f64 * 0.1, 0.5, 0.5, 0.5));
    }

    // Should only keep last 3
    assert_eq!(monitor.history.len(), 3);

    let display = format!("{}", monitor);
    assert!(display.contains("WorkflowResourceMonitor"));
    assert!(display.contains("samples=3"));
}
