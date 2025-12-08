use celers_core::{RetryStrategy, SerializedTask, StateHistory, TaskDag, TaskMetadata, TaskState};
use criterion::{black_box, criterion_group, criterion_main, Criterion};
use uuid::Uuid;

fn bench_task_metadata_creation(c: &mut Criterion) {
    c.bench_function("task_metadata_new", |b| {
        b.iter(|| {
            black_box(TaskMetadata::new("test_task".to_string()));
        });
    });

    c.bench_function("task_metadata_with_options", |b| {
        b.iter(|| {
            black_box(
                TaskMetadata::new("test_task".to_string())
                    .with_max_retries(5)
                    .with_timeout(60)
                    .with_priority(10),
            );
        });
    });

    c.bench_function("task_metadata_with_dependencies", |b| {
        let deps: Vec<Uuid> = (0..10).map(|_| Uuid::new_v4()).collect();
        b.iter(|| {
            black_box(TaskMetadata::new("test_task".to_string()).with_dependencies(deps.clone()));
        });
    });
}

fn bench_task_metadata_clone(c: &mut Criterion) {
    let metadata = TaskMetadata::new("test_task".to_string())
        .with_max_retries(5)
        .with_timeout(60)
        .with_priority(10);

    c.bench_function("task_metadata_clone_simple", |b| {
        b.iter(|| {
            black_box(metadata.clone());
        });
    });

    let metadata_with_deps = TaskMetadata::new("test_task".to_string())
        .with_dependencies((0..10).map(|_| Uuid::new_v4()));

    c.bench_function("task_metadata_clone_with_deps", |b| {
        b.iter(|| {
            black_box(metadata_with_deps.clone());
        });
    });
}

fn bench_serialized_task_creation(c: &mut Criterion) {
    let payload = vec![1u8; 1024]; // 1KB payload

    c.bench_function("serialized_task_new_1kb", |b| {
        b.iter(|| {
            black_box(SerializedTask::new(
                "test_task".to_string(),
                payload.clone(),
            ));
        });
    });

    let large_payload = vec![1u8; 1024 * 100]; // 100KB payload

    c.bench_function("serialized_task_new_100kb", |b| {
        b.iter(|| {
            black_box(SerializedTask::new(
                "test_task".to_string(),
                large_payload.clone(),
            ));
        });
    });
}

fn bench_task_validation(c: &mut Criterion) {
    let task = SerializedTask::new("test_task".to_string(), vec![1, 2, 3, 4, 5]);

    c.bench_function("task_validate", |b| {
        b.iter(|| {
            black_box(task.validate()).ok();
        });
    });

    let metadata = TaskMetadata::new("test_task".to_string());

    c.bench_function("metadata_validate", |b| {
        b.iter(|| {
            black_box(metadata.validate()).ok();
        });
    });
}

fn bench_state_transitions(c: &mut Criterion) {
    c.bench_function("state_transition_pending_to_running", |b| {
        b.iter(|| {
            let mut history = StateHistory::with_initial(TaskState::Pending);
            black_box(history.transition(TaskState::Running));
        });
    });

    c.bench_function("state_transition_full_success_lifecycle", |b| {
        b.iter(|| {
            let mut history = StateHistory::with_initial(TaskState::Pending);
            history.transition(TaskState::Reserved);
            history.transition(TaskState::Running);
            black_box(history.transition(TaskState::Succeeded(vec![])));
        });
    });

    c.bench_function("state_transition_with_retry", |b| {
        b.iter(|| {
            let mut history = StateHistory::with_initial(TaskState::Pending);
            history.transition(TaskState::Reserved);
            history.transition(TaskState::Running);
            history.transition(TaskState::Failed("error".to_string()));
            black_box(history.transition(TaskState::Retrying(1)));
        });
    });

    c.bench_function("state_is_terminal_check", |b| {
        let state = TaskState::Succeeded(vec![]);
        b.iter(|| {
            black_box(state.is_terminal());
        });
    });
}

fn bench_retry_strategies(c: &mut Criterion) {
    c.bench_function("retry_fixed_delay", |b| {
        let strategy = RetryStrategy::fixed(5000);
        b.iter(|| {
            black_box(strategy.calculate_delay(5, Some(5000)));
        });
    });

    c.bench_function("retry_exponential_backoff", |b| {
        let strategy = RetryStrategy::exponential(1000, 2.0);
        b.iter(|| {
            black_box(strategy.calculate_delay(5, Some(16000)));
        });
    });

    c.bench_function("retry_fibonacci", |b| {
        let strategy = RetryStrategy::fibonacci(100);
        b.iter(|| {
            black_box(strategy.calculate_delay(10, Some(5500)));
        });
    });

    c.bench_function("retry_full_jitter", |b| {
        let strategy = RetryStrategy::full_jitter(1000, 2.0, 60000);
        b.iter(|| {
            black_box(strategy.calculate_delay(5, Some(16000)));
        });
    });

    c.bench_function("retry_decorrelated_jitter", |b| {
        let strategy = RetryStrategy::decorrelated_jitter(1000, 60000);
        b.iter(|| {
            black_box(strategy.calculate_delay(5, Some(8000)));
        });
    });
}

fn bench_dag_operations(c: &mut Criterion) {
    c.bench_function("dag_add_node", |b| {
        b.iter(|| {
            let mut dag = TaskDag::new();
            black_box(dag.add_node(Uuid::new_v4(), "test_task"));
        });
    });

    c.bench_function("dag_add_dependency", |b| {
        let mut dag = TaskDag::new();
        let parent = Uuid::new_v4();
        let child = Uuid::new_v4();
        dag.add_node(parent, "parent");
        dag.add_node(child, "child");
        b.iter(|| {
            let _ = black_box(dag.add_dependency(child, parent));
        });
    });

    c.bench_function("dag_validate_small", |b| {
        let mut dag = TaskDag::new();
        let ids: Vec<_> = (0..5).map(|_| Uuid::new_v4()).collect();
        for (i, id) in ids.iter().enumerate() {
            dag.add_node(*id, &format!("task_{}", i));
        }
        for i in 1..ids.len() {
            dag.add_dependency(ids[i], ids[i - 1]).unwrap();
        }
        b.iter(|| {
            black_box(dag.validate()).ok();
        });
    });

    c.bench_function("dag_topological_sort_small", |b| {
        let mut dag = TaskDag::new();
        let ids: Vec<_> = (0..5).map(|_| Uuid::new_v4()).collect();
        for (i, id) in ids.iter().enumerate() {
            dag.add_node(*id, &format!("task_{}", i));
        }
        for i in 1..ids.len() {
            dag.add_dependency(ids[i], ids[i - 1]).unwrap();
        }
        b.iter(|| {
            black_box(dag.topological_sort()).ok();
        });
    });

    c.bench_function("dag_topological_sort_medium", |b| {
        let mut dag = TaskDag::new();
        let ids: Vec<_> = (0..20).map(|_| Uuid::new_v4()).collect();
        for (i, id) in ids.iter().enumerate() {
            dag.add_node(*id, &format!("task_{}", i));
        }
        // Create a more complex dependency graph
        for i in 2..ids.len() {
            dag.add_dependency(ids[i], ids[i - 1]).unwrap();
            if i >= 2 {
                dag.add_dependency(ids[i], ids[i - 2]).unwrap();
            }
        }
        b.iter(|| {
            black_box(dag.topological_sort()).ok();
        });
    });
}

criterion_group!(
    benches,
    bench_task_metadata_creation,
    bench_task_metadata_clone,
    bench_serialized_task_creation,
    bench_task_validation,
    bench_state_transitions,
    bench_retry_strategies,
    bench_dag_operations
);
criterion_main!(benches);
