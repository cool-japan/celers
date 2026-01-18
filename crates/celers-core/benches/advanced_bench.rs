use celers_core::{
    exception::{ExceptionCategory, ExceptionPolicy, TaskException},
    router::{PatternMatcher, RouteRule, Router},
    task::batch,
    SerializedTask, TaskState,
};
use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;
// =============================================================================
// Exception Handling Benchmarks
// =============================================================================

fn bench_exception_creation(c: &mut Criterion) {
    c.bench_function("exception_new_simple", |b| {
        b.iter(|| {
            black_box(TaskException::new(
                "ValueError",
                "Invalid input: negative number",
            ));
        });
    });

    c.bench_function("exception_new_with_traceback", |b| {
        let traceback = vec![
            ("process_data".to_string(), "tasks.py".to_string(), 42),
            (
                "validate_input".to_string(),
                "validators.py".to_string(),
                15,
            ),
            ("main".to_string(), "app.py".to_string(), 10),
        ];
        b.iter(|| {
            black_box(
                TaskException::new("ValueError", "Invalid input")
                    .with_traceback(traceback.clone())
                    .with_category(ExceptionCategory::Retryable),
            );
        });
    });

    c.bench_function("exception_clone", |b| {
        let exception = TaskException::new("ValueError", "Invalid input")
            .with_traceback(vec![
                ("func1".to_string(), "file1.py".to_string(), 1),
                ("func2".to_string(), "file2.py".to_string(), 2),
            ])
            .with_category(ExceptionCategory::Retryable);
        b.iter(|| {
            black_box(exception.clone());
        });
    });
}

fn bench_exception_policy(c: &mut Criterion) {
    c.bench_function("exception_policy_create", |b| {
        b.iter(|| {
            black_box(
                ExceptionPolicy::new()
                    .retry_on(&["TimeoutError", "ConnectionError"])
                    .fail_on(&["ValidationError", "ValueError"])
                    .ignore_on(&["NotFoundError"]),
            );
        });
    });

    c.bench_function("exception_policy_get_action_retry", |b| {
        let policy = ExceptionPolicy::new()
            .retry_on(&["TimeoutError", "ConnectionError"])
            .fail_on(&["ValidationError"]);
        let exception = TaskException::new("TimeoutError", "Connection timed out after 30s");
        b.iter(|| {
            black_box(policy.get_action(&exception));
        });
    });

    c.bench_function("exception_policy_get_action_fail", |b| {
        let policy = ExceptionPolicy::new()
            .retry_on(&["TimeoutError"])
            .fail_on(&["ValidationError"]);
        let exception = TaskException::new("ValidationError", "Invalid data");
        b.iter(|| {
            black_box(policy.get_action(&exception));
        });
    });

    c.bench_function("exception_policy_pattern_matching", |b| {
        let policy = ExceptionPolicy::new()
            .retry_on(&["*Error", "Timeout*"])
            .fail_on(&["Validation*"]);
        let exception = TaskException::new("NetworkError", "Connection failed");
        b.iter(|| {
            black_box(policy.get_action(&exception));
        });
    });
}

fn bench_exception_serialization(c: &mut Criterion) {
    c.bench_function("exception_to_json", |b| {
        let exception = TaskException::new("ValueError", "Invalid input")
            .with_traceback(vec![
                ("process".to_string(), "tasks.py".to_string(), 42),
                ("validate".to_string(), "validators.py".to_string(), 15),
            ])
            .with_category(ExceptionCategory::Retryable);
        b.iter(|| {
            black_box(serde_json::to_string(&exception).unwrap());
        });
    });

    c.bench_function("exception_from_json", |b| {
        let exception = TaskException::new("ValueError", "Invalid input")
            .with_traceback(vec![("process".to_string(), "tasks.py".to_string(), 42)])
            .with_category(ExceptionCategory::Retryable);
        let json = serde_json::to_string(&exception).unwrap();
        b.iter(|| {
            black_box(serde_json::from_str::<TaskException>(&json).unwrap());
        });
    });
}

// =============================================================================
// Router/Pattern Matching Benchmarks
// =============================================================================

fn bench_pattern_matching(c: &mut Criterion) {
    c.bench_function("pattern_exact_match", |b| {
        let matcher = PatternMatcher::exact("tasks.process_data");
        b.iter(|| {
            black_box(matcher.matches("tasks.process_data"));
        });
    });

    c.bench_function("pattern_glob_match_simple", |b| {
        let matcher = PatternMatcher::glob("tasks.*");
        b.iter(|| {
            black_box(matcher.matches("tasks.process_data"));
        });
    });

    c.bench_function("pattern_glob_match_complex", |b| {
        let matcher = PatternMatcher::glob("tasks.*.urgent");
        b.iter(|| {
            black_box(matcher.matches("tasks.email.urgent"));
        });
    });

    c.bench_function("pattern_regex_match", |b| {
        let matcher = PatternMatcher::regex(r"tasks\.[a-z]+\.\d+").unwrap();
        b.iter(|| {
            black_box(matcher.matches("tasks.process.123"));
        });
    });

    c.bench_function("pattern_all_match", |b| {
        let matcher = PatternMatcher::all();
        b.iter(|| {
            black_box(matcher.matches("any.task.name"));
        });
    });
}

fn bench_router_operations(c: &mut Criterion) {
    c.bench_function("router_add_rule", |b| {
        b.iter(|| {
            let mut router = Router::new();
            router.add_rule(RouteRule::new(PatternMatcher::glob("tasks.*"), "default"));
            black_box(());
        });
    });

    c.bench_function("router_route_simple", |b| {
        let mut router = Router::new();
        router.add_rule(RouteRule::new(
            PatternMatcher::glob("email.*"),
            "email_queue",
        ));
        router.add_rule(RouteRule::new(PatternMatcher::glob("tasks.*"), "default"));
        b.iter(|| {
            black_box(router.route("email.send_newsletter"));
        });
    });

    c.bench_function("router_route_multiple_rules", |b| {
        let mut router = Router::new();
        router.add_rule(RouteRule::new(
            PatternMatcher::glob("email.*"),
            "email_queue",
        ));
        router.add_rule(RouteRule::new(PatternMatcher::glob("sms.*"), "sms_queue"));
        router.add_rule(RouteRule::new(
            PatternMatcher::glob("urgent.*"),
            "high_priority",
        ));
        router.add_rule(RouteRule::new(PatternMatcher::glob("batch.*"), "batch"));
        router.add_rule(RouteRule::new(PatternMatcher::all(), "default"));
        b.iter(|| {
            black_box(router.route("urgent.process_payment"));
        });
    });

    c.bench_function("router_route_with_fallback", |b| {
        let mut router = Router::new();
        router.add_rule(RouteRule::new(
            PatternMatcher::glob("email.*"),
            "email_queue",
        ));
        router.set_default_queue("default".to_string());
        b.iter(|| {
            black_box(router.route("unknown.task"));
        });
    });
}

// =============================================================================
// Batch Operations Benchmarks
// =============================================================================

fn bench_batch_operations(c: &mut Criterion) {
    let tasks: Vec<SerializedTask> = (0..100)
        .map(|i| {
            let mut task = SerializedTask::new(format!("task_{}", i % 10), vec![1u8; 100]);
            if i % 3 == 0 {
                task.metadata.priority = 10;
            }
            if i % 5 == 0 {
                task.metadata.state = TaskState::Running;
            }
            task
        })
        .collect();

    c.bench_function("batch_validate_all_100_tasks", |b| {
        b.iter(|| {
            black_box(batch::validate_all(&tasks));
        });
    });

    c.bench_function("batch_filter_high_priority", |b| {
        b.iter(|| {
            black_box(batch::filter_high_priority(&tasks));
        });
    });

    c.bench_function("batch_count_by_state", |b| {
        b.iter(|| {
            black_box(batch::count_by_state(&tasks));
        });
    });

    c.bench_function("batch_sort_by_priority", |b| {
        let mut tasks_copy = tasks.clone();
        b.iter(|| {
            batch::sort_by_priority(&mut tasks_copy);
            black_box(());
        });
    });

    c.bench_function("batch_total_payload_size", |b| {
        b.iter(|| {
            black_box(batch::total_payload_size(&tasks));
        });
    });

    c.bench_function("batch_filter_by_state", |b| {
        b.iter(|| {
            black_box(batch::filter_by_state(&tasks, TaskState::is_running));
        });
    });

    c.bench_function("batch_filter_retryable", |b| {
        b.iter(|| {
            black_box(batch::filter_retryable(&tasks));
        });
    });

    c.bench_function("batch_average_payload_size", |b| {
        b.iter(|| {
            black_box(batch::average_payload_size(&tasks));
        });
    });

    c.bench_function("batch_find_oldest", |b| {
        b.iter(|| {
            black_box(batch::find_oldest(&tasks));
        });
    });

    c.bench_function("batch_find_newest", |b| {
        b.iter(|| {
            black_box(batch::find_newest(&tasks));
        });
    });
}

fn bench_batch_filtering(c: &mut Criterion) {
    let tasks: Vec<SerializedTask> = (0..500)
        .map(|i| {
            let mut task = SerializedTask::new(
                if i % 2 == 0 {
                    format!("email.task_{i}")
                } else {
                    format!("sms.task_{i}")
                },
                vec![1u8; 50],
            );
            if i % 7 == 0 {
                task.metadata.priority = 10;
            }
            task
        })
        .collect();

    c.bench_function("batch_filter_by_name_pattern_500", |b| {
        b.iter(|| {
            black_box(batch::filter_by_name_pattern(&tasks, "email.*"));
        });
    });

    c.bench_function("batch_filter_terminal", |b| {
        b.iter(|| {
            black_box(batch::filter_terminal(&tasks));
        });
    });

    c.bench_function("batch_filter_active", |b| {
        b.iter(|| {
            black_box(batch::filter_active(&tasks));
        });
    });

    c.bench_function("batch_has_expired_tasks", |b| {
        b.iter(|| {
            black_box(batch::has_expired_tasks(&tasks));
        });
    });
}

criterion_group!(
    exception_benches,
    bench_exception_creation,
    bench_exception_policy,
    bench_exception_serialization
);

criterion_group!(
    router_benches,
    bench_pattern_matching,
    bench_router_operations
);

criterion_group!(batch_benches, bench_batch_operations, bench_batch_filtering);

criterion_main!(exception_benches, router_benches, batch_benches);
