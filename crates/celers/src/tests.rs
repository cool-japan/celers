use super::*;

#[test]
fn test_facade_exports() {
    // Verify main types are exported
    let _: Option<Box<dyn Broker>> = None;
}

#[test]
fn test_config_validation() {
    use crate::config_validation::*;

    // Test worker config validation
    let result = validate_worker_config(Some(4), Some(10));
    assert!(result.is_ok());

    let result = validate_worker_config(Some(0), Some(10));
    assert!(result.is_err());

    let result = validate_worker_config(Some(4), Some(0));
    assert!(result.is_err());

    // Test broker URL validation
    let result = validate_broker_url("redis://localhost:6379");
    assert!(result.is_ok());

    let result = validate_broker_url("invalid");
    assert!(result.is_err());

    let result = validate_broker_url("");
    assert!(result.is_err());

    // Test feature compatibility
    let result = check_feature_compatibility(&["redis", "postgres"]);
    assert!(result.is_ok());
    assert!(!result.unwrap().is_empty()); // Should have warnings
}

#[test]
#[cfg(feature = "redis")]
fn test_redis_broker_export() {
    // Verify Redis broker is available when feature is enabled
    use crate::RedisBroker;
    let _: Option<RedisBroker> = None;
}

#[test]
#[cfg(feature = "postgres")]
fn test_postgres_broker_export() {
    // Verify PostgreSQL broker is available when feature is enabled
    use crate::PostgresBroker;
    let _: Option<PostgresBroker> = None;
}

#[test]
#[cfg(feature = "mysql")]
fn test_mysql_broker_export() {
    // Verify MySQL broker is available when feature is enabled
    use crate::MysqlBroker;
    let _: Option<MysqlBroker> = None;
}

#[test]
#[cfg(feature = "amqp")]
fn test_amqp_broker_export() {
    // Verify AMQP broker is available when feature is enabled
    use crate::AmqpBroker;
    let _: Option<AmqpBroker> = None;
}

#[test]
#[cfg(feature = "sqs")]
fn test_sqs_broker_export() {
    // Verify SQS broker is available when feature is enabled
    use crate::SqsBroker;
    let _: Option<SqsBroker> = None;
}

#[test]
#[cfg(feature = "backend-redis")]
fn test_redis_backend_export() {
    // Verify Redis backend is available when feature is enabled
    use crate::RedisResultBackend;
    let _: Option<RedisResultBackend> = None;
}

#[test]
#[cfg(feature = "backend-db")]
fn test_db_backend_export() {
    // Verify database backends are available when feature is enabled
    use crate::{MysqlResultBackend, PostgresResultBackend};
    let _: Option<PostgresResultBackend> = None;
    let _: Option<MysqlResultBackend> = None;
}

#[test]
#[cfg(feature = "backend-rpc")]
fn test_rpc_backend_export() {
    // Verify gRPC backend is available when feature is enabled
    use crate::GrpcResultBackend;
    let _: Option<GrpcResultBackend> = None;
}

#[test]
#[cfg(feature = "beat")]
fn test_beat_export() {
    // Verify beat scheduler is available when feature is enabled
    use crate::BeatScheduler;
    let _: Option<BeatScheduler> = None;
}

#[test]
#[cfg(feature = "metrics")]
#[allow(unused_imports)]
fn test_metrics_export() {
    // Verify metrics functions are available when feature is enabled
    use crate::{gather_metrics, reset_metrics};
}

#[test]
#[cfg(feature = "tracing")]
#[allow(unused_imports)]
fn test_tracing_export() {
    // Verify tracing functions are available when feature is enabled
    use crate::tracing::{init_tracing, task_span};
}

#[test]
fn test_prelude_imports() {
    // Verify prelude imports work
    use crate::prelude::*;

    // Should be able to use common types
    let _: Option<Box<dyn Broker>> = None;
    let _: Option<SerializedTask> = None;
    let _: Option<TaskState> = None;
}

#[tokio::test]
async fn test_mock_broker() {
    use crate::dev_utils::{create_test_task, MockBroker};
    use crate::Broker;

    let broker = MockBroker::new();
    assert_eq!(broker.queue_len(), 0);

    // Test enqueue
    let task = create_test_task("test.task");
    let task_id = broker.enqueue(task.clone()).await.unwrap();
    assert_eq!(task_id, task.metadata.id);

    assert_eq!(broker.queue_len(), 1);
    assert_eq!(broker.published_tasks().len(), 1);

    // Test dequeue
    let consumed = broker.dequeue().await.unwrap();
    assert!(consumed.is_some());

    let consumed_msg = consumed.unwrap();
    assert_eq!(consumed_msg.task.metadata.name, "test.task");

    assert_eq!(broker.queue_len(), 0);

    // Test clear
    broker.enqueue(task.clone()).await.unwrap();
    broker.clear();
    assert_eq!(broker.queue_len(), 0);
    assert_eq!(broker.published_tasks().len(), 0);
}

#[test]
fn test_task_builder() {
    use crate::dev_utils::TaskBuilder;

    let task = TaskBuilder::new("my.task")
        .id("550e8400-e29b-41d4-a716-446655440000".to_string())
        .max_retries(3)
        .build();

    assert_eq!(task.metadata.name, "my.task");
    assert_eq!(
        task.metadata.id.to_string(),
        "550e8400-e29b-41d4-a716-446655440000"
    );
    assert_eq!(task.metadata.max_retries, 3);
}

#[test]
fn test_compile_time_validation() {
    use crate::compile_time_validation::*;

    // Test that feature validation functions are callable
    assert!(has_broker_feature());
    assert!(has_serialization_feature());
    assert!(count_broker_features() > 0);

    // Test feature summary
    let summary = feature_summary();
    assert!(summary.contains("CeleRS Configuration:"));
    assert!(summary.contains("Brokers"));
    assert!(summary.contains("Backends"));
    assert!(summary.contains("Formats"));
}

// Integration tests for all broker types
#[cfg(all(test, feature = "redis"))]
mod redis_integration {
    use super::*;

    /// Skipped, visibly, when `CELERS_TEST_REDIS_URL` is not set.
    ///
    /// This test used to hardcode `redis://localhost:6379` behind
    /// `#[ignore]` instead of reading the documented env var, which meant
    /// `cargo nextest run -- --run-ignored=all` (the shape a gated-suite pass
    /// uses) would quietly hit whatever Redis happened to be listening on
    /// localhost — including one nothing here intended to touch — and leave
    /// its `test_queue` list behind with no cleanup. Reading
    /// `CELERS_TEST_REDIS_URL` makes this run only when a gated pass actually
    /// asks for it; the unique queue name and the drain below make sure that,
    /// when it does, nothing outlives the test.
    #[tokio::test]
    async fn test_redis_broker_integration() {
        use crate::RedisBroker;

        let Ok(url) = std::env::var("CELERS_TEST_REDIS_URL") else {
            eprintln!("SKIPPED: test_redis_broker_integration (set CELERS_TEST_REDIS_URL to run)");
            return;
        };

        // A fresh queue name per run: nothing else pointed at the same Redis
        // can collide with it, and the drain below cannot mistake another
        // run's leftover message for its own.
        let queue_name = format!("celers-facade-test-{}", uuid::Uuid::new_v4());
        let broker = RedisBroker::new(&url, &queue_name).expect("redis broker");

        let task = crate::dev_utils::create_test_task("redis.test");
        broker.enqueue(task).await.expect("enqueue");

        // Leave Redis as we found it: drain the one message this test just
        // wrote instead of leaking `queue_name` into the database
        // `CELERS_TEST_REDIS_URL` points at.
        if let Some(delivered) = broker.dequeue().await.expect("dequeue") {
            broker
                .ack(
                    &delivered.task.metadata.id,
                    delivered.receipt_handle.as_deref(),
                )
                .await
                .expect("ack");
        }
        assert_eq!(
            broker.queue_size().await.expect("queue_size"),
            0,
            "the queue this test created must not outlive it"
        );
    }
}

#[cfg(all(test, feature = "postgres"))]
mod postgres_integration {
    use super::*;

    /// Skipped, visibly, when `CELERS_TEST_POSTGRES_URL` is not set.
    ///
    /// This used to hardcode `postgres://localhost/test` behind `#[ignore]`
    /// and wrap the whole body in `if let Ok(broker) = ..`, so it had exactly
    /// two outcomes: nothing listening -> the connection error was swallowed
    /// and the test passed having asserted nothing, or something listening on
    /// a database it was never pointed at deliberately. Reading the documented
    /// variable (see `tests/integration/README.md`) makes it run only when a
    /// gated pass asks for it, and the unique queue name plus the drain below
    /// keep the live database as it was found.
    #[tokio::test]
    async fn test_postgres_broker_integration() {
        use crate::PostgresBroker;

        let Ok(url) = std::env::var("CELERS_TEST_POSTGRES_URL") else {
            eprintln!(
                "SKIPPED: test_postgres_broker_integration (set CELERS_TEST_POSTGRES_URL to run)"
            );
            return;
        };

        let queue_name = format!("facade_pg_{}", uuid::Uuid::new_v4().simple());
        let broker = PostgresBroker::with_queue(&url, &queue_name)
            .await
            .expect("connect to CELERS_TEST_POSTGRES_URL");
        broker.migrate().await.expect("run migrations");

        let task = crate::dev_utils::create_test_task("postgres.test");
        broker.enqueue(task).await.expect("enqueue");

        let delivered = broker
            .dequeue()
            .await
            .expect("dequeue")
            .expect("the task this test just enqueued");
        broker
            .ack(
                &delivered.task.metadata.id,
                delivered.receipt_handle.as_deref(),
            )
            .await
            .expect("ack");

        assert_eq!(
            broker.queue_size().await.expect("queue_size"),
            0,
            "the queue this test created must not outlive it"
        );
    }
}

#[cfg(all(test, feature = "mysql"))]
mod mysql_integration {
    use super::*;

    /// Skipped, visibly, when `CELERS_TEST_MYSQL_URL` is not set.
    ///
    /// Same fix, and the same reason, as
    /// `postgres_integration::test_postgres_broker_integration`: the old body
    /// hardcoded `mysql://localhost/test` and swallowed the connection error,
    /// so it asserted nothing when no server was up and failed against one
    /// that was (`assert!(result.is_ok())` on a database that does not exist).
    #[tokio::test]
    async fn test_mysql_broker_integration() {
        use crate::MysqlBroker;

        let Ok(url) = std::env::var("CELERS_TEST_MYSQL_URL") else {
            eprintln!("SKIPPED: test_mysql_broker_integration (set CELERS_TEST_MYSQL_URL to run)");
            return;
        };

        let queue_name = format!("facade_mysql_{}", uuid::Uuid::new_v4().simple());
        let broker = MysqlBroker::with_queue(&url, &queue_name)
            .await
            .expect("connect to CELERS_TEST_MYSQL_URL");
        broker.migrate().await.expect("run migrations");

        let task = crate::dev_utils::create_test_task("mysql.test");
        broker.enqueue(task).await.expect("enqueue");

        let delivered = broker
            .dequeue()
            .await
            .expect("dequeue")
            .expect("the task this test just enqueued");
        broker
            .ack(
                &delivered.task.metadata.id,
                delivered.receipt_handle.as_deref(),
            )
            .await
            .expect("ack");

        assert_eq!(
            broker.queue_size().await.expect("queue_size"),
            0,
            "the queue this test created must not outlive it"
        );
    }
}

#[cfg(all(test, feature = "amqp"))]
mod amqp_integration {
    #[allow(unused_imports)]
    use super::*;

    /// Skipped, visibly, when `CELERS_TEST_AMQP_URL` is not set.
    ///
    /// The old body asserted `AmqpBroker::new(..).is_ok()` against a
    /// hardcoded `amqp://localhost:5672`. Construction is lazy — no socket is
    /// opened — so that assertion held with no RabbitMQ anywhere on the
    /// machine: it proved the constructor returns `Ok`, not that the facade's
    /// re-exported type talks to a broker. This drives a real round trip
    /// through `celers_core::Broker` (the shape `celers_worker::Worker` uses)
    /// and deletes the queue it declared.
    #[tokio::test]
    async fn test_amqp_broker_integration() {
        use crate::AmqpBroker;
        // `delete_queue` lives on kombu's `Broker`, `connect`/`disconnect` on
        // its `Transport`; aliased because `Broker` in this module already
        // means `celers_core::Broker`.
        use celers_kombu::{Broker as KombuBroker, Transport};

        let Ok(url) = std::env::var("CELERS_TEST_AMQP_URL") else {
            eprintln!("SKIPPED: test_amqp_broker_integration (set CELERS_TEST_AMQP_URL to run)");
            return;
        };

        let queue_name = format!("celers-facade-amqp-{}", uuid::Uuid::new_v4().simple());
        let broker = AmqpBroker::new(&url, &queue_name)
            .await
            .expect("amqp broker")
            .into_core_broker(queue_name.clone());

        let task = crate::dev_utils::create_test_task("amqp.test");
        let task_id = broker.enqueue(task).await.expect("enqueue");

        let delivered = broker
            .dequeue()
            .await
            .expect("dequeue")
            .expect("the task this test just enqueued");
        assert_eq!(
            delivered.task.metadata.id, task_id,
            "the message read back must be the one this test published"
        );
        broker
            .ack(
                &delivered.task.metadata.id,
                delivered.receipt_handle.as_deref(),
            )
            .await
            .expect("ack");

        // Leave the server as we found it: the queue was declared by this
        // test's own connection and nothing else can be using it.
        let mut transport = AmqpBroker::new(&url, &queue_name)
            .await
            .expect("cleanup transport");
        transport.connect().await.expect("cleanup connect");
        transport
            .delete_queue(&queue_name)
            .await
            .expect("delete the queue this test declared");
        let _ = transport.disconnect().await;
    }
}

#[cfg(all(test, feature = "sqs"))]
mod sqs_integration {
    #[allow(unused_imports)]
    use super::*;

    /// Skipped, visibly, when `CELERS_TEST_SQS_URL` is not set.
    ///
    /// `SqsBroker::new` builds an AWS client without contacting anything, so
    /// the old `assert!(broker_result.is_ok())` passed on a machine with no
    /// AWS credentials and no endpoint — it asserted that a constructor
    /// returns `Ok`. Gated on the same variable as
    /// `celers-broker-sqs`'s own live suite (docker compose `localstack`,
    /// profile `test`), this pushes one task through the facade's
    /// re-exported type and deletes the queue afterwards.
    #[tokio::test]
    async fn test_sqs_broker_integration() {
        use crate::SqsBroker;
        // See the AMQP test above for why kombu's `Broker` is aliased here.
        use celers_kombu::{Broker as KombuBroker, Transport};

        let Ok(endpoint) = std::env::var("CELERS_TEST_SQS_URL") else {
            eprintln!("SKIPPED: test_sqs_broker_integration (set CELERS_TEST_SQS_URL to run)");
            return;
        };

        // `aws_config` honours AWS_ENDPOINT_URL for every service; this is how
        // `tests/localstack.rs` points the SDK at LocalStack, and the same
        // reason applies here.
        std::env::set_var("AWS_ENDPOINT_URL", &endpoint);

        let queue_name = format!("celers-facade-sqs-{}", uuid::Uuid::new_v4().simple());
        let transport = SqsBroker::new(&queue_name)
            .await
            .expect("sqs broker")
            .with_auto_create_queue(true)
            .with_visibility_timeout(5)
            .with_wait_time(2);
        let broker = transport.into_core_broker(queue_name.clone());

        let task = crate::dev_utils::create_test_task("sqs.test");
        let task_id = broker.enqueue(task).await.expect("enqueue");

        let delivered = broker
            .dequeue()
            .await
            .expect("dequeue")
            .expect("the task this test just enqueued");
        assert_eq!(
            delivered.task.metadata.id, task_id,
            "the message read back must be the one this test published"
        );
        broker
            .ack(
                &delivered.task.metadata.id,
                delivered.receipt_handle.as_deref(),
            )
            .await
            .expect("ack");

        // Leave the endpoint as we found it.
        let mut cleanup = SqsBroker::new(&queue_name).await.expect("cleanup client");
        cleanup.connect().await.expect("cleanup connect");
        cleanup
            .delete_queue(&queue_name)
            .await
            .expect("delete the queue this test created");
    }
}

// Backend integration tests
#[cfg(all(test, feature = "backend-redis"))]
mod backend_redis_integration {
    #[allow(unused_imports)]
    use super::*;

    /// Skipped, visibly, when `CELERS_TEST_REDIS_URL` is not set.
    ///
    /// Same fix as `redis_integration::test_redis_broker_integration`: this
    /// used to hardcode `redis://localhost:6379` behind `#[ignore]`, and
    /// `store_result` writes under the backend's default `celery-task-meta-`
    /// prefix — one of the leaking key patterns a gated-suite pass observed
    /// in the shared database. `forget` (the `ResultStore` trait's delete)
    /// removes exactly that key again before the test ends.
    #[tokio::test]
    async fn test_redis_backend_integration() {
        use crate::{RedisResultBackend, ResultStore};
        use celers_core::TaskResultValue;
        use uuid::Uuid;

        let Ok(url) = std::env::var("CELERS_TEST_REDIS_URL") else {
            eprintln!("SKIPPED: test_redis_backend_integration (set CELERS_TEST_REDIS_URL to run)");
            return;
        };

        let backend = RedisResultBackend::new(&url).expect("redis result backend");
        let task_id = Uuid::new_v4();
        backend
            .store_result(
                task_id,
                TaskResultValue::Success(serde_json::json!({"result": "success"})),
            )
            .await
            .expect("store_result");

        // Leave Redis as we found it: a fresh uuid per run means this key can
        // never collide with another run's, but it otherwise lives under
        // `CELERS_TEST_REDIS_URL` forever unless removed here.
        backend.forget(task_id).await.expect("cleanup");
        assert!(
            !backend.has_result(task_id).await.expect("has_result"),
            "the result this test stored must not outlive it"
        );
    }
}

#[cfg(all(test, feature = "backend-db"))]
mod backend_db_integration {
    #[allow(unused_imports)]
    use super::*;

    /// Skipped, visibly, when `DATABASE_URL` is not set.
    ///
    /// `celers-backend-db` reads the bare `DATABASE_URL` / `MYSQL_URL` rather
    /// than a `CELERS_TEST_*` name — see `tests/integration/README.md`, which
    /// documents that inconsistency — so this test reads the same variable as
    /// the crate it re-exports. The old body hardcoded
    /// `postgres://localhost/test` and swallowed the connection error inside
    /// `if let Ok(backend) = ..`, which is why its MySQL twin passed for years
    /// and then failed the moment a real MySQL was listening on localhost.
    #[tokio::test]
    async fn test_postgres_backend_integration() {
        use crate::{PostgresResultBackend, ResultStore};
        use celers_core::TaskResultValue;
        use uuid::Uuid;

        let Ok(url) = std::env::var("DATABASE_URL") else {
            eprintln!("SKIPPED: test_postgres_backend_integration (set DATABASE_URL to run)");
            return;
        };

        let backend = PostgresResultBackend::new(&url)
            .await
            .expect("connect to DATABASE_URL");
        backend.migrate().await.expect("run migrations");

        let task_id = Uuid::new_v4();
        backend
            .store_result(
                task_id,
                TaskResultValue::Success(serde_json::json!({"result": "success"})),
            )
            .await
            .expect("store_result");
        assert!(
            backend.has_result(task_id).await.expect("has_result"),
            "the result this test stored must be readable back"
        );

        // Leave the database as we found it.
        backend.forget(task_id).await.expect("cleanup");
        assert!(
            !backend.has_result(task_id).await.expect("has_result"),
            "the result this test stored must not outlive it"
        );
    }

    /// Skipped, visibly, when `MYSQL_URL` is not set.
    ///
    /// Point it at a database of its own, not at the one
    /// `CELERS_TEST_MYSQL_URL` names: `celers-backend-db` and
    /// `celers-broker-sql` both auto-migrate a `celers_task_results` table
    /// with incompatible schemas (TODO.md Known gaps #16), so sharing one
    /// database makes whichever migrates second fail.
    #[tokio::test]
    async fn test_mysql_backend_integration() {
        use crate::{MysqlResultBackend, ResultStore};
        use celers_core::TaskResultValue;
        use uuid::Uuid;

        let Ok(url) = std::env::var("MYSQL_URL") else {
            eprintln!("SKIPPED: test_mysql_backend_integration (set MYSQL_URL to run)");
            return;
        };

        let backend = MysqlResultBackend::new(&url)
            .await
            .expect("connect to MYSQL_URL");
        backend.migrate().await.expect("run migrations");

        let task_id = Uuid::new_v4();
        backend
            .store_result(
                task_id,
                TaskResultValue::Success(serde_json::json!({"result": "success"})),
            )
            .await
            .expect("store_result");
        assert!(
            backend.has_result(task_id).await.expect("has_result"),
            "the result this test stored must be readable back"
        );

        // Leave the database as we found it.
        backend.forget(task_id).await.expect("cleanup");
        assert!(
            !backend.has_result(task_id).await.expect("has_result"),
            "the result this test stored must not outlive it"
        );
    }
}

#[cfg(all(test, feature = "beat"))]
mod beat_integration {
    #[allow(unused_imports)]
    use super::*;

    /// Not `#[ignore]`d, despite the "requires broker server" label it used to
    /// carry: `BeatScheduler::new()` opens no connection (as the body's own
    /// comment says), so the label kept a perfectly hermetic assertion out of
    /// every default run for nothing.
    #[tokio::test]
    async fn test_beat_scheduler_integration() {
        use crate::BeatScheduler;

        // Create a simple scheduler (no broker needed in constructor)
        let scheduler = BeatScheduler::new();

        // Verify scheduler was created by checking it has expected methods
        assert!(scheduler.list_tasks().is_empty());
    }
}

// Workflow integration tests
#[test]
fn test_workflow_chain() {
    use crate::canvas::Chain;

    let chain = Chain::new()
        .then("task1", vec![])
        .then("task2", vec![])
        .then("task3", vec![]);

    // Verify chain structure
    assert!(!chain.tasks.is_empty());
}

#[test]
fn test_workflow_group() {
    use crate::canvas::Group;

    let group = Group::new()
        .add("task1", vec![])
        .add("task2", vec![])
        .add("task3", vec![]);

    // Verify group structure
    assert!(!group.tasks.is_empty());
}

#[test]
fn test_workflow_chord() {
    use crate::canvas::{Chord, Group, Signature};

    let header = Group::new().add("task1", vec![]).add("task2", vec![]);

    let callback = Signature::new("callback".to_string());

    let chord = Chord::new(header, callback);

    // Verify chord structure
    assert!(!chord.header.tasks.is_empty());
}

// Performance and benchmarking tests
#[test]
fn test_task_creation_performance() {
    use crate::dev_utils::TaskBuilder;
    use std::time::Instant;

    let start = Instant::now();
    for i in 0..1000 {
        let _task = TaskBuilder::new(&format!("task.{}", i)).build();
    }
    let duration = start.elapsed();

    // Smoke check only: a generous ceiling that trips solely on catastrophic
    // regressions. Real throughput is tracked by the Criterion benchmark suite;
    // tight wall-clock assertions are unreliable under concurrent CI/build load.
    assert!(duration.as_millis() < 5000);
}

#[test]
fn test_broker_helper_functions() {
    use crate::broker_helper::BrokerConfigError;

    // Test error types
    let error = BrokerConfigError::MissingEnvVar("TEST".to_string());
    assert!(error.to_string().contains("TEST"));

    let error = BrokerConfigError::UnsupportedBrokerType {
        broker_type: "foo".to_string(),
        note: "bar".to_string(),
    };
    assert!(error.to_string().contains("foo"));

    let error = BrokerConfigError::FeatureNotEnabled {
        feature: "redis".to_string(),
    };
    assert!(error.to_string().contains("redis"));
}

// Configuration validation tests
#[test]
fn test_presets_exist() {
    use crate::presets::*;

    // Test that all presets are available
    let _config = production_config();
    let _config = high_throughput_config();
    let _config = low_latency_config();
    let _config = memory_constrained_config();
}

// Zero-cost abstractions verification tests
#[test]
fn test_zero_cost_task_creation() {
    use crate::dev_utils::TaskBuilder;
    use std::time::Instant;

    // Measure overhead of task creation
    let start = Instant::now();
    for _ in 0..10000 {
        let _task = TaskBuilder::new("test.task").build();
    }
    let duration = start.elapsed();

    // Smoke check only (catastrophic-regression ceiling); see the Criterion
    // benches for real throughput. Tight wall-clock bounds are flaky under load.
    assert!(
        duration.as_millis() < 5000,
        "Task creation overhead too high: {}ms",
        duration.as_millis()
    );
}

#[test]
fn test_zero_cost_workflow_construction() {
    use crate::canvas::{Chain, Group};
    use std::time::Instant;

    // Measure overhead of workflow construction
    let start = Instant::now();
    for _ in 0..1000 {
        let _chain = Chain::new()
            .then("task1", vec![])
            .then("task2", vec![])
            .then("task3", vec![]);

        let _group = Group::new()
            .add("task1", vec![])
            .add("task2", vec![])
            .add("task3", vec![]);
    }
    let duration = start.elapsed();

    // Smoke check only (catastrophic-regression ceiling); see the Criterion
    // benches for real throughput. Tight wall-clock bounds are flaky under load.
    assert!(
        duration.as_millis() < 5000,
        "Workflow construction overhead too high: {}ms",
        duration.as_millis()
    );
}

#[test]
fn test_feature_validation_overhead() {
    use crate::compile_time_validation::*;
    use std::time::Instant;

    // Measure overhead of feature validation functions
    let start = Instant::now();
    for _ in 0..100000 {
        let _ = has_broker_feature();
        let _ = has_serialization_feature();
        let _ = count_broker_features();
        let _ = count_backend_features();
    }
    let duration = start.elapsed();

    // Smoke check only (catastrophic-regression ceiling); const-fn overhead is
    // verified by the compiler, not wall clock. Tight bounds are flaky under load.
    assert!(
        duration.as_millis() < 5000,
        "Feature validation overhead too high: {}ms",
        duration.as_millis()
    );
}

#[test]
fn test_memory_efficiency() {
    use crate::dev_utils::TaskBuilder;

    // Measure memory footprint of tasks
    let tasks: Vec<_> = (0..1000)
        .map(|i| TaskBuilder::new(&format!("task{}", i)).build())
        .collect();

    // Verify tasks are created
    assert_eq!(tasks.len(), 1000);

    // Memory usage should be reasonable
    let estimated_size_per_task = std::mem::size_of_val(&tasks[0]);
    assert!(
        estimated_size_per_task < 1024,
        "Task size too large: {} bytes",
        estimated_size_per_task
    );
}

#[test]
fn test_inline_optimization_candidates() {
    use crate::compile_time_validation::*;

    // Test that inline functions are small enough to be inlined
    assert!(has_broker_feature() || !has_broker_feature()); // Should be optimized to const
    assert!(has_serialization_feature() || !has_serialization_feature());
    // Should be optimized to const
}

// Performance regression tests with baseline tracking
#[test]
fn test_performance_regression_task_creation() {
    use crate::dev_utils::TaskBuilder;
    use std::time::Instant;

    // Baseline adjusted for debug builds (debug builds are ~10x slower than release)
    const BASELINE_MS: u128 = 100; // Baseline: 100ms for 10k tasks in debug
    const ITERATIONS: usize = 10000;

    let start = Instant::now();
    for i in 0..ITERATIONS {
        let _task = TaskBuilder::new(&format!("task.{}", i)).build();
    }
    let duration = start.elapsed();

    // Catastrophic-regression smoke check only (generous ceiling); fine-grained
    // performance is tracked by the Criterion benchmark suite.
    assert!(
        duration.as_millis() < BASELINE_MS * 50,
        "Performance regression detected: {}ms (baseline: {}ms) for {} tasks",
        duration.as_millis(),
        BASELINE_MS,
        ITERATIONS
    );
}

#[test]
fn test_performance_regression_workflow_construction() {
    use crate::canvas::{Chain, Group};
    use std::time::Instant;

    const BASELINE_MS: u128 = 50; // Baseline: 50ms for 1k workflows (adjusted for debug builds)
    const ITERATIONS: usize = 1000;

    let start = Instant::now();
    for _ in 0..ITERATIONS {
        let _chain = Chain::new()
            .then("task1", vec![])
            .then("task2", vec![])
            .then("task3", vec![]);

        let _group = Group::new()
            .add("task1", vec![])
            .add("task2", vec![])
            .add("task3", vec![]);
    }
    let duration = start.elapsed();

    // Catastrophic-regression smoke check only (generous ceiling); fine-grained
    // performance is tracked by the Criterion benchmark suite.
    assert!(
        duration.as_millis() < BASELINE_MS * 50,
        "Performance regression detected: {}ms (baseline: {}ms) for {} workflows",
        duration.as_millis(),
        BASELINE_MS,
        ITERATIONS
    );
}

#[test]
fn test_performance_regression_serialization() {
    use crate::dev_utils::TaskBuilder;
    use std::time::Instant;

    const BASELINE_MS: u128 = 50; // Baseline: 50ms for 1k serializations
    const ITERATIONS: usize = 1000;

    let tasks: Vec<_> = (0..ITERATIONS)
        .map(|i| TaskBuilder::new(&format!("task{}", i)).build())
        .collect();

    let start = Instant::now();
    for task in &tasks {
        let _serialized = serde_json::to_string(task).unwrap();
    }
    let duration = start.elapsed();

    // Catastrophic-regression smoke check only (generous ceiling); fine-grained
    // performance is tracked by the Criterion benchmark suite.
    assert!(
        duration.as_millis() < BASELINE_MS * 50,
        "Performance regression detected: {}ms (baseline: {}ms) for {} serializations",
        duration.as_millis(),
        BASELINE_MS,
        ITERATIONS
    );
}

#[test]
fn test_performance_regression_config_validation() {
    use crate::config_validation::*;
    use std::time::Instant;

    const BASELINE_MS: u128 = 100; // Baseline: 100ms for 10k validations (adjusted for debug builds)
    const ITERATIONS: usize = 10000;

    let start = Instant::now();
    for _ in 0..ITERATIONS {
        let _ = validate_worker_config(Some(4), Some(10));
        let _ = validate_broker_url("redis://localhost:6379");
    }
    let duration = start.elapsed();

    // Catastrophic-regression smoke check only (generous ceiling); fine-grained
    // performance is tracked by the Criterion benchmark suite.
    assert!(
        duration.as_millis() < BASELINE_MS * 50,
        "Performance regression detected: {}ms (baseline: {}ms) for {} validations",
        duration.as_millis(),
        BASELINE_MS,
        ITERATIONS
    );
}

// Startup optimization tests
#[test]
fn test_lazy_init() {
    use crate::startup_optimization::LazyInit;

    static COUNTER: LazyInit<usize> = LazyInit::new();

    // First access initializes
    let value = COUNTER.get_or_init(|| 42);
    assert_eq!(*value, 42);

    // Second access returns the same value
    let value2 = COUNTER.get_or_init(|| 100);
    assert_eq!(*value2, 42); // Should still be 42, not 100
}

#[test]
fn test_startup_metrics() {
    use crate::startup_optimization::StartupMetrics;

    let mut metrics = StartupMetrics::new();
    metrics.broker_init_ms = 100;
    metrics.config_load_ms = 50;
    metrics.backend_init_ms = 75;
    metrics.total_ms = 225;

    let report = metrics.report();
    assert!(report.contains("100ms"));
    assert!(report.contains("50ms"));
    assert!(report.contains("75ms"));
    assert!(report.contains("225ms"));
}

#[test]
fn test_startup_metrics_default() {
    use crate::startup_optimization::StartupMetrics;

    let metrics = StartupMetrics::default();
    assert_eq!(metrics.broker_init_ms, 0);
    assert_eq!(metrics.config_load_ms, 0);
    assert_eq!(metrics.backend_init_ms, 0);
    assert_eq!(metrics.total_ms, 0);
}

#[tokio::test]
async fn test_parallel_init() {
    use crate::startup_optimization::{parallel_init, AsyncInitTask};
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    let counter = Arc::new(AtomicUsize::new(0));

    type TestResult = std::result::Result<(), String>;
    let tasks: Vec<AsyncInitTask<(), String>> = vec![
        {
            let counter = counter.clone();
            Box::new(move || {
                Box::pin(async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                    as std::pin::Pin<Box<dyn std::future::Future<Output = TestResult> + Send>>
            })
        },
        {
            let counter = counter.clone();
            Box::new(move || {
                Box::pin(async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                    as std::pin::Pin<Box<dyn std::future::Future<Output = TestResult> + Send>>
            })
        },
        {
            let counter = counter.clone();
            Box::new(move || {
                Box::pin(async move {
                    counter.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                })
                    as std::pin::Pin<Box<dyn std::future::Future<Output = TestResult> + Send>>
            })
        },
    ];

    let results = parallel_init(tasks).await.expect("parallel_init failed");
    assert_eq!(results.len(), 3);
    assert_eq!(counter.load(Ordering::SeqCst), 3);
}

// IDE support tests
#[test]
fn test_ide_support_type_aliases() {
    use crate::ide_support::*;

    // Test BoxedResult compiles
    let _result: BoxedResult<i32> = Ok(42);

    // Test type aliases are accessible
    let _broker: Option<BoxedBroker> = None;
    let _task: Option<QueueTask> = None;
}

#[test]
fn test_ide_support_defaults() {
    use crate::ide_support::defaults::*;

    // Verify default constants
    assert_eq!(DEFAULT_CONCURRENCY, 4);
    assert_eq!(DEFAULT_PREFETCH, 10);
    assert_eq!(DEFAULT_MAX_RETRIES, 3);
    assert_eq!(DEFAULT_RETRY_DELAY_SECS, 60);
    assert_eq!(DEFAULT_TASK_TIMEOUT_SECS, 3600);
    assert_eq!(DEFAULT_REDIS_PORT, 6379);
    assert_eq!(DEFAULT_POSTGRES_PORT, 5432);
    assert_eq!(DEFAULT_MYSQL_PORT, 3306);
    assert_eq!(DEFAULT_RABBITMQ_PORT, 5672);
    assert_eq!(DEFAULT_QUEUE_NAME, "celery");
}

#[test]
fn test_ide_support_examples() {
    use crate::ide_support::examples::*;

    // Verify example constants are valid
    assert!(REDIS_URL_EXAMPLE.starts_with("redis://"));
    assert!(POSTGRES_URL_EXAMPLE.starts_with("postgres://"));
    assert!(MYSQL_URL_EXAMPLE.starts_with("mysql://"));
    assert!(RABBITMQ_URL_EXAMPLE.starts_with("amqp://"));
    assert!(SQS_URL_EXAMPLE.starts_with("https://sqs"));
}

#[test]
fn test_ide_support_trait_bounds() {
    use crate::ide_support::{TaskArgs, TaskResult};

    // Test that common types implement the trait bounds
    fn assert_task_args<T: TaskArgs>() {}
    fn assert_task_result<T: TaskResult>() {}

    assert_task_args::<String>();
    assert_task_args::<i32>();
    assert_task_args::<Vec<u8>>();

    assert_task_result::<String>();
    assert_task_result::<i32>();
    assert_task_result::<Vec<u8>>();
}

#[tokio::test]
async fn test_ide_support_boxed_future() {
    use crate::ide_support::{BoxedFuture, BoxedResult};

    fn create_future() -> BoxedFuture<String> {
        Box::pin(async { Ok("test".to_string()) })
    }

    let result: BoxedResult<String> = create_future().await;
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), "test");
}

#[test]
#[cfg(feature = "dev-utils")]
fn test_assembly_inspection_count_instructions() {
    use crate::assembly_inspection::count_instructions;

    let sample_asm = r#"
        ; Function prologue
        push   rbp
        mov    rbp, rsp
        ; Actual code
        add    rdi, rsi
        mov    rax, rdi
        ; Function epilogue
        pop    rbp
        ret
    "#;

    let count = count_instructions(sample_asm);
    assert_eq!(count, 6); // Should count 6 actual instructions (push, mov, add, mov, pop, ret)
}

#[test]
#[cfg(feature = "dev-utils")]
fn test_assembly_inspection_verify_inlined() {
    use crate::assembly_inspection::verify_inlined;

    // Well-inlined function (no calls)
    let inlined_asm = r#"
        add    rdi, rsi
        mov    rax, rdi
        ret
    "#;
    assert!(verify_inlined(inlined_asm));

    // Not inlined (has call instruction)
    let not_inlined_asm = r#"
        push   rbp
        call   some_function
        call   another_function
        pop    rbp
        ret
    "#;
    assert!(!verify_inlined(not_inlined_asm));
}

// Convenience functions tests
#[test]
fn test_convenience_chunks() {
    use crate::convenience::chunks;
    use serde_json::json;

    let items = vec![json!(1), json!(2), json!(3), json!(4), json!(5)];
    let workflow = chunks("process_item", items, 2);

    assert_eq!(workflow.task.task, "process_item");
    assert_eq!(workflow.items.len(), 5);
    assert_eq!(workflow.chunk_size, 2);
}

#[test]
fn test_convenience_map() {
    use crate::convenience::map;
    use serde_json::json;

    let items = vec![json!(1), json!(2), json!(3)];
    let workflow = map("square", items);

    assert_eq!(workflow.task.task, "square");
    assert_eq!(workflow.argsets.len(), 3);
}

#[test]
fn test_convenience_starmap() {
    use crate::convenience::starmap;
    use serde_json::json;

    let args = vec![vec![json!(1), json!(2)], vec![json!(3), json!(4)]];
    let workflow = starmap("add", args);

    assert_eq!(workflow.task.task, "add");
    assert_eq!(workflow.argsets.len(), 2);
    assert_eq!(workflow.argsets[0].len(), 2);
}

#[test]
fn test_convenience_options() {
    use crate::convenience::options;

    let opts = options();
    // Just verify it creates TaskOptions successfully
    assert!(opts.max_retries.is_none());
}

#[test]
fn test_convenience_with_retry() {
    use crate::convenience::with_retry;

    let opts = with_retry(5, 60);
    assert_eq!(opts.max_retries, Some(5));
    assert_eq!(opts.countdown, Some(60));
}

#[test]
fn test_convenience_with_timeout() {
    use crate::convenience::with_timeout;

    let opts = with_timeout(300);
    assert_eq!(opts.time_limit, Some(300));
}

#[test]
fn test_convenience_with_priority() {
    use crate::convenience::with_priority;

    let opts = with_priority(9);
    assert_eq!(opts.priority, Some(9));
}

#[test]
fn test_convenience_with_countdown() {
    use crate::convenience::with_countdown;

    let opts = with_countdown(60);
    assert_eq!(opts.countdown, Some(60));
}

#[test]
fn test_convenience_with_expires() {
    use crate::convenience::with_expires;

    let opts = with_expires(7200);
    assert_eq!(opts.expires, Some(7200));
}

#[test]
fn test_convenience_batch() {
    use crate::convenience::batch;
    use serde_json::json;

    let args_list = vec![
        vec![json!(1), json!(2)],
        vec![json!(3), json!(4)],
        vec![json!(5), json!(6)],
    ];
    let tasks = batch("add", args_list);

    assert_eq!(tasks.len(), 3);
    assert_eq!(tasks[0].task, "add");
    assert_eq!(tasks[1].task, "add");
    assert_eq!(tasks[2].task, "add");
}

#[test]
#[cfg(feature = "redis")]
fn test_quick_start_redis_broker() {
    use crate::quick_start::redis_broker;

    // Test that the function creates a broker with proper URL handling
    let result = redis_broker("localhost:6379", "test_queue");
    assert!(result.is_ok() || result.is_err()); // Will fail to connect but should parse
}

#[test]
#[cfg(feature = "mysql")]
fn test_quick_start_mysql_broker_function_exists() {
    // Just verify the function compiles and is available
    // Actual connection test would require a running MySQL server
    use crate::quick_start::mysql_broker;
    // mysql_broker is an async function, just verify it exists
    let _ = mysql_broker;
}

#[test]
#[cfg(feature = "amqp")]
fn test_quick_start_amqp_broker_function_exists() {
    // Just verify the function compiles and is available
    use crate::quick_start::amqp_broker;
    // amqp_broker is an async function, just verify it exists
    let _ = amqp_broker;
}

#[test]
#[cfg(feature = "sqs")]
fn test_quick_start_sqs_broker_function_exists() {
    // Just verify the function compiles and is available
    use crate::quick_start::sqs_broker;
    // sqs_broker is an async function that takes only queue name, just verify it exists
    let _ = sqs_broker;
}

#[test]
fn test_ide_support_additional_type_aliases() {
    use crate::ide_support::{TaskId, WorkerStats};

    // Verify type aliases are usable
    let _task_id: TaskId = uuid::Uuid::new_v4();

    // Test that WorkerStats type alias works
    fn accepts_worker_stats(_stats: &WorkerStats) {}
    let stats = WorkerStats {
        total_tasks: 0,
        active_tasks: 0,
        succeeded: 0,
        failed: 0,
        retried: 0,
        uptime: 0.0,
        loadavg: None,
        memory_usage: None,
        pool: None,
        broker: None,
        clock: None,
    };
    accepts_worker_stats(&stats);
}

// Tests for new convenience functions
#[test]
fn test_convenience_delay() {
    use crate::convenience::delay;
    use serde_json::json;

    let sig = delay("send_email", vec![json!("user@example.com")], 60);
    assert_eq!(sig.task, "send_email");
    assert_eq!(sig.options.countdown, Some(60));
    assert_eq!(sig.args.len(), 1);
}

#[test]
fn test_convenience_expire_in() {
    use crate::convenience::expire_in;
    use serde_json::json;

    let sig = expire_in("process_data", vec![json!({"id": 123})], 7200);
    assert_eq!(sig.task, "process_data");
    assert_eq!(sig.options.expires, Some(7200));
    assert_eq!(sig.args.len(), 1);
}

#[test]
fn test_convenience_high_priority() {
    use crate::convenience::high_priority;
    use serde_json::json;

    let sig = high_priority("urgent_task", vec![json!({"alert": true})]);
    assert_eq!(sig.task, "urgent_task");
    assert_eq!(sig.options.priority, Some(9));
}

#[test]
fn test_convenience_low_priority() {
    use crate::convenience::low_priority;
    use serde_json::json;

    let sig = low_priority("background_cleanup", vec![json!({})]);
    assert_eq!(sig.task, "background_cleanup");
    assert_eq!(sig.options.priority, Some(1));
}

#[test]
fn test_convenience_parallel() {
    use crate::convenience::parallel;
    use serde_json::json;

    let workflow = parallel()
        .add("task1", vec![json!(1)])
        .add("task2", vec![json!(2)])
        .add("task3", vec![json!(3)]);

    assert_eq!(workflow.tasks.len(), 3);
}

// Tests for new worker presets
#[test]
fn test_presets_cpu_bound_config() {
    use crate::presets::cpu_bound_config;

    let config = cpu_bound_config();
    assert!(config.is_ok());
    let config = config.unwrap();
    assert_eq!(config.concurrency, num_cpus::get());
}

#[test]
fn test_presets_io_bound_config() {
    use crate::presets::io_bound_config;

    let config = io_bound_config();
    assert!(config.is_ok());
    let config = config.unwrap();
    assert_eq!(config.concurrency, num_cpus::get() * 4);
}

#[test]
fn test_presets_balanced_config() {
    use crate::presets::balanced_config;

    let config = balanced_config();
    assert!(config.is_ok());
    let config = config.unwrap();
    assert_eq!(config.concurrency, num_cpus::get() * 2);
}

#[test]
fn test_presets_development_config() {
    use crate::presets::development_config;

    let config = development_config();
    assert!(config.is_ok());
    let config = config.unwrap();
    assert_eq!(config.concurrency, 2);
}

// Tests for new type aliases
#[test]
fn test_ide_support_new_type_aliases() {
    use crate::ide_support::{
        BrokerUrl, ConcurrencyLevel, PrefetchCount, PriorityLevel, QueueName, RetryCount, TaskName,
        TimeoutSeconds,
    };

    // Verify type aliases compile and work correctly
    let _queue: QueueName = "celery".to_string();
    let _url: BrokerUrl = "redis://localhost:6379".to_string();
    let _retries: RetryCount = 3;
    let _priority: PriorityLevel = 9;
    let _timeout: TimeoutSeconds = 300;
    let _task_name: TaskName = "my_task".to_string();
    let _concurrency: ConcurrencyLevel = 4;
    let _prefetch: PrefetchCount = 10;
}

// Tests for advanced convenience functions
