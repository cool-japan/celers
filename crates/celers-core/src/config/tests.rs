//! Unit tests for [`crate::config`].
//!
//! Kept in their own file so `config.rs` stays comfortably under the 2000-line
//! limit; as a child module of `config` it still has access to that module's
//! private helpers.

use super::*;
use std::sync::Mutex;

// Mutex to serialize env-var-mutating tests (env vars are process-global)
static ENV_LOCK: Mutex<()> = Mutex::new(());

/// Helper to set env vars safely in tests.
/// SAFETY: These are test-only calls. We hold ENV_LOCK to prevent races.
fn set_env(key: &str, val: &str) {
    unsafe { std::env::set_var(key, val) };
}

/// Helper to remove env vars safely in tests.
fn remove_env(key: &str) {
    unsafe { std::env::remove_var(key) };
}

/// List of all CELERY_* env var keys used by from_env(), for cleanup.
const ALL_ENV_KEYS: &[&str] = &[
    "CELERY_BROKER_URL",
    "CELERY_RESULT_BACKEND",
    "CELERY_TASK_SERIALIZER",
    "CELERY_RESULT_SERIALIZER",
    "CELERY_TIMEZONE",
    "CELERY_DEFAULT_QUEUE",
    "CELERY_DEFAULT_EXCHANGE",
    "CELERY_DEFAULT_EXCHANGE_TYPE",
    "CELERY_DEFAULT_ROUTING_KEY",
    "CELERY_ENABLE_UTC",
    "CELERY_TASK_TRACK_STARTED",
    "CELERY_TASK_SEND_SENT_EVENT",
    "CELERY_TASK_ACKS_LATE",
    "CELERY_TASK_REJECT_ON_WORKER_LOST",
    "CELERYD_CONCURRENCY",
    "CELERYD_PREFETCH_MULTIPLIER",
    "CELERYD_MAX_TASKS_PER_CHILD",
    "CELERYD_MAX_MEMORY_PER_CHILD",
    "CELERY_TASK_TIME_LIMIT",
    "CELERY_TASK_SOFT_TIME_LIMIT",
    "CELERY_TASK_DEFAULT_RETRY_DELAY",
    "CELERY_TASK_MAX_RETRIES",
    "CELERY_RESULT_EXPIRES",
    "CELERY_WORKER_HEARTBEAT",
    "CELERY_ACCEPT_CONTENT",
    "CELERY_RESULT_COMPRESSION",
    "CELERY_RESULT_COMPRESSION_THRESHOLD",
];

fn cleanup_env() {
    for key in ALL_ENV_KEYS {
        remove_env(key);
    }
}

#[test]
fn test_default_config() {
    let config = CeleryConfig::default();
    assert_eq!(config.broker_url, "redis://localhost:6379/0");
    assert_eq!(config.task_serializer, "json");
    assert_eq!(config.timezone, "UTC");
    assert!(config.enable_utc);
}

#[test]
fn test_config_builder() {
    let config = CeleryConfig::new("redis://localhost:6379/0")
        .with_result_backend("redis://localhost:6379/1")
        .with_worker_concurrency(8)
        .with_default_queue("my_queue");

    assert_eq!(config.worker_concurrency, 8);
    assert_eq!(config.task_default_queue, "my_queue");
}

#[test]
fn test_config_validation() {
    let config = CeleryConfig::default();
    assert!(config.validate().is_ok());

    let invalid = CeleryConfig {
        broker_url: String::new(),
        ..Default::default()
    };
    assert!(invalid.validate().is_err());
}

#[test]
fn test_task_route() {
    let route = TaskRoute {
        queue: "high_priority".to_string(),
        exchange: Some("tasks".to_string()),
        routing_key: Some("task.high".to_string()),
        priority: Some(9),
    };

    let config = CeleryConfig::default().with_task_route("important_task", route);

    assert!(config.get_task_route("important_task").is_some());
}

#[test]
fn test_duration_conversions() {
    let config = CeleryConfig::default();
    assert_eq!(config.result_expires_duration(), Duration::from_secs(86400));
}

#[test]
fn test_from_env_boolean_vars() {
    let _guard = ENV_LOCK.lock();
    cleanup_env();

    set_env("CELERY_ENABLE_UTC", "true");
    set_env("CELERY_TASK_TRACK_STARTED", "1");
    set_env("CELERY_TASK_SEND_SENT_EVENT", "yes");
    set_env("CELERY_TASK_ACKS_LATE", "on");
    set_env("CELERY_TASK_REJECT_ON_WORKER_LOST", "false");

    let config = CeleryConfig::from_env();
    assert!(config.enable_utc);
    assert!(config.task_track_started);
    assert!(config.task_send_sent_event);
    assert!(config.task_acks_late);
    assert!(!config.task_reject_on_worker_lost);

    cleanup_env();
}

#[test]
fn test_from_env_numeric_vars() {
    let _guard = ENV_LOCK.lock();
    cleanup_env();

    set_env("CELERYD_PREFETCH_MULTIPLIER", "8");
    set_env("CELERYD_CONCURRENCY", "16");
    set_env("CELERYD_MAX_TASKS_PER_CHILD", "1000");
    set_env("CELERYD_MAX_MEMORY_PER_CHILD", "524288");

    let config = CeleryConfig::from_env();
    assert_eq!(config.worker_prefetch_multiplier, 8);
    assert_eq!(config.worker_concurrency, 16);
    assert_eq!(config.worker_max_tasks_per_child, Some(1000));
    assert_eq!(config.worker_max_memory_per_child, Some(524288));

    cleanup_env();
}

#[test]
fn test_from_env_string_vars() {
    let _guard = ENV_LOCK.lock();
    cleanup_env();

    set_env("CELERY_DEFAULT_QUEUE", "myqueue");
    set_env("CELERY_DEFAULT_EXCHANGE", "myexchange");
    set_env("CELERY_DEFAULT_EXCHANGE_TYPE", "topic");
    set_env("CELERY_DEFAULT_ROUTING_KEY", "task.default");
    set_env("CELERY_RESULT_SERIALIZER", "msgpack");

    let config = CeleryConfig::from_env();
    assert_eq!(config.task_default_queue, "myqueue");
    assert_eq!(config.task_default_exchange, "myexchange");
    assert_eq!(config.task_default_exchange_type, "topic");
    assert_eq!(config.task_default_routing_key, "task.default");
    assert_eq!(config.result_serializer, "msgpack");

    cleanup_env();
}

#[test]
fn test_from_env_duration_vars() {
    let _guard = ENV_LOCK.lock();
    cleanup_env();

    set_env("CELERY_TASK_TIME_LIMIT", "300");
    set_env("CELERY_TASK_SOFT_TIME_LIMIT", "240");
    set_env("CELERY_TASK_DEFAULT_RETRY_DELAY", "60");
    set_env("CELERY_TASK_MAX_RETRIES", "5");
    set_env("CELERY_RESULT_EXPIRES", "3600");

    let config = CeleryConfig::from_env();
    assert_eq!(config.task_time_limit, Some(300));
    assert_eq!(config.task_soft_time_limit, Some(240));
    assert_eq!(config.task_default_retry_delay, 60);
    assert_eq!(config.task_max_retries, 5);
    assert_eq!(config.result_expires, 3600);

    cleanup_env();
}

#[test]
fn test_parse_env_bool_variants() {
    let _guard = ENV_LOCK.lock();
    cleanup_env();

    // Truthy values
    for val in &["true", "TRUE", "True", "1", "yes", "YES", "on", "ON"] {
        set_env("CELERY_ENABLE_UTC", val);
        assert_eq!(
            parse_env_bool_checked("CELERY_ENABLE_UTC"),
            Ok(Some(true)),
            "failed for {}",
            val
        );
    }

    // Falsy values
    for val in &["false", "FALSE", "False", "0", "no", "NO", "off", "OFF"] {
        set_env("CELERY_ENABLE_UTC", val);
        assert_eq!(
            parse_env_bool_checked("CELERY_ENABLE_UTC"),
            Ok(Some(false)),
            "failed for {}",
            val
        );
    }

    // Invalid values are reported as errors (not silently ignored)
    set_env("CELERY_ENABLE_UTC", "maybe");
    assert_eq!(
        parse_env_bool_checked("CELERY_ENABLE_UTC"),
        Err("maybe".to_string())
    );

    // Missing var is "unset", which is distinct from "invalid"
    remove_env("CELERY_ENABLE_UTC");
    assert_eq!(parse_env_bool_checked("CELERY_ENABLE_UTC"), Ok(None));

    cleanup_env();
}

#[test]
fn test_validate_detailed_valid_config() {
    let config = CeleryConfig::default();
    let validation = config.validate_detailed();
    assert!(validation.is_valid());
    assert_eq!(validation.error_count(), 0);
}

#[test]
fn test_validate_detailed_invalid_broker_url() {
    let config = CeleryConfig {
        broker_url: "ftp://bad-scheme".to_string(),
        ..Default::default()
    };
    let validation = config.validate_detailed();
    assert!(!validation.is_valid());
    assert!(validation.errors.iter().any(|e| e.field == "broker_url"));
}

#[test]
fn test_validate_detailed_invalid_serializer() {
    let config = CeleryConfig {
        task_serializer: "xml".to_string(),
        ..Default::default()
    };
    let validation = config.validate_detailed();
    assert!(!validation.is_valid());
    assert!(validation
        .errors
        .iter()
        .any(|e| e.field == "task_serializer"));
}

#[test]
fn test_validate_detailed_zero_concurrency() {
    let config = CeleryConfig {
        worker_concurrency: 0,
        ..Default::default()
    };
    let validation = config.validate_detailed();
    assert!(!validation.is_valid());
    assert!(validation
        .errors
        .iter()
        .any(|e| e.field == "worker_concurrency"));
}

#[test]
fn test_validate_detailed_time_limit_warning() {
    let config = CeleryConfig {
        task_time_limit: Some(60),
        task_soft_time_limit: Some(120), // soft >= hard
        ..Default::default()
    };
    let validation = config.validate_detailed();
    assert!(validation.has_warnings());
    assert!(validation
        .warnings
        .iter()
        .any(|w| w.field == "task_soft_time_limit"));
}

#[test]
fn test_to_env_vars_roundtrip() {
    let config = CeleryConfig::new("amqp://localhost:5672")
        .with_result_backend("redis://localhost:6379/1")
        .with_task_serializer("msgpack")
        .with_result_serializer("json")
        .with_timezone("US/Eastern")
        .with_enable_utc(false)
        .with_worker_concurrency(12)
        .with_prefetch_multiplier(2)
        .with_default_queue("tasks");

    let vars = config.to_env_vars();

    // Check that key env vars are present with correct values
    let find_var = |key: &str| -> Option<String> {
        vars.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone())
    };

    assert_eq!(
        find_var("CELERY_BROKER_URL").as_deref(),
        Some("amqp://localhost:5672")
    );
    assert_eq!(
        find_var("CELERY_RESULT_BACKEND").as_deref(),
        Some("redis://localhost:6379/1")
    );
    assert_eq!(
        find_var("CELERY_TASK_SERIALIZER").as_deref(),
        Some("msgpack")
    );
    assert_eq!(
        find_var("CELERY_RESULT_SERIALIZER").as_deref(),
        Some("json")
    );
    assert_eq!(find_var("CELERY_TIMEZONE").as_deref(), Some("US/Eastern"));
    assert_eq!(find_var("CELERY_ENABLE_UTC").as_deref(), Some("false"));
    assert_eq!(find_var("CELERYD_CONCURRENCY").as_deref(), Some("12"));
    assert_eq!(
        find_var("CELERYD_PREFETCH_MULTIPLIER").as_deref(),
        Some("2")
    );
    assert_eq!(find_var("CELERY_DEFAULT_QUEUE").as_deref(), Some("tasks"));
}

#[test]
fn test_dump_output() {
    let config = CeleryConfig::default();
    let output = config.dump();

    assert!(output.starts_with("CeleRS Configuration:\n"));
    assert!(output.contains("broker_url:"));
    assert!(output.contains("task_serializer:"));
    assert!(output.contains("worker_concurrency:"));
    assert!(output.contains("result_expires:"));
    assert!(output.contains("task_routes:"));
    assert!(output.contains("beat_schedule:"));
}

#[test]
fn test_config_validation_display() {
    let error = ConfigError {
        field: "broker_url".to_string(),
        message: "invalid URL".to_string(),
        suggestion: Some("use redis://".to_string()),
    };
    let display = format!("{}", error);
    assert!(display.contains("[broker_url]"));
    assert!(display.contains("invalid URL"));
    assert!(display.contains("suggestion: use redis://"));

    let error_no_suggestion = ConfigError {
        field: "concurrency".to_string(),
        message: "must be positive".to_string(),
        suggestion: None,
    };
    let display2 = format!("{}", error_no_suggestion);
    assert!(display2.contains("[concurrency]"));
    assert!(display2.contains("must be positive"));
    assert!(!display2.contains("suggestion"));

    let warning = ConfigWarning {
        field: "prefetch".to_string(),
        message: "value too high".to_string(),
    };
    let display3 = format!("{}", warning);
    assert!(display3.contains("[prefetch]"));
    assert!(display3.contains("value too high"));
}

#[test]
fn test_validate_detailed_high_concurrency_warning() {
    let config = CeleryConfig {
        worker_concurrency: 2048,
        ..Default::default()
    };
    let validation = config.validate_detailed();
    assert!(validation.has_warnings());
    assert!(validation
        .warnings
        .iter()
        .any(|w| w.field == "worker_concurrency"));
}

#[test]
fn test_validate_detailed_prefetch_zero_warning() {
    let config = CeleryConfig {
        worker_prefetch_multiplier: 0,
        ..Default::default()
    };
    let validation = config.validate_detailed();
    assert!(validation.has_warnings());
    assert!(validation
        .warnings
        .iter()
        .any(|w| w.field == "worker_prefetch_multiplier"));
}

#[test]
fn test_validate_detailed_high_retries_warning() {
    let config = CeleryConfig {
        task_max_retries: 200,
        ..Default::default()
    };
    let validation = config.validate_detailed();
    assert!(validation.has_warnings());
    assert!(validation
        .warnings
        .iter()
        .any(|w| w.field == "task_max_retries"));
}

#[test]
fn test_validate_detailed_invalid_result_backend() {
    let config = CeleryConfig {
        result_backend: Some("ftp://invalid".to_string()),
        ..Default::default()
    };
    let validation = config.validate_detailed();
    assert!(!validation.is_valid());
    assert!(validation
        .errors
        .iter()
        .any(|e| e.field == "result_backend"));
}

#[test]
fn test_validate_detailed_invalid_result_serializer() {
    let config = CeleryConfig {
        result_serializer: "xml".to_string(),
        ..Default::default()
    };
    let validation = config.validate_detailed();
    assert!(!validation.is_valid());
    assert!(validation
        .errors
        .iter()
        .any(|e| e.field == "result_serializer"));
}

#[test]
fn test_accept_content_is_enforceable() {
    // Regression: `accept_content` was declared, defaulted and settable but
    // nothing could ask it whether a content type was allowed.
    let config =
        CeleryConfig::new("redis://localhost:6379").with_accept_content(vec!["json".to_string()]);

    assert!(config.is_content_type_accepted("json"));
    assert!(config.is_content_type_accepted("application/json"));
    assert!(config.is_content_type_accepted("APPLICATION/JSON; charset=utf-8"));

    // Serializers the operator did not approve are refused.
    assert!(!config.is_content_type_accepted("msgpack"));
    assert!(!config.is_content_type_accepted("application/x-msgpack"));
    assert!(!config.is_content_type_accepted("application/x-python-serialize"));
    assert!(!config.is_content_type_accepted("pickle"));
    assert!(!config.is_content_type_accepted(""));

    assert_eq!(config.accepted_serializer_names(), vec!["json".to_string()]);
}

#[test]
fn test_pickle_is_rejected_by_validation() {
    // Regression: `pickle` passed validation even though no pickle
    // serializer exists in this workspace.
    let config = CeleryConfig {
        task_serializer: "pickle".to_string(),
        ..Default::default()
    };
    let validation = config.validate_detailed();
    assert!(!validation.is_valid());
    assert!(validation
        .errors
        .iter()
        .any(|e| e.field == "task_serializer" && e.message.contains("pickle")));
    assert!(config.validate().is_err());

    let config = CeleryConfig::new("redis://localhost:6379")
        .with_accept_content(vec!["json".to_string(), "pickle".to_string()]);
    assert!(config
        .validate_detailed()
        .errors
        .iter()
        .any(|e| e.field == "accept_content"));
}

#[test]
fn test_validate_agrees_with_validate_detailed() {
    // Regression: the two validators disagreed on the valid serializer set.
    for serializer in ["json", "msgpack", "yaml", "bson", "protobuf"] {
        let config = CeleryConfig::new("redis://localhost:6379")
            .with_task_serializer(serializer)
            .with_result_serializer(serializer)
            .with_accept_content(vec![serializer.to_string()]);
        assert_eq!(
            config.validate().is_ok(),
            config.validate_detailed().is_valid(),
            "validators disagree for {serializer}"
        );
        assert!(config.validate().is_ok(), "{serializer} should be valid");
    }

    // And `validate()` now also catches what only `validate_detailed()` used
    // to check (e.g. an unknown broker scheme).
    let config = CeleryConfig::new("ftp://localhost");
    assert!(config.validate().is_err());
    assert_eq!(
        config.validate().is_ok(),
        config.validate_detailed().is_valid()
    );
}

#[test]
fn test_serializer_must_be_accepted() {
    let config = CeleryConfig::new("redis://localhost:6379")
        .with_task_serializer("msgpack")
        .with_accept_content(vec!["json".to_string()]);
    let validation = config.validate_detailed();
    assert!(!validation.is_valid());
    assert!(validation
        .errors
        .iter()
        .any(|e| e.field == "task_serializer" && e.message.contains("accept_content")));
}

#[test]
fn test_typo_in_custom_keys_is_surfaced() {
    // Regression: `#[serde(flatten)] custom` absorbed typo'd keys silently.
    let json = r#"{
        "broker_url": "redis://localhost:6379",
        "worker_concurency": 8
    }"#;
    let config: CeleryConfig = serde_json::from_str(json).expect("deserializes");
    assert_eq!(config.custom.len(), 1);

    let validation = config.validate_detailed();
    let warning = validation
        .warnings
        .iter()
        .find(|w| w.field == "custom")
        .expect("typo should produce a warning");
    assert!(warning.message.contains("worker_concurency"));
    assert!(warning.message.contains("worker_concurrency"));
}

#[test]
fn test_from_env_checked_rejects_unparseable_values() {
    // Regression: `CELERYD_CONCURRENCY=eight` was silently ignored and the
    // process started on the default.
    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    cleanup_env();
    set_env("CELERYD_CONCURRENCY", "eight");
    set_env("CELERY_TASK_ACKS_LATE", "perhaps");

    let errors = CeleryConfig::from_env_checked().expect_err("should report both bad values");
    assert_eq!(errors.len(), 2);
    assert!(errors.iter().any(|e| e.field == "CELERYD_CONCURRENCY"));
    assert!(errors.iter().any(|e| e.field == "CELERY_TASK_ACKS_LATE"));

    // The lenient loader still returns defaults (and logs).
    let config = CeleryConfig::from_env();
    assert_eq!(config.worker_concurrency, default_concurrency());

    // A well-formed value parses through both paths.
    set_env("CELERYD_CONCURRENCY", "7");
    remove_env("CELERY_TASK_ACKS_LATE");
    let config = CeleryConfig::from_env_checked().expect("valid env");
    assert_eq!(config.worker_concurrency, 7);

    cleanup_env();
}

#[test]
fn test_env_round_trip_covers_scalar_fields() {
    // Regression: `to_env_vars()` omitted several scalar fields, making
    // `from_env(to_env_vars(cfg))` a lossy round-trip with no warning.
    let _guard = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
    cleanup_env();

    let config = CeleryConfig::new("redis://localhost:6379")
        .with_accept_content(vec!["json".to_string(), "msgpack".to_string()])
        .with_worker_concurrency(3);
    let mut config = config;
    config.worker_heartbeat = 42;
    config.result_compression = Some("gzip".to_string());
    config.result_compression_threshold = 2048;

    for (key, value) in config.to_env_vars() {
        set_env(&key, &value);
    }
    let restored = CeleryConfig::from_env_checked().expect("round trip parses");

    assert_eq!(restored.accept_content, config.accept_content);
    assert_eq!(restored.worker_heartbeat, 42);
    assert_eq!(restored.result_compression.as_deref(), Some("gzip"));
    assert_eq!(restored.result_compression_threshold, 2048);
    assert_eq!(restored.worker_concurrency, 3);

    cleanup_env();
}

#[test]
fn test_config_validation_counts() {
    let mut validation = ConfigValidation::new();
    assert!(validation.is_valid());
    assert!(!validation.has_warnings());
    assert_eq!(validation.error_count(), 0);
    assert_eq!(validation.warning_count(), 0);

    validation.add_error("f1", "e1", None);
    validation.add_warning("f2", "w1");
    assert!(!validation.is_valid());
    assert!(validation.has_warnings());
    assert_eq!(validation.error_count(), 1);
    assert_eq!(validation.warning_count(), 1);
}
