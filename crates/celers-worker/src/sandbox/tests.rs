//! Unit tests for [`super::Sandbox`] and its configuration/statistics types.

use super::*;
use std::path::Path;
use std::time::Duration;

fn make_sandbox(config: SandboxConfig) -> Sandbox {
    Sandbox::new(config).expect("configuration should be supported")
}

#[test]
fn test_sandbox_config_default() {
    let config = SandboxConfig::default();
    assert_eq!(config.max_memory_mb(), Some(1024));
    assert_eq!(config.max_cpu_percent(), Some(100));
    assert!(config.is_network_allowed());
    assert!(config.is_valid());
}

#[test]
fn test_sandbox_config_builder() {
    let config = SandboxConfig::new()
        .with_max_memory_mb(512)
        .with_max_cpu_percent(80)
        .with_timeout_secs(60)
        .with_network_access(false);

    assert_eq!(config.max_memory_mb(), Some(512));
    assert_eq!(config.max_cpu_percent(), Some(80));
    assert_eq!(config.timeout(), Some(Duration::from_secs(60)));
    assert!(!config.is_network_allowed());
}

#[test]
fn test_sandbox_config_presets() {
    let strict = SandboxConfig::strict();
    assert_eq!(strict.max_memory_mb(), Some(512));
    assert!(!strict.is_network_allowed());
    // The strict preset must stay constructible: it may only ask for
    // controls this build can actually enforce.
    assert_eq!(strict.isolation_level(), IsolationLevel::Basic);
    assert!(!strict.is_seccomp_enabled());
    assert!(Sandbox::new(strict).is_ok());

    let lenient = SandboxConfig::lenient();
    assert_eq!(lenient.max_memory_mb(), Some(4096));
    assert!(lenient.is_network_allowed());

    let balanced = SandboxConfig::balanced();
    assert_eq!(balanced.max_memory_mb(), Some(1024));
}

#[test]
fn test_sandbox_config_validation() {
    let config = SandboxConfig::new().with_max_cpu_percent(150);
    assert_eq!(config.max_cpu_percent(), Some(100)); // Clamped to 100

    let config = SandboxConfig::new().with_max_cpu_percent(0);
    assert!(!config.is_valid());
    assert!(matches!(
        Sandbox::new(config),
        Err(SandboxError::InvalidConfig(_))
    ));
}

#[test]
fn test_isolation_level_display() {
    assert_eq!(format!("{}", IsolationLevel::None), "None");
    assert_eq!(format!("{}", IsolationLevel::Full), "Full");
}

// --- Regression: unsupported isolation must be a hard error (idx 169) ---

#[test]
fn test_unsupported_isolation_levels_are_rejected() {
    for level in [IsolationLevel::Container, IsolationLevel::Full] {
        let config = SandboxConfig::new().with_isolation_level(level);
        match Sandbox::new(config) {
            Err(SandboxError::Unsupported { control, .. }) => {
                assert!(control.contains(&level.to_string()), "control: {}", control);
            }
            other => panic!("{:?} must be rejected, got {:?}", level, other.map(|_| ())),
        }
    }
}

/// Without a Linux target *and* the `seccomp` feature there is no filter
/// to install, so asking for one must be a hard error rather than a
/// silently-dropped control. This is what the default build compiles.
#[cfg(not(all(target_os = "linux", feature = "seccomp")))]
#[test]
fn test_seccomp_request_is_rejected_when_not_compiled_in() {
    let config = SandboxConfig::new().with_seccomp(true);
    match Sandbox::new(config) {
        Err(SandboxError::Unsupported { control, reason }) => {
            assert_eq!(control, "seccomp");
            assert!(reason.contains("seccomp"), "reason: {}", reason);
            assert!(reason.contains("feature"), "reason: {}", reason);
        }
        other => panic!("seccomp must be rejected, got {:?}", other.map(|_| ())),
    }
}

/// With the filter compiled in, a configuration that can actually receive
/// it must construct — and one that cannot must still be refused.
#[cfg(all(target_os = "linux", feature = "seccomp"))]
#[test]
fn test_seccomp_request_is_accepted_only_at_isolation_level_process() {
    // Installed by `enforce_process_limits`, which requires
    // IsolationLevel::Process — so a Basic-level sandbox asking for
    // seccomp would silently never get it, and must be refused.
    let basic = SandboxConfig::new().with_seccomp(true);
    match Sandbox::new(basic) {
        Err(SandboxError::Unsupported { control, reason }) => {
            assert_eq!(control, "seccomp");
            assert!(
                reason.contains("IsolationLevel::Process"),
                "reason: {reason}"
            );
        }
        other => panic!(
            "seccomp below IsolationLevel::Process must be rejected, got {:?}",
            other.map(|_| ())
        ),
    }

    let process = SandboxConfig::new()
        .with_seccomp(true)
        .with_isolation_level(IsolationLevel::Process);
    let sandbox = Sandbox::new(process).expect("seccomp is supported in this build");
    assert!(sandbox.config().is_seccomp_enabled());
    assert!(sandbox.enforcement().syscall_filter);
}

#[test]
fn test_supported_levels_are_accepted() {
    for level in [IsolationLevel::None, IsolationLevel::Basic] {
        let config = SandboxConfig::new().with_isolation_level(level);
        assert!(
            Sandbox::new(config).is_ok(),
            "{:?} must be supported",
            level
        );
    }

    let process = SandboxConfig::new().with_isolation_level(IsolationLevel::Process);
    assert_eq!(Sandbox::new(process).is_ok(), cfg!(unix));
}

#[test]
fn test_enforcement_report_is_honest() {
    let sandbox = make_sandbox(
        SandboxConfig::new()
            .with_timeout_secs(5)
            .with_network_access(false)
            .with_fs_write_access(false)
            .with_allowed_path("/var".to_string()),
    );

    let report = sandbox.enforcement();
    assert!(report.timeout);
    assert!(report.path_allowlist);
    assert!(report.read_only_filesystem);
    assert!(report.network_gate);
    // Never claim syscall filtering; it is not implemented.
    assert!(!report.syscall_filter);
    // Basic isolation never applies OS limits.
    assert!(!report.os_resource_limits);
}

#[test]
fn test_process_limits_require_process_level() {
    let sandbox = make_sandbox(SandboxConfig::new());
    assert!(matches!(
        sandbox.enforce_process_limits(),
        Err(SandboxError::Unsupported { .. })
    ));
}

#[cfg(unix)]
#[test]
fn test_process_limits_report_capability_truthfully() {
    let sandbox = make_sandbox(
        SandboxConfig::new()
            .with_isolation_level(IsolationLevel::Process)
            .with_network_access(false),
    );
    assert_eq!(
        sandbox.enforcement().os_resource_limits,
        cfg!(feature = "rlimit")
    );

    // Without the feature the call must fail loudly rather than pretend.
    #[cfg(not(feature = "rlimit"))]
    assert!(matches!(
        sandbox.enforce_process_limits(),
        Err(SandboxError::Unsupported { .. })
    ));
}

#[cfg(all(unix, feature = "rlimit"))]
#[test]
fn test_rlimit_clamps_to_hard_limit() {
    use super::rlimit_impl::{clamp_to_hard, rlim_infinity};

    assert_eq!(clamp_to_hard(100, 50), 50);
    assert_eq!(clamp_to_hard(10, 50), 10);
    assert_eq!(clamp_to_hard(u64::MAX - 1, rlim_infinity()), u64::MAX - 1);
}

#[cfg(all(unix, feature = "rlimit"))]
#[test]
fn test_enforce_process_limits_with_no_limits_is_a_noop() {
    // Deliberately configures *no* limits: applying a real RLIMIT_AS to
    // the test process would break the test runner itself.
    let config = SandboxConfig {
        max_memory_mb: None,
        max_cpu_percent: None,
        timeout: None,
        max_file_descriptors: None,
        allow_network: true,
        allow_fs_write: true,
        allowed_paths: Vec::new(),
        enable_seccomp: false,
        isolation_level: IsolationLevel::Process,
    };
    let sandbox = make_sandbox(config);
    assert!(sandbox.enforce_process_limits().is_ok());
}

#[test]
fn test_sandbox_stats_default() {
    let stats = SandboxStats::default();
    assert_eq!(stats.total_executions(), 0);
    assert_eq!(stats.successful(), 0);
    assert_eq!(stats.success_rate(), 0.0);
    assert_eq!(stats.total_violations(), 0);
}

#[test]
fn test_sandbox_stats_rates() {
    let stats = SandboxStats {
        total_executions: 100,
        successful: 80,
        timeout_violations: 10,
        memory_violations: 5,
        ..Default::default()
    };

    assert_eq!(stats.success_rate(), 0.8);
    assert_eq!(stats.total_violations(), 15);
    assert_eq!(stats.violation_rate(), 0.15);
}

#[test]
fn test_sandbox_creation() {
    let config = SandboxConfig::default();
    let sandbox = make_sandbox(config);
    assert!(sandbox.config().is_valid());
}

#[test]
fn test_sandbox_path_allowed() {
    let temp_dir = std::env::temp_dir().to_string_lossy().to_string();
    let config = SandboxConfig::new()
        .with_allowed_path(temp_dir.clone())
        .with_allowed_path("/var".to_string());

    let sandbox = make_sandbox(config);
    let temp_file = std::env::temp_dir().join("file.txt");
    assert!(sandbox.is_path_allowed_path(&temp_file));
    assert!(sandbox.is_path_allowed("/var/log/app.log"));
    assert!(!sandbox.is_path_allowed("/etc/passwd"));
}

#[test]
fn test_sandbox_path_allowed_empty() {
    let config = SandboxConfig::new();
    let sandbox = make_sandbox(config);

    // When no paths specified, all paths are allowed
    assert!(sandbox.is_path_allowed("/any/path"));
}

// --- Regression tests for the allowlist bypasses (idx 170) ---

#[test]
fn test_path_traversal_is_denied() {
    let config = SandboxConfig::new().with_allowed_path("/data".to_string());
    let sandbox = make_sandbox(config);

    assert!(sandbox.is_path_allowed("/data/report.csv"));
    assert!(
        !sandbox.is_path_allowed("/data/../etc/passwd"),
        "`..` must be resolved before the allowlist comparison"
    );
    assert!(!sandbox.is_path_allowed("/data/sub/../../etc/shadow"));
}

#[test]
fn test_prefix_collision_is_denied() {
    let config = SandboxConfig::new().with_allowed_path("/data".to_string());
    let sandbox = make_sandbox(config);

    assert!(
        !sandbox.is_path_allowed("/data-secret/keys"),
        "comparison must be component-wise, not string-prefix"
    );
    assert!(!sandbox.is_path_allowed("/database/dump.sql"));
}

#[test]
fn test_escape_above_root_is_denied() {
    let config = SandboxConfig::new().with_allowed_path("/data".to_string());
    let sandbox = make_sandbox(config);
    assert!(!sandbox.is_path_allowed("/../../etc/passwd"));
}

#[cfg(unix)]
#[test]
fn test_symlink_escaping_allowlist_is_denied() {
    use std::fs;

    let root = std::env::temp_dir().join(format!("celers-sandbox-{}", uuid::Uuid::new_v4()));
    let allowed = root.join("allowed");
    let outside = root.join("outside");
    fs::create_dir_all(&allowed).expect("create allowed dir");
    fs::create_dir_all(&outside).expect("create outside dir");
    let secret = outside.join("secret.txt");
    fs::write(&secret, b"secret").expect("write secret");

    let link = allowed.join("escape.txt");
    std::os::unix::fs::symlink(&secret, &link).expect("create symlink");

    let config = SandboxConfig::new().with_allowed_path(allowed.to_string_lossy().to_string());
    let sandbox = make_sandbox(config);

    let inside = allowed.join("ok.txt");
    fs::write(&inside, b"ok").expect("write inside file");
    assert!(sandbox.is_path_allowed_path(&inside));
    assert!(
        !sandbox.is_path_allowed_path(&link),
        "a symlink pointing outside the allowlist must be denied"
    );

    let _ = fs::remove_dir_all(&root);
}

#[tokio::test]
async fn test_check_path_gates_writes_and_paths() {
    let temp_dir = std::env::temp_dir();
    let config = SandboxConfig::new()
        .with_allowed_path(temp_dir.to_string_lossy().to_string())
        .with_fs_write_access(false);
    let sandbox = make_sandbox(config);

    let inside = temp_dir.join("celers-sandbox-check.txt");
    assert!(sandbox.check_path(&inside, false).await.is_ok());
    assert_eq!(
        sandbox.check_path(&inside, true).await,
        Err(SandboxViolation::FilesystemWriteDenied)
    );
    assert!(matches!(
        sandbox.check_path(Path::new("/etc/passwd"), false).await,
        Err(SandboxViolation::PathDenied { .. })
    ));

    let stats = sandbox.stats().await;
    assert_eq!(stats.path_violations(), 2);
}

#[test]
fn test_check_network_gate() {
    let open = make_sandbox(SandboxConfig::new());
    assert!(open.check_network().is_ok());

    let closed = make_sandbox(SandboxConfig::new().with_network_access(false));
    assert_eq!(closed.check_network(), Err(SandboxViolation::NetworkDenied));
}

#[test]
fn test_sanitize_env_drops_credentials() {
    let sandbox = make_sandbox(SandboxConfig::new());
    let scrubbed = sandbox.sanitize_env([
        ("PATH", "/usr/bin"),
        ("AWS_SECRET_ACCESS_KEY", "shhh"),
        ("celers_broker_password", "shhh"),
        ("HOME", "/home/worker"),
        ("GITHUB_TOKEN", "shhh"),
    ]);

    let keys: Vec<&str> = scrubbed.iter().map(|(k, _)| k.as_str()).collect();
    assert_eq!(keys, vec!["PATH", "HOME"]);

    // IsolationLevel::None explicitly opts out of scrubbing.
    let passthrough = make_sandbox(SandboxConfig::new().with_isolation_level(IsolationLevel::None));
    assert_eq!(
        passthrough.sanitize_env([("GITHUB_TOKEN", "shhh")]).len(),
        1
    );
}

#[tokio::test]
async fn test_execute_enforces_timeout() {
    tokio::time::pause();

    let sandbox = make_sandbox(SandboxConfig::new().with_timeout(Duration::from_secs(30)));

    let ok = sandbox.execute(async { 21 * 2 }).await;
    assert_eq!(ok, Ok(42));

    let timed_out = sandbox
        .execute(async {
            std::future::pending::<()>().await;
        })
        .await;
    assert!(matches!(timed_out, Err(SandboxViolation::Timeout { .. })));

    let stats = sandbox.stats().await;
    assert_eq!(stats.total_executions(), 2);
    assert_eq!(stats.successful(), 1);
    assert_eq!(stats.failed(), 1);
    assert_eq!(stats.timeout_violations(), 1);
}

#[tokio::test]
async fn test_execute_without_timeout_runs_to_completion() {
    let config = SandboxConfig {
        timeout: None,
        ..SandboxConfig::new()
    };
    let sandbox = make_sandbox(config);
    assert_eq!(sandbox.execute(async { "done" }).await, Ok("done"));
    assert!(!sandbox.enforcement().timeout);
}

#[tokio::test]
async fn test_sandbox_validate_resources() {
    let config = SandboxConfig::new()
        .with_max_memory_mb(1024)
        .with_max_cpu_percent(80);

    let sandbox = make_sandbox(config);

    // Within limits
    assert!(sandbox.validate_resources(512, 50).await.is_ok());

    // Exceed memory limit
    let result = sandbox.validate_resources(2048, 50).await;
    assert!(matches!(result, Err(SandboxViolation::MemoryLimit { .. })));

    // Exceed CPU limit
    let result = sandbox.validate_resources(512, 90).await;
    assert!(matches!(result, Err(SandboxViolation::CpuLimit { .. })));
}

#[tokio::test]
async fn test_sandbox_record_execution() {
    let config = SandboxConfig::default();
    let sandbox = make_sandbox(config);

    sandbox.record_execution(true, 100, 256).await;
    sandbox.record_execution(false, 200, 512).await;

    let stats = sandbox.stats().await;
    assert_eq!(stats.total_executions(), 2);
    assert_eq!(stats.successful(), 1);
    assert_eq!(stats.failed(), 1);
    assert_eq!(stats.avg_execution_time_ms(), 150);
    assert_eq!(stats.peak_memory_mb(), 512);
}

#[tokio::test]
async fn test_sandbox_record_violations() {
    let config = SandboxConfig::default();
    let sandbox = make_sandbox(config);

    sandbox.record_timeout_violation().await;
    sandbox.record_fd_violation().await;

    let stats = sandbox.stats().await;
    assert_eq!(stats.timeout_violations(), 1);
    assert_eq!(stats.fd_violations(), 1);
    assert_eq!(stats.total_violations(), 2);
}

#[tokio::test]
async fn test_sandbox_reset_stats() {
    let config = SandboxConfig::default();
    let sandbox = make_sandbox(config);

    sandbox.record_execution(true, 100, 256).await;
    sandbox.reset_stats().await;

    let stats = sandbox.stats().await;
    assert_eq!(stats.total_executions(), 0);
}

#[test]
fn test_sandbox_violation_display() {
    let violation = SandboxViolation::MemoryLimit {
        used: 2048,
        limit: 1024,
    };
    assert_eq!(
        format!("{}", violation),
        "Memory limit exceeded: 2048MB > 1024MB"
    );

    let violation = SandboxViolation::NetworkDenied;
    assert_eq!(format!("{}", violation), "Network access denied");
}

#[test]
fn test_sandbox_error_display() {
    let err = SandboxError::Unsupported {
        control: "seccomp".to_string(),
        reason: "not implemented".to_string(),
    };
    assert_eq!(
        format!("{}", err),
        "Sandbox control 'seccomp' is not supported: not implemented"
    );
}

#[test]
fn test_normalize_path_resolves_relative_paths() {
    let normalized = normalize_path(Path::new("."));
    assert!(normalized.is_some_and(|p| p.is_absolute()));
}
