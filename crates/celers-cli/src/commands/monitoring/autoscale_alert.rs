//! Auto-scaling service and webhook alert monitoring.
//!
//! 🤖 Generated with [SplitRS](https://github.com/cool-japan/splitrs)

use celers_broker_redis::RedisBroker;
use celers_core::Broker;
use chrono::Utc;
use colored::Colorize;
use oxihttp_client::Client;

use super::report::scan_worker_heartbeat_keys;

/// Environment variable naming an executable to run on every scale
/// decision made by `autoscale_start`.
///
/// CeleRS itself is not a process manager: it has no way to start or stop
/// OS processes it does not own, so actual actuation is delegated to an
/// operator-provided hook. When set, the named path is invoked directly
/// (never through a shell, so no argument/shell-injection surface) as
/// `<exec> <direction> <current> <target>` where `direction` is `"up"` or
/// `"down"`. The same three values are also exported as
/// `CELERS_SCALE_DIRECTION` / `CELERS_SCALE_FROM` / `CELERS_SCALE_TO` in the
/// child's environment for scripts that prefer reading env vars over argv.
/// A nonzero exit status is treated as a failed actuation and reported, but
/// never stops the monitor loop.
pub const AUTOSCALE_EXEC_ENV: &str = "CELERS_AUTOSCALE_EXEC";

/// Environment variable naming a file that receives the latest recommended
/// worker count (plain decimal text) on every scale decision.
///
/// The file is written atomically (a sibling `.tmp` file, then renamed over
/// the destination) so an external process manager polling it never
/// observes a partially-written value. This is the "desired-count file"
/// actuation hook: point a Kubernetes operator, a systemd template unit, or
/// a supervisor script at this file to let it drive the real scaling.
pub const AUTOSCALE_DESIRED_COUNT_FILE_ENV: &str = "CELERS_AUTOSCALE_DESIRED_COUNT_FILE";

/// The result of attempting to actuate one scale decision.
///
/// Distinguishes "no backend configured" (honest recommendation-only mode)
/// from "a backend is configured but this attempt failed", so the caller
/// never has to guess -- and never has to claim a scale happened when it
/// did not.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ActuationOutcome {
    /// Neither [`AUTOSCALE_EXEC_ENV`] nor [`AUTOSCALE_DESIRED_COUNT_FILE_ENV`]
    /// is set.
    NotConfigured,
    /// At least one hook was attempted; `errors` holds a message per failed
    /// hook (empty means every configured hook succeeded).
    Attempted { errors: Vec<String> },
}

impl ActuationOutcome {
    /// Human-readable, unambiguous summary suitable for appending to the
    /// per-tick recommendation line.
    fn describe(&self) -> String {
        match self {
            ActuationOutcome::NotConfigured => {
                "(recommendation only; no actuation backend configured)".to_string()
            }
            ActuationOutcome::Attempted { errors } if errors.is_empty() => {
                "(actuation hook succeeded)".to_string()
            }
            ActuationOutcome::Attempted { errors } => {
                format!("(actuation hook FAILED: {})", errors.join("; "))
            }
        }
    }
}

/// A single scale decision computed from the current queue depth and
/// worker count.
///
/// Extracted as a pure function (no I/O) from the polling loop so the
/// clamping arithmetic (`min`/`max`/`saturating_sub` against
/// `min_workers`/`max_workers`) is unit-testable without a broker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ScaleDecision {
    Up { current: usize, target: usize },
    Down { current: usize, target: usize },
    Hold,
}

/// Pure: decide whether `queue_size` (observed with `current_workers`
/// active) crosses an autoscale threshold, and if so, in which direction
/// and to what clamped target worker count.
fn decide_scale(
    queue_size: usize,
    current_workers: usize,
    config: &crate::config::AutoScaleConfig,
) -> ScaleDecision {
    if queue_size > config.scale_up_threshold && current_workers < config.max_workers {
        let target = config.max_workers.min(current_workers + 1);
        ScaleDecision::Up {
            current: current_workers,
            target,
        }
    } else if queue_size < config.scale_down_threshold && current_workers > config.min_workers {
        let target = config.min_workers.max(current_workers.saturating_sub(1));
        ScaleDecision::Down {
            current: current_workers,
            target,
        }
    } else {
        ScaleDecision::Hold
    }
}

/// Whether any actuation hook is currently configured via the environment.
fn actuation_configured() -> bool {
    env_hook_value(AUTOSCALE_EXEC_ENV).is_some()
        || env_hook_value(AUTOSCALE_DESIRED_COUNT_FILE_ENV).is_some()
}

/// Read an actuation-hook env var, treating an unset or blank value as
/// "not configured" (so `CELERS_AUTOSCALE_EXEC=` in an inherited
/// environment does not silently attempt to exec an empty path).
fn env_hook_value(var: &str) -> Option<String> {
    std::env::var(var).ok().filter(|s| !s.trim().is_empty())
}

/// Run whichever actuation hooks are configured for a scale decision from
/// `current` to `target` workers (`direction` is `"up"` or `"down"`).
///
/// This is the real-scaling half of `celers autoscale start`. See
/// [`AUTOSCALE_EXEC_ENV`] / [`AUTOSCALE_DESIRED_COUNT_FILE_ENV`] for the two
/// supported hooks. Never panics and never propagates a hook failure to the
/// caller as an `Err` -- a failed (or unconfigured) actuation must not kill
/// the monitor loop, only be reported honestly.
async fn run_actuation_hooks(direction: &str, current: usize, target: usize) -> ActuationOutcome {
    let exec_path = env_hook_value(AUTOSCALE_EXEC_ENV);
    let desired_count_file = env_hook_value(AUTOSCALE_DESIRED_COUNT_FILE_ENV);

    if exec_path.is_none() && desired_count_file.is_none() {
        return ActuationOutcome::NotConfigured;
    }

    let mut errors = Vec::new();

    if let Some(exec_path) = exec_path {
        match tokio::process::Command::new(&exec_path)
            .arg(direction)
            .arg(current.to_string())
            .arg(target.to_string())
            .env("CELERS_SCALE_DIRECTION", direction)
            .env("CELERS_SCALE_FROM", current.to_string())
            .env("CELERS_SCALE_TO", target.to_string())
            .status()
            .await
        {
            Ok(status) if status.success() => {}
            Ok(status) => errors.push(format!("{exec_path} exited with {status}")),
            Err(e) => errors.push(format!("failed to run {exec_path}: {e}")),
        }
    }

    if let Some(path) = desired_count_file {
        if let Err(e) = write_desired_count_file(&path, target).await {
            errors.push(format!("failed to write desired-count file {path}: {e}"));
        }
    }

    ActuationOutcome::Attempted { errors }
}

/// Atomically write `target` (as plain decimal text) to `path`: write a
/// sibling `.tmp` file, then rename it over the destination, so a
/// concurrent reader never observes a partially-written value.
async fn write_desired_count_file(path: &str, target: usize) -> std::io::Result<()> {
    let tmp_path = format!("{path}.tmp");
    tokio::fs::write(&tmp_path, format!("{target}\n")).await?;
    tokio::fs::rename(&tmp_path, path).await?;
    Ok(())
}

/// Start auto-scaling service
pub async fn autoscale_start(
    broker_url: &str,
    queue: &str,
    autoscale_config: Option<crate::config::AutoScaleConfig>,
) -> anyhow::Result<()> {
    println!("{}", "=== Auto-Scaling Service ===".bold().green());
    println!();

    let config = match autoscale_config {
        Some(cfg) if cfg.enabled => cfg,
        Some(_) => {
            println!(
                "{}",
                "⚠️  Auto-scaling is disabled in configuration".yellow()
            );
            return Ok(());
        }
        None => {
            println!("{}", "⚠️  No auto-scaling configuration found".yellow());
            println!("Add [autoscale] section to your celers.toml");
            return Ok(());
        }
    };

    println!("Configuration:");
    println!("  Min workers: {}", config.min_workers.to_string().cyan());
    println!("  Max workers: {}", config.max_workers.to_string().cyan());
    println!(
        "  Scale up threshold: {}",
        config.scale_up_threshold.to_string().cyan()
    );
    println!(
        "  Scale down threshold: {}",
        config.scale_down_threshold.to_string().cyan()
    );
    println!(
        "  Check interval: {}s",
        config.check_interval_secs.to_string().cyan()
    );
    println!();

    let broker = RedisBroker::new(broker_url, queue)?;
    println!("{}", "✓ Connected to broker".green());

    // Actuation hooks are read once up front purely to print an honest,
    // unmissable banner about what this run will (and will not) do; the
    // env vars are re-read on every decision (`run_actuation_hooks`) so a
    // hook can be added/removed by editing the environment of a long-lived
    // process supervisor without restarting `celers autoscale start`.
    if actuation_configured() {
        println!(
            "{}",
            "✓ Actuation backend configured -- scale decisions below will be enacted"
                .green()
                .bold()
        );
    } else {
        println!(
            "{}",
            format!(
                "⚠️  No actuation backend configured (set {AUTOSCALE_EXEC_ENV} and/or \
                 {AUTOSCALE_DESIRED_COUNT_FILE_ENV}) -- running in RECOMMENDATION-ONLY mode; \
                 no workers will be started or stopped automatically."
            )
            .yellow()
            .bold()
        );
    }
    println!();
    println!("{}", "Starting auto-scaling monitor...".green().bold());
    println!("{}", "  Press Ctrl+C to stop".dimmed());
    println!();

    // Hoisted out of the loop: a fresh client/connection per tick churned
    // TCP connections for the lifetime of the service. `scan_worker_heartbeat_keys`
    // additionally replaces a blocking `KEYS celers:worker:*:heartbeat`
    // (O(N) over the whole keyspace) with cursor-based `SCAN`.
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(config.check_interval_secs)).await;

        let queue_size = broker.queue_size().await?;
        let worker_keys = scan_worker_heartbeat_keys(&mut conn).await?;
        let current_workers = worker_keys.len();

        println!(
            "[{}] Queue: {}, Workers: {}",
            Utc::now().format("%H:%M:%S").to_string().dimmed(),
            queue_size.to_string().yellow(),
            current_workers.to_string().cyan()
        );

        match decide_scale(queue_size, current_workers, &config) {
            ScaleDecision::Up { current, target } => {
                let outcome = run_actuation_hooks("up", current, target).await;
                println!(
                    "  {} Scale up: {} -> {} {}",
                    "↑".green().bold(),
                    current,
                    target,
                    outcome.describe().dimmed()
                );
            }
            ScaleDecision::Down { current, target } => {
                let outcome = run_actuation_hooks("down", current, target).await;
                println!(
                    "  {} Scale down: {} -> {} {}",
                    "↓".yellow().bold(),
                    current,
                    target,
                    outcome.describe().dimmed()
                );
            }
            ScaleDecision::Hold => {}
        }
    }
}

/// Show auto-scaling status
pub async fn autoscale_status(
    broker_url: &str,
    autoscale_config: Option<crate::config::AutoScaleConfig>,
) -> anyhow::Result<()> {
    println!("{}", "=== Auto-Scaling Status ===".bold().cyan());
    println!();

    if let Some(cfg) = autoscale_config {
        println!(
            "Status: {}",
            if cfg.enabled {
                "Enabled".green()
            } else {
                "Disabled".red()
            }
        );
        println!();
        println!("Configuration:");
        println!("  Min workers: {}", cfg.min_workers);
        println!("  Max workers: {}", cfg.max_workers);
        println!("  Scale up threshold: {}", cfg.scale_up_threshold);
        println!("  Scale down threshold: {}", cfg.scale_down_threshold);
        println!("  Check interval: {}s", cfg.check_interval_secs);
        println!();

        let client = redis::Client::open(broker_url)?;
        let mut conn = client.get_multiplexed_async_connection().await?;
        let worker_keys = scan_worker_heartbeat_keys(&mut conn).await?;

        println!("Current State:");
        println!("  Active workers: {}", worker_keys.len().to_string().cyan());
    } else {
        println!("{}", "Auto-scaling is not configured".yellow());
        println!("Add [autoscale] section to your celers.toml");
    }

    Ok(())
}

/// Start alert monitoring service
pub async fn alert_start(
    broker_url: &str,
    queue: &str,
    alert_config: Option<crate::config::AlertConfig>,
) -> anyhow::Result<()> {
    println!("{}", "=== Alert Monitoring Service ===".bold().green());
    println!();

    let config = match alert_config {
        Some(cfg) if cfg.enabled => cfg,
        Some(_) => {
            println!(
                "{}",
                "⚠️  Alert monitoring is disabled in configuration".yellow()
            );
            return Ok(());
        }
        None => {
            println!("{}", "⚠️  No alert configuration found".yellow());
            println!("Add [alerts] section to your celers.toml");
            return Ok(());
        }
    };

    if config.webhook_url.is_none() {
        println!("{}", "⚠️  No webhook URL configured".yellow());
        return Ok(());
    }

    println!("Configuration:");
    println!(
        "  Webhook URL: {}",
        config
            .webhook_url
            .as_ref()
            .expect("webhook_url validated to be Some")
            .cyan()
    );
    println!(
        "  DLQ threshold: {}",
        config.dlq_threshold.to_string().cyan()
    );
    println!(
        "  Failed threshold: {}",
        config.failed_threshold.to_string().cyan()
    );
    println!(
        "  Check interval: {}s",
        config.check_interval_secs.to_string().cyan()
    );
    println!();

    let broker = RedisBroker::new(broker_url, queue)?;
    println!("{}", "✓ Connected to broker".green());
    println!();
    println!("{}", "Starting alert monitor...".green().bold());
    println!("{}", "  Press Ctrl+C to stop".dimmed());
    println!();

    let webhook_url = config
        .webhook_url
        .expect("webhook_url validated to be Some");

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(config.check_interval_secs)).await;

        let dlq_size = broker.dlq_size().await?;

        println!(
            "[{}] DLQ size: {}",
            Utc::now().format("%H:%M:%S").to_string().dimmed(),
            dlq_size.to_string().yellow()
        );

        if dlq_size > config.dlq_threshold {
            let message = format!(
                "⚠️ DLQ size ({}) exceeded threshold ({})",
                dlq_size, config.dlq_threshold
            );
            println!("  {} Sending alert...", "!".red().bold());

            if let Err(e) = send_webhook_alert(&webhook_url, &message).await {
                println!("  {} Failed to send alert: {}", "✗".red(), e);
            } else {
                println!("  {} Alert sent", "✓".green());
            }
        }
    }
}

/// Test webhook notification
pub async fn alert_test(webhook_url: &str, message: &str) -> anyhow::Result<()> {
    println!("{}", "=== Testing Webhook ===".bold().cyan());
    println!();
    println!("Webhook URL: {}", webhook_url.cyan());
    println!("Message: {}", message.yellow());
    println!();

    println!("Sending test notification...");
    send_webhook_alert(webhook_url, message).await?;

    println!("{}", "✓ Test notification sent successfully".green());

    Ok(())
}

/// Helper function to send webhook alert
async fn send_webhook_alert(webhook_url: &str, message: &str) -> anyhow::Result<()> {
    let client = Client::builder().with_webpki_roots().build_https()?;
    let payload = serde_json::json!({
        "text": message,
        "timestamp": Utc::now().to_rfc3339(),
    });

    let response = client.post(webhook_url)?.json(&payload)?.send().await?;

    if !response.status().is_success() {
        anyhow::bail!("Webhook request failed with status: {}", response.status());
    }

    Ok(())
}

#[cfg(test)]
mod actuation_tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn base_config() -> crate::config::AutoScaleConfig {
        crate::config::AutoScaleConfig {
            enabled: true,
            min_workers: 2,
            max_workers: 10,
            scale_up_threshold: 100,
            scale_down_threshold: 10,
            check_interval_secs: 30,
        }
    }

    /// Unique-ish path under the OS temp dir, so parallel test processes
    /// (nextest runs each test in its own process) never collide.
    fn unique_temp_path(label: &str) -> std::path::PathBuf {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!(
            "celers-autoscale-test-{label}-{}-{nanos}",
            std::process::id()
        ))
    }

    // ---- decide_scale ---------------------------------------------------

    #[test]
    fn decide_scale_recommends_up_past_threshold() {
        let cfg = base_config();
        let decision = decide_scale(150, 3, &cfg);
        assert_eq!(
            decision,
            ScaleDecision::Up {
                current: 3,
                target: 4
            }
        );
    }

    #[test]
    fn decide_scale_up_clamped_to_max_workers() {
        let cfg = base_config();
        // Already at max_workers: no further scale-up even though queue is high.
        let decision = decide_scale(500, cfg.max_workers, &cfg);
        assert_eq!(decision, ScaleDecision::Hold);
    }

    #[test]
    fn decide_scale_recommends_down_below_threshold() {
        let cfg = base_config();
        let decision = decide_scale(1, 5, &cfg);
        assert_eq!(
            decision,
            ScaleDecision::Down {
                current: 5,
                target: 4
            }
        );
    }

    #[test]
    fn decide_scale_down_clamped_to_min_workers() {
        let cfg = base_config();
        let decision = decide_scale(0, cfg.min_workers, &cfg);
        assert_eq!(decision, ScaleDecision::Hold);
    }

    #[test]
    fn decide_scale_holds_between_thresholds() {
        let cfg = base_config();
        let decision = decide_scale(50, 5, &cfg);
        assert_eq!(decision, ScaleDecision::Hold);
    }

    // ---- ActuationOutcome::describe --------------------------------------

    #[test]
    fn describe_not_configured_is_explicit() {
        let desc = ActuationOutcome::NotConfigured.describe();
        assert!(desc.contains("recommendation only"));
        assert!(desc.contains("no actuation backend"));
    }

    #[test]
    fn describe_success_and_failure_are_distinguishable() {
        let ok = ActuationOutcome::Attempted { errors: vec![] }.describe();
        assert!(ok.contains("succeeded"));

        let failed = ActuationOutcome::Attempted {
            errors: vec!["boom".to_string()],
        }
        .describe();
        assert!(failed.contains("FAILED"));
        assert!(failed.contains("boom"));
    }

    // ---- write_desired_count_file ----------------------------------------

    #[tokio::test]
    async fn write_desired_count_file_writes_atomically() {
        let path = unique_temp_path("desired-count");
        let path_str = path.to_string_lossy().to_string();

        write_desired_count_file(&path_str, 7).await.unwrap();
        let contents = tokio::fs::read_to_string(&path).await.unwrap();
        assert_eq!(contents.trim(), "7");

        // No leftover .tmp file after a successful write.
        let tmp_path = format!("{path_str}.tmp");
        assert!(!std::path::Path::new(&tmp_path).exists());

        // Overwriting updates the value in place.
        write_desired_count_file(&path_str, 3).await.unwrap();
        let contents = tokio::fs::read_to_string(&path).await.unwrap();
        assert_eq!(contents.trim(), "3");

        let _ = tokio::fs::remove_file(&path).await;
    }

    // ---- run_actuation_hooks: not configured ------------------------------

    #[tokio::test]
    async fn run_actuation_hooks_reports_not_configured_when_env_unset() {
        std::env::remove_var(AUTOSCALE_EXEC_ENV);
        std::env::remove_var(AUTOSCALE_DESIRED_COUNT_FILE_ENV);
        assert!(!actuation_configured());

        let outcome = run_actuation_hooks("up", 2, 3).await;
        assert_eq!(outcome, ActuationOutcome::NotConfigured);
    }

    #[tokio::test]
    async fn run_actuation_hooks_treats_blank_env_as_unconfigured() {
        std::env::set_var(AUTOSCALE_EXEC_ENV, "   ");
        std::env::remove_var(AUTOSCALE_DESIRED_COUNT_FILE_ENV);

        let outcome = run_actuation_hooks("up", 1, 2).await;
        assert_eq!(outcome, ActuationOutcome::NotConfigured);

        std::env::remove_var(AUTOSCALE_EXEC_ENV);
    }

    // ---- run_actuation_hooks: desired-count-file hook ---------------------

    #[tokio::test]
    async fn run_actuation_hooks_writes_desired_count_file_when_configured() {
        std::env::remove_var(AUTOSCALE_EXEC_ENV);
        let path = unique_temp_path("hook-desired-count");
        let path_str = path.to_string_lossy().to_string();
        std::env::set_var(AUTOSCALE_DESIRED_COUNT_FILE_ENV, &path_str);

        assert!(actuation_configured());
        let outcome = run_actuation_hooks("down", 6, 5).await;
        assert_eq!(outcome, ActuationOutcome::Attempted { errors: vec![] });

        let contents = tokio::fs::read_to_string(&path).await.unwrap();
        assert_eq!(contents.trim(), "5");

        std::env::remove_var(AUTOSCALE_DESIRED_COUNT_FILE_ENV);
        let _ = tokio::fs::remove_file(&path).await;
    }

    #[tokio::test]
    async fn run_actuation_hooks_reports_error_when_file_write_fails() {
        std::env::remove_var(AUTOSCALE_EXEC_ENV);
        // A directory path can never be written as a file: `.tmp` write fails.
        let dir_path = unique_temp_path("hook-desired-count-dir");
        tokio::fs::create_dir_all(&dir_path).await.unwrap();
        std::env::set_var(
            AUTOSCALE_DESIRED_COUNT_FILE_ENV,
            dir_path.to_string_lossy().to_string(),
        );

        let outcome = run_actuation_hooks("up", 1, 2).await;
        match outcome {
            ActuationOutcome::Attempted { errors } => assert!(!errors.is_empty()),
            other => panic!("expected Attempted with an error, got {other:?}"),
        }

        std::env::remove_var(AUTOSCALE_DESIRED_COUNT_FILE_ENV);
        let _ = tokio::fs::remove_dir_all(&dir_path).await;
    }

    // ---- run_actuation_hooks: exec hook ------------------------------------

    #[cfg(unix)]
    #[tokio::test]
    async fn run_actuation_hooks_execs_hook_with_args_and_env() {
        use std::os::unix::fs::PermissionsExt;

        std::env::remove_var(AUTOSCALE_DESIRED_COUNT_FILE_ENV);

        let script_path = unique_temp_path("hook-script.sh");
        let out_path = unique_temp_path("hook-script-out");
        let script = format!(
            "#!/bin/sh\necho \"$1 $2 $3 $CELERS_SCALE_DIRECTION $CELERS_SCALE_FROM $CELERS_SCALE_TO\" > \"{}\"\n",
            out_path.to_string_lossy()
        );
        tokio::fs::write(&script_path, script).await.unwrap();
        let mut perms = tokio::fs::metadata(&script_path)
            .await
            .unwrap()
            .permissions();
        perms.set_mode(0o755);
        tokio::fs::set_permissions(&script_path, perms)
            .await
            .unwrap();

        std::env::set_var(
            AUTOSCALE_EXEC_ENV,
            script_path.to_string_lossy().to_string(),
        );

        let outcome = run_actuation_hooks("up", 4, 5).await;
        assert_eq!(outcome, ActuationOutcome::Attempted { errors: vec![] });

        let contents = tokio::fs::read_to_string(&out_path).await.unwrap();
        assert_eq!(contents.trim(), "up 4 5 up 4 5");

        std::env::remove_var(AUTOSCALE_EXEC_ENV);
        let _ = tokio::fs::remove_file(&script_path).await;
        let _ = tokio::fs::remove_file(&out_path).await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_actuation_hooks_reports_nonzero_exit_as_error() {
        std::env::remove_var(AUTOSCALE_DESIRED_COUNT_FILE_ENV);
        // `/bin/false` is present on every POSIX system this crate targets
        // and reliably exits nonzero with no side effects.
        std::env::set_var(AUTOSCALE_EXEC_ENV, "/bin/false");

        let outcome = run_actuation_hooks("down", 3, 2).await;
        match outcome {
            ActuationOutcome::Attempted { errors } => {
                assert_eq!(errors.len(), 1);
                assert!(errors[0].contains("/bin/false"));
            }
            other => panic!("expected Attempted with an error, got {other:?}"),
        }

        std::env::remove_var(AUTOSCALE_EXEC_ENV);
    }
}
