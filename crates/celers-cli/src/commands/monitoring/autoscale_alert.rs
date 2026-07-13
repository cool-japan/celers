//! Auto-scaling service and webhook alert monitoring.
//!
//! 🤖 Generated with [SplitRS](https://github.com/cool-japan/splitrs)

use celers_broker_redis::RedisBroker;
use celers_core::Broker;
use chrono::Utc;
use colored::Colorize;
use oxihttp_client::Client;

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
    println!();
    println!("{}", "Starting auto-scaling monitor...".green().bold());
    println!("{}", "  Press Ctrl+C to stop".dimmed());
    println!();

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(config.check_interval_secs)).await;

        let queue_size = broker.queue_size().await?;

        let client = redis::Client::open(broker_url)?;
        let mut conn = client.get_multiplexed_async_connection().await?;
        let worker_keys: Vec<String> = redis::cmd("KEYS")
            .arg("celers:worker:*:heartbeat")
            .query_async(&mut conn)
            .await?;
        let current_workers = worker_keys.len();

        println!(
            "[{}] Queue: {}, Workers: {}",
            Utc::now().format("%H:%M:%S").to_string().dimmed(),
            queue_size.to_string().yellow(),
            current_workers.to_string().cyan()
        );

        if queue_size > config.scale_up_threshold && current_workers < config.max_workers {
            let needed = config.max_workers.min(current_workers + 1);
            println!(
                "  {} Scale up recommended: {} -> {}",
                "↑".green().bold(),
                current_workers,
                needed
            );
        } else if queue_size < config.scale_down_threshold && current_workers > config.min_workers {
            let target = config.min_workers.max(current_workers.saturating_sub(1));
            println!(
                "  {} Scale down possible: {} -> {}",
                "↓".yellow().bold(),
                current_workers,
                target
            );
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
        let worker_keys: Vec<String> = redis::cmd("KEYS")
            .arg("celers:worker:*:heartbeat")
            .query_async(&mut conn)
            .await?;

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
