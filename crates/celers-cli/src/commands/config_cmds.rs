//! Configuration command implementations.

use colored::Colorize;

/// Generate a default configuration file with template settings.
///
/// Creates a TOML configuration file with commented examples for all settings.
/// The generated file can be customized for different environments.
///
/// # Arguments
///
/// * `path` - Output file path (default: "celers.toml")
///
/// # Returns
///
/// Returns `Ok(())` on success, or an error if file creation fails.
///
/// # Examples
///
/// ```no_run
/// # use celers_cli::commands::init_config;
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// // Create default config
/// init_config("celers.toml").await?;
///
/// // Create production config
/// init_config("celers-prod.toml").await?;
/// # Ok(())
/// # }
/// ```
#[allow(clippy::unused_async)]
pub async fn init_config(path: &str) -> anyhow::Result<()> {
    let config = crate::config::Config::default_config();

    config.to_file(path)?;

    println!("{}", "✓ Configuration file created".green().bold());
    println!("  Location: {}", path.cyan());
    println!();
    println!("Edit the file and run:");
    println!("  celers worker --config {path}");

    Ok(())
}

/// Interactively assemble and write a validated configuration file.
///
/// This is the `--wizard` counterpart to [`init_config`]: instead of
/// writing static defaults, it walks the user through broker selection, a
/// live connection test, queue configuration (with validation), worker
/// settings (with recommended defaults), auto-scaling/alert setup, and
/// profile selection via [`crate::commands::wizard::run_wizard`], then
/// writes the assembled configuration with [`crate::config::Config::to_file`].
///
/// The actual file written may differ from `path` when a non-baseline
/// profile is selected: [`crate::commands::wizard::resolve_wizard_output_path`]
/// redirects `staging`/`prod` (or any profile other than `dev`) to a
/// `{stem}.{profile}.{ext}` overlay file alongside `path`, matching the
/// naming convention `config_layer::load_profile_overlay` looks for. This
/// mirrors how the non-interactive path writes one config file, but keeps
/// wizard-generated profile configs layerable via `--profile`.
///
/// # Arguments
///
/// * `path` - Requested output file path (default: "celers.toml"). Used
///   as-is for the `dev` profile; used as the base name for the
///   profile-specific overlay file otherwise.
///
/// # Returns
///
/// Returns `Ok(())` on success, or an error if a prompt fails, the
/// collected answers are structurally invalid, or the file cannot be
/// written.
///
/// # Examples
///
/// ```no_run
/// # use celers_cli::commands::init_config_wizard;
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// init_config_wizard("celers.toml").await?;
/// # Ok(())
/// # }
/// ```
pub async fn init_config_wizard(path: &str) -> anyhow::Result<()> {
    let config = crate::commands::wizard::run_wizard().await?;

    let profile = config.profile.clone().unwrap_or_else(|| "dev".to_string());
    let output_path = crate::commands::wizard::resolve_wizard_output_path(path, &profile);

    config.to_file(&output_path)?;

    println!();
    println!("{}", "✓ Configuration file created".green().bold());
    println!("  Location: {}", output_path.display().to_string().cyan());
    println!("  Profile: {}", profile.cyan());
    println!();
    println!("Edit the file and run:");
    println!(
        "  celers worker --config {} --profile {profile}",
        output_path.display()
    );

    Ok(())
}

/// Validate configuration file
pub async fn validate_config(config_path: &str, test_connection: bool) -> anyhow::Result<()> {
    use std::path::Path;

    println!("{}", "=== Configuration Validation ===".bold().green());
    println!();

    // Check if file exists
    if !Path::new(config_path).exists() {
        println!(
            "{}",
            format!("✗ Configuration file not found: {config_path}")
                .red()
                .bold()
        );
        return Ok(());
    }

    println!(
        "{}",
        format!("📄 Loading configuration from '{config_path}'...").cyan()
    );
    println!();

    // Try to load and parse the configuration
    let config = match crate::config::Config::from_file(config_path) {
        Ok(cfg) => {
            println!("{}", "✓ Configuration file is valid TOML".green());
            cfg
        }
        Err(e) => {
            println!("{}", "✗ Failed to parse configuration file:".red().bold());
            println!("  {}", format!("{e}").red());
            return Ok(());
        }
    };

    println!();
    println!("{}", "Configuration Details:".bold());
    println!();

    // Validate broker configuration
    println!("{}", "Broker Configuration:".cyan().bold());
    println!("  {} {}", "Type:".yellow(), config.broker.broker_type);
    println!("  {} {}", "URL:".yellow(), config.broker.url);
    println!("  {} {}", "Queue:".yellow(), config.broker.queue);
    println!("  {} {}", "Mode:".yellow(), config.broker.mode);

    println!();

    // Validate worker configuration
    println!("{}", "Worker Configuration:".cyan().bold());
    println!(
        "  {} {}",
        "Concurrency:".yellow(),
        config.worker.concurrency
    );
    println!(
        "  {} {} ms",
        "Poll Interval:".yellow(),
        config.worker.poll_interval_ms
    );
    println!(
        "  {} {}",
        "Max Retries:".yellow(),
        config.worker.max_retries
    );
    println!(
        "  {} {} seconds",
        "Default Timeout:".yellow(),
        config.worker.default_timeout_secs
    );

    println!();

    // Run validation and show warnings
    let warnings = config.validate()?;
    if warnings.is_empty() {
        println!(
            "{}",
            "✓ Configuration is valid with no warnings".green().bold()
        );
    } else {
        println!("{}", "Configuration Warnings:".yellow().bold());
        for warning in &warnings {
            println!("  {} {}", "⚠".yellow(), warning.yellow());
        }
    }

    println!();

    // Show configured queues
    if !config.queues.is_empty() {
        println!("{}", "Configured Queues:".cyan().bold());
        for queue in &config.queues {
            println!("  • {}", queue.dimmed());
        }
        println!();
    }

    // Test broker connection if requested
    if test_connection {
        println!("{}", "Testing broker connection...".cyan().bold());
        println!();

        match config.broker.broker_type.to_lowercase().as_str() {
            "redis" => {
                match redis::Client::open(config.broker.url.as_str()) {
                    Ok(client) => {
                        match client
                            .get_multiplexed_async_connection_with_config(
                                &crate::pool::async_connection_config(),
                            )
                            .await
                        {
                            Ok(mut conn) => {
                                // Test a simple PING command
                                match redis::cmd("PING").query_async::<String>(&mut conn).await {
                                    Ok(_) => {
                                        println!(
                                            "{}",
                                            "✓ Successfully connected to Redis broker"
                                                .green()
                                                .bold()
                                        );
                                    }
                                    Err(e) => {
                                        println!(
                                            "{}",
                                            "✗ Failed to PING Redis broker:".red().bold()
                                        );
                                        println!("  {}", format!("{e}").red());
                                    }
                                }
                            }
                            Err(e) => {
                                println!("{}", "✗ Failed to connect to Redis broker:".red().bold());
                                println!("  {}", format!("{e}").red());
                            }
                        }
                    }
                    Err(e) => {
                        println!("{}", "✗ Invalid Redis URL:".red().bold());
                        println!("  {}", format!("{e}").red());
                    }
                }
            }
            "postgres" | "postgresql" => {
                // Test PostgreSQL connection. TLS mode is derived from the
                // broker URL's `sslmode` query parameter (see
                // `crate::tls_mode`) rather than hardcoded, so a caller who
                // wrote `sslmode=require` actually gets an encrypted
                // connection instead of a silent downgrade to plain-text.
                let tls_mode = match crate::tls_mode::pg_tls_mode_for_url(&config.broker.url) {
                    Ok(mode) => mode,
                    Err(e) => {
                        println!(
                            "{}",
                            "✗ Failed to build TLS config for PostgreSQL broker:"
                                .red()
                                .bold()
                        );
                        println!("  {}", format!("{e}").red());
                        println!();
                        return Ok(());
                    }
                };
                match oxisql_postgres::PgConnection::connect(&config.broker.url, tls_mode).await {
                    Ok(conn) => {
                        // Test with a simple query
                        match oxisql_core::Connection::query(&conn, "SELECT 1", &[]).await {
                            Ok(rows) => {
                                if rows.into_iter().next().is_some() {
                                    println!(
                                        "{}",
                                        "✓ Successfully connected to PostgreSQL broker"
                                            .green()
                                            .bold()
                                    );
                                } else {
                                    println!(
                                        "{}",
                                        "✗ Failed to query PostgreSQL broker:".red().bold()
                                    );
                                    println!("  {}", "SELECT 1 returned no rows".red());
                                }
                            }
                            Err(e) => {
                                println!("{}", "✗ Failed to query PostgreSQL broker:".red().bold());
                                println!("  {}", format!("{e}").red());
                            }
                        }
                    }
                    Err(e) => {
                        println!(
                            "{}",
                            "✗ Failed to connect to PostgreSQL broker:".red().bold()
                        );
                        println!("  {}", format!("{e}").red());
                    }
                }
            }
            "mysql" => {
                println!(
                    "{}",
                    "ℹ MySQL connection testing via CLI not yet available".cyan()
                );
                println!("  {}", "To test MySQL connection:".dimmed());
                println!(
                    "  {}",
                    "  1. Use the 'db test-connection' command with your MySQL URL".dimmed()
                );
                println!(
                    "  {}",
                    "  2. Or use mysql client: mysql -h <host> -u <user> -p".dimmed()
                );
            }
            "amqp" | "rabbitmq" => {
                println!(
                    "{}",
                    "ℹ AMQP connection testing via CLI not yet available".cyan()
                );
                println!("  {}", "To test RabbitMQ connection:".dimmed());
                println!(
                    "  {}",
                    "  1. Check RabbitMQ Management UI at http://<host>:15672".dimmed()
                );
                println!("  {}", "  2. Or use rabbitmq-diagnostics ping".dimmed());
                println!(
                    "  {}",
                    "  3. Verify credentials and virtual host configuration".dimmed()
                );
            }
            "sqs" => {
                println!(
                    "{}",
                    "ℹ SQS connection testing via CLI not yet available".cyan()
                );
                println!("  {}", "To test AWS SQS connection:".dimmed());
                println!(
                    "  {}",
                    "  1. Ensure AWS credentials are configured (aws configure)".dimmed()
                );
                println!("  {}", "  2. Test with: aws sqs list-queues".dimmed());
                println!(
                    "  {}",
                    "  3. Verify IAM permissions for SQS operations".dimmed()
                );
            }
            _ => {
                println!(
                    "{}",
                    format!(
                        "⚠ Cannot test connection for broker type '{}'",
                        config.broker.broker_type
                    )
                    .yellow()
                );
            }
        }

        println!();
    }

    println!("{}", "✓ Configuration validation complete".green().bold());

    Ok(())
}
