//! Prometheus metrics display: watch mode, filtering, and text/JSON/Prometheus formatting.
//!
//! 🤖 Generated with [SplitRS](https://github.com/cool-japan/splitrs)

use chrono::Utc;
use colored::Colorize;

/// Display Prometheus metrics
pub async fn show_metrics(
    format: &str,
    output_file: Option<&str>,
    pattern: Option<&str>,
    watch_interval: Option<u64>,
) -> anyhow::Result<()> {
    // If watch mode is enabled and output_file is set, it doesn't make sense
    if watch_interval.is_some() && output_file.is_some() {
        println!(
            "{}",
            "⚠ Watch mode cannot be used with file output".yellow()
        );
        return Ok(());
    }

    if let Some(interval) = watch_interval {
        // Watch mode - refresh metrics periodically
        println!("{}", "=== Metrics Watch Mode ===".bold().green());
        println!(
            "{}",
            format!("Refreshing every {interval} seconds (Ctrl+C to stop)").dimmed()
        );
        println!();

        loop {
            // Clear screen for better readability
            print!("\x1B[2J\x1B[1;1H"); // ANSI escape codes to clear screen

            // Display current time
            println!(
                "{}",
                format!("Last updated: {}", Utc::now().format("%Y-%m-%d %H:%M:%S")).dimmed()
            );
            println!();

            // Gather and format metrics
            format_and_display_metrics(format, pattern)?;

            // Sleep for the specified interval
            tokio::time::sleep(tokio::time::Duration::from_secs(interval)).await;
        }
    } else {
        // One-time display
        format_and_output_metrics(format, output_file, pattern)?;
        Ok(())
    }
}

/// Format and output metrics (for one-time display)
fn format_and_output_metrics(
    format: &str,
    output_file: Option<&str>,
    pattern: Option<&str>,
) -> anyhow::Result<()> {
    // Gather metrics from Prometheus registry
    let metrics_text = celers_metrics::gather_metrics();

    // Filter metrics if pattern is provided
    let filtered_metrics = filter_metrics(&metrics_text, pattern);

    // Format metrics based on format parameter
    let output = format_metrics_output(format, &filtered_metrics, pattern)?;

    // Write to file or stdout
    if let Some(file_path) = output_file {
        std::fs::write(file_path, &output)?;
        println!(
            "{}",
            format!("✓ Metrics exported to '{file_path}'")
                .green()
                .bold()
        );
        println!("  {} {}", "Format:".cyan(), format);
        if let Some(pat) = pattern {
            println!("  {} {}", "Filter:".cyan(), pat);
        }
    } else {
        println!("{output}");
    }

    Ok(())
}

/// Format and display metrics (for watch mode)
fn format_and_display_metrics(format: &str, pattern: Option<&str>) -> anyhow::Result<()> {
    let metrics_text = celers_metrics::gather_metrics();
    let filtered_metrics = filter_metrics(&metrics_text, pattern);
    let output = format_metrics_output(format, &filtered_metrics, pattern)?;
    println!("{output}");
    Ok(())
}

/// Filter metrics text by pattern
fn filter_metrics(metrics_text: &str, pattern: Option<&str>) -> String {
    if let Some(pat) = pattern {
        metrics_text
            .lines()
            .filter(|line| {
                if line.starts_with("# HELP") || line.starts_with("# TYPE") {
                    line.contains(pat)
                } else if line.starts_with('#') {
                    false
                } else {
                    line.contains(pat)
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
    } else {
        metrics_text.to_string()
    }
}

/// Format metrics into the requested output format
fn format_metrics_output(
    format: &str,
    filtered_metrics: &str,
    pattern: Option<&str>,
) -> anyhow::Result<String> {
    match format.to_lowercase().as_str() {
        "json" => {
            let mut metrics_map = serde_json::Map::new();
            for line in filtered_metrics.lines() {
                if line.starts_with('#') || line.trim().is_empty() {
                    continue;
                }
                if let Some(space_idx) = line.rfind(' ') {
                    let (metric_part, value_str) = line.split_at(space_idx);
                    let value_str = value_str.trim();
                    if let Ok(value) = value_str.parse::<f64>() {
                        let metric_name = if let Some(brace_idx) = metric_part.find('{') {
                            &metric_part[..brace_idx]
                        } else {
                            metric_part
                        };
                        metrics_map.insert(metric_name.to_string(), serde_json::json!(value));
                    }
                }
            }
            Ok(serde_json::to_string_pretty(&metrics_map)?)
        }
        "prometheus" | "prom" => Ok(filtered_metrics.to_string()),
        _ => {
            let mut output = String::new();
            output.push_str(&format!("{}\n\n", "=== CeleRS Metrics ===".bold().green()));
            let mut current_metric = String::new();
            let mut help_text = String::new();

            for line in filtered_metrics.lines() {
                if line.starts_with("# HELP") {
                    if let Some(help) = line.strip_prefix("# HELP ") {
                        let parts: Vec<&str> = help.splitn(2, ' ').collect();
                        if parts.len() == 2 {
                            current_metric = parts[0].to_string();
                            help_text = parts[1].to_string();
                        }
                    }
                } else if line.starts_with('#') || line.trim().is_empty() {
                    // Skip
                } else if let Some(space_idx) = line.rfind(' ') {
                    let (metric_part, value_str) = line.split_at(space_idx);
                    let value_str = value_str.trim();
                    let metric_name = if let Some(brace_idx) = metric_part.find('{') {
                        &metric_part[..brace_idx]
                    } else {
                        metric_part
                    };
                    if metric_name == current_metric && !help_text.is_empty() {
                        output.push_str(&format!("{}\n", metric_name.cyan().bold()));
                        output.push_str(&format!("  {}\n", help_text.dimmed()));
                        output.push_str(&format!(
                            "  {} {}\n\n",
                            "Value:".yellow(),
                            value_str.green()
                        ));
                        help_text.clear();
                    }
                }
            }

            if output.trim().is_empty() {
                output = format!("{}\n", "No metrics found".yellow());
                if pattern.is_some() {
                    output.push_str(&format!(
                        "{}\n",
                        "Try adjusting your filter pattern".dimmed()
                    ));
                }
            }
            Ok(output)
        }
    }
}
