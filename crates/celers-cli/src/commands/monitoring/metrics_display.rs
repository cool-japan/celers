//! Prometheus metrics display: watch mode, filtering, and text/JSON/Prometheus formatting.
//!
//! 🤖 Generated with [SplitRS](https://github.com/cool-japan/splitrs)
//!
//! # Why this does not use the CLI's own in-process registry
//!
//! `celers-cli` is a one-shot command binary: it never runs a worker loop or
//! processes tasks within `show_metrics`'s own invocation, so
//! `celers_metrics::gather_metrics()` (this process's local Prometheus
//! registry) is *always* an all-zero snapshot of a system this process was
//! never part of -- every counter/gauge it declares is only ever
//! incremented by `celers-worker`/broker code paths that do not run inside
//! `celers metrics`. Formatting and printing that as though it reflected
//! live cluster state would be actively misleading. Instead, this module
//! reuses [`crate::commands::metrics_cmds`]'s already-correct, already-tested
//! HTTP scrape + parser (the same one `--endpoint`/`celers monitor` use) to
//! reach a real source by default (`DEFAULT_METRICS_ENDPOINT`), and reports
//! a clear error -- never a fabricated empty report -- when no such source
//! is reachable.

use chrono::Utc;
use colored::Colorize;

use crate::commands::metrics_cmds::{
    fetch_metrics, format_value, parse_prometheus, PrometheusMetrics, DEFAULT_METRICS_ENDPOINT,
};

/// Display Prometheus metrics.
///
/// Scrapes [`DEFAULT_METRICS_ENDPOINT`] (the same convention
/// `celers monitor`/`celers metrics --endpoint` use) and renders the result
/// in the requested `format`. There is no in-process fallback: this CLI
/// binary never records its own task/broker/worker metrics, so an
/// in-process registry would always be empty and printing it would be
/// indistinguishable from (and mistakeable for) real data. `--format`,
/// `--output`, `--pattern`, and `--watch` all remain fully supported; for a
/// non-default endpoint, callers should use
/// [`crate::commands::metrics_cmds::run_metrics`] (wired to `--endpoint`)
/// instead.
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
            format!(
                "Endpoint {DEFAULT_METRICS_ENDPOINT}, refreshing every {interval} seconds (Ctrl+C to stop)"
            )
            .dimmed()
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

            // A transient scrape failure must not kill a long-running watch
            // session (mirrors `metrics_cmds::run_metrics`'s watch loop):
            // report it clearly and keep polling rather than exiting.
            match fetch_and_format(format, pattern).await {
                Ok(output) => println!("{output}"),
                Err(e) => println!("{}", format!("✗ {e}").red()),
            }

            // Sleep for the specified interval
            tokio::time::sleep(tokio::time::Duration::from_secs(interval)).await;
        }
    } else {
        // One-time display: a failed scrape here *does* end the command
        // (nonzero exit), matching ordinary one-shot CLI failure semantics.
        let output = fetch_and_format(format, pattern).await?;

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
}

/// Scrape [`DEFAULT_METRICS_ENDPOINT`] and render the result in `format`,
/// filtered by `pattern`.
///
/// This is the single I/O + formatting entry point shared by the one-shot
/// and watch-mode paths, so both apply an identical, honest error when no
/// real metrics source is reachable.
async fn fetch_and_format(format: &str, pattern: Option<&str>) -> anyhow::Result<String> {
    let body = fetch_metrics(DEFAULT_METRICS_ENDPOINT).await.map_err(|e| {
        anyhow::anyhow!(
            "no metrics source reachable at {DEFAULT_METRICS_ENDPOINT}: {e}\n  \
             This CLI process does not execute tasks itself, so it has no in-process \
             metrics of its own to fall back to -- only a live exporter's data would \
             be real. Pass --endpoint <url> to point at a different broker/worker \
             metrics endpoint (e.g. celers metrics --endpoint http://localhost:9100/metrics)."
        )
    })?;

    match format.to_lowercase().as_str() {
        "prometheus" | "prom" => Ok(filter_raw_text(&body, pattern)),
        "json" | "text" => {
            let metrics = parse_prometheus(&body);
            format_parsed_metrics(&metrics, format, pattern)
        }
        other => anyhow::bail!(
            "unknown metrics format '{other}' (expected one of: text, json, prometheus)"
        ),
    }
}

/// Render already-parsed metrics in `format` (`"json"` or `"text"`),
/// filtered by `pattern`.
///
/// Building on [`PrometheusMetrics`] (rather than re-scanning the raw
/// exposition text line-by-line) is what makes this correct where the
/// previous line-based implementation was not: every label combination of a
/// metric becomes its own entry (JSON no longer collapses distinct
/// `{task_name="a"}` / `{task_name="b"}` series of the same metric onto one
/// key, silently keeping only the last), and every series of a family is
/// printed in `text` mode (the old code cleared its `help_text` after the
/// first match, silencing every subsequent series of a multi-series
/// family).
fn format_parsed_metrics(
    metrics: &PrometheusMetrics,
    format: &str,
    pattern: Option<&str>,
) -> anyhow::Result<String> {
    let view = metrics.filtered(pattern);
    match format.to_lowercase().as_str() {
        "json" => {
            let mut entries = Vec::with_capacity(view.sample_count());
            for family in &view.families {
                for sample in &family.samples {
                    entries.push(serde_json::json!({
                        "name": sample.name,
                        "labels": sample.labels,
                        "value": sample.value,
                    }));
                }
            }
            Ok(serde_json::to_string_pretty(&entries)?)
        }
        "text" => Ok(render_text_report(&view, pattern)),
        other => anyhow::bail!(
            "unknown metrics format '{other}' (expected one of: text, json, prometheus)"
        ),
    }
}

/// Human-readable `text`-format report: every family, its help text (if
/// any), and *every* sample (fixing the original single-series-per-family
/// bug).
fn render_text_report(view: &PrometheusMetrics, pattern: Option<&str>) -> String {
    let mut output = String::new();
    output.push_str(&format!("{}\n\n", "=== CeleRS Metrics ===".bold().green()));

    if view.is_empty() {
        output.push_str(&format!("{}\n", "No metrics found".yellow()));
        if pattern.is_some() {
            output.push_str(&format!(
                "{}\n",
                "Try adjusting your filter pattern".dimmed()
            ));
        }
        return output;
    }

    for family in &view.families {
        output.push_str(&format!("{}\n", family.name.cyan().bold()));
        if let Some(help) = &family.help {
            output.push_str(&format!("  {}\n", help.dimmed()));
        }
        if family.samples.is_empty() {
            output.push_str(&format!("  {} -\n", "Value:".yellow()));
        }
        for sample in &family.samples {
            let labels = sample.labels_display();
            if labels.is_empty() {
                output.push_str(&format!(
                    "  {} {}\n",
                    "Value:".yellow(),
                    format_value(sample.value).green()
                ));
            } else {
                output.push_str(&format!(
                    "  {} {}\n",
                    labels.dimmed(),
                    format_value(sample.value).green()
                ));
            }
        }
        output.push('\n');
    }

    output
}

/// Filter raw Prometheus exposition text by (line-level) substring match on
/// `pattern`, preserving the original text verbatim for the `prometheus`
/// output format (a pass-through, so there is nothing to reconstruct from
/// the parsed model for this format).
fn filter_raw_text(metrics_text: &str, pattern: Option<&str>) -> String {
    let Some(pat) = pattern else {
        return metrics_text.to_string();
    };
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
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_body() -> String {
        "\
# HELP celers_tasks_completed_by_type_total Total tasks completed by type
# TYPE celers_tasks_completed_by_type_total counter
celers_tasks_completed_by_type_total{task_name=\"a\"} 5
celers_tasks_completed_by_type_total{task_name=\"b\"} 7
# HELP celers_queue_depth Queue depth
# TYPE celers_queue_depth gauge
celers_queue_depth 3
"
        .to_string()
    }

    // ---- format_parsed_metrics: json keeps every labeled series ----------

    #[test]
    fn json_format_keeps_every_label_combination() {
        let metrics = parse_prometheus(&sample_body());
        let output = format_parsed_metrics(&metrics, "json", None).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        let entries = parsed.as_array().expect("json output is an array");

        // Both {task_name="a"} and {task_name="b"} series must survive as
        // distinct entries -- the original bug collapsed both onto one
        // `celers_tasks_completed_by_type_total` key, keeping only the last.
        let completed: Vec<&serde_json::Value> = entries
            .iter()
            .filter(|e| e["name"] == "celers_tasks_completed_by_type_total")
            .collect();
        assert_eq!(completed.len(), 2, "both labeled series must be present");

        let values: Vec<f64> = completed
            .iter()
            .map(|e| e["value"].as_f64().unwrap())
            .collect();
        assert!(values.contains(&5.0));
        assert!(values.contains(&7.0));

        // Labels are preserved as a structured object, not flattened away.
        let a_entry = completed
            .iter()
            .find(|e| e["value"].as_f64() == Some(5.0))
            .unwrap();
        assert_eq!(a_entry["labels"]["task_name"], "a");
    }

    #[test]
    fn json_format_is_a_flat_array_of_name_labels_value() {
        let metrics = parse_prometheus(&sample_body());
        let output = format_parsed_metrics(&metrics, "json", None).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&output).unwrap();
        let entries = parsed.as_array().unwrap();
        // 2 labeled series + 1 gauge sample = 3 total entries.
        assert_eq!(entries.len(), 3);
        for entry in entries {
            assert!(entry.get("name").is_some());
            assert!(entry.get("labels").is_some());
            assert!(entry.get("value").is_some());
        }
    }

    // ---- format_parsed_metrics: text prints every series ------------------

    #[test]
    fn text_format_prints_every_series_of_a_multi_series_family() {
        let metrics = parse_prometheus(&sample_body());
        let output = format_parsed_metrics(&metrics, "text", None).unwrap();

        // The original bug cleared `help_text` after the first match, so
        // only one of the two {task_name=...} values ever printed.
        assert!(output.contains("5"));
        assert!(output.contains("7"));
        assert!(output.contains("task_name=\"a\""));
        assert!(output.contains("task_name=\"b\""));
    }

    #[test]
    fn text_format_includes_help_text_once_per_family() {
        let metrics = parse_prometheus(&sample_body());
        let output = format_parsed_metrics(&metrics, "text", None).unwrap();
        assert_eq!(output.matches("Total tasks completed by type").count(), 1);
    }

    #[test]
    fn text_format_pattern_filters_families() {
        let metrics = parse_prometheus(&sample_body());
        let output = format_parsed_metrics(&metrics, "text", Some("queue")).unwrap();
        assert!(output.contains("celers_queue_depth"));
        assert!(!output.contains("celers_tasks_completed_by_type_total"));
    }

    #[test]
    fn text_format_empty_view_says_no_metrics_found() {
        let metrics = PrometheusMetrics::default();
        let output = format_parsed_metrics(&metrics, "text", None).unwrap();
        assert!(output.contains("No metrics found"));
    }

    // ---- format_parsed_metrics: unknown format is rejected -----------------

    #[test]
    fn unknown_format_is_rejected_not_silently_treated_as_text() {
        let metrics = parse_prometheus(&sample_body());
        let result = format_parsed_metrics(&metrics, "yaml", None);
        assert!(result.is_err());
        let msg = result.unwrap_err().to_string();
        assert!(msg.contains("yaml"));
    }

    #[test]
    fn format_matching_is_case_insensitive() {
        let metrics = parse_prometheus(&sample_body());
        assert!(format_parsed_metrics(&metrics, "JSON", None).is_ok());
        assert!(format_parsed_metrics(&metrics, "Text", None).is_ok());
    }

    // ---- filter_raw_text (prometheus/prom pass-through) --------------------

    #[test]
    fn filter_raw_text_keeps_matching_lines_and_their_metadata() {
        let body = sample_body();
        let filtered = filter_raw_text(&body, Some("queue"));
        assert!(filtered.contains("celers_queue_depth"));
        assert!(!filtered.contains("celers_tasks_completed_by_type_total"));
    }

    #[test]
    fn filter_raw_text_no_pattern_returns_everything() {
        let body = sample_body();
        assert_eq!(filter_raw_text(&body, None), body);
    }

    // ---- metrics_cmds re-exports resolve (compile-time sanity) ------------

    #[test]
    fn default_metrics_endpoint_is_a_url_like_string() {
        assert!(DEFAULT_METRICS_ENDPOINT.starts_with("http://"));
    }
}
