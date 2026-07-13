//! Utility functions shared across CLI commands.
//!
//! This module provides common helper functions used by various CLI commands,
//! including formatting, validation, and retry logic.

use tabled::{builder::Builder as TableBuilder, settings::Style};

/// Format duration into human-readable string.
///
/// Converts seconds into a human-readable format (e.g., "1h 30m", "2d 5h").
///
/// # Examples
///
/// ```
/// # use celers_cli::command_utils::format_duration;
/// assert_eq!(format_duration(90), "1m 30s");
/// assert_eq!(format_duration(3660), "1h 1m");
/// ```
pub fn format_duration(seconds: u64) -> String {
    if seconds < 60 {
        format!("{seconds}s")
    } else if seconds < 3600 {
        let minutes = seconds / 60;
        let secs = seconds % 60;
        if secs == 0 {
            format!("{minutes}m")
        } else {
            format!("{minutes}m {secs}s")
        }
    } else if seconds < 86400 {
        let hours = seconds / 3600;
        let minutes = (seconds % 3600) / 60;
        if minutes == 0 {
            format!("{hours}h")
        } else {
            format!("{hours}h {minutes}m")
        }
    } else {
        let days = seconds / 86400;
        let hours = (seconds % 86400) / 3600;
        if hours == 0 {
            format!("{days}d")
        } else {
            format!("{days}d {hours}h")
        }
    }
}

/// Write data to CSV file
///
/// # Arguments
///
/// * `file_path` - Output file path
/// * `headers` - CSV column headers
/// * `rows` - Data rows
///
/// # Examples
///
/// ```no_run
/// use celers_cli::command_utils::write_csv;
///
/// # fn example() -> anyhow::Result<()> {
/// let headers = vec!["Name", "Count"];
/// let rows = vec![
///     vec!["Tasks".to_string(), "100".to_string()],
///     vec!["Workers".to_string(), "5".to_string()],
/// ];
/// write_csv("report.csv", &headers, &rows)?;
/// # Ok(())
/// # }
/// ```
pub fn write_csv(file_path: &str, headers: &[&str], rows: &[Vec<String>]) -> anyhow::Result<()> {
    use std::fs::File;
    let file = File::create(file_path)?;
    let mut writer = csv::Writer::from_writer(file);

    // Write headers
    writer.write_record(headers)?;

    // Write rows
    for row in rows {
        writer.write_record(row)?;
    }

    writer.flush()?;
    Ok(())
}

// ============================================================================
// Report data records + pure CSV/HTML/SVG/template formatters.
//
// These types/functions are intentionally free of I/O (no Redis, no file
// access) so they can be exercised directly by unit tests; the `commands`
// handlers that gather data from the broker are responsible for building the
// records below and choosing how to emit them (stdout vs. file, via
// `write_csv`/`csv_to_string`/`render_html_report`/`render_table_string`).
// ============================================================================

/// A single day's aggregated task-execution metrics, used by `report history`.
///
/// Mirrors the JSON stored under the `celers:metrics:<queue>:daily:<date>`
/// Redis key (see `commands::report_daily`), reshaped into a plain,
/// CSV/HTML-friendly record.
#[derive(Debug, Clone, PartialEq)]
pub struct HistoryRecord {
    /// Date the metrics were recorded for (`YYYY-MM-DD`).
    pub date: String,
    /// Queue the metrics were recorded against.
    pub queue: String,
    /// Total tasks processed that day.
    pub total: u64,
    /// Tasks that succeeded.
    pub succeeded: u64,
    /// Tasks that failed.
    pub failed: u64,
    /// Tasks that were retried.
    pub retried: u64,
    /// Average execution time in seconds, if recorded.
    pub avg_execution_time: Option<f64>,
}

/// Column headers matching the row layout produced by [`format_history_csv`].
pub const HISTORY_CSV_HEADERS: [&str; 7] = [
    "Date",
    "Queue",
    "Total",
    "Succeeded",
    "Failed",
    "Retried",
    "Avg Execution Time (s)",
];

/// Format task execution history as CSV rows (see [`write_csv`]).
///
/// # Examples
///
/// ```
/// use celers_cli::command_utils::{format_history_csv, HistoryRecord};
///
/// let records = vec![HistoryRecord {
///     date: "2026-07-01".to_string(),
///     queue: "default".to_string(),
///     total: 10,
///     succeeded: 8,
///     failed: 2,
///     retried: 1,
///     avg_execution_time: Some(1.5),
/// }];
/// let rows = format_history_csv(&records);
/// assert_eq!(rows.len(), 1);
/// assert_eq!(rows[0][0], "2026-07-01");
/// ```
pub fn format_history_csv(records: &[HistoryRecord]) -> Vec<Vec<String>> {
    records
        .iter()
        .map(|r| {
            vec![
                r.date.clone(),
                r.queue.clone(),
                r.total.to_string(),
                r.succeeded.to_string(),
                r.failed.to_string(),
                r.retried.to_string(),
                r.avg_execution_time
                    .map_or_else(String::new, |t| format!("{t:.3}")),
            ]
        })
        .collect()
}

/// A single worker's status/statistics snapshot, used by `report workers`.
#[derive(Debug, Clone, PartialEq)]
pub struct WorkerStatRecord {
    /// Worker identifier.
    pub worker_id: String,
    /// Human status label (e.g. "Active", "Paused", "Draining").
    pub status: String,
    /// Tasks processed successfully.
    pub processed: u64,
    /// Tasks that failed.
    pub failed: u64,
    /// Last heartbeat timestamp (as stored by the worker), or "N/A".
    pub last_heartbeat: String,
}

/// Column headers matching the row layout produced by [`format_worker_csv`].
pub const WORKER_CSV_HEADERS: [&str; 5] = [
    "Worker ID",
    "Status",
    "Processed",
    "Failed",
    "Last Heartbeat",
];

/// Format worker statistics as CSV rows (see [`write_csv`]).
///
/// # Examples
///
/// ```
/// use celers_cli::command_utils::{format_worker_csv, WorkerStatRecord};
///
/// let records = vec![WorkerStatRecord {
///     worker_id: "w1".to_string(),
///     status: "Active".to_string(),
///     processed: 100,
///     failed: 3,
///     last_heartbeat: "2026-07-12T00:00:00Z".to_string(),
/// }];
/// let rows = format_worker_csv(&records);
/// assert_eq!(rows.len(), 1);
/// assert_eq!(rows[0][0], "w1");
/// ```
pub fn format_worker_csv(records: &[WorkerStatRecord]) -> Vec<Vec<String>> {
    records
        .iter()
        .map(|r| {
            vec![
                r.worker_id.clone(),
                r.status.clone(),
                r.processed.to_string(),
                r.failed.to_string(),
                r.last_heartbeat.clone(),
            ]
        })
        .collect()
}

/// A single queue's depth/health metrics, used by `report queues`.
#[derive(Debug, Clone, PartialEq)]
pub struct QueueMetricRecord {
    /// Queue name.
    pub queue: String,
    /// Human queue-type label (e.g. "FIFO", "Priority").
    pub queue_type: String,
    /// Pending (not yet claimed) task count.
    pub pending: u64,
    /// Currently-processing task count.
    pub processing: u64,
    /// Dead-letter-queue task count.
    pub dlq: u64,
    /// Delayed (scheduled for later) task count.
    pub delayed: u64,
}

/// Column headers matching the row layout produced by [`format_queue_csv`].
pub const QUEUE_CSV_HEADERS: [&str; 6] =
    ["Queue", "Type", "Pending", "Processing", "DLQ", "Delayed"];

/// Format queue metrics as CSV rows (see [`write_csv`]).
///
/// # Examples
///
/// ```
/// use celers_cli::command_utils::{format_queue_csv, QueueMetricRecord};
///
/// let records = vec![QueueMetricRecord {
///     queue: "default".to_string(),
///     queue_type: "FIFO".to_string(),
///     pending: 10,
///     processing: 2,
///     dlq: 1,
///     delayed: 0,
/// }];
/// let rows = format_queue_csv(&records);
/// assert_eq!(rows[0][0], "default");
/// assert_eq!(rows[0][2], "10");
/// ```
pub fn format_queue_csv(records: &[QueueMetricRecord]) -> Vec<Vec<String>> {
    records
        .iter()
        .map(|r| {
            vec![
                r.queue.clone(),
                r.queue_type.clone(),
                r.pending.to_string(),
                r.processing.to_string(),
                r.dlq.to_string(),
                r.delayed.to_string(),
            ]
        })
        .collect()
}

/// Render `headers`/`rows` as CSV text in memory.
///
/// Used to preview `--format csv` output on stdout when no `--output` file
/// is given. File output should continue to go through [`write_csv`], which
/// is the canonical CSV-file writer.
///
/// # Errors
///
/// Returns an error if CSV encoding fails, or if the internal buffer is not
/// valid UTF-8 (not expected for the string data this crate produces).
pub fn csv_to_string(headers: &[&str], rows: &[Vec<String>]) -> anyhow::Result<String> {
    let mut writer = csv::Writer::from_writer(Vec::new());
    writer.write_record(headers)?;
    for row in rows {
        writer.write_record(row)?;
    }
    let bytes = writer
        .into_inner()
        .map_err(|e| anyhow::anyhow!("failed to finalize CSV buffer: {e}"))?;
    Ok(String::from_utf8(bytes)?)
}

/// Render `headers`/`rows` as a colored, rounded-border console table.
///
/// Pure string formatting shared by every `report`/`profile` command's
/// `--format table` (default) path.
///
/// # Examples
///
/// ```
/// use celers_cli::command_utils::render_table_string;
///
/// let out = render_table_string(&["Name", "Count"], &[vec!["a".to_string(), "1".to_string()]]);
/// assert!(out.contains("Name"));
/// assert!(out.contains('1'));
/// ```
pub fn render_table_string(headers: &[&str], rows: &[Vec<String>]) -> String {
    let mut builder = TableBuilder::default();
    builder.push_record(headers.iter().map(|h| (*h).to_string()));
    for row in rows {
        builder.push_record(row.clone());
    }
    builder.build().with(Style::rounded()).to_string()
}

/// Escape a string for safe interpolation into HTML/XML (SVG) text content.
///
/// Escapes `&`, `<`, `>`, `"`, and `'`. Used by [`render_html_report`] and
/// [`render_svg_chart`] to neutralize task/queue/worker names before they
/// are interpolated into generated markup: that data originates from the
/// broker (task names, queue names, worker ids) and must be treated as
/// untrusted.
///
/// # Examples
///
/// ```
/// use celers_cli::command_utils::html_escape;
///
/// assert_eq!(html_escape("<script>"), "&lt;script&gt;");
/// assert_eq!(html_escape("a & b"), "a &amp; b");
/// assert_eq!(html_escape("\"quoted\""), "&quot;quoted&quot;");
/// ```
pub fn html_escape(input: &str) -> String {
    let mut escaped = String::with_capacity(input.len());
    for ch in input.chars() {
        match ch {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            _ => escaped.push(ch),
        }
    }
    escaped
}

/// Render `headers`/`rows` as an HTML `<table>` fragment.
///
/// Every header and cell value is passed through [`html_escape`] before
/// interpolation. Returns a fragment (not a full document); combine with
/// [`wrap_html_document`] (or use [`render_html_report`], which does this
/// for you) to produce a shareable, self-contained HTML file.
pub fn render_table_html(headers: &[&str], rows: &[Vec<String>]) -> String {
    let mut html = String::from("<table class=\"celers-report-table\">\n<thead>\n<tr>");
    for header in headers {
        html.push_str("<th>");
        html.push_str(&html_escape(header));
        html.push_str("</th>");
    }
    html.push_str("</tr>\n</thead>\n<tbody>\n");
    for row in rows {
        html.push_str("<tr>");
        for cell in row {
            html.push_str("<td>");
            html.push_str(&html_escape(cell));
            html.push_str("</td>");
        }
        html.push_str("</tr>\n");
    }
    html.push_str("</tbody>\n</table>\n");
    html
}

/// Wrap a pre-rendered HTML body fragment in a complete, self-contained HTML
/// document: doctype, a `<title>`, and inline CSS only -- no external
/// stylesheets, fonts, or scripts, so the resulting file is fully shareable
/// on its own (see the `report --format html` family).
///
/// `title` is HTML-escaped; `body_html` is inserted verbatim and must
/// already be escaped/sanitized by the caller (e.g. via
/// [`render_table_html`] and/or [`render_svg_chart`], both of which escape
/// their inputs).
pub fn wrap_html_document(title: &str, body_html: &str) -> String {
    let safe_title = html_escape(title);
    format!(
        "<!doctype html>\n\
<html lang=\"en\">\n\
<head>\n\
<meta charset=\"utf-8\">\n\
<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n\
<title>{safe_title}</title>\n\
<style>\n\
  body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; margin: 2rem; color: #1a1a1a; background: #fafafa; }}\n\
  h1 {{ color: #222; font-size: 1.5rem; }}\n\
  table.celers-report-table {{ border-collapse: collapse; width: 100%; margin-top: 1rem; background: #fff; }}\n\
  table.celers-report-table th, table.celers-report-table td {{ border: 1px solid #ddd; padding: 8px 12px; text-align: left; }}\n\
  table.celers-report-table th {{ background: #f0f0f0; }}\n\
  table.celers-report-table tr:hover {{ background: #eef6ff; }}\n\
  .celers-chart {{ margin-top: 1.5rem; }}\n\
  footer {{ margin-top: 2rem; color: #888; font-size: 0.8rem; }}\n\
</style>\n\
</head>\n\
<body>\n\
<h1>{safe_title}</h1>\n\
{body_html}\n\
<footer>Generated by celers report</footer>\n\
</body>\n\
</html>\n"
    )
}

/// Render a complete, self-contained HTML report page: a `<table>` built
/// from `headers`/`rows`, wrapped in [`wrap_html_document`].
///
/// All cell values are HTML-escaped (see [`render_table_html`]), so it is
/// safe to pass broker-supplied strings (task names, queue names, worker
/// ids, etc.) directly.
///
/// A thin wrapper around [`render_html_report_with_chart`] with
/// `chart: None`; see that function to also render an SVG bar chart above
/// the table.
///
/// # Examples
///
/// ```
/// use celers_cli::command_utils::render_html_report;
///
/// let html = render_html_report(
///     "Daily Report",
///     &[vec!["<script>".to_string(), "1".to_string()]],
///     &["Task", "Count"],
/// );
/// assert!(html.contains("&lt;script&gt;"));
/// assert!(!html.contains("<script>"));
/// ```
// `emit_report`'s "html" branch now calls `render_html_report_with_chart`
// directly (so it can pass a real chart series for some report types),
// which makes this thin wrapper unreachable from this crate's own `bin`
// target (it is still reachable -- and covered by tests/doctests -- via the
// public `celers_cli::command_utils` library API, which is the surface this
// function exists for). Kept, not renamed/removed, per its public API
// contract.
#[allow(dead_code)]
pub fn render_html_report(title: &str, rows: &[Vec<String>], headers: &[&str]) -> String {
    render_html_report_with_chart(title, rows, headers, None)
}

/// Render a complete, self-contained HTML report page: an optional chart --
/// an inline SVG bar chart (see [`render_svg_chart`]) wrapped in
/// `<div class="celers-chart">` -- followed by a `<table>` built from
/// `headers`/`rows`, all wrapped in [`wrap_html_document`].
///
/// When `chart` is `Some(series)`, the rendered SVG is prepended (inside its
/// `<div class="celers-chart">` wrapper, styled by the reserved
/// `.celers-chart` rule in [`wrap_html_document`]'s CSS) to the table
/// fragment. When `chart` is `None`, the output is byte-identical to
/// [`render_html_report`] (which delegates to this function).
///
/// All cell values (see [`render_table_html`]) and chart labels (see
/// [`render_svg_chart`]) are HTML-escaped, so it is safe to pass
/// broker-supplied strings (task names, queue names, worker ids, etc.)
/// directly.
///
/// # Examples
///
/// ```
/// use celers_cli::command_utils::render_html_report_with_chart;
///
/// let html = render_html_report_with_chart(
///     "Queue Metrics",
///     &[vec!["default".to_string(), "5".to_string()]],
///     &["Queue", "Pending"],
///     Some(&[("default".to_string(), 5.0)]),
/// );
/// assert!(html.contains("class=\"celers-chart\""));
/// assert!(html.contains("<svg"));
/// assert!(html.contains("celers-bar"));
///
/// // `chart: None` reproduces `render_html_report` exactly.
/// let no_chart = render_html_report_with_chart(
///     "Queue Metrics",
///     &[vec!["default".to_string(), "5".to_string()]],
///     &["Queue", "Pending"],
///     None,
/// );
/// // The reserved `.celers-chart` CSS *rule* is always in the stylesheet
/// // (see `wrap_html_document`), but the `<div class="celers-chart">
/// // wrapper itself must not be there when there's no chart.
/// assert!(!no_chart.contains("class=\"celers-chart\""));
/// ```
pub fn render_html_report_with_chart(
    title: &str,
    rows: &[Vec<String>],
    headers: &[&str],
    chart: Option<&[(String, f64)]>,
) -> String {
    let table_html = render_table_html(headers, rows);
    let body_html = match chart {
        Some(series) => format!(
            "<div class=\"celers-chart\">{}</div>\n{table_html}",
            render_svg_chart(series)
        ),
        None => table_html,
    };
    wrap_html_document(title, &body_html)
}

/// Render a simple inline-SVG bar chart from `(label, value)` pairs.
///
/// The output is a self-contained `<svg>` fragment (styling lives in a
/// `<style>` element embedded in the SVG itself, so it renders correctly
/// even when spliced into another document) with a `:hover` highlight and a
/// `<title>` tooltip per bar. There is no JavaScript charting library --
/// "interactive" here means CSS `:hover` plus native `<title>` tooltips.
///
/// Every label is escaped via [`html_escape`], since labels typically come
/// from broker data (task/queue/worker names).
///
/// # Examples
///
/// ```
/// use celers_cli::command_utils::render_svg_chart;
///
/// let svg = render_svg_chart(&[("ok".to_string(), 10.0), ("<bad>".to_string(), 2.0)]);
/// assert!(svg.starts_with("<svg"));
/// assert!(svg.contains("10.00"));
/// assert!(svg.contains("&lt;bad&gt;"));
/// assert!(!svg.contains("<bad>"));
/// ```
pub fn render_svg_chart(series: &[(String, f64)]) -> String {
    const WIDTH: f64 = 640.0;
    const HEIGHT: f64 = 320.0;
    const MARGIN: f64 = 48.0;

    if series.is_empty() {
        let half = HEIGHT / 2.0;
        return format!(
            "<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 {WIDTH} {HEIGHT}\" width=\"100%\" role=\"img\" aria-label=\"Empty chart\">\
<text x=\"{MARGIN}\" y=\"{half}\" font-family=\"sans-serif\" font-size=\"14\">No data</text></svg>\n"
        );
    }

    let chart_width = WIDTH - 2.0 * MARGIN;
    let chart_height = HEIGHT - 2.0 * MARGIN;
    let max_value = series
        .iter()
        .map(|(_, v)| *v)
        .fold(f64::MIN, f64::max)
        .max(0.0);
    let scale = if max_value > 0.0 { max_value } else { 1.0 };

    let bar_count = series.len();
    let gap = 8.0_f64;
    let bar_width =
        ((chart_width - gap * bar_count.saturating_sub(1) as f64) / bar_count as f64).max(1.0);

    let mut svg = format!(
        "<svg xmlns=\"http://www.w3.org/2000/svg\" viewBox=\"0 0 {WIDTH} {HEIGHT}\" width=\"100%\" height=\"{HEIGHT}\" role=\"img\" aria-label=\"Bar chart\">\n"
    );
    svg.push_str(
        "<style>\n\
  .celers-bar { fill: #4f8ef7; transition: fill 0.15s ease-in-out; }\n\
  .celers-bar:hover { fill: #ff9800; }\n\
  .celers-bar-label { font: 10px sans-serif; fill: #333; text-anchor: middle; }\n\
  .celers-bar-value { font: 10px sans-serif; fill: #333; text-anchor: middle; }\n\
</style>\n",
    );

    for (idx, (label, value)) in series.iter().enumerate() {
        let safe_label = html_escape(label);
        let bar_height = ((value / scale) * chart_height).clamp(0.0, chart_height);
        let x = MARGIN + idx as f64 * (bar_width + gap);
        let y = MARGIN + (chart_height - bar_height);
        let label_x = x + bar_width / 2.0;
        let value_y = (y - 4.0).max(10.0);
        let label_y = HEIGHT - MARGIN + 14.0;

        svg.push_str(&format!(
            "<g><title>{safe_label}: {value:.2}</title>\
<rect class=\"celers-bar\" x=\"{x:.2}\" y=\"{y:.2}\" width=\"{bar_width:.2}\" height=\"{bar_height:.2}\" />\
<text class=\"celers-bar-value\" x=\"{label_x:.2}\" y=\"{value_y:.2}\">{value:.2}</text>\
<text class=\"celers-bar-label\" x=\"{label_x:.2}\" y=\"{label_y:.2}\">{safe_label}</text></g>\n"
        ));
    }

    svg.push_str("</svg>\n");
    svg
}

/// Substitute `{{key}}` tokens in `template` with values from `fields`.
///
/// A deliberately small, dependency-free template engine backing "custom
/// report templates" (`report ... --template <STRING>`): each `{{key}}`
/// occurrence (whitespace around `key` is trimmed) is replaced with
/// `fields[key]` if present.
///
/// # Missing keys
///
/// A `{{key}}` token whose `key` is **not** present in `fields` is left
/// untouched in the output (rather than silently replaced with an empty
/// string), so template typos stay visible in the rendered output instead
/// of disappearing silently. An unterminated `{{` (no matching `}}`) is
/// also emitted verbatim. This function never panics.
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use celers_cli::command_utils::apply_template;
///
/// let mut fields = HashMap::new();
/// fields.insert("name".to_string(), "world".to_string());
///
/// assert_eq!(apply_template("Hello, {{name}}!", &fields), "Hello, world!");
/// // Missing keys are left as-is:
/// assert_eq!(apply_template("Hi {{missing}}", &fields), "Hi {{missing}}");
/// ```
pub fn apply_template(
    template: &str,
    fields: &std::collections::HashMap<String, String>,
) -> String {
    let mut result = String::with_capacity(template.len());
    let mut rest = template;

    while let Some(start) = rest.find("{{") {
        result.push_str(&rest[..start]);
        let after_open = &rest[start + 2..];

        match after_open.find("}}") {
            Some(end) => {
                let key = after_open[..end].trim();
                if let Some(value) = fields.get(key) {
                    result.push_str(value);
                } else {
                    result.push_str("{{");
                    result.push_str(&after_open[..end]);
                    result.push_str("}}");
                }
                rest = &after_open[end + 2..];
            }
            None => {
                // Unterminated token: no closing `}}`, emit the rest verbatim.
                result.push_str("{{");
                result.push_str(after_open);
                rest = "";
            }
        }
    }

    result.push_str(rest);
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(0), "0s");
        assert_eq!(format_duration(30), "30s");
        assert_eq!(format_duration(60), "1m");
        assert_eq!(format_duration(90), "1m 30s");
        assert_eq!(format_duration(3600), "1h");
        assert_eq!(format_duration(3660), "1h 1m");
        assert_eq!(format_duration(86400), "1d");
        assert_eq!(format_duration(90000), "1d 1h");
    }

    #[test]
    fn test_format_history_csv() {
        let records = vec![
            HistoryRecord {
                date: "2026-07-01".to_string(),
                queue: "default".to_string(),
                total: 10,
                succeeded: 8,
                failed: 2,
                retried: 1,
                avg_execution_time: Some(1.5),
            },
            HistoryRecord {
                date: "2026-07-02".to_string(),
                queue: "default".to_string(),
                total: 5,
                succeeded: 5,
                failed: 0,
                retried: 0,
                avg_execution_time: None,
            },
        ];
        let rows = format_history_csv(&records);
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0],
            vec!["2026-07-01", "default", "10", "8", "2", "1", "1.500"]
        );
        assert_eq!(
            rows[1],
            vec!["2026-07-02", "default", "5", "5", "0", "0", ""]
        );
        assert_eq!(HISTORY_CSV_HEADERS.len(), rows[0].len());
    }

    #[test]
    fn test_format_worker_csv() {
        let records = vec![WorkerStatRecord {
            worker_id: "w1".to_string(),
            status: "Active".to_string(),
            processed: 100,
            failed: 3,
            last_heartbeat: "2026-07-12T00:00:00Z".to_string(),
        }];
        let rows = format_worker_csv(&records);
        assert_eq!(
            rows,
            vec![vec![
                "w1".to_string(),
                "Active".to_string(),
                "100".to_string(),
                "3".to_string(),
                "2026-07-12T00:00:00Z".to_string(),
            ]]
        );
        assert_eq!(WORKER_CSV_HEADERS.len(), rows[0].len());
    }

    #[test]
    fn test_format_queue_csv() {
        let records = vec![QueueMetricRecord {
            queue: "default".to_string(),
            queue_type: "FIFO".to_string(),
            pending: 10,
            processing: 2,
            dlq: 1,
            delayed: 0,
        }];
        let rows = format_queue_csv(&records);
        assert_eq!(rows[0][0], "default");
        assert_eq!(rows[0][1], "FIFO");
        assert_eq!(rows[0][2], "10");
        assert_eq!(QUEUE_CSV_HEADERS.len(), rows[0].len());
    }

    #[test]
    fn test_csv_to_string() {
        let out = csv_to_string(&["A", "B"], &[vec!["1".to_string(), "2".to_string()]])
            .expect("well-formed rows should encode to CSV");
        assert!(out.contains("A,B"));
        assert!(out.contains("1,2"));
    }

    #[test]
    fn test_render_table_string() {
        let out = render_table_string(
            &["Name", "Count"],
            &[vec!["tasks".to_string(), "5".to_string()]],
        );
        assert!(out.contains("Name"));
        assert!(out.contains("tasks"));
        assert!(out.contains('5'));
    }

    #[test]
    fn test_html_escape() {
        assert_eq!(
            html_escape("<script>alert('x')</script>"),
            "&lt;script&gt;alert(&#39;x&#39;)&lt;/script&gt;"
        );
        assert_eq!(html_escape("a & b"), "a &amp; b");
        assert_eq!(html_escape("\"q\""), "&quot;q&quot;");
        assert_eq!(html_escape("plain"), "plain");
    }

    #[test]
    fn test_render_table_html_escapes_values() {
        let html = render_table_html(
            &["Task", "Count"],
            &[vec![
                "<script>alert(1)</script>".to_string(),
                "42".to_string(),
            ]],
        );
        assert!(html.contains("&lt;script&gt;"));
        assert!(!html.contains("<script>"));
        assert!(html.contains("42"));
    }

    #[test]
    fn test_wrap_html_document_escapes_title() {
        let doc = wrap_html_document("<Title> & \"Quoted\"", "<p>body</p>");
        assert!(doc.starts_with("<!doctype html>"));
        assert!(doc.contains("&lt;Title&gt; &amp; &quot;Quoted&quot;"));
        // Body is inserted verbatim (caller is responsible for escaping it).
        assert!(doc.contains("<p>body</p>"));
    }

    #[test]
    fn test_render_html_report_escapes_and_contains_values() {
        let html = render_html_report(
            "My <Report> & \"Title\"",
            &[vec![
                "<script>alert(1)</script>".to_string(),
                "42".to_string(),
            ]],
            &["Task", "Count"],
        );
        assert!(html.contains("&lt;script&gt;"));
        assert!(!html.contains("<script>"));
        assert!(html.contains("42"));
        assert!(html.contains("&lt;Report&gt;"));
        assert!(html.starts_with("<!doctype html>"));
    }

    #[test]
    fn test_render_html_report_with_chart_includes_chart_markup() {
        let html = render_html_report_with_chart(
            "Queue Metrics Report",
            &[vec!["default".to_string(), "5".to_string()]],
            &["Queue", "Pending"],
            Some(&[("default".to_string(), 5.0), ("<bad>".to_string(), 2.0)]),
        );
        // The `.celers-chart` CSS rule lives unconditionally in
        // `wrap_html_document`'s stylesheet, so check for the actual
        // `<div class="celers-chart">` wrapper, not just the class-name
        // substring (which would trivially match even without a chart).
        assert!(html.contains("<div class=\"celers-chart\">"));
        assert!(html.contains("<svg"));
        assert!(html.contains("celers-bar"));
        // Chart labels are escaped, same as `render_svg_chart` on its own.
        assert!(html.contains("&lt;bad&gt;"));
        assert!(!html.contains("<bad>"));
        // The table is still rendered alongside the chart.
        assert!(html.contains("celers-report-table"));
        assert!(html.contains("Pending"));
        assert!(html.starts_with("<!doctype html>"));
    }

    #[test]
    fn test_render_html_report_with_chart_none_matches_render_html_report() {
        let title = "My <Report> & \"Title\"";
        let rows = vec![vec![
            "<script>alert(1)</script>".to_string(),
            "42".to_string(),
        ]];
        let headers = ["Task", "Count"];

        let via_old_fn = render_html_report(title, &rows, &headers);
        let via_new_fn = render_html_report_with_chart(title, &rows, &headers, None);
        // Regression guard: `chart: None` must reproduce `render_html_report`
        // byte-for-byte.
        assert_eq!(via_old_fn, via_new_fn);
        // No chart div is emitted when `chart` is `None` (the reserved
        // `.celers-chart` CSS *rule* is always present in the stylesheet --
        // see above -- but the `<div class="celers-chart">` wrapper itself
        // must not be).
        assert!(!via_new_fn.contains("<div class=\"celers-chart\">"));
    }

    #[test]
    fn test_render_svg_chart_contains_values_and_escapes() {
        let svg = render_svg_chart(&[("ok".to_string(), 12.5), ("<bad>&".to_string(), 3.0)]);
        assert!(svg.starts_with("<svg"));
        assert!(svg.contains("12.50"));
        assert!(svg.contains("3.00"));
        assert!(svg.contains("&lt;bad&gt;&amp;"));
        assert!(!svg.contains("<bad>"));
        assert!(svg.contains("<title>ok: 12.50</title>"));
        assert!(svg.contains(":hover"));
    }

    #[test]
    fn test_render_svg_chart_empty_series() {
        let svg = render_svg_chart(&[]);
        assert!(svg.starts_with("<svg"));
        assert!(svg.contains("No data"));
    }

    #[test]
    fn test_apply_template_basic_and_missing_key() {
        let mut fields = std::collections::HashMap::new();
        fields.insert("name".to_string(), "world".to_string());

        assert_eq!(apply_template("Hello, {{name}}!", &fields), "Hello, world!");
        // Missing keys are left as-is (documented behavior), never panics.
        assert_eq!(apply_template("Hi {{missing}}", &fields), "Hi {{missing}}");
        assert_eq!(apply_template("no tokens here", &fields), "no tokens here");
        assert_eq!(
            apply_template("unterminated {{oops", &fields),
            "unterminated {{oops"
        );
        // Whitespace inside the braces is trimmed.
        assert_eq!(apply_template("{{ name }}", &fields), "world");
        // Repeated tokens all get substituted.
        assert_eq!(apply_template("{{name}}-{{name}}", &fields), "world-world");
        // Empty template / empty fields never panic.
        assert_eq!(apply_template("", &fields), "");
        assert_eq!(
            apply_template("{{name}}", &std::collections::HashMap::new()),
            "{{name}}"
        );
    }
}
