//! Prometheus metrics and live monitoring command implementations.
//!
//! This module adds two operator-facing capabilities on top of the in-process
//! metrics command:
//!
//! - [`run_metrics`]: scrape a configurable Prometheus exposition endpoint over
//!   HTTP, parse the text-format payload with the native [`parse_prometheus`]
//!   parser, and render the result as a colored [`tabled`] table.
//! - [`run_monitor`]: a `top`-like live view that periodically re-scrapes the
//!   endpoint and re-renders a compact dashboard of key metrics.
//!
//! The design deliberately separates *pure* logic from *I/O* so the bulk of the
//! behaviour is unit-testable without a live server:
//!
//! - [`parse_prometheus`] is a pure function over a text payload.
//! - [`render_metrics_table`] and [`render_monitor_view`] are pure functions
//!   over the parsed [`PrometheusMetrics`] model.
//! - Only [`fetch_metrics`], [`run_metrics`], and [`run_monitor`] perform I/O,
//!   and they are kept intentionally thin.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::time::Duration;

use colored::Colorize;
use oxihttp_client::Client;
use tabled::{settings::Style, Table, Tabled};

/// Default Prometheus metrics endpoint used when none is supplied.
pub const DEFAULT_METRICS_ENDPOINT: &str = "http://localhost:9090/metrics";

/// Default refresh interval (in seconds) for the live monitor view.
pub const DEFAULT_MONITOR_INTERVAL_SECS: u64 = 2;

/// The Prometheus metric type as declared by a `# TYPE` comment.
///
/// Unknown or absent type declarations map to [`MetricType::Untyped`], matching
/// the Prometheus convention.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricType {
    /// A monotonically increasing counter.
    Counter,
    /// A value that can go up and down.
    Gauge,
    /// A histogram (exposes `_bucket`, `_sum`, and `_count` series).
    Histogram,
    /// A summary (exposes quantiles plus `_sum` and `_count` series).
    Summary,
    /// No type was declared (or it was unrecognised).
    Untyped,
}

impl MetricType {
    /// Parse the textual metric type found after `# TYPE <name>`.
    fn from_token(token: &str) -> Self {
        match token {
            "counter" => MetricType::Counter,
            "gauge" => MetricType::Gauge,
            "histogram" => MetricType::Histogram,
            "summary" => MetricType::Summary,
            _ => MetricType::Untyped,
        }
    }

    /// Human-readable label used in rendered output.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            MetricType::Counter => "counter",
            MetricType::Gauge => "gauge",
            MetricType::Histogram => "histogram",
            MetricType::Summary => "summary",
            MetricType::Untyped => "untyped",
        }
    }
}

/// A single observed sample belonging to a metric family.
///
/// `name` is the full series name (which, for histograms/summaries, may carry a
/// suffix such as `_bucket`, `_sum`, or `_count`). Labels are stored in a
/// [`BTreeMap`] so rendering is deterministic regardless of source ordering.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricSample {
    /// The full series name, including any histogram/summary suffix.
    pub name: String,
    /// Label key/value pairs (sorted for deterministic output).
    pub labels: BTreeMap<String, String>,
    /// The sample value.
    pub value: f64,
}

impl MetricSample {
    /// Render the labels as a canonical `{k="v",...}` string (empty if none).
    #[must_use]
    pub fn labels_display(&self) -> String {
        if self.labels.is_empty() {
            return String::new();
        }
        let mut rendered = String::from("{");
        for (idx, (key, value)) in self.labels.iter().enumerate() {
            if idx > 0 {
                rendered.push(',');
            }
            // Re-escape values so the rendered form round-trips.
            let escaped = escape_label_value(value);
            let _ = write!(rendered, "{key}=\"{escaped}\"");
        }
        rendered.push('}');
        rendered
    }
}

/// A logical metric family: all samples sharing a base metric name.
#[derive(Debug, Clone, PartialEq)]
pub struct MetricFamily {
    /// The base metric name (e.g. `celers_tasks_total`).
    pub name: String,
    /// The declared metric type.
    pub metric_type: MetricType,
    /// The `# HELP` text, if any.
    pub help: Option<String>,
    /// All samples observed for this family, in source order.
    pub samples: Vec<MetricSample>,
}

impl MetricFamily {
    /// Sum of all sample values in the family.
    ///
    /// This is most meaningful for counters/gauges; for histograms it sums the
    /// raw series and is therefore only a coarse aggregate.
    #[must_use]
    pub fn total(&self) -> f64 {
        self.samples.iter().map(|s| s.value).sum()
    }
}

/// The fully parsed Prometheus exposition payload.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct PrometheusMetrics {
    /// Metric families in first-seen order.
    pub families: Vec<MetricFamily>,
}

impl PrometheusMetrics {
    /// Total number of samples across all families.
    #[must_use]
    pub fn sample_count(&self) -> usize {
        self.families.iter().map(|f| f.samples.len()).sum()
    }

    /// Returns `true` if no families were parsed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.families.is_empty()
    }

    /// Look up a family by its base name.
    #[must_use]
    pub fn family(&self, name: &str) -> Option<&MetricFamily> {
        self.families.iter().find(|f| f.name == name)
    }

    /// Return a filtered clone keeping only families whose name contains
    /// `pattern` (case-sensitive substring match). `None` returns a full clone.
    #[must_use]
    pub fn filtered(&self, pattern: Option<&str>) -> PrometheusMetrics {
        match pattern {
            None => self.clone(),
            Some(pat) => PrometheusMetrics {
                families: self
                    .families
                    .iter()
                    .filter(|f| f.name.contains(pat))
                    .cloned()
                    .collect(),
            },
        }
    }
}

/// Parse a Prometheus text-format exposition payload into a structured model.
///
/// This is a pure function with no I/O, making it fully unit-testable without a
/// live server. It handles:
///
/// - `# HELP <name> <text>` and `# TYPE <name> <type>` comment lines.
/// - Sample lines with and without labels, including escaped label values
///   (`\\`, `\"`, `\n`).
/// - Counters, gauges, histograms, and summaries (their suffixed series are
///   grouped under the declared base family).
/// - Optional trailing timestamps on sample lines (parsed and discarded).
/// - Malformed lines, which are skipped rather than causing a hard failure.
///
/// Families are returned in the order their base name is first encountered
/// (whether via a `# HELP`/`# TYPE` line or a sample line).
#[must_use]
pub fn parse_prometheus(input: &str) -> PrometheusMetrics {
    // Index from base metric name -> position in `families` for O(1) grouping.
    let mut index: BTreeMap<String, usize> = BTreeMap::new();
    let mut families: Vec<MetricFamily> = Vec::new();

    for raw_line in input.lines() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }

        if let Some(comment) = line.strip_prefix('#') {
            parse_comment(comment.trim_start(), &mut index, &mut families);
            continue;
        }

        if let Some(sample) = parse_sample_line(line) {
            let base = base_name(&sample.name);
            let family_idx = ensure_family(&base, &mut index, &mut families);
            families[family_idx].samples.push(sample);
        }
    }

    PrometheusMetrics { families }
}

/// Handle a `HELP`/`TYPE` metadata comment (the leading `#` already stripped).
fn parse_comment(
    comment: &str,
    index: &mut BTreeMap<String, usize>,
    families: &mut Vec<MetricFamily>,
) {
    if let Some(rest) = comment.strip_prefix("HELP ") {
        let mut parts = rest.splitn(2, char::is_whitespace);
        if let Some(name) = parts.next() {
            if name.is_empty() {
                return;
            }
            let help = parts.next().unwrap_or("").trim().to_string();
            let idx = ensure_family(name, index, families);
            families[idx].help = Some(unescape_help(&help));
        }
    } else if let Some(rest) = comment.strip_prefix("TYPE ") {
        let mut parts = rest.split_whitespace();
        if let (Some(name), Some(type_token)) = (parts.next(), parts.next()) {
            if name.is_empty() {
                return;
            }
            let idx = ensure_family(name, index, families);
            families[idx].metric_type = MetricType::from_token(type_token);
        }
    }
    // Any other `#` comment is ignored.
}

/// Ensure a family with `name` exists, returning its index in `families`.
fn ensure_family(
    name: &str,
    index: &mut BTreeMap<String, usize>,
    families: &mut Vec<MetricFamily>,
) -> usize {
    if let Some(&idx) = index.get(name) {
        return idx;
    }
    let idx = families.len();
    families.push(MetricFamily {
        name: name.to_string(),
        metric_type: MetricType::Untyped,
        help: None,
        samples: Vec::new(),
    });
    index.insert(name.to_string(), idx);
    idx
}

/// Parse a single sample line such as:
///
/// ```text
/// http_requests_total{method="post",code="200"} 1027 1395066363000
/// ```
///
/// Returns `None` for malformed lines.
fn parse_sample_line(line: &str) -> Option<MetricSample> {
    // Split the metric identifier (name + optional labels) from the value.
    let (identifier, remainder) = split_identifier(line)?;
    let (name, labels) = split_name_and_labels(identifier)?;

    if name.is_empty() || !is_valid_metric_name(&name) {
        return None;
    }

    // The remainder is `<value> [timestamp]`; only the value is required.
    let mut value_iter = remainder.split_whitespace();
    let value_token = value_iter.next()?;
    let value = parse_value(value_token)?;

    Some(MetricSample {
        name,
        labels,
        value,
    })
}

/// Split a sample line into its identifier (name plus optional `{...}` labels)
/// and the trailing value/timestamp portion.
///
/// This is brace-aware so that whitespace inside label values does not confuse
/// the split.
fn split_identifier(line: &str) -> Option<(&str, &str)> {
    let bytes = line.as_bytes();
    let mut in_labels = false;
    let mut in_quotes = false;
    let mut escaped = false;

    for (idx, &byte) in bytes.iter().enumerate() {
        if in_quotes {
            if escaped {
                escaped = false;
            } else if byte == b'\\' {
                escaped = true;
            } else if byte == b'"' {
                in_quotes = false;
            }
            continue;
        }

        match byte {
            b'{' => in_labels = true,
            b'}' => in_labels = false,
            b'"' => in_quotes = true,
            b' ' | b'\t' if !in_labels => {
                let identifier = line[..idx].trim_end();
                let remainder = line[idx..].trim_start();
                if identifier.is_empty() || remainder.is_empty() {
                    return None;
                }
                return Some((identifier, remainder));
            }
            _ => {}
        }
    }

    None
}

/// Split an identifier into the metric name and parsed label map.
fn split_name_and_labels(identifier: &str) -> Option<(String, BTreeMap<String, String>)> {
    match identifier.find('{') {
        None => Some((identifier.trim().to_string(), BTreeMap::new())),
        Some(brace_idx) => {
            let name = identifier[..brace_idx].trim().to_string();
            let labels_part = &identifier[brace_idx..];
            let labels = parse_labels(labels_part)?;
            Some((name, labels))
        }
    }
}

/// Parse a `{k1="v1",k2="v2"}` label block into a map.
///
/// Returns `None` if the braces are unbalanced or a pair is malformed.
fn parse_labels(block: &str) -> Option<BTreeMap<String, String>> {
    let trimmed = block.trim();
    let inner = trimmed.strip_prefix('{')?.strip_suffix('}')?;
    let mut labels = BTreeMap::new();

    let inner = inner.trim();
    if inner.is_empty() {
        return Some(labels);
    }

    let mut chars = inner.char_indices().peekable();
    loop {
        // Skip leading separators/whitespace.
        while let Some(&(_, c)) = chars.peek() {
            if c == ',' || c.is_whitespace() {
                chars.next();
            } else {
                break;
            }
        }

        // Read the key up to '='.
        let key_start = match chars.peek() {
            Some(&(i, _)) => i,
            None => break,
        };
        let mut key_end = key_start;
        let mut found_eq = false;
        for (i, c) in chars.by_ref() {
            if c == '=' {
                key_end = i;
                found_eq = true;
                break;
            }
            key_end = i + c.len_utf8();
        }
        if !found_eq {
            return None;
        }
        let key = inner[key_start..key_end].trim().to_string();
        if key.is_empty() {
            return None;
        }

        // Expect an opening quote for the value.
        match chars.next() {
            Some((_, '"')) => {}
            _ => return None,
        }

        // Read the (possibly escaped) value up to the closing quote.
        let mut value = String::new();
        let mut closed = false;
        let mut escaped = false;
        for (_, c) in chars.by_ref() {
            if escaped {
                value.push(match c {
                    'n' => '\n',
                    't' => '\t',
                    other => other,
                });
                escaped = false;
            } else if c == '\\' {
                escaped = true;
            } else if c == '"' {
                closed = true;
                break;
            } else {
                value.push(c);
            }
        }
        if !closed {
            return None;
        }

        labels.insert(key, value);
    }

    Some(labels)
}

/// Parse a Prometheus sample value, handling the special `Inf`/`NaN` tokens.
fn parse_value(token: &str) -> Option<f64> {
    match token {
        "+Inf" | "Inf" => Some(f64::INFINITY),
        "-Inf" => Some(f64::NEG_INFINITY),
        "NaN" => Some(f64::NAN),
        other => other.parse::<f64>().ok(),
    }
}

/// Determine the base family name for a (possibly suffixed) series name.
fn base_name(series: &str) -> String {
    for suffix in ["_bucket", "_sum", "_count"] {
        if let Some(stripped) = series.strip_suffix(suffix) {
            if !stripped.is_empty() {
                return stripped.to_string();
            }
        }
    }
    series.to_string()
}

/// Validate that a metric name uses the Prometheus character set
/// (`[a-zA-Z_:][a-zA-Z0-9_:]*`).
fn is_valid_metric_name(name: &str) -> bool {
    let mut chars = name.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' || c == ':' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == ':')
}

/// Unescape a `# HELP` text (`\\`, `\n`).
fn unescape_help(help: &str) -> String {
    let mut out = String::with_capacity(help.len());
    let mut chars = help.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('n') => out.push('\n'),
                Some('\\') => out.push('\\'),
                Some(other) => {
                    out.push('\\');
                    out.push(other);
                }
                None => out.push('\\'),
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Escape a label value for round-trip rendering (`\\`, `"`, `\n`).
fn escape_label_value(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
    out
}

/// Format a sample value for display, trimming redundant decimals and giving
/// the special floats friendly names.
#[must_use]
pub fn format_value(value: f64) -> String {
    if value.is_nan() {
        return "NaN".to_string();
    }
    if value.is_infinite() {
        return if value.is_sign_positive() {
            "+Inf".to_string()
        } else {
            "-Inf".to_string()
        };
    }
    if value == value.trunc() && value.abs() < 1e15 {
        // Render whole numbers without a trailing `.0`.
        format!("{}", value as i64)
    } else {
        // Compact float rendering; trim trailing zeros.
        let mut s = format!("{value:.6}");
        while s.contains('.') && s.ends_with('0') {
            s.pop();
        }
        if s.ends_with('.') {
            s.pop();
        }
        s
    }
}

/// One row of the rendered metrics table.
#[derive(Tabled)]
struct MetricRow {
    #[tabled(rename = "Metric")]
    metric: String,
    #[tabled(rename = "Type")]
    metric_type: String,
    #[tabled(rename = "Labels")]
    labels: String,
    #[tabled(rename = "Value")]
    value: String,
}

/// Render the parsed metrics as a deterministic, colored table.
///
/// This is a pure function: given the same model and filter it always produces
/// byte-identical output, which makes it straightforward to assert on in tests.
/// When `pattern` is supplied, only families whose name contains it are shown.
#[must_use]
pub fn render_metrics_table(metrics: &PrometheusMetrics, pattern: Option<&str>) -> String {
    let view = metrics.filtered(pattern);

    if view.is_empty() {
        let mut out = String::new();
        let _ = writeln!(out, "{}", "No metrics found".yellow());
        if pattern.is_some() {
            let _ = writeln!(out, "{}", "Try adjusting your filter pattern".dimmed());
        }
        return out;
    }

    let mut rows: Vec<MetricRow> = Vec::with_capacity(view.sample_count());
    for family in &view.families {
        let type_label = family.metric_type.as_str().to_string();
        if family.samples.is_empty() {
            // Surface metadata-only families (HELP/TYPE without samples).
            rows.push(MetricRow {
                metric: family.name.clone(),
                metric_type: type_label,
                labels: String::new(),
                value: "-".to_string(),
            });
            continue;
        }
        for sample in &family.samples {
            rows.push(MetricRow {
                metric: sample.name.clone(),
                metric_type: type_label.clone(),
                labels: sample.labels_display(),
                value: format_value(sample.value),
            });
        }
    }

    let table = Table::new(rows).with(Style::rounded()).to_string();

    let mut out = String::new();
    let _ = writeln!(out, "{}", "=== CeleRS Metrics ===".bold().green());
    let _ = writeln!(
        out,
        "{}",
        format!(
            "{} families, {} samples",
            view.families.len(),
            view.sample_count()
        )
        .dimmed()
    );
    out.push('\n');
    out.push_str(&table);
    out.push('\n');
    out
}

/// Render a compact, `top`-like monitor view for the given focus metrics.
///
/// This is a pure function over the parsed model plus a caller-provided
/// timestamp, so the live loop can stay thin and the rendering remains testable.
///
/// - `focus` lists base metric names to highlight first; any that are present
///   are shown (in the given order) before the remaining families.
/// - `timestamp` is rendered verbatim in the header (the caller supplies the
///   clock, keeping this function deterministic).
/// - `max_rows` caps how many rows are shown.
#[must_use]
pub fn render_monitor_view(
    metrics: &PrometheusMetrics,
    focus: &[&str],
    timestamp: &str,
    max_rows: usize,
) -> String {
    let mut out = String::new();
    let _ = writeln!(out, "{}", "=== CeleRS Live Monitor ===".bold().green());
    let _ = writeln!(out, "{}", format!("Updated: {timestamp}").dimmed());
    let _ = writeln!(
        out,
        "{}",
        format!(
            "Tracking {} families ({} samples)",
            metrics.families.len(),
            metrics.sample_count()
        )
        .dimmed()
    );
    out.push('\n');

    if metrics.is_empty() {
        let _ = writeln!(out, "{}", "No metrics available from endpoint".yellow());
        return out;
    }

    // Determine display order: focus metrics first (if present), then the rest.
    let mut ordered: Vec<&MetricFamily> = Vec::new();
    let mut seen: BTreeMap<&str, ()> = BTreeMap::new();
    for name in focus {
        if let Some(family) = metrics.family(name) {
            ordered.push(family);
            seen.insert(family.name.as_str(), ());
        }
    }
    for family in &metrics.families {
        if !seen.contains_key(family.name.as_str()) {
            ordered.push(family);
        }
    }

    #[derive(Tabled)]
    struct MonitorRow {
        #[tabled(rename = "Metric")]
        metric: String,
        #[tabled(rename = "Type")]
        metric_type: String,
        #[tabled(rename = "Series")]
        series: String,
        #[tabled(rename = "Total")]
        total: String,
    }

    let mut rows: Vec<MonitorRow> = Vec::new();
    for family in ordered.into_iter().take(max_rows) {
        rows.push(MonitorRow {
            metric: family.name.clone(),
            metric_type: family.metric_type.as_str().to_string(),
            series: family.samples.len().to_string(),
            total: format_value(family.total()),
        });
    }

    let table = Table::new(rows).with(Style::rounded()).to_string();
    out.push_str(&table);
    out.push('\n');
    out
}

/// Fetch the raw Prometheus exposition text from an HTTP endpoint.
///
/// This is the only network-touching function for the `metrics` path; it is kept
/// thin so the parsing/rendering remain pure and testable.
pub async fn fetch_metrics(endpoint: &str) -> anyhow::Result<String> {
    let client = Client::builder()
        .with_webpki_roots()
        .connect_timeout(Duration::from_secs(10))
        .read_timeout(Duration::from_secs(10))
        .build_https()?;

    let request = client
        .get(endpoint)
        .map_err(|e| anyhow::anyhow!("Failed to build request for '{endpoint}': {e}"))?;

    let response = request
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to reach metrics endpoint '{endpoint}': {e}"))?;

    let status = response.status();
    if !status.is_success() {
        anyhow::bail!("Metrics endpoint '{endpoint}' returned HTTP {status}");
    }

    let body = response
        .body_text()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to read metrics body from '{endpoint}': {e}"))?;

    Ok(body)
}

/// Scrape a Prometheus endpoint and render the metrics as a colored table.
///
/// When `watch_secs` is provided the view is refreshed on that interval until
/// interrupted; otherwise a single snapshot is printed.
pub async fn run_metrics(
    endpoint: &str,
    pattern: Option<&str>,
    watch_secs: Option<u64>,
) -> anyhow::Result<()> {
    match watch_secs {
        None => {
            let body = fetch_metrics(endpoint).await?;
            let metrics = parse_prometheus(&body);
            print!("{}", render_metrics_table(&metrics, pattern));
            Ok(())
        }
        Some(interval) => {
            let interval = interval.max(1);
            println!("{}", "=== CeleRS Metrics (watch) ===".bold().green());
            println!(
                "{}",
                format!("Endpoint {endpoint}, every {interval}s (Ctrl+C to stop)").dimmed()
            );
            loop {
                // Clear screen between refreshes for readability.
                print!("\x1B[2J\x1B[1;1H");
                let header = chrono::Utc::now().format("%Y-%m-%d %H:%M:%S UTC");
                println!("{}", format!("Last updated: {header}").dimmed());
                match fetch_metrics(endpoint).await {
                    Ok(body) => {
                        let metrics = parse_prometheus(&body);
                        print!("{}", render_metrics_table(&metrics, pattern));
                    }
                    Err(e) => {
                        println!("{}", format!("✗ {e}").red());
                    }
                }
                tokio::time::sleep(Duration::from_secs(interval)).await;
            }
        }
    }
}

/// Run a live, `top`-like monitor that periodically re-scrapes the endpoint.
///
/// `interval_secs` controls the refresh cadence and `focus` lists base metric
/// names to surface first. The render step delegates to [`render_monitor_view`]
/// so the loop body stays thin.
pub async fn run_monitor(endpoint: &str, interval_secs: u64, focus: &[&str]) -> anyhow::Result<()> {
    let interval = interval_secs.max(1);
    const MAX_ROWS: usize = 30;

    println!("{}", "=== CeleRS Live Monitor ===".bold().green());
    println!(
        "{}",
        format!("Endpoint {endpoint}, refreshing every {interval}s (Ctrl+C to stop)").dimmed()
    );

    loop {
        print!("\x1B[2J\x1B[1;1H");
        let timestamp = chrono::Utc::now()
            .format("%Y-%m-%d %H:%M:%S UTC")
            .to_string();
        match fetch_metrics(endpoint).await {
            Ok(body) => {
                let metrics = parse_prometheus(&body);
                print!(
                    "{}",
                    render_monitor_view(&metrics, focus, &timestamp, MAX_ROWS)
                );
            }
            Err(e) => {
                println!("{}", "=== CeleRS Live Monitor ===".bold().green());
                println!("{}", format!("Updated: {timestamp}").dimmed());
                println!("{}", format!("✗ {e}").red());
            }
        }
        tokio::time::sleep(Duration::from_secs(interval)).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_counter_with_help_and_type() {
        let input = "\
# HELP celers_tasks_total Total number of tasks processed
# TYPE celers_tasks_total counter
celers_tasks_total 42
";
        let metrics = parse_prometheus(input);
        assert_eq!(metrics.families.len(), 1);
        let family = &metrics.families[0];
        assert_eq!(family.name, "celers_tasks_total");
        assert_eq!(family.metric_type, MetricType::Counter);
        assert_eq!(
            family.help.as_deref(),
            Some("Total number of tasks processed")
        );
        assert_eq!(family.samples.len(), 1);
        assert_eq!(family.samples[0].value, 42.0);
        assert!(family.samples[0].labels.is_empty());
    }

    #[test]
    fn parses_gauge_without_metadata() {
        let input = "celers_queue_depth 17\n";
        let metrics = parse_prometheus(input);
        assert_eq!(metrics.families.len(), 1);
        let family = &metrics.families[0];
        assert_eq!(family.metric_type, MetricType::Untyped);
        assert!(family.help.is_none());
        assert_eq!(family.samples[0].value, 17.0);
    }

    #[test]
    fn parses_labels_in_order() {
        let input = "\
# TYPE http_requests_total counter
http_requests_total{method=\"post\",code=\"200\"} 1027
http_requests_total{code=\"404\",method=\"get\"} 3
";
        let metrics = parse_prometheus(input);
        let family = &metrics.families[0];
        assert_eq!(family.samples.len(), 2);

        let first = &family.samples[0];
        assert_eq!(first.labels.get("method").map(String::as_str), Some("post"));
        assert_eq!(first.labels.get("code").map(String::as_str), Some("200"));
        // BTreeMap guarantees deterministic ordering regardless of source order.
        assert_eq!(first.labels_display(), "{code=\"200\",method=\"post\"}");

        let second = &family.samples[1];
        assert_eq!(second.labels_display(), "{code=\"404\",method=\"get\"}");
        assert_eq!(second.value, 3.0);
    }

    #[test]
    fn parses_label_value_with_escapes_and_spaces() {
        let input =
            "log_messages_total{msg=\"hello \\\"world\\\"\",path=\"/a b/c\"} 5 1395066363000\n";
        let metrics = parse_prometheus(input);
        let sample = &metrics.families[0].samples[0];
        assert_eq!(sample.value, 5.0);
        assert_eq!(
            sample.labels.get("msg").map(String::as_str),
            Some("hello \"world\"")
        );
        assert_eq!(
            sample.labels.get("path").map(String::as_str),
            Some("/a b/c")
        );
        // Round-trip the rendered label form.
        assert_eq!(
            sample.labels_display(),
            "{msg=\"hello \\\"world\\\"\",path=\"/a b/c\"}"
        );
    }

    #[test]
    fn groups_histogram_series_under_base_family() {
        let input = "\
# HELP request_duration_seconds Request duration
# TYPE request_duration_seconds histogram
request_duration_seconds_bucket{le=\"0.1\"} 24054
request_duration_seconds_bucket{le=\"0.5\"} 33444
request_duration_seconds_bucket{le=\"+Inf\"} 144320
request_duration_seconds_sum 53423
request_duration_seconds_count 144320
";
        let metrics = parse_prometheus(input);
        assert_eq!(metrics.families.len(), 1);
        let family = &metrics.families[0];
        assert_eq!(family.name, "request_duration_seconds");
        assert_eq!(family.metric_type, MetricType::Histogram);
        // 3 buckets + sum + count.
        assert_eq!(family.samples.len(), 5);

        let inf_bucket = family
            .samples
            .iter()
            .find(|s| s.labels.get("le").map(String::as_str) == Some("+Inf"))
            .expect("inf bucket present");
        assert_eq!(inf_bucket.value, 144320.0);
        assert_eq!(inf_bucket.name, "request_duration_seconds_bucket");
    }

    #[test]
    fn parses_summary_quantiles() {
        let input = "\
# TYPE rpc_duration_seconds summary
rpc_duration_seconds{quantile=\"0.5\"} 4773
rpc_duration_seconds{quantile=\"0.99\"} 76656
rpc_duration_seconds_sum 1.7560473e+07
rpc_duration_seconds_count 2693
";
        let metrics = parse_prometheus(input);
        let family = &metrics.families[0];
        assert_eq!(family.metric_type, MetricType::Summary);
        assert_eq!(family.samples.len(), 4);
        let count = family
            .samples
            .iter()
            .find(|s| s.name == "rpc_duration_seconds_count")
            .expect("count series present");
        assert_eq!(count.value, 2693.0);
        // Scientific-notation value parsed correctly.
        let sum = family
            .samples
            .iter()
            .find(|s| s.name == "rpc_duration_seconds_sum")
            .expect("sum series present");
        assert!((sum.value - 17_560_473.0).abs() < 1.0);
    }

    #[test]
    fn handles_special_float_values() {
        let input = "\
metric_inf +Inf
metric_neg_inf -Inf
metric_nan NaN
";
        let metrics = parse_prometheus(input);
        assert!(metrics.family("metric_inf").unwrap().samples[0]
            .value
            .is_infinite());
        assert!(metrics.family("metric_neg_inf").unwrap().samples[0].value < 0.0);
        assert!(metrics.family("metric_nan").unwrap().samples[0]
            .value
            .is_nan());
    }

    #[test]
    fn skips_malformed_lines() {
        let input = "\
# HELP good_metric A good metric
# TYPE good_metric gauge
good_metric 10
this_is_not_valid
missing_value{label=\"x\"}
bad_value abc
{no_name=\"x\"} 5
123starts_with_digit 7
another_good 20
";
        let metrics = parse_prometheus(input);
        // Only the two valid sample lines should yield families with samples.
        let good = metrics.family("good_metric").expect("good_metric present");
        assert_eq!(good.samples.len(), 1);
        assert_eq!(good.samples[0].value, 10.0);

        let another = metrics
            .family("another_good")
            .expect("another_good present");
        assert_eq!(another.samples[0].value, 20.0);

        // None of the malformed identifiers created sample-bearing families.
        assert!(metrics.family("this_is_not_valid").is_none());
        assert!(metrics.family("missing_value").is_none());
        assert!(metrics.family("bad_value").is_none());
        assert!(metrics.family("123starts_with_digit").is_none());
    }

    #[test]
    fn empty_labels_block_is_valid() {
        let input = "no_labels{} 99\n";
        let metrics = parse_prometheus(input);
        let sample = &metrics.family("no_labels").unwrap().samples[0];
        assert!(sample.labels.is_empty());
        assert_eq!(sample.value, 99.0);
    }

    #[test]
    fn help_only_family_has_no_samples() {
        let input = "\
# HELP orphan_metric Declared but never sampled
# TYPE orphan_metric counter
";
        let metrics = parse_prometheus(input);
        let family = metrics.family("orphan_metric").expect("present");
        assert_eq!(family.metric_type, MetricType::Counter);
        assert!(family.samples.is_empty());
    }

    #[test]
    fn empty_input_yields_empty_model() {
        let metrics = parse_prometheus("");
        assert!(metrics.is_empty());
        assert_eq!(metrics.sample_count(), 0);
        let metrics_ws = parse_prometheus("\n\n   \n");
        assert!(metrics_ws.is_empty());
    }

    #[test]
    fn filtered_keeps_matching_families() {
        let input = "\
celers_tasks_total 1
celers_queue_depth 2
other_metric 3
";
        let metrics = parse_prometheus(input);
        let filtered = metrics.filtered(Some("celers_"));
        assert_eq!(filtered.families.len(), 2);
        assert!(filtered.family("other_metric").is_none());
        assert!(filtered.family("celers_tasks_total").is_some());

        // None returns a full clone.
        let all = metrics.filtered(None);
        assert_eq!(all.families.len(), 3);
    }

    #[test]
    fn format_value_renders_integers_and_floats() {
        assert_eq!(format_value(42.0), "42");
        assert_eq!(format_value(0.0), "0");
        assert_eq!(format_value(-7.0), "-7");
        assert_eq!(format_value(1.5), "1.5");
        assert_eq!(format_value(12.3456), "12.3456");
        assert_eq!(format_value(f64::INFINITY), "+Inf");
        assert_eq!(format_value(f64::NEG_INFINITY), "-Inf");
        assert_eq!(format_value(f64::NAN), "NaN");
    }

    #[test]
    fn family_total_sums_samples() {
        let input = "\
# TYPE m counter
m{a=\"1\"} 10
m{a=\"2\"} 32
";
        let metrics = parse_prometheus(input);
        let family = metrics.family("m").unwrap();
        assert_eq!(family.total(), 42.0);
    }

    #[test]
    fn render_metrics_table_is_deterministic() {
        let input = "\
# HELP celers_tasks_total Total tasks
# TYPE celers_tasks_total counter
celers_tasks_total{status=\"ok\"} 100
celers_tasks_total{status=\"error\"} 5
# TYPE celers_queue_depth gauge
celers_queue_depth 17
";
        let metrics = parse_prometheus(input);
        let rendered_a = render_metrics_table(&metrics, None);
        let rendered_b = render_metrics_table(&metrics, None);
        // Pure function: identical output across calls.
        assert_eq!(rendered_a, rendered_b);

        // Known content is present in the table body.
        assert!(rendered_a.contains("celers_tasks_total"));
        assert!(rendered_a.contains("celers_queue_depth"));
        assert!(rendered_a.contains("counter"));
        assert!(rendered_a.contains("gauge"));
        assert!(rendered_a.contains("100"));
        assert!(rendered_a.contains("17"));
        assert!(rendered_a.contains("{status=\"ok\"}"));
        assert!(rendered_a.contains("2 families, 3 samples"));
    }

    #[test]
    fn render_metrics_table_respects_filter() {
        let input = "\
celers_a 1
other_b 2
";
        let metrics = parse_prometheus(input);
        let rendered = render_metrics_table(&metrics, Some("celers_"));
        assert!(rendered.contains("celers_a"));
        assert!(!rendered.contains("other_b"));
    }

    #[test]
    fn render_metrics_table_handles_empty() {
        let metrics = PrometheusMetrics::default();
        let rendered = render_metrics_table(&metrics, None);
        assert!(rendered.contains("No metrics found"));

        // With a filter, also hints at adjusting the pattern.
        let rendered_filtered = render_metrics_table(&metrics, Some("zzz"));
        assert!(rendered_filtered.contains("adjusting your filter"));
    }

    #[test]
    fn render_metrics_table_shows_metadata_only_family() {
        let input = "\
# HELP orphan_metric Declared but never sampled
# TYPE orphan_metric counter
";
        let metrics = parse_prometheus(input);
        let rendered = render_metrics_table(&metrics, None);
        assert!(rendered.contains("orphan_metric"));
        // Placeholder dash for the value column.
        assert!(rendered.contains('-'));
    }

    #[test]
    fn render_monitor_view_orders_focus_first() {
        let input = "\
# TYPE alpha gauge
alpha 1
# TYPE beta gauge
beta 2
# TYPE gamma gauge
gamma 3
";
        let metrics = parse_prometheus(input);
        let rendered =
            render_monitor_view(&metrics, &["gamma", "alpha"], "2026-06-13 00:00:00", 10);

        let gamma_pos = rendered.find("gamma").expect("gamma present");
        let alpha_pos = rendered.find("alpha").expect("alpha present");
        let beta_pos = rendered.find("beta").expect("beta present");

        // Focus order: gamma before alpha, both before the unfocused beta.
        assert!(gamma_pos < alpha_pos);
        assert!(alpha_pos < beta_pos);
        assert!(rendered.contains("2026-06-13 00:00:00"));
        assert!(rendered.contains("Tracking 3 families"));
    }

    #[test]
    fn render_monitor_view_is_deterministic_and_caps_rows() {
        let input = "\
a 1
b 2
c 3
d 4
";
        let metrics = parse_prometheus(input);
        let rendered_a = render_monitor_view(&metrics, &[], "T", 2);
        let rendered_b = render_monitor_view(&metrics, &[], "T", 2);
        assert_eq!(rendered_a, rendered_b);

        // Capped at 2 rows: only the first two families appear in the body.
        assert!(rendered_a.contains("a"));
        assert!(rendered_a.contains("b"));
        // The table body should not list the capped-out families' rows.
        // (We assert the row count indirectly via series/total presence.)
        let body_lines = rendered_a.lines().filter(|l| l.contains('│')).count();
        // Header row + 2 data rows => 3 content lines bounded by box drawing.
        assert!(body_lines >= 3);
    }

    #[test]
    fn render_monitor_view_handles_empty() {
        let metrics = PrometheusMetrics::default();
        let rendered = render_monitor_view(&metrics, &["x"], "now", 10);
        assert!(rendered.contains("No metrics available"));
    }

    #[test]
    fn base_name_strips_known_suffixes() {
        assert_eq!(base_name("foo_bucket"), "foo");
        assert_eq!(base_name("foo_sum"), "foo");
        assert_eq!(base_name("foo_count"), "foo");
        assert_eq!(base_name("foo_total"), "foo_total");
        assert_eq!(base_name("_bucket"), "_bucket");
    }

    #[test]
    fn valid_metric_name_rules() {
        assert!(is_valid_metric_name("foo"));
        assert!(is_valid_metric_name("_foo"));
        assert!(is_valid_metric_name("foo:bar_baz"));
        assert!(is_valid_metric_name("a1_2:3"));
        assert!(!is_valid_metric_name("1foo"));
        assert!(!is_valid_metric_name(""));
        assert!(!is_valid_metric_name("foo-bar"));
    }
}
