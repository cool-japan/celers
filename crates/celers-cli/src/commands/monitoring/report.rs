//! Report generation: daily/weekly/history/workers/queues, with table/csv/html/template output.
//!
//! 🤖 Generated with [SplitRS](https://github.com/cool-japan/splitrs)

use crate::command_utils;
use chrono::Utc;
use colored::Colorize;
use tabled::{settings::Style, Table, Tabled};

/// Generate daily execution report
///
/// Not reachable from this crate's own `bin` target: `cli::dispatch`'s
/// `Report::Daily` arm calls `report_daily_formatted` (the
/// `--format table|csv|html --output <path> --template <STRING>`-aware
/// sibling defined below) exclusively, so nothing in the bin's own call
/// graph reaches this stdout-only original. It is still reachable -- and
/// actually called -- via the public `celers_cli::commands` library API,
/// which is the surface this function exists for:
/// `examples/monitoring_and_diagnostics.rs` calls `commands::report_daily`
/// directly. Kept, not renamed/removed, per its public API contract. (Its
/// sibling `report_weekly` had no such caller anywhere in the crate and was
/// removed; only `report_weekly_formatted` remains.)
#[allow(dead_code)]
pub async fn report_daily(broker_url: &str, queue: &str) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client
        .get_multiplexed_async_connection_with_config(&crate::pool::async_connection_config())
        .await?;

    println!("{}", "=== Daily Execution Report ===".bold().cyan());
    println!();

    let now = Utc::now();
    let today_key = format!("celers:metrics:{}:daily:{}", queue, now.format("%Y-%m-%d"));

    let metrics: Option<String> = redis::cmd("GET")
        .arg(&today_key)
        .query_async(&mut conn)
        .await?;

    if let Some(metrics_str) = metrics {
        if let Ok(metrics_json) = serde_json::from_str::<serde_json::Value>(&metrics_str) {
            println!("{}", format!("Date: {}", now.format("%Y-%m-%d")).yellow());
            println!();

            #[derive(Tabled)]
            struct DailyMetric {
                #[tabled(rename = "Metric")]
                metric: String,
                #[tabled(rename = "Count")]
                count: String,
            }

            let mut daily_metrics = vec![];

            if let Some(total) = metrics_json
                .get("total_tasks")
                .and_then(serde_json::Value::as_u64)
            {
                daily_metrics.push(DailyMetric {
                    metric: "Total Tasks".to_string(),
                    count: total.to_string(),
                });
            }

            if let Some(succeeded) = metrics_json
                .get("succeeded")
                .and_then(serde_json::Value::as_u64)
            {
                daily_metrics.push(DailyMetric {
                    metric: "Succeeded".to_string(),
                    count: succeeded.to_string(),
                });
            }

            if let Some(failed) = metrics_json
                .get("failed")
                .and_then(serde_json::Value::as_u64)
            {
                daily_metrics.push(DailyMetric {
                    metric: "Failed".to_string(),
                    count: failed.to_string(),
                });
            }

            if let Some(retried) = metrics_json
                .get("retried")
                .and_then(serde_json::Value::as_u64)
            {
                daily_metrics.push(DailyMetric {
                    metric: "Retried".to_string(),
                    count: retried.to_string(),
                });
            }

            if let Some(avg_time) = metrics_json
                .get("avg_execution_time")
                .and_then(serde_json::Value::as_f64)
            {
                daily_metrics.push(DailyMetric {
                    metric: "Avg Execution Time".to_string(),
                    count: format!("{avg_time:.2}s"),
                });
            }

            let table = Table::new(daily_metrics).with(Style::rounded()).to_string();
            println!("{table}");
        }
    } else {
        println!("{}", "No metrics available for today".yellow());
        println!();
        println!("Metrics are collected automatically when tasks are executed.");
        println!("Ensure workers are running and processing tasks.");
    }

    Ok(())
}

/// Render `rows` (with `headers`) per `format` (`"table"`, `"csv"`, or
/// `"html"`) and either print to stdout or write to `output`.
///
/// When `template` is set it takes precedence over `format`: each row is
/// rendered independently via [`command_utils::apply_template`] (fields
/// keyed by header name) and the results are joined with newlines.
///
/// `chart`, when `Some`, is forwarded only to the `"html"` branch (via
/// [`command_utils::render_html_report_with_chart`]) to render an SVG bar
/// chart above the table; it is ignored by the `csv`/`table`/template
/// paths.
///
/// CSV file output goes through [`command_utils::write_csv`] (the
/// canonical CSV-file writer); CSV preview (no `--output`) and HTML both go
/// through the pure `command_utils` renderers.
pub(crate) fn emit_report(
    title: &str,
    headers: &[&str],
    rows: &[Vec<String>],
    format: &str,
    output: Option<&str>,
    template: Option<&str>,
    chart: Option<&[(String, f64)]>,
) -> anyhow::Result<()> {
    if let Some(tpl) = template {
        let rendered = rows
            .iter()
            .map(|row| {
                let fields: std::collections::HashMap<String, String> = headers
                    .iter()
                    .zip(row.iter())
                    .map(|(header, value)| ((*header).to_string(), value.clone()))
                    .collect();
                command_utils::apply_template(tpl, &fields)
            })
            .collect::<Vec<_>>()
            .join("\n");
        return write_or_print(title, "template", &rendered, output);
    }

    match format.to_lowercase().as_str() {
        "csv" => match output {
            Some(path) => {
                command_utils::write_csv(path, headers, rows)?;
                println!(
                    "{}",
                    format!("✓ {title} exported to '{path}' (csv)")
                        .green()
                        .bold()
                );
                Ok(())
            }
            None => {
                let rendered = command_utils::csv_to_string(headers, rows)?;
                print!("{rendered}");
                Ok(())
            }
        },
        "html" => {
            let rendered =
                command_utils::render_html_report_with_chart(title, rows, headers, chart);
            write_or_print(title, "html", &rendered, output)
        }
        "table" => {
            let rendered = command_utils::render_table_string(headers, rows);
            write_or_print(title, "table", &rendered, output)
        }
        other => {
            anyhow::bail!("Unknown report format '{other}'. Supported formats: table, csv, html")
        }
    }
}

/// Write `content` to `output` if given (with a confirmation message),
/// otherwise print it to stdout.
pub(crate) fn write_or_print(
    title: &str,
    kind: &str,
    content: &str,
    output: Option<&str>,
) -> anyhow::Result<()> {
    match output {
        Some(path) => {
            std::fs::write(path, content)?;
            println!(
                "{}",
                format!("✓ {title} exported to '{path}' ({kind})")
                    .green()
                    .bold()
            );
        }
        None => println!("{content}"),
    }
    Ok(())
}

/// Generate the daily execution report with a selectable output format
/// (`table`, `csv`, `html`) and optional file output / custom template.
pub async fn report_daily_formatted(
    broker_url: &str,
    queue: &str,
    format: &str,
    output: Option<&str>,
    template: Option<&str>,
) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client
        .get_multiplexed_async_connection_with_config(&crate::pool::async_connection_config())
        .await?;

    let now = Utc::now();
    let today_key = format!("celers:metrics:{}:daily:{}", queue, now.format("%Y-%m-%d"));
    let metrics: Option<String> = redis::cmd("GET")
        .arg(&today_key)
        .query_async(&mut conn)
        .await?;

    let Some(metrics_str) = metrics else {
        println!("{}", "No metrics available for today".yellow());
        println!();
        println!("Metrics are collected automatically when tasks are executed.");
        println!("Ensure workers are running and processing tasks.");
        return Ok(());
    };

    let Ok(metrics_json) = serde_json::from_str::<serde_json::Value>(&metrics_str) else {
        println!("{}", "⚠ Could not parse today's metrics".yellow());
        return Ok(());
    };

    let headers = ["Metric", "Value"];
    let rows = daily_metric_rows(&metrics_json);
    let title = format!("Daily Execution Report ({})", now.format("%Y-%m-%d"));
    emit_report(&title, &headers, &rows, format, output, template, None)
}

/// Pure helper: turn a daily-metrics JSON blob into `["Metric", "Value"]` rows.
pub(crate) fn daily_metric_rows(metrics_json: &serde_json::Value) -> Vec<Vec<String>> {
    let mut rows = Vec::new();
    if let Some(total) = metrics_json
        .get("total_tasks")
        .and_then(serde_json::Value::as_u64)
    {
        rows.push(vec!["Total Tasks".to_string(), total.to_string()]);
    }
    if let Some(succeeded) = metrics_json
        .get("succeeded")
        .and_then(serde_json::Value::as_u64)
    {
        rows.push(vec!["Succeeded".to_string(), succeeded.to_string()]);
    }
    if let Some(failed) = metrics_json
        .get("failed")
        .and_then(serde_json::Value::as_u64)
    {
        rows.push(vec!["Failed".to_string(), failed.to_string()]);
    }
    if let Some(retried) = metrics_json
        .get("retried")
        .and_then(serde_json::Value::as_u64)
    {
        rows.push(vec!["Retried".to_string(), retried.to_string()]);
    }
    if let Some(avg_time) = metrics_json
        .get("avg_execution_time")
        .and_then(serde_json::Value::as_f64)
    {
        rows.push(vec![
            "Avg Execution Time".to_string(),
            format!("{avg_time:.2}s"),
        ]);
    }
    rows
}

/// Generate the weekly statistics report with a selectable output format
/// (`table`, `csv`, `html`) and optional file output / custom template.
pub async fn report_weekly_formatted(
    broker_url: &str,
    queue: &str,
    format: &str,
    output: Option<&str>,
    template: Option<&str>,
) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client
        .get_multiplexed_async_connection_with_config(&crate::pool::async_connection_config())
        .await?;

    let now = Utc::now();
    let week_start = now - chrono::Duration::days(7);

    let mut total_tasks = 0u64;
    let mut total_succeeded = 0u64;
    let mut total_failed = 0u64;
    let mut total_retried = 0u64;

    for day_offset in 0..7 {
        let day = now - chrono::Duration::days(day_offset);
        let day_key = format!("celers:metrics:{}:daily:{}", queue, day.format("%Y-%m-%d"));
        let metrics: Option<String> = redis::cmd("GET")
            .arg(&day_key)
            .query_async(&mut conn)
            .await?;
        if let Some(metrics_str) = metrics {
            if let Ok(metrics_json) = serde_json::from_str::<serde_json::Value>(&metrics_str) {
                accumulate_daily_totals(
                    &metrics_json,
                    &mut total_tasks,
                    &mut total_succeeded,
                    &mut total_failed,
                    &mut total_retried,
                );
            }
        }
    }

    let rows = weekly_metric_rows(total_tasks, total_succeeded, total_failed, total_retried);
    let headers = ["Metric", "Count", "Percentage"];
    let title = format!(
        "Weekly Statistics Report ({} to {})",
        week_start.format("%Y-%m-%d"),
        now.format("%Y-%m-%d")
    );
    emit_report(&title, &headers, &rows, format, output, template, None)
}

/// Pure helper: accumulate one day's metrics JSON into running weekly totals.
pub(crate) fn accumulate_daily_totals(
    metrics_json: &serde_json::Value,
    total_tasks: &mut u64,
    total_succeeded: &mut u64,
    total_failed: &mut u64,
    total_retried: &mut u64,
) {
    if let Some(v) = metrics_json
        .get("total_tasks")
        .and_then(serde_json::Value::as_u64)
    {
        *total_tasks += v;
    }
    if let Some(v) = metrics_json
        .get("succeeded")
        .and_then(serde_json::Value::as_u64)
    {
        *total_succeeded += v;
    }
    if let Some(v) = metrics_json
        .get("failed")
        .and_then(serde_json::Value::as_u64)
    {
        *total_failed += v;
    }
    if let Some(v) = metrics_json
        .get("retried")
        .and_then(serde_json::Value::as_u64)
    {
        *total_retried += v;
    }
}

/// Pure helper: turn weekly totals into `["Metric", "Count", "Percentage"]` rows.
pub(crate) fn weekly_metric_rows(
    total: u64,
    succeeded: u64,
    failed: u64,
    retried: u64,
) -> Vec<Vec<String>> {
    let success_rate = if total > 0 {
        (succeeded as f64 / total as f64) * 100.0
    } else {
        0.0
    };
    let failure_rate = if total > 0 {
        (failed as f64 / total as f64) * 100.0
    } else {
        0.0
    };
    vec![
        vec![
            "Total Tasks".to_string(),
            total.to_string(),
            "100%".to_string(),
        ],
        vec![
            "Succeeded".to_string(),
            succeeded.to_string(),
            format!("{success_rate:.1}%"),
        ],
        vec![
            "Failed".to_string(),
            failed.to_string(),
            format!("{failure_rate:.1}%"),
        ],
        vec!["Retried".to_string(), retried.to_string(), "-".to_string()],
    ]
}

/// Export task execution history for `queue` over the last `days` days
/// (one row per day that has recorded metrics), with a selectable output
/// format and optional file output / custom template.
pub async fn report_history(
    broker_url: &str,
    queue: &str,
    days: u32,
    format: &str,
    output: Option<&str>,
    template: Option<&str>,
) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client
        .get_multiplexed_async_connection_with_config(&crate::pool::async_connection_config())
        .await?;
    let now = Utc::now();
    let days = days.max(1);

    let mut records = Vec::new();
    for day_offset in 0..days {
        let day = now - chrono::Duration::days(i64::from(day_offset));
        let date = day.format("%Y-%m-%d").to_string();
        let day_key = format!("celers:metrics:{queue}:daily:{date}");
        let metrics: Option<String> = redis::cmd("GET")
            .arg(&day_key)
            .query_async(&mut conn)
            .await?;
        if let Some(metrics_str) = metrics {
            if let Ok(metrics_json) = serde_json::from_str::<serde_json::Value>(&metrics_str) {
                records.push(history_record_from_json(queue, &date, &metrics_json));
            }
        }
    }
    records.sort_by(|a, b| a.date.cmp(&b.date));

    if records.is_empty() {
        println!(
            "{}",
            format!("No history available for queue '{queue}' in the last {days} day(s)").yellow()
        );
        return Ok(());
    }

    let headers = command_utils::HISTORY_CSV_HEADERS;
    let rows = command_utils::format_history_csv(&records);
    let chart_series: Vec<(String, f64)> = records
        .iter()
        .map(|r| (r.date.clone(), r.total as f64))
        .collect();
    let title = format!("Task Execution History: {queue} (last {days}d)");
    emit_report(
        &title,
        &headers,
        &rows,
        format,
        output,
        template,
        Some(&chart_series),
    )
}

/// Pure helper: build a [`command_utils::HistoryRecord`] from a daily-metrics JSON blob.
pub(crate) fn history_record_from_json(
    queue: &str,
    date: &str,
    metrics_json: &serde_json::Value,
) -> command_utils::HistoryRecord {
    command_utils::HistoryRecord {
        date: date.to_string(),
        queue: queue.to_string(),
        total: metrics_json
            .get("total_tasks")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
        succeeded: metrics_json
            .get("succeeded")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
        failed: metrics_json
            .get("failed")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
        retried: metrics_json
            .get("retried")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0),
        avg_execution_time: metrics_json
            .get("avg_execution_time")
            .and_then(serde_json::Value::as_f64),
    }
}

/// Scan Redis for all `celers:worker:*:heartbeat` keys (cursor-based
/// `SCAN`, safe for large keyspaces -- unlike a single blocking `KEYS`
/// call). Shared by [`report_workers`] and [`crate::commands::profile_worker`].
pub(crate) async fn scan_worker_heartbeat_keys(
    conn: &mut redis::aio::MultiplexedConnection,
) -> anyhow::Result<Vec<String>> {
    let mut cursor = 0u64;
    let mut keys = Vec::new();
    loop {
        let (new_cursor, batch): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg("celers:worker:*:heartbeat")
            .arg("COUNT")
            .arg(100)
            .query_async(conn)
            .await?;
        keys.extend(batch);
        cursor = new_cursor;
        if cursor == 0 {
            break;
        }
    }
    Ok(keys)
}

/// Export worker statistics (status, processed/failed counts, last
/// heartbeat) for every currently-known worker, with a selectable output
/// format and optional file output / custom template.
pub async fn report_workers(
    broker_url: &str,
    format: &str,
    output: Option<&str>,
    template: Option<&str>,
) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client
        .get_multiplexed_async_connection_with_config(&crate::pool::async_connection_config())
        .await?;

    let heartbeat_keys = scan_worker_heartbeat_keys(&mut conn).await?;

    let mut records = Vec::new();
    for key in &heartbeat_keys {
        let Some(worker_id) = extract_worker_id(key) else {
            continue;
        };
        let worker_id = worker_id.to_string();

        let heartbeat: Option<String> = redis::cmd("GET").arg(key).query_async(&mut conn).await?;
        let stats: Option<String> = redis::cmd("GET")
            .arg(format!("celers:worker:{worker_id}:stats"))
            .query_async(&mut conn)
            .await?;
        let paused: bool = redis::cmd("EXISTS")
            .arg(format!("celers:worker:{worker_id}:paused"))
            .query_async(&mut conn)
            .await?;
        let draining: bool = redis::cmd("EXISTS")
            .arg(format!("celers:worker:{worker_id}:draining"))
            .query_async(&mut conn)
            .await?;

        let (processed, failed) = stats
            .as_deref()
            .and_then(|s| serde_json::from_str::<serde_json::Value>(s).ok())
            .map(|json| {
                (
                    json.get("tasks_processed")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or(0),
                    json.get("tasks_failed")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or(0),
                )
            })
            .unwrap_or((0, 0));

        records.push(command_utils::WorkerStatRecord {
            worker_id,
            status: worker_status_label(paused, draining),
            processed,
            failed,
            last_heartbeat: heartbeat.unwrap_or_else(|| "N/A".to_string()),
        });
    }

    records.sort_by(|a, b| a.worker_id.cmp(&b.worker_id));

    if records.is_empty() {
        println!("{}", "No active workers found".yellow());
        return Ok(());
    }

    let headers = command_utils::WORKER_CSV_HEADERS;
    let rows = command_utils::format_worker_csv(&records);
    let chart_series: Vec<(String, f64)> = records
        .iter()
        .map(|r| (r.worker_id.clone(), r.processed as f64))
        .collect();
    let title = format!("Worker Statistics Report ({} worker(s))", records.len());
    emit_report(
        &title,
        &headers,
        &rows,
        format,
        output,
        template,
        Some(&chart_series),
    )
}

/// Pure helper: extract the worker id from a `celers:worker:<id>:heartbeat` key.
pub(crate) fn extract_worker_id(heartbeat_key: &str) -> Option<&str> {
    heartbeat_key
        .strip_prefix("celers:worker:")
        .and_then(|rest| rest.strip_suffix(":heartbeat"))
}

/// Pure helper: derive a human worker status label from pause/drain flags.
pub(crate) fn worker_status_label(paused: bool, draining: bool) -> String {
    match (paused, draining) {
        (true, true) => "Paused+Draining".to_string(),
        (true, false) => "Paused".to_string(),
        (false, true) => "Draining".to_string(),
        (false, false) => "Active".to_string(),
    }
}

/// Export per-queue depth/health metrics for every discovered primary
/// queue, with a selectable output format and optional file output /
/// custom template.
pub async fn report_queues(
    broker_url: &str,
    format: &str,
    output: Option<&str>,
    template: Option<&str>,
) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client
        .get_multiplexed_async_connection_with_config(&crate::pool::async_connection_config())
        .await?;

    // `MATCH *` (not `celers:*`): real `RedisBroker` queue keys carry no
    // shared prefix at all, so a `celers:*`-scoped scan used to match zero
    // of them against a live, non-empty broker (idx 331/337; see
    // `base_queue_name`'s docs for the full rationale).
    let mut cursor = 0u64;
    let mut keys = Vec::new();
    loop {
        let (new_cursor, batch): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg("*")
            .arg("COUNT")
            .arg(100)
            .query_async(&mut conn)
            .await?;
        keys.extend(batch);
        cursor = new_cursor;
        if cursor == 0 {
            break;
        }
    }

    let mut queue_names: Vec<String> = keys.iter().filter_map(|k| base_queue_name(k)).collect();
    queue_names.sort();
    queue_names.dedup();

    let mut records = Vec::new();
    for queue in &queue_names {
        let queue_key = crate::keys::main(queue);
        let queue_type: String = redis::cmd("TYPE")
            .arg(&queue_key)
            .query_async(&mut conn)
            .await?;

        // `base_queue_name` filters by prefix/suffix, not by type -- narrow
        // further to the only two Redis types `RedisBroker` ever creates a
        // primary queue key as, so a stray non-queue key elsewhere in the
        // keyspace that happened to survive the name filter is dropped here
        // rather than reported as an empty "Unknown" queue.
        if queue_type != "list" && queue_type != "zset" {
            continue;
        }

        let pending: u64 = match queue_type.as_str() {
            "list" => {
                redis::cmd("LLEN")
                    .arg(&queue_key)
                    .query_async(&mut conn)
                    .await?
            }
            "zset" => {
                redis::cmd("ZCARD")
                    .arg(&queue_key)
                    .query_async(&mut conn)
                    .await?
            }
            _ => 0,
        };
        let processing: u64 = redis::cmd("LLEN")
            .arg(crate::keys::processing(queue))
            .query_async(&mut conn)
            .await
            .unwrap_or(0);
        let dlq: u64 = redis::cmd("LLEN")
            .arg(crate::keys::dlq(queue))
            .query_async(&mut conn)
            .await
            .unwrap_or(0);
        let delayed: u64 = redis::cmd("ZCARD")
            .arg(crate::keys::delayed(queue))
            .query_async(&mut conn)
            .await
            .unwrap_or(0);

        records.push(command_utils::QueueMetricRecord {
            queue: queue.clone(),
            queue_type: queue_type_label(&queue_type),
            pending,
            processing,
            dlq,
            delayed,
        });
    }

    if records.is_empty() {
        println!("{}", "No queues found".yellow());
        return Ok(());
    }

    let headers = command_utils::QUEUE_CSV_HEADERS;
    let rows = command_utils::format_queue_csv(&records);
    let chart_series: Vec<(String, f64)> = records
        .iter()
        .map(|r| (r.queue.clone(), r.pending as f64))
        .collect();
    let title = format!("Queue Metrics Report ({} queue(s))", records.len());
    emit_report(
        &title,
        &headers,
        &rows,
        format,
        output,
        template,
        Some(&chart_series),
    )
}

/// Pure helper: classify a Redis `TYPE` result into a human queue-type label.
pub(crate) fn queue_type_label(redis_type: &str) -> String {
    match redis_type {
        "list" => "FIFO".to_string(),
        "zset" => "Priority".to_string(),
        other => format!("Unknown ({other})"),
    }
}

/// Pure helper: extract the base queue name from a scanned Redis key,
/// filtering out this CLI's own `celers:`-namespaced non-queue keys
/// (worker/task/metrics/schedule/alias, see
/// [`crate::keys::is_reserved_namespace_key`]) and queue-derived suffix keys
/// (`:dlq`, `:processing`, `:delayed`, `:paused`, `:drain`, see
/// [`crate::keys::has_queue_family_suffix`]), leaving only primary queue
/// keys.
///
/// Unlike every other namespace this CLI writes into Redis -- all of which
/// are deliberately `celers:`-prefixed -- a real `RedisBroker` queue-family
/// key carries no shared prefix at all (see [`crate::keys`]'s module docs):
/// the bare queue name *is* the key. So this used to (incorrectly) require
/// a `celers:` prefix and strip it off before treating the rest as a queue
/// name (idx 331/337); scanning `celers:*` for that scheme matched nothing
/// for a real, non-prefixed queue, silently making every caller of this
/// function see zero queues against a live, non-empty broker. The fix is to
/// treat the *absence* of a `celers:` prefix (and of a recognized
/// queue-family suffix) as the positive signal for "this is a queue key",
/// since that is the one property real queue keys and every other namespace
/// this CLI owns can always be told apart by.
///
/// The suffix check runs *before* the namespace check, not after: the
/// configured *default* queue name is literally `"celers"` (see
/// `Config::default_config`), so that queue's own sibling keys
/// (`celers:dlq`, `celers:processing`, `celers:delayed`) would otherwise be
/// misclassified as `celers:`-namespaced bookkeeping instead of as this
/// queue's own buckets -- silently hiding the default queue's DLQ/delayed
/// rows from every caller.
///
/// This is necessarily a heuristic rather than a guarantee: `RedisBroker`
/// reserves no namespace of its own, so a key written by something entirely
/// unrelated to `celers` (sharing the same Redis logical DB) that happens to
/// avoid both the reserved namespaces and every queue-family suffix cannot
/// be distinguished from a real queue by name alone. Callers that scan the
/// keyspace additionally filter by Redis `TYPE` (`list`/`zset`, the only
/// types `RedisBroker` ever creates a queue-family key as) to narrow this
/// further.
pub(crate) fn base_queue_name(key: &str) -> Option<String> {
    if key.is_empty() || crate::keys::has_queue_family_suffix(key) {
        return None;
    }
    if crate::keys::is_reserved_namespace_key(key) {
        return None;
    }
    Some(key.to_string())
}
