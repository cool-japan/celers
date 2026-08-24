//! Performance profiling: task execution trend, worker performance analysis, resource usage tracking.
//!
//! 🤖 Generated with [SplitRS](https://github.com/cool-japan/splitrs)

use crate::command_utils;
use celers_metrics::history::MetricHistory;
use chrono::Utc;
use colored::Colorize;

use super::report::{emit_report, extract_worker_id, scan_worker_heartbeat_keys};

/// Aggregate statistics derived from a [`MetricHistory`] series, used to
/// render `analyze profile` reports.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ProfileSummary {
    pub(crate) count: usize,
    pub(crate) mean: f64,
    pub(crate) std_dev: f64,
    pub(crate) min: f64,
    pub(crate) max: f64,
    pub(crate) trend_per_sample: Option<f64>,
    pub(crate) moving_average: Option<f64>,
    pub(crate) latest: Option<f64>,
}
impl ProfileSummary {
    /// Render as `["Metric", "Value"]` rows for [`emit_report`].
    pub(crate) fn to_rows(&self) -> Vec<Vec<String>> {
        vec![
            vec!["Samples".to_string(), self.count.to_string()],
            vec!["Mean".to_string(), format!("{:.3}", self.mean)],
            vec!["Std Dev".to_string(), format!("{:.3}", self.std_dev)],
            vec!["Min".to_string(), format!("{:.3}", self.min)],
            vec!["Max".to_string(), format!("{:.3}", self.max)],
            vec![
                "Trend".to_string(),
                self.trend_per_sample.map_or_else(
                    || "n/a".to_string(),
                    |t| format!("{t:.4}/s ({})", trend_label(t)),
                ),
            ],
            vec![
                "Moving Average".to_string(),
                self.moving_average
                    .map_or_else(|| "n/a".to_string(), |m| format!("{m:.3}")),
            ],
            vec![
                "Latest".to_string(),
                self.latest
                    .map_or_else(|| "n/a".to_string(), |l| format!("{l:.3}")),
            ],
        ]
    }
}
/// Pure helper: describe a trend value (rate of change per second) in words.
pub(crate) fn trend_label(trend: f64) -> &'static str {
    if trend > 1e-9 {
        "rising"
    } else if trend < -1e-9 {
        "falling"
    } else {
        "flat"
    }
}

/// Summarize a [`MetricHistory`] into aggregate profiling statistics: mean,
/// std-dev, min/max, trend (rate of change), and a windowed moving average.
///
/// Pure with respect to `history`'s already-recorded samples (no I/O).
pub(crate) fn summarize_metric_history(history: &MetricHistory, window: usize) -> ProfileSummary {
    let snapshot = history.snapshot();
    ProfileSummary {
        count: snapshot.count,
        mean: snapshot.mean,
        std_dev: snapshot.std_dev,
        min: history.min().unwrap_or(0.0),
        max: history.max().unwrap_or(0.0),
        trend_per_sample: snapshot.trend,
        moving_average: history.moving_average_window(window),
        latest: snapshot.latest,
    }
}

/// Profile task-execution time for `queue` over the last `days` days:
/// fetches each day's `avg_execution_time` from the daily-metrics Redis
/// keys (the same keys [`crate::commands::report_history`] reads) into a [`MetricHistory`],
/// then reports trend/moving-average/min/max statistics.
pub async fn profile_task(
    broker_url: &str,
    queue: &str,
    days: u32,
    format: &str,
    output: Option<&str>,
) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;
    let now = Utc::now();
    let days = days.max(1);

    let history = MetricHistory::new(days as usize);
    let mut samples = Vec::new();

    for day_offset in (0..days).rev() {
        let day = now - chrono::Duration::days(i64::from(day_offset));
        let day_key = format!("celers:metrics:{}:daily:{}", queue, day.format("%Y-%m-%d"));
        let metrics: Option<String> = redis::cmd("GET")
            .arg(&day_key)
            .query_async(&mut conn)
            .await?;
        if let Some(metrics_str) = metrics {
            if let Ok(metrics_json) = serde_json::from_str::<serde_json::Value>(&metrics_str) {
                if let Some(avg_time) = metrics_json
                    .get("avg_execution_time")
                    .and_then(serde_json::Value::as_f64)
                {
                    let timestamp = u64::try_from(day.timestamp()).unwrap_or(0);
                    samples.push((timestamp, avg_time));
                }
            }
        }
    }

    if samples.is_empty() {
        println!(
            "{}",
            format!(
                "No execution-time metrics available for queue '{queue}' in the last {days} day(s)"
            )
            .yellow()
        );
        return Ok(());
    }

    history.record_batch(&samples);
    let summary = summarize_metric_history(&history, 3);

    let headers = ["Metric", "Value"];
    let rows = summary.to_rows();
    let title = format!("Task Execution Profile: {queue} (last {days}d, avg execution time)");
    emit_report(&title, &headers, &rows, format, output, None, None)
}

/// Analyze per-worker performance: either a single worker's current
/// throughput/failure-rate (if `worker_id` is given) or a ranked
/// comparison across all active workers (busiest first) when it is not.
pub async fn profile_worker(
    broker_url: &str,
    worker_id: Option<&str>,
    format: &str,
    output: Option<&str>,
) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    let worker_ids: Vec<String> = if let Some(id) = worker_id {
        vec![id.to_string()]
    } else {
        let heartbeat_keys = scan_worker_heartbeat_keys(&mut conn).await?;
        heartbeat_keys
            .iter()
            .filter_map(|k| extract_worker_id(k).map(str::to_string))
            .collect()
    };

    let mut rows = Vec::new();
    for id in &worker_ids {
        let stats: Option<String> = redis::cmd("GET")
            .arg(format!("celers:worker:{id}:stats"))
            .query_async(&mut conn)
            .await?;
        let Some(stats_str) = stats else { continue };
        let Ok(stats_json) = serde_json::from_str::<serde_json::Value>(&stats_str) else {
            continue;
        };

        let processed = stats_json
            .get("tasks_processed")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let failed = stats_json
            .get("tasks_failed")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let uptime = stats_json
            .get("uptime_seconds")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);

        rows.push(worker_performance_row(id, processed, failed, uptime));
    }

    if rows.is_empty() {
        println!("{}", "No worker statistics available".yellow());
        return Ok(());
    }

    // Busiest workers (most processed) first.
    rows.sort_by(|a, b| {
        let pa: u64 = a[1].parse().unwrap_or(0);
        let pb: u64 = b[1].parse().unwrap_or(0);
        pb.cmp(&pa)
    });

    let headers = [
        "Worker ID",
        "Processed",
        "Throughput (tasks/s)",
        "Failure Rate",
        "Uptime",
    ];
    let title = format!("Worker Performance Analysis ({} worker(s))", rows.len());
    emit_report(&title, &headers, &rows, format, output, None, None)
}

/// Pure helper: compute a single worker's performance row (throughput,
/// failure rate, human-readable uptime) from its raw stats.
pub(crate) fn worker_performance_row(
    worker_id: &str,
    processed: u64,
    failed: u64,
    uptime_seconds: u64,
) -> Vec<String> {
    let throughput = if uptime_seconds > 0 {
        processed as f64 / uptime_seconds as f64
    } else {
        0.0
    };
    let attempted = processed + failed;
    let failure_rate = if attempted > 0 {
        (failed as f64 / attempted as f64) * 100.0
    } else {
        0.0
    };
    vec![
        worker_id.to_string(),
        processed.to_string(),
        format!("{throughput:.4}"),
        format!("{failure_rate:.1}%"),
        command_utils::format_duration(uptime_seconds),
    ]
}

/// Track resource usage for `queue`: current queue depth / worker count /
/// DLQ size / broker memory (the same reads as [`crate::commands::analyze_bottlenecks`])
/// plus a [`MetricHistory`]-driven load trend built from `days` days of
/// daily task-volume metrics.
pub async fn profile_resources(
    broker_url: &str,
    queue: &str,
    days: u32,
    format: &str,
    output: Option<&str>,
) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;
    let now = Utc::now();
    let days = days.max(1);

    // Current snapshot (same reads as `analyze_bottlenecks`).
    let queue_key = format!("celers:{queue}");
    let queue_type: String = redis::cmd("TYPE")
        .arg(&queue_key)
        .query_async(&mut conn)
        .await?;
    let queue_size: u64 = match queue_type.as_str() {
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
    let worker_keys = scan_worker_heartbeat_keys(&mut conn).await?;
    let dlq_size: u64 = redis::cmd("LLEN")
        .arg(format!("{queue_key}:dlq"))
        .query_async(&mut conn)
        .await
        .unwrap_or(0);

    let used_memory: Option<String> = match redis::cmd("INFO")
        .arg("memory")
        .query_async::<String>(&mut conn)
        .await
    {
        Ok(info) => parse_info_field(&info, "used_memory_human"),
        Err(_) => None,
    };

    // Historical load trend (daily task volume).
    let history = MetricHistory::new(days as usize);
    let mut samples = Vec::new();
    for day_offset in (0..days).rev() {
        let day = now - chrono::Duration::days(i64::from(day_offset));
        let day_key = format!("celers:metrics:{}:daily:{}", queue, day.format("%Y-%m-%d"));
        let metrics: Option<String> = redis::cmd("GET")
            .arg(&day_key)
            .query_async(&mut conn)
            .await?;
        if let Some(metrics_str) = metrics {
            if let Ok(metrics_json) = serde_json::from_str::<serde_json::Value>(&metrics_str) {
                if let Some(total) = metrics_json
                    .get("total_tasks")
                    .and_then(serde_json::Value::as_u64)
                {
                    let timestamp = u64::try_from(day.timestamp()).unwrap_or(0);
                    samples.push((timestamp, total as f64));
                }
            }
        }
    }
    history.record_batch(&samples);
    let summary = summarize_metric_history(&history, 3);

    let mut rows = vec![
        vec!["Queue Depth (current)".to_string(), queue_size.to_string()],
        vec!["Active Workers".to_string(), worker_keys.len().to_string()],
        vec!["DLQ Size (current)".to_string(), dlq_size.to_string()],
        vec![
            "Broker Memory".to_string(),
            used_memory.unwrap_or_else(|| "n/a".to_string()),
        ],
    ];
    rows.extend(summary.to_rows().into_iter().map(|mut r| {
        r[0] = format!("Daily Volume {}", r[0]);
        r
    }));

    let headers = ["Metric", "Value"];
    let title = format!("Resource Usage Tracking: {queue} (last {days}d)");
    emit_report(&title, &headers, &rows, format, output, None, None)
}

/// Pure helper: extract a `key:value` field from Redis `INFO` output text.
pub(crate) fn parse_info_field(info: &str, field: &str) -> Option<String> {
    let prefix = format!("{field}:");
    info.lines()
        .find_map(|line| line.strip_prefix(prefix.as_str()))
        .map(|value| value.trim().to_string())
}
