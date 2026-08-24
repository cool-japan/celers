//! Auto-generated module
//!
//! 🤖 Generated with [SplitRS](https://github.com/cool-japan/splitrs)

use celers_metrics::history::MetricHistory;

use super::profile::{
    parse_info_field, summarize_metric_history, trend_label, worker_performance_row,
};
use super::report::{
    accumulate_daily_totals, base_queue_name, daily_metric_rows, emit_report, extract_worker_id,
    history_record_from_json, queue_type_label, weekly_metric_rows, worker_status_label,
};

#[cfg(test)]
mod tests_report {
    use super::*;

    fn unique_temp_path(tag: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!(
            "celers_cli_monitoring_test_{}_{}_{tag}",
            std::process::id(),
            nanos
        ))
    }

    #[test]
    fn test_daily_metric_rows() {
        let json = serde_json::json!({
            "total_tasks": 10,
            "succeeded": 8,
            "failed": 2,
            "retried": 1,
            "avg_execution_time": 1.25,
        });
        let rows = daily_metric_rows(&json);
        assert_eq!(rows.len(), 5);
        assert_eq!(rows[0], vec!["Total Tasks".to_string(), "10".to_string()]);
        assert_eq!(
            rows[4],
            vec!["Avg Execution Time".to_string(), "1.25s".to_string()]
        );
    }

    #[test]
    fn test_daily_metric_rows_partial() {
        let json = serde_json::json!({ "total_tasks": 3 });
        let rows = daily_metric_rows(&json);
        assert_eq!(rows, vec![vec!["Total Tasks".to_string(), "3".to_string()]]);
    }

    #[test]
    fn test_accumulate_and_weekly_rows() {
        let mut total = 0u64;
        let mut succeeded = 0u64;
        let mut failed = 0u64;
        let mut retried = 0u64;
        let day1 =
            serde_json::json!({"total_tasks": 10, "succeeded": 8, "failed": 2, "retried": 1});
        let day2 = serde_json::json!({"total_tasks": 5, "succeeded": 5, "failed": 0, "retried": 0});
        accumulate_daily_totals(&day1, &mut total, &mut succeeded, &mut failed, &mut retried);
        accumulate_daily_totals(&day2, &mut total, &mut succeeded, &mut failed, &mut retried);
        assert_eq!((total, succeeded, failed, retried), (15, 13, 2, 1));

        let rows = weekly_metric_rows(total, succeeded, failed, retried);
        assert_eq!(
            rows[0],
            vec![
                "Total Tasks".to_string(),
                "15".to_string(),
                "100%".to_string()
            ]
        );
        assert_eq!(rows[1][2], "86.7%");
        assert_eq!(rows[2][2], "13.3%");
    }

    #[test]
    fn test_weekly_metric_rows_zero_total() {
        let rows = weekly_metric_rows(0, 0, 0, 0);
        assert_eq!(rows[1][2], "0.0%");
        assert_eq!(rows[2][2], "0.0%");
    }

    #[test]
    fn test_history_record_from_json() {
        let json = serde_json::json!({"total_tasks": 10, "succeeded": 8, "failed": 2, "retried": 0, "avg_execution_time": 2.5});
        let record = history_record_from_json("myqueue", "2026-07-01", &json);
        assert_eq!(record.queue, "myqueue");
        assert_eq!(record.date, "2026-07-01");
        assert_eq!(record.total, 10);
        assert_eq!(record.avg_execution_time, Some(2.5));
    }

    #[test]
    fn test_history_record_from_json_missing_fields_default_zero() {
        let json = serde_json::json!({});
        let record = history_record_from_json("q", "2026-01-01", &json);
        assert_eq!(record.total, 0);
        assert_eq!(record.avg_execution_time, None);
    }

    #[test]
    fn test_extract_worker_id() {
        assert_eq!(
            extract_worker_id("celers:worker:w-1:heartbeat"),
            Some("w-1")
        );
        assert_eq!(extract_worker_id("celers:worker:w-1:stats"), None);
        assert_eq!(extract_worker_id("not-a-key"), None);
    }

    #[test]
    fn test_worker_status_label() {
        assert_eq!(worker_status_label(false, false), "Active");
        assert_eq!(worker_status_label(true, false), "Paused");
        assert_eq!(worker_status_label(false, true), "Draining");
        assert_eq!(worker_status_label(true, true), "Paused+Draining");
    }

    #[test]
    fn test_queue_type_label() {
        assert_eq!(queue_type_label("list"), "FIFO");
        assert_eq!(queue_type_label("zset"), "Priority");
        assert_eq!(queue_type_label("none"), "Unknown (none)");
    }

    #[test]
    fn test_base_queue_name_filters_non_queue_keys() {
        // Regression test for idx 331/337: a real `RedisBroker` queue-family
        // key carries no shared prefix at all (see `crate::keys`'s module
        // docs) -- a bare key is the primary queue, unlike every other
        // namespace this CLI writes into Redis, which *is* `celers:`-
        // prefixed. The old version of this function (and this test)
        // encoded the opposite, incorrect assumption.
        assert_eq!(base_queue_name("default"), Some("default".to_string()));
        assert_eq!(
            base_queue_name("high-priority"),
            Some("high-priority".to_string())
        );
        // Suffixed sibling keys of the same queue family are not
        // themselves primary queues.
        assert_eq!(base_queue_name("default:dlq"), None);
        assert_eq!(base_queue_name("default:processing"), None);
        assert_eq!(base_queue_name("default:delayed"), None);
        assert_eq!(base_queue_name("default:paused"), None);
        assert_eq!(base_queue_name("default:drain"), None);
        // Every other namespace this crate owns in Redis is `celers:`-
        // prefixed, however deeply nested, so it is excluded regardless of
        // whether this function knows that exact sub-namespace by name.
        assert_eq!(base_queue_name("celers:worker:w1:heartbeat"), None);
        assert_eq!(base_queue_name("celers:task:abc:logs"), None);
        assert_eq!(
            base_queue_name("celers:metrics:default:daily:2026-07-01"),
            None
        );
        assert_eq!(base_queue_name("celers:schedule:job1"), None);
        assert_eq!(base_queue_name("celers:"), None);
        assert_eq!(base_queue_name(""), None);
        // A name that merely *contains* a colon without a reserved
        // sub-namespace prefix or a recognized queue-family suffix is
        // indistinguishable from a real queue name by this heuristic -- a
        // documented, inherent limitation (see this function's docs) rather
        // than an oversight.
        assert_eq!(
            base_queue_name("other:default"),
            Some("other:default".to_string())
        );
    }

    /// Regression test: `Config::default_config` names the default queue
    /// literally `"celers"`, so its own sibling keys (`celers:dlq`,
    /// `celers:processing`, `celers:delayed`) collide syntactically with the
    /// `celers:`-prefixed bookkeeping namespaces this function also filters
    /// out. `base_queue_name` must recognize the *default queue itself* and
    /// still fold its siblings into it rather than misclassifying either as
    /// bookkeeping -- and must not swallow a differently-named queue that
    /// merely starts with "celers:" either.
    #[test]
    fn test_base_queue_name_handles_the_default_queue_named_celers() {
        assert_eq!(base_queue_name("celers"), Some("celers".to_string()));
        assert_eq!(base_queue_name("celers:dlq"), None);
        assert_eq!(base_queue_name("celers:processing"), None);
        assert_eq!(base_queue_name("celers:delayed"), None);
        assert_eq!(base_queue_name("celers:paused"), None);
        assert_eq!(base_queue_name("celers:drain"), None);
        // A queue literally named "celers:staging" is not one of the five
        // reserved sub-namespaces and must still be recognized.
        assert_eq!(
            base_queue_name("celers:staging"),
            Some("celers:staging".to_string())
        );
    }

    #[test]
    fn test_emit_report_table_csv_html_and_template_to_file() {
        let headers = ["Name", "Count"];
        let rows = vec![
            vec!["alpha".to_string(), "1".to_string()],
            vec!["beta".to_string(), "2".to_string()],
        ];

        // table -> file
        let table_path = unique_temp_path("table.txt");
        emit_report(
            "Test Report",
            &headers,
            &rows,
            "table",
            Some(
                table_path
                    .to_str()
                    .expect("temp path should be valid UTF-8"),
            ),
            None,
            None,
        )
        .expect("table emit should succeed");
        let table_content = std::fs::read_to_string(&table_path).expect("table file should exist");
        assert!(table_content.contains("alpha"));
        let _ = std::fs::remove_file(&table_path);

        // csv -> file (goes through write_csv)
        let csv_path = unique_temp_path("data.csv");
        emit_report(
            "Test Report",
            &headers,
            &rows,
            "csv",
            Some(csv_path.to_str().expect("temp path should be valid UTF-8")),
            None,
            None,
        )
        .expect("csv emit should succeed");
        let csv_content = std::fs::read_to_string(&csv_path).expect("csv file should exist");
        assert!(csv_content.contains("Name,Count"));
        assert!(csv_content.contains("alpha,1"));
        let _ = std::fs::remove_file(&csv_path);

        // html -> file, escaped + self-contained
        let html_path = unique_temp_path("report.html");
        let html_rows = vec![vec!["<script>x</script>".to_string(), "3".to_string()]];
        emit_report(
            "Test <Report>",
            &headers,
            &html_rows,
            "html",
            Some(html_path.to_str().expect("temp path should be valid UTF-8")),
            None,
            None,
        )
        .expect("html emit should succeed");
        let html_content = std::fs::read_to_string(&html_path).expect("html file should exist");
        assert!(html_content.starts_with("<!doctype html>"));
        assert!(html_content.contains("&lt;script&gt;"));
        assert!(!html_content.contains("<script>x</script>"));
        let _ = std::fs::remove_file(&html_path);

        // template -> file, keyed by header name, missing key left as-is.
        let template_path = unique_temp_path("template.txt");
        emit_report(
            "Test Report",
            &headers,
            &rows,
            "table",
            Some(
                template_path
                    .to_str()
                    .expect("temp path should be valid UTF-8"),
            ),
            Some("{{Name}} => {{Count}} ({{missing}})"),
            None,
        )
        .expect("template emit should succeed");
        let template_content =
            std::fs::read_to_string(&template_path).expect("template file should exist");
        assert!(template_content.contains("alpha => 1 ({{missing}})"));
        assert!(template_content.contains("beta => 2 ({{missing}})"));
        let _ = std::fs::remove_file(&template_path);
    }

    #[test]
    fn test_emit_report_unknown_format_errors() {
        let headers = ["A"];
        let rows: Vec<Vec<String>> = vec![];
        let result = emit_report("Title", &headers, &rows, "yaml", None, None, None);
        assert!(result.is_err());
    }
}

#[cfg(test)]
mod tests_profile {
    use super::*;

    #[test]
    fn test_worker_performance_row() {
        let row = worker_performance_row("w1", 100, 5, 50);
        assert_eq!(row[0], "w1");
        assert_eq!(row[1], "100");
        assert_eq!(row[2], "2.0000");
        assert_eq!(row[3], "4.8%");
        assert_eq!(row[4], "50s");
    }

    #[test]
    fn test_worker_performance_row_zero_uptime() {
        let row = worker_performance_row("w2", 0, 0, 0);
        assert_eq!(row[2], "0.0000");
        assert_eq!(row[3], "0.0%");
    }

    #[test]
    fn test_parse_info_field() {
        let info = "# Memory\r\nused_memory:1048576\r\nused_memory_human:1.00M\r\nmaxmemory:0\r\n";
        assert_eq!(
            parse_info_field(info, "used_memory_human"),
            Some("1.00M".to_string())
        );
        assert_eq!(parse_info_field(info, "missing_field"), None);
    }

    #[test]
    fn test_trend_label() {
        assert_eq!(trend_label(1.0), "rising");
        assert_eq!(trend_label(-1.0), "falling");
        assert_eq!(trend_label(0.0), "flat");
    }

    #[test]
    fn test_summarize_metric_history_synthetic_series() {
        let history = MetricHistory::new(10);
        history.record_batch(&[(0, 10.0), (100, 20.0), (200, 30.0)]);

        let summary = summarize_metric_history(&history, 2);
        assert_eq!(summary.count, 3);
        assert!((summary.mean - 20.0).abs() < 1e-9);
        assert_eq!(summary.min, 10.0);
        assert_eq!(summary.max, 30.0);
        assert_eq!(summary.latest, Some(30.0));
        // Trend: (30 - 10) / (200 - 0) = 0.1 per second, rising.
        let trend = summary
            .trend_per_sample
            .expect("2+ samples should produce a trend");
        assert!((trend - 0.1).abs() < 1e-9);
        // Moving average of last 2 samples: (20 + 30) / 2 = 25.
        let moving_avg = summary
            .moving_average
            .expect("non-empty history should have a moving average");
        assert!((moving_avg - 25.0).abs() < 1e-9);

        let rows = summary.to_rows();
        assert!(rows
            .iter()
            .any(|r| r[0] == "Trend" && r[1].contains("rising")));
    }

    #[test]
    fn test_summarize_metric_history_empty() {
        let history = MetricHistory::new(10);
        let summary = summarize_metric_history(&history, 3);
        assert_eq!(summary.count, 0);
        assert_eq!(summary.min, 0.0);
        assert_eq!(summary.max, 0.0);
        assert_eq!(summary.trend_per_sample, None);
        assert_eq!(summary.latest, None);
    }
}
