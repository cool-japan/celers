//! Metrics export for Prometheus and StatsD
//!
//! This module provides utilities for exporting AMQP broker metrics to popular
//! monitoring systems like Prometheus and StatsD.
//!
//! # Supported Formats
//!
//! - **Prometheus**: Text-based exposition format
//! - **StatsD**: UDP-based metrics protocol
//! - **JSON**: Structured metrics export
//!
//! # Examples
//!
//! ## Prometheus Export
//!
//! ```
//! use celers_broker_amqp::metrics_export::{MetricsCollector, PrometheusExporter};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let collector = MetricsCollector::new();
//! collector.record_message_published("high_priority", 1);
//! collector.record_message_consumed("high_priority", 1);
//!
//! let exporter = PrometheusExporter::new("amqp_broker");
//! let prometheus_text = exporter.export(&collector).await;
//! println!("{}", prometheus_text);
//! # Ok(())
//! # }
//! ```
//!
//! ## StatsD Export
//!
//! ```
//! use celers_broker_amqp::metrics_export::{MetricsCollector, StatsDExporter};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let collector = MetricsCollector::new();
//! collector.record_message_published("high_priority", 5);
//!
//! let exporter = StatsDExporter::new("amqp_broker", "localhost:8125");
//! let statsd_metrics = exporter.format(&collector).await;
//! for metric in statsd_metrics {
//!     println!("{}", metric);
//! }
//! # Ok(())
//! # }
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Metrics collector for AMQP broker
#[derive(Debug, Clone)]
pub struct MetricsCollector {
    state: Arc<Mutex<MetricsState>>,
}

#[derive(Debug, Default)]
struct MetricsState {
    /// Messages published by queue
    published: HashMap<String, u64>,
    /// Messages consumed by queue
    consumed: HashMap<String, u64>,
    /// Messages acknowledged by queue
    acknowledged: HashMap<String, u64>,
    /// Messages rejected by queue
    rejected: HashMap<String, u64>,
    /// Connection count
    connections: u64,
    /// Channel count
    channels: u64,
    /// Error count by type
    errors: HashMap<String, u64>,
}

impl MetricsCollector {
    /// Create a new metrics collector
    pub fn new() -> Self {
        Self {
            state: Arc::new(Mutex::new(MetricsState::default())),
        }
    }

    /// Record message published
    pub fn record_message_published(&self, queue: impl Into<String>, count: u64) {
        let queue = queue.into();
        let state = self.state.clone();
        tokio::spawn(async move {
            let mut state = state.lock().await;
            *state.published.entry(queue).or_insert(0) += count;
        });
    }

    /// Record message consumed
    pub fn record_message_consumed(&self, queue: impl Into<String>, count: u64) {
        let queue = queue.into();
        let state = self.state.clone();
        tokio::spawn(async move {
            let mut state = state.lock().await;
            *state.consumed.entry(queue).or_insert(0) += count;
        });
    }

    /// Record message acknowledged
    pub fn record_message_acknowledged(&self, queue: impl Into<String>, count: u64) {
        let queue = queue.into();
        let state = self.state.clone();
        tokio::spawn(async move {
            let mut state = state.lock().await;
            *state.acknowledged.entry(queue).or_insert(0) += count;
        });
    }

    /// Record message rejected
    pub fn record_message_rejected(&self, queue: impl Into<String>, count: u64) {
        let queue = queue.into();
        let state = self.state.clone();
        tokio::spawn(async move {
            let mut state = state.lock().await;
            *state.rejected.entry(queue).or_insert(0) += count;
        });
    }

    /// Set connection count
    pub fn set_connections(&self, count: u64) {
        let state = self.state.clone();
        tokio::spawn(async move {
            let mut state = state.lock().await;
            state.connections = count;
        });
    }

    /// Set channel count
    pub fn set_channels(&self, count: u64) {
        let state = self.state.clone();
        tokio::spawn(async move {
            let mut state = state.lock().await;
            state.channels = count;
        });
    }

    /// Record error
    pub fn record_error(&self, error_type: impl Into<String>) {
        let error_type = error_type.into();
        let state = self.state.clone();
        tokio::spawn(async move {
            let mut state = state.lock().await;
            *state.errors.entry(error_type).or_insert(0) += 1;
        });
    }

    /// Get snapshot of current metrics
    pub async fn snapshot(&self) -> MetricsSnapshot {
        let state = self.state.lock().await;
        MetricsSnapshot {
            published: state.published.clone(),
            consumed: state.consumed.clone(),
            acknowledged: state.acknowledged.clone(),
            rejected: state.rejected.clone(),
            connections: state.connections,
            channels: state.channels,
            errors: state.errors.clone(),
        }
    }

    /// Reset all metrics
    pub async fn reset(&self) {
        let mut state = self.state.lock().await;
        *state = MetricsState::default();
    }
}

impl Default for MetricsCollector {
    fn default() -> Self {
        Self::new()
    }
}

/// Snapshot of metrics at a point in time
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsSnapshot {
    /// Messages published by queue
    pub published: HashMap<String, u64>,
    /// Messages consumed by queue
    pub consumed: HashMap<String, u64>,
    /// Messages acknowledged by queue
    pub acknowledged: HashMap<String, u64>,
    /// Messages rejected by queue
    pub rejected: HashMap<String, u64>,
    /// Connection count
    pub connections: u64,
    /// Channel count
    pub channels: u64,
    /// Error count by type
    pub errors: HashMap<String, u64>,
}

impl MetricsSnapshot {
    /// Get total messages published
    pub fn total_published(&self) -> u64 {
        self.published.values().sum()
    }

    /// Get total messages consumed
    pub fn total_consumed(&self) -> u64 {
        self.consumed.values().sum()
    }

    /// Get total messages acknowledged
    pub fn total_acknowledged(&self) -> u64 {
        self.acknowledged.values().sum()
    }

    /// Get total messages rejected
    pub fn total_rejected(&self) -> u64 {
        self.rejected.values().sum()
    }

    /// Get total errors
    pub fn total_errors(&self) -> u64 {
        self.errors.values().sum()
    }
}

/// Prometheus exporter
pub struct PrometheusExporter {
    prefix: String,
}

impl PrometheusExporter {
    /// Create a new Prometheus exporter
    ///
    /// # Arguments
    ///
    /// * `prefix` - Metric name prefix (e.g., "amqp_broker")
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
        }
    }

    /// Export metrics in Prometheus text format
    pub async fn export(&self, collector: &MetricsCollector) -> String {
        let snapshot = collector.snapshot().await;

        let mut output = String::new();

        // Messages published
        output.push_str(&format!(
            "# HELP {}_messages_published_total Total messages published\n",
            self.prefix
        ));
        output.push_str(&format!(
            "# TYPE {}_messages_published_total counter\n",
            self.prefix
        ));
        for (queue, count) in &snapshot.published {
            output.push_str(&format!(
                "{}_messages_published_total{{queue=\"{}\"}} {}\n",
                self.prefix, queue, count
            ));
        }

        // Messages consumed
        output.push_str(&format!(
            "# HELP {}_messages_consumed_total Total messages consumed\n",
            self.prefix
        ));
        output.push_str(&format!(
            "# TYPE {}_messages_consumed_total counter\n",
            self.prefix
        ));
        for (queue, count) in &snapshot.consumed {
            output.push_str(&format!(
                "{}_messages_consumed_total{{queue=\"{}\"}} {}\n",
                self.prefix, queue, count
            ));
        }

        // Messages acknowledged
        output.push_str(&format!(
            "# HELP {}_messages_acknowledged_total Total messages acknowledged\n",
            self.prefix
        ));
        output.push_str(&format!(
            "# TYPE {}_messages_acknowledged_total counter\n",
            self.prefix
        ));
        for (queue, count) in &snapshot.acknowledged {
            output.push_str(&format!(
                "{}_messages_acknowledged_total{{queue=\"{}\"}} {}\n",
                self.prefix, queue, count
            ));
        }

        // Messages rejected
        output.push_str(&format!(
            "# HELP {}_messages_rejected_total Total messages rejected\n",
            self.prefix
        ));
        output.push_str(&format!(
            "# TYPE {}_messages_rejected_total counter\n",
            self.prefix
        ));
        for (queue, count) in &snapshot.rejected {
            output.push_str(&format!(
                "{}_messages_rejected_total{{queue=\"{}\"}} {}\n",
                self.prefix, queue, count
            ));
        }

        // Connections
        output.push_str(&format!(
            "# HELP {}_connections Current number of connections\n",
            self.prefix
        ));
        output.push_str(&format!("# TYPE {}_connections gauge\n", self.prefix));
        output.push_str(&format!(
            "{}_connections {}\n",
            self.prefix, snapshot.connections
        ));

        // Channels
        output.push_str(&format!(
            "# HELP {}_channels Current number of channels\n",
            self.prefix
        ));
        output.push_str(&format!("# TYPE {}_channels gauge\n", self.prefix));
        output.push_str(&format!("{}_channels {}\n", self.prefix, snapshot.channels));

        // Errors
        output.push_str(&format!(
            "# HELP {}_errors_total Total errors by type\n",
            self.prefix
        ));
        output.push_str(&format!("# TYPE {}_errors_total counter\n", self.prefix));
        for (error_type, count) in &snapshot.errors {
            output.push_str(&format!(
                "{}_errors_total{{type=\"{}\"}} {}\n",
                self.prefix, error_type, count
            ));
        }

        output
    }
}

/// StatsD exporter
pub struct StatsDExporter {
    prefix: String,
    #[allow(dead_code)]
    host: String,
}

impl StatsDExporter {
    /// Create a new StatsD exporter
    ///
    /// # Arguments
    ///
    /// * `prefix` - Metric name prefix (e.g., "amqp_broker")
    /// * `host` - StatsD host address (e.g., "localhost:8125")
    pub fn new(prefix: impl Into<String>, host: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
            host: host.into(),
        }
    }

    /// Format metrics in StatsD format
    pub async fn format(&self, collector: &MetricsCollector) -> Vec<String> {
        let snapshot = collector.snapshot().await;

        let mut metrics = Vec::new();

        // Messages published (counter)
        for (queue, count) in &snapshot.published {
            metrics.push(format!(
                "{}.messages.published.{}:{}|c",
                self.prefix, queue, count
            ));
        }

        // Messages consumed (counter)
        for (queue, count) in &snapshot.consumed {
            metrics.push(format!(
                "{}.messages.consumed.{}:{}|c",
                self.prefix, queue, count
            ));
        }

        // Messages acknowledged (counter)
        for (queue, count) in &snapshot.acknowledged {
            metrics.push(format!(
                "{}.messages.acknowledged.{}:{}|c",
                self.prefix, queue, count
            ));
        }

        // Messages rejected (counter)
        for (queue, count) in &snapshot.rejected {
            metrics.push(format!(
                "{}.messages.rejected.{}:{}|c",
                self.prefix, queue, count
            ));
        }

        // Connections (gauge)
        metrics.push(format!(
            "{}.connections:{}|g",
            self.prefix, snapshot.connections
        ));

        // Channels (gauge)
        metrics.push(format!("{}.channels:{}|g", self.prefix, snapshot.channels));

        // Errors (counter)
        for (error_type, count) in &snapshot.errors {
            metrics.push(format!("{}.errors.{}:{}|c", self.prefix, error_type, count));
        }

        metrics
    }
}

/// JSON exporter
pub struct JsonExporter;

impl JsonExporter {
    /// Export metrics as JSON
    pub async fn export(collector: &MetricsCollector) -> Result<String, serde_json::Error> {
        let snapshot = collector.snapshot().await;
        serde_json::to_string_pretty(&snapshot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_metrics_collector() {
        let collector = MetricsCollector::new();

        collector.record_message_published("queue1", 10);
        collector.record_message_consumed("queue1", 8);
        collector.record_message_acknowledged("queue1", 7);
        collector.record_message_rejected("queue1", 1);

        // Give spawned tasks time to complete
        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let snapshot = collector.snapshot().await;
        assert_eq!(snapshot.published.get("queue1"), Some(&10));
        assert_eq!(snapshot.consumed.get("queue1"), Some(&8));
        assert_eq!(snapshot.acknowledged.get("queue1"), Some(&7));
        assert_eq!(snapshot.rejected.get("queue1"), Some(&1));
    }

    #[tokio::test]
    async fn test_metrics_snapshot() {
        let collector = MetricsCollector::new();

        collector.record_message_published("queue1", 10);
        collector.record_message_published("queue2", 20);
        collector.record_message_consumed("queue1", 5);
        collector.set_connections(3);
        collector.set_channels(15);
        collector.record_error("connection_failed");
        collector.record_error("connection_failed");

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let snapshot = collector.snapshot().await;
        assert_eq!(snapshot.total_published(), 30);
        assert_eq!(snapshot.total_consumed(), 5);
        assert_eq!(snapshot.connections, 3);
        assert_eq!(snapshot.channels, 15);
        assert_eq!(snapshot.total_errors(), 2);
    }

    #[tokio::test]
    async fn test_prometheus_exporter() {
        let collector = MetricsCollector::new();
        collector.record_message_published("test_queue", 100);
        collector.set_connections(5);

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let exporter = PrometheusExporter::new("test");
        let output = exporter.export(&collector).await;

        assert!(output.contains("test_messages_published_total{queue=\"test_queue\"} 100"));
        assert!(output.contains("test_connections 5"));
        assert!(output.contains("# HELP"));
        assert!(output.contains("# TYPE"));
    }

    #[tokio::test]
    async fn test_statsd_exporter() {
        let collector = MetricsCollector::new();
        collector.record_message_published("test_queue", 50);
        collector.set_channels(10);

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let exporter = StatsDExporter::new("test", "localhost:8125");
        let metrics = exporter.format(&collector).await;

        assert!(metrics
            .iter()
            .any(|m| m.contains("test.messages.published.test_queue:50|c")));
        assert!(metrics.iter().any(|m| m.contains("test.channels:10|g")));
    }

    #[tokio::test]
    async fn test_json_exporter() {
        let collector = MetricsCollector::new();
        collector.record_message_published("queue1", 10);
        collector.set_connections(2);

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let json = JsonExporter::export(&collector).await.unwrap();
        assert!(json.contains("\"queue1\""));
        assert!(json.contains("\"connections\""));
        assert!(json.contains("2"));
    }

    #[tokio::test]
    async fn test_metrics_reset() {
        let collector = MetricsCollector::new();
        collector.record_message_published("queue1", 100);

        tokio::time::sleep(tokio::time::Duration::from_millis(10)).await;

        let snapshot = collector.snapshot().await;
        assert_eq!(snapshot.total_published(), 100);

        collector.reset().await;

        let snapshot = collector.snapshot().await;
        assert_eq!(snapshot.total_published(), 0);
    }
}
