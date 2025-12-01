//! Redis health check implementation
//!
//! Provides health monitoring capabilities for the Redis broker including:
//! - Connection health (ping/pong)
//! - Memory usage monitoring
//! - Server info retrieval
//! - Keyspace statistics

use celers_core::{CelersError, Result};
use redis::AsyncCommands;
use std::collections::HashMap;

/// Redis health status
#[derive(Debug, Clone, Default)]
pub struct RedisHealthStatus {
    /// Whether Redis is reachable
    pub is_healthy: bool,
    /// Ping latency in milliseconds
    pub latency_ms: u64,
    /// Used memory in bytes (if available)
    pub used_memory: Option<u64>,
    /// Max memory in bytes (if configured)
    pub max_memory: Option<u64>,
    /// Memory usage percentage (if both used and max are available)
    pub memory_usage_percent: Option<f64>,
    /// Number of connected clients
    pub connected_clients: Option<u64>,
    /// Redis version
    pub redis_version: Option<String>,
    /// Role (master/slave/sentinel)
    pub role: Option<String>,
    /// Error message if unhealthy
    pub error: Option<String>,
}

impl RedisHealthStatus {
    /// Create a healthy status with latency
    pub fn healthy(latency_ms: u64) -> Self {
        Self {
            is_healthy: true,
            latency_ms,
            ..Default::default()
        }
    }

    /// Create an unhealthy status with error message
    pub fn unhealthy(error: String) -> Self {
        Self {
            is_healthy: false,
            error: Some(error),
            ..Default::default()
        }
    }

    /// Check if memory usage is critical (> threshold percent)
    pub fn is_memory_critical(&self, threshold_percent: f64) -> bool {
        self.memory_usage_percent
            .map(|usage| usage > threshold_percent)
            .unwrap_or(false)
    }
}

/// Queue statistics
#[derive(Debug, Clone, Default)]
pub struct QueueStats {
    /// Number of tasks in the main queue
    pub pending: usize,
    /// Number of tasks in the processing queue
    pub processing: usize,
    /// Number of tasks in the dead letter queue
    pub dlq: usize,
    /// Number of tasks in the delayed queue
    pub delayed: usize,
}

impl QueueStats {
    /// Total number of tasks across all queues
    pub fn total(&self) -> usize {
        self.pending + self.processing + self.dlq + self.delayed
    }
}

/// Health checker for Redis broker
pub struct HealthChecker {
    client: redis::Client,
}

impl HealthChecker {
    /// Create a new health checker
    pub fn new(client: redis::Client) -> Self {
        Self { client }
    }

    /// Perform a simple ping check
    pub async fn ping(&self) -> Result<u64> {
        let start = std::time::Instant::now();

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to connect: {}", e)))?;

        redis::cmd("PING")
            .query_async::<String>(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Ping failed: {}", e)))?;

        Ok(start.elapsed().as_millis() as u64)
    }

    /// Get comprehensive health status
    pub async fn check_health(&self) -> RedisHealthStatus {
        let start = std::time::Instant::now();

        let conn_result = self.client.get_multiplexed_async_connection().await;
        let mut conn = match conn_result {
            Ok(c) => c,
            Err(e) => return RedisHealthStatus::unhealthy(format!("Connection failed: {}", e)),
        };

        // Ping check
        if let Err(e) = redis::cmd("PING").query_async::<String>(&mut conn).await {
            return RedisHealthStatus::unhealthy(format!("Ping failed: {}", e));
        }

        let latency_ms = start.elapsed().as_millis() as u64;
        let mut status = RedisHealthStatus::healthy(latency_ms);

        // Get server info
        if let Ok(info) = redis::cmd("INFO").query_async::<String>(&mut conn).await {
            let info_map = parse_redis_info(&info);

            status.redis_version = info_map.get("redis_version").cloned();
            status.role = info_map.get("role").cloned();

            if let Some(used) = info_map.get("used_memory").and_then(|v| v.parse().ok()) {
                status.used_memory = Some(used);
            }

            if let Some(max) = info_map.get("maxmemory").and_then(|v| v.parse().ok()) {
                if max > 0 {
                    status.max_memory = Some(max);
                    if let Some(used) = status.used_memory {
                        status.memory_usage_percent = Some((used as f64 / max as f64) * 100.0);
                    }
                }
            }

            if let Some(clients) = info_map
                .get("connected_clients")
                .and_then(|v| v.parse().ok())
            {
                status.connected_clients = Some(clients);
            }
        }

        status
    }

    /// Get queue statistics
    pub async fn get_queue_stats(
        &self,
        queue_name: &str,
        is_priority_mode: bool,
    ) -> Result<QueueStats> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to connect: {}", e)))?;

        let processing_queue = format!("{}:processing", queue_name);
        let dlq_name = format!("{}:dlq", queue_name);
        let delayed_queue = format!("{}:delayed", queue_name);

        let pending: usize = if is_priority_mode {
            conn.zcard(queue_name).await.unwrap_or(0)
        } else {
            conn.llen(queue_name).await.unwrap_or(0)
        };

        let processing: usize = conn.llen(&processing_queue).await.unwrap_or(0);
        let dlq: usize = conn.llen(&dlq_name).await.unwrap_or(0);
        let delayed: usize = conn.zcard(&delayed_queue).await.unwrap_or(0);

        Ok(QueueStats {
            pending,
            processing,
            dlq,
            delayed,
        })
    }

    /// Get Redis memory info
    pub async fn get_memory_info(&self) -> Result<HashMap<String, String>> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to connect: {}", e)))?;

        let info: String = redis::cmd("INFO")
            .arg("memory")
            .query_async(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get memory info: {}", e)))?;

        Ok(parse_redis_info(&info))
    }
}

/// Parse Redis INFO command output into a HashMap
fn parse_redis_info(info: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();

    for line in info.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        if let Some((key, value)) = line.split_once(':') {
            map.insert(key.to_string(), value.to_string());
        }
    }

    map
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redis_health_status_healthy() {
        let status = RedisHealthStatus::healthy(5);
        assert!(status.is_healthy);
        assert_eq!(status.latency_ms, 5);
        assert!(status.error.is_none());
    }

    #[test]
    fn test_redis_health_status_unhealthy() {
        let status = RedisHealthStatus::unhealthy("Connection refused".to_string());
        assert!(!status.is_healthy);
        assert_eq!(status.error, Some("Connection refused".to_string()));
    }

    #[test]
    fn test_memory_critical() {
        let mut status = RedisHealthStatus::healthy(5);
        status.memory_usage_percent = Some(85.0);
        assert!(!status.is_memory_critical(90.0));
        assert!(status.is_memory_critical(80.0));
    }

    #[test]
    fn test_queue_stats_total() {
        let stats = QueueStats {
            pending: 10,
            processing: 5,
            dlq: 2,
            delayed: 3,
        };
        assert_eq!(stats.total(), 20);
    }

    #[test]
    fn test_parse_redis_info() {
        let info = r#"
# Server
redis_version:7.0.0
role:master

# Clients
connected_clients:10

# Memory
used_memory:1024000
maxmemory:10240000
"#;

        let map = parse_redis_info(info);
        assert_eq!(map.get("redis_version"), Some(&"7.0.0".to_string()));
        assert_eq!(map.get("role"), Some(&"master".to_string()));
        assert_eq!(map.get("connected_clients"), Some(&"10".to_string()));
        assert_eq!(map.get("used_memory"), Some(&"1024000".to_string()));
        assert_eq!(map.get("maxmemory"), Some(&"10240000".to_string()));
    }
}
