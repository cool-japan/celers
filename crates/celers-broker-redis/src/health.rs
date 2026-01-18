//! Redis health check implementation
//!
//! Provides health monitoring capabilities for the Redis broker including:
//! - Connection health (ping/pong)
//! - Memory usage monitoring
//! - Server info retrieval
//! - Keyspace statistics
//! - Replication lag monitoring

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

/// Keyspace statistics for a Redis database
#[derive(Debug, Clone, Default)]
pub struct KeyspaceStats {
    /// Database number
    pub db: u32,
    /// Number of keys
    pub keys: u64,
    /// Number of keys with expiration
    pub expires: u64,
    /// Average TTL in milliseconds
    pub avg_ttl: Option<u64>,
}

/// Replication information
#[derive(Debug, Clone, Default)]
pub struct ReplicationInfo {
    /// Role (master, slave, sentinel)
    pub role: String,
    /// Number of connected slaves (if master)
    pub connected_slaves: Option<u32>,
    /// Master host (if slave)
    pub master_host: Option<String>,
    /// Master port (if slave)
    pub master_port: Option<u16>,
    /// Replication lag in seconds (if slave)
    pub replication_lag_secs: Option<u64>,
    /// Master link status (if slave)
    pub master_link_status: Option<String>,
    /// Is replication healthy?
    pub is_healthy: bool,
}

impl ReplicationInfo {
    /// Check if this is a master
    pub fn is_master(&self) -> bool {
        self.role == "master"
    }

    /// Check if this is a slave
    pub fn is_slave(&self) -> bool {
        self.role == "slave"
    }

    /// Check if replication lag is critical (> threshold seconds)
    pub fn is_lag_critical(&self, threshold_secs: u64) -> bool {
        self.replication_lag_secs
            .map(|lag| lag > threshold_secs)
            .unwrap_or(false)
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

    /// Get keyspace statistics for all databases
    pub async fn get_keyspace_stats(&self) -> Result<Vec<KeyspaceStats>> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to connect: {}", e)))?;

        let info: String = redis::cmd("INFO")
            .arg("keyspace")
            .query_async(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get keyspace info: {}", e)))?;

        let info_map = parse_redis_info(&info);
        let mut stats = Vec::new();

        // Parse keyspace entries (e.g., "db0:keys=10,expires=2,avg_ttl=1000")
        for (key, value) in info_map {
            if let Some(db_num) = key.strip_prefix("db") {
                if let Ok(db) = db_num.parse::<u32>() {
                    let mut keys = 0;
                    let mut expires = 0;
                    let mut avg_ttl = None;

                    // Parse the value (format: "keys=10,expires=2,avg_ttl=1000")
                    for part in value.split(',') {
                        if let Some((k, v)) = part.split_once('=') {
                            match k {
                                "keys" => keys = v.parse().unwrap_or(0),
                                "expires" => expires = v.parse().unwrap_or(0),
                                "avg_ttl" => avg_ttl = v.parse().ok(),
                                _ => {}
                            }
                        }
                    }

                    stats.push(KeyspaceStats {
                        db,
                        keys,
                        expires,
                        avg_ttl,
                    });
                }
            }
        }

        Ok(stats)
    }

    /// Get replication information
    pub async fn get_replication_info(&self) -> Result<ReplicationInfo> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to connect: {}", e)))?;

        let info: String = redis::cmd("INFO")
            .arg("replication")
            .query_async(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get replication info: {}", e)))?;

        let info_map = parse_redis_info(&info);

        let role = info_map
            .get("role")
            .cloned()
            .unwrap_or_else(|| "unknown".to_string());

        let mut repl_info = ReplicationInfo {
            role: role.clone(),
            ..Default::default()
        };

        if role == "master" {
            repl_info.connected_slaves = info_map
                .get("connected_slaves")
                .and_then(|v| v.parse().ok());
            repl_info.is_healthy = true;
        } else if role == "slave" {
            repl_info.master_host = info_map.get("master_host").cloned();
            repl_info.master_port = info_map.get("master_port").and_then(|v| v.parse().ok());
            repl_info.master_link_status = info_map.get("master_link_status").cloned();

            // Calculate replication lag
            if let Some(lag_str) = info_map.get("master_repl_offset") {
                if let (Some(master_offset), Some(slave_offset)) = (
                    lag_str.parse::<u64>().ok(),
                    info_map
                        .get("slave_repl_offset")
                        .and_then(|v| v.parse::<u64>().ok()),
                ) {
                    // Rough estimation: assume 1 byte = 1 microsecond (adjust as needed)
                    let lag = master_offset.saturating_sub(slave_offset);
                    repl_info.replication_lag_secs = Some(lag / 1_000_000);
                }
            }

            // Check if master link is up
            repl_info.is_healthy = repl_info
                .master_link_status
                .as_ref()
                .map(|s| s == "up")
                .unwrap_or(false);
        } else {
            repl_info.is_healthy = true; // Sentinel or unknown
        }

        Ok(repl_info)
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

    #[test]
    fn test_keyspace_stats() {
        let stats = KeyspaceStats {
            db: 0,
            keys: 100,
            expires: 20,
            avg_ttl: Some(5000),
        };
        assert_eq!(stats.db, 0);
        assert_eq!(stats.keys, 100);
        assert_eq!(stats.expires, 20);
        assert_eq!(stats.avg_ttl, Some(5000));
    }

    #[test]
    fn test_replication_info_master() {
        let info = ReplicationInfo {
            role: "master".to_string(),
            connected_slaves: Some(2),
            is_healthy: true,
            ..Default::default()
        };
        assert!(info.is_master());
        assert!(!info.is_slave());
        assert!(info.is_healthy);
        assert_eq!(info.connected_slaves, Some(2));
    }

    #[test]
    fn test_replication_info_slave() {
        let info = ReplicationInfo {
            role: "slave".to_string(),
            master_host: Some("localhost".to_string()),
            master_port: Some(6379),
            master_link_status: Some("up".to_string()),
            replication_lag_secs: Some(5),
            is_healthy: true,
            ..Default::default()
        };
        assert!(!info.is_master());
        assert!(info.is_slave());
        assert!(info.is_healthy);
        assert_eq!(info.replication_lag_secs, Some(5));
    }

    #[test]
    fn test_replication_lag_critical() {
        let mut info = ReplicationInfo {
            role: "slave".to_string(),
            replication_lag_secs: Some(100),
            ..Default::default()
        };
        assert!(info.is_lag_critical(50));
        assert!(!info.is_lag_critical(150));

        info.replication_lag_secs = None;
        assert!(!info.is_lag_critical(50));
    }
}
