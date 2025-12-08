//! Redis Cluster support for distributed task queuing
//!
//! Provides cluster-aware key distribution, cross-slot operations, and failover handling.

use crate::{CelersError, Result};
use redis::cluster::ClusterClient;

/// Redis deployment mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RedisMode {
    /// Standalone Redis instance
    Standalone,
    /// Redis Cluster (distributed)
    Cluster,
    /// Redis Sentinel (HA with master/slave)
    Sentinel,
}

impl std::fmt::Display for RedisMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RedisMode::Standalone => write!(f, "Standalone"),
            RedisMode::Cluster => write!(f, "Cluster"),
            RedisMode::Sentinel => write!(f, "Sentinel"),
        }
    }
}

/// Redis Cluster configuration
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// Cluster node URLs
    pub nodes: Vec<String>,

    /// Enable readonly mode for replicas
    pub readonly: bool,

    /// Connection timeout in seconds
    pub connection_timeout_secs: u64,

    /// Response timeout in seconds
    pub response_timeout_secs: u64,

    /// Username for authentication (Redis 6+)
    pub username: Option<String>,

    /// Password for authentication
    pub password: Option<String>,

    /// Retry on cluster errors
    pub retry_on_error: bool,

    /// Maximum number of redirections
    pub max_redirections: u32,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            nodes: vec!["redis://localhost:7000".to_string()],
            readonly: false,
            connection_timeout_secs: 5,
            response_timeout_secs: 5,
            username: None,
            password: None,
            retry_on_error: true,
            max_redirections: 16,
        }
    }
}

impl ClusterConfig {
    /// Create a new builder
    pub fn builder() -> ClusterConfigBuilder {
        ClusterConfigBuilder::default()
    }

    /// Build a ClusterClient from this configuration
    pub fn build_client(&self) -> Result<ClusterClient> {
        ClusterClient::new(self.nodes.clone())
            .map_err(|e| CelersError::Broker(format!("Failed to create cluster client: {}", e)))
    }
}

/// Builder for ClusterConfig
#[derive(Default)]
pub struct ClusterConfigBuilder {
    nodes: Vec<String>,
    readonly: bool,
    connection_timeout_secs: Option<u64>,
    response_timeout_secs: Option<u64>,
    username: Option<String>,
    password: Option<String>,
    retry_on_error: bool,
    max_redirections: Option<u32>,
}

impl ClusterConfigBuilder {
    /// Add a cluster node
    pub fn node(mut self, url: impl Into<String>) -> Self {
        self.nodes.push(url.into());
        self
    }

    /// Set cluster nodes
    pub fn nodes(mut self, nodes: Vec<String>) -> Self {
        self.nodes = nodes;
        self
    }

    /// Enable readonly mode
    pub fn readonly(mut self, readonly: bool) -> Self {
        self.readonly = readonly;
        self
    }

    /// Set connection timeout
    pub fn connection_timeout_secs(mut self, secs: u64) -> Self {
        self.connection_timeout_secs = Some(secs);
        self
    }

    /// Set response timeout
    pub fn response_timeout_secs(mut self, secs: u64) -> Self {
        self.response_timeout_secs = Some(secs);
        self
    }

    /// Set username
    pub fn username(mut self, username: impl Into<String>) -> Self {
        self.username = Some(username.into());
        self
    }

    /// Set password
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Enable/disable retry on error
    pub fn retry_on_error(mut self, retry: bool) -> Self {
        self.retry_on_error = retry;
        self
    }

    /// Set maximum redirections
    pub fn max_redirections(mut self, max: u32) -> Self {
        self.max_redirections = Some(max);
        self
    }

    /// Build the configuration
    pub fn build(self) -> Result<ClusterConfig> {
        if self.nodes.is_empty() {
            return Err(CelersError::Configuration(
                "At least one cluster node is required".to_string(),
            ));
        }

        Ok(ClusterConfig {
            nodes: self.nodes,
            readonly: self.readonly,
            connection_timeout_secs: self.connection_timeout_secs.unwrap_or(5),
            response_timeout_secs: self.response_timeout_secs.unwrap_or(5),
            username: self.username,
            password: self.password,
            retry_on_error: self.retry_on_error,
            max_redirections: self.max_redirections.unwrap_or(16),
        })
    }
}

/// Hash slot calculator for Redis Cluster
pub struct HashSlot;

impl HashSlot {
    /// Calculate the hash slot for a key (Redis Cluster uses CRC16)
    pub fn calculate(key: &str) -> u16 {
        // Extract the hash tag if present (between {})
        let hash_key = if let Some(start) = key.find('{') {
            if let Some(end) = key[start + 1..].find('}') {
                let tag = &key[start + 1..start + 1 + end];
                if !tag.is_empty() {
                    tag
                } else {
                    key
                }
            } else {
                key
            }
        } else {
            key
        };

        // Calculate CRC16
        let crc = crc16(hash_key.as_bytes());
        crc % 16384 // Redis Cluster has 16384 slots
    }

    /// Check if two keys are in the same slot
    pub fn same_slot(key1: &str, key2: &str) -> bool {
        Self::calculate(key1) == Self::calculate(key2)
    }

    /// Get the hash tag for a key to ensure same slot placement
    pub fn hash_tag(key: &str) -> String {
        format!("{{{}}}", key)
    }

    /// Apply hash tag to multiple keys
    pub fn apply_tag(keys: &[&str], tag: &str) -> Vec<String> {
        keys.iter()
            .map(|key| format!("{{{}}}{}", tag, key))
            .collect()
    }
}

/// CRC16 implementation for Redis Cluster hash slot calculation
fn crc16(data: &[u8]) -> u16 {
    const CRC16_TAB: [u16; 256] = [
        0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7, 0x8108, 0x9129, 0xa14a,
        0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef, 0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294,
        0x72f7, 0x62d6, 0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de, 0x2462,
        0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485, 0xa56a, 0xb54b, 0x8528, 0x9509,
        0xe5ee, 0xf5cf, 0xc5ac, 0xd58d, 0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695,
        0x46b4, 0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc, 0x48c4, 0x58e5,
        0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823, 0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948,
        0x9969, 0xa90a, 0xb92b, 0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
        0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a, 0x6ca6, 0x7c87, 0x4ce4,
        0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41, 0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b,
        0x8d68, 0x9d49, 0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70, 0xff9f,
        0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78, 0x9188, 0x81a9, 0xb1ca, 0xa1eb,
        0xd10c, 0xc12d, 0xf14e, 0xe16f, 0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046,
        0x6067, 0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e, 0x02b1, 0x1290,
        0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256, 0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e,
        0xe54f, 0xd52c, 0xc50d, 0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
        0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c, 0x26d3, 0x36f2, 0x0691,
        0x16b0, 0x6657, 0x7676, 0x4615, 0x5634, 0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9,
        0xb98a, 0xa9ab, 0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3, 0xcb7d,
        0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a, 0x4a75, 0x5a54, 0x6a37, 0x7a16,
        0x0af1, 0x1ad0, 0x2ab3, 0x3a92, 0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8,
        0x8dc9, 0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1, 0xef1f, 0xff3e,
        0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8, 0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93,
        0x3eb2, 0x0ed1, 0x1ef0,
    ];

    let mut crc: u16 = 0;
    for &byte in data {
        let idx = ((crc >> 8) ^ u16::from(byte)) & 0xff;
        crc = (crc << 8) ^ CRC16_TAB[idx as usize];
    }
    crc
}

/// Cluster node information
#[derive(Debug, Clone)]
pub struct ClusterNode {
    /// Node ID
    pub id: String,

    /// Node address (host:port)
    pub address: String,

    /// Node role (master/slave)
    pub role: ClusterNodeRole,

    /// Slot ranges this node is responsible for (master only)
    pub slots: Vec<(u16, u16)>,

    /// Master node ID (for slaves)
    pub master_id: Option<String>,
}

/// Cluster node role
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterNodeRole {
    Master,
    Slave,
}

impl std::fmt::Display for ClusterNodeRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusterNodeRole::Master => write!(f, "master"),
            ClusterNodeRole::Slave => write!(f, "slave"),
        }
    }
}

/// Cluster topology information
#[derive(Debug, Clone)]
pub struct ClusterTopology {
    /// Cluster nodes
    pub nodes: Vec<ClusterNode>,

    /// Total number of slots (should be 16384)
    pub total_slots: u16,

    /// Number of master nodes
    pub master_count: usize,

    /// Number of slave nodes
    pub slave_count: usize,
}

impl ClusterTopology {
    /// Get the node responsible for a given hash slot
    pub fn node_for_slot(&self, slot: u16) -> Option<&ClusterNode> {
        self.nodes.iter().find(|node| {
            node.role == ClusterNodeRole::Master
                && node
                    .slots
                    .iter()
                    .any(|(start, end)| slot >= *start && slot <= *end)
        })
    }

    /// Get the node responsible for a given key
    pub fn node_for_key(&self, key: &str) -> Option<&ClusterNode> {
        let slot = HashSlot::calculate(key);
        self.node_for_slot(slot)
    }

    /// Get all master nodes
    pub fn masters(&self) -> Vec<&ClusterNode> {
        self.nodes
            .iter()
            .filter(|n| n.role == ClusterNodeRole::Master)
            .collect()
    }

    /// Get all slave nodes
    pub fn slaves(&self) -> Vec<&ClusterNode> {
        self.nodes
            .iter()
            .filter(|n| n.role == ClusterNodeRole::Slave)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_redis_mode_display() {
        assert_eq!(RedisMode::Standalone.to_string(), "Standalone");
        assert_eq!(RedisMode::Cluster.to_string(), "Cluster");
        assert_eq!(RedisMode::Sentinel.to_string(), "Sentinel");
    }

    #[test]
    fn test_cluster_config_default() {
        let config = ClusterConfig::default();

        assert_eq!(config.nodes.len(), 1);
        assert!(!config.readonly);
        assert_eq!(config.connection_timeout_secs, 5);
        assert_eq!(config.response_timeout_secs, 5);
        assert!(config.retry_on_error);
        assert_eq!(config.max_redirections, 16);
    }

    #[test]
    fn test_cluster_config_builder() {
        let config = ClusterConfig::builder()
            .node("redis://node1:7000")
            .node("redis://node2:7001")
            .readonly(true)
            .connection_timeout_secs(10)
            .response_timeout_secs(10)
            .username("user")
            .password("pass")
            .retry_on_error(false)
            .max_redirections(32)
            .build()
            .unwrap();

        assert_eq!(config.nodes.len(), 2);
        assert!(config.readonly);
        assert_eq!(config.connection_timeout_secs, 10);
        assert_eq!(config.response_timeout_secs, 10);
        assert_eq!(config.username, Some("user".to_string()));
        assert_eq!(config.password, Some("pass".to_string()));
        assert!(!config.retry_on_error);
        assert_eq!(config.max_redirections, 32);
    }

    #[test]
    fn test_cluster_config_builder_no_nodes() {
        let result = ClusterConfig::builder().build();
        assert!(result.is_err());
    }

    #[test]
    fn test_hash_slot_calculate() {
        // Test basic key
        let slot = HashSlot::calculate("mykey");
        assert!(slot < 16384);

        // Same key should give same slot
        let slot2 = HashSlot::calculate("mykey");
        assert_eq!(slot, slot2);
    }

    #[test]
    fn test_hash_slot_with_tag() {
        // Keys with same hash tag should be in same slot
        let slot1 = HashSlot::calculate("{user:123}:profile");
        let slot2 = HashSlot::calculate("{user:123}:settings");

        assert_eq!(slot1, slot2);
    }

    #[test]
    fn test_hash_slot_same_slot() {
        assert!(HashSlot::same_slot("{tag}key1", "{tag}key2"));
        assert!(!HashSlot::same_slot("key1", "key2")); // Might be different
    }

    #[test]
    fn test_hash_slot_hash_tag() {
        let tagged = HashSlot::hash_tag("mykey");
        assert_eq!(tagged, "{mykey}");
    }

    #[test]
    fn test_hash_slot_apply_tag() {
        let keys = vec!["key1", "key2", "key3"];
        let tagged = HashSlot::apply_tag(&keys, "mytag");

        assert_eq!(tagged.len(), 3);
        assert_eq!(tagged[0], "{mytag}key1");
        assert_eq!(tagged[1], "{mytag}key2");
        assert_eq!(tagged[2], "{mytag}key3");

        // All should be in same slot
        let slot1 = HashSlot::calculate(&tagged[0]);
        let slot2 = HashSlot::calculate(&tagged[1]);
        let slot3 = HashSlot::calculate(&tagged[2]);

        assert_eq!(slot1, slot2);
        assert_eq!(slot2, slot3);
    }

    #[test]
    fn test_crc16() {
        // Test CRC16 calculation
        let crc = crc16(b"hello");
        assert!(crc > 0);

        // Same input should give same output
        let crc2 = crc16(b"hello");
        assert_eq!(crc, crc2);

        // Different input should (likely) give different output
        let crc3 = crc16(b"world");
        assert_ne!(crc, crc3);
    }

    #[test]
    fn test_cluster_node_role_display() {
        assert_eq!(ClusterNodeRole::Master.to_string(), "master");
        assert_eq!(ClusterNodeRole::Slave.to_string(), "slave");
    }

    #[test]
    fn test_cluster_topology_node_for_slot() {
        let topology = ClusterTopology {
            nodes: vec![
                ClusterNode {
                    id: "node1".to_string(),
                    address: "127.0.0.1:7000".to_string(),
                    role: ClusterNodeRole::Master,
                    slots: vec![(0, 5460)],
                    master_id: None,
                },
                ClusterNode {
                    id: "node2".to_string(),
                    address: "127.0.0.1:7001".to_string(),
                    role: ClusterNodeRole::Master,
                    slots: vec![(5461, 10922)],
                    master_id: None,
                },
            ],
            total_slots: 16384,
            master_count: 2,
            slave_count: 0,
        };

        let node = topology.node_for_slot(100);
        assert!(node.is_some());
        assert_eq!(node.unwrap().id, "node1");

        let node = topology.node_for_slot(6000);
        assert!(node.is_some());
        assert_eq!(node.unwrap().id, "node2");
    }

    #[test]
    fn test_cluster_topology_node_for_key() {
        let topology = ClusterTopology {
            nodes: vec![ClusterNode {
                id: "node1".to_string(),
                address: "127.0.0.1:7000".to_string(),
                role: ClusterNodeRole::Master,
                slots: vec![(0, 16383)],
                master_id: None,
            }],
            total_slots: 16384,
            master_count: 1,
            slave_count: 0,
        };

        let node = topology.node_for_key("mykey");
        assert!(node.is_some());
        assert_eq!(node.unwrap().id, "node1");
    }

    #[test]
    fn test_cluster_topology_masters() {
        let topology = ClusterTopology {
            nodes: vec![
                ClusterNode {
                    id: "node1".to_string(),
                    address: "127.0.0.1:7000".to_string(),
                    role: ClusterNodeRole::Master,
                    slots: vec![(0, 5460)],
                    master_id: None,
                },
                ClusterNode {
                    id: "node2".to_string(),
                    address: "127.0.0.1:7001".to_string(),
                    role: ClusterNodeRole::Slave,
                    slots: vec![],
                    master_id: Some("node1".to_string()),
                },
            ],
            total_slots: 16384,
            master_count: 1,
            slave_count: 1,
        };

        let masters = topology.masters();
        assert_eq!(masters.len(), 1);
        assert_eq!(masters[0].id, "node1");
    }

    #[test]
    fn test_cluster_topology_slaves() {
        let topology = ClusterTopology {
            nodes: vec![
                ClusterNode {
                    id: "node1".to_string(),
                    address: "127.0.0.1:7000".to_string(),
                    role: ClusterNodeRole::Master,
                    slots: vec![(0, 5460)],
                    master_id: None,
                },
                ClusterNode {
                    id: "node2".to_string(),
                    address: "127.0.0.1:7001".to_string(),
                    role: ClusterNodeRole::Slave,
                    slots: vec![],
                    master_id: Some("node1".to_string()),
                },
            ],
            total_slots: 16384,
            master_count: 1,
            slave_count: 1,
        };

        let slaves = topology.slaves();
        assert_eq!(slaves.len(), 1);
        assert_eq!(slaves[0].id, "node2");
    }
}
