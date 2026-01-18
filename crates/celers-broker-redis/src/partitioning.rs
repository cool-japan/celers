//! Queue partitioning for horizontal scaling
//!
//! Provides consistent hashing and key-based sharding to distribute
//! tasks across multiple Redis instances or queue partitions.

use crate::{CelersError, Result, SerializedTask};
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

/// Hash algorithm for consistent hashing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HashAlgorithm {
    /// CRC32 hashing (fast, good distribution)
    Crc32,
    /// XXHash-based hashing (very fast)
    XxHash,
}

impl HashAlgorithm {
    /// Compute hash value for given key
    pub fn hash(&self, key: &[u8]) -> u64 {
        match self {
            HashAlgorithm::Crc32 => {
                let hash = crc32fast::hash(key);
                hash as u64
            }
            HashAlgorithm::XxHash => {
                // Simple FNV-1a hash as a placeholder for xxhash
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                key.hash(&mut hasher);
                hasher.finish()
            }
        }
    }
}

/// Partitioning strategy for task distribution
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PartitionStrategy {
    /// Partition by task ID
    ById,
    /// Partition by task name
    ByName,
    /// Partition by custom routing key
    ByRoutingKey(String),
    /// Partition by task payload hash
    ByPayloadHash,
}

impl PartitionStrategy {
    /// Extract partition key from task
    pub fn extract_key(&self, task: &SerializedTask) -> String {
        match self {
            PartitionStrategy::ById => task.metadata.id.to_string(),
            PartitionStrategy::ByName => task.metadata.name.clone(),
            PartitionStrategy::ByRoutingKey(key) => key.clone(),
            PartitionStrategy::ByPayloadHash => {
                format!("{:x}", crc32fast::hash(&task.payload))
            }
        }
    }
}

/// Virtual node in consistent hash ring
#[derive(Debug, Clone)]
struct VirtualNode {
    hash: u64,
    partition_id: usize,
}

/// Consistent hash ring for partition selection
pub struct ConsistentHashRing {
    virtual_nodes: Vec<VirtualNode>,
    num_partitions: usize,
    replicas: usize,
    algorithm: HashAlgorithm,
}

impl ConsistentHashRing {
    /// Create a new consistent hash ring
    ///
    /// # Arguments
    /// * `num_partitions` - Number of physical partitions
    /// * `replicas` - Number of virtual nodes per partition (higher = better distribution)
    /// * `algorithm` - Hash algorithm to use
    pub fn new(num_partitions: usize, replicas: usize, algorithm: HashAlgorithm) -> Self {
        let mut ring = Self {
            virtual_nodes: Vec::new(),
            num_partitions,
            replicas,
            algorithm,
        };
        ring.rebuild();
        ring
    }

    /// Rebuild the hash ring
    fn rebuild(&mut self) {
        self.virtual_nodes.clear();

        for partition_id in 0..self.num_partitions {
            for replica in 0..self.replicas {
                let key = format!("partition-{}-replica-{}", partition_id, replica);
                let hash = self.algorithm.hash(key.as_bytes());
                self.virtual_nodes.push(VirtualNode { hash, partition_id });
            }
        }

        // Sort by hash for binary search
        self.virtual_nodes.sort_by_key(|node| node.hash);
    }

    /// Get partition ID for a given key
    pub fn get_partition(&self, key: &str) -> usize {
        if self.virtual_nodes.is_empty() {
            return 0;
        }

        let hash = self.algorithm.hash(key.as_bytes());

        // Binary search for the first node with hash >= key_hash
        match self
            .virtual_nodes
            .binary_search_by_key(&hash, |node| node.hash)
        {
            Ok(idx) => self.virtual_nodes[idx].partition_id,
            Err(idx) => {
                if idx >= self.virtual_nodes.len() {
                    // Wrap around to first node
                    self.virtual_nodes[0].partition_id
                } else {
                    self.virtual_nodes[idx].partition_id
                }
            }
        }
    }

    /// Add a new partition to the ring
    pub fn add_partition(&mut self) {
        self.num_partitions += 1;
        self.rebuild();
    }

    /// Remove a partition from the ring (redistributes keys)
    pub fn remove_partition(&mut self, partition_id: usize) -> Result<()> {
        if partition_id >= self.num_partitions {
            return Err(CelersError::Other(format!(
                "Invalid partition ID: {}",
                partition_id
            )));
        }
        if self.num_partitions <= 1 {
            return Err(CelersError::Other(
                "Cannot remove last partition".to_string(),
            ));
        }

        self.num_partitions -= 1;
        self.rebuild();
        Ok(())
    }

    /// Get the number of partitions
    pub fn num_partitions(&self) -> usize {
        self.num_partitions
    }

    /// Get partition distribution statistics
    pub fn get_stats(&self) -> PartitionStats {
        let mut partition_vnodes: HashMap<usize, usize> = HashMap::new();

        for vnode in &self.virtual_nodes {
            *partition_vnodes.entry(vnode.partition_id).or_insert(0) += 1;
        }

        PartitionStats {
            num_partitions: self.num_partitions,
            num_virtual_nodes: self.virtual_nodes.len(),
            replicas_per_partition: self.replicas,
            partition_vnode_counts: partition_vnodes,
        }
    }
}

/// Statistics about partition distribution
#[derive(Debug, Clone)]
pub struct PartitionStats {
    /// Number of physical partitions
    pub num_partitions: usize,
    /// Total number of virtual nodes
    pub num_virtual_nodes: usize,
    /// Replicas per partition
    pub replicas_per_partition: usize,
    /// Virtual node count per partition
    pub partition_vnode_counts: HashMap<usize, usize>,
}

/// Partition manager for distributed queues
pub struct PartitionManager {
    ring: Arc<ConsistentHashRing>,
    strategy: PartitionStrategy,
    partition_prefix: String,
}

impl PartitionManager {
    /// Create a new partition manager
    pub fn new(num_partitions: usize, strategy: PartitionStrategy, partition_prefix: &str) -> Self {
        Self::with_algorithm(
            num_partitions,
            strategy,
            partition_prefix,
            HashAlgorithm::Crc32,
        )
    }

    /// Create a new partition manager with custom hash algorithm
    pub fn with_algorithm(
        num_partitions: usize,
        strategy: PartitionStrategy,
        partition_prefix: &str,
        algorithm: HashAlgorithm,
    ) -> Self {
        let ring = ConsistentHashRing::new(num_partitions, 150, algorithm);
        Self {
            ring: Arc::new(ring),
            strategy,
            partition_prefix: partition_prefix.to_string(),
        }
    }

    /// Get the partition ID for a task
    pub fn get_partition_id(&self, task: &SerializedTask) -> usize {
        let key = self.strategy.extract_key(task);
        self.ring.get_partition(&key)
    }

    /// Get the queue name for a task
    pub fn get_queue_name(&self, task: &SerializedTask) -> String {
        let partition_id = self.get_partition_id(task);
        self.get_queue_name_by_id(partition_id)
    }

    /// Get the queue name for a partition ID
    pub fn get_queue_name_by_id(&self, partition_id: usize) -> String {
        format!("{}:{}", self.partition_prefix, partition_id)
    }

    /// Get all queue names
    pub fn get_all_queue_names(&self) -> Vec<String> {
        (0..self.ring.num_partitions())
            .map(|id| self.get_queue_name_by_id(id))
            .collect()
    }

    /// Get the number of partitions
    pub fn num_partitions(&self) -> usize {
        self.ring.num_partitions()
    }

    /// Get partition statistics
    pub fn get_stats(&self) -> PartitionStats {
        self.ring.get_stats()
    }

    /// Get the partitioning strategy
    pub fn strategy(&self) -> &PartitionStrategy {
        &self.strategy
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use celers_core::{TaskId, TaskMetadata, TaskState};
    use chrono::Utc;
    use std::collections::HashSet;

    fn create_test_task(id: &str, name: &str) -> SerializedTask {
        SerializedTask {
            metadata: TaskMetadata {
                id: TaskId::parse_str(id).unwrap(),
                name: name.to_string(),
                state: TaskState::Pending,
                created_at: Utc::now(),
                updated_at: Utc::now(),
                priority: 0,
                max_retries: 3,
                timeout_secs: None,
                group_id: None,
                chord_id: None,
                dependencies: HashSet::new(),
            },
            payload: b"test payload".to_vec(),
        }
    }

    #[test]
    fn test_hash_algorithm_crc32() {
        let algo = HashAlgorithm::Crc32;
        let hash1 = algo.hash(b"test");
        let hash2 = algo.hash(b"test");
        let hash3 = algo.hash(b"different");

        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_hash_algorithm_xxhash() {
        let algo = HashAlgorithm::XxHash;
        let hash1 = algo.hash(b"test");
        let hash2 = algo.hash(b"test");
        let hash3 = algo.hash(b"different");

        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_partition_strategy_by_id() {
        let task = create_test_task("550e8400-e29b-41d4-a716-446655440000", "test_task");
        let strategy = PartitionStrategy::ById;

        let key = strategy.extract_key(&task);
        assert_eq!(key, "550e8400-e29b-41d4-a716-446655440000");
    }

    #[test]
    fn test_partition_strategy_by_name() {
        let task = create_test_task("550e8400-e29b-41d4-a716-446655440000", "my_task");
        let strategy = PartitionStrategy::ByName;

        let key = strategy.extract_key(&task);
        assert_eq!(key, "my_task");
    }

    #[test]
    fn test_partition_strategy_by_routing_key() {
        let task = create_test_task("550e8400-e29b-41d4-a716-446655440000", "test_task");
        let strategy = PartitionStrategy::ByRoutingKey("custom_key".to_string());

        let key = strategy.extract_key(&task);
        assert_eq!(key, "custom_key");
    }

    #[test]
    fn test_partition_strategy_by_payload_hash() {
        let task = create_test_task("550e8400-e29b-41d4-a716-446655440000", "test_task");
        let strategy = PartitionStrategy::ByPayloadHash;

        let key = strategy.extract_key(&task);
        assert!(!key.is_empty());
        // Hash should be consistent
        let key2 = strategy.extract_key(&task);
        assert_eq!(key, key2);
    }

    #[test]
    fn test_consistent_hash_ring_creation() {
        let ring = ConsistentHashRing::new(4, 100, HashAlgorithm::Crc32);
        assert_eq!(ring.num_partitions(), 4);
        assert_eq!(ring.virtual_nodes.len(), 400); // 4 partitions * 100 replicas
    }

    #[test]
    fn test_consistent_hash_ring_get_partition() {
        let ring = ConsistentHashRing::new(4, 100, HashAlgorithm::Crc32);

        let partition1 = ring.get_partition("key1");
        let partition2 = ring.get_partition("key1");
        let partition3 = ring.get_partition("key2");

        // Same key should always map to same partition
        assert_eq!(partition1, partition2);

        // Partitions should be valid
        assert!(partition1 < 4);
        assert!(partition3 < 4);
    }

    #[test]
    fn test_consistent_hash_ring_add_partition() {
        let mut ring = ConsistentHashRing::new(4, 100, HashAlgorithm::Crc32);
        ring.add_partition();

        assert_eq!(ring.num_partitions(), 5);
        assert_eq!(ring.virtual_nodes.len(), 500);
    }

    #[test]
    fn test_consistent_hash_ring_remove_partition() {
        let mut ring = ConsistentHashRing::new(4, 100, HashAlgorithm::Crc32);
        let result = ring.remove_partition(0);

        assert!(result.is_ok());
        assert_eq!(ring.num_partitions(), 3);
        assert_eq!(ring.virtual_nodes.len(), 300);
    }

    #[test]
    fn test_consistent_hash_ring_remove_last_partition() {
        let mut ring = ConsistentHashRing::new(1, 100, HashAlgorithm::Crc32);
        let result = ring.remove_partition(0);

        assert!(result.is_err());
    }

    #[test]
    fn test_consistent_hash_ring_stats() {
        let ring = ConsistentHashRing::new(4, 100, HashAlgorithm::Crc32);
        let stats = ring.get_stats();

        assert_eq!(stats.num_partitions, 4);
        assert_eq!(stats.num_virtual_nodes, 400);
        assert_eq!(stats.replicas_per_partition, 100);
        assert_eq!(stats.partition_vnode_counts.len(), 4);

        for count in stats.partition_vnode_counts.values() {
            assert_eq!(*count, 100);
        }
    }

    #[test]
    fn test_partition_manager_creation() {
        let manager = PartitionManager::new(4, PartitionStrategy::ById, "queue");

        assert_eq!(manager.num_partitions(), 4);
        assert_eq!(manager.strategy(), &PartitionStrategy::ById);
    }

    #[test]
    fn test_partition_manager_get_partition_id() {
        let manager = PartitionManager::new(4, PartitionStrategy::ById, "queue");
        let task = create_test_task("550e8400-e29b-41d4-a716-446655440000", "test_task");

        let partition_id = manager.get_partition_id(&task);
        assert!(partition_id < 4);

        // Should be consistent
        let partition_id2 = manager.get_partition_id(&task);
        assert_eq!(partition_id, partition_id2);
    }

    #[test]
    fn test_partition_manager_get_queue_name() {
        let manager = PartitionManager::new(4, PartitionStrategy::ById, "my_queue");
        let task = create_test_task("550e8400-e29b-41d4-a716-446655440000", "test_task");

        let queue_name = manager.get_queue_name(&task);
        assert!(queue_name.starts_with("my_queue:"));

        // Should be consistent
        let queue_name2 = manager.get_queue_name(&task);
        assert_eq!(queue_name, queue_name2);
    }

    #[test]
    fn test_partition_manager_get_queue_name_by_id() {
        let manager = PartitionManager::new(4, PartitionStrategy::ById, "queue");

        assert_eq!(manager.get_queue_name_by_id(0), "queue:0");
        assert_eq!(manager.get_queue_name_by_id(1), "queue:1");
        assert_eq!(manager.get_queue_name_by_id(2), "queue:2");
        assert_eq!(manager.get_queue_name_by_id(3), "queue:3");
    }

    #[test]
    fn test_partition_manager_get_all_queue_names() {
        let manager = PartitionManager::new(4, PartitionStrategy::ById, "queue");
        let queue_names = manager.get_all_queue_names();

        assert_eq!(queue_names.len(), 4);
        assert_eq!(
            queue_names,
            vec!["queue:0", "queue:1", "queue:2", "queue:3"]
        );
    }

    #[test]
    fn test_partition_manager_with_algorithm() {
        let manager = PartitionManager::with_algorithm(
            4,
            PartitionStrategy::ByName,
            "queue",
            HashAlgorithm::XxHash,
        );

        let task = create_test_task("550e8400-e29b-41d4-a716-446655440000", "test_task");
        let partition_id = manager.get_partition_id(&task);

        assert!(partition_id < 4);
    }

    #[test]
    fn test_partition_distribution() {
        let manager = PartitionManager::new(4, PartitionStrategy::ById, "queue");

        let mut partition_counts: HashMap<usize, usize> = HashMap::new();

        // Create 1000 tasks and check distribution
        for i in 0..1000 {
            let task_id = format!("550e8400-e29b-41d4-a716-{:012}", i);
            let task = create_test_task(&task_id, "test_task");
            let partition_id = manager.get_partition_id(&task);

            *partition_counts.entry(partition_id).or_insert(0) += 1;
        }

        // All partitions should have some tasks
        assert_eq!(partition_counts.len(), 4);

        // Check that distribution is reasonable (not perfect, but should be balanced)
        for count in partition_counts.values() {
            // Each partition should have roughly 250 tasks (1000/4)
            // Allow 50% variance
            assert!(*count > 125 && *count < 375);
        }
    }
}
