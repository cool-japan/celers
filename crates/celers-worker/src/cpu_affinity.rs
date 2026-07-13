//! CPU affinity support for worker threads
//!
//! This module provides CPU affinity configuration for worker threads, allowing you to:
//! - Pin workers to specific CPU cores for better cache locality
//! - Implement NUMA-aware task placement
//! - Optimize thread pool performance by reducing context switches
//!
//! # Example
//!
//! ```
//! use celers_worker::cpu_affinity::{AffinityConfig, AffinityPolicy, NumaNode};
//!
//! // Pin each worker to a specific core
//! let config = AffinityConfig::default()
//!     .with_policy(AffinityPolicy::PerWorker)
//!     .with_cores(vec![0, 1, 2, 3]);
//!
//! // Use NUMA-aware placement
//! let numa_config = AffinityConfig::default()
//!     .with_policy(AffinityPolicy::NumaAware)
//!     .with_numa_nodes(vec![
//!         NumaNode { id: 0, cores: vec![0, 1, 2, 3] },
//!         NumaNode { id: 1, cores: vec![4, 5, 6, 7] },
//!     ]);
//! ```

use serde::{Deserialize, Serialize};
use std::fmt;

/// NUMA node configuration
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct NumaNode {
    /// NUMA node ID
    pub id: usize,
    /// CPU cores in this NUMA node
    pub cores: Vec<usize>,
}

impl NumaNode {
    /// Create a new NUMA node
    pub fn new(id: usize, cores: Vec<usize>) -> Self {
        Self { id, cores }
    }

    /// Check if this NUMA node contains a specific core
    pub fn contains_core(&self, core: usize) -> bool {
        self.cores.contains(&core)
    }

    /// Get the number of cores in this NUMA node
    pub fn core_count(&self) -> usize {
        self.cores.len()
    }

    /// Get the core at the specified index
    pub fn get_core(&self, index: usize) -> Option<usize> {
        self.cores.get(index).copied()
    }
}

impl fmt::Display for NumaNode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "NUMA node {} (cores: {:?})", self.id, self.cores)
    }
}

/// CPU affinity policy
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
pub enum AffinityPolicy {
    /// No CPU affinity (default)
    #[default]
    None,
    /// Pin each worker to a specific core
    PerWorker,
    /// Allow workers to use any core in a set
    Shared,
    /// NUMA-aware placement (workers prefer cores on the same NUMA node)
    NumaAware,
}

impl fmt::Display for AffinityPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => write!(f, "None"),
            Self::PerWorker => write!(f, "PerWorker"),
            Self::Shared => write!(f, "Shared"),
            Self::NumaAware => write!(f, "NumaAware"),
        }
    }
}

/// CPU affinity configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AffinityConfig {
    /// Affinity policy
    policy: AffinityPolicy,
    /// CPU cores to use (for PerWorker and Shared policies)
    cores: Vec<usize>,
    /// NUMA nodes (for NumaAware policy)
    numa_nodes: Vec<NumaNode>,
    /// Whether to enable strict affinity (fail if cannot set affinity)
    strict: bool,
}

impl Default for AffinityConfig {
    fn default() -> Self {
        Self {
            policy: AffinityPolicy::None,
            cores: Vec::new(),
            numa_nodes: Vec::new(),
            strict: false,
        }
    }
}

/// Parse a Linux cpulist string (e.g. `"0-3,6,8-10"`) into a sorted, deduplicated
/// `Vec<usize>`.  Single integers and inclusive ranges separated by commas are supported.
#[cfg(target_os = "linux")]
fn parse_cpulist(s: &str) -> Vec<usize> {
    let mut cores = Vec::new();
    for piece in s.trim().split(',') {
        let piece = piece.trim();
        if piece.is_empty() {
            continue;
        }
        if let Some((a, b)) = piece.split_once('-') {
            if let (Ok(start), Ok(end)) = (a.parse::<usize>(), b.parse::<usize>()) {
                cores.extend(start..=end);
            }
        } else if let Ok(n) = piece.parse::<usize>() {
            cores.push(n);
        }
    }
    cores.sort_unstable();
    cores.dedup();
    cores
}

/// Read the real NUMA topology from `/sys/devices/system/node`.
/// Returns an empty `Vec` when the path does not exist or is not readable.
#[cfg(target_os = "linux")]
fn read_numa_topology() -> Vec<NumaNode> {
    let sys_node = std::path::Path::new("/sys/devices/system/node");
    if !sys_node.exists() {
        return Vec::new();
    }

    let mut nodes: Vec<NumaNode> = Vec::new();

    let Ok(entries) = std::fs::read_dir(sys_node) else {
        return Vec::new();
    };

    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();
        // Only process directories named "nodeN" where N is a non-negative integer
        let Some(id_str) = name.strip_prefix("node") else {
            continue;
        };
        let Ok(id) = id_str.parse::<usize>() else {
            continue;
        };

        let cpulist_path = entry.path().join("cpulist");
        let Ok(content) = std::fs::read_to_string(&cpulist_path) else {
            continue;
        };
        let cores = parse_cpulist(&content);
        if cores.is_empty() {
            continue;
        }

        nodes.push(NumaNode::new(id, cores));
    }

    // Sort by NUMA node ID for deterministic output
    nodes.sort_by_key(|n| n.id);
    nodes
}

impl AffinityConfig {
    /// Create a new affinity configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the affinity policy
    pub fn with_policy(mut self, policy: AffinityPolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Set the CPU cores to use
    pub fn with_cores(mut self, cores: Vec<usize>) -> Self {
        self.cores = cores;
        self
    }

    /// Set the NUMA nodes
    pub fn with_numa_nodes(mut self, numa_nodes: Vec<NumaNode>) -> Self {
        self.numa_nodes = numa_nodes;
        self
    }

    /// Set strict affinity mode
    pub fn with_strict(mut self, strict: bool) -> Self {
        self.strict = strict;
        self
    }

    /// Get the affinity policy
    pub fn policy(&self) -> AffinityPolicy {
        self.policy
    }

    /// Get the CPU cores
    pub fn cores(&self) -> &[usize] {
        &self.cores
    }

    /// Get the NUMA nodes
    pub fn numa_nodes(&self) -> &[NumaNode] {
        &self.numa_nodes
    }

    /// Check if strict mode is enabled
    pub fn is_strict(&self) -> bool {
        self.strict
    }

    /// Check if the configuration is valid
    pub fn is_valid(&self) -> bool {
        match self.policy {
            AffinityPolicy::None => true,
            AffinityPolicy::PerWorker | AffinityPolicy::Shared => !self.cores.is_empty(),
            AffinityPolicy::NumaAware => {
                !self.numa_nodes.is_empty()
                    && self.numa_nodes.iter().all(|node| !node.cores.is_empty())
            }
        }
    }

    /// Get the core for a specific worker index
    pub fn get_core_for_worker(&self, worker_index: usize) -> Option<usize> {
        match self.policy {
            AffinityPolicy::None => None,
            AffinityPolicy::PerWorker => {
                if self.cores.is_empty() {
                    None
                } else {
                    Some(self.cores[worker_index % self.cores.len()])
                }
            }
            AffinityPolicy::Shared => None, // Workers share all cores
            AffinityPolicy::NumaAware => {
                // Distribute workers across NUMA nodes
                if self.numa_nodes.is_empty() {
                    None
                } else {
                    let node = &self.numa_nodes[worker_index % self.numa_nodes.len()];
                    if node.cores.is_empty() {
                        None
                    } else {
                        let core_idx = (worker_index / self.numa_nodes.len()) % node.cores.len();
                        Some(node.cores[core_idx])
                    }
                }
            }
        }
    }

    /// Get all cores that can be used by a worker
    pub fn get_cores_for_worker(&self, worker_index: usize) -> Vec<usize> {
        match self.policy {
            AffinityPolicy::None => Vec::new(),
            AffinityPolicy::PerWorker => {
                if let Some(core) = self.get_core_for_worker(worker_index) {
                    vec![core]
                } else {
                    Vec::new()
                }
            }
            AffinityPolicy::Shared => self.cores.clone(),
            AffinityPolicy::NumaAware => {
                if self.numa_nodes.is_empty() {
                    Vec::new()
                } else {
                    let node = &self.numa_nodes[worker_index % self.numa_nodes.len()];
                    node.cores.clone()
                }
            }
        }
    }

    /// Get the NUMA node for a specific worker index
    pub fn get_numa_node_for_worker(&self, worker_index: usize) -> Option<&NumaNode> {
        if self.policy == AffinityPolicy::NumaAware && !self.numa_nodes.is_empty() {
            Some(&self.numa_nodes[worker_index % self.numa_nodes.len()])
        } else {
            None
        }
    }

    /// Create a configuration for automatic core detection
    pub fn auto_detect() -> Self {
        let num_cpus = num_cpus::get();
        let cores = (0..num_cpus).collect();

        Self {
            policy: AffinityPolicy::PerWorker,
            cores,
            numa_nodes: Vec::new(),
            strict: false,
        }
    }

    /// Create a configuration for NUMA-aware placement with auto-detection
    #[cfg(target_os = "linux")]
    pub fn auto_detect_numa() -> Self {
        let numa_nodes = read_numa_topology();
        if numa_nodes.is_empty() {
            // No NUMA or /sys not readable — fall back to single-node auto_detect
            return Self::auto_detect();
        }
        Self {
            policy: AffinityPolicy::NumaAware,
            cores: Vec::new(),
            numa_nodes,
            strict: false,
        }
    }

    #[cfg(not(target_os = "linux"))]
    pub fn auto_detect_numa() -> Self {
        // Fall back to auto_detect on non-Linux systems
        Self::auto_detect()
    }
}

impl fmt::Display for AffinityConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "AffinityConfig(policy={}, cores={:?}, numa_nodes={}, strict={})",
            self.policy,
            self.cores,
            self.numa_nodes.len(),
            self.strict
        )
    }
}

/// Apply CPU affinity to the current thread
#[cfg(target_os = "linux")]
pub fn set_thread_affinity(cores: &[usize]) -> Result<(), String> {
    use std::mem;

    if cores.is_empty() {
        return Ok(());
    }

    // Use libc to set CPU affinity
    unsafe {
        let mut cpu_set: libc::cpu_set_t = mem::zeroed();
        libc::CPU_ZERO(&mut cpu_set);

        for &core in cores {
            if core >= libc::CPU_SETSIZE as usize {
                return Err(format!("Core {} is out of range", core));
            }
            libc::CPU_SET(core, &mut cpu_set);
        }

        let result = libc::sched_setaffinity(
            0, // current thread
            mem::size_of::<libc::cpu_set_t>(),
            &cpu_set,
        );

        if result != 0 {
            Err(format!(
                "Failed to set CPU affinity: {}",
                std::io::Error::last_os_error()
            ))
        } else {
            Ok(())
        }
    }
}

#[cfg(not(target_os = "linux"))]
pub fn set_thread_affinity(_cores: &[usize]) -> Result<(), String> {
    // CPU affinity is not supported on this platform
    Err("CPU affinity is not supported on this platform".to_string())
}

/// Get the current thread's CPU affinity
#[cfg(target_os = "linux")]
pub fn get_thread_affinity() -> Result<Vec<usize>, String> {
    use std::mem;

    unsafe {
        let mut cpu_set: libc::cpu_set_t = mem::zeroed();

        let result = libc::sched_getaffinity(
            0, // current thread
            mem::size_of::<libc::cpu_set_t>(),
            &mut cpu_set,
        );

        if result != 0 {
            return Err(format!(
                "Failed to get CPU affinity: {}",
                std::io::Error::last_os_error()
            ));
        }

        let mut cores = Vec::new();
        for i in 0..libc::CPU_SETSIZE as usize {
            if libc::CPU_ISSET(i, &cpu_set) {
                cores.push(i);
            }
        }

        Ok(cores)
    }
}

#[cfg(not(target_os = "linux"))]
pub fn get_thread_affinity() -> Result<Vec<usize>, String> {
    Err("CPU affinity is not supported on this platform".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_numa_node() {
        let node = NumaNode::new(0, vec![0, 1, 2, 3]);
        assert_eq!(node.id, 0);
        assert_eq!(node.cores, vec![0, 1, 2, 3]);
        assert!(node.contains_core(2));
        assert!(!node.contains_core(4));
        assert_eq!(node.core_count(), 4);
        assert_eq!(node.get_core(1), Some(1));
        assert_eq!(node.get_core(10), None);
    }

    #[test]
    fn test_affinity_policy_default() {
        let policy = AffinityPolicy::default();
        assert_eq!(policy, AffinityPolicy::None);
    }

    #[test]
    fn test_affinity_config_default() {
        let config = AffinityConfig::default();
        assert_eq!(config.policy(), AffinityPolicy::None);
        assert!(config.cores().is_empty());
        assert!(config.numa_nodes().is_empty());
        assert!(!config.is_strict());
    }

    #[test]
    fn test_affinity_config_builder() {
        let config = AffinityConfig::new()
            .with_policy(AffinityPolicy::PerWorker)
            .with_cores(vec![0, 1, 2, 3])
            .with_strict(true);

        assert_eq!(config.policy(), AffinityPolicy::PerWorker);
        assert_eq!(config.cores(), &[0, 1, 2, 3]);
        assert!(config.is_strict());
    }

    #[test]
    fn test_affinity_config_validation() {
        // None policy is always valid
        let config = AffinityConfig::new();
        assert!(config.is_valid());

        // PerWorker requires cores
        let config = AffinityConfig::new().with_policy(AffinityPolicy::PerWorker);
        assert!(!config.is_valid());

        let config = config.with_cores(vec![0, 1]);
        assert!(config.is_valid());

        // NumaAware requires NUMA nodes with cores
        let config = AffinityConfig::new().with_policy(AffinityPolicy::NumaAware);
        assert!(!config.is_valid());

        let config = config.with_numa_nodes(vec![NumaNode::new(0, vec![0, 1])]);
        assert!(config.is_valid());
    }

    #[test]
    fn test_get_core_for_worker_per_worker() {
        let config = AffinityConfig::new()
            .with_policy(AffinityPolicy::PerWorker)
            .with_cores(vec![0, 1, 2, 3]);

        assert_eq!(config.get_core_for_worker(0), Some(0));
        assert_eq!(config.get_core_for_worker(1), Some(1));
        assert_eq!(config.get_core_for_worker(2), Some(2));
        assert_eq!(config.get_core_for_worker(3), Some(3));
        // Wrap around
        assert_eq!(config.get_core_for_worker(4), Some(0));
    }

    #[test]
    fn test_get_core_for_worker_shared() {
        let config = AffinityConfig::new()
            .with_policy(AffinityPolicy::Shared)
            .with_cores(vec![0, 1, 2, 3]);

        // Shared policy doesn't assign specific cores
        assert_eq!(config.get_core_for_worker(0), None);
        assert_eq!(config.get_core_for_worker(1), None);
    }

    #[test]
    fn test_get_core_for_worker_numa_aware() {
        let config = AffinityConfig::new()
            .with_policy(AffinityPolicy::NumaAware)
            .with_numa_nodes(vec![
                NumaNode::new(0, vec![0, 1, 2, 3]),
                NumaNode::new(1, vec![4, 5, 6, 7]),
            ]);

        // Workers alternate between NUMA nodes and cycle through cores within each node
        // Worker 0: NUMA node 0, core 0
        assert_eq!(config.get_core_for_worker(0), Some(0));
        // Worker 1: NUMA node 1, core 4
        assert_eq!(config.get_core_for_worker(1), Some(4));
        // Worker 2: NUMA node 0, core 1
        assert_eq!(config.get_core_for_worker(2), Some(1));
        // Worker 3: NUMA node 1, core 5
        assert_eq!(config.get_core_for_worker(3), Some(5));
    }

    #[test]
    fn test_get_cores_for_worker() {
        let config = AffinityConfig::new()
            .with_policy(AffinityPolicy::PerWorker)
            .with_cores(vec![0, 1, 2, 3]);

        assert_eq!(config.get_cores_for_worker(0), vec![0]);
        assert_eq!(config.get_cores_for_worker(1), vec![1]);

        let config = AffinityConfig::new()
            .with_policy(AffinityPolicy::Shared)
            .with_cores(vec![0, 1, 2, 3]);

        assert_eq!(config.get_cores_for_worker(0), vec![0, 1, 2, 3]);
        assert_eq!(config.get_cores_for_worker(1), vec![0, 1, 2, 3]);
    }

    #[test]
    fn test_get_numa_node_for_worker() {
        let node0 = NumaNode::new(0, vec![0, 1, 2, 3]);
        let node1 = NumaNode::new(1, vec![4, 5, 6, 7]);

        let config = AffinityConfig::new()
            .with_policy(AffinityPolicy::NumaAware)
            .with_numa_nodes(vec![node0.clone(), node1.clone()]);

        assert_eq!(config.get_numa_node_for_worker(0), Some(&node0));
        assert_eq!(config.get_numa_node_for_worker(1), Some(&node1));
        assert_eq!(config.get_numa_node_for_worker(2), Some(&node0));
    }

    #[test]
    fn test_auto_detect() {
        let config = AffinityConfig::auto_detect();
        assert_eq!(config.policy(), AffinityPolicy::PerWorker);
        assert!(!config.cores().is_empty());
        assert!(config.is_valid());
    }

    #[test]
    fn test_display() {
        let config = AffinityConfig::new()
            .with_policy(AffinityPolicy::PerWorker)
            .with_cores(vec![0, 1, 2, 3]);

        let display = format!("{}", config);
        assert!(display.contains("PerWorker"));
        assert!(display.contains("[0, 1, 2, 3]"));
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn test_parse_cpulist_single() {
        assert_eq!(parse_cpulist("3"), vec![3]);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn test_parse_cpulist_range() {
        assert_eq!(parse_cpulist("0-3"), vec![0, 1, 2, 3]);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn test_parse_cpulist_mixed() {
        assert_eq!(parse_cpulist("0-3,6,8-10"), vec![0, 1, 2, 3, 6, 8, 9, 10]);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn test_parse_cpulist_dedup() {
        assert_eq!(parse_cpulist("0,0,1"), vec![0, 1]);
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn test_read_numa_topology_returns_something() {
        // On Linux, /sys/devices/system/node always exists; should find at least node0
        let nodes = read_numa_topology();
        if std::path::Path::new("/sys/devices/system/node/node0/cpulist").exists() {
            assert!(!nodes.is_empty(), "Expected at least one NUMA node");
            assert!(
                !nodes[0].cores.is_empty(),
                "node0 must have at least one CPU"
            );
        }
    }

    #[test]
    #[cfg(target_os = "linux")]
    fn test_auto_detect_numa_valid() {
        let config = AffinityConfig::auto_detect_numa();
        // Must be either NumaAware (with nodes) or PerWorker (fallback)
        match config.policy() {
            AffinityPolicy::NumaAware => {
                let total_cores: usize = config.numa_nodes().iter().map(|n| n.cores.len()).sum();
                assert!(
                    total_cores > 0,
                    "NumaAware config must expose at least one core"
                );
            }
            AffinityPolicy::PerWorker => {
                // Fallback branch — auto_detect() was used; cores must be non-empty
                assert!(
                    !config.cores().is_empty(),
                    "PerWorker fallback must have cores"
                );
            }
            other => panic!("Unexpected policy from auto_detect_numa: {:?}", other),
        }
    }
}
