//! Worker routing and tagging system
//!
//! This module provides a flexible routing system that allows workers to declare
//! what types of tasks they can handle through tags, and enables task routing
//! based on worker capabilities.
//!
//! # Use Cases
//!
//! - **Specialized Workers**: Dedicated workers for CPU-intensive, GPU, or I/O tasks
//! - **Environment Separation**: Development/staging/production workers
//! - **Resource Isolation**: High-memory tasks routed to specific workers
//! - **Geographic Distribution**: Tasks routed based on region or data locality
//!
//! # Example
//!
//! ```rust
//! use celers_worker::routing::{WorkerTags, RoutingStrategy};
//!
//! let tags = WorkerTags::new()
//!     .with_tag("cpu-intensive")
//!     .with_tag("region:us-west")
//!     .with_capability("gpu", "nvidia-a100");
//!
//! assert!(tags.has_tag("cpu-intensive"));
//! assert!(tags.matches_any(&["cpu-intensive", "io-bound"]));
//! ```

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Worker tags for routing and task matching
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkerTags {
    /// Simple tags (e.g., "cpu-intensive", "gpu-enabled")
    tags: HashSet<String>,

    /// Key-value capabilities (e.g., "region" => "us-west", "gpu" => "nvidia-a100")
    capabilities: HashMap<String, String>,

    /// Task type allowlist (if empty, accepts all tasks)
    task_types: HashSet<String>,

    /// Task type denylist (takes precedence over allowlist)
    excluded_task_types: HashSet<String>,
}

impl WorkerTags {
    /// Create a new empty set of worker tags
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a simple tag
    pub fn with_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.insert(tag.into());
        self
    }

    /// Add a key-value capability
    pub fn with_capability(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.capabilities.insert(key.into(), value.into());
        self
    }

    /// Add an allowed task type
    pub fn with_task_type(mut self, task_type: impl Into<String>) -> Self {
        self.task_types.insert(task_type.into());
        self
    }

    /// Add an excluded task type
    pub fn with_excluded_task_type(mut self, task_type: impl Into<String>) -> Self {
        self.excluded_task_types.insert(task_type.into());
        self
    }

    /// Check if worker has a specific tag
    pub fn has_tag(&self, tag: &str) -> bool {
        self.tags.contains(tag)
    }

    /// Check if worker has a specific capability
    pub fn has_capability(&self, key: &str) -> bool {
        self.capabilities.contains_key(key)
    }

    /// Get the value of a capability
    pub fn get_capability(&self, key: &str) -> Option<&str> {
        self.capabilities.get(key).map(|s| s.as_str())
    }

    /// Check if worker matches a capability key-value pair
    pub fn matches_capability(&self, key: &str, value: &str) -> bool {
        self.capabilities.get(key).is_some_and(|v| v == value)
    }

    /// Check if worker has any of the given tags
    pub fn matches_any(&self, tags: &[&str]) -> bool {
        tags.iter().any(|tag| self.has_tag(tag))
    }

    /// Check if worker has all of the given tags
    pub fn matches_all(&self, tags: &[&str]) -> bool {
        tags.iter().all(|tag| self.has_tag(tag))
    }

    /// Check if worker can handle a specific task type
    pub fn can_handle_task(&self, task_type: &str) -> bool {
        // Check denylist first
        if self.excluded_task_types.contains(task_type) {
            return false;
        }

        // If allowlist is empty, accept all (except denied)
        if self.task_types.is_empty() {
            return true;
        }

        // Check allowlist
        self.task_types.contains(task_type)
    }

    /// Get all tags
    pub fn tags(&self) -> &HashSet<String> {
        &self.tags
    }

    /// Get all capabilities
    pub fn capabilities(&self) -> &HashMap<String, String> {
        &self.capabilities
    }

    /// Get allowed task types
    pub fn task_types(&self) -> &HashSet<String> {
        &self.task_types
    }

    /// Get excluded task types
    pub fn excluded_task_types(&self) -> &HashSet<String> {
        &self.excluded_task_types
    }

    /// Check if worker accepts all task types
    pub fn accepts_all_tasks(&self) -> bool {
        self.task_types.is_empty() && self.excluded_task_types.is_empty()
    }

    /// Get the number of tags
    pub fn tag_count(&self) -> usize {
        self.tags.len()
    }

    /// Get the number of capabilities
    pub fn capability_count(&self) -> usize {
        self.capabilities.len()
    }

    /// Merge tags from another WorkerTags instance
    pub fn merge(&mut self, other: &WorkerTags) {
        self.tags.extend(other.tags.iter().cloned());
        self.capabilities.extend(
            other
                .capabilities
                .iter()
                .map(|(k, v)| (k.clone(), v.clone())),
        );
        self.task_types.extend(other.task_types.iter().cloned());
        self.excluded_task_types
            .extend(other.excluded_task_types.iter().cloned());
    }

    /// Clear all tags and capabilities
    pub fn clear(&mut self) {
        self.tags.clear();
        self.capabilities.clear();
        self.task_types.clear();
        self.excluded_task_types.clear();
    }
}

/// Routing strategy for task assignment
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum RoutingStrategy {
    /// Route tasks only to workers with matching tags (strict)
    Strict,

    /// Route tasks to any available worker if no match found (lenient)
    #[default]
    Lenient,

    /// Route based on task type only (ignore other tags)
    TaskTypeOnly,
}

impl RoutingStrategy {
    /// Check if strategy is strict
    pub fn is_strict(&self) -> bool {
        matches!(self, RoutingStrategy::Strict)
    }

    /// Check if strategy is lenient
    pub fn is_lenient(&self) -> bool {
        matches!(self, RoutingStrategy::Lenient)
    }

    /// Check if strategy is task-type-only
    pub fn is_task_type_only(&self) -> bool {
        matches!(self, RoutingStrategy::TaskTypeOnly)
    }
}

impl std::fmt::Display for RoutingStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RoutingStrategy::Strict => write!(f, "Strict"),
            RoutingStrategy::Lenient => write!(f, "Lenient"),
            RoutingStrategy::TaskTypeOnly => write!(f, "TaskTypeOnly"),
        }
    }
}

/// Task routing requirements
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct TaskRoutingRequirements {
    /// Required tags (worker must have ALL of these)
    required_tags: HashSet<String>,

    /// Preferred tags (worker should have at least one)
    preferred_tags: HashSet<String>,

    /// Required capabilities (worker must match ALL)
    required_capabilities: HashMap<String, String>,

    /// Preferred capabilities (worker should match at least one)
    preferred_capabilities: HashMap<String, String>,
}

impl TaskRoutingRequirements {
    /// Create new empty requirements
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a required tag
    pub fn require_tag(mut self, tag: impl Into<String>) -> Self {
        self.required_tags.insert(tag.into());
        self
    }

    /// Add a preferred tag
    pub fn prefer_tag(mut self, tag: impl Into<String>) -> Self {
        self.preferred_tags.insert(tag.into());
        self
    }

    /// Add a required capability
    pub fn require_capability(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.required_capabilities.insert(key.into(), value.into());
        self
    }

    /// Add a preferred capability
    pub fn prefer_capability(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.preferred_capabilities.insert(key.into(), value.into());
        self
    }

    /// Check if worker satisfies all required conditions
    pub fn is_satisfied_by(&self, worker_tags: &WorkerTags) -> bool {
        // Check required tags
        if !self
            .required_tags
            .iter()
            .all(|tag| worker_tags.has_tag(tag))
        {
            return false;
        }

        // Check required capabilities
        if !self
            .required_capabilities
            .iter()
            .all(|(k, v)| worker_tags.matches_capability(k, v))
        {
            return false;
        }

        true
    }

    /// Calculate match score (0.0 = no match, 1.0 = perfect match)
    pub fn match_score(&self, worker_tags: &WorkerTags) -> f64 {
        if !self.is_satisfied_by(worker_tags) {
            return 0.0;
        }

        let mut score = 0.5; // Base score for satisfying requirements

        // Bonus for matching preferred tags
        let preferred_tag_matches = self
            .preferred_tags
            .iter()
            .filter(|tag| worker_tags.has_tag(tag))
            .count();

        if !self.preferred_tags.is_empty() {
            score += 0.25 * (preferred_tag_matches as f64 / self.preferred_tags.len() as f64);
        }

        // Bonus for matching preferred capabilities
        let preferred_cap_matches = self
            .preferred_capabilities
            .iter()
            .filter(|(k, v)| worker_tags.matches_capability(k, v))
            .count();

        if !self.preferred_capabilities.is_empty() {
            score +=
                0.25 * (preferred_cap_matches as f64 / self.preferred_capabilities.len() as f64);
        }

        score.min(1.0)
    }

    /// Check if there are any requirements
    pub fn has_requirements(&self) -> bool {
        !self.required_tags.is_empty() || !self.required_capabilities.is_empty()
    }

    /// Check if there are any preferences
    pub fn has_preferences(&self) -> bool {
        !self.preferred_tags.is_empty() || !self.preferred_capabilities.is_empty()
    }

    /// Get required tags
    pub fn required_tags(&self) -> &HashSet<String> {
        &self.required_tags
    }

    /// Get preferred tags
    pub fn preferred_tags(&self) -> &HashSet<String> {
        &self.preferred_tags
    }

    /// Get required capabilities
    pub fn required_capabilities(&self) -> &HashMap<String, String> {
        &self.required_capabilities
    }

    /// Get preferred capabilities
    pub fn preferred_capabilities(&self) -> &HashMap<String, String> {
        &self.preferred_capabilities
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_tags_new() {
        let tags = WorkerTags::new();
        assert_eq!(tags.tag_count(), 0);
        assert_eq!(tags.capability_count(), 0);
        assert!(tags.accepts_all_tasks());
    }

    #[test]
    fn test_worker_tags_with_tag() {
        let tags = WorkerTags::new()
            .with_tag("cpu-intensive")
            .with_tag("gpu-enabled");

        assert!(tags.has_tag("cpu-intensive"));
        assert!(tags.has_tag("gpu-enabled"));
        assert!(!tags.has_tag("io-bound"));
        assert_eq!(tags.tag_count(), 2);
    }

    #[test]
    fn test_worker_tags_with_capability() {
        let tags = WorkerTags::new()
            .with_capability("region", "us-west")
            .with_capability("gpu", "nvidia-a100");

        assert!(tags.has_capability("region"));
        assert!(tags.has_capability("gpu"));
        assert!(!tags.has_capability("cpu"));

        assert_eq!(tags.get_capability("region"), Some("us-west"));
        assert_eq!(tags.get_capability("gpu"), Some("nvidia-a100"));
        assert_eq!(tags.get_capability("cpu"), None);

        assert!(tags.matches_capability("region", "us-west"));
        assert!(!tags.matches_capability("region", "us-east"));
    }

    #[test]
    fn test_worker_tags_matches_any() {
        let tags = WorkerTags::new()
            .with_tag("cpu-intensive")
            .with_tag("gpu-enabled");

        assert!(tags.matches_any(&["cpu-intensive", "io-bound"]));
        assert!(tags.matches_any(&["gpu-enabled"]));
        assert!(!tags.matches_any(&["io-bound", "network"]));
    }

    #[test]
    fn test_worker_tags_matches_all() {
        let tags = WorkerTags::new()
            .with_tag("cpu-intensive")
            .with_tag("gpu-enabled");

        assert!(tags.matches_all(&["cpu-intensive", "gpu-enabled"]));
        assert!(tags.matches_all(&["cpu-intensive"]));
        assert!(!tags.matches_all(&["cpu-intensive", "io-bound"]));
    }

    #[test]
    fn test_worker_tags_task_type_handling() {
        let tags = WorkerTags::new()
            .with_task_type("image_processing")
            .with_task_type("video_encoding");

        assert!(tags.can_handle_task("image_processing"));
        assert!(tags.can_handle_task("video_encoding"));
        assert!(!tags.can_handle_task("data_analysis"));
        assert!(!tags.accepts_all_tasks());
    }

    #[test]
    fn test_worker_tags_excluded_task_types() {
        let tags = WorkerTags::new().with_excluded_task_type("heavy_computation");

        assert!(!tags.can_handle_task("heavy_computation"));
        assert!(tags.can_handle_task("light_task"));
    }

    #[test]
    fn test_worker_tags_allowlist_and_denylist() {
        let tags = WorkerTags::new()
            .with_task_type("image_processing")
            .with_task_type("video_encoding")
            .with_excluded_task_type("image_processing"); // Denylist takes precedence

        assert!(!tags.can_handle_task("image_processing")); // Denied
        assert!(tags.can_handle_task("video_encoding")); // Allowed
        assert!(!tags.can_handle_task("other_task")); // Not in allowlist
    }

    #[test]
    fn test_worker_tags_merge() {
        let mut tags1 = WorkerTags::new()
            .with_tag("cpu-intensive")
            .with_capability("region", "us-west");

        let tags2 = WorkerTags::new()
            .with_tag("gpu-enabled")
            .with_capability("gpu", "nvidia-a100");

        tags1.merge(&tags2);

        assert!(tags1.has_tag("cpu-intensive"));
        assert!(tags1.has_tag("gpu-enabled"));
        assert_eq!(tags1.get_capability("region"), Some("us-west"));
        assert_eq!(tags1.get_capability("gpu"), Some("nvidia-a100"));
    }

    #[test]
    fn test_worker_tags_clear() {
        let mut tags = WorkerTags::new()
            .with_tag("cpu-intensive")
            .with_capability("region", "us-west");

        assert_eq!(tags.tag_count(), 1);
        assert_eq!(tags.capability_count(), 1);

        tags.clear();

        assert_eq!(tags.tag_count(), 0);
        assert_eq!(tags.capability_count(), 0);
    }

    #[test]
    fn test_routing_strategy_predicates() {
        assert!(RoutingStrategy::Strict.is_strict());
        assert!(!RoutingStrategy::Strict.is_lenient());
        assert!(!RoutingStrategy::Strict.is_task_type_only());

        assert!(!RoutingStrategy::Lenient.is_strict());
        assert!(RoutingStrategy::Lenient.is_lenient());
        assert!(!RoutingStrategy::Lenient.is_task_type_only());

        assert!(!RoutingStrategy::TaskTypeOnly.is_strict());
        assert!(!RoutingStrategy::TaskTypeOnly.is_lenient());
        assert!(RoutingStrategy::TaskTypeOnly.is_task_type_only());
    }

    #[test]
    fn test_routing_strategy_display() {
        assert_eq!(format!("{}", RoutingStrategy::Strict), "Strict");
        assert_eq!(format!("{}", RoutingStrategy::Lenient), "Lenient");
        assert_eq!(format!("{}", RoutingStrategy::TaskTypeOnly), "TaskTypeOnly");
    }

    #[test]
    fn test_task_routing_requirements_basic() {
        let reqs = TaskRoutingRequirements::new()
            .require_tag("gpu-enabled")
            .require_capability("region", "us-west");

        let worker = WorkerTags::new()
            .with_tag("gpu-enabled")
            .with_capability("region", "us-west");

        assert!(reqs.is_satisfied_by(&worker));
    }

    #[test]
    fn test_task_routing_requirements_missing_tag() {
        let reqs = TaskRoutingRequirements::new().require_tag("gpu-enabled");

        let worker = WorkerTags::new().with_tag("cpu-intensive");

        assert!(!reqs.is_satisfied_by(&worker));
    }

    #[test]
    fn test_task_routing_requirements_missing_capability() {
        let reqs = TaskRoutingRequirements::new().require_capability("region", "us-west");

        let worker = WorkerTags::new().with_capability("region", "us-east");

        assert!(!reqs.is_satisfied_by(&worker));
    }

    #[test]
    fn test_task_routing_requirements_match_score() {
        let reqs = TaskRoutingRequirements::new()
            .require_tag("gpu-enabled")
            .prefer_tag("high-memory")
            .prefer_capability("region", "us-west");

        // Worker with only required tags (base score 0.5)
        let worker1 = WorkerTags::new().with_tag("gpu-enabled");
        assert_eq!(reqs.match_score(&worker1), 0.5);

        // Worker with required + one preferred tag (0.5 + 0.125 = 0.625)
        let worker2 = WorkerTags::new()
            .with_tag("gpu-enabled")
            .with_tag("high-memory");
        assert_eq!(reqs.match_score(&worker2), 0.75);

        // Worker with required + one preferred capability (0.5 + 0.25 = 0.75)
        let worker3 = WorkerTags::new()
            .with_tag("gpu-enabled")
            .with_capability("region", "us-west");
        assert_eq!(reqs.match_score(&worker3), 0.75);

        // Worker with all requirements and preferences (perfect match)
        let worker4 = WorkerTags::new()
            .with_tag("gpu-enabled")
            .with_tag("high-memory")
            .with_capability("region", "us-west");
        assert_eq!(reqs.match_score(&worker4), 1.0);

        // Worker without required tags (no match)
        let worker5 = WorkerTags::new().with_tag("cpu-intensive");
        assert_eq!(reqs.match_score(&worker5), 0.0);
    }

    #[test]
    fn test_task_routing_requirements_predicates() {
        let empty_reqs = TaskRoutingRequirements::new();
        assert!(!empty_reqs.has_requirements());
        assert!(!empty_reqs.has_preferences());

        let reqs_with_required = TaskRoutingRequirements::new().require_tag("gpu-enabled");
        assert!(reqs_with_required.has_requirements());
        assert!(!reqs_with_required.has_preferences());

        let reqs_with_preferred = TaskRoutingRequirements::new().prefer_tag("high-memory");
        assert!(!reqs_with_preferred.has_requirements());
        assert!(reqs_with_preferred.has_preferences());

        let reqs_with_both = TaskRoutingRequirements::new()
            .require_tag("gpu-enabled")
            .prefer_tag("high-memory");
        assert!(reqs_with_both.has_requirements());
        assert!(reqs_with_both.has_preferences());
    }

    #[test]
    fn test_task_routing_requirements_getters() {
        let reqs = TaskRoutingRequirements::new()
            .require_tag("gpu-enabled")
            .prefer_tag("high-memory")
            .require_capability("region", "us-west")
            .prefer_capability("gpu", "nvidia-a100");

        assert!(reqs.required_tags().contains("gpu-enabled"));
        assert!(reqs.preferred_tags().contains("high-memory"));
        assert_eq!(
            reqs.required_capabilities().get("region"),
            Some(&"us-west".to_string())
        );
        assert_eq!(
            reqs.preferred_capabilities().get("gpu"),
            Some(&"nvidia-a100".to_string())
        );
    }
}
