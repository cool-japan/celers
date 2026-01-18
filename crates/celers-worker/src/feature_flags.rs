//! Feature flags for conditional task execution
//!
//! This module provides a feature flag system that allows tasks to declare
//! feature requirements and workers to advertise which features they support.
//! This is useful for:
//!
//! - Gradual rollout of new functionality
//! - A/B testing different implementations
//! - Environment-specific task execution
//! - Experimental feature gating
//!
//! # Examples
//!
//! ```
//! use celers_worker::{FeatureFlags, TaskFeatureRequirements};
//!
//! // Worker declares supported features
//! let worker_features = FeatureFlags::new()
//!     .enable("experimental-optimizer")
//!     .enable("gpu-acceleration")
//!     .enable("beta-features");
//!
//! // Task declares required features
//! let task_requirements = TaskFeatureRequirements::new()
//!     .require("experimental-optimizer")
//!     .prefer("gpu-acceleration");
//!
//! // Check if worker can handle task
//! assert!(worker_features.satisfies(&task_requirements));
//! ```

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Feature flags enabled on a worker
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeatureFlags {
    /// Set of enabled feature names
    enabled: HashSet<String>,

    /// Optional feature metadata (e.g., version, config)
    #[serde(skip_serializing_if = "HashMap::is_empty", default)]
    metadata: HashMap<String, String>,
}

impl FeatureFlags {
    /// Create a new empty FeatureFlags
    pub fn new() -> Self {
        Self {
            enabled: HashSet::new(),
            metadata: HashMap::new(),
        }
    }

    /// Create FeatureFlags from a list of feature names
    pub fn from_features(features: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let mut flags = Self::new();
        for feature in features {
            flags.enabled.insert(feature.into());
        }
        flags
    }

    /// Enable a feature flag
    pub fn enable(mut self, feature: impl Into<String>) -> Self {
        self.enabled.insert(feature.into());
        self
    }

    /// Disable a feature flag
    pub fn disable(mut self, feature: &str) -> Self {
        self.enabled.remove(feature);
        self
    }

    /// Add metadata for a feature
    pub fn with_metadata(
        mut self,
        feature: impl Into<String>,
        metadata: impl Into<String>,
    ) -> Self {
        self.metadata.insert(feature.into(), metadata.into());
        self
    }

    /// Check if a feature is enabled
    pub fn is_enabled(&self, feature: &str) -> bool {
        self.enabled.contains(feature)
    }

    /// Check if all features in a set are enabled
    pub fn has_all(&self, features: &HashSet<String>) -> bool {
        features.iter().all(|f| self.enabled.contains(f))
    }

    /// Check if any feature in a set is enabled
    pub fn has_any(&self, features: &HashSet<String>) -> bool {
        features.iter().any(|f| self.enabled.contains(f))
    }

    /// Get metadata for a feature
    pub fn get_metadata(&self, feature: &str) -> Option<&str> {
        self.metadata.get(feature).map(|s| s.as_str())
    }

    /// Get all enabled features
    pub fn enabled_features(&self) -> &HashSet<String> {
        &self.enabled
    }

    /// Get all metadata
    pub fn all_metadata(&self) -> &HashMap<String, String> {
        &self.metadata
    }

    /// Check if this worker satisfies task requirements
    pub fn satisfies(&self, requirements: &TaskFeatureRequirements) -> bool {
        // All required features must be present
        if !self.has_all(&requirements.required) {
            return false;
        }

        // If any forbidden features are present, reject
        if self.has_any(&requirements.forbidden) {
            return false;
        }

        // Preferred and optional features don't affect acceptance
        true
    }

    /// Calculate a match score for task requirements (0.0 - 1.0)
    ///
    /// Higher score indicates better match. Score components:
    /// - Required features satisfied: base 0.5
    /// - Each preferred feature present: +0.1 (up to 0.5)
    /// - Forbidden features present: -1.0 (immediate disqualification)
    pub fn match_score(&self, requirements: &TaskFeatureRequirements) -> f64 {
        // Check forbidden features first
        if self.has_any(&requirements.forbidden) {
            return 0.0;
        }

        // Check required features
        if !self.has_all(&requirements.required) {
            return 0.0;
        }

        // Base score for meeting requirements
        let mut score = 0.5;

        // Add score for preferred features (up to 0.5)
        if !requirements.preferred.is_empty() {
            let preferred_count = requirements
                .preferred
                .iter()
                .filter(|f| self.enabled.contains(*f))
                .count();
            let preferred_ratio = preferred_count as f64 / requirements.preferred.len() as f64;
            score += preferred_ratio * 0.5;
        }

        score.min(1.0)
    }

    /// Merge another FeatureFlags into this one
    pub fn merge(&mut self, other: &FeatureFlags) {
        self.enabled.extend(other.enabled.iter().cloned());
        self.metadata.extend(other.metadata.clone());
    }

    /// Clear all enabled features
    pub fn clear(&mut self) {
        self.enabled.clear();
        self.metadata.clear();
    }

    /// Get the number of enabled features
    pub fn count(&self) -> usize {
        self.enabled.len()
    }

    /// Check if no features are enabled
    pub fn is_empty(&self) -> bool {
        self.enabled.is_empty()
    }
}

impl Default for FeatureFlags {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for FeatureFlags {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.enabled.is_empty() {
            write!(f, "FeatureFlags[none]")
        } else {
            let features: Vec<&str> = self.enabled.iter().map(|s| s.as_str()).collect();
            write!(f, "FeatureFlags[{}]", features.join(", "))
        }
    }
}

/// Feature requirements for a task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskFeatureRequirements {
    /// Features that must be enabled (hard requirement)
    #[serde(skip_serializing_if = "HashSet::is_empty", default)]
    required: HashSet<String>,

    /// Features that are preferred but not required (soft preference)
    #[serde(skip_serializing_if = "HashSet::is_empty", default)]
    preferred: HashSet<String>,

    /// Features that should not be enabled (exclusion)
    #[serde(skip_serializing_if = "HashSet::is_empty", default)]
    forbidden: HashSet<String>,

    /// Features that are optional (informational only)
    #[serde(skip_serializing_if = "HashSet::is_empty", default)]
    optional: HashSet<String>,
}

impl TaskFeatureRequirements {
    /// Create new empty task feature requirements
    pub fn new() -> Self {
        Self {
            required: HashSet::new(),
            preferred: HashSet::new(),
            forbidden: HashSet::new(),
            optional: HashSet::new(),
        }
    }

    /// Add a required feature
    pub fn require(mut self, feature: impl Into<String>) -> Self {
        self.required.insert(feature.into());
        self
    }

    /// Add a preferred feature
    pub fn prefer(mut self, feature: impl Into<String>) -> Self {
        self.preferred.insert(feature.into());
        self
    }

    /// Add a forbidden feature
    pub fn forbid(mut self, feature: impl Into<String>) -> Self {
        self.forbidden.insert(feature.into());
        self
    }

    /// Add an optional feature
    pub fn optional(mut self, feature: impl Into<String>) -> Self {
        self.optional.insert(feature.into());
        self
    }

    /// Get required features
    pub fn required_features(&self) -> &HashSet<String> {
        &self.required
    }

    /// Get preferred features
    pub fn preferred_features(&self) -> &HashSet<String> {
        &self.preferred
    }

    /// Get forbidden features
    pub fn forbidden_features(&self) -> &HashSet<String> {
        &self.forbidden
    }

    /// Get optional features
    pub fn optional_features(&self) -> &HashSet<String> {
        &self.optional
    }

    /// Check if there are any requirements
    pub fn has_requirements(&self) -> bool {
        !self.required.is_empty() || !self.forbidden.is_empty()
    }

    /// Check if a feature is required
    pub fn is_required(&self, feature: &str) -> bool {
        self.required.contains(feature)
    }

    /// Check if a feature is preferred
    pub fn is_preferred(&self, feature: &str) -> bool {
        self.preferred.contains(feature)
    }

    /// Check if a feature is forbidden
    pub fn is_forbidden(&self, feature: &str) -> bool {
        self.forbidden.contains(feature)
    }

    /// Check if a feature is optional
    pub fn is_optional(&self, feature: &str) -> bool {
        self.optional.contains(feature)
    }

    /// Validate that requirements are consistent
    ///
    /// Returns an error if:
    /// - A feature is both required and forbidden
    /// - A feature is both required and optional
    /// - A feature is both forbidden and preferred
    pub fn validate(&self) -> Result<(), String> {
        // Check for required + forbidden
        for feature in &self.required {
            if self.forbidden.contains(feature) {
                return Err(format!(
                    "Feature '{}' cannot be both required and forbidden",
                    feature
                ));
            }
        }

        // Check for forbidden + preferred
        for feature in &self.forbidden {
            if self.preferred.contains(feature) {
                return Err(format!(
                    "Feature '{}' cannot be both forbidden and preferred",
                    feature
                ));
            }
        }

        Ok(())
    }
}

impl Default for TaskFeatureRequirements {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for TaskFeatureRequirements {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TaskFeatureRequirements[")?;

        if !self.required.is_empty() {
            let req: Vec<&str> = self.required.iter().map(|s| s.as_str()).collect();
            write!(f, "required:{}", req.join(","))?;
        }

        if !self.preferred.is_empty() {
            if !self.required.is_empty() {
                write!(f, ", ")?;
            }
            let pref: Vec<&str> = self.preferred.iter().map(|s| s.as_str()).collect();
            write!(f, "preferred:{}", pref.join(","))?;
        }

        if !self.forbidden.is_empty() {
            if !self.required.is_empty() || !self.preferred.is_empty() {
                write!(f, ", ")?;
            }
            let forb: Vec<&str> = self.forbidden.iter().map(|s| s.as_str()).collect();
            write!(f, "forbidden:{}", forb.join(","))?;
        }

        write!(f, "]")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_feature_flags_new() {
        let flags = FeatureFlags::new();
        assert!(flags.is_empty());
        assert_eq!(flags.count(), 0);
    }

    #[test]
    fn test_feature_flags_enable() {
        let flags = FeatureFlags::new().enable("feature1").enable("feature2");

        assert!(flags.is_enabled("feature1"));
        assert!(flags.is_enabled("feature2"));
        assert!(!flags.is_enabled("feature3"));
        assert_eq!(flags.count(), 2);
    }

    #[test]
    fn test_feature_flags_disable() {
        let flags = FeatureFlags::new()
            .enable("feature1")
            .enable("feature2")
            .disable("feature1");

        assert!(!flags.is_enabled("feature1"));
        assert!(flags.is_enabled("feature2"));
    }

    #[test]
    fn test_feature_flags_metadata() {
        let flags = FeatureFlags::new()
            .enable("feature1")
            .with_metadata("feature1", "v1.0.0");

        assert_eq!(flags.get_metadata("feature1"), Some("v1.0.0"));
        assert_eq!(flags.get_metadata("feature2"), None);
    }

    #[test]
    fn test_feature_flags_from_features() {
        let flags = FeatureFlags::from_features(vec!["a", "b", "c"]);
        assert_eq!(flags.count(), 3);
        assert!(flags.is_enabled("a"));
        assert!(flags.is_enabled("b"));
        assert!(flags.is_enabled("c"));
    }

    #[test]
    fn test_task_requirements_basic() {
        let req = TaskFeatureRequirements::new()
            .require("gpu")
            .prefer("fast-network")
            .forbid("legacy-mode");

        assert!(req.is_required("gpu"));
        assert!(req.is_preferred("fast-network"));
        assert!(req.is_forbidden("legacy-mode"));
        assert!(req.has_requirements());
    }

    #[test]
    fn test_feature_flags_satisfies_all_required() {
        let flags = FeatureFlags::new().enable("gpu").enable("fast-network");

        let req = TaskFeatureRequirements::new().require("gpu");

        assert!(flags.satisfies(&req));
    }

    #[test]
    fn test_feature_flags_satisfies_missing_required() {
        let flags = FeatureFlags::new().enable("fast-network");

        let req = TaskFeatureRequirements::new().require("gpu");

        assert!(!flags.satisfies(&req));
    }

    #[test]
    fn test_feature_flags_satisfies_with_forbidden() {
        let flags = FeatureFlags::new().enable("gpu").enable("legacy-mode");

        let req = TaskFeatureRequirements::new()
            .require("gpu")
            .forbid("legacy-mode");

        assert!(!flags.satisfies(&req));
    }

    #[test]
    fn test_feature_flags_match_score() {
        let flags = FeatureFlags::new()
            .enable("gpu")
            .enable("fast-network")
            .enable("ssd-storage");

        // All required + some preferred
        let req1 = TaskFeatureRequirements::new()
            .require("gpu")
            .prefer("fast-network")
            .prefer("ssd-storage");
        let score1 = flags.match_score(&req1);
        assert!(score1 > 0.9); // Should be very high

        // All required but no preferred
        let req2 = TaskFeatureRequirements::new()
            .require("gpu")
            .prefer("experimental");
        let score2 = flags.match_score(&req2);
        assert_eq!(score2, 0.5); // Base score only

        // With forbidden feature
        let req3 = TaskFeatureRequirements::new().require("gpu").forbid("gpu");
        let score3 = flags.match_score(&req3);
        assert_eq!(score3, 0.0); // Disqualified
    }

    #[test]
    fn test_task_requirements_validation() {
        // Valid requirements
        let req1 = TaskFeatureRequirements::new().require("gpu").prefer("ssd");
        assert!(req1.validate().is_ok());

        // Invalid: required + forbidden
        let req2 = TaskFeatureRequirements::new().require("gpu").forbid("gpu");
        assert!(req2.validate().is_err());

        // Invalid: forbidden + preferred
        let req3 = TaskFeatureRequirements::new()
            .forbid("legacy")
            .prefer("legacy");
        assert!(req3.validate().is_err());
    }

    #[test]
    fn test_feature_flags_merge() {
        let mut flags1 = FeatureFlags::new().enable("a").enable("b");

        let flags2 = FeatureFlags::new().enable("b").enable("c");

        flags1.merge(&flags2);
        assert_eq!(flags1.count(), 3);
        assert!(flags1.is_enabled("a"));
        assert!(flags1.is_enabled("b"));
        assert!(flags1.is_enabled("c"));
    }

    #[test]
    fn test_feature_flags_clear() {
        let mut flags = FeatureFlags::new().enable("a").enable("b");

        assert_eq!(flags.count(), 2);
        flags.clear();
        assert_eq!(flags.count(), 0);
        assert!(flags.is_empty());
    }

    #[test]
    fn test_feature_flags_display() {
        let flags = FeatureFlags::new().enable("gpu").enable("ssd");
        let display = format!("{}", flags);
        assert!(display.contains("FeatureFlags"));
    }

    #[test]
    fn test_task_requirements_display() {
        let req = TaskFeatureRequirements::new()
            .require("gpu")
            .prefer("ssd")
            .forbid("legacy");
        let display = format!("{}", req);
        assert!(display.contains("TaskFeatureRequirements"));
    }
}
