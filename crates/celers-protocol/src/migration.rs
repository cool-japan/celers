//! Protocol version migration helpers
//!
//! This module provides utilities for migrating messages between different
//! Celery protocol versions (v2 and v5).
//!
//! # Example
//!
//! ```
//! use celers_protocol::migration::{ProtocolMigrator, MigrationStrategy};
//! use celers_protocol::{ProtocolVersion, Message, TaskArgs};
//! use uuid::Uuid;
//!
//! let task_id = Uuid::new_v4();
//! let body = serde_json::to_vec(&TaskArgs::new()).unwrap();
//! let msg = Message::new("tasks.add".to_string(), task_id, body);
//!
//! let migrator = ProtocolMigrator::new(MigrationStrategy::Conservative);
//! let info = migrator.check_compatibility(&msg, ProtocolVersion::V5);
//! assert!(info.is_compatible);
//! ```

use crate::{Message, ProtocolVersion};

/// Migration strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MigrationStrategy {
    /// Conservative: Only migrate if fully compatible
    Conservative,
    /// Permissive: Migrate with warnings for potential issues
    Permissive,
    /// Strict: Require exact feature parity
    Strict,
}

impl Default for MigrationStrategy {
    fn default() -> Self {
        Self::Conservative
    }
}

/// Migration compatibility information
#[derive(Debug, Clone)]
pub struct CompatibilityInfo {
    /// Whether the message is compatible with the target version
    pub is_compatible: bool,
    /// Protocol version being migrated from
    pub from_version: ProtocolVersion,
    /// Protocol version being migrated to
    pub to_version: ProtocolVersion,
    /// Any warnings or issues
    pub warnings: Vec<String>,
    /// Features that may not be supported
    pub unsupported_features: Vec<String>,
}

/// Protocol migrator for version transitions
pub struct ProtocolMigrator {
    strategy: MigrationStrategy,
}

impl ProtocolMigrator {
    /// Create a new protocol migrator with the given strategy
    pub fn new(strategy: MigrationStrategy) -> Self {
        Self { strategy }
    }

    /// Check if a message is compatible with a target protocol version
    pub fn check_compatibility(
        &self,
        message: &Message,
        target_version: ProtocolVersion,
    ) -> CompatibilityInfo {
        let from_version = ProtocolVersion::V2; // Default assumption
        let mut warnings = Vec::new();
        let unsupported_features = Vec::new();

        // Check for features that may not be fully supported across versions
        if message.has_group() && target_version == ProtocolVersion::V2 {
            warnings
                .push("Group ID is supported in v2 but may have limited functionality".to_string());
        }

        if message.has_parent() || message.has_root() {
            warnings.push(
                "Workflow tracking (parent/root) support varies between versions".to_string(),
            );
        }

        // Check priority support
        if message.properties.priority.is_some() {
            warnings.push("Priority support may vary between broker implementations".to_string());
        }

        // Determine compatibility based on strategy
        let is_compatible = match self.strategy {
            MigrationStrategy::Conservative => {
                warnings.is_empty() && unsupported_features.is_empty()
            }
            MigrationStrategy::Permissive => true, // Always allow migration
            MigrationStrategy::Strict => {
                warnings.is_empty()
                    && unsupported_features.is_empty()
                    && self.check_strict_compatibility(message, target_version)
            }
        };

        CompatibilityInfo {
            is_compatible,
            from_version,
            to_version: target_version,
            warnings,
            unsupported_features,
        }
    }

    /// Migrate a message to a different protocol version
    pub fn migrate(
        &self,
        message: Message,
        target_version: ProtocolVersion,
    ) -> Result<Message, MigrationError> {
        let compat = self.check_compatibility(&message, target_version);

        if !compat.is_compatible && self.strategy == MigrationStrategy::Conservative {
            return Err(MigrationError::IncompatibleVersion {
                from: compat.from_version,
                to: target_version,
                reasons: compat.warnings,
            });
        }

        // For now, message structure is the same between v2 and v5
        // In a real implementation, you might transform headers or properties
        Ok(message)
    }

    fn check_strict_compatibility(&self, _message: &Message, _target: ProtocolVersion) -> bool {
        // In strict mode, ensure all features are fully supported
        // For now, return true as v2 and v5 are largely compatible
        true
    }

    /// Get the current strategy
    pub fn strategy(&self) -> MigrationStrategy {
        self.strategy
    }

    /// Set a new strategy
    pub fn set_strategy(&mut self, strategy: MigrationStrategy) {
        self.strategy = strategy;
    }
}

impl Default for ProtocolMigrator {
    fn default() -> Self {
        Self::new(MigrationStrategy::Conservative)
    }
}

/// Migration error
#[derive(Debug, Clone)]
pub enum MigrationError {
    /// Version incompatibility
    IncompatibleVersion {
        from: ProtocolVersion,
        to: ProtocolVersion,
        reasons: Vec<String>,
    },
    /// Feature not supported in target version
    UnsupportedFeature {
        feature: String,
        version: ProtocolVersion,
    },
    /// Validation error
    ValidationError(String),
}

impl std::fmt::Display for MigrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MigrationError::IncompatibleVersion { from, to, reasons } => {
                write!(
                    f,
                    "Incompatible migration from {} to {}: {}",
                    from,
                    to,
                    reasons.join(", ")
                )
            }
            MigrationError::UnsupportedFeature { feature, version } => {
                write!(f, "Feature '{}' not supported in {}", feature, version)
            }
            MigrationError::ValidationError(msg) => write!(f, "Validation error: {}", msg),
        }
    }
}

impl std::error::Error for MigrationError {}

/// Helper function to create a migration plan
pub fn create_migration_plan(from: ProtocolVersion, to: ProtocolVersion) -> Vec<MigrationStep> {
    let mut steps = Vec::new();

    if from != to {
        steps.push(MigrationStep {
            description: format!("Migrate from {} to {}", from, to),
            from_version: from,
            to_version: to,
            required: true,
        });

        // Add any intermediate steps if needed
        if from == ProtocolVersion::V2 && to == ProtocolVersion::V5 {
            steps.push(MigrationStep {
                description: "Verify message format compatibility".to_string(),
                from_version: from,
                to_version: to,
                required: true,
            });

            steps.push(MigrationStep {
                description: "Update any version-specific headers".to_string(),
                from_version: from,
                to_version: to,
                required: false,
            });
        }
    }

    steps
}

/// Migration step
#[derive(Debug, Clone)]
pub struct MigrationStep {
    /// Description of the step
    pub description: String,
    /// Source version
    pub from_version: ProtocolVersion,
    /// Target version
    pub to_version: ProtocolVersion,
    /// Whether this step is required
    pub required: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TaskArgs;
    use uuid::Uuid;

    #[test]
    fn test_migrator_default() {
        let migrator = ProtocolMigrator::default();
        assert_eq!(migrator.strategy(), MigrationStrategy::Conservative);
    }

    #[test]
    fn test_migrator_set_strategy() {
        let mut migrator = ProtocolMigrator::default();
        migrator.set_strategy(MigrationStrategy::Permissive);
        assert_eq!(migrator.strategy(), MigrationStrategy::Permissive);
    }

    #[test]
    fn test_check_compatibility_basic() {
        let task_id = Uuid::new_v4();
        let body = serde_json::to_vec(&TaskArgs::new()).unwrap();
        let msg = Message::new("tasks.add".to_string(), task_id, body);

        let migrator = ProtocolMigrator::new(MigrationStrategy::Conservative);
        let info = migrator.check_compatibility(&msg, ProtocolVersion::V5);

        assert!(info.is_compatible);
        assert_eq!(info.to_version, ProtocolVersion::V5);
    }

    #[test]
    fn test_check_compatibility_with_warnings() {
        let task_id = Uuid::new_v4();
        let body = serde_json::to_vec(&TaskArgs::new()).unwrap();
        let msg = Message::new("tasks.add".to_string(), task_id, body)
            .with_priority(5)
            .with_group(Uuid::new_v4());

        let migrator = ProtocolMigrator::new(MigrationStrategy::Permissive);
        let info = migrator.check_compatibility(&msg, ProtocolVersion::V2);

        assert!(info.is_compatible); // Permissive allows it
        assert!(!info.warnings.is_empty());
    }

    #[test]
    fn test_migrate_basic_message() {
        let task_id = Uuid::new_v4();
        let body = serde_json::to_vec(&TaskArgs::new()).unwrap();
        let msg = Message::new("tasks.add".to_string(), task_id, body.clone());

        let migrator = ProtocolMigrator::new(MigrationStrategy::Conservative);
        let migrated = migrator.migrate(msg, ProtocolVersion::V5).unwrap();

        assert_eq!(migrated.task_id(), task_id);
        assert_eq!(migrated.body, body);
    }

    #[test]
    fn test_migrate_permissive() {
        let task_id = Uuid::new_v4();
        let body = serde_json::to_vec(&TaskArgs::new()).unwrap();
        let msg = Message::new("tasks.add".to_string(), task_id, body)
            .with_priority(9)
            .with_group(Uuid::new_v4());

        let migrator = ProtocolMigrator::new(MigrationStrategy::Permissive);
        let result = migrator.migrate(msg, ProtocolVersion::V5);

        assert!(result.is_ok());
    }

    #[test]
    fn test_create_migration_plan_same_version() {
        let plan = create_migration_plan(ProtocolVersion::V2, ProtocolVersion::V2);
        assert_eq!(plan.len(), 0);
    }

    #[test]
    fn test_create_migration_plan_v2_to_v5() {
        let plan = create_migration_plan(ProtocolVersion::V2, ProtocolVersion::V5);
        assert!(!plan.is_empty());
        assert!(plan.iter().any(|step| step.required));
    }

    #[test]
    fn test_migration_error_display() {
        let err = MigrationError::IncompatibleVersion {
            from: ProtocolVersion::V2,
            to: ProtocolVersion::V5,
            reasons: vec!["test reason".to_string()],
        };
        assert!(err.to_string().contains("Incompatible migration"));

        let err = MigrationError::UnsupportedFeature {
            feature: "test_feature".to_string(),
            version: ProtocolVersion::V2,
        };
        assert!(err.to_string().contains("not supported"));

        let err = MigrationError::ValidationError("test error".to_string());
        assert!(err.to_string().contains("Validation error"));
    }

    #[test]
    fn test_compatibility_info_structure() {
        let task_id = Uuid::new_v4();
        let body = vec![1, 2, 3];
        let msg = Message::new("tasks.test".to_string(), task_id, body);

        let migrator = ProtocolMigrator::new(MigrationStrategy::Strict);
        let info = migrator.check_compatibility(&msg, ProtocolVersion::V5);

        assert_eq!(info.from_version, ProtocolVersion::V2);
        assert_eq!(info.to_version, ProtocolVersion::V5);
    }

    #[test]
    fn test_migration_strategy_equality() {
        assert_eq!(
            MigrationStrategy::Conservative,
            MigrationStrategy::Conservative
        );
        assert_ne!(
            MigrationStrategy::Conservative,
            MigrationStrategy::Permissive
        );
    }

    #[test]
    fn test_migration_strategy_default() {
        assert_eq!(
            MigrationStrategy::default(),
            MigrationStrategy::Conservative
        );
    }
}
