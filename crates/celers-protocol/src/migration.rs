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

/// Header key under which the migrated-to protocol version is recorded in a
/// message's `headers.extra` map. The stored value is the version's numeric
/// string (e.g. `"2"` or `"5"`), matching [`ProtocolVersion::as_number_str`].
pub const PROTOCOL_VERSION_HEADER: &str = "protocol_version";

/// Migration strategy
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum MigrationStrategy {
    /// Conservative: Only migrate if fully compatible
    #[default]
    Conservative,
    /// Permissive: Migrate with warnings for potential issues
    Permissive,
    /// Strict: Require exact feature parity
    Strict,
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

    /// Determine the protocol version a message is currently stamped with.
    ///
    /// Reads the [`PROTOCOL_VERSION_HEADER`] stamp written by
    /// [`ProtocolMigrator::migrate`], [`crate::v5::build_v5_message`] and
    /// [`crate::v5::to_v5_wire`]. An unstamped message is assumed to be v2 --
    /// the Celery default (`task_protocol = 2`) and the format a plain
    /// [`Message`] is built in.
    pub fn source_version(message: &Message) -> ProtocolVersion {
        crate::negotiation::parse_version_from_headers(&message.headers.extra)
            .unwrap_or(ProtocolVersion::V2)
    }

    /// Check if a message is compatible with a target protocol version
    pub fn check_compatibility(
        &self,
        message: &Message,
        target_version: ProtocolVersion,
    ) -> CompatibilityInfo {
        // The source version is read off the message rather than assumed, so
        // that `CompatibilityInfo::from_version` and the `IncompatibleVersion`
        // error report the real transition.
        let from_version = Self::source_version(message);
        let mut warnings = Vec::new();
        let unsupported_features = Vec::new();

        // A v5 -> v2 downgrade loses the inline workflow stamping that v5
        // carries natively; `migrate` mirrors the identifiers into `_legacy_*`
        // headers, which a stock v2 consumer will not interpret.
        if from_version == ProtocolVersion::V5
            && target_version == ProtocolVersion::V2
            && (message.has_group() || message.has_parent() || message.has_root())
        {
            warnings.push(
                "Downgrading v5 -> v2: inline workflow stamping (group/parent/root) is mirrored \
                 into `_legacy_*` headers and is not interpreted by stock v2 consumers"
                    .to_string(),
            );
        }

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

    /// Migrate a message to a different protocol version.
    ///
    /// The Celery v2 and v5 message envelopes share the same wire structure,
    /// so the migration is primarily a *re-stamping* of the target protocol
    /// version onto the message together with the conservative, version-aware
    /// adjustments described below. The target version is recorded in the
    /// message's `headers.extra` under [`PROTOCOL_VERSION_HEADER`] so that the
    /// migration is observable by downstream consumers (and testable).
    ///
    /// Version-specific transformations applied:
    /// * Migrating **to v2**: v2 brokers/consumers do not understand the
    ///   group/parent/root workflow stamping that v5 carries inline, so any
    ///   such identifiers are preserved into `headers.extra` (as a compatible
    ///   fallback) before being left in place. This is non-destructive: the
    ///   typed fields are kept, and a string mirror is added under the
    ///   `_legacy_*` keys for v2-only consumers.
    /// * Migrating **to v5**: the priority is normalised into `headers.extra`
    ///   under `delivery_priority` (v5 surfaces priority in headers in addition
    ///   to AMQP properties), and the inline workflow fields are left untouched
    ///   since v5 supports them natively.
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

        let mut message = message;

        // Record the target protocol version so the migration is observable.
        message.headers.extra.insert(
            PROTOCOL_VERSION_HEADER.to_string(),
            serde_json::Value::String(target_version.as_number_str().to_string()),
        );

        match target_version {
            ProtocolVersion::V2 => {
                // v2 consumers cannot rely on inline v5 workflow stamping; mirror
                // the identifiers into `extra` as a non-destructive fallback.
                if let Some(group) = message.headers.group {
                    message.headers.extra.insert(
                        "_legacy_group".to_string(),
                        serde_json::Value::String(group.to_string()),
                    );
                }
                if let Some(parent) = message.headers.parent_id {
                    message.headers.extra.insert(
                        "_legacy_parent_id".to_string(),
                        serde_json::Value::String(parent.to_string()),
                    );
                }
                if let Some(root) = message.headers.root_id {
                    message.headers.extra.insert(
                        "_legacy_root_id".to_string(),
                        serde_json::Value::String(root.to_string()),
                    );
                }
            }
            ProtocolVersion::V5 => {
                // v5 surfaces priority in the headers in addition to AMQP
                // properties; mirror it so header-only consumers can route on it.
                if let Some(priority) = message.properties.priority {
                    message.headers.extra.insert(
                        "delivery_priority".to_string(),
                        serde_json::Value::Number(priority.into()),
                    );
                }
            }
        }

        Ok(message)
    }

    fn check_strict_compatibility(&self, message: &Message, target: ProtocolVersion) -> bool {
        // In strict mode, a message is only considered compatible when it does
        // not rely on features whose semantics are not fully preserved by the
        // target version. We use the same signals as `check_compatibility`:
        // workflow tracking (group/parent/root) and broker-dependent priority.
        match target {
            ProtocolVersion::V2 => {
                // v2 lacks first-class, inline workflow stamping and has only
                // broker-dependent priority support, so a message using any of
                // these features is not strictly compatible.
                !message.has_group()
                    && !message.has_parent()
                    && !message.has_root()
                    && message.properties.priority.is_none()
            }
            ProtocolVersion::V5 => {
                // v5 natively supports inline workflow tracking; priority remains
                // broker-dependent, so it is the only strict blocker here.
                message.properties.priority.is_none()
            }
        }
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
        let migrated = migrator
            .migrate(msg, ProtocolVersion::V5)
            .expect("conservative migration of a plain message must succeed");

        assert_eq!(migrated.task_id(), task_id);
        assert_eq!(migrated.body, body);

        // The migration must stamp the target protocol version so it is
        // observable on the migrated message.
        assert_eq!(
            migrated.headers.extra.get(PROTOCOL_VERSION_HEADER),
            Some(&serde_json::Value::String("5".to_string()))
        );
    }

    #[test]
    fn test_migrate_stamps_version_v2() {
        let task_id = Uuid::new_v4();
        let body = serde_json::to_vec(&TaskArgs::new()).unwrap();
        let msg = Message::new("tasks.add".to_string(), task_id, body);

        let migrator = ProtocolMigrator::new(MigrationStrategy::Conservative);
        let migrated = migrator
            .migrate(msg, ProtocolVersion::V2)
            .expect("conservative migration of a plain message must succeed");

        assert_eq!(
            migrated.headers.extra.get(PROTOCOL_VERSION_HEADER),
            Some(&serde_json::Value::String("2".to_string()))
        );
    }

    #[test]
    fn test_migrate_to_v5_mirrors_priority() {
        let task_id = Uuid::new_v4();
        let body = serde_json::to_vec(&TaskArgs::new()).unwrap();
        let msg = Message::new("tasks.add".to_string(), task_id, body).with_priority(7);

        // Permissive lets a message carrying broker-dependent priority through.
        let migrator = ProtocolMigrator::new(MigrationStrategy::Permissive);
        let migrated = migrator
            .migrate(msg, ProtocolVersion::V5)
            .expect("permissive migration must succeed");

        // v5 mirrors priority into the headers for header-only routing.
        assert_eq!(
            migrated.headers.extra.get("delivery_priority"),
            Some(&serde_json::Value::Number(7u8.into()))
        );
        // The original AMQP property is preserved.
        assert_eq!(migrated.properties.priority, Some(7));
    }

    #[test]
    fn test_migrate_to_v2_mirrors_workflow_fields() {
        let task_id = Uuid::new_v4();
        let group = Uuid::new_v4();
        let parent = Uuid::new_v4();
        let body = serde_json::to_vec(&TaskArgs::new()).unwrap();
        let msg = Message::new("tasks.add".to_string(), task_id, body)
            .with_group(group)
            .with_parent(parent);

        // Permissive migration to v2 mirrors workflow identifiers into `extra`
        // for v2-only consumers without destroying the typed fields.
        let migrator = ProtocolMigrator::new(MigrationStrategy::Permissive);
        let migrated = migrator
            .migrate(msg, ProtocolVersion::V2)
            .expect("permissive migration must succeed");

        assert_eq!(
            migrated.headers.extra.get("_legacy_group"),
            Some(&serde_json::Value::String(group.to_string()))
        );
        assert_eq!(
            migrated.headers.extra.get("_legacy_parent_id"),
            Some(&serde_json::Value::String(parent.to_string()))
        );
        // Typed fields remain intact (non-destructive).
        assert_eq!(migrated.headers.group, Some(group));
        assert_eq!(migrated.headers.parent_id, Some(parent));
    }

    #[test]
    fn test_migrate_conservative_rejects_unsupported() {
        let task_id = Uuid::new_v4();
        let body = serde_json::to_vec(&TaskArgs::new()).unwrap();
        let msg = Message::new("tasks.add".to_string(), task_id, body)
            .with_priority(9)
            .with_group(Uuid::new_v4());

        // Conservative refuses to migrate a message that triggers warnings.
        let migrator = ProtocolMigrator::new(MigrationStrategy::Conservative);
        let result = migrator.migrate(msg, ProtocolVersion::V5);

        assert!(matches!(
            result,
            Err(MigrationError::IncompatibleVersion { .. })
        ));
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
    fn test_strict_compatibility_plain_message() {
        let task_id = Uuid::new_v4();
        let body = serde_json::to_vec(&TaskArgs::new()).unwrap();
        let msg = Message::new("tasks.add".to_string(), task_id, body);

        // A plain message with no advanced features is strictly compatible.
        let migrator = ProtocolMigrator::new(MigrationStrategy::Strict);
        let info_v5 = migrator.check_compatibility(&msg, ProtocolVersion::V5);
        let info_v2 = migrator.check_compatibility(&msg, ProtocolVersion::V2);

        assert!(info_v5.is_compatible);
        assert!(info_v2.is_compatible);
    }

    #[test]
    fn test_strict_compatibility_priority_blocks() {
        let task_id = Uuid::new_v4();
        let body = serde_json::to_vec(&TaskArgs::new()).unwrap();
        let msg = Message::new("tasks.add".to_string(), task_id, body).with_priority(5);

        // Broker-dependent priority is not strictly compatible with either
        // target version.
        let migrator = ProtocolMigrator::new(MigrationStrategy::Strict);
        assert!(
            !migrator
                .check_compatibility(&msg, ProtocolVersion::V5)
                .is_compatible
        );
        assert!(
            !migrator
                .check_compatibility(&msg, ProtocolVersion::V2)
                .is_compatible
        );
    }

    #[test]
    fn test_strict_compatibility_workflow_blocks_v2_only() {
        let task_id = Uuid::new_v4();
        let body = serde_json::to_vec(&TaskArgs::new()).unwrap();
        let msg = Message::new("tasks.add".to_string(), task_id, body).with_group(Uuid::new_v4());

        let migrator = ProtocolMigrator::new(MigrationStrategy::Strict);

        // The strict check itself: workflow stamping is unsupported by v2 but
        // native to v5.
        assert!(!migrator.check_strict_compatibility(&msg, ProtocolVersion::V2));
        assert!(migrator.check_strict_compatibility(&msg, ProtocolVersion::V5));
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

    /// Regression: `from_version` used to be hardcoded to `V2`, so a message
    /// this crate itself produced as v5 was reported as migrating *from* v2 --
    /// in `CompatibilityInfo` and in every `IncompatibleVersion` error message.
    #[test]
    fn test_check_compatibility_reads_source_version_from_message() {
        let v5 = crate::v5::V5MessageSpec::new("tasks.add", Uuid::new_v4())
            .build()
            .expect("v5 build must succeed")
            .into_message();

        assert_eq!(
            ProtocolMigrator::source_version(&v5),
            ProtocolVersion::V5,
            "a message carrying the v5 stamp must be recognised as v5"
        );

        let migrator = ProtocolMigrator::new(MigrationStrategy::Conservative);
        let info = migrator.check_compatibility(&v5, ProtocolVersion::V2);
        assert_eq!(info.from_version, ProtocolVersion::V5);
        assert_eq!(info.to_version, ProtocolVersion::V2);

        // An unstamped message is still assumed to be v2 (the Celery default).
        let body = serde_json::to_vec(&TaskArgs::new()).unwrap();
        let plain = Message::new("tasks.add".to_string(), Uuid::new_v4(), body);
        assert_eq!(
            ProtocolMigrator::source_version(&plain),
            ProtocolVersion::V2
        );
        assert_eq!(
            migrator
                .check_compatibility(&plain, ProtocolVersion::V5)
                .from_version,
            ProtocolVersion::V2
        );
    }

    /// A v5 -> v2 downgrade of a workflow-stamped message must warn (and be
    /// refused by the conservative strategy), and the error must name the real
    /// source version.
    #[test]
    fn test_v5_to_v2_downgrade_warns_about_inline_workflow_stamping() {
        let v5 = crate::v5::V5MessageSpec::new("tasks.chord_callback", Uuid::new_v4())
            .with_group(Uuid::new_v4())
            .build()
            .expect("v5 build must succeed")
            .into_message();

        let migrator = ProtocolMigrator::new(MigrationStrategy::Conservative);
        let info = migrator.check_compatibility(&v5, ProtocolVersion::V2);

        assert_eq!(info.from_version, ProtocolVersion::V5);
        assert!(
            info.warnings.iter().any(|w| w.contains("v5 -> v2")),
            "expected a downgrade warning, got: {:?}",
            info.warnings
        );
        assert!(!info.is_compatible);

        match migrator.migrate(v5, ProtocolVersion::V2) {
            Err(MigrationError::IncompatibleVersion { from, to, .. }) => {
                assert_eq!(from, ProtocolVersion::V5);
                assert_eq!(to, ProtocolVersion::V2);
            }
            other => panic!("expected IncompatibleVersion, got {:?}", other),
        }
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
