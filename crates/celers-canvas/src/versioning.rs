//! Workflow schema versioning and migration.
//!
//! This module introduces a *schema-version* concept for serialized workflows.
//! A workflow ([`Chain`], [`Group`], [`Chord`], [`CanvasElement`]) can be
//! serialized into a *versioned* JSON envelope that embeds the schema version
//! of the on-disk representation alongside the workflow payload:
//!
//! ```text
//! { "schema_version": <u32>, "payload": <workflow-json> }
//! ```
//!
//! When such an envelope is loaded with [`VersionedWorkflow::from_versioned_json`],
//! the embedded version is read and — if it is older than
//! [`CURRENT_WORKFLOW_VERSION`] — a chain of registered [`Migrator`]s is applied
//! (`v1 -> v2 -> ... -> current`) to upgrade the payload before it is finally
//! deserialized into the concrete workflow type.
//!
//! The existing `to_json` / `from_json` helpers on individual types are left
//! untouched; the versioned helpers in this module are purely additive.
//!
//! # Example
//!
//! ```
//! use celers_canvas::{Chain, VersionedWorkflow, CURRENT_WORKFLOW_VERSION};
//!
//! let chain = Chain::new()
//!     .then("a", vec![serde_json::json!(1)])
//!     .then("b", vec![serde_json::json!(2)]);
//!
//! // Serialize with an embedded schema version.
//! let json = chain.to_versioned_json();
//!
//! // The version is recoverable without fully decoding the payload.
//! assert_eq!(
//!     celers_canvas::schema_version_of(&json).unwrap(),
//!     CURRENT_WORKFLOW_VERSION,
//! );
//!
//! // Round-trips back to an equivalent workflow.
//! let restored = Chain::from_versioned_json(&json).unwrap();
//! assert_eq!(restored, chain);
//! ```

use crate::{CanvasError, Chain, Chord, Group};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// The current workflow schema version.
///
/// This is the version embedded by [`VersionedWorkflow::to_versioned_json`] and
/// the target version that [`MigrationRegistry::upgrade`] migrates older
/// payloads up to. Bump this whenever the serialized shape of a workflow type
/// changes in a backwards-incompatible way, and register a [`Migrator`] from
/// the previous version to the new one.
pub const CURRENT_WORKFLOW_VERSION: u32 = 1;

/// JSON key holding the embedded schema version inside a versioned envelope.
const SCHEMA_VERSION_KEY: &str = "schema_version";

/// JSON key holding the workflow payload inside a versioned envelope.
const PAYLOAD_KEY: &str = "payload";

/// A versioned envelope wrapping a serialized workflow payload.
///
/// The envelope embeds the [`CURRENT_WORKFLOW_VERSION`] (at write time) or the
/// originating schema version (when read back) alongside the opaque workflow
/// JSON. The payload is kept as a [`serde_json::Value`] so that migrations can
/// rewrite it before the final, type-specific deserialization step.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VersionedEnvelope {
    /// Schema version of the embedded payload.
    #[serde(rename = "schema_version")]
    pub schema_version: u32,

    /// The opaque, version-specific workflow payload.
    pub payload: serde_json::Value,
}

impl VersionedEnvelope {
    /// Build an envelope for `payload` stamped with `schema_version`.
    pub fn new(schema_version: u32, payload: serde_json::Value) -> Self {
        Self {
            schema_version,
            payload,
        }
    }

    /// Serialize this envelope to a JSON string.
    pub fn to_json(&self) -> Result<String, CanvasError> {
        serde_json::to_string(self).map_err(|e| CanvasError::Serialization(e.to_string()))
    }

    /// Parse a versioned envelope from a JSON string.
    ///
    /// Returns [`CanvasError::Serialization`] if the input is not valid JSON,
    /// and [`CanvasError::Invalid`] if it is valid JSON but does not carry the
    /// required `schema_version` / `payload` fields.
    pub fn from_json(json: &str) -> Result<Self, CanvasError> {
        let value: serde_json::Value =
            serde_json::from_str(json).map_err(|e| CanvasError::Serialization(e.to_string()))?;
        Self::from_value(value)
    }

    /// Build a [`VersionedEnvelope`] from an already-parsed JSON value.
    fn from_value(value: serde_json::Value) -> Result<Self, CanvasError> {
        let obj = value.as_object().ok_or_else(|| {
            CanvasError::Invalid("versioned workflow envelope must be a JSON object".to_string())
        })?;

        let schema_version = obj
            .get(SCHEMA_VERSION_KEY)
            .ok_or_else(|| {
                CanvasError::Invalid(format!(
                    "versioned workflow envelope is missing the '{SCHEMA_VERSION_KEY}' field"
                ))
            })?
            .as_u64()
            .and_then(|v| u32::try_from(v).ok())
            .ok_or_else(|| {
                CanvasError::Invalid(format!(
                    "'{SCHEMA_VERSION_KEY}' must be a non-negative integer that fits in u32"
                ))
            })?;

        let payload = obj
            .get(PAYLOAD_KEY)
            .ok_or_else(|| {
                CanvasError::Invalid(format!(
                    "versioned workflow envelope is missing the '{PAYLOAD_KEY}' field"
                ))
            })?
            .clone();

        Ok(Self::new(schema_version, payload))
    }
}

/// Read the embedded schema version from a versioned-workflow JSON string.
///
/// This is a lightweight helper that inspects only the `schema_version` field
/// without attempting to fully decode (or migrate) the payload.
///
/// # Errors
///
/// Returns [`CanvasError::Serialization`] for malformed JSON and
/// [`CanvasError::Invalid`] when the `schema_version` field is absent or not a
/// `u32`-representable integer.
pub fn schema_version_of(json: &str) -> Result<u32, CanvasError> {
    VersionedEnvelope::from_json(json).map(|env| env.schema_version)
}

/// Upgrades a workflow payload from one schema version to the next.
///
/// A [`Migrator`] is responsible for exactly one step in the upgrade chain
/// (`from_version` -> `to_version`, where `to_version == from_version + 1`). The
/// [`MigrationRegistry`] composes individual migrators into a full upgrade path.
///
/// Implementors receive the payload as an opaque [`serde_json::Value`] and must
/// return the rewritten payload in the shape expected by the next schema
/// version. Returning an error aborts the migration and surfaces a descriptive
/// [`CanvasError`].
pub trait Migrator: Send + Sync {
    /// The schema version this migrator upgrades *from*.
    fn source_version(&self) -> u32;

    /// The schema version this migrator upgrades *to*.
    ///
    /// Must equal `source_version() + 1`; the registry validates this when the
    /// migrator is registered.
    fn target_version(&self) -> u32;

    /// Rewrite `payload` from [`Migrator::source_version`] to
    /// [`Migrator::target_version`].
    fn migrate(&self, payload: serde_json::Value) -> Result<serde_json::Value, CanvasError>;
}

/// A [`Migrator`] backed by a closure.
///
/// Convenient for registering one-off migration steps without defining a
/// dedicated type.
///
/// # Example
///
/// ```
/// use celers_canvas::{FnMigrator, MigrationRegistry, Migrator};
///
/// let migrator = FnMigrator::new(1, 2, |mut payload| {
///     if let Some(obj) = payload.as_object_mut() {
///         obj.insert("new_field".to_string(), serde_json::json!(true));
///     }
///     Ok(payload)
/// });
/// assert_eq!(migrator.source_version(), 1);
/// assert_eq!(migrator.target_version(), 2);
///
/// let mut registry = MigrationRegistry::empty();
/// registry.register(migrator).unwrap();
/// ```
pub struct FnMigrator<F>
where
    F: Fn(serde_json::Value) -> Result<serde_json::Value, CanvasError> + Send + Sync,
{
    from_version: u32,
    to_version: u32,
    func: F,
}

impl<F> FnMigrator<F>
where
    F: Fn(serde_json::Value) -> Result<serde_json::Value, CanvasError> + Send + Sync,
{
    /// Build a closure-backed migrator from `from_version` to `to_version`.
    pub fn new(from_version: u32, to_version: u32, func: F) -> Self {
        Self {
            from_version,
            to_version,
            func,
        }
    }
}

impl<F> Migrator for FnMigrator<F>
where
    F: Fn(serde_json::Value) -> Result<serde_json::Value, CanvasError> + Send + Sync,
{
    fn source_version(&self) -> u32 {
        self.from_version
    }

    fn target_version(&self) -> u32 {
        self.to_version
    }

    fn migrate(&self, payload: serde_json::Value) -> Result<serde_json::Value, CanvasError> {
        (self.func)(payload)
    }
}

/// A registry of [`Migrator`]s that upgrades older payloads to the current
/// schema version.
///
/// Migrators are keyed by their [`source_version`](Migrator::source_version);
/// the registry walks the chain one step at a time (`v -> v+1`) until it reaches
/// [`MigrationRegistry::target_version`].
///
/// # Errors during upgrade
///
/// * [`CanvasError::Invalid`] if the source version is newer than the target
///   (a payload written by a newer release cannot be downgraded).
/// * [`CanvasError::Invalid`] if a step in the chain has no registered migrator.
/// * Any error returned by an individual [`Migrator::migrate`] call is
///   propagated unchanged.
pub struct MigrationRegistry {
    /// Map from a source schema version to the migrator that upgrades it.
    migrators: HashMap<u32, Box<dyn Migrator>>,

    /// Version that [`MigrationRegistry::upgrade`] migrates payloads up to.
    target_version: u32,
}

impl std::fmt::Debug for MigrationRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut steps: Vec<u32> = self.migrators.keys().copied().collect();
        steps.sort_unstable();
        f.debug_struct("MigrationRegistry")
            .field("target_version", &self.target_version)
            .field("registered_steps", &steps)
            .finish()
    }
}

impl MigrationRegistry {
    /// Create an empty registry targeting [`CURRENT_WORKFLOW_VERSION`].
    pub fn empty() -> Self {
        Self::with_target(CURRENT_WORKFLOW_VERSION)
    }

    /// Create an empty registry targeting an explicit version.
    ///
    /// Targeting an explicit version is primarily useful in tests that exercise
    /// migration logic against a synthetic "current" version.
    pub fn with_target(target_version: u32) -> Self {
        Self {
            migrators: HashMap::new(),
            target_version,
        }
    }

    /// The version this registry migrates payloads up to.
    pub fn target_version(&self) -> u32 {
        self.target_version
    }

    /// Number of registered migration steps.
    pub fn len(&self) -> usize {
        self.migrators.len()
    }

    /// Whether the registry has no registered migration steps.
    pub fn is_empty(&self) -> bool {
        self.migrators.is_empty()
    }

    /// Whether a migrator is registered for the given source version.
    pub fn has_step(&self, from_version: u32) -> bool {
        self.migrators.contains_key(&from_version)
    }

    /// Register a migration step.
    ///
    /// # Errors
    ///
    /// * [`CanvasError::Invalid`] if `to_version != from_version + 1` (migrators
    ///   must describe a single, contiguous step).
    /// * [`CanvasError::Invalid`] if a migrator is already registered for the
    ///   same source version.
    pub fn register<M: Migrator + 'static>(&mut self, migrator: M) -> Result<(), CanvasError> {
        let from = migrator.source_version();
        let to = migrator.target_version();

        if to
            != from.checked_add(1).ok_or_else(|| {
                CanvasError::Invalid(format!(
                    "migrator source version {from} overflows when computing the next version"
                ))
            })?
        {
            return Err(CanvasError::Invalid(format!(
                "migrator must upgrade exactly one version (got {from} -> {to}, expected {from} -> {})",
                from + 1
            )));
        }

        if self.migrators.contains_key(&from) {
            return Err(CanvasError::Invalid(format!(
                "a migrator from version {from} is already registered"
            )));
        }

        self.migrators.insert(from, Box::new(migrator));
        Ok(())
    }

    /// Register a closure-backed migration step.
    ///
    /// Convenience wrapper around [`FnMigrator`] + [`MigrationRegistry::register`].
    pub fn register_fn<F>(
        &mut self,
        from_version: u32,
        to_version: u32,
        func: F,
    ) -> Result<(), CanvasError>
    where
        F: Fn(serde_json::Value) -> Result<serde_json::Value, CanvasError> + Send + Sync + 'static,
    {
        self.register(FnMigrator::new(from_version, to_version, func))
    }

    /// Upgrade `payload` from `from_version` to [`MigrationRegistry::target_version`].
    ///
    /// Walks the registered migrators one step at a time. If `from_version`
    /// already equals the target, the payload is returned unchanged.
    ///
    /// # Errors
    ///
    /// * [`CanvasError::Invalid`] if `from_version` is greater than the target
    ///   version (the payload was written by a newer schema).
    /// * [`CanvasError::Invalid`] if any intermediate step lacks a registered
    ///   migrator.
    /// * Propagates any [`CanvasError`] returned by a [`Migrator::migrate`] call.
    pub fn upgrade(
        &self,
        from_version: u32,
        payload: serde_json::Value,
    ) -> Result<serde_json::Value, CanvasError> {
        if from_version > self.target_version {
            return Err(CanvasError::Invalid(format!(
                "workflow schema version {from_version} is newer than the supported version {}; \
                 upgrade the library to load this workflow",
                self.target_version
            )));
        }

        let mut current_version = from_version;
        let mut current_payload = payload;

        while current_version < self.target_version {
            let migrator = self.migrators.get(&current_version).ok_or_else(|| {
                CanvasError::Invalid(format!(
                    "no migrator registered to upgrade workflow schema from version \
                     {current_version} to {} (target {})",
                    current_version + 1,
                    self.target_version
                ))
            })?;

            // A registered migrator always advances exactly one version (enforced
            // at registration time), so this loop is guaranteed to terminate.
            current_payload = migrator.migrate(current_payload)?;
            current_version = migrator.target_version();
        }

        Ok(current_payload)
    }
}

impl Default for MigrationRegistry {
    fn default() -> Self {
        Self::empty()
    }
}

/// Additive serialization helpers that embed and honour a schema version.
///
/// Implemented for the core workflow types ([`Chain`], [`Group`], [`Chord`],
/// and [`crate::CanvasElement`]). The existing `to_json` / `from_json` helpers
/// on those types are unaffected; these methods wrap the payload in a
/// [`VersionedEnvelope`].
pub trait VersionedWorkflow: Sized {
    /// Serialize `self` into a versioned JSON envelope embedding
    /// [`CURRENT_WORKFLOW_VERSION`].
    ///
    /// This is infallible: every workflow type serializes cleanly. In the
    /// extraordinarily unlikely event the payload cannot be encoded, a
    /// well-formed envelope carrying a `null` payload (and the current version)
    /// is returned so the version stamp is never lost.
    fn to_versioned_json(&self) -> String;

    /// Like [`VersionedWorkflow::to_versioned_json`] but surfaces serialization
    /// failures instead of falling back.
    fn try_to_versioned_json(&self) -> Result<String, CanvasError>;

    /// Deserialize a workflow from a versioned JSON envelope using the default
    /// [`MigrationRegistry`] (i.e. one with no registered steps).
    ///
    /// A payload already at [`CURRENT_WORKFLOW_VERSION`] is decoded directly;
    /// any older version requires a registry with the relevant migrators, so
    /// use [`VersionedWorkflow::from_versioned_json_with`] for that case.
    ///
    /// # Errors
    ///
    /// See [`VersionedWorkflow::from_versioned_json_with`].
    fn from_versioned_json(json: &str) -> Result<Self, CanvasError> {
        Self::from_versioned_json_with(json, &MigrationRegistry::empty())
    }

    /// Deserialize a workflow from a versioned JSON envelope, applying the
    /// `registry`'s migrators to upgrade older payloads to
    /// [`CURRENT_WORKFLOW_VERSION`] before decoding.
    ///
    /// # Errors
    ///
    /// * [`CanvasError::Serialization`] for malformed JSON.
    /// * [`CanvasError::Invalid`] for a malformed envelope, a version newer than
    ///   supported, or a missing migration step.
    /// * [`CanvasError::Serialization`] if the (migrated) payload does not
    ///   deserialize into `Self`.
    fn from_versioned_json_with(
        json: &str,
        registry: &MigrationRegistry,
    ) -> Result<Self, CanvasError>;
}

/// Shared implementation backing [`VersionedWorkflow::try_to_versioned_json`].
fn encode_versioned<T: Serialize>(value: &T) -> Result<String, CanvasError> {
    let payload =
        serde_json::to_value(value).map_err(|e| CanvasError::Serialization(e.to_string()))?;
    VersionedEnvelope::new(CURRENT_WORKFLOW_VERSION, payload).to_json()
}

/// Shared implementation backing [`VersionedWorkflow::to_versioned_json`].
///
/// Falls back to a version-stamped `null` payload if encoding fails, so the
/// embedded version is preserved without panicking.
fn encode_versioned_lossy<T: Serialize>(value: &T) -> String {
    match encode_versioned(value) {
        Ok(json) => json,
        Err(_) => VersionedEnvelope::new(CURRENT_WORKFLOW_VERSION, serde_json::Value::Null)
            .to_json()
            .unwrap_or_else(|_| {
                format!("{{\"{SCHEMA_VERSION_KEY}\":{CURRENT_WORKFLOW_VERSION},\"{PAYLOAD_KEY}\":null}}")
            }),
    }
}

/// Shared implementation backing [`VersionedWorkflow::from_versioned_json_with`].
fn decode_versioned<T: DeserializeOwned>(
    json: &str,
    registry: &MigrationRegistry,
) -> Result<T, CanvasError> {
    let envelope = VersionedEnvelope::from_json(json)?;
    let upgraded = registry.upgrade(envelope.schema_version, envelope.payload)?;
    serde_json::from_value(upgraded).map_err(|e| CanvasError::Serialization(e.to_string()))
}

macro_rules! impl_versioned_workflow {
    ($ty:ty) => {
        impl VersionedWorkflow for $ty {
            fn to_versioned_json(&self) -> String {
                encode_versioned_lossy(self)
            }

            fn try_to_versioned_json(&self) -> Result<String, CanvasError> {
                encode_versioned(self)
            }

            fn from_versioned_json_with(
                json: &str,
                registry: &MigrationRegistry,
            ) -> Result<Self, CanvasError> {
                decode_versioned::<$ty>(json, registry)
            }
        }
    };
}

impl_versioned_workflow!(Chain);
impl_versioned_workflow!(Group);
impl_versioned_workflow!(Chord);
impl_versioned_workflow!(crate::CanvasElement);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CanvasElement, Signature};

    fn sample_chain() -> Chain {
        Chain::new()
            .then("first", vec![serde_json::json!(1)])
            .then("second", vec![serde_json::json!("x")])
    }

    fn sample_group() -> Group {
        let mut group = Group::new();
        group.group_id = None; // keep deterministic for equality assertions
        group
            .add("a", vec![serde_json::json!(1)])
            .add("b", vec![serde_json::json!(2)])
    }

    fn sample_chord() -> Chord {
        Chord::new(sample_group(), Signature::new("callback".to_string()))
    }

    #[test]
    fn embeds_current_version() {
        let json = sample_chain().to_versioned_json();
        assert_eq!(schema_version_of(&json).unwrap(), CURRENT_WORKFLOW_VERSION);

        let envelope = VersionedEnvelope::from_json(&json).unwrap();
        assert_eq!(envelope.schema_version, CURRENT_WORKFLOW_VERSION);
        // Payload is the same as the plain serialization of the workflow.
        let plain: serde_json::Value = serde_json::to_value(sample_chain()).unwrap();
        assert_eq!(envelope.payload, plain);
    }

    #[test]
    fn versioned_json_is_deterministic() {
        let a = sample_chain().to_versioned_json();
        let b = sample_chain().to_versioned_json();
        assert_eq!(a, b);
    }

    #[test]
    fn current_version_round_trips_unchanged() {
        let chain = sample_chain();
        let json = chain.to_versioned_json();
        let restored = Chain::from_versioned_json(&json).unwrap();
        assert_eq!(restored, chain);

        let group = sample_group();
        let restored_group = Group::from_versioned_json(&group.to_versioned_json()).unwrap();
        assert_eq!(restored_group, group);

        let chord = sample_chord();
        let restored_chord = Chord::from_versioned_json(&chord.to_versioned_json()).unwrap();
        assert_eq!(restored_chord, chord);
    }

    #[test]
    fn canvas_element_round_trips() {
        let element = CanvasElement::chain(sample_chain());
        let json = element.to_versioned_json();
        assert_eq!(schema_version_of(&json).unwrap(), CURRENT_WORKFLOW_VERSION);
        let restored = CanvasElement::from_versioned_json(&json).unwrap();
        // CanvasElement is not PartialEq, compare via plain serialization.
        assert_eq!(
            serde_json::to_value(&restored).unwrap(),
            serde_json::to_value(&element).unwrap(),
        );
    }

    #[test]
    fn try_to_versioned_json_matches_lossy() {
        let chain = sample_chain();
        assert_eq!(
            chain.try_to_versioned_json().unwrap(),
            chain.to_versioned_json()
        );
    }

    #[test]
    fn registering_v1_to_v2_upgrades_old_payload_on_load() {
        // Synthetic "current" version is 2 for this test.
        let mut registry = MigrationRegistry::with_target(2);
        registry
            .register_fn(1, 2, |mut payload| {
                // v2 of a Chain (here) is identical in shape, but a real
                // migrator would reshape the payload. We simply pass it through
                // after asserting we received a v1 object.
                if let Some(obj) = payload.as_object_mut() {
                    obj.entry("tasks")
                        .or_insert_with(|| serde_json::Value::Array(Vec::new()));
                }
                Ok(payload)
            })
            .unwrap();

        // An "old" (v1) envelope written before the bump.
        let v1_payload = serde_json::to_value(sample_chain()).unwrap();
        let old_json = VersionedEnvelope::new(1, v1_payload).to_json().unwrap();

        let restored = Chain::from_versioned_json_with(&old_json, &registry).unwrap();
        assert_eq!(restored, sample_chain());
    }

    #[test]
    fn multi_step_chain_runs_in_order() {
        // v1 -> v2 -> v3, each step appends a marker so we can verify ordering.
        let mut registry = MigrationRegistry::with_target(3);
        registry
            .register_fn(1, 2, |mut payload| {
                let markers = payload
                    .as_object_mut()
                    .and_then(|o| o.get_mut("markers"))
                    .and_then(|m| m.as_array_mut());
                if let Some(markers) = markers {
                    markers.push(serde_json::json!("v2"));
                }
                Ok(payload)
            })
            .unwrap();
        registry
            .register_fn(2, 3, |mut payload| {
                let markers = payload
                    .as_object_mut()
                    .and_then(|o| o.get_mut("markers"))
                    .and_then(|m| m.as_array_mut());
                if let Some(markers) = markers {
                    markers.push(serde_json::json!("v3"));
                }
                Ok(payload)
            })
            .unwrap();

        let payload = serde_json::json!({ "markers": [] });
        let upgraded = registry.upgrade(1, payload).unwrap();
        assert_eq!(upgraded, serde_json::json!({ "markers": ["v2", "v3"] }));
    }

    #[test]
    fn newer_version_is_rejected() {
        let registry = MigrationRegistry::empty(); // target = CURRENT_WORKFLOW_VERSION
        let future = CURRENT_WORKFLOW_VERSION + 5;
        let json = VersionedEnvelope::new(future, serde_json::json!({ "tasks": [] }))
            .to_json()
            .unwrap();

        let err = Chain::from_versioned_json_with(&json, &registry).unwrap_err();
        assert!(err.is_invalid(), "expected Invalid, got {err:?}");
        assert!(err.to_string().contains("newer"));
    }

    #[test]
    fn missing_migrator_step_errors() {
        // Target 3 but only the 2->3 step registered: 1->2 is missing.
        let mut registry = MigrationRegistry::with_target(3);
        registry.register_fn(2, 3, Ok).unwrap();

        let json = VersionedEnvelope::new(1, serde_json::json!({ "tasks": [] }))
            .to_json()
            .unwrap();

        let err = decode_versioned::<Chain>(&json, &registry).unwrap_err();
        assert!(err.is_invalid(), "expected Invalid, got {err:?}");
        let msg = err.to_string();
        assert!(msg.contains("no migrator"), "unexpected message: {msg}");
        assert!(msg.contains('1'), "should mention the failing step: {msg}");
    }

    #[test]
    fn migrator_error_is_propagated() {
        let mut registry = MigrationRegistry::with_target(2);
        registry
            .register_fn(1, 2, |_payload| {
                Err(CanvasError::Invalid("boom".to_string()))
            })
            .unwrap();

        let json = VersionedEnvelope::new(1, serde_json::json!({}))
            .to_json()
            .unwrap();
        let err = decode_versioned::<Chain>(&json, &registry).unwrap_err();
        assert_eq!(err.to_string(), "Invalid workflow: boom");
    }

    #[test]
    fn register_rejects_non_contiguous_step() {
        let mut registry = MigrationRegistry::with_target(5);
        let err = registry.register_fn(1, 3, Ok).unwrap_err();
        assert!(err.is_invalid());
        assert!(err.to_string().contains("exactly one version"));
    }

    #[test]
    fn register_rejects_duplicate_step() {
        let mut registry = MigrationRegistry::with_target(3);
        registry.register_fn(1, 2, Ok).unwrap();
        let err = registry.register_fn(1, 2, Ok).unwrap_err();
        assert!(err.is_invalid());
        assert!(err.to_string().contains("already registered"));
    }

    #[test]
    fn upgrade_is_noop_when_already_current() {
        let registry = MigrationRegistry::with_target(CURRENT_WORKFLOW_VERSION);
        let payload = serde_json::json!({ "tasks": [], "extra": 7 });
        let out = registry
            .upgrade(CURRENT_WORKFLOW_VERSION, payload.clone())
            .unwrap();
        assert_eq!(out, payload);
    }

    #[test]
    fn malformed_json_is_serialization_error() {
        let err = Chain::from_versioned_json("not json").unwrap_err();
        assert!(
            err.is_serialization(),
            "expected Serialization, got {err:?}"
        );
    }

    #[test]
    fn missing_version_field_is_invalid() {
        let json = serde_json::json!({ "payload": { "tasks": [] } }).to_string();
        let err = schema_version_of(&json).unwrap_err();
        assert!(err.is_invalid());
        assert!(err.to_string().contains("schema_version"));
    }

    #[test]
    fn missing_payload_field_is_invalid() {
        let json = serde_json::json!({ "schema_version": 1 }).to_string();
        let err = VersionedEnvelope::from_json(&json).unwrap_err();
        assert!(err.is_invalid());
        assert!(err.to_string().contains("payload"));
    }

    #[test]
    fn registry_introspection() {
        let mut registry = MigrationRegistry::with_target(3);
        assert!(registry.is_empty());
        registry.register_fn(1, 2, Ok).unwrap();
        registry.register_fn(2, 3, Ok).unwrap();
        assert_eq!(registry.len(), 2);
        assert!(!registry.is_empty());
        assert!(registry.has_step(1));
        assert!(registry.has_step(2));
        assert!(!registry.has_step(3));
        assert_eq!(registry.target_version(), 3);
        // Debug output lists sorted steps.
        let dbg = format!("{registry:?}");
        assert!(dbg.contains("registered_steps"));
    }

    #[test]
    fn default_registry_targets_current_version() {
        let registry = MigrationRegistry::default();
        assert_eq!(registry.target_version(), CURRENT_WORKFLOW_VERSION);
        assert!(registry.is_empty());
    }
}
