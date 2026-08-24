//! Wire-format tail for the "extended" [`TaskMeta`] fields that have no
//! dedicated protobuf field of their own.
//!
//! `TaskMeta` (defined in `celers-backend-redis`) carries `progress`,
//! `version`, `tags`, `metadata`, `worker_hostname`, `runtime_ms`,
//! `memory_bytes`, `retries`, and `queue` in addition to the core fields
//! (`task_id`, `task_name`, `result`, timestamps, `worker`) that
//! `proto::TaskMeta` has dedicated fields for. Before this module existed,
//! [`crate::codec::to_proto_meta`]/[`crate::codec::from_proto_meta`] silently
//! dropped every one of those extended fields — writing hardcoded
//! `None`/`0`/empty on encode and decode — so a task's progress, tags, and
//! custom metadata never actually survived a round trip through the gRPC
//! backend.
//!
//! [`TaskMetaExtra`] is the serialized "tail" of `TaskMeta`: one JSON blob
//! carried in `proto::TaskMeta::extra_json`, mirroring the `extra` column
//! `celers-backend-db`'s `task_meta_extra` module writes for the same
//! reason. Keeping it as one opaque blob (rather than one proto field per
//! extended field) means new extended fields can be added to `TaskMeta` in
//! the future without a wire-format version bump — only
//! [`TaskMetaExtra::from_meta`] and [`TaskMetaExtra::apply_to`] need to grow
//! a field.

use celers_backend_redis::{ProgressInfo, TaskMeta};
use std::collections::HashMap;

/// The subset of [`TaskMeta`] fields persisted as one JSON blob in
/// `proto::TaskMeta::extra_json`, rather than as dedicated proto fields.
///
/// All fields default on missing/absent so that messages written before
/// this field existed (`extra_json` is `None`) decode to an all-default
/// value rather than failing to parse.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub(crate) struct TaskMetaExtra {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub progress: Option<ProgressInfo>,
    #[serde(default)]
    pub version: u32,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub worker_hostname: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_bytes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retries: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,
}

impl TaskMetaExtra {
    /// Extract the extended fields from a [`TaskMeta`] for serialization.
    pub(crate) fn from_meta(meta: &TaskMeta) -> Self {
        Self {
            progress: meta.progress.clone(),
            version: meta.version,
            tags: meta.tags.clone(),
            metadata: meta.metadata.clone(),
            worker_hostname: meta.worker_hostname.clone(),
            runtime_ms: meta.runtime_ms,
            memory_bytes: meta.memory_bytes,
            retries: meta.retries,
            queue: meta.queue.clone(),
        }
    }

    /// Serialize to the JSON text carried in `proto::TaskMeta::extra_json`.
    ///
    /// Fails only if a field cannot round-trip through JSON (e.g. a
    /// non-finite `f64` inside [`ProgressInfo::percent`]) — a genuine
    /// caller error, surfaced as [`celers_backend_redis::BackendError::Serialization`]
    /// by [`crate::codec::to_proto_meta`] rather than silently dropped.
    pub(crate) fn to_json_string(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Parse `proto::TaskMeta::extra_json` (if present) back into a
    /// `TaskMetaExtra`, defaulting to [`TaskMetaExtra::default`] when the
    /// field is absent (messages from writers that predate this field) or
    /// an empty string.
    ///
    /// A non-empty but malformed value is a genuine wire-corruption
    /// problem (not merely "predates this feature"), so it is surfaced as
    /// an error rather than silently discarded.
    pub(crate) fn from_field(raw: Option<&str>) -> Result<Self, serde_json::Error> {
        match raw {
            None => Ok(Self::default()),
            Some("") => Ok(Self::default()),
            Some(s) => serde_json::from_str(s),
        }
    }

    /// Overlay these extended fields onto an existing [`TaskMeta`] (whose
    /// core fields — `task_id`, `task_name`, `result`, timestamps, `worker`
    /// — were already populated from the message's dedicated fields).
    pub(crate) fn apply_to(self, meta: &mut TaskMeta) {
        meta.progress = self.progress;
        meta.version = self.version;
        meta.tags = self.tags;
        meta.metadata = self.metadata;
        meta.worker_hostname = self.worker_hostname;
        meta.runtime_ms = self.runtime_ms;
        meta.memory_bytes = self.memory_bytes;
        meta.retries = self.retries;
        meta.queue = self.queue;
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use uuid::Uuid;

    fn sample_meta() -> TaskMeta {
        let mut meta = TaskMeta::new(Uuid::new_v4(), "demo_task".to_string());
        meta.progress = Some(ProgressInfo::new(3, 10));
        meta.version = 2;
        meta.tags = vec!["a".to_string(), "b".to_string()];
        meta.metadata
            .insert("k".to_string(), serde_json::json!("v"));
        meta.worker_hostname = Some("host-1".to_string());
        meta.runtime_ms = Some(1234);
        meta.memory_bytes = Some(4096);
        meta.retries = Some(2);
        meta.queue = Some("high".to_string());
        meta
    }

    #[test]
    fn round_trips_every_extended_field() {
        let original = sample_meta();
        let extra = TaskMetaExtra::from_meta(&original);
        let json = extra.to_json_string().expect("serialize");

        let parsed = TaskMetaExtra::from_field(Some(&json)).expect("deserialize");

        let mut restored = TaskMeta::new(original.task_id, original.task_name.clone());
        parsed.apply_to(&mut restored);

        assert_eq!(restored.progress.map(|p| p.current), Some(3));
        assert_eq!(restored.version, 2);
        assert_eq!(restored.tags, vec!["a".to_string(), "b".to_string()]);
        assert_eq!(restored.metadata.get("k"), Some(&serde_json::json!("v")));
        assert_eq!(restored.worker_hostname.as_deref(), Some("host-1"));
        assert_eq!(restored.runtime_ms, Some(1234));
        assert_eq!(restored.memory_bytes, Some(4096));
        assert_eq!(restored.retries, Some(2));
        assert_eq!(restored.queue.as_deref(), Some("high"));
    }

    #[test]
    fn from_field_defaults_on_absent() {
        let extra = TaskMetaExtra::from_field(None).expect("absent extra_json");
        assert_eq!(extra.version, 0);
        assert!(extra.tags.is_empty());
        assert!(extra.progress.is_none());
    }

    #[test]
    fn from_field_defaults_on_empty_string() {
        let extra = TaskMetaExtra::from_field(Some("")).expect("empty extra_json");
        assert_eq!(extra.version, 0);
    }

    #[test]
    fn from_field_errors_on_malformed_json() {
        let result = TaskMetaExtra::from_field(Some("{not valid json"));
        assert!(result.is_err());
    }

    #[test]
    fn default_extra_serializes_compactly() {
        // A brand-new TaskMeta (no progress/tags/metadata set) should
        // serialize to a small JSON object, not error or bloat the wire
        // payload.
        let meta = TaskMeta::new(Uuid::new_v4(), "t".to_string());
        let extra = TaskMetaExtra::from_meta(&meta);
        let json = extra.to_json_string().expect("serialize default");
        // Round-trips back to an all-default value.
        let parsed = TaskMetaExtra::from_field(Some(&json)).expect("deserialize");
        assert_eq!(parsed.version, 0);
        assert!(parsed.tags.is_empty());
    }
}
