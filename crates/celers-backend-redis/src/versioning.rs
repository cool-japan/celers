//! Versioned result history for the Redis backend
//!
//! [`store_versioned_result`](crate::ResultBackend::store_versioned_result)
//! keeps a bounded history of previous payloads so
//! [`get_result_version`](crate::ResultBackend::get_result_version) can answer
//! for an older version instead of silently substituting the latest one.
//!
//! # Key layout
//!
//! * `{task_key}:version` — monotonically increasing counter (`INCR`, so two
//!   concurrent writers can never be handed the same version).
//! * `{task_key}:v{n}` — the payload of version `n`, written through the same
//!   encode path (compression, encryption, chunking, TTL) as the live record.
//!
//! Retention is capped by [`VersioningConfig::max_versions`]; the oldest
//! version outside the window is deleted on every write so history cannot grow
//! without bound.

use redis::AsyncCommands;
use uuid::Uuid;

use crate::backend::RedisResultBackend;
use crate::codec;
use crate::result_backend_trait::ResultBackend;
use crate::types::{Result, TaskMeta};

impl RedisResultBackend {
    /// Implementation behind
    /// [`ResultBackend::store_versioned_result`](crate::ResultBackend::store_versioned_result).
    pub(crate) async fn store_versioned_result_impl(
        &mut self,
        task_id: Uuid,
        meta: &TaskMeta,
    ) -> Result<u32> {
        let counter_key = self.version_counter_key(task_id);
        let ttl = self.ttl_config.get_ttl(&meta.task_name);
        let ttl_secs = ttl.map(|t| t.as_secs().max(1));

        // A server-side INCR is the only way to make concurrent writers agree
        // on distinct versions; the previous read-modify-write under-counted.
        let new_version: u32 = self
            .run_with_retry("store_versioned_result", |mut conn| {
                let counter_key = counter_key.clone();
                async move {
                    let mut pipe = redis::pipe();
                    pipe.atomic();
                    pipe.incr(&counter_key, 1);
                    if let Some(secs) = ttl_secs {
                        pipe.expire(&counter_key, secs as i64).ignore();
                    }
                    let (version,): (u32,) = pipe.query_async(&mut conn).await?;
                    Ok(version)
                }
            })
            .await?;

        let mut versioned_meta = meta.clone();
        versioned_meta.version = new_version;

        // Live record first: a crash before the history write leaves the live
        // record at version N with no `{task_key}:vN` entry, which
        // `get_result_version` still answers correctly from the live record.
        // The reverse order would publish history for a version the live record
        // has not reached.
        self.store_result(task_id, &versioned_meta).await?;

        if self.versioning.enabled && self.versioning.max_versions > 0 {
            let version_key = self.version_key(task_id, new_version);
            // Not the live key: nothing subscribes to a historical version's
            // channel, and `store_result` above already announced the
            // *live* record's write.
            self.write_meta_to_key(&version_key, &versioned_meta, ttl, None, false)
                .await?;

            // Drop the version that just fell out of the retention window.
            if let Some(expired) = new_version.checked_sub(self.versioning.max_versions) {
                if expired > 0 {
                    let stale_key = self.version_key(task_id, expired);
                    let command = codec::delete_command(&stale_key);
                    self.run_with_retry("prune_result_version", |mut conn| {
                        let command = command.clone();
                        async move {
                            command.query_async::<i64>(&mut conn).await?;
                            Ok(())
                        }
                    })
                    .await?;
                }
            }
        }

        Ok(new_version)
    }

    /// Implementation behind
    /// [`ResultBackend::get_result_version`](crate::ResultBackend::get_result_version).
    ///
    /// Version `0` means "latest". A version that was never written, or that
    /// has already fallen out of the retention window, yields `Ok(None)` rather
    /// than a different version's data.
    pub(crate) async fn get_result_version_impl(
        &mut self,
        task_id: Uuid,
        version: u32,
    ) -> Result<Option<TaskMeta>> {
        if version == 0 {
            return self.get_result(task_id).await;
        }

        // Look at both the historical key and the live record: the requested
        // version may simply be the current one.
        let keys = vec![self.version_key(task_id, version), self.task_key(task_id)];
        let mut found = self.read_metas_from_keys(&keys).await?;

        let live = found.pop().flatten();
        let historical = found.pop().flatten();

        if let Some(meta) = historical {
            return Ok(Some(meta));
        }

        match live {
            Some(meta) if meta.version == version => Ok(Some(meta)),
            _ => Ok(None),
        }
    }

    /// List the versions currently retained for a task, oldest first.
    ///
    /// Only versions still inside the retention window are reported, and only
    /// those whose history key exists — the newest version can be absent here
    /// if a crash interrupted the write between the live record and its history
    /// entry. [`Self::get_result_version_impl`] still answers for it from the
    /// live record.
    pub async fn list_result_versions(&mut self, task_id: Uuid) -> Result<Vec<u32>> {
        let counter_key = self.version_counter_key(task_id);

        let latest: Option<u32> = self
            .run_with_retry("list_result_versions", |mut conn| {
                let counter_key = counter_key.clone();
                async move { Ok(conn.get::<_, Option<u32>>(&counter_key).await?) }
            })
            .await?;

        let Some(latest) = latest else {
            return Ok(Vec::new());
        };

        if !self.versioning.enabled || self.versioning.max_versions == 0 {
            return Ok(vec![latest]);
        }

        let oldest = latest
            .saturating_sub(self.versioning.max_versions - 1)
            .max(1);
        let candidates: Vec<u32> = (oldest..=latest).collect();
        let keys: Vec<String> = candidates
            .iter()
            .map(|v| self.version_key(task_id, *v))
            .collect();

        let present = self.read_metas_from_keys(&keys).await?;
        Ok(candidates
            .into_iter()
            .zip(present)
            .filter_map(|(version, meta)| meta.map(|_| version))
            .collect())
    }
}
