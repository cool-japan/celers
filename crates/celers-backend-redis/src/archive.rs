//! Result archival for the Redis backend
//!
//! Archiving copies a task result to `{key_prefix}archive:{task_id}` with an
//! independent (usually much longer) TTL, so important results survive normal
//! result expiry.
//!
//! The archive is written through the *normal* encode path rather than Redis
//! `COPY`: `COPY` duplicates only the primary key, which silently loses every
//! chunk of a chunked result, and it cannot report that the source was missing
//! (a `COPY` of a non-existent key succeeds with `0`). Re-encoding also means
//! archived values are compressed and encrypted exactly like live ones.

use std::time::Duration;

use uuid::Uuid;

use crate::backend::RedisResultBackend;
use crate::codec;
use crate::types::{BackendError, Result, TaskMeta};

impl RedisResultBackend {
    /// Copy a task result to its archive key with a longer TTL
    ///
    /// # Errors
    /// Returns [`BackendError::NotFound`] when the task has no stored result —
    /// previously this silently "succeeded" while archiving nothing.
    ///
    /// # Example
    /// ```no_run
    /// use celers_backend_redis::RedisResultBackend;
    /// use uuid::Uuid;
    /// use std::time::Duration;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut backend = RedisResultBackend::new("redis://localhost")?;
    /// let task_id = Uuid::new_v4();
    ///
    /// // Archive with 90-day retention
    /// backend.archive_result(task_id, Duration::from_secs(90 * 86400)).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn archive_result(&mut self, task_id: Uuid, archive_ttl: Duration) -> Result<()> {
        let Some(meta) = self.get_result_uncached(task_id).await? else {
            return Err(BackendError::NotFound(task_id));
        };

        let archive_key = self.archive_key(task_id);
        self.write_meta_to_key(&archive_key, &meta, Some(archive_ttl), None)
            .await?;

        Ok(())
    }

    /// Retrieve an archived task result
    ///
    /// Decodes through the same pipeline as live results, so compressed,
    /// encrypted and chunked archives all read back correctly.
    pub async fn get_archived_result(&mut self, task_id: Uuid) -> Result<Option<TaskMeta>> {
        let archive_key = self.archive_key(task_id);
        let mut found = self
            .read_metas_from_keys(std::slice::from_ref(&archive_key))
            .await?;
        Ok(found.pop().flatten())
    }

    /// Remove an archived task result together with any chunk keys it owns.
    pub async fn delete_archived_result(&mut self, task_id: Uuid) -> Result<bool> {
        let archive_key = self.archive_key(task_id);
        let command = codec::delete_command(&archive_key);

        let deleted: i64 = self
            .run_with_retry("delete_archived_result", |mut conn| {
                let command = command.clone();
                async move { Ok(command.query_async(&mut conn).await?) }
            })
            .await?;

        Ok(deleted > 0)
    }

    /// Bulk archive multiple task results
    ///
    /// Returns the number of results actually archived. Tasks with no stored
    /// result are skipped; any other failure is propagated rather than counted
    /// as a success.
    pub async fn archive_results_batch(
        &mut self,
        task_ids: &[Uuid],
        archive_ttl: Duration,
    ) -> Result<usize> {
        let mut count = 0;

        for &task_id in task_ids {
            match self.archive_result(task_id, archive_ttl).await {
                Ok(()) => count += 1,
                Err(BackendError::NotFound(_)) => continue,
                Err(e) => return Err(e),
            }
        }

        Ok(count)
    }
}
