//! ResultBackend trait implementation for RedisResultBackend
//!
//! Contains the `impl ResultBackend for RedisResultBackend` with all required
//! and optimized batch operations, plus the low-level inherent helpers every
//! read/write path shares.
//!
//! All writes go through [`crate::codec::write_command`] and all reads through
//! [`crate::codec::decode_many`], so compression, encryption, chunking and TTL
//! behave identically no matter which entry point a caller uses.

use async_trait::async_trait;
use chrono::Utc;
use redis::aio::ConnectionManager;
use redis::AsyncCommands;
use std::time::Duration;
use uuid::Uuid;

use crate::backend::RedisResultBackend;
use crate::result_backend_trait::ResultBackend;
use crate::telemetry::{OperationContext, OperationSpan, OperationType as TelemetryOp};
use crate::types::{BackendError, ChordState, Result, TaskMeta};
use crate::{codec, metrics};

// =============================================================================
// Low-level shared helpers
// =============================================================================

impl RedisResultBackend {
    /// Record a failed operation on the metrics collector.
    ///
    /// Without this, `BackendMetrics::error_count()` stayed at zero forever and
    /// the health check's error-rate branch was dead code.
    pub(crate) fn record_failure(&self, error: &BackendError) {
        self.metrics.record_error(error.category());
    }

    /// Execute a Redis operation with the configured timeout and retry policy.
    ///
    /// The closure receives an owned connection handle, so it never has to
    /// borrow `self` mutably — which is what makes retrying possible at all.
    pub(crate) async fn run_with_retry<T, F, Fut>(&self, what: &str, mut op: F) -> Result<T>
    where
        F: FnMut(ConnectionManager) -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let max_attempts = self.retry_strategy.max_attempts.max(1);
        let mut attempt: u32 = 0;

        loop {
            attempt += 1;

            let attempt_result = self
                .with_timeout(what, async {
                    let conn = self.connection().await?;
                    op(conn).await
                })
                .await;

            match attempt_result {
                Ok(value) => return Ok(value),
                Err(error) => {
                    if attempt >= max_attempts || !self.retry_strategy.is_retryable(&error) {
                        self.record_failure(&error);
                        return Err(error);
                    }

                    let backoff = self.retry_strategy.backoff_duration(attempt - 1);
                    tracing::debug!(
                        operation = what,
                        attempt,
                        backoff_ms = backoff.as_millis(),
                        error = %error,
                        "Retrying Redis operation after transient failure"
                    );
                    tokio::time::sleep(backoff).await;
                }
            }
        }
    }

    /// Encode `meta` and write it to `key`.
    ///
    /// Handles chunking, cleanup of chunk keys left by a previous encoding, TTL
    /// and the optional compare-and-swap `guard` in a single atomic
    /// server-side step. Returns `false` only when a `guard` was supplied and
    /// did not match the bytes currently stored.
    ///
    /// `notify` controls the Celery-compatible pub/sub announcement (see
    /// [`codec::publish_command`]): `true` for a write to the *live* result
    /// key (a real Celery client's `AsyncResult.get()` waits on exactly that
    /// channel), `false` for a write to any other key derived from it —
    /// an archival copy or a historical version — which nothing ever
    /// subscribes to. The publish only fires when the write actually
    /// happened (`written == 1`); a rejected compare-and-swap changes
    /// nothing, so there is nothing to announce.
    pub(crate) async fn write_meta_to_key(
        &self,
        key: &str,
        meta: &TaskMeta,
        ttl: Option<Duration>,
        guard: Option<&[u8]>,
        notify: bool,
    ) -> Result<bool> {
        let encoded = match codec::encode_meta(
            meta,
            &self.compression_config,
            &self.encryption_config,
            &self.chunker,
        ) {
            Ok(encoded) => encoded,
            Err(error) => {
                self.record_failure(&error);
                return Err(error);
            }
        };

        if encoded.is_chunked() {
            tracing::debug!(
                key,
                chunks = encoded.chunks.len(),
                stored_bytes = encoded.stored_size,
                "Storing chunked task result"
            );
        }

        let command = codec::write_command(key, &encoded, ttl, guard);

        let written: i64 = self
            .run_with_retry("write_result", |mut conn| {
                let command = command.clone();
                async move { Ok(command.query_async(&mut conn).await?) }
            })
            .await?;

        self.compression_stats
            .record(encoded.original_size, encoded.stored_size);
        self.metrics
            .record_data_size(encoded.original_size, encoded.stored_size);

        if written == 1 && notify {
            self.publish_key_notification(key, &encoded.main).await;
        }

        Ok(written == 1)
    }

    /// Publish the exact bytes just written to `key`, on the Celery-compatible
    /// channel named after the key itself.
    ///
    /// This is the other half of `write_meta_to_key`'s "SET and PUBLISH"
    /// contract — see [`codec::publish_command`] for why the channel and the
    /// payload must be exactly what they are. Distinct from
    /// [`publish_notification`](Self::publish_notification)'s
    /// `:notify`-suffixed channel, which is CeleRS' own internal wait
    /// mechanism and carries a different (lighter) payload; both fire so a
    /// waiter using either mechanism is woken.
    ///
    /// Best-effort, exactly like `publish_notification`: a publish failure
    /// is logged and swallowed rather than propagated, because the result
    /// itself is already durably stored and a waiter that misses the
    /// notification simply falls back to polling.
    pub(crate) async fn publish_key_notification(&self, key: &str, payload: &[u8]) {
        if !self.notify_on_store {
            return;
        }

        if let Ok(mut conn) = self.connection().await {
            let cmd = codec::publish_command(key, payload);
            if let Err(e) = cmd.query_async::<i64>(&mut conn).await {
                tracing::debug!(
                    key,
                    error = %e,
                    "Failed to publish Celery-compatible result notification"
                );
            }
        }
    }

    /// Read and decode the values stored at `keys`, reassembling chunked ones.
    pub(crate) async fn read_metas_from_keys(
        &self,
        keys: &[String],
    ) -> Result<Vec<Option<TaskMeta>>> {
        if keys.is_empty() {
            return Ok(Vec::new());
        }

        let chunker = &self.chunker;
        let encryption_config = &self.encryption_config;

        self.run_with_retry("read_results", |mut conn| async move {
            let mut pipe = redis::pipe();
            for key in keys {
                pipe.get(key);
            }
            let raws: Vec<Option<Vec<u8>>> = pipe.query_async(&mut conn).await?;
            codec::decode_many(&mut conn, keys, raws, chunker, encryption_config).await
        })
        .await
    }

    /// Read the raw stored bytes for a key without decoding them.
    ///
    /// Used by [`compare_and_swap`](Self::compare_and_swap) as the optimistic
    /// concurrency token: those exact bytes are what the server compares.
    pub(crate) async fn read_raw(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let key = key.to_string();
        self.run_with_retry("read_raw", |mut conn| {
            let key = key.clone();
            async move { Ok(conn.get::<_, Option<Vec<u8>>>(&key).await?) }
        })
        .await
    }

    /// Fetch a result straight from Redis, bypassing the in-memory cache.
    ///
    /// Polling loops and monitoring probes must use this: the cache is
    /// authoritative only for terminal states.
    pub async fn get_result_uncached(&mut self, task_id: Uuid) -> Result<Option<TaskMeta>> {
        let start = std::time::Instant::now();
        let span = OperationSpan::start(
            self.telemetry.as_ref(),
            OperationContext::new(TelemetryOp::Get).with_task_id(task_id),
        );

        let key = self.task_key(task_id);
        let result = self.read_metas_from_keys(std::slice::from_ref(&key)).await;

        self.metrics
            .record_operation(metrics::OperationType::GetResult, start.elapsed());

        match result {
            Ok(mut metas) => {
                let meta = metas.pop().flatten();
                if let Some(ref meta) = meta {
                    self.cache_terminal(task_id, meta);
                }
                span.ok(None);
                Ok(meta)
            }
            Err(error) => {
                span.err(&error);
                Err(error)
            }
        }
    }

    /// Cache a result only when it is in a terminal state.
    ///
    /// Caching `Pending`/`Started`/`Retry` would pin a task's observed state
    /// for the whole cache TTL, so every later poll would return the stale
    /// value and waiters would never see the real completion.
    pub(crate) fn cache_terminal(&self, task_id: Uuid, meta: &TaskMeta) {
        if meta.is_terminal() {
            self.cache.put(task_id, meta.clone());
        } else {
            // A task can leave a terminal state again (a retry after failure),
            // so drop any entry that would now be stale.
            self.cache.invalidate(task_id);
        }
    }

    /// Announce a result write so waiters wake up immediately instead of
    /// waiting out their next poll interval.
    pub(crate) async fn publish_notification(&self, task_id: Uuid, meta: &TaskMeta) {
        if !self.notify_on_store {
            return;
        }

        let channel = self.notify_channel(task_id);
        let payload = meta.result.to_string();

        if let Ok(mut conn) = self.connection().await {
            if let Err(e) = conn.publish::<_, _, ()>(&channel, payload).await {
                // Notifications are an optimisation: waiters fall back to
                // polling, so a publish failure must never fail the write.
                tracing::debug!(%task_id, error = %e, "Failed to publish result notification");
            }
        }
    }
}

// =============================================================================
// ResultBackend implementation
// =============================================================================

#[async_trait]
impl ResultBackend for RedisResultBackend {
    async fn store_result(&mut self, task_id: Uuid, meta: &TaskMeta) -> Result<()> {
        let start = std::time::Instant::now();
        let span = OperationSpan::start(
            self.telemetry.as_ref(),
            OperationContext::new(TelemetryOp::Store).with_task_id(task_id),
        );

        let key = self.task_key(task_id);
        let ttl = self.ttl_config.get_ttl(&meta.task_name);

        match self.write_meta_to_key(&key, meta, ttl, None, true).await {
            Ok(_) => {
                self.cache_terminal(task_id, meta);
                self.publish_notification(task_id, meta).await;
                self.metrics
                    .record_operation(metrics::OperationType::StoreResult, start.elapsed());
                span.ok(None);
                Ok(())
            }
            Err(error) => {
                span.err(&error);
                Err(error)
            }
        }
    }

    async fn get_result(&mut self, task_id: Uuid) -> Result<Option<TaskMeta>> {
        let start = std::time::Instant::now();

        // The cache only ever holds terminal results, so a hit is authoritative.
        if let Some(meta) = self.cache.get(task_id) {
            if meta.is_terminal() {
                self.metrics.record_cache_hit();
                self.metrics
                    .record_operation(metrics::OperationType::GetResult, start.elapsed());
                return Ok(Some(meta));
            }
            // Defensive: a non-terminal entry must never be served.
            self.cache.invalidate(task_id);
        }

        self.metrics.record_cache_miss();
        self.get_result_uncached(task_id).await
    }

    async fn delete_result(&mut self, task_id: Uuid) -> Result<()> {
        let start = std::time::Instant::now();
        let span = OperationSpan::start(
            self.telemetry.as_ref(),
            OperationContext::new(TelemetryOp::Delete).with_task_id(task_id),
        );

        let key = self.task_key(task_id);
        let command = codec::delete_command(&key);

        let outcome = self
            .run_with_retry("delete_result", |mut conn| {
                let command = command.clone();
                async move { Ok(command.query_async::<i64>(&mut conn).await?) }
            })
            .await;

        self.cache.invalidate(task_id);
        self.metrics
            .record_operation(metrics::OperationType::DeleteResult, start.elapsed());

        match outcome {
            Ok(_) => {
                span.ok(None);
                Ok(())
            }
            Err(error) => {
                span.err(&error);
                Err(error)
            }
        }
    }

    async fn set_expiration(&mut self, task_id: Uuid, ttl: Duration) -> Result<()> {
        let key = self.task_key(task_id);
        let command = codec::expire_command(&key, ttl);

        self.run_with_retry("set_expiration", |mut conn| {
            let command = command.clone();
            async move {
                command.query_async::<i64>(&mut conn).await?;
                Ok(())
            }
        })
        .await
    }

    async fn chord_init(&mut self, state: ChordState) -> Result<()> {
        let key = self.chord_key(state.chord_id);
        let counter_key = self.chord_counter_key(state.chord_id);
        let value = match serde_json::to_string(&state) {
            Ok(value) => value,
            Err(e) => {
                let error = BackendError::Serialization(e.to_string());
                self.record_failure(&error);
                return Err(error);
            }
        };
        let ttl_secs = self.ttl_config.chord_ttl().map(|t| t.as_secs().max(1));

        self.run_with_retry("chord_init", |mut conn| {
            let key = key.clone();
            let counter_key = counter_key.clone();
            let value = value.clone();
            async move {
                // State and counter are created together, and both carry a TTL
                // so an abandoned chord cannot leak two keys forever.
                let mut pipe = redis::pipe();
                pipe.atomic();
                match ttl_secs {
                    Some(secs) => {
                        pipe.cmd("SET").arg(&key).arg(&value).arg("EX").arg(secs);
                        pipe.cmd("SET").arg(&counter_key).arg(0).arg("EX").arg(secs);
                    }
                    None => {
                        pipe.set(&key, &value);
                        pipe.set(&counter_key, 0);
                    }
                }
                pipe.query_async::<()>(&mut conn).await?;
                Ok(())
            }
        })
        .await
    }

    async fn chord_update_state(&mut self, state: ChordState) -> Result<()> {
        let key = self.chord_key(state.chord_id);
        let value = match serde_json::to_string(&state) {
            Ok(value) => value,
            Err(e) => {
                let error = BackendError::Serialization(e.to_string());
                self.record_failure(&error);
                return Err(error);
            }
        };
        let ttl_secs = self.ttl_config.chord_ttl().map(|t| t.as_secs().max(1));

        self.run_with_retry("chord_update_state", |mut conn| {
            let key = key.clone();
            let value = value.clone();
            async move {
                // Deliberately does NOT touch the completion counter: this
                // persists a state change, it is not a reset.
                match ttl_secs {
                    Some(secs) => {
                        redis::cmd("SET")
                            .arg(&key)
                            .arg(&value)
                            .arg("EX")
                            .arg(secs)
                            .query_async::<()>(&mut conn)
                            .await?;
                    }
                    None => {
                        conn.set::<_, _, ()>(&key, &value).await?;
                    }
                }
                Ok(())
            }
        })
        .await
    }

    async fn chord_complete_task(&mut self, chord_id: Uuid) -> Result<usize> {
        let start = std::time::Instant::now();
        let counter_key = self.chord_counter_key(chord_id);

        let count: usize = self
            .run_with_retry("chord_complete_task", |mut conn| {
                let counter_key = counter_key.clone();
                async move { Ok(conn.incr::<_, _, usize>(&counter_key, 1).await?) }
            })
            .await?;

        self.metrics
            .record_operation(metrics::OperationType::ChordOperation, start.elapsed());

        Ok(count)
    }

    async fn chord_get_state(&mut self, chord_id: Uuid) -> Result<Option<ChordState>> {
        let key = self.chord_key(chord_id);
        let counter_key = self.chord_counter_key(chord_id);

        let (value, counter): (Option<String>, Option<usize>) = self
            .run_with_retry("chord_get_state", |mut conn| {
                let key = key.clone();
                let counter_key = counter_key.clone();
                async move {
                    let mut pipe = redis::pipe();
                    pipe.get(&key);
                    pipe.get(&counter_key);
                    Ok(pipe.query_async(&mut conn).await?)
                }
            })
            .await?;

        match value {
            Some(v) => {
                let mut state: ChordState = match serde_json::from_str(&v) {
                    Ok(state) => state,
                    Err(e) => {
                        let error = BackendError::Serialization(e.to_string());
                        self.record_failure(&error);
                        return Err(error);
                    }
                };
                // The completion count lives in its own key so it can be
                // incremented atomically; merge it back in so `is_complete()`,
                // `percent_complete()` and chord cleanup all see reality.
                if let Some(completed) = counter {
                    state.completed = completed.max(state.completed);
                }
                Ok(Some(state))
            }
            None => Ok(None),
        }
    }

    async fn chord_cancel(&mut self, chord_id: Uuid, reason: Option<String>) -> Result<()> {
        if let Some(mut state) = self.chord_get_state(chord_id).await? {
            state.cancel(reason);
            // Must not reset the completion counter: tasks that already
            // finished stay finished.
            self.chord_update_state(state).await?;
        }
        Ok(())
    }

    // Optimized batch operations using Redis pipelining

    async fn store_results_batch(&mut self, results: &[(Uuid, TaskMeta)]) -> Result<()> {
        if results.is_empty() {
            return Ok(());
        }

        let start = std::time::Instant::now();
        let span = OperationSpan::start(
            self.telemetry.as_ref(),
            OperationContext::new(TelemetryOp::BatchStore).with_batch_size(results.len()),
        );

        let batch_limit = self.pipeline_config.max_batch_size.max(1);
        let mut stored_total = 0usize;

        for group in results.chunks(batch_limit) {
            let mut commands = Vec::with_capacity(group.len());

            for (task_id, meta) in group {
                let key = self.task_key(*task_id);
                let encoded = match codec::encode_meta(
                    meta,
                    &self.compression_config,
                    &self.encryption_config,
                    &self.chunker,
                ) {
                    Ok(encoded) => encoded,
                    Err(error) => {
                        self.record_failure(&error);
                        span.err(&error);
                        return Err(error);
                    }
                };

                stored_total += encoded.stored_size;
                self.metrics
                    .record_data_size(encoded.original_size, encoded.stored_size);
                self.compression_stats
                    .record(encoded.original_size, encoded.stored_size);

                // The batch path applies exactly the same TTL policy as the
                // single-key path.
                let ttl = self.ttl_config.get_ttl(&meta.task_name);
                // No CAS guard on a batch write, so it always lands — the
                // Celery-compatible PUBLISH can safely ride the same
                // pipeline unconditionally (see `write_meta_to_key`'s doc
                // comment for why the single-key path cannot do this when a
                // guard is present).
                commands.extend(codec::write_and_notify_commands(
                    &key,
                    &encoded,
                    ttl,
                    None,
                    self.notify_on_store,
                ));
            }

            let outcome = self
                .run_with_retry("store_results_batch", |mut conn| {
                    let commands = commands.clone();
                    async move {
                        let mut pipe = redis::pipe();
                        for command in commands {
                            pipe.add_command(command);
                        }
                        pipe.query_async::<Vec<i64>>(&mut conn).await?;
                        Ok(())
                    }
                })
                .await;

            if let Err(error) = outcome {
                span.err(&error);
                return Err(error);
            }
        }

        // Keep the cache consistent with what was just written.
        for (task_id, meta) in results {
            self.cache_terminal(*task_id, meta);
        }

        self.metrics
            .record_operation(metrics::OperationType::StoreBatch, start.elapsed());
        span.ok(Some(stored_total));

        Ok(())
    }

    async fn get_results_batch(&mut self, task_ids: &[Uuid]) -> Result<Vec<Option<TaskMeta>>> {
        if task_ids.is_empty() {
            return Ok(Vec::new());
        }

        let start = std::time::Instant::now();
        let span = OperationSpan::start(
            self.telemetry.as_ref(),
            OperationContext::new(TelemetryOp::BatchGet).with_batch_size(task_ids.len()),
        );

        let batch_limit = self.pipeline_config.max_batch_size.max(1);
        let mut results = Vec::with_capacity(task_ids.len());

        for group in task_ids.chunks(batch_limit) {
            let keys: Vec<String> = group.iter().map(|id| self.task_key(*id)).collect();
            match self.read_metas_from_keys(&keys).await {
                Ok(metas) => results.extend(metas),
                Err(error) => {
                    span.err(&error);
                    return Err(error);
                }
            }
        }

        for (task_id, meta) in task_ids.iter().zip(results.iter()) {
            if let Some(meta) = meta {
                self.cache_terminal(*task_id, meta);
            }
        }

        self.metrics
            .record_operation(metrics::OperationType::GetBatch, start.elapsed());
        span.ok(None);

        Ok(results)
    }

    async fn delete_results_batch(&mut self, task_ids: &[Uuid]) -> Result<()> {
        if task_ids.is_empty() {
            return Ok(());
        }

        let start = std::time::Instant::now();
        let batch_limit = self.pipeline_config.max_batch_size.max(1);

        for group in task_ids.chunks(batch_limit) {
            let commands: Vec<redis::Cmd> = group
                .iter()
                .map(|id| codec::delete_command(&self.task_key(*id)))
                .collect();

            self.run_with_retry("delete_results_batch", |mut conn| {
                let commands = commands.clone();
                async move {
                    let mut pipe = redis::pipe();
                    for command in commands {
                        pipe.add_command(command);
                    }
                    pipe.query_async::<Vec<i64>>(&mut conn).await?;
                    Ok(())
                }
            })
            .await?;
        }

        // Deleted results must not keep being served from the cache.
        for task_id in task_ids {
            self.cache.invalidate(*task_id);
        }

        self.metrics
            .record_operation(metrics::OperationType::DeleteBatch, start.elapsed());

        Ok(())
    }

    async fn store_versioned_result(&mut self, task_id: Uuid, meta: &TaskMeta) -> Result<u32> {
        self.store_versioned_result_impl(task_id, meta).await
    }

    async fn get_result_version(
        &mut self,
        task_id: Uuid,
        version: u32,
    ) -> Result<Option<TaskMeta>> {
        self.get_result_version_impl(task_id, version).await
    }

    async fn mark_completed(&mut self, task_id: Uuid, result: crate::TaskResult) -> Result<()> {
        // Read through Redis rather than the cache so a concurrently updated
        // record is not clobbered with a stale copy.
        if let Some(mut meta) = self.get_result_uncached(task_id).await? {
            meta.result = result;
            meta.completed_at = Some(Utc::now());
            self.store_result(task_id, &meta).await?;
        }
        Ok(())
    }
}
