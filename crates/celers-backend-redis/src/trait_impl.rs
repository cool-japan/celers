//! ResultBackend trait implementation for RedisResultBackend
//!
//! Contains the `impl ResultBackend for RedisResultBackend` with all required
//! and optimized batch operations including chunking integration.

use async_trait::async_trait;
use redis::AsyncCommands;
use std::time::Duration;
use uuid::Uuid;

use crate::backend::RedisResultBackend;
use crate::result_backend_trait::ResultBackend;
use crate::types::{BackendError, ChordState, Result, TaskMeta};
use crate::{chunking, compression, encryption, metrics};

#[async_trait]
impl ResultBackend for RedisResultBackend {
    async fn store_result(&mut self, task_id: Uuid, meta: &TaskMeta) -> Result<()> {
        let start = std::time::Instant::now();

        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let key = self.task_key(task_id);
        let value =
            serde_json::to_string(meta).map_err(|e| BackendError::Serialization(e.to_string()))?;

        let original_size = value.len();

        // Apply compression if configured
        let compressed = compression::maybe_compress(value.as_bytes(), &self.compression_config)
            .map_err(|e| BackendError::Serialization(format!("Compression error: {}", e)))?;

        // Apply encryption if configured
        let data = encryption::encrypt(&compressed, &self.encryption_config)
            .map_err(|e| BackendError::Serialization(format!("Encryption error: {}", e)))?;

        let stored_size = data.len();

        // Apply chunking for large payloads
        if self.chunker.needs_chunking(&data) {
            let (metadata, chunks) = self.chunker.split_chunks(&data);
            let sentinel = self.chunker.create_sentinel(&metadata);

            // Use pipeline for atomic multi-key store
            let mut pipe = redis::pipe();

            // Store sentinel at main key
            pipe.set(&key, &sentinel);

            // Store chunk metadata
            let meta_key = chunking::ResultChunker::metadata_key(&key);
            let meta_json = serde_json::to_vec(&metadata)
                .map_err(|e| BackendError::Serialization(format!("Chunk metadata error: {}", e)))?;
            pipe.set(&meta_key, &meta_json);

            // Store each chunk
            for (i, chunk) in chunks.iter().enumerate() {
                let chunk_key = format!("{}:chunk:{}", key, i);
                pipe.set(&chunk_key, chunk.as_slice());
            }

            // Set TTL on all keys if configured
            if let Some(ttl) = self.ttl_config.get_ttl(&meta.task_name) {
                let ttl_secs = ttl.as_secs() as i64;
                pipe.expire(&key, ttl_secs);
                pipe.expire(&meta_key, ttl_secs);
                for i in 0..chunks.len() {
                    let chunk_key = format!("{}:chunk:{}", key, i);
                    pipe.expire(&chunk_key, ttl_secs);
                }
            }

            pipe.query_async::<()>(&mut conn).await?;

            // Update cache
            self.cache.put(task_id, meta.clone());

            // Track compression stats
            self.compression_stats.record(original_size, stored_size);

            // Record metrics
            self.metrics
                .record_operation(metrics::OperationType::StoreResult, start.elapsed());
            self.metrics.record_data_size(original_size, stored_size);

            return Ok(());
        }

        conn.set::<_, _, ()>(&key, &data).await?;

        // Apply per-task TTL if configured
        if let Some(ttl) = self.ttl_config.get_ttl(&meta.task_name) {
            conn.expire::<_, ()>(&key, ttl.as_secs() as i64).await?;
        }

        // Update cache
        self.cache.put(task_id, meta.clone());

        // Track compression stats
        self.compression_stats.record(original_size, stored_size);

        // Record metrics
        self.metrics
            .record_operation(metrics::OperationType::StoreResult, start.elapsed());
        self.metrics.record_data_size(original_size, stored_size);

        Ok(())
    }

    async fn get_result(&mut self, task_id: Uuid) -> Result<Option<TaskMeta>> {
        let start = std::time::Instant::now();

        // Check cache first
        if let Some(meta) = self.cache.get(task_id) {
            self.metrics.record_cache_hit();
            self.metrics
                .record_operation(metrics::OperationType::GetResult, start.elapsed());
            return Ok(Some(meta));
        }

        // Cache miss, fetch from Redis
        self.metrics.record_cache_miss();

        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let key = self.task_key(task_id);

        let value: Option<Vec<u8>> = conn.get(&key).await?;
        let result = match value {
            Some(raw_data) => {
                // Check if the data is chunked and reassemble if needed
                let data = if chunking::ResultChunker::is_chunked(&raw_data) {
                    let metadata =
                        chunking::ResultChunker::parse_sentinel(&raw_data).map_err(|e| {
                            BackendError::Serialization(format!("Chunk sentinel error: {}", e))
                        })?;

                    // Read all chunks using pipeline
                    let chunk_keys =
                        chunking::ResultChunker::chunk_keys(&key, metadata.total_chunks);
                    let mut pipe = redis::pipe();
                    for ck in &chunk_keys {
                        pipe.get(ck);
                    }
                    let chunks: Vec<Vec<u8>> = pipe.query_async(&mut conn).await?;

                    // Reassemble
                    self.chunker
                        .reassemble_chunks(&metadata, &chunks)
                        .map_err(|e| {
                            BackendError::Serialization(format!("Chunk reassembly error: {}", e))
                        })?
                } else {
                    raw_data
                };

                // Decrypt if needed
                let decrypted = encryption::decrypt(&data, &self.encryption_config)
                    .map_err(|e| BackendError::Serialization(format!("Decryption error: {}", e)))?;

                // Decompress if needed
                let decompressed = compression::maybe_decompress(&decrypted).map_err(|e| {
                    BackendError::Serialization(format!("Decompression error: {}", e))
                })?;

                let v = String::from_utf8(decompressed)
                    .map_err(|e| BackendError::Serialization(format!("UTF-8 error: {}", e)))?;

                let meta: TaskMeta = serde_json::from_str(&v)
                    .map_err(|e| BackendError::Serialization(e.to_string()))?;

                // Store in cache
                self.cache.put(task_id, meta.clone());

                Ok(Some(meta))
            }
            None => Ok(None),
        };

        // Record metrics
        self.metrics
            .record_operation(metrics::OperationType::GetResult, start.elapsed());

        result
    }

    async fn delete_result(&mut self, task_id: Uuid) -> Result<()> {
        let start = std::time::Instant::now();

        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let key = self.task_key(task_id);

        // Clean up chunk keys if this was a chunked result (best-effort)
        let meta_key = chunking::ResultChunker::metadata_key(&key);
        if let Ok(Some(meta_bytes)) = conn.get::<_, Option<Vec<u8>>>(&meta_key).await {
            if let Ok(metadata) = serde_json::from_slice::<chunking::ChunkMetadata>(&meta_bytes) {
                let mut pipe = redis::pipe();
                pipe.del(&meta_key);
                for i in 0..metadata.total_chunks {
                    pipe.del(format!("{}:chunk:{}", key, i));
                }
                // Best-effort cleanup: ignore errors from chunk deletion
                let _ = pipe.query_async::<()>(&mut conn).await;
            }
        }

        // Delete the main key (sentinel or normal data)
        conn.del::<_, ()>(&key).await?;

        // Invalidate cache
        self.cache.invalidate(task_id);

        // Record metrics
        self.metrics
            .record_operation(metrics::OperationType::DeleteResult, start.elapsed());

        Ok(())
    }

    async fn set_expiration(&mut self, task_id: Uuid, ttl: Duration) -> Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let key = self.task_key(task_id);
        conn.expire::<_, ()>(&key, ttl.as_secs() as i64).await?;
        Ok(())
    }

    async fn chord_init(&mut self, state: ChordState) -> Result<()> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let key = self.chord_key(state.chord_id);
        let counter_key = self.chord_counter_key(state.chord_id);

        let value = serde_json::to_string(&state)
            .map_err(|e| BackendError::Serialization(e.to_string()))?;

        // Store chord state
        conn.set::<_, _, ()>(&key, value).await?;

        // Initialize counter to 0
        conn.set::<_, _, ()>(&counter_key, 0).await?;

        Ok(())
    }

    async fn chord_complete_task(&mut self, chord_id: Uuid) -> Result<usize> {
        let start = std::time::Instant::now();

        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let counter_key = self.chord_counter_key(chord_id);

        // Atomically increment and return new value
        let count: usize = conn.incr(&counter_key, 1).await?;

        // Record metrics
        self.metrics
            .record_operation(metrics::OperationType::ChordOperation, start.elapsed());

        Ok(count)
    }

    async fn chord_get_state(&mut self, chord_id: Uuid) -> Result<Option<ChordState>> {
        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let key = self.chord_key(chord_id);

        let value: Option<String> = conn.get(&key).await?;
        match value {
            Some(v) => {
                let state = serde_json::from_str(&v)
                    .map_err(|e| BackendError::Serialization(e.to_string()))?;
                Ok(Some(state))
            }
            None => Ok(None),
        }
    }

    async fn chord_cancel(&mut self, chord_id: Uuid, reason: Option<String>) -> Result<()> {
        if let Some(mut state) = self.chord_get_state(chord_id).await? {
            state.cancel(reason);
            self.chord_init(state).await?;
        }
        Ok(())
    }

    // Optimized batch operations using Redis pipelining

    async fn store_results_batch(&mut self, results: &[(Uuid, TaskMeta)]) -> Result<()> {
        if results.is_empty() {
            return Ok(());
        }

        let start = std::time::Instant::now();

        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let mut pipe = redis::pipe();

        for (task_id, meta) in results {
            let key = self.task_key(*task_id);
            let value = serde_json::to_string(meta)
                .map_err(|e| BackendError::Serialization(e.to_string()))?;

            let original_size = value.len();

            // Apply compression if configured
            let compressed =
                compression::maybe_compress(value.as_bytes(), &self.compression_config).map_err(
                    |e| BackendError::Serialization(format!("Compression error: {}", e)),
                )?;

            // Apply encryption if configured
            let data = encryption::encrypt(&compressed, &self.encryption_config)
                .map_err(|e| BackendError::Serialization(format!("Encryption error: {}", e)))?;

            let stored_size = data.len();

            self.metrics.record_data_size(original_size, stored_size);
            pipe.set(&key, data);
        }

        pipe.query_async::<()>(&mut conn).await?;

        // Record metrics
        self.metrics
            .record_operation(metrics::OperationType::StoreBatch, start.elapsed());

        Ok(())
    }

    async fn get_results_batch(&mut self, task_ids: &[Uuid]) -> Result<Vec<Option<TaskMeta>>> {
        if task_ids.is_empty() {
            return Ok(Vec::new());
        }

        let start = std::time::Instant::now();

        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let mut pipe = redis::pipe();

        for task_id in task_ids {
            let key = self.task_key(*task_id);
            pipe.get(&key);
        }

        let values: Vec<Option<Vec<u8>>> = pipe.query_async(&mut conn).await?;

        let mut results = Vec::with_capacity(values.len());
        for value_opt in values {
            match value_opt {
                Some(data) => {
                    // Decrypt if needed
                    let decrypted =
                        encryption::decrypt(&data, &self.encryption_config).map_err(|e| {
                            BackendError::Serialization(format!("Decryption error: {}", e))
                        })?;

                    // Decompress if needed
                    let decompressed = compression::maybe_decompress(&decrypted).map_err(|e| {
                        BackendError::Serialization(format!("Decompression error: {}", e))
                    })?;

                    let v = String::from_utf8(decompressed)
                        .map_err(|e| BackendError::Serialization(format!("UTF-8 error: {}", e)))?;

                    let meta = serde_json::from_str(&v)
                        .map_err(|e| BackendError::Serialization(e.to_string()))?;
                    results.push(Some(meta));
                }
                None => results.push(None),
            }
        }

        // Record metrics
        self.metrics
            .record_operation(metrics::OperationType::GetBatch, start.elapsed());

        Ok(results)
    }

    async fn delete_results_batch(&mut self, task_ids: &[Uuid]) -> Result<()> {
        if task_ids.is_empty() {
            return Ok(());
        }

        let start = std::time::Instant::now();

        let mut conn = self.client.get_multiplexed_async_connection().await?;
        let mut pipe = redis::pipe();

        for task_id in task_ids {
            let key = self.task_key(*task_id);
            pipe.del(&key);
        }

        pipe.query_async::<()>(&mut conn).await?;

        // Record metrics
        self.metrics
            .record_operation(metrics::OperationType::DeleteBatch, start.elapsed());

        Ok(())
    }
}
