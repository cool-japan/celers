//! Shared encode/decode pipeline for stored task results.
//!
//! Every *write* path of the Redis backend (`store_result`, `store_results_batch`,
//! `compare_and_swap`, `atomic_store_multiple`, archival and versioned writes)
//! funnels through [`encode_meta`] + [`write_command`], and every *read* path
//! funnels through [`decode_many`] / [`decode_payload`].
//!
//! Keeping a single code path guarantees that compression, encryption, chunking
//! and TTL are applied uniformly: a value written by one API can always be read
//! back by any other, and an encrypted deployment never accidentally writes
//! plaintext through a secondary entry point.
//!
//! # Storage layout
//!
//! * `{key}` — the payload, or a `CELERS_CHUNKED:` sentinel when the payload
//!   exceeded the chunking threshold.
//! * `{key}:chunks` — JSON [`ChunkMetadata`] describing the chunk layout.
//! * `{key}:chunk:{i}` — the individual chunk payloads.

use std::time::Duration;

use redis::aio::ConnectionManager;

use crate::chunking::{ChunkMetadata, ResultChunker};
use crate::types::{BackendError, Result, TaskMeta};
use crate::{compression, encryption};

/// Lua script performing a complete result write in one atomic server-side step.
///
/// Responsibilities (all of which used to be spread over several non-atomic
/// round trips, or skipped entirely on secondary write paths):
///
/// 1. optional compare-and-swap guard on the *raw stored bytes*;
/// 2. removal of chunk keys left over from a previous, differently sized
///    encoding of the same task (otherwise they leak forever);
/// 3. the write itself — payload/sentinel, chunk metadata and every chunk;
/// 4. TTL application via `SET ... EX`, so a crash can never leave a result
///    without its expiry.
///
/// `KEYS[1]` primary key, `KEYS[2]` chunk-metadata key.
/// `ARGV[1]` CAS guard (empty = unconditional), `ARGV[2]` TTL seconds
/// (`"0"` = no expiry), `ARGV[3]` primary value, `ARGV[4]` chunk metadata JSON
/// (empty = not chunked), `ARGV[5..]` chunk payloads.
///
/// Returns `1` when the write happened, `0` when the CAS guard rejected it.
pub(crate) const WRITE_RESULT_SCRIPT: &str = r#"
local k = KEYS[1]
local m = KEYS[2]
if ARGV[1] ~= '' then
  if redis.call('GET', k) ~= ARGV[1] then return 0 end
end
local old = redis.call('GET', m)
if old then
  local ok, decoded = pcall(cjson.decode, old)
  if ok and type(decoded) == 'table' and tonumber(decoded.total_chunks) then
    for i = 0, tonumber(decoded.total_chunks) - 1 do
      redis.call('DEL', k .. ':chunk:' .. i)
    end
  end
  redis.call('DEL', m)
end
local ttl = ARGV[2]
if ttl ~= '0' then
  redis.call('SET', k, ARGV[3], 'EX', ttl)
else
  redis.call('SET', k, ARGV[3])
end
if ARGV[4] ~= '' then
  if ttl ~= '0' then
    redis.call('SET', m, ARGV[4], 'EX', ttl)
  else
    redis.call('SET', m, ARGV[4])
  end
  for i = 5, #ARGV do
    local c = k .. ':chunk:' .. (i - 5)
    if ttl ~= '0' then
      redis.call('SET', c, ARGV[i], 'EX', ttl)
    else
      redis.call('SET', c, ARGV[i])
    end
  end
end
return 1
"#;

/// Lua script deleting a result together with every key that belongs to it.
///
/// `KEYS[1]` primary key, `KEYS[2]` chunk-metadata key. Returns the number of
/// primary keys removed (0 or 1) so callers can count real deletions.
pub(crate) const DELETE_RESULT_SCRIPT: &str = r#"
local k = KEYS[1]
local m = KEYS[2]
local old = redis.call('GET', m)
if old then
  local ok, decoded = pcall(cjson.decode, old)
  if ok and type(decoded) == 'table' and tonumber(decoded.total_chunks) then
    for i = 0, tonumber(decoded.total_chunks) - 1 do
      redis.call('DEL', k .. ':chunk:' .. i)
    end
  end
  redis.call('DEL', m)
end
return redis.call('DEL', k)
"#;

/// Lua script applying a TTL to a result *and* every key that belongs to it.
///
/// `KEYS[1]` primary key, `KEYS[2]` chunk-metadata key, `ARGV[1]` TTL seconds.
/// Returns 1 when the primary key existed.
pub(crate) const EXPIRE_RESULT_SCRIPT: &str = r#"
local k = KEYS[1]
local m = KEYS[2]
local t = ARGV[1]
local applied = redis.call('EXPIRE', k, t)
local old = redis.call('GET', m)
if old then
  local ok, decoded = pcall(cjson.decode, old)
  if ok and type(decoded) == 'table' and tonumber(decoded.total_chunks) then
    for i = 0, tonumber(decoded.total_chunks) - 1 do
      redis.call('EXPIRE', k .. ':chunk:' .. i, t)
    end
  end
  redis.call('EXPIRE', m, t)
end
return applied
"#;

/// A fully encoded `TaskMeta`, ready to be handed to [`write_command`].
#[derive(Debug, Clone)]
pub(crate) struct EncodedResult {
    /// Value for the primary key: the payload, or the chunk sentinel.
    pub(crate) main: Vec<u8>,
    /// Serialized [`ChunkMetadata`], present only for chunked payloads.
    pub(crate) chunk_meta: Option<Vec<u8>>,
    /// Individual chunk payloads (empty when not chunked).
    pub(crate) chunks: Vec<Vec<u8>>,
    /// Size of the serialized JSON before compression/encryption.
    pub(crate) original_size: usize,
    /// Size after compression/encryption, before chunk splitting.
    pub(crate) stored_size: usize,
}

impl EncodedResult {
    /// Whether this payload was split across chunk keys.
    pub(crate) fn is_chunked(&self) -> bool {
        self.chunk_meta.is_some()
    }
}

/// Serialize → compress → encrypt → (optionally) chunk a task result.
pub(crate) fn encode_meta(
    meta: &TaskMeta,
    compression_config: &compression::CompressionConfig,
    encryption_config: &encryption::EncryptionConfig,
    chunker: &ResultChunker,
) -> Result<EncodedResult> {
    let value =
        serde_json::to_string(meta).map_err(|e| BackendError::Serialization(e.to_string()))?;
    let original_size = value.len();

    let compressed = compression::maybe_compress(value.as_bytes(), compression_config)
        .map_err(|e| BackendError::Serialization(format!("Compression error: {}", e)))?;

    let data = encryption::encrypt(&compressed, encryption_config)
        .map_err(|e| BackendError::Serialization(format!("Encryption error: {}", e)))?;

    let stored_size = data.len();

    if chunker.needs_chunking(&data) {
        let (metadata, chunks) = chunker.split_chunks(&data);
        let sentinel = chunker.create_sentinel(&metadata);
        let chunk_meta = serde_json::to_vec(&metadata)
            .map_err(|e| BackendError::Serialization(format!("Chunk metadata error: {}", e)))?;
        Ok(EncodedResult {
            main: sentinel,
            chunk_meta: Some(chunk_meta),
            chunks,
            original_size,
            stored_size,
        })
    } else {
        Ok(EncodedResult {
            main: data,
            chunk_meta: None,
            chunks: Vec::new(),
            original_size,
            stored_size,
        })
    }
}

/// Build the `EVAL` command that writes `encoded` to `key`.
///
/// The returned [`redis::Cmd`] can be issued directly or appended to a pipeline
/// with [`redis::Pipeline::add_command`], which is what the batch write paths do.
///
/// `guard` turns the write into a compare-and-swap against the exact bytes
/// currently stored at `key`.
pub(crate) fn write_command(
    key: &str,
    encoded: &EncodedResult,
    ttl: Option<Duration>,
    guard: Option<&[u8]>,
) -> redis::Cmd {
    // A configured TTL of zero would be indistinguishable from "no TTL" in the
    // script, so clamp it to the smallest expiry Redis accepts.
    let ttl_secs = ttl.map(|t| t.as_secs().max(1)).unwrap_or(0);

    let mut cmd = redis::cmd("EVAL");
    cmd.arg(WRITE_RESULT_SCRIPT)
        .arg(2)
        .arg(key)
        .arg(ResultChunker::metadata_key(key))
        .arg(guard.unwrap_or(&[]))
        .arg(ttl_secs.to_string())
        .arg(encoded.main.as_slice())
        .arg(encoded.chunk_meta.as_deref().unwrap_or(&[]));

    for chunk in &encoded.chunks {
        cmd.arg(chunk.as_slice());
    }

    cmd
}

/// Build the `EVAL` command that deletes `key` and all of its chunk keys.
pub(crate) fn delete_command(key: &str) -> redis::Cmd {
    let mut cmd = redis::cmd("EVAL");
    cmd.arg(DELETE_RESULT_SCRIPT)
        .arg(2)
        .arg(key)
        .arg(ResultChunker::metadata_key(key));
    cmd
}

/// Build the `EVAL` command that applies `ttl` to `key` and all of its chunks.
pub(crate) fn expire_command(key: &str, ttl: Duration) -> redis::Cmd {
    let mut cmd = redis::cmd("EVAL");
    cmd.arg(EXPIRE_RESULT_SCRIPT)
        .arg(2)
        .arg(key)
        .arg(ResultChunker::metadata_key(key))
        .arg(ttl.as_secs().max(1));
    cmd
}

/// Decrypt → decompress → deserialize a fully assembled stored payload.
pub(crate) fn decode_payload(
    data: &[u8],
    encryption_config: &encryption::EncryptionConfig,
) -> Result<TaskMeta> {
    let decrypted = encryption::decrypt(data, encryption_config)
        .map_err(|e| BackendError::Serialization(format!("Decryption error: {}", e)))?;

    let decompressed = compression::maybe_decompress(&decrypted)
        .map_err(|e| BackendError::Serialization(format!("Decompression error: {}", e)))?;

    let value = String::from_utf8(decompressed)
        .map_err(|e| BackendError::Serialization(format!("UTF-8 error: {}", e)))?;

    serde_json::from_str(&value).map_err(|e| BackendError::Serialization(e.to_string()))
}

/// Inspect a raw stored value and, when it is a chunk sentinel, return the
/// chunk metadata together with the keys the chunks live under.
pub(crate) fn chunk_plan(key: &str, raw: &[u8]) -> Result<Option<(ChunkMetadata, Vec<String>)>> {
    if !ResultChunker::is_chunked(raw) {
        return Ok(None);
    }

    let metadata = ResultChunker::parse_sentinel(raw)
        .map_err(|e| BackendError::Serialization(format!("Chunk sentinel error: {}", e)))?;
    let keys = ResultChunker::chunk_keys(key, metadata.total_chunks);
    Ok(Some((metadata, keys)))
}

/// Decode a batch of raw stored values, transparently reassembling any chunked
/// entries.
///
/// Chunk fetching costs at most one extra pipelined round trip for the whole
/// batch, regardless of how many entries turned out to be chunked. `keys` and
/// `raws` must be index-aligned; the returned vector preserves that alignment,
/// including `None` for keys that do not exist.
pub(crate) async fn decode_many(
    conn: &mut ConnectionManager,
    keys: &[String],
    raws: Vec<Option<Vec<u8>>>,
    chunker: &ResultChunker,
    encryption_config: &encryption::EncryptionConfig,
) -> Result<Vec<Option<TaskMeta>>> {
    debug_assert_eq!(keys.len(), raws.len());

    // Pass 1: classify each entry and collect the chunk keys we still need.
    let mut plans: Vec<Option<(ChunkMetadata, std::ops::Range<usize>)>> =
        Vec::with_capacity(raws.len());
    let mut chunk_keys: Vec<String> = Vec::new();

    for (key, raw) in keys.iter().zip(raws.iter()) {
        match raw {
            Some(bytes) => match chunk_plan(key, bytes)? {
                Some((metadata, ck)) => {
                    let start = chunk_keys.len();
                    chunk_keys.extend(ck);
                    plans.push(Some((metadata, start..chunk_keys.len())));
                }
                None => plans.push(None),
            },
            None => plans.push(None),
        }
    }

    // Pass 2: one pipelined fetch for every chunk of every chunked entry.
    let chunk_values: Vec<Option<Vec<u8>>> = if chunk_keys.is_empty() {
        Vec::new()
    } else {
        let mut pipe = redis::pipe();
        for ck in &chunk_keys {
            pipe.get(ck);
        }
        pipe.query_async(conn).await?
    };

    // Pass 3: reassemble and decode.
    let mut results = Vec::with_capacity(raws.len());
    for (raw, plan) in raws.into_iter().zip(plans) {
        let Some(bytes) = raw else {
            results.push(None);
            continue;
        };

        let payload = match plan {
            Some((metadata, range)) => {
                let mut chunks = Vec::with_capacity(range.len());
                for (offset, value) in chunk_values[range.clone()].iter().enumerate() {
                    match value {
                        Some(chunk) => chunks.push(chunk.clone()),
                        None => {
                            return Err(BackendError::Serialization(format!(
                                "Missing chunk {} of {} for key {}",
                                offset,
                                metadata.total_chunks,
                                chunk_keys[range.start + offset]
                            )))
                        }
                    }
                }
                chunker.reassemble_chunks(&metadata, &chunks).map_err(|e| {
                    BackendError::Serialization(format!("Chunk reassembly error: {}", e))
                })?
            }
            None => bytes,
        };

        results.push(Some(decode_payload(&payload, encryption_config)?));
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::chunking::ChunkingConfig;
    use crate::encryption::{EncryptionConfig, EncryptionKey};
    use crate::types::TaskResult;
    use uuid::Uuid;

    fn sample_meta(payload_len: usize) -> TaskMeta {
        let task_id = Uuid::new_v4();
        let mut meta = TaskMeta::new(task_id, "codec.sample".to_string());
        meta.result = TaskResult::Success(serde_json::json!({ "data": "z".repeat(payload_len) }));
        meta
    }

    /// The encode side must round-trip through the decode side for every
    /// combination of compression, encryption and chunking.
    #[test]
    fn test_encode_decode_roundtrip_all_modes() {
        let keyed = EncryptionConfig::new(EncryptionKey::generate());
        let modes: Vec<(compression::CompressionConfig, EncryptionConfig)> = vec![
            (
                compression::CompressionConfig::disabled(),
                EncryptionConfig::disabled(),
            ),
            (
                compression::CompressionConfig::default(),
                EncryptionConfig::disabled(),
            ),
            (compression::CompressionConfig::disabled(), keyed.clone()),
            (compression::CompressionConfig::default(), keyed),
        ];

        for (comp, enc) in modes {
            // Chunking disabled -> single value.
            let chunker = ResultChunker::new(ChunkingConfig::disabled());
            let meta = sample_meta(4096);
            let encoded = encode_meta(&meta, &comp, &enc, &chunker).expect("encode");
            assert!(!encoded.is_chunked());
            let decoded = decode_payload(&encoded.main, &enc).expect("decode");
            assert_eq!(decoded.task_id, meta.task_id);
            assert_eq!(decoded.result, meta.result);

            // Chunking enabled with a tiny threshold -> sentinel + chunks.
            let chunker =
                ResultChunker::new(ChunkingConfig::new().with_threshold(64).with_chunk_size(32));
            let encoded = encode_meta(&meta, &comp, &enc, &chunker).expect("encode chunked");
            assert!(encoded.is_chunked(), "payload should have been chunked");
            assert!(ResultChunker::is_chunked(&encoded.main));

            let (metadata, chunk_keys) = chunk_plan("celery-task-meta-x", &encoded.main)
                .expect("chunk plan")
                .expect("sentinel detected");
            assert_eq!(chunk_keys.len(), encoded.chunks.len());
            let reassembled = chunker
                .reassemble_chunks(&metadata, &encoded.chunks)
                .expect("reassemble");
            let decoded = decode_payload(&reassembled, &enc).expect("decode chunked");
            assert_eq!(decoded.task_id, meta.task_id);
            assert_eq!(decoded.result, meta.result);
        }
    }

    #[test]
    fn test_chunk_plan_ignores_plain_values() {
        let plan = chunk_plan("celery-task-meta-x", b"{\"task_id\":\"...\"}").expect("plan");
        assert!(plan.is_none());
    }

    #[test]
    fn test_write_command_shapes() {
        let chunker = ResultChunker::new(ChunkingConfig::disabled());
        let meta = sample_meta(16);
        let encoded = encode_meta(
            &meta,
            &compression::CompressionConfig::disabled(),
            &EncryptionConfig::disabled(),
            &chunker,
        )
        .expect("encode");

        // EVAL, script, numkeys, 2 keys, guard, ttl, main value, chunk-meta.
        const FIXED_ARGS: usize = 9;

        let plain = write_command("k", &encoded, None, None);
        assert_eq!(plain.args_iter().count(), FIXED_ARGS);

        // A CAS write has the same arity but a non-empty guard argument.
        let guarded = write_command("k", &encoded, Some(Duration::from_secs(60)), Some(b"prev"));
        assert_eq!(guarded.args_iter().count(), FIXED_ARGS);

        // Chunked writes append one argument per chunk.
        let chunker =
            ResultChunker::new(ChunkingConfig::new().with_threshold(8).with_chunk_size(8));
        let chunked = encode_meta(
            &meta,
            &compression::CompressionConfig::disabled(),
            &EncryptionConfig::disabled(),
            &chunker,
        )
        .expect("encode chunked");
        assert!(chunked.is_chunked());
        let cmd = write_command("k", &chunked, None, None);
        assert_eq!(cmd.args_iter().count(), FIXED_ARGS + chunked.chunks.len());
    }
}
