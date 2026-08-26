//! Optional compression for the `result_data` JSON/JSONB column.
//!
//! `result_data` is a native JSON column -- `JSONB` on Postgres, `JSON` on
//! MySQL (see the `migrations/` schema) -- and both server types reject
//! anything stored there that is not valid JSON. Compressing it therefore
//! cannot mean writing raw compressed bytes into the column: it means
//! replacing a large-enough value with a small JSON *envelope* (see
//! `maybe_compress`/`maybe_decompress`) carrying the compressed bytes
//! (base64-encoded, since JSON strings are text) and the algorithm that
//! produced them.
//!
//! A row written with compression disabled, below the configured
//! threshold, or by a version of this backend that predates this module
//! entirely is exactly the caller's original JSON value with **no**
//! envelope wrapping it — so every existing row keeps reading back
//! unchanged, with no schema migration and no version flag anywhere else.
//! This mirrors `celers-backend-redis`'s own marker-prefix precedent for
//! the identical "old, uncompressed records must still read" requirement;
//! it is not literally the `ResultMetadata`-based "metadata marking" that
//! name suggests, because a pre-existing row has no metadata at all to mark
//! — a self-describing payload is the only thing that actually satisfies
//! "uncompressed old results still read" without a migration.
//!
//! Compression itself is delegated to `celers_core::ResultCompressor`, the
//! shared codec seam every result-storing backend in this workspace can
//! register against (see `celers_core::result::compression`'s module docs
//! for the algorithm table).

use std::sync::Arc;

use base64::engine::general_purpose::STANDARD as BASE64;
use base64::Engine;
use celers_core::ResultCompressor;
use serde_json::{json, Value};

use crate::{BackendError, Result};

/// Object key marking a compressed envelope. Paired with [`DATA_KEY`] and
/// chosen to be implausible as a real task-result field name;
/// `maybe_decompress` additionally requires the object to have *exactly*
/// these two keys before treating it as an envelope at all, which is the
/// practical mitigation for the (unavoidable, for any marker embedded in an
/// otherwise schema-free JSON value) risk of a genuine result coincidentally
/// taking this exact shape.
const MARKER_KEY: &str = "__celers_compressed";
/// Object key holding the base64-encoded compressed bytes. See [`MARKER_KEY`].
const DATA_KEY: &str = "__celers_data";

/// Compression settings for a DB result backend's `result_data` column.
///
/// Disabled by default (see [`CompressionConfig::disabled`], what
/// `PostgresResultBackend::new`/`MysqlResultBackend::new` install): unlike
/// the Redis backend, a database deployment may have other tools querying
/// `celers_task_results` directly (dashboards, ad-hoc SQL, a BI export), so
/// turning every large result into an opaque envelope is an opt-in
/// behavior change here, not a default one.
#[derive(Debug, Clone)]
pub struct CompressionConfig {
    enabled: bool,
    compressor: Arc<ResultCompressor>,
    algorithm: String,
}

impl CompressionConfig {
    /// Compression off. `maybe_compress` always returns its input
    /// unchanged; `maybe_decompress` still works normally, because
    /// decoding a `celers_core` codec needs no configuration beyond the
    /// codec being registered — which every [`ResultCompressor`] this
    /// module builds always has, regardless of `enabled`. That is what
    /// lets a row another backend instance (or an earlier, compression-on
    /// configuration of this same one) wrote *with* compression keep
    /// reading correctly here even after compression is turned off.
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            compressor: Arc::new(ResultCompressor::default()),
            algorithm: "none".to_string(),
        }
    }

    /// Compress `result_data` at or above `threshold_bytes` (measured on
    /// its encoded JSON form), using `algorithm` — e.g. `"zstd"`, `"gzip"`,
    /// `"zlib"`, whichever this build's `celers-core` has the codec
    /// compiled in for (see `celers_core::result::compression`'s table).
    /// `"zstd"` is the usual choice: better ratio than DEFLATE and several
    /// times faster to decompress.
    #[must_use]
    pub fn new(threshold_bytes: usize, algorithm: impl Into<String>) -> Self {
        Self {
            enabled: true,
            compressor: Arc::new(ResultCompressor::new(threshold_bytes)),
            algorithm: algorithm.into(),
        }
    }

    /// Whether this config compresses on write. (Decompression on read is
    /// unconditional — see [`Self::disabled`]'s doc comment.)
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// The algorithm new writes compress with, when enabled.
    #[must_use]
    pub fn algorithm(&self) -> &str {
        &self.algorithm
    }
}

/// Compress `value` into the envelope shape when `config` is enabled and
/// the encoded payload is at or above its threshold; otherwise (disabled,
/// below threshold, or the codec could not actually shrink it) `value` is
/// returned unchanged — so a small or already-compact `result_data` never
/// grows for nothing, and old rows never need a "compression: off" tag to
/// read correctly, because a plain value literally has no envelope.
///
/// # Errors
///
/// Returns [`BackendError::Serialization`] if `value` cannot be
/// JSON-encoded (a caller error — every task result is expected to be
/// JSON-representable already) or the configured codec fails to compress.
pub(crate) fn maybe_compress(value: &Value, config: &CompressionConfig) -> Result<Value> {
    if !config.enabled {
        return Ok(value.clone());
    }

    let serialized = serde_json::to_vec(value)
        .map_err(|e| BackendError::Serialization(format!("failed to encode result_data: {e}")))?;
    if !config.compressor.should_compress(&serialized) {
        return Ok(value.clone());
    }

    let compressed = config
        .compressor
        .compress(&serialized, &config.algorithm)
        .map_err(|e| BackendError::Serialization(format!("failed to compress result_data: {e}")))?;

    // A codec that did not actually shrink the payload (already-compressed
    // data, a tiny value just over the threshold) is not worth the
    // envelope's own overhead.
    if compressed.len() >= serialized.len() {
        return Ok(value.clone());
    }

    Ok(json!({
        MARKER_KEY: config.algorithm,
        DATA_KEY: BASE64.encode(&compressed),
    }))
}

/// Reverse of [`maybe_compress`]. Always attempted, independent of
/// `config`'s own `enabled` flag — see [`CompressionConfig::disabled`]'s
/// doc comment for why that is exactly what makes a compressed row written
/// elsewhere (or earlier, under a different configuration) still readable.
///
/// A value that is not a JSON object with *exactly* the two keys
/// [`MARKER_KEY`]/[`DATA_KEY`] is passed through completely unchanged —
/// this is what makes every pre-existing, never-compressed row (and every
/// row written with compression disabled or below threshold) read back
/// exactly as it always did, with no migration and no version flag.
///
/// # Errors
///
/// Returns [`BackendError::Serialization`] when the value *does* match the
/// envelope shape but its `data` is not valid base64, its algorithm is not
/// one this build registers, or the decompressed bytes are not valid JSON.
/// A corrupt or truncated envelope must be a loud error, never silently
/// returned as if it were a literal stored value.
pub(crate) fn maybe_decompress(value: Value, config: &CompressionConfig) -> Result<Value> {
    let Value::Object(map) = &value else {
        return Ok(value);
    };
    if map.len() != 2 {
        return Ok(value);
    }
    let (Some(Value::String(algorithm)), Some(Value::String(data))) =
        (map.get(MARKER_KEY), map.get(DATA_KEY))
    else {
        return Ok(value);
    };

    let compressed = BASE64.decode(data.as_bytes()).map_err(|e| {
        BackendError::Serialization(format!("corrupt compressed result_data (bad base64): {e}"))
    })?;
    let raw = config
        .compressor
        .decompress(&compressed, algorithm)
        .map_err(|e| {
            BackendError::Serialization(format!("failed to decompress result_data: {e}"))
        })?;
    serde_json::from_slice(&raw)
        .map_err(|e| BackendError::Serialization(format!("corrupt decompressed result_data: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A payload with real structure -- repetitive enough that a real codec
    /// beats it, not a single repeated byte (which flatters RLE-ish paths).
    fn corpus() -> Value {
        let items: Vec<Value> = (0..256)
            .map(|i| json!({"task": "tasks.add", "seq": i, "note": "same shape every time"}))
            .collect();
        json!({ "items": items })
    }

    #[test]
    fn disabled_never_wraps_and_never_shrinks() {
        let config = CompressionConfig::disabled();
        assert!(!config.is_enabled());

        let value = corpus();
        let compressed = maybe_compress(&value, &config).expect("compress");
        assert_eq!(
            compressed, value,
            "disabled must return the value unchanged"
        );
    }

    #[test]
    fn below_threshold_is_stored_plain() {
        let config = CompressionConfig::new(1024 * 1024, "zstd");
        let value = json!({"tiny": true});
        let compressed = maybe_compress(&value, &config).expect("compress");
        assert_eq!(compressed, value, "below the threshold must not be wrapped");
    }

    #[test]
    fn round_trips_through_the_envelope_when_enabled() {
        let config = CompressionConfig::new(16, "zstd");
        let value = corpus();

        let compressed = maybe_compress(&value, &config).expect("compress");
        assert_ne!(
            compressed, value,
            "an eligible, compressible payload must be wrapped in the envelope"
        );
        assert!(
            compressed.is_object() && compressed.as_object().unwrap().len() == 2,
            "the envelope must be a two-key object: {compressed}"
        );
        assert_eq!(
            compressed
                .get("__celers_compressed")
                .and_then(Value::as_str),
            Some("zstd")
        );

        let decompressed = maybe_decompress(compressed, &config).expect("decompress");
        assert_eq!(
            decompressed, value,
            "must round-trip byte-for-byte (as JSON)"
        );
    }

    /// The exact backward-compatibility property this module exists for: a
    /// plain value with no envelope -- what every row written before this
    /// feature existed looks like -- must decode as itself, unchanged.
    #[test]
    fn a_plain_value_with_no_envelope_passes_through_decompress_unchanged() {
        let config = CompressionConfig::new(16, "zstd");

        for value in [
            json!({"ordinary": "result", "n": 42}),
            json!([1, 2, 3]),
            json!(null),
            json!("a bare string"),
            json!(7),
            // Two keys, but not the envelope's -- must not be misread as one.
            json!({"a": 1, "b": 2}),
        ] {
            let decompressed = maybe_decompress(value.clone(), &config).expect("decompress");
            assert_eq!(
                decompressed, value,
                "a non-envelope value must pass through as-is"
            );
        }
    }

    #[test]
    fn a_value_that_does_not_compress_smaller_is_not_wrapped() {
        // Threshold of 0 means "always eligible", but if the codec cannot
        // actually shrink the (tiny, already near-minimal) payload, the
        // envelope's own overhead would make it larger for nothing.
        let config = CompressionConfig::new(0, "zstd");
        let value = json!(1);
        let compressed = maybe_compress(&value, &config).expect("compress");
        assert_eq!(
            compressed, value,
            "a payload the codec cannot shrink must be stored plain, not wrapped anyway"
        );
    }

    #[test]
    fn decompression_works_even_when_this_backend_instance_has_compression_disabled() {
        // Simulates a row written by a writer with compression ON being
        // read by a reader configured with it OFF: decompression is
        // unconditional (see `CompressionConfig::disabled`'s doc comment).
        let writer = CompressionConfig::new(16, "zstd");
        let reader = CompressionConfig::disabled();

        let value = corpus();
        let compressed = maybe_compress(&value, &writer).expect("compress");
        assert_ne!(compressed, value);

        let decompressed =
            maybe_decompress(compressed, &reader).expect("decompress with a disabled config");
        assert_eq!(decompressed, value);
    }

    #[test]
    fn a_truncated_envelope_is_a_loud_error_not_silent_corruption() {
        let config = CompressionConfig::new(16, "zstd");
        let compressed = maybe_compress(&corpus(), &config).expect("compress");

        let mut truncated = compressed.as_object().unwrap().clone();
        let data = truncated.get("__celers_data").unwrap().as_str().unwrap();
        truncated.insert("__celers_data".to_string(), json!(&data[..data.len() / 2]));

        let err = maybe_decompress(Value::Object(truncated), &config)
            .expect_err("a truncated compressed payload must not decode as if it were valid");
        assert!(matches!(err, BackendError::Serialization(_)));
    }

    #[test]
    fn an_unregistered_algorithm_is_a_loud_error() {
        let config = CompressionConfig::new(16, "zstd");
        let envelope = json!({
            "__celers_compressed": "brotli-9000",
            "__celers_data": BASE64.encode(b"whatever"),
        });

        let err = maybe_decompress(envelope, &config)
            .expect_err("an algorithm this build never registered must be a named error");
        assert!(matches!(err, BackendError::Serialization(_)));
    }
}
