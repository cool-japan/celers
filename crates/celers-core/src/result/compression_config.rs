//! The result-compression **wiring seam**: what a result backend needs in
//! order to store a compressed result and read it back.
//!
//! [`ResultCompressor`] and its codecs answer "how do I turn these bytes into
//! those bytes". They deliberately do not answer the questions a backend
//! actually has to make a decision about:
//!
//! * *Should* this payload be compressed at all — is it big enough to be worth
//!   a header, did the codec even make it smaller?
//! * What has to be written alongside it so a reader that was configured
//!   differently (or not at all) can still restore it?
//!
//! [`CompressionConfig`] is the configuration surface for the first question,
//! and [`ResultCompressionPolicy`] is the helper that answers both: it
//! compresses when the policy says so and **marks
//! [`ResultMetadata`] with what it actually did**, so
//! [`decompress`](ResultCompressionPolicy::decompress) can be driven by the
//! stored metadata rather than by the reader's own configuration.
//!
//! ```
//! use celers_core::{CompressionConfig, ResultCompressionPolicy, ResultMetadata};
//!
//! # fn main() -> celers_core::Result<()> {
//! // A backend that compresses anything at or over 1 KiB.
//! # #[cfg(feature = "compression-zstd")]
//! # {
//! let policy = ResultCompressionPolicy::new(CompressionConfig::new("zstd", 1024))?;
//!
//! let payload = vec![b'x'; 8192];
//! let mut metadata = ResultMetadata::new();
//! let stored = policy.compress(&payload, &mut metadata)?;
//!
//! // The metadata now describes the stored bytes...
//! assert!(metadata.compressed);
//! assert_eq!(metadata.compression_algorithm.as_deref(), Some("zstd"));
//!
//! // ...which is all a reader needs, even one with compression switched off.
//! let reader = ResultCompressionPolicy::disabled();
//! assert_eq!(reader.decompress(&stored, &metadata)?, payload);
//! # }
//! # Ok(())
//! # }
//! ```
//!
//! # The two "not compressed after all" paths
//!
//! A payload below [`CompressionConfig::min_size_bytes`], and a payload the
//! codec made *larger* (random or already-compressed data), are both stored
//! verbatim with the metadata explicitly cleared. Marking either as compressed
//! would make the result unreadable: the reader would hand plaintext to a codec
//! that has never seen it.
//!
//! # What is deliberately not here
//!
//! Nothing in this module performs I/O or knows what a result *is*. It is the
//! seam a backend crate calls on the way to its store and on the way back, so
//! that every backend marks compression the same way and a result written by
//! one is readable by another.

use super::compression::ResultCompressor;
use super::ResultMetadata;
use crate::{CelersError, Result};
use serde::{Deserialize, Serialize};

/// The algorithm name that means "stored verbatim".
///
/// The same string [`IdentityCodec`](super::IdentityCodec) answers to, and what
/// `celers_protocol` writes as the `identity` content encoding.
pub const NO_COMPRESSION: &str = "none";

/// Default [`CompressionConfig::min_size_bytes`]: 1 KiB.
///
/// Below roughly this size the framing every real codec adds (a gzip header and
/// CRC-32 trailer is 18 bytes, a zstd frame header at least 6) eats the saving,
/// and the CPU is spent for nothing.
pub const DEFAULT_COMPRESSION_MIN_SIZE: usize = 1024;

/// When a result backend should compress a result, and with what.
///
/// Deserializable, so it can come straight from an application's configuration
/// file. The default is [`CompressionConfig::disabled`] — a backend must opt
/// in, because compression changes the bytes on the wire for every reader of
/// that result store.
///
/// The `algorithm` is a name from the table in
/// [`result::compression`](super::compression), *not* an enum: the set of
/// codecs depends on this build's Cargo features and can be extended with
/// [`ResultCompressor::with_codec`], and a name is what travels in
/// [`ResultMetadata::compression_algorithm`] anyway.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct CompressionConfig {
    /// Algorithm name, e.g. `"none"`, `"gzip"`, `"zlib"`, `"zstd"`.
    pub algorithm: String,

    /// Payloads smaller than this are stored verbatim.
    pub min_size_bytes: usize,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self::disabled()
    }
}

impl CompressionConfig {
    /// Compress with `algorithm` anything at or over `min_size_bytes`.
    ///
    /// The algorithm is not checked here — a `CompressionConfig` is plain
    /// configuration data, and a name this build has no codec for must be
    /// reportable as such rather than unrepresentable.
    /// [`ResultCompressionPolicy::new`] is where it is validated.
    #[must_use]
    pub fn new(algorithm: impl Into<String>, min_size_bytes: usize) -> Self {
        Self {
            algorithm: algorithm.into(),
            min_size_bytes,
        }
    }

    /// Store every result verbatim.
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            algorithm: NO_COMPRESSION.to_string(),
            min_size_bytes: DEFAULT_COMPRESSION_MIN_SIZE,
        }
    }

    /// Compress with `algorithm` from [`DEFAULT_COMPRESSION_MIN_SIZE`] upwards.
    #[must_use]
    pub fn with_algorithm(algorithm: impl Into<String>) -> Self {
        Self::new(algorithm, DEFAULT_COMPRESSION_MIN_SIZE)
    }

    /// Set the size at or above which a payload is worth compressing.
    #[must_use]
    pub fn min_size_bytes(mut self, min_size_bytes: usize) -> Self {
        self.min_size_bytes = min_size_bytes;
        self
    }

    /// Whether this configuration asks for any compression at all.
    ///
    /// `"none"` and the empty string both mean "no": the empty string is what a
    /// half-filled configuration file produces, and treating it as an algorithm
    /// name would turn a typo into a lookup failure at store time.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        !(self.algorithm.is_empty() || self.algorithm == NO_COMPRESSION)
    }

    /// Whether a payload of `len` bytes is worth compressing under this policy.
    #[must_use]
    pub fn should_compress_len(&self, len: usize) -> bool {
        self.is_enabled() && len >= self.min_size_bytes
    }
}

/// A [`CompressionConfig`] bound to the codecs that can carry it out.
///
/// This is what a result backend holds: one call on the way into the store, one
/// on the way out, and the metadata in between carries everything a reader
/// needs.
///
/// # Reading is never disabled
///
/// Even [`ResultCompressionPolicy::disabled`] keeps the full codec registry, so
/// a backend with compression switched off still reads results some other
/// writer compressed. Only the *writing* decision is configurable.
#[derive(Debug, Clone)]
pub struct ResultCompressionPolicy {
    config: CompressionConfig,
    compressor: ResultCompressor,
}

impl Default for ResultCompressionPolicy {
    fn default() -> Self {
        Self::disabled()
    }
}

impl ResultCompressionPolicy {
    /// Bind `config` to this build's built-in codecs.
    ///
    /// # Errors
    ///
    /// Returns [`CelersError::Configuration`] if no codec is registered under
    /// `config.algorithm` — a build without the `compression-zstd` feature
    /// configured for `"zstd"`, or a misspelled name. Failing here is the whole
    /// point: the alternative is discovering it on the first large result, or
    /// worse, silently storing plaintext under a header that claims otherwise.
    pub fn new(config: CompressionConfig) -> Result<Self> {
        let compressor = ResultCompressor::new(config.min_size_bytes);
        Self::with_compressor(config, compressor)
    }

    /// Bind `config` to a caller-assembled codec registry.
    ///
    /// For a backend that registers a codec of its own (a dictionary-trained
    /// zstd, an encrypt-then-compress wrapper) or that refuses to inherit
    /// whatever this build compiled in, via
    /// [`ResultCompressor::identity_only`].
    ///
    /// `config.min_size_bytes` is authoritative for the compress-or-not
    /// decision; the compressor's own
    /// [`threshold_bytes`](ResultCompressor::threshold_bytes) is not consulted.
    ///
    /// # Errors
    ///
    /// Returns [`CelersError::Configuration`] if `compressor` has no codec
    /// registered under `config.algorithm`.
    pub fn with_compressor(
        config: CompressionConfig,
        compressor: ResultCompressor,
    ) -> Result<Self> {
        if config.is_enabled() && !compressor.supports(&config.algorithm) {
            return Err(CelersError::Configuration(format!(
                "result compression is configured for '{}', which is not registered; available: {}",
                config.algorithm,
                compressor.algorithms().join(", ")
            )));
        }
        Ok(Self { config, compressor })
    }

    /// A policy that writes every result verbatim but can still read compressed
    /// ones.
    #[must_use]
    pub fn disabled() -> Self {
        let config = CompressionConfig::disabled();
        let compressor = ResultCompressor::new(config.min_size_bytes);
        Self { config, compressor }
    }

    /// The configuration this policy was built from.
    #[must_use]
    pub const fn config(&self) -> &CompressionConfig {
        &self.config
    }

    /// The codec registry compression and decompression go through.
    #[must_use]
    pub const fn compressor(&self) -> &ResultCompressor {
        &self.compressor
    }

    /// Whether `payload` is worth compressing under this policy.
    ///
    /// Says nothing about whether compression will actually *pay* — that is
    /// only known after the codec has run, which is why
    /// [`compress`](Self::compress) can still store a payload verbatim after
    /// this returns `true`.
    #[must_use]
    pub fn should_compress(&self, payload: &[u8]) -> bool {
        self.config.should_compress_len(payload.len())
    }

    /// Compress `payload` if the policy says so, recording what happened in
    /// `metadata`.
    ///
    /// Returns the bytes to store. `metadata` is marked with
    /// [`ResultMetadata::mark_compression`] when — and only when — the returned
    /// bytes really are compressed; on every other path it is cleared with
    /// [`ResultMetadata::clear_compression`], so a metadata value reused across
    /// results cannot leave a stale marking behind.
    ///
    /// A payload the codec did not actually shrink is stored verbatim. Storing
    /// the "compressed" form would cost bytes *and* a decompression on every
    /// read.
    ///
    /// # Errors
    ///
    /// Returns the codec's error if compression fails, or
    /// [`CelersError::Configuration`] if the configured algorithm is not
    /// registered (only reachable when the registry changed after
    /// construction).
    pub fn compress(&self, payload: &[u8], metadata: &mut ResultMetadata) -> Result<Vec<u8>> {
        if !self.should_compress(payload) {
            metadata.clear_compression();
            return Ok(payload.to_vec());
        }

        let squeezed = self.compressor.compress(payload, &self.config.algorithm)?;
        if squeezed.len() >= payload.len() {
            // Incompressible input: the framing cost more than it saved.
            metadata.clear_compression();
            return Ok(payload.to_vec());
        }

        metadata.mark_compression(&self.config.algorithm, payload.len(), squeezed.len());
        Ok(squeezed)
    }

    /// Restore a payload using the algorithm recorded in `metadata`.
    ///
    /// The *stored* metadata decides, never this policy's configuration: that
    /// is what lets a reader with compression disabled — or configured for a
    /// different algorithm — read what a writer stored.
    /// [`ResultMetadata::compressed`] is authoritative; a payload that is not
    /// marked compressed is returned verbatim.
    ///
    /// # Errors
    ///
    /// * [`CelersError::Configuration`] if the metadata claims compression but
    ///   records no algorithm, or names one this build has no codec for. A
    ///   result that cannot be read must say so rather than hand back the
    ///   compressed bytes as if they were the value.
    /// * [`CelersError::Deserialization`] if the codec rejects the bytes, or if
    ///   the restored payload is not the length the metadata recorded — a
    ///   truncated result that happened to decompress must not pass for a whole
    ///   one.
    pub fn decompress(&self, stored: &[u8], metadata: &ResultMetadata) -> Result<Vec<u8>> {
        if !metadata.compressed {
            return Ok(stored.to_vec());
        }

        let Some(algorithm) = metadata.compression_algorithm.as_deref() else {
            return Err(CelersError::Configuration(
                "result metadata is marked compressed but records no algorithm".to_string(),
            ));
        };

        let restored = self.compressor.decompress(stored, algorithm)?;
        if let Some(expected) = metadata.original_size {
            if restored.len() != expected {
                return Err(CelersError::Deserialization(format!(
                    "decompressed result is {} bytes, but its metadata records {expected}",
                    restored.len()
                )));
            }
        }
        Ok(restored)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Repetitive but structured, so every real codec beats it comfortably.
    fn corpus() -> Vec<u8> {
        let mut data = Vec::with_capacity(8192);
        for i in 0..512u32 {
            data.extend_from_slice(format!("{{\"task\":\"tasks.add\",\"seq\":{i}}}\n").as_bytes());
        }
        data
    }

    /// The algorithm every build has, so the seam's own behaviour can be tested
    /// without depending on which codec features are on.
    fn any_real_algorithm() -> Option<String> {
        super::super::builtin_codecs()
            .into_iter()
            .find(|codec| codec.name() != NO_COMPRESSION)
            .map(|codec| codec.name().to_string())
    }

    #[test]
    fn default_config_is_disabled_so_nothing_changes_by_accident() {
        let config = CompressionConfig::default();
        assert_eq!(config.algorithm, NO_COMPRESSION);
        assert_eq!(config.min_size_bytes, DEFAULT_COMPRESSION_MIN_SIZE);
        assert!(!config.is_enabled());
        assert!(!config.should_compress_len(usize::MAX));

        // An empty algorithm — a half-filled config file — is "no", not a
        // lookup failure at store time.
        assert!(!CompressionConfig::new("", 0).is_enabled());
    }

    #[test]
    fn config_round_trips_through_serde() {
        let config = CompressionConfig::new("gzip", 4096);
        let json = serde_json::to_string(&config).expect("serialize");
        let back: CompressionConfig = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(back, config);

        // Missing fields fall back to the disabled default.
        let partial: CompressionConfig = serde_json::from_str("{}").expect("deserialize");
        assert_eq!(partial, CompressionConfig::disabled());
    }

    #[test]
    fn an_unregistered_algorithm_is_refused_at_construction() {
        let err = ResultCompressionPolicy::new(CompressionConfig::with_algorithm("brotli"))
            .expect_err("an absent codec must be refused up front");
        assert!(
            err.to_string().contains("brotli"),
            "the error must name the algorithm: {err}"
        );
        // ...and list what this build does have, so it is diagnosable.
        assert!(
            err.to_string().contains(NO_COMPRESSION),
            "unexpected: {err}"
        );

        // The identity algorithm is always available, in every build.
        assert!(ResultCompressionPolicy::new(CompressionConfig::disabled()).is_ok());
    }

    #[test]
    fn a_payload_below_the_threshold_is_stored_verbatim_and_unmarked() {
        let Some(algorithm) = any_real_algorithm() else {
            return; // --no-default-features: nothing to compress with
        };
        let policy = ResultCompressionPolicy::new(CompressionConfig::new(algorithm, 1024))
            .expect("built-in codec");

        let payload = vec![b'a'; 1023];
        let mut metadata = ResultMetadata::new();
        let stored = policy.compress(&payload, &mut metadata).expect("compress");

        assert_eq!(stored, payload, "a small payload must be stored as-is");
        assert!(!metadata.compressed);
        assert!(metadata.compression_algorithm.is_none());
        assert!(metadata.original_size.is_none());
        assert!(metadata.compressed_size.is_none());

        // ...and reads back through the same seam.
        assert_eq!(
            policy.decompress(&stored, &metadata).expect("decompress"),
            payload
        );
    }

    #[test]
    fn a_payload_the_codec_grew_is_stored_verbatim_and_unmarked() {
        // The trap this guards: `with_compression`/`mark_compression` set
        // `compressed = true` unconditionally, so marking on a path that stored
        // plaintext hands a reader bytes no codec can decode.
        //
        // The growth is arranged rather than hoped for: whether a *real* codec
        // grows a given input depends on the codec, the level and the entropy
        // of the bytes, none of which this branch should be hostage to.
        #[derive(Debug)]
        struct GrowingCodec;

        impl super::super::CompressionCodec for GrowingCodec {
            fn name(&self) -> &str {
                "grow"
            }
            fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
                let mut out = b"pad!".to_vec();
                out.extend_from_slice(data);
                Ok(out)
            }
            fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
                Ok(data.get(4..).unwrap_or_default().to_vec())
            }
        }

        let compressor =
            ResultCompressor::new(64).with_codec(std::sync::Arc::new(GrowingCodec) as _);
        let policy = ResultCompressionPolicy::with_compressor(
            CompressionConfig::new("grow", 16),
            compressor,
        )
        .expect("registered codec");

        let payload = corpus();
        let mut metadata = ResultMetadata::new();
        assert!(policy.should_compress(&payload));
        let stored = policy.compress(&payload, &mut metadata).expect("compress");

        assert_eq!(stored, payload, "the verbatim payload must be stored");
        assert!(
            !metadata.compressed,
            "a payload the codec grew must not be marked compressed"
        );
        assert!(metadata.compression_algorithm.is_none());
        assert!(metadata.compressed_size.is_none());
        assert_eq!(
            policy.decompress(&stored, &metadata).expect("decompress"),
            payload
        );
    }

    #[test]
    fn a_compressed_payload_is_marked_with_what_actually_happened() {
        let Some(algorithm) = any_real_algorithm() else {
            return;
        };
        let policy =
            ResultCompressionPolicy::new(CompressionConfig::new(&algorithm, 1024)).expect("codec");

        let payload = corpus();
        let mut metadata = ResultMetadata::new();
        let stored = policy.compress(&payload, &mut metadata).expect("compress");

        assert!(stored.len() < payload.len());
        assert!(metadata.compressed);
        assert_eq!(metadata.compression_algorithm.as_deref(), Some(&*algorithm));
        assert_eq!(metadata.original_size, Some(payload.len()));
        assert_eq!(metadata.compressed_size, Some(stored.len()));
        let ratio = metadata.compression_ratio().expect("both sizes recorded");
        assert!(
            ratio < 1.0,
            "the recorded ratio must be a real saving: {ratio}"
        );

        assert_eq!(
            policy.decompress(&stored, &metadata).expect("decompress"),
            payload
        );
    }

    #[test]
    fn the_stored_metadata_drives_reading_not_the_readers_config() {
        // The whole reason the algorithm travels with the result: a reader that
        // was configured differently — or not at all — still restores it.
        let Some(algorithm) = any_real_algorithm() else {
            return;
        };
        let writer =
            ResultCompressionPolicy::new(CompressionConfig::new(algorithm, 64)).expect("codec");
        let payload = corpus();
        let mut metadata = ResultMetadata::new();
        let stored = writer.compress(&payload, &mut metadata).expect("compress");
        assert!(metadata.compressed);

        let reader = ResultCompressionPolicy::disabled();
        assert!(!reader.config().is_enabled());
        assert_eq!(
            reader.decompress(&stored, &metadata).expect("decompress"),
            payload
        );
    }

    #[test]
    fn a_stale_marking_is_cleared_rather_than_carried_over() {
        let Some(algorithm) = any_real_algorithm() else {
            return;
        };
        let policy =
            ResultCompressionPolicy::new(CompressionConfig::new(algorithm, 1024)).expect("codec");

        // Metadata reused from a previous, compressed result.
        let mut metadata = ResultMetadata::new().with_compression("zstd", 4096, 100);
        assert!(metadata.compressed);

        let payload = b"small".to_vec();
        let stored = policy.compress(&payload, &mut metadata).expect("compress");
        assert_eq!(stored, payload);
        assert!(
            !metadata.compressed,
            "the previous result's marking must not survive"
        );
        assert!(metadata.compression_algorithm.is_none());
    }

    #[test]
    fn unreadable_metadata_is_an_error_not_the_raw_bytes() {
        let policy = ResultCompressionPolicy::disabled();

        // Marked compressed, no algorithm recorded.
        let mut metadata = ResultMetadata::new();
        metadata.compressed = true;
        let err = policy
            .decompress(b"whatever", &metadata)
            .expect_err("a result that cannot be read must say so");
        assert!(
            err.to_string().contains("no algorithm"),
            "unexpected: {err}"
        );

        // Marked with an algorithm this build has no codec for.
        let mut metadata = ResultMetadata::new();
        metadata.mark_compression("brotli", 10, 5);
        let err = policy
            .decompress(b"whatever", &metadata)
            .expect_err("an unknown algorithm must not fall back to plaintext");
        assert!(
            err.to_string()
                .contains("unsupported compression algorithm"),
            "unexpected: {err}"
        );
    }

    #[test]
    fn a_truncated_result_that_still_decodes_is_rejected() {
        let Some(algorithm) = any_real_algorithm() else {
            return;
        };
        let policy =
            ResultCompressionPolicy::new(CompressionConfig::new(algorithm, 64)).expect("codec");

        let payload = corpus();
        let mut metadata = ResultMetadata::new();
        let stored = policy.compress(&payload, &mut metadata).expect("compress");

        // The recorded original size is a second, independent check on the
        // restored bytes.
        metadata.original_size = Some(payload.len() + 1);
        let err = policy
            .decompress(&stored, &metadata)
            .expect_err("a length mismatch must not pass for a whole result");
        assert!(err.to_string().contains("bytes"), "unexpected: {err}");
    }

    #[test]
    fn a_custom_registry_is_honoured_and_a_bare_one_refuses() {
        // A caller that refuses to inherit this build's codecs gets exactly
        // what it registered — and a configuration naming anything else fails
        // at construction rather than at store time.
        let bare = ResultCompressor::identity_only(64);
        let err = ResultCompressionPolicy::with_compressor(
            CompressionConfig::new("zstd", 64),
            bare.clone(),
        )
        .expect_err("an identity-only registry supports nothing else");
        assert!(err.to_string().contains("zstd"), "unexpected: {err}");

        assert!(
            ResultCompressionPolicy::with_compressor(CompressionConfig::disabled(), bare).is_ok()
        );
    }

    #[test]
    fn the_configs_threshold_wins_over_the_compressors_own() {
        // `ResultCompressor` carries a threshold of its own; the policy's
        // config is the one that decides, so the two cannot disagree silently.
        let Some(algorithm) = any_real_algorithm() else {
            return;
        };
        let compressor = ResultCompressor::new(1_000_000);
        let policy = ResultCompressionPolicy::with_compressor(
            CompressionConfig::new(algorithm, 64),
            compressor,
        )
        .expect("codec");

        let payload = corpus();
        assert!(policy.should_compress(&payload));
        let mut metadata = ResultMetadata::new();
        let stored = policy.compress(&payload, &mut metadata).expect("compress");
        assert!(metadata.compressed, "the config's threshold must decide");
        assert!(stored.len() < payload.len());
    }
}
