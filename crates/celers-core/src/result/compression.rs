//! Result compression: the [`CompressionCodec`] seam and the codecs CeleRS
//! ships.
//!
//! A large task result is expensive to move and expensive to store, so
//! [`ResultCompressor`] lets a backend squeeze one before it writes it and
//! expand it on the way back. The algorithm travels with the result in
//! [`ResultMetadata::compression_algorithm`](super::ResultMetadata), so the
//! reader does not have to be configured the same way the writer was — it
//! looks the codec up by name.
//!
//! # Algorithm names
//!
//! The names are the ones `celers_protocol::compression::CompressionType`
//! puts in a message's `content-encoding` header, so a result compressed here
//! and a message body compressed there agree on what a name means:
//!
//! | name       | codec                  | Cargo feature          |
//! |------------|------------------------|------------------------|
//! | `"none"`   | [`IdentityCodec`]      | always available       |
//! | `"gzip"`   | [`GzipCodec`]          | `compression-deflate`  |
//! | `"zlib"`   | [`ZlibCodec`]          | `compression-deflate`  |
//! | `"zstd"`   | [`ZstdCodec`]          | `compression-zstd`     |
//!
//! `"none"` is CeleRS' spelling of "stored verbatim"; the protocol crate
//! writes that same state as the `utf-8`/`identity` content encoding.
//!
//! # What is registered by default
//!
//! [`ResultCompressor::new`] registers the identity codec plus **every codec
//! compiled into this build** — with default features that is gzip, zlib and
//! zstd. Nothing has to be wired up for `compressor.compress(data, "zstd")` to
//! work:
//!
//! ```
//! # #[cfg(feature = "compression-zstd")] {
//! use celers_core::result::ResultCompressor;
//!
//! let compressor = ResultCompressor::new(64);
//! let payload = vec![b'x'; 4096];
//!
//! let squeezed = compressor.compress(&payload, "zstd").expect("compresses");
//! assert!(squeezed.len() < payload.len());
//! assert_eq!(
//!     compressor.decompress(&squeezed, "zstd").expect("decompresses"),
//!     payload
//! );
//! # }
//! ```
//!
//! Building with `--no-default-features` leaves only `"none"` registered, and
//! asking for an absent algorithm is a named
//! [`CelersError::Configuration`](crate::CelersError::Configuration) rather
//! than a silent fallback to uncompressed bytes — a backend must never store
//! plaintext under a header claiming it is compressed.
//!
//! # Adding your own
//!
//! The seam is still open: implement [`CompressionCodec`] and register it with
//! [`ResultCompressor::with_codec`]. A codec registered under an existing name
//! replaces the built-in one.

use std::sync::Arc;

/// A pluggable compression codec.
///
/// The codecs CeleRS ships ([`GzipCodec`], [`ZlibCodec`], [`ZstdCodec`]) are
/// registered by [`ResultCompressor::new`] already; this trait is the seam for
/// anything else — a codec whose dependency lives in a backend crate, a
/// dictionary-trained zstd, an encrypt-then-compress wrapper.
///
/// A codec must round-trip its own output exactly: `decompress(compress(x))
/// == x` for every `x`, including the empty slice. A result store has no other
/// copy of the payload.
pub trait CompressionCodec: Send + Sync + std::fmt::Debug {
    /// Name this codec answers to (e.g. `"zstd"`, `"gzip"`).
    ///
    /// This is the string that ends up in
    /// [`ResultMetadata::compression_algorithm`](super::ResultMetadata) and
    /// the key the reader looks the codec up by, so it must match on both
    /// sides. See the module docs for the names already spoken for.
    fn name(&self) -> &str;

    /// Compress `data`.
    ///
    /// # Errors
    ///
    /// Returns an error if the codec cannot process the input.
    fn compress(&self, data: &[u8]) -> crate::Result<Vec<u8>>;

    /// Decompress `data`.
    ///
    /// # Errors
    ///
    /// Returns an error if the input is not valid for this codec.
    fn decompress(&self, data: &[u8]) -> crate::Result<Vec<u8>>;
}

/// The identity codec: stores payloads verbatim under the name `"none"`.
///
/// Always available, so a `ResultCompressor` is never a type whose operations
/// unconditionally fail. It is what `compression_algorithm: "none"` means on
/// the wire.
#[derive(Debug, Clone, Copy, Default)]
pub struct IdentityCodec;

impl CompressionCodec for IdentityCodec {
    fn name(&self) -> &str {
        "none"
    }

    fn compress(&self, data: &[u8]) -> crate::Result<Vec<u8>> {
        Ok(data.to_vec())
    }

    fn decompress(&self, data: &[u8]) -> crate::Result<Vec<u8>> {
        Ok(data.to_vec())
    }
}

/// Compression helper for result values
///
/// Holds a registry of [`CompressionCodec`]s keyed by algorithm name.
/// [`ResultCompressor::new`] registers the identity codec (`"none"`) plus every
/// codec compiled into this build — see the module docs for the table — and
/// [`ResultCompressor::with_codec`] adds or replaces one.
#[derive(Debug, Clone)]
pub struct ResultCompressor {
    threshold_bytes: usize,
    codecs: std::collections::HashMap<String, Arc<dyn CompressionCodec>>,
}

impl ResultCompressor {
    /// Create a compressor with the given threshold and every built-in codec
    /// registered.
    ///
    /// Payloads at or above `threshold_bytes` are worth compressing; see
    /// [`ResultCompressor::should_compress`].
    #[must_use]
    pub fn new(threshold_bytes: usize) -> Self {
        let mut compressor = Self::identity_only(threshold_bytes);
        for codec in builtin_codecs() {
            compressor.register_codec(codec);
        }
        compressor
    }

    /// Create a compressor with *only* the identity codec registered.
    ///
    /// For a caller that wants to decide the full algorithm set itself —
    /// refusing anything it did not register, rather than inheriting whatever
    /// this build happens to have compiled in.
    #[must_use]
    pub fn identity_only(threshold_bytes: usize) -> Self {
        let mut codecs: std::collections::HashMap<String, Arc<dyn CompressionCodec>> =
            std::collections::HashMap::new();
        codecs.insert("none".to_string(), Arc::new(IdentityCodec));
        Self {
            threshold_bytes,
            codecs,
        }
    }

    /// Register a codec, replacing any codec already registered under its name.
    #[must_use]
    pub fn with_codec(mut self, codec: Arc<dyn CompressionCodec>) -> Self {
        self.codecs.insert(codec.name().to_string(), codec);
        self
    }

    /// Register a codec on an existing compressor.
    pub fn register_codec(&mut self, codec: Arc<dyn CompressionCodec>) {
        self.codecs.insert(codec.name().to_string(), codec);
    }

    /// Names of the registered algorithms, sorted.
    #[must_use]
    pub fn algorithms(&self) -> Vec<String> {
        let mut names: Vec<String> = self.codecs.keys().cloned().collect();
        names.sort();
        names
    }

    /// Whether an algorithm is available.
    #[must_use]
    pub fn supports(&self, algorithm: &str) -> bool {
        self.codecs.contains_key(algorithm)
    }

    /// Check if value should be compressed
    ///
    /// True once the payload reaches the configured threshold. Below it,
    /// compression usually costs more bytes than it saves — every framed format
    /// has a header — as well as the CPU.
    #[must_use]
    pub const fn should_compress(&self, data: &[u8]) -> bool {
        data.len() >= self.threshold_bytes
    }

    /// The payload size at or above which [`should_compress`] says yes.
    ///
    /// [`should_compress`]: ResultCompressor::should_compress
    #[must_use]
    pub const fn threshold_bytes(&self) -> usize {
        self.threshold_bytes
    }

    /// Look up a codec by name.
    fn codec(&self, algorithm: &str) -> crate::Result<&Arc<dyn CompressionCodec>> {
        self.codecs.get(algorithm).ok_or_else(|| {
            crate::CelersError::Configuration(format!(
                "unsupported compression algorithm '{algorithm}'; registered: {}",
                self.algorithms().join(", ")
            ))
        })
    }

    /// Compress data with the named algorithm.
    ///
    /// # Errors
    ///
    /// Returns [`crate::CelersError::Configuration`] if no codec is registered
    /// under `algorithm`, or the codec's own error if compression fails.
    pub fn compress(&self, data: &[u8], algorithm: &str) -> crate::Result<Vec<u8>> {
        self.codec(algorithm)?.compress(data)
    }

    /// Decompress data with the named algorithm.
    ///
    /// # Errors
    ///
    /// Returns [`crate::CelersError::Configuration`] if no codec is registered
    /// under `algorithm`, or the codec's own error if decompression fails.
    pub fn decompress(&self, data: &[u8], algorithm: &str) -> crate::Result<Vec<u8>> {
        self.codec(algorithm)?.decompress(data)
    }
}

impl Default for ResultCompressor {
    fn default() -> Self {
        Self::new(1024 * 1024) // 1MB threshold
    }
}
/// Compression level used by [`GzipCodec`] and [`ZlibCodec`] when none is
/// given.
///
/// 6 is the DEFLATE default across zlib, gzip(1) and every implementation that
/// follows them: near the best ratio the format offers, at a fraction of the
/// time level 9 costs.
#[cfg(feature = "compression-deflate")]
pub const DEFAULT_DEFLATE_LEVEL: u8 = 6;

/// Compression level used by [`ZstdCodec`] when none is given.
///
/// 3 is upstream Zstandard's own default: it is the level the format is tuned
/// around, and it compresses faster than DEFLATE while beating its ratio.
#[cfg(feature = "compression-zstd")]
pub const DEFAULT_ZSTD_LEVEL: i32 = 3;

/// gzip (RFC 1952) — a DEFLATE stream inside a gzip header/trailer.
///
/// Answers to the name `"gzip"`. The framing is what makes a stored result
/// self-describing: the two magic bytes `1f 8b` identify it, and the trailer
/// carries a CRC-32 that [`decompress`](CompressionCodec::decompress) checks,
/// so a truncated or corrupted result is an error rather than silent garbage.
///
/// Pure Rust, via `oxiarc-deflate`.
#[cfg(feature = "compression-deflate")]
#[derive(Debug, Clone, Copy)]
pub struct GzipCodec {
    level: u8,
}

#[cfg(feature = "compression-deflate")]
impl Default for GzipCodec {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "compression-deflate")]
impl GzipCodec {
    /// A gzip codec at [`DEFAULT_DEFLATE_LEVEL`].
    #[must_use]
    pub const fn new() -> Self {
        Self {
            level: DEFAULT_DEFLATE_LEVEL,
        }
    }

    /// A gzip codec at an explicit level.
    ///
    /// Levels above 9 are clamped to 9: DEFLATE defines no level beyond it,
    /// and clamping keeps a mis-configured level from becoming a runtime
    /// error on a path whose job is to store a result.
    #[must_use]
    pub const fn with_level(level: u8) -> Self {
        Self {
            level: if level > 9 { 9 } else { level },
        }
    }

    /// The level this codec compresses at.
    #[must_use]
    pub const fn level(&self) -> u8 {
        self.level
    }
}

#[cfg(feature = "compression-deflate")]
impl CompressionCodec for GzipCodec {
    fn name(&self) -> &str {
        "gzip"
    }

    fn compress(&self, data: &[u8]) -> crate::Result<Vec<u8>> {
        oxiarc_deflate::gzip_compress(data, self.level)
            .map_err(|e| crate::CelersError::Serialization(format!("gzip compress: {e}")))
    }

    fn decompress(&self, data: &[u8]) -> crate::Result<Vec<u8>> {
        oxiarc_deflate::gzip_decompress(data)
            .map_err(|e| crate::CelersError::Deserialization(format!("gzip decompress: {e}")))
    }
}

/// zlib (RFC 1950) — a DEFLATE stream inside the compact zlib wrapper.
///
/// Answers to the name `"zlib"`, which is also what Celery's `deflate`
/// content encoding means on the wire. Same DEFLATE core as [`GzipCodec`] with
/// a smaller header and an Adler-32 rather than a CRC-32 check.
///
/// Pure Rust, via `oxiarc-deflate`.
#[cfg(feature = "compression-deflate")]
#[derive(Debug, Clone, Copy)]
pub struct ZlibCodec {
    level: u8,
}

#[cfg(feature = "compression-deflate")]
impl Default for ZlibCodec {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "compression-deflate")]
impl ZlibCodec {
    /// A zlib codec at [`DEFAULT_DEFLATE_LEVEL`].
    #[must_use]
    pub const fn new() -> Self {
        Self {
            level: DEFAULT_DEFLATE_LEVEL,
        }
    }

    /// A zlib codec at an explicit level, clamped to 9 (see
    /// [`GzipCodec::with_level`]).
    #[must_use]
    pub const fn with_level(level: u8) -> Self {
        Self {
            level: if level > 9 { 9 } else { level },
        }
    }

    /// The level this codec compresses at.
    #[must_use]
    pub const fn level(&self) -> u8 {
        self.level
    }
}

#[cfg(feature = "compression-deflate")]
impl CompressionCodec for ZlibCodec {
    fn name(&self) -> &str {
        "zlib"
    }

    fn compress(&self, data: &[u8]) -> crate::Result<Vec<u8>> {
        oxiarc_deflate::zlib_compress(data, self.level)
            .map_err(|e| crate::CelersError::Serialization(format!("zlib compress: {e}")))
    }

    fn decompress(&self, data: &[u8]) -> crate::Result<Vec<u8>> {
        oxiarc_deflate::zlib_decompress(data)
            .map_err(|e| crate::CelersError::Deserialization(format!("zlib decompress: {e}")))
    }
}

/// Zstandard (RFC 8878).
///
/// Answers to the name `"zstd"`. The usual choice for result payloads: it
/// compresses better than DEFLATE and decompresses several times faster, which
/// is the direction that matters when many workers read results back.
///
/// Pure Rust, via `oxiarc-zstd`.
#[cfg(feature = "compression-zstd")]
#[derive(Debug, Clone, Copy)]
pub struct ZstdCodec {
    level: i32,
}

#[cfg(feature = "compression-zstd")]
impl Default for ZstdCodec {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(feature = "compression-zstd")]
impl ZstdCodec {
    /// A zstd codec at [`DEFAULT_ZSTD_LEVEL`].
    #[must_use]
    pub const fn new() -> Self {
        Self {
            level: DEFAULT_ZSTD_LEVEL,
        }
    }

    /// A zstd codec at an explicit level, clamped to the format's `1..=22`.
    ///
    /// Clamping rather than erroring for the same reason as
    /// [`GzipCodec::with_level`]: a mis-configured level must not turn storing
    /// a result into a failure.
    #[must_use]
    pub const fn with_level(level: i32) -> Self {
        let level = if level < 1 {
            1
        } else if level > 22 {
            22
        } else {
            level
        };
        Self { level }
    }

    /// The level this codec compresses at.
    #[must_use]
    pub const fn level(&self) -> i32 {
        self.level
    }
}

#[cfg(feature = "compression-zstd")]
impl CompressionCodec for ZstdCodec {
    fn name(&self) -> &str {
        "zstd"
    }

    fn compress(&self, data: &[u8]) -> crate::Result<Vec<u8>> {
        oxiarc_zstd::encode_all(data, self.level)
            .map_err(|e| crate::CelersError::Serialization(format!("zstd compress: {e}")))
    }

    fn decompress(&self, data: &[u8]) -> crate::Result<Vec<u8>> {
        oxiarc_zstd::decode_all(data)
            .map_err(|e| crate::CelersError::Deserialization(format!("zstd decompress: {e}")))
    }
}

/// Every codec compiled into this build, identity first.
///
/// This is what [`ResultCompressor::new`] registers. It is public so a caller
/// assembling its own registry — or a test asserting which algorithms a build
/// actually supports — does not have to restate the feature gates.
#[must_use]
pub fn builtin_codecs() -> Vec<Arc<dyn CompressionCodec>> {
    let codecs: Vec<Arc<dyn CompressionCodec>> = vec![
        Arc::new(IdentityCodec),
        #[cfg(feature = "compression-deflate")]
        Arc::new(GzipCodec::new()),
        #[cfg(feature = "compression-deflate")]
        Arc::new(ZlibCodec::new()),
        #[cfg(feature = "compression-zstd")]
        Arc::new(ZstdCodec::new()),
    ];
    codecs
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Regression: `ResultCompressor` was a public, constructible type whose
    /// only two operations always failed.
    #[test]
    fn result_compressor_dispatches_to_registered_codecs() {
        #[derive(Debug)]
        struct XorCodec;

        impl CompressionCodec for XorCodec {
            fn name(&self) -> &str {
                "xor"
            }
            fn compress(&self, data: &[u8]) -> crate::Result<Vec<u8>> {
                Ok(data.iter().map(|b| b ^ 0x5a).collect())
            }
            fn decompress(&self, data: &[u8]) -> crate::Result<Vec<u8>> {
                Ok(data.iter().map(|b| b ^ 0x5a).collect())
            }
        }

        let compressor = ResultCompressor::new(4).with_codec(Arc::new(XorCodec));

        // Exactly the built-ins of this build, plus the one just registered —
        // asserted as a set, not a hard-coded list, so the check stays exact
        // under every feature combination.
        let mut expected = ResultCompressor::new(4).algorithms();
        expected.push("xor".to_string());
        expected.sort();
        assert_eq!(compressor.algorithms(), expected);
        assert!(compressor.supports("none"));
        assert!(compressor.supports("xor"));
        assert!(!compressor.supports("brotli"));

        assert!(compressor.should_compress(b"12345"));
        assert!(!compressor.should_compress(b"123"));
        assert_eq!(compressor.threshold_bytes(), 4);

        // The always-present identity codec round-trips verbatim.
        let identity = compressor.compress(b"payload", "none").expect("compress");
        assert_eq!(identity, b"payload");
        assert_eq!(
            compressor
                .decompress(&identity, "none")
                .expect("decompress"),
            b"payload"
        );

        // The registered codec is actually used.
        let squeezed = compressor.compress(b"payload", "xor").expect("compress");
        assert_ne!(squeezed, b"payload");
        assert_eq!(
            compressor.decompress(&squeezed, "xor").expect("decompress"),
            b"payload"
        );

        // An unregistered algorithm is a named configuration error, not a
        // blanket "compression not available".
        let err = compressor
            .compress(b"payload", "brotli")
            .expect_err("unknown algorithm must be rejected");
        assert!(
            err.to_string()
                .contains("unsupported compression algorithm"),
            "unexpected: {err}"
        );
        // ...and it names what *is* registered, so the misconfiguration is
        // diagnosable from the message alone.
        assert!(err.to_string().contains("none"), "unexpected: {err}");
    }

    /// A test payload with real structure: repetitive enough that every codec
    /// must beat it, but not a single byte repeated (which flatters RLE-ish
    /// paths and hides framing bugs).
    fn corpus() -> Vec<u8> {
        let mut data = Vec::with_capacity(8192);
        for i in 0..512u32 {
            data.extend_from_slice(format!("{{\"task\":\"tasks.add\",\"seq\":{i}}}\n").as_bytes());
        }
        data
    }

    /// Every codec this build registered must round-trip byte for byte, over
    /// the awkward inputs as well as the easy one.
    #[test]
    fn every_builtin_codec_round_trips() {
        let single_byte = vec![0u8];
        let all_same = vec![b'a'; 4096];
        let incompressible: Vec<u8> = (0..4096u32)
            .map(|i| (i.wrapping_mul(2_654_435_761) >> 24) as u8)
            .collect();
        let corpus = corpus();
        let payloads: [&[u8]; 5] = [b"", &single_byte, &all_same, &incompressible, &corpus];

        for codec in builtin_codecs() {
            for payload in payloads {
                let squeezed = codec
                    .compress(payload)
                    .unwrap_or_else(|e| panic!("{} compress: {e}", codec.name()));
                let restored = codec
                    .decompress(&squeezed)
                    .unwrap_or_else(|e| panic!("{} decompress: {e}", codec.name()));
                assert_eq!(
                    restored,
                    payload,
                    "{} did not round-trip a {}-byte payload",
                    codec.name(),
                    payload.len()
                );
            }
        }
    }

    /// The point of registering real codecs: the bytes actually get smaller.
    /// The identity codec is exempt — it is defined not to shrink anything.
    #[test]
    fn real_codecs_actually_compress() {
        let payload = corpus();
        let compressor = ResultCompressor::new(1024);

        for codec in builtin_codecs() {
            if codec.name() == "none" {
                assert_eq!(
                    codec.compress(&payload).expect("identity compress").len(),
                    payload.len(),
                    "the identity codec must store payloads verbatim"
                );
                continue;
            }

            let squeezed = codec.compress(&payload).expect("compress");
            assert!(
                squeezed.len() < payload.len() / 2,
                "{} only got {} bytes down to {}",
                codec.name(),
                payload.len(),
                squeezed.len()
            );

            // ...and the registry reaches the same codec by name.
            let via_registry = compressor
                .compress(&payload, codec.name())
                .expect("registry compress");
            assert_eq!(
                compressor
                    .decompress(&via_registry, codec.name())
                    .expect("registry decompress"),
                payload
            );
        }
    }

    #[test]
    fn threshold_decides_what_is_worth_compressing() {
        // The threshold is the whole reason a small result is stored verbatim:
        // every framed format has a header, so squeezing 8 bytes makes them
        // bigger.
        let compressor = ResultCompressor::new(1024);
        assert!(!compressor.should_compress(&[0u8; 1023]));
        assert!(compressor.should_compress(&[0u8; 1024]));
        assert!(compressor.should_compress(&[0u8; 4096]));

        // Default threshold is 1 MiB.
        let default = ResultCompressor::default();
        assert_eq!(default.threshold_bytes(), 1024 * 1024);
        assert!(!default.should_compress(&[0u8; 1024]));

        // A threshold of 0 means "always worth it", including for empty data.
        assert!(ResultCompressor::new(0).should_compress(b""));
    }

    #[test]
    fn identity_only_registers_nothing_else() {
        // The escape hatch for a caller that refuses to inherit whatever this
        // build compiled in.
        let bare = ResultCompressor::identity_only(64);
        assert_eq!(bare.algorithms(), vec!["none".to_string()]);
        assert_eq!(bare.compress(b"abc", "none").expect("identity"), b"abc");
        assert!(bare.compress(b"abc", "gzip").is_err());
    }

    #[test]
    fn a_registered_codec_replaces_a_builtin_of_the_same_name() {
        #[derive(Debug)]
        struct Sentinel;
        impl CompressionCodec for Sentinel {
            fn name(&self) -> &str {
                "none"
            }
            fn compress(&self, _: &[u8]) -> crate::Result<Vec<u8>> {
                Ok(b"sentinel".to_vec())
            }
            fn decompress(&self, _: &[u8]) -> crate::Result<Vec<u8>> {
                Ok(b"sentinel".to_vec())
            }
        }

        let compressor = ResultCompressor::new(4).with_codec(Arc::new(Sentinel));
        assert_eq!(
            compressor.compress(b"payload", "none").expect("compress"),
            b"sentinel"
        );
        // Registering under an existing name replaces rather than duplicates.
        assert_eq!(
            compressor.algorithms(),
            ResultCompressor::new(4).algorithms()
        );
    }

    #[test]
    fn corrupted_input_is_an_error_not_silent_garbage() {
        // A framed codec must reject bytes it did not produce. This is what
        // stops a truncated result being handed back to a caller as data.
        for codec in builtin_codecs() {
            if codec.name() == "none" {
                continue; // the identity codec accepts every byte string by definition
            }
            let err = codec.decompress(b"definitely not a compressed frame");
            assert!(
                err.is_err(),
                "{} accepted a payload it never produced",
                codec.name()
            );

            // Truncation must be caught too, not just a bad header.
            let good = codec.compress(&corpus()).expect("compress");
            let truncated = &good[..good.len() / 2];
            assert!(
                codec.decompress(truncated).is_err(),
                "{} accepted a truncated frame",
                codec.name()
            );
        }
    }

    #[cfg(feature = "compression-deflate")]
    #[test]
    fn gzip_and_zlib_are_registered_and_framed_as_their_formats_say() {
        let compressor = ResultCompressor::new(64);
        assert!(compressor.supports("gzip"));
        assert!(compressor.supports("zlib"));

        // RFC 1952: a gzip member starts with the magic bytes 1f 8b.
        let gzip = compressor.compress(&corpus(), "gzip").expect("gzip");
        assert_eq!(&gzip[..2], &[0x1f, 0x8b], "not a gzip frame");

        // RFC 1950: the zlib header's low nibble of CMF is 8 (deflate) and the
        // two header bytes are a multiple of 31 when read big-endian.
        let zlib = compressor.compress(&corpus(), "zlib").expect("zlib");
        assert_eq!(zlib[0] & 0x0f, 8, "zlib CM must be deflate");
        assert_eq!(
            (u16::from(zlib[0]) << 8 | u16::from(zlib[1])) % 31,
            0,
            "zlib header check bits are wrong"
        );

        // The two formats wrap the same DEFLATE core, so neither can read the
        // other's frame.
        assert!(compressor.decompress(&gzip, "zlib").is_err());
        assert!(compressor.decompress(&zlib, "gzip").is_err());

        // Levels are honoured and clamped at the format's ceiling.
        assert_eq!(GzipCodec::new().level(), DEFAULT_DEFLATE_LEVEL);
        assert_eq!(GzipCodec::with_level(99).level(), 9);
        assert_eq!(ZlibCodec::with_level(1).level(), 1);

        let fast = GzipCodec::with_level(1).compress(&corpus()).expect("l1");
        let best = GzipCodec::with_level(9).compress(&corpus()).expect("l9");
        assert!(
            best.len() <= fast.len(),
            "level 9 ({}) should not be larger than level 1 ({})",
            best.len(),
            fast.len()
        );
    }

    #[cfg(feature = "compression-zstd")]
    #[test]
    fn zstd_is_registered_and_framed_as_the_format_says() {
        let compressor = ResultCompressor::new(64);
        assert!(compressor.supports("zstd"));

        // RFC 8878: a Zstandard frame starts with the magic number 0xFD2FB528,
        // little-endian.
        let zstd = compressor.compress(&corpus(), "zstd").expect("zstd");
        assert_eq!(&zstd[..4], &[0x28, 0xb5, 0x2f, 0xfd], "not a zstd frame");

        assert_eq!(ZstdCodec::new().level(), DEFAULT_ZSTD_LEVEL);
        assert_eq!(ZstdCodec::with_level(-5).level(), 1);
        assert_eq!(ZstdCodec::with_level(99).level(), 22);

        assert_eq!(
            compressor.decompress(&zstd, "zstd").expect("decompress"),
            corpus()
        );
    }

    /// The names are a cross-crate contract: a result written with one of them
    /// is read back by looking the same string up. They must match the
    /// `content-encoding` vocabulary `celers-protocol` uses.
    #[test]
    fn builtin_names_are_the_expected_wire_strings() {
        let names = ResultCompressor::new(64).algorithms();

        assert!(names.contains(&"none".to_string()));
        #[cfg(feature = "compression-deflate")]
        {
            assert!(names.contains(&"gzip".to_string()));
            assert!(names.contains(&"zlib".to_string()));
        }
        #[cfg(feature = "compression-zstd")]
        assert!(names.contains(&"zstd".to_string()));

        // Every registered codec answers to the name it is filed under.
        for codec in builtin_codecs() {
            assert!(
                names.contains(&codec.name().to_string()),
                "{} is built in but not registered",
                codec.name()
            );
        }
    }

    /// The metadata a backend writes alongside a compressed result must
    /// describe what actually happened.
    #[test]
    fn compression_metadata_describes_the_real_saving() {
        let payload = corpus();
        let compressor = ResultCompressor::new(1024);
        assert!(compressor.should_compress(&payload));

        // Pick whichever real codec this build has; identity is not a saving.
        let Some(codec) = builtin_codecs().into_iter().find(|c| c.name() != "none") else {
            return; // --no-default-features build: nothing to measure
        };

        let squeezed = codec.compress(&payload).expect("compress");
        let metadata = super::super::ResultMetadata::new().with_compression(
            codec.name(),
            payload.len(),
            squeezed.len(),
        );

        assert!(metadata.compressed);
        assert_eq!(
            metadata.compression_algorithm.as_deref(),
            Some(codec.name())
        );
        let ratio = metadata.compression_ratio().expect("both sizes recorded");
        assert!(ratio < 0.5, "expected a real saving, got {ratio}");
    }
}
