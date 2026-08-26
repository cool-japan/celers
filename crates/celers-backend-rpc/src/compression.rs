//! Optional compression for `result_data` on the gRPC wire.
//!
//! Unlike `celers-backend-db`'s `result_data` (a native JSON/JSONB column
//! that rejects non-JSON), a protobuf message field can hold arbitrary
//! binary directly — see `proto::TaskMeta::result_data_compressed`, a
//! `bytes` field, no base64 needed. Compression here is delegated to
//! `celers_core::ResultCompressor`, the shared codec seam every
//! result-storing backend in this workspace can register against.

use std::sync::Arc;

use celers_core::ResultCompressor;

/// Compression settings for `GrpcResultBackend`'s `result_data`.
///
/// Disabled by default (what [`crate::GrpcResultBackend::connect`]
/// installs): compression is an opt-in wire-size optimization, not a
/// silent default behavior change for existing deployments.
#[derive(Debug, Clone)]
pub struct CompressionConfig {
    enabled: bool,
    compressor: Arc<ResultCompressor>,
    algorithm: String,
}

impl CompressionConfig {
    /// Compression off: `to_proto_meta_with_compression`
    /// always leaves `result_data_compressed` unset. Decoding an *incoming*
    /// compressed message is unconditional and needs no config at all (see
    /// `from_proto_meta`), so this only affects what this side writes.
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
    #[must_use]
    pub fn new(threshold_bytes: usize, algorithm: impl Into<String>) -> Self {
        Self {
            enabled: true,
            compressor: Arc::new(ResultCompressor::new(threshold_bytes)),
            algorithm: algorithm.into(),
        }
    }

    /// Whether this config compresses on write.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub(crate) fn compressor(&self) -> &ResultCompressor {
        &self.compressor
    }

    pub(crate) fn algorithm(&self) -> &str {
        &self.algorithm
    }
}
