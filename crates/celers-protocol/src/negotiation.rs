//! Protocol version negotiation and detection
//!
//! This module provides utilities for detecting and negotiating Celery protocol
//! versions between CeleRS and Python Celery workers/clients.
//!
//! # Protocol Versions
//!
//! - **Protocol v1**: Legacy format (Celery 3.x and earlier) - Not supported
//! - **Protocol v2**: Current stable format (Celery 4.x+) - Fully supported
//! - **Protocol v5**: Extended format (Celery 5.x+) - Fully supported
//!
//! # Example
//!
//! ```
//! use celers_protocol::negotiation::{ProtocolNegotiator, negotiate_protocol};
//! use celers_protocol::ProtocolVersion;
//!
//! // Negotiate between supported versions
//! let negotiator = ProtocolNegotiator::new()
//!     .prefer(ProtocolVersion::V5)
//!     .support(ProtocolVersion::V2);
//!
//! let agreed = negotiator.negotiate(&[ProtocolVersion::V2]).unwrap();
//! assert_eq!(agreed, ProtocolVersion::V2);
//! ```

use crate::ProtocolVersion;
use std::collections::HashSet;

/// Protocol detection result
#[derive(Debug, Clone, PartialEq)]
pub struct ProtocolDetection {
    /// Detected protocol version
    pub version: ProtocolVersion,
    /// Confidence level (0.0 - 1.0)
    pub confidence: f32,
    /// Detection method used
    pub method: DetectionMethod,
}

/// Method used for protocol detection
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetectionMethod {
    /// Detected from message headers
    Headers,
    /// Detected from message structure
    Structure,
    /// Detected from content type
    ContentType,
    /// Default assumption
    Default,
}

impl std::fmt::Display for DetectionMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DetectionMethod::Headers => write!(f, "headers"),
            DetectionMethod::Structure => write!(f, "structure"),
            DetectionMethod::ContentType => write!(f, "content-type"),
            DetectionMethod::Default => write!(f, "default"),
        }
    }
}

/// Protocol negotiation error
#[derive(Debug, Clone)]
pub enum NegotiationError {
    /// No common protocol version found
    NoCommonVersion,
    /// Protocol version not supported
    UnsupportedVersion(ProtocolVersion),
    /// Invalid protocol data
    InvalidData(String),
}

impl std::fmt::Display for NegotiationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            NegotiationError::NoCommonVersion => {
                write!(f, "No common protocol version found")
            }
            NegotiationError::UnsupportedVersion(v) => {
                write!(f, "Protocol version {} is not supported", v)
            }
            NegotiationError::InvalidData(msg) => {
                write!(f, "Invalid protocol data: {}", msg)
            }
        }
    }
}

impl std::error::Error for NegotiationError {}

/// Protocol negotiator for version agreement
#[derive(Debug, Clone)]
pub struct ProtocolNegotiator {
    /// Supported protocol versions
    supported: HashSet<ProtocolVersion>,
    /// Preferred protocol version (highest priority)
    preferred: Option<ProtocolVersion>,
}

impl Default for ProtocolNegotiator {
    fn default() -> Self {
        Self::new()
    }
}

impl ProtocolNegotiator {
    /// Create a new negotiator with default support (v2 and v5)
    pub fn new() -> Self {
        let mut supported = HashSet::new();
        supported.insert(ProtocolVersion::V2);
        supported.insert(ProtocolVersion::V5);

        Self {
            supported,
            preferred: Some(ProtocolVersion::V2), // Default to v2 for compatibility
        }
    }

    /// Create a negotiator that only supports v2
    pub fn v2_only() -> Self {
        let mut supported = HashSet::new();
        supported.insert(ProtocolVersion::V2);

        Self {
            supported,
            preferred: Some(ProtocolVersion::V2),
        }
    }

    /// Create a negotiator that prefers v5
    pub fn prefer_v5() -> Self {
        let mut supported = HashSet::new();
        supported.insert(ProtocolVersion::V2);
        supported.insert(ProtocolVersion::V5);

        Self {
            supported,
            preferred: Some(ProtocolVersion::V5),
        }
    }

    /// Set the preferred protocol version
    #[must_use]
    pub fn prefer(mut self, version: ProtocolVersion) -> Self {
        self.preferred = Some(version);
        self.supported.insert(version);
        self
    }

    /// Add support for a protocol version
    #[must_use]
    pub fn support(mut self, version: ProtocolVersion) -> Self {
        self.supported.insert(version);
        self
    }

    /// Remove support for a protocol version
    #[must_use]
    pub fn unsupport(mut self, version: ProtocolVersion) -> Self {
        self.supported.remove(&version);
        if self.preferred == Some(version) {
            self.preferred = None;
        }
        self
    }

    /// Check if a protocol version is supported
    #[inline]
    pub fn is_supported(&self, version: ProtocolVersion) -> bool {
        self.supported.contains(&version)
    }

    /// Get all supported versions
    #[inline]
    pub fn supported_versions(&self) -> Vec<ProtocolVersion> {
        self.supported.iter().copied().collect()
    }

    /// Get the preferred version
    #[inline]
    pub fn preferred_version(&self) -> Option<ProtocolVersion> {
        self.preferred
    }

    /// Negotiate a protocol version with a remote party
    ///
    /// Returns the agreed version based on mutual support, preferring
    /// our preferred version if mutually supported.
    pub fn negotiate(
        &self,
        remote_versions: &[ProtocolVersion],
    ) -> Result<ProtocolVersion, NegotiationError> {
        // Find common versions
        let remote_set: HashSet<_> = remote_versions.iter().copied().collect();
        let common: Vec<_> = self.supported.intersection(&remote_set).copied().collect();

        if common.is_empty() {
            return Err(NegotiationError::NoCommonVersion);
        }

        // If our preferred version is in common, use it
        if let Some(preferred) = self.preferred {
            if common.contains(&preferred) {
                return Ok(preferred);
            }
        }

        // Otherwise, prefer v5 over v2
        if common.contains(&ProtocolVersion::V5) {
            Ok(ProtocolVersion::V5)
        } else {
            Ok(ProtocolVersion::V2)
        }
    }

    /// Validate that a message uses a supported protocol version
    pub fn validate_version(&self, version: ProtocolVersion) -> Result<(), NegotiationError> {
        if self.is_supported(version) {
            Ok(())
        } else {
            Err(NegotiationError::UnsupportedVersion(version))
        }
    }
}

/// Detect protocol version from a JSON message
///
/// Analyzes the message structure to determine which protocol version it uses.
pub fn detect_protocol(json: &serde_json::Value) -> ProtocolDetection {
    // Check for protocol header (v5 style)
    if let Some(headers) = json.get("headers") {
        if headers.get("protocol").is_some() {
            return ProtocolDetection {
                version: ProtocolVersion::V5,
                confidence: 1.0,
                method: DetectionMethod::Headers,
            };
        }

        // v2 has lang header
        if headers.get("lang").is_some() {
            return ProtocolDetection {
                version: ProtocolVersion::V2,
                confidence: 0.9,
                method: DetectionMethod::Headers,
            };
        }
    }

    // Check message structure
    if json.get("headers").is_some()
        && json.get("properties").is_some()
        && json.get("body").is_some()
    {
        return ProtocolDetection {
            version: ProtocolVersion::V2,
            confidence: 0.8,
            method: DetectionMethod::Structure,
        };
    }

    // Default to v2
    ProtocolDetection {
        version: ProtocolVersion::V2,
        confidence: 0.5,
        method: DetectionMethod::Default,
    }
}

/// Detect protocol version from message bytes
pub fn detect_protocol_from_bytes(bytes: &[u8]) -> Result<ProtocolDetection, NegotiationError> {
    let json: serde_json::Value =
        serde_json::from_slice(bytes).map_err(|e| NegotiationError::InvalidData(e.to_string()))?;

    Ok(detect_protocol(&json))
}

/// Simple negotiation helper function
///
/// Negotiates between local and remote supported versions.
pub fn negotiate_protocol(
    local: &[ProtocolVersion],
    remote: &[ProtocolVersion],
) -> Result<ProtocolVersion, NegotiationError> {
    let mut negotiator = ProtocolNegotiator::new();

    // Clear default and add only specified versions
    negotiator.supported.clear();
    for v in local {
        negotiator = negotiator.support(*v);
    }

    if let Some(&first) = local.first() {
        negotiator = negotiator.prefer(first);
    }

    negotiator.negotiate(remote)
}

/// Header/property key under which the protocol version is advertised and
/// parsed on the wire. The stored value is the version's numeric string
/// (e.g. `"2"` or `"5"`), matching [`ProtocolVersion::as_number_str`] and the
/// `protocol_version` stamp used by [`crate::migration`] and [`crate::v5`].
pub const PROTOCOL_VERSION_KEY: &str = "protocol_version";

/// Advertise the protocol versions supported locally by this CeleRS build.
///
/// The list is returned in descending preference order (highest version first),
/// which is also the order honoured by [`negotiate_version`]. Use this when
/// announcing capabilities to a remote party (e.g. in a handshake header).
///
/// # Example
///
/// ```
/// use celers_protocol::negotiation::locally_supported_versions;
/// use celers_protocol::ProtocolVersion;
///
/// let advertised = locally_supported_versions();
/// assert_eq!(advertised, vec![ProtocolVersion::V5, ProtocolVersion::V2]);
/// ```
pub fn locally_supported_versions() -> Vec<ProtocolVersion> {
    // Highest first so that the preference order is explicit and stable.
    vec![ProtocolVersion::V5, ProtocolVersion::V2]
}

/// Negotiate the highest mutually-supported protocol version.
///
/// Given the versions supported by the local party and those supported by the
/// remote party, returns the **highest** version present in both sets, or
/// [`None`] when there is no overlap at all. Ordering relies on the `Ord`
/// implementation of [`ProtocolVersion`] (`V2 < V5`).
///
/// This is a pure, allocation-light function that does not depend on a
/// [`ProtocolNegotiator`] instance, making it convenient for simple handshakes.
///
/// # Examples
///
/// ```
/// use celers_protocol::negotiation::negotiate_version;
/// use celers_protocol::ProtocolVersion;
///
/// // Both support v2 and v5 -> the highest common version (v5) wins.
/// let agreed = negotiate_version(
///     &[ProtocolVersion::V2, ProtocolVersion::V5],
///     &[ProtocolVersion::V2, ProtocolVersion::V5],
/// );
/// assert_eq!(agreed, Some(ProtocolVersion::V5));
///
/// // Only v2 is common.
/// let agreed = negotiate_version(
///     &[ProtocolVersion::V2, ProtocolVersion::V5],
///     &[ProtocolVersion::V2],
/// );
/// assert_eq!(agreed, Some(ProtocolVersion::V2));
///
/// // No overlap.
/// let agreed = negotiate_version(&[ProtocolVersion::V2], &[ProtocolVersion::V5]);
/// assert_eq!(agreed, None);
/// ```
pub fn negotiate_version(
    local_supported: &[ProtocolVersion],
    remote_supported: &[ProtocolVersion],
) -> Option<ProtocolVersion> {
    let remote_set: HashSet<ProtocolVersion> = remote_supported.iter().copied().collect();
    local_supported
        .iter()
        .copied()
        .filter(|version| remote_set.contains(version))
        .max()
}

/// Parse a [`ProtocolVersion`] from a header/property string value.
///
/// Accepts both the numeric form (`"2"`, `"5"`) and the prefixed form
/// (`"v2"`, `"V5"`), delegating to [`ProtocolVersion`]'s `FromStr`. Returns
/// [`None`] for any unrecognised value.
///
/// # Example
///
/// ```
/// use celers_protocol::negotiation::parse_version;
/// use celers_protocol::ProtocolVersion;
///
/// assert_eq!(parse_version("5"), Some(ProtocolVersion::V5));
/// assert_eq!(parse_version("v2"), Some(ProtocolVersion::V2));
/// assert_eq!(parse_version("nope"), None);
/// ```
pub fn parse_version(value: &str) -> Option<ProtocolVersion> {
    value.parse::<ProtocolVersion>().ok()
}

/// Encode a [`ProtocolVersion`] into its canonical header/property string.
///
/// This is the numeric form (`"2"` / `"5"`) used for the `protocol_version`
/// header, matching [`ProtocolVersion::as_number_str`].
///
/// # Example
///
/// ```
/// use celers_protocol::negotiation::encode_version;
/// use celers_protocol::ProtocolVersion;
///
/// assert_eq!(encode_version(ProtocolVersion::V5), "5");
/// ```
#[inline]
pub fn encode_version(version: ProtocolVersion) -> &'static str {
    version.as_number_str()
}

/// Extract a [`ProtocolVersion`] from a map of message headers.
///
/// Looks up [`PROTOCOL_VERSION_KEY`] and, if present and parseable (as either a
/// JSON string like `"5"` or a JSON number like `5`), returns the corresponding
/// version. Returns [`None`] when the header is absent or unrecognised.
///
/// This understands the same stamp written by [`crate::v5::build_v5_message`]
/// and [`crate::migration::ProtocolMigrator::migrate`].
pub fn parse_version_from_headers(
    headers: &std::collections::HashMap<String, serde_json::Value>,
) -> Option<ProtocolVersion> {
    let value = headers.get(PROTOCOL_VERSION_KEY)?;
    match value {
        serde_json::Value::String(s) => parse_version(s),
        serde_json::Value::Number(n) => n.as_u64().and_then(|num| match num {
            2 => Some(ProtocolVersion::V2),
            5 => Some(ProtocolVersion::V5),
            _ => None,
        }),
        _ => None,
    }
}

/// Encode a [`ProtocolVersion`] into a map of message headers.
///
/// Inserts (or overwrites) [`PROTOCOL_VERSION_KEY`] with the canonical numeric
/// string value (`"2"` / `"5"`), so the stamp is consistent with the rest of
/// the crate.
pub fn encode_version_into_headers(
    headers: &mut std::collections::HashMap<String, serde_json::Value>,
    version: ProtocolVersion,
) {
    headers.insert(
        PROTOCOL_VERSION_KEY.to_string(),
        serde_json::Value::String(encode_version(version).to_string()),
    );
}

/// Protocol capabilities
#[derive(Debug, Clone, Default)]
pub struct ProtocolCapabilities {
    /// Supports task chains
    pub chains: bool,
    /// Supports task groups
    pub groups: bool,
    /// Supports chords
    pub chords: bool,
    /// Supports ETA/countdown
    pub eta: bool,
    /// Supports task expiration
    pub expires: bool,
    /// Supports task revocation
    pub revocation: bool,
    /// Supports task events
    pub events: bool,
    /// Supports result backends
    pub results: bool,
}

impl ProtocolCapabilities {
    /// Get capabilities for protocol v2
    pub fn v2() -> Self {
        Self {
            chains: true,
            groups: true,
            chords: true,
            eta: true,
            expires: true,
            revocation: true,
            events: true,
            results: true,
        }
    }

    /// Get capabilities for protocol v5
    pub fn v5() -> Self {
        Self {
            chains: true,
            groups: true,
            chords: true,
            eta: true,
            expires: true,
            revocation: true,
            events: true,
            results: true,
        }
    }

    /// Get capabilities for a protocol version
    pub fn for_version(version: ProtocolVersion) -> Self {
        match version {
            ProtocolVersion::V2 => Self::v2(),
            ProtocolVersion::V5 => Self::v5(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_protocol_negotiator_default() {
        let negotiator = ProtocolNegotiator::new();
        assert!(negotiator.is_supported(ProtocolVersion::V2));
        assert!(negotiator.is_supported(ProtocolVersion::V5));
        assert_eq!(negotiator.preferred_version(), Some(ProtocolVersion::V2));
    }

    #[test]
    fn test_protocol_negotiator_v2_only() {
        let negotiator = ProtocolNegotiator::v2_only();
        assert!(negotiator.is_supported(ProtocolVersion::V2));
        assert!(!negotiator.is_supported(ProtocolVersion::V5));
    }

    #[test]
    fn test_protocol_negotiator_prefer_v5() {
        let negotiator = ProtocolNegotiator::prefer_v5();
        assert!(negotiator.is_supported(ProtocolVersion::V2));
        assert!(negotiator.is_supported(ProtocolVersion::V5));
        assert_eq!(negotiator.preferred_version(), Some(ProtocolVersion::V5));
    }

    #[test]
    fn test_negotiate_common_version() {
        let negotiator = ProtocolNegotiator::new();
        let result = negotiator.negotiate(&[ProtocolVersion::V2]);
        assert_eq!(result.unwrap(), ProtocolVersion::V2);
    }

    #[test]
    fn test_negotiate_prefers_preferred() {
        let negotiator = ProtocolNegotiator::new().prefer(ProtocolVersion::V5);
        let result = negotiator.negotiate(&[ProtocolVersion::V2, ProtocolVersion::V5]);
        assert_eq!(result.unwrap(), ProtocolVersion::V5);
    }

    #[test]
    fn test_negotiate_no_common() {
        let negotiator = ProtocolNegotiator::v2_only();
        let result = negotiator.negotiate(&[ProtocolVersion::V5]);
        assert!(matches!(result, Err(NegotiationError::NoCommonVersion)));
    }

    #[test]
    fn test_validate_version_supported() {
        let negotiator = ProtocolNegotiator::new();
        assert!(negotiator.validate_version(ProtocolVersion::V2).is_ok());
    }

    #[test]
    fn test_validate_version_unsupported() {
        let negotiator = ProtocolNegotiator::v2_only().unsupport(ProtocolVersion::V2);
        let result = negotiator.validate_version(ProtocolVersion::V5);
        assert!(matches!(
            result,
            Err(NegotiationError::UnsupportedVersion(_))
        ));
    }

    #[test]
    fn test_detect_protocol_v2() {
        let msg = json!({
            "headers": {
                "task": "test",
                "id": "123",
                "lang": "py"
            },
            "properties": {},
            "body": "test"
        });

        let detection = detect_protocol(&msg);
        assert_eq!(detection.version, ProtocolVersion::V2);
        assert!(detection.confidence >= 0.8);
    }

    #[test]
    fn test_detect_protocol_v5() {
        let msg = json!({
            "headers": {
                "task": "test",
                "id": "123",
                "protocol": 2
            },
            "properties": {},
            "body": "test"
        });

        let detection = detect_protocol(&msg);
        assert_eq!(detection.version, ProtocolVersion::V5);
        assert_eq!(detection.confidence, 1.0);
    }

    #[test]
    fn test_detect_protocol_from_bytes() {
        let bytes = br#"{"headers":{"lang":"py"},"properties":{},"body":""}"#;
        let detection = detect_protocol_from_bytes(bytes).unwrap();
        assert_eq!(detection.version, ProtocolVersion::V2);
    }

    #[test]
    fn test_negotiate_protocol_helper() {
        let result = negotiate_protocol(
            &[ProtocolVersion::V2, ProtocolVersion::V5],
            &[ProtocolVersion::V2],
        );
        assert_eq!(result.unwrap(), ProtocolVersion::V2);
    }

    #[test]
    fn test_protocol_capabilities() {
        let caps = ProtocolCapabilities::for_version(ProtocolVersion::V2);
        assert!(caps.chains);
        assert!(caps.groups);
        assert!(caps.chords);
        assert!(caps.events);
    }

    #[test]
    fn test_detection_method_display() {
        assert_eq!(DetectionMethod::Headers.to_string(), "headers");
        assert_eq!(DetectionMethod::Structure.to_string(), "structure");
        assert_eq!(DetectionMethod::ContentType.to_string(), "content-type");
        assert_eq!(DetectionMethod::Default.to_string(), "default");
    }

    #[test]
    fn test_negotiation_error_display() {
        let err = NegotiationError::NoCommonVersion;
        assert_eq!(err.to_string(), "No common protocol version found");

        let err = NegotiationError::UnsupportedVersion(ProtocolVersion::V5);
        assert!(err.to_string().contains("v5"));

        let err = NegotiationError::InvalidData("test".to_string());
        assert!(err.to_string().contains("test"));
    }

    #[test]
    fn test_supported_versions() {
        let negotiator = ProtocolNegotiator::new();
        let versions = negotiator.supported_versions();
        assert!(versions.contains(&ProtocolVersion::V2));
        assert!(versions.contains(&ProtocolVersion::V5));
    }

    #[test]
    fn test_locally_supported_versions_descending_preference() {
        let advertised = locally_supported_versions();
        assert_eq!(advertised, vec![ProtocolVersion::V5, ProtocolVersion::V2]);
    }

    #[test]
    fn test_negotiate_version_full_matrix() {
        use ProtocolVersion::{V2, V5};

        // Both support everything -> highest common (v5).
        assert_eq!(negotiate_version(&[V2, V5], &[V2, V5]), Some(V5));
        // Order of inputs must not matter.
        assert_eq!(negotiate_version(&[V5, V2], &[V2, V5]), Some(V5));

        // Single common version: v2 only.
        assert_eq!(negotiate_version(&[V2, V5], &[V2]), Some(V2));
        assert_eq!(negotiate_version(&[V2], &[V2, V5]), Some(V2));

        // Single common version: v5 only.
        assert_eq!(negotiate_version(&[V2, V5], &[V5]), Some(V5));
        assert_eq!(negotiate_version(&[V5], &[V2, V5]), Some(V5));

        // Exact single-version match on both sides.
        assert_eq!(negotiate_version(&[V2], &[V2]), Some(V2));
        assert_eq!(negotiate_version(&[V5], &[V5]), Some(V5));

        // No overlap -> None.
        assert_eq!(negotiate_version(&[V2], &[V5]), None);
        assert_eq!(negotiate_version(&[V5], &[V2]), None);

        // Empty inputs -> None.
        assert_eq!(negotiate_version(&[], &[V2, V5]), None);
        assert_eq!(negotiate_version(&[V2, V5], &[]), None);
        assert_eq!(negotiate_version(&[], &[]), None);
    }

    #[test]
    fn test_negotiate_version_picks_highest_common() {
        // Even if the local list prefers v2 first, the *highest* common version
        // is chosen.
        assert_eq!(
            negotiate_version(
                &[ProtocolVersion::V2, ProtocolVersion::V5],
                &[ProtocolVersion::V5, ProtocolVersion::V2],
            ),
            Some(ProtocolVersion::V5)
        );
    }

    #[test]
    fn test_parse_and_encode_version() {
        assert_eq!(parse_version("2"), Some(ProtocolVersion::V2));
        assert_eq!(parse_version("v2"), Some(ProtocolVersion::V2));
        assert_eq!(parse_version("5"), Some(ProtocolVersion::V5));
        assert_eq!(parse_version("V5"), Some(ProtocolVersion::V5));
        assert_eq!(parse_version("7"), None);
        assert_eq!(parse_version(""), None);

        assert_eq!(encode_version(ProtocolVersion::V2), "2");
        assert_eq!(encode_version(ProtocolVersion::V5), "5");

        // Round-trip.
        for v in [ProtocolVersion::V2, ProtocolVersion::V5] {
            assert_eq!(parse_version(encode_version(v)), Some(v));
        }
    }

    #[test]
    fn test_version_header_round_trip() {
        let mut headers = std::collections::HashMap::new();

        // Absent header -> None.
        assert_eq!(parse_version_from_headers(&headers), None);

        encode_version_into_headers(&mut headers, ProtocolVersion::V5);
        assert_eq!(
            headers.get(PROTOCOL_VERSION_KEY),
            Some(&serde_json::Value::String("5".to_string()))
        );
        assert_eq!(
            parse_version_from_headers(&headers),
            Some(ProtocolVersion::V5)
        );

        // Overwrite with v2.
        encode_version_into_headers(&mut headers, ProtocolVersion::V2);
        assert_eq!(
            parse_version_from_headers(&headers),
            Some(ProtocolVersion::V2)
        );
    }

    #[test]
    fn test_parse_version_from_headers_numeric_and_invalid() {
        // Numeric JSON value is accepted.
        let mut headers = std::collections::HashMap::new();
        headers.insert(
            PROTOCOL_VERSION_KEY.to_string(),
            serde_json::Value::Number(5u8.into()),
        );
        assert_eq!(
            parse_version_from_headers(&headers),
            Some(ProtocolVersion::V5)
        );

        // Unknown numeric value -> None.
        headers.insert(
            PROTOCOL_VERSION_KEY.to_string(),
            serde_json::Value::Number(9u8.into()),
        );
        assert_eq!(parse_version_from_headers(&headers), None);

        // Wrong JSON type -> None.
        headers.insert(
            PROTOCOL_VERSION_KEY.to_string(),
            serde_json::Value::Bool(true),
        );
        assert_eq!(parse_version_from_headers(&headers), None);
    }
}
