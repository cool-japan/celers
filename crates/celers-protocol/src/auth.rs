//! Message authentication and signing
//!
//! This module provides HMAC-based message signing for Celery protocol messages.
//! It ensures message integrity and authenticity by generating and verifying
//! cryptographic signatures.
//!
//! # Example
//!
//! ```
//! use celers_protocol::auth::{MessageSigner, SignatureError};
//!
//! # #[cfg(feature = "signing")]
//! # {
//! let secret = b"my-secret-key";
//! let signer = MessageSigner::new(secret);
//!
//! let message = b"task message body";
//! let signature = signer.sign(message);
//!
//! // Verify signature
//! assert!(signer.verify(message, &signature).is_ok());
//! # }
//! ```

#[cfg(feature = "signing")]
use hmac::{Hmac, Mac};
#[cfg(feature = "signing")]
use sha2::Sha256;

use std::fmt;

/// Error type for signature operations
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignatureError {
    /// Invalid signature
    InvalidSignature,
    /// Signature verification failed
    VerificationFailed,
    /// Invalid key length
    InvalidKeyLength,
}

impl fmt::Display for SignatureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SignatureError::InvalidSignature => write!(f, "Invalid signature format"),
            SignatureError::VerificationFailed => write!(f, "Signature verification failed"),
            SignatureError::InvalidKeyLength => write!(f, "Invalid key length"),
        }
    }
}

impl std::error::Error for SignatureError {}

#[cfg(feature = "signing")]
type HmacSha256 = Hmac<Sha256>;

/// Message signer using HMAC-SHA256
///
/// Provides cryptographic signing and verification of messages using HMAC-SHA256.
/// This is compatible with Python Celery's message signing when using SHA256.
#[cfg(feature = "signing")]
pub struct MessageSigner {
    key: Vec<u8>,
}

#[cfg(feature = "signing")]
impl MessageSigner {
    /// Create a new message signer with the given secret key
    ///
    /// # Arguments
    ///
    /// * `key` - Secret key for HMAC signing (recommended: at least 32 bytes)
    pub fn new(key: &[u8]) -> Self {
        Self { key: key.to_vec() }
    }

    /// Sign a message and return the signature
    ///
    /// # Arguments
    ///
    /// * `message` - The message bytes to sign
    ///
    /// # Returns
    ///
    /// The HMAC-SHA256 signature as a byte vector
    pub fn sign(&self, message: &[u8]) -> Vec<u8> {
        let mut mac = HmacSha256::new_from_slice(&self.key).expect("HMAC can take key of any size");
        mac.update(message);
        mac.finalize().into_bytes().to_vec()
    }

    /// Verify a message signature
    ///
    /// # Arguments
    ///
    /// * `message` - The message bytes that were signed
    /// * `signature` - The signature to verify
    ///
    /// # Returns
    ///
    /// `Ok(())` if the signature is valid, `Err(SignatureError)` otherwise
    pub fn verify(&self, message: &[u8], signature: &[u8]) -> Result<(), SignatureError> {
        let mut mac = HmacSha256::new_from_slice(&self.key).expect("HMAC can take key of any size");
        mac.update(message);

        mac.verify_slice(signature)
            .map_err(|_| SignatureError::VerificationFailed)
    }

    /// Sign a message and return the signature as a hex string
    ///
    /// # Arguments
    ///
    /// * `message` - The message bytes to sign
    ///
    /// # Returns
    ///
    /// The signature encoded as a lowercase hex string
    pub fn sign_hex(&self, message: &[u8]) -> String {
        let sig = self.sign(message);
        hex::encode(sig)
    }

    /// Verify a hex-encoded signature
    ///
    /// # Arguments
    ///
    /// * `message` - The message bytes that were signed
    /// * `signature_hex` - The hex-encoded signature to verify
    ///
    /// # Returns
    ///
    /// `Ok(())` if the signature is valid, `Err(SignatureError)` otherwise
    pub fn verify_hex(&self, message: &[u8], signature_hex: &str) -> Result<(), SignatureError> {
        let signature = hex::decode(signature_hex).map_err(|_| SignatureError::InvalidSignature)?;
        self.verify(message, &signature)
    }
}

// Placeholder implementation when signing feature is disabled
#[cfg(not(feature = "signing"))]
pub struct MessageSigner;

#[cfg(not(feature = "signing"))]
impl MessageSigner {
    pub fn new(_key: &[u8]) -> Self {
        Self
    }
}

#[cfg(all(test, feature = "signing"))]
mod tests {
    use super::*;

    #[test]
    fn test_sign_and_verify() {
        let secret = b"my-secret-key";
        let signer = MessageSigner::new(secret);

        let message = b"test message";
        let signature = signer.sign(message);

        assert!(signer.verify(message, &signature).is_ok());
    }

    #[test]
    fn test_verify_invalid_signature() {
        let secret = b"my-secret-key";
        let signer = MessageSigner::new(secret);

        let message = b"test message";
        let wrong_signature = vec![0u8; 32];

        assert!(signer.verify(message, &wrong_signature).is_err());
    }

    #[test]
    fn test_verify_wrong_message() {
        let secret = b"my-secret-key";
        let signer = MessageSigner::new(secret);

        let message = b"test message";
        let signature = signer.sign(message);

        let wrong_message = b"different message";
        assert!(signer.verify(wrong_message, &signature).is_err());
    }

    #[test]
    fn test_sign_hex() {
        let secret = b"my-secret-key";
        let signer = MessageSigner::new(secret);

        let message = b"test message";
        let signature_hex = signer.sign_hex(message);

        // Verify it's valid hex
        assert!(hex::decode(&signature_hex).is_ok());

        // Verify using hex verification
        assert!(signer.verify_hex(message, &signature_hex).is_ok());
    }

    #[test]
    fn test_verify_hex_invalid() {
        let secret = b"my-secret-key";
        let signer = MessageSigner::new(secret);

        let message = b"test message";
        let invalid_hex = "not-valid-hex!!";

        assert_eq!(
            signer.verify_hex(message, invalid_hex),
            Err(SignatureError::InvalidSignature)
        );
    }

    #[test]
    fn test_signature_deterministic() {
        let secret = b"my-secret-key";
        let signer = MessageSigner::new(secret);

        let message = b"test message";
        let sig1 = signer.sign(message);
        let sig2 = signer.sign(message);

        assert_eq!(sig1, sig2);
    }

    #[test]
    fn test_different_keys_different_signatures() {
        let message = b"test message";

        let signer1 = MessageSigner::new(b"key1");
        let signer2 = MessageSigner::new(b"key2");

        let sig1 = signer1.sign(message);
        let sig2 = signer2.sign(message);

        assert_ne!(sig1, sig2);
    }

    #[test]
    fn test_signature_error_display() {
        assert_eq!(
            SignatureError::InvalidSignature.to_string(),
            "Invalid signature format"
        );
        assert_eq!(
            SignatureError::VerificationFailed.to_string(),
            "Signature verification failed"
        );
        assert_eq!(
            SignatureError::InvalidKeyLength.to_string(),
            "Invalid key length"
        );
    }
}
