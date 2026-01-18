//! Result encryption for sensitive task data
//!
//! This module provides AES-256-GCM encryption for task results to protect
//! sensitive data at rest in Redis.
//!
//! # Security Features
//!
//! - AES-256-GCM authenticated encryption
//! - Random nonce generation for each encryption
//! - Base64 encoding for storage
//! - Automatic encryption/decryption
//!
//! # Example
//!
//! ```
//! use celers_backend_redis::encryption::{EncryptionConfig, EncryptionKey};
//!
//! // Generate a random encryption key
//! let key = EncryptionKey::generate();
//!
//! // Or use an existing key (32 bytes)
//! let key_bytes = [0u8; 32];
//! let key = EncryptionKey::from_bytes(&key_bytes).unwrap();
//!
//! let config = EncryptionConfig::new(key);
//! ```

use aes_gcm::{
    aead::{Aead, KeyInit, OsRng},
    Aes256Gcm, Nonce,
};
use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use std::fmt;

/// Encryption marker prefix
const ENCRYPTION_MARKER: &[u8] = b"CELERS_ENC:";

/// Nonce size for AES-GCM (96 bits / 12 bytes)
const NONCE_SIZE: usize = 12;

/// Encryption key (256-bit / 32 bytes)
#[derive(Clone)]
pub struct EncryptionKey {
    bytes: [u8; 32],
}

impl EncryptionKey {
    /// Generate a random encryption key
    pub fn generate() -> Self {
        use aes_gcm::aead::rand_core::RngCore;
        let mut bytes = [0u8; 32];
        OsRng.fill_bytes(&mut bytes);
        Self { bytes }
    }

    /// Create a key from 32 bytes
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, EncryptionError> {
        if bytes.len() != 32 {
            return Err(EncryptionError::InvalidKeySize(bytes.len()));
        }
        let mut key_bytes = [0u8; 32];
        key_bytes.copy_from_slice(bytes);
        Ok(Self { bytes: key_bytes })
    }

    /// Create a key from a hex string (64 hex characters = 32 bytes)
    pub fn from_hex(hex: &str) -> Result<Self, EncryptionError> {
        if hex.len() != 64 {
            return Err(EncryptionError::InvalidHexKey(hex.len()));
        }
        let mut bytes = [0u8; 32];
        for i in 0..32 {
            bytes[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
                .map_err(|_| EncryptionError::InvalidHexKey(hex.len()))?;
        }
        Ok(Self { bytes })
    }

    /// Get the key bytes
    pub fn as_bytes(&self) -> &[u8; 32] {
        &self.bytes
    }

    /// Convert to hex string
    pub fn to_hex(&self) -> String {
        hex::encode(self.bytes)
    }
}

impl fmt::Debug for EncryptionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("EncryptionKey([REDACTED])")
    }
}

/// Encryption configuration
#[derive(Debug, Clone)]
pub struct EncryptionConfig {
    /// Encryption key
    key: EncryptionKey,

    /// Enable encryption
    pub enabled: bool,
}

impl EncryptionConfig {
    /// Create new encryption configuration
    pub fn new(key: EncryptionKey) -> Self {
        Self { key, enabled: true }
    }

    /// Create disabled encryption configuration
    pub fn disabled() -> Self {
        Self {
            key: EncryptionKey { bytes: [0u8; 32] },
            enabled: false,
        }
    }

    /// Enable or disable encryption
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Get the encryption key
    pub(crate) fn key(&self) -> &EncryptionKey {
        &self.key
    }
}

/// Encryption errors
#[derive(Debug, thiserror::Error)]
pub enum EncryptionError {
    #[error("Invalid key size: expected 32 bytes, got {0}")]
    InvalidKeySize(usize),

    #[error("Invalid hex key: expected 64 characters, got {0}")]
    InvalidHexKey(usize),

    #[error("Encryption failed: {0}")]
    EncryptionFailed(String),

    #[error("Decryption failed: {0}")]
    DecryptionFailed(String),

    #[error("Invalid encrypted data format")]
    InvalidFormat,

    #[error("Base64 decoding failed: {0}")]
    Base64Error(#[from] base64::DecodeError),
}

/// Encrypt data with AES-256-GCM
///
/// Returns encrypted data with format: ENCRYPTION_MARKER + base64(nonce + ciphertext)
pub fn encrypt(data: &[u8], config: &EncryptionConfig) -> Result<Vec<u8>, EncryptionError> {
    if !config.enabled {
        return Ok(data.to_vec());
    }

    // Create cipher
    let cipher = Aes256Gcm::new(config.key().as_bytes().into());

    // Generate random nonce
    use aes_gcm::aead::rand_core::RngCore;
    let mut nonce_bytes = [0u8; NONCE_SIZE];
    OsRng.fill_bytes(&mut nonce_bytes);
    let nonce = Nonce::from_slice(&nonce_bytes);

    // Encrypt
    let ciphertext = cipher
        .encrypt(nonce, data)
        .map_err(|e| EncryptionError::EncryptionFailed(e.to_string()))?;

    // Combine nonce + ciphertext
    let mut combined = Vec::with_capacity(NONCE_SIZE + ciphertext.len());
    combined.extend_from_slice(&nonce_bytes);
    combined.extend_from_slice(&ciphertext);

    // Base64 encode
    let encoded = BASE64.encode(&combined);

    // Add marker prefix
    let mut result = Vec::with_capacity(ENCRYPTION_MARKER.len() + encoded.len());
    result.extend_from_slice(ENCRYPTION_MARKER);
    result.extend_from_slice(encoded.as_bytes());

    Ok(result)
}

/// Decrypt data with AES-256-GCM
///
/// Automatically detects if data is encrypted and decrypts it.
/// Returns the original data if not encrypted.
pub fn decrypt(data: &[u8], config: &EncryptionConfig) -> Result<Vec<u8>, EncryptionError> {
    // Check for encryption marker
    if data.len() < ENCRYPTION_MARKER.len() || &data[..ENCRYPTION_MARKER.len()] != ENCRYPTION_MARKER
    {
        return Ok(data.to_vec());
    }

    if !config.enabled {
        return Err(EncryptionError::DecryptionFailed(
            "Encryption is disabled but data is encrypted".to_string(),
        ));
    }

    // Remove marker and base64 decode
    let encoded = &data[ENCRYPTION_MARKER.len()..];
    let combined = BASE64.decode(encoded)?;

    // Separate nonce and ciphertext
    if combined.len() < NONCE_SIZE {
        return Err(EncryptionError::InvalidFormat);
    }

    let nonce = Nonce::from_slice(&combined[..NONCE_SIZE]);
    let ciphertext = &combined[NONCE_SIZE..];

    // Create cipher and decrypt
    let cipher = Aes256Gcm::new(config.key().as_bytes().into());
    let plaintext = cipher
        .decrypt(nonce, ciphertext)
        .map_err(|e| EncryptionError::DecryptionFailed(e.to_string()))?;

    Ok(plaintext)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encryption_key_generate() {
        let key1 = EncryptionKey::generate();
        let key2 = EncryptionKey::generate();

        // Keys should be different
        assert_ne!(key1.as_bytes(), key2.as_bytes());
    }

    #[test]
    fn test_encryption_key_from_bytes() {
        let bytes = [42u8; 32];
        let key = EncryptionKey::from_bytes(&bytes).unwrap();
        assert_eq!(key.as_bytes(), &bytes);

        // Wrong size should fail
        let result = EncryptionKey::from_bytes(&[0u8; 16]);
        assert!(result.is_err());
    }

    #[test]
    fn test_encryption_key_hex() {
        let key = EncryptionKey::generate();
        let hex = key.to_hex();
        assert_eq!(hex.len(), 64);

        let key2 = EncryptionKey::from_hex(&hex).unwrap();
        assert_eq!(key.as_bytes(), key2.as_bytes());
    }

    #[test]
    fn test_encryption_config() {
        let key = EncryptionKey::generate();
        let config = EncryptionConfig::new(key);
        assert!(config.enabled);

        let config = EncryptionConfig::disabled();
        assert!(!config.enabled);
    }

    #[test]
    fn test_encrypt_decrypt() {
        let key = EncryptionKey::generate();
        let config = EncryptionConfig::new(key);

        let data = b"sensitive information";
        let encrypted = encrypt(data, &config).unwrap();

        // Should be different from original
        assert_ne!(encrypted, data);

        // Should have encryption marker
        assert!(encrypted.starts_with(ENCRYPTION_MARKER));

        // Decrypt
        let decrypted = decrypt(&encrypted, &config).unwrap();
        assert_eq!(decrypted, data);
    }

    #[test]
    fn test_encrypt_disabled() {
        let config = EncryptionConfig::disabled();
        let data = b"test data";

        let result = encrypt(data, &config).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_decrypt_unencrypted() {
        let key = EncryptionKey::generate();
        let config = EncryptionConfig::new(key);

        let data = b"unencrypted data";
        let decrypted = decrypt(data, &config).unwrap();
        assert_eq!(decrypted, data);
    }

    #[test]
    fn test_decrypt_wrong_key() {
        let key1 = EncryptionKey::generate();
        let key2 = EncryptionKey::generate();

        let config1 = EncryptionConfig::new(key1);
        let config2 = EncryptionConfig::new(key2);

        let data = b"secret";
        let encrypted = encrypt(data, &config1).unwrap();

        // Decrypting with wrong key should fail
        let result = decrypt(&encrypted, &config2);
        assert!(result.is_err());
    }

    #[test]
    fn test_encrypt_different_nonces() {
        let key = EncryptionKey::generate();
        let config = EncryptionConfig::new(key);

        let data = b"same data";
        let encrypted1 = encrypt(data, &config).unwrap();
        let encrypted2 = encrypt(data, &config).unwrap();

        // Should produce different ciphertexts due to random nonces
        assert_ne!(encrypted1, encrypted2);

        // But both should decrypt to same plaintext
        let decrypted1 = decrypt(&encrypted1, &config).unwrap();
        let decrypted2 = decrypt(&encrypted2, &config).unwrap();
        assert_eq!(decrypted1, data);
        assert_eq!(decrypted2, data);
    }

    #[test]
    fn test_encryption_key_debug() {
        let key = EncryptionKey::generate();
        let debug_str = format!("{:?}", key);
        assert!(debug_str.contains("REDACTED"));
        assert!(!debug_str.contains(&key.to_hex()));
    }
}
