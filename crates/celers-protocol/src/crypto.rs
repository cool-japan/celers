//! Message encryption and decryption
//!
//! This module provides AES-256-GCM encryption for Celery protocol messages.
//! It ensures message confidentiality by encrypting message bodies.
//!
//! # Example
//!
//! ```
//! use celers_protocol::crypto::{MessageEncryptor, EncryptionError};
//!
//! # #[cfg(feature = "encryption")]
//! # {
//! let key = b"32-byte-secret-key-for-aes-256!!";
//! let encryptor = MessageEncryptor::new(key).unwrap();
//!
//! let plaintext = b"secret task data";
//! let (ciphertext, nonce) = encryptor.encrypt(plaintext).unwrap();
//!
//! // Decrypt
//! let decrypted = encryptor.decrypt(&ciphertext, &nonce).unwrap();
//! assert_eq!(decrypted, plaintext);
//! # }
//! ```

#[cfg(feature = "encryption")]
use aes_gcm::{
    aead::{Aead, AeadCore, KeyInit, OsRng},
    Aes256Gcm, Nonce,
};

use std::fmt;

/// Error type for encryption operations
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EncryptionError {
    /// Invalid key length (must be 32 bytes for AES-256)
    InvalidKeyLength,
    /// Encryption failed
    EncryptionFailed,
    /// Decryption failed
    DecryptionFailed,
    /// Invalid nonce length (must be 12 bytes for GCM)
    InvalidNonceLength,
}

impl fmt::Display for EncryptionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            EncryptionError::InvalidKeyLength => {
                write!(f, "Invalid key length (must be 32 bytes for AES-256)")
            }
            EncryptionError::EncryptionFailed => write!(f, "Encryption failed"),
            EncryptionError::DecryptionFailed => write!(f, "Decryption failed"),
            EncryptionError::InvalidNonceLength => {
                write!(f, "Invalid nonce length (must be 12 bytes)")
            }
        }
    }
}

impl std::error::Error for EncryptionError {}

/// Nonce size for AES-GCM (96 bits = 12 bytes)
#[cfg(feature = "encryption")]
pub const NONCE_SIZE: usize = 12;

/// Key size for AES-256 (256 bits = 32 bytes)
pub const KEY_SIZE: usize = 32;

/// Message encryptor using AES-256-GCM
///
/// Provides authenticated encryption of messages using AES-256-GCM.
/// GCM (Galois/Counter Mode) provides both confidentiality and authenticity.
#[cfg(feature = "encryption")]
pub struct MessageEncryptor {
    cipher: Aes256Gcm,
}

#[cfg(feature = "encryption")]
impl MessageEncryptor {
    /// Create a new message encryptor with the given key
    ///
    /// # Arguments
    ///
    /// * `key` - 32-byte secret key for AES-256
    ///
    /// # Returns
    ///
    /// `Ok(MessageEncryptor)` if the key is valid, `Err(EncryptionError)` otherwise
    pub fn new(key: &[u8]) -> Result<Self, EncryptionError> {
        if key.len() != KEY_SIZE {
            return Err(EncryptionError::InvalidKeyLength);
        }

        let cipher =
            Aes256Gcm::new_from_slice(key).map_err(|_| EncryptionError::InvalidKeyLength)?;

        Ok(Self { cipher })
    }

    /// Encrypt a message
    ///
    /// # Arguments
    ///
    /// * `plaintext` - The message bytes to encrypt
    ///
    /// # Returns
    ///
    /// A tuple of (ciphertext, nonce) on success, or `EncryptionError` on failure
    pub fn encrypt(&self, plaintext: &[u8]) -> Result<(Vec<u8>, Vec<u8>), EncryptionError> {
        // Generate a random nonce
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);

        // Encrypt
        let ciphertext = self
            .cipher
            .encrypt(&nonce, plaintext)
            .map_err(|_| EncryptionError::EncryptionFailed)?;

        Ok((ciphertext, nonce.to_vec()))
    }

    /// Decrypt a message
    ///
    /// # Arguments
    ///
    /// * `ciphertext` - The encrypted message bytes
    /// * `nonce` - The nonce used during encryption (12 bytes)
    ///
    /// # Returns
    ///
    /// The decrypted plaintext on success, or `EncryptionError` on failure
    pub fn decrypt(&self, ciphertext: &[u8], nonce: &[u8]) -> Result<Vec<u8>, EncryptionError> {
        if nonce.len() != NONCE_SIZE {
            return Err(EncryptionError::InvalidNonceLength);
        }

        let nonce = Nonce::from_slice(nonce);

        let plaintext = self
            .cipher
            .decrypt(nonce, ciphertext)
            .map_err(|_| EncryptionError::DecryptionFailed)?;

        Ok(plaintext)
    }

    /// Encrypt a message and return hex-encoded ciphertext and nonce
    ///
    /// # Arguments
    ///
    /// * `plaintext` - The message bytes to encrypt
    ///
    /// # Returns
    ///
    /// A tuple of (ciphertext_hex, nonce_hex) on success
    pub fn encrypt_hex(&self, plaintext: &[u8]) -> Result<(String, String), EncryptionError> {
        let (ciphertext, nonce) = self.encrypt(plaintext)?;
        Ok((hex::encode(ciphertext), hex::encode(nonce)))
    }

    /// Decrypt a hex-encoded message
    ///
    /// # Arguments
    ///
    /// * `ciphertext_hex` - The hex-encoded ciphertext
    /// * `nonce_hex` - The hex-encoded nonce
    ///
    /// # Returns
    ///
    /// The decrypted plaintext on success, or `EncryptionError` on failure
    pub fn decrypt_hex(
        &self,
        ciphertext_hex: &str,
        nonce_hex: &str,
    ) -> Result<Vec<u8>, EncryptionError> {
        let ciphertext =
            hex::decode(ciphertext_hex).map_err(|_| EncryptionError::DecryptionFailed)?;
        let nonce = hex::decode(nonce_hex).map_err(|_| EncryptionError::InvalidNonceLength)?;

        self.decrypt(&ciphertext, &nonce)
    }
}

// Placeholder implementation when encryption feature is disabled
#[cfg(not(feature = "encryption"))]
pub struct MessageEncryptor;

#[cfg(not(feature = "encryption"))]
impl MessageEncryptor {
    pub fn new(_key: &[u8]) -> Result<Self, EncryptionError> {
        Err(EncryptionError::EncryptionFailed)
    }
}

#[cfg(all(test, feature = "encryption"))]
mod tests {
    use super::*;

    #[test]
    fn test_encrypt_decrypt() {
        let key = b"32-byte-secret-key-for-aes-256!!";
        let encryptor = MessageEncryptor::new(key).unwrap();

        let plaintext = b"secret message";
        let (ciphertext, nonce) = encryptor.encrypt(plaintext).unwrap();

        let decrypted = encryptor.decrypt(&ciphertext, &nonce).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_invalid_key_length() {
        let short_key = b"too-short";
        assert!(MessageEncryptor::new(short_key).is_err());

        let long_key = b"this-key-is-way-too-long-for-aes-256-encryption";
        assert!(MessageEncryptor::new(long_key).is_err());
    }

    #[test]
    fn test_decrypt_wrong_nonce() {
        let key = b"32-byte-secret-key-for-aes-256!!";
        let encryptor = MessageEncryptor::new(key).unwrap();

        let plaintext = b"secret message";
        let (ciphertext, _) = encryptor.encrypt(plaintext).unwrap();

        let wrong_nonce = vec![0u8; NONCE_SIZE];
        assert!(encryptor.decrypt(&ciphertext, &wrong_nonce).is_err());
    }

    #[test]
    fn test_decrypt_tampered_ciphertext() {
        let key = b"32-byte-secret-key-for-aes-256!!";
        let encryptor = MessageEncryptor::new(key).unwrap();

        let plaintext = b"secret message";
        let (mut ciphertext, nonce) = encryptor.encrypt(plaintext).unwrap();

        // Tamper with ciphertext
        if !ciphertext.is_empty() {
            ciphertext[0] ^= 1;
        }

        assert!(encryptor.decrypt(&ciphertext, &nonce).is_err());
    }

    #[test]
    fn test_invalid_nonce_length() {
        let key = b"32-byte-secret-key-for-aes-256!!";
        let encryptor = MessageEncryptor::new(key).unwrap();

        let plaintext = b"secret message";
        let (ciphertext, _) = encryptor.encrypt(plaintext).unwrap();

        let wrong_nonce = vec![0u8; 8]; // Wrong size
        assert_eq!(
            encryptor.decrypt(&ciphertext, &wrong_nonce),
            Err(EncryptionError::InvalidNonceLength)
        );
    }

    #[test]
    fn test_encrypt_hex() {
        let key = b"32-byte-secret-key-for-aes-256!!";
        let encryptor = MessageEncryptor::new(key).unwrap();

        let plaintext = b"secret message";
        let (ciphertext_hex, nonce_hex) = encryptor.encrypt_hex(plaintext).unwrap();

        // Verify hex encoding
        assert!(hex::decode(&ciphertext_hex).is_ok());
        assert!(hex::decode(&nonce_hex).is_ok());

        // Decrypt using hex
        let decrypted = encryptor.decrypt_hex(&ciphertext_hex, &nonce_hex).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_different_nonces() {
        let key = b"32-byte-secret-key-for-aes-256!!";
        let encryptor = MessageEncryptor::new(key).unwrap();

        let plaintext = b"secret message";
        let (_, nonce1) = encryptor.encrypt(plaintext).unwrap();
        let (_, nonce2) = encryptor.encrypt(plaintext).unwrap();

        // Each encryption should use a different nonce
        assert_ne!(nonce1, nonce2);
    }

    #[test]
    fn test_encryption_error_display() {
        assert_eq!(
            EncryptionError::InvalidKeyLength.to_string(),
            "Invalid key length (must be 32 bytes for AES-256)"
        );
        assert_eq!(
            EncryptionError::EncryptionFailed.to_string(),
            "Encryption failed"
        );
        assert_eq!(
            EncryptionError::DecryptionFailed.to_string(),
            "Decryption failed"
        );
        assert_eq!(
            EncryptionError::InvalidNonceLength.to_string(),
            "Invalid nonce length (must be 12 bytes)"
        );
    }

    #[test]
    fn test_empty_message() {
        let key = b"32-byte-secret-key-for-aes-256!!";
        let encryptor = MessageEncryptor::new(key).unwrap();

        let plaintext = b"";
        let (ciphertext, nonce) = encryptor.encrypt(plaintext).unwrap();

        let decrypted = encryptor.decrypt(&ciphertext, &nonce).unwrap();
        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_large_message() {
        let key = b"32-byte-secret-key-for-aes-256!!";
        let encryptor = MessageEncryptor::new(key).unwrap();

        let plaintext = vec![42u8; 10000];
        let (ciphertext, nonce) = encryptor.encrypt(&plaintext).unwrap();

        let decrypted = encryptor.decrypt(&ciphertext, &nonce).unwrap();
        assert_eq!(decrypted, plaintext);
    }
}
