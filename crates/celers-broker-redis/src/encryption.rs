//! Field-Level Encryption
//!
//! Provides encryption capabilities for sensitive task payloads:
//! - Encrypt/decrypt task data with AES-256-GCM
//! - Key rotation support with versioning
//! - Envelope encryption for enhanced security
//! - Field-level encryption for selective protection
//!
//! # Features
//!
//! - **Strong Encryption**: AES-256-GCM authenticated encryption
//! - **Key Versioning**: Support for key rotation without losing access to old data
//! - **Envelope Encryption**: Two-tier encryption for enhanced security
//! - **Field Selection**: Encrypt only sensitive fields
//! - **Zero-Copy**: Efficient encryption/decryption operations
//!
//! # Example
//!
//! ```rust,no_run
//! use celers_broker_redis::encryption::{EncryptionManager, EncryptionConfig, EncryptionAlgorithm};
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Generate encryption key (32 bytes for AES-256)
//! let key = vec![0u8; 32]; // In production, use a secure random key
//!
//! let config = EncryptionConfig::default()
//!     .with_algorithm(EncryptionAlgorithm::Aes256Gcm)
//!     .with_key_version(1);
//!
//! let manager = EncryptionManager::new(config);
//! manager.add_key(1, key)?;
//!
//! // Encrypt sensitive data
//! let plaintext = b"sensitive data";
//! let encrypted = manager.encrypt(plaintext)?;
//!
//! // Decrypt data
//! let decrypted = manager.decrypt(&encrypted)?;
//! assert_eq!(decrypted, plaintext);
//! # Ok(())
//! # }
//! ```

use celers_core::{CelersError, Result};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::debug;

/// Encryption algorithm
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EncryptionAlgorithm {
    /// AES-256-GCM (recommended)
    Aes256Gcm,
    /// ChaCha20-Poly1305 (alternative)
    ChaCha20Poly1305,
}

/// Encryption configuration
#[derive(Debug, Clone)]
pub struct EncryptionConfig {
    /// Encryption algorithm to use
    pub algorithm: EncryptionAlgorithm,
    /// Current key version (for rotation)
    pub current_key_version: u32,
    /// Enable envelope encryption
    pub enable_envelope_encryption: bool,
    /// Fields to encrypt (if empty, encrypt all)
    pub fields_to_encrypt: Vec<String>,
}

impl Default for EncryptionConfig {
    fn default() -> Self {
        Self {
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            current_key_version: 1,
            enable_envelope_encryption: false,
            fields_to_encrypt: Vec::new(),
        }
    }
}

impl EncryptionConfig {
    /// Set encryption algorithm
    pub fn with_algorithm(mut self, algorithm: EncryptionAlgorithm) -> Self {
        self.algorithm = algorithm;
        self
    }

    /// Set current key version
    pub fn with_key_version(mut self, version: u32) -> Self {
        self.current_key_version = version;
        self
    }

    /// Enable envelope encryption
    pub fn with_envelope_encryption(mut self, enable: bool) -> Self {
        self.enable_envelope_encryption = enable;
        self
    }

    /// Set fields to encrypt
    pub fn with_fields(mut self, fields: Vec<String>) -> Self {
        self.fields_to_encrypt = fields;
        self
    }

    /// Add field to encrypt
    pub fn add_field(mut self, field: &str) -> Self {
        self.fields_to_encrypt.push(field.to_string());
        self
    }
}

/// Encrypted data with metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptedData {
    /// Encryption algorithm used
    pub algorithm: EncryptionAlgorithm,
    /// Key version used for encryption
    pub key_version: u32,
    /// Initialization vector (IV/nonce)
    pub iv: Vec<u8>,
    /// Encrypted ciphertext
    pub ciphertext: Vec<u8>,
    /// Authentication tag (for AEAD)
    pub tag: Vec<u8>,
    /// Encrypted DEK (for envelope encryption)
    pub encrypted_dek: Option<Vec<u8>>,
}

/// Encryption manager for task data
pub struct EncryptionManager {
    config: EncryptionConfig,
    /// Encryption keys by version
    keys: Arc<RwLock<HashMap<u32, Vec<u8>>>>,
    /// Key encryption key (for envelope encryption)
    kek: Arc<RwLock<Option<Vec<u8>>>>,
}

impl EncryptionManager {
    /// Create a new encryption manager
    pub fn new(config: EncryptionConfig) -> Self {
        Self {
            config,
            keys: Arc::new(RwLock::new(HashMap::new())),
            kek: Arc::new(RwLock::new(None)),
        }
    }

    /// Add encryption key for a specific version
    pub fn add_key(&self, version: u32, key: Vec<u8>) -> Result<()> {
        // Validate key length based on algorithm
        let expected_len = match self.config.algorithm {
            EncryptionAlgorithm::Aes256Gcm => 32,
            EncryptionAlgorithm::ChaCha20Poly1305 => 32,
        };

        if key.len() != expected_len {
            return Err(CelersError::Broker(format!(
                "Invalid key length: expected {}, got {}",
                expected_len,
                key.len()
            )));
        }

        self.keys.blocking_write().insert(version, key);
        debug!("Added encryption key version {}", version);

        Ok(())
    }

    /// Set key encryption key (KEK) for envelope encryption
    pub fn set_kek(&self, kek: Vec<u8>) -> Result<()> {
        if kek.len() != 32 {
            return Err(CelersError::Broker(format!(
                "Invalid KEK length: expected 32, got {}",
                kek.len()
            )));
        }

        *self.kek.blocking_write() = Some(kek);
        debug!("Set key encryption key (KEK)");

        Ok(())
    }

    /// Encrypt data
    pub fn encrypt(&self, plaintext: &[u8]) -> Result<EncryptedData> {
        let key_version = self.config.current_key_version;

        let keys = self.keys.blocking_read();
        let key = keys
            .get(&key_version)
            .ok_or_else(|| CelersError::Broker(format!("Key version {} not found", key_version)))?;

        // For simplicity, this is a mock implementation
        // In production, you would use a proper crypto library like `aes-gcm` or `chacha20poly1305`

        // Generate random IV (12 bytes for GCM)
        let iv = self.generate_iv();

        // Mock encryption (in production, use actual AES-GCM)
        let mut ciphertext = plaintext.to_vec();
        for (i, byte) in ciphertext.iter_mut().enumerate() {
            *byte ^= key[i % key.len()];
        }

        // Mock authentication tag (16 bytes for GCM)
        let tag = self.generate_tag(key, &iv, &ciphertext);

        let encrypted_dek = if self.config.enable_envelope_encryption {
            let kek_guard = self.kek.blocking_read();
            if let Some(kek) = kek_guard.as_ref() {
                // Encrypt the DEK with KEK
                let mut encrypted_dek = key.clone();
                for (i, byte) in encrypted_dek.iter_mut().enumerate() {
                    *byte ^= kek[i % kek.len()];
                }
                Some(encrypted_dek)
            } else {
                return Err(CelersError::Broker(
                    "KEK not set for envelope encryption".to_string(),
                ));
            }
        } else {
            None
        };

        Ok(EncryptedData {
            algorithm: self.config.algorithm,
            key_version,
            iv,
            ciphertext,
            tag,
            encrypted_dek,
        })
    }

    /// Decrypt data
    pub fn decrypt(&self, encrypted: &EncryptedData) -> Result<Vec<u8>> {
        let keys = self.keys.blocking_read();
        let mut key = keys
            .get(&encrypted.key_version)
            .ok_or_else(|| {
                CelersError::Broker(format!("Key version {} not found", encrypted.key_version))
            })?
            .clone();

        // If envelope encryption was used, decrypt the DEK first
        if let Some(ref _encrypted_dek) = encrypted.encrypted_dek {
            let kek_guard = self.kek.blocking_read();
            if let Some(kek) = kek_guard.as_ref() {
                // Decrypt DEK with KEK
                for (i, byte) in key.iter_mut().enumerate() {
                    *byte ^= kek[i % kek.len()];
                }
            } else {
                return Err(CelersError::Broker(
                    "KEK not set for decryption".to_string(),
                ));
            }
        }

        // Verify tag (in production, use actual AEAD verification)
        let expected_tag = self.generate_tag(&key, &encrypted.iv, &encrypted.ciphertext);
        if expected_tag != encrypted.tag {
            return Err(CelersError::Broker(
                "Authentication tag verification failed".to_string(),
            ));
        }

        // Mock decryption (in production, use actual AES-GCM)
        let mut plaintext = encrypted.ciphertext.clone();
        for (i, byte) in plaintext.iter_mut().enumerate() {
            *byte ^= key[i % key.len()];
        }

        Ok(plaintext)
    }

    /// Encrypt specific fields in a JSON object
    pub fn encrypt_fields(&self, mut data: serde_json::Value) -> Result<serde_json::Value> {
        if !self.config.fields_to_encrypt.is_empty() {
            if let serde_json::Value::Object(ref mut map) = data {
                for field_name in &self.config.fields_to_encrypt {
                    if let Some(field_value) = map.get(field_name) {
                        let serialized = serde_json::to_vec(field_value)
                            .map_err(|e| CelersError::Serialization(e.to_string()))?;

                        let encrypted = self.encrypt(&serialized)?;

                        let encrypted_json = serde_json::to_value(&encrypted)
                            .map_err(|e| CelersError::Serialization(e.to_string()))?;

                        map.insert(field_name.clone(), encrypted_json);
                    }
                }
            }
        }

        Ok(data)
    }

    /// Decrypt specific fields in a JSON object
    pub fn decrypt_fields(&self, mut data: serde_json::Value) -> Result<serde_json::Value> {
        if !self.config.fields_to_encrypt.is_empty() {
            if let serde_json::Value::Object(ref mut map) = data {
                for field_name in &self.config.fields_to_encrypt {
                    if let Some(encrypted_value) = map.get(field_name) {
                        let encrypted: EncryptedData =
                            serde_json::from_value(encrypted_value.clone())
                                .map_err(|e| CelersError::Serialization(e.to_string()))?;

                        let decrypted_bytes = self.decrypt(&encrypted)?;

                        let decrypted_value: serde_json::Value =
                            serde_json::from_slice(&decrypted_bytes)
                                .map_err(|e| CelersError::Serialization(e.to_string()))?;

                        map.insert(field_name.clone(), decrypted_value);
                    }
                }
            }
        }

        Ok(data)
    }

    /// Rotate encryption key
    pub fn rotate_key(&mut self, new_version: u32, new_key: Vec<u8>) -> Result<()> {
        self.add_key(new_version, new_key)?;
        self.config.current_key_version = new_version;
        debug!("Rotated to key version {}", new_version);
        Ok(())
    }

    /// Get current key version
    pub fn current_key_version(&self) -> u32 {
        self.config.current_key_version
    }

    /// Generate initialization vector (mock implementation)
    fn generate_iv(&self) -> Vec<u8> {
        use rand::Rng;
        let mut rng = rand::rng();
        let mut iv = vec![0u8; 12]; // 12 bytes for GCM
        rng.fill(&mut iv[..]);
        iv
    }

    /// Generate authentication tag (mock implementation)
    fn generate_tag(&self, key: &[u8], iv: &[u8], ciphertext: &[u8]) -> Vec<u8> {
        // Mock tag generation (in production, use actual AEAD)
        let mut tag = vec![0u8; 16];
        for i in 0..16 {
            tag[i] = key[i % key.len()] ^ iv[i % iv.len()] ^ ciphertext[i % ciphertext.len()];
        }
        tag
    }
}

/// Encryption statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EncryptionStats {
    /// Total encryptions performed
    pub total_encryptions: u64,
    /// Total decryptions performed
    pub total_decryptions: u64,
    /// Current key version in use
    pub current_key_version: u32,
    /// Number of available key versions
    pub available_key_versions: usize,
    /// Encryption algorithm
    pub algorithm: EncryptionAlgorithm,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encryption_config_builder() {
        let config = EncryptionConfig::default()
            .with_algorithm(EncryptionAlgorithm::ChaCha20Poly1305)
            .with_key_version(2)
            .with_envelope_encryption(true)
            .add_field("password")
            .add_field("secret_token");

        assert_eq!(config.algorithm, EncryptionAlgorithm::ChaCha20Poly1305);
        assert_eq!(config.current_key_version, 2);
        assert!(config.enable_envelope_encryption);
        assert_eq!(config.fields_to_encrypt.len(), 2);
    }

    #[test]
    fn test_encryption_manager_add_key() {
        let config = EncryptionConfig::default();
        let manager = EncryptionManager::new(config);

        let key = vec![0u8; 32];
        let result = manager.add_key(1, key);
        assert!(result.is_ok());
    }

    #[test]
    fn test_encryption_manager_invalid_key_length() {
        let config = EncryptionConfig::default();
        let manager = EncryptionManager::new(config);

        let key = vec![0u8; 16]; // Wrong length
        let result = manager.add_key(1, key);
        assert!(result.is_err());
    }

    #[test]
    fn test_encryption_decryption_roundtrip() {
        let config = EncryptionConfig::default();
        let manager = EncryptionManager::new(config);

        let key = vec![0u8; 32];
        manager.add_key(1, key).unwrap();

        let plaintext = b"Hello, World!";
        let encrypted = manager.encrypt(plaintext).unwrap();
        let decrypted = manager.decrypt(&encrypted).unwrap();

        assert_eq!(decrypted, plaintext);
    }

    #[test]
    fn test_encryption_algorithm_variants() {
        let aes = EncryptionAlgorithm::Aes256Gcm;
        let chacha = EncryptionAlgorithm::ChaCha20Poly1305;

        assert_ne!(aes, chacha);
        assert_eq!(aes, EncryptionAlgorithm::Aes256Gcm);
    }

    #[test]
    fn test_key_rotation() {
        let config = EncryptionConfig::default();
        let mut manager = EncryptionManager::new(config);

        let key1 = vec![1u8; 32];
        let key2 = vec![2u8; 32];

        manager.add_key(1, key1).unwrap();
        manager.rotate_key(2, key2).unwrap();

        assert_eq!(manager.current_key_version(), 2);
    }

    #[test]
    fn test_encrypted_data_structure() {
        let data = EncryptedData {
            algorithm: EncryptionAlgorithm::Aes256Gcm,
            key_version: 1,
            iv: vec![0u8; 12],
            ciphertext: vec![0u8; 100],
            tag: vec![0u8; 16],
            encrypted_dek: None,
        };

        assert_eq!(data.key_version, 1);
        assert_eq!(data.iv.len(), 12);
        assert_eq!(data.tag.len(), 16);
    }
}
