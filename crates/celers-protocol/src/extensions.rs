//! Message extensions and utilities
//!
//! This module provides helper functions and extensions for working with
//! Celery protocol messages, including signing, encryption, and validation.
//!
//! # Example
//!
//! ```
//! use celers_protocol::extensions::MessageExt;
//! use celers_protocol::{Message, TaskArgs};
//! use uuid::Uuid;
//!
//! let task_id = Uuid::new_v4();
//! let body = serde_json::to_vec(&TaskArgs::new()).unwrap();
//! let msg = Message::new("tasks.add".to_string(), task_id, body);
//!
//! // Validate the message
//! assert!(msg.validate_basic().is_ok());
//! ```

use crate::Message;

#[cfg(feature = "signing")]
use crate::auth::{MessageSigner, SignatureError};

#[cfg(feature = "encryption")]
use crate::crypto::{EncryptionError, MessageEncryptor};

use std::fmt;

/// Error type for message extension operations
#[derive(Debug)]
pub enum ExtensionError {
    /// Signature error
    #[cfg(feature = "signing")]
    Signature(SignatureError),
    /// Encryption error
    #[cfg(feature = "encryption")]
    Encryption(EncryptionError),
    /// Validation error
    Validation(String),
    /// Serialization error
    Serialization(String),
}

impl fmt::Display for ExtensionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            #[cfg(feature = "signing")]
            ExtensionError::Signature(e) => write!(f, "Signature error: {}", e),
            #[cfg(feature = "encryption")]
            ExtensionError::Encryption(e) => write!(f, "Encryption error: {}", e),
            ExtensionError::Validation(msg) => write!(f, "Validation error: {}", msg),
            ExtensionError::Serialization(msg) => write!(f, "Serialization error: {}", msg),
        }
    }
}

impl std::error::Error for ExtensionError {}

#[cfg(feature = "signing")]
impl From<SignatureError> for ExtensionError {
    fn from(e: SignatureError) -> Self {
        ExtensionError::Signature(e)
    }
}

#[cfg(feature = "encryption")]
impl From<EncryptionError> for ExtensionError {
    fn from(e: EncryptionError) -> Self {
        ExtensionError::Encryption(e)
    }
}

/// Extension trait for Message with additional utilities
pub trait MessageExt {
    /// Validate basic message structure
    fn validate_basic(&self) -> Result<(), ExtensionError>;

    /// Check if message is expired
    fn is_expired(&self) -> bool;

    /// Check if message is scheduled for future execution
    fn is_scheduled(&self) -> bool;

    /// Get message age in seconds
    fn get_age_seconds(&self) -> Option<i64>;

    /// Sign the message body
    #[cfg(feature = "signing")]
    fn sign_body(&self, signer: &MessageSigner) -> Vec<u8>;

    /// Verify the message body signature
    #[cfg(feature = "signing")]
    fn verify_body(&self, signer: &MessageSigner, signature: &[u8]) -> Result<(), ExtensionError>;

    /// Encrypt the message body
    #[cfg(feature = "encryption")]
    fn encrypt_body(&mut self, encryptor: &MessageEncryptor) -> Result<Vec<u8>, ExtensionError>;

    /// Decrypt the message body
    #[cfg(feature = "encryption")]
    fn decrypt_body(
        &self,
        encryptor: &MessageEncryptor,
        nonce: &[u8],
    ) -> Result<Vec<u8>, ExtensionError>;
}

impl MessageExt for Message {
    fn validate_basic(&self) -> Result<(), ExtensionError> {
        self.validate().map_err(ExtensionError::Validation)
    }

    fn is_expired(&self) -> bool {
        if let Some(expires) = self.headers.expires {
            chrono::Utc::now() > expires
        } else {
            false
        }
    }

    fn is_scheduled(&self) -> bool {
        if let Some(eta) = self.headers.eta {
            chrono::Utc::now() < eta
        } else {
            false
        }
    }

    fn get_age_seconds(&self) -> Option<i64> {
        // In a real implementation, you'd track message creation time
        // For now, return None as we don't store creation timestamp
        None
    }

    #[cfg(feature = "signing")]
    fn sign_body(&self, signer: &MessageSigner) -> Vec<u8> {
        signer.sign(&self.body)
    }

    #[cfg(feature = "signing")]
    fn verify_body(&self, signer: &MessageSigner, signature: &[u8]) -> Result<(), ExtensionError> {
        signer.verify(&self.body, signature)?;
        Ok(())
    }

    #[cfg(feature = "encryption")]
    fn encrypt_body(&mut self, encryptor: &MessageEncryptor) -> Result<Vec<u8>, ExtensionError> {
        let (ciphertext, nonce) = encryptor.encrypt(&self.body)?;
        self.body = ciphertext;
        Ok(nonce)
    }

    #[cfg(feature = "encryption")]
    fn decrypt_body(
        &self,
        encryptor: &MessageEncryptor,
        nonce: &[u8],
    ) -> Result<Vec<u8>, ExtensionError> {
        let plaintext = encryptor.decrypt(&self.body, nonce)?;
        Ok(plaintext)
    }
}

/// Signed message wrapper
#[cfg(feature = "signing")]
#[derive(Debug, Clone)]
pub struct SignedMessage {
    /// The message
    pub message: Message,
    /// The signature
    pub signature: Vec<u8>,
}

#[cfg(feature = "signing")]
impl SignedMessage {
    /// Create a new signed message
    pub fn new(message: Message, signer: &MessageSigner) -> Self {
        let signature = message.sign_body(signer);
        Self { message, signature }
    }

    /// Verify the signature
    pub fn verify(&self, signer: &MessageSigner) -> Result<(), ExtensionError> {
        self.message.verify_body(signer, &self.signature)
    }

    /// Get the signature as hex string
    pub fn signature_hex(&self) -> String {
        hex::encode(&self.signature)
    }
}

/// Encrypted message wrapper
#[cfg(feature = "encryption")]
#[derive(Debug, Clone)]
pub struct EncryptedMessage {
    /// The encrypted message
    pub message: Message,
    /// The nonce used for encryption
    pub nonce: Vec<u8>,
}

#[cfg(feature = "encryption")]
impl EncryptedMessage {
    /// Create a new encrypted message
    pub fn new(mut message: Message, encryptor: &MessageEncryptor) -> Result<Self, ExtensionError> {
        let nonce = message.encrypt_body(encryptor)?;
        Ok(Self { message, nonce })
    }

    /// Decrypt the message body
    pub fn decrypt(&self, encryptor: &MessageEncryptor) -> Result<Vec<u8>, ExtensionError> {
        self.message.decrypt_body(encryptor, &self.nonce)
    }

    /// Get the nonce as hex string
    pub fn nonce_hex(&self) -> String {
        hex::encode(&self.nonce)
    }
}

/// Result type for build_secure method
#[cfg(all(feature = "signing", feature = "encryption"))]
pub type SecureBuildResult = Result<(Message, Option<Vec<u8>>, Option<Vec<u8>>), ExtensionError>;

/// Message builder with security features
pub struct SecureMessageBuilder {
    message: Message,
    #[cfg(feature = "signing")]
    signer: Option<MessageSigner>,
    #[cfg(feature = "encryption")]
    encryptor: Option<MessageEncryptor>,
}

impl SecureMessageBuilder {
    /// Create a new secure message builder
    pub fn new(task: String, id: uuid::Uuid, body: Vec<u8>) -> Self {
        Self {
            message: Message::new(task, id, body),
            #[cfg(feature = "signing")]
            signer: None,
            #[cfg(feature = "encryption")]
            encryptor: None,
        }
    }

    /// Set the message signer
    #[cfg(feature = "signing")]
    pub fn with_signer(mut self, key: &[u8]) -> Self {
        self.signer = Some(MessageSigner::new(key));
        self
    }

    /// Set the message encryptor
    #[cfg(feature = "encryption")]
    pub fn with_encryptor(mut self, key: &[u8]) -> Result<Self, ExtensionError> {
        self.encryptor = Some(MessageEncryptor::new(key)?);
        Ok(self)
    }

    /// Set priority
    pub fn with_priority(mut self, priority: u8) -> Self {
        self.message = self.message.with_priority(priority);
        self
    }

    /// Build the message with optional signing
    #[cfg(feature = "signing")]
    #[cfg(not(feature = "encryption"))]
    pub fn build(self) -> Result<(Message, Option<Vec<u8>>), ExtensionError> {
        let signature = self.signer.as_ref().map(|s| self.message.sign_body(s));
        Ok((self.message, signature))
    }

    /// Build the message with optional encryption
    #[cfg(feature = "encryption")]
    #[cfg(not(feature = "signing"))]
    pub fn build(mut self) -> Result<(Message, Option<Vec<u8>>), ExtensionError> {
        let nonce = if let Some(enc) = self.encryptor.as_ref() {
            Some(self.message.encrypt_body(enc)?)
        } else {
            None
        };
        Ok((self.message, nonce))
    }

    /// Build with both signing and encryption
    #[cfg(all(feature = "signing", feature = "encryption"))]
    pub fn build_secure(mut self) -> SecureBuildResult {
        let signature = self.signer.as_ref().map(|s| self.message.sign_body(s));
        let nonce = if let Some(enc) = self.encryptor.as_ref() {
            Some(self.message.encrypt_body(enc)?)
        } else {
            None
        };
        Ok((self.message, signature, nonce))
    }

    /// Build without security features
    #[cfg(not(any(feature = "signing", feature = "encryption")))]
    pub fn build(self) -> Message {
        self.message
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TaskArgs;
    use uuid::Uuid;

    #[test]
    fn test_message_validate_basic() {
        let task_id = Uuid::new_v4();
        let body = serde_json::to_vec(&TaskArgs::new()).unwrap();
        let msg = Message::new("tasks.add".to_string(), task_id, body);

        assert!(msg.validate_basic().is_ok());
    }

    #[test]
    fn test_message_is_expired() {
        let task_id = Uuid::new_v4();
        let body = vec![1, 2, 3];
        let mut msg = Message::new("tasks.test".to_string(), task_id, body);

        // Not expired initially
        assert!(!msg.is_expired());

        // Set expiration in the past
        msg.headers.expires = Some(chrono::Utc::now() - chrono::Duration::hours(1));
        assert!(msg.is_expired());

        // Set expiration in the future
        msg.headers.expires = Some(chrono::Utc::now() + chrono::Duration::hours(1));
        assert!(!msg.is_expired());
    }

    #[test]
    fn test_message_is_scheduled() {
        let task_id = Uuid::new_v4();
        let body = vec![1, 2, 3];
        let mut msg = Message::new("tasks.test".to_string(), task_id, body);

        // Not scheduled initially
        assert!(!msg.is_scheduled());

        // Set ETA in the future
        msg.headers.eta = Some(chrono::Utc::now() + chrono::Duration::hours(1));
        assert!(msg.is_scheduled());

        // Set ETA in the past
        msg.headers.eta = Some(chrono::Utc::now() - chrono::Duration::hours(1));
        assert!(!msg.is_scheduled());
    }

    #[cfg(feature = "signing")]
    #[test]
    fn test_sign_and_verify_message() {
        let task_id = Uuid::new_v4();
        let body = serde_json::to_vec(&TaskArgs::new()).unwrap();
        let msg = Message::new("tasks.add".to_string(), task_id, body);

        let signer = MessageSigner::new(b"secret-key");
        let signature = msg.sign_body(&signer);

        assert!(msg.verify_body(&signer, &signature).is_ok());
    }

    #[cfg(feature = "signing")]
    #[test]
    fn test_signed_message_wrapper() {
        let task_id = Uuid::new_v4();
        let body = serde_json::to_vec(&TaskArgs::new()).unwrap();
        let msg = Message::new("tasks.add".to_string(), task_id, body);

        let signer = MessageSigner::new(b"secret-key");
        let signed = SignedMessage::new(msg, &signer);

        assert!(signed.verify(&signer).is_ok());
        assert!(!signed.signature_hex().is_empty());
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn test_encrypt_and_decrypt_message() {
        let task_id = Uuid::new_v4();
        let body = b"secret data".to_vec();
        let mut msg = Message::new("tasks.add".to_string(), task_id, body.clone());

        let encryptor = MessageEncryptor::new(b"32-byte-secret-key-for-aes-256!!").unwrap();
        let nonce = msg.encrypt_body(&encryptor).unwrap();

        // Body should be different after encryption
        assert_ne!(msg.body, body);

        // Decrypt should recover original
        let decrypted = msg.decrypt_body(&encryptor, &nonce).unwrap();
        assert_eq!(decrypted, body);
    }

    #[cfg(feature = "encryption")]
    #[test]
    fn test_encrypted_message_wrapper() {
        let task_id = Uuid::new_v4();
        let body = b"secret data".to_vec();
        let msg = Message::new("tasks.add".to_string(), task_id, body.clone());

        let encryptor = MessageEncryptor::new(b"32-byte-secret-key-for-aes-256!!").unwrap();
        let encrypted = EncryptedMessage::new(msg, &encryptor).unwrap();

        let decrypted = encrypted.decrypt(&encryptor).unwrap();
        assert_eq!(decrypted, body);
        assert!(!encrypted.nonce_hex().is_empty());
    }

    #[cfg(feature = "signing")]
    #[test]
    fn test_secure_message_builder_with_signing() {
        let task_id = Uuid::new_v4();
        let body = serde_json::to_vec(&TaskArgs::new()).unwrap();

        let builder = SecureMessageBuilder::new("tasks.add".to_string(), task_id, body)
            .with_signer(b"secret-key")
            .with_priority(5);

        #[cfg(not(feature = "encryption"))]
        {
            let (msg, signature) = builder.build().unwrap();
            assert_eq!(msg.properties.priority, Some(5));
            assert!(signature.is_some());
        }

        #[cfg(feature = "encryption")]
        {
            let _ = builder; // Use builder to avoid warning
        }
    }

    #[test]
    fn test_extension_error_display() {
        let err = ExtensionError::Validation("test error".to_string());
        assert_eq!(err.to_string(), "Validation error: test error");

        let err = ExtensionError::Serialization("parse failed".to_string());
        assert_eq!(err.to_string(), "Serialization error: parse failed");
    }
}
