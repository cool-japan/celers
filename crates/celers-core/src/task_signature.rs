//! Task message signature verification (HMAC-SHA256).
//!
//! This module lets a producer cryptographically sign a task message and a
//! consumer verify it. The signature is an [HMAC] over a *canonical*
//! serialization of the task's execution-bearing fields, so two semantically
//! identical messages always produce the same MAC regardless of incidental map
//! ordering.
//!
//! # What is automatic, and what is opt-in
//!
//! **Nothing here runs automatically.** This module is the primitive: it signs
//! and verifies a [`SignedFields`] value. It does not sign anything on enqueue
//! and does not reject anything on dequeue.
//!
//! | Behaviour | Automatic? | How to turn it on |
//! |---|---|---|
//! | Messages leave a producer signed | **no** | call [`crate::task_security::sign_task`] on the message before enqueueing it |
//! | The worker refuses an unsigned or tampered message before dispatch | **no** | set `WorkerConfig::signature_verification` to a `SignatureVerification` (`celers-worker`) |
//! | Freshness (`signed_at`) is enforced | only if the verifier asks | `SignatureVerification::with_freshness` |
//! | Replays are rejected | only if the verifier asks, and only within one process | `SignatureVerification::with_replay_guard`; see [`ReplayGuard`] |
//! | Broker headers, routing, or delivery metadata are protected | **no**, ever | out of scope — see *What is and is not authenticated* below |
//!
//! Until a worker is configured with a verifier, a signature on a message is
//! inert decoration: nothing checks it. The `security_wiring` example in the
//! `celers` crate shows both halves wired together.
//!
//! # Where the signature travels
//!
//! On a [`SerializedTask`](crate::SerializedTask) the MAC lives in
//! [`TaskMetadata::signature`](crate::TaskMetadata::signature), as a
//! [`SignatureEnvelope`](crate::task_security::SignatureEnvelope) carrying the
//! tag plus the `signed_at` and `nonce` the producer stamped (both are MAC
//! *inputs*, so a verifier cannot reconstruct them). The projection from a
//! message to the [`SignedFields`] below is
//! [`crate::task_security::signed_fields`] — producer and consumer must use
//! that same function or every verification fails.
//!
//! # What is and is not authenticated
//!
//! Scheme version: **`celers.task.sig.v2`**. The domain tag is part of the
//! MAC input, so a `v1` tag can never verify against `v2` fields and vice
//! versa — see [`SignedFields::canonical_bytes`].
//!
//! Authenticated (covered by the MAC — tampering is detected):
//!
//! | Field | Notes |
//! |---|---|
//! | `id` | task UUID |
//! | `name` | task name |
//! | `args` | positional arguments, in order |
//! | `kwargs` | keyword arguments, canonically sorted by key |
//! | `callbacks` | success links — *these name further tasks to execute* |
//! | `errbacks` | failure links |
//! | `chain` | the remaining chain, in order |
//! | `chord` | the chord body callback |
//! | `eta` | earliest execution time |
//! | `expires` | message deadline |
//! | `signed_at` | when the producer signed (freshness) |
//! | `nonce` | single-use token (replay) |
//!
//! Every field of a [`SignedCallback`] — `task`, `task_id`, `args`, `kwargs`,
//! `options`, `immutable`, `subtask_type` — is covered, so an attacker cannot
//! flip a callback's `immutable` flag or rewrite its options either.
//!
//! **Not authenticated.** These are not part of the MAC input, and a message
//! whose signature verifies says nothing about them:
//!
//! * Broker/transport headers: queue, exchange, routing key, delivery tag,
//!   content type, compression, retry counters, `origin`/`parent_id`/`root_id`
//!   and the `group` id that `celers-protocol`'s `EmbedOptions` carries, plus
//!   any custom `extra` embed keys.
//! * The message body's serialization *format*: a producer and consumer that
//!   disagree about it will disagree about what was signed.
//! * Confidentiality. This is an authenticity primitive only — the payload
//!   travels in the clear.
//! * The [`TaskMetadata`](crate::TaskMetadata) fields a broker legitimately
//!   rewrites in flight — `state`, `created_at`/`updated_at`, `priority`,
//!   `max_retries`, `timeout_secs`, `group_id`, `chord_id`, `dependencies`.
//!   [`crate::task_security::signed_fields`] lists the exact split.
//!
//! # Freshness and replay
//!
//! [`TaskSigner::verify`] answers "was this produced by a holder of the key?"
//! and nothing more; a captured message stays valid forever. For replay
//! resistance:
//!
//! * Set [`SignedFields::signed_at`] on the producer and verify with
//!   [`TaskSigner::verify_fresh`] and a caller-chosen [`FreshnessWindow`].
//!   Messages older than the window — and messages dated further into the
//!   future than the allowed clock skew — are rejected.
//! * Set [`SignedFields::nonce`] as well and verify through a [`ReplayGuard`],
//!   which additionally rejects a nonce it has already seen.
//!
//! [`ReplayGuard`] is an **in-process** cache: two worker processes each accept
//! the same replayed message once. A deployment that needs global single-use
//! semantics must back the nonce check with shared storage (Redis `SET NX`,
//! a unique index) — this type is the local half of that, not a substitute.
//!
//! # Why a native HMAC implementation?
//!
//! `celers-core` is intentionally dependency-light and additive: it does not
//! pull in a crypto stack. Rather than add `hmac`/`sha2` here, this module
//! ships a small, self-contained, constant-foldable implementation of
//! **SHA-256** and **HMAC-SHA256** in pure Rust (verified against the FIPS
//! 180-4 and RFC 4231 test vectors in the unit tests). The public surface is
//! intentionally generic so it can be swapped for a hardware/`RustCrypto`
//! backend later without breaking callers.
//!
//! [HMAC]: https://datatracker.ietf.org/doc/html/rfc2104
//!
//! # Example
//!
//! ```rust
//! use celers_core::task_signature::{TaskSigner, SignedFields};
//! use celers_core::sanitize::TaskValue;
//! use uuid::Uuid;
//!
//! let signer = TaskSigner::new(b"super-secret-shared-key");
//!
//! let fields = SignedFields::new(Uuid::nil(), "tasks.add")
//!     .with_args(vec![TaskValue::from(2), TaskValue::from(3)])
//!     .with_kwarg("note", TaskValue::from("hello"));
//!
//! let sig = signer.sign(&fields);
//!
//! // The same signer verifies an untouched message.
//! assert!(signer.verify(&fields, &sig).is_ok());
//!
//! // Any tampering (here, a changed argument) is rejected.
//! let tampered = SignedFields::new(Uuid::nil(), "tasks.add")
//!     .with_args(vec![TaskValue::from(2), TaskValue::from(4)]);
//! assert!(signer.verify(&tampered, &sig).is_err());
//! ```
//!
//! # Example: workflow links, freshness and replay
//!
//! ```rust
//! use celers_core::task_signature::{
//!     FreshnessWindow, ReplayGuard, SignedCallback, SignedFields, TaskSigner,
//! };
//! use celers_core::sanitize::TaskValue;
//! use std::time::Duration;
//! use uuid::Uuid;
//!
//! let signer = TaskSigner::new(b"super-secret-shared-key");
//!
//! let fields = SignedFields::new(Uuid::nil(), "billing.charge")
//!     .with_args(vec![TaskValue::from(42)])
//!     .with_callback(SignedCallback::new("billing.receipt"))
//!     .signed_now()
//!     .with_random_nonce();
//!
//! let sig = signer.sign(&fields);
//!
//! // Rewriting the callback to a different task invalidates the signature.
//! let hijacked = fields
//!     .clone()
//!     .with_callbacks(vec![SignedCallback::new("attacker.exfiltrate")]);
//! assert!(signer.verify(&hijacked, &sig).is_err());
//!
//! // A guard rejects the second delivery of the same message.
//! let guard = ReplayGuard::new(FreshnessWindow::new(Duration::from_secs(300)));
//! assert!(guard.verify(&signer, &fields, &sig).is_ok());
//! assert!(guard.verify(&signer, &fields, &sig).is_err());
//! ```

use crate::sanitize::TaskValue;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::sync::Mutex;
use std::time::Duration;
use uuid::Uuid;

// ===========================================================================
// SHA-256 (FIPS 180-4)
// ===========================================================================

/// SHA-256 block size in bytes.
const SHA256_BLOCK_BYTES: usize = 64;

/// SHA-256 digest size in bytes.
pub const SHA256_DIGEST_BYTES: usize = 32;

/// SHA-256 round constants (first 32 bits of the fractional parts of the cube
/// roots of the first 64 primes).
const SHA256_K: [u32; 64] = [
    0x428a_2f98,
    0x7137_4491,
    0xb5c0_fbcf,
    0xe9b5_dba5,
    0x3956_c25b,
    0x59f1_11f1,
    0x923f_82a4,
    0xab1c_5ed5,
    0xd807_aa98,
    0x1283_5b01,
    0x2431_85be,
    0x550c_7dc3,
    0x72be_5d74,
    0x80de_b1fe,
    0x9bdc_06a7,
    0xc19b_f174,
    0xe49b_69c1,
    0xefbe_4786,
    0x0fc1_9dc6,
    0x240c_a1cc,
    0x2de9_2c6f,
    0x4a74_84aa,
    0x5cb0_a9dc,
    0x76f9_88da,
    0x983e_5152,
    0xa831_c66d,
    0xb003_27c8,
    0xbf59_7fc7,
    0xc6e0_0bf3,
    0xd5a7_9147,
    0x06ca_6351,
    0x1429_2967,
    0x27b7_0a85,
    0x2e1b_2138,
    0x4d2c_6dfc,
    0x5338_0d13,
    0x650a_7354,
    0x766a_0abb,
    0x81c2_c92e,
    0x9272_2c85,
    0xa2bf_e8a1,
    0xa81a_664b,
    0xc24b_8b70,
    0xc76c_51a3,
    0xd192_e819,
    0xd699_0624,
    0xf40e_3585,
    0x106a_a070,
    0x19a4_c116,
    0x1e37_6c08,
    0x2748_774c,
    0x34b0_bcb5,
    0x391c_0cb3,
    0x4ed8_aa4a,
    0x5b9c_ca4f,
    0x682e_6ff3,
    0x748f_82ee,
    0x78a5_636f,
    0x84c8_7814,
    0x8cc7_0208,
    0x90be_fffa,
    0xa450_6ceb,
    0xbef9_a3f7,
    0xc671_78f2,
];

/// SHA-256 initial hash values (first 32 bits of the fractional parts of the
/// square roots of the first 8 primes).
const SHA256_H0: [u32; 8] = [
    0x6a09_e667,
    0xbb67_ae85,
    0x3c6e_f372,
    0xa54f_f53a,
    0x510e_527f,
    0x9b05_688c,
    0x1f83_d9ab,
    0x5be0_cd19,
];

/// Streaming SHA-256 hasher (pure Rust, no external dependencies).
///
/// Used internally to build [`HmacSha256`]. It is exposed publicly because a
/// canonical message hash is occasionally useful on its own (for example, to
/// key a deduplication cache), and exposing it is purely additive.
#[derive(Clone)]
pub struct Sha256 {
    state: [u32; 8],
    buffer: [u8; SHA256_BLOCK_BYTES],
    buffer_len: usize,
    total_len: u64,
}

impl Default for Sha256 {
    #[inline]
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for Sha256 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never leak partial state contents.
        f.debug_struct("Sha256")
            .field("total_len", &self.total_len)
            .finish_non_exhaustive()
    }
}

impl Sha256 {
    /// Create a new, empty SHA-256 hasher.
    #[inline]
    #[must_use]
    pub const fn new() -> Self {
        Self {
            state: SHA256_H0,
            buffer: [0u8; SHA256_BLOCK_BYTES],
            buffer_len: 0,
            total_len: 0,
        }
    }

    /// Feed more data into the hasher.
    pub fn update(&mut self, mut data: &[u8]) {
        self.total_len = self.total_len.wrapping_add(data.len() as u64);

        // Fill an existing partial buffer first.
        if self.buffer_len > 0 {
            let need = SHA256_BLOCK_BYTES - self.buffer_len;
            let take = need.min(data.len());
            self.buffer[self.buffer_len..self.buffer_len + take].copy_from_slice(&data[..take]);
            self.buffer_len += take;
            data = &data[take..];

            if self.buffer_len == SHA256_BLOCK_BYTES {
                let block = self.buffer;
                Self::compress(&mut self.state, &block);
                self.buffer_len = 0;
            }
        }

        // Process full blocks directly from the input.
        while data.len() >= SHA256_BLOCK_BYTES {
            let mut block = [0u8; SHA256_BLOCK_BYTES];
            block.copy_from_slice(&data[..SHA256_BLOCK_BYTES]);
            Self::compress(&mut self.state, &block);
            data = &data[SHA256_BLOCK_BYTES..];
        }

        // Stash the remainder.
        if !data.is_empty() {
            self.buffer[..data.len()].copy_from_slice(data);
            self.buffer_len = data.len();
        }
    }

    /// Consume the hasher and return the 32-byte digest.
    #[must_use]
    pub fn finalize(mut self) -> [u8; SHA256_DIGEST_BYTES] {
        let bit_len = self.total_len.wrapping_mul(8);

        // Append the 0x80 padding byte.
        self.update_byte(0x80);

        // Pad with zeros until the buffer is 56 bytes mod 64 (room for the
        // 8-byte length).
        while self.buffer_len != 56 {
            self.update_byte(0x00);
        }

        // Append the message length as a 64-bit big-endian integer.
        let len_bytes = bit_len.to_be_bytes();
        for b in len_bytes {
            self.update_byte(b);
        }

        debug_assert_eq!(self.buffer_len, 0, "buffer must be flushed after padding");

        let mut out = [0u8; SHA256_DIGEST_BYTES];
        for (chunk, word) in out.chunks_exact_mut(4).zip(self.state.iter()) {
            chunk.copy_from_slice(&word.to_be_bytes());
        }
        out
    }

    /// One-shot convenience: hash `data` and return its digest.
    #[must_use]
    pub fn digest(data: &[u8]) -> [u8; SHA256_DIGEST_BYTES] {
        let mut h = Self::new();
        h.update(data);
        h.finalize()
    }

    /// Internal helper used only during finalization, where we know `total_len`
    /// has already been accounted for and must not be double-counted.
    #[inline]
    fn update_byte(&mut self, byte: u8) {
        self.buffer[self.buffer_len] = byte;
        self.buffer_len += 1;
        if self.buffer_len == SHA256_BLOCK_BYTES {
            let block = self.buffer;
            Self::compress(&mut self.state, &block);
            self.buffer_len = 0;
        }
    }

    /// The SHA-256 compression function operating on a single 64-byte block.
    #[allow(clippy::many_single_char_names)]
    fn compress(state: &mut [u32; 8], block: &[u8; SHA256_BLOCK_BYTES]) {
        let mut w = [0u32; 64];
        for (i, word) in w.iter_mut().enumerate().take(16) {
            let j = i * 4;
            *word = u32::from_be_bytes([block[j], block[j + 1], block[j + 2], block[j + 3]]);
        }
        for i in 16..64 {
            let s0 = w[i - 15].rotate_right(7) ^ w[i - 15].rotate_right(18) ^ (w[i - 15] >> 3);
            let s1 = w[i - 2].rotate_right(17) ^ w[i - 2].rotate_right(19) ^ (w[i - 2] >> 10);
            w[i] = w[i - 16]
                .wrapping_add(s0)
                .wrapping_add(w[i - 7])
                .wrapping_add(s1);
        }

        let mut a = state[0];
        let mut b = state[1];
        let mut c = state[2];
        let mut d = state[3];
        let mut e = state[4];
        let mut f = state[5];
        let mut g = state[6];
        let mut h = state[7];

        for i in 0..64 {
            let s1 = e.rotate_right(6) ^ e.rotate_right(11) ^ e.rotate_right(25);
            let ch = (e & f) ^ ((!e) & g);
            let temp1 = h
                .wrapping_add(s1)
                .wrapping_add(ch)
                .wrapping_add(SHA256_K[i])
                .wrapping_add(w[i]);
            let s0 = a.rotate_right(2) ^ a.rotate_right(13) ^ a.rotate_right(22);
            let maj = (a & b) ^ (a & c) ^ (b & c);
            let temp2 = s0.wrapping_add(maj);

            h = g;
            g = f;
            f = e;
            e = d.wrapping_add(temp1);
            d = c;
            c = b;
            b = a;
            a = temp1.wrapping_add(temp2);
        }

        state[0] = state[0].wrapping_add(a);
        state[1] = state[1].wrapping_add(b);
        state[2] = state[2].wrapping_add(c);
        state[3] = state[3].wrapping_add(d);
        state[4] = state[4].wrapping_add(e);
        state[5] = state[5].wrapping_add(f);
        state[6] = state[6].wrapping_add(g);
        state[7] = state[7].wrapping_add(h);
    }
}

// ===========================================================================
// HMAC-SHA256 (RFC 2104 / RFC 4231)
// ===========================================================================

/// Inner/outer pad constants for HMAC.
const HMAC_IPAD: u8 = 0x36;
const HMAC_OPAD: u8 = 0x5c;

/// HMAC-SHA256 keyed-hash message authentication code (pure Rust).
///
/// Construct with a secret key, feed message bytes via [`HmacSha256::update`],
/// then call [`HmacSha256::finalize`] for the 32-byte tag.
#[derive(Clone)]
pub struct HmacSha256 {
    inner: Sha256,
    outer_key: [u8; SHA256_BLOCK_BYTES],
}

impl fmt::Debug for HmacSha256 {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never leak the derived key material.
        f.debug_struct("HmacSha256").finish_non_exhaustive()
    }
}

impl HmacSha256 {
    /// Create a new HMAC-SHA256 instance keyed with `key`.
    ///
    /// Keys longer than the block size (64 bytes) are first hashed, exactly as
    /// specified by RFC 2104; shorter keys are zero-padded.
    #[must_use]
    pub fn new(key: &[u8]) -> Self {
        let mut block_key = [0u8; SHA256_BLOCK_BYTES];
        if key.len() > SHA256_BLOCK_BYTES {
            let digest = Sha256::digest(key);
            block_key[..SHA256_DIGEST_BYTES].copy_from_slice(&digest);
        } else {
            block_key[..key.len()].copy_from_slice(key);
        }

        let mut inner_key = [0u8; SHA256_BLOCK_BYTES];
        let mut outer_key = [0u8; SHA256_BLOCK_BYTES];
        for i in 0..SHA256_BLOCK_BYTES {
            inner_key[i] = block_key[i] ^ HMAC_IPAD;
            outer_key[i] = block_key[i] ^ HMAC_OPAD;
        }

        let mut inner = Sha256::new();
        inner.update(&inner_key);

        // Best-effort scrub of the intermediate key buffers.
        block_key.iter_mut().for_each(|b| *b = 0);
        inner_key.iter_mut().for_each(|b| *b = 0);

        Self { inner, outer_key }
    }

    /// Feed message bytes into the MAC.
    #[inline]
    pub fn update(&mut self, data: &[u8]) {
        self.inner.update(data);
    }

    /// Finish and return the 32-byte authentication tag.
    #[must_use]
    pub fn finalize(self) -> [u8; SHA256_DIGEST_BYTES] {
        let inner_digest = self.inner.finalize();
        let mut outer = Sha256::new();
        outer.update(&self.outer_key);
        outer.update(&inner_digest);
        outer.finalize()
    }

    /// One-shot convenience: compute HMAC-SHA256 over `data` with `key`.
    #[must_use]
    pub fn mac(key: &[u8], data: &[u8]) -> [u8; SHA256_DIGEST_BYTES] {
        let mut h = Self::new(key);
        h.update(data);
        h.finalize()
    }
}

/// Compare two byte slices in constant time with respect to their contents.
///
/// Returns `true` only when the slices are equal. The running time depends on
/// the (public) length of the slices but not on *where* they first differ, so
/// it does not leak how many leading bytes of a forged tag were correct.
#[must_use]
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff: u8 = 0;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

// ===========================================================================
// Hex helpers (lower-case, no external dependency)
// ===========================================================================

/// Encode bytes as a lower-case hexadecimal string.
#[must_use]
pub fn to_hex(bytes: &[u8]) -> String {
    const LUT: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(LUT[(b >> 4) as usize] as char);
        out.push(LUT[(b & 0x0f) as usize] as char);
    }
    out
}

/// Decode a hexadecimal string (any case) into bytes.
///
/// # Errors
///
/// Returns [`SignatureError::MalformedSignature`] if the input has an odd
/// length or contains a non-hex character.
pub fn from_hex(s: &str) -> Result<Vec<u8>, SignatureError> {
    let bytes = s.as_bytes();
    if !bytes.len().is_multiple_of(2) {
        return Err(SignatureError::MalformedSignature(
            "hex string has odd length".to_string(),
        ));
    }
    let mut out = Vec::with_capacity(bytes.len() / 2);
    let mut i = 0;
    while i < bytes.len() {
        let hi = hex_val(bytes[i])?;
        let lo = hex_val(bytes[i + 1])?;
        out.push((hi << 4) | lo);
        i += 2;
    }
    Ok(out)
}

#[inline]
fn hex_val(c: u8) -> Result<u8, SignatureError> {
    match c {
        b'0'..=b'9' => Ok(c - b'0'),
        b'a'..=b'f' => Ok(c - b'a' + 10),
        b'A'..=b'F' => Ok(c - b'A' + 10),
        _ => Err(SignatureError::MalformedSignature(format!(
            "invalid hex character: {:?}",
            c as char
        ))),
    }
}

// ===========================================================================
// Signature errors
// ===========================================================================

/// Errors produced while signing or verifying a task message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SignatureError {
    /// The provided signature did not match the recomputed MAC. The message
    /// was tampered with, signed with a different key, or corrupted.
    Mismatch,

    /// The message carried no signature but verification was requested.
    MissingSignature,

    /// The signature bytes/string were not well-formed (e.g. invalid hex or a
    /// wrong-length tag).
    MalformedSignature(String),

    /// The signature used an algorithm this verifier does not understand.
    UnsupportedAlgorithm(String),

    /// Freshness verification was requested but the message carried no
    /// `signed_at` timestamp, so its age cannot be established.
    MissingSignedAt,

    /// The message is authentic but older than the caller's freshness window.
    Stale {
        /// How old the message is, in seconds.
        age_secs: i64,
        /// The window the caller allowed, in seconds.
        max_age_secs: u64,
    },

    /// The message is authentic but dated further into the future than the
    /// allowed clock skew — without this check a future-dated `signed_at`
    /// would keep a captured message valid indefinitely.
    FutureDated {
        /// How far in the future the message is dated, in seconds.
        skew_secs: i64,
        /// The skew the caller allowed, in seconds.
        max_skew_secs: u64,
    },

    /// The message is authentic but its own (signed) `expires` deadline has
    /// passed.
    MessageExpired {
        /// How long ago the deadline passed, in seconds.
        expired_secs_ago: i64,
    },

    /// Replay checking was requested but the message carried no nonce.
    MissingNonce,

    /// The nonce has already been seen: this is a replay of a message that was
    /// accepted earlier.
    Replayed(String),
}

impl fmt::Display for SignatureError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SignatureError::Mismatch => {
                write!(f, "task signature verification failed: MAC mismatch")
            }
            SignatureError::MissingSignature => {
                write!(f, "task message is unsigned but a signature is required")
            }
            SignatureError::MalformedSignature(why) => {
                write!(f, "malformed task signature: {why}")
            }
            SignatureError::UnsupportedAlgorithm(alg) => {
                write!(f, "unsupported task signature algorithm: {alg}")
            }
            SignatureError::MissingSignedAt => write!(
                f,
                "task message has no signed_at timestamp but freshness was required"
            ),
            SignatureError::Stale {
                age_secs,
                max_age_secs,
            } => write!(
                f,
                "task signature is stale: signed {age_secs}s ago, window is {max_age_secs}s"
            ),
            SignatureError::FutureDated {
                skew_secs,
                max_skew_secs,
            } => write!(
                f,
                "task signature is dated {skew_secs}s in the future, allowed clock skew is {max_skew_secs}s"
            ),
            SignatureError::MessageExpired { expired_secs_ago } => write!(
                f,
                "task message expired {expired_secs_ago}s ago"
            ),
            SignatureError::MissingNonce => write!(
                f,
                "task message has no nonce but replay protection was required"
            ),
            SignatureError::Replayed(nonce) => {
                write!(f, "task message replayed: nonce {nonce} was already used")
            }
        }
    }
}

impl std::error::Error for SignatureError {}

impl From<SignatureError> for crate::CelersError {
    fn from(err: SignatureError) -> Self {
        crate::CelersError::Other(format!("signature error: {err}"))
    }
}

// ===========================================================================
// Signed fields & canonical serialization
// ===========================================================================

/// The domain-separation tag prefixed to every canonical serialization.
///
/// Bumped from `celers.task.sig.v1` when the signature scope grew to cover the
/// workflow links and the scheduling fields. Because the tag is inside the MAC
/// input, a `v1` tag can never verify against `v2` fields.
pub const SIGNATURE_DOMAIN_TAG: &[u8] = b"celers.task.sig.v2";

/// A task referenced by another task's workflow links (`callbacks`,
/// `errbacks`, `chain`, `chord`).
///
/// This is `celers-core`'s local projection of `celers-protocol`'s
/// `CallbackSignature`. It is kept structurally identical — `task`, `task_id`,
/// `args`, `kwargs`, `options`, `immutable`, `subtask_type` — so that every
/// field of a wire-level callback can be covered by the MAC. Projecting a
/// protocol callback into this type is the caller's job (`celers-core` does not
/// depend on `celers-protocol`); dropping any field while projecting reopens
/// exactly the hole this type exists to close.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SignedCallback {
    /// Name of the task to invoke.
    pub task: String,
    /// Pre-assigned task id, if the producer chose one.
    pub task_id: Option<Uuid>,
    /// Positional arguments.
    pub args: Vec<TaskValue>,
    /// Keyword arguments. Order is irrelevant — canonicalization sorts keys.
    pub kwargs: Vec<(String, TaskValue)>,
    /// Execution options (queue, countdown, …). Order is irrelevant.
    pub options: Vec<(String, TaskValue)>,
    /// Whether the parent's result is withheld from this callback.
    pub immutable: bool,
    /// Subtask type marker (`"chord"`, `"group"`, …).
    pub subtask_type: Option<String>,
}

impl SignedCallback {
    /// Create a callback that invokes `task` with no arguments.
    #[must_use]
    pub fn new(task: impl Into<String>) -> Self {
        Self {
            task: task.into(),
            ..Self::default()
        }
    }

    /// Set the pre-assigned task id.
    #[must_use]
    pub fn with_task_id(mut self, task_id: Uuid) -> Self {
        self.task_id = Some(task_id);
        self
    }

    /// Set the positional arguments.
    #[must_use]
    pub fn with_args(mut self, args: Vec<TaskValue>) -> Self {
        self.args = args;
        self
    }

    /// Append a single keyword argument.
    #[must_use]
    pub fn with_kwarg(mut self, key: impl Into<String>, value: TaskValue) -> Self {
        self.kwargs.push((key.into(), value));
        self
    }

    /// Append a single execution option.
    #[must_use]
    pub fn with_option(mut self, key: impl Into<String>, value: TaskValue) -> Self {
        self.options.push((key.into(), value));
        self
    }

    /// Set the immutable flag (Celery's `.si()` / `immutable=True`).
    #[must_use]
    pub fn immutable(mut self, immutable: bool) -> Self {
        self.immutable = immutable;
        self
    }

    /// Set the subtask type marker.
    #[must_use]
    pub fn with_subtask_type(mut self, subtask_type: impl Into<String>) -> Self {
        self.subtask_type = Some(subtask_type.into());
        self
    }

    /// Canonical, collision-free byte encoding of this callback.
    ///
    /// Every field participates, each with its own tag, so no two structurally
    /// different callbacks share an encoding.
    #[must_use]
    fn canonical_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        write_field(&mut out, b'T', self.task.as_bytes());
        write_optional_field(
            &mut out,
            b'D',
            self.task_id.as_ref().map(|id| id.as_bytes().as_slice()),
        );

        write_len(&mut out, b'A', self.args.len() as u64);
        for arg in &self.args {
            write_field(&mut out, b'a', &canonical_value_bytes(arg));
        }

        write_pairs(&mut out, b'K', b'k', b'v', &self.kwargs);
        write_pairs(&mut out, b'P', b'p', b'q', &self.options);

        write_flag(&mut out, b'M', self.immutable);
        write_optional_field(
            &mut out,
            b'S',
            self.subtask_type.as_ref().map(|s| s.as_bytes()),
        );
        out
    }
}

/// The fields of a task message that participate in the signature.
///
/// Every field here is covered by the MAC. See the [module
/// documentation][self] for the authoritative list of what is authenticated
/// and — just as importantly — what is not: transport headers, routing,
/// `parent_id`/`root_id`/`group` and any custom embed keys are outside the
/// signature, so a verified signature says nothing about them.
///
/// The workflow links (`callbacks`, `errbacks`, `chain`, `chord`) are signed
/// precisely because they *name further tasks to execute*: leaving them out
/// would let anyone with broker write access redirect a signed message's
/// continuation to a different registered task while the signature still
/// verified.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct SignedFields {
    /// Unique task identifier.
    pub id: Uuid,
    /// Task name / type.
    pub name: String,
    /// Positional arguments.
    pub args: Vec<TaskValue>,
    /// Keyword arguments. Order is irrelevant — canonicalization sorts keys.
    pub kwargs: Vec<(String, TaskValue)>,
    /// Tasks to run when this one succeeds (Celery's `link`).
    pub callbacks: Vec<SignedCallback>,
    /// Tasks to run when this one fails (Celery's `link_error`).
    pub errbacks: Vec<SignedCallback>,
    /// The remaining tasks of the chain this task belongs to, in order.
    pub chain: Vec<SignedCallback>,
    /// The chord body, run once the group completes.
    pub chord: Option<SignedCallback>,
    /// Earliest time at which the task may run (Celery's `eta`).
    pub eta: Option<DateTime<Utc>>,
    /// Deadline after which the message must not be executed (Celery's
    /// `expires`).
    pub expires: Option<DateTime<Utc>>,
    /// When the producer signed the message. Required by
    /// [`TaskSigner::verify_fresh`].
    pub signed_at: Option<DateTime<Utc>>,
    /// Single-use token. Required by [`ReplayGuard`].
    pub nonce: Option<String>,
}

impl SignedFields {
    /// Create a new set of signed fields from a task id and name.
    #[must_use]
    pub fn new(id: Uuid, name: impl Into<String>) -> Self {
        Self {
            id,
            name: name.into(),
            ..Self::default()
        }
    }

    /// Set the positional arguments.
    #[must_use]
    pub fn with_args(mut self, args: Vec<TaskValue>) -> Self {
        self.args = args;
        self
    }

    /// Append a single positional argument.
    #[must_use]
    pub fn with_arg(mut self, value: TaskValue) -> Self {
        self.args.push(value);
        self
    }

    /// Set the keyword arguments.
    #[must_use]
    pub fn with_kwargs(mut self, kwargs: Vec<(String, TaskValue)>) -> Self {
        self.kwargs = kwargs;
        self
    }

    /// Append a single keyword argument.
    #[must_use]
    pub fn with_kwarg(mut self, key: impl Into<String>, value: TaskValue) -> Self {
        self.kwargs.push((key.into(), value));
        self
    }

    /// Set the success links.
    #[must_use]
    pub fn with_callbacks(mut self, callbacks: Vec<SignedCallback>) -> Self {
        self.callbacks = callbacks;
        self
    }

    /// Append one success link.
    #[must_use]
    pub fn with_callback(mut self, callback: SignedCallback) -> Self {
        self.callbacks.push(callback);
        self
    }

    /// Set the failure links.
    #[must_use]
    pub fn with_errbacks(mut self, errbacks: Vec<SignedCallback>) -> Self {
        self.errbacks = errbacks;
        self
    }

    /// Append one failure link.
    #[must_use]
    pub fn with_errback(mut self, errback: SignedCallback) -> Self {
        self.errbacks.push(errback);
        self
    }

    /// Set the remaining chain, in execution order.
    #[must_use]
    pub fn with_chain(mut self, chain: Vec<SignedCallback>) -> Self {
        self.chain = chain;
        self
    }

    /// Set the chord body callback.
    #[must_use]
    pub fn with_chord(mut self, chord: SignedCallback) -> Self {
        self.chord = Some(chord);
        self
    }

    /// Set the earliest execution time.
    #[must_use]
    pub fn with_eta(mut self, eta: DateTime<Utc>) -> Self {
        self.eta = Some(eta);
        self
    }

    /// Set the message-expiry deadline.
    #[must_use]
    pub fn with_expires(mut self, expires: DateTime<Utc>) -> Self {
        self.expires = Some(expires);
        self
    }

    /// Stamp an explicit signing time.
    #[must_use]
    pub fn with_signed_at(mut self, signed_at: DateTime<Utc>) -> Self {
        self.signed_at = Some(signed_at);
        self
    }

    /// Stamp the current time as the signing time.
    #[must_use]
    pub fn signed_now(self) -> Self {
        let now = Utc::now();
        self.with_signed_at(now)
    }

    /// Set an explicit nonce.
    ///
    /// The nonce only has to be unique per key and freshness window; it is not
    /// required to be secret.
    #[must_use]
    pub fn with_nonce(mut self, nonce: impl Into<String>) -> Self {
        self.nonce = Some(nonce.into());
        self
    }

    /// Generate a fresh 128-bit random nonce and attach it.
    #[must_use]
    pub fn with_random_nonce(self) -> Self {
        use rand::RngExt;
        let mut rng = rand::rng();
        let high: u64 = rng.random();
        let low: u64 = rng.random();
        self.with_nonce(format!("{high:016x}{low:016x}"))
    }

    /// Produce the canonical byte serialization that is fed to the MAC.
    ///
    /// The encoding is length-prefixed and field-tagged so that no two
    /// distinct field layouts can collide (i.e. it is *unambiguous*):
    /// concatenating differently-split components can never yield the same
    /// byte stream. Keyword arguments and callback options are sorted by key so
    /// map ordering does not affect the result; ordered sequences (`args`,
    /// `chain`, `callbacks`, `errbacks`) keep their order, because reordering
    /// them changes what executes.
    ///
    /// The stream starts with [`SIGNATURE_DOMAIN_TAG`], which both separates
    /// these MACs from raw HMACs of arbitrary data and pins the scheme version.
    #[must_use]
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();

        out.extend_from_slice(SIGNATURE_DOMAIN_TAG);

        // id (always 16 bytes, but length-prefix anyway for uniformity).
        write_field(&mut out, b'I', self.id.as_bytes());

        // name.
        write_field(&mut out, b'N', self.name.as_bytes());

        // args: count, then each value's canonical bytes.
        write_len(&mut out, b'A', self.args.len() as u64);
        for arg in &self.args {
            let encoded = canonical_value_bytes(arg);
            write_field(&mut out, b'a', &encoded);
        }

        // kwargs: sorted by key, count, then key/value pairs.
        write_pairs(&mut out, b'K', b'k', b'v', &self.kwargs);

        // Workflow links. Each list gets its own tag so moving a callback into
        // the chain (or vice versa) changes the encoding.
        write_callbacks(&mut out, b'C', b'c', &self.callbacks);
        write_callbacks(&mut out, b'E', b'e', &self.errbacks);
        write_callbacks(&mut out, b'H', b'h', &self.chain);
        write_optional_field(
            &mut out,
            b'R',
            self.chord
                .as_ref()
                .map(SignedCallback::canonical_bytes)
                .as_deref(),
        );

        // Scheduling / freshness timestamps.
        write_optional_timestamp(&mut out, b'X', self.eta);
        write_optional_timestamp(&mut out, b'Z', self.expires);
        write_optional_timestamp(&mut out, b'W', self.signed_at);
        write_optional_field(&mut out, b'O', self.nonce.as_ref().map(|n| n.as_bytes()));

        out
    }
}

/// Write a tagged, length-prefixed field: `tag | len(8, be) | bytes`.
fn write_field(out: &mut Vec<u8>, tag: u8, bytes: &[u8]) {
    out.push(tag);
    out.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
    out.extend_from_slice(bytes);
}

/// Write a tagged count: `tag | value(8, be)`.
fn write_len(out: &mut Vec<u8>, tag: u8, value: u64) {
    out.push(tag);
    out.extend_from_slice(&value.to_be_bytes());
}

/// Write a tagged optional field: `tag | 0` when absent, `tag | 1 | len | bytes`
/// when present. The presence byte keeps "absent" and "empty" distinct.
fn write_optional_field(out: &mut Vec<u8>, tag: u8, bytes: Option<&[u8]>) {
    out.push(tag);
    match bytes {
        None => out.push(0),
        Some(value) => {
            out.push(1);
            out.extend_from_slice(&(value.len() as u64).to_be_bytes());
            out.extend_from_slice(value);
        }
    }
}

/// Write a tagged boolean: `tag | 0|1`.
fn write_flag(out: &mut Vec<u8>, tag: u8, flag: bool) {
    out.push(tag);
    out.push(u8::from(flag));
}

/// Write a tagged optional instant as `seconds(i64, be) | nanos(u32, be)`.
///
/// Encoded numerically rather than as text so two spellings of the same instant
/// (`Z` vs `+00:00`, differing sub-second precision) cannot produce different
/// MACs.
fn write_optional_timestamp(out: &mut Vec<u8>, tag: u8, at: Option<DateTime<Utc>>) {
    let encoded = at.map(|at| {
        let mut buf = Vec::with_capacity(12);
        buf.extend_from_slice(&at.timestamp().to_be_bytes());
        buf.extend_from_slice(&at.timestamp_subsec_nanos().to_be_bytes());
        buf
    });
    write_optional_field(out, tag, encoded.as_deref());
}

/// Write a key/value map: a tagged count followed by key/value fields, sorted
/// by key so map iteration order cannot change the MAC.
fn write_pairs(
    out: &mut Vec<u8>,
    count_tag: u8,
    key_tag: u8,
    value_tag: u8,
    pairs: &[(String, TaskValue)],
) {
    let mut sorted: Vec<&(String, TaskValue)> = pairs.iter().collect();
    sorted.sort_by(|x, y| x.0.cmp(&y.0));
    write_len(out, count_tag, sorted.len() as u64);
    for (key, value) in sorted {
        write_field(out, key_tag, key.as_bytes());
        write_field(out, value_tag, &canonical_value_bytes(value));
    }
}

/// Write an ordered list of callbacks: a tagged count followed by each
/// callback's canonical encoding, in order.
fn write_callbacks(out: &mut Vec<u8>, count_tag: u8, item_tag: u8, callbacks: &[SignedCallback]) {
    write_len(out, count_tag, callbacks.len() as u64);
    for callback in callbacks {
        write_field(out, item_tag, &callback.canonical_bytes());
    }
}

/// Canonical, collision-free byte encoding of a single [`TaskValue`].
///
/// Each variant is tagged so that, e.g., the integer `1` and the string
/// `"1"` and the boolean `true` never share an encoding. Containers recurse
/// with the same length-prefixed framing used at the top level, and object
/// keys are sorted.
fn canonical_value_bytes(value: &TaskValue) -> Vec<u8> {
    let mut out = Vec::new();
    match value {
        TaskValue::Null => out.push(b'0'),
        TaskValue::Bool(b) => {
            out.push(b'b');
            out.push(u8::from(*b));
        }
        TaskValue::Int(i) => {
            out.push(b'i');
            out.extend_from_slice(&i.to_be_bytes());
        }
        TaskValue::UInt(u) => {
            out.push(b'u');
            out.extend_from_slice(&u.to_be_bytes());
        }
        TaskValue::Float(f) => {
            out.push(b'f');
            // Normalize NaN so distinct NaN bit-patterns canonicalize equally,
            // and normalize the sign of zero so +0.0 / -0.0 agree.
            let bits = if f.is_nan() {
                f64::NAN.to_bits()
            } else if *f == 0.0 {
                0.0_f64.to_bits()
            } else {
                f.to_bits()
            };
            out.extend_from_slice(&bits.to_be_bytes());
        }
        TaskValue::String(s) => {
            out.push(b's');
            out.extend_from_slice(&(s.len() as u64).to_be_bytes());
            out.extend_from_slice(s.as_bytes());
        }
        TaskValue::Bytes(bytes) => {
            out.push(b'B');
            out.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
            out.extend_from_slice(bytes);
        }
        TaskValue::Array(items) => {
            out.push(b'L');
            out.extend_from_slice(&(items.len() as u64).to_be_bytes());
            for item in items {
                let encoded = canonical_value_bytes(item);
                out.extend_from_slice(&(encoded.len() as u64).to_be_bytes());
                out.extend_from_slice(&encoded);
            }
        }
        TaskValue::Object(entries) => {
            out.push(b'O');
            let mut sorted: Vec<&(String, TaskValue)> = entries.iter().collect();
            sorted.sort_by(|x, y| x.0.cmp(&y.0));
            out.extend_from_slice(&(sorted.len() as u64).to_be_bytes());
            for (k, v) in sorted {
                out.extend_from_slice(&(k.len() as u64).to_be_bytes());
                out.extend_from_slice(k.as_bytes());
                let encoded = canonical_value_bytes(v);
                out.extend_from_slice(&(encoded.len() as u64).to_be_bytes());
                out.extend_from_slice(&encoded);
            }
        }
    }
    out
}

// ===========================================================================
// Signature value
// ===========================================================================

/// Algorithm identifier carried alongside a signature for forward
/// compatibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SignatureAlgorithm {
    /// HMAC with SHA-256.
    #[serde(rename = "HMAC-SHA256")]
    HmacSha256,
}

impl SignatureAlgorithm {
    /// The wire/string name of the algorithm.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            SignatureAlgorithm::HmacSha256 => "HMAC-SHA256",
        }
    }
}

impl fmt::Display for SignatureAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A computed task signature: the algorithm plus the hex-encoded MAC tag.
///
/// This is a serializable value that can be attached to a task message (for
/// example, in a header or sidecar field) and round-tripped through any of the
/// crate's serializers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskSignature {
    /// MAC algorithm used to produce `tag`.
    pub algorithm: SignatureAlgorithm,
    /// Lower-case hex encoding of the MAC tag.
    pub tag: String,
}

impl TaskSignature {
    /// Construct an HMAC-SHA256 signature from raw tag bytes.
    #[must_use]
    pub fn hmac_sha256(tag: &[u8]) -> Self {
        Self {
            algorithm: SignatureAlgorithm::HmacSha256,
            tag: to_hex(tag),
        }
    }

    /// Decode the hex tag back into raw bytes.
    ///
    /// # Errors
    ///
    /// Returns [`SignatureError::MalformedSignature`] if the hex is invalid.
    pub fn tag_bytes(&self) -> Result<Vec<u8>, SignatureError> {
        from_hex(&self.tag)
    }
}

impl fmt::Display for TaskSignature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.algorithm.as_str(), self.tag)
    }
}

// ===========================================================================
// Signer
// ===========================================================================

/// Signs and verifies task messages with a shared secret using HMAC-SHA256.
///
/// A `TaskSigner` holds the secret key. Producers call [`TaskSigner::sign`] to
/// obtain a [`TaskSignature`]; consumers call [`TaskSigner::verify`] (or
/// [`TaskSigner::verify_optional`]) to authenticate a received message.
#[derive(Clone)]
pub struct TaskSigner {
    key: Vec<u8>,
}

impl fmt::Debug for TaskSigner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never print the key.
        f.debug_struct("TaskSigner")
            .field("key_len", &self.key.len())
            .finish()
    }
}

impl TaskSigner {
    /// Create a signer from a shared secret key.
    ///
    /// Any key length is accepted (HMAC handles short and long keys), but for
    /// HMAC-SHA256 a key of at least 32 bytes of entropy is recommended.
    #[must_use]
    pub fn new(key: impl AsRef<[u8]>) -> Self {
        Self {
            key: key.as_ref().to_vec(),
        }
    }

    /// Compute the raw MAC tag bytes over the canonical serialization.
    #[must_use]
    pub fn mac(&self, fields: &SignedFields) -> [u8; SHA256_DIGEST_BYTES] {
        HmacSha256::mac(&self.key, &fields.canonical_bytes())
    }

    /// Sign a set of task fields, returning a serializable [`TaskSignature`].
    #[must_use]
    pub fn sign(&self, fields: &SignedFields) -> TaskSignature {
        TaskSignature::hmac_sha256(&self.mac(fields))
    }

    /// Verify a task message against its signature.
    ///
    /// The comparison is constant-time. Any tampering with the signed fields,
    /// a wrong key, or a corrupted/forged tag yields an error.
    ///
    /// # Errors
    ///
    /// * [`SignatureError::UnsupportedAlgorithm`] if the signature names an
    ///   algorithm other than HMAC-SHA256.
    /// * [`SignatureError::MalformedSignature`] if the tag is not valid hex or
    ///   has the wrong length.
    /// * [`SignatureError::Mismatch`] if the recomputed MAC does not match.
    pub fn verify(
        &self,
        fields: &SignedFields,
        signature: &TaskSignature,
    ) -> Result<(), SignatureError> {
        if signature.algorithm != SignatureAlgorithm::HmacSha256 {
            return Err(SignatureError::UnsupportedAlgorithm(
                signature.algorithm.as_str().to_string(),
            ));
        }

        let provided = signature.tag_bytes()?;
        if provided.len() != SHA256_DIGEST_BYTES {
            return Err(SignatureError::MalformedSignature(format!(
                "expected {SHA256_DIGEST_BYTES}-byte tag, got {} bytes",
                provided.len()
            )));
        }

        let expected = self.mac(fields);
        if constant_time_eq(&expected, &provided) {
            Ok(())
        } else {
            Err(SignatureError::Mismatch)
        }
    }

    /// Verify a message whose signature may be absent.
    ///
    /// When `signature` is `None` this returns
    /// [`SignatureError::MissingSignature`], which lets a caller distinguish an
    /// *unsigned* message from a *badly-signed* one and reject both.
    ///
    /// # Errors
    ///
    /// Returns [`SignatureError::MissingSignature`] when no signature is
    /// supplied, or any error from [`TaskSigner::verify`] otherwise.
    pub fn verify_optional(
        &self,
        fields: &SignedFields,
        signature: Option<&TaskSignature>,
    ) -> Result<(), SignatureError> {
        match signature {
            Some(sig) => self.verify(fields, sig),
            None => Err(SignatureError::MissingSignature),
        }
    }

    /// Convenience predicate: `true` iff the message verifies.
    ///
    /// Authenticity only — this says nothing about freshness or replay. See
    /// [`TaskSigner::verify_fresh`].
    #[must_use]
    pub fn is_valid(&self, fields: &SignedFields, signature: &TaskSignature) -> bool {
        self.verify(fields, signature).is_ok()
    }

    /// Verify authenticity **and** freshness against the current clock.
    ///
    /// Equivalent to [`TaskSigner::verify_fresh_at`] with `now = Utc::now()`.
    ///
    /// # Errors
    ///
    /// Anything [`TaskSigner::verify`] returns, plus
    /// [`SignatureError::MissingSignedAt`], [`SignatureError::Stale`],
    /// [`SignatureError::FutureDated`] and [`SignatureError::MessageExpired`].
    pub fn verify_fresh(
        &self,
        fields: &SignedFields,
        signature: &TaskSignature,
        window: &FreshnessWindow,
    ) -> Result<(), SignatureError> {
        self.verify_fresh_at(fields, signature, window, Utc::now())
    }

    /// Verify authenticity **and** freshness against an explicit `now`.
    ///
    /// The order matters: the MAC is checked first, so the timestamps this
    /// method reasons about are already known to be authentic rather than
    /// attacker-supplied.
    ///
    /// Three separate conditions are enforced:
    ///
    /// 1. `signed_at` must be present and no older than `window.max_age`.
    /// 2. `signed_at` must not be further in the future than
    ///    `window.max_clock_skew` — otherwise a future-dated message would stay
    ///    valid indefinitely.
    /// 3. If the message carries an `expires` deadline (itself signed), that
    ///    deadline must not have passed.
    ///
    /// # Errors
    ///
    /// See [`TaskSigner::verify_fresh`].
    pub fn verify_fresh_at(
        &self,
        fields: &SignedFields,
        signature: &TaskSignature,
        window: &FreshnessWindow,
        now: DateTime<Utc>,
    ) -> Result<(), SignatureError> {
        self.verify(fields, signature)?;
        window.check_at(fields, now)
    }
}

// ===========================================================================
// Freshness & replay
// ===========================================================================

/// How old an authenticated message may be before it is rejected.
///
/// Both bounds are caller-supplied on purpose: the right window depends on the
/// deployment (a few seconds for an RPC-like queue, minutes for a batch
/// pipeline), and no default can be correct for all of them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FreshnessWindow {
    /// Maximum age of `signed_at` relative to now.
    pub max_age: Duration,
    /// Tolerance for a `signed_at` in the future (producer clock ahead).
    pub max_clock_skew: Duration,
}

impl FreshnessWindow {
    /// Default tolerance for a producer clock running ahead of the consumer.
    pub const DEFAULT_CLOCK_SKEW: Duration = Duration::from_secs(60);

    /// A window of `max_age` with [`Self::DEFAULT_CLOCK_SKEW`].
    #[must_use]
    pub const fn new(max_age: Duration) -> Self {
        Self {
            max_age,
            max_clock_skew: Self::DEFAULT_CLOCK_SKEW,
        }
    }

    /// Override the permitted clock skew.
    #[must_use]
    pub const fn with_max_clock_skew(mut self, max_clock_skew: Duration) -> Self {
        self.max_clock_skew = max_clock_skew;
        self
    }

    /// Check the freshness of already-authenticated fields against `now`.
    ///
    /// Exposed so a caller that verified the MAC separately (or that batches
    /// verification) can still apply the same policy.
    ///
    /// # Errors
    ///
    /// [`SignatureError::MissingSignedAt`], [`SignatureError::Stale`],
    /// [`SignatureError::FutureDated`] or [`SignatureError::MessageExpired`].
    pub fn check_at(
        &self,
        fields: &SignedFields,
        now: DateTime<Utc>,
    ) -> Result<(), SignatureError> {
        let signed_at = fields.signed_at.ok_or(SignatureError::MissingSignedAt)?;
        let age = now - signed_at;

        if age.num_seconds() < 0 {
            let skew_secs = -age.num_seconds();
            let max_skew_secs = self.max_clock_skew.as_secs();
            if skew_secs > i64::try_from(max_skew_secs).unwrap_or(i64::MAX) {
                return Err(SignatureError::FutureDated {
                    skew_secs,
                    max_skew_secs,
                });
            }
        } else {
            let max_age_secs = self.max_age.as_secs();
            if age.num_seconds() > i64::try_from(max_age_secs).unwrap_or(i64::MAX) {
                return Err(SignatureError::Stale {
                    age_secs: age.num_seconds(),
                    max_age_secs,
                });
            }
        }

        if let Some(expires) = fields.expires {
            if now > expires {
                return Err(SignatureError::MessageExpired {
                    expired_secs_ago: (now - expires).num_seconds(),
                });
            }
        }

        Ok(())
    }
}

/// Rejects a signed message whose nonce has already been accepted.
///
/// # Scope
///
/// This cache lives in **one process**. It stops the same message being
/// executed twice by the same worker, which is what a local `verify` loop can
/// guarantee on its own; it does not coordinate between workers, so N worker
/// processes will each accept a replayed message once. A deployment that needs
/// global single-use semantics must back the check with shared storage (a Redis
/// `SET key NX PX`, a unique index on the nonce) — this type is the local half
/// of that design, not a substitute for it.
///
/// Entries are pruned lazily: a nonce older than the freshness window can be
/// dropped because such a message is rejected by the freshness check anyway.
#[derive(Debug)]
pub struct ReplayGuard {
    window: FreshnessWindow,
    seen: Mutex<HashMap<String, DateTime<Utc>>>,
}

impl ReplayGuard {
    /// Create a guard enforcing `window` plus single-use nonces.
    #[must_use]
    pub fn new(window: FreshnessWindow) -> Self {
        Self {
            window,
            seen: Mutex::new(HashMap::new()),
        }
    }

    /// The freshness window this guard enforces.
    #[must_use]
    pub const fn window(&self) -> &FreshnessWindow {
        &self.window
    }

    /// Number of nonces currently remembered.
    #[must_use]
    pub fn remembered(&self) -> usize {
        self.lock_seen().len()
    }

    /// Full check against the current clock: MAC, freshness, and single-use
    /// nonce.
    ///
    /// # Errors
    ///
    /// See [`ReplayGuard::verify_at`].
    pub fn verify(
        &self,
        signer: &TaskSigner,
        fields: &SignedFields,
        signature: &TaskSignature,
    ) -> Result<(), SignatureError> {
        self.verify_at(signer, fields, signature, Utc::now())
    }

    /// Full check against an explicit `now`.
    ///
    /// The nonce is recorded only after the signature and the freshness window
    /// have both passed, so a forged or stale message cannot poison the cache
    /// and lock out the genuine one.
    ///
    /// # Errors
    ///
    /// Anything [`TaskSigner::verify_fresh_at`] returns, plus
    /// [`SignatureError::MissingNonce`] when the message carries no nonce and
    /// [`SignatureError::Replayed`] when the nonce was already accepted.
    pub fn verify_at(
        &self,
        signer: &TaskSigner,
        fields: &SignedFields,
        signature: &TaskSignature,
        now: DateTime<Utc>,
    ) -> Result<(), SignatureError> {
        signer.verify_fresh_at(fields, signature, &self.window, now)?;

        let nonce = fields.nonce.as_ref().ok_or(SignatureError::MissingNonce)?;

        let mut seen = self.lock_seen();
        Self::prune(&mut seen, &self.window, now);
        if seen.contains_key(nonce) {
            return Err(SignatureError::Replayed(nonce.clone()));
        }
        seen.insert(nonce.clone(), now);
        Ok(())
    }

    /// Drop remembered nonces that can no longer be replayed within the
    /// freshness window.
    pub fn prune_at(&self, now: DateTime<Utc>) {
        let mut seen = self.lock_seen();
        Self::prune(&mut seen, &self.window, now);
    }

    fn prune(
        seen: &mut HashMap<String, DateTime<Utc>>,
        window: &FreshnessWindow,
        now: DateTime<Utc>,
    ) {
        let horizon = i64::try_from(
            window
                .max_age
                .as_secs()
                .saturating_add(window.max_clock_skew.as_secs()),
        )
        .unwrap_or(i64::MAX);
        seen.retain(|_, accepted_at| (now - *accepted_at).num_seconds() <= horizon);
    }

    /// Lock the cache, recovering from poisoning rather than panicking: a
    /// poisoned mutex here only means some other thread panicked mid-check, and
    /// the remembered set is still sound to use.
    fn lock_seen(&self) -> std::sync::MutexGuard<'_, HashMap<String, DateTime<Utc>>> {
        self.seen.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- SHA-256 known-answer tests (FIPS 180-4 examples) ---

    #[test]
    fn sha256_empty() {
        let d = Sha256::digest(b"");
        assert_eq!(
            to_hex(&d),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn sha256_abc() {
        let d = Sha256::digest(b"abc");
        assert_eq!(
            to_hex(&d),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
    }

    #[test]
    fn sha256_two_block() {
        let d = Sha256::digest(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq");
        assert_eq!(
            to_hex(&d),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );
    }

    #[test]
    fn sha256_million_a() {
        // FIPS 180-4: one million 'a' characters.
        let mut h = Sha256::new();
        let chunk = vec![b'a'; 1000];
        for _ in 0..1000 {
            h.update(&chunk);
        }
        let d = h.finalize();
        assert_eq!(
            to_hex(&d),
            "cdc76e5c9914fb9281a1c7e284d73e67f1809a48a497200e046d39ccc7112cd0"
        );
    }

    #[test]
    fn sha256_streaming_matches_oneshot() {
        let data = b"the quick brown fox jumps over the lazy dog";
        let oneshot = Sha256::digest(data);
        let mut h = Sha256::new();
        for byte in data {
            h.update(std::slice::from_ref(byte));
        }
        assert_eq!(oneshot, h.finalize());
    }

    // --- HMAC-SHA256 known-answer tests (RFC 4231) ---

    #[test]
    fn hmac_rfc4231_case1() {
        let key = [0x0b_u8; 20];
        let data = b"Hi There";
        let mac = HmacSha256::mac(&key, data);
        assert_eq!(
            to_hex(&mac),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );
    }

    #[test]
    fn hmac_rfc4231_case2() {
        let key = b"Jefe";
        let data = b"what do ya want for nothing?";
        let mac = HmacSha256::mac(key, data);
        assert_eq!(
            to_hex(&mac),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );
    }

    #[test]
    fn hmac_rfc4231_case3() {
        let key = [0xaa_u8; 20];
        let data = [0xdd_u8; 50];
        let mac = HmacSha256::mac(&key, &data);
        assert_eq!(
            to_hex(&mac),
            "773ea91e36800e46854db8ebd09181a72959098b3ef8c122d9635514ced565fe"
        );
    }

    #[test]
    fn hmac_rfc4231_case6_long_key() {
        // Key longer than the block size must be hashed first.
        let key = [0xaa_u8; 131];
        let data = b"Test Using Larger Than Block-Size Key - Hash Key First";
        let mac = HmacSha256::mac(&key, data);
        assert_eq!(
            to_hex(&mac),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
    }

    // --- hex helpers ---

    #[test]
    fn hex_roundtrip() {
        let bytes = [0x00, 0x0f, 0xa5, 0xff, 0x10];
        let s = to_hex(&bytes);
        assert_eq!(s, "000fa5ff10");
        assert_eq!(from_hex(&s).unwrap(), bytes);
        // Mixed case decoding.
        assert_eq!(
            from_hex("00Fa5Fff10").unwrap(),
            [0x00, 0xfa, 0x5f, 0xff, 0x10]
        );
    }

    #[test]
    fn hex_rejects_bad_input() {
        assert!(from_hex("abc").is_err()); // odd length
        assert!(from_hex("zz").is_err()); // non-hex
    }

    #[test]
    fn constant_time_eq_works() {
        assert!(constant_time_eq(b"abcd", b"abcd"));
        assert!(!constant_time_eq(b"abcd", b"abce"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
    }

    // --- sign / verify round-trip ---

    fn sample_fields() -> SignedFields {
        SignedFields::new(Uuid::from_u128(0x1234_5678_9abc_def0), "tasks.add")
            .with_args(vec![TaskValue::from(2_i64), TaskValue::from(3_i64)])
            .with_kwarg("note", TaskValue::from("hello"))
            .with_kwarg("flag", TaskValue::from(true))
    }

    #[test]
    fn sign_verify_roundtrip() {
        let signer = TaskSigner::new(b"shared-secret-key-shared-secret!");
        let fields = sample_fields();
        let sig = signer.sign(&fields);
        assert_eq!(sig.algorithm, SignatureAlgorithm::HmacSha256);
        assert!(signer.verify(&fields, &sig).is_ok());
        assert!(signer.is_valid(&fields, &sig));
    }

    #[test]
    fn kwarg_order_does_not_matter() {
        let signer = TaskSigner::new(b"key");
        let a = SignedFields::new(Uuid::nil(), "t")
            .with_kwarg("a", TaskValue::from(1_i64))
            .with_kwarg("b", TaskValue::from(2_i64));
        let b = SignedFields::new(Uuid::nil(), "t")
            .with_kwarg("b", TaskValue::from(2_i64))
            .with_kwarg("a", TaskValue::from(1_i64));
        assert_eq!(signer.mac(&a), signer.mac(&b));
    }

    #[test]
    fn tamper_args_detected() {
        let signer = TaskSigner::new(b"key");
        let fields = sample_fields();
        let sig = signer.sign(&fields);

        let tampered = SignedFields::new(fields.id, &fields.name)
            .with_args(vec![TaskValue::from(2_i64), TaskValue::from(4_i64)])
            .with_kwarg("note", TaskValue::from("hello"))
            .with_kwarg("flag", TaskValue::from(true));
        assert_eq!(
            signer.verify(&tampered, &sig),
            Err(SignatureError::Mismatch)
        );
    }

    #[test]
    fn tamper_name_detected() {
        let signer = TaskSigner::new(b"key");
        let fields = sample_fields();
        let sig = signer.sign(&fields);
        let mut tampered = fields.clone();
        tampered.name = "tasks.subtract".to_string();
        assert!(signer.verify(&tampered, &sig).is_err());
    }

    #[test]
    fn tamper_id_detected() {
        let signer = TaskSigner::new(b"key");
        let fields = sample_fields();
        let sig = signer.sign(&fields);
        let mut tampered = fields.clone();
        tampered.id = Uuid::from_u128(0xdead_beef);
        assert!(signer.verify(&tampered, &sig).is_err());
    }

    #[test]
    fn tamper_kwarg_value_detected() {
        let signer = TaskSigner::new(b"key");
        let fields = sample_fields();
        let sig = signer.sign(&fields);
        let tampered = SignedFields::new(fields.id, &fields.name)
            .with_args(fields.args.clone())
            .with_kwarg("note", TaskValue::from("HELLO"))
            .with_kwarg("flag", TaskValue::from(true));
        assert!(signer.verify(&tampered, &sig).is_err());
    }

    #[test]
    fn wrong_key_detected() {
        let signer = TaskSigner::new(b"key-one");
        let other = TaskSigner::new(b"key-two");
        let fields = sample_fields();
        let sig = signer.sign(&fields);
        assert_eq!(other.verify(&fields, &sig), Err(SignatureError::Mismatch));
    }

    #[test]
    fn unsigned_rejected() {
        let signer = TaskSigner::new(b"key");
        let fields = sample_fields();
        assert_eq!(
            signer.verify_optional(&fields, None),
            Err(SignatureError::MissingSignature)
        );
        // And a signed message still passes through verify_optional.
        let sig = signer.sign(&fields);
        assert!(signer.verify_optional(&fields, Some(&sig)).is_ok());
    }

    #[test]
    fn malformed_signature_rejected() {
        let signer = TaskSigner::new(b"key");
        let fields = sample_fields();

        let bad_hex = TaskSignature {
            algorithm: SignatureAlgorithm::HmacSha256,
            tag: "nothex!!".to_string(),
        };
        assert!(matches!(
            signer.verify(&fields, &bad_hex),
            Err(SignatureError::MalformedSignature(_))
        ));

        let wrong_len = TaskSignature {
            algorithm: SignatureAlgorithm::HmacSha256,
            tag: "aabb".to_string(),
        };
        assert!(matches!(
            signer.verify(&fields, &wrong_len),
            Err(SignatureError::MalformedSignature(_))
        ));
    }

    #[test]
    fn distinct_types_do_not_collide() {
        // Integer 1, string "1", and bool true must canonicalize differently.
        let signer = TaskSigner::new(b"key");
        let as_int = SignedFields::new(Uuid::nil(), "t").with_arg(TaskValue::from(1_i64));
        let as_str = SignedFields::new(Uuid::nil(), "t").with_arg(TaskValue::from("1"));
        let as_bool = SignedFields::new(Uuid::nil(), "t").with_arg(TaskValue::from(true));
        let m_int = signer.mac(&as_int);
        let m_str = signer.mac(&as_str);
        let m_bool = signer.mac(&as_bool);
        assert_ne!(m_int, m_str);
        assert_ne!(m_int, m_bool);
        assert_ne!(m_str, m_bool);
    }

    #[test]
    fn arg_boundary_not_ambiguous() {
        // ["a", "b"] must not collide with ["ab"] thanks to length prefixing.
        let signer = TaskSigner::new(b"key");
        let split = SignedFields::new(Uuid::nil(), "t")
            .with_args(vec![TaskValue::from("a"), TaskValue::from("b")]);
        let joined = SignedFields::new(Uuid::nil(), "t").with_arg(TaskValue::from("ab"));
        assert_ne!(signer.mac(&split), signer.mac(&joined));
    }

    #[test]
    fn signature_serde_roundtrip() {
        let signer = TaskSigner::new(b"key");
        let fields = sample_fields();
        let sig = signer.sign(&fields);
        let json = serde_json::to_string(&sig).expect("serialize");
        let restored: TaskSignature = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(sig, restored);
        assert!(signer.verify(&fields, &restored).is_ok());
    }

    #[test]
    fn signature_display_and_debug_do_not_leak_key() {
        let signer = TaskSigner::new(b"top-secret-key-material-here!!!!");
        let dbg = format!("{signer:?}");
        assert!(!dbg.contains("top-secret"));
        assert!(dbg.contains("key_len"));
    }
}
