//! Task message signature verification (HMAC-SHA256).
//!
//! This module lets a producer cryptographically sign a task message and a
//! consumer verify it, so that tampered or unsigned messages can be rejected
//! before execution. The signature is an [HMAC] over a *canonical*
//! serialization of the task's identifying fields — its id, name, positional
//! arguments and keyword arguments — so two semantically identical messages
//! always produce the same MAC regardless of incidental map ordering.
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

use crate::sanitize::TaskValue;
use serde::{Deserialize, Serialize};
use std::fmt;
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

/// The identifying fields of a task that participate in the signature.
///
/// Deliberately a *projection* of a full task message: only the fields that
/// determine task identity and behaviour are signed (id, name, args, kwargs),
/// so signatures are stable across transport metadata that may legitimately
/// change in flight (timestamps, retry counters, routing hints, …).
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
}

impl SignedFields {
    /// Create a new set of signed fields from a task id and name.
    #[must_use]
    pub fn new(id: Uuid, name: impl Into<String>) -> Self {
        Self {
            id,
            name: name.into(),
            args: Vec::new(),
            kwargs: Vec::new(),
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

    /// Produce the canonical byte serialization that is fed to the MAC.
    ///
    /// The encoding is length-prefixed and field-tagged so that no two
    /// distinct field layouts can collide (i.e. it is *unambiguous*):
    /// concatenating differently-split components can never yield the same
    /// byte stream. Keyword arguments are sorted by key so map ordering does
    /// not affect the result.
    #[must_use]
    pub fn canonical_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();

        // Domain separation tag + version, so signatures from this scheme can
        // never be confused with raw HMACs of arbitrary data.
        out.extend_from_slice(b"celers.task.sig.v1");

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
        let mut sorted: Vec<&(String, TaskValue)> = self.kwargs.iter().collect();
        sorted.sort_by(|x, y| x.0.cmp(&y.0));
        write_len(&mut out, b'K', sorted.len() as u64);
        for (key, value) in sorted {
            write_field(&mut out, b'k', key.as_bytes());
            let encoded = canonical_value_bytes(value);
            write_field(&mut out, b'v', &encoded);
        }

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
    #[must_use]
    pub fn is_valid(&self, fields: &SignedFields, signature: &TaskSignature) -> bool {
        self.verify(fields, signature).is_ok()
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
