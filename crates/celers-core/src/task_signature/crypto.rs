//! The pure-Rust cryptographic primitives the signature scheme is built on.
//!
//! Split out of [`task_signature`](super) so the parent module keeps room for
//! the scheme itself. Nothing here is specific to CeleRS: [`Sha256`] is
//! FIPS 180-4, [`HmacSha256`] is RFC 2104/4231, and both are covered by their
//! published known-answer vectors in this file's tests.
//!
//! Everything public here is re-exported from
//! [`crate::task_signature`], which is the path the rest of the workspace
//! uses.

use super::SignatureError;
use std::fmt;

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
}
