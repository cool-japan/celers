//! Personally Identifiable Information (PII) detection and masking.
//!
//! This module scans task arguments (and arbitrary strings) for common kinds
//! of PII and can mask the matches it finds, returning a [`PiiReport`] that
//! records what was detected and where. It is useful for redacting sensitive
//! data before task arguments are written to logs, traces, or a result
//! backend.
//!
//! The detectors are **hand-written byte/char scanners** (no regular-expression
//! engine) so they are fast, allocation-light, and have predictable behaviour:
//!
//! * **Email addresses** — `local@domain.tld` with a conservative grammar.
//! * **Credit-card numbers** — 13–19 digit runs (optionally separated by
//!   single spaces or hyphens) that pass the **Luhn** checksum. Numbers that
//!   fail the Luhn check are deliberately *ignored* to cut false positives.
//! * **Phone numbers** — North-American / E.164-style numbers with optional
//!   `+`, country code, separators and parentheses, 10–15 digits.
//! * **US Social-Security-style numbers** — `NNN-NN-NNNN` (and the spaced /
//!   bare 9-digit forms) excluding ranges the SSA never issues.
//!
//! # Example
//!
//! ```rust
//! use celers_core::pii::{PiiDetector, PiiKind};
//!
//! let detector = PiiDetector::new();
//! let text = "contact me at jane.doe@example.com or 555-123-4567";
//!
//! let report = detector.scan_str(text);
//! assert!(report.contains_kind(PiiKind::Email));
//! assert!(report.contains_kind(PiiKind::Phone));
//!
//! let (masked, report) = detector.mask_str(text);
//! assert!(!masked.contains("jane.doe@example.com"));
//! assert_eq!(report.total(), 2);
//! ```

use crate::sanitize::TaskValue;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::fmt;

/// The category of a detected PII item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub enum PiiKind {
    /// An email address.
    Email,
    /// A credit-card number (validated with the Luhn algorithm).
    CreditCard,
    /// A telephone number.
    Phone,
    /// A US Social-Security-style number.
    Ssn,
}

impl PiiKind {
    /// Human-readable label.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            PiiKind::Email => "email",
            PiiKind::CreditCard => "credit_card",
            PiiKind::Phone => "phone",
            PiiKind::Ssn => "ssn",
        }
    }
}

impl fmt::Display for PiiKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.label())
    }
}

/// A single PII match found in a string.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PiiMatch {
    /// What kind of PII this is.
    pub kind: PiiKind,
    /// Byte offset of the match start within the scanned string.
    pub start: usize,
    /// Byte offset of the match end (exclusive) within the scanned string.
    pub end: usize,
    /// The exact matched text.
    pub matched: String,
}

impl PiiMatch {
    /// Length of the match in bytes.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.end - self.start
    }

    /// Whether the match is empty (never true for real matches; provided to
    /// satisfy the `len`/`is_empty` convention).
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.start == self.end
    }
}

/// A report describing all PII found during a scan.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PiiReport {
    /// All matches, in the order discovered.
    pub matches: Vec<PiiMatch>,
}

impl PiiReport {
    /// An empty report.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Total number of matches across all kinds.
    #[must_use]
    pub fn total(&self) -> usize {
        self.matches.len()
    }

    /// Whether any PII at all was found.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.matches.is_empty()
    }

    /// Whether at least one match of `kind` was found.
    #[must_use]
    pub fn contains_kind(&self, kind: PiiKind) -> bool {
        self.matches.iter().any(|m| m.kind == kind)
    }

    /// Number of matches of a particular kind.
    #[must_use]
    pub fn count_kind(&self, kind: PiiKind) -> usize {
        self.matches.iter().filter(|m| m.kind == kind).count()
    }

    /// The distinct set of kinds present in the report.
    #[must_use]
    pub fn kinds(&self) -> BTreeSet<PiiKind> {
        self.matches.iter().map(|m| m.kind).collect()
    }

    /// Append all matches from `other` into this report.
    pub fn merge(&mut self, other: PiiReport) {
        self.matches.extend(other.matches);
    }
}

/// Which PII categories a [`PiiDetector`] looks for, and how it masks them.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PiiConfig {
    /// Detect email addresses.
    pub detect_email: bool,
    /// Detect credit-card numbers (Luhn-validated).
    pub detect_credit_card: bool,
    /// Detect phone numbers.
    pub detect_phone: bool,
    /// Detect US SSN-style numbers.
    pub detect_ssn: bool,
    /// The mask substituted for each matched value.
    pub mask: String,
    /// If `true`, masking preserves the original match length by repeating the
    /// first character of `mask`; if `false`, the whole match is replaced by
    /// `mask` verbatim.
    pub preserve_length: bool,
    /// Scan numeric (`Int`/`UInt`) task values as digit strings.
    ///
    /// `TaskValue::from(serde_json::Value)` maps a JSON number to `Int`/`UInt`,
    /// so an ordinary body such as `{"card": 4111111111111111}` produced a value
    /// the detector skipped entirely — even though the very same digits would be
    /// caught instantly if they had arrived as a string.
    pub scan_numbers: bool,
    /// Scan `Bytes` values as lossy UTF-8 when they look like text.
    pub scan_bytes: bool,
    /// Scan object *keys* in addition to their values.
    ///
    /// A key such as `jane.doe@example.com` in a map is a realistic leak, but
    /// keys are also often structural, so this is opt-in.
    pub scan_keys: bool,
}

impl Default for PiiConfig {
    fn default() -> Self {
        Self {
            detect_email: true,
            detect_credit_card: true,
            detect_phone: true,
            detect_ssn: true,
            mask: "[REDACTED]".to_string(),
            preserve_length: false,
            scan_numbers: true,
            scan_bytes: true,
            scan_keys: false,
        }
    }
}

impl PiiConfig {
    /// Enable only email detection (others off). Useful as a base to build on.
    #[must_use]
    pub fn email_only() -> Self {
        Self {
            detect_email: true,
            detect_credit_card: false,
            detect_phone: false,
            detect_ssn: false,
            ..Self::default()
        }
    }

    /// Set the mask string.
    #[must_use]
    pub fn with_mask(mut self, mask: impl Into<String>) -> Self {
        self.mask = mask.into();
        self
    }

    /// Enable or disable length-preserving masking.
    #[must_use]
    pub const fn with_preserve_length(mut self, preserve: bool) -> Self {
        self.preserve_length = preserve;
        self
    }

    /// Enable or disable scanning of numeric values.
    #[must_use]
    pub const fn with_scan_numbers(mut self, scan: bool) -> Self {
        self.scan_numbers = scan;
        self
    }

    /// Enable or disable scanning of byte blobs as lossy UTF-8.
    #[must_use]
    pub const fn with_scan_bytes(mut self, scan: bool) -> Self {
        self.scan_bytes = scan;
        self
    }

    /// Enable or disable scanning of object keys.
    #[must_use]
    pub const fn with_scan_keys(mut self, scan: bool) -> Self {
        self.scan_keys = scan;
        self
    }
}

/// Render an integral [`TaskValue`] as its decimal digit string.
///
/// Returns `None` for anything that is not `Int`/`UInt`.
fn number_as_text(value: &TaskValue) -> Option<String> {
    match value {
        TaskValue::Int(v) => Some(v.to_string()),
        TaskValue::UInt(v) => Some(v.to_string()),
        _ => None,
    }
}

/// Detects and masks PII in strings and task values.
#[derive(Debug, Clone)]
pub struct PiiDetector {
    config: PiiConfig,
}

impl Default for PiiDetector {
    fn default() -> Self {
        Self::new()
    }
}

impl PiiDetector {
    /// Create a detector with default configuration (all categories on).
    #[must_use]
    pub fn new() -> Self {
        Self {
            config: PiiConfig::default(),
        }
    }

    /// Create a detector with a custom configuration.
    #[must_use]
    pub fn with_config(config: PiiConfig) -> Self {
        Self { config }
    }

    /// Borrow the active configuration.
    #[must_use]
    pub const fn config(&self) -> &PiiConfig {
        &self.config
    }

    /// Scan a string and return every PII match (non-overlapping, ordered by
    /// position; on a position tie, earlier-listed kinds win).
    #[must_use]
    pub fn scan_str(&self, text: &str) -> PiiReport {
        let mut candidates: Vec<PiiMatch> = Vec::new();

        if self.config.detect_email {
            scan_emails(text, &mut candidates);
        }
        if self.config.detect_credit_card {
            scan_credit_cards(text, &mut candidates);
        }
        if self.config.detect_ssn {
            scan_ssn(text, &mut candidates);
        }
        if self.config.detect_phone {
            scan_phones(text, &mut candidates);
        }

        // Resolve overlaps: sort by start, then by a kind priority that prefers
        // the more specific detector, then by longer match. Keep the first of
        // any overlapping group.
        candidates.sort_by(|a, b| {
            a.start
                .cmp(&b.start)
                .then_with(|| kind_priority(a.kind).cmp(&kind_priority(b.kind)))
                .then_with(|| b.len().cmp(&a.len()))
        });

        let mut report = PiiReport::new();
        let mut last_end = 0usize;
        for m in candidates {
            if m.start >= last_end {
                last_end = m.end;
                report.matches.push(m);
            }
        }
        report
    }

    /// Mask all PII in a string, returning the masked string and the report of
    /// what was masked.
    #[must_use]
    pub fn mask_str(&self, text: &str) -> (String, PiiReport) {
        let report = self.scan_str(text);
        if report.is_empty() {
            return (text.to_string(), report);
        }

        // Matches are already non-overlapping and ordered by start.
        let mut out = String::with_capacity(text.len());
        let mut cursor = 0usize;
        for m in &report.matches {
            if m.start > cursor {
                out.push_str(&text[cursor..m.start]);
            }
            out.push_str(&self.mask_for(m));
            cursor = m.end;
        }
        if cursor < text.len() {
            out.push_str(&text[cursor..]);
        }
        (out, report)
    }

    /// Recursively scan a [`TaskValue`], accumulating matches from every string
    /// it contains (including object keys' values, but not the keys
    /// themselves).
    #[must_use]
    pub fn scan_value(&self, value: &TaskValue) -> PiiReport {
        let mut report = PiiReport::new();
        self.scan_value_into(value, &mut report);
        report
    }

    fn scan_value_into(&self, value: &TaskValue, report: &mut PiiReport) {
        match value {
            TaskValue::String(s) => report.merge(self.scan_str(s)),
            TaskValue::Int(_) | TaskValue::UInt(_) => {
                if self.config.scan_numbers {
                    if let Some(text) = number_as_text(value) {
                        report.merge(self.scan_str(&text));
                    }
                }
            }
            TaskValue::Bytes(bytes) => {
                if self.config.scan_bytes {
                    if let Ok(text) = std::str::from_utf8(bytes) {
                        report.merge(self.scan_str(text));
                    }
                }
            }
            TaskValue::Array(items) => {
                for item in items {
                    self.scan_value_into(item, report);
                }
            }
            TaskValue::Object(entries) => {
                for (key, v) in entries {
                    if self.config.scan_keys {
                        report.merge(self.scan_str(key));
                    }
                    self.scan_value_into(v, report);
                }
            }
            TaskValue::Null | TaskValue::Bool(_) | TaskValue::Float(_) => {}
        }
    }

    /// Recursively mask PII inside a [`TaskValue`] in place, returning a
    /// combined report of everything masked.
    pub fn mask_value(&self, value: &mut TaskValue) -> PiiReport {
        let mut report = PiiReport::new();
        self.mask_value_into(value, &mut report);
        report
    }

    fn mask_value_into(&self, value: &mut TaskValue, report: &mut PiiReport) {
        match value {
            TaskValue::String(s) => {
                let (masked, found) = self.mask_str(s);
                if !found.is_empty() {
                    *s = masked;
                    report.merge(found);
                }
            }
            // A masked number can no longer be a number, so it becomes a string.
            TaskValue::Int(_) | TaskValue::UInt(_) => {
                if self.config.scan_numbers {
                    if let Some(text) = number_as_text(value) {
                        let (masked, found) = self.mask_str(&text);
                        if !found.is_empty() {
                            *value = TaskValue::String(masked);
                            report.merge(found);
                        }
                    }
                }
            }
            TaskValue::Bytes(bytes) => {
                if self.config.scan_bytes {
                    if let Ok(text) = std::str::from_utf8(bytes) {
                        let (masked, found) = self.mask_str(text);
                        if !found.is_empty() {
                            *value = TaskValue::String(masked);
                            report.merge(found);
                        }
                    }
                }
            }
            TaskValue::Array(items) => {
                for item in items.iter_mut() {
                    self.mask_value_into(item, report);
                }
            }
            TaskValue::Object(entries) => {
                for (key, v) in entries.iter_mut() {
                    if self.config.scan_keys {
                        let (masked, found) = self.mask_str(key);
                        if !found.is_empty() {
                            *key = masked;
                            report.merge(found);
                        }
                    }
                    self.mask_value_into(v, report);
                }
            }
            TaskValue::Null | TaskValue::Bool(_) | TaskValue::Float(_) => {}
        }
    }

    /// Scan a full task call (positional + keyword arguments).
    #[must_use]
    pub fn scan_call(&self, args: &[TaskValue], kwargs: &[(String, TaskValue)]) -> PiiReport {
        let mut report = PiiReport::new();
        for v in args {
            self.scan_value_into(v, &mut report);
        }
        for (_, v) in kwargs {
            self.scan_value_into(v, &mut report);
        }
        report
    }

    /// Mask a full task call in place.
    pub fn mask_call(
        &self,
        args: &mut [TaskValue],
        kwargs: &mut [(String, TaskValue)],
    ) -> PiiReport {
        let mut report = PiiReport::new();
        for v in args.iter_mut() {
            self.mask_value_into(v, &mut report);
        }
        for (_, v) in kwargs.iter_mut() {
            self.mask_value_into(v, &mut report);
        }
        report
    }

    /// Produce the replacement text for a single match.
    fn mask_for(&self, m: &PiiMatch) -> String {
        if self.config.preserve_length {
            let fill = self.config.mask.chars().next().unwrap_or('*');
            std::iter::repeat_n(fill, m.matched.chars().count()).collect()
        } else {
            self.config.mask.clone()
        }
    }
}

/// Detector priority used to break overlap ties (lower = preferred).
const fn kind_priority(kind: PiiKind) -> u8 {
    match kind {
        PiiKind::Email => 0,
        PiiKind::CreditCard => 1,
        PiiKind::Ssn => 2,
        PiiKind::Phone => 3,
    }
}

// ===========================================================================
// Character classification helpers
// ===========================================================================

#[inline]
const fn is_ascii_digit(b: u8) -> bool {
    b.is_ascii_digit()
}

/// `true` for characters allowed in the local part of an email (a pragmatic
/// subset of RFC 5321 dot-atom plus common specials).
#[inline]
fn is_email_local(b: u8) -> bool {
    b.is_ascii_alphanumeric()
        || matches!(
            b,
            b'.' | b'_'
                | b'%'
                | b'+'
                | b'-'
                | b'!'
                | b'#'
                | b'$'
                | b'&'
                | b'\''
                | b'*'
                | b'='
                | b'?'
                | b'^'
                | b'`'
                | b'{'
                | b'|'
                | b'}'
                | b'~'
        )
}

/// `true` for characters allowed inside an email domain label or between
/// labels (alphanumeric, hyphen, dot).
#[inline]
fn is_email_domain(b: u8) -> bool {
    b.is_ascii_alphanumeric() || b == b'-' || b == b'.'
}

// ===========================================================================
// Email scanner
// ===========================================================================

fn scan_emails(text: &str, out: &mut Vec<PiiMatch>) {
    let bytes = text.as_bytes();
    let n = bytes.len();
    let mut i = 0usize;

    while i < n {
        if bytes[i] != b'@' {
            i += 1;
            continue;
        }
        let at = i;

        // Walk backwards over the local part.
        let mut start = at;
        while start > 0 && is_email_local(bytes[start - 1]) {
            start -= 1;
        }
        // Local part must be non-empty and not start/end with a dot.
        if start == at || bytes[start] == b'.' || bytes[at - 1] == b'.' {
            i += 1;
            continue;
        }

        // Walk forwards over the domain.
        let mut end = at + 1;
        while end < n && is_email_domain(bytes[end]) {
            end += 1;
        }
        let domain = &bytes[at + 1..end];
        if let Some(domain_len) = valid_email_domain(domain) {
            let real_end = at + 1 + domain_len;
            out.push(PiiMatch {
                kind: PiiKind::Email,
                start,
                end: real_end,
                matched: text[start..real_end].to_string(),
            });
            i = real_end;
        } else {
            i = at + 1;
        }
    }
}

/// Validate an email domain candidate (`bytes` is everything matched after the
/// `@`). Returns the *trimmed* length to keep (so a trailing dot or partial
/// final label is excluded), or `None` if it isn't a valid domain.
///
/// Rules: at least two dot-separated labels, each label non-empty, not
/// starting/ending with a hyphen, and the final label (the TLD) at least two
/// ASCII letters.
fn valid_email_domain(bytes: &[u8]) -> Option<usize> {
    // Trim trailing dots/hyphens which are not valid domain terminators.
    let mut end = bytes.len();
    while end > 0 && (bytes[end - 1] == b'.' || bytes[end - 1] == b'-') {
        end -= 1;
    }
    let domain = &bytes[..end];
    if domain.is_empty() {
        return None;
    }

    let labels: Vec<&[u8]> = domain.split(|&b| b == b'.').collect();
    if labels.len() < 2 {
        return None;
    }
    for label in &labels {
        if label.is_empty() {
            return None;
        }
        if label[0] == b'-' || label[label.len() - 1] == b'-' {
            return None;
        }
        if !label
            .iter()
            .all(|&b| b.is_ascii_alphanumeric() || b == b'-')
        {
            return None;
        }
    }
    // TLD must be all letters, length >= 2.
    let tld = labels[labels.len() - 1];
    if tld.len() < 2 || !tld.iter().all(u8::is_ascii_alphabetic) {
        return None;
    }

    Some(end)
}

// ===========================================================================
// Credit-card scanner (Luhn-validated)
// ===========================================================================

/// Validate a sequence of digit characters with the Luhn (mod-10) algorithm.
///
/// `digits` must contain only ASCII digit bytes. Returns `true` if the
/// checksum is valid. An all-zero or empty sequence is not considered a valid
/// card.
#[must_use]
pub fn luhn_check(digits: &[u8]) -> bool {
    if digits.is_empty() {
        return false;
    }
    let mut sum = 0u32;
    let mut double = false;
    let mut all_zero = true;
    // Process from the rightmost digit.
    for &d in digits.iter().rev() {
        if !d.is_ascii_digit() {
            return false;
        }
        let mut v = u32::from(d - b'0');
        if v != 0 {
            all_zero = false;
        }
        if double {
            v *= 2;
            if v > 9 {
                v -= 9;
            }
        }
        sum += v;
        double = !double;
    }
    !all_zero && sum.is_multiple_of(10)
}

fn scan_credit_cards(text: &str, out: &mut Vec<PiiMatch>) {
    let bytes = text.as_bytes();
    let n = bytes.len();
    let mut i = 0usize;

    while i < n {
        if !is_ascii_digit(bytes[i]) {
            i += 1;
            continue;
        }
        // Reject if the previous byte is a digit (we want maximal runs only
        // anchored at a non-digit boundary).
        if i > 0 && is_ascii_digit(bytes[i - 1]) {
            i += 1;
            continue;
        }

        // Consume a run of digits with optional single space/hyphen separators
        // between digit groups.
        let start = i;
        let mut j = i;
        let mut digits: Vec<u8> = Vec::with_capacity(19);
        let mut last_digit_end = i;
        while j < n {
            if is_ascii_digit(bytes[j]) {
                digits.push(bytes[j]);
                j += 1;
                last_digit_end = j;
                if digits.len() > 19 {
                    break;
                }
            } else if (bytes[j] == b' ' || bytes[j] == b'-')
                && j + 1 < n
                && is_ascii_digit(bytes[j + 1])
            {
                // Separator between two digit groups; keep scanning.
                j += 1;
            } else {
                break;
            }
        }

        // A credit card has 13..=19 digits. Ensure the character right after
        // the last digit is not another digit (it cannot be, by construction).
        let count = digits.len();
        if (13..=19).contains(&count) && luhn_check(&digits) {
            // Ensure we are not in the middle of a longer pure-digit run.
            let after_ok = last_digit_end >= n || !is_ascii_digit(bytes[last_digit_end]);
            let before_ok = start == 0 || !is_ascii_digit(bytes[start - 1]);
            if after_ok && before_ok {
                out.push(PiiMatch {
                    kind: PiiKind::CreditCard,
                    start,
                    end: last_digit_end,
                    matched: text[start..last_digit_end].to_string(),
                });
                i = last_digit_end;
                continue;
            }
        }
        // Advance past this run to avoid quadratic rescanning.
        i = if last_digit_end > start {
            last_digit_end
        } else {
            start + 1
        };
    }
}

// ===========================================================================
// SSN scanner (US Social-Security-style)
// ===========================================================================

/// Validate the three SSN component values against ranges the SSA never issues.
fn valid_ssn_parts(area: u16, group: u8, serial: u16) -> bool {
    // Area 000, 666, and 900-999 are never assigned.
    if area == 0 || area == 666 || area >= 900 {
        return false;
    }
    // Group 00 is never used.
    if group == 0 {
        return false;
    }
    // Serial 0000 is never used.
    if serial == 0 {
        return false;
    }
    true
}

fn scan_ssn(text: &str, out: &mut Vec<PiiMatch>) {
    let bytes = text.as_bytes();
    let n = bytes.len();
    let mut i = 0usize;

    while i + 9 <= n {
        if !is_ascii_digit(bytes[i]) {
            i += 1;
            continue;
        }
        if i > 0 && is_ascii_digit(bytes[i - 1]) {
            i += 1;
            continue;
        }

        // Try the separated form NNN-NN-NNNN (with '-' or ' ' separators).
        if let Some((end, area, group, serial)) = parse_ssn_separated(bytes, i) {
            if valid_ssn_parts(area, group, serial)
                && (end >= n || !is_ascii_digit(bytes[end]))
                && (end >= n || bytes[end] != b'-')
            {
                out.push(PiiMatch {
                    kind: PiiKind::Ssn,
                    start: i,
                    end,
                    matched: text[i..end].to_string(),
                });
                i = end;
                continue;
            }
        }

        // Try the bare 9-digit form, but only when it is exactly 9 digits long
        // (not part of a longer number).
        if i + 9 <= n
            && bytes[i..i + 9].iter().all(|&b| is_ascii_digit(b))
            && (i + 9 >= n || !is_ascii_digit(bytes[i + 9]))
        {
            let area = parse_u16(&bytes[i..i + 3]);
            let group = (bytes[i + 3] - b'0') * 10 + (bytes[i + 4] - b'0');
            let serial = parse_u16(&bytes[i + 5..i + 9]);
            if valid_ssn_parts(area, group, serial) {
                out.push(PiiMatch {
                    kind: PiiKind::Ssn,
                    start: i,
                    end: i + 9,
                    matched: text[i..i + 9].to_string(),
                });
                i += 9;
                continue;
            }
        }

        i += 1;
    }
}

/// Parse `NNN<sep>NN<sep>NNNN` starting at `pos`, where `<sep>` is a single
/// `-` or space (the same separator must be used for both, mirroring real
/// formatting). Returns `(end, area, group, serial)`.
fn parse_ssn_separated(bytes: &[u8], pos: usize) -> Option<(usize, u16, u8, u16)> {
    let n = bytes.len();
    // Need at least 11 chars: 3 + 1 + 2 + 1 + 4.
    if pos + 11 > n {
        return None;
    }
    // Area: 3 digits.
    if !bytes[pos..pos + 3].iter().all(|&b| is_ascii_digit(b)) {
        return None;
    }
    let sep = bytes[pos + 3];
    if sep != b'-' && sep != b' ' {
        return None;
    }
    // Group: 2 digits.
    if !bytes[pos + 4..pos + 6].iter().all(|&b| is_ascii_digit(b)) {
        return None;
    }
    if bytes[pos + 6] != sep {
        return None;
    }
    // Serial: 4 digits.
    if !bytes[pos + 7..pos + 11].iter().all(|&b| is_ascii_digit(b)) {
        return None;
    }
    let area = parse_u16(&bytes[pos..pos + 3]);
    let group = (bytes[pos + 4] - b'0') * 10 + (bytes[pos + 5] - b'0');
    let serial = parse_u16(&bytes[pos + 7..pos + 11]);
    Some((pos + 11, area, group, serial))
}

#[inline]
fn parse_u16(digits: &[u8]) -> u16 {
    let mut v = 0u16;
    for &d in digits {
        v = v * 10 + u16::from(d - b'0');
    }
    v
}

// ===========================================================================
// Phone scanner
// ===========================================================================

/// Scan for telephone numbers.
///
/// Recognizes a leading optional `+`, then runs of digits interspersed with the
/// usual separators (space, `-`, `.`, parentheses). The total digit count must
/// be 10..=15 (E.164 allows up to 15). To reduce false positives the candidate
/// must contain at least one separator or a leading `+` — bare 10–15 digit runs
/// are left to the SSN/credit-card detectors or skipped.
fn scan_phones(text: &str, out: &mut Vec<PiiMatch>) {
    let bytes = text.as_bytes();
    let n = bytes.len();
    let mut i = 0usize;

    while i < n {
        let has_plus = bytes[i] == b'+';
        let digit_start = if has_plus { i + 1 } else { i };
        if digit_start >= n || !is_ascii_digit(bytes[digit_start]) {
            i += 1;
            continue;
        }
        // Anchor at a non-digit, non-plus boundary.
        if i > 0 && (is_ascii_digit(bytes[i - 1]) || bytes[i - 1] == b'+') {
            i += 1;
            continue;
        }

        let start = i;
        let mut j = digit_start;
        let mut digit_count = 0usize;
        let mut has_separator = false;
        let mut last_digit_end = digit_start;

        while j < n {
            let b = bytes[j];
            if is_ascii_digit(b) {
                digit_count += 1;
                j += 1;
                last_digit_end = j;
                if digit_count > 15 {
                    break;
                }
            } else if matches!(b, b' ' | b'-' | b'.' | b'(' | b')')
                && j + 1 < n
                && (is_ascii_digit(bytes[j + 1])
                    || matches!(bytes[j + 1], b' ' | b'-' | b'.' | b'(' | b')'))
            {
                has_separator = true;
                j += 1;
            } else {
                break;
            }
        }

        let qualifies = (10..=15).contains(&digit_count) && (has_separator || has_plus);
        if qualifies {
            let end = last_digit_end;
            out.push(PiiMatch {
                kind: PiiKind::Phone,
                start,
                end,
                matched: text[start..end].to_string(),
            });
            i = end;
        } else {
            i = if last_digit_end > start {
                last_digit_end
            } else {
                start + 1
            };
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ----- Luhn -----

    #[test]
    fn luhn_known_valid() {
        // Classic Visa test number.
        assert!(luhn_check(b"4111111111111111"));
        // Mastercard test number.
        assert!(luhn_check(b"5500005555555559"));
        // Amex test number.
        assert!(luhn_check(b"378282246310005"));
        // Discover.
        assert!(luhn_check(b"6011000990139424"));
    }

    #[test]
    fn luhn_known_invalid() {
        assert!(!luhn_check(b"4111111111111112"));
        assert!(!luhn_check(b"1234567812345678"));
        assert!(!luhn_check(b"0000000000000000"));
        assert!(!luhn_check(b""));
    }

    // ----- Email -----

    #[test]
    fn detects_simple_email() {
        let d = PiiDetector::new();
        let r = d.scan_str("reach me: jane.doe@example.com please");
        assert_eq!(r.count_kind(PiiKind::Email), 1);
        assert_eq!(r.matches[0].matched, "jane.doe@example.com");
    }

    #[test]
    fn detects_email_with_plus_and_subdomain() {
        let d = PiiDetector::new();
        let r = d.scan_str("a+b@mail.sub.example.co.uk");
        assert_eq!(r.count_kind(PiiKind::Email), 1);
        assert_eq!(r.matches[0].matched, "a+b@mail.sub.example.co.uk");
    }

    #[test]
    fn ignores_non_email_at() {
        let d = PiiDetector::new();
        assert!(d.scan_str("@handle and user@ and @ alone").is_empty());
        // No TLD.
        assert!(!d.scan_str("foo@localhost").contains_kind(PiiKind::Email));
    }

    #[test]
    fn email_trailing_dot_trimmed() {
        let d = PiiDetector::new();
        let r = d.scan_str("ping bob@example.com. done");
        assert_eq!(r.matches[0].matched, "bob@example.com");
    }

    // ----- Credit card -----

    #[test]
    fn detects_credit_card_plain() {
        let d = PiiDetector::new();
        let r = d.scan_str("card 4111111111111111 on file");
        assert_eq!(r.count_kind(PiiKind::CreditCard), 1);
        assert_eq!(r.matches[0].matched, "4111111111111111");
    }

    #[test]
    fn detects_credit_card_with_separators() {
        let d = PiiDetector::new();
        let r = d.scan_str("4111-1111-1111-1111");
        assert_eq!(r.count_kind(PiiKind::CreditCard), 1);
        assert_eq!(r.matches[0].matched, "4111-1111-1111-1111");

        let r2 = d.scan_str("4111 1111 1111 1111");
        assert_eq!(r2.count_kind(PiiKind::CreditCard), 1);
    }

    #[test]
    fn ignores_invalid_luhn_card() {
        let d = PiiDetector::new();
        // 16 digits but fails Luhn.
        let r = d.scan_str("4111111111111112");
        assert!(!r.contains_kind(PiiKind::CreditCard));
    }

    #[test]
    fn ignores_too_short_or_too_long_digit_runs() {
        let d = PiiDetector::new();
        // 12 digits (too short for a card), no separators -> nothing.
        assert!(!d
            .scan_str("411111111111")
            .contains_kind(PiiKind::CreditCard));
        // 20+ digits -> not a card.
        assert!(!d
            .scan_str("411111111111111111111")
            .contains_kind(PiiKind::CreditCard));
    }

    // ----- SSN -----

    #[test]
    fn detects_ssn_dashed() {
        let d = PiiDetector::new();
        let r = d.scan_str("ssn: 123-45-6789");
        assert_eq!(r.count_kind(PiiKind::Ssn), 1);
        assert_eq!(r.matches[0].matched, "123-45-6789");
    }

    #[test]
    fn detects_ssn_spaced_and_bare() {
        let d = PiiDetector::new();
        assert!(d.scan_str("123 45 6789").contains_kind(PiiKind::Ssn));
        assert!(d.scan_str("078051120").contains_kind(PiiKind::Ssn));
    }

    #[test]
    fn ignores_invalid_ssn_ranges() {
        let d = PiiDetector::new();
        // Area 000 invalid.
        assert!(!d.scan_str("000-45-6789").contains_kind(PiiKind::Ssn));
        // Area 666 invalid.
        assert!(!d.scan_str("666-45-6789").contains_kind(PiiKind::Ssn));
        // Group 00 invalid.
        assert!(!d.scan_str("123-00-6789").contains_kind(PiiKind::Ssn));
        // Serial 0000 invalid.
        assert!(!d.scan_str("123-45-0000").contains_kind(PiiKind::Ssn));
        // Area 900+ invalid.
        assert!(!d.scan_str("900-45-6789").contains_kind(PiiKind::Ssn));
    }

    // ----- Phone -----

    #[test]
    fn detects_phone_us_formats() {
        let d = PiiDetector::new();
        assert!(d
            .scan_str("call 555-123-4567")
            .contains_kind(PiiKind::Phone));
        assert!(d.scan_str("(555) 123-4567").contains_kind(PiiKind::Phone));
        assert!(d.scan_str("+1 555 123 4567").contains_kind(PiiKind::Phone));
        assert!(d.scan_str("+44 20 7946 0958").contains_kind(PiiKind::Phone));
    }

    #[test]
    fn phone_requires_separator_or_plus() {
        let d = PiiDetector::default();
        // A bare 10-digit run with no separators and no plus must NOT be a
        // phone match (avoids matching arbitrary numbers).
        let r = d.scan_str("5551234567");
        assert!(!r.contains_kind(PiiKind::Phone));
    }

    // ----- Masking -----

    #[test]
    fn mask_replaces_all_pii() {
        let d = PiiDetector::new();
        let text = "email a@b.com card 4111111111111111 ssn 123-45-6789 tel 555-123-4567";
        let (masked, report) = d.mask_str(text);
        assert!(!masked.contains("a@b.com"));
        assert!(!masked.contains("4111111111111111"));
        assert!(!masked.contains("123-45-6789"));
        assert!(!masked.contains("555-123-4567"));
        assert_eq!(report.total(), 4);
        assert!(report.contains_kind(PiiKind::Email));
        assert!(report.contains_kind(PiiKind::CreditCard));
        assert!(report.contains_kind(PiiKind::Ssn));
        assert!(report.contains_kind(PiiKind::Phone));
    }

    #[test]
    fn mask_preserve_length() {
        let config = PiiConfig::default()
            .with_mask("*")
            .with_preserve_length(true);
        let d = PiiDetector::with_config(config);
        let (masked, _) = d.mask_str("a@b.com");
        assert_eq!(masked, "*".repeat("a@b.com".chars().count()));
    }

    #[test]
    fn mask_preserves_surrounding_text() {
        let d = PiiDetector::new();
        let (masked, _) = d.mask_str("before a@b.com after");
        assert!(masked.starts_with("before "));
        assert!(masked.ends_with(" after"));
        assert!(masked.contains("[REDACTED]"));
    }

    #[test]
    fn clean_text_unchanged() {
        let d = PiiDetector::new();
        let (masked, report) = d.mask_str("nothing sensitive here at all");
        assert_eq!(masked, "nothing sensitive here at all");
        assert!(report.is_empty());
    }

    // ----- TaskValue integration -----

    #[test]
    fn scan_and_mask_task_values() {
        let d = PiiDetector::new();
        let mut value = TaskValue::Object(vec![
            ("name".to_string(), TaskValue::from("Alice")),
            ("contact".to_string(), TaskValue::from("alice@example.com")),
            (
                "cards".to_string(),
                TaskValue::Array(vec![TaskValue::from("4111111111111111")]),
            ),
        ]);

        let scan = d.scan_value(&value);
        assert_eq!(scan.count_kind(PiiKind::Email), 1);
        assert_eq!(scan.count_kind(PiiKind::CreditCard), 1);

        let report = d.mask_value(&mut value);
        assert_eq!(report.total(), 2);
        let json = serde_json::to_string(&value).unwrap();
        assert!(!json.contains("alice@example.com"));
        assert!(!json.contains("4111111111111111"));
    }

    #[test]
    fn scan_call_args_and_kwargs() {
        let d = PiiDetector::new();
        let args = vec![TaskValue::from("ping bob@host.org")];
        let kwargs = vec![("ssn".to_string(), TaskValue::from("123-45-6789"))];
        let report = d.scan_call(&args, &kwargs);
        assert!(report.contains_kind(PiiKind::Email));
        assert!(report.contains_kind(PiiKind::Ssn));
    }

    #[test]
    fn report_helpers() {
        let d = PiiDetector::new();
        let report = d.scan_str("a@b.com c@d.com");
        assert_eq!(report.total(), 2);
        assert_eq!(report.count_kind(PiiKind::Email), 2);
        assert_eq!(report.kinds().len(), 1);
        assert!(!report.is_empty());
    }

    #[test]
    fn config_serde_roundtrip() {
        let config = PiiConfig::email_only().with_mask("###");
        let json = serde_json::to_string(&config).expect("serialize");
        let restored: PiiConfig = serde_json::from_str(&json).expect("deserialize");
        assert!(restored.detect_email);
        assert!(!restored.detect_phone);
        assert_eq!(restored.mask, "###");
    }

    #[test]
    fn disabled_detectors_skip() {
        let d = PiiDetector::with_config(PiiConfig::email_only());
        let r = d.scan_str("card 4111111111111111 mail x@y.com");
        assert!(r.contains_kind(PiiKind::Email));
        assert!(!r.contains_kind(PiiKind::CreditCard));
    }

    #[test]
    fn non_overlapping_matches() {
        let d = PiiDetector::new();
        // An SSN-looking sequence should be reported once, not double-counted
        // by the phone detector overlapping it.
        let r = d.scan_str("123-45-6789");
        assert_eq!(r.total(), 1);
        assert_eq!(r.matches[0].kind, PiiKind::Ssn);
    }

    // ------------------------------------------------------------------
    // Regression tests
    // ------------------------------------------------------------------

    /// Regression: only `TaskValue::String` was inspected, so a JSON body such
    /// as `{"card": 4111111111111111}` — which maps to `TaskValue::Int` — was
    /// reported as containing no PII and passed through masking unchanged.
    #[test]
    fn numeric_values_are_scanned_and_masked() {
        let detector = PiiDetector::default();

        let card = TaskValue::Int(4_111_111_111_111_111);
        let report = detector.scan_value(&card);
        assert!(
            report.contains_kind(PiiKind::CreditCard),
            "a Luhn-valid card number must be detected as a number too"
        );

        let mut value = card.clone();
        let report = detector.mask_value(&mut value);
        assert!(report.contains_kind(PiiKind::CreditCard));
        assert!(
            matches!(&value, TaskValue::String(s) if s.contains("[REDACTED]")),
            "masked value: {value:?}"
        );

        // Unsigned values too.
        let unsigned = TaskValue::UInt(4_111_111_111_111_111);
        assert!(detector
            .scan_value(&unsigned)
            .contains_kind(PiiKind::CreditCard));

        // A plain number that is not PII is left completely alone.
        let mut plain = TaskValue::Int(42);
        assert!(detector.mask_value(&mut plain).is_empty());
        assert_eq!(plain, TaskValue::Int(42));
    }

    #[test]
    fn numeric_scanning_is_configurable() {
        let detector = PiiDetector::with_config(PiiConfig::default().with_scan_numbers(false));
        let card = TaskValue::Int(4_111_111_111_111_111);
        assert!(detector.scan_value(&card).is_empty());

        let mut value = card.clone();
        assert!(detector.mask_value(&mut value).is_empty());
        assert_eq!(value, card);
    }

    #[test]
    fn numeric_values_inside_containers_are_scanned() {
        let detector = PiiDetector::default();
        let payload = TaskValue::Object(vec![
            ("card".to_string(), TaskValue::Int(4_111_111_111_111_111)),
            (
                "nested".to_string(),
                TaskValue::Array(vec![TaskValue::UInt(4_111_111_111_111_111)]),
            ),
        ]);
        let report = detector.scan_value(&payload);
        assert_eq!(report.count_kind(PiiKind::CreditCard), 2);
    }

    #[test]
    fn byte_blobs_are_scanned_as_text() {
        let detector = PiiDetector::default();
        let bytes = TaskValue::Bytes(b"contact jane.doe@example.com".to_vec());
        assert!(detector.scan_value(&bytes).contains_kind(PiiKind::Email));

        let mut value = bytes;
        let report = detector.mask_value(&mut value);
        assert!(report.contains_kind(PiiKind::Email));
        assert!(matches!(&value, TaskValue::String(s) if !s.contains("jane.doe@example.com")));

        // Non-UTF-8 blobs are left alone rather than mangled.
        let mut binary = TaskValue::Bytes(vec![0xff, 0xfe, 0x00]);
        assert!(detector.mask_value(&mut binary).is_empty());
        assert_eq!(binary, TaskValue::Bytes(vec![0xff, 0xfe, 0x00]));
    }

    #[test]
    fn object_keys_are_scanned_when_enabled() {
        let payload = || {
            TaskValue::Object(vec![(
                "jane.doe@example.com".to_string(),
                TaskValue::from("ok"),
            )])
        };

        // Off by default: keys are often structural.
        let default_detector = PiiDetector::default();
        assert!(default_detector.scan_value(&payload()).is_empty());

        let detector = PiiDetector::with_config(PiiConfig::default().with_scan_keys(true));
        assert!(detector
            .scan_value(&payload())
            .contains_kind(PiiKind::Email));

        let mut value = payload();
        let report = detector.mask_value(&mut value);
        assert!(report.contains_kind(PiiKind::Email));
        match &value {
            TaskValue::Object(entries) => {
                assert!(!entries[0].0.contains("jane.doe@example.com"));
            }
            other => panic!("expected an object, got {other:?}"),
        }
    }

    #[test]
    fn mask_call_covers_numeric_arguments() {
        let detector = PiiDetector::default();
        let mut args = vec![TaskValue::Int(4_111_111_111_111_111)];
        let mut kwargs = vec![("ssn".to_string(), TaskValue::from("123-45-6789"))];

        let report = detector.mask_call(&mut args, &mut kwargs);
        assert!(report.contains_kind(PiiKind::CreditCard));
        assert!(report.contains_kind(PiiKind::Ssn));
        assert!(matches!(&args[0], TaskValue::String(s) if s.contains("[REDACTED]")));
    }
}
