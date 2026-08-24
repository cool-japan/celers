// Copyright (c) 2026 COOLJAPAN OU (Team KitaSan)
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Retry policy helpers for the SQS broker.
//!
//! This module contains the *pure* pieces of the broker's retry behaviour so
//! that they can be unit tested without talking to AWS:
//!
//! - [`backoff_delay_ms`] computes a bounded, overflow-free exponential backoff
//!   delay (the previous implementation used `base * (1 << attempt)` which
//!   overflows for large attempt counts).
//! - [`is_retryable_error`] classifies a [`BrokerError`] so that client-side
//!   validation failures (for example a FIFO `MissingParameter`) fail fast
//!   instead of burning the full retry budget.
//! - [`is_retryable_batch_code`] does the same for the per-entry error codes
//!   returned by the SQS `*Batch` APIs.
//!
//! # At-least-once semantics
//!
//! Retrying `SendMessage` against a *standard* queue can produce a duplicate
//! when the original request was accepted but the response was lost. SQS
//! standard queues offer no deduplication id, so the broker documents
//! at-least-once delivery rather than pretending otherwise. FIFO queues do get
//! a stable `MessageDeduplicationId` derived from the task id (see
//! [`crate::fifo`]), which makes retries there genuinely idempotent.

use celers_kombu::BrokerError;

/// Maximum exponent used when computing the exponential backoff delay.
///
/// `1u64 << 20` is roughly one million; combined with the saturating
/// multiplication and the [`MAX_BACKOFF_DELAY_MS`] ceiling this makes the
/// computation total for every possible `attempt`.
pub const MAX_BACKOFF_EXPONENT: u32 = 20;

/// Upper bound for a single backoff sleep, in milliseconds (60 seconds).
pub const MAX_BACKOFF_DELAY_MS: u64 = 60_000;

/// Hard ceiling applied to the user supplied `max_retries`.
pub const MAX_RETRY_ATTEMPTS: u32 = 20;

/// Compute the backoff delay for a retry attempt.
///
/// The delay is `base_delay_ms * 2^attempt`, with the exponent clamped to
/// [`MAX_BACKOFF_EXPONENT`], the multiplication saturating, and the result
/// capped at [`MAX_BACKOFF_DELAY_MS`]. A deterministic jitter fraction in
/// `0..=jitter_permille` (per mille of the computed delay) is subtracted so
/// that concurrent workers do not retry in lockstep.
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::retry_policy::backoff_delay_ms;
///
/// // Deterministic when no jitter is requested.
/// assert_eq!(backoff_delay_ms(100, 0, 0), 100);
/// assert_eq!(backoff_delay_ms(100, 1, 0), 200);
/// assert_eq!(backoff_delay_ms(100, 3, 0), 800);
///
/// // Never overflows, never exceeds the 60s ceiling.
/// assert_eq!(backoff_delay_ms(u64::MAX, u32::MAX, 0), 60_000);
/// ```
pub fn backoff_delay_ms(base_delay_ms: u64, attempt: u32, jitter_permille: u64) -> u64 {
    let exponent = attempt.min(MAX_BACKOFF_EXPONENT);
    let delay = base_delay_ms
        .saturating_mul(1u64 << exponent)
        .min(MAX_BACKOFF_DELAY_MS);

    if jitter_permille == 0 || delay == 0 {
        return delay;
    }

    let span = delay.saturating_mul(jitter_permille.min(1000)) / 1000;
    delay.saturating_sub(span)
}

/// Derive a pseudo-random jitter fraction (per mille) from the wall clock.
///
/// This avoids pulling in an RNG dependency while still de-synchronising
/// concurrent retriers. The value is in `0..=250` (up to 25% of the delay).
pub fn wall_clock_jitter_permille() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| u64::from(d.subsec_nanos()) % 251)
        .unwrap_or(0)
}

/// Substrings that identify a non-retryable, client-side AWS failure.
///
/// Retrying any of these simply burns the retry budget and delays surfacing
/// the real problem (a malformed request, a missing permission, an expired
/// receipt handle, ...).
const NON_RETRYABLE_MARKERS: &[&str] = &[
    "MissingParameter",
    "InvalidParameterValue",
    "InvalidParameterCombination",
    "InvalidAttributeName",
    "InvalidAttributeValue",
    "InvalidAddress",
    "InvalidBatchEntryId",
    "ValidationError",
    "ValidationException",
    "AccessDenied",
    "AccessDeniedException",
    "UnrecognizedClient",
    "InvalidClientTokenId",
    "SignatureDoesNotMatch",
    "AuthorizationError",
    "QueueDoesNotExist",
    "NonExistentQueue",
    "QueueDeletedRecently",
    "ReceiptHandleIsInvalid",
    "InvalidIdFormat",
    "MessageNotInflight",
    "UnsupportedOperation",
    "BatchRequestTooLong",
    "TooManyEntriesInBatchRequest",
    "EmptyBatchRequest",
    "BatchEntryIdsNotDistinct",
    "PurgeQueueInProgress",
    "InvalidSecurity",
    "KMSAccessDenied",
    "KMSInvalidState",
    "KMSNotFound",
    "KMSDisabled",
];

/// Decide whether a [`BrokerError`] is worth retrying.
///
/// Serialization and configuration failures are deterministic and never
/// retried. Operation failures are inspected for the AWS error codes listed in
/// [`NON_RETRYABLE_MARKERS`]; anything else (throttling, 5xx, transport
/// hiccups) is treated as transient.
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::retry_policy::is_retryable_error;
/// use celers_kombu::BrokerError;
///
/// let validation = BrokerError::OperationFailed(
///     "Failed to send message: MissingParameter: MessageGroupId".to_string(),
/// );
/// assert!(!is_retryable_error(&validation));
///
/// let throttled = BrokerError::OperationFailed(
///     "Failed to send message: RequestThrottled".to_string(),
/// );
/// assert!(is_retryable_error(&throttled));
/// ```
pub fn is_retryable_error(error: &BrokerError) -> bool {
    match error {
        BrokerError::Serialization(_)
        | BrokerError::Configuration(_)
        | BrokerError::QueueNotFound(_)
        | BrokerError::MessageNotFound(_) => false,
        BrokerError::Timeout => true,
        BrokerError::Connection(message) | BrokerError::OperationFailed(message) => {
            !NON_RETRYABLE_MARKERS
                .iter()
                .any(|marker| message.contains(marker))
        }
    }
}

/// Decide whether a per-entry batch failure is worth retrying.
///
/// SQS reports per-entry failures with an error code and a `SenderFault` flag.
/// A sender fault means the request itself was wrong, so retrying it verbatim
/// cannot succeed — except for throttling, which AWS occasionally reports as a
/// sender fault and which *is* transient.
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::retry_policy::is_retryable_batch_code;
///
/// assert!(is_retryable_batch_code("InternalError", false));
/// assert!(!is_retryable_batch_code("InvalidParameterValue", true));
/// assert!(is_retryable_batch_code("RequestThrottled", true));
/// ```
pub fn is_retryable_batch_code(code: &str, sender_fault: bool) -> bool {
    if code.contains("Throttl") || code.contains("TooManyRequests") {
        return true;
    }
    !sender_fault
}

/// Render an error and its full `source` chain into a single diagnostic string.
///
/// The AWS SDK's `SdkError` `Display` implementation is deliberately terse
/// (`"service error"`, `"dispatch failure"`); the useful detail lives in the
/// source chain. Losing it is what made "queue does not exist" the answer to
/// every possible failure in `get_queue_url`.
pub fn describe_error(error: &dyn std::error::Error) -> String {
    let mut description = error.to_string();
    let mut source = error.source();
    let mut depth = 0usize;

    while let Some(current) = source {
        // Guard against pathological/cyclic chains.
        if depth >= 8 {
            break;
        }
        let text = current.to_string();
        if !text.is_empty() && !description.contains(&text) {
            description.push_str(": ");
            description.push_str(&text);
        }
        source = current.source();
        depth += 1;
    }

    description
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_is_exponential_without_jitter() {
        assert_eq!(backoff_delay_ms(100, 0, 0), 100);
        assert_eq!(backoff_delay_ms(100, 1, 0), 200);
        assert_eq!(backoff_delay_ms(100, 2, 0), 400);
        assert_eq!(backoff_delay_ms(100, 5, 0), 3_200);
    }

    #[test]
    fn backoff_never_overflows_for_large_attempts() {
        // The old implementation (`base * (1 << attempt)`) panicked in debug
        // builds for attempt >= 64 and produced garbage in release builds.
        for attempt in [20u32, 31, 32, 63, 64, 128, u32::MAX] {
            let delay = backoff_delay_ms(100, attempt, 0);
            assert!(delay <= MAX_BACKOFF_DELAY_MS, "attempt {attempt}");
        }
        assert_eq!(backoff_delay_ms(u64::MAX, 3, 0), MAX_BACKOFF_DELAY_MS);
    }

    #[test]
    fn backoff_jitter_reduces_delay_within_bounds() {
        let base = backoff_delay_ms(1_000, 2, 0);
        assert_eq!(base, 4_000);

        let jittered = backoff_delay_ms(1_000, 2, 250);
        assert!(jittered <= base);
        assert!(jittered >= base - base / 4);
    }

    #[test]
    fn jitter_permille_is_bounded() {
        for _ in 0..64 {
            assert!(wall_clock_jitter_permille() <= 250);
        }
    }

    #[test]
    fn validation_errors_are_not_retried() {
        let error = BrokerError::OperationFailed(
            "Failed to send message: MissingParameter: The request must contain the parameter MessageGroupId."
                .to_string(),
        );
        assert!(!is_retryable_error(&error));
    }

    #[test]
    fn transient_errors_are_retried() {
        assert!(is_retryable_error(&BrokerError::OperationFailed(
            "Failed to send message: ServiceUnavailable".to_string()
        )));
        assert!(is_retryable_error(&BrokerError::Timeout));
        assert!(is_retryable_error(&BrokerError::Connection(
            "dispatch failure: io error".to_string()
        )));
    }

    #[test]
    fn deterministic_errors_are_not_retried() {
        assert!(!is_retryable_error(&BrokerError::Serialization(
            "bad json".to_string()
        )));
        assert!(!is_retryable_error(&BrokerError::Configuration(
            "bad config".to_string()
        )));
        assert!(!is_retryable_error(&BrokerError::QueueNotFound(
            "q".to_string()
        )));
        assert!(!is_retryable_error(&BrokerError::Connection(
            "GetQueueUrl failed: AccessDenied".to_string()
        )));
    }

    #[test]
    fn batch_codes_classified() {
        assert!(is_retryable_batch_code("InternalError", false));
        assert!(is_retryable_batch_code("ServiceUnavailable", false));
        assert!(is_retryable_batch_code("RequestThrottled", true));
        assert!(is_retryable_batch_code("KMS.ThrottlingException", true));
        assert!(!is_retryable_batch_code("InvalidParameterValue", true));
        assert!(!is_retryable_batch_code(
            "AWS.SimpleQueueService.InvalidBatchEntryId",
            true
        ));
    }

    #[derive(Debug)]
    struct Layer {
        message: &'static str,
        source: Option<Box<Layer>>,
    }

    impl std::fmt::Display for Layer {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.message)
        }
    }

    impl std::error::Error for Layer {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.source
                .as_ref()
                .map(|inner| inner.as_ref() as &(dyn std::error::Error + 'static))
        }
    }

    #[test]
    fn describe_error_walks_source_chain() {
        let error = Layer {
            message: "service error",
            source: Some(Box::new(Layer {
                message: "AccessDenied",
                source: None,
            })),
        };

        assert_eq!(describe_error(&error), "service error: AccessDenied");
    }
}
