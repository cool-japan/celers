// Copyright (c) 2026 COOLJAPAN OU (Team KitaSan)
// SPDX-License-Identifier: Apache-2.0

//! Batch operations for the SQS broker.
//!
//! Every SQS `*Batch` API accepts at most 10 entries and (for `SendMessageBatch`)
//! at most 256 KB of *aggregate* payload. The previous implementation truncated
//! the caller's input with `.min(10)` and returned only the number of
//! successes, which meant:
//!
//! * `publish_batch` silently dropped messages 11..n — message loss on enqueue;
//! * `ack_batch` silently left receipt handles 11..n undeleted — the messages
//!   became visible again and were re-executed;
//! * per-entry `Failed` results (id / code / `SenderFault` / message) were
//!   reduced to a `warn!` with a count, so partial failures were
//!   indistinguishable from success.
//!
//! The operations here chunk internally, account for the aggregate size limit,
//! retry the per-entry failures whose error code is transient, and surface
//! everything that remains through [`BatchOutcome`].
//!
//! # Shared retry budget
//!
//! Each chunk used to retry independently up to `max_retries` times, so a
//! 100-message call (10 chunks) against a persistently throttled queue could
//! issue up to `max_retries * 10` requests with no call-wide cap. A
//! `RetryBudget` is now shared across every chunk of one top-level call:
//! every chunk still gets its unconditional *first* attempt (entries can
//! never be silently dropped — see above), but attempts beyond the first draw
//! from a pool sized so that small/medium batches (up to
//! `RETRY_BUDGET_CHUNK_CAP` chunks) are unaffected, while a call with many
//! chunks can no longer retry every single one of them to the hilt. An entry
//! that loses a retry to budget exhaustion is reported as
//! [`RETRY_BUDGET_EXHAUSTED_CODE`], distinguishable from a real AWS error
//! code, rather than silently continuing to hammer AWS.
//!
//! The pool is intentionally a *constant* per call (independent of how many
//! chunks it has), not a fraction of the chunk count: it is sized for the
//! motivating scenario -- a queue throttled call-wide, where every chunk is
//! failing -- rather than a handful of scattered per-chunk blips spread
//! across a very large call, which will now see some chunks denied a retry
//! sooner than before. For `ack_batch`/`extend_visibility_batch` specifically,
//! an entry denied a retry this way is exactly as safe as one AWS itself
//! rejected: it surfaces through [`BatchOutcome::failed`] /
//! [`BatchOutcome::into_count`] exactly like any other failure, so a caller
//! already handling that documented contract sees a real, attributable error
//! rather than a silent success -- the message simply becomes visible again
//! and is redelivered, which is the same at-least-once behaviour an
//! undeleted handle always has.

use std::collections::HashMap;

use aws_sdk_sqs::types::{
    BatchResultErrorEntry, ChangeMessageVisibilityBatchRequestEntry,
    DeleteMessageBatchRequestEntry, MessageAttributeValue, SendMessageBatchRequestEntry,
};
use celers_kombu::{BrokerError, Result};
use celers_protocol::Message;
use tracing::{debug, info, warn};

use crate::broker_core::SqsBroker;
use crate::delivery::decode_delivery_tag;
use crate::retry_policy::{backoff_delay_ms, is_retryable_batch_code, wall_clock_jitter_permille};

/// Maximum number of entries in a single SQS batch request.
pub const SQS_MAX_BATCH_ENTRIES: usize = 10;

/// Maximum aggregate payload of a single `SendMessageBatch` request, in bytes.
pub const SQS_MAX_BATCH_BYTES: usize = 262_144;

/// A single entry that the SQS batch API rejected.
///
/// SQS reports batch failures per entry rather than failing the whole request,
/// so this is the only place the reason for a partial failure exists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchEntryFailure {
    /// Zero-based index of the entry within the *caller's* input vector.
    pub index: usize,
    /// AWS error code (for example `InternalError`, `InvalidParameterValue`).
    pub code: String,
    /// Human readable detail, when AWS supplied one.
    pub message: Option<String>,
    /// Whether AWS attributes the failure to the sender (a bad request).
    pub sender_fault: bool,
}

impl std::fmt::Display for BatchEntryFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "entry {}: {}", self.index, self.code)?;
        if let Some(ref message) = self.message {
            write!(f, " ({message})")?;
        }
        if self.sender_fault {
            write!(f, " [sender fault]")?;
        }
        Ok(())
    }
}

/// Outcome of a batch operation that was chunked across several requests.
///
/// Unlike a bare `usize`, this distinguishes "everything went through" from
/// "some entries were rejected and here is why".
#[derive(Debug, Clone, Default)]
pub struct BatchOutcome {
    /// Number of entries the service accepted.
    pub successful: usize,
    /// Entries the service rejected, with their reason.
    pub failed: Vec<BatchEntryFailure>,
}

impl BatchOutcome {
    /// Total number of entries submitted.
    pub fn total(&self) -> usize {
        self.successful + self.failed.len()
    }

    /// Whether every submitted entry succeeded.
    pub fn is_complete(&self) -> bool {
        self.failed.is_empty()
    }

    /// Collapse into `Ok(successful)` or an error describing the failures.
    ///
    /// Used by the `usize`-returning convenience wrappers so that a partial
    /// failure cannot be mistaken for success.
    pub fn into_count(self) -> Result<usize> {
        if self.failed.is_empty() {
            return Ok(self.successful);
        }

        let detail = self
            .failed
            .iter()
            .map(BatchEntryFailure::to_string)
            .collect::<Vec<_>>()
            .join("; ");

        Err(BrokerError::OperationFailed(format!(
            "{} of {} batch entries failed: {}",
            self.failed.len(),
            self.total(),
            detail
        )))
    }

    fn merge(&mut self, other: BatchOutcome) {
        self.successful += other.successful;
        self.failed.extend(other.failed);
    }
}

/// Split a batch into chunks that respect both SQS limits.
///
/// `sizes[i]` is the wire size of entry `i`. Chunks contain at most
/// `max_entries` entries and — unless a single entry is already oversized — at
/// most `max_bytes` of aggregate payload. An oversized entry gets a chunk of
/// its own so that the service rejects only that entry instead of the whole
/// request.
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::batch_ops::plan_batch_chunks;
///
/// // 25 small entries -> three chunks (10 + 10 + 5), nothing dropped.
/// let sizes = vec![100usize; 25];
/// let chunks = plan_batch_chunks(&sizes, 10, 262_144);
/// assert_eq!(chunks, vec![0..10, 10..20, 20..25]);
/// ```
pub fn plan_batch_chunks(
    sizes: &[usize],
    max_entries: usize,
    max_bytes: usize,
) -> Vec<std::ops::Range<usize>> {
    let max_entries = max_entries.max(1);
    let mut chunks = Vec::new();
    let mut start = 0usize;
    let mut running_bytes = 0usize;

    for (index, size) in sizes.iter().enumerate() {
        let would_exceed_entries = index - start >= max_entries;
        let would_exceed_bytes = index > start && running_bytes.saturating_add(*size) > max_bytes;

        if would_exceed_entries || would_exceed_bytes {
            chunks.push(start..index);
            start = index;
            running_bytes = 0;
        }

        running_bytes = running_bytes.saturating_add(*size);
    }

    if start < sizes.len() {
        chunks.push(start..sizes.len());
    }

    chunks
}

/// Approximate the wire size of a `SendMessageBatch` entry.
///
/// SQS charges the message body plus every attribute name, data type and value
/// against the 256 KB limit.
pub fn entry_wire_size(body: &str, attributes: &HashMap<String, MessageAttributeValue>) -> usize {
    let mut size = body.len();

    for (name, value) in attributes {
        size += name.len();
        size += value.data_type().len();
        if let Some(string_value) = value.string_value() {
            size += string_value.len();
        }
        if let Some(binary_value) = value.binary_value() {
            size += binary_value.as_ref().len();
        }
    }

    size
}

/// AWS error code reported for an entry that lost a retry to the shared
/// `RetryBudget` rather than to an AWS-side rejection.
///
/// Prefixed so it can never collide with a real AWS batch error code (all of
/// which are bare PascalCase identifiers such as `InternalError`), letting a
/// caller inspecting [`BatchOutcome::failed`] tell "AWS rejected this entry"
/// apart from "we gave up retrying it to protect the queue".
pub const RETRY_BUDGET_EXHAUSTED_CODE: &str = "CeleRS.RetryBudgetExhausted";

/// Number of chunks' worth of independent retries pooled into one call's
/// shared [`RetryBudget`].
///
/// A call that fits in this many chunks or fewer never notices the budget:
/// every chunk still gets its full, unshared `max_retries`. Only a call split
/// into *more* chunks than this starts sharing retries across them, which is
/// exactly the case (a large batch against a throttled queue) that used to
/// multiply `max_retries` by the chunk count with no ceiling.
const RETRY_BUDGET_CHUNK_CAP: usize = 4;

/// Shared retry-attempt budget for one top-level batch call (`publish_batch`,
/// `ack_batch`, `publish_fifo_batch`, `extend_visibility_batch`), pooled
/// across every chunk the call is split into.
///
/// Each chunk previously retried independently up to `max_retries` times, so
/// a 100-message `publish_batch` (10 chunks) against a persistently throttled
/// queue could issue up to `max_retries * 10` requests with nothing capping
/// the total. A single [`RetryBudget`] is now created once per top-level call
/// and threaded through every [`run_batch_with_retry`] invocation for that
/// call (across every chunk, and — for `ack_batch`/`extend_visibility_batch`
/// — every source queue too).
///
/// Every chunk's *first* attempt is never charged against this budget:
/// [`plan_batch_chunks`] guarantees every entry is covered by exactly one
/// chunk, and skipping a chunk's first attempt would silently drop its
/// entries from the returned [`BatchOutcome`] — the exact enqueue-loss bug
/// this module's chunking exists to prevent (see the module docs). Only
/// attempts *beyond* the first, on any chunk, draw from this pool.
#[derive(Debug)]
struct RetryBudget {
    remaining: usize,
}

impl RetryBudget {
    /// Size the pool so a call that never exceeds [`RETRY_BUDGET_CHUNK_CAP`]
    /// chunks behaves exactly as before: every chunk gets the full
    /// `max_retries - 1` retries, unshared.
    fn new(max_retries: u32) -> Self {
        let per_chunk_retries =
            usize::try_from(max_retries.saturating_sub(1)).unwrap_or(usize::MAX);
        Self {
            remaining: per_chunk_retries.saturating_mul(RETRY_BUDGET_CHUNK_CAP),
        }
    }

    /// Try to spend one retry attempt from the shared pool.
    ///
    /// Returns `false` once the pool is exhausted, in which case the caller
    /// must stop retrying rather than proceed as if the attempt were free.
    fn try_spend(&mut self) -> bool {
        match self.remaining.checked_sub(1) {
            Some(left) => {
                self.remaining = left;
                true
            }
            None => false,
        }
    }
}

/// Run one batch request, retrying the entries whose failure is transient.
///
/// `send` receives the entries still in flight and returns
/// `(successful_count, failed_entries)`. Entry ids are the *caller's* indices
/// rendered as decimal strings, which is how a `Failed` entry is mapped back to
/// the input vector.
///
/// `budget` pools retry attempts across every chunk of the enclosing
/// top-level call (see [`RetryBudget`]); this chunk's first attempt is always
/// made regardless of the budget's state.
async fn run_batch_with_retry<E, F, Fut>(
    entries: Vec<(usize, E)>,
    send: F,
    max_retries: u32,
    base_delay_ms: u64,
    budget: &mut RetryBudget,
) -> Result<BatchOutcome>
where
    E: Clone,
    F: Fn(Vec<E>) -> Fut,
    Fut: std::future::Future<Output = Result<(usize, Vec<BatchResultErrorEntry>)>>,
{
    let mut outcome = BatchOutcome::default();
    let mut pending = entries;
    let mut attempt = 0u32;

    while !pending.is_empty() {
        // The first attempt for this chunk (attempt == 0) is unconditional:
        // every entry must be tried at least once or it would silently
        // disappear from `outcome` instead of chunking safely (see the
        // module docs). Only retries beyond that draw from the shared pool.
        if attempt > 0 && !budget.try_spend() {
            warn!(
                "Shared retry budget exhausted after {} attempt(s); failing {} remaining \
                 entries instead of retrying further",
                attempt,
                pending.len()
            );
            for (index, _) in &pending {
                outcome.failed.push(BatchEntryFailure {
                    index: *index,
                    code: RETRY_BUDGET_EXHAUSTED_CODE.to_string(),
                    message: Some(format!(
                        "retry budget for this batch call was exhausted after {attempt} \
                         attempt(s) shared across chunks"
                    )),
                    sender_fault: false,
                });
            }
            break;
        }

        let by_id: HashMap<String, (usize, E)> = pending
            .iter()
            .map(|(index, entry)| (index.to_string(), (*index, entry.clone())))
            .collect();

        let (successful, failed) = send(pending.iter().map(|(_, e)| e.clone()).collect()).await?;
        outcome.successful += successful;

        let mut retryable = Vec::new();
        for failure in failed {
            let Some((index, entry)) = by_id.get(failure.id()).cloned() else {
                // Should not happen: SQS echoes the id we sent.
                outcome.failed.push(BatchEntryFailure {
                    index: usize::MAX,
                    code: failure.code().to_string(),
                    message: failure.message().map(str::to_string),
                    sender_fault: failure.sender_fault(),
                });
                continue;
            };

            if attempt + 1 < max_retries
                && is_retryable_batch_code(failure.code(), failure.sender_fault())
            {
                retryable.push((index, entry));
            } else {
                outcome.failed.push(BatchEntryFailure {
                    index,
                    code: failure.code().to_string(),
                    message: failure.message().map(str::to_string),
                    sender_fault: failure.sender_fault(),
                });
            }
        }

        if retryable.is_empty() {
            break;
        }

        attempt += 1;
        let delay = backoff_delay_ms(base_delay_ms, attempt, wall_clock_jitter_permille());
        debug!(
            "Retrying {} failed batch entries (attempt {}/{}), waiting {}ms",
            retryable.len(),
            attempt,
            max_retries,
            delay
        );
        tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
        pending = retryable;
    }

    Ok(outcome)
}

impl SqsBroker {
    /// Publish multiple messages, chunking internally to respect SQS limits.
    ///
    /// The input vector may be of any length: it is split into chunks of at
    /// most 10 entries and 256 KB aggregate payload. Transient per-entry
    /// failures are retried; anything that still fails is reported as an error
    /// rather than silently reducing the returned count.
    ///
    /// # Returns
    ///
    /// The number of messages the service accepted (equal to `messages.len()`
    /// on success).
    ///
    /// # Errors
    ///
    /// Returns [`BrokerError::OperationFailed`] when the request itself fails,
    /// or when any entry was permanently rejected. Use
    /// [`publish_batch_detailed`](Self::publish_batch_detailed) to inspect the
    /// individual failures instead.
    pub async fn publish_batch(&mut self, queue: &str, messages: Vec<Message>) -> Result<usize> {
        self.publish_batch_detailed(queue, messages)
            .await?
            .into_count()
    }

    /// Publish a large batch with automatic chunking.
    ///
    /// Retained for backwards compatibility: [`publish_batch`](Self::publish_batch)
    /// now chunks by itself, so this is a straight alias.
    pub async fn publish_batch_chunked(
        &mut self,
        queue: &str,
        messages: Vec<Message>,
    ) -> Result<usize> {
        self.publish_batch(queue, messages).await
    }

    /// Publish multiple messages and report per-entry outcomes.
    pub async fn publish_batch_detailed(
        &mut self,
        queue: &str,
        messages: Vec<Message>,
    ) -> Result<BatchOutcome> {
        if messages.is_empty() {
            return Ok(BatchOutcome::default());
        }

        // FIFO queues need a MessageGroupId on every entry; route through the
        // FIFO batch path so the generic interface works there too.
        let physical_queue = self.resolve_queue_name(queue);
        if self.is_fifo_queue(&physical_queue) {
            let entries = messages
                .into_iter()
                .map(|message| {
                    let group_id = self.derive_group_id(&physical_queue, &message);
                    (message, group_id, None)
                })
                .collect();
            return self.publish_fifo_batch_detailed(queue, entries).await;
        }

        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(&physical_queue).await?;

        let mut bodies = Vec::with_capacity(messages.len());
        let mut attribute_sets = Vec::with_capacity(messages.len());
        let mut sizes = Vec::with_capacity(messages.len());

        for message in &messages {
            let body = self.encode_body(message)?;
            let attributes = self.build_attributes(message)?;
            sizes.push(entry_wire_size(&body, &attributes));
            bodies.push(body);
            attribute_sets.push(attributes);
        }

        let mut entries = Vec::with_capacity(messages.len());
        for (index, body) in bodies.into_iter().enumerate() {
            let mut builder = SendMessageBatchRequestEntry::builder()
                .id(index.to_string())
                .message_body(body);

            let attributes = std::mem::take(&mut attribute_sets[index]);
            if !attributes.is_empty() {
                builder = builder.set_message_attributes(Some(attributes));
            }

            entries.push((
                index,
                builder
                    .build()
                    .map_err(|e| BrokerError::OperationFailed(e.to_string()))?,
            ));
        }

        let mut outcome = BatchOutcome::default();
        let mut budget = RetryBudget::new(self.max_retries);
        for chunk in plan_batch_chunks(&sizes, SQS_MAX_BATCH_ENTRIES, SQS_MAX_BATCH_BYTES) {
            let chunk_entries = entries[chunk.clone()].to_vec();
            let client = client.clone();
            let queue_url = queue_url.clone();

            let chunk_outcome = run_batch_with_retry(
                chunk_entries,
                move |batch| {
                    let client = client.clone();
                    let queue_url = queue_url.clone();
                    async move {
                        let result = client
                            .send_message_batch()
                            .queue_url(&queue_url)
                            .set_entries(Some(batch))
                            .send()
                            .await
                            .map_err(|e| {
                                BrokerError::OperationFailed(format!(
                                    "Failed to send batch: {}",
                                    crate::retry_policy::describe_error(&e)
                                ))
                            })?;
                        Ok((result.successful().len(), result.failed().to_vec()))
                    }
                },
                self.max_retries,
                self.retry_base_delay_ms,
                &mut budget,
            )
            .await?;

            outcome.merge(chunk_outcome);
        }

        if outcome.is_complete() {
            self.mark_healthy();
        }
        log_outcome("publish_batch", queue, &outcome);
        Ok(outcome)
    }

    /// Acknowledge (delete) multiple messages, chunking internally.
    ///
    /// Delivery tags produced by [`consume`](celers_kombu::Consumer::consume) /
    /// [`consume_batch`](Self::consume_batch) carry the queue they were
    /// received from, so handles from several queues can be mixed freely — each
    /// one is deleted against its own queue URL. Plain receipt handles fall
    /// back to `queue`.
    ///
    /// # Errors
    ///
    /// Returns an error when any handle could not be deleted, so an undeleted
    /// handle (which would be redelivered) can never look like success.
    pub async fn ack_batch(&mut self, queue: &str, receipt_handles: Vec<String>) -> Result<usize> {
        self.ack_batch_detailed(queue, receipt_handles)
            .await?
            .into_count()
    }

    /// Acknowledge multiple messages and report per-entry outcomes.
    pub async fn ack_batch_detailed(
        &mut self,
        queue: &str,
        receipt_handles: Vec<String>,
    ) -> Result<BatchOutcome> {
        if receipt_handles.is_empty() {
            return Ok(BatchOutcome::default());
        }

        for tag in &receipt_handles {
            self.stop_visibility_heartbeat(tag);
            self.forget_receipt_metadata(tag);
        }

        let grouped = self.group_handles_by_queue(queue, &receipt_handles);
        let client = self.get_client().await?;
        let mut outcome = BatchOutcome::default();
        let mut budget = RetryBudget::new(self.max_retries);

        for (source_queue, handles) in grouped {
            let queue_url = self.get_queue_url(&source_queue).await?;

            let mut entries = Vec::with_capacity(handles.len());
            for (index, handle) in handles {
                entries.push((
                    index,
                    DeleteMessageBatchRequestEntry::builder()
                        .id(index.to_string())
                        .receipt_handle(handle)
                        .build()
                        .map_err(|e| BrokerError::OperationFailed(e.to_string()))?,
                ));
            }

            for chunk in entries.chunks(SQS_MAX_BATCH_ENTRIES) {
                let client = client.clone();
                let queue_url = queue_url.clone();

                let chunk_outcome = run_batch_with_retry(
                    chunk.to_vec(),
                    move |batch| {
                        let client = client.clone();
                        let queue_url = queue_url.clone();
                        async move {
                            let result = client
                                .delete_message_batch()
                                .queue_url(&queue_url)
                                .set_entries(Some(batch))
                                .send()
                                .await
                                .map_err(|e| {
                                    BrokerError::OperationFailed(format!(
                                        "Failed to delete batch: {}",
                                        crate::retry_policy::describe_error(&e)
                                    ))
                                })?;
                            Ok((result.successful().len(), result.failed().to_vec()))
                        }
                    },
                    self.max_retries,
                    self.retry_base_delay_ms,
                    &mut budget,
                )
                .await?;

                outcome.merge(chunk_outcome);
            }
        }

        if outcome.is_complete() {
            self.mark_healthy();
        }
        log_outcome("ack_batch", queue, &outcome);
        Ok(outcome)
    }

    /// Publish multiple messages to a FIFO queue, chunking internally.
    ///
    /// # Arguments
    /// * `queue` - Queue name (must end with `.fifo`)
    /// * `messages` - `(message, message_group_id, optional_deduplication_id)`
    pub async fn publish_fifo_batch(
        &mut self,
        queue: &str,
        messages: Vec<(Message, String, Option<String>)>,
    ) -> Result<usize> {
        self.publish_fifo_batch_detailed(queue, messages)
            .await?
            .into_count()
    }

    /// Publish multiple FIFO messages and report per-entry outcomes.
    pub async fn publish_fifo_batch_detailed(
        &mut self,
        queue: &str,
        messages: Vec<(Message, String, Option<String>)>,
    ) -> Result<BatchOutcome> {
        if messages.is_empty() {
            return Ok(BatchOutcome::default());
        }

        let physical_queue = self.resolve_queue_name(queue);
        if !physical_queue.ends_with(".fifo") {
            return Err(BrokerError::OperationFailed(
                "FIFO queue name must end with '.fifo'".to_string(),
            ));
        }

        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(&physical_queue).await?;

        let content_based_dedup = self
            .fifo_config
            .as_ref()
            .is_some_and(|c| c.content_based_deduplication);

        let mut entries = Vec::with_capacity(messages.len());
        let mut sizes = Vec::with_capacity(messages.len());

        for (index, (message, group_id, dedup_id)) in messages.iter().enumerate() {
            let body = self.encode_body(message)?;
            let attributes = self.build_attributes(message)?;
            sizes.push(entry_wire_size(&body, &attributes));

            let mut builder = SendMessageBatchRequestEntry::builder()
                .id(index.to_string())
                .message_body(body)
                .message_group_id(crate::fifo::sanitize_fifo_id(group_id));

            if let Some(dedup) = crate::fifo::derive_deduplication_id(
                content_based_dedup,
                dedup_id.as_deref(),
                message,
            ) {
                builder = builder.message_deduplication_id(dedup);
            }

            if !attributes.is_empty() {
                builder = builder.set_message_attributes(Some(attributes));
            }

            entries.push((
                index,
                builder
                    .build()
                    .map_err(|e| BrokerError::OperationFailed(e.to_string()))?,
            ));
        }

        let mut outcome = BatchOutcome::default();
        let mut budget = RetryBudget::new(self.max_retries);
        for chunk in plan_batch_chunks(&sizes, SQS_MAX_BATCH_ENTRIES, SQS_MAX_BATCH_BYTES) {
            let chunk_entries = entries[chunk.clone()].to_vec();
            let client = client.clone();
            let queue_url = queue_url.clone();

            let chunk_outcome = run_batch_with_retry(
                chunk_entries,
                move |batch| {
                    let client = client.clone();
                    let queue_url = queue_url.clone();
                    async move {
                        let result = client
                            .send_message_batch()
                            .queue_url(&queue_url)
                            .set_entries(Some(batch))
                            .send()
                            .await
                            .map_err(|e| {
                                BrokerError::OperationFailed(format!(
                                    "Failed to send FIFO batch: {}",
                                    crate::retry_policy::describe_error(&e)
                                ))
                            })?;
                        Ok((result.successful().len(), result.failed().to_vec()))
                    }
                },
                self.max_retries,
                self.retry_base_delay_ms,
                &mut budget,
            )
            .await?;

            outcome.merge(chunk_outcome);
        }

        if outcome.is_complete() {
            self.mark_healthy();
        }
        log_outcome("publish_fifo_batch", queue, &outcome);
        Ok(outcome)
    }

    /// Extend the visibility timeout of several in-flight messages.
    ///
    /// Accepts any number of entries and chunks them into requests of 10.
    /// Delivery tags carrying a source queue are routed to that queue.
    ///
    /// # Arguments
    /// * `queue` - Fallback queue for plain receipt handles
    /// * `entries` - `(delivery_tag, timeout_seconds)` pairs
    pub async fn extend_visibility_batch(
        &mut self,
        queue: &str,
        entries: Vec<(String, i32)>,
    ) -> Result<usize> {
        self.extend_visibility_batch_detailed(queue, entries)
            .await?
            .into_count()
    }

    /// Extend visibility for several messages and report per-entry outcomes.
    pub async fn extend_visibility_batch_detailed(
        &mut self,
        queue: &str,
        entries: Vec<(String, i32)>,
    ) -> Result<BatchOutcome> {
        if entries.is_empty() {
            return Ok(BatchOutcome::default());
        }

        let tags: Vec<String> = entries.iter().map(|(tag, _)| tag.clone()).collect();
        let grouped = self.group_handles_by_queue(queue, &tags);
        let client = self.get_client().await?;
        let mut outcome = BatchOutcome::default();
        let mut budget = RetryBudget::new(self.max_retries);

        for (source_queue, handles) in grouped {
            let queue_url = self.get_queue_url(&source_queue).await?;

            let mut request_entries = Vec::with_capacity(handles.len());
            for (index, handle) in handles {
                let timeout = entries[index].1.clamp(0, 43_200);
                request_entries.push((
                    index,
                    ChangeMessageVisibilityBatchRequestEntry::builder()
                        .id(index.to_string())
                        .receipt_handle(handle)
                        .visibility_timeout(timeout)
                        .build()
                        .map_err(|e| BrokerError::OperationFailed(e.to_string()))?,
                ));
            }

            for chunk in request_entries.chunks(SQS_MAX_BATCH_ENTRIES) {
                let client = client.clone();
                let queue_url = queue_url.clone();

                let chunk_outcome = run_batch_with_retry(
                    chunk.to_vec(),
                    move |batch| {
                        let client = client.clone();
                        let queue_url = queue_url.clone();
                        async move {
                            let result = client
                                .change_message_visibility_batch()
                                .queue_url(&queue_url)
                                .set_entries(Some(batch))
                                .send()
                                .await
                                .map_err(|e| {
                                    BrokerError::OperationFailed(format!(
                                        "Failed to extend visibility batch: {}",
                                        crate::retry_policy::describe_error(&e)
                                    ))
                                })?;
                            Ok((result.successful().len(), result.failed().to_vec()))
                        }
                    },
                    self.max_retries,
                    self.retry_base_delay_ms,
                    &mut budget,
                )
                .await?;

                outcome.merge(chunk_outcome);
            }
        }

        log_outcome("extend_visibility_batch", queue, &outcome);
        Ok(outcome)
    }

    /// Group delivery tags by the queue they were received from.
    ///
    /// Returns `queue -> [(caller index, raw receipt handle)]`.
    pub(crate) fn group_handles_by_queue(
        &self,
        fallback_queue: &str,
        tags: &[String],
    ) -> Vec<(String, Vec<(usize, String)>)> {
        let fallback = self.resolve_queue_name(fallback_queue);
        let mut order: Vec<String> = Vec::new();
        let mut grouped: HashMap<String, Vec<(usize, String)>> = HashMap::new();

        for (index, tag) in tags.iter().enumerate() {
            let (source, handle) = decode_delivery_tag(tag);
            let source_queue = source
                .map(str::to_string)
                .unwrap_or_else(|| fallback.clone());

            grouped
                .entry(source_queue.clone())
                .or_insert_with(|| {
                    order.push(source_queue.clone());
                    Vec::new()
                })
                .push((index, handle.to_string()));
        }

        order
            .into_iter()
            .filter_map(|queue| grouped.remove(&queue).map(|handles| (queue, handles)))
            .collect()
    }
}

fn log_outcome(operation: &str, queue: &str, outcome: &BatchOutcome) {
    if outcome.is_complete() {
        debug!(
            "{} completed for queue {}: {} entries accepted",
            operation, queue, outcome.successful
        );
    } else {
        warn!(
            "{} for queue {}: {} accepted, {} rejected: {}",
            operation,
            queue,
            outcome.successful,
            outcome.failed.len(),
            outcome
                .failed
                .iter()
                .map(BatchEntryFailure::to_string)
                .collect::<Vec<_>>()
                .join("; ")
        );
    }

    if outcome.successful > 0 && !outcome.is_complete() {
        info!(
            "{} partially applied for queue {} ({}/{})",
            operation,
            queue,
            outcome.successful,
            outcome.total()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn twenty_five_entries_are_chunked_not_truncated() {
        let sizes = vec![128usize; 25];
        let chunks = plan_batch_chunks(&sizes, SQS_MAX_BATCH_ENTRIES, SQS_MAX_BATCH_BYTES);

        assert_eq!(chunks, vec![0..10, 10..20, 20..25]);
        let total: usize = chunks.iter().map(|c| c.len()).sum();
        assert_eq!(
            total, 25,
            "every entry must be covered by exactly one chunk"
        );
    }

    #[test]
    fn chunks_cover_every_index_exactly_once() {
        let sizes = vec![1usize; 37];
        let chunks = plan_batch_chunks(&sizes, SQS_MAX_BATCH_ENTRIES, SQS_MAX_BATCH_BYTES);

        let mut covered = Vec::new();
        for chunk in &chunks {
            covered.extend(chunk.clone());
        }
        assert_eq!(covered, (0..37).collect::<Vec<_>>());
    }

    #[test]
    fn aggregate_size_limit_starts_a_new_chunk() {
        // Three entries of 100 KB: the 256 KB aggregate limit allows only two
        // per request even though the entry limit would allow ten.
        let sizes = vec![100_000usize; 3];
        let chunks = plan_batch_chunks(&sizes, SQS_MAX_BATCH_ENTRIES, SQS_MAX_BATCH_BYTES);

        assert_eq!(chunks, vec![0..2, 2..3]);
    }

    #[test]
    fn oversized_entry_gets_its_own_chunk() {
        let sizes = vec![10, SQS_MAX_BATCH_BYTES + 1, 10];
        let chunks = plan_batch_chunks(&sizes, SQS_MAX_BATCH_ENTRIES, SQS_MAX_BATCH_BYTES);

        assert_eq!(chunks, vec![0..1, 1..2, 2..3]);
    }

    #[test]
    fn empty_input_plans_no_chunks() {
        assert!(plan_batch_chunks(&[], SQS_MAX_BATCH_ENTRIES, SQS_MAX_BATCH_BYTES).is_empty());
    }

    #[test]
    fn wire_size_counts_attributes() {
        let mut attributes = HashMap::new();
        attributes.insert(
            "priority".to_string(),
            MessageAttributeValue::builder()
                .data_type("Number")
                .string_value("9")
                .build()
                .expect("attribute builds"),
        );

        // body(4) + name(8) + data_type(6) + value(1)
        assert_eq!(entry_wire_size("body", &attributes), 19);
    }

    #[test]
    fn outcome_into_count_reports_failures() {
        let outcome = BatchOutcome {
            successful: 8,
            failed: vec![BatchEntryFailure {
                index: 9,
                code: "InvalidParameterValue".to_string(),
                message: Some("bad".to_string()),
                sender_fault: true,
            }],
        };

        assert_eq!(outcome.total(), 9);
        assert!(!outcome.is_complete());

        let error = outcome
            .into_count()
            .expect_err("partial failure is an error");
        let rendered = error.to_string();
        assert!(rendered.contains("entry 9"), "{rendered}");
        assert!(rendered.contains("InvalidParameterValue"), "{rendered}");
    }

    #[test]
    fn outcome_into_count_returns_success_count() {
        let outcome = BatchOutcome {
            successful: 25,
            failed: Vec::new(),
        };
        assert_eq!(outcome.into_count().expect("complete"), 25);
    }

    #[test]
    fn retry_budget_never_exhausts_a_single_chunk() {
        // A chunk's own loop can retry at most `max_retries - 1` times
        // (gated by `attempt + 1 < max_retries`) before giving up on its
        // own; the shared budget must never run out before that point for a
        // call that is only ever a single chunk, whatever `max_retries` is.
        for max_retries in [1u32, 2, 3, 5, 20] {
            let mut budget = RetryBudget::new(max_retries);
            let chunk_ceiling = max_retries.saturating_sub(1);
            for spend in 0..chunk_ceiling {
                assert!(
                    budget.try_spend(),
                    "budget exhausted at spend {spend}/{chunk_ceiling} for max_retries={max_retries}"
                );
            }
        }
    }

    #[test]
    fn retry_budget_is_finite() {
        // per_chunk_retries = 2 - 1 = 1, pooled across RETRY_BUDGET_CHUNK_CAP
        // chunks' worth = 4 total spends available.
        let mut budget = RetryBudget::new(2);
        for _ in 0..RETRY_BUDGET_CHUNK_CAP {
            assert!(budget.try_spend());
        }
        assert!(
            !budget.try_spend(),
            "budget must not grant unlimited retries"
        );
    }

    #[test]
    fn retry_budget_grants_no_retries_when_max_retries_is_one() {
        // max_retries == 1 means "try once, never retry" -- the budget must
        // reflect that instead of granting spends nobody should use.
        let mut budget = RetryBudget::new(1);
        assert!(!budget.try_spend());
    }

    /// Build a single-entry `Failed` response as if AWS reported the given
    /// id as throttled -- a transient, retryable failure.
    fn throttled_failure(id: &str) -> (usize, Vec<BatchResultErrorEntry>) {
        let failure = BatchResultErrorEntry::builder()
            .id(id.to_string())
            .code("ServiceUnavailable")
            .sender_fault(false)
            .build()
            .expect("well-formed error entry");
        (0usize, vec![failure])
    }

    #[tokio::test]
    async fn shared_budget_caps_total_attempts_below_the_old_per_chunk_bound() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        // Every attempt on every chunk is throttled, so without a shared
        // budget each of the `chunk_count` chunks would independently retry
        // up to `max_retries` times: `max_retries * chunk_count` requests.
        let max_retries = 3u32;
        let chunk_count = 6usize;
        let call_count = Arc::new(AtomicUsize::new(0));

        let mut budget = RetryBudget::new(max_retries);
        let mut outcome = BatchOutcome::default();

        for chunk_index in 0..chunk_count {
            let entries = vec![(chunk_index, ())];
            let id = chunk_index.to_string();
            let call_count = call_count.clone();

            let chunk_outcome = run_batch_with_retry(
                entries,
                move |_batch: Vec<()>| {
                    call_count.fetch_add(1, Ordering::SeqCst);
                    let id = id.clone();
                    async move { Ok(throttled_failure(&id)) }
                },
                max_retries,
                1, // base_delay_ms: the retry path really sleeps; keep it short
                &mut budget,
            )
            .await
            .expect("run_batch_with_retry itself never fails in this test");

            outcome.merge(chunk_outcome);
        }

        let naive_worst_case = max_retries as usize * chunk_count;
        let observed = call_count.load(Ordering::SeqCst);
        assert!(
            observed < naive_worst_case,
            "shared budget should cap total attempts below the unshared \
             max_retries * chunk_count bound (observed {observed}, bound {naive_worst_case})"
        );

        // Every entry is accounted for exactly once -- by AWS or by the
        // budget -- never silently dropped (see the module docs).
        assert_eq!(outcome.total(), chunk_count);
        assert!(!outcome.is_complete());
        assert!(
            outcome
                .failed
                .iter()
                .any(|f| f.code == RETRY_BUDGET_EXHAUSTED_CODE),
            "at least one entry should have been failed by budget exhaustion, not AWS: {:?}",
            outcome.failed
        );
        // Budget-exhausted failures carry the caller's real index, never
        // `usize::MAX` (that sentinel is reserved for an id AWS itself
        // failed to echo back).
        assert!(outcome.failed.iter().all(|f| f.index < chunk_count));
    }

    #[tokio::test]
    async fn single_chunk_call_is_unaffected_by_the_shared_budget() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        // A call that never splits into more than one chunk must retry
        // exactly as many times as `max_retries` always allowed, matching
        // pre-budget behaviour.
        let max_retries = 4u32;
        let call_count = Arc::new(AtomicUsize::new(0));
        let mut budget = RetryBudget::new(max_retries);

        let entries = vec![(0usize, ())];
        let outcome = run_batch_with_retry(
            entries,
            {
                let call_count = call_count.clone();
                move |_batch: Vec<()>| {
                    call_count.fetch_add(1, Ordering::SeqCst);
                    async move { Ok(throttled_failure("0")) }
                }
            },
            max_retries,
            1,
            &mut budget,
        )
        .await
        .expect("run_batch_with_retry itself never fails in this test");

        assert_eq!(call_count.load(Ordering::SeqCst), max_retries as usize);
        assert_eq!(outcome.total(), 1);
        assert_eq!(
            outcome.failed[0].code, "ServiceUnavailable",
            "a single chunk must exhaust its own retry ceiling before ever \
             touching the shared budget's exhaustion path"
        );
    }
}
