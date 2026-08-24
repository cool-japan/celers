//! Lua scripts for atomic Redis operations (Kombu compatibility)
//!
//! These scripts ensure atomicity for complex operations that cannot be
//! achieved with single Redis commands.
//!
//! The ScriptManager handles script loading and caching using SCRIPT LOAD
//! for optimal performance.

use crate::connection::RedisClientExt;
use celers_core::{CelersError, Result};
use redis::{Client, Script};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Pop from queue with visibility timeout (Kombu-compatible)
///
/// This script atomically:
/// 1. Pops a message from the queue (RPOP)
/// 2. Adds it to the unacked sorted set with timeout score (ZADD)
///
/// This ensures that if a worker crashes, the message can be recovered
/// after the visibility timeout expires.
///
/// `KEYS[1]`: queue name (e.g., "celery")
/// `KEYS[2]`: unacked set name (e.g., "celery:unacked")
/// `ARGV[1]`: visibility timeout (Unix timestamp)
///
/// Returns: message data or nil
pub const POP_WITH_VISIBILITY: &str = r#"
local queue = KEYS[1]
local unacked_set = KEYS[2]
local timeout_at = ARGV[1]

-- Pop from queue (non-blocking)
local msg = redis.call('RPOP', queue)

if msg then
    -- Add to unacked set with timeout score
    redis.call('ZADD', unacked_set, timeout_at, msg)
    return msg
end

return nil
"#;

/// Acknowledge (ACK) a message
///
/// Removes the message from the unacked set.
///
/// `KEYS[1]`: unacked set name
/// `ARGV[1]`: message data
///
/// Returns: 1 if removed, 0 if not found
pub const ACK_MESSAGE: &str = r#"
local unacked_set = KEYS[1]
local msg = ARGV[1]

return redis.call('ZREM', unacked_set, msg)
"#;

/// Reject a message (NACK) with optional requeue
///
/// `KEYS[1]`: unacked set name
/// `KEYS[2]`: queue name (for requeue)
/// `KEYS[3]`: dead letter queue name
/// `ARGV[1]`: message data
/// `ARGV[2]`: requeue flag (1 = requeue, 0 = send to DLQ)
///
/// Returns: "requeued", "dlq", or "removed"
pub const NACK_MESSAGE: &str = r#"
local unacked_set = KEYS[1]
local queue = KEYS[2]
local dlq = KEYS[3]
local msg = ARGV[1]
local requeue = ARGV[2]

-- Remove from unacked set
redis.call('ZREM', unacked_set, msg)

if requeue == "1" then
    -- Requeue to original queue
    redis.call('LPUSH', queue, msg)
    return "requeued"
else
    -- Send to dead letter queue
    redis.call('LPUSH', dlq, msg)
    return "dlq"
end
"#;

/// Recover timed-out messages
///
/// Moves messages from unacked set back to queue if they've exceeded
/// the visibility timeout.
///
/// `KEYS[1]`: unacked set name
/// `KEYS[2]`: queue name
/// `ARGV[1]`: current time (Unix timestamp)
/// `ARGV[2]`: max messages to recover
///
/// Returns: number of messages recovered
pub const RECOVER_TIMED_OUT: &str = r#"
local unacked_set = KEYS[1]
local queue = KEYS[2]
local current_time = ARGV[1]
local max_count = ARGV[2]

-- Get messages with score (timeout) less than current time
local messages = redis.call('ZRANGEBYSCORE', unacked_set, '-inf', current_time, 'LIMIT', 0, max_count)

if #messages > 0 then
    -- Remove from unacked set
    for i, msg in ipairs(messages) do
        redis.call('ZREM', unacked_set, msg)
        -- Requeue
        redis.call('LPUSH', queue, msg)
    end
    return #messages
end

return 0
"#;

/// Priority queue pop
///
/// Pops from multiple queues in priority order.
///
/// `KEYS[1..N]`: queue names in priority order (high to low)
/// `KEYS[N+1]`: unacked set name
/// `ARGV[1]`: visibility timeout
///
/// Returns: [queue_name, message] or nil
pub const POP_PRIORITY_WITH_VISIBILITY: &str = r#"
local unacked_set = table.remove(KEYS)
local timeout_at = ARGV[1]

-- Try each queue in order (high priority first)
for i, queue in ipairs(KEYS) do
    local msg = redis.call('RPOP', queue)
    if msg then
        -- Add to unacked set
        redis.call('ZADD', unacked_set, timeout_at, msg)
        return {queue, msg}
    end
end

return nil
"#;

/// Enqueue with priority
///
/// Adds a message to the appropriate priority queue.
///
/// `KEYS[1]`: base queue name
/// `ARGV[1]`: priority (0-9, higher = more priority)
/// `ARGV[2]`: message data
///
/// Returns: queue name used
pub const ENQUEUE_WITH_PRIORITY: &str = r#"
local base_queue = KEYS[1]
local priority = tonumber(ARGV[1])
local msg = ARGV[2]

local queue_name
if priority and priority > 0 then
    -- Kombu priority queue naming convention
    queue_name = base_queue .. '\x06\x16' .. priority
else
    queue_name = base_queue
end

redis.call('LPUSH', queue_name, msg)
return queue_name
"#;

// ---------------------------------------------------------------------------
// Broker-level atomic scripts
//
// Queue direction convention (matches Kombu and the scripts above):
//   * producers push to the **head** (`LPUSH`)
//   * consumers pop from the **tail** (`RPOP` / `BRPOPLPUSH`)
// so the tail holds the oldest message and delivery is FIFO. Every requeue
// path (nack, timeout recovery, delayed promotion) therefore pushes to the
// head, i.e. to the *back* of the delivery order, so recovered or retried
// work never jumps ahead of tasks that were already waiting.
//
// Message field extraction: these scripts never `cjson.decode` a message.
// `SerializedTask::payload` is a `Vec<u8>`, which serde emits as a JSON array
// of numbers — decoding a 1 MB payload would build a million-element Lua
// table while the (single-threaded) server is blocked. Instead the two fields
// that matter are lifted straight out of the raw text:
//
//   * id       — `^{"metadata":{"id":"([^"]+)"` (anchored: `metadata` is the
//                first field of `SerializedTask` and `id` the first field of
//                `TaskMetadata`, so this can only ever match the real id).
//   * priority — `,"priority":(%-?%d+)` (the leading `,"` cannot occur inside
//                a JSON string literal, where a quote is always escaped as
//                `\"`, so a task whose error message happens to contain the
//                text `"priority":7` cannot spoof the match).
//
// `MESSAGE_ID_PATTERN` / `MESSAGE_PRIORITY_PATTERN` below are the single
// source of truth for those patterns and are covered by a layout test.
// ---------------------------------------------------------------------------

/// Lua pattern used by the broker scripts to lift a task id out of a raw
/// serialized message. See the module-level notes on field extraction.
pub const MESSAGE_ID_PATTERN: &str = r#"^{"metadata":{"id":"([^"]+)""#;

/// Lua pattern used by the broker scripts to lift a task priority out of a raw
/// serialized message. See the module-level notes on field extraction.
pub const MESSAGE_PRIORITY_PATTERN: &str = r#","priority":(%-?%d+)"#;

/// Atomic dequeue into the unacked set.
///
/// Pops one message and, in the same server-side step, stages it on the
/// processing list *and* records its visibility deadline in the unacked
/// sorted set. There is therefore no window in which a message exists in
/// neither the queue nor a recoverable structure.
///
/// Messages whose task id is present in the revoked set are dropped (not
/// delivered, not requeued) and the next candidate is examined, up to
/// `ARGV[3]` attempts.
///
/// `KEYS[1]`: queue name (list in FIFO mode, sorted set in Priority mode)
/// `KEYS[2]`: processing list
/// `KEYS[3]`: unacked sorted set
/// `KEYS[4]`: revoked sorted set
/// `KEYS[5]`: pause flag key
/// `ARGV[1]`: visibility deadline (Unix timestamp)
/// `ARGV[2]`: queue mode (`priority` or `fifo`)
/// `ARGV[3]`: maximum pop attempts (revoked messages consume an attempt)
/// `ARGV[4]`: current time (Unix timestamp)
///
/// Returns: `{code, message}` where code `0` = message, `1` = queue empty,
/// `2` = queue paused.
pub const POP_TO_UNACKED: &str = r#"
local queue = KEYS[1]
local processing = KEYS[2]
local unacked = KEYS[3]
local revoked = KEYS[4]
local pause_key = KEYS[5]
local deadline = ARGV[1]
local mode = ARGV[2]
local max_attempts = tonumber(ARGV[3])
local now = ARGV[4]

if redis.call('EXISTS', pause_key) == 1 then
    return {2, ''}
end

local revoked_count = redis.call('ZCARD', revoked)
if revoked_count > 0 then
    redis.call('ZREMRANGEBYSCORE', revoked, '-inf', now)
    revoked_count = redis.call('ZCARD', revoked)
end

local attempts = 0
while attempts < max_attempts do
    attempts = attempts + 1
    local msg
    if mode == 'priority' then
        local popped = redis.call('ZPOPMIN', queue, 1)
        if popped and popped[1] then
            msg = popped[1]
        end
    else
        msg = redis.call('RPOP', queue)
    end

    if not msg then
        return {1, ''}
    end

    local drop = false
    if revoked_count > 0 then
        local id = string.match(msg, '^{"metadata":{"id":"([^"]+)"')
        if id and redis.call('ZSCORE', revoked, id) then
            drop = true
        end
    end

    if not drop then
        redis.call('LPUSH', processing, msg)
        redis.call('ZADD', unacked, deadline, msg)
        return {0, msg}
    end
end

return {1, ''}
"#;

/// Atomic batch dequeue into the unacked set.
///
/// Same contract as [`POP_TO_UNACKED`] but pops up to `ARGV[3]` messages in a
/// single round trip. Returns the (possibly empty) list of messages; an empty
/// list is returned when the queue is drained *or* paused.
///
/// `KEYS[1]`: queue name
/// `KEYS[2]`: processing list
/// `KEYS[3]`: unacked sorted set
/// `KEYS[4]`: revoked sorted set
/// `KEYS[5]`: pause flag key
/// `ARGV[1]`: visibility deadline (Unix timestamp)
/// `ARGV[2]`: queue mode (`priority` or `fifo`)
/// `ARGV[3]`: maximum number of messages to return
/// `ARGV[4]`: current time (Unix timestamp)
/// `ARGV[5]`: maximum number of revoked messages to skip
pub const POP_BATCH_TO_UNACKED: &str = r#"
local queue = KEYS[1]
local processing = KEYS[2]
local unacked = KEYS[3]
local revoked = KEYS[4]
local pause_key = KEYS[5]
local deadline = ARGV[1]
local mode = ARGV[2]
local count = tonumber(ARGV[3])
local now = ARGV[4]
local max_skip = tonumber(ARGV[5])

local results = {}

if redis.call('EXISTS', pause_key) == 1 then
    return results
end

local revoked_count = redis.call('ZCARD', revoked)
if revoked_count > 0 then
    redis.call('ZREMRANGEBYSCORE', revoked, '-inf', now)
    revoked_count = redis.call('ZCARD', revoked)
end

local skipped = 0
while #results < count do
    local msg
    if mode == 'priority' then
        local popped = redis.call('ZPOPMIN', queue, 1)
        if popped and popped[1] then
            msg = popped[1]
        end
    else
        msg = redis.call('RPOP', queue)
    end

    if not msg then
        break
    end

    local drop = false
    if revoked_count > 0 then
        local id = string.match(msg, '^{"metadata":{"id":"([^"]+)"')
        if id and redis.call('ZSCORE', revoked, id) then
            drop = true
        end
    end

    if drop then
        skipped = skipped + 1
        if skipped > max_skip then
            break
        end
    else
        redis.call('LPUSH', processing, msg)
        redis.call('ZADD', unacked, deadline, msg)
        results[#results + 1] = msg
    end
end

return results
"#;

/// Acknowledge a message staged by [`POP_TO_UNACKED`].
///
/// Clears both in-flight structures: the visibility deadline (sorted set) and
/// the processing list entry.
///
/// `KEYS[1]`: unacked sorted set
/// `KEYS[2]`: processing list
/// `ARGV[1]`: message data
///
/// Returns: 1 if the message still held a visibility deadline, 0 otherwise.
pub const ACK_UNACKED: &str = r#"
local removed = redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('LREM', KEYS[2], 1, ARGV[1])
return removed
"#;

/// Reject a message staged by [`POP_TO_UNACKED`].
///
/// Clears both in-flight structures and then either requeues the (possibly
/// rewritten, e.g. retry-count-bumped) payload at the *back* of the queue or
/// moves the original payload to the dead letter queue.
///
/// `KEYS[1]`: unacked sorted set
/// `KEYS[2]`: processing list
/// `KEYS[3]`: queue name
/// `KEYS[4]`: dead letter queue name
/// `ARGV[1]`: original message data (as delivered)
/// `ARGV[2]`: payload to requeue (ignored when not requeueing)
/// `ARGV[3]`: requeue flag (`1` = requeue, `0` = dead letter queue)
/// `ARGV[4]`: queue mode (`priority` or `fifo`)
/// `ARGV[5]`: sorted-set score to requeue with (Priority mode only)
///
/// Returns: `"requeued"` or `"dlq"`.
pub const NACK_UNACKED: &str = r#"
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('LREM', KEYS[2], 1, ARGV[1])

if ARGV[3] == '1' then
    if ARGV[4] == 'priority' then
        redis.call('ZADD', KEYS[3], tonumber(ARGV[5]), ARGV[2])
    else
        redis.call('LPUSH', KEYS[3], ARGV[2])
    end
    return 'requeued'
end

redis.call('LPUSH', KEYS[4], ARGV[1])
return 'dlq'
"#;

/// Reclaim in-flight messages whose visibility timeout has expired.
///
/// Runs in two phases:
///
/// 1. *Orphan adoption* — any message staged on the processing list that
///    carries no visibility deadline (because the worker died between the
///    blocking `BRPOPLPUSH` and the deadline write) is given one, so it can
///    never be stranded. The oldest entries are inspected, which is the tail
///    of the list: producers and stagers both push to the head.
/// 2. *Recovery* — messages whose deadline has passed are removed from both
///    in-flight structures and requeued at the back of the queue.
///
/// `KEYS[1]`: unacked sorted set
/// `KEYS[2]`: queue name
/// `KEYS[3]`: processing list
/// `ARGV[1]`: current time (Unix timestamp)
/// `ARGV[2]`: deadline to give adopted orphans (Unix timestamp)
/// `ARGV[3]`: maximum messages to inspect/recover in one pass
/// `ARGV[4]`: queue mode (`priority` or `fifo`)
///
/// Returns: number of messages requeued.
pub const REAP_EXPIRED: &str = r#"
local unacked = KEYS[1]
local queue = KEYS[2]
local processing = KEYS[3]
local now = ARGV[1]
local orphan_deadline = ARGV[2]
local max_count = tonumber(ARGV[3])
local mode = ARGV[4]

local staged = redis.call('LRANGE', processing, -max_count, -1)
for i = 1, #staged do
    local msg = staged[i]
    if not redis.call('ZSCORE', unacked, msg) then
        redis.call('ZADD', unacked, orphan_deadline, msg)
    end
end

local expired = redis.call('ZRANGEBYSCORE', unacked, '-inf', now, 'LIMIT', 0, max_count)
local recovered = 0
for i = 1, #expired do
    local msg = expired[i]
    if redis.call('ZREM', unacked, msg) == 1 then
        redis.call('LREM', processing, 1, msg)
        if mode == 'priority' then
            local score = 0
            local priority = string.match(msg, ',"priority":(%-?%d+)')
            if priority then
                score = -tonumber(priority)
            end
            redis.call('ZADD', queue, score, msg)
        else
            redis.call('LPUSH', queue, msg)
        end
        recovered = recovered + 1
    end
end

return recovered
"#;

/// Move due delayed tasks onto the main queue.
///
/// The `ZREM` happens before the push, inside the script, so concurrent
/// sweepers cannot both claim the same message — which is exactly the
/// duplicate-delivery hole in a read-then-write pipeline.
///
/// `KEYS[1]`: delayed sorted set
/// `KEYS[2]`: queue name
/// `ARGV[1]`: current time (Unix timestamp)
/// `ARGV[2]`: maximum messages to promote in one pass
/// `ARGV[3]`: queue mode (`priority` or `fifo`)
///
/// Returns: number of messages promoted.
pub const PROMOTE_DELAYED: &str = r#"
local delayed = KEYS[1]
local queue = KEYS[2]
local now = ARGV[1]
local max_count = ARGV[2]
local mode = ARGV[3]

local ready = redis.call('ZRANGEBYSCORE', delayed, '-inf', now, 'LIMIT', 0, max_count)
local moved = 0

for i = 1, #ready do
    local msg = ready[i]
    if redis.call('ZREM', delayed, msg) == 1 then
        if mode == 'priority' then
            local score = 0
            local priority = string.match(msg, ',"priority":(%-?%d+)')
            if priority then
                score = -tonumber(priority)
            end
            redis.call('ZADD', queue, score, msg)
        else
            redis.call('LPUSH', queue, msg)
        end
        moved = moved + 1
    end
end

return moved
"#;

/// Durably revoke a task id and best-effort remove it from the pending
/// structures.
///
/// The revocation is recorded in a sorted set scored by its expiry, so
/// consumers can reject the task even if it is already in flight elsewhere,
/// and expired revocations are pruned on every call rather than accumulating.
///
/// `KEYS[1]`: revoked sorted set
/// `KEYS[2]`: queue name
/// `KEYS[3]`: delayed sorted set
/// `ARGV[1]`: task id
/// `ARGV[2]`: current time (Unix timestamp)
/// `ARGV[3]`: revocation lifetime in seconds
/// `ARGV[4]`: queue mode (`priority` or `fifo`)
/// `ARGV[5]`: maximum entries to scan per structure
///
/// Returns: number of pending copies removed.
pub const REVOKE_TASK: &str = r#"
local revoked = KEYS[1]
local queue = KEYS[2]
local delayed = KEYS[3]
local task_id = ARGV[1]
local now = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
local mode = ARGV[4]
local scan_limit = tonumber(ARGV[5])

redis.call('ZREMRANGEBYSCORE', revoked, '-inf', now)
redis.call('ZADD', revoked, now + ttl, task_id)

local removed = 0

local function purge(key, is_list)
    local entries
    if is_list then
        entries = redis.call('LRANGE', key, 0, scan_limit - 1)
    else
        entries = redis.call('ZRANGE', key, 0, scan_limit - 1)
    end
    for i = 1, #entries do
        local entry = entries[i]
        local id = string.match(entry, '^{"metadata":{"id":"([^"]+)"')
        if id == task_id then
            if is_list then
                redis.call('LREM', key, 1, entry)
            else
                redis.call('ZREM', key, entry)
            end
            removed = removed + 1
        end
    end
end

purge(queue, mode ~= 'priority')
purge(delayed, false)

return removed
"#;

/// Move a batch of dead-letter entries back onto the main queue in one pass.
///
/// `ARGV` carries `(original, replacement, score)` triples so the caller can
/// rewrite each payload (resetting task state) without giving up atomicity:
/// the `LREM` that claims an entry and the push that re-enqueues it happen in
/// the same server-side step, so a crash cannot duplicate or lose a task.
///
/// `KEYS[1]`: dead letter queue name
/// `KEYS[2]`: queue name
/// `ARGV[1]`: queue mode (`priority` or `fifo`)
/// `ARGV[2..]`: repeating `(original, replacement, score)` triples
///
/// Returns: number of entries actually moved.
pub const REPLAY_DLQ: &str = r#"
local dlq = KEYS[1]
local queue = KEYS[2]
local mode = ARGV[1]

local moved = 0
local i = 2
while i + 2 <= #ARGV do
    local original = ARGV[i]
    local replacement = ARGV[i + 1]
    local score = tonumber(ARGV[i + 2])
    if redis.call('LREM', dlq, 1, original) > 0 then
        if mode == 'priority' then
            redis.call('ZADD', queue, score, replacement)
        else
            redis.call('LPUSH', queue, replacement)
        end
        moved = moved + 1
    end
    i = i + 3
end

return moved
"#;

/// Current script version (increment when scripts change)
pub const SCRIPT_VERSION: u32 = 2;

/// Script identifier for easy lookup
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScriptId {
    /// Pop with visibility timeout
    PopWithVisibility,
    /// Acknowledge message
    AckMessage,
    /// Reject message
    NackMessage,
    /// Recover timed-out messages
    RecoverTimedOut,
    /// Priority queue pop with visibility
    PopPriorityWithVisibility,
    /// Enqueue with priority
    EnqueueWithPriority,
    /// Atomic dequeue into the unacked set
    PopToUnacked,
    /// Atomic batch dequeue into the unacked set
    PopBatchToUnacked,
    /// Acknowledge a message staged by [`ScriptId::PopToUnacked`]
    AckUnacked,
    /// Reject a message staged by [`ScriptId::PopToUnacked`]
    NackUnacked,
    /// Reclaim in-flight messages past their visibility deadline
    ReapExpired,
    /// Promote due delayed tasks onto the main queue
    PromoteDelayed,
    /// Record a revocation and purge pending copies
    RevokeTask,
    /// Replay a batch of dead-letter entries
    ReplayDlq,
}

impl ScriptId {
    /// Get the script source code
    pub fn source(&self) -> &'static str {
        match self {
            ScriptId::PopWithVisibility => POP_WITH_VISIBILITY,
            ScriptId::AckMessage => ACK_MESSAGE,
            ScriptId::NackMessage => NACK_MESSAGE,
            ScriptId::RecoverTimedOut => RECOVER_TIMED_OUT,
            ScriptId::PopPriorityWithVisibility => POP_PRIORITY_WITH_VISIBILITY,
            ScriptId::EnqueueWithPriority => ENQUEUE_WITH_PRIORITY,
            ScriptId::PopToUnacked => POP_TO_UNACKED,
            ScriptId::PopBatchToUnacked => POP_BATCH_TO_UNACKED,
            ScriptId::AckUnacked => ACK_UNACKED,
            ScriptId::NackUnacked => NACK_UNACKED,
            ScriptId::ReapExpired => REAP_EXPIRED,
            ScriptId::PromoteDelayed => PROMOTE_DELAYED,
            ScriptId::RevokeTask => REVOKE_TASK,
            ScriptId::ReplayDlq => REPLAY_DLQ,
        }
    }

    /// Get a human-readable name
    pub fn name(&self) -> &'static str {
        match self {
            ScriptId::PopWithVisibility => "pop_with_visibility",
            ScriptId::AckMessage => "ack_message",
            ScriptId::NackMessage => "nack_message",
            ScriptId::RecoverTimedOut => "recover_timed_out",
            ScriptId::PopPriorityWithVisibility => "pop_priority_with_visibility",
            ScriptId::EnqueueWithPriority => "enqueue_with_priority",
            ScriptId::PopToUnacked => "pop_to_unacked",
            ScriptId::PopBatchToUnacked => "pop_batch_to_unacked",
            ScriptId::AckUnacked => "ack_unacked",
            ScriptId::NackUnacked => "nack_unacked",
            ScriptId::ReapExpired => "reap_expired",
            ScriptId::PromoteDelayed => "promote_delayed",
            ScriptId::RevokeTask => "revoke_task",
            ScriptId::ReplayDlq => "replay_dlq",
        }
    }

    /// Get all script IDs
    pub fn all() -> Vec<ScriptId> {
        vec![
            ScriptId::PopWithVisibility,
            ScriptId::AckMessage,
            ScriptId::NackMessage,
            ScriptId::RecoverTimedOut,
            ScriptId::PopPriorityWithVisibility,
            ScriptId::EnqueueWithPriority,
            ScriptId::PopToUnacked,
            ScriptId::PopBatchToUnacked,
            ScriptId::AckUnacked,
            ScriptId::NackUnacked,
            ScriptId::ReapExpired,
            ScriptId::PromoteDelayed,
            ScriptId::RevokeTask,
            ScriptId::ReplayDlq,
        ]
    }
}

/// Performance metrics for a single script
#[derive(Debug, Clone, Default)]
pub struct ScriptPerformance {
    /// Number of times the script was executed
    pub execution_count: u64,
    /// Total execution time
    pub total_duration: Duration,
    /// Minimum execution time
    pub min_duration: Option<Duration>,
    /// Maximum execution time
    pub max_duration: Option<Duration>,
    /// Last execution time
    pub last_execution: Option<Instant>,
}

impl ScriptPerformance {
    /// Get average execution time
    pub fn avg_duration(&self) -> Option<Duration> {
        if self.execution_count > 0 {
            Some(self.total_duration / self.execution_count as u32)
        } else {
            None
        }
    }

    /// Record a new execution
    pub fn record(&mut self, duration: Duration) {
        self.execution_count += 1;
        self.total_duration += duration;
        self.last_execution = Some(Instant::now());

        match self.min_duration {
            None => self.min_duration = Some(duration),
            Some(min) if duration < min => self.min_duration = Some(duration),
            _ => {}
        }

        match self.max_duration {
            None => self.max_duration = Some(duration),
            Some(max) if duration > max => self.max_duration = Some(duration),
            _ => {}
        }
    }

    /// Reset all statistics
    pub fn reset(&mut self) {
        *self = Self::default();
    }
}

/// Manages Lua script loading and caching
pub struct ScriptManager {
    client: Client,
    /// Maps script ID to SHA1 hash
    sha_cache: Arc<RwLock<HashMap<ScriptId, String>>>,
    /// Maps script ID to Script object
    script_cache: Arc<RwLock<HashMap<ScriptId, Script>>>,
    /// Performance tracking for each script
    performance: Arc<RwLock<HashMap<ScriptId, ScriptPerformance>>>,
    /// Script version
    version: u32,
}

impl ScriptManager {
    /// Create a new script manager
    pub fn new(client: Client) -> Self {
        Self {
            client,
            sha_cache: Arc::new(RwLock::new(HashMap::new())),
            script_cache: Arc::new(RwLock::new(HashMap::new())),
            performance: Arc::new(RwLock::new(HashMap::new())),
            version: SCRIPT_VERSION,
        }
    }

    /// Get the script version
    pub fn version(&self) -> u32 {
        self.version
    }

    /// Record script execution performance
    pub async fn record_execution(&self, script_id: ScriptId, duration: Duration) {
        let mut perf = self.performance.write().await;
        perf.entry(script_id).or_default().record(duration);

        // Warn if execution is slow
        if duration.as_millis() > 100 {
            warn!(
                "Slow script execution: {} took {}ms",
                script_id.name(),
                duration.as_millis()
            );
        }
    }

    /// Get performance metrics for a script
    pub async fn get_performance(&self, script_id: ScriptId) -> Option<ScriptPerformance> {
        self.performance.read().await.get(&script_id).cloned()
    }

    /// Get all performance metrics
    pub async fn get_all_performance(&self) -> HashMap<ScriptId, ScriptPerformance> {
        self.performance.read().await.clone()
    }

    /// Reset performance metrics for a specific script
    pub async fn reset_performance(&self, script_id: ScriptId) {
        let mut perf = self.performance.write().await;
        if let Some(p) = perf.get_mut(&script_id) {
            p.reset();
        }
    }

    /// Reset all performance metrics
    pub async fn reset_all_performance(&self) {
        let mut perf = self.performance.write().await;
        for p in perf.values_mut() {
            p.reset();
        }
    }

    /// Load all scripts into Redis and cache their SHA1 hashes
    pub async fn load_all(&self) -> Result<()> {
        let mut conn = self
            .client
            .celers_multiplexed_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let mut sha_cache = self.sha_cache.write().await;
        let mut script_cache = self.script_cache.write().await;

        for script_id in ScriptId::all() {
            let source = script_id.source();
            let script = Script::new(source);

            // Load script and get SHA1
            let sha: String = redis::cmd("SCRIPT")
                .arg("LOAD")
                .arg(source)
                .query_async(&mut conn)
                .await
                .map_err(|e| {
                    CelersError::Broker(format!(
                        "Failed to load script {}: {}",
                        script_id.name(),
                        e
                    ))
                })?;

            debug!("Loaded script {} with SHA: {}", script_id.name(), sha);

            sha_cache.insert(script_id, sha);
            script_cache.insert(script_id, script);
        }

        info!("Loaded {} Lua scripts into Redis", ScriptId::all().len());

        Ok(())
    }

    /// Get the SHA1 hash for a script
    pub async fn get_sha(&self, script_id: ScriptId) -> Option<String> {
        self.sha_cache.read().await.get(&script_id).cloned()
    }

    /// Get a Script object for execution
    pub async fn get_script(&self, script_id: ScriptId) -> Option<Script> {
        self.script_cache.read().await.get(&script_id).cloned()
    }

    /// Load a single script
    pub async fn load_script(&self, script_id: ScriptId) -> Result<String> {
        let mut conn = self
            .client
            .celers_multiplexed_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let source = script_id.source();
        let script = Script::new(source);

        // Load script and get SHA1
        let sha: String = redis::cmd("SCRIPT")
            .arg("LOAD")
            .arg(source)
            .query_async(&mut conn)
            .await
            .map_err(|e| {
                CelersError::Broker(format!("Failed to load script {}: {}", script_id.name(), e))
            })?;

        debug!("Loaded script {} with SHA: {}", script_id.name(), sha);

        // Update caches
        let mut sha_cache = self.sha_cache.write().await;
        let mut script_cache = self.script_cache.write().await;

        sha_cache.insert(script_id, sha.clone());
        script_cache.insert(script_id, script);

        Ok(sha)
    }

    /// Check if a script is loaded in Redis
    pub async fn is_loaded(&self, script_id: ScriptId) -> Result<bool> {
        let sha = match self.get_sha(script_id).await {
            Some(sha) => sha,
            None => return Ok(false),
        };

        let mut conn = self
            .client
            .celers_multiplexed_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let exists: Vec<bool> = redis::cmd("SCRIPT")
            .arg("EXISTS")
            .arg(&sha)
            .query_async(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to check script: {}", e)))?;

        Ok(exists.first().copied().unwrap_or(false))
    }

    /// Clear the script cache (useful for testing or after Redis restart)
    pub async fn clear_cache(&self) {
        let mut sha_cache = self.sha_cache.write().await;
        let mut script_cache = self.script_cache.write().await;

        sha_cache.clear();
        script_cache.clear();

        debug!("Cleared script cache");
    }

    /// Get statistics about loaded scripts
    pub async fn stats(&self) -> ScriptStats {
        let sha_cache = self.sha_cache.read().await;
        let script_cache = self.script_cache.read().await;
        let perf = self.performance.read().await;

        let total_executions: u64 = perf.values().map(|p| p.execution_count).sum();

        ScriptStats {
            total_scripts: ScriptId::all().len(),
            loaded_scripts: sha_cache.len(),
            cached_scripts: script_cache.len(),
            version: self.version,
            total_executions,
        }
    }
}

/// Script manager statistics
#[derive(Debug, Clone)]
pub struct ScriptStats {
    /// Total number of available scripts
    pub total_scripts: usize,
    /// Number of scripts loaded in Redis
    pub loaded_scripts: usize,
    /// Number of scripts cached in memory
    pub cached_scripts: usize,
    /// Script version
    pub version: u32,
    /// Total number of script executions
    pub total_executions: u64,
}

impl ScriptStats {
    /// Check if all scripts are loaded
    pub fn all_loaded(&self) -> bool {
        self.loaded_scripts == self.total_scripts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::const_is_empty)]
    fn test_scripts_are_valid() {
        // Verify scripts are non-empty
        for script_id in ScriptId::all() {
            assert!(
                !script_id.source().is_empty(),
                "script {} has no source",
                script_id.name()
            );
        }
    }

    #[test]
    fn test_script_syntax() {
        // Basic syntax validation (contains essential keywords)
        assert!(POP_WITH_VISIBILITY.contains("RPOP"));
        assert!(POP_WITH_VISIBILITY.contains("ZADD"));

        assert!(ACK_MESSAGE.contains("ZREM"));

        assert!(NACK_MESSAGE.contains("LPUSH"));

        assert!(RECOVER_TIMED_OUT.contains("ZRANGEBYSCORE"));

        // The broker-level scripts must stage into *both* in-flight
        // structures, otherwise a crashed worker's message is unrecoverable.
        assert!(POP_TO_UNACKED.contains("LPUSH"));
        assert!(POP_TO_UNACKED.contains("ZADD"));
        assert!(ACK_UNACKED.contains("ZREM"));
        assert!(ACK_UNACKED.contains("LREM"));
        assert!(REAP_EXPIRED.contains("ZRANGEBYSCORE"));
        assert!(PROMOTE_DELAYED.contains("ZREM"));
        assert!(REVOKE_TASK.contains("ZREMRANGEBYSCORE"));
        assert!(REPLAY_DLQ.contains("LREM"));
    }

    /// The dequeue/consume direction must be the mirror image of the produce
    /// direction, or the queue silently becomes a stack. Producers push to
    /// the head (`LPUSH`), consumers pop from the tail (`RPOP`).
    #[test]
    fn test_scripts_agree_on_queue_direction() {
        for script in [POP_WITH_VISIBILITY, POP_PRIORITY_WITH_VISIBILITY] {
            assert!(
                script.contains("'RPOP'"),
                "consumers must pop from the tail"
            );
            assert!(
                !script.contains("'LPOP'") && !script.contains("'BRPOP'"),
                "consumers must not pop from the head, and must never block inside a script"
            );
        }

        for script in [
            ENQUEUE_WITH_PRIORITY,
            NACK_MESSAGE,
            RECOVER_TIMED_OUT,
            NACK_UNACKED,
            REAP_EXPIRED,
            PROMOTE_DELAYED,
            REPLAY_DLQ,
        ] {
            assert!(
                script.contains("'LPUSH'"),
                "producers/requeue paths must push to the head"
            );
            assert!(
                !script.contains("'RPUSH'"),
                "pushing to the tail would put the message at the front of the delivery order"
            );
        }
    }

    /// Blocking commands are forbidden inside Lua: Redis is single-threaded,
    /// so a script may not park the server. `BRPOP` inside `EVAL` silently
    /// behaves as if the timeout had already elapsed.
    #[test]
    fn test_no_blocking_commands_in_scripts() {
        for script_id in ScriptId::all() {
            let source = script_id.source();
            for blocking in [
                "BRPOP",
                "BLPOP",
                "BRPOPLPUSH",
                "BLMOVE",
                "BLMPOP",
                "BZPOPMIN",
            ] {
                assert!(
                    !source.contains(blocking),
                    "script {} calls the blocking command {}",
                    script_id.name(),
                    blocking
                );
            }
        }
    }

    /// The scripts lift `id`/`priority` out of the raw message text rather
    /// than `cjson.decode`ing it (payloads are JSON arrays of bytes, which are
    /// ruinous to decode server-side). Keep the patterns in the scripts and
    /// the documented constants from drifting apart.
    #[test]
    fn test_scripts_use_documented_field_patterns() {
        for script in [POP_TO_UNACKED, POP_BATCH_TO_UNACKED, REVOKE_TASK] {
            assert!(
                script.contains(MESSAGE_ID_PATTERN),
                "id extraction must use the documented pattern"
            );
        }
        for script in [REAP_EXPIRED, PROMOTE_DELAYED] {
            assert!(
                script.contains(MESSAGE_PRIORITY_PATTERN),
                "priority extraction must use the documented pattern"
            );
        }
        for script_id in ScriptId::all() {
            assert!(
                !script_id.source().contains("cjson"),
                "script {} decodes JSON server-side",
                script_id.name()
            );
        }
    }

    #[test]
    fn test_script_id_source() {
        assert_eq!(ScriptId::PopWithVisibility.source(), POP_WITH_VISIBILITY);
        assert_eq!(ScriptId::AckMessage.source(), ACK_MESSAGE);
        assert_eq!(ScriptId::NackMessage.source(), NACK_MESSAGE);
        assert_eq!(ScriptId::RecoverTimedOut.source(), RECOVER_TIMED_OUT);
        assert_eq!(
            ScriptId::PopPriorityWithVisibility.source(),
            POP_PRIORITY_WITH_VISIBILITY
        );
        assert_eq!(
            ScriptId::EnqueueWithPriority.source(),
            ENQUEUE_WITH_PRIORITY
        );
        assert_eq!(ScriptId::PopToUnacked.source(), POP_TO_UNACKED);
        assert_eq!(ScriptId::PopBatchToUnacked.source(), POP_BATCH_TO_UNACKED);
        assert_eq!(ScriptId::AckUnacked.source(), ACK_UNACKED);
        assert_eq!(ScriptId::NackUnacked.source(), NACK_UNACKED);
        assert_eq!(ScriptId::ReapExpired.source(), REAP_EXPIRED);
        assert_eq!(ScriptId::PromoteDelayed.source(), PROMOTE_DELAYED);
        assert_eq!(ScriptId::RevokeTask.source(), REVOKE_TASK);
        assert_eq!(ScriptId::ReplayDlq.source(), REPLAY_DLQ);
    }

    #[test]
    fn test_script_id_name() {
        assert_eq!(ScriptId::PopWithVisibility.name(), "pop_with_visibility");
        assert_eq!(ScriptId::AckMessage.name(), "ack_message");
        assert_eq!(ScriptId::NackMessage.name(), "nack_message");
        assert_eq!(ScriptId::RecoverTimedOut.name(), "recover_timed_out");
        assert_eq!(
            ScriptId::PopPriorityWithVisibility.name(),
            "pop_priority_with_visibility"
        );
        assert_eq!(
            ScriptId::EnqueueWithPriority.name(),
            "enqueue_with_priority"
        );
        assert_eq!(ScriptId::PopToUnacked.name(), "pop_to_unacked");
        assert_eq!(ScriptId::PopBatchToUnacked.name(), "pop_batch_to_unacked");
        assert_eq!(ScriptId::ReapExpired.name(), "reap_expired");
        assert_eq!(ScriptId::PromoteDelayed.name(), "promote_delayed");
        assert_eq!(ScriptId::RevokeTask.name(), "revoke_task");
        assert_eq!(ScriptId::ReplayDlq.name(), "replay_dlq");
    }

    #[test]
    fn test_script_id_all() {
        let all_scripts = ScriptId::all();
        assert_eq!(all_scripts.len(), 14);

        // Every variant must be reachable through `all()` (and therefore be
        // loaded by `load_all`), and no variant may appear twice.
        let unique: std::collections::HashSet<_> = all_scripts.iter().collect();
        assert_eq!(unique.len(), all_scripts.len());

        assert!(all_scripts.contains(&ScriptId::PopWithVisibility));
        assert!(all_scripts.contains(&ScriptId::AckMessage));
        assert!(all_scripts.contains(&ScriptId::NackMessage));
        assert!(all_scripts.contains(&ScriptId::RecoverTimedOut));
        assert!(all_scripts.contains(&ScriptId::PopPriorityWithVisibility));
        assert!(all_scripts.contains(&ScriptId::EnqueueWithPriority));
        assert!(all_scripts.contains(&ScriptId::PopToUnacked));
        assert!(all_scripts.contains(&ScriptId::PopBatchToUnacked));
        assert!(all_scripts.contains(&ScriptId::AckUnacked));
        assert!(all_scripts.contains(&ScriptId::NackUnacked));
        assert!(all_scripts.contains(&ScriptId::ReapExpired));
        assert!(all_scripts.contains(&ScriptId::PromoteDelayed));
        assert!(all_scripts.contains(&ScriptId::RevokeTask));
        assert!(all_scripts.contains(&ScriptId::ReplayDlq));
    }

    #[test]
    fn test_script_stats() {
        let total = ScriptId::all().len();
        let stats = ScriptStats {
            total_scripts: total,
            loaded_scripts: total,
            cached_scripts: total,
            version: SCRIPT_VERSION,
            total_executions: 0,
        };

        assert!(stats.all_loaded());
        assert_eq!(stats.version, SCRIPT_VERSION);

        let stats_incomplete = ScriptStats {
            total_scripts: total,
            loaded_scripts: total - 2,
            cached_scripts: total - 2,
            version: SCRIPT_VERSION,
            total_executions: 0,
        };

        assert!(!stats_incomplete.all_loaded());
    }

    #[test]
    fn test_script_performance() {
        let mut perf = ScriptPerformance::default();
        assert_eq!(perf.execution_count, 0);
        assert_eq!(perf.avg_duration(), None);

        perf.record(Duration::from_millis(10));
        assert_eq!(perf.execution_count, 1);
        assert_eq!(perf.avg_duration(), Some(Duration::from_millis(10)));
        assert_eq!(perf.min_duration, Some(Duration::from_millis(10)));
        assert_eq!(perf.max_duration, Some(Duration::from_millis(10)));

        perf.record(Duration::from_millis(20));
        assert_eq!(perf.execution_count, 2);
        assert_eq!(perf.avg_duration(), Some(Duration::from_millis(15)));
        assert_eq!(perf.min_duration, Some(Duration::from_millis(10)));
        assert_eq!(perf.max_duration, Some(Duration::from_millis(20)));

        perf.reset();
        assert_eq!(perf.execution_count, 0);
        assert_eq!(perf.avg_duration(), None);
    }

    #[test]
    fn test_script_version() {
        assert_eq!(SCRIPT_VERSION, 2);
    }

    /// The Lua field patterns are only safe because of the exact JSON layout
    /// serde produces for `SerializedTask`. Pin that layout here: if the
    /// field order ever changes, this fails instead of the scripts silently
    /// extracting the wrong value at runtime.
    #[test]
    fn test_serialized_task_json_layout_matches_lua_patterns() {
        use celers_core::SerializedTask;

        let mut task = SerializedTask::new("layout_probe".to_string(), vec![0, 34, 255]);
        task.metadata.priority = -3;
        let json = serde_json::to_string(&task).expect("serialize");

        // `MESSAGE_ID_PATTERN` is anchored at the start of the message.
        let expected_prefix = format!(r#"{{"metadata":{{"id":"{}""#, task.metadata.id);
        assert!(
            json.starts_with(&expected_prefix),
            "message must start with metadata.id, got: {}",
            &json[..json.len().min(80)]
        );

        // `MESSAGE_PRIORITY_PATTERN` matches on a leading `,"` which cannot
        // appear inside a JSON string literal (a quote is escaped there).
        assert!(
            json.contains(r#","priority":-3"#),
            "negative priorities must be matchable: {json}"
        );

        // The payload really is a JSON array of numbers -- the reason the
        // scripts must not `cjson.decode` a message.
        assert!(json.contains(r#""payload":[0,34,255]"#), "{json}");

        // `group_id`/`chord_id` must not be mistaken for the task id: the
        // anchored pattern only matches at offset 0, and even unanchored the
        // `_id` spelling differs.
        assert!(!json.contains(r#""group_id""#));
    }
}
