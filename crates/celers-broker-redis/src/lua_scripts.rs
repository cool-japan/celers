//! Lua scripts for atomic Redis operations (Kombu compatibility)
//!
//! These scripts ensure atomicity for complex operations that cannot be
//! achieved with single Redis commands.

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

/// Blocking pop with visibility timeout
///
/// Like POP_WITH_VISIBILITY but uses BRPOP for blocking behavior.
///
/// `KEYS[1]`: queue name
/// `KEYS[2]`: unacked set name
/// `ARGV[1]`: visibility timeout (Unix timestamp)
/// `ARGV[2]`: block timeout in seconds
///
/// Returns: message data or nil
pub const BRPOP_WITH_VISIBILITY: &str = r#"
local queue = KEYS[1]
local unacked_set = KEYS[2]
local timeout_at = ARGV[1]
local block_timeout = ARGV[2]

-- Blocking pop from queue
local result = redis.call('BRPOP', queue, block_timeout)

if result then
    local msg = result[2]  -- BRPOP returns [queue_name, message]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[allow(clippy::const_is_empty)]
    fn test_scripts_are_valid() {
        // Verify scripts are non-empty
        assert!(!POP_WITH_VISIBILITY.is_empty());
        assert!(!BRPOP_WITH_VISIBILITY.is_empty());
        assert!(!ACK_MESSAGE.is_empty());
        assert!(!NACK_MESSAGE.is_empty());
        assert!(!RECOVER_TIMED_OUT.is_empty());
        assert!(!POP_PRIORITY_WITH_VISIBILITY.is_empty());
        assert!(!ENQUEUE_WITH_PRIORITY.is_empty());
    }

    #[test]
    fn test_script_syntax() {
        // Basic syntax validation (contains essential keywords)
        assert!(POP_WITH_VISIBILITY.contains("RPOP"));
        assert!(POP_WITH_VISIBILITY.contains("ZADD"));

        assert!(ACK_MESSAGE.contains("ZREM"));

        assert!(NACK_MESSAGE.contains("LPUSH"));

        assert!(RECOVER_TIMED_OUT.contains("ZRANGEBYSCORE"));
    }
}
