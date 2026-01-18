//! Lua scripts for atomic Redis operations (Kombu compatibility)
//!
//! These scripts ensure atomicity for complex operations that cannot be
//! achieved with single Redis commands.
//!
//! The ScriptManager handles script loading and caching using SCRIPT LOAD
//! for optimal performance.

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

/// Current script version (increment when scripts change)
pub const SCRIPT_VERSION: u32 = 1;

/// Script identifier for easy lookup
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ScriptId {
    /// Pop with visibility timeout
    PopWithVisibility,
    /// Blocking pop with visibility timeout
    BrpopWithVisibility,
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
}

impl ScriptId {
    /// Get the script source code
    pub fn source(&self) -> &'static str {
        match self {
            ScriptId::PopWithVisibility => POP_WITH_VISIBILITY,
            ScriptId::BrpopWithVisibility => BRPOP_WITH_VISIBILITY,
            ScriptId::AckMessage => ACK_MESSAGE,
            ScriptId::NackMessage => NACK_MESSAGE,
            ScriptId::RecoverTimedOut => RECOVER_TIMED_OUT,
            ScriptId::PopPriorityWithVisibility => POP_PRIORITY_WITH_VISIBILITY,
            ScriptId::EnqueueWithPriority => ENQUEUE_WITH_PRIORITY,
        }
    }

    /// Get a human-readable name
    pub fn name(&self) -> &'static str {
        match self {
            ScriptId::PopWithVisibility => "pop_with_visibility",
            ScriptId::BrpopWithVisibility => "brpop_with_visibility",
            ScriptId::AckMessage => "ack_message",
            ScriptId::NackMessage => "nack_message",
            ScriptId::RecoverTimedOut => "recover_timed_out",
            ScriptId::PopPriorityWithVisibility => "pop_priority_with_visibility",
            ScriptId::EnqueueWithPriority => "enqueue_with_priority",
        }
    }

    /// Get all script IDs
    pub fn all() -> Vec<ScriptId> {
        vec![
            ScriptId::PopWithVisibility,
            ScriptId::BrpopWithVisibility,
            ScriptId::AckMessage,
            ScriptId::NackMessage,
            ScriptId::RecoverTimedOut,
            ScriptId::PopPriorityWithVisibility,
            ScriptId::EnqueueWithPriority,
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
            .get_multiplexed_async_connection()
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
            .get_multiplexed_async_connection()
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
            .get_multiplexed_async_connection()
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

    #[test]
    fn test_script_id_source() {
        assert_eq!(ScriptId::PopWithVisibility.source(), POP_WITH_VISIBILITY);
        assert_eq!(
            ScriptId::BrpopWithVisibility.source(),
            BRPOP_WITH_VISIBILITY
        );
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
    }

    #[test]
    fn test_script_id_name() {
        assert_eq!(ScriptId::PopWithVisibility.name(), "pop_with_visibility");
        assert_eq!(
            ScriptId::BrpopWithVisibility.name(),
            "brpop_with_visibility"
        );
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
    }

    #[test]
    fn test_script_id_all() {
        let all_scripts = ScriptId::all();
        assert_eq!(all_scripts.len(), 7);
        assert!(all_scripts.contains(&ScriptId::PopWithVisibility));
        assert!(all_scripts.contains(&ScriptId::BrpopWithVisibility));
        assert!(all_scripts.contains(&ScriptId::AckMessage));
        assert!(all_scripts.contains(&ScriptId::NackMessage));
        assert!(all_scripts.contains(&ScriptId::RecoverTimedOut));
        assert!(all_scripts.contains(&ScriptId::PopPriorityWithVisibility));
        assert!(all_scripts.contains(&ScriptId::EnqueueWithPriority));
    }

    #[test]
    fn test_script_stats() {
        let stats = ScriptStats {
            total_scripts: 7,
            loaded_scripts: 7,
            cached_scripts: 7,
            version: SCRIPT_VERSION,
            total_executions: 0,
        };

        assert!(stats.all_loaded());
        assert_eq!(stats.version, SCRIPT_VERSION);

        let stats_incomplete = ScriptStats {
            total_scripts: 7,
            loaded_scripts: 5,
            cached_scripts: 5,
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
        assert_eq!(SCRIPT_VERSION, 1);
    }
}
