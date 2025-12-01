//! Visibility timeout implementation using Lua scripts
//!
//! This module provides Kombu-compatible visibility timeout for Redis,
//! ensuring tasks can be recovered if workers crash.

use crate::lua_scripts;
use redis::{aio::MultiplexedConnection, Script};
use std::time::{SystemTime, UNIX_EPOCH};

/// Helper for executing Lua scripts with visibility timeout
pub struct VisibilityManager {
    pop_script: Script,
    brpop_script: Script,
    ack_script: Script,
    nack_script: Script,
    recover_script: Script,
}

impl VisibilityManager {
    pub fn new() -> Self {
        Self {
            pop_script: Script::new(lua_scripts::POP_WITH_VISIBILITY),
            brpop_script: Script::new(lua_scripts::BRPOP_WITH_VISIBILITY),
            ack_script: Script::new(lua_scripts::ACK_MESSAGE),
            nack_script: Script::new(lua_scripts::NACK_MESSAGE),
            recover_script: Script::new(lua_scripts::RECOVER_TIMED_OUT),
        }
    }

    /// Pop a message with visibility timeout (non-blocking)
    pub async fn pop_with_visibility(
        &self,
        conn: &mut MultiplexedConnection,
        queue: &str,
        unacked_set: &str,
        visibility_timeout_secs: u64,
    ) -> redis::RedisResult<Option<String>> {
        let timeout_at = current_timestamp() + visibility_timeout_secs;

        let result: Option<String> = self
            .pop_script
            .key(queue)
            .key(unacked_set)
            .arg(timeout_at)
            .invoke_async(conn)
            .await?;

        Ok(result)
    }

    /// Blocking pop with visibility timeout
    pub async fn brpop_with_visibility(
        &self,
        conn: &mut MultiplexedConnection,
        queue: &str,
        unacked_set: &str,
        visibility_timeout_secs: u64,
        block_timeout_secs: u64,
    ) -> redis::RedisResult<Option<String>> {
        let timeout_at = current_timestamp() + visibility_timeout_secs;

        let result: Option<String> = self
            .brpop_script
            .key(queue)
            .key(unacked_set)
            .arg(timeout_at)
            .arg(block_timeout_secs)
            .invoke_async(conn)
            .await?;

        Ok(result)
    }

    /// Acknowledge a message (remove from unacked set)
    pub async fn ack(
        &self,
        conn: &mut MultiplexedConnection,
        unacked_set: &str,
        message: &str,
    ) -> redis::RedisResult<i64> {
        let removed: i64 = self
            .ack_script
            .key(unacked_set)
            .arg(message)
            .invoke_async(conn)
            .await?;

        Ok(removed)
    }

    /// Reject a message (send to DLQ or requeue)
    pub async fn nack(
        &self,
        conn: &mut MultiplexedConnection,
        unacked_set: &str,
        queue: &str,
        dlq: &str,
        message: &str,
        requeue: bool,
    ) -> redis::RedisResult<String> {
        let result: String = self
            .nack_script
            .key(unacked_set)
            .key(queue)
            .key(dlq)
            .arg(message)
            .arg(if requeue { "1" } else { "0" })
            .invoke_async(conn)
            .await?;

        Ok(result)
    }

    /// Recover timed-out messages (move back to queue)
    pub async fn recover_timed_out(
        &self,
        conn: &mut MultiplexedConnection,
        unacked_set: &str,
        queue: &str,
        max_count: usize,
    ) -> redis::RedisResult<i64> {
        let current_time = current_timestamp();

        let recovered: i64 = self
            .recover_script
            .key(unacked_set)
            .key(queue)
            .arg(current_time)
            .arg(max_count)
            .invoke_async(conn)
            .await?;

        Ok(recovered)
    }
}

impl Default for VisibilityManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Get current Unix timestamp in seconds
fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("Time went backwards")
        .as_secs()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_visibility_manager_creation() {
        let manager = VisibilityManager::new();
        // Verify scripts are initialized
        assert!(!manager.pop_script.get_hash().is_empty());
    }

    #[test]
    fn test_current_timestamp() {
        let ts = current_timestamp();
        assert!(ts > 1_600_000_000); // After Sep 2020
        assert!(ts < 2_000_000_000); // Before May 2033
    }
}
