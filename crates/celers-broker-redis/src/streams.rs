//! Redis Streams integration for task queuing
//!
//! Provides stream-based task queues with consumer groups for distributed processing.

use crate::{CelersError, Result, SerializedTask};
use redis::{AsyncCommands, Client, Value};
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

/// Stream configuration
#[derive(Debug, Clone)]
pub struct StreamConfig {
    /// Stream name
    pub stream_name: String,

    /// Consumer group name
    pub group_name: String,

    /// Consumer name
    pub consumer_name: String,

    /// Maximum stream length (for trimming)
    pub max_len: Option<usize>,

    /// Approximate trimming (more efficient)
    pub approximate_trim: bool,

    /// Block time in milliseconds for XREADGROUP
    pub block_ms: Option<usize>,

    /// Maximum number of messages to read per batch
    pub count: Option<usize>,

    /// Auto-claim idle messages after this duration (ms)
    pub auto_claim_idle_ms: Option<u64>,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            stream_name: "celers:stream".to_string(),
            group_name: "celers:workers".to_string(),
            consumer_name: "worker-1".to_string(),
            max_len: Some(10000),
            approximate_trim: true,
            block_ms: Some(1000),
            count: Some(10),
            auto_claim_idle_ms: Some(60000), // 1 minute
        }
    }
}

impl StreamConfig {
    /// Create a new builder
    pub fn builder() -> StreamConfigBuilder {
        StreamConfigBuilder::default()
    }
}

/// Builder for StreamConfig
#[derive(Default)]
pub struct StreamConfigBuilder {
    stream_name: Option<String>,
    group_name: Option<String>,
    consumer_name: Option<String>,
    max_len: Option<usize>,
    approximate_trim: bool,
    block_ms: Option<usize>,
    count: Option<usize>,
    auto_claim_idle_ms: Option<u64>,
}

impl StreamConfigBuilder {
    /// Set stream name
    pub fn stream_name(mut self, name: impl Into<String>) -> Self {
        self.stream_name = Some(name.into());
        self
    }

    /// Set consumer group name
    pub fn group_name(mut self, name: impl Into<String>) -> Self {
        self.group_name = Some(name.into());
        self
    }

    /// Set consumer name
    pub fn consumer_name(mut self, name: impl Into<String>) -> Self {
        self.consumer_name = Some(name.into());
        self
    }

    /// Set maximum stream length
    pub fn max_len(mut self, len: usize) -> Self {
        self.max_len = Some(len);
        self
    }

    /// Enable/disable approximate trimming
    pub fn approximate_trim(mut self, enabled: bool) -> Self {
        self.approximate_trim = enabled;
        self
    }

    /// Set block time in milliseconds
    pub fn block_ms(mut self, ms: usize) -> Self {
        self.block_ms = Some(ms);
        self
    }

    /// Set read count
    pub fn count(mut self, count: usize) -> Self {
        self.count = Some(count);
        self
    }

    /// Set auto-claim idle time in milliseconds
    pub fn auto_claim_idle_ms(mut self, ms: u64) -> Self {
        self.auto_claim_idle_ms = Some(ms);
        self
    }

    /// Build the StreamConfig
    pub fn build(self) -> Result<StreamConfig> {
        Ok(StreamConfig {
            stream_name: self
                .stream_name
                .unwrap_or_else(|| "celers:stream".to_string()),
            group_name: self
                .group_name
                .unwrap_or_else(|| "celers:workers".to_string()),
            consumer_name: self.consumer_name.unwrap_or_else(|| "worker-1".to_string()),
            max_len: self.max_len,
            approximate_trim: self.approximate_trim,
            block_ms: self.block_ms,
            count: self.count,
            auto_claim_idle_ms: self.auto_claim_idle_ms,
        })
    }
}

/// Stream message ID
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StreamMessageId(pub String);

impl StreamMessageId {
    /// Create a new message ID
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Get the ID as a string
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for StreamMessageId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Stream entry with task data
#[derive(Debug, Clone)]
pub struct StreamEntry {
    /// Message ID
    pub id: StreamMessageId,

    /// Task data
    pub task: SerializedTask,
}

/// Stream statistics
#[derive(Debug, Clone, Default)]
pub struct StreamStats {
    /// Total messages in stream
    pub length: u64,

    /// Number of consumer groups
    pub groups: u64,

    /// First entry ID
    pub first_entry_id: Option<StreamMessageId>,

    /// Last entry ID
    pub last_entry_id: Option<StreamMessageId>,

    /// Pending messages count
    pub pending_count: u64,
}

/// Redis Streams client for task queuing
pub struct StreamsClient {
    client: Client,
    config: StreamConfig,
}

impl StreamsClient {
    /// Create a new Streams client
    pub fn new(client: Client, config: StreamConfig) -> Self {
        Self { client, config }
    }

    /// Initialize the stream and consumer group
    pub async fn init(&self) -> Result<()> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        // Try to create consumer group (ignore error if already exists)
        let result: std::result::Result<(), redis::RedisError> = redis::cmd("XGROUP")
            .arg("CREATE")
            .arg(&self.config.stream_name)
            .arg(&self.config.group_name)
            .arg("0")
            .arg("MKSTREAM")
            .query_async(&mut conn)
            .await;

        match result {
            Ok(_) => {
                info!(
                    "Created consumer group '{}' for stream '{}'",
                    self.config.group_name, self.config.stream_name
                );
                Ok(())
            }
            Err(e) => {
                let err_str = e.to_string();
                if err_str.contains("BUSYGROUP") {
                    debug!("Consumer group already exists");
                    Ok(())
                } else {
                    Err(CelersError::Broker(format!(
                        "Failed to create consumer group: {}",
                        e
                    )))
                }
            }
        }
    }

    /// Add a task to the stream
    pub async fn add_task(&self, task: &SerializedTask) -> Result<StreamMessageId> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let serialized =
            serde_json::to_string(task).map_err(|e| CelersError::Serialization(e.to_string()))?;

        let mut cmd = redis::cmd("XADD");
        cmd.arg(&self.config.stream_name);

        // Add MAXLEN if configured
        if let Some(max_len) = self.config.max_len {
            if self.config.approximate_trim {
                cmd.arg("MAXLEN").arg("~").arg(max_len);
            } else {
                cmd.arg("MAXLEN").arg(max_len);
            }
        }

        cmd.arg("*").arg("task").arg(&serialized);

        let message_id: String = cmd
            .query_async(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to add task to stream: {}", e)))?;

        debug!(
            "Added task {} to stream with ID {}",
            task.metadata.id, message_id
        );

        Ok(StreamMessageId::new(message_id))
    }

    /// Read tasks from the stream as a consumer group member
    pub async fn read_tasks(&self) -> Result<Vec<StreamEntry>> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let mut cmd = redis::cmd("XREADGROUP");
        cmd.arg("GROUP")
            .arg(&self.config.group_name)
            .arg(&self.config.consumer_name);

        if let Some(count) = self.config.count {
            cmd.arg("COUNT").arg(count);
        }

        if let Some(block_ms) = self.config.block_ms {
            cmd.arg("BLOCK").arg(block_ms);
        }

        cmd.arg("STREAMS").arg(&self.config.stream_name).arg(">");

        let result: Value = cmd
            .query_async(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to read from stream: {}", e)))?;

        self.parse_stream_entries(result)
    }

    /// Acknowledge a message
    pub async fn ack(&self, message_id: &StreamMessageId) -> Result<()> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let _: i32 = redis::cmd("XACK")
            .arg(&self.config.stream_name)
            .arg(&self.config.group_name)
            .arg(message_id.as_str())
            .query_async(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to acknowledge message: {}", e)))?;

        debug!("Acknowledged message {}", message_id);

        Ok(())
    }

    /// Get pending messages for the consumer
    pub async fn get_pending(&self) -> Result<Vec<StreamMessageId>> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let result: Value = redis::cmd("XPENDING")
            .arg(&self.config.stream_name)
            .arg(&self.config.group_name)
            .arg(&self.config.consumer_name)
            .query_async(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get pending messages: {}", e)))?;

        // Parse pending message IDs
        let mut message_ids = Vec::new();
        if let Value::Array(items) = result {
            for item in items {
                if let Value::Array(ref parts) = item {
                    if let Some(Value::BulkString(ref id_bytes)) = parts.first() {
                        if let Ok(id) = String::from_utf8(id_bytes.clone()) {
                            message_ids.push(StreamMessageId::new(id));
                        }
                    }
                }
            }
        }

        Ok(message_ids)
    }

    /// Claim idle messages
    pub async fn claim_idle(&self) -> Result<Vec<StreamEntry>> {
        if self.config.auto_claim_idle_ms.is_none() {
            return Ok(Vec::new());
        }

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let idle_ms = self
            .config
            .auto_claim_idle_ms
            .expect("auto_claim_idle_ms should be configured");

        let result: Value = redis::cmd("XAUTOCLAIM")
            .arg(&self.config.stream_name)
            .arg(&self.config.group_name)
            .arg(&self.config.consumer_name)
            .arg(idle_ms)
            .arg("0-0")
            .query_async(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to auto-claim messages: {}", e)))?;

        // Parse claimed entries
        if let Value::Array(parts) = result {
            if parts.len() >= 2 {
                if let Value::Array(ref entries) = parts[1] {
                    return self.parse_entries_array(entries);
                }
            }
        }

        Ok(Vec::new())
    }

    /// Get stream statistics
    pub async fn get_stats(&self) -> Result<StreamStats> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let result: Value = redis::cmd("XINFO")
            .arg("STREAM")
            .arg(&self.config.stream_name)
            .query_async(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get stream info: {}", e)))?;

        let mut stats = StreamStats::default();

        if let Value::Array(items) = result {
            let mut i = 0;
            while i < items.len() - 1 {
                if let (Value::BulkString(ref key), ref value) = (&items[i], &items[i + 1]) {
                    let key_str = String::from_utf8_lossy(key);

                    match key_str.as_ref() {
                        "length" => {
                            if let Value::Int(len) = value {
                                stats.length = *len as u64;
                            }
                        }
                        "groups" => {
                            if let Value::Int(groups) = value {
                                stats.groups = *groups as u64;
                            }
                        }
                        "first-entry" => {
                            if let Value::Array(ref entry) = value {
                                if let Some(Value::BulkString(ref id)) = entry.first() {
                                    if let Ok(id_str) = String::from_utf8(id.clone()) {
                                        stats.first_entry_id = Some(StreamMessageId::new(id_str));
                                    }
                                }
                            }
                        }
                        "last-entry" => {
                            if let Value::Array(ref entry) = value {
                                if let Some(Value::BulkString(ref id)) = entry.first() {
                                    if let Ok(id_str) = String::from_utf8(id.clone()) {
                                        stats.last_entry_id = Some(StreamMessageId::new(id_str));
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
                i += 2;
            }
        }

        Ok(stats)
    }

    /// Delete the stream
    pub async fn delete_stream(&self) -> Result<()> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let _: i32 = conn
            .del(&self.config.stream_name)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to delete stream: {}", e)))?;

        info!("Deleted stream '{}'", self.config.stream_name);

        Ok(())
    }

    /// Parse stream entries from Redis response
    fn parse_stream_entries(&self, value: Value) -> Result<Vec<StreamEntry>> {
        let mut entries = Vec::new();

        // XREADGROUP returns: [[stream_name, [[id1, data1], [id2, data2], ...]]]
        if let Value::Array(streams) = value {
            for stream in streams {
                if let Value::Array(ref parts) = stream {
                    if parts.len() >= 2 {
                        if let Value::Array(ref messages) = parts[1] {
                            entries.extend(self.parse_entries_array(messages)?);
                        }
                    }
                }
            }
        }

        Ok(entries)
    }

    /// Parse entries array
    fn parse_entries_array(&self, messages: &[Value]) -> Result<Vec<StreamEntry>> {
        let mut entries = Vec::new();

        for message in messages {
            if let Value::Array(ref parts) = message {
                if parts.len() >= 2 {
                    let id = if let Value::BulkString(ref id_bytes) = parts[0] {
                        String::from_utf8(id_bytes.clone())
                            .map_err(|e| CelersError::Deserialization(e.to_string()))?
                    } else {
                        continue;
                    };

                    if let Value::Array(ref fields) = parts[1] {
                        // Fields are key-value pairs: [key1, value1, key2, value2, ...]
                        let mut i = 0;
                        while i < fields.len() - 1 {
                            if let (Value::BulkString(ref key), Value::BulkString(ref value)) =
                                (&fields[i], &fields[i + 1])
                            {
                                let key_str = String::from_utf8_lossy(key);
                                if key_str == "task" {
                                    let task_str = String::from_utf8(value.clone())
                                        .map_err(|e| CelersError::Deserialization(e.to_string()))?;

                                    let task: SerializedTask = serde_json::from_str(&task_str)
                                        .map_err(|e| CelersError::Deserialization(e.to_string()))?;

                                    entries.push(StreamEntry {
                                        id: StreamMessageId::new(id.clone()),
                                        task,
                                    });

                                    break;
                                }
                            }
                            i += 2;
                        }
                    }
                }
            }
        }

        Ok(entries)
    }

    /// Get the stream configuration
    pub fn config(&self) -> &StreamConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_config_default() {
        let config = StreamConfig::default();

        assert_eq!(config.stream_name, "celers:stream");
        assert_eq!(config.group_name, "celers:workers");
        assert_eq!(config.consumer_name, "worker-1");
        assert_eq!(config.max_len, Some(10000));
        assert!(config.approximate_trim);
        assert_eq!(config.block_ms, Some(1000));
        assert_eq!(config.count, Some(10));
        assert_eq!(config.auto_claim_idle_ms, Some(60000));
    }

    #[test]
    fn test_stream_config_builder() {
        let config = StreamConfig::builder()
            .stream_name("my-stream")
            .group_name("my-group")
            .consumer_name("consumer-1")
            .max_len(5000)
            .approximate_trim(false)
            .block_ms(2000)
            .count(20)
            .auto_claim_idle_ms(30000)
            .build()
            .unwrap();

        assert_eq!(config.stream_name, "my-stream");
        assert_eq!(config.group_name, "my-group");
        assert_eq!(config.consumer_name, "consumer-1");
        assert_eq!(config.max_len, Some(5000));
        assert!(!config.approximate_trim);
        assert_eq!(config.block_ms, Some(2000));
        assert_eq!(config.count, Some(20));
        assert_eq!(config.auto_claim_idle_ms, Some(30000));
    }

    #[test]
    fn test_stream_message_id() {
        let id = StreamMessageId::new("1234567890-0");

        assert_eq!(id.as_str(), "1234567890-0");
        assert_eq!(id.to_string(), "1234567890-0");
    }

    #[test]
    fn test_stream_message_id_equality() {
        let id1 = StreamMessageId::new("1234567890-0");
        let id2 = StreamMessageId::new("1234567890-0");
        let id3 = StreamMessageId::new("9876543210-0");

        assert_eq!(id1, id2);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_stream_stats_default() {
        let stats = StreamStats::default();

        assert_eq!(stats.length, 0);
        assert_eq!(stats.groups, 0);
        assert!(stats.first_entry_id.is_none());
        assert!(stats.last_entry_id.is_none());
        assert_eq!(stats.pending_count, 0);
    }
}
