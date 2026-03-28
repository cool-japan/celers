//! RabbitMQ Management API client and data types.

use celers_kombu::{BrokerError, Result};

/// RabbitMQ Management API client
#[derive(Clone)]
pub(crate) struct ManagementApiClient {
    pub(crate) base_url: String,
    pub(crate) username: String,
    pub(crate) password: String,
    pub(crate) client: reqwest::Client,
}

impl ManagementApiClient {
    /// Create a new Management API client
    pub(crate) fn new(base_url: String, username: String, password: String) -> Self {
        Self {
            base_url,
            username,
            password,
            client: reqwest::Client::new(),
        }
    }

    /// List all queues
    pub(crate) async fn list_queues(&self) -> Result<Vec<QueueInfo>> {
        let url = format!("{}/api/queues", self.base_url);
        let response = self
            .client
            .get(&url)
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await
            .map_err(|e| {
                BrokerError::Connection(format!("Management API request failed: {}", e))
            })?;

        if !response.status().is_success() {
            return Err(BrokerError::Connection(format!(
                "Management API returned error: {}",
                response.status()
            )));
        }

        let queues: Vec<QueueInfo> = response
            .json()
            .await
            .map_err(|e| BrokerError::Connection(format!("Failed to parse queue info: {}", e)))?;

        Ok(queues)
    }

    /// Get detailed queue statistics
    pub(crate) async fn get_queue_stats(
        &self,
        vhost: &str,
        queue_name: &str,
    ) -> Result<QueueStats> {
        let vhost_encoded = urlencoding::encode(vhost);
        let queue_encoded = urlencoding::encode(queue_name);
        let url = format!(
            "{}/api/queues/{}/{}",
            self.base_url, vhost_encoded, queue_encoded
        );

        let response = self
            .client
            .get(&url)
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await
            .map_err(|e| {
                BrokerError::Connection(format!("Management API request failed: {}", e))
            })?;

        if !response.status().is_success() {
            return Err(BrokerError::Connection(format!(
                "Management API returned error: {}",
                response.status()
            )));
        }

        let stats: QueueStats = response
            .json()
            .await
            .map_err(|e| BrokerError::Connection(format!("Failed to parse queue stats: {}", e)))?;

        Ok(stats)
    }

    /// Get overview of RabbitMQ server
    pub(crate) async fn get_overview(&self) -> Result<ServerOverview> {
        let url = format!("{}/api/overview", self.base_url);
        let response = self
            .client
            .get(&url)
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await
            .map_err(|e| {
                BrokerError::Connection(format!("Management API request failed: {}", e))
            })?;

        if !response.status().is_success() {
            return Err(BrokerError::Connection(format!(
                "Management API returned error: {}",
                response.status()
            )));
        }

        let overview: ServerOverview = response.json().await.map_err(|e| {
            BrokerError::Connection(format!("Failed to parse server overview: {}", e))
        })?;

        Ok(overview)
    }

    /// List all connections
    pub(crate) async fn list_connections(&self) -> Result<Vec<ConnectionInfo>> {
        let url = format!("{}/api/connections", self.base_url);
        let response = self
            .client
            .get(&url)
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await
            .map_err(|e| {
                BrokerError::Connection(format!("Management API request failed: {}", e))
            })?;

        if !response.status().is_success() {
            return Err(BrokerError::Connection(format!(
                "Management API returned error: {}",
                response.status()
            )));
        }

        let connections: Vec<ConnectionInfo> = response.json().await.map_err(|e| {
            BrokerError::Connection(format!("Failed to parse connection info: {}", e))
        })?;

        Ok(connections)
    }

    /// List all channels
    pub(crate) async fn list_channels(&self) -> Result<Vec<ChannelInfo>> {
        let url = format!("{}/api/channels", self.base_url);
        let response = self
            .client
            .get(&url)
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await
            .map_err(|e| {
                BrokerError::Connection(format!("Management API request failed: {}", e))
            })?;

        if !response.status().is_success() {
            return Err(BrokerError::Connection(format!(
                "Management API returned error: {}",
                response.status()
            )));
        }

        let channels: Vec<ChannelInfo> = response
            .json()
            .await
            .map_err(|e| BrokerError::Connection(format!("Failed to parse channel info: {}", e)))?;

        Ok(channels)
    }

    /// List all exchanges
    pub(crate) async fn list_exchanges(&self, vhost: &str) -> Result<Vec<ExchangeInfo>> {
        let vhost_encoded = urlencoding::encode(vhost);
        let url = format!("{}/api/exchanges/{}", self.base_url, vhost_encoded);
        let response = self
            .client
            .get(&url)
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await
            .map_err(|e| {
                BrokerError::Connection(format!("Management API request failed: {}", e))
            })?;

        if !response.status().is_success() {
            return Err(BrokerError::Connection(format!(
                "Management API returned error: {}",
                response.status()
            )));
        }

        let exchanges: Vec<ExchangeInfo> = response.json().await.map_err(|e| {
            BrokerError::Connection(format!("Failed to parse exchange info: {}", e))
        })?;

        Ok(exchanges)
    }

    /// List all bindings for a queue
    pub(crate) async fn list_queue_bindings(
        &self,
        vhost: &str,
        queue_name: &str,
    ) -> Result<Vec<BindingInfo>> {
        let vhost_encoded = urlencoding::encode(vhost);
        let queue_encoded = urlencoding::encode(queue_name);
        let url = format!(
            "{}/api/queues/{}/{}/bindings",
            self.base_url, vhost_encoded, queue_encoded
        );
        let response = self
            .client
            .get(&url)
            .basic_auth(&self.username, Some(&self.password))
            .send()
            .await
            .map_err(|e| {
                BrokerError::Connection(format!("Management API request failed: {}", e))
            })?;

        if !response.status().is_success() {
            return Err(BrokerError::Connection(format!(
                "Management API returned error: {}",
                response.status()
            )));
        }

        let bindings: Vec<BindingInfo> = response
            .json()
            .await
            .map_err(|e| BrokerError::Connection(format!("Failed to parse binding info: {}", e)))?;

        Ok(bindings)
    }
}

/// Queue information from Management API
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct QueueInfo {
    pub name: String,
    pub vhost: String,
    pub durable: bool,
    pub auto_delete: bool,
    #[serde(default)]
    pub messages: u64,
    #[serde(default)]
    pub messages_ready: u64,
    #[serde(default)]
    pub messages_unacknowledged: u64,
    #[serde(default)]
    pub consumers: u32,
    #[serde(default)]
    pub memory: u64,
}

impl QueueInfo {
    /// Check if the queue is empty (no messages).
    pub fn is_empty(&self) -> bool {
        self.messages == 0
    }

    /// Check if the queue has any consumers.
    pub fn has_consumers(&self) -> bool {
        self.consumers > 0
    }

    /// Check if the queue is idle (no messages and no consumers).
    pub fn is_idle(&self) -> bool {
        self.is_empty() && !self.has_consumers()
    }

    /// Get the percentage of messages that are ready (not unacknowledged).
    pub fn ready_percentage(&self) -> f64 {
        if self.messages == 0 {
            return 0.0;
        }
        (self.messages_ready as f64 / self.messages as f64) * 100.0
    }

    /// Get the percentage of messages that are unacknowledged.
    pub fn unacked_percentage(&self) -> f64 {
        if self.messages == 0 {
            return 0.0;
        }
        (self.messages_unacknowledged as f64 / self.messages as f64) * 100.0
    }

    /// Get memory usage in megabytes.
    pub fn memory_mb(&self) -> f64 {
        self.memory as f64 / 1024.0 / 1024.0
    }

    /// Get average memory per message in bytes.
    pub fn avg_message_memory(&self) -> f64 {
        if self.messages == 0 {
            return 0.0;
        }
        self.memory as f64 / self.messages as f64
    }
}

/// Detailed queue statistics from Management API
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct QueueStats {
    pub name: String,
    pub vhost: String,
    pub durable: bool,
    pub auto_delete: bool,
    #[serde(default)]
    pub messages: u64,
    #[serde(default)]
    pub messages_ready: u64,
    #[serde(default)]
    pub messages_unacknowledged: u64,
    #[serde(default)]
    pub consumers: u32,
    #[serde(default)]
    pub memory: u64,
    #[serde(default)]
    pub message_bytes: u64,
    #[serde(default)]
    pub message_bytes_ready: u64,
    #[serde(default)]
    pub message_bytes_unacknowledged: u64,
    pub message_stats: Option<MessageStats>,
}

impl QueueStats {
    /// Check if the queue is empty (no messages).
    pub fn is_empty(&self) -> bool {
        self.messages == 0
    }

    /// Check if the queue has any consumers.
    pub fn has_consumers(&self) -> bool {
        self.consumers > 0
    }

    /// Check if the queue is idle (no messages and no consumers).
    pub fn is_idle(&self) -> bool {
        self.is_empty() && !self.has_consumers()
    }

    /// Get the percentage of messages that are ready.
    pub fn ready_percentage(&self) -> f64 {
        if self.messages == 0 {
            return 0.0;
        }
        (self.messages_ready as f64 / self.messages as f64) * 100.0
    }

    /// Get the percentage of messages that are unacknowledged.
    pub fn unacked_percentage(&self) -> f64 {
        if self.messages == 0 {
            return 0.0;
        }
        (self.messages_unacknowledged as f64 / self.messages as f64) * 100.0
    }

    /// Get memory usage in megabytes.
    pub fn memory_mb(&self) -> f64 {
        self.memory as f64 / 1024.0 / 1024.0
    }

    /// Get message bytes in megabytes.
    pub fn message_bytes_mb(&self) -> f64 {
        self.message_bytes as f64 / 1024.0 / 1024.0
    }

    /// Get average message size in bytes.
    pub fn avg_message_size(&self) -> f64 {
        if self.messages == 0 {
            return 0.0;
        }
        self.message_bytes as f64 / self.messages as f64
    }

    /// Get average memory per message in bytes.
    pub fn avg_message_memory(&self) -> f64 {
        if self.messages == 0 {
            return 0.0;
        }
        self.memory as f64 / self.messages as f64
    }

    /// Get the publish rate (messages/second).
    pub fn publish_rate(&self) -> Option<f64> {
        self.message_stats
            .as_ref()
            .and_then(|s| s.publish_details.as_ref().map(|d| d.rate))
    }

    /// Get the deliver rate (messages/second).
    pub fn deliver_rate(&self) -> Option<f64> {
        self.message_stats
            .as_ref()
            .and_then(|s| s.deliver_details.as_ref().map(|d| d.rate))
    }

    /// Get the ack rate (messages/second).
    pub fn ack_rate(&self) -> Option<f64> {
        self.message_stats
            .as_ref()
            .and_then(|s| s.ack_details.as_ref().map(|d| d.rate))
    }

    /// Check if the queue is growing (publish rate > deliver rate).
    pub fn is_growing(&self) -> bool {
        match (self.publish_rate(), self.deliver_rate()) {
            (Some(pub_rate), Some(del_rate)) => pub_rate > del_rate,
            _ => false,
        }
    }

    /// Check if the queue is shrinking (deliver rate > publish rate).
    pub fn is_shrinking(&self) -> bool {
        match (self.publish_rate(), self.deliver_rate()) {
            (Some(pub_rate), Some(del_rate)) => del_rate > pub_rate,
            _ => false,
        }
    }

    /// Check if consumers are keeping up (ack rate >= 95% of deliver rate).
    pub fn consumers_keeping_up(&self) -> bool {
        match (self.ack_rate(), self.deliver_rate()) {
            (Some(ack_rate), Some(del_rate)) => ack_rate >= del_rate * 0.95,
            _ => false,
        }
    }
}

/// Message statistics
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct MessageStats {
    #[serde(default)]
    pub publish: u64,
    pub publish_details: Option<RateDetails>,
    #[serde(default)]
    pub deliver: u64,
    pub deliver_details: Option<RateDetails>,
    #[serde(default)]
    pub ack: u64,
    pub ack_details: Option<RateDetails>,
}

impl MessageStats {
    /// Get total processed messages (publish + deliver + ack).
    pub fn total_processed(&self) -> u64 {
        self.publish + self.deliver + self.ack
    }

    /// Get publish rate.
    pub fn publish_rate(&self) -> Option<f64> {
        self.publish_details.as_ref().map(|d| d.rate)
    }

    /// Get deliver rate.
    pub fn deliver_rate(&self) -> Option<f64> {
        self.deliver_details.as_ref().map(|d| d.rate)
    }

    /// Get ack rate.
    pub fn ack_rate(&self) -> Option<f64> {
        self.ack_details.as_ref().map(|d| d.rate)
    }
}

/// Rate details for message statistics
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct RateDetails {
    pub rate: f64,
}

/// RabbitMQ server overview
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ServerOverview {
    pub management_version: String,
    pub rabbitmq_version: String,
    pub erlang_version: String,
    pub cluster_name: String,
    pub queue_totals: Option<QueueTotals>,
    pub object_totals: Option<ObjectTotals>,
}

impl ServerOverview {
    /// Get total messages across all queues.
    pub fn total_messages(&self) -> u64 {
        self.queue_totals.as_ref().map(|t| t.messages).unwrap_or(0)
    }

    /// Get total ready messages across all queues.
    pub fn total_messages_ready(&self) -> u64 {
        self.queue_totals
            .as_ref()
            .map(|t| t.messages_ready)
            .unwrap_or(0)
    }

    /// Get total unacked messages across all queues.
    pub fn total_messages_unacked(&self) -> u64 {
        self.queue_totals
            .as_ref()
            .map(|t| t.messages_unacknowledged)
            .unwrap_or(0)
    }

    /// Get total queue count.
    pub fn total_queues(&self) -> u32 {
        self.object_totals.as_ref().map(|t| t.queues).unwrap_or(0)
    }

    /// Get total connection count.
    pub fn total_connections(&self) -> u32 {
        self.object_totals
            .as_ref()
            .map(|t| t.connections)
            .unwrap_or(0)
    }

    /// Get total channel count.
    pub fn total_channels(&self) -> u32 {
        self.object_totals.as_ref().map(|t| t.channels).unwrap_or(0)
    }

    /// Get total consumer count.
    pub fn total_consumers(&self) -> u32 {
        self.object_totals
            .as_ref()
            .map(|t| t.consumers)
            .unwrap_or(0)
    }

    /// Check if there are any connections.
    pub fn has_connections(&self) -> bool {
        self.total_connections() > 0
    }

    /// Check if there are any messages.
    pub fn has_messages(&self) -> bool {
        self.total_messages() > 0
    }
}

/// Queue totals across all queues
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct QueueTotals {
    #[serde(default)]
    pub messages: u64,
    #[serde(default)]
    pub messages_ready: u64,
    #[serde(default)]
    pub messages_unacknowledged: u64,
}

impl QueueTotals {
    /// Get the percentage of messages that are ready.
    pub fn ready_percentage(&self) -> f64 {
        if self.messages == 0 {
            return 0.0;
        }
        (self.messages_ready as f64 / self.messages as f64) * 100.0
    }

    /// Get the percentage of messages that are unacknowledged.
    pub fn unacked_percentage(&self) -> f64 {
        if self.messages == 0 {
            return 0.0;
        }
        (self.messages_unacknowledged as f64 / self.messages as f64) * 100.0
    }
}

/// Object totals (queues, exchanges, connections, channels)
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ObjectTotals {
    #[serde(default)]
    pub consumers: u32,
    #[serde(default)]
    pub queues: u32,
    #[serde(default)]
    pub exchanges: u32,
    #[serde(default)]
    pub connections: u32,
    #[serde(default)]
    pub channels: u32,
}

impl ObjectTotals {
    /// Get average channels per connection.
    pub fn avg_channels_per_connection(&self) -> f64 {
        if self.connections == 0 {
            return 0.0;
        }
        self.channels as f64 / self.connections as f64
    }

    /// Get average consumers per queue.
    pub fn avg_consumers_per_queue(&self) -> f64 {
        if self.queues == 0 {
            return 0.0;
        }
        self.consumers as f64 / self.queues as f64
    }

    /// Check if there are idle queues (more queues than consumers).
    pub fn has_idle_queues(&self) -> bool {
        self.queues > self.consumers
    }
}

/// Connection information from Management API
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ConnectionInfo {
    pub name: String,
    pub vhost: String,
    pub user: String,
    pub state: String,
    #[serde(default)]
    pub channels: u32,
    #[serde(default)]
    pub peer_host: String,
    #[serde(default)]
    pub peer_port: u16,
    #[serde(default)]
    pub recv_oct: u64,
    #[serde(default)]
    pub send_oct: u64,
    #[serde(default)]
    pub recv_cnt: u64,
    #[serde(default)]
    pub send_cnt: u64,
}

impl ConnectionInfo {
    /// Check if the connection is running.
    pub fn is_running(&self) -> bool {
        self.state == "running"
    }

    /// Check if the connection has any channels.
    pub fn has_channels(&self) -> bool {
        self.channels > 0
    }

    /// Get total bytes transferred (sent + received).
    pub fn total_bytes(&self) -> u64 {
        self.recv_oct + self.send_oct
    }

    /// Get received bytes in megabytes.
    pub fn recv_mb(&self) -> f64 {
        self.recv_oct as f64 / 1024.0 / 1024.0
    }

    /// Get sent bytes in megabytes.
    pub fn send_mb(&self) -> f64 {
        self.send_oct as f64 / 1024.0 / 1024.0
    }

    /// Get total messages (sent + received packets).
    pub fn total_messages(&self) -> u64 {
        self.recv_cnt + self.send_cnt
    }

    /// Get average message size in bytes.
    pub fn avg_message_size(&self) -> f64 {
        let total_msgs = self.total_messages();
        if total_msgs == 0 {
            return 0.0;
        }
        self.total_bytes() as f64 / total_msgs as f64
    }

    /// Get peer address as "host:port".
    pub fn peer_address(&self) -> String {
        format!("{}:{}", self.peer_host, self.peer_port)
    }
}

/// Channel information from Management API
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ChannelInfo {
    pub name: String,
    pub connection_details: Option<ConnectionDetails>,
    pub vhost: String,
    pub user: String,
    #[serde(default)]
    pub number: u32,
    #[serde(default)]
    pub consumers: u32,
    #[serde(default)]
    pub messages_unacknowledged: u64,
    #[serde(default)]
    pub messages_uncommitted: u64,
    #[serde(default)]
    pub acks_uncommitted: u64,
    #[serde(default)]
    pub prefetch_count: u32,
    pub state: String,
}

impl ChannelInfo {
    /// Check if the channel is running.
    pub fn is_running(&self) -> bool {
        self.state == "running"
    }

    /// Check if the channel has any consumers.
    pub fn has_consumers(&self) -> bool {
        self.consumers > 0
    }

    /// Check if there are any unacknowledged messages.
    pub fn has_unacked_messages(&self) -> bool {
        self.messages_unacknowledged > 0
    }

    /// Check if the channel is in a transaction.
    pub fn is_in_transaction(&self) -> bool {
        self.messages_uncommitted > 0 || self.acks_uncommitted > 0
    }

    /// Check if the channel has a prefetch limit set.
    pub fn has_prefetch(&self) -> bool {
        self.prefetch_count > 0
    }

    /// Get the channel utilization (unacked / prefetch * 100).
    pub fn utilization(&self) -> f64 {
        if self.prefetch_count == 0 {
            return 0.0;
        }
        (self.messages_unacknowledged as f64 / self.prefetch_count as f64) * 100.0
    }

    /// Get peer address from connection details.
    pub fn peer_address(&self) -> Option<String> {
        self.connection_details
            .as_ref()
            .map(|details| format!("{}:{}", details.peer_host, details.peer_port))
    }
}

/// Connection details for a channel
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ConnectionDetails {
    pub name: String,
    pub peer_host: String,
    pub peer_port: u16,
}

/// Exchange information from Management API
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct ExchangeInfo {
    pub name: String,
    pub vhost: String,
    #[serde(rename = "type")]
    pub exchange_type: String,
    pub durable: bool,
    pub auto_delete: bool,
    pub internal: bool,
    #[serde(default)]
    pub arguments: serde_json::Value,
    pub message_stats: Option<MessageStats>,
}

/// Binding information from Management API
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct BindingInfo {
    pub source: String,
    pub vhost: String,
    pub destination: String,
    pub destination_type: String,
    pub routing_key: String,
    #[serde(default)]
    pub arguments: serde_json::Value,
    #[serde(default)]
    pub properties_key: String,
}
