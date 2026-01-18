//! Task result streaming for memory-efficient handling of large payloads
//!
//! This module provides utilities for streaming task results incrementally
//! to avoid loading entire payloads into memory at once. This is particularly
//! useful for tasks that produce large outputs or process large datasets.
//!
//! # Features
//!
//! - **Chunked Streaming**: Stream data in configurable chunks
//! - **Memory Limits**: Enforce maximum memory usage per task
//! - **Backpressure**: Automatic flow control to prevent memory exhaustion
//! - **Progress Tracking**: Monitor streaming progress
//!
//! # Example
//!
//! ```rust
//! use celers_worker::streaming::{StreamConfig, ResultStreamer};
//!
//! let config = StreamConfig::default()
//!     .with_chunk_size(4096)
//!     .with_max_buffer_size(1024 * 1024); // 1 MB
//!
//! assert_eq!(config.chunk_size, 4096);
//! ```

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, info, warn};

/// Configuration for result streaming
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamConfig {
    /// Size of each chunk in bytes
    pub chunk_size: usize,

    /// Maximum buffer size in bytes
    pub max_buffer_size: usize,

    /// Enable streaming for results larger than this size
    pub stream_threshold: usize,

    /// Channel buffer size for backpressure
    pub channel_buffer: usize,

    /// Enable compression for streamed data
    pub enable_compression: bool,

    /// Maximum memory usage per task in bytes
    pub max_memory_per_task: usize,
}

impl Default for StreamConfig {
    fn default() -> Self {
        Self {
            chunk_size: 8192,            // 8 KB chunks
            max_buffer_size: 10_485_760, // 10 MB buffer
            stream_threshold: 1_048_576, // 1 MB threshold
            channel_buffer: 100,         // Buffer up to 100 chunks
            enable_compression: false,
            max_memory_per_task: 104_857_600, // 100 MB per task
        }
    }
}

impl StreamConfig {
    /// Create a new streaming configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the chunk size
    pub fn with_chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    /// Set the maximum buffer size
    pub fn with_max_buffer_size(mut self, max_buffer_size: usize) -> Self {
        self.max_buffer_size = max_buffer_size;
        self
    }

    /// Set the streaming threshold
    pub fn with_stream_threshold(mut self, stream_threshold: usize) -> Self {
        self.stream_threshold = stream_threshold;
        self
    }

    /// Set the channel buffer size
    pub fn with_channel_buffer(mut self, channel_buffer: usize) -> Self {
        self.channel_buffer = channel_buffer;
        self
    }

    /// Enable or disable compression
    pub fn with_compression(mut self, enable: bool) -> Self {
        self.enable_compression = enable;
        self
    }

    /// Set the maximum memory per task
    pub fn with_max_memory_per_task(mut self, max_memory: usize) -> Self {
        self.max_memory_per_task = max_memory;
        self
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.chunk_size == 0 {
            return Err("chunk_size must be greater than 0".to_string());
        }
        if self.max_buffer_size == 0 {
            return Err("max_buffer_size must be greater than 0".to_string());
        }
        if self.max_buffer_size < self.chunk_size {
            return Err("max_buffer_size must be >= chunk_size".to_string());
        }
        if self.stream_threshold == 0 {
            return Err("stream_threshold must be greater than 0".to_string());
        }
        if self.channel_buffer == 0 {
            return Err("channel_buffer must be greater than 0".to_string());
        }
        if self.max_memory_per_task == 0 {
            return Err("max_memory_per_task must be greater than 0".to_string());
        }
        if self.max_memory_per_task < self.max_buffer_size {
            return Err("max_memory_per_task must be >= max_buffer_size".to_string());
        }
        Ok(())
    }

    /// Check if data should be streamed based on size
    pub fn should_stream(&self, data_size: usize) -> bool {
        data_size > self.stream_threshold
    }

    /// Check if compression is enabled
    pub fn is_compression_enabled(&self) -> bool {
        self.enable_compression
    }

    /// Calculate the number of chunks for a given size
    pub fn calculate_chunks(&self, data_size: usize) -> usize {
        data_size.div_ceil(self.chunk_size)
    }
}

/// A chunk of streamed data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataChunk {
    /// Chunk sequence number
    pub sequence: u64,

    /// Total number of chunks
    pub total_chunks: u64,

    /// Data payload
    pub data: Vec<u8>,

    /// Indicates if this is the last chunk
    pub is_last: bool,

    /// Checksum for data integrity
    pub checksum: Option<u32>,
}

impl DataChunk {
    /// Create a new data chunk
    pub fn new(sequence: u64, total_chunks: u64, data: Vec<u8>, is_last: bool) -> Self {
        Self {
            sequence,
            total_chunks,
            data,
            is_last,
            checksum: None,
        }
    }

    /// Add a checksum to the chunk
    pub fn with_checksum(mut self, checksum: u32) -> Self {
        self.checksum = Some(checksum);
        self
    }

    /// Get the size of this chunk in bytes
    pub fn size(&self) -> usize {
        self.data.len()
    }

    /// Verify the checksum if present
    pub fn verify_checksum(&self) -> bool {
        if let Some(expected) = self.checksum {
            let actual = Self::calculate_checksum(&self.data);
            actual == expected
        } else {
            true // No checksum to verify
        }
    }

    /// Calculate CRC32 checksum for data
    fn calculate_checksum(data: &[u8]) -> u32 {
        // Simple CRC32 implementation
        let mut crc: u32 = 0xFFFFFFFF;
        for &byte in data {
            crc ^= u32::from(byte);
            for _ in 0..8 {
                if crc & 1 != 0 {
                    crc = (crc >> 1) ^ 0xEDB88320;
                } else {
                    crc >>= 1;
                }
            }
        }
        !crc
    }

    /// Create a chunk with automatic checksum
    pub fn new_with_checksum(
        sequence: u64,
        total_chunks: u64,
        data: Vec<u8>,
        is_last: bool,
    ) -> Self {
        let checksum = Self::calculate_checksum(&data);
        Self {
            sequence,
            total_chunks,
            data,
            is_last,
            checksum: Some(checksum),
        }
    }
}

/// Statistics for result streaming
#[derive(Debug, Clone, Default)]
pub struct StreamStats {
    /// Total bytes streamed
    pub bytes_streamed: usize,

    /// Number of chunks sent
    pub chunks_sent: usize,

    /// Number of chunks received
    pub chunks_received: usize,

    /// Current buffer usage in bytes
    pub buffer_usage: usize,

    /// Peak buffer usage in bytes
    pub peak_buffer_usage: usize,

    /// Number of backpressure events
    pub backpressure_count: usize,

    /// Number of memory limit hits
    pub memory_limit_hits: usize,
}

impl StreamStats {
    /// Create new streaming statistics
    pub fn new() -> Self {
        Self::default()
    }

    /// Reset all statistics
    pub fn reset(&mut self) {
        *self = Self::default();
    }

    /// Update buffer usage and track peak
    pub fn update_buffer_usage(&mut self, current_usage: usize) {
        self.buffer_usage = current_usage;
        if current_usage > self.peak_buffer_usage {
            self.peak_buffer_usage = current_usage;
        }
    }

    /// Get the buffer utilization percentage
    pub fn buffer_utilization(&self, max_buffer: usize) -> f64 {
        if max_buffer == 0 {
            0.0
        } else {
            (self.buffer_usage as f64 / max_buffer as f64) * 100.0
        }
    }

    /// Check if streaming is under backpressure
    pub fn is_under_backpressure(&self) -> bool {
        self.backpressure_count > 0
    }

    /// Get the average chunk size
    pub fn avg_chunk_size(&self) -> f64 {
        if self.chunks_sent == 0 {
            0.0
        } else {
            self.bytes_streamed as f64 / self.chunks_sent as f64
        }
    }
}

/// Result streamer for handling large task outputs
pub struct ResultStreamer {
    config: StreamConfig,
    stats: Arc<RwLock<StreamStats>>,
}

impl ResultStreamer {
    /// Create a new result streamer
    pub fn new(config: StreamConfig) -> Self {
        Self {
            config,
            stats: Arc::new(RwLock::new(StreamStats::new())),
        }
    }

    /// Create a sender for streaming chunks
    pub fn create_sender(&self) -> (ChunkSender, ChunkReceiver) {
        let (tx, rx) = mpsc::channel(self.config.channel_buffer);
        let sender = ChunkSender {
            tx,
            stats: self.stats.clone(),
            config: self.config.clone(),
            sequence: 0,
        };
        let receiver = ChunkReceiver {
            rx,
            stats: self.stats.clone(),
            received_chunks: Vec::new(),
        };
        (sender, receiver)
    }

    /// Get streaming statistics
    pub async fn get_stats(&self) -> StreamStats {
        self.stats.read().await.clone()
    }

    /// Reset statistics
    pub async fn reset_stats(&self) {
        self.stats.write().await.reset();
    }

    /// Check if data should be streamed
    pub fn should_stream(&self, data_size: usize) -> bool {
        self.config.should_stream(data_size)
    }

    /// Split data into chunks
    pub fn split_into_chunks(&self, data: Vec<u8>) -> Vec<DataChunk> {
        let chunk_size = self.config.chunk_size;
        let total_size = data.len();
        let total_chunks = self.config.calculate_chunks(total_size);

        let mut chunks = Vec::new();
        for (i, chunk_data) in data.chunks(chunk_size).enumerate() {
            let is_last = i == total_chunks - 1;
            let chunk = DataChunk::new_with_checksum(
                i as u64,
                total_chunks as u64,
                chunk_data.to_vec(),
                is_last,
            );
            chunks.push(chunk);
        }
        chunks
    }

    /// Get the configuration
    pub fn config(&self) -> &StreamConfig {
        &self.config
    }
}

/// Sender for streaming data chunks
pub struct ChunkSender {
    tx: mpsc::Sender<DataChunk>,
    stats: Arc<RwLock<StreamStats>>,
    config: StreamConfig,
    sequence: u64,
}

impl ChunkSender {
    /// Send a chunk of data
    pub async fn send(&mut self, mut chunk: DataChunk) -> Result<(), String> {
        // Check memory limit
        let stats = self.stats.read().await;
        if stats.buffer_usage + chunk.size() > self.config.max_memory_per_task {
            drop(stats);
            let mut stats = self.stats.write().await;
            stats.memory_limit_hits += 1;
            return Err("Memory limit exceeded".to_string());
        }
        drop(stats);

        // Set sequence number
        chunk.sequence = self.sequence;
        self.sequence += 1;

        // Send chunk
        match self.tx.send(chunk.clone()).await {
            Ok(_) => {
                let mut stats = self.stats.write().await;
                stats.bytes_streamed += chunk.size();
                stats.chunks_sent += 1;
                let new_buffer_usage = stats.buffer_usage + chunk.size();
                stats.update_buffer_usage(new_buffer_usage);
                debug!("Sent chunk {} of {} bytes", chunk.sequence, chunk.size());
                Ok(())
            }
            Err(_) => {
                let mut stats = self.stats.write().await;
                stats.backpressure_count += 1;
                Err("Failed to send chunk: receiver dropped".to_string())
            }
        }
    }

    /// Send raw data by chunking it
    pub async fn send_data(&mut self, data: Vec<u8>, total_chunks: u64) -> Result<(), String> {
        let chunk_size = self.config.chunk_size;
        for (i, chunk_data) in data.chunks(chunk_size).enumerate() {
            let is_last = (i as u64) == total_chunks - 1;
            let chunk =
                DataChunk::new_with_checksum(i as u64, total_chunks, chunk_data.to_vec(), is_last);
            self.send(chunk).await?;
        }
        Ok(())
    }

    /// Close the sender
    pub fn close(self) {
        drop(self.tx);
        info!("Chunk sender closed");
    }
}

/// Receiver for streaming data chunks
pub struct ChunkReceiver {
    rx: mpsc::Receiver<DataChunk>,
    stats: Arc<RwLock<StreamStats>>,
    received_chunks: Vec<DataChunk>,
}

impl ChunkReceiver {
    /// Receive the next chunk
    pub async fn recv(&mut self) -> Option<DataChunk> {
        match self.rx.recv().await {
            Some(chunk) => {
                // Update stats
                let mut stats = self.stats.write().await;
                stats.chunks_received += 1;
                let new_buffer_usage = stats.buffer_usage.saturating_sub(chunk.size());
                stats.update_buffer_usage(new_buffer_usage);
                drop(stats);

                // Verify checksum
                if !chunk.verify_checksum() {
                    warn!("Chunk {} failed checksum verification", chunk.sequence);
                }

                self.received_chunks.push(chunk.clone());
                debug!("Received chunk {}", chunk.sequence);
                Some(chunk)
            }
            None => None,
        }
    }

    /// Receive all chunks and reassemble data
    pub async fn recv_all(&mut self) -> Result<Vec<u8>, String> {
        let mut all_data = Vec::new();

        while let Some(chunk) = self.recv().await {
            all_data.extend_from_slice(&chunk.data);
            if chunk.is_last {
                break;
            }
        }

        if all_data.is_empty() {
            Err("No data received".to_string())
        } else {
            Ok(all_data)
        }
    }

    /// Get the number of received chunks
    pub fn received_count(&self) -> usize {
        self.received_chunks.len()
    }

    /// Check if all chunks have been received
    pub fn is_complete(&self) -> bool {
        if let Some(last_chunk) = self.received_chunks.last() {
            last_chunk.is_last
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stream_config_default() {
        let config = StreamConfig::default();
        assert_eq!(config.chunk_size, 8192);
        assert_eq!(config.max_buffer_size, 10_485_760);
        assert_eq!(config.stream_threshold, 1_048_576);
        assert_eq!(config.channel_buffer, 100);
        assert!(!config.enable_compression);
    }

    #[test]
    fn test_stream_config_builder() {
        let config = StreamConfig::new()
            .with_chunk_size(4096)
            .with_max_buffer_size(5_242_880)
            .with_stream_threshold(524_288)
            .with_channel_buffer(50)
            .with_compression(true)
            .with_max_memory_per_task(52_428_800);

        assert_eq!(config.chunk_size, 4096);
        assert_eq!(config.max_buffer_size, 5_242_880);
        assert_eq!(config.stream_threshold, 524_288);
        assert_eq!(config.channel_buffer, 50);
        assert!(config.enable_compression);
        assert_eq!(config.max_memory_per_task, 52_428_800);
    }

    #[test]
    fn test_stream_config_validation() {
        let mut config = StreamConfig::default();
        assert!(config.validate().is_ok());

        config.chunk_size = 0;
        assert!(config.validate().is_err());

        config = StreamConfig::default();
        config.max_buffer_size = 0;
        assert!(config.validate().is_err());

        config = StreamConfig::default();
        config.max_buffer_size = config.chunk_size - 1;
        assert!(config.validate().is_err());

        config = StreamConfig::default();
        config.stream_threshold = 0;
        assert!(config.validate().is_err());

        config = StreamConfig::default();
        config.channel_buffer = 0;
        assert!(config.validate().is_err());

        config = StreamConfig::default();
        config.max_memory_per_task = 0;
        assert!(config.validate().is_err());

        config = StreamConfig::default();
        config.max_memory_per_task = config.max_buffer_size - 1;
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_stream_config_should_stream() {
        let config = StreamConfig::default();
        assert!(!config.should_stream(1024));
        assert!(!config.should_stream(1_048_576));
        assert!(config.should_stream(1_048_577));
        assert!(config.should_stream(10_485_760));
    }

    #[test]
    fn test_stream_config_calculate_chunks() {
        let config = StreamConfig::default().with_chunk_size(1024);
        assert_eq!(config.calculate_chunks(1024), 1);
        assert_eq!(config.calculate_chunks(2048), 2);
        assert_eq!(config.calculate_chunks(2049), 3);
        assert_eq!(config.calculate_chunks(10240), 10);
    }

    #[test]
    fn test_data_chunk_creation() {
        let data = vec![1, 2, 3, 4, 5];
        let chunk = DataChunk::new(0, 1, data.clone(), true);
        assert_eq!(chunk.sequence, 0);
        assert_eq!(chunk.total_chunks, 1);
        assert_eq!(chunk.data, data);
        assert!(chunk.is_last);
        assert!(chunk.checksum.is_none());
    }

    #[test]
    fn test_data_chunk_with_checksum() {
        let data = vec![1, 2, 3, 4, 5];
        let chunk = DataChunk::new_with_checksum(0, 1, data.clone(), true);
        assert!(chunk.checksum.is_some());
        assert!(chunk.verify_checksum());
    }

    #[test]
    fn test_data_chunk_size() {
        let data = vec![1, 2, 3, 4, 5];
        let chunk = DataChunk::new(0, 1, data, true);
        assert_eq!(chunk.size(), 5);
    }

    #[test]
    fn test_stream_stats_default() {
        let stats = StreamStats::new();
        assert_eq!(stats.bytes_streamed, 0);
        assert_eq!(stats.chunks_sent, 0);
        assert_eq!(stats.chunks_received, 0);
        assert_eq!(stats.buffer_usage, 0);
        assert_eq!(stats.peak_buffer_usage, 0);
        assert_eq!(stats.backpressure_count, 0);
    }

    #[test]
    fn test_stream_stats_update_buffer_usage() {
        let mut stats = StreamStats::new();
        stats.update_buffer_usage(1000);
        assert_eq!(stats.buffer_usage, 1000);
        assert_eq!(stats.peak_buffer_usage, 1000);

        stats.update_buffer_usage(500);
        assert_eq!(stats.buffer_usage, 500);
        assert_eq!(stats.peak_buffer_usage, 1000);

        stats.update_buffer_usage(2000);
        assert_eq!(stats.buffer_usage, 2000);
        assert_eq!(stats.peak_buffer_usage, 2000);
    }

    #[test]
    fn test_stream_stats_buffer_utilization() {
        let mut stats = StreamStats::new();
        stats.update_buffer_usage(5000);
        assert_eq!(stats.buffer_utilization(10000), 50.0);
        assert_eq!(stats.buffer_utilization(0), 0.0);
    }

    #[test]
    fn test_stream_stats_avg_chunk_size() {
        let mut stats = StreamStats::new();
        assert_eq!(stats.avg_chunk_size(), 0.0);

        stats.bytes_streamed = 10000;
        stats.chunks_sent = 5;
        assert_eq!(stats.avg_chunk_size(), 2000.0);
    }

    #[test]
    fn test_result_streamer_creation() {
        let config = StreamConfig::default();
        let streamer = ResultStreamer::new(config);
        assert_eq!(streamer.config().chunk_size, 8192);
    }

    #[test]
    fn test_result_streamer_should_stream() {
        let config = StreamConfig::default();
        let streamer = ResultStreamer::new(config);
        assert!(!streamer.should_stream(1024));
        assert!(streamer.should_stream(2_000_000));
    }

    #[test]
    fn test_result_streamer_split_into_chunks() {
        let config = StreamConfig::default().with_chunk_size(10);
        let streamer = ResultStreamer::new(config);

        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15];
        let chunks = streamer.split_into_chunks(data);

        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].data.len(), 10);
        assert_eq!(chunks[1].data.len(), 5);
        assert!(!chunks[0].is_last);
        assert!(chunks[1].is_last);
    }

    #[tokio::test]
    async fn test_chunk_sender_receiver() {
        let config = StreamConfig::default();
        let streamer = ResultStreamer::new(config);
        let (mut sender, mut receiver) = streamer.create_sender();

        let chunk = DataChunk::new(0, 1, vec![1, 2, 3], true);
        sender.send(chunk.clone()).await.unwrap();

        let received = receiver.recv().await.unwrap();
        assert_eq!(received.data, chunk.data);
    }

    #[tokio::test]
    async fn test_chunk_receiver_recv_all() {
        let config = StreamConfig::default().with_chunk_size(5);
        let streamer = ResultStreamer::new(config);
        let (mut sender, mut receiver) = streamer.create_sender();

        let data = vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10];
        sender.send_data(data.clone(), 2).await.unwrap();
        sender.close();

        let received_data = receiver.recv_all().await.unwrap();
        assert_eq!(received_data, data);
    }
}
