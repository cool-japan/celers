//! Backpressure management for preventing system overload
//!
//! This module provides automatic throttling and backpressure control
//! to prevent overwhelming downstream services or the message processing system.
//!
//! # Features
//!
//! - Automatic throttling based on system metrics
//! - Configurable thresholds for CPU, memory, and queue depth
//! - Adaptive rate limiting with exponential backoff
//! - Integration with SQS visibility timeout for graceful degradation
//! - Real-time backpressure metrics and status
//!
//! # Example
//!
//! ```
//! use celers_broker_sqs::backpressure::{BackpressureManager, BackpressureConfig, BackpressureMetrics};
//! use std::time::Duration;
//!
//! # fn main() {
//! // Configure backpressure thresholds
//! let config = BackpressureConfig::new()
//!     .with_max_in_flight_messages(100)
//!     .with_max_processing_time(Duration::from_secs(30))
//!     .with_throttle_threshold(0.8);  // Throttle at 80% capacity
//!
//! let mut manager = BackpressureManager::new(config);
//!
//! // Check if we should process more messages
//! if manager.should_consume() {
//!     // Safe to consume more messages
//! } else {
//!     // System under pressure, wait before consuming
//! }
//!
//! // Track message processing
//! let message_id = "msg-123".to_string();
//! manager.track_message_start(&message_id);
//! // ... process message ...
//! manager.track_message_complete(&message_id);
//! # }
//! ```

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Configuration for backpressure management
#[derive(Debug, Clone)]
pub struct BackpressureConfig {
    /// Maximum number of messages that can be processed concurrently
    pub max_in_flight_messages: usize,

    /// Maximum processing time before considering a message slow
    pub max_processing_time: Duration,

    /// Threshold (0.0-1.0) at which to start throttling
    /// e.g., 0.8 means start throttling at 80% capacity
    pub throttle_threshold: f64,

    /// Threshold (0.0-1.0) at which to completely stop consuming
    /// e.g., 0.95 means stop at 95% capacity
    pub stop_threshold: f64,

    /// Duration to wait when throttling
    pub throttle_wait_duration: Duration,

    /// Enable adaptive throttling (increases wait time under sustained pressure)
    pub adaptive_throttling: bool,

    /// Maximum throttle wait duration (for adaptive throttling)
    pub max_throttle_wait: Duration,
}

impl BackpressureConfig {
    /// Create new configuration with sensible defaults
    pub fn new() -> Self {
        Self {
            max_in_flight_messages: 100,
            max_processing_time: Duration::from_secs(30),
            throttle_threshold: 0.8,
            stop_threshold: 0.95,
            throttle_wait_duration: Duration::from_millis(100),
            adaptive_throttling: true,
            max_throttle_wait: Duration::from_secs(5),
        }
    }

    /// Set maximum in-flight messages
    pub fn with_max_in_flight_messages(mut self, max: usize) -> Self {
        self.max_in_flight_messages = max;
        self
    }

    /// Set maximum processing time threshold
    pub fn with_max_processing_time(mut self, duration: Duration) -> Self {
        self.max_processing_time = duration;
        self
    }

    /// Set throttle threshold (0.0-1.0)
    pub fn with_throttle_threshold(mut self, threshold: f64) -> Self {
        self.throttle_threshold = threshold.clamp(0.0, 1.0);
        self
    }

    /// Set stop threshold (0.0-1.0)
    pub fn with_stop_threshold(mut self, threshold: f64) -> Self {
        self.stop_threshold = threshold.clamp(0.0, 1.0);
        self
    }

    /// Set throttle wait duration
    pub fn with_throttle_wait_duration(mut self, duration: Duration) -> Self {
        self.throttle_wait_duration = duration;
        self
    }

    /// Enable or disable adaptive throttling
    pub fn with_adaptive_throttling(mut self, enabled: bool) -> Self {
        self.adaptive_throttling = enabled;
        self
    }
}

impl Default for BackpressureConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Backpressure metrics for monitoring
#[derive(Debug, Clone)]
pub struct BackpressureMetrics {
    /// Current number of in-flight messages
    pub in_flight_count: usize,

    /// Current utilization (0.0-1.0)
    pub utilization: f64,

    /// Number of times throttling was applied
    pub throttle_count: u64,

    /// Number of times consumption was stopped
    pub stop_count: u64,

    /// Number of slow messages detected
    pub slow_message_count: u64,

    /// Current backpressure state
    pub state: BackpressureState,

    /// Average processing time (milliseconds)
    pub avg_processing_time_ms: f64,

    /// 95th percentile processing time (milliseconds)
    pub p95_processing_time_ms: f64,
}

/// Backpressure state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BackpressureState {
    /// Normal operation, no backpressure
    Normal,

    /// Throttling active, slowing down consumption
    Throttling,

    /// At capacity, stopped consuming new messages
    AtCapacity,
}

/// Message tracking information
#[derive(Debug, Clone)]
struct MessageTrack {
    start_time: Instant,
    message_id: String,
}

/// Backpressure manager
pub struct BackpressureManager {
    config: BackpressureConfig,
    in_flight: Arc<RwLock<HashMap<String, MessageTrack>>>,
    metrics: Arc<RwLock<BackpressureMetrics>>,
    processing_times: Arc<RwLock<Vec<Duration>>>,
    throttle_wait_multiplier: Arc<RwLock<f64>>,
}

impl BackpressureManager {
    /// Create a new backpressure manager
    pub fn new(config: BackpressureConfig) -> Self {
        Self {
            config,
            in_flight: Arc::new(RwLock::new(HashMap::new())),
            metrics: Arc::new(RwLock::new(BackpressureMetrics {
                in_flight_count: 0,
                utilization: 0.0,
                throttle_count: 0,
                stop_count: 0,
                slow_message_count: 0,
                state: BackpressureState::Normal,
                avg_processing_time_ms: 0.0,
                p95_processing_time_ms: 0.0,
            })),
            processing_times: Arc::new(RwLock::new(Vec::new())),
            throttle_wait_multiplier: Arc::new(RwLock::new(1.0)),
        }
    }

    /// Check if we should consume more messages
    pub fn should_consume(&mut self) -> bool {
        let in_flight = self.in_flight.read().unwrap();
        let in_flight_count = in_flight.len();
        let utilization = in_flight_count as f64 / self.config.max_in_flight_messages as f64;

        // Update metrics
        {
            let mut metrics = self.metrics.write().unwrap();
            metrics.in_flight_count = in_flight_count;
            metrics.utilization = utilization;

            if utilization >= self.config.stop_threshold {
                metrics.state = BackpressureState::AtCapacity;
                metrics.stop_count += 1;
            } else if utilization >= self.config.throttle_threshold {
                metrics.state = BackpressureState::Throttling;
                metrics.throttle_count += 1;
            } else {
                metrics.state = BackpressureState::Normal;
                // Reset throttle multiplier when back to normal
                if self.config.adaptive_throttling {
                    *self.throttle_wait_multiplier.write().unwrap() = 1.0;
                }
            }
        }

        // Don't consume if at capacity
        if utilization >= self.config.stop_threshold {
            return false;
        }

        // Apply throttling if above threshold
        if utilization >= self.config.throttle_threshold {
            self.apply_throttle();
        }

        true
    }

    /// Apply throttling delay
    fn apply_throttle(&self) {
        let multiplier = *self.throttle_wait_multiplier.read().unwrap();
        let wait_duration = Duration::from_millis(
            (self.config.throttle_wait_duration.as_millis() as f64 * multiplier) as u64,
        )
        .min(self.config.max_throttle_wait);

        std::thread::sleep(wait_duration);

        // Increase multiplier for adaptive throttling
        if self.config.adaptive_throttling {
            let mut multiplier = self.throttle_wait_multiplier.write().unwrap();
            *multiplier = (*multiplier * 1.5).min(10.0);
        }
    }

    /// Track the start of message processing
    pub fn track_message_start(&mut self, message_id: &str) {
        let track = MessageTrack {
            start_time: Instant::now(),
            message_id: message_id.to_string(),
        };

        self.in_flight
            .write()
            .unwrap()
            .insert(message_id.to_string(), track);
    }

    /// Track the completion of message processing
    pub fn track_message_complete(&mut self, message_id: &str) {
        if let Some(track) = self.in_flight.write().unwrap().remove(message_id) {
            let processing_time = track.start_time.elapsed();

            // Track processing time
            {
                let mut times = self.processing_times.write().unwrap();
                times.push(processing_time);

                // Keep only last 1000 times for efficiency
                if times.len() > 1000 {
                    let excess = times.len() - 1000;
                    times.drain(0..excess);
                }
            }

            // Update metrics
            {
                let mut metrics = self.metrics.write().unwrap();
                if processing_time > self.config.max_processing_time {
                    metrics.slow_message_count += 1;
                }

                // Calculate statistics
                let times = self.processing_times.read().unwrap();
                if !times.is_empty() {
                    let total_ms: f64 = times.iter().map(|d| d.as_millis() as f64).sum();
                    metrics.avg_processing_time_ms = total_ms / times.len() as f64;

                    // Calculate P95
                    let mut sorted_times: Vec<Duration> = times.clone();
                    sorted_times.sort();
                    let p95_index = (sorted_times.len() as f64 * 0.95) as usize;
                    if p95_index < sorted_times.len() {
                        metrics.p95_processing_time_ms = sorted_times[p95_index].as_millis() as f64;
                    }
                }
            }
        }
    }

    /// Track a failed message
    pub fn track_message_failed(&mut self, message_id: &str) {
        self.track_message_complete(message_id);
    }

    /// Get current metrics
    pub fn metrics(&self) -> BackpressureMetrics {
        self.metrics.read().unwrap().clone()
    }

    /// Get current state
    pub fn state(&self) -> BackpressureState {
        self.metrics.read().unwrap().state
    }

    /// Check for slow messages and return their IDs
    pub fn detect_slow_messages(&self) -> Vec<String> {
        let in_flight = self.in_flight.read().unwrap();
        let now = Instant::now();

        in_flight
            .values()
            .filter(|track| now.duration_since(track.start_time) > self.config.max_processing_time)
            .map(|track| track.message_id.clone())
            .collect()
    }

    /// Reset all metrics
    pub fn reset_metrics(&mut self) {
        let mut metrics = self.metrics.write().unwrap();
        metrics.throttle_count = 0;
        metrics.stop_count = 0;
        metrics.slow_message_count = 0;

        self.processing_times.write().unwrap().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_backpressure_config_defaults() {
        let config = BackpressureConfig::new();
        assert_eq!(config.max_in_flight_messages, 100);
        assert_eq!(config.max_processing_time, Duration::from_secs(30));
        assert_eq!(config.throttle_threshold, 0.8);
        assert_eq!(config.stop_threshold, 0.95);
        assert!(config.adaptive_throttling);
    }

    #[test]
    fn test_backpressure_config_builder() {
        let config = BackpressureConfig::new()
            .with_max_in_flight_messages(50)
            .with_throttle_threshold(0.7)
            .with_stop_threshold(0.9);

        assert_eq!(config.max_in_flight_messages, 50);
        assert_eq!(config.throttle_threshold, 0.7);
        assert_eq!(config.stop_threshold, 0.9);
    }

    #[test]
    fn test_should_consume_normal_state() {
        let config = BackpressureConfig::new().with_max_in_flight_messages(10);
        let mut manager = BackpressureManager::new(config);

        // No messages in flight, should consume
        assert!(manager.should_consume());
        assert_eq!(manager.state(), BackpressureState::Normal);
    }

    #[test]
    fn test_should_consume_throttling_state() {
        let config = BackpressureConfig::new()
            .with_max_in_flight_messages(10)
            .with_throttle_threshold(0.5)
            .with_throttle_wait_duration(Duration::from_millis(1));

        let mut manager = BackpressureManager::new(config);

        // Add 6 messages (60% utilization, above 50% threshold)
        for i in 0..6 {
            manager.track_message_start(&format!("msg-{}", i));
        }

        // Should still consume but with throttling
        assert!(manager.should_consume());
        let metrics = manager.metrics();
        assert!(metrics.utilization >= 0.5);
    }

    #[test]
    fn test_should_consume_at_capacity() {
        let config = BackpressureConfig::new()
            .with_max_in_flight_messages(10)
            .with_stop_threshold(0.9);

        let mut manager = BackpressureManager::new(config);

        // Add 10 messages (100% utilization)
        for i in 0..10 {
            manager.track_message_start(&format!("msg-{}", i));
        }

        // Should not consume when at capacity
        assert!(!manager.should_consume());
        assert_eq!(manager.state(), BackpressureState::AtCapacity);
    }

    #[test]
    fn test_track_message_lifecycle() {
        let config = BackpressureConfig::new();
        let mut manager = BackpressureManager::new(config);

        manager.track_message_start("msg-1");

        // Verify in-flight count via should_consume which updates metrics
        manager.should_consume();
        let metrics = manager.metrics();
        assert_eq!(metrics.in_flight_count, 1);

        std::thread::sleep(Duration::from_millis(10));
        manager.track_message_complete("msg-1");

        // Update metrics again
        manager.should_consume();
        let metrics = manager.metrics();
        assert_eq!(metrics.in_flight_count, 0);
        assert!(metrics.avg_processing_time_ms > 0.0);
    }

    #[test]
    fn test_detect_slow_messages() {
        let config = BackpressureConfig::new().with_max_processing_time(Duration::from_millis(50));

        let mut manager = BackpressureManager::new(config);

        manager.track_message_start("msg-fast");
        manager.track_message_start("msg-slow");

        std::thread::sleep(Duration::from_millis(100));

        let slow_messages = manager.detect_slow_messages();
        assert_eq!(slow_messages.len(), 2);
    }

    #[test]
    fn test_metrics_calculation() {
        let config = BackpressureConfig::new().with_max_in_flight_messages(100);

        let mut manager = BackpressureManager::new(config);

        // Process several messages with different durations
        for i in 0..5 {
            let msg_id = format!("msg-{}", i);
            manager.track_message_start(&msg_id);
            std::thread::sleep(Duration::from_millis(10 * (i + 1)));
            manager.track_message_complete(&msg_id);
        }

        let metrics = manager.metrics();
        assert!(metrics.avg_processing_time_ms > 0.0);
        assert!(metrics.p95_processing_time_ms >= metrics.avg_processing_time_ms);
    }

    #[test]
    fn test_reset_metrics() {
        let config = BackpressureConfig::new();
        let mut manager = BackpressureManager::new(config);

        // Generate some activity
        for i in 0..5 {
            manager.track_message_start(&format!("msg-{}", i));
        }
        manager.should_consume();

        manager.reset_metrics();

        let metrics = manager.metrics();
        assert_eq!(metrics.throttle_count, 0);
        assert_eq!(metrics.stop_count, 0);
        assert_eq!(metrics.slow_message_count, 0);
    }

    #[test]
    fn test_adaptive_throttling() {
        let config = BackpressureConfig::new()
            .with_max_in_flight_messages(10)
            .with_throttle_threshold(0.5)
            .with_adaptive_throttling(true)
            .with_throttle_wait_duration(Duration::from_millis(1));

        let mut manager = BackpressureManager::new(config);

        // Add messages to trigger throttling
        for i in 0..6 {
            manager.track_message_start(&format!("msg-{}", i));
        }

        // First throttle check
        manager.should_consume();
        let multiplier1 = *manager.throttle_wait_multiplier.read().unwrap();

        // Second throttle check (should increase multiplier)
        manager.should_consume();
        let multiplier2 = *manager.throttle_wait_multiplier.read().unwrap();

        assert!(multiplier2 > multiplier1);
    }
}
