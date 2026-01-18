//! Advanced Workload-Specific Configuration Presets
//!
//! Provides production-tested configuration presets for common workload patterns.
//! Each preset is optimized for specific use cases with detailed explanations.
//!
//! # Features
//!
//! - E-commerce transaction processing
//! - Real-time notification systems
//! - Batch ETL pipelines
//! - Event-driven microservices
//! - IoT data ingestion
//! - Log aggregation
//! - Video processing pipelines
//! - Financial trading systems
//!
//! # Example
//!
//! ```
//! use celers_broker_sqs::workload_presets::WorkloadPreset;
//!
//! // Get optimized config for e-commerce workload
//! let config = WorkloadPreset::ecommerce_transactions();
//! println!("Visibility timeout: {}s", config.visibility_timeout_seconds);
//! println!("Max messages per receive: {}", config.max_messages);
//! println!("Long polling wait time: {}s", config.wait_time_seconds);
//! ```

/// Complete configuration preset for a specific workload
#[derive(Debug, Clone)]
pub struct WorkloadConfig {
    /// Visibility timeout in seconds
    pub visibility_timeout_seconds: u32,
    /// Long polling wait time in seconds
    pub wait_time_seconds: u32,
    /// Maximum messages per receive
    pub max_messages: u32,
    /// Message retention period in days
    pub message_retention_days: u32,
    /// Whether to use FIFO queue
    pub use_fifo: bool,
    /// Whether to enable compression
    pub enable_compression: bool,
    /// Compression threshold in bytes
    pub compression_threshold: usize,
    /// Enable auto-create queue
    pub auto_create_queue: bool,
    /// Recommended concurrent consumers
    pub recommended_consumers: usize,
    /// Recommended batch size for processing
    pub recommended_batch_size: usize,
    /// Description of the workload
    pub description: String,
    /// Cost optimization notes
    pub cost_notes: String,
}

/// Workload-specific configuration presets
pub struct WorkloadPreset;

impl WorkloadPreset {
    /// E-commerce transaction processing
    ///
    /// Optimized for:
    /// - Order processing
    /// - Payment verification
    /// - Inventory updates
    /// - Email confirmations
    ///
    /// Characteristics:
    /// - Medium to high throughput (100-10K TPS)
    /// - Low latency requirement (< 500ms)
    /// - Strict ordering per customer (FIFO)
    /// - High reliability (99.9%+ uptime)
    pub fn ecommerce_transactions() -> WorkloadConfig {
        WorkloadConfig {
            visibility_timeout_seconds: 120, // 2 minutes for payment processing
            wait_time_seconds: 10,           // Moderate latency tolerance
            max_messages: 10,                // Batch for efficiency
            message_retention_days: 4,       // Standard retention
            use_fifo: true,                  // Ordering per customer
            enable_compression: false,       // Small messages
            compression_threshold: 256 * 1024,
            auto_create_queue: true,
            recommended_consumers: 10, // Scale based on load
            recommended_batch_size: 5,
            description: "E-commerce transaction processing with FIFO ordering".to_string(),
            cost_notes: "Use FIFO for order integrity. Cost ~$0.50/M requests".to_string(),
        }
    }

    /// Real-time notification system
    ///
    /// Optimized for:
    /// - Push notifications
    /// - Email delivery
    /// - SMS messages
    /// - Webhooks
    ///
    /// Characteristics:
    /// - High throughput (10K-100K TPS)
    /// - Best-effort delivery
    /// - Low latency (< 1 second)
    /// - Large fan-out
    pub fn realtime_notifications() -> WorkloadConfig {
        WorkloadConfig {
            visibility_timeout_seconds: 30, // Quick retry if failed
            wait_time_seconds: 5,           // Low latency
            max_messages: 10,               // Process in batches
            message_retention_days: 1,      // Short retention for freshness
            use_fifo: false,                // Order not critical
            enable_compression: false,      // Small payloads
            compression_threshold: 256 * 1024,
            auto_create_queue: true,
            recommended_consumers: 50, // High concurrency
            recommended_batch_size: 10,
            description: "Real-time notifications with high throughput".to_string(),
            cost_notes: "Standard queue for lowest cost. ~$0.40/M requests".to_string(),
        }
    }

    /// Batch ETL pipeline
    ///
    /// Optimized for:
    /// - Data warehouse loading
    /// - Report generation
    /// - Analytics processing
    /// - Data transformation
    ///
    /// Characteristics:
    /// - Medium throughput (100-1K TPS)
    /// - High latency tolerance (minutes to hours)
    /// - Large message sizes
    /// - Batch-oriented processing
    pub fn batch_etl_pipeline() -> WorkloadConfig {
        WorkloadConfig {
            visibility_timeout_seconds: 3600, // 1 hour for long-running jobs
            wait_time_seconds: 20,            // Maximum long polling
            max_messages: 10,                 // Process in batches
            message_retention_days: 14,       // Maximum retention
            use_fifo: false,                  // Order not required
            enable_compression: true,         // Large data payloads
            compression_threshold: 10 * 1024, // 10KB threshold
            auto_create_queue: true,
            recommended_consumers: 5, // Limited concurrency
            recommended_batch_size: 10,
            description: "Batch ETL with large messages and long processing".to_string(),
            cost_notes: "Enable compression for large payloads. Use batching".to_string(),
        }
    }

    /// Event-driven microservices
    ///
    /// Optimized for:
    /// - Service-to-service communication
    /// - Event sourcing
    /// - CQRS patterns
    /// - Saga orchestration
    ///
    /// Characteristics:
    /// - Medium throughput (1K-10K TPS)
    /// - Medium latency (< 5 seconds)
    /// - Ordering per aggregate
    /// - Idempotent processing
    pub fn event_driven_microservices() -> WorkloadConfig {
        WorkloadConfig {
            visibility_timeout_seconds: 300, // 5 minutes for service calls
            wait_time_seconds: 15,           // Balance cost and latency
            max_messages: 10,
            message_retention_days: 7, // Week-long retention for replay
            use_fifo: true,            // Event ordering critical
            enable_compression: false,
            compression_threshold: 256 * 1024,
            auto_create_queue: true,
            recommended_consumers: 20,
            recommended_batch_size: 5,
            description: "Event-driven microservices with FIFO ordering".to_string(),
            cost_notes: "FIFO for event ordering. Consider deduplication".to_string(),
        }
    }

    /// IoT data ingestion
    ///
    /// Optimized for:
    /// - Sensor data collection
    /// - Telemetry processing
    /// - Device state updates
    /// - Time-series data
    ///
    /// Characteristics:
    /// - Very high throughput (100K+ TPS)
    /// - Best-effort delivery
    /// - Small messages
    /// - Bursty traffic
    pub fn iot_data_ingestion() -> WorkloadConfig {
        WorkloadConfig {
            visibility_timeout_seconds: 60, // Quick processing
            wait_time_seconds: 5,           // Low latency for real-time
            max_messages: 10,
            message_retention_days: 1,       // Short retention
            use_fifo: false,                 // Order not critical
            enable_compression: true,        // Aggregate small messages
            compression_threshold: 5 * 1024, // 5KB threshold
            auto_create_queue: true,
            recommended_consumers: 100, // Very high concurrency
            recommended_batch_size: 10,
            description: "IoT data ingestion with very high throughput".to_string(),
            cost_notes: "Use batching aggressively. Standard queue for scale".to_string(),
        }
    }

    /// Log aggregation
    ///
    /// Optimized for:
    /// - Application logs
    /// - Audit trails
    /// - Security events
    /// - Metrics collection
    ///
    /// Characteristics:
    /// - High throughput (10K-100K TPS)
    /// - High latency tolerance
    /// - Medium message sizes
    /// - Batch processing preferred
    pub fn log_aggregation() -> WorkloadConfig {
        WorkloadConfig {
            visibility_timeout_seconds: 600, // 10 minutes for batch processing
            wait_time_seconds: 20,           // Max long polling for cost
            max_messages: 10,
            message_retention_days: 7,        // Week-long retention
            use_fifo: false,                  // Order not critical
            enable_compression: true,         // Compress log data
            compression_threshold: 20 * 1024, // 20KB threshold
            auto_create_queue: true,
            recommended_consumers: 10,
            recommended_batch_size: 10,
            description: "Log aggregation with compression and batching".to_string(),
            cost_notes: "Maximize batching and compression for cost savings".to_string(),
        }
    }

    /// Video processing pipeline
    ///
    /// Optimized for:
    /// - Video transcoding
    /// - Thumbnail generation
    /// - Content moderation
    /// - Media analysis
    ///
    /// Characteristics:
    /// - Low to medium throughput (10-100 TPS)
    /// - Very high latency tolerance (hours)
    /// - Large messages (metadata/URLs)
    /// - CPU-intensive processing
    pub fn video_processing() -> WorkloadConfig {
        WorkloadConfig {
            visibility_timeout_seconds: 7200, // 2 hours for transcoding
            wait_time_seconds: 20,            // Max long polling
            max_messages: 1,                  // Process one at a time
            message_retention_days: 14,       // Max retention for retry
            use_fifo: false,
            enable_compression: false, // Metadata only
            compression_threshold: 256 * 1024,
            auto_create_queue: true,
            recommended_consumers: 5, // Limited by compute
            recommended_batch_size: 1,
            description: "Video processing with long-running tasks".to_string(),
            cost_notes: "Long visibility timeout. Consider spot instances".to_string(),
        }
    }

    /// Financial trading system
    ///
    /// Optimized for:
    /// - Trade execution
    /// - Order matching
    /// - Risk calculations
    /// - Settlement processing
    ///
    /// Characteristics:
    /// - Medium throughput (1K-10K TPS)
    /// - Ultra-low latency (< 100ms)
    /// - Strict ordering
    /// - High reliability
    pub fn financial_trading() -> WorkloadConfig {
        WorkloadConfig {
            visibility_timeout_seconds: 60, // Quick processing required
            wait_time_seconds: 1,           // Minimum latency
            max_messages: 1,                // Process sequentially
            message_retention_days: 14,     // Audit trail
            use_fifo: true,                 // Strict ordering
            enable_compression: false,      // Low latency priority
            compression_threshold: 256 * 1024,
            auto_create_queue: true,
            recommended_consumers: 10, // Controlled concurrency
            recommended_batch_size: 1,
            description: "Financial trading with strict ordering and low latency".to_string(),
            cost_notes: "FIFO for ordering. Consider dedicated throughput mode".to_string(),
        }
    }

    /// Machine learning inference
    ///
    /// Optimized for:
    /// - ML model serving
    /// - Batch predictions
    /// - Feature extraction
    /// - Model scoring
    ///
    /// Characteristics:
    /// - Medium throughput (100-1K TPS)
    /// - Medium latency (1-10 seconds)
    /// - Variable message sizes
    /// - GPU/CPU intensive
    pub fn ml_inference() -> WorkloadConfig {
        WorkloadConfig {
            visibility_timeout_seconds: 600, // 10 minutes for inference
            wait_time_seconds: 15,
            max_messages: 5,           // Batch for GPU efficiency
            message_retention_days: 7, // Week-long retention
            use_fifo: false,
            enable_compression: true,         // Large feature vectors
            compression_threshold: 50 * 1024, // 50KB threshold
            auto_create_queue: true,
            recommended_consumers: 5, // Limited by GPU
            recommended_batch_size: 5,
            description: "ML inference with batch processing".to_string(),
            cost_notes: "Batch inference for GPU efficiency. Use compression".to_string(),
        }
    }

    /// API webhook delivery
    ///
    /// Optimized for:
    /// - Webhook notifications
    /// - API callbacks
    /// - Third-party integrations
    /// - Event notifications
    ///
    /// Characteristics:
    /// - Medium throughput (100-10K TPS)
    /// - Retry with backoff
    /// - Variable latency tolerance
    /// - HTTP-based delivery
    pub fn api_webhooks() -> WorkloadConfig {
        WorkloadConfig {
            visibility_timeout_seconds: 180, // 3 minutes with retries
            wait_time_seconds: 10,
            max_messages: 10,
            message_retention_days: 3, // Short retention
            use_fifo: false,           // Order not critical
            enable_compression: false,
            compression_threshold: 256 * 1024,
            auto_create_queue: true,
            recommended_consumers: 20, // High concurrency
            recommended_batch_size: 5,
            description: "API webhook delivery with retry logic".to_string(),
            cost_notes: "Standard queue for best-effort delivery".to_string(),
        }
    }

    /// Get all available presets
    pub fn all_presets() -> Vec<(&'static str, WorkloadConfig)> {
        vec![
            ("ecommerce_transactions", Self::ecommerce_transactions()),
            ("realtime_notifications", Self::realtime_notifications()),
            ("batch_etl_pipeline", Self::batch_etl_pipeline()),
            (
                "event_driven_microservices",
                Self::event_driven_microservices(),
            ),
            ("iot_data_ingestion", Self::iot_data_ingestion()),
            ("log_aggregation", Self::log_aggregation()),
            ("video_processing", Self::video_processing()),
            ("financial_trading", Self::financial_trading()),
            ("ml_inference", Self::ml_inference()),
            ("api_webhooks", Self::api_webhooks()),
        ]
    }

    /// Compare configurations side-by-side
    #[allow(clippy::type_complexity)]
    pub fn compare_presets(preset_names: &[&str]) -> String {
        let mut output = String::from("Preset Comparison\n");
        output.push_str("=================\n\n");

        let presets: Vec<(&str, WorkloadConfig)> = preset_names
            .iter()
            .filter_map(|&name| Self::all_presets().into_iter().find(|(n, _)| n == &name))
            .collect();

        if presets.is_empty() {
            return "No valid presets found".to_string();
        }

        // Headers
        output.push_str(&format!("{:<30} | ", "Configuration"));
        for (name, _) in &presets {
            output.push_str(&format!("{:<20} | ", name));
        }
        output.push('\n');
        output.push_str(&"-".repeat(30 + (presets.len() * 23)));
        output.push('\n');

        // Rows
        let rows: Vec<(&str, Box<dyn Fn(&WorkloadConfig) -> String>)> = vec![
            (
                "Visibility Timeout (s)",
                Box::new(|c: &WorkloadConfig| c.visibility_timeout_seconds.to_string()),
            ),
            (
                "Wait Time (s)",
                Box::new(|c: &WorkloadConfig| c.wait_time_seconds.to_string()),
            ),
            (
                "Max Messages",
                Box::new(|c: &WorkloadConfig| c.max_messages.to_string()),
            ),
            (
                "Retention (days)",
                Box::new(|c: &WorkloadConfig| c.message_retention_days.to_string()),
            ),
            (
                "Use FIFO",
                Box::new(|c: &WorkloadConfig| c.use_fifo.to_string()),
            ),
            (
                "Compression",
                Box::new(|c: &WorkloadConfig| c.enable_compression.to_string()),
            ),
            (
                "Recommended Consumers",
                Box::new(|c: &WorkloadConfig| c.recommended_consumers.to_string()),
            ),
            (
                "Batch Size",
                Box::new(|c: &WorkloadConfig| c.recommended_batch_size.to_string()),
            ),
        ];

        for (label, getter) in &rows {
            output.push_str(&format!("{:<30} | ", label));
            for (_, config) in &presets {
                output.push_str(&format!("{:<20} | ", getter(config)));
            }
            output.push('\n');
        }

        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ecommerce_preset() {
        let config = WorkloadPreset::ecommerce_transactions();
        assert_eq!(config.visibility_timeout_seconds, 120);
        assert!(config.use_fifo);
        assert_eq!(config.recommended_consumers, 10);
    }

    #[test]
    fn test_realtime_notifications_preset() {
        let config = WorkloadPreset::realtime_notifications();
        assert_eq!(config.visibility_timeout_seconds, 30);
        assert!(!config.use_fifo);
        assert_eq!(config.wait_time_seconds, 5);
    }

    #[test]
    fn test_batch_etl_preset() {
        let config = WorkloadPreset::batch_etl_pipeline();
        assert_eq!(config.visibility_timeout_seconds, 3600);
        assert!(config.enable_compression);
        assert_eq!(config.message_retention_days, 14);
    }

    #[test]
    fn test_iot_ingestion_preset() {
        let config = WorkloadPreset::iot_data_ingestion();
        assert_eq!(config.recommended_consumers, 100);
        assert!(!config.use_fifo);
        assert!(config.enable_compression);
    }

    #[test]
    fn test_financial_trading_preset() {
        let config = WorkloadPreset::financial_trading();
        assert_eq!(config.wait_time_seconds, 1); // Minimum latency
        assert!(config.use_fifo); // Strict ordering
        assert_eq!(config.recommended_batch_size, 1); // Sequential
    }

    #[test]
    fn test_video_processing_preset() {
        let config = WorkloadPreset::video_processing();
        assert_eq!(config.visibility_timeout_seconds, 7200); // 2 hours
        assert_eq!(config.max_messages, 1); // One at a time
    }

    #[test]
    fn test_all_presets() {
        let presets = WorkloadPreset::all_presets();
        assert_eq!(presets.len(), 10);
        assert!(presets
            .iter()
            .any(|(name, _)| *name == "ecommerce_transactions"));
    }

    #[test]
    fn test_compare_presets() {
        let comparison =
            WorkloadPreset::compare_presets(&["ecommerce_transactions", "realtime_notifications"]);
        assert!(comparison.contains("Visibility Timeout"));
        assert!(comparison.contains("ecommerce_transactions"));
        assert!(comparison.contains("realtime_notifications"));
    }

    #[test]
    fn test_ml_inference_preset() {
        let config = WorkloadPreset::ml_inference();
        assert!(config.enable_compression);
        assert_eq!(config.recommended_batch_size, 5);
    }

    #[test]
    fn test_api_webhooks_preset() {
        let config = WorkloadPreset::api_webhooks();
        assert!(!config.use_fifo);
        assert_eq!(config.visibility_timeout_seconds, 180);
    }
}
