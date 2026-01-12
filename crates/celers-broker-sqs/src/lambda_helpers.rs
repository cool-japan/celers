//! AWS Lambda integration helpers for SQS event processing
//!
//! This module provides utilities for processing SQS events in AWS Lambda functions,
//! including batch processing, error handling, and partial batch responses.
//!
//! # Features
//!
//! - SQS event parsing and deserialization
//! - Batch processing with partial failure handling
//! - Automatic retry for failed messages
//! - Dead Letter Queue integration
//! - Message attribute extraction
//! - Lambda context integration
//!
//! # Example
//!
//! ```
//! use celers_broker_sqs::lambda_helpers::LambdaEventProcessor;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let processor = LambdaEventProcessor::new();
//!
//! // Simulate Lambda SQS event
//! let event_json = r#"{
//!     "Records": [{
//!         "messageId": "msg-123",
//!         "body": "{}",
//!         "receiptHandle": "handle-123",
//!         "messageAttributes": {}
//!     }]
//! }"#;
//!
//! // Process the event
//! let results = processor.process_event(event_json, |record| {
//!     // Your message processing logic
//!     Ok(())
//! })?;
//!
//! // Check for failures
//! if results.has_failures() {
//!     println!("Failed messages: {}", results.failed_count);
//! }
//! # Ok(())
//! # }
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// SQS event record from Lambda
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SqsEventRecord {
    /// Message ID
    pub message_id: String,

    /// Receipt handle for message deletion
    pub receipt_handle: String,

    /// Message body (JSON string)
    pub body: String,

    /// Message attributes
    #[serde(default)]
    pub message_attributes: HashMap<String, MessageAttribute>,

    /// MD5 of body
    #[serde(default)]
    pub md5_of_body: Option<String>,

    /// Queue ARN
    #[serde(default)]
    pub event_source_arn: Option<String>,

    /// Event source (usually "aws:sqs")
    #[serde(default)]
    pub event_source: Option<String>,

    /// AWS region
    #[serde(default)]
    pub aws_region: Option<String>,
}

/// Message attribute from SQS
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageAttribute {
    /// Data type (String, Number, Binary)
    pub data_type: String,

    /// String value
    #[serde(default)]
    pub string_value: Option<String>,

    /// Binary value (base64 encoded)
    #[serde(default)]
    pub binary_value: Option<String>,
}

/// Lambda SQS event
#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct LambdaSqsEvent {
    /// SQS records
    pub records: Vec<SqsEventRecord>,
}

/// Processing result for a single record
#[derive(Debug, Clone)]
pub struct RecordProcessingResult {
    /// Message ID
    pub message_id: String,

    /// Whether processing succeeded
    pub success: bool,

    /// Error message if failed
    pub error: Option<String>,
}

/// Batch processing results
#[derive(Debug, Clone)]
pub struct BatchProcessingResults {
    /// Individual record results
    pub results: Vec<RecordProcessingResult>,

    /// Total number of records processed
    pub total_count: usize,

    /// Number of successful records
    pub success_count: usize,

    /// Number of failed records
    pub failed_count: usize,
}

impl BatchProcessingResults {
    /// Check if any records failed
    pub fn has_failures(&self) -> bool {
        self.failed_count > 0
    }

    /// Get failed message IDs
    pub fn failed_message_ids(&self) -> Vec<String> {
        self.results
            .iter()
            .filter(|r| !r.success)
            .map(|r| r.message_id.clone())
            .collect()
    }

    /// Get successful message IDs
    pub fn successful_message_ids(&self) -> Vec<String> {
        self.results
            .iter()
            .filter(|r| r.success)
            .map(|r| r.message_id.clone())
            .collect()
    }

    /// Get batch failure response for Lambda (partial batch response)
    pub fn to_batch_item_failures(&self) -> BatchItemFailures {
        BatchItemFailures {
            batch_item_failures: self
                .results
                .iter()
                .filter(|r| !r.success)
                .map(|r| BatchItemFailure {
                    item_identifier: r.message_id.clone(),
                })
                .collect(),
        }
    }
}

/// Lambda batch item failure response
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchItemFailures {
    /// Failed message IDs
    pub batch_item_failures: Vec<BatchItemFailure>,
}

/// Single batch item failure
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchItemFailure {
    /// Message ID that failed
    pub item_identifier: String,
}

/// Lambda event processor for SQS
pub struct LambdaEventProcessor {
    /// Maximum number of retries for transient errors
    pub max_retries: usize,

    /// Enable detailed error logging
    pub detailed_logging: bool,
}

impl LambdaEventProcessor {
    /// Create new event processor
    pub fn new() -> Self {
        Self {
            max_retries: 0,
            detailed_logging: true,
        }
    }

    /// Set maximum retries
    pub fn with_max_retries(mut self, retries: usize) -> Self {
        self.max_retries = retries;
        self
    }

    /// Enable or disable detailed logging
    pub fn with_detailed_logging(mut self, enabled: bool) -> Self {
        self.detailed_logging = enabled;
        self
    }

    /// Process Lambda SQS event
    pub fn process_event<F>(
        &self,
        event_json: &str,
        processor: F,
    ) -> Result<BatchProcessingResults, String>
    where
        F: Fn(&SqsEventRecord) -> Result<(), String>,
    {
        // Parse event
        let event: LambdaSqsEvent = serde_json::from_str(event_json)
            .map_err(|e| format!("Failed to parse SQS event: {}", e))?;

        let total_count = event.records.len();
        let mut results = Vec::new();

        // Process each record
        for record in &event.records {
            let result = self.process_record(record, &processor);
            results.push(result);
        }

        let success_count = results.iter().filter(|r| r.success).count();
        let failed_count = results.len() - success_count;

        Ok(BatchProcessingResults {
            results,
            total_count,
            success_count,
            failed_count,
        })
    }

    /// Process a single record
    fn process_record<F>(&self, record: &SqsEventRecord, processor: &F) -> RecordProcessingResult
    where
        F: Fn(&SqsEventRecord) -> Result<(), String>,
    {
        let mut attempts = 0;
        let mut last_error = None;

        while attempts <= self.max_retries {
            match processor(record) {
                Ok(()) => {
                    return RecordProcessingResult {
                        message_id: record.message_id.clone(),
                        success: true,
                        error: None,
                    };
                }
                Err(e) => {
                    last_error = Some(e.clone());
                    if self.detailed_logging {
                        eprintln!(
                            "Error processing message {} (attempt {}): {}",
                            record.message_id,
                            attempts + 1,
                            e
                        );
                    }
                    attempts += 1;
                }
            }
        }

        RecordProcessingResult {
            message_id: record.message_id.clone(),
            success: false,
            error: last_error,
        }
    }

    /// Extract message attribute as string
    pub fn get_string_attribute(record: &SqsEventRecord, key: &str) -> Option<String> {
        record
            .message_attributes
            .get(key)
            .and_then(|attr| attr.string_value.clone())
    }

    /// Extract message attribute as number
    pub fn get_number_attribute(record: &SqsEventRecord, key: &str) -> Option<f64> {
        Self::get_string_attribute(record, key).and_then(|s| s.parse::<f64>().ok())
    }

    /// Check if record is from FIFO queue
    pub fn is_fifo_queue(record: &SqsEventRecord) -> bool {
        record
            .event_source_arn
            .as_ref()
            .map(|arn| arn.ends_with(".fifo"))
            .unwrap_or(false)
    }

    /// Extract queue name from ARN
    pub fn extract_queue_name(record: &SqsEventRecord) -> Option<String> {
        record
            .event_source_arn
            .as_ref()
            .and_then(|arn| arn.split(':').next_back().map(|s| s.to_string()))
    }
}

impl Default for LambdaEventProcessor {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper to create batch failure response JSON
pub fn create_batch_failure_response(failed_message_ids: &[String]) -> String {
    let failures = BatchItemFailures {
        batch_item_failures: failed_message_ids
            .iter()
            .map(|id| BatchItemFailure {
                item_identifier: id.clone(),
            })
            .collect(),
    };

    serde_json::to_string(&failures).unwrap_or_else(|_| "{}".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_event(message_count: usize) -> String {
        let records: Vec<String> = (0..message_count)
            .map(|i| {
                format!(
                    r#"{{
                        "messageId": "msg-{}",
                        "receiptHandle": "handle-{}",
                        "body": "{{\"task\": \"test\"}}",
                        "messageAttributes": {{}}
                    }}"#,
                    i, i
                )
            })
            .collect();

        format!(r#"{{"Records": [{}]}}"#, records.join(","))
    }

    #[test]
    fn test_process_event_all_success() {
        let processor = LambdaEventProcessor::new();
        let event_json = create_test_event(3);

        let results = processor
            .process_event(&event_json, |_record| Ok(()))
            .unwrap();

        assert_eq!(results.total_count, 3);
        assert_eq!(results.success_count, 3);
        assert_eq!(results.failed_count, 0);
        assert!(!results.has_failures());
    }

    #[test]
    fn test_process_event_partial_failure() {
        let processor = LambdaEventProcessor::new();
        let event_json = create_test_event(3);

        let results = processor
            .process_event(&event_json, |record| {
                if record.message_id == "msg-1" {
                    Err("Processing error".to_string())
                } else {
                    Ok(())
                }
            })
            .unwrap();

        assert_eq!(results.total_count, 3);
        assert_eq!(results.success_count, 2);
        assert_eq!(results.failed_count, 1);
        assert!(results.has_failures());

        let failed_ids = results.failed_message_ids();
        assert_eq!(failed_ids.len(), 1);
        assert_eq!(failed_ids[0], "msg-1");
    }

    #[test]
    fn test_process_event_all_failure() {
        let processor = LambdaEventProcessor::new();
        let event_json = create_test_event(2);

        let results = processor
            .process_event(&event_json, |_record| Err("All fail".to_string()))
            .unwrap();

        assert_eq!(results.total_count, 2);
        assert_eq!(results.success_count, 0);
        assert_eq!(results.failed_count, 2);
    }

    #[test]
    fn test_batch_item_failures_response() {
        let processor = LambdaEventProcessor::new();
        let event_json = create_test_event(3);

        let results = processor
            .process_event(&event_json, |record| {
                if record.message_id == "msg-0" {
                    Err("Error".to_string())
                } else {
                    Ok(())
                }
            })
            .unwrap();

        let failures = results.to_batch_item_failures();
        assert_eq!(failures.batch_item_failures.len(), 1);
        assert_eq!(failures.batch_item_failures[0].item_identifier, "msg-0");
    }

    #[test]
    fn test_retry_logic() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;

        let processor = LambdaEventProcessor::new().with_max_retries(2);
        let event_json = create_test_event(1);

        let attempt_count = Arc::new(AtomicUsize::new(0));
        let attempt_count_clone = attempt_count.clone();

        let results = processor
            .process_event(&event_json, |_record| {
                let count = attempt_count_clone.fetch_add(1, Ordering::SeqCst) + 1;
                if count < 3 {
                    Err("Transient error".to_string())
                } else {
                    Ok(())
                }
            })
            .unwrap();

        assert_eq!(results.success_count, 1);
        assert_eq!(attempt_count.load(Ordering::SeqCst), 3); // Initial + 2 retries
    }

    #[test]
    fn test_get_string_attribute() {
        let record = SqsEventRecord {
            message_id: "msg-1".to_string(),
            receipt_handle: "handle-1".to_string(),
            body: "{}".to_string(),
            message_attributes: {
                let mut attrs = HashMap::new();
                attrs.insert(
                    "priority".to_string(),
                    MessageAttribute {
                        data_type: "Number".to_string(),
                        string_value: Some("5".to_string()),
                        binary_value: None,
                    },
                );
                attrs
            },
            md5_of_body: None,
            event_source_arn: None,
            event_source: None,
            aws_region: None,
        };

        let priority = LambdaEventProcessor::get_string_attribute(&record, "priority");
        assert_eq!(priority, Some("5".to_string()));
    }

    #[test]
    fn test_get_number_attribute() {
        let record = SqsEventRecord {
            message_id: "msg-1".to_string(),
            receipt_handle: "handle-1".to_string(),
            body: "{}".to_string(),
            message_attributes: {
                let mut attrs = HashMap::new();
                attrs.insert(
                    "retry_count".to_string(),
                    MessageAttribute {
                        data_type: "Number".to_string(),
                        string_value: Some("3".to_string()),
                        binary_value: None,
                    },
                );
                attrs
            },
            md5_of_body: None,
            event_source_arn: None,
            event_source: None,
            aws_region: None,
        };

        let count = LambdaEventProcessor::get_number_attribute(&record, "retry_count");
        assert_eq!(count, Some(3.0));
    }

    #[test]
    fn test_is_fifo_queue() {
        let mut record = SqsEventRecord {
            message_id: "msg-1".to_string(),
            receipt_handle: "handle-1".to_string(),
            body: "{}".to_string(),
            message_attributes: HashMap::new(),
            md5_of_body: None,
            event_source_arn: Some("arn:aws:sqs:us-east-1:123456789:my-queue.fifo".to_string()),
            event_source: None,
            aws_region: None,
        };

        assert!(LambdaEventProcessor::is_fifo_queue(&record));

        record.event_source_arn = Some("arn:aws:sqs:us-east-1:123456789:my-queue".to_string());
        assert!(!LambdaEventProcessor::is_fifo_queue(&record));
    }

    #[test]
    fn test_extract_queue_name() {
        let record = SqsEventRecord {
            message_id: "msg-1".to_string(),
            receipt_handle: "handle-1".to_string(),
            body: "{}".to_string(),
            message_attributes: HashMap::new(),
            md5_of_body: None,
            event_source_arn: Some("arn:aws:sqs:us-east-1:123456789:my-queue".to_string()),
            event_source: None,
            aws_region: None,
        };

        let queue_name = LambdaEventProcessor::extract_queue_name(&record);
        assert_eq!(queue_name, Some("my-queue".to_string()));
    }

    #[test]
    fn test_create_batch_failure_response() {
        let failed_ids = vec!["msg-1".to_string(), "msg-3".to_string()];
        let response = create_batch_failure_response(&failed_ids);

        assert!(response.contains("msg-1"));
        assert!(response.contains("msg-3"));
        assert!(response.contains("batchItemFailures"));
    }
}
