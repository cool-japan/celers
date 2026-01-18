//! AWS Lambda SQS Handler Example
//!
//! This example demonstrates how to use the lambda_helpers module to process
//! SQS events in an AWS Lambda function with proper error handling and
//! partial batch response support.
//!
//! Run with:
//! ```bash
//! cargo run --example lambda_sqs_handler
//! ```

use celers_broker_sqs::lambda_helpers::{
    create_batch_failure_response, LambdaEventProcessor, SqsEventRecord,
};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== AWS Lambda SQS Handler Example ===\n");

    // Simulate a Lambda SQS event
    let event_json = create_sample_event();

    println!(
        "Processing Lambda SQS event with {} records\n",
        count_records(&event_json)
    );

    // Create processor with retry configuration
    let processor = LambdaEventProcessor::new()
        .with_max_retries(2) // Retry up to 2 times for transient errors
        .with_detailed_logging(true);

    // Process the event
    let results = processor.process_event(&event_json, process_message)?;

    // Display results
    println!("\n=== Processing Results ===");
    println!("Total messages: {}", results.total_count);
    println!("Successful: {}", results.success_count);
    println!("Failed: {}", results.failed_count);

    if results.has_failures() {
        println!("\nFailed message IDs:");
        for failed_id in results.failed_message_ids() {
            println!("  - {}", failed_id);
        }

        // Generate batch failure response for Lambda
        let failure_response = results.to_batch_item_failures();
        let response_json = serde_json::to_string_pretty(&failure_response)?;

        println!("\nPartial Batch Response (to be returned to Lambda):");
        println!("{}", response_json);

        println!("\nNote: Failed messages will be retried by SQS automatically");
    } else {
        println!("\n✓ All messages processed successfully!");
    }

    // Demonstrate helper functions
    println!("\n=== Helper Functions Demo ===\n");
    demonstrate_helper_functions();

    Ok(())
}

/// Process a single SQS message
fn process_message(record: &SqsEventRecord) -> Result<(), String> {
    println!("Processing message: {}", record.message_id);

    // Extract message attributes
    if let Some(priority) = LambdaEventProcessor::get_string_attribute(record, "priority") {
        println!("  Priority: {}", priority);
    }

    if let Some(retry_count) = LambdaEventProcessor::get_number_attribute(record, "retry_count") {
        println!("  Retry count: {}", retry_count);
    }

    // Check if from FIFO queue
    if LambdaEventProcessor::is_fifo_queue(record) {
        println!("  Message from FIFO queue (ordered processing)");
    }

    // Extract queue name
    if let Some(queue_name) = LambdaEventProcessor::extract_queue_name(record) {
        println!("  Queue: {}", queue_name);
    }

    // Parse and process the message body
    let body: serde_json::Value = serde_json::from_str(&record.body)
        .map_err(|e| format!("Failed to parse message body: {}", e))?;

    println!("  Body: {}", body);

    // Simulate processing logic
    if let Some(task) = body.get("task").and_then(|v| v.as_str()) {
        match task {
            "fail" => {
                return Err("Simulated processing failure".to_string());
            }
            "slow" => {
                std::thread::sleep(std::time::Duration::from_millis(100));
                println!("  ⏱️  Slow task completed");
            }
            _ => {
                println!("  ✓ Task '{}' processed successfully", task);
            }
        }
    }

    Ok(())
}

/// Create a sample SQS event for demonstration
fn create_sample_event() -> String {
    r#"{
  "Records": [
    {
      "messageId": "msg-001",
      "receiptHandle": "handle-001",
      "body": "{\"task\": \"process_order\", \"order_id\": 12345}",
      "messageAttributes": {
        "priority": {
          "dataType": "Number",
          "stringValue": "5"
        }
      },
      "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:my-queue"
    },
    {
      "messageId": "msg-002",
      "receiptHandle": "handle-002",
      "body": "{\"task\": \"send_email\", \"email\": \"user@example.com\"}",
      "messageAttributes": {},
      "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:my-queue"
    },
    {
      "messageId": "msg-003",
      "receiptHandle": "handle-003",
      "body": "{\"task\": \"fail\"}",
      "messageAttributes": {
        "retry_count": {
          "dataType": "Number",
          "stringValue": "2"
        }
      },
      "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:my-queue"
    },
    {
      "messageId": "msg-004",
      "receiptHandle": "handle-004",
      "body": "{\"task\": \"update_inventory\", \"item_id\": 789}",
      "messageAttributes": {},
      "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:my-queue.fifo"
    },
    {
      "messageId": "msg-005",
      "receiptHandle": "handle-005",
      "body": "{\"task\": \"slow\"}",
      "messageAttributes": {},
      "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:my-queue"
    }
  ]
}"#
    .to_string()
}

/// Count records in event
fn count_records(event_json: &str) -> usize {
    match serde_json::from_str::<serde_json::Value>(event_json) {
        Ok(v) => v
            .get("Records")
            .and_then(|r| r.as_array())
            .map(|a| a.len())
            .unwrap_or(0),
        Err(_) => 0,
    }
}

/// Demonstrate helper functions
fn demonstrate_helper_functions() {
    println!("1. Creating batch failure response:");
    let failed_ids = vec!["msg-123".to_string(), "msg-456".to_string()];
    let response = create_batch_failure_response(&failed_ids);
    println!("   {}\n", response);

    println!("2. Message attribute extraction:");
    println!("   Use LambdaEventProcessor::get_string_attribute()");
    println!("   Use LambdaEventProcessor::get_number_attribute()\n");

    println!("3. Queue type detection:");
    println!("   Use LambdaEventProcessor::is_fifo_queue()");
    println!("   Returns true for queues ending in .fifo\n");

    println!("4. Queue name extraction:");
    println!("   Use LambdaEventProcessor::extract_queue_name()");
    println!("   Extracts queue name from ARN\n");

    println!("Best Practices:");
    println!("• Always return partial batch response for failed messages");
    println!("• Use retry logic for transient errors");
    println!("• Extract message attributes for routing and priority");
    println!("• Handle FIFO queues differently (ordered processing)");
    println!("• Set appropriate Lambda timeout (> max processing time)");
    println!("• Monitor DLQ for messages that consistently fail");
}
