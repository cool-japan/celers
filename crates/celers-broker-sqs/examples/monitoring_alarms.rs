//! CloudWatch monitoring and alarms for AWS SQS
//!
//! This example demonstrates:
//! - Publishing queue metrics to CloudWatch
//! - Creating CloudWatch alarms for queue monitoring
//! - Setting up alerts for common scenarios
//! - Monitoring queue health and performance
//! - Integration with SNS for notifications
//!
//! # Common Monitoring Scenarios
//!
//! 1. **High Queue Depth** - Too many messages waiting
//! 2. **Old Messages** - Messages stuck in queue (backlog)
//! 3. **High In-Flight Messages** - Too many messages being processed
//! 4. **Empty Queue** - No work available (cost optimization opportunity)
//!
//! # Running this example
//!
//! ```bash
//! export AWS_ACCESS_KEY_ID=your_key_id
//! export AWS_SECRET_ACCESS_KEY=your_secret_key
//! export AWS_REGION=us-east-1
//!
//! # Optional: Create an SNS topic for notifications
//! aws sns create-topic --name celers-sqs-alerts
//!
//! cargo run --example monitoring_alarms
//! ```

use celers_broker_sqs::{AlarmConfig, CloudWatchConfig, SqsBroker};
use celers_kombu::Transport;

#[tokio::main]
#[allow(dead_code)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    println!("=== AWS SQS Monitoring and Alarms Example ===\n");

    // Example 1: CloudWatch metrics
    example_1_cloudwatch_metrics().await?;

    // Example 2: Queue health monitoring
    example_2_health_monitoring().await?;

    // Example 3: Creating alarms
    example_3_create_alarms().await?;

    // Example 4: Common alarm scenarios
    example_4_alarm_scenarios().await?;

    // Example 5: Managing alarms
    example_5_manage_alarms().await?;

    println!("\n=== Monitoring examples completed ===");
    Ok(())
}

/// Example 1: Publish metrics to CloudWatch
async fn example_1_cloudwatch_metrics() -> Result<(), Box<dyn std::error::Error>> {
    println!("--- Example 1: CloudWatch Metrics ---");

    // Configure CloudWatch with custom dimensions
    let cw_config = CloudWatchConfig::new("CeleRS/SQS")
        .with_dimension("Environment", "production")
        .with_dimension("Application", "task-processor")
        .with_dimension("Team", "backend");

    let mut broker = SqsBroker::new("celers-metrics-demo")
        .await?
        .with_cloudwatch(cw_config);

    broker.connect().await?;
    println!("✓ Broker with CloudWatch metrics created");

    // Get queue stats
    let stats = broker.get_queue_stats("celers-metrics-demo").await?;
    println!("\n✓ Current queue statistics:");
    println!(
        "  - Messages available: {}",
        stats.approximate_message_count
    );
    println!(
        "  - Messages in flight: {}",
        stats.approximate_not_visible_count
    );
    println!("  - Messages delayed: {}", stats.approximate_delayed_count);
    if let Some(age) = stats.approximate_age_of_oldest_message {
        println!("  - Oldest message age: {}s", age);
    }

    // Publish metrics to CloudWatch
    broker.publish_metrics("celers-metrics-demo").await?;
    println!("\n✓ Metrics published to CloudWatch:");
    println!("  - Namespace: CeleRS/SQS");
    println!("  - Dimensions: Environment=production, Application=task-processor, Team=backend");
    println!("  - Metrics:");
    println!("    • ApproximateNumberOfMessages");
    println!("    • ApproximateNumberOfMessagesNotVisible");
    println!("    • ApproximateNumberOfMessagesDelayed");
    println!("    • ApproximateAgeOfOldestMessage");

    println!("\nView metrics in AWS Console:");
    println!("  CloudWatch > Metrics > CeleRS/SQS");

    broker.disconnect().await?;

    Ok(())
}

/// Example 2: Monitor queue health
async fn example_2_health_monitoring() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Example 2: Queue Health Monitoring ---");

    let mut broker = SqsBroker::new("celers-health-demo").await?;
    broker.connect().await?;

    // Health check (useful for Kubernetes probes)
    let is_healthy = broker.health_check("celers-health-demo").await?;
    println!(
        "✓ Queue health check: {}",
        if is_healthy {
            "HEALTHY ✓"
        } else {
            "UNHEALTHY ✗"
        }
    );

    // Detailed health assessment
    let stats = broker.get_queue_stats("celers-health-demo").await?;

    println!("\n✓ Health assessment:");

    // Check 1: Queue depth
    if stats.approximate_message_count > 10000 {
        println!(
            "  ⚠ WARNING: High queue depth ({})",
            stats.approximate_message_count
        );
        println!("    Recommendation: Scale up workers");
    } else {
        println!(
            "  ✓ Queue depth normal ({})",
            stats.approximate_message_count
        );
    }

    // Check 2: Message age
    if let Some(age) = stats.approximate_age_of_oldest_message {
        if age > 600 {
            // 10 minutes
            println!("  ⚠ WARNING: Old messages detected ({}s)", age);
            println!("    Recommendation: Check for processing failures");
        } else {
            println!("  ✓ Message age acceptable ({}s)", age);
        }
    } else {
        println!("  ✓ Queue is empty");
    }

    // Check 3: In-flight messages
    if stats.approximate_not_visible_count > stats.approximate_message_count * 2 {
        println!(
            "  ⚠ WARNING: High in-flight messages ({})",
            stats.approximate_not_visible_count
        );
        println!("    Recommendation: Check visibility timeout or worker processing time");
    } else {
        println!(
            "  ✓ In-flight messages normal ({})",
            stats.approximate_not_visible_count
        );
    }

    broker.disconnect().await?;

    Ok(())
}

/// Example 3: Create CloudWatch alarms
async fn example_3_create_alarms() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Example 3: Creating Alarms ---");

    let mut broker = SqsBroker::new("celers-alarms-demo").await?;
    broker.connect().await?;

    // Note: Replace with your SNS topic ARN
    let sns_topic = "arn:aws:sns:us-east-1:123456789012:celers-sqs-alerts";

    // Alarm 1: High queue depth
    let high_queue_alarm =
        AlarmConfig::queue_depth_alarm("CelersHighQueueDepth", "celers-alarms-demo", 1000.0)
            .with_description("Alert when queue has more than 1000 messages")
            .with_alarm_action(sns_topic)
            .with_evaluation_periods(2) // Must breach for 2 consecutive periods
            .with_period(300); // 5-minute periods

    broker.create_alarm(high_queue_alarm).await?;
    println!("✓ Created alarm: CelersHighQueueDepth");
    println!("  - Threshold: > 1000 messages");
    println!("  - Evaluation: 2 periods of 5 minutes");
    println!("  - Action: Send to SNS topic");

    // Alarm 2: Old messages (backlog)
    let old_messages_alarm = AlarmConfig::message_age_alarm(
        "CelersOldMessages",
        "celers-alarms-demo",
        600.0, // 10 minutes
    )
    .with_description("Alert when oldest message is older than 10 minutes")
    .with_alarm_action(sns_topic);

    broker.create_alarm(old_messages_alarm).await?;
    println!("\n✓ Created alarm: CelersOldMessages");
    println!("  - Threshold: > 600 seconds (10 minutes)");
    println!("  - Indicates: Processing backlog or stuck messages");

    // Alarm 3: Custom alarm for in-flight messages
    let inflight_alarm = AlarmConfig::new(
        "CelersHighInflightMessages",
        "ApproximateNumberOfMessagesNotVisible",
        500.0,
    )
    .with_description("Alert when too many messages are being processed")
    .with_dimension("QueueName", "celers-alarms-demo")
    .with_comparison_operator("GreaterThanThreshold")
    .with_statistic("Average")
    .with_period(300)
    .with_evaluation_periods(1)
    .with_alarm_action(sns_topic);

    broker.create_alarm(inflight_alarm).await?;
    println!("\n✓ Created alarm: CelersHighInflightMessages");
    println!("  - Threshold: > 500 in-flight messages");
    println!("  - Indicates: Workers may be overloaded");

    broker.disconnect().await?;

    Ok(())
}

/// Example 4: Common alarm scenarios
async fn example_4_alarm_scenarios() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Example 4: Common Alarm Scenarios ---");

    let mut broker = SqsBroker::new("celers-scenarios").await?;
    broker.connect().await?;

    let sns_topic = "arn:aws:sns:us-east-1:123456789012:celers-sqs-alerts";

    println!("Setting up comprehensive monitoring...\n");

    // Scenario 1: Production critical alerts
    println!("1. Production Critical Alerts:");

    let critical_depth =
        AlarmConfig::queue_depth_alarm("ProdCriticalDepth", "celers-scenarios", 5000.0)
            .with_alarm_action(sns_topic)
            .with_evaluation_periods(1) // Immediate alert
            .with_period(60); // 1 minute

    broker.create_alarm(critical_depth).await?;
    println!("  ✓ Critical queue depth (> 5000 messages, 1 min check)");

    // Scenario 2: Processing stall detection
    println!("\n2. Processing Stall Detection:");

    let stall_alarm = AlarmConfig::message_age_alarm("ProcessingStall", "celers-scenarios", 1800.0)
        .with_alarm_action(sns_topic)
        .with_evaluation_periods(3) // 15 minutes total
        .with_period(300);

    broker.create_alarm(stall_alarm).await?;
    println!("  ✓ Messages older than 30 minutes (3 × 5 min checks)");

    // Scenario 3: Cost optimization - empty queue
    println!("\n3. Cost Optimization Alert:");

    let empty_queue = AlarmConfig::new("QueueEmpty", "ApproximateNumberOfMessages", 0.0)
        .with_description("Queue has been empty - consider scaling down")
        .with_dimension("QueueName", "celers-scenarios")
        .with_comparison_operator("LessThanOrEqualToThreshold")
        .with_statistic("Maximum")
        .with_period(3600) // 1 hour
        .with_evaluation_periods(2) // 2 hours total
        .with_treat_missing_data("notBreaching");

    broker.create_alarm(empty_queue).await?;
    println!("  ✓ Queue empty for 2+ hours (scale down opportunity)");

    // Scenario 4: Worker overload
    println!("\n4. Worker Overload Detection:");

    let overload = AlarmConfig::new(
        "WorkerOverload",
        "ApproximateNumberOfMessagesNotVisible",
        1000.0,
    )
    .with_description("Too many messages in flight - workers struggling")
    .with_dimension("QueueName", "celers-scenarios")
    .with_comparison_operator("GreaterThanThreshold")
    .with_statistic("Average")
    .with_period(300)
    .with_evaluation_periods(2);

    broker.create_alarm(overload).await?;
    println!("  ✓ High in-flight messages (workers may be slow)");

    broker.disconnect().await?;

    Ok(())
}

/// Example 5: Managing alarms
async fn example_5_manage_alarms() -> Result<(), Box<dyn std::error::Error>> {
    println!("\n--- Example 5: Managing Alarms ---");

    let mut broker = SqsBroker::new("celers-manage-alarms").await?;
    broker.connect().await?;

    // List all alarms for the queue
    let alarms = broker.list_alarms("celers-manage-alarms").await?;
    println!("✓ Current alarms: {}", alarms.len());
    for alarm in &alarms {
        println!("  - {}", alarm);
    }

    // Delete an alarm
    if !alarms.is_empty() {
        let alarm_to_delete = &alarms[0];
        broker.delete_alarm(alarm_to_delete).await?;
        println!("\n✓ Deleted alarm: {}", alarm_to_delete);
    }

    // Verify deletion
    let remaining = broker.list_alarms("celers-manage-alarms").await?;
    println!("✓ Remaining alarms: {}", remaining.len());

    println!("\nBest Practices:");
    println!("  1. Use descriptive alarm names (e.g., Prod-QueueDepth-Critical)");
    println!("  2. Set appropriate evaluation periods (avoid false alarms)");
    println!("  3. Configure SNS topics for team notifications");
    println!("  4. Monitor alarm state in CloudWatch console");
    println!("  5. Test alarms by simulating conditions");
    println!("  6. Document alarm thresholds in runbooks");

    broker.disconnect().await?;

    Ok(())
}
