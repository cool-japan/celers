//! Advanced features example
//!
//! This example demonstrates advanced broker features including:
//! - Message scheduling (delayed delivery)
//! - Consumer groups (load-balanced consumption)
//! - Message replay (debugging and recovery)
//! - Quota management (resource limits)
//!
//! Run with: cargo run --example advanced_features

use celers_kombu::*;
use std::time::Duration;
use uuid::Uuid;

#[tokio::main]
async fn main() -> Result<()> {
    println!("🚀 Advanced Features Example\n");

    // =========================================================================
    // 1. Message Scheduling
    // =========================================================================
    println!("📅 Message Scheduling");
    println!("   Configure delayed message delivery\n");

    // Schedule with delay
    let schedule_delay =
        ScheduleConfig::delay(Duration::from_secs(60)).with_window(Duration::from_secs(10));

    println!("   Delay Schedule:");
    println!("     Delay: {:?}", schedule_delay.delay);
    println!("     Ready now: {}", schedule_delay.is_ready());
    println!("     Window: {:?}", schedule_delay.execution_window);

    // Schedule at specific timestamp
    let target_time = (std::time::SystemTime::now() + Duration::from_secs(300))
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let schedule_at = ScheduleConfig::at(target_time).with_window(Duration::from_secs(5));

    println!("\n   Timestamp Schedule:");
    println!("     Target time: {}", target_time);
    println!("     Scheduled at: {:?}", schedule_at.scheduled_at);
    println!("     Ready now: {}", schedule_at.is_ready());

    // Example: Scheduled message workflow
    println!("\n   ✅ Use Case: Schedule batch processing at night");
    println!("      - Schedule messages for off-peak hours");
    println!("      - Implement delayed notifications");
    println!("      - Build workflow automation with timing");

    // =========================================================================
    // 2. Consumer Groups
    // =========================================================================
    println!("\n📊 Consumer Groups");
    println!("   Load-balanced consumption across multiple consumers\n");

    let consumer_group =
        ConsumerGroupConfig::new("processing-group".to_string(), "consumer-001".to_string())
            .with_max_consumers(10)
            .with_rebalance_timeout(Duration::from_secs(30))
            .with_heartbeat_interval(Duration::from_secs(5));

    println!("   Group Configuration:");
    println!("     Group ID: {}", consumer_group.group_id);
    println!("     Max consumers: {:?}", consumer_group.max_consumers);
    println!(
        "     Rebalance timeout: {:?}",
        consumer_group.rebalance_timeout
    );
    println!(
        "     Heartbeat interval: {:?}",
        consumer_group.heartbeat_interval
    );

    println!("\n   ✅ Use Case: Horizontal scaling");
    println!("      - Multiple workers process messages in parallel");
    println!("      - Automatic load balancing across consumers");
    println!("      - Heartbeat-based membership management");
    println!("      - Automatic rebalancing on consumer join/leave");

    // =========================================================================
    // 3. Message Replay
    // =========================================================================
    println!("\n⏮️  Message Replay");
    println!("   Replay messages for debugging and recovery\n");

    // Replay from duration
    let replay_duration = ReplayConfig::from_duration(Duration::from_secs(3600))
        .with_max_messages(1000)
        .with_speed(2.0);

    println!("   Duration-based Replay:");
    println!("     Start time: {:?}", replay_duration.start_timestamp());
    println!("     Max messages: {:?}", replay_duration.max_messages);
    println!("     Speed: {}x", replay_duration.speed_multiplier);

    // Replay from timestamp
    let start_time_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs()
        - 7200;
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let replay_timestamp = ReplayConfig::from_timestamp(start_time_secs)
        .until(now_secs)
        .with_max_messages(500);

    println!("\n   Timestamp-based Replay:");
    println!("     Start: {}", start_time_secs);
    println!("     Until: {:?}", replay_timestamp.until_timestamp);
    println!("     Max messages: {:?}", replay_timestamp.max_messages);

    // Replay progress tracking
    let progress = ReplayProgress {
        replay_id: "replay-session-123".to_string(),
        messages_replayed: 75,
        total_messages: Some(100),
        current_timestamp: now_secs,
        completed: false,
    };

    println!("\n   Replay Progress:");
    println!("     Session: {}", progress.replay_id);
    println!(
        "     Messages: {}/{:?}",
        progress.messages_replayed, progress.total_messages
    );
    if let Some(pct) = progress.completion_percent() {
        println!("     Completion: {:.1}%", pct);
    }
    println!("     Completed: {}", progress.completed);

    println!("\n   ✅ Use Case: Debugging and recovery");
    println!("      - Replay messages after system failure");
    println!("      - Debug processing issues with historical data");
    println!("      - Test changes with production traffic replay");
    println!("      - Audit message processing history");

    // =========================================================================
    // 4. Quota Management
    // =========================================================================
    println!("\n💰 Quota Management");
    println!("   Resource limits and enforcement\n");

    let quota = QuotaConfig::new()
        .with_max_messages(10000)
        .with_max_bytes(100 * 1024 * 1024) // 100 MB
        .with_max_rate(100.0)
        .with_max_per_consumer(50)
        .with_enforcement(QuotaEnforcement::Throttle);

    println!("   Quota Configuration:");
    println!("     Max messages: {:?}", quota.max_messages);
    println!("     Max bytes: {:?}", quota.max_bytes);
    println!("     Max rate: {:?} msg/sec", quota.max_rate_per_sec);
    println!(
        "     Max per consumer: {:?}",
        quota.max_messages_per_consumer
    );
    println!("     Enforcement: {:?}", quota.enforcement);

    // Quota usage tracking
    let usage = QuotaUsage {
        message_count: 7500,
        bytes_used: 80 * 1024 * 1024, // 80 MB
        current_rate: 85.0,
        exceeded: false,
    };

    println!("\n   Current Usage:");
    println!(
        "     Messages: {}/{} ({:.1}%)",
        usage.message_count,
        quota.max_messages.unwrap(),
        usage.usage_percent(&quota).unwrap_or(0.0)
    );
    println!(
        "     Bytes: {}/{} MB",
        usage.bytes_used / (1024 * 1024),
        quota.max_bytes.unwrap() / (1024 * 1024)
    );
    println!(
        "     Rate: {:.1}/{} msg/sec",
        usage.current_rate,
        quota.max_rate_per_sec.unwrap()
    );

    println!("\n   Quota Status:");
    println!(
        "     Messages exceeded: {}",
        usage.is_message_quota_exceeded(&quota)
    );
    println!(
        "     Bytes exceeded: {}",
        usage.is_bytes_quota_exceeded(&quota)
    );
    println!(
        "     Rate exceeded: {}",
        usage.is_rate_quota_exceeded(&quota)
    );
    println!("     Overall exceeded: {}", usage.exceeded);

    // Enforcement policies
    println!("\n   Enforcement Policies:");
    println!("     Reject: Reject messages when quota exceeded");
    println!("     Throttle: Slow down message processing");
    println!("     Warn: Log warnings but allow messages");

    println!("\n   ✅ Use Case: Resource management");
    println!("      - Prevent resource exhaustion");
    println!("      - Implement fair usage policies");
    println!("      - Multi-tenant quota enforcement");
    println!("      - Rate limiting per user/organization");

    // =========================================================================
    // 5. Practical Workflow Example
    // =========================================================================
    println!("\n🔄 Practical Workflow Example");
    println!("   Combining multiple features\n");

    let task_id = Uuid::new_v4();
    println!("   Scenario: High-priority batch job");
    println!("     Task ID: {}", task_id);
    println!("\n   Workflow:");
    println!("     1. Check quota before scheduling");
    println!("     2. Schedule job for off-peak hours (2 AM)");
    println!("     3. Assign to consumer group for parallel processing");
    println!("     4. Monitor quota usage during processing");
    println!("     5. If failure, replay messages from last checkpoint");
    println!("\n   Benefits:");
    println!("     ✓ Resource-aware scheduling");
    println!("     ✓ Automatic load balancing");
    println!("     ✓ Quota enforcement prevents overload");
    println!("     ✓ Replay capability for recovery");

    println!("\n✨ Advanced features example completed successfully!");
    Ok(())
}
