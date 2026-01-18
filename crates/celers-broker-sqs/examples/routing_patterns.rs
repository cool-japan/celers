// Copyright (c) 2026 COOLJAPAN OU (Team KitaSan)
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Multi-Queue Routing Patterns Example
//!
//! Demonstrates various routing patterns for distributing messages
//! across multiple SQS queues.
//!
//! This example shows:
//! - Priority-based routing
//! - Task pattern routing
//! - Metadata-based routing
//! - Priority queue simulation
//! - Complex routing rules

use celers_broker_sqs::router::{MessageRouter, PriorityQueueRouter, RoutingRule};
use serde_json::json;

fn main() {
    println!("=== Multi-Queue Routing Patterns Example ===\n");

    // Example 1: Priority-Based Routing
    example_priority_routing();

    // Example 2: Task Pattern Routing
    example_task_pattern_routing();

    // Example 3: Metadata-Based Routing
    example_metadata_routing();

    // Example 4: Priority Queue Simulation
    example_priority_queue_simulation();

    // Example 5: Complex Multi-Rule Routing
    example_complex_routing();

    // Example 6: Dynamic Routing Configuration
    example_dynamic_routing();
}

fn example_priority_routing() {
    println!("--- Example 1: Priority-Based Routing ---");

    let mut router = MessageRouter::new();

    // Configure priority-based routing
    router.add_rule(RoutingRule::priority_based("express-queue", 8, 10).with_priority(3));
    router.add_rule(RoutingRule::priority_based("standard-queue", 4, 7).with_priority(2));
    router.add_rule(RoutingRule::priority_based("slow-queue", 0, 3).with_priority(1));
    router.set_default_queue("default-queue");

    println!("Routing rules:");
    println!("  Priority 8-10 → express-queue");
    println!("  Priority 4-7  → standard-queue");
    println!("  Priority 0-3  → slow-queue");
    println!("  Default       → default-queue\n");

    // Test messages with different priorities
    let test_cases = vec![
        (10, "Urgent task"),
        (7, "Important task"),
        (3, "Normal task"),
        (0, "Low priority task"),
    ];

    for (priority, description) in test_cases {
        let metadata = json!({"priority": priority});
        let queue = router.route(&metadata);
        println!("  Priority {}: {} → {}", priority, description, queue);
    }

    println!();
}

fn example_task_pattern_routing() {
    println!("--- Example 2: Task Pattern Routing ---");

    let mut router = MessageRouter::new();

    // Configure task pattern routing
    router.add_rule(RoutingRule::task_pattern("email-queue", "email.*"));
    router.add_rule(RoutingRule::task_pattern("sms-queue", "sms.*"));
    router.add_rule(RoutingRule::task_pattern("payment-queue", "payment.*"));
    router.add_rule(RoutingRule::task_pattern("analytics-queue", "analytics.*"));
    router.set_default_queue("general-queue");

    println!("Routing rules (glob patterns):");
    println!("  email.*     → email-queue");
    println!("  sms.*       → sms-queue");
    println!("  payment.*   → payment-queue");
    println!("  analytics.* → analytics-queue");
    println!("  *           → general-queue\n");

    // Test various task names
    let tasks = vec![
        "email.send",
        "email.verify",
        "sms.send",
        "payment.process",
        "payment.refund",
        "analytics.track",
        "user.register",
    ];

    for task in tasks {
        let queue = router.route_with_task_name(Some(task), &json!({}));
        println!("  Task '{}' → {}", task, queue);
    }

    println!();
}

fn example_metadata_routing() {
    println!("--- Example 3: Metadata-Based Routing ---");

    let mut router = MessageRouter::new();

    // Configure metadata-based routing
    router
        .add_rule(RoutingRule::metadata_match("premium-queue", "tier", "premium").with_priority(3));

    router.add_rule(
        RoutingRule::metadata_match("business-queue", "tier", "business").with_priority(2),
    );

    router
        .add_rule(RoutingRule::metadata_match("urgent-queue", "urgency", "high").with_priority(10)); // Higher priority than tier

    router.set_default_queue("standard-queue");

    println!("Routing rules:");
    println!("  urgency=high → urgent-queue (priority 10)");
    println!("  tier=premium → premium-queue (priority 3)");
    println!("  tier=business → business-queue (priority 2)");
    println!("  default → standard-queue\n");

    // Test various metadata combinations
    let test_cases = vec![
        (json!({"tier": "premium"}), "Premium customer"),
        (json!({"tier": "business"}), "Business customer"),
        (json!({"tier": "standard"}), "Standard customer"),
        (
            json!({"tier": "premium", "urgency": "high"}),
            "Urgent premium request",
        ),
        (json!({"urgency": "high"}), "Urgent request (any tier)"),
    ];

    for (metadata, description) in test_cases {
        let queue = router.route(&metadata);
        println!("  {} → {}", description, queue);
    }

    println!();
}

fn example_priority_queue_simulation() {
    println!("--- Example 4: Priority Queue Simulation ---");
    println!("Using multiple SQS queues to simulate priority queues\n");

    // Create priority queue router with 3 levels
    let mut router = PriorityQueueRouter::new("tasks");

    println!("Queue configuration:");
    println!("  High priority (8-10)   → tasks-high");
    println!("  Medium priority (4-7)  → tasks-medium");
    println!("  Low priority (0-3)     → tasks-low");
    println!("  Default (no priority)  → tasks-medium\n");

    // Simulate tasks with different priorities
    let tasks = vec![
        (Some(10), "Critical system alert", "tasks-high"),
        (Some(9), "VIP customer request", "tasks-high"),
        (Some(6), "Regular processing", "tasks-medium"),
        (Some(2), "Batch job", "tasks-low"),
        (None, "No priority specified", "tasks-medium"),
    ];

    println!("Task routing:");
    for (priority, description, expected_queue) in tasks {
        let metadata = if let Some(p) = priority {
            json!({"priority": p})
        } else {
            json!({})
        };

        let queue = router.route(&metadata);
        let priority_str = priority
            .map(|p| p.to_string())
            .unwrap_or_else(|| "none".to_string());

        println!(
            "  [P: {:>4}] {} → {} {}",
            priority_str,
            description,
            queue,
            if queue == expected_queue {
                "✓"
            } else {
                "✗"
            }
        );
    }

    println!("\nWorker configuration suggestion:");
    println!("  High priority queue: 5 workers (always processing)");
    println!("  Medium priority queue: 10 workers (main workload)");
    println!("  Low priority queue: 2 workers (background processing)");

    println!();
}

fn example_complex_routing() {
    println!("--- Example 5: Complex Multi-Rule Routing ---");

    let mut router = MessageRouter::new();

    // Rule 1: VIP customers always go to premium queue (highest priority)
    router.add_rule(
        RoutingRule::metadata_match("vip-queue", "customer_type", "vip").with_priority(100),
    );

    // Rule 2: Urgent tasks go to express queue
    router.add_rule(
        RoutingRule::metadata_match("express-queue", "urgency", "high").with_priority(90),
    );

    // Rule 3: Large orders go to special processing queue
    router.add_rule(
        RoutingRule::metadata_match("large-order-queue", "order_size", "large").with_priority(80),
    );

    // Rule 4: International orders
    router.add_rule(
        RoutingRule::metadata_match("international-queue", "region", "international")
            .with_priority(70),
    );

    // Rule 5: Priority-based routing for everything else
    router.add_rule(RoutingRule::priority_based("high-queue", 7, 10).with_priority(60));
    router.add_rule(RoutingRule::priority_based("medium-queue", 4, 6).with_priority(50));
    router.add_rule(RoutingRule::priority_based("low-queue", 0, 3).with_priority(40));

    router.set_default_queue("default-queue");

    println!("Complex routing rules (evaluated in priority order):");
    println!("  1. VIP customers → vip-queue");
    println!("  2. Urgent tasks → express-queue");
    println!("  3. Large orders → large-order-queue");
    println!("  4. International → international-queue");
    println!("  5. High priority (7-10) → high-queue");
    println!("  6. Medium priority (4-6) → medium-queue");
    println!("  7. Low priority (0-3) → low-queue");
    println!("  8. Default → default-queue\n");

    // Test complex scenarios
    let scenarios = vec![
        (
            json!({"customer_type": "vip", "urgency": "high", "priority": 10}),
            "VIP urgent high-priority order",
        ),
        (
            json!({"urgency": "high", "order_size": "large"}),
            "Urgent large order",
        ),
        (
            json!({"order_size": "large", "region": "international"}),
            "Large international order",
        ),
        (json!({"priority": 8}), "High priority order"),
        (json!({"priority": 5}), "Medium priority order"),
        (json!({"region": "domestic"}), "Regular domestic order"),
    ];

    for (metadata, description) in scenarios {
        let queue = router.route(&metadata);
        println!("  {} → {}", description, queue);
    }

    let stats = router.stats();
    println!("\nRouter statistics:");
    println!("  Total rules: {}", stats.total_rules);
    println!("  Enabled rules: {}", stats.enabled_rules);
    println!("  Default queue: {}", stats.default_queue);

    println!();
}

fn example_dynamic_routing() {
    println!("--- Example 6: Dynamic Routing Configuration ---");

    let mut router = MessageRouter::new();

    // Initial configuration
    router.add_rule(RoutingRule::priority_based("high-queue", 8, 10));
    router.add_rule(RoutingRule::priority_based("low-queue", 0, 7));

    println!("Initial configuration:");
    let stats = router.stats();
    println!("  Active rules: {}", stats.enabled_rules);

    // Test with high priority
    let queue = router.route(&json!({"priority": 9}));
    println!("  Priority 9 → {}\n", queue);

    // Temporarily disable high-queue during maintenance
    println!("Maintenance mode: Disabling high-queue rule...");
    router.set_rule_enabled("priority-8-10", false);

    let stats = router.stats();
    println!("  Active rules: {}", stats.enabled_rules);

    // Same test now goes to low-queue
    let queue = router.route(&json!({"priority": 9}));
    println!("  Priority 9 → {} (redirected)\n", queue);

    // Re-enable after maintenance
    println!("Maintenance complete: Re-enabling high-queue...");
    router.set_rule_enabled("priority-8-10", true);

    let stats = router.stats();
    println!("  Active rules: {}", stats.enabled_rules);

    let queue = router.route(&json!({"priority": 9}));
    println!("  Priority 9 → {} (restored)", queue);

    println!();
}
