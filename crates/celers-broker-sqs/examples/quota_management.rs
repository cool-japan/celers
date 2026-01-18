// Copyright (c) 2026 COOLJAPAN OU (Team KitaSan)
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Quota and Budget Management Example
//!
//! Demonstrates how to use the quota management utilities to control
//! API usage and costs for AWS SQS operations.
//!
//! This example shows:
//! - Setting up rate limits (per second/minute/hour/day)
//! - Configuring daily and monthly budgets
//! - Tracking quota usage in real-time
//! - Handling quota exceeded errors
//! - Alert thresholds for proactive monitoring

use celers_broker_sqs::quota_manager::{QuotaConfig, QuotaError, QuotaManager};

fn main() {
    println!("=== Quota and Budget Management Example ===\n");

    // Example 1: Basic Rate Limiting
    example_basic_rate_limiting();

    // Example 2: Budget Limits
    example_budget_limits();

    // Example 3: Multi-Level Quotas
    example_multi_level_quotas();

    // Example 4: Alert Thresholds
    example_alert_thresholds();

    // Example 5: Production Configuration
    example_production_config();
}

fn example_basic_rate_limiting() {
    println!("--- Example 1: Basic Rate Limiting ---");

    // Configure rate limit: 10 requests per second
    let config = QuotaConfig::new().with_max_requests_per_second(10);

    let mut quota_mgr = QuotaManager::new(config);

    println!("Rate limit: 10 requests/second");
    println!("Attempting to make 12 requests...\n");

    let mut successful = 0;
    let mut rejected = 0;

    for i in 1..=12 {
        match quota_mgr.can_make_request(1) {
            Ok(()) => {
                quota_mgr.record_request(1, 0.0004); // $0.0004 per request
                successful += 1;
                println!("  Request {}: ✓ Allowed", i);
            }
            Err(QuotaError::RateLimitExceeded(msg)) => {
                rejected += 1;
                println!("  Request {}: ✗ Rejected - {}", i, msg);
            }
            _ => {}
        }
    }

    println!("\nResults:");
    println!("  Successful: {}", successful);
    println!("  Rejected: {}", rejected);

    let status = quota_mgr.status();
    println!(
        "  Current rate: {} requests/second",
        status.requests_last_second
    );

    println!();
}

fn example_budget_limits() {
    println!("--- Example 2: Budget Limits ---");

    // Configure daily budget: $1.00
    let config = QuotaConfig::new().with_daily_budget_usd(1.0);

    let mut quota_mgr = QuotaManager::new(config);

    println!("Daily budget: $1.00");
    println!("Cost per operation: $0.30\n");

    for i in 1..=5 {
        let cost = 0.30;

        match quota_mgr.can_incur_cost(cost) {
            Ok(()) => {
                quota_mgr.record_request(1, cost);
                let status = quota_mgr.status();
                println!(
                    "  Operation {}: ✓ Allowed (Total spent: ${:.2})",
                    i, status.cost_today_usd
                );
            }
            Err(QuotaError::BudgetExceeded(msg)) => {
                let status = quota_mgr.status();
                println!(
                    "  Operation {}: ✗ Rejected - {} (Total spent: ${:.2})",
                    i, msg, status.cost_today_usd
                );
                break;
            }
            _ => {}
        }
    }

    println!();
}

fn example_multi_level_quotas() {
    println!("--- Example 3: Multi-Level Quotas ---");

    // Configure multiple quota levels
    let config = QuotaConfig::new()
        .with_max_requests_per_second(5)
        .with_max_requests_per_minute(200)
        .with_max_requests_per_hour(10_000)
        .with_max_requests_per_day(200_000)
        .with_daily_budget_usd(50.0)
        .with_monthly_budget_usd(1500.0);

    let mut quota_mgr = QuotaManager::new(config);

    println!("Quota configuration:");
    println!("  Per second: 5 requests");
    println!("  Per minute: 200 requests");
    println!("  Per hour: 10,000 requests");
    println!("  Per day: 200,000 requests");
    println!("  Daily budget: $50.00");
    println!("  Monthly budget: $1,500.00");

    // Simulate burst of requests
    println!("\nSimulating burst traffic...");
    let mut successful = 0;

    for i in 1..=10 {
        match quota_mgr.can_make_request(1) {
            Ok(()) => {
                quota_mgr.record_request(1, 0.0004);
                successful += 1;
            }
            Err(QuotaError::RateLimitExceeded(msg)) => {
                println!("  Request {} rejected: {}", i, msg);
                break;
            }
            _ => {}
        }
    }

    let status = quota_mgr.status();
    println!("\nStatus after burst:");
    println!("  Successful requests: {}", successful);
    println!("  Last second: {} requests", status.requests_last_second);
    println!("  Last minute: {} requests", status.requests_last_minute);
    println!("  Last hour: {} requests", status.requests_last_hour);
    println!("  Today: {} requests", status.requests_today);
    println!("  Cost today: ${:.4}", status.cost_today_usd);

    println!();
}

fn example_alert_thresholds() {
    println!("--- Example 4: Alert Thresholds ---");

    // Configure with alert threshold at 80%
    let config = QuotaConfig::new()
        .with_max_requests_per_day(100)
        .with_alert_threshold(0.8); // Alert at 80%

    let mut quota_mgr = QuotaManager::new(config);

    println!("Daily quota: 100 requests");
    println!("Alert threshold: 80% (80 requests)");
    println!();

    // Simulate gradual usage
    let checkpoints: Vec<u64> = vec![50, 75, 80, 85, 90, 95];

    for target in checkpoints {
        // Record requests to reach target
        let current = quota_mgr.status().requests_today;
        let to_add = target.saturating_sub(current);
        if to_add > 0 {
            quota_mgr.record_request(to_add, to_add as f64 * 0.0004);
        }

        let status = quota_mgr.status();
        let usage_pct = (status.requests_today as f64 / 100.0) * 100.0;

        print!("  Usage: {}/100 ({:.0}%)", status.requests_today, usage_pct);

        if status.alert_triggered {
            println!(" - ⚠️  ALERT: {}", status.alert_message.unwrap());
        } else {
            println!(" - ✓ Normal");
        }
    }

    println!();
}

fn example_production_config() {
    println!("--- Example 5: Production Configuration ---");

    // Realistic production configuration
    let config = QuotaConfig::new()
        // Rate limits
        .with_max_requests_per_second(100) // Handle burst traffic
        .with_max_requests_per_minute(5_000) // Smooth out spikes
        .with_max_requests_per_hour(250_000) // Hourly cap
        .with_max_requests_per_day(5_000_000) // Daily cap
        // Budget limits
        .with_daily_budget_usd(100.0) // $100/day
        .with_monthly_budget_usd(3000.0) // $3000/month
        // Alert threshold
        .with_alert_threshold(0.85); // Alert at 85%

    let mut quota_mgr = QuotaManager::new(config);

    println!("Production quota configuration:");
    println!("  Rate Limits:");
    println!("    - Per second: 100 requests");
    println!("    - Per minute: 5,000 requests");
    println!("    - Per hour: 250,000 requests");
    println!("    - Per day: 5,000,000 requests");
    println!("  Budget Limits:");
    println!("    - Daily: $100.00");
    println!("    - Monthly: $3,000.00");
    println!("  Alert Threshold: 85%");

    // Simulate production load
    println!("\nSimulating production load...");

    // Batch 1: Normal load
    for _ in 0..50 {
        if quota_mgr.can_make_request(1).is_ok() {
            quota_mgr.record_request(1, 0.0004);
        }
    }

    let status = quota_mgr.status();
    println!("\nAfter 50 requests:");
    println!("  Last second: {} requests", status.requests_last_second);
    println!("  Today: {} requests", status.requests_today);
    println!("  Cost today: ${:.4}", status.cost_today_usd);
    println!(
        "  Alert: {}",
        if status.alert_triggered { "YES" } else { "No" }
    );

    // Batch 2: Heavy load
    std::thread::sleep(std::time::Duration::from_millis(1100)); // Wait for new second

    let mut batch_successful = 0;
    for _ in 0..150 {
        if quota_mgr.can_make_request(1).is_ok() {
            quota_mgr.record_request(1, 0.0004);
            batch_successful += 1;
        } else {
            break;
        }
    }

    let status = quota_mgr.status();
    println!("\nAfter heavy load (150 requests attempted):");
    println!("  Successful: {} requests", batch_successful);
    println!("  Last second: {} requests", status.requests_last_second);
    println!("  Total today: {} requests", status.requests_today);
    println!("  Cost today: ${:.4}", status.cost_today_usd);

    // Cost analysis
    println!("\n--- Cost Analysis ---");
    let projected_monthly = status.cost_today_usd * 30.0;
    let budget_usage = (status.cost_today_usd / 100.0) * 100.0;
    println!("  Projected monthly cost: ${:.2}", projected_monthly);
    println!("  Daily budget usage: {:.2}%", budget_usage);
    println!(
        "  Monthly budget usage: {:.2}%",
        (status.cost_this_month_usd / 3000.0) * 100.0
    );

    // Recommendations
    println!("\n--- Recommendations ---");
    if projected_monthly > 3000.0 {
        println!("  ⚠️  Projected monthly cost exceeds budget!");
        println!("  Consider:");
        println!("    - Enabling batch operations (10x cost reduction)");
        println!("    - Using long polling to reduce empty receives");
        println!("    - Implementing adaptive polling strategies");
    } else if status.requests_last_second > 80 {
        println!("  ℹ️  High request rate detected");
        println!("  Consider:");
        println!("    - Batch operations to reduce API calls");
        println!("    - Connection pooling for efficiency");
    } else {
        println!("  ✓ Usage is within normal parameters");
    }

    println!();
}
