// Copyright (c) 2026 COOLJAPAN OU (Team KitaSan)
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Quota and budget management for SQS API usage
//!
//! This module provides utilities for tracking and enforcing quotas
//! on SQS API calls and costs to prevent runaway expenses.
//!
//! # Features
//!
//! - API call quotas (requests per second/minute/hour/day)
//! - Cost budgets (daily/monthly limits)
//! - Real-time quota tracking and enforcement
//! - Automatic quota reset at intervals
//! - Alerts when approaching limits
//!
//! # Example
//!
//! ```rust
//! use celers_broker_sqs::quota_manager::{QuotaManager, QuotaConfig};
//! use std::time::Duration;
//!
//! // Create a quota manager with limits
//! let config = QuotaConfig::new()
//!     .with_max_requests_per_second(100)
//!     .with_max_requests_per_hour(10_000)
//!     .with_daily_budget_usd(10.0);
//!
//! let mut quota_mgr = QuotaManager::new(config);
//!
//! // Check if we can make a request
//! if quota_mgr.can_make_request(1).is_ok() {
//!     // Make the request
//!     quota_mgr.record_request(1, 0.0004); // 1 request, $0.0004 cost
//! }
//!
//! // Check quota status
//! let status = quota_mgr.status();
//! println!("Requests today: {}", status.requests_today);
//! println!("Cost today: ${:.4}", status.cost_today_usd);
//! ```

use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};
use thiserror::Error;

/// Quota manager errors
#[derive(Debug, Error, Clone, PartialEq)]
pub enum QuotaError {
    /// Rate limit exceeded
    #[error("Rate limit exceeded: {0}")]
    RateLimitExceeded(String),

    /// Budget exceeded
    #[error("Budget exceeded: {0}")]
    BudgetExceeded(String),

    /// Quota configuration error
    #[error("Invalid quota configuration: {0}")]
    InvalidConfig(String),
}

/// Quota configuration
#[derive(Debug, Clone)]
pub struct QuotaConfig {
    /// Maximum requests per second (0 = unlimited)
    pub max_requests_per_second: u64,
    /// Maximum requests per minute (0 = unlimited)
    pub max_requests_per_minute: u64,
    /// Maximum requests per hour (0 = unlimited)
    pub max_requests_per_hour: u64,
    /// Maximum requests per day (0 = unlimited)
    pub max_requests_per_day: u64,
    /// Daily budget in USD (0.0 = unlimited)
    pub daily_budget_usd: f64,
    /// Monthly budget in USD (0.0 = unlimited)
    pub monthly_budget_usd: f64,
    /// Alert threshold (0.0-1.0, e.g., 0.8 = 80% of quota)
    pub alert_threshold: f64,
}

impl Default for QuotaConfig {
    fn default() -> Self {
        Self {
            max_requests_per_second: 0,
            max_requests_per_minute: 0,
            max_requests_per_hour: 0,
            max_requests_per_day: 0,
            daily_budget_usd: 0.0,
            monthly_budget_usd: 0.0,
            alert_threshold: 0.8,
        }
    }
}

impl QuotaConfig {
    /// Create a new quota configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set maximum requests per second
    pub fn with_max_requests_per_second(mut self, limit: u64) -> Self {
        self.max_requests_per_second = limit;
        self
    }

    /// Set maximum requests per minute
    pub fn with_max_requests_per_minute(mut self, limit: u64) -> Self {
        self.max_requests_per_minute = limit;
        self
    }

    /// Set maximum requests per hour
    pub fn with_max_requests_per_hour(mut self, limit: u64) -> Self {
        self.max_requests_per_hour = limit;
        self
    }

    /// Set maximum requests per day
    pub fn with_max_requests_per_day(mut self, limit: u64) -> Self {
        self.max_requests_per_day = limit;
        self
    }

    /// Set daily budget in USD
    pub fn with_daily_budget_usd(mut self, budget: f64) -> Self {
        self.daily_budget_usd = budget.max(0.0);
        self
    }

    /// Set monthly budget in USD
    pub fn with_monthly_budget_usd(mut self, budget: f64) -> Self {
        self.monthly_budget_usd = budget.max(0.0);
        self
    }

    /// Set alert threshold (0.0-1.0)
    pub fn with_alert_threshold(mut self, threshold: f64) -> Self {
        self.alert_threshold = threshold.clamp(0.0, 1.0);
        self
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), QuotaError> {
        if self.alert_threshold < 0.0 || self.alert_threshold > 1.0 {
            return Err(QuotaError::InvalidConfig(
                "alert_threshold must be between 0.0 and 1.0".to_string(),
            ));
        }
        Ok(())
    }
}

/// Request record for sliding window tracking
#[derive(Debug, Clone)]
struct RequestRecord {
    timestamp: u64,
    count: u64,
    #[allow(dead_code)]
    cost_usd: f64,
}

/// Quota status information
#[derive(Debug, Clone)]
pub struct QuotaStatus {
    /// Requests in the last second
    pub requests_last_second: u64,
    /// Requests in the last minute
    pub requests_last_minute: u64,
    /// Requests in the last hour
    pub requests_last_hour: u64,
    /// Requests today
    pub requests_today: u64,
    /// Cost today in USD
    pub cost_today_usd: f64,
    /// Cost this month in USD
    pub cost_this_month_usd: f64,
    /// Whether alert threshold is exceeded
    pub alert_triggered: bool,
    /// Alert message (if any)
    pub alert_message: Option<String>,
}

/// Quota manager for tracking and enforcing API usage limits
#[derive(Debug)]
pub struct QuotaManager {
    config: QuotaConfig,
    requests: VecDeque<RequestRecord>,
    day_start: u64,
    month_start: u64,
    daily_requests: u64,
    daily_cost_usd: f64,
    monthly_cost_usd: f64,
}

impl QuotaManager {
    /// Create a new quota manager
    pub fn new(config: QuotaConfig) -> Self {
        let now = Self::current_timestamp();
        let day_start = Self::day_start_timestamp(now);
        let month_start = Self::month_start_timestamp(now);

        Self {
            config,
            requests: VecDeque::new(),
            day_start,
            month_start,
            daily_requests: 0,
            daily_cost_usd: 0.0,
            monthly_cost_usd: 0.0,
        }
    }

    /// Get current Unix timestamp in seconds
    fn current_timestamp() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    /// Get start of current day (midnight UTC)
    fn day_start_timestamp(now: u64) -> u64 {
        now - (now % 86400)
    }

    /// Get start of current month (approximate, for simplicity)
    fn month_start_timestamp(now: u64) -> u64 {
        now - (now % (86400 * 30))
    }

    /// Check if a request can be made
    ///
    /// Returns Ok(()) if request is allowed, Err(QuotaError) if quota exceeded.
    pub fn can_make_request(&mut self, request_count: u64) -> Result<(), QuotaError> {
        self.cleanup_old_requests();
        self.reset_daily_quota_if_needed();
        self.reset_monthly_quota_if_needed();

        let now = Self::current_timestamp();

        // Check per-second limit
        if self.config.max_requests_per_second > 0 {
            let requests_last_second = self.count_requests_since(now - 1);
            if requests_last_second + request_count > self.config.max_requests_per_second {
                return Err(QuotaError::RateLimitExceeded(format!(
                    "Exceeded {} requests/second limit (current: {}, requested: {})",
                    self.config.max_requests_per_second, requests_last_second, request_count
                )));
            }
        }

        // Check per-minute limit
        if self.config.max_requests_per_minute > 0 {
            let requests_last_minute = self.count_requests_since(now - 60);
            if requests_last_minute + request_count > self.config.max_requests_per_minute {
                return Err(QuotaError::RateLimitExceeded(format!(
                    "Exceeded {} requests/minute limit (current: {}, requested: {})",
                    self.config.max_requests_per_minute, requests_last_minute, request_count
                )));
            }
        }

        // Check per-hour limit
        if self.config.max_requests_per_hour > 0 {
            let requests_last_hour = self.count_requests_since(now - 3600);
            if requests_last_hour + request_count > self.config.max_requests_per_hour {
                return Err(QuotaError::RateLimitExceeded(format!(
                    "Exceeded {} requests/hour limit (current: {}, requested: {})",
                    self.config.max_requests_per_hour, requests_last_hour, request_count
                )));
            }
        }

        // Check per-day limit
        if self.config.max_requests_per_day > 0
            && self.daily_requests + request_count > self.config.max_requests_per_day
        {
            return Err(QuotaError::RateLimitExceeded(format!(
                "Exceeded {} requests/day limit (current: {}, requested: {})",
                self.config.max_requests_per_day, self.daily_requests, request_count
            )));
        }

        Ok(())
    }

    /// Check if a cost can be incurred
    pub fn can_incur_cost(&self, cost_usd: f64) -> Result<(), QuotaError> {
        // Check daily budget
        if self.config.daily_budget_usd > 0.0
            && self.daily_cost_usd + cost_usd > self.config.daily_budget_usd
        {
            return Err(QuotaError::BudgetExceeded(format!(
                "Exceeded ${:.2} daily budget (current: ${:.4}, requested: ${:.4})",
                self.config.daily_budget_usd, self.daily_cost_usd, cost_usd
            )));
        }

        // Check monthly budget
        if self.config.monthly_budget_usd > 0.0
            && self.monthly_cost_usd + cost_usd > self.config.monthly_budget_usd
        {
            return Err(QuotaError::BudgetExceeded(format!(
                "Exceeded ${:.2} monthly budget (current: ${:.4}, requested: ${:.4})",
                self.config.monthly_budget_usd, self.monthly_cost_usd, cost_usd
            )));
        }

        Ok(())
    }

    /// Record a request
    ///
    /// Call this after successfully making an API request.
    pub fn record_request(&mut self, request_count: u64, cost_usd: f64) {
        let now = Self::current_timestamp();

        self.requests.push_back(RequestRecord {
            timestamp: now,
            count: request_count,
            cost_usd,
        });

        self.daily_requests += request_count;
        self.daily_cost_usd += cost_usd;
        self.monthly_cost_usd += cost_usd;
    }

    /// Get quota status
    pub fn status(&mut self) -> QuotaStatus {
        self.cleanup_old_requests();
        self.reset_daily_quota_if_needed();
        self.reset_monthly_quota_if_needed();

        let now = Self::current_timestamp();

        let requests_last_second = self.count_requests_since(now - 1);
        let requests_last_minute = self.count_requests_since(now - 60);
        let requests_last_hour = self.count_requests_since(now - 3600);

        let (alert_triggered, alert_message) = self.check_alerts();

        QuotaStatus {
            requests_last_second,
            requests_last_minute,
            requests_last_hour,
            requests_today: self.daily_requests,
            cost_today_usd: self.daily_cost_usd,
            cost_this_month_usd: self.monthly_cost_usd,
            alert_triggered,
            alert_message,
        }
    }

    /// Reset daily quotas if a new day has started
    fn reset_daily_quota_if_needed(&mut self) {
        let now = Self::current_timestamp();
        let current_day_start = Self::day_start_timestamp(now);

        if current_day_start > self.day_start {
            self.day_start = current_day_start;
            self.daily_requests = 0;
            self.daily_cost_usd = 0.0;
        }
    }

    /// Reset monthly quotas if a new month has started
    fn reset_monthly_quota_if_needed(&mut self) {
        let now = Self::current_timestamp();
        let current_month_start = Self::month_start_timestamp(now);

        if current_month_start > self.month_start {
            self.month_start = current_month_start;
            self.monthly_cost_usd = 0.0;
        }
    }

    /// Count requests since a given timestamp
    fn count_requests_since(&self, since: u64) -> u64 {
        self.requests
            .iter()
            .filter(|r| r.timestamp >= since)
            .map(|r| r.count)
            .sum()
    }

    /// Remove old requests (older than 1 hour)
    fn cleanup_old_requests(&mut self) {
        let cutoff = Self::current_timestamp() - 3600;
        while let Some(record) = self.requests.front() {
            if record.timestamp < cutoff {
                self.requests.pop_front();
            } else {
                break;
            }
        }
    }

    /// Check if alert thresholds are exceeded
    fn check_alerts(&self) -> (bool, Option<String>) {
        let threshold = self.config.alert_threshold;

        // Check daily request quota
        if self.config.max_requests_per_day > 0 {
            let usage_ratio = self.daily_requests as f64 / self.config.max_requests_per_day as f64;
            if usage_ratio >= threshold {
                return (
                    true,
                    Some(format!(
                        "Daily request quota at {:.1}% ({}/{})",
                        usage_ratio * 100.0,
                        self.daily_requests,
                        self.config.max_requests_per_day
                    )),
                );
            }
        }

        // Check daily budget
        if self.config.daily_budget_usd > 0.0 {
            let usage_ratio = self.daily_cost_usd / self.config.daily_budget_usd;
            if usage_ratio >= threshold {
                return (
                    true,
                    Some(format!(
                        "Daily budget at {:.1}% (${:.4}/${:.2})",
                        usage_ratio * 100.0,
                        self.daily_cost_usd,
                        self.config.daily_budget_usd
                    )),
                );
            }
        }

        // Check monthly budget
        if self.config.monthly_budget_usd > 0.0 {
            let usage_ratio = self.monthly_cost_usd / self.config.monthly_budget_usd;
            if usage_ratio >= threshold {
                return (
                    true,
                    Some(format!(
                        "Monthly budget at {:.1}% (${:.4}/${:.2})",
                        usage_ratio * 100.0,
                        self.monthly_cost_usd,
                        self.config.monthly_budget_usd
                    )),
                );
            }
        }

        (false, None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quota_config_builder() {
        let config = QuotaConfig::new()
            .with_max_requests_per_second(100)
            .with_max_requests_per_minute(5000)
            .with_max_requests_per_hour(100_000)
            .with_max_requests_per_day(1_000_000)
            .with_daily_budget_usd(10.0)
            .with_monthly_budget_usd(300.0)
            .with_alert_threshold(0.8);

        assert_eq!(config.max_requests_per_second, 100);
        assert_eq!(config.max_requests_per_minute, 5000);
        assert_eq!(config.max_requests_per_hour, 100_000);
        assert_eq!(config.max_requests_per_day, 1_000_000);
        assert_eq!(config.daily_budget_usd, 10.0);
        assert_eq!(config.monthly_budget_usd, 300.0);
        assert_eq!(config.alert_threshold, 0.8);
    }

    #[test]
    fn test_quota_config_validation() {
        let valid_config = QuotaConfig::new().with_alert_threshold(0.75);
        assert!(valid_config.validate().is_ok());

        let invalid_config = QuotaConfig {
            alert_threshold: 1.5,
            ..Default::default()
        };
        assert!(invalid_config.validate().is_err());
    }

    #[test]
    fn test_can_make_request() {
        let config = QuotaConfig::new()
            .with_max_requests_per_second(10)
            .with_max_requests_per_day(1000);

        let mut quota_mgr = QuotaManager::new(config);

        // Should allow first request
        assert!(quota_mgr.can_make_request(1).is_ok());
        quota_mgr.record_request(1, 0.0004);

        // Should allow up to 10 requests per second
        for _ in 0..9 {
            assert!(quota_mgr.can_make_request(1).is_ok());
            quota_mgr.record_request(1, 0.0004);
        }

        // 11th request should be denied
        let result = quota_mgr.can_make_request(1);
        assert!(result.is_err());
        assert!(matches!(result, Err(QuotaError::RateLimitExceeded(_))));
    }

    #[test]
    fn test_can_incur_cost() {
        let config = QuotaConfig::new()
            .with_daily_budget_usd(1.0)
            .with_monthly_budget_usd(30.0);

        let mut quota_mgr = QuotaManager::new(config);

        // Should allow cost within budget
        assert!(quota_mgr.can_incur_cost(0.5).is_ok());
        quota_mgr.record_request(1, 0.5);

        // Should allow another 0.4
        assert!(quota_mgr.can_incur_cost(0.4).is_ok());
        quota_mgr.record_request(1, 0.4);

        // Should deny 0.2 (would exceed daily budget of 1.0)
        let result = quota_mgr.can_incur_cost(0.2);
        assert!(result.is_err());
        assert!(matches!(result, Err(QuotaError::BudgetExceeded(_))));
    }

    #[test]
    fn test_record_request() {
        let config = QuotaConfig::new();
        let mut quota_mgr = QuotaManager::new(config);

        quota_mgr.record_request(5, 0.002);
        assert_eq!(quota_mgr.daily_requests, 5);
        assert!((quota_mgr.daily_cost_usd - 0.002).abs() < 1e-10);
        assert!((quota_mgr.monthly_cost_usd - 0.002).abs() < 1e-10);

        quota_mgr.record_request(3, 0.0012);
        assert_eq!(quota_mgr.daily_requests, 8);
        assert!((quota_mgr.daily_cost_usd - 0.0032).abs() < 1e-10);
        assert!((quota_mgr.monthly_cost_usd - 0.0032).abs() < 1e-10);
    }

    #[test]
    fn test_quota_status() {
        let config = QuotaConfig::new();
        let mut quota_mgr = QuotaManager::new(config);

        quota_mgr.record_request(10, 0.004);

        let status = quota_mgr.status();
        assert!(status.requests_last_second >= 10);
        assert_eq!(status.requests_today, 10);
        assert_eq!(status.cost_today_usd, 0.004);
        assert!(!status.alert_triggered);
    }

    #[test]
    fn test_alert_threshold() {
        let config = QuotaConfig::new()
            .with_max_requests_per_day(100)
            .with_alert_threshold(0.8);

        let mut quota_mgr = QuotaManager::new(config);

        // Record 79 requests (below threshold)
        quota_mgr.record_request(79, 0.0);
        let status = quota_mgr.status();
        assert!(!status.alert_triggered);

        // Record 1 more request (80% threshold)
        quota_mgr.record_request(1, 0.0);
        let status = quota_mgr.status();
        assert!(status.alert_triggered);
        assert!(status.alert_message.is_some());
    }

    #[test]
    fn test_budget_alert() {
        let config = QuotaConfig::new()
            .with_daily_budget_usd(10.0)
            .with_alert_threshold(0.8);

        let mut quota_mgr = QuotaManager::new(config);

        // Record cost below threshold
        quota_mgr.record_request(1, 7.9);
        let status = quota_mgr.status();
        assert!(!status.alert_triggered);

        // Record cost at/above threshold
        quota_mgr.record_request(1, 0.2);
        let status = quota_mgr.status();
        assert!(status.alert_triggered);
        assert!(status.alert_message.is_some());
        assert!(status.alert_message.unwrap().contains("Daily budget"));
    }

    #[test]
    fn test_cleanup_old_requests() {
        let config = QuotaConfig::new();
        let mut quota_mgr = QuotaManager::new(config);

        // Record some requests
        for _ in 0..10 {
            quota_mgr.record_request(1, 0.0004);
        }

        assert_eq!(quota_mgr.requests.len(), 10);

        // Cleanup (no old requests yet)
        quota_mgr.cleanup_old_requests();
        assert_eq!(quota_mgr.requests.len(), 10);
    }

    #[test]
    fn test_per_minute_limit() {
        let config = QuotaConfig::new().with_max_requests_per_minute(100);

        let mut quota_mgr = QuotaManager::new(config);

        // Should allow 100 requests
        for _ in 0..100 {
            assert!(quota_mgr.can_make_request(1).is_ok());
            quota_mgr.record_request(1, 0.0);
        }

        // 101st should be denied
        let result = quota_mgr.can_make_request(1);
        assert!(result.is_err());
    }

    #[test]
    fn test_per_hour_limit() {
        let config = QuotaConfig::new().with_max_requests_per_hour(1000);

        let mut quota_mgr = QuotaManager::new(config);

        // Should allow 1000 requests
        for _ in 0..100 {
            assert!(quota_mgr.can_make_request(10).is_ok());
            quota_mgr.record_request(10, 0.0);
        }

        // 1001st should be denied
        let result = quota_mgr.can_make_request(1);
        assert!(result.is_err());
    }
}
