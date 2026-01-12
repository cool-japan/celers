//! Real-time AWS SQS cost tracking and monitoring
//!
//! This module provides utilities for tracking AWS SQS costs in real-time,
//! helping you monitor and optimize your SQS usage costs.
//!
//! # Examples
//!
//! ```
//! use celers_broker_sqs::cost_tracker::*;
//!
//! # async fn example() {
//! let tracker = CostTracker::new();
//!
//! // Track operations
//! tracker.record_send_message().await;
//! tracker.record_receive_message().await;
//! tracker.record_delete_message().await;
//!
//! // Get cost summary
//! let summary = tracker.get_summary(false).await; // Standard queue
//! println!("Estimated cost: ${:.4}", summary.total_cost_usd);
//! # }
//! ```

use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::Mutex;

/// AWS SQS pricing (as of 2024)
pub const STANDARD_QUEUE_PRICE_PER_MILLION: f64 = 0.40;
pub const FIFO_QUEUE_PRICE_PER_MILLION: f64 = 0.50;
pub const FREE_TIER_REQUESTS_PER_MONTH: u64 = 1_000_000;

/// Cost tracker for AWS SQS operations
#[derive(Debug, Clone)]
pub struct CostTracker {
    stats: Arc<Mutex<CostStats>>,
}

/// Statistics for cost tracking
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CostStats {
    /// Number of SendMessage requests
    pub send_message_count: u64,
    /// Number of SendMessageBatch requests
    pub send_message_batch_count: u64,
    /// Number of ReceiveMessage requests
    pub receive_message_count: u64,
    /// Number of DeleteMessage requests
    pub delete_message_count: u64,
    /// Number of DeleteMessageBatch requests
    pub delete_message_batch_count: u64,
    /// Number of ChangeMessageVisibility requests
    pub change_visibility_count: u64,
    /// Number of ChangeMessageVisibilityBatch requests
    pub change_visibility_batch_count: u64,
    /// Number of GetQueueAttributes requests
    pub get_queue_attributes_count: u64,
    /// Number of SetQueueAttributes requests
    pub set_queue_attributes_count: u64,
    /// Number of other API requests (CreateQueue, DeleteQueue, etc.)
    pub other_requests_count: u64,
}

/// Cost summary
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostSummary {
    /// Total number of API requests
    pub total_requests: u64,
    /// Estimated cost in USD
    pub total_cost_usd: f64,
    /// Cost per request in USD
    pub cost_per_request_usd: f64,
    /// Requests within free tier
    pub free_tier_requests: u64,
    /// Billable requests (beyond free tier)
    pub billable_requests: u64,
    /// Estimated monthly cost if this rate continues (USD)
    pub projected_monthly_cost_usd: f64,
    /// Cost breakdown by operation type
    pub breakdown: CostBreakdown,
}

/// Cost breakdown by operation type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CostBreakdown {
    /// Cost of send operations
    pub send_cost_usd: f64,
    /// Cost of receive operations
    pub receive_cost_usd: f64,
    /// Cost of delete operations
    pub delete_cost_usd: f64,
    /// Cost of visibility operations
    pub visibility_cost_usd: f64,
    /// Cost of queue management operations
    pub management_cost_usd: f64,
}

impl CostTracker {
    /// Create a new cost tracker
    pub fn new() -> Self {
        Self {
            stats: Arc::new(Mutex::new(CostStats::default())),
        }
    }

    /// Record a SendMessage request
    pub async fn record_send_message(&self) {
        let mut stats = self.stats.lock().await;
        stats.send_message_count += 1;
    }

    /// Record a SendMessageBatch request
    pub async fn record_send_message_batch(&self) {
        let mut stats = self.stats.lock().await;
        stats.send_message_batch_count += 1;
    }

    /// Record a ReceiveMessage request
    pub async fn record_receive_message(&self) {
        let mut stats = self.stats.lock().await;
        stats.receive_message_count += 1;
    }

    /// Record a DeleteMessage request
    pub async fn record_delete_message(&self) {
        let mut stats = self.stats.lock().await;
        stats.delete_message_count += 1;
    }

    /// Record a DeleteMessageBatch request
    pub async fn record_delete_message_batch(&self) {
        let mut stats = self.stats.lock().await;
        stats.delete_message_batch_count += 1;
    }

    /// Record a ChangeMessageVisibility request
    pub async fn record_change_visibility(&self) {
        let mut stats = self.stats.lock().await;
        stats.change_visibility_count += 1;
    }

    /// Record a ChangeMessageVisibilityBatch request
    pub async fn record_change_visibility_batch(&self) {
        let mut stats = self.stats.lock().await;
        stats.change_visibility_batch_count += 1;
    }

    /// Record a GetQueueAttributes request
    pub async fn record_get_queue_attributes(&self) {
        let mut stats = self.stats.lock().await;
        stats.get_queue_attributes_count += 1;
    }

    /// Record a SetQueueAttributes request
    pub async fn record_set_queue_attributes(&self) {
        let mut stats = self.stats.lock().await;
        stats.set_queue_attributes_count += 1;
    }

    /// Record other API requests
    pub async fn record_other_request(&self) {
        let mut stats = self.stats.lock().await;
        stats.other_requests_count += 1;
    }

    /// Get current cost statistics
    pub async fn get_stats(&self) -> CostStats {
        self.stats.lock().await.clone()
    }

    /// Get cost summary
    ///
    /// # Arguments
    /// * `is_fifo` - Whether this is a FIFO queue (affects pricing)
    pub async fn get_summary(&self, is_fifo: bool) -> CostSummary {
        let stats = self.stats.lock().await.clone();

        let total_requests = stats.send_message_count
            + stats.send_message_batch_count
            + stats.receive_message_count
            + stats.delete_message_count
            + stats.delete_message_batch_count
            + stats.change_visibility_count
            + stats.change_visibility_batch_count
            + stats.get_queue_attributes_count
            + stats.set_queue_attributes_count
            + stats.other_requests_count;

        // Calculate billable requests (after free tier)
        let free_tier_requests = total_requests.min(FREE_TIER_REQUESTS_PER_MONTH);
        let billable_requests = total_requests.saturating_sub(FREE_TIER_REQUESTS_PER_MONTH);

        // Calculate costs
        let price_per_million = if is_fifo {
            FIFO_QUEUE_PRICE_PER_MILLION
        } else {
            STANDARD_QUEUE_PRICE_PER_MILLION
        };

        let total_cost_usd = (billable_requests as f64 / 1_000_000.0) * price_per_million;
        let cost_per_request_usd = if total_requests > 0 {
            total_cost_usd / total_requests as f64
        } else {
            0.0
        };

        // Project monthly cost (assuming current rate)
        let projected_monthly_cost_usd = if total_requests > 0 {
            let requests_per_month = total_requests;
            let billable_per_month =
                requests_per_month.saturating_sub(FREE_TIER_REQUESTS_PER_MONTH);
            (billable_per_month as f64 / 1_000_000.0) * price_per_million
        } else {
            0.0
        };

        // Cost breakdown
        let send_requests = stats.send_message_count + stats.send_message_batch_count;
        let receive_requests = stats.receive_message_count;
        let delete_requests = stats.delete_message_count + stats.delete_message_batch_count;
        let visibility_requests =
            stats.change_visibility_count + stats.change_visibility_batch_count;
        let management_requests = stats.get_queue_attributes_count
            + stats.set_queue_attributes_count
            + stats.other_requests_count;

        let breakdown = CostBreakdown {
            send_cost_usd: (send_requests as f64 / 1_000_000.0) * price_per_million,
            receive_cost_usd: (receive_requests as f64 / 1_000_000.0) * price_per_million,
            delete_cost_usd: (delete_requests as f64 / 1_000_000.0) * price_per_million,
            visibility_cost_usd: (visibility_requests as f64 / 1_000_000.0) * price_per_million,
            management_cost_usd: (management_requests as f64 / 1_000_000.0) * price_per_million,
        };

        CostSummary {
            total_requests,
            total_cost_usd,
            cost_per_request_usd,
            free_tier_requests,
            billable_requests,
            projected_monthly_cost_usd,
            breakdown,
        }
    }

    /// Reset all statistics
    pub async fn reset(&self) {
        let mut stats = self.stats.lock().await;
        *stats = CostStats::default();
    }

    /// Compare costs between batching and individual operations
    ///
    /// Returns (individual_cost, batch_cost, savings_percent)
    pub fn compare_batch_savings(message_count: usize, is_fifo: bool) -> (f64, f64, f64) {
        let price_per_million = if is_fifo {
            FIFO_QUEUE_PRICE_PER_MILLION
        } else {
            STANDARD_QUEUE_PRICE_PER_MILLION
        };

        // Individual operations: 1 request per message
        let individual_requests = message_count;
        let individual_cost = (individual_requests as f64 / 1_000_000.0) * price_per_million;

        // Batch operations: 1 request per 10 messages
        let batch_requests = message_count.div_ceil(10);
        let batch_cost = (batch_requests as f64 / 1_000_000.0) * price_per_million;

        let savings_percent = if individual_cost > 0.0 {
            ((individual_cost - batch_cost) / individual_cost) * 100.0
        } else {
            0.0
        };

        (individual_cost, batch_cost, savings_percent)
    }
}

impl Default for CostTracker {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_cost_tracker_creation() {
        let tracker = CostTracker::new();
        let stats = tracker.get_stats().await;

        assert_eq!(stats.send_message_count, 0);
        assert_eq!(stats.receive_message_count, 0);
    }

    #[tokio::test]
    async fn test_record_operations() {
        let tracker = CostTracker::new();

        tracker.record_send_message().await;
        tracker.record_receive_message().await;
        tracker.record_delete_message().await;

        let stats = tracker.get_stats().await;
        assert_eq!(stats.send_message_count, 1);
        assert_eq!(stats.receive_message_count, 1);
        assert_eq!(stats.delete_message_count, 1);
    }

    #[tokio::test]
    async fn test_cost_summary() {
        let tracker = CostTracker::new();

        // Record 1000 operations
        for _ in 0..1000 {
            tracker.record_send_message().await;
        }

        let summary = tracker.get_summary(false).await;
        assert_eq!(summary.total_requests, 1000);
        assert!(summary.total_cost_usd < 0.001); // Very small cost
    }

    #[tokio::test]
    async fn test_free_tier() {
        let tracker = CostTracker::new();

        // Simulate 500k requests (within free tier)
        let mut stats = tracker.stats.lock().await;
        stats.send_message_count = 500_000;
        drop(stats);

        let summary = tracker.get_summary(false).await;
        assert_eq!(summary.free_tier_requests, 500_000);
        assert_eq!(summary.billable_requests, 0);
        assert_eq!(summary.total_cost_usd, 0.0);
    }

    #[tokio::test]
    async fn test_beyond_free_tier() {
        let tracker = CostTracker::new();

        // Simulate 1.5M requests (beyond free tier)
        let mut stats = tracker.stats.lock().await;
        stats.send_message_count = 1_500_000;
        drop(stats);

        let summary = tracker.get_summary(false).await;
        assert_eq!(summary.total_requests, 1_500_000);
        assert_eq!(summary.free_tier_requests, 1_000_000);
        assert_eq!(summary.billable_requests, 500_000);
        assert!(summary.total_cost_usd > 0.0);
    }

    #[tokio::test]
    async fn test_fifo_pricing() {
        let tracker = CostTracker::new();

        let mut stats = tracker.stats.lock().await;
        stats.send_message_count = 1_500_000;
        drop(stats);

        let standard_summary = tracker.get_summary(false).await;
        let fifo_summary = tracker.get_summary(true).await;

        // FIFO should cost more than standard
        assert!(fifo_summary.total_cost_usd > standard_summary.total_cost_usd);
    }

    #[tokio::test]
    async fn test_reset() {
        let tracker = CostTracker::new();

        tracker.record_send_message().await;
        tracker.record_receive_message().await;

        tracker.reset().await;

        let stats = tracker.get_stats().await;
        assert_eq!(stats.send_message_count, 0);
        assert_eq!(stats.receive_message_count, 0);
    }

    #[test]
    fn test_batch_savings_comparison() {
        let (individual, batch, savings) = CostTracker::compare_batch_savings(100, false);

        assert!(batch < individual);
        assert!(savings > 80.0); // Should save > 80%
    }

    #[tokio::test]
    async fn test_cost_breakdown() {
        let tracker = CostTracker::new();

        tracker.record_send_message().await;
        tracker.record_receive_message().await;
        tracker.record_delete_message().await;

        let summary = tracker.get_summary(false).await;

        assert!(summary.breakdown.send_cost_usd >= 0.0);
        assert!(summary.breakdown.receive_cost_usd >= 0.0);
        assert!(summary.breakdown.delete_cost_usd >= 0.0);
    }
}
