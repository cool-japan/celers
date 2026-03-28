//! Performance profiling utilities for Redis backend
//!
//! This module provides tools for profiling and analyzing backend performance,
//! including operation timing, memory usage, and throughput measurements.

use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Operation profile data
#[derive(Debug, Clone)]
pub struct OperationProfile {
    /// Operation name
    pub name: String,
    /// Number of calls
    pub call_count: u64,
    /// Total duration across all calls
    pub total_duration: Duration,
    /// Minimum duration observed
    pub min_duration: Duration,
    /// Maximum duration observed
    pub max_duration: Duration,
    /// Data size statistics (in bytes)
    pub total_bytes: u64,
    /// Error count
    pub error_count: u64,
}

impl OperationProfile {
    /// Create a new operation profile
    pub fn new(name: String) -> Self {
        Self {
            name,
            call_count: 0,
            total_duration: Duration::ZERO,
            min_duration: Duration::MAX,
            max_duration: Duration::ZERO,
            total_bytes: 0,
            error_count: 0,
        }
    }

    /// Record a successful operation
    pub fn record(&mut self, duration: Duration, bytes: usize) {
        self.call_count += 1;
        self.total_duration += duration;
        self.min_duration = self.min_duration.min(duration);
        self.max_duration = self.max_duration.max(duration);
        self.total_bytes += bytes as u64;
    }

    /// Record an error
    pub fn record_error(&mut self, duration: Duration) {
        self.call_count += 1;
        self.error_count += 1;
        self.total_duration += duration;
        self.min_duration = self.min_duration.min(duration);
        self.max_duration = self.max_duration.max(duration);
    }

    /// Get average duration
    pub fn avg_duration(&self) -> Duration {
        if self.call_count == 0 {
            Duration::ZERO
        } else {
            self.total_duration / self.call_count as u32
        }
    }

    /// Get average bytes per operation
    pub fn avg_bytes(&self) -> f64 {
        if self.call_count == 0 {
            0.0
        } else {
            self.total_bytes as f64 / self.call_count as f64
        }
    }

    /// Get error rate (0.0 - 1.0)
    pub fn error_rate(&self) -> f64 {
        if self.call_count == 0 {
            0.0
        } else {
            self.error_count as f64 / self.call_count as f64
        }
    }

    /// Get throughput in operations per second
    pub fn throughput(&self) -> f64 {
        let total_secs = self.total_duration.as_secs_f64();
        if total_secs == 0.0 {
            0.0
        } else {
            self.call_count as f64 / total_secs
        }
    }
}

impl std::fmt::Display for OperationProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Operation: {}", self.name)?;
        writeln!(f, "  Calls: {}", self.call_count)?;
        writeln!(
            f,
            "  Errors: {} ({:.1}%)",
            self.error_count,
            self.error_rate() * 100.0
        )?;
        writeln!(
            f,
            "  Avg Duration: {:.2}ms",
            self.avg_duration().as_secs_f64() * 1000.0
        )?;
        writeln!(
            f,
            "  Min Duration: {:.2}ms",
            self.min_duration.as_secs_f64() * 1000.0
        )?;
        writeln!(
            f,
            "  Max Duration: {:.2}ms",
            self.max_duration.as_secs_f64() * 1000.0
        )?;
        writeln!(f, "  Avg Bytes: {:.1}", self.avg_bytes())?;
        writeln!(f, "  Throughput: {:.1} ops/sec", self.throughput())?;
        Ok(())
    }
}

/// Performance profiler for tracking operation metrics
#[derive(Debug, Default)]
pub struct Profiler {
    profiles: HashMap<String, OperationProfile>,
    enabled: bool,
}

impl Profiler {
    /// Create a new profiler
    pub fn new() -> Self {
        Self {
            profiles: HashMap::new(),
            enabled: true,
        }
    }

    /// Enable profiling
    pub fn enable(&mut self) {
        self.enabled = true;
    }

    /// Disable profiling
    pub fn disable(&mut self) {
        self.enabled = false;
    }

    /// Check if profiling is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Start profiling an operation
    pub fn start(&self, name: impl Into<String>) -> ProfileGuard {
        ProfileGuard {
            name: name.into(),
            start: Instant::now(),
            bytes: 0,
        }
    }

    /// Record a completed operation
    pub fn record(&mut self, name: String, duration: Duration, bytes: usize, success: bool) {
        if !self.enabled {
            return;
        }

        let profile = self
            .profiles
            .entry(name.clone())
            .or_insert_with(|| OperationProfile::new(name));

        if success {
            profile.record(duration, bytes);
        } else {
            profile.record_error(duration);
        }
    }

    /// Get profile for an operation
    pub fn get_profile(&self, name: &str) -> Option<&OperationProfile> {
        self.profiles.get(name)
    }

    /// Get all profiles
    pub fn profiles(&self) -> &HashMap<String, OperationProfile> {
        &self.profiles
    }

    /// Get sorted profiles by total duration (slowest first)
    pub fn slowest_operations(&self) -> Vec<&OperationProfile> {
        let mut profiles: Vec<&OperationProfile> = self.profiles.values().collect();
        profiles.sort_by_key(|b| std::cmp::Reverse(b.total_duration));
        profiles
    }

    /// Get sorted profiles by error rate (highest first)
    pub fn most_errors(&self) -> Vec<&OperationProfile> {
        let mut profiles: Vec<&OperationProfile> = self.profiles.values().collect();
        profiles.sort_by_key(|b| std::cmp::Reverse(b.error_count));
        profiles
    }

    /// Reset all profiles
    pub fn reset(&mut self) {
        self.profiles.clear();
    }

    /// Generate a summary report
    pub fn summary(&self) -> String {
        let mut report = String::new();
        report.push_str("=== Performance Profile Summary ===\n\n");

        if self.profiles.is_empty() {
            report.push_str("No profiles recorded\n");
            return report;
        }

        // Overall statistics
        let total_calls: u64 = self.profiles.values().map(|p| p.call_count).sum();
        let total_errors: u64 = self.profiles.values().map(|p| p.error_count).sum();
        let total_duration: Duration = self.profiles.values().map(|p| p.total_duration).sum();

        report.push_str(&format!("Total Operations: {}\n", total_calls));
        report.push_str(&format!("Total Errors: {}\n", total_errors));
        report.push_str(&format!(
            "Total Duration: {:.2}s\n",
            total_duration.as_secs_f64()
        ));
        report.push_str(&format!(
            "Overall Error Rate: {:.1}%\n\n",
            if total_calls > 0 {
                total_errors as f64 / total_calls as f64 * 100.0
            } else {
                0.0
            }
        ));

        // Slowest operations
        report.push_str("Slowest Operations (by total time):\n");
        for (i, profile) in self.slowest_operations().iter().take(5).enumerate() {
            report.push_str(&format!(
                "  {}. {} - {:.2}s total\n",
                i + 1,
                profile.name,
                profile.total_duration.as_secs_f64()
            ));
        }

        report
    }
}

/// RAII guard for profiling operations
pub struct ProfileGuard {
    name: String,
    start: Instant,
    bytes: usize,
}

impl ProfileGuard {
    /// Set the number of bytes processed
    pub fn with_bytes(mut self, bytes: usize) -> Self {
        self.bytes = bytes;
        self
    }

    /// Finish profiling and return the result
    pub fn finish(self) -> (String, Duration, usize) {
        let duration = self.start.elapsed();
        (self.name, duration, self.bytes)
    }

    /// Get elapsed time so far
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

/// Throughput analyzer for measuring operation rates
#[derive(Debug)]
pub struct ThroughputAnalyzer {
    window_size: Duration,
    samples: Vec<(Instant, u64)>,
}

impl ThroughputAnalyzer {
    /// Create a new throughput analyzer with a time window
    pub fn new(window_size: Duration) -> Self {
        Self {
            window_size,
            samples: Vec::new(),
        }
    }

    /// Record operations at the current time
    pub fn record(&mut self, count: u64) {
        let now = Instant::now();
        self.samples.push((now, count));
        self.cleanup(now);
    }

    /// Remove old samples outside the time window
    fn cleanup(&mut self, now: Instant) {
        self.samples
            .retain(|(time, _)| now.duration_since(*time) <= self.window_size);
    }

    /// Get current throughput (operations per second)
    pub fn throughput(&self) -> f64 {
        if self.samples.is_empty() {
            return 0.0;
        }

        let total_ops: u64 = self.samples.iter().map(|(_, count)| count).sum();
        let duration = if self.samples.len() > 1 {
            let first = self
                .samples
                .first()
                .expect("collection validated to be non-empty")
                .0;
            let last = self
                .samples
                .last()
                .expect("collection validated to be non-empty")
                .0;
            last.duration_since(first)
        } else {
            self.window_size
        };

        if duration.as_secs_f64() == 0.0 {
            0.0
        } else {
            total_ops as f64 / duration.as_secs_f64()
        }
    }

    /// Get the number of samples in the current window
    pub fn sample_count(&self) -> usize {
        self.samples.len()
    }

    /// Reset the analyzer
    pub fn reset(&mut self) {
        self.samples.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operation_profile_creation() {
        let profile = OperationProfile::new("test_op".to_string());
        assert_eq!(profile.name, "test_op");
        assert_eq!(profile.call_count, 0);
        assert_eq!(profile.error_count, 0);
    }

    #[test]
    fn test_operation_profile_record() {
        let mut profile = OperationProfile::new("test".to_string());
        profile.record(Duration::from_millis(100), 1024);
        profile.record(Duration::from_millis(200), 2048);

        assert_eq!(profile.call_count, 2);
        assert_eq!(profile.total_bytes, 3072);
        assert_eq!(profile.min_duration, Duration::from_millis(100));
        assert_eq!(profile.max_duration, Duration::from_millis(200));
    }

    #[test]
    fn test_operation_profile_error() {
        let mut profile = OperationProfile::new("test".to_string());
        profile.record(Duration::from_millis(100), 1024);
        profile.record_error(Duration::from_millis(50));

        assert_eq!(profile.call_count, 2);
        assert_eq!(profile.error_count, 1);
        assert_eq!(profile.error_rate(), 0.5);
    }

    #[test]
    fn test_operation_profile_averages() {
        let mut profile = OperationProfile::new("test".to_string());
        profile.record(Duration::from_millis(100), 1000);
        profile.record(Duration::from_millis(200), 2000);

        assert_eq!(profile.avg_duration(), Duration::from_millis(150));
        assert_eq!(profile.avg_bytes(), 1500.0);
    }

    #[test]
    fn test_profiler_creation() {
        let profiler = Profiler::new();
        assert!(profiler.is_enabled());
        assert!(profiler.profiles().is_empty());
    }

    #[test]
    fn test_profiler_enable_disable() {
        let mut profiler = Profiler::new();
        assert!(profiler.is_enabled());

        profiler.disable();
        assert!(!profiler.is_enabled());

        profiler.enable();
        assert!(profiler.is_enabled());
    }

    #[test]
    fn test_profiler_record() {
        let mut profiler = Profiler::new();
        profiler.record(
            "test_op".to_string(),
            Duration::from_millis(100),
            1024,
            true,
        );

        let profile = profiler.get_profile("test_op").unwrap();
        assert_eq!(profile.call_count, 1);
        assert_eq!(profile.total_bytes, 1024);
    }

    #[test]
    fn test_profiler_disabled_recording() {
        let mut profiler = Profiler::new();
        profiler.disable();
        profiler.record(
            "test_op".to_string(),
            Duration::from_millis(100),
            1024,
            true,
        );

        assert!(profiler.get_profile("test_op").is_none());
    }

    #[test]
    fn test_profile_guard() {
        let guard = ProfileGuard {
            name: "test".to_string(),
            start: Instant::now(),
            bytes: 0,
        };

        let guard = guard.with_bytes(1024);
        let (name, _duration, bytes) = guard.finish();

        assert_eq!(name, "test");
        assert_eq!(bytes, 1024);
    }

    #[test]
    fn test_profiler_slowest_operations() {
        let mut profiler = Profiler::new();
        profiler.record("fast".to_string(), Duration::from_millis(10), 100, true);
        profiler.record("slow".to_string(), Duration::from_millis(100), 100, true);
        profiler.record("medium".to_string(), Duration::from_millis(50), 100, true);

        let slowest = profiler.slowest_operations();
        assert_eq!(slowest.len(), 3);
        assert_eq!(slowest[0].name, "slow");
        assert_eq!(slowest[2].name, "fast");
    }

    #[test]
    fn test_profiler_most_errors() {
        let mut profiler = Profiler::new();
        profiler.record("ok".to_string(), Duration::from_millis(10), 100, true);
        profiler.record("bad".to_string(), Duration::from_millis(10), 100, false);
        profiler.record("bad".to_string(), Duration::from_millis(10), 100, false);

        let most_errors = profiler.most_errors();
        assert_eq!(most_errors[0].name, "bad");
        assert_eq!(most_errors[0].error_count, 2);
    }

    #[test]
    fn test_profiler_reset() {
        let mut profiler = Profiler::new();
        profiler.record("test".to_string(), Duration::from_millis(10), 100, true);
        assert_eq!(profiler.profiles().len(), 1);

        profiler.reset();
        assert_eq!(profiler.profiles().len(), 0);
    }

    #[test]
    fn test_throughput_analyzer() {
        let mut analyzer = ThroughputAnalyzer::new(Duration::from_secs(10));
        analyzer.record(100);
        analyzer.record(200);

        assert_eq!(analyzer.sample_count(), 2);
        assert!(analyzer.throughput() >= 0.0);
    }

    #[test]
    fn test_throughput_analyzer_reset() {
        let mut analyzer = ThroughputAnalyzer::new(Duration::from_secs(10));
        analyzer.record(100);
        assert_eq!(analyzer.sample_count(), 1);

        analyzer.reset();
        assert_eq!(analyzer.sample_count(), 0);
    }
}
