use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Performance profile for a task or operation
#[derive(Debug, Clone)]
pub struct PerformanceProfile {
    /// Name of the profiled operation
    pub name: String,
    /// Total execution time
    pub total_duration: Duration,
    /// Time spent in child operations
    pub children_duration: Duration,
    /// Self time (total - children)
    pub self_duration: Duration,
    /// Number of invocations
    pub invocation_count: usize,
}

impl PerformanceProfile {
    /// Gets average duration per invocation
    pub fn avg_duration(&self) -> Duration {
        if self.invocation_count == 0 {
            Duration::from_secs(0)
        } else {
            self.total_duration / self.invocation_count as u32
        }
    }

    /// Gets percentage of time spent in self vs children
    pub fn self_time_percentage(&self) -> f64 {
        if self.total_duration.as_millis() == 0 {
            0.0
        } else {
            (self.self_duration.as_millis() as f64 / self.total_duration.as_millis() as f64) * 100.0
        }
    }
}

/// Performance profiler for tracking execution time
pub struct PerformanceProfiler {
    profiles: Arc<Mutex<HashMap<String, PerformanceProfile>>>,
    active_spans: Arc<Mutex<Vec<(String, Instant)>>>,
}

impl PerformanceProfiler {
    /// Creates a new performance profiler
    pub fn new() -> Self {
        Self {
            profiles: Arc::new(Mutex::new(HashMap::new())),
            active_spans: Arc::new(Mutex::new(Vec::new())),
        }
    }

    /// Starts profiling an operation
    pub fn start_span(&self, name: impl Into<String>) {
        let name = name.into();
        self.active_spans
            .lock()
            .unwrap()
            .push((name, Instant::now()));
    }

    /// Ends the current profiling span
    pub fn end_span(&self) {
        let span_data = self
            .active_spans
            .lock()
            .expect("lock should not be poisoned")
            .pop();
        if let Some((name, start_time)) = span_data {
            let duration = start_time.elapsed();
            let mut profiles = self.profiles.lock().expect("lock should not be poisoned");

            let profile = profiles
                .entry(name.clone())
                .or_insert_with(|| PerformanceProfile {
                    name: name.clone(),
                    total_duration: Duration::from_secs(0),
                    children_duration: Duration::from_secs(0),
                    self_duration: Duration::from_secs(0),
                    invocation_count: 0,
                });

            profile.total_duration += duration;
            profile.invocation_count += 1;
            profile.self_duration = profile.total_duration - profile.children_duration;
        }
    }

    /// Gets the profile for a specific operation
    pub fn get_profile(&self, name: &str) -> Option<PerformanceProfile> {
        self.profiles
            .lock()
            .expect("lock should not be poisoned")
            .get(name)
            .cloned()
    }

    /// Gets all profiles
    pub fn get_all_profiles(&self) -> Vec<PerformanceProfile> {
        self.profiles
            .lock()
            .expect("lock should not be poisoned")
            .values()
            .cloned()
            .collect()
    }

    /// Gets profiles sorted by total duration (slowest first)
    pub fn get_slowest_operations(&self, limit: usize) -> Vec<PerformanceProfile> {
        let mut profiles = self.get_all_profiles();
        profiles.sort_by_key(|p| std::cmp::Reverse(p.total_duration));
        profiles.truncate(limit);
        profiles
    }

    /// Generates a performance report
    pub fn generate_report(&self) -> String {
        let profiles = self.get_all_profiles();
        if profiles.is_empty() {
            return "No profiling data available".to_string();
        }

        let mut report = String::from("Performance Profile Report\n");
        report.push_str(&format!("{}\n", "=".repeat(80)));

        for profile in profiles {
            report.push_str(&format!(
                "{:<30} | Count: {:>6} | Total: {:>8.2}ms | Avg: {:>8.2}ms | Self: {:>6.1}%\n",
                profile.name,
                profile.invocation_count,
                profile.total_duration.as_secs_f64() * 1000.0,
                profile.avg_duration().as_secs_f64() * 1000.0,
                profile.self_time_percentage()
            ));
        }

        report
    }

    /// Resets all profiling data
    pub fn reset(&self) {
        self.profiles
            .lock()
            .expect("lock should not be poisoned")
            .clear();
        self.active_spans
            .lock()
            .expect("lock should not be poisoned")
            .clear();
    }
}

impl Default for PerformanceProfiler {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for PerformanceProfiler {
    fn clone(&self) -> Self {
        Self {
            profiles: Arc::clone(&self.profiles),
            active_spans: Arc::clone(&self.active_spans),
        }
    }
}

/// RAII guard for automatic span tracking
pub struct ProfileSpan<'a> {
    profiler: &'a PerformanceProfiler,
}

impl<'a> ProfileSpan<'a> {
    /// Creates a new profile span
    pub fn new(profiler: &'a PerformanceProfiler, name: impl Into<String>) -> Self {
        profiler.start_span(name);
        Self { profiler }
    }
}

impl<'a> Drop for ProfileSpan<'a> {
    fn drop(&mut self) {
        self.profiler.end_span();
    }
}
