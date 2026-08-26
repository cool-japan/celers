use std::sync::OnceLock;

/// Type alias for async initialization tasks used in parallel_init
pub type AsyncInitTask<T, E> = Box<
    dyn FnOnce() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, E>> + Send>>
        + Send,
>;

/// Lazy initialization helper using OnceLock for thread-safe static initialization
///
/// # Example
///
/// ```rust
/// use celers::startup_optimization::LazyInit;
///
/// static MY_CONFIG: LazyInit<String> = LazyInit::new();
///
/// fn get_config() -> &'static String {
///     MY_CONFIG.get_or_init(|| {
///         // Expensive initialization happens only once
///         String::from("config_value")
///     })
/// }
/// ```
pub struct LazyInit<T> {
    cell: OnceLock<T>,
}

impl<T> LazyInit<T> {
    /// Create a new lazy initialization wrapper
    pub const fn new() -> Self {
        Self {
            cell: OnceLock::new(),
        }
    }

    /// Get the value, initializing it if necessary
    #[inline]
    pub fn get_or_init<F>(&self, f: F) -> &T
    where
        F: FnOnce() -> T,
    {
        self.cell.get_or_init(f)
    }

    /// Try to get the value if it's already initialized
    #[inline]
    pub fn get(&self) -> Option<&T> {
        self.cell.get()
    }
}

impl<T> Default for LazyInit<T> {
    fn default() -> Self {
        Self::new()
    }
}

// Pre-compiled pattern cache for faster startup
//
// Note: For regex caching, add the `regex` crate to your dependencies and
// implement a pattern using `OnceLock<Mutex<HashMap<String, &'static Regex>>>`,
// for example:
//
// ```rust,ignore
// use std::sync::OnceLock;
// use std::collections::HashMap;
// use std::sync::Mutex;
// use regex::Regex;
//
// pub fn cached_regex(pattern: &str) -> &'static Regex {
//     static CACHE: OnceLock<Mutex<HashMap<String, &'static Regex>>> = OnceLock::new();
//     let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
//     let mut cache = cache.lock().expect("lock should not be poisoned");
//
//     if let Some(regex) = cache.get(pattern) {
//         return regex;
//     }
//
//     let regex = Box::leak(Box::new(Regex::new(pattern).expect("Invalid regex")));
//     cache.insert(pattern.to_string(), regex);
//     regex
// }
// ```
//
// This is a plain (non-doc) comment rather than a worked example function,
// since a runnable one would require pulling in the optional `regex` crate
// as a dependency this crate does not otherwise take on.

/// Parallel initialization helper for running multiple initialization tasks concurrently
///
/// # Errors
///
/// Returns an error if any spawned task panics or is cancelled (i.e. a `tokio::task::JoinError`
/// occurs). Individual task errors are propagated as `Err` variants inside the returned `Vec`.
///
/// # Example
///
/// ```rust,no_run
/// use celers::startup_optimization::parallel_init;
///
/// # async fn example() -> anyhow::Result<()> {
/// let results = parallel_init(vec![
///     Box::new(|| Box::pin(async { /* Initialize DB */ Ok::<(), String>(()) })),
///     Box::new(|| Box::pin(async { /* Connect to broker */ Ok::<(), String>(()) })),
///     Box::new(|| Box::pin(async { /* Load config */ Ok::<(), String>(()) })),
/// ]).await?;
/// # Ok(())
/// # }
/// ```
pub async fn parallel_init<T, E>(
    tasks: Vec<AsyncInitTask<T, E>>,
) -> anyhow::Result<Vec<Result<T, E>>>
where
    T: Send + 'static,
    E: Send + 'static,
{
    let handles: Vec<_> = tasks
        .into_iter()
        .map(|task| tokio::spawn(async move { task().await }))
        .collect();

    let mut results = Vec::new();
    for handle in handles {
        match handle.await {
            Ok(result) => results.push(result),
            Err(e) => {
                return Err(anyhow::anyhow!("Task join error: {:?}", e));
            }
        }
    }
    Ok(results)
}

/// Startup performance metrics
///
/// Every field starts at `0` from [`StartupMetrics::new`] / `Default`; call
/// the `record_*` methods with real [`std::time::Duration`]s (typically
/// captured with [`crate::time_init!`] around each startup stage, or with a plain
/// [`std::time::Instant`]) to populate them. `total_ms` is kept in sync
/// automatically: it is recomputed as the sum of the three stage timings
/// every time one of them is recorded, so it never needs to be set by hand.
///
/// # Example
///
/// ```rust
/// use celers::startup_optimization::StartupMetrics;
/// use celers::time_init;
///
/// let mut metrics = StartupMetrics::new();
///
/// let (_config, duration) = time_init!({
///     // Load configuration from the environment, a file, etc.
///     42
/// });
/// metrics.record_config_load(duration);
///
/// let (_broker, duration) = time_init!({
///     // Connect to the broker.
///     "redis"
/// });
/// metrics.record_broker_init(duration);
///
/// assert_eq!(metrics.total_ms, metrics.broker_init_ms + metrics.config_load_ms);
/// println!("{}", metrics.report());
/// ```
#[derive(Debug, Clone)]
pub struct StartupMetrics {
    /// Time spent initializing brokers
    pub broker_init_ms: u64,
    /// Time spent loading configuration
    pub config_load_ms: u64,
    /// Time spent connecting to backends
    pub backend_init_ms: u64,
    /// Total startup time (sum of the three stages above)
    pub total_ms: u64,
}

impl StartupMetrics {
    /// Create a new startup metrics tracker with every field at zero.
    pub fn new() -> Self {
        Self {
            broker_init_ms: 0,
            config_load_ms: 0,
            backend_init_ms: 0,
            total_ms: 0,
        }
    }

    /// Record how long broker initialization took and refresh `total_ms`.
    ///
    /// `duration` is typically the second element of the tuple returned by
    /// [`crate::time_init!`] wrapped around the broker-creation call.
    pub fn record_broker_init(&mut self, duration: std::time::Duration) {
        self.broker_init_ms = duration.as_millis() as u64;
        self.recompute_total();
    }

    /// Record how long configuration loading took and refresh `total_ms`.
    pub fn record_config_load(&mut self, duration: std::time::Duration) {
        self.config_load_ms = duration.as_millis() as u64;
        self.recompute_total();
    }

    /// Record how long backend initialization took and refresh `total_ms`.
    pub fn record_backend_init(&mut self, duration: std::time::Duration) {
        self.backend_init_ms = duration.as_millis() as u64;
        self.recompute_total();
    }

    /// Recompute `total_ms` as the sum of the three recorded stages.
    ///
    /// The `record_*` methods already call this automatically; it is
    /// exposed publicly only so callers who set the stage fields directly
    /// (bypassing `record_*`) can resynchronize `total_ms` afterward.
    pub fn recompute_total(&mut self) {
        self.total_ms = self
            .broker_init_ms
            .saturating_add(self.config_load_ms)
            .saturating_add(self.backend_init_ms);
    }

    /// Finish recording: resynchronize and return `total_ms`.
    ///
    /// Equivalent to calling [`recompute_total`](Self::recompute_total) and
    /// then reading `total_ms`, provided as a single convenience call for
    /// the end of a startup sequence.
    pub fn finish(&mut self) -> u64 {
        self.recompute_total();
        self.total_ms
    }

    /// Report the metrics as a formatted string
    pub fn report(&self) -> String {
        format!(
            "Startup Performance:\n\
             - Broker Init: {}ms\n\
             - Config Load: {}ms\n\
             - Backend Init: {}ms\n\
             - Total: {}ms",
            self.broker_init_ms, self.config_load_ms, self.backend_init_ms, self.total_ms
        )
    }
}

impl Default for StartupMetrics {
    fn default() -> Self {
        Self::new()
    }
}

/// Helper macro for timing initialization steps
///
/// Returns a `(result, duration)` tuple; feed `duration` into the matching
/// [`StartupMetrics`] `record_*` method to turn it into a persisted metric.
///
/// # Example
///
/// ```rust
/// use celers::time_init;
///
/// let (value, duration) = time_init!({
///     // Expensive initialization code
///     1 + 1
/// });
/// assert_eq!(value, 2);
/// println!("Initialization took {}ms", duration.as_millis());
/// ```
#[macro_export]
macro_rules! time_init {
    ($block:block) => {{
        let start = std::time::Instant::now();
        let result = $block;
        let duration = start.elapsed();
        (result, duration)
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    // All assertions below feed `record_*` explicit, synthetic `Duration`
    // values rather than measuring real wall-clock time, so these tests are
    // fully deterministic (no sleeping, no timing-dependent thresholds).

    #[test]
    fn new_and_default_start_at_zero() {
        let metrics = StartupMetrics::new();
        assert_eq!(metrics.broker_init_ms, 0);
        assert_eq!(metrics.config_load_ms, 0);
        assert_eq!(metrics.backend_init_ms, 0);
        assert_eq!(metrics.total_ms, 0);

        let default_metrics = StartupMetrics::default();
        assert_eq!(default_metrics.total_ms, 0);
    }

    #[test]
    fn record_broker_init_sets_field_and_total() {
        let mut metrics = StartupMetrics::new();
        metrics.record_broker_init(Duration::from_millis(150));
        assert_eq!(metrics.broker_init_ms, 150);
        assert_eq!(metrics.config_load_ms, 0);
        assert_eq!(metrics.backend_init_ms, 0);
        assert_eq!(metrics.total_ms, 150);
    }

    #[test]
    fn recording_all_three_stages_sums_into_total() {
        let mut metrics = StartupMetrics::new();
        metrics.record_broker_init(Duration::from_millis(120));
        metrics.record_config_load(Duration::from_millis(30));
        metrics.record_backend_init(Duration::from_millis(50));

        assert_eq!(metrics.broker_init_ms, 120);
        assert_eq!(metrics.config_load_ms, 30);
        assert_eq!(metrics.backend_init_ms, 50);
        // total_ms must never be left at the always-zero placeholder value
        // once any stage has been recorded.
        assert_eq!(metrics.total_ms, 200);
    }

    #[test]
    fn total_stays_in_sync_when_a_stage_is_re_recorded() {
        let mut metrics = StartupMetrics::new();
        metrics.record_broker_init(Duration::from_millis(100));
        metrics.record_config_load(Duration::from_millis(20));
        assert_eq!(metrics.total_ms, 120);

        // Re-recording an earlier stage (e.g. a retried broker connection)
        // must update the total, not just the individual field.
        metrics.record_broker_init(Duration::from_millis(300));
        assert_eq!(metrics.broker_init_ms, 300);
        assert_eq!(metrics.total_ms, 320);
    }

    #[test]
    fn finish_returns_and_matches_total_ms() {
        let mut metrics = StartupMetrics::new();
        metrics.record_broker_init(Duration::from_millis(10));
        metrics.record_config_load(Duration::from_millis(20));
        metrics.record_backend_init(Duration::from_millis(30));

        let finished = metrics.finish();
        assert_eq!(finished, 60);
        assert_eq!(metrics.total_ms, 60);
    }

    #[test]
    fn recompute_total_resyncs_after_direct_field_mutation() {
        let mut metrics = StartupMetrics::new();
        // Fields are `pub` for backward compatibility; a caller that pokes
        // them directly instead of using `record_*` can still resync.
        metrics.broker_init_ms = 7;
        metrics.config_load_ms = 8;
        metrics.backend_init_ms = 9;
        assert_eq!(
            metrics.total_ms, 0,
            "total_ms is not auto-synced on direct field writes"
        );

        metrics.recompute_total();
        assert_eq!(metrics.total_ms, 24);
    }

    #[test]
    fn report_reflects_recorded_values() {
        let mut metrics = StartupMetrics::new();
        metrics.record_broker_init(Duration::from_millis(11));
        metrics.record_config_load(Duration::from_millis(22));
        metrics.record_backend_init(Duration::from_millis(33));

        let report = metrics.report();
        assert!(report.contains("Broker Init: 11ms"));
        assert!(report.contains("Config Load: 22ms"));
        assert!(report.contains("Backend Init: 33ms"));
        assert!(report.contains("Total: 66ms"));
    }

    #[test]
    fn time_init_macro_integrates_with_record_methods() {
        // `time_init!` measures real elapsed time around a (here, trivial
        // and effectively instantaneous) block. Rather than asserting an
        // exact millisecond count -- which would make this test flaky --
        // compare the recorded field against the *actual* duration
        // `time_init!` returned, which holds deterministically regardless
        // of how fast the machine executing the test is.
        let (value, duration) = crate::time_init!({ 1 + 1 });
        assert_eq!(value, 2);

        let mut metrics = StartupMetrics::new();
        metrics.record_broker_init(duration);
        assert_eq!(metrics.broker_init_ms, duration.as_millis() as u64);
        assert_eq!(metrics.total_ms, metrics.broker_init_ms);
    }
}
