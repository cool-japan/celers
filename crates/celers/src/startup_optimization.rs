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

/// Pre-compiled pattern cache for faster startup
///
/// Note: For regex caching, add the `regex` crate to your dependencies
/// and implement a similar pattern using `OnceLock<Mutex<HashMap<String, &'static Regex>>>`.
///
/// # Example
///
/// ```rust,ignore
/// use std::sync::OnceLock;
/// use std::collections::HashMap;
/// use std::sync::Mutex;
/// use regex::Regex;
///
/// pub fn cached_regex(pattern: &str) -> &'static Regex {
///     static CACHE: OnceLock<Mutex<HashMap<String, &'static Regex>>> = OnceLock::new();
///     let cache = CACHE.get_or_init(|| Mutex::new(HashMap::new()));
///     let mut cache = cache.lock().expect("lock should not be poisoned");
///
///     if let Some(regex) = cache.get(pattern) {
///         return regex;
///     }
///
///     let regex = Box::leak(Box::new(Regex::new(pattern).expect("Invalid regex")));
///     cache.insert(pattern.to_string(), regex);
///     regex
/// }
/// ```
#[allow(dead_code)]
fn _cached_pattern_example() {
    // This is a documentation placeholder
}

/// Parallel initialization helper for running multiple initialization tasks concurrently
///
/// # Example
///
/// ```rust,no_run
/// use celers::startup_optimization::parallel_init;
///
/// # async fn example() {
/// let results = parallel_init(vec![
///     Box::new(|| Box::pin(async { /* Initialize DB */ Ok::<(), String>(()) })),
///     Box::new(|| Box::pin(async { /* Connect to broker */ Ok::<(), String>(()) })),
///     Box::new(|| Box::pin(async { /* Load config */ Ok::<(), String>(()) })),
/// ]).await;
/// # }
/// ```
pub async fn parallel_init<T, E>(tasks: Vec<AsyncInitTask<T, E>>) -> Vec<Result<T, E>>
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
                // Handle join error - convert to result type
                // For now, we'll panic as this indicates a serious issue
                panic!("Task panicked: {:?}", e);
            }
        }
    }
    results
}

/// Startup performance metrics
#[derive(Debug, Clone)]
pub struct StartupMetrics {
    /// Time spent initializing brokers
    pub broker_init_ms: u64,
    /// Time spent loading configuration
    pub config_load_ms: u64,
    /// Time spent connecting to backends
    pub backend_init_ms: u64,
    /// Total startup time
    pub total_ms: u64,
}

impl StartupMetrics {
    /// Create a new startup metrics tracker
    pub fn new() -> Self {
        Self {
            broker_init_ms: 0,
            config_load_ms: 0,
            backend_init_ms: 0,
            total_ms: 0,
        }
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
/// # Example
///
/// ```rust,ignore
/// use celers::time_init;
///
/// let duration = time_init!({
///     // Expensive initialization code
///     initialize_database().await
/// });
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
