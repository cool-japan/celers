//! Worker struct and core implementation for task execution.

use crate::circuit_breaker::CircuitBreaker;
use crate::dlq::{self, DlqHandler};
use crate::middleware;
use crate::types::{DynamicConfig, WorkerConfig, WorkerHandle, WorkerMode, WorkerStats};

use celers_core::{
    Broker, Event, EventEmitter, NoOpEventEmitter, Result, TaskEventBuilder, TaskRegistry,
    TaskState, WorkerEventBuilder,
};

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration as StdDuration;
use tokio::sync::mpsc;
use tokio::time::{sleep, timeout, Duration, Instant};
use tracing::{debug, error, info, warn};

#[cfg(feature = "metrics")]
use celers_metrics::{
    TASKS_COMPLETED_BY_TYPE, TASKS_COMPLETED_TOTAL, TASKS_FAILED_BY_TYPE, TASKS_FAILED_TOTAL,
    TASKS_RETRIED_BY_TYPE, TASKS_RETRIED_TOTAL, TASK_EXECUTION_TIME, TASK_EXECUTION_TIME_BY_TYPE,
};

/// Worker runtime for consuming and executing tasks
pub struct Worker<B: Broker, E: EventEmitter = NoOpEventEmitter> {
    pub(crate) broker: Arc<B>,
    pub(crate) registry: Arc<TaskRegistry>,
    pub(crate) config: WorkerConfig,
    pub(crate) circuit_breaker: Option<Arc<CircuitBreaker>>,
    pub(crate) dlq_handler: Option<Arc<DlqHandler>>,
    pub(crate) shutdown_tx: Option<mpsc::Sender<()>>,
    pub(crate) event_emitter: Arc<E>,
    pub(crate) stats: Arc<WorkerStats>,
    pub(crate) mode: Arc<AtomicU8>, // Stores WorkerMode as u8
    pub(crate) dynamic_config: Arc<RwLock<DynamicConfig>>,
    pub(crate) middleware_stack: Option<Arc<middleware::MiddlewareStack>>,
}

impl<B: Broker + 'static> Worker<B, NoOpEventEmitter> {
    /// Create a new worker with default (no-op) event emitter
    pub fn new(broker: B, registry: TaskRegistry, config: WorkerConfig) -> Self {
        Worker::with_event_emitter(broker, registry, config, NoOpEventEmitter::new())
    }
}

impl<B: Broker + 'static, E: EventEmitter + 'static> Worker<B, E> {
    /// Create a new worker with a custom event emitter
    pub fn with_event_emitter(
        broker: B,
        registry: TaskRegistry,
        config: WorkerConfig,
        event_emitter: E,
    ) -> Self {
        let circuit_breaker = if config.enable_circuit_breaker {
            Some(Arc::new(CircuitBreaker::with_config(
                config.circuit_breaker_config.clone(),
            )))
        } else {
            None
        };

        let dlq_handler = if config.enable_dlq {
            Some(Arc::new(DlqHandler::new(config.dlq_config.clone())))
        } else {
            None
        };

        // Initialize dynamic config from static config
        let dynamic_config = DynamicConfig {
            poll_interval_ms: config.poll_interval_ms,
            default_timeout_secs: config.default_timeout_secs,
            max_retries: config.max_retries,
        };

        Self {
            broker: Arc::new(broker),
            registry: Arc::new(registry),
            config,
            circuit_breaker,
            dlq_handler,
            shutdown_tx: None,
            event_emitter: Arc::new(event_emitter),
            stats: Arc::new(WorkerStats::new()),
            mode: Arc::new(AtomicU8::new(WorkerMode::Normal as u8)),
            dynamic_config: Arc::new(RwLock::new(dynamic_config)),
            middleware_stack: None,
        }
    }

    /// Set a custom middleware stack
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_worker::{Worker, WorkerConfig, MiddlewareStack, TracingMiddleware};
    /// use celers_core::TaskRegistry;
    /// # use celers_core::Broker;
    /// # async fn example<B: Broker + 'static>(broker: B) {
    /// let registry = TaskRegistry::new();
    /// let config = WorkerConfig::default();
    ///
    /// let middleware = MiddlewareStack::new()
    ///     .add(TracingMiddleware::new(true));
    ///
    /// let worker = Worker::new(broker, registry, config)
    ///     .with_middleware(middleware);
    /// # }
    /// ```
    pub fn with_middleware(mut self, stack: middleware::MiddlewareStack) -> Self {
        self.middleware_stack = Some(Arc::new(stack));
        self
    }

    /// Get the worker statistics
    pub fn stats(&self) -> &WorkerStats {
        &self.stats
    }

    /// Get the DLQ handler (if enabled)
    pub fn dlq_handler(&self) -> Option<&Arc<DlqHandler>> {
        self.dlq_handler.as_ref()
    }

    /// Check if this worker can handle a specific task type based on routing configuration
    fn can_handle_task(&self, task_name: &str) -> bool {
        if !self.config.enable_routing {
            return true; // Routing disabled, accept all tasks
        }

        self.config.worker_tags.can_handle_task(task_name)
    }

    /// Start the worker loop with graceful shutdown support
    /// Returns a WorkerHandle that can be used to signal shutdown
    pub async fn run_with_shutdown(mut self) -> Result<WorkerHandle> {
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);
        let handle = WorkerHandle {
            shutdown_tx: shutdown_tx.clone(),
            mode: Arc::clone(&self.mode),
            stats: Arc::clone(&self.stats),
            dynamic_config: Arc::clone(&self.dynamic_config),
        };

        self.shutdown_tx = Some(shutdown_tx);

        tokio::spawn(async move {
            if let Err(e) = self.run_loop(Some(&mut shutdown_rx)).await {
                error!("Worker error: {}", e);
            }
        });

        Ok(handle)
    }

    /// Start the worker loop (blocks until shutdown or error)
    pub async fn run(&self) -> Result<()> {
        self.run_loop(None).await
    }

    /// Internal worker loop implementation
    async fn run_loop(&self, mut shutdown_rx: Option<&mut mpsc::Receiver<()>>) -> Result<()> {
        let hostname = self.config.hostname.clone();
        let pid = std::process::id();

        info!(
            "Starting worker with concurrency {} and max retries {}",
            self.config.concurrency, self.config.max_retries
        );

        // Emit worker online event
        if self.config.enable_events {
            let event = WorkerEventBuilder::new(&hostname).online();
            if let Err(e) = self.event_emitter.emit(event).await {
                warn!("Failed to emit worker-online event: {}", e);
            }
        }

        // Start heartbeat task if configured
        let heartbeat_handle =
            if self.config.enable_events && self.config.heartbeat_interval_secs > 0 {
                let heartbeat_hostname = hostname.clone();
                let heartbeat_interval = Duration::from_secs(self.config.heartbeat_interval_secs);
                let heartbeat_emitter = Arc::clone(&self.event_emitter);
                let heartbeat_stats = Arc::clone(&self.stats);
                let heartbeat_freq = self.config.heartbeat_interval_secs as f64;

                Some(tokio::spawn(async move {
                    Self::heartbeat_loop(
                        heartbeat_hostname,
                        heartbeat_interval,
                        heartbeat_emitter,
                        heartbeat_stats,
                        heartbeat_freq,
                    )
                    .await;
                }))
            } else {
                None
            };

        let result = self.run_loop_inner(&mut shutdown_rx, &hostname, pid).await;

        // Stop heartbeat task
        if let Some(handle) = heartbeat_handle {
            handle.abort();
        }

        // Emit worker offline event
        if self.config.enable_events {
            let event = WorkerEventBuilder::new(&hostname).offline();
            if let Err(e) = self.event_emitter.emit(event).await {
                warn!("Failed to emit worker-offline event: {}", e);
            }
        }

        result
    }

    /// Heartbeat loop that periodically emits worker-heartbeat events
    async fn heartbeat_loop<EE: EventEmitter>(
        hostname: String,
        interval: Duration,
        event_emitter: Arc<EE>,
        stats: Arc<WorkerStats>,
        freq: f64,
    ) {
        loop {
            sleep(interval).await;

            let active = stats.active() as u32;
            let processed = stats.processed();

            // Get system load average (on Unix systems)
            let loadavg = Self::get_load_average();

            let event =
                WorkerEventBuilder::new(&hostname).heartbeat(active, processed, loadavg, freq);

            if let Err(e) = event_emitter.emit(event).await {
                debug!("Failed to emit worker-heartbeat event: {}", e);
            }
        }
    }

    /// Get system load average (returns [0.0, 0.0, 0.0] on non-Unix systems)
    fn get_load_average() -> [f64; 3] {
        #[cfg(unix)]
        {
            use std::fs;
            if let Ok(contents) = fs::read_to_string("/proc/loadavg") {
                let parts: Vec<&str> = contents.split_whitespace().collect();
                if parts.len() >= 3 {
                    let load1 = parts[0].parse().unwrap_or(0.0);
                    let load5 = parts[1].parse().unwrap_or(0.0);
                    let load15 = parts[2].parse().unwrap_or(0.0);
                    return [load1, load5, load15];
                }
            }
            [0.0, 0.0, 0.0]
        }

        #[cfg(not(unix))]
        {
            [0.0, 0.0, 0.0]
        }
    }

    /// Inner worker loop (separated to ensure offline event is always emitted)
    async fn run_loop_inner(
        &self,
        shutdown_rx: &mut Option<&mut mpsc::Receiver<()>>,
        hostname: &str,
        pid: u32,
    ) -> Result<()> {
        loop {
            // Check current worker mode
            let current_mode = WorkerMode::from(self.mode.load(Ordering::SeqCst));

            // If draining, wait for active tasks to complete and exit
            if current_mode.is_draining() {
                info!("Worker in draining mode, waiting for active tasks to complete");
                while self.stats.active() > 0 {
                    debug!("Waiting for {} active tasks", self.stats.active());
                    sleep(Duration::from_millis(100)).await;
                }
                info!("All tasks completed, exiting");
                return Ok(());
            }

            // Check for shutdown signal if receiver is provided
            if let Some(ref mut rx) = shutdown_rx {
                match rx.try_recv() {
                    Ok(_) => {
                        info!("Shutdown signal received, stopping worker gracefully");
                        return Ok(());
                    }
                    Err(mpsc::error::TryRecvError::Disconnected) => {
                        warn!("Shutdown channel disconnected, stopping worker");
                        return Ok(());
                    }
                    Err(mpsc::error::TryRecvError::Empty) => {
                        // No shutdown signal, continue
                    }
                }
            }

            // If in maintenance mode, skip dequeuing and sleep
            if current_mode.is_maintenance() {
                let poll_interval = self
                    .dynamic_config
                    .read()
                    .map(|c| c.poll_interval_ms)
                    .unwrap_or(1000);
                debug!(
                    "Worker in maintenance mode, sleeping for {}ms",
                    poll_interval
                );
                sleep(Duration::from_millis(poll_interval)).await;
                continue;
            }

            // Dequeue tasks (single or batch depending on configuration)
            let messages_result = if self.config.enable_batch_dequeue {
                debug!(
                    "Batch dequeue enabled, fetching up to {} tasks",
                    self.config.batch_size
                );
                self.broker.dequeue_batch(self.config.batch_size).await
            } else {
                // Single task dequeue (convert to Vec for uniform handling)
                match self.broker.dequeue().await {
                    Ok(Some(msg)) => Ok(vec![msg]),
                    Ok(None) => Ok(vec![]),
                    Err(e) => Err(e),
                }
            };

            match messages_result {
                Ok(messages) if !messages.is_empty() => {
                    if self.config.enable_batch_dequeue {
                        info!("Dequeued {} tasks in batch", messages.len());
                    }

                    // Process each message
                    for msg in messages {
                        let task_id = msg.task.metadata.id;
                        let task_name = msg.task.metadata.name.clone();
                        info!("Processing task {} ({})", task_id, task_name);

                        // Emit task-received event
                        if self.config.enable_events {
                            let event = TaskEventBuilder::new(task_id, &task_name)
                                .hostname(hostname)
                                .pid(pid)
                                .received();
                            if let Err(e) = self.event_emitter.emit(event).await {
                                debug!("Failed to emit task-received event: {}", e);
                            }
                        }

                        // Check routing - can this worker handle this task type?
                        if !self.can_handle_task(&task_name) {
                            warn!(
                                "Worker routing: cannot handle task type '{}', rejecting task {}",
                                task_name, task_id
                            );

                            // Emit task-rejected event
                            if self.config.enable_events {
                                let event = Event::Task(celers_core::TaskEvent::Rejected {
                                    task_id,
                                    task_name: Some(task_name.clone()),
                                    hostname: hostname.to_string(),
                                    timestamp: chrono::Utc::now(),
                                    reason: "Worker routing mismatch".to_string(),
                                });
                                if let Err(e) = self.event_emitter.emit(event).await {
                                    debug!("Failed to emit task-rejected event: {}", e);
                                }
                            }

                            // Reject task with requeue (another worker might handle it)
                            if let Err(e) = self
                                .broker
                                .reject(&task_id, msg.receipt_handle.as_deref(), true)
                                .await
                            {
                                error!("Failed to reject task {}: {}", task_id, e);
                            }
                            continue;
                        }

                        // Check circuit breaker
                        if let Some(ref cb) = self.circuit_breaker {
                            if !cb.should_allow(&task_name).await {
                                warn!(
                                    "Circuit breaker OPEN for task type '{}', rejecting task {}",
                                    task_name, task_id
                                );

                                // Emit task-rejected event
                                if self.config.enable_events {
                                    let event = Event::Task(celers_core::TaskEvent::Rejected {
                                        task_id,
                                        task_name: Some(task_name.clone()),
                                        hostname: hostname.to_string(),
                                        timestamp: chrono::Utc::now(),
                                        reason: "Circuit breaker OPEN".to_string(),
                                    });
                                    if let Err(e) = self.event_emitter.emit(event).await {
                                        debug!("Failed to emit task-rejected event: {}", e);
                                    }
                                }

                                // Reject task without retrying
                                if let Err(e) = self
                                    .broker
                                    .reject(&task_id, msg.receipt_handle.as_deref(), false)
                                    .await
                                {
                                    error!("Failed to reject task {}: {}", task_id, e);
                                }
                                continue;
                            }
                        }

                        // Execute task with timeout (use dynamic config if task doesn't specify)
                        let default_timeout = self
                            .dynamic_config
                            .read()
                            .map(|c| c.default_timeout_secs)
                            .unwrap_or(300);
                        let timeout_secs =
                            msg.task.metadata.timeout_secs.unwrap_or(default_timeout);

                        let broker = Arc::clone(&self.broker);
                        let registry = Arc::clone(&self.registry);
                        let circuit_breaker = self.circuit_breaker.clone();
                        let receipt_handle = msg.receipt_handle.clone();
                        let task = msg.task.clone();
                        let event_emitter = Arc::clone(&self.event_emitter);
                        let enable_events = self.config.enable_events;
                        let hostname = hostname.to_string();
                        let stats = Arc::clone(&self.stats);
                        let middleware = self.middleware_stack.clone();
                        let dlq_handler = self.dlq_handler.clone();

                        tokio::spawn(async move {
                            let start_time = Instant::now();

                            // Track active task
                            stats.task_started();

                            // Create middleware context
                            let mut ctx = middleware::TaskContext {
                                task_id: task_id.to_string(),
                                task_name: task.metadata.name.clone(),
                                retry_count: match task.metadata.state {
                                    TaskState::Retrying(count) => count,
                                    _ => 0,
                                },
                                worker_name: hostname.clone(),
                                metadata: std::collections::HashMap::new(),
                            };

                            // Emit task-started event
                            if enable_events {
                                let event = TaskEventBuilder::new(task_id, &task.metadata.name)
                                    .hostname(&hostname)
                                    .pid(pid)
                                    .started();
                                if let Err(e) = event_emitter.emit(event).await {
                                    debug!("Failed to emit task-started event: {}", e);
                                }
                            }

                            // Call before_task middleware
                            if let Some(ref mw) = middleware {
                                if let Err(e) = mw.before_task(&mut ctx).await {
                                    warn!("Middleware before_task error: {}", e);
                                }
                            }

                            match timeout(
                                Duration::from_secs(timeout_secs),
                                registry.execute(&task),
                            )
                            .await
                            {
                                Ok(Ok(result)) => {
                                    // Task succeeded
                                    let duration = start_time.elapsed();
                                    info!(
                                        "Task {} completed successfully in {:?}",
                                        task_id, duration
                                    );
                                    debug!("Result size: {} bytes", result.len());

                                    // Parse result as JSON for middleware (best effort)
                                    let result_json = serde_json::from_slice(&result)
                                        .unwrap_or(serde_json::json!({"result": "binary"}));

                                    // Call after_task middleware
                                    if let Some(ref mw) = middleware {
                                        if let Err(e) = mw.after_task(&ctx, &result_json).await {
                                            warn!("Middleware after_task error: {}", e);
                                        }
                                    }

                                    // Emit task-succeeded event
                                    if enable_events {
                                        let event =
                                            TaskEventBuilder::new(task_id, &task.metadata.name)
                                                .hostname(&hostname)
                                                .pid(pid)
                                                .succeeded(duration.as_secs_f64());
                                        if let Err(e) = event_emitter.emit(event).await {
                                            debug!("Failed to emit task-succeeded event: {}", e);
                                        }
                                    }

                                    // Record success in circuit breaker
                                    if let Some(ref cb) = circuit_breaker {
                                        cb.record_success(&task.metadata.name).await;
                                    }

                                    #[cfg(feature = "metrics")]
                                    {
                                        TASKS_COMPLETED_TOTAL.inc();
                                        TASK_EXECUTION_TIME.observe(duration.as_secs_f64());

                                        // Track per-task-type metrics
                                        let task_name = &task.metadata.name;
                                        TASKS_COMPLETED_BY_TYPE
                                            .with_label_values(&[task_name])
                                            .inc();
                                        TASK_EXECUTION_TIME_BY_TYPE
                                            .with_label_values(&[task_name])
                                            .observe(duration.as_secs_f64());
                                    }

                                    if let Err(e) =
                                        broker.ack(&task_id, receipt_handle.as_deref()).await
                                    {
                                        error!("Failed to acknowledge task {}: {}", task_id, e);
                                    }
                                }
                                Ok(Err(e)) => {
                                    // Task failed
                                    let error_msg = e.to_string();
                                    error!("Task {} failed: {}", task_id, error_msg);

                                    // Check if we should retry
                                    let current_retry = match task.metadata.state {
                                        TaskState::Retrying(count) => count,
                                        _ => 0,
                                    };

                                    if current_retry < task.metadata.max_retries {
                                        // Requeue for retry
                                        warn!(
                                            "Requeuing task {} for retry {}/{}",
                                            task_id,
                                            current_retry + 1,
                                            task.metadata.max_retries
                                        );

                                        // Call on_retry middleware
                                        if let Some(ref mw) = middleware {
                                            if let Err(e) =
                                                mw.on_retry(&ctx, current_retry + 1).await
                                            {
                                                warn!("Middleware on_retry error: {}", e);
                                            }
                                        }

                                        // Emit task-retried event
                                        if enable_events {
                                            let event =
                                                TaskEventBuilder::new(task_id, &task.metadata.name)
                                                    .hostname(&hostname)
                                                    .pid(pid)
                                                    .retried(&error_msg, current_retry + 1);
                                            if let Err(e) = event_emitter.emit(event).await {
                                                debug!("Failed to emit task-retried event: {}", e);
                                            }
                                        }

                                        #[cfg(feature = "metrics")]
                                        {
                                            TASKS_RETRIED_TOTAL.inc();

                                            // Track per-task-type metrics
                                            let task_name = &task.metadata.name;
                                            TASKS_RETRIED_BY_TYPE
                                                .with_label_values(&[task_name])
                                                .inc();
                                        }

                                        if let Err(e) = broker
                                            .reject(&task_id, receipt_handle.as_deref(), true)
                                            .await
                                        {
                                            error!("Failed to requeue task {}: {}", task_id, e);
                                        }
                                    } else {
                                        // Max retries reached, permanently fail
                                        error!(
                                            "Task {} failed permanently after {} retries",
                                            task_id, current_retry
                                        );

                                        // Call on_error middleware
                                        if let Some(ref mw) = middleware {
                                            if let Err(e) = mw.on_error(&ctx, &error_msg).await {
                                                warn!("Middleware on_error error: {}", e);
                                            }
                                        }

                                        // Emit task-failed event
                                        if enable_events {
                                            let event =
                                                TaskEventBuilder::new(task_id, &task.metadata.name)
                                                    .hostname(&hostname)
                                                    .pid(pid)
                                                    .failed(&error_msg);
                                            if let Err(e) = event_emitter.emit(event).await {
                                                debug!("Failed to emit task-failed event: {}", e);
                                            }
                                        }

                                        // Record failure in circuit breaker
                                        if let Some(ref cb) = circuit_breaker {
                                            cb.record_failure(&task.metadata.name).await;
                                        }

                                        // Add to DLQ if enabled
                                        if let Some(ref dlq) = dlq_handler {
                                            let dlq_entry = dlq::DlqEntry::new(
                                                task.clone(),
                                                task_id,
                                                current_retry,
                                                error_msg.clone(),
                                                hostname.clone(),
                                            )
                                            .with_metadata("failure_type", "execution_error");

                                            if let Err(e) = dlq.add_entry(dlq_entry).await {
                                                warn!(
                                                    "Failed to add task {} to DLQ: {}",
                                                    task_id, e
                                                );
                                            }
                                        }

                                        #[cfg(feature = "metrics")]
                                        {
                                            TASKS_FAILED_TOTAL.inc();

                                            // Track per-task-type metrics
                                            let task_name = &task.metadata.name;
                                            TASKS_FAILED_BY_TYPE
                                                .with_label_values(&[task_name])
                                                .inc();
                                        }

                                        if let Err(e) = broker
                                            .reject(&task_id, receipt_handle.as_deref(), false)
                                            .await
                                        {
                                            error!("Failed to reject task {}: {}", task_id, e);
                                        }
                                    }
                                }
                                Err(_) => {
                                    // Timeout
                                    let error_msg =
                                        format!("Task timed out after {}s", timeout_secs);
                                    error!("Task {} timed out after {}s", task_id, timeout_secs);

                                    // Requeue if retries remaining
                                    let current_retry = match task.metadata.state {
                                        TaskState::Retrying(count) => count,
                                        _ => 0,
                                    };

                                    if current_retry < task.metadata.max_retries {
                                        // Call on_retry middleware
                                        if let Some(ref mw) = middleware {
                                            if let Err(e) =
                                                mw.on_retry(&ctx, current_retry + 1).await
                                            {
                                                warn!("Middleware on_retry error: {}", e);
                                            }
                                        }

                                        // Emit task-retried event
                                        if enable_events {
                                            let event =
                                                TaskEventBuilder::new(task_id, &task.metadata.name)
                                                    .hostname(&hostname)
                                                    .pid(pid)
                                                    .retried(&error_msg, current_retry + 1);
                                            if let Err(e) = event_emitter.emit(event).await {
                                                debug!("Failed to emit task-retried event: {}", e);
                                            }
                                        }

                                        #[cfg(feature = "metrics")]
                                        {
                                            TASKS_RETRIED_TOTAL.inc();

                                            // Track per-task-type metrics
                                            let task_name = &task.metadata.name;
                                            TASKS_RETRIED_BY_TYPE
                                                .with_label_values(&[task_name])
                                                .inc();
                                        }

                                        if let Err(e) = broker
                                            .reject(&task_id, receipt_handle.as_deref(), true)
                                            .await
                                        {
                                            error!("Failed to requeue task {}: {}", task_id, e);
                                        }
                                    } else {
                                        // Call on_error middleware
                                        if let Some(ref mw) = middleware {
                                            if let Err(e) = mw.on_error(&ctx, &error_msg).await {
                                                warn!("Middleware on_error error: {}", e);
                                            }
                                        }

                                        // Record timeout failure in circuit breaker
                                        if let Some(ref cb) = circuit_breaker {
                                            cb.record_failure(&task.metadata.name).await;
                                        }

                                        // Add to DLQ if enabled
                                        if let Some(ref dlq) = dlq_handler {
                                            let dlq_entry = dlq::DlqEntry::new(
                                                task.clone(),
                                                task_id,
                                                current_retry,
                                                error_msg.clone(),
                                                hostname.clone(),
                                            )
                                            .with_metadata("failure_type", "timeout")
                                            .with_metadata(
                                                "timeout_secs",
                                                timeout_secs.to_string(),
                                            );

                                            if let Err(e) = dlq.add_entry(dlq_entry).await {
                                                warn!(
                                                    "Failed to add task {} to DLQ: {}",
                                                    task_id, e
                                                );
                                            }
                                        }

                                        // Emit task-failed event
                                        if enable_events {
                                            let event =
                                                TaskEventBuilder::new(task_id, &task.metadata.name)
                                                    .hostname(&hostname)
                                                    .pid(pid)
                                                    .failed(&error_msg);
                                            if let Err(e) = event_emitter.emit(event).await {
                                                debug!("Failed to emit task-failed event: {}", e);
                                            }
                                        }

                                        #[cfg(feature = "metrics")]
                                        {
                                            TASKS_FAILED_TOTAL.inc();

                                            // Track per-task-type metrics
                                            let task_name = &task.metadata.name;
                                            TASKS_FAILED_BY_TYPE
                                                .with_label_values(&[task_name])
                                                .inc();
                                        }

                                        if let Err(e) = broker
                                            .reject(&task_id, receipt_handle.as_deref(), false)
                                            .await
                                        {
                                            error!("Failed to reject task {}: {}", task_id, e);
                                        }
                                    }
                                }
                            };

                            // Track task completion
                            stats.task_completed();
                        });
                    }
                }
                Ok(_) => {
                    // Queue is empty (messages vec is empty), sleep before next poll
                    let poll_interval = self
                        .dynamic_config
                        .read()
                        .map(|c| c.poll_interval_ms)
                        .unwrap_or(1000);
                    debug!("Queue empty, sleeping for {}ms", poll_interval);
                    sleep(Duration::from_millis(poll_interval)).await;
                }
                Err(e) => {
                    let poll_interval = self
                        .dynamic_config
                        .read()
                        .map(|c| c.poll_interval_ms)
                        .unwrap_or(1000);
                    error!("Error dequeueing tasks: {}", e);
                    sleep(Duration::from_millis(poll_interval)).await;
                }
            }
        }
    }

    /// Calculate exponential backoff delay
    #[allow(dead_code)]
    pub(crate) fn calculate_backoff_delay(&self, retry_count: u32) -> StdDuration {
        let delay_ms = self.config.retry_base_delay_ms * 2_u64.pow(retry_count);
        let delay_ms = delay_ms.min(self.config.retry_max_delay_ms);
        StdDuration::from_millis(delay_ms)
    }
}
