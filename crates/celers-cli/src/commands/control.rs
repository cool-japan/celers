//! Remote worker control and inspection commands.
//!
//! These drive `CeleRS`'s remote control protocol
//! ([`celers_core::control`]) over Redis Pub/Sub: a command is broadcast on a
//! control channel every running worker subscribes to, and replies are gathered
//! from a per-request reply channel until a deadline.
//!
//! # Reading the output
//!
//! A control broadcast cannot know how many workers exist, so "no replies" is a
//! normal answer, not an error — it means no worker answered within the
//! timeout. Every command therefore prints how many workers replied.
//!
//! # Revoke is not only a broadcast
//!
//! [`revoke_tasks`] records the revocation in the queue's durable revoked-id set
//! *before* broadcasting, so it works with no worker running and reaches workers
//! that have no control channel. Every other command here is a broadcast and
//! nothing more: a worker that is not listening does not act on it.
//!
//! # Not `celery -A app control`
//!
//! The channel and message format are `CeleRS`-native. A Python Celery worker
//! (kombu pidbox) will not answer these commands, and `celery inspect` will not
//! see a `CeleRS` worker. See [`celers_core::control_transport`] for the wire
//! format.

use anyhow::{Context, Result};
use celers_broker_redis::{QueueMode, RedisBroker, RedisControlTransport};
use celers_core::control::{
    ControlCommand, ControlResponse, InspectCommand, InspectResponse, QueueResponse,
};
use celers_core::control_transport::{ControlClient, ControlReply, ControlTransport};
use celers_core::Broker;
use colored::Colorize;
use std::sync::Arc;
use std::time::Duration;

/// Default seconds to gather replies when the caller does not say.
pub const DEFAULT_CONTROL_TIMEOUT_SECS: f64 = 2.0;

/// The connection and output options every control/inspect command shares.
#[derive(Debug, Clone)]
pub struct ControlOptions {
    /// Redis URL carrying the control channel.
    pub broker_url: String,
    /// Control channel name; `None` uses
    /// [`celers_core::control_transport::DEFAULT_CONTROL_CHANNEL`].
    pub channel: Option<String>,
    /// How long to gather replies, in seconds.
    pub timeout_secs: f64,
    /// Restrict the command to these worker hostnames (empty = all workers).
    pub destination: Vec<String>,
    /// Print the raw replies as JSON instead of a rendered summary.
    pub json: bool,
}

impl ControlOptions {
    /// Options pointing at `broker_url` with every default.
    #[must_use]
    pub fn new(broker_url: impl Into<String>) -> Self {
        Self {
            broker_url: broker_url.into(),
            channel: None,
            timeout_secs: DEFAULT_CONTROL_TIMEOUT_SECS,
            destination: Vec::new(),
            json: false,
        }
    }

    /// The gather deadline as a [`Duration`], with nonsense values clamped.
    ///
    /// A zero or negative timeout would return before any worker could
    /// possibly answer, which reads as "the cluster is down".
    fn timeout(&self) -> Duration {
        if self.timeout_secs.is_finite() && self.timeout_secs > 0.0 {
            Duration::from_secs_f64(self.timeout_secs.min(3600.0))
        } else {
            Duration::from_secs_f64(DEFAULT_CONTROL_TIMEOUT_SECS)
        }
    }
}

/// Build a control client from the given options.
fn client(options: &ControlOptions) -> Result<ControlClient> {
    let transport = match options.channel {
        Some(ref channel) => RedisControlTransport::with_channel(&options.broker_url, channel),
        None => RedisControlTransport::new(&options.broker_url),
    }
    .with_context(|| {
        format!(
            "failed to open the control channel on {}",
            options.broker_url
        )
    })?;

    let mut client = ControlClient::new(Arc::new(transport) as Arc<dyn ControlTransport>)
        .with_timeout(options.timeout());
    if !options.destination.is_empty() {
        client = client.with_destination(options.destination.clone());
    }
    Ok(client)
}

/// Broadcast `command` and render the replies.
///
/// # Errors
///
/// Returns an error if the control channel cannot be opened or the broadcast
/// fails. Zero replies is a successful outcome, not an error.
pub async fn run_control(options: &ControlOptions, command: ControlCommand) -> Result<()> {
    let label = describe(&command);
    let replies = client(options)?
        .broadcast(command)
        .await
        .with_context(|| format!("failed to broadcast '{label}'"))?;

    if options.json {
        return print_json(&replies);
    }

    print_header(&label, &replies);
    for reply in &replies {
        print_response(&reply.hostname, &reply.response);
    }
    Ok(())
}

/// Run an inspect command and render the replies.
///
/// # Errors
///
/// Returns an error if the control channel cannot be opened or the broadcast
/// fails.
pub async fn run_inspect(options: &ControlOptions, command: InspectCommand) -> Result<()> {
    run_control(options, ControlCommand::Inspect(command)).await
}

/// Revoke `task_ids`, durably and then over the control channel.
///
/// Two steps, in this order, because neither alone is a revocation:
///
/// 1. **Record it in the broker.** [`Broker::revoke`] writes the id into the
///    queue's persisted revoked-id set (with a TTL), removes pending copies,
///    and publishes a revocation notice on the queue's channel. This is what
///    makes the revocation stick when *no worker is running* — a control
///    broadcast into an empty cluster revokes nothing at all — and what reaches
///    a worker wired up with `Worker::with_broker_revocation` but no control
///    channel.
/// 2. **Broadcast the control command.** Workers subscribed to the control
///    channel act immediately and reply, so the operator sees which ones did.
///
/// The revoked set is scoped to one queue, so `queue` must be the queue the
/// task was enqueued on; it is printed, because a mismatch would otherwise look
/// exactly like a successful revocation.
///
/// # Errors
///
/// Returns an error if the broker cannot be reached (the durable record is the
/// half that must not fail silently) or if the broadcast fails.
pub async fn revoke_tasks(
    options: &ControlOptions,
    queue: &str,
    queue_mode: &str,
    task_ids: &[uuid::Uuid],
    terminate: bool,
) -> Result<()> {
    let broker = RedisBroker::with_mode(&options.broker_url, queue, parse_queue_mode(queue_mode))
        .with_context(|| {
        format!(
            "failed to open queue '{queue}' on {} to record the revocation",
            options.broker_url
        )
    })?;

    for task_id in task_ids {
        broker
            .revoke(task_id, terminate)
            .await
            .with_context(|| format!("failed to record the revocation of {task_id}"))?;
    }

    if !options.json {
        // Never on stdout under `--json`: the output there must stay parseable.
        println!(
            "{} recorded {} revocation(s) in queue '{}' (durable: a worker that starts \
             later still refuses {})",
            "✓".green().bold(),
            task_ids.len(),
            queue.cyan(),
            if task_ids.len() == 1 { "it" } else { "them" }
        );
        if terminate {
            println!(
                "  {}",
                "--terminate: workers already running these tasks are asked to abort them".dimmed()
            );
        }
    }

    let command = if task_ids.len() == 1 {
        // A single id gets the single-id command so a worker's reply names the
        // task it acted on.
        ControlCommand::revoke(task_ids[0], terminate)
    } else {
        ControlCommand::bulk_revoke(task_ids.to_vec(), terminate)
    };
    run_control(options, command).await
}

/// The queue mode named by a configuration string.
///
/// Anything other than `priority` is FIFO, matching `celers worker`'s own
/// parsing — the two must agree, or the revocation would scan the wrong
/// structure for pending copies.
fn parse_queue_mode(mode: &str) -> QueueMode {
    if mode.eq_ignore_ascii_case("priority") {
        QueueMode::Priority
    } else {
        QueueMode::Fifo
    }
}

/// Ping every reachable worker.
///
/// # Errors
///
/// Returns an error if the control channel cannot be opened or the broadcast
/// fails.
pub async fn ping_workers(options: &ControlOptions) -> Result<()> {
    // The reply address is filled in by the client; the placeholder is never
    // put on the wire.
    run_control(options, ControlCommand::ping(String::new())).await
}

/// Print the raw replies as JSON.
fn print_json(replies: &[ControlReply]) -> Result<()> {
    let rendered =
        serde_json::to_string_pretty(replies).context("failed to render replies as JSON")?;
    println!("{rendered}");
    Ok(())
}

/// Print how many workers answered.
fn print_header(label: &str, replies: &[ControlReply]) {
    if replies.is_empty() {
        println!(
            "{} no worker answered '{}' within the timeout",
            "!".yellow().bold(),
            label
        );
        println!(
            "  {}",
            "(a control broadcast reaches only workers that are running and subscribed)".dimmed()
        );
        return;
    }
    println!(
        "{} {} worker(s) answered '{}'",
        "✓".green().bold(),
        replies.len(),
        label
    );
}

/// Render one worker's response.
fn print_response(hostname: &str, response: &ControlResponse) {
    match *response {
        ControlResponse::Pong {
            ref hostname,
            timestamp,
        } => {
            println!("  {} pong (clock {timestamp:.3})", hostname.cyan().bold());
        }
        ControlResponse::Ack { ok, ref message } => {
            let marker = if ok {
                "ok".green().to_string()
            } else {
                "failed".red().to_string()
            };
            println!("  {} {marker}", hostname.cyan().bold());
            if let Some(ref message) = *message {
                println!("      {message}");
            }
        }
        ControlResponse::Error { ref error } => {
            println!("  {} {}", hostname.cyan().bold(), "error".red().bold());
            println!("      {error}");
        }
        ControlResponse::Queue(ref queue) => {
            println!("  {}", hostname.cyan().bold());
            print_queue(queue);
        }
        ControlResponse::Inspect(ref payload) => {
            println!("  {}", hostname.cyan().bold());
            print_inspect(payload);
        }
    }
}

/// Render one worker's queue-operation result.
fn print_queue(response: &QueueResponse) {
    match *response {
        QueueResponse::Length {
            ref queue,
            message_count,
        } => println!("      {queue}: {message_count} message(s)"),
        QueueResponse::Purged {
            ref queue,
            message_count,
        } => println!("      {queue}: purged {message_count} message(s)"),
        QueueResponse::Deleted { ref queue } => println!("      {queue}: deleted"),
        QueueResponse::Bound {
            ref queue,
            ref exchange,
            ref routing_key,
        } => println!("      {queue}: bound to {exchange} via '{routing_key}'"),
        QueueResponse::Unbound {
            ref queue,
            ref exchange,
            ref routing_key,
        } => println!("      {queue}: unbound from {exchange} via '{routing_key}'"),
        QueueResponse::Declared {
            ref queue,
            message_count,
            consumer_count,
        } => println!(
            "      {queue}: declared ({message_count} message(s), {consumer_count} consumer(s))"
        ),
    }
}

/// Render one worker's inspection payload.
#[allow(clippy::too_many_lines)]
fn print_inspect(payload: &InspectResponse) {
    match *payload {
        InspectResponse::Active(ref tasks) => {
            if tasks.is_empty() {
                println!("      {}", "(no active tasks)".dimmed());
            }
            for task in tasks {
                println!("      {} {}", task.id, task.name.bold());
                if !task.args.is_empty() {
                    println!("        args: {}", task.args);
                }
                println!("        started: {:.3}", task.started);
                if let Some(ref delivery) = task.delivery_info {
                    println!("        queue: {}", delivery.queue);
                }
            }
        }
        InspectResponse::Scheduled(ref tasks) => {
            if tasks.is_empty() {
                println!(
                    "      {}",
                    "(none: CeleRS holds ETA tasks in the broker's delayed queue, \
                     not in the worker)"
                        .dimmed()
                );
            }
            for task in tasks {
                println!("      {} {} eta {:.3}", task.id, task.name, task.eta);
            }
        }
        InspectResponse::Reserved(ref tasks) => {
            if tasks.is_empty() {
                println!(
                    "      {}",
                    "(none: a CeleRS worker dispatches what it dequeues instead of \
                     holding a prefetch reserve)"
                        .dimmed()
                );
            }
            for task in tasks {
                println!("      {} {}", task.id, task.name);
            }
        }
        InspectResponse::Revoked(ref ids) => {
            if ids.is_empty() {
                println!("      {}", "(no revoked task ids)".dimmed());
            }
            for id in ids {
                println!("      {id}");
            }
        }
        InspectResponse::Registered(ref names) => {
            if names.is_empty() {
                println!("      {}", "(no registered tasks)".dimmed());
            }
            for name in names {
                println!("      {name}");
            }
        }
        InspectResponse::Stats(ref stats) => {
            println!(
                "      tasks: {} total, {} active, {} succeeded, {} failed, {} retried",
                stats.total_tasks, stats.active_tasks, stats.succeeded, stats.failed, stats.retried
            );
            println!("      uptime: {:.0}s", stats.uptime);
            if let Some(load) = stats.loadavg {
                println!(
                    "      loadavg: {:.2} {:.2} {:.2}",
                    load[0], load[1], load[2]
                );
            }
            if let Some(memory) = stats.memory_usage {
                println!("      memory: {} bytes", memory);
            }
            if let Some(ref pool) = stats.pool {
                println!(
                    "      pool: {} ({} available of {})",
                    pool.pool_type, pool.available, pool.max_concurrency
                );
            }
            if let Some(ref broker) = stats.broker {
                println!("      broker: {} ({})", broker.url, broker.transport);
            }
        }
        InspectResponse::QueueInfo(ref queues) => {
            let mut names: Vec<&String> = queues.keys().collect();
            names.sort();
            for name in names {
                if let Some(stats) = queues.get(name) {
                    println!(
                        "      {}: {} message(s), {} consumer(s)",
                        name, stats.messages, stats.consumers
                    );
                }
            }
        }
        InspectResponse::Report(ref report) => {
            println!(
                "      {} {} {}",
                report.sw_sys, report.sw_ver, report.hostname
            );
            println!(
                "      tasks: {} total, {} active, {} registered",
                report.stats.total_tasks,
                report.stats.active_tasks,
                report.registered.len()
            );
            for task in &report.active {
                println!("      active: {} {}", task.id, task.name);
            }
        }
        InspectResponse::Conf(ref conf) => {
            println!("      hostname: {}", conf.hostname);
            println!("      broker: {}", conf.broker_url);
            if let Some(ref backend) = conf.result_backend {
                println!("      result backend: {backend}");
            }
            println!("      queue: {}", conf.default_queue);
            println!("      concurrency: {}", conf.concurrency);
            println!("      prefetch: {}", conf.prefetch_multiplier);
            println!("      acks late: {}", conf.task_acks_late);
            if let Some(limit) = conf.task_time_limit {
                println!("      default time limit: {limit}s");
            }
        }
        InspectResponse::CircuitBreakers(ref states) => {
            if states.is_empty() {
                println!(
                    "      {}",
                    "(no circuit breakers: the worker runs without one)".dimmed()
                );
            }
            let mut names: Vec<&String> = states.keys().collect();
            names.sort();
            for name in names {
                if let Some(state) = states.get(name) {
                    println!("      {name}: {state}");
                }
            }
        }
    }
}

/// A short human label for a command, used in output and error context.
fn describe(command: &ControlCommand) -> String {
    match *command {
        ControlCommand::Ping { .. } => "ping".to_string(),
        ControlCommand::Inspect(ref inspect) => format!("inspect {}", describe_inspect(inspect)),
        ControlCommand::Shutdown { .. } => "shutdown".to_string(),
        ControlCommand::Revoke { task_id, .. } => format!("revoke {task_id}"),
        ControlCommand::BulkRevoke { ref task_ids, .. } => {
            format!("revoke {} task(s)", task_ids.len())
        }
        ControlCommand::RevokeByPattern { ref pattern, .. } => {
            format!("revoke pattern '{pattern}'")
        }
        ControlCommand::RateLimit { ref task_name, .. } => format!("rate-limit {task_name}"),
        ControlCommand::TimeLimit { ref task_name, .. } => format!("time-limit {task_name}"),
        ControlCommand::AddConsumer { ref queue } => format!("add-consumer {queue}"),
        ControlCommand::CancelConsumer { ref queue } => format!("cancel-consumer {queue}"),
        ControlCommand::Queue(ref queue) => format!("queue {}", queue.queue_name()),
        ControlCommand::ResetCircuitBreaker { ref task_name } => match *task_name {
            Some(ref name) => format!("reset-circuit-breaker {name}"),
            None => "reset-circuit-breaker (all)".to_string(),
        },
    }
}

/// A short human label for an inspect sub-command.
fn describe_inspect(command: &InspectCommand) -> &'static str {
    match *command {
        InspectCommand::Active => "active",
        InspectCommand::Scheduled => "scheduled",
        InspectCommand::Reserved => "reserved",
        InspectCommand::Revoked => "revoked",
        InspectCommand::Registered => "registered",
        InspectCommand::Stats => "stats",
        InspectCommand::QueueInfo => "queues",
        InspectCommand::Report => "report",
        InspectCommand::Conf => "conf",
        InspectCommand::CircuitBreakers => "circuit-breakers",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn timeout_falls_back_on_a_nonsense_value() {
        let mut options = ControlOptions::new("redis://127.0.0.1:6379");
        assert_eq!(
            options.timeout(),
            Duration::from_secs_f64(DEFAULT_CONTROL_TIMEOUT_SECS)
        );

        options.timeout_secs = 5.0;
        assert_eq!(options.timeout(), Duration::from_secs(5));

        // A zero deadline would return before any worker could answer, which
        // reads to an operator as "the cluster is down".
        for bad in [0.0, -1.0, f64::NAN] {
            options.timeout_secs = bad;
            assert_eq!(
                options.timeout(),
                Duration::from_secs_f64(DEFAULT_CONTROL_TIMEOUT_SECS),
                "a timeout of {bad} must fall back to the default"
            );
        }

        // And an absurd one is capped rather than parking the CLI for a week.
        options.timeout_secs = 1e9;
        assert_eq!(options.timeout(), Duration::from_secs(3600));
    }

    #[test]
    fn client_rejects_a_url_it_cannot_parse() {
        let options = ControlOptions::new("not-a-redis-url");
        assert!(client(&options).is_err());
    }

    #[test]
    fn every_command_has_a_label() {
        let id = Uuid::new_v4();
        let cases = vec![
            ControlCommand::ping(String::new()),
            ControlCommand::inspect_active(),
            ControlCommand::inspect_circuit_breakers(),
            ControlCommand::shutdown(Some(10)),
            ControlCommand::revoke(id, true),
            ControlCommand::bulk_revoke(vec![id], false),
            ControlCommand::revoke_by_pattern("report.*", true),
            ControlCommand::rate_limit("send_email", Some(10.0)),
            ControlCommand::time_limit("send_email", Some(10), Some(30)),
            ControlCommand::add_consumer("celery"),
            ControlCommand::cancel_consumer("celery"),
            ControlCommand::queue_length("celery"),
            ControlCommand::reset_circuit_breaker(None),
        ];
        for command in cases {
            let label = describe(&command);
            assert!(!label.is_empty(), "unlabelled command: {command:?}");
        }
    }

    #[test]
    fn inspect_labels_match_the_subcommand_names() {
        assert_eq!(describe_inspect(&InspectCommand::Active), "active");
        assert_eq!(describe_inspect(&InspectCommand::QueueInfo), "queues");
        assert_eq!(
            describe_inspect(&InspectCommand::CircuitBreakers),
            "circuit-breakers"
        );
    }

    #[test]
    fn destination_is_only_applied_when_set() {
        // A default (empty) destination must stay a broadcast: passing an empty
        // list would still be a broadcast, but the intent is worth pinning.
        let options = ControlOptions::new("redis://127.0.0.1:6379");
        assert!(options.destination.is_empty());
        assert!(client(&options).is_ok());

        let targeted = ControlOptions {
            destination: vec!["worker-1".to_string()],
            ..ControlOptions::new("redis://127.0.0.1:6379")
        };
        assert!(client(&targeted).is_ok());
    }
}
