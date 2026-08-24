//! Interactive REPL mode for CeleRS CLI.
//!
//! Provides an interactive shell for running multiple commands without restarting
//! the CLI. Features include command history, tab completion, and session state.

use anyhow::Result;
use colored::Colorize;
use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;
use rustyline::{Editor, Result as RustylineResult};

use crate::cache::CacheStats;
use crate::commands::queue::{queue_cache_stats, worker_cache_stats};
use crate::config::Config;
use crate::pool::redis_connection_pool;

/// `(name-and-aliases, description)` pairs for every command recognized by
/// [`InteractiveSession::process_command`], in the order [`print_help`]
/// displays them.
///
/// This is the single source of truth for the REPL's command surface:
/// [`InteractiveSession::print_help`] renders it directly, and
/// [`known_command_names`] derives the primary/canonical name list from it
/// for the unknown-command "did you mean" suggestion, so the two never
/// drift apart into independently maintained lists.
///
/// [`print_help`]: InteractiveSession::print_help
const COMMAND_HELP: &[(&str, &str)] = &[
    ("status, st", "Show queue status"),
    ("queues, ls", "List all queues"),
    ("workers, w", "List all workers"),
    ("health, h", "Run health diagnostics"),
    ("doctor, d", "Automatic problem detection"),
    ("metrics, m", "Display metrics"),
    ("stats, cs", "Show connection pool & cache statistics"),
    ("dlq inspect [limit]", "Inspect DLQ tasks"),
    ("dlq clear", "Clear all DLQ tasks"),
    ("use <queue>", "Switch to different queue"),
    ("broker [url]", "Show/set broker URL"),
    ("clear, cls", "Clear screen"),
    ("help, ?", "Show this help"),
    ("exit, quit, q", "Exit interactive mode"),
];

/// The primary/canonical command name for each [`COMMAND_HELP`] entry (the
/// leading token of its name-and-aliases column, before any comma or
/// argument placeholder) — e.g. `"status, st"` yields `"status"` and
/// `"dlq inspect [limit]"` yields `"dlq"`.
///
/// Used as the candidate list for
/// [`smart_defaults::context_suggestion`](crate::smart_defaults::context_suggestion)
/// when [`InteractiveSession::process_command`] sees an unrecognized
/// command.
fn known_command_names() -> Vec<String> {
    let mut names: Vec<String> = COMMAND_HELP
        .iter()
        .map(|(names, _desc)| {
            names
                .split(|c: char| c == ',' || c.is_whitespace())
                .next()
                .unwrap_or(names)
                .to_string()
        })
        .collect();
    names.sort();
    names.dedup();
    names
}

/// Whether REPL history (both loading a previous `~/.celers_history` on
/// startup and persisting the current session's commands to it on exit) is
/// disabled for this process, via the `CELERS_NO_HISTORY` environment
/// variable.
///
/// Follows the common Unix convention (e.g. `NO_COLOR`) of treating the
/// variable's mere presence as "on", regardless of its value.
fn history_disabled() -> bool {
    std::env::var_os("CELERS_NO_HISTORY").is_some()
}

/// Redact credentials from every URL-shaped token in `line` before it is
/// recorded in REPL history.
///
/// The REPL's own `broker <url>` command invites pasting a connection
/// string, which commonly embeds credentials
/// (`redis://user:pass@host`, `postgres://user:pass@host/db`); rustyline
/// persists whatever is passed to `add_history_entry` verbatim to
/// `~/.celers_history` on exit, with no redaction of its own (idx 348).
///
/// Every whitespace-separated token containing `://` is treated as a URL
/// and passed through [`crate::commands::utils::mask_password`], so
/// `broker redis://user:pass@host` is recorded as `broker
/// redis://user:****@host`. This covers the `broker <url>` command
/// specifically -- the only place a full connection string is typed at the
/// prompt today -- without hardcoding a check on the command name, so any
/// future command accepting a credentialed URL is covered automatically.
fn redact_line_for_history(line: &str) -> String {
    line.split_whitespace()
        .map(|token| {
            if token.contains("://") {
                crate::commands::utils::mask_password(token)
            } else {
                token.to_string()
            }
        })
        .collect::<Vec<_>>()
        .join(" ")
}

/// Restrict `path`'s file permissions to owner-read/write only (`0600`) on
/// Unix, best-effort. Called immediately after saving REPL history, which
/// may now contain a broker URL's masked-but-still-partially-identifying
/// host/user (idx 348) -- there is inherently a brief window between
/// rustyline creating the file and this call where it may carry more
/// permissive default permissions, since rustyline's `save_history` does
/// not itself accept a mode, but this closes that window as tightly as
/// possible without forking rustyline.
#[cfg(unix)]
fn restrict_history_file_permissions(path: &std::path::Path) {
    use std::os::unix::fs::PermissionsExt;
    let _ = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600));
}

#[cfg(not(unix))]
fn restrict_history_file_permissions(_path: &std::path::Path) {}

/// Format one [`CacheStats`] snapshot as an indented `label: ...` line.
///
/// Pulled out as a pure, unit-testable formatter -- as opposed to a
/// `println!` baked directly into
/// [`InteractiveSession::print_cache_pool_stats`] -- so its exact field
/// layout can be asserted on without capturing stdout.
fn format_cache_stats_line(label: &str, stats: &CacheStats) -> String {
    let label_col = format!("{label}:");
    format!(
        "  {label_col:<16} len={} hits={} misses={} hit_ratio={:.1}%",
        stats.len,
        stats.hits,
        stats.misses,
        stats.hit_ratio() * 100.0
    )
}

/// Interactive REPL session state
pub struct InteractiveSession {
    /// Command line editor with history
    editor: Editor<(), DefaultHistory>,
    /// Current broker URL (can be changed during session)
    pub broker_url: String,
    /// Current queue name (can be changed during session)
    pub queue_name: String,
}

impl InteractiveSession {
    /// Create a new interactive session
    pub fn new(config: Config) -> RustylineResult<Self> {
        let mut editor = Editor::<(), DefaultHistory>::new()?;

        // Load command history if it exists (unless CELERS_NO_HISTORY opts
        // out of history entirely -- see `history_disabled`).
        let history_path = dirs::home_dir().map(|mut p| {
            p.push(".celers_history");
            p
        });

        if !history_disabled() {
            if let Some(ref path) = history_path {
                let _ = editor.load_history(path);
            }
        }

        let broker_url = config.broker.url;
        let queue_name = config.broker.queue;

        Ok(Self {
            editor,
            broker_url,
            queue_name,
        })
    }

    /// Get the prompt string with current context
    fn get_prompt(&self) -> String {
        format!(
            "{}@{} {} ",
            "celers".cyan().bold(),
            self.queue_name.yellow(),
            "❯".green().bold()
        )
    }

    /// Run the interactive REPL loop
    pub async fn run(&mut self) -> Result<()> {
        println!("{}", "CeleRS Interactive Mode".green().bold());
        println!(
            "Type {} for help, {} to exit\n",
            "help".cyan(),
            "exit".cyan()
        );
        println!("Current broker: {}", self.broker_url.yellow());
        println!("Current queue: {}\n", self.queue_name.yellow());

        loop {
            let prompt = self.get_prompt();

            match self.editor.readline(&prompt) {
                Ok(line) => {
                    let line = line.trim();

                    // Skip empty lines
                    if line.is_empty() {
                        continue;
                    }

                    // Add to history, redacting any URL-shaped token's
                    // credentials first (idx 348) -- unless
                    // CELERS_NO_HISTORY opts out of history entirely.
                    if !history_disabled() {
                        let _ = self.editor.add_history_entry(redact_line_for_history(line));
                    }

                    // Handle exit commands
                    if matches!(line, "exit" | "quit" | "q") {
                        println!("{}", "Goodbye!".green());
                        break;
                    }

                    // Process command
                    if let Err(e) = self.process_command(line).await {
                        eprintln!("{} {}", "Error:".red().bold(), e);
                    }
                }
                Err(ReadlineError::Interrupted) => {
                    println!("{}", "^C".yellow());
                    continue;
                }
                Err(ReadlineError::Eof) => {
                    println!("{}", "Goodbye!".green());
                    break;
                }
                Err(err) => {
                    eprintln!("{} {}", "Error:".red().bold(), err);
                    break;
                }
            }
        }

        // Save history (unless CELERS_NO_HISTORY opts out entirely), then
        // restrict its permissions to owner-only (idx 348) -- every entry
        // added above already had URL credentials redacted, but the file
        // itself should not be world-readable regardless.
        if !history_disabled() {
            if let Some(mut path) = dirs::home_dir() {
                path.push(".celers_history");
                if self.editor.save_history(&path).is_ok() {
                    restrict_history_file_permissions(&path);
                }
            }
        }

        Ok(())
    }

    /// Process a single command
    async fn process_command(&mut self, line: &str) -> Result<()> {
        let parts: Vec<&str> = line.split_whitespace().collect();

        if parts.is_empty() {
            return Ok(());
        }

        match parts[0] {
            "help" | "?" => {
                self.print_help();
            }
            "status" | "st" => {
                crate::commands::show_status(&self.broker_url, &self.queue_name).await?;
            }
            "queues" | "ls" => {
                crate::commands::list_queues(&self.broker_url).await?;
            }
            "workers" | "w" => {
                crate::commands::list_workers(&self.broker_url).await?;
            }
            "health" | "h" => {
                crate::commands::health_check(&self.broker_url, &self.queue_name).await?;
            }
            "doctor" | "d" => {
                crate::commands::doctor(&self.broker_url, &self.queue_name).await?;
            }
            "metrics" | "m" => {
                crate::commands::show_metrics("text", None, None, None).await?;
            }
            "stats" | "cs" => {
                self.print_cache_pool_stats();
            }
            "dlq" => {
                if parts.len() < 2 {
                    println!("{} dlq <inspect|clear>", "Usage:".yellow());
                    return Ok(());
                }
                match parts[1] {
                    "inspect" | "i" => {
                        let limit = if parts.len() > 2 {
                            parts[2].parse().unwrap_or(10)
                        } else {
                            10
                        };
                        crate::commands::inspect_dlq(&self.broker_url, &self.queue_name, limit)
                            .await?;
                    }
                    "clear" | "c" => {
                        println!(
                            "{}",
                            "This will delete all DLQ tasks. Are you sure? (yes/no)".yellow()
                        );
                        let confirm_prompt = format!("{} ", "❯".green());
                        if let Ok(response) = self.editor.readline(&confirm_prompt) {
                            if response.trim() == "yes" {
                                crate::commands::clear_dlq(
                                    &self.broker_url,
                                    &self.queue_name,
                                    true,
                                )
                                .await?;
                            } else {
                                println!("{}", "Cancelled".yellow());
                            }
                        }
                    }
                    _ => println!("{} dlq <inspect|clear>", "Usage:".yellow()),
                }
            }
            "use" => {
                if parts.len() < 2 {
                    println!("{} use <queue_name>", "Usage:".yellow());
                    return Ok(());
                }
                let requested = parts[1];

                // Best-effort "did you mean" hint: queues can legitimately
                // not exist yet (e.g. before any task has been published),
                // and the broker may be briefly unreachable, so a failed
                // lookup or a lack of a suggestion never blocks the switch
                // below — this mirrors `broker [url]`'s no-validation style.
                if let Ok(available) = crate::commands::queue::queue_names(&self.broker_url).await {
                    if let Some(suggestion) =
                        crate::smart_defaults::suggest_queue(&available, Some(requested))
                    {
                        println!("{} '{}'?", "Did you mean".yellow(), suggestion.cyan());
                    }
                }

                self.queue_name = requested.to_string();
                println!(
                    "{} {}",
                    "Switched to queue:".green(),
                    self.queue_name.yellow()
                );
            }
            "broker" => {
                if parts.len() < 2 {
                    println!("{} Current: {}", "Broker:".cyan(), self.broker_url.yellow());
                    return Ok(());
                }
                self.broker_url = parts[1].to_string();
                println!(
                    "{} {}",
                    "Switched to broker:".green(),
                    self.broker_url.yellow()
                );
            }
            "clear" | "cls" => {
                print!("\x1B[2J\x1B[1;1H");
            }
            _ => {
                println!("{} Unknown command: {}", "Error:".red().bold(), parts[0]);
                println!("Type {} for available commands", "help".cyan());
                if let Some(suggestion) =
                    crate::smart_defaults::context_suggestion(parts[0], &known_command_names())
                {
                    println!("Did you mean '{}'?", suggestion.cyan());
                }
            }
        }

        Ok(())
    }

    /// Print the shared Redis connection pool's live utilization/reuse
    /// ratio and every `queue`/`worker` read-path cache's live hit ratio.
    ///
    /// Unlike the top-level `celers cache-stats` command (which only
    /// reports configured capacity/TTL, since a one-shot process exits
    /// before its counters could accumulate), this REPL keeps one process
    /// alive across many commands, so these ratios are actually meaningful
    /// here -- see the module docs on `crate::cache::TtlCache` and
    /// `crate::pool::ClientPool` for why.
    fn print_cache_pool_stats(&self) {
        println!("\n{}", "Connection Pool & Cache Statistics:".green().bold());
        println!();

        let pool_stats = redis_connection_pool().stats();
        println!("{}", "Redis connection pool".cyan().bold());
        println!(
            "  {:<16} {} / {} ({:.1}% utilized)",
            "Size / max:",
            pool_stats.size,
            pool_stats.max_size,
            pool_stats.utilization_pct()
        );
        println!(
            "  {:<16} {:.1}% ({} reused, {} created)",
            "Reuse ratio:",
            pool_stats.reuse_ratio() * 100.0,
            pool_stats.reuse_count,
            pool_stats.created_count
        );
        println!();

        println!("{}", "Queue read-path caches".cyan().bold());
        let (queue_list, queue_stats) = queue_cache_stats();
        println!("{}", format_cache_stats_line("list cache", &queue_list));
        println!("{}", format_cache_stats_line("stats cache", &queue_stats));
        println!();

        println!("{}", "Worker read-path caches".cyan().bold());
        let (worker_list, worker_stats) = worker_cache_stats();
        println!("{}", format_cache_stats_line("list cache", &worker_list));
        println!("{}", format_cache_stats_line("stats cache", &worker_stats));
        println!();
    }

    /// Print help message
    fn print_help(&self) {
        println!("\n{}", "Available Commands:".green().bold());
        println!();

        for (cmd, desc) in COMMAND_HELP.iter().copied() {
            println!("  {:<25} {}", cmd.cyan(), desc);
        }
        println!();
    }
}

/// Start interactive REPL mode
///
/// # Examples
///
/// ```no_run
/// use celers_cli::interactive::start_interactive;
/// use celers_cli::config::Config;
///
/// # async fn example() -> anyhow::Result<()> {
/// let config = Config::default_config();
/// start_interactive(config).await?;
/// # Ok(())
/// # }
/// ```
pub async fn start_interactive(config: Config) -> Result<()> {
    let mut session = InteractiveSession::new(config)?;
    session.run().await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Serializes tests that mutate the process-wide `CELERS_NO_HISTORY`
    /// environment variable. Only matters for the plain `cargo test`
    /// fallback runner (nextest, this crate's primary runner, isolates each
    /// test in its own process).
    fn no_history_env_guard() -> std::sync::MutexGuard<'static, ()> {
        static LOCK: std::sync::OnceLock<std::sync::Mutex<()>> = std::sync::OnceLock::new();
        LOCK.get_or_init(|| std::sync::Mutex::new(()))
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Regression test for idx 348: a `broker <url>` command carrying
    /// embedded credentials must never reach `~/.celers_history` verbatim.
    #[test]
    fn redact_line_for_history_masks_password_in_url_shaped_tokens() {
        let redacted = redact_line_for_history("broker redis://user:s3cr3t@host:6379");
        assert_eq!(redacted, "broker redis://user:****@host:6379");
        assert!(!redacted.contains("s3cr3t"));
    }

    #[test]
    fn redact_line_for_history_leaves_plain_commands_unchanged() {
        assert_eq!(redact_line_for_history("status"), "status");
        assert_eq!(redact_line_for_history("use my_queue"), "use my_queue");
    }

    #[test]
    fn redact_line_for_history_masks_only_the_url_shaped_token_among_several() {
        let redacted = redact_line_for_history("dlq inspect redis://user:s3cr3t@host 10");
        assert!(redacted.contains("dlq inspect"));
        assert!(redacted.contains("10"));
        assert!(!redacted.contains("s3cr3t"));
    }

    #[test]
    fn history_disabled_reflects_celers_no_history_env_var() {
        let _guard = no_history_env_guard();

        std::env::remove_var("CELERS_NO_HISTORY");
        assert!(!history_disabled());

        std::env::set_var("CELERS_NO_HISTORY", "1");
        assert!(history_disabled());

        std::env::remove_var("CELERS_NO_HISTORY");
        assert!(!history_disabled());
    }

    #[cfg(unix)]
    #[test]
    fn restrict_history_file_permissions_sets_owner_only_mode() {
        use std::os::unix::fs::PermissionsExt;

        let path = std::env::temp_dir().join(format!(
            "celers_cli_history_perm_test_{}",
            uuid::Uuid::new_v4()
        ));
        std::fs::write(&path, "dummy history").expect("write temp file");
        // Start from something more permissive so the assertion below
        // actually proves this function tightened it, rather than the file
        // happening to already be 0600 from umask defaults.
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))
            .expect("loosen perms");

        restrict_history_file_permissions(&path);

        let mode = std::fs::metadata(&path).expect("stat").permissions().mode() & 0o777;
        assert_eq!(mode, 0o600, "history file must be owner-read/write only");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn format_cache_stats_line_includes_all_fields() {
        let stats = CacheStats {
            len: 3,
            hits: 7,
            misses: 3,
        };
        let line = format_cache_stats_line("list cache", &stats);
        assert!(line.contains("list cache:"));
        assert!(line.contains("len=3"));
        assert!(line.contains("hits=7"));
        assert!(line.contains("misses=3"));
        assert!(line.contains("hit_ratio=70.0%"));
    }

    #[test]
    fn known_command_names_includes_stats() {
        let names = known_command_names();
        assert!(
            names.iter().any(|n| n == "stats"),
            "the new `stats` REPL command must be discoverable via the same \
             COMMAND_HELP-derived list used for \"did you mean\" suggestions"
        );
    }
}
