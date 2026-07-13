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

        // Load command history if it exists
        let history_path = dirs::home_dir().map(|mut p| {
            p.push(".celers_history");
            p
        });

        if let Some(ref path) = history_path {
            let _ = editor.load_history(path);
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

                    // Add to history
                    let _ = self.editor.add_history_entry(line);

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

        // Save history
        if let Some(mut path) = dirs::home_dir() {
            path.push(".celers_history");
            let _ = self.editor.save_history(&path);
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
