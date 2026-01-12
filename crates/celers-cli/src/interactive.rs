//! Interactive REPL mode for CeleRS CLI.
//!
//! Provides an interactive shell for running multiple commands without restarting
//! the CLI. Features include command history, tab completion, and session state.

use anyhow::Result;
use colored::Colorize;
use rustyline::error::ReadlineError;
use rustyline::history::DefaultHistory;
use rustyline::{Editor, Result as RustylineResult};

use crate::config::Config;

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
                self.queue_name = parts[1].to_string();
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
            }
        }

        Ok(())
    }

    /// Print help message
    fn print_help(&self) {
        println!("\n{}", "Available Commands:".green().bold());
        println!();

        let commands = vec![
            ("status, st", "Show queue status"),
            ("queues, ls", "List all queues"),
            ("workers, w", "List all workers"),
            ("health, h", "Run health diagnostics"),
            ("doctor, d", "Automatic problem detection"),
            ("metrics, m", "Display metrics"),
            ("dlq inspect [limit]", "Inspect DLQ tasks"),
            ("dlq clear", "Clear all DLQ tasks"),
            ("use <queue>", "Switch to different queue"),
            ("broker [url]", "Show/set broker URL"),
            ("clear, cls", "Clear screen"),
            ("help, ?", "Show this help"),
            ("exit, quit, q", "Exit interactive mode"),
        ];

        for (cmd, desc) in commands {
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
