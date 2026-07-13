//! Structured logging setup for the `celers` CLI: output format, level
//! filtering, and streaming to external sinks.
//!
//! This module owns the *one* place in the binary that installs the global
//! `tracing` subscriber. It intentionally knows nothing about `clap` or
//! [`CliConfigArgs`](crate::config_layer::CliConfigArgs) precedence -- the
//! caller (`main.rs`) is expected to have already resolved the effective log
//! level (`--log-level` > `RUST_LOG` > `"info"`, via
//! [`CliConfigArgs::resolve_log_level`](crate::config_layer::CliConfigArgs::resolve_log_level))
//! and simply hands the final string to [`init_logging`].
//!
//! # Output format
//!
//! [`LogFormat::Text`] reproduces the CLI's historical format (no target, no
//! level tag). [`LogFormat::Json`] emits one JSON object per event, suitable
//! for ingestion by log-shipping agents.
//!
//! # Sinks
//!
//! [`LogSink`] selects where formatted lines go: standard output (the
//! default), an append-mode file, or a TCP connection to a log collector.
//! Parsing a `--log-sink` value is supported via [`LogSink`]'s [`FromStr`]
//! implementation (`"stdout"`, `"file:<path>"`, `"tcp:<host:port>"`).
//!
//! # Examples
//!
//! ```no_run
//! use celers_cli::logging::{self, LogFormat, LogSink};
//!
//! # fn main() -> anyhow::Result<()> {
//! logging::init_logging("info", LogFormat::Text, LogSink::Stdout)?;
//! tracing::info!("celers starting up");
//! # Ok(())
//! # }
//! ```

use std::fs::{File, OpenOptions};
use std::io;
use std::net::TcpStream;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Mutex;

use anyhow::Context;
use tracing::Subscriber;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

/// Output format for CLI log lines.
///
/// Selected via `--log-format text|json`; defaults to [`LogFormat::Text`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum LogFormat {
    /// Human-readable text: no target, no level tag (the CLI's historical
    /// default output).
    #[default]
    Text,
    /// Newline-delimited JSON, one object per log event.
    Json,
}

impl std::fmt::Display for LogFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Text => "text",
            Self::Json => "json",
        })
    }
}

/// Destination that formatted log lines are written to.
///
/// Selected via `--log-sink stdout|file:<path>|tcp:<host:port>`; defaults to
/// [`LogSink::Stdout`]. See the [`FromStr`] impl for the exact accepted
/// syntax.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum LogSink {
    /// Write to the process's standard output.
    #[default]
    Stdout,
    /// Append to a file at the given path, creating it if it does not exist.
    File(PathBuf),
    /// Stream lines to a log collector listening at `host:port` over TCP.
    Tcp(String),
}

impl std::fmt::Display for LogSink {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Stdout => f.write_str("stdout"),
            Self::File(path) => write!(f, "file:{}", path.display()),
            Self::Tcp(addr) => write!(f, "tcp:{addr}"),
        }
    }
}

/// Error returned when a `--log-sink` value does not match a supported form.
#[derive(Debug, PartialEq, Eq)]
pub struct LogSinkParseError(String);

impl std::fmt::Display for LogSinkParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl std::error::Error for LogSinkParseError {}

impl FromStr for LogSink {
    type Err = LogSinkParseError;

    /// Parses `"stdout"` (or `"-"`), `"file:<path>"`, or `"tcp:<host:port>"`.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s == "stdout" || s == "-" {
            Ok(Self::Stdout)
        } else if let Some(path) = s.strip_prefix("file:") {
            Ok(Self::File(PathBuf::from(path)))
        } else if let Some(addr) = s.strip_prefix("tcp:") {
            Ok(Self::Tcp(addr.to_string()))
        } else {
            Err(LogSinkParseError(format!(
                "invalid --log-sink {s:?}: expected \"stdout\", \"file:<path>\", or \"tcp:<host:port>\""
            )))
        }
    }
}

/// Concrete writer behind a resolved [`LogSink`].
///
/// `tracing_subscriber::fmt`'s builder needs a single, statically-typed
/// writer, but the sink is only known at runtime, so the three possible
/// destinations are unified behind one [`io::Write`] impl here rather than
/// picking a generic parameter per branch.
enum SinkTarget {
    Stdout(io::Stdout),
    File(File),
    Tcp(TcpStream),
}

impl io::Write for SinkTarget {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Stdout(w) => w.write(buf),
            Self::File(w) => w.write(buf),
            Self::Tcp(w) => w.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Stdout(w) => w.flush(),
            Self::File(w) => w.flush(),
            Self::Tcp(w) => w.flush(),
        }
    }
}

/// Resolve a [`LogSink`] into the writer used to build the subscriber.
///
/// File sinks are opened in append mode, creating the file (and requiring
/// its parent directory to already exist) if it is missing. TCP sinks
/// connect eagerly so a bad address is reported once at startup rather than
/// silently dropping every subsequent log line.
///
/// The returned [`Mutex`] is handed directly to
/// `tracing_subscriber::fmt`'s `with_writer`: `tracing-subscriber` ships a
/// blanket `MakeWriter` impl for `Mutex<W: io::Write>`, so no custom
/// `MakeWriter` impl is needed here.
fn resolve_sink(sink: LogSink) -> anyhow::Result<Mutex<SinkTarget>> {
    let target = match sink {
        LogSink::Stdout => SinkTarget::Stdout(io::stdout()),
        LogSink::File(path) => {
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .with_context(|| format!("failed to open log file {}", path.display()))?;
            SinkTarget::File(file)
        }
        LogSink::Tcp(addr) => {
            let stream = TcpStream::connect(addr.as_str())
                .with_context(|| format!("failed to connect to TCP log sink at {addr}"))?;
            // Best-effort: small log lines benefit from disabling Nagle's
            // algorithm, but this is a latency nicety, not a correctness
            // requirement, so a failure here is not fatal to logging setup.
            let _ = stream.set_nodelay(true);
            SinkTarget::Tcp(stream)
        }
    };
    Ok(Mutex::new(target))
}

/// Build the [`EnvFilter`] used for level filtering.
///
/// `level` is parsed as-is; an unparsable value silently falls back to
/// `"info"`, exactly matching the CLI's pre-existing inline behavior. Callers
/// are expected to have already resolved the `--log-level` / `RUST_LOG` /
/// default precedence (see
/// [`CliConfigArgs::resolve_log_level`](crate::config_layer::CliConfigArgs::resolve_log_level))
/// before calling this -- `level` here is just the final string.
fn build_env_filter(level: &str) -> EnvFilter {
    EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"))
}

/// Initialize the global `tracing` subscriber for the `celers` CLI.
///
/// This is the single call site that installs the process-wide default
/// subscriber. Unlike `tracing_subscriber`'s plain `.init()` (which panics
/// if a global default is already set), this uses `try_init()` internally
/// and surfaces that failure as an `Err`, so a double-init can never abort
/// the process -- callers get a normal `anyhow` error to handle or report
/// instead. In normal CLI operation this is called exactly once, from
/// `main`, before any other `tracing` calls.
///
/// `format` selects [`LogFormat::Json`] (`tracing_subscriber`'s `.json()`
/// layer) or [`LogFormat::Text`] (the CLI's historical
/// `with_target(false).with_level(false)` layer). `sink` selects where the
/// formatted lines are written; see [`LogSink`].
///
/// # Errors
///
/// Returns an error if the requested [`LogSink`] cannot be opened or
/// connected to, or if a global subscriber has already been installed in
/// this process.
pub fn init_logging(level: &str, format: LogFormat, sink: LogSink) -> anyhow::Result<()> {
    let env_filter = build_env_filter(level);
    let writer = resolve_sink(sink)?;

    let subscriber: Box<dyn Subscriber + Send + Sync + 'static> = match format {
        LogFormat::Json => Box::new(
            tracing_subscriber::fmt()
                .json()
                .with_env_filter(env_filter)
                .with_writer(writer)
                .finish(),
        ),
        LogFormat::Text => Box::new(
            tracing_subscriber::fmt()
                .with_target(false)
                .with_level(false)
                .with_env_filter(env_filter)
                .with_writer(writer)
                .finish(),
        ),
    };

    subscriber
        .try_init()
        .map_err(|err| anyhow::anyhow!("failed to initialize logging subscriber: {err}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::sync::{Arc, PoisonError};
    use tracing_subscriber::fmt::MakeWriter;

    // ---------------------------------------------------------------
    // LogFormat / LogSink value semantics
    // ---------------------------------------------------------------

    #[test]
    fn test_log_format_default_is_text() {
        assert_eq!(LogFormat::default(), LogFormat::Text);
    }

    #[test]
    fn test_log_sink_default_is_stdout() {
        assert_eq!(LogSink::default(), LogSink::Stdout);
    }

    #[test]
    fn test_log_sink_from_str_parses_all_forms() {
        assert_eq!("stdout".parse::<LogSink>(), Ok(LogSink::Stdout));
        assert_eq!("-".parse::<LogSink>(), Ok(LogSink::Stdout));
        assert_eq!(
            "file:relative/log/path.log".parse::<LogSink>(),
            Ok(LogSink::File(PathBuf::from("relative/log/path.log")))
        );
        assert_eq!(
            "tcp:127.0.0.1:9000".parse::<LogSink>(),
            Ok(LogSink::Tcp("127.0.0.1:9000".to_string()))
        );
        assert!("not-a-valid-sink".parse::<LogSink>().is_err());
    }

    #[test]
    fn test_log_sink_display_round_trips_through_from_str() {
        for sink in [
            LogSink::Stdout,
            LogSink::File(PathBuf::from("relative/log/path.log")),
            LogSink::Tcp("127.0.0.1:9000".to_string()),
        ] {
            let rendered = sink.to_string();
            let parsed: LogSink = rendered
                .parse()
                .unwrap_or_else(|e| panic!("failed to re-parse displayed sink {rendered:?}: {e}"));
            assert_eq!(parsed, sink);
        }
    }

    // ---------------------------------------------------------------
    // Filter construction: same fallback-to-"info" behavior as today.
    // ---------------------------------------------------------------

    #[test]
    fn test_build_env_filter_uses_valid_levels_as_is() {
        for level in [
            "error",
            "warn",
            "info",
            "debug",
            "trace",
            "celers_cli=debug,warn",
        ] {
            let filter = build_env_filter(level);
            let expected = EnvFilter::try_new(level)
                .unwrap_or_else(|e| panic!("expected {level:?} to be a valid filter: {e}"))
                .to_string();
            assert_eq!(filter.to_string(), expected, "level={level}");
        }
    }

    #[test]
    fn test_build_env_filter_falls_back_to_info_on_invalid_directive() {
        // Each of these starts with a character the directive grammar
        // rejects outright (not alphanumeric and not one of '-', ':', '_'),
        // so `EnvFilter::try_new` is guaranteed to return `Err` for them.
        for bogus in ["???", "!!!not-a-directive", "==", "#nope"] {
            assert!(
                EnvFilter::try_new(bogus).is_err(),
                "test premise broken: {bogus:?} unexpectedly parsed as a valid filter"
            );

            let filter = build_env_filter(bogus);
            let fallback = EnvFilter::new("info").to_string();
            assert_eq!(
                filter.to_string(),
                fallback,
                "bogus level {bogus:?} did not fall back to \"info\""
            );
        }
    }

    // ---------------------------------------------------------------
    // Format selection: Json vs Text produce observably different output.
    // ---------------------------------------------------------------

    /// A `MakeWriter` that appends every write into a shared in-memory
    /// buffer, so tests can inspect exactly what a subscriber produced.
    #[derive(Clone, Default)]
    struct SharedBufWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedBufWriter {
        fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .write(buf)
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for SharedBufWriter {
        type Writer = Self;

        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    /// Builds a subscriber matching `init_logging`'s branching (same two
    /// layer configurations), but targets an in-memory buffer and installs
    /// it as a *scoped* (thread-local) default via `with_default` instead of
    /// the process-wide global. This deliberately avoids the global
    /// subscriber slot so it is safe to call from multiple tests regardless
    /// of whether they share a process (plain `cargo test`) or not
    /// (`cargo nextest`, one process per test).
    fn capture_output(format: LogFormat) -> String {
        let buf = SharedBufWriter::default();
        let env_filter = build_env_filter("info");

        match format {
            LogFormat::Json => {
                let subscriber = tracing_subscriber::fmt()
                    .json()
                    .with_env_filter(env_filter)
                    .with_writer(buf.clone())
                    .finish();
                tracing::subscriber::with_default(subscriber, || {
                    tracing::info!(sample_field = 42, "hello from logging test");
                });
            }
            LogFormat::Text => {
                let subscriber = tracing_subscriber::fmt()
                    .with_target(false)
                    .with_level(false)
                    .with_env_filter(env_filter)
                    .with_writer(buf.clone())
                    .finish();
                tracing::subscriber::with_default(subscriber, || {
                    tracing::info!(sample_field = 42, "hello from logging test");
                });
            }
        }

        let bytes = buf.0.lock().unwrap_or_else(PoisonError::into_inner).clone();
        String::from_utf8(bytes).unwrap_or_default()
    }

    #[test]
    fn test_json_format_emits_valid_json_lines() {
        let output = capture_output(LogFormat::Json);
        let lines: Vec<&str> = output.lines().filter(|l| !l.trim().is_empty()).collect();
        assert!(!lines.is_empty(), "expected at least one JSON log line");

        for line in lines {
            let parsed: serde_json::Value = serde_json::from_str(line)
                .unwrap_or_else(|e| panic!("json log line failed to parse ({e}): {line}"));
            assert!(parsed.is_object(), "expected a JSON object, got: {line}");
        }
    }

    #[test]
    fn test_text_format_is_not_json_and_omits_target_and_level() {
        let output = capture_output(LogFormat::Text);
        assert!(!output.trim().is_empty(), "expected some log output");

        for line in output.lines().filter(|l| !l.trim().is_empty()) {
            assert!(
                serde_json::from_str::<serde_json::Value>(line).is_err(),
                "text format line unexpectedly parsed as JSON: {line}"
            );
            // `with_target(false)` / `with_level(false)`: neither the module
            // path nor an "INFO"-style level tag should appear.
            assert!(
                !line.contains("logging::tests"),
                "line unexpectedly contains target: {line}"
            );
            assert!(
                !line.contains("INFO"),
                "line unexpectedly contains a level tag: {line}"
            );
        }
    }

    // ---------------------------------------------------------------
    // Sink construction: file sink actually writes to disk.
    // ---------------------------------------------------------------

    #[test]
    fn test_file_sink_writes_and_reads_back() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        let path = std::env::temp_dir().join(format!(
            "celers_cli_logging_test_{}_{nanos}.log",
            std::process::id()
        ));

        let sink = resolve_sink(LogSink::File(path.clone()))
            .unwrap_or_else(|e| panic!("failed to resolve file sink: {e}"));
        {
            let mut guard = sink.lock().unwrap_or_else(PoisonError::into_inner);
            guard
                .write_all(b"hello celers-cli logging\n")
                .unwrap_or_else(|e| panic!("failed to write to file sink: {e}"));
            guard
                .flush()
                .unwrap_or_else(|e| panic!("failed to flush file sink: {e}"));
        }

        let contents = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("failed to read back log file {}: {e}", path.display()));
        assert!(contents.contains("hello celers-cli logging"));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_tcp_sink_actually_streams_bytes() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap_or_else(|e| panic!("failed to bind ephemeral TCP listener: {e}"));
        let addr = listener
            .local_addr()
            .unwrap_or_else(|e| panic!("failed to read listener local addr: {e}"));

        let handle = std::thread::spawn(move || -> io::Result<Vec<u8>> {
            let (mut conn, _) = listener.accept()?;
            let mut received = vec![0_u8; b"hello over tcp\n".len()];
            conn.read_exact(&mut received)?;
            Ok(received)
        });

        let sink = resolve_sink(LogSink::Tcp(addr.to_string()))
            .unwrap_or_else(|e| panic!("failed to resolve tcp sink: {e}"));
        {
            let mut guard = sink.lock().unwrap_or_else(PoisonError::into_inner);
            guard
                .write_all(b"hello over tcp\n")
                .unwrap_or_else(|e| panic!("failed to write to tcp sink: {e}"));
            guard
                .flush()
                .unwrap_or_else(|e| panic!("failed to flush tcp sink: {e}"));
        }
        // Ensure the write is flushed to the socket before we block on the
        // listener thread joining.
        drop(sink);

        let received = handle
            .join()
            .unwrap_or_else(|_| panic!("listener thread panicked"))
            .unwrap_or_else(|e| panic!("listener thread failed to read: {e}"));
        assert_eq!(received, b"hello over tcp\n");
    }

    // ---------------------------------------------------------------
    // init_logging: never panics on double-init.
    // ---------------------------------------------------------------

    #[test]
    fn test_init_logging_double_init_reports_error_not_panic() {
        // Whatever state the process-wide subscriber slot is in when this
        // test runs (it may already be claimed by another test in this
        // binary, since plain `cargo test` shares one process across tests
        // -- `cargo nextest` instead gives each test its own process),
        // calling `init_logging` a second time back-to-back must always
        // report an error rather than panicking: after the first call
        // returns (`Ok` because *we* won the slot, or `Err` because someone
        // else already had it), the global slot is guaranteed occupied, so
        // the second call can never succeed.
        let _first = init_logging("info", LogFormat::Text, LogSink::Stdout);
        let second = init_logging("info", LogFormat::Text, LogSink::Stdout);
        assert!(
            second.is_err(),
            "second back-to-back init_logging call must fail cleanly (double-init guard)"
        );
    }
}
