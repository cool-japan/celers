//! Command-line argument surface and dispatch for the `celers` binary.
//!
//! This module owns the `clap` derive surface (the [`Cli`] entry struct, the
//! top-level [`crate::cli::types::Commands`] enum, and every nested `*Commands` enum) together
//! with the [`dispatch()`] routing function that turns a parsed [`Cli`] into a
//! call against [`crate::commands`], [`crate::backup`], [`crate::interactive`],
//! or [`crate::config_layer`]. `main.rs` only parses arguments, initializes
//! logging, and calls [`dispatch()`].

mod dispatch;
mod types;

pub(crate) use dispatch::dispatch;
pub(crate) use types::Cli;
