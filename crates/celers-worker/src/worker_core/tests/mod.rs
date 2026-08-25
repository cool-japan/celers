//! Regression tests for the worker execution loop.
//!
//! Every test here pins behaviour that was previously broken: unbounded
//! concurrency, panics skipping cleanup, shutdown abandoning in-flight work,
//! retries that never advanced, admission deferrals spinning the loop, and the
//! configuration options the runtime silently ignored.
//!
//! Split into one file per behavioural area so no single file carries the
//! whole suite; [`doubles`] holds the mocks and fixtures every other
//! submodule shares.

mod doubles;

mod support_fns;

mod in_flight_registry;

mod retry_and_panics;

mod dispatch_and_shutdown;

mod circuit_breaker;

mod admission_control;

mod coalescing;

mod lifecycle_events;

mod execution_limits;

mod time_limits;

#[cfg(feature = "canvas")]
mod canvas;

mod dlq;

mod poison_pill_and_health;

mod checkpoints;
