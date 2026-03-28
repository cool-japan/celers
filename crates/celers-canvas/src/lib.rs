//! Canvas workflow primitives
//!
//! This crate provides distributed workflow patterns for task orchestration.
//!
//! # Workflow Primitives
//!
//! - **Chain**: Execute tasks sequentially, passing results as arguments
//! - **Group**: Execute tasks in parallel
//! - **Chord**: Execute tasks in parallel, then run callback with all results
//! - **Map**: Apply a task to multiple argument sets in parallel
//!
//! # Example
//!
//! ```ignore
//! // Chain: task1 -> task2 -> task3
//! let workflow = Chain::new()
//!     .then("task1", args1)
//!     .then("task2", args2)
//!     .then("task3", args3);
//!
//! // Chord: (task1 | task2 | task3) -> callback
//! let workflow = Chord::new()
//!     .add("task1", args1)
//!     .add("task2", args2)
//!     .add("task3", args3)
//!     .callback("aggregate_results", callback_args);
//! ```

mod error;
pub use error::*;

mod signature;
pub use signature::*;

mod chain;
pub use chain::*;

mod group;
pub use group::*;

mod chord;
pub use chord::*;

mod conditional;
pub use conditional::*;

mod canvas_element;
pub use canvas_element::*;

mod error_handling;
pub use error_handling::*;

mod dag_export;
pub use dag_export::*;

mod result_types;
pub use result_types::*;

mod advanced;
pub use advanced::*;

mod checkpoint;
pub use checkpoint::*;

mod compiler;
pub use compiler::*;

mod dependency;
pub use dependency::*;

mod events;
pub use events::*;

mod scheduling;
pub use scheduling::*;

mod streaming;
pub use streaming::*;

mod monitoring;
pub use monitoring::*;

mod visualization;
pub use visualization::*;

mod runtime;
pub use runtime::*;

#[cfg(test)]
mod tests_basic;

#[cfg(test)]
mod tests_advanced;
