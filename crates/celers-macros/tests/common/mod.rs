//! Shared test helpers for `celers-macros` integration tests.
//!
//! Every integration-test file uses the *real* `celers_core::{Task,
//! CelersError, Result}` (a dev-dependency of this package) rather than a
//! hand-rolled mirror of them: a mock can drift out of sync with the real
//! crate's shape without anything here noticing (that happened once already
//! -- see the removed `mod celers_core` stub this file replaces, which for a
//! time defined `CelersError` as a plain tuple struct while the macros
//! generate `celers_core::CelersError::TaskExecution(..)` enum-variant
//! construction, so a wrong shape here would still have compiled). Using the
//! real crate means a real API change breaks these tests immediately.
//!
//! This file is `tests/common/mod.rs` rather than `tests/common.rs`
//! specifically so Cargo does *not* treat it as its own integration-test
//! binary (a bare `tests/common.rs` would compile and run as an empty test
//! target); a test file that needs it pulls it in with `mod common;`, which
//! resolves to this file per Rust's standard module-file lookup.

/// Accessor for the message wrapped by `CelersError::TaskExecution`.
///
/// The real `celers_core::CelersError` only exposes its message via
/// `Display`/`to_string()` (through `thiserror`'s
/// `#[error("Task execution failed: {0}")]`), which every exact-string
/// assertion in this suite would otherwise have to strip a fixed prefix
/// from. This mirrors the direct accessor the old mock provided, without
/// reaching into `celers_core` (not owned by this crate) to add one there.
pub trait CelersErrorMessage {
    /// Returns the raw message wrapped by `CelersError::TaskExecution`.
    ///
    /// # Panics
    ///
    /// Panics if `self` is any other `CelersError` variant. Every task in
    /// this suite reports validation/execution failures exclusively via
    /// `TaskExecution` (see `celers-macros/src/validation.rs`'s codegen), so
    /// a different variant here would mean the macro's error path changed in
    /// a way these tests need to see, not silently paper over.
    fn message(&self) -> &str;
}

impl CelersErrorMessage for celers_core::CelersError {
    fn message(&self) -> &str {
        match self {
            celers_core::CelersError::TaskExecution(msg) => msg,
            other => panic!(
                "test helper `.message()` only supports `CelersError::TaskExecution`, got: {other:?}"
            ),
        }
    }
}
