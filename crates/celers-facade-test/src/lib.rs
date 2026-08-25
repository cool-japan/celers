//! What a downstream crate really needs in order to use `#[celers::task]`
//! (audit idx 319).
//!
//! This crate exists for its dependency list — `celers` and `serde`, nothing
//! else. See `Cargo.toml` for the full reasoning; the short version:
//!
//! * `celers_core::Task` and `#[async_trait::async_trait]` appear in the macro
//!   expansion as ordinary paths, and resolve through the facade's root
//!   re-exports plus the `use celers::*;` below. **This crate is the only
//!   regression test for that** — every other workspace member depends on
//!   `celers-core` and `async-trait` directly, so all of them would keep
//!   compiling if the facade dropped those re-exports.
//! * `serde::Serialize` / `serde::Deserialize` appear in a *derive* position,
//!   and the same glob import does **not** reach them: delete the `serde`
//!   dependency and this file fails with `error[E0463]: can't find crate for
//!   'serde'` pointing at `#[celers::task]`. A downstream crate must declare
//!   `serde` (with `derive`) itself. The likely reason is that a derive
//!   path's first segment is not satisfied by a glob import; the
//!   reproduction is the fact, that explanation is an inference.
//!
//! Keep this crate compiling and its dependency list at two entries.

// The import under test. Deliberately a glob: it is the idiomatic "just use
// the facade" form a new user writes, and it is what carries the re-exported
// `celers_core` and `async_trait` names into this module's scope.
use celers::*;

/// A task declared exactly the way the facade's own documentation shows.
///
/// It takes real parameters so the generated input struct has fields for the
/// `serde` derives to expand over; an empty struct would derive trivially and
/// prove less.
#[celers::task]
async fn ping(message: String, count: u32) -> celers_core::Result<String> {
    Ok(format!("{message} x{count}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Statically asserts that the macro-generated input struct really got
    /// both `serde` derives, without needing a `serde_json` dependency (which
    /// would push this crate past its two-entry dependency budget).
    fn assert_serde_round_trippable<T>()
    where
        T: serde::Serialize + for<'de> serde::Deserialize<'de>,
    {
    }

    /// Compiling at all is the real assertion; this exercises the generated
    /// items so a future change cannot keep them compiling while making them
    /// unusable.
    #[test]
    fn the_generated_task_resolves_through_the_facade_re_exports() {
        // The `Task` impl the macro wrote against the re-exported trait, with
        // the re-exported `#[async_trait]` on it.
        let task = PingTask;
        assert_eq!(celers_core::Task::name(&task), "ping");

        // The generated input struct exists, is constructible, and carries the
        // derives the macro asked `serde` for.
        let input = PingTaskInput {
            message: "pong".to_string(),
            count: 3,
        };
        assert_eq!(input.message, "pong");
        assert_eq!(input.count, 3);
        assert_serde_round_trippable::<PingTaskInput>();
    }
}
