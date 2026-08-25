// `#[derive(Task)]`'s `#[task(...)]` rejects an unrecognized key with a real
// compile error. This is one of the two bugs `tests/integration_test.rs`
// documents as needing a `trybuild`-style harness to assert on directly: the
// derive macro used to discard `parse_nested_meta`'s error via `let _ =
// ...`, so a typo like `#[task(nmae = "...")]` silently fell back to
// defaults instead of failing to compile. See
// `celers-macros/src/derive_macro.rs::derive_task_impl`, the `else` arm
// inside the `parse_nested_meta` closure.
use celers_macros::Task;

#[derive(Task)]
#[task(bogus = "value")]
struct MyTask;

impl MyTask {
    async fn execute_impl(
        &self,
        input: serde_json::Value,
    ) -> celers_core::Result<serde_json::Value> {
        Ok(input)
    }
}

fn main() {}
