// `#[derive(Task)]`'s `#[task(input = "...")]` rejects a string that does not
// parse as a Rust type with a real compile error. This is the second bug
// `tests/integration_test.rs` documents as needing a `trybuild`-style
// harness: an unparsable `input`/`output` type string used to be silently
// swallowed and the field defaulted to `serde_json::Value`, so a typo'd type
// name compiled into a `Task` impl with the wrong `Input`/`Output` type and
// only surfaced as a confusing trait-mismatch error far from the attribute
// that caused it. See `celers-macros/src/derive_macro.rs::parse_type_attr`.
use celers_macros::Task;

#[derive(Task)]
#[task(input = "not a valid type <<<")]
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
