//! Compile-fail UI harness for `#[task(...)]` / `#[derive(Task)]` /
//! `#[validate(...)]` misuse.
//!
//! Asserts on the *exact* diagnostic each macro emits for a class of bad
//! input, rather than only on the paths that compile successfully (which is
//! all the rest of this test crate covers). Two of the fixtures under
//! `tests/ui/` pin down bugs that `tests/integration_test.rs` documents as
//! needing exactly this kind of harness to test directly: `parse_nested_meta`
//! errors that used to be discarded via `let _ = ...`, and an unparsable
//! `input`/`output` type string that used to silently become
//! `serde_json::Value` instead of failing to compile.
//!
//! `tests/ui/*.rs` is a plain glob under `tests/`, not `tests/ui/main.rs` or
//! `tests/ui/mod.rs`, so Cargo does not pick any of those files up as their
//! own test targets -- only this file (a direct child of `tests/`) is.
//!
//! Regenerate the `.stderr` snapshots after a deliberate diagnostic-text
//! change with `TRYBUILD=overwrite cargo test -p celers-macros --test
//! ui_compile_fail`, then review the diff before committing it.
#[test]
fn ui_compile_fail() {
    let t = trybuild::TestCases::new();
    t.compile_fail("tests/ui/*.rs");
}
