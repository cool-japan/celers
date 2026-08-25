// `#[task]` refuses a non-`async fn` with a real compile error instead of
// generating an `execute` that never awaits anything correctly. See
// `celers-macros/src/task_macro.rs::task_macro_impl`'s `asyncness` check.
use celers_macros::task;

#[task]
fn not_async() -> celers_core::Result<()> {
    Ok(())
}

fn main() {}
