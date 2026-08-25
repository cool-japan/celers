// `#[task(...)]` (attribute macro) rejects an unrecognized key with a real
// compile error instead of silently ignoring it. See
// `celers-macros/src/task_attr.rs`'s `Parse for TaskAttr`, the `_ =>` arm.
use celers_macros::task;

#[task(bogus = "value")]
async fn my_task() -> celers_core::Result<()> {
    Ok(())
}

fn main() {}
