// `#[validate(...)]` rejects an unrecognized parameter with a real compile
// error instead of silently ignoring it. See
// `celers-macros/src/validation.rs::FieldValidation::from_attributes`, the
// final `else` arm inside the `parse_nested_meta` closure.
use celers_macros::task;

#[task]
async fn my_task(#[validate(bogus)] value: i32) -> celers_core::Result<i32> {
    Ok(value)
}

fn main() {}
