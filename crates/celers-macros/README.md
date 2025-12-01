# celers-macros

Procedural macros for CeleRS task definitions.

## Overview

**Status: Not Yet Implemented**

This crate will provide convenient derive macros and attributes for defining CeleRS tasks
without boilerplate.

## Planned Features

### `#[derive(Task)]` Macro

Automatically implement the `Task` trait for structs:

```rust
use celers_macros::Task;
use serde::{Deserialize, Serialize};

#[derive(Task)]
#[task(name = "process_order")]
struct ProcessOrderTask;

#[derive(Serialize, Deserialize)]
struct OrderInput {
    order_id: String,
    amount: f64,
}

#[derive(Serialize, Deserialize)]
struct OrderResult {
    success: bool,
    transaction_id: String,
}

impl ProcessOrderTask {
    async fn execute(&self, input: OrderInput) -> Result<OrderResult> {
        // Task logic here
        Ok(OrderResult {
            success: true,
            transaction_id: "txn_123".to_string(),
        })
    }
}
```

### `#[task]` Attribute Macro

Convert async functions directly into tasks:

```rust
use celers_macros::task;

#[task(name = "send_email", max_retries = 5, timeout = 30)]
async fn send_email(to: String, subject: String, body: String) -> Result<()> {
    // Email sending logic
    Ok(())
}

// Usage
let task_id = send_email.enqueue(&broker,
    "user@example.com".to_string(),
    "Welcome!".to_string(),
    "Thanks for signing up.".to_string()
).await?;
```

### Compile-Time Validation

The macros will enforce:
- Input/output types must implement `Serialize` + `Deserialize`
- Task names must be valid identifiers
- Configuration values must be reasonable (e.g., max_retries > 0)

## Implementation Status

- [ ] Basic derive macro skeleton
- [ ] Task trait implementation generation
- [ ] Attribute parsing and validation
- [ ] Function-to-task conversion
- [ ] Error message improvements
- [ ] Documentation and examples

## See Also

- `celers-core`: Core Task trait definition
