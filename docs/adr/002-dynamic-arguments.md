# ADR-002: Dynamic Task Arguments

## Status
Accepted

## Context

Python Celery allows tasks to accept arbitrary `*args` and `**kwargs`, providing maximum flexibility. Rust, being statically typed, requires us to decide how to represent task arguments at compile time while maintaining interoperability with Python workers.

### Options Considered

1. **Fully Dynamic**: Use `serde_json::Value` for all arguments
2. **Fully Static**: Require all tasks to define strict argument types
3. **Hybrid**: Enum wrapper allowing both dynamic and typed arguments
4. **Code Generation**: Auto-generate type-safe wrappers from signatures

## Decision

We will use a **hybrid approach** combining:

1. **Protocol Layer**: Use `serde_json::Value` in `celers-protocol` for wire format compatibility
2. **Task Layer**: Provide `#[celers::task]` macro to automatically deserialize into user-defined struct types
3. **Fallback**: Allow tasks to accept raw `TaskArgs` for dynamic handling

### Implementation

```rust
// Protocol layer (wire format)
pub struct TaskArgs {
    pub args: Vec<serde_json::Value>,
    pub kwargs: HashMap<String, serde_json::Value>,
}

// User code (type-safe)
#[derive(Deserialize)]
struct AddArgs {
    x: i32,
    y: i32,
}

#[celers::task]
async fn add(args: AddArgs) -> i32 {
    args.x + args.y
}

// The macro expands to:
// 1. Accept TaskArgs from broker
// 2. Deserialize into AddArgs
// 3. Call user function
// 4. Serialize result
```

## Consequences

### Positive

1. **Type Safety**: Users get compile-time guarantees for their task signatures
2. **Flexibility**: Tasks can opt into dynamic behavior when needed
3. **Interoperability**: Wire format is JSON-compatible with Python Celery
4. **Developer Experience**: Macros hide serialization complexity

### Negative

1. **Runtime Errors**: Type mismatches are caught at deserialization time, not compile time
2. **Debugging**: Macro-generated code can be harder to debug
3. **Complexity**: Two-layer approach (protocol + macro) requires understanding both

### Mitigations

1. **Clear Error Messages**: Provide detailed deserialization error context
2. **Validation**: Add argument validation in task definitions
3. **Documentation**: Comprehensive examples showing both approaches
4. **Testing**: Provide utilities to test task signatures against expected JSON

## References

- Python Celery: [Task Signatures](https://docs.celeryq.dev/en/stable/userguide/canvas.html#signatures)
- Serde JSON: [Arbitrary JSON Values](https://docs.rs/serde_json/latest/serde_json/enum.Value.html)
- Design influenced by web frameworks (actix-web, axum) that extract typed data from dynamic requests
