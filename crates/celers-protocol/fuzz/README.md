# celers-protocol-fuzz

Fuzz testing suite for the `celers-protocol` crate. Uses `libfuzzer-sys` via `cargo-fuzz` to find panics, crashes, and logic errors in protocol serialization and message handling.

## Fuzz Targets

| Target | Description |
|--------|-------------|
| `fuzz_message` | Deserialize arbitrary bytes as `Message`, validate, and round-trip |
| `fuzz_serializer` | Round-trip fuzz testing of `JsonSerializer` |
| `fuzz_compression` | Fuzz compression/decompression codepaths |
| `fuzz_security` | Security-focused fuzzing for malicious inputs |

## Requirements

```bash
cargo install cargo-fuzz
```

## Running

Run a specific fuzz target:

```bash
cd crates/celers-protocol
cargo fuzz run fuzz_message
cargo fuzz run fuzz_serializer
cargo fuzz run fuzz_compression
cargo fuzz run fuzz_security
```

Run with a time limit:

```bash
cargo fuzz run fuzz_message -- -max_total_time=300
```

## Corpus

Fuzzing corpora are stored in `corpus/<target_name>/` and are automatically managed by `cargo-fuzz`. Interesting inputs that trigger new code paths are saved for future regression testing.

## Part of CeleRS

This crate is part of the [CeleRS](https://github.com/cool-japan/celers) project, a Celery-compatible distributed task queue for Rust.

## License

Apache-2.0

Copyright (c) COOLJAPAN OU (Team Kitasan)
