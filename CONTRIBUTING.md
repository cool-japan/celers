# Contributing to CeleRS

CeleRS is developed in the open but built and released by COOLJAPAN OU (Team KitaSan). This
document is a factual description of how the repository actually works today — the gates a change
has to pass, the suites that need a live service, and how a release actually gets cut — not an
aspirational process. If something below and the repository disagree, the repository is right;
please open an issue.

## Before you start

- **Read [CHANGELOG.md](CHANGELOG.md) and [TODO.md](TODO.md) → "Known gaps"** for the current,
  evidence-backed list of what is missing or partial. A change that closes one of those gaps should
  update the row/entry it closes.
- **Read [docs/CELERY_COMPATIBILITY.md](docs/CELERY_COMPATIBILITY.md)** before touching anything
  under `celers-protocol`, a broker, or a result backend. Its status column (✅/🟢/🟡/🔴/⛔) is only
  trustworthy if every row still names a real test; if your change moves a row, move the evidence
  pointer with it in the same commit.
- For anything nontrivial, open an issue or draft PR first. This is a small team maintaining a large
  surface (18 published crates); a design conversation before code saves everyone a rewrite.

## Development setup

**MSRV**: 1.89 for the workspace, except `celers-broker-sqs` — and therefore `celers/sqs`,
`celers/full`, and any `--all-features` build — which needs **1.94.1** because its AWS SDK
dependencies (`aws-config`, `aws-sdk-sqs`, `aws-sdk-cloudwatch`) are not optional. Both numbers are
declared as `rust-version` in the respective manifests; re-derive the effective floor for your build
with `cargo metadata --format-version 1 --all-features` and take the highest `rust_version` in the
resolved graph, rather than trusting either number in isolation.

```bash
git clone https://github.com/cool-japan/celers.git
cd celers

# Build and run the default-feature test set
cargo build --workspace
cargo nextest run --workspace

# Build and run everything, every broker/backend included
cargo build --workspace --all-features
cargo nextest run --workspace --all-features

# Doctests -- nextest does not run these
cargo test --doc --workspace --all-features
```

`cargo nextest` is not optional tooling here: `cargo test` still works, but every command in this
document and in [tests/integration/README.md](tests/integration/README.md) assumes nextest's
`--run-ignored` and per-crate `-p` filtering. Install it with `cargo install cargo-nextest`.

The repository root is a **virtual workspace** with no `[package]` of its own. `cargo run --example
<name>` with no `-p` fails to select a package — pass `-p celers-examples` (or `-p celers-cli`, which
also ships benches under the same names). Two workspace members are never published
(`publish = false`) but are still built and tested by every `--workspace` command:
`celers-examples` (runnable examples and benchmarks) and `celers-facade-test` (pins the minimum
dependency set `#[celers::task]` needs downstream).

### Running the env-gated, live-service suites

Roughly 63 tests only assert anything when a matching `CELERS_TEST_*` (or, for two crates, a
differently-named) environment variable points at a reachable service; unset, they print an
`eprintln!` skip line and report **PASS having run zero assertions** — see
[tests/integration/README.md](tests/integration/README.md) for how to tell the two apart. A further
~114 tests are `#[ignore]`d outright and are not attempted at all without `--run-ignored all`. A
green default run therefore tells you much less than it looks like; if your change touches broker or
backend code, run the suite that actually exercises it before opening a PR.

Bring up the services with the root `docker-compose.yml`:

```bash
docker-compose up -d                    # redis, postgres, rabbitmq, prometheus, grafana, worker
docker-compose --profile test up -d     # + mysql, localstack (SQS emulator)
docker-compose --profile python-compat up -d   # + a real Celery worker image
```

Then export the variable(s) the suite you're touching reads and run with `--run-ignored all` so both
gating styles are covered — [tests/integration/README.md](tests/integration/README.md) has the full
variable → service → crate → exact-invocation table; do not duplicate it here, it drifts. The short
version, for a local Redis at the default port:

```bash
CELERS_TEST_REDIS_URL=redis://127.0.0.1:6379 \
  cargo nextest run --workspace --all-features --run-ignored all
```

### Python Celery interoperability

Celery-wire compatibility is proved, not asserted, in [`tests/python-compat/`](tests/python-compat/):
a real `celery -A tasks worker` and a real Redis, with the CeleRS side going through
`celers-protocol`'s own public API. Run it with:

```bash
CELERS_TEST_REDIS_URL=redis://127.0.0.1:6379 tests/python-compat/run.sh
```

`run.sh` is idempotent — it creates a virtualenv under `$TMPDIR` (never in the source tree), installs
a pinned Celery, builds the Rust bridge (`crates/celers-protocol/examples/celery_bridge.rs`), and runs
the suite. Without a reachable Redis it prints a `SKIPPED:` line and exits 0. `run.sh capture`
re-records the verbatim wire captures under `crates/celers-protocol/tests/fixtures/` after a Celery
upgrade — read the diff before committing a re-capture; a changed fixture is a claim that Celery's
wire format itself changed. See [tests/python-compat/README.md](tests/python-compat/README.md) for
what each file in that directory does.

## Code standards

These are enforced mechanically, not by review discretion. A PR that fails any of these will not be
merged regardless of what it fixes:

- **No warnings.** `cargo clippy --workspace --all-targets --all-features -- -D warnings` must be
  clean. Fix the lint; do not `#[allow]` it away without a comment explaining why the lint is wrong
  for this call site.
- **Documentation builds clean.** `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features
  --no-deps --keep-going` must be clean — broken intra-doc links included. Always pass
  `--keep-going`: without it, `cargo doc` stops scheduling new crates once enough failures
  accumulate, so a run that "passes" can still be hiding errors in crates it never reached.
- **No `unwrap()` / `expect()` outside test code.** Production code returns a typed error
  (`CelersError`) or documents, in a comment at the call site, exactly why the panic is genuinely
  unreachable — and prefers a safe fallback over a panic even then. `#[cfg(test)]` code and files
  under `tests/` are exempt.
- **Pure Rust by default.** No C, C++, Fortran, or vendored assembly in a default build, with
  `--all-features`, or under the `celers` facade's `full` feature. This is enforced by
  [`deny.toml`](deny.toml)'s `[bans] deny` list and its `[graph] exclude`, which is currently
  **empty** — every workspace member is checked, `celers-broker-sqs` included. Do not add anything
  back to `exclude`: it hides a whole subtree from every ban, not just the one crate that motivated
  adding it. If your change needs a new dependency, check the COOLJAPAN replacement table before
  reaching for the usual crate — `bincode` → `oxicode`, `zip`/`flate2`/`zstd`/`tar` → `oxiarc-*`,
  `rusqlite` → `oxisql-sqlite-compat`, `rustfft` → `oxifft`, and any TLS/crypto FFI → `oxitls`. Run
  `cargo deny check bans` before opening a PR; `cargo tree -e features -i aws-lc-sys --all-features`
  should report "did not match any packages".
- **Formatting.** `cargo fmt --all --check` must pass. Run `cargo fmt --all` before committing.
- **File size.** Keep source files under 2000 lines; split into submodules when a file approaches the
  limit rather than after it crosses it. (One file is currently over —
  `crates/celers-beat/src/tests/tests_schedule.rs` at 2004 lines — tracked in
  [TODO.md](TODO.md#known-gaps--the-roadmap-after-031) rather than hidden.)
- **Naming.** Standard Rust conventions (`snake_case` items, `UpperCamelCase` types), enforced by
  `clippy`'s style lints as part of the no-warnings gate above.
- **Real implementations.** Do not land a stub, a `todo!()`, or a test weakened to pass around a bug.
  A behavior change needs a test that would fail without it.
- **Workspace-level dependencies.** Declare a crate's version once in the root `Cargo.toml`'s
  `[workspace.dependencies]` and reference it from member manifests as `dep = { workspace = true }`
  (or `dep.workspace = true`). Don't pin a version in an individual crate's `Cargo.toml`.

Every one of the checks above (build, clippy, tests, doctests, rustdoc, fmt, deny) is what
[README.md → Contributing](README.md#-contributing) lists as the local equivalent of CI — run them
before pushing, in this order, since an early failure makes the later ones moot:

```bash
cargo build --workspace --all-features
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo nextest run --workspace --all-features
cargo test --doc --workspace --all-features
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --keep-going
cargo fmt --all --check
cargo deny check bans
```

## Documentation changes

Markdown lives at the repository root and under `docs/`; there is deliberately no separate wiki. A
few rules that keep the docs honest, learned the hard way during the 0.3.1 hardening pass:

- A status claim (a test count, a "✅ Implemented" row, a feature-default statement) needs a
  re-check against the source it describes, not a copy-forward from the last release. Numbers drift
  every time a test file gains or loses a case.
- [docs/CELERY_COMPATIBILITY.md](docs/CELERY_COMPATIBILITY.md)'s status symbols are load-bearing:
  ✅ **Interop-verified** means a named test puts CeleRS on one side of the wire and a real Python
  Celery on the other. Do not upgrade a row to ✅ without that test existing.
- Prefer a dated snapshot ("verified 2026-08-26 with `<exact command>`: N tests") over a claim that
  reads as live and evergreen — the former stays true after it's superseded; the latter just becomes
  wrong.

## Continuous integration and releases

There is currently **no CI that runs on push or on a PR**. `.github/` holds only `dependabot.yml`,
`FUNDING.yml`, and a `workflows.disabled/` directory (`ci.yml`, `release.yml`) that is not active.
This means the gates in [Code standards](#code-standards) above are enforced by running them
yourself and by review, not by a bot — please run the full local sequence before opening a PR, and
say in the PR description which of the env-gated suites you exercised and against what.

Releases are cut manually. Version bumps happen on a `0.x.y` development branch; `master`/`main`
receives only the release commit itself (conventionally `Availability of X.Y.Z`, matching the
existing tags for 0.1.0/0.2.0/0.3.0) once that branch's `Cargo.toml` and `CHANGELOG.md` are ready.
Publishing to crates.io is a separate, deliberate step taken by a maintainer — it is not triggered by
merging to `master`. If your change is release-relevant (a new feature, a breaking change, a fixed
defect), add an entry to the `[Unreleased]`/in-progress section of [CHANGELOG.md](CHANGELOG.md) in
the same PR; don't leave it for the release to reconstruct from `git log`.

## License

CeleRS is licensed under [Apache-2.0](LICENSE). By contributing, you agree your contribution is
licensed under the same terms. (Every source file's `SPDX-License-Identifier` header, where
present, was normalized to `Apache-2.0` during the 0.3.1 hardening pass — see
[CHANGELOG.md](CHANGELOG.md)'s Fixed section; none carries the stale `MIT OR Apache-2.0` header
left over from the project's early dual-license plan, and no `LICENSE-MIT` is present.)
