//! Drive the live Python Celery interoperability suite from `cargo test`.
//!
//! `tests/python-compat` is a pytest suite that runs a real Celery client and a
//! real `celery -A tasks worker` against a real Redis, piping every message
//! that crosses the boundary through the `celery_bridge` example so that the
//! CeleRS side is this crate's own public API. This test is the Rust entry
//! point to it, so the round trip is reachable from the same command as
//! everything else.
//!
//! # Running it
//!
//! ```sh
//! cargo build -p celers-protocol --example celery_bridge
//! CELERS_TEST_REDIS_URL=redis://127.0.0.1:6379 \
//! CELERS_PYTHON=/path/to/venv/bin/python \
//!   cargo test -p celers-protocol --test python_interop
//! ```
//!
//! `tests/python-compat/run.sh` creates that venv, builds the bridge and runs
//! the same suite directly; use it when iterating on the Python side.
//!
//! # Gating
//!
//! Without a Redis, without a Python that has Celery installed, or without a
//! built bridge, this **skips visibly** -- it prints a `SKIPPED:` line naming
//! the variable to set and returns. It never passes quietly, and it never
//! fails for want of an optional service.
//!
//! The assertions that need no services at all are in `celery_golden.rs`, which
//! checks this crate against the fixtures the Python suite recorded.

use std::path::{Path, PathBuf};
use std::process::Command;

/// The repository root, derived from this crate's manifest directory.
fn repo_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(2)
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."))
}

fn suite_dir() -> PathBuf {
    repo_root().join("tests").join("python-compat")
}

/// Where cargo put the `celery_bridge` example for this build.
///
/// The test binary lives in `<target>/<profile>/deps/`, so its grandparent is
/// the profile directory that also holds `examples/`. `CELERS_BRIDGE` overrides
/// the search for anyone who has built it elsewhere.
fn bridge_path() -> Option<PathBuf> {
    if let Ok(explicit) = std::env::var("CELERS_BRIDGE") {
        let path = PathBuf::from(explicit);
        return path.is_file().then_some(path);
    }
    let exe = std::env::current_exe().ok()?;
    let profile_dir = exe.parent()?.parent()?;
    let candidate = profile_dir.join("examples").join("celery_bridge");
    candidate.is_file().then_some(candidate)
}

/// Print one skip line and return `true` when a prerequisite is absent.
fn skip(reason: &str, hint: &str) -> bool {
    eprintln!("SKIPPED: python_interop -- {reason} (set {hint} to run)");
    true
}

#[test]
fn the_python_celery_interop_suite_passes() {
    let Ok(redis_url) = std::env::var("CELERS_TEST_REDIS_URL") else {
        skip("no Redis configured", "CELERS_TEST_REDIS_URL");
        return;
    };
    let Ok(python) = std::env::var("CELERS_PYTHON") else {
        skip(
            "no Celery interpreter configured; tests/python-compat/run.sh builds one",
            "CELERS_PYTHON",
        );
        return;
    };
    if !Path::new(&python).is_file() {
        skip(
            &format!("CELERS_PYTHON={python} is not a file"),
            "CELERS_PYTHON",
        );
        return;
    }
    let Some(bridge) = bridge_path() else {
        skip(
            "the celery_bridge example is not built \
             (cargo build -p celers-protocol --example celery_bridge)",
            "CELERS_BRIDGE",
        );
        return;
    };

    let suite = suite_dir();
    assert!(
        suite.join("conftest.py").is_file(),
        "the interop suite is missing from {}",
        suite.display()
    );

    // A missing `celery` is a *configuration* problem, not a protocol failure,
    // so it skips rather than failing -- but it says which interpreter it
    // checked, because "celery is not installed" is otherwise baffling when a
    // venv exists three directories away.
    let probe = Command::new(&python)
        .args(["-c", "import celery, redis, pytest"])
        .output()
        .expect("the configured CELERS_PYTHON must be executable");
    if !probe.status.success() {
        skip(
            &format!(
                "{python} cannot import celery/redis/pytest: {}",
                String::from_utf8_lossy(&probe.stderr).trim()
            ),
            "CELERS_PYTHON",
        );
        return;
    }

    let output = Command::new(&python)
        .args(["-m", "pytest", "-q", "--color=no"])
        .current_dir(&suite)
        .env("CELERS_TEST_REDIS_URL", &redis_url)
        .env("CELERS_PYTHON", &python)
        .env("CELERS_BRIDGE", &bridge)
        .env("PYTHONPATH", &suite)
        .env("PYTHONDONTWRITEBYTECODE", "1")
        .output()
        .expect("pytest must be launchable");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    eprintln!("--- tests/python-compat ---\n{stdout}\n{stderr}");

    assert!(
        output.status.success(),
        "the Python Celery interop suite failed (exit {:?}).\n\
         Reproduce with: CELERS_TEST_REDIS_URL={redis_url} tests/python-compat/run.sh",
        output.status.code()
    );

    // pytest exits 0 when it collects nothing, which would make this test a
    // reassuring no-op; require that it actually ran something.
    assert!(
        stdout.contains(" passed"),
        "pytest reported no passing tests; it may have collected nothing:\n{stdout}"
    );
}
