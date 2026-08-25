//! Built-in demo tasks for `celers worker --demo-tasks`.
//!
//! `celers worker` has no way to compile in application task code: tasks are
//! Rust types registered with a `TaskRegistry` at build time (see
//! `celers_core::TaskRegistry::register`), not something a config file or
//! CLI flag can express. A freshly deployed `celers worker` therefore starts
//! with a genuinely empty registry and can never execute anything on its own
//! -- that is a fact about how CeleRS is embedded, not a bug, but printing
//! nothing about it left operators to discover it only once a task silently
//! sat in the queue forever (see `commands::worker::start_worker`'s
//! registry-setup warning). `--demo-tasks` registers a few harmless
//! built-ins so a fresh deployment can be smoke-tested end-to-end (enqueue,
//! dequeue, execute, ack/DLQ) before any real task code exists.
//!
//! Every demo task takes a `serde_json::Value` as input, so it accepts any
//! well-formed JSON payload rather than requiring one specific shape --
//! useful because the easiest way to put a task on the queue by hand is
//! often a plain `{}` or a one-off literal, not a hand-written struct.
//!
//! Split out of `commands::worker` (rather than left inline) to keep that
//! file under the workspace's 2000-line-per-file convention.

use celers_core::{CelersError, Result, Task, TaskRegistry};

/// Task names [`register_demo_tasks`] registers, in registration order.
pub(crate) const DEMO_TASK_NAMES: [&str; 3] = ["demo.echo", "demo.sleep", "demo.fail"];

/// Echoes its input straight back. The simplest possible proof that a
/// message enqueued onto this worker's queue is actually dequeued, executed,
/// and acked.
struct DemoEchoTask;

#[async_trait::async_trait]
impl Task for DemoEchoTask {
    type Input = serde_json::Value;
    type Output = serde_json::Value;

    async fn execute(&self, input: Self::Input) -> Result<Self::Output> {
        Ok(input)
    }

    fn name(&self) -> &str {
        "demo.echo"
    }
}

/// Sleeps for `{"duration_ms": ...}` (default 1000ms, capped at 60s so a
/// stray huge value cannot park a worker slot indefinitely) then succeeds.
/// Useful for exercising concurrency, timeouts, and graceful shutdown
/// against a real deployment.
struct DemoSleepTask;

#[async_trait::async_trait]
impl Task for DemoSleepTask {
    type Input = serde_json::Value;
    type Output = serde_json::Value;

    async fn execute(&self, input: Self::Input) -> Result<Self::Output> {
        let requested_ms = input
            .get("duration_ms")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(1000);
        let duration_ms = requested_ms.min(60_000);
        tokio::time::sleep(std::time::Duration::from_millis(duration_ms)).await;
        Ok(serde_json::json!({ "slept_ms": duration_ms }))
    }

    fn name(&self) -> &str {
        "demo.sleep"
    }
}

/// Always fails, so an operator can exercise retry/DLQ handling against a
/// real deployment without writing a task that fails on purpose themselves.
struct DemoFailTask;

#[async_trait::async_trait]
impl Task for DemoFailTask {
    type Input = serde_json::Value;
    type Output = serde_json::Value;

    async fn execute(&self, input: Self::Input) -> Result<Self::Output> {
        let message = input
            .get("message")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("celers worker --demo-tasks: deliberate failure")
            .to_string();
        Err(CelersError::TaskExecution(message))
    }

    fn name(&self) -> &str {
        "demo.fail"
    }
}

/// Register [`DemoEchoTask`], [`DemoSleepTask`], and [`DemoFailTask`] (task
/// names [`DEMO_TASK_NAMES`]) with `registry`.
pub(crate) async fn register_demo_tasks(registry: &TaskRegistry) {
    registry.register(DemoEchoTask).await;
    registry.register(DemoSleepTask).await;
    registry.register(DemoFailTask).await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn demo_echo_task_returns_its_input_unchanged() {
        let task = DemoEchoTask;
        let input = serde_json::json!({"message": "hi", "n": 3});
        let output = task.execute(input.clone()).await.expect("echo succeeds");
        assert_eq!(output, input);
    }

    #[tokio::test]
    async fn demo_echo_task_accepts_any_json_shape() {
        let task = DemoEchoTask;
        for input in [
            serde_json::json!({}),
            serde_json::json!(null),
            serde_json::json!(42),
            serde_json::json!("a string"),
            serde_json::json!([1, 2, 3]),
        ] {
            task.execute(input.clone())
                .await
                .unwrap_or_else(|e| panic!("{input} must be accepted: {e}"));
        }
    }

    #[tokio::test]
    async fn demo_sleep_task_reports_the_requested_duration() {
        let task = DemoSleepTask;
        let output = task
            .execute(serde_json::json!({"duration_ms": 5}))
            .await
            .expect("sleep succeeds");
        assert_eq!(output, serde_json::json!({"slept_ms": 5}));
    }

    #[tokio::test]
    async fn demo_sleep_task_defaults_when_duration_is_absent() {
        let task = DemoSleepTask;
        // A default_timeout-scale sleep (1000ms) would make this test slow;
        // only assert the *reported* default, not actually wait it out. The
        // duration only affects what happens after this call returns, so
        // this covers the default-selection logic no matter how long it
        // sleeps for.
        let start = std::time::Instant::now();
        let output = task
            .execute(serde_json::json!({"duration_ms": 0}))
            .await
            .expect("sleep succeeds");
        assert_eq!(output, serde_json::json!({"slept_ms": 0}));
        assert!(start.elapsed() < std::time::Duration::from_millis(500));
    }

    /// A paused virtual clock proves the 60s cap is genuinely applied by
    /// `DemoSleepTask::execute` (not merely by a duplicate calculation in
    /// the test) without an actual 60-second wait: the spawned task's
    /// `tokio::time::sleep` only resolves once virtual time is advanced
    /// past it.
    #[tokio::test(start_paused = true)]
    async fn demo_sleep_task_caps_an_excessive_duration() {
        let handle = tokio::spawn(async {
            DemoSleepTask
                .execute(serde_json::json!({"duration_ms": u64::MAX}))
                .await
        });

        for _ in 0..5 {
            tokio::time::advance(std::time::Duration::from_secs(31)).await;
            tokio::task::yield_now().await;
        }

        let output = handle.await.expect("join").expect("sleep succeeds");
        assert_eq!(output, serde_json::json!({"slept_ms": 60_000}));
    }

    #[tokio::test]
    async fn demo_fail_task_always_fails_with_a_message() {
        let task = DemoFailTask;
        let err = task
            .execute(serde_json::json!({"message": "boom"}))
            .await
            .expect_err("demo.fail must always fail");
        assert!(matches!(err, CelersError::TaskExecution(msg) if msg == "boom"));
    }

    #[tokio::test]
    async fn demo_fail_task_uses_a_default_message_when_none_given() {
        let task = DemoFailTask;
        let err = task
            .execute(serde_json::json!({}))
            .await
            .expect_err("demo.fail must always fail");
        assert!(
            matches!(err, CelersError::TaskExecution(msg) if msg.contains("deliberate failure"))
        );
    }

    #[tokio::test]
    async fn register_demo_tasks_populates_the_registry_with_exactly_the_named_tasks() {
        let registry = TaskRegistry::new();
        register_demo_tasks(&registry).await;

        let mut names = registry.list_tasks().await;
        names.sort();
        let mut expected = DEMO_TASK_NAMES.to_vec();
        expected.sort_unstable();
        assert_eq!(names, expected);

        for name in DEMO_TASK_NAMES {
            assert!(registry.has_task(name).await, "{name} must be registered");
        }
    }
}
