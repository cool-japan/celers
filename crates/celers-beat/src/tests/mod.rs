//! Test suite for celers-beat
//!
//! Tests are split into submodules by functionality:
//! - tests_schedule: Schedule types, task basics, scheduler, persistence, jitter, catch-up, groups, retry
//! - tests_execution: Execution history, health checks, metrics, statistics, versioning, dependencies
//! - tests_advanced: Lock manager, conflict detection, timezone, scheduler loop, WFQ, heartbeat
//! - tests_hardening: Regression tests for scheduler-correctness defects
//!   (dispatch locking, catch-up, persistence durability, jitter stability)

mod tests_advanced;
mod tests_execution;
mod tests_hardening;
mod tests_schedule;
