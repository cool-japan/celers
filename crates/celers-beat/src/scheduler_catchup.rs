//! Catch-up planning for the beat tick
//!
//! Turns "this entry is due" into the concrete list of fire instants a tick
//! should dispatch. Occurrences that elapsed while the scheduler was down are
//! enumerated from the schedule grid and filtered by the entry's
//! [`CatchupPolicy`], bounded by a look-back window and a per-tick rate cap so
//! a long outage cannot burst an unbounded number of dispatches.

use crate::alert::{Alert, AlertCondition, AlertLevel};
use crate::catchup::compute_missed;
use crate::config::ScheduleError;
use crate::history::CatchupPolicy;
use crate::schedule::Schedule;
use crate::scheduler::BeatScheduler;
use crate::task::ScheduledTask;
use chrono::{DateTime, Duration, Utc};
use std::sync::atomic::Ordering;

impl BeatScheduler {
    /// Compute the fire instants a task should dispatch at `now`.
    ///
    /// This is where the task's [`CatchupPolicy`] finally takes effect. The
    /// occurrences of the schedule that elapsed since the task last ran are
    /// enumerated, the most recent one is the *current* fire, and the earlier
    /// ones are genuine misses that the policy decides the fate of:
    ///
    /// * [`CatchupPolicy::Skip`] — drop every miss (one fire).
    /// * [`CatchupPolicy::RunOnce`] — replay only the most recent miss.
    /// * [`CatchupPolicy::RunMultiple`] — replay up to `max_catchup` misses.
    /// * [`CatchupPolicy::TimeWindow`] — replay misses inside the window.
    ///
    /// The returned instants are **schedule-grid occurrences**, not dispatch
    /// times. Jitter and calendars decide *whether* an occurrence is eligible
    /// yet (an occurrence is dispatched once `occurrence + jitter`, advanced
    /// past any calendar restriction, has arrived) but never change the
    /// occurrence's identity. That separation matters twice over:
    ///
    /// * The grid stays anchored. Recording a jittered instant as `last_run_at`
    ///   would re-anchor the schedule to it on every fire, turning a bounded
    ///   ±window smear into accumulating interval drift — and, for a cron slot
    ///   with a negative offset, into a slot that re-fires on every tick.
    /// * Two instances contend for the same dispatch-lock key even if their
    ///   jitter configuration differs, because the key is derived from the
    ///   occurrence rather than from the smeared dispatch moment.
    ///
    /// The result is ascending and capped at `max_catchup_fires_per_tick`.
    ///
    /// An empty vector means the task is not due.
    pub fn planned_fires(
        &self,
        task: &ScheduledTask,
        now: DateTime<Utc>,
    ) -> Result<Vec<DateTime<Utc>>, ScheduleError> {
        let is_onetime = task.schedule.is_onetime();
        let enum_base = match task.last_run_at {
            Some(last_run) => last_run,
            None => match &task.schedule {
                // A never-run one-time task must be able to reach `run_at`,
                // even when it was scheduled for a moment before registration.
                Schedule::OneTime { run_at } => {
                    std::cmp::min(task.created_at, *run_at - Duration::seconds(1))
                }
                _ => task.created_at,
            },
        };

        // Bound how far back we enumerate; a one-second interval that has not
        // fired for a year would otherwise walk tens of millions of
        // occurrences. One-time schedules yield at most one occurrence, so they
        // are exempt.
        let lookback = self.catchup_lookback_secs.max(0);
        let horizon = now - Duration::seconds(lookback);
        let (enum_from, clamped) = if !is_onetime && enum_base < horizon {
            (horizon, true)
        } else {
            (enum_base, false)
        };

        // A negative jitter offset makes an occurrence eligible *before* its
        // grid instant, so enumeration has to reach that far ahead or the
        // early half of a symmetric window would never fire.
        let early_window = task
            .jitter
            .as_ref()
            .map(|jitter| (-jitter.min_seconds).max(0))
            .unwrap_or(0);
        let enum_until = now
            .checked_add_signed(Duration::try_seconds(early_window).unwrap_or_else(Duration::zero))
            .unwrap_or(now);

        let missed = compute_missed(&task.schedule, enum_from, enum_until)?;
        if clamped || missed.truncated {
            tracing::warn!(
                task = %task.name,
                lookback_secs = lookback,
                "catch-up enumeration was truncated; occurrences older than the \
                 look-back window will not be replayed"
            );
        }

        // Keep the occurrences whose *dispatch* moment (jitter + calendars
        // applied) has arrived, but keep carrying the grid occurrence itself.
        let eligible: Vec<DateTime<Utc>> = missed
            .instants
            .iter()
            .copied()
            .filter(|occurrence| task.dispatch_time_for(*occurrence) <= now)
            .collect();

        let Some((current, misses)) = eligible.split_last() else {
            // The schedule has not produced an occurrence yet. An explicit
            // "run once at startup" entry still fires, keyed on its
            // registration instant rather than on the wall clock — a
            // clock-relative instant would yield a fresh dispatch-lock key on
            // every evaluation and defeat mutual exclusion.
            if task.run_on_startup && task.last_run_at.is_none() {
                return Ok(vec![task.created_at]);
            }
            return Ok(Vec::new());
        };

        let mut fires = self.select_catchup_fires(&task.catchup_policy, misses, now);
        fires.push(*current);

        // Rate cap: keep the most recent fires so a long outage cannot burst an
        // unbounded number of dispatches in a single tick.
        let cap = self.max_catchup_fires_per_tick.max(1);
        if fires.len() > cap {
            let skipped = fires.len() - cap;
            tracing::warn!(
                task = %task.name,
                skipped,
                cap,
                "catch-up fires exceeded the per-tick cap; oldest occurrences dropped"
            );
            fires.drain(0..skipped);
        }

        Ok(fires)
    }

    /// Apply a task's catch-up policy to its *missed* occurrences (the current
    /// fire is always dispatched and is not part of `misses`).
    fn select_catchup_fires(
        &self,
        policy: &CatchupPolicy,
        misses: &[DateTime<Utc>],
        now: DateTime<Utc>,
    ) -> Vec<DateTime<Utc>> {
        if misses.is_empty() {
            return Vec::new();
        }

        // Map onto the occurrence-list policy so both catch-up models share one
        // implementation of "which of these instants do we fire".
        let selected = policy.occurrence_policy().apply(misses);

        match policy {
            CatchupPolicy::RunMultiple { max_catchup } => {
                let keep = (*max_catchup as usize).min(selected.len());
                selected[selected.len() - keep..].to_vec()
            }
            CatchupPolicy::TimeWindow { window_seconds } => {
                let cutoff = now - Duration::seconds(*window_seconds as i64);
                selected.into_iter().filter(|t| *t >= cutoff).collect()
            }
            CatchupPolicy::Skip | CatchupPolicy::RunOnce => selected,
        }
    }

    /// Configure how far back catch-up enumeration looks, in seconds.
    ///
    /// Occurrences older than this are not replayed; the truncation is logged.
    pub fn with_catchup_lookback_secs(&mut self, seconds: i64) -> &mut Self {
        self.catchup_lookback_secs = seconds.max(0);
        self
    }

    /// Configure the maximum number of fires a single task may produce in one
    /// tick (the catch-up rate cap). Values below 1 are treated as 1.
    pub fn with_max_catchup_fires_per_tick(&mut self, max_fires: usize) -> &mut Self {
        self.max_catchup_fires_per_tick = max_fires.max(1);
        self
    }

    /// Collect the fires this tick should dispatch, in priority order.
    ///
    /// Returns `(task_name, fire_instant)` pairs. A task with missed
    /// occurrences and a replaying [`CatchupPolicy`] contributes one pair per
    /// surviving occurrence.
    pub fn collect_due_fires(&self, now: DateTime<Utc>) -> Vec<(String, DateTime<Utc>)> {
        let mut fires = Vec::new();

        for task in self.get_due_tasks_by_priority() {
            match self.planned_fires(task, now) {
                Ok(instants) => {
                    for instant in instants {
                        fires.push((task.name.clone(), instant));
                    }
                }
                Err(e) => {
                    self.schedule_eval_errors.fetch_add(1, Ordering::Relaxed);
                    tracing::error!(
                        task = %task.name,
                        error = %e,
                        "failed to compute fire instants; task will not be dispatched"
                    );
                }
            }
        }

        fires
    }

    /// Raise an alert for every task whose schedule currently fails to
    /// evaluate, so a permanently inert task is visible to operators.
    pub(crate) fn raise_schedule_evaluation_alerts(&mut self) {
        let broken: Vec<(String, String)> = self
            .tasks
            .values()
            // A spent one-time schedule cannot produce a further instant; that
            // is completion, not breakage.
            .filter(|task| task.enabled && !task.is_spent_onetime())
            .filter_map(|task| match task.validate_schedule() {
                Ok(()) => None,
                Err(e) => Some((task.name.clone(), e.to_string())),
            })
            .collect();

        for (task_name, issue) in broken {
            let alert = Alert::new(
                task_name.clone(),
                AlertLevel::Critical,
                AlertCondition::TaskUnhealthy {
                    issues: vec![format!("schedule evaluation failed: {}", issue)],
                },
                format!(
                    "Task '{}' has an unevaluable schedule and will never run: {}",
                    task_name, issue
                ),
            );
            self.alert_manager.record_alert(alert);
        }
    }
}
