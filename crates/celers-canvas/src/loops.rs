//! Workflow loops and iteration primitives.
//!
//! This module provides [`WorkflowLoop`], a primitive that repeats a *body*
//! workflow either a bounded number of times or while a **pure** predicate over
//! an accumulator holds (always under a hard maximum-iteration cap), expanding
//! deterministically into a flat [`Chain`] of iterations. It also provides
//! [`WorkflowMap`], a map-style iteration that applies a body chain over a list
//! of inputs, producing one parallel branch per input.
//!
//! Unlike the runtime-evaluated [`crate::WhileLoop`]/[`crate::ForEach`] helpers
//! (which describe a loop to a worker), the primitives here are *expanded ahead
//! of time* on the client into concrete [`Chain`]/[`crate::NestedGroup`] values.
//! Because the predicate and the accumulator update are pure and deterministic,
//! `expand()` is fully reproducible and contains no clocks or randomness.
//!
//! # Example
//!
//! ```
//! use celers_canvas::{WorkflowLoop, Chain};
//!
//! // Repeat a two-step body exactly 3 times -> 6 tasks.
//! let body = Chain::new().then("fetch", vec![]).then("store", vec![]);
//! let looped = WorkflowLoop::times(body, 3);
//! let chain = looped.expand().expect("bounded loop expands");
//! assert_eq!(chain.len(), 6);
//! ```

use crate::{CanvasError, Chain, Condition, NestedChain, NestedGroup, Signature};
use serde::{Deserialize, Serialize};

/// Hard upper bound applied to *every* loop expansion, regardless of the
/// configured cap, so a malformed predicate can never expand without limit.
pub const ABSOLUTE_MAX_ITERATIONS: u32 = 100_000;

/// A pure, deterministic update applied to the loop accumulator after each
/// iteration of a [`LoopBound::While`] loop.
///
/// The accumulator is a single [`serde_json::Value`]; updates are total
/// functions with no side effects so loop expansion is reproducible.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AccumulatorUpdate {
    /// Leave the accumulator unchanged (useful when the body is purely for
    /// effect and the predicate is driven by the iteration count instead).
    Identity,

    /// Add `step` to a numeric accumulator. A non-numeric accumulator is
    /// treated as `0` before the addition.
    AddNumber(f64),

    /// Multiply a numeric accumulator by `factor`. A non-numeric accumulator is
    /// treated as `0` before the multiplication.
    MulNumber(f64),

    /// Replace the accumulator with a fixed value.
    Set(serde_json::Value),
}

impl AccumulatorUpdate {
    /// Apply the update to `acc`, returning the next accumulator value.
    pub fn apply(&self, acc: &serde_json::Value) -> serde_json::Value {
        match self {
            Self::Identity => acc.clone(),
            Self::AddNumber(step) => {
                let current = acc.as_f64().unwrap_or(0.0);
                json_number(current + step)
            }
            Self::MulNumber(factor) => {
                let current = acc.as_f64().unwrap_or(0.0);
                json_number(current * factor)
            }
            Self::Set(value) => value.clone(),
        }
    }
}

/// Convert an `f64` to a JSON value, preserving integers as integers so that
/// expansions compare cleanly in tests and serialised output.
fn json_number(value: f64) -> serde_json::Value {
    if value.fract() == 0.0 && value.is_finite() && value.abs() < i64::MAX as f64 {
        serde_json::Value::from(value as i64)
    } else {
        serde_json::Number::from_f64(value)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null)
    }
}

/// How a [`WorkflowLoop`] decides when to stop.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LoopBound {
    /// Repeat the body exactly `count` times.
    Times {
        /// Number of iterations.
        count: u32,
    },

    /// Repeat the body while `predicate` holds for the current accumulator,
    /// updating the accumulator with `update` after each iteration. Expansion
    /// always stops once `max_iterations` is reached even if the predicate is
    /// still true, guaranteeing termination.
    While {
        /// Pure predicate evaluated against the accumulator before each
        /// iteration. The body runs only while this is `true`.
        predicate: Condition,
        /// Initial accumulator value.
        initial: serde_json::Value,
        /// Pure update applied to the accumulator after each iteration.
        update: AccumulatorUpdate,
        /// Hard cap on the number of iterations (safety limit).
        max_iterations: u32,
    },
}

/// The body of a [`WorkflowLoop`]: the workflow repeated on each iteration.
///
/// Storing the body as a [`Chain`] keeps expansion linearisable into a flat
/// chain. Each iteration's tasks may optionally be suffixed with the iteration
/// index so that repeated tasks remain individually addressable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoopBody {
    /// The chain executed once per iteration.
    pub chain: Chain,

    /// When `true`, every task name in an iteration is suffixed with
    /// `_iter<N>` (zero-based) during expansion. This is invaluable for
    /// monitoring/DAG export where each iteration must be a distinct node.
    pub suffix_iterations: bool,
}

impl LoopBody {
    /// Wrap a chain as a loop body (no per-iteration suffixing).
    pub fn new(chain: Chain) -> Self {
        Self {
            chain,
            suffix_iterations: false,
        }
    }

    /// Enable per-iteration task-name suffixing (`task_iter0`, `task_iter1`, …).
    pub fn with_iteration_suffix(mut self) -> Self {
        self.suffix_iterations = true;
        self
    }

    /// Produce the tasks for iteration `index`, applying suffixing if enabled.
    fn tasks_for_iteration(&self, index: u32) -> Vec<Signature> {
        self.chain
            .tasks
            .iter()
            .map(|sig| {
                let mut next = sig.clone();
                if self.suffix_iterations {
                    next.task = format!("{}_iter{}", next.task, index);
                }
                next
            })
            .collect()
    }
}

impl From<Chain> for LoopBody {
    fn from(chain: Chain) -> Self {
        Self::new(chain)
    }
}

/// A bounded workflow loop that expands into a flat [`Chain`] of iterations.
///
/// See the module documentation for the design rationale.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowLoop {
    /// The workflow repeated on each iteration.
    pub body: LoopBody,

    /// Termination policy.
    pub bound: LoopBound,
}

impl WorkflowLoop {
    /// Create a loop that repeats `body` exactly `count` times.
    pub fn times(body: impl Into<LoopBody>, count: u32) -> Self {
        Self {
            body: body.into(),
            bound: LoopBound::Times { count },
        }
    }

    /// Create a `while`-style loop driven by a pure predicate over an
    /// accumulator. `max_iterations` is a hard cap that guarantees termination
    /// even if the predicate never becomes false.
    pub fn while_loop(
        body: impl Into<LoopBody>,
        predicate: Condition,
        initial: serde_json::Value,
        update: AccumulatorUpdate,
        max_iterations: u32,
    ) -> Self {
        Self {
            body: body.into(),
            bound: LoopBound::While {
                predicate,
                initial,
                update,
                max_iterations,
            },
        }
    }

    /// Enable per-iteration task-name suffixing on the body.
    pub fn with_iteration_suffix(mut self) -> Self {
        self.body.suffix_iterations = true;
        self
    }

    /// Compute the number of iterations this loop will expand to *without*
    /// materialising the body, honouring the absolute cap.
    pub fn iteration_count(&self) -> u32 {
        match &self.bound {
            LoopBound::Times { count } => (*count).min(ABSOLUTE_MAX_ITERATIONS),
            LoopBound::While {
                predicate,
                initial,
                update,
                max_iterations,
            } => {
                let cap = (*max_iterations).min(ABSOLUTE_MAX_ITERATIONS);
                let mut acc = initial.clone();
                let mut iterations = 0u32;
                while iterations < cap && predicate.evaluate(&acc) {
                    acc = update.apply(&acc);
                    iterations += 1;
                }
                iterations
            }
        }
    }

    /// Expand the loop into a single flat [`Chain`].
    ///
    /// The resulting chain concatenates the body's tasks once per iteration. An
    /// empty body, or a loop that resolves to zero iterations, is rejected as an
    /// invalid (empty) workflow.
    pub fn expand(&self) -> Result<Chain, CanvasError> {
        if self.body.chain.tasks.is_empty() {
            return Err(CanvasError::Invalid(
                "WorkflowLoop body cannot be empty".to_string(),
            ));
        }

        let iterations = self.iteration_count();
        if iterations == 0 {
            return Err(CanvasError::Invalid(
                "WorkflowLoop expanded to zero iterations".to_string(),
            ));
        }

        let mut tasks = Vec::with_capacity(self.body.chain.tasks.len() * iterations as usize);
        for index in 0..iterations {
            tasks.extend(self.body.tasks_for_iteration(index));
        }

        Ok(Chain { tasks })
    }
}

impl std::fmt::Display for WorkflowLoop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.bound {
            LoopBound::Times { count } => write!(
                f,
                "WorkflowLoop[times={}, body={} tasks]",
                count,
                self.body.chain.len()
            ),
            LoopBound::While {
                predicate,
                max_iterations,
                ..
            } => write!(
                f,
                "WorkflowLoop[while {} (max={}), body={} tasks]",
                predicate,
                max_iterations,
                self.body.chain.len()
            ),
        }
    }
}

/// Map-style iteration: apply a body chain over a list of input argument sets,
/// producing one parallel branch per input.
///
/// Each input is a positional-argument vector that becomes the arguments of the
/// **first** task of the body for that branch (mirroring how a chain threads its
/// initial arguments). Expansion yields either a [`Vec<Chain>`] (one chain per
/// input) or a [`NestedGroup`]/[`NestedChain`] for direct composition.
///
/// # Example
///
/// ```
/// use celers_canvas::{WorkflowMap, Chain};
/// use serde_json::json;
///
/// let body = Chain::new().then("download", vec![]).then("convert", vec![]);
/// let mapped = WorkflowMap::new(body, vec![vec![json!("a")], vec![json!("b")]]);
/// let branches = mapped.expand_chains().expect("non-empty map expands");
/// assert_eq!(branches.len(), 2);
/// assert_eq!(branches[0].first().unwrap().args, vec![json!("a")]);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowMap {
    /// The chain applied to every input.
    pub body: Chain,

    /// One positional-argument vector per branch.
    pub inputs: Vec<Vec<serde_json::Value>>,

    /// When `true`, the body task names of branch `i` are suffixed with
    /// `_item<i>` during expansion for individual addressability.
    pub suffix_items: bool,
}

impl WorkflowMap {
    /// Create a map-style iteration over `inputs`.
    pub fn new(body: Chain, inputs: Vec<Vec<serde_json::Value>>) -> Self {
        Self {
            body,
            inputs,
            suffix_items: false,
        }
    }

    /// Enable per-item task-name suffixing (`task_item0`, `task_item1`, …).
    pub fn with_item_suffix(mut self) -> Self {
        self.suffix_items = true;
        self
    }

    /// Number of branches (equals the number of inputs).
    pub fn len(&self) -> usize {
        self.inputs.len()
    }

    /// Whether there are no inputs.
    pub fn is_empty(&self) -> bool {
        self.inputs.is_empty()
    }

    /// Build the per-branch chain for input `index`.
    fn branch(&self, index: usize, input: &[serde_json::Value]) -> Chain {
        let mut tasks = self.body.tasks.clone();
        if let Some(first) = tasks.first_mut() {
            if !first.immutable {
                first.args = input.to_vec();
            }
        }
        if self.suffix_items {
            for sig in &mut tasks {
                sig.task = format!("{}_item{}", sig.task, index);
            }
        }
        Chain { tasks }
    }

    /// Expand into one [`Chain`] per input.
    pub fn expand_chains(&self) -> Result<Vec<Chain>, CanvasError> {
        if self.body.tasks.is_empty() {
            return Err(CanvasError::Invalid(
                "WorkflowMap body cannot be empty".to_string(),
            ));
        }
        if self.inputs.is_empty() {
            return Err(CanvasError::Invalid(
                "WorkflowMap requires at least one input".to_string(),
            ));
        }

        Ok(self
            .inputs
            .iter()
            .enumerate()
            .map(|(index, input)| self.branch(index, input))
            .collect())
    }

    /// Expand into a [`NestedGroup`] whose branches run in parallel.
    pub fn expand_group(&self) -> Result<NestedGroup, CanvasError> {
        let branches = self.expand_chains()?;
        let mut group = NestedGroup::new();
        for branch in branches {
            group = group.add_chain(branch);
        }
        Ok(group)
    }

    /// Expand into a [`NestedChain`] whose branches run sequentially. This is
    /// useful when the inputs must be processed one after another rather than in
    /// parallel.
    pub fn expand_sequential(&self) -> Result<NestedChain, CanvasError> {
        let branches = self.expand_chains()?;
        let mut chain = NestedChain::new();
        for branch in branches {
            chain = chain.then_chain(branch);
        }
        Ok(chain)
    }
}

impl std::fmt::Display for WorkflowMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WorkflowMap[body={} tasks, {} inputs]",
            self.body.len(),
            self.inputs.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn times_loop_expands_to_exact_multiple() {
        let body = Chain::new().then("a", vec![]).then("b", vec![]);
        let looped = WorkflowLoop::times(body, 4);
        assert_eq!(looped.iteration_count(), 4);
        let chain = looped.expand().expect("expand");
        assert_eq!(chain.len(), 8);
        assert_eq!(
            chain.task_names(),
            vec!["a", "b", "a", "b", "a", "b", "a", "b"]
        );
    }

    #[test]
    fn times_loop_with_suffix_makes_unique_names() {
        let body = Chain::new().then("step", vec![]);
        let looped = WorkflowLoop::times(body, 3).with_iteration_suffix();
        let chain = looped.expand().expect("expand");
        assert_eq!(
            chain.task_names(),
            vec!["step_iter0", "step_iter1", "step_iter2"]
        );
    }

    #[test]
    fn empty_body_is_rejected() {
        let looped = WorkflowLoop::times(Chain::new(), 5);
        let err = looped.expand().expect_err("empty body must error");
        assert!(err.is_invalid());
    }

    #[test]
    fn zero_times_is_rejected() {
        let body = Chain::new().then("a", vec![]);
        let looped = WorkflowLoop::times(body, 0);
        let err = looped.expand().expect_err("zero iterations must error");
        assert!(err.is_invalid());
    }

    #[test]
    fn while_loop_counts_up_to_threshold() {
        // acc starts at 0, predicate "acc < 5", +1 each iteration -> 5 iterations.
        let body = Chain::new().then("work", vec![]);
        let looped = WorkflowLoop::while_loop(
            body,
            Condition::less_than(5.0),
            json!(0),
            AccumulatorUpdate::AddNumber(1.0),
            1000,
        );
        assert_eq!(looped.iteration_count(), 5);
        let chain = looped.expand().expect("expand");
        assert_eq!(chain.len(), 5);
    }

    #[test]
    fn while_loop_respects_hard_cap() {
        // Predicate is always true; the cap must bound the expansion.
        let body = Chain::new().then("work", vec![]).then("more", vec![]);
        let looped = WorkflowLoop::while_loop(
            body,
            Condition::Always,
            json!(0),
            AccumulatorUpdate::Identity,
            7,
        );
        assert_eq!(looped.iteration_count(), 7);
        let chain = looped.expand().expect("expand");
        // 2 tasks per iteration * 7 = 14.
        assert_eq!(chain.len(), 14);
    }

    #[test]
    fn while_loop_false_predicate_yields_zero_and_errors() {
        let body = Chain::new().then("work", vec![]);
        let looped = WorkflowLoop::while_loop(
            body,
            Condition::Never,
            json!(0),
            AccumulatorUpdate::AddNumber(1.0),
            10,
        );
        assert_eq!(looped.iteration_count(), 0);
        assert!(looped.expand().is_err());
    }

    #[test]
    fn while_loop_multiplicative_accumulator() {
        // acc: 1 -> 2 -> 4 -> 8 -> 16; predicate "acc < 16" stops at 16 => 4 iters.
        let body = Chain::new().then("double", vec![]);
        let looped = WorkflowLoop::while_loop(
            body,
            Condition::less_than(16.0),
            json!(1),
            AccumulatorUpdate::MulNumber(2.0),
            64,
        );
        assert_eq!(looped.iteration_count(), 4);
    }

    #[test]
    fn absolute_cap_bounds_even_huge_times() {
        let body = Chain::new().then("x", vec![]);
        let looped = WorkflowLoop::times(body, u32::MAX);
        assert_eq!(looped.iteration_count(), ABSOLUTE_MAX_ITERATIONS);
    }

    #[test]
    fn accumulator_update_preserves_integers() {
        let v = AccumulatorUpdate::AddNumber(1.0).apply(&json!(2));
        assert_eq!(v, json!(3));
        assert!(v.is_i64());
    }

    #[test]
    fn workflow_map_expands_per_input() {
        let body = Chain::new()
            .then("download", vec![])
            .then("convert", vec![]);
        let mapped = WorkflowMap::new(
            body,
            vec![vec![json!("a")], vec![json!("b")], vec![json!("c")]],
        );
        assert_eq!(mapped.len(), 3);
        let branches = mapped.expand_chains().expect("expand");
        assert_eq!(branches.len(), 3);
        assert_eq!(branches[0].first().expect("first").args, vec![json!("a")]);
        assert_eq!(branches[2].first().expect("first").args, vec![json!("c")]);
        // Body shape preserved in each branch.
        assert_eq!(branches[1].len(), 2);
    }

    #[test]
    fn workflow_map_suffix_disambiguates_branches() {
        let body = Chain::new().then("proc", vec![]);
        let mapped =
            WorkflowMap::new(body, vec![vec![json!(1)], vec![json!(2)]]).with_item_suffix();
        let branches = mapped.expand_chains().expect("expand");
        assert_eq!(branches[0].first().expect("first").task, "proc_item0");
        assert_eq!(branches[1].first().expect("first").task, "proc_item1");
    }

    #[test]
    fn workflow_map_empty_inputs_error() {
        let body = Chain::new().then("x", vec![]);
        let mapped = WorkflowMap::new(body, vec![]);
        assert!(mapped.is_empty());
        assert!(mapped.expand_chains().is_err());
    }

    #[test]
    fn workflow_map_empty_body_error() {
        let mapped = WorkflowMap::new(Chain::new(), vec![vec![json!(1)]]);
        assert!(mapped.expand_chains().is_err());
    }

    #[test]
    fn workflow_map_expands_to_group_and_sequential() {
        let body = Chain::new().then("a", vec![]).then("b", vec![]);
        let mapped = WorkflowMap::new(body, vec![vec![json!(1)], vec![json!(2)]]);
        let group = mapped.expand_group().expect("group");
        assert_eq!(group.len(), 2);
        let seq = mapped.expand_sequential().expect("sequential");
        assert_eq!(seq.len(), 2);
    }

    #[test]
    fn workflow_map_immutable_first_task_keeps_args() {
        let mut first = Signature::new("locked".to_string()).with_args(vec![json!("keep")]);
        first.immutable = true;
        let body = Chain::new().then_signature(first);
        let mapped = WorkflowMap::new(body, vec![vec![json!("override")]]);
        let branches = mapped.expand_chains().expect("expand");
        // Immutable first task retains its original args.
        assert_eq!(
            branches[0].first().expect("first").args,
            vec![json!("keep")]
        );
    }
}
