//! Sub-workflows and nested composition.
//!
//! This module lets a whole workflow ([`Chain`], [`Group`], or [`Chord`]) be
//! embedded as a *single composable element* inside another workflow via
//! [`SubWorkflow`], and composed together with [`WorkflowComposition`]. Two
//! explicit lowering steps are provided:
//!
//! * [`WorkflowComposition::expand`] — lower the composition into a
//!   [`NestedChain`] preserving the structure of every embedded sub-workflow
//!   (groups stay parallel, chords keep their callback).
//! * [`WorkflowComposition::flatten`] — collapse the composition into a single
//!   flat [`Chain`] when, and only when, every embedded sub-workflow is
//!   linearisable (i.e. a chain or a single-task group). Non-linearisable
//!   members (multi-task groups, chords) cause a descriptive error.
//!
//! # Example
//!
//! ```
//! use celers_canvas::{WorkflowComposition, SubWorkflow, Chain, Group};
//!
//! let phase_one = Chain::new().then("extract", vec![]).then("clean", vec![]);
//! let phase_two = Chain::new().then("load", vec![]);
//!
//! let composed = WorkflowComposition::new()
//!     .then(SubWorkflow::chain(phase_one))
//!     .then(SubWorkflow::chain(phase_two));
//!
//! // Linearisable -> flatten into one chain of 3 tasks.
//! let flat = composed.flatten().expect("all members linearisable");
//! assert_eq!(flat.len(), 3);
//! ```

use crate::{CanvasError, Chain, Chord, Group, NestedChain, Signature};
use serde::{Deserialize, Serialize};

/// A complete workflow embedded as one composable element of another workflow.
///
/// This is intentionally narrower than [`crate::CanvasElement`]: it carries only
/// the three *composite* workflow shapes (chain / group / chord) so that
/// composition and flattening have well-defined semantics.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "sub_workflow")]
pub enum SubWorkflow {
    /// An embedded sequential chain.
    Chain(Chain),
    /// An embedded parallel group.
    Group(Group),
    /// An embedded chord (parallel header + callback body).
    ///
    /// Boxed because a [`Chord`] (group header + callback signature) is
    /// substantially larger than the other variants.
    Chord(Box<Chord>),
}

impl SubWorkflow {
    /// Embed a chain.
    pub fn chain(chain: Chain) -> Self {
        Self::Chain(chain)
    }

    /// Embed a group.
    pub fn group(group: Group) -> Self {
        Self::Group(group)
    }

    /// Embed a chord from its header group and callback body.
    pub fn chord(header: Group, body: Signature) -> Self {
        Self::Chord(Box::new(Chord::new(header, body)))
    }

    /// The number of leaf tasks contained in this sub-workflow (a chord counts
    /// its header tasks plus the single callback body).
    pub fn task_count(&self) -> usize {
        match self {
            Self::Chain(chain) => chain.tasks.len(),
            Self::Group(group) => group.tasks.len(),
            Self::Chord(chord) => chord.header.tasks.len() + 1,
        }
    }

    /// Whether this sub-workflow contains no leaf tasks.
    pub fn is_empty(&self) -> bool {
        match self {
            Self::Chain(chain) => chain.tasks.is_empty(),
            Self::Group(group) => group.tasks.is_empty(),
            // A chord always has a callback body, so it is empty only when the
            // header is empty (which is itself invalid, surfaced on flatten/expand).
            Self::Chord(chord) => chord.header.tasks.is_empty(),
        }
    }

    /// A short human-readable kind name (`"chain"`, `"group"`, `"chord"`).
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Chain(_) => "chain",
            Self::Group(_) => "group",
            Self::Chord(_) => "chord",
        }
    }

    /// Whether this sub-workflow can be flattened into a linear sequence of
    /// tasks. Chains are always linearisable; a group is linearisable only when
    /// it holds a single task (parallelism of two or more cannot be expressed in
    /// a flat chain); chords are never linearisable (the callback fan-in has no
    /// flat representation).
    pub fn is_linearizable(&self) -> bool {
        match self {
            Self::Chain(_) => true,
            Self::Group(group) => group.tasks.len() <= 1,
            Self::Chord(_) => false,
        }
    }

    /// Attempt to flatten this single sub-workflow into a [`Chain`].
    ///
    /// Returns [`CanvasError::Invalid`] if the sub-workflow is not linearisable
    /// or is empty.
    pub fn flatten(&self) -> Result<Chain, CanvasError> {
        if self.is_empty() {
            return Err(CanvasError::Invalid(format!(
                "cannot flatten empty {} sub-workflow",
                self.kind()
            )));
        }
        match self {
            Self::Chain(chain) => Ok(chain.clone()),
            Self::Group(group) if group.tasks.len() == 1 => Ok(Chain {
                tasks: group.tasks.clone(),
            }),
            Self::Group(group) => Err(CanvasError::Invalid(format!(
                "cannot flatten parallel group of {} tasks into a chain",
                group.tasks.len()
            ))),
            Self::Chord(_) => Err(CanvasError::Invalid(
                "cannot flatten a chord into a chain (callback fan-in is not linear)".to_string(),
            )),
        }
    }

    /// Lower this sub-workflow into a [`crate::CanvasElement`] for nested
    /// execution.
    pub fn to_element(&self) -> crate::CanvasElement {
        match self {
            Self::Chain(chain) => crate::CanvasElement::Chain(chain.clone()),
            Self::Group(group) => crate::CanvasElement::Group(group.clone()),
            Self::Chord(chord) => crate::CanvasElement::Chord {
                header: chord.header.clone(),
                body: chord.body.clone(),
            },
        }
    }
}

impl From<Chain> for SubWorkflow {
    fn from(chain: Chain) -> Self {
        Self::Chain(chain)
    }
}

impl From<Group> for SubWorkflow {
    fn from(group: Group) -> Self {
        Self::Group(group)
    }
}

impl From<Chord> for SubWorkflow {
    fn from(chord: Chord) -> Self {
        Self::Chord(Box::new(chord))
    }
}

impl std::fmt::Display for SubWorkflow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Chain(chain) => write!(f, "Sub({})", chain),
            Self::Group(group) => write!(f, "Sub({})", group),
            Self::Chord(chord) => write!(f, "Sub({})", chord),
        }
    }
}

/// An ordered composition of embedded sub-workflows, executed sequentially.
///
/// A composition is the natural "outer" workflow: each member is a whole
/// sub-workflow that runs (and, for groups/chords, fans out) before the next
/// member begins. Use [`expand`](Self::expand) to lower into a
/// [`NestedChain`], or [`flatten`](Self::flatten) to collapse into a single
/// [`Chain`] when every member is linearisable.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct WorkflowComposition {
    /// The embedded sub-workflows, in execution order.
    pub members: Vec<SubWorkflow>,
}

impl WorkflowComposition {
    /// Create an empty composition.
    pub fn new() -> Self {
        Self {
            members: Vec::new(),
        }
    }

    /// Append a sub-workflow to the composition.
    pub fn then(mut self, sub: impl Into<SubWorkflow>) -> Self {
        self.members.push(sub.into());
        self
    }

    /// Append a chain sub-workflow.
    pub fn then_chain(self, chain: Chain) -> Self {
        self.then(SubWorkflow::Chain(chain))
    }

    /// Append a group sub-workflow.
    pub fn then_group(self, group: Group) -> Self {
        self.then(SubWorkflow::Group(group))
    }

    /// Append a chord sub-workflow.
    pub fn then_chord(self, header: Group, body: Signature) -> Self {
        self.then(SubWorkflow::chord(header, body))
    }

    /// Number of embedded sub-workflows.
    pub fn len(&self) -> usize {
        self.members.len()
    }

    /// Whether the composition has no members.
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// Total number of leaf tasks across all members.
    pub fn total_task_count(&self) -> usize {
        self.members.iter().map(SubWorkflow::task_count).sum()
    }

    /// Whether every member can be flattened into a linear sequence.
    pub fn is_linearizable(&self) -> bool {
        !self.is_empty() && self.members.iter().all(SubWorkflow::is_linearizable)
    }

    /// Expand the composition into a [`NestedChain`], preserving the structure
    /// of every embedded sub-workflow.
    ///
    /// Returns [`CanvasError::Invalid`] for an empty composition or an empty
    /// member.
    pub fn expand(&self) -> Result<NestedChain, CanvasError> {
        if self.members.is_empty() {
            return Err(CanvasError::Invalid(
                "WorkflowComposition cannot be empty".to_string(),
            ));
        }

        let mut chain = NestedChain::new();
        for (index, member) in self.members.iter().enumerate() {
            if member.is_empty() {
                return Err(CanvasError::Invalid(format!(
                    "sub-workflow at index {} ({}) is empty",
                    index,
                    member.kind()
                )));
            }
            chain = chain.then_element(member.to_element());
        }
        Ok(chain)
    }

    /// Flatten the composition into a single flat [`Chain`].
    ///
    /// Every member must be linearisable; otherwise a descriptive
    /// [`CanvasError::Invalid`] is returned naming the offending member.
    pub fn flatten(&self) -> Result<Chain, CanvasError> {
        if self.members.is_empty() {
            return Err(CanvasError::Invalid(
                "WorkflowComposition cannot be empty".to_string(),
            ));
        }

        let mut tasks = Vec::with_capacity(self.total_task_count());
        for (index, member) in self.members.iter().enumerate() {
            let flat = member.flatten().map_err(|e| {
                CanvasError::Invalid(format!(
                    "sub-workflow at index {} ({}) is not flattenable: {}",
                    index,
                    member.kind(),
                    e
                ))
            })?;
            tasks.extend(flat.tasks);
        }
        Ok(Chain { tasks })
    }
}

impl std::fmt::Display for WorkflowComposition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let parts: Vec<String> = self.members.iter().map(|m| format!("{}", m)).collect();
        write!(f, "WorkflowComposition[{}]", parts.join(" => "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn flatten_all_chains() {
        let composed = WorkflowComposition::new()
            .then_chain(Chain::new().then("a", vec![]).then("b", vec![]))
            .then_chain(Chain::new().then("c", vec![]));
        assert!(composed.is_linearizable());
        let flat = composed.flatten().expect("flatten");
        assert_eq!(flat.task_names(), vec!["a", "b", "c"]);
    }

    #[test]
    fn flatten_single_task_group_ok() {
        let composed = WorkflowComposition::new()
            .then_chain(Chain::new().then("start", vec![]))
            .then_group(Group::new().add("only", vec![]));
        let flat = composed
            .flatten()
            .expect("single-task group is linearisable");
        assert_eq!(flat.task_names(), vec!["start", "only"]);
    }

    #[test]
    fn flatten_multi_task_group_errors() {
        let composed = WorkflowComposition::new()
            .then_chain(Chain::new().then("start", vec![]))
            .then_group(Group::new().add("p1", vec![]).add("p2", vec![]));
        assert!(!composed.is_linearizable());
        let err = composed
            .flatten()
            .expect_err("parallel group cannot flatten");
        assert!(err.is_invalid());
        assert!(format!("{}", err).contains("index 1"));
    }

    #[test]
    fn flatten_chord_errors() {
        let composed = WorkflowComposition::new()
            .then_chain(Chain::new().then("start", vec![]))
            .then_chord(
                Group::new().add("h1", vec![]).add("h2", vec![]),
                Signature::new("cb".to_string()),
            );
        let err = composed.flatten().expect_err("chord cannot flatten");
        assert!(err.is_invalid());
    }

    #[test]
    fn expand_preserves_structure() {
        let composed = WorkflowComposition::new()
            .then_chain(Chain::new().then("a", vec![]))
            .then_group(Group::new().add("p1", vec![]).add("p2", vec![]))
            .then_chord(
                Group::new().add("h1", vec![]),
                Signature::new("cb".to_string()),
            );
        let nested = composed.expand().expect("expand");
        assert_eq!(nested.len(), 3);
        assert!(nested.elements[0].is_chain());
        assert!(nested.elements[1].is_group());
        assert!(nested.elements[2].is_chord());
    }

    #[test]
    fn empty_composition_errors() {
        let composed = WorkflowComposition::new();
        assert!(composed.is_empty());
        assert!(composed.flatten().is_err());
        assert!(composed.expand().is_err());
    }

    #[test]
    fn empty_member_errors_on_expand() {
        let composed = WorkflowComposition::new().then_chain(Chain::new());
        let err = composed.expand().expect_err("empty member");
        assert!(err.is_invalid());
    }

    #[test]
    fn total_task_count_includes_chord_callback() {
        let composed = WorkflowComposition::new()
            .then_chain(Chain::new().then("a", vec![]).then("b", vec![]))
            .then_chord(
                Group::new().add("h1", vec![]).add("h2", vec![]),
                Signature::new("cb".to_string()),
            );
        // chain(2) + chord header(2) + callback(1) = 5
        assert_eq!(composed.total_task_count(), 5);
    }

    #[test]
    fn sub_workflow_from_conversions() {
        let from_chain: SubWorkflow = Chain::new().then("x", vec![]).into();
        assert_eq!(from_chain.kind(), "chain");
        let from_group: SubWorkflow = Group::new().add("x", vec![]).into();
        assert_eq!(from_group.kind(), "group");
        let from_chord: SubWorkflow = Chord::new(
            Group::new().add("h", vec![]),
            Signature::new("cb".to_string()),
        )
        .into();
        assert_eq!(from_chord.kind(), "chord");
        assert!(!from_chord.is_linearizable());
    }

    #[test]
    fn single_sub_workflow_flatten() {
        let sub = SubWorkflow::chain(Chain::new().then("a", vec![]).then("b", vec![]));
        let flat = sub.flatten().expect("flatten chain sub-workflow");
        assert_eq!(flat.len(), 2);
    }
}
