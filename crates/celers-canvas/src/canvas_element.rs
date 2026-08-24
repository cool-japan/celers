use crate::dispatch::{self, ChainStep};
use crate::{Branch, CanvasError, Chain, Group, Map, Signature, Switch};
use celers_core::Broker;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(feature = "backend-redis")]
use celers_backend_redis::ResultBackend;

/// A canvas element that can be either a simple signature or a nested workflow
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "element_type")]
pub enum CanvasElement {
    /// A simple task signature
    Signature(Signature),

    /// A chain of tasks
    Chain(Chain),

    /// A group of parallel tasks
    Group(Group),

    /// A chord (group + callback)
    Chord {
        /// Header group
        header: Group,
        /// Callback signature
        body: Signature,
    },

    /// A map operation
    Map {
        /// Task to apply
        task: Signature,
        /// Argument sets
        argsets: Vec<Vec<serde_json::Value>>,
    },

    /// A conditional branch.
    ///
    /// The condition is evaluated by the worker against the **result of the
    /// preceding step**, and only the selected arm is enqueued. It is therefore
    /// only meaningful inside a [`NestedChain`], in a non-leading position: a
    /// leading branch, or a branch used as a [`NestedGroup`] member, has no
    /// predecessor result to evaluate and is rejected by `validate()`.
    Branch(Branch),

    /// A switch statement.
    ///
    /// Same runtime semantics as [`CanvasElement::Branch`]: the first matching
    /// case (or the default) is selected by the worker from the preceding
    /// step's result.
    Switch(Switch),
}

impl CanvasElement {
    /// Create a signature element
    pub fn signature(sig: Signature) -> Self {
        Self::Signature(sig)
    }

    /// Create a task element (shorthand for signature)
    pub fn task(name: impl Into<String>, args: Vec<serde_json::Value>) -> Self {
        Self::Signature(Signature::new(name.into()).with_args(args))
    }

    /// Create a chain element
    pub fn chain(chain: Chain) -> Self {
        Self::Chain(chain)
    }

    /// Create a group element
    pub fn group(group: Group) -> Self {
        Self::Group(group)
    }

    /// Create a chord element
    pub fn chord(header: Group, body: Signature) -> Self {
        Self::Chord { header, body }
    }

    /// Create a map element
    pub fn map(task: Signature, argsets: Vec<Vec<serde_json::Value>>) -> Self {
        Self::Map { task, argsets }
    }

    /// Create a branch element
    pub fn branch(branch: Branch) -> Self {
        Self::Branch(branch)
    }

    /// Create a switch element
    pub fn switch(switch: Switch) -> Self {
        Self::Switch(switch)
    }

    /// Check if this is a simple signature
    pub fn is_signature(&self) -> bool {
        matches!(self, Self::Signature(_))
    }

    /// Check if this is a chain
    pub fn is_chain(&self) -> bool {
        matches!(self, Self::Chain(_))
    }

    /// Check if this is a group
    pub fn is_group(&self) -> bool {
        matches!(self, Self::Group(_))
    }

    /// Check if this is a chord
    pub fn is_chord(&self) -> bool {
        matches!(self, Self::Chord { .. })
    }

    /// Get the element type as a string
    pub fn element_type(&self) -> &'static str {
        match self {
            Self::Signature(_) => "signature",
            Self::Chain(_) => "chain",
            Self::Group(_) => "group",
            Self::Chord { .. } => "chord",
            Self::Map { .. } => "map",
            Self::Branch(_) => "branch",
            Self::Switch(_) => "switch",
        }
    }
}

impl std::fmt::Display for CanvasElement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Signature(sig) => write!(f, "Signature[{}]", sig.task),
            Self::Chain(chain) => write!(f, "{}", chain),
            Self::Group(group) => write!(f, "{}", group),
            Self::Chord { header, body } => {
                write!(f, "Chord[header={}, body={}]", header, body.task)
            }
            Self::Map { task, argsets } => {
                write!(f, "Map[task={}, {} argsets]", task.task, argsets.len())
            }
            Self::Branch(branch) => write!(f, "{}", branch),
            Self::Switch(switch) => write!(f, "{}", switch),
        }
    }
}

impl From<Signature> for CanvasElement {
    fn from(sig: Signature) -> Self {
        Self::Signature(sig)
    }
}

impl From<Chain> for CanvasElement {
    fn from(chain: Chain) -> Self {
        Self::Chain(chain)
    }
}

impl From<Group> for CanvasElement {
    fn from(group: Group) -> Self {
        Self::Group(group)
    }
}

impl From<Branch> for CanvasElement {
    fn from(branch: Branch) -> Self {
        Self::Branch(branch)
    }
}

impl From<Switch> for CanvasElement {
    fn from(switch: Switch) -> Self {
        Self::Switch(switch)
    }
}

/// Message used when a nested workflow contains a chord but `apply` was called
/// without a result backend.
const NESTED_CHORD_REQUIRES_BACKEND: &str =
    "A nested Chord needs a result backend to establish its barrier: the callback must run once, \
     after every header task has completed, with their results. Call `apply_with_backend` (the \
     `backend-redis` feature) instead of `apply`.";

/// Message used when a nested chain contains a fan-out step that cannot be
/// sequenced with the link mechanism.
const NESTED_FANOUT_NOT_SEQUENCEABLE: &str =
    "A Group/Map/Chord step inside a NestedChain can only be sequenced by a completion barrier, \
     which the chain-link mechanism cannot express: the following step would start immediately, \
     in parallel with the fan-out, instead of after it. Restructure the workflow (dispatch the \
     fan-out as a Chord whose callback is the following step), or use a NestedGroup if the steps \
     really are concurrent.";

/// Message used when a conditional step appears where there is no predecessor
/// result to evaluate it against.
const CONDITIONAL_NEEDS_PREDECESSOR: &str =
    "A Branch/Switch step is evaluated against the result of the step before it, so it cannot be \
     the first step of a NestedChain or a branch of a NestedGroup, where no predecessor result \
     exists.";

/// Flatten a run of canvas elements into linear chain steps.
///
/// Returns `Err` for any element that cannot be expressed as a linear step.
fn flatten_element(element: &CanvasElement, steps: &mut Vec<ChainStep>) -> Result<(), CanvasError> {
    match element {
        CanvasElement::Signature(sig) => {
            steps.push(ChainStep::Task(sig.clone()));
            Ok(())
        }
        CanvasElement::Chain(chain) => {
            steps.extend(chain.tasks.iter().cloned().map(ChainStep::Task));
            Ok(())
        }
        CanvasElement::Branch(branch) => {
            if steps.is_empty() {
                return Err(CanvasError::Invalid(
                    CONDITIONAL_NEEDS_PREDECESSOR.to_string(),
                ));
            }
            steps.push(ChainStep::Branch(branch.clone()));
            Ok(())
        }
        CanvasElement::Switch(switch) => {
            if steps.is_empty() {
                return Err(CanvasError::Invalid(
                    CONDITIONAL_NEEDS_PREDECESSOR.to_string(),
                ));
            }
            steps.push(ChainStep::Switch(switch.clone()));
            Ok(())
        }
        CanvasElement::Group(_) | CanvasElement::Map { .. } => Err(CanvasError::Invalid(
            NESTED_FANOUT_NOT_SEQUENCEABLE.to_string(),
        )),
        CanvasElement::Chord { .. } => Err(CanvasError::Invalid(
            NESTED_FANOUT_NOT_SEQUENCEABLE.to_string(),
        )),
    }
}

/// Dispatch a single fan-out element (a lone [`CanvasElement::Group`] or
/// [`CanvasElement::Map`]) under `group_id`.
async fn dispatch_fanout_element<B: Broker>(
    broker: &B,
    element: &CanvasElement,
    group_id: Uuid,
) -> Result<(), CanvasError> {
    match element {
        CanvasElement::Group(group) => {
            group.apply_within(broker, group_id).await?;
            Ok(())
        }
        CanvasElement::Map { task, argsets } => {
            let group = Map::new(task.clone(), argsets.clone()).to_group();
            group.apply_within(broker, group_id).await?;
            Ok(())
        }
        other => Err(CanvasError::Invalid(format!(
            "{} is not a fan-out element",
            other.element_type()
        ))),
    }
}

/// Dispatch a run of linear chain steps: the head is enqueued now and the rest
/// travel with it as the chain tail.
async fn dispatch_steps<B: Broker>(broker: &B, steps: Vec<ChainStep>) -> Result<Uuid, CanvasError> {
    let mut steps = steps;
    if steps.is_empty() {
        return Err(CanvasError::Invalid("No steps to dispatch".to_string()));
    }

    let tail = steps.split_off(1);
    match steps.pop() {
        Some(ChainStep::Task(head)) => dispatch::dispatch_signature(broker, &head, &tail).await,
        // `flatten_element` refuses a leading conditional, so this is
        // unreachable through the public API; keep it as a typed guard rather
        // than a panic.
        Some(_) | None => Err(CanvasError::Invalid(
            CONDITIONAL_NEEDS_PREDECESSOR.to_string(),
        )),
    }
}

/// A nested chain that can contain any canvas element
///
/// Unlike the basic Chain that only contains Signatures, NestedChain
/// can contain Groups, Chords, or other Chains as steps.
///
/// # Example
/// ```
/// use celers_canvas::{NestedChain, CanvasElement, Group, Signature};
///
/// let workflow = NestedChain::new()
///     .then_element(CanvasElement::task("step1".to_string(), vec![]))
///     .then_element(CanvasElement::group(
///         Group::new()
///             .add("parallel_a", vec![])
///             .add("parallel_b", vec![])
///     ))
///     .then_element(CanvasElement::task("step2".to_string(), vec![]));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NestedChain {
    /// Elements in the chain
    pub elements: Vec<CanvasElement>,
}

impl NestedChain {
    /// Create a new empty nested chain
    pub fn new() -> Self {
        Self {
            elements: Vec::new(),
        }
    }

    /// Add an element to the chain
    pub fn then_element(mut self, element: CanvasElement) -> Self {
        self.elements.push(element);
        self
    }

    /// Add a signature to the chain
    pub fn then_signature(mut self, sig: Signature) -> Self {
        self.elements.push(CanvasElement::Signature(sig));
        self
    }

    /// Add a simple task to the chain
    pub fn then(mut self, task: &str, args: Vec<serde_json::Value>) -> Self {
        self.elements.push(CanvasElement::task(task, args));
        self
    }

    /// Add a group to the chain (parallel execution point)
    pub fn then_group(mut self, group: Group) -> Self {
        self.elements.push(CanvasElement::Group(group));
        self
    }

    /// Add a chord to the chain
    pub fn then_chord(mut self, header: Group, body: Signature) -> Self {
        self.elements.push(CanvasElement::Chord { header, body });
        self
    }

    /// Add a branch to the chain
    pub fn then_branch(mut self, branch: Branch) -> Self {
        self.elements.push(CanvasElement::Branch(branch));
        self
    }

    /// Add another chain as a nested element
    pub fn then_chain(mut self, chain: Chain) -> Self {
        self.elements.push(CanvasElement::Chain(chain));
        self
    }

    /// Check if the chain is empty
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// Get the number of elements
    pub fn len(&self) -> usize {
        self.elements.len()
    }

    /// Flatten the nested chain into a sequence of signatures where possible
    ///
    /// This is useful for simpler execution when nested workflows aren't needed.
    /// Note: This will return None if the chain contains elements that can't be
    /// flattened to signatures (groups, chords, etc.)
    pub fn flatten_signatures(&self) -> Option<Vec<Signature>> {
        let mut result = Vec::new();

        for element in &self.elements {
            match element {
                CanvasElement::Signature(sig) => result.push(sig.clone()),
                CanvasElement::Chain(chain) => {
                    result.extend(chain.tasks.clone());
                }
                _ => return None, // Can't flatten non-signature elements
            }
        }

        Some(result)
    }

    /// Check that this nested chain can actually be dispatched.
    ///
    /// Validation happens at *build* time so an unsupported composition is
    /// caught while the workflow is being assembled rather than surfacing as a
    /// surprise error at `apply()`. [`apply`](Self::apply) runs the same checks,
    /// so calling this is optional — but a builder that validates as it goes
    /// gets much better error locality.
    ///
    /// A nested chain is valid when either
    ///
    /// * every element is linear ([`CanvasElement::Signature`],
    ///   [`CanvasElement::Chain`], and [`CanvasElement::Branch`] /
    ///   [`CanvasElement::Switch`] in any non-leading position), or
    /// * it consists of a single fan-out element
    ///   ([`CanvasElement::Group`] / [`CanvasElement::Map`]), which is just a
    ///   parallel dispatch with nothing to sequence it against.
    ///
    /// A fan-out element in any other position is rejected: sequencing it needs
    /// a completion barrier that the chain-link mechanism cannot express, and
    /// dispatching it anyway would run the following step concurrently with it.
    pub fn validate(&self) -> Result<(), CanvasError> {
        if self.elements.is_empty() {
            return Err(CanvasError::Invalid(
                "NestedChain cannot be empty".to_string(),
            ));
        }

        if self.elements.len() == 1 {
            return match &self.elements[0] {
                CanvasElement::Group(group) if group.tasks.is_empty() => {
                    Err(CanvasError::Invalid("Group cannot be empty".to_string()))
                }
                CanvasElement::Map { argsets, .. } if argsets.is_empty() => {
                    Err(CanvasError::Invalid("Map cannot be empty".to_string()))
                }
                CanvasElement::Group(_) | CanvasElement::Map { .. } => Ok(()),
                CanvasElement::Chord { header, .. } if header.tasks.is_empty() => Err(
                    CanvasError::Invalid("Chord header cannot be empty".to_string()),
                ),
                CanvasElement::Chord { .. } => Err(CanvasError::Invalid(
                    NESTED_CHORD_REQUIRES_BACKEND.to_string(),
                )),
                other => {
                    let mut steps = Vec::new();
                    flatten_element(other, &mut steps)
                }
            };
        }

        let mut steps = Vec::with_capacity(self.elements.len());
        for element in &self.elements {
            if let CanvasElement::Chord { header, .. } = element {
                if header.tasks.is_empty() {
                    return Err(CanvasError::Invalid(
                        "Chord header cannot be empty".to_string(),
                    ));
                }
                // A chord in a multi-step chain is unsequenceable regardless of
                // whether a backend is available: its callback is triggered by
                // the barrier, and nothing can be linked after it.
                return Err(CanvasError::Invalid(
                    NESTED_FANOUT_NOT_SEQUENCEABLE.to_string(),
                ));
            }
            flatten_element(element, &mut steps)?;
        }

        Ok(())
    }

    /// Execute the nested chain, sequencing its elements for real.
    ///
    /// Every element is flattened into one linear chain and dispatched as such:
    /// the head task is enqueued now and steps 2..N ride along inside its
    /// payload (see [`crate::dispatch`]). Each step therefore starts only after
    /// its predecessor has *completed*, which is what "chain" is supposed to
    /// mean — previously the elements were merely enqueued back-to-back, giving
    /// a `NestedChain` and a [`NestedGroup`] byte-identical broker traffic.
    ///
    /// [`CanvasElement::Branch`] and [`CanvasElement::Switch`] steps are carried
    /// in the chain tail and evaluated by the worker against the preceding
    /// step's result, so conditional workflows execute rather than being
    /// rejected at dispatch time.
    ///
    /// See [`validate`](Self::validate) for exactly which compositions are
    /// supported; unsupported ones fail here rather than being dispatched with
    /// the wrong ordering.
    ///
    /// Returns the id of the head task (or the group id, for a lone fan-out
    /// element).
    pub async fn apply<B: Broker>(&self, broker: &B) -> Result<Uuid, CanvasError> {
        self.validate()?;

        // A lone fan-out element is a plain parallel dispatch.
        if self.elements.len() == 1
            && matches!(
                self.elements[0],
                CanvasElement::Group(_) | CanvasElement::Map { .. }
            )
        {
            let group_id = Uuid::new_v4();
            dispatch_fanout_element(broker, &self.elements[0], group_id).await?;
            return Ok(group_id);
        }

        let mut steps = Vec::with_capacity(self.elements.len());
        for element in &self.elements {
            flatten_element(element, &mut steps)?;
        }

        dispatch_steps(broker, steps).await
    }

    /// Execute the nested chain with a result backend available, so a chord
    /// element can establish a real barrier.
    ///
    /// A chord is only sequenceable as the sole element of a nested chain (its
    /// callback is triggered by the barrier, and nothing can be linked after
    /// it while [`ChordState`](celers_backend_redis::ChordState) carries only a
    /// callback *name*). Every other composition behaves exactly as
    /// [`apply`](Self::apply).
    #[cfg(feature = "backend-redis")]
    pub async fn apply_with_backend<B: Broker, R: ResultBackend>(
        &self,
        broker: &B,
        backend: &mut R,
    ) -> Result<Uuid, CanvasError> {
        if self.elements.len() == 1 {
            if let CanvasElement::Chord { header, body } = &self.elements[0] {
                let chord_id = Uuid::new_v4();
                crate::Chord::register_and_dispatch(broker, backend, chord_id, header, body)
                    .await?;
                return Ok(chord_id);
            }
        }

        self.apply(broker).await
    }
}

impl Default for NestedChain {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for NestedChain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let element_strs: Vec<String> = self.elements.iter().map(|e| format!("{}", e)).collect();
        write!(f, "NestedChain[{}]", element_strs.join(" -> "))
    }
}

/// A nested group that can contain any canvas element
///
/// Unlike the basic Group that only contains Signatures, NestedGroup
/// can contain Chains, other Groups, or Chords as parallel tasks.
///
/// # Example
/// ```
/// use celers_canvas::{NestedGroup, CanvasElement, Chain, Signature};
///
/// let workflow = NestedGroup::new()
///     .add_element(CanvasElement::chain(
///         Chain::new().then("step1", vec![]).then("step2", vec![])
///     ))
///     .add_element(CanvasElement::chain(
///         Chain::new().then("step3", vec![]).then("step4", vec![])
///     ));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NestedGroup {
    /// Elements in the group (executed in parallel)
    pub elements: Vec<CanvasElement>,
}

impl NestedGroup {
    /// Create a new empty nested group
    pub fn new() -> Self {
        Self {
            elements: Vec::new(),
        }
    }

    /// Add an element to the group
    pub fn add_element(mut self, element: CanvasElement) -> Self {
        self.elements.push(element);
        self
    }

    /// Add a signature to the group
    pub fn add_signature(mut self, sig: Signature) -> Self {
        self.elements.push(CanvasElement::Signature(sig));
        self
    }

    /// Add a simple task to the group
    pub fn add(mut self, task: &str, args: Vec<serde_json::Value>) -> Self {
        self.elements.push(CanvasElement::task(task, args));
        self
    }

    /// Add a chain to the group
    pub fn add_chain(mut self, chain: Chain) -> Self {
        self.elements.push(CanvasElement::Chain(chain));
        self
    }

    /// Check if the group is empty
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// Get the number of elements
    pub fn len(&self) -> usize {
        self.elements.len()
    }

    /// Flatten to signatures if possible
    pub fn flatten_signatures(&self) -> Option<Vec<Signature>> {
        let mut result = Vec::new();

        for element in &self.elements {
            match element {
                CanvasElement::Signature(sig) => result.push(sig.clone()),
                _ => return None,
            }
        }

        Some(result)
    }

    /// Check that this nested group can actually be dispatched.
    ///
    /// Every element must be a self-contained parallel branch: a signature, a
    /// chain, a sub-group, a map, or (via
    /// [`apply_with_backend`](Self::apply_with_backend)) a chord.
    ///
    /// [`CanvasElement::Branch`] / [`CanvasElement::Switch`] are rejected: a
    /// conditional is evaluated against the result of the step *before* it, and
    /// a group branch has no predecessor. Put the conditional inside a
    /// [`NestedChain`] branch instead.
    pub fn validate(&self) -> Result<(), CanvasError> {
        if self.elements.is_empty() {
            return Err(CanvasError::Invalid(
                "NestedGroup cannot be empty".to_string(),
            ));
        }

        for element in &self.elements {
            match element {
                CanvasElement::Branch(_) | CanvasElement::Switch(_) => {
                    return Err(CanvasError::Invalid(
                        CONDITIONAL_NEEDS_PREDECESSOR.to_string(),
                    ));
                }
                CanvasElement::Group(group) if group.tasks.is_empty() => {
                    return Err(CanvasError::Invalid("Group cannot be empty".to_string()));
                }
                CanvasElement::Chord { header, .. } if header.tasks.is_empty() => {
                    return Err(CanvasError::Invalid(
                        "Chord header cannot be empty".to_string(),
                    ));
                }
                _ => {}
            }
        }

        Ok(())
    }

    /// Execute all elements in parallel
    ///
    /// All elements in the group are started concurrently. Every task enqueued
    /// on behalf of this group — including the members of nested groups and maps
    /// — is stamped with the group id this method returns, so the returned
    /// handle can actually be used to track the fan-out (it used to be minted
    /// and then thrown away).
    ///
    /// A [`CanvasElement::Chord`] branch needs a completion barrier and is
    /// rejected here; use [`apply_with_backend`](Self::apply_with_backend).
    pub async fn apply<B: Broker>(&self, broker: &B) -> Result<Uuid, CanvasError> {
        self.validate()?;

        // Generate a group ID for tracking
        let group_id = Uuid::new_v4();

        for element in &self.elements {
            if matches!(element, CanvasElement::Chord { .. }) {
                return Err(CanvasError::Invalid(
                    NESTED_CHORD_REQUIRES_BACKEND.to_string(),
                ));
            }
            Self::dispatch_branch(broker, element, group_id).await?;
        }

        Ok(group_id)
    }

    /// Execute all elements in parallel, establishing a real chord barrier for
    /// any [`CanvasElement::Chord`] branch.
    ///
    /// The chord's header tasks are registered in the backend and enqueued; the
    /// callback is **not** enqueued here — the worker enqueues it when the
    /// barrier's completion counter reaches the header size, with the header
    /// results as its argument. That is the difference between a chord and a
    /// group with a stray extra task.
    ///
    /// A chord branch's header tasks are stamped with the chord's own id rather
    /// than the enclosing group's, since
    /// [`group_id`](celers_core::TaskMetadata::group_id) holds a single value
    /// and the chord identity is the more useful one for those tasks.
    #[cfg(feature = "backend-redis")]
    pub async fn apply_with_backend<B: Broker, R: ResultBackend>(
        &self,
        broker: &B,
        backend: &mut R,
    ) -> Result<Uuid, CanvasError> {
        self.validate()?;

        let group_id = Uuid::new_v4();

        for element in &self.elements {
            match element {
                CanvasElement::Chord { header, body } => {
                    let chord_id = Uuid::new_v4();
                    crate::Chord::register_and_dispatch(broker, backend, chord_id, header, body)
                        .await?;
                }
                other => Self::dispatch_branch(broker, other, group_id).await?,
            }
        }

        Ok(group_id)
    }

    /// Dispatch one non-chord parallel branch under `group_id`.
    async fn dispatch_branch<B: Broker>(
        broker: &B,
        element: &CanvasElement,
        group_id: Uuid,
    ) -> Result<(), CanvasError> {
        match element {
            CanvasElement::Signature(sig) => {
                let mut task = dispatch::build_task(sig, &[])?;
                task.metadata.group_id = Some(group_id);
                dispatch::dispatch(broker, task, dispatch::Schedule::from_options(&sig.options))
                    .await?;
                Ok(())
            }
            CanvasElement::Chain(chain) => {
                let Some((head, tail)) = chain.tasks.split_first() else {
                    return Err(CanvasError::Invalid("Chain cannot be empty".to_string()));
                };
                let steps: Vec<ChainStep> = tail.iter().cloned().map(ChainStep::Task).collect();
                let mut task = dispatch::build_task(head, &steps)?;
                task.metadata.group_id = Some(group_id);
                dispatch::dispatch(
                    broker,
                    task,
                    dispatch::Schedule::from_options(&head.options),
                )
                .await?;
                Ok(())
            }
            CanvasElement::Group(_) | CanvasElement::Map { .. } => {
                dispatch_fanout_element(broker, element, group_id).await
            }
            CanvasElement::Chord { .. } => Err(CanvasError::Invalid(
                NESTED_CHORD_REQUIRES_BACKEND.to_string(),
            )),
            CanvasElement::Branch(_) | CanvasElement::Switch(_) => Err(CanvasError::Invalid(
                CONDITIONAL_NEEDS_PREDECESSOR.to_string(),
            )),
        }
    }
}

impl Default for NestedGroup {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for NestedGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let element_strs: Vec<String> = self.elements.iter().map(|e| format!("{}", e)).collect();
        write!(f, "NestedGroup[{}]", element_strs.join(" | "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use celers_core::SerializedTask;
    use std::sync::{Arc, Mutex};

    /// How a task reached the broker.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub(crate) enum Dispatched {
        /// `enqueue`
        Now,
        /// `enqueue_after(delay_secs)`
        After(u64),
        /// `enqueue_at(unix_timestamp)`
        At(i64),
    }

    /// Broker that records every enqueued task, in order, together with the
    /// enqueue variant it arrived through.
    #[derive(Clone, Default)]
    pub(crate) struct RecordingBroker {
        tasks: Arc<Mutex<Vec<(SerializedTask, Dispatched)>>>,
    }

    impl RecordingBroker {
        pub(crate) fn new() -> Self {
            Self::default()
        }

        fn entries(&self) -> Vec<(SerializedTask, Dispatched)> {
            self.tasks
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }

        pub(crate) fn tasks(&self) -> Vec<SerializedTask> {
            self.entries().into_iter().map(|(task, _)| task).collect()
        }

        pub(crate) fn names(&self) -> Vec<String> {
            self.tasks()
                .into_iter()
                .map(|task| task.metadata.name)
                .collect()
        }

        pub(crate) fn count(&self) -> usize {
            self.entries().len()
        }

        pub(crate) fn last_task(&self) -> Option<SerializedTask> {
            self.tasks().pop()
        }

        pub(crate) fn task_named(&self, name: &str) -> Option<SerializedTask> {
            self.tasks()
                .into_iter()
                .find(|task| task.metadata.name == name)
        }

        /// (task name, delay in seconds) for every enqueued task.
        pub(crate) fn schedules(&self) -> Vec<(String, Option<u64>)> {
            self.entries()
                .into_iter()
                .map(|(task, dispatched)| {
                    let delay = match dispatched {
                        Dispatched::Now => None,
                        Dispatched::After(secs) => Some(secs),
                        Dispatched::At(_) => None,
                    };
                    (task.metadata.name, delay)
                })
                .collect()
        }

        fn record(&self, task: SerializedTask, dispatched: Dispatched) -> celers_core::TaskId {
            let id = task.metadata.id;
            self.tasks
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push((task, dispatched));
            id
        }
    }

    #[async_trait::async_trait]
    impl celers_core::Broker for RecordingBroker {
        async fn enqueue(&self, task: SerializedTask) -> celers_core::Result<celers_core::TaskId> {
            Ok(self.record(task, Dispatched::Now))
        }

        async fn dequeue(&self) -> celers_core::Result<Option<celers_core::BrokerMessage>> {
            Ok(None)
        }

        async fn ack(
            &self,
            _task_id: &celers_core::TaskId,
            _receipt_handle: Option<&str>,
        ) -> celers_core::Result<()> {
            Ok(())
        }

        async fn reject(
            &self,
            _task_id: &celers_core::TaskId,
            _receipt_handle: Option<&str>,
            _requeue: bool,
        ) -> celers_core::Result<()> {
            Ok(())
        }

        async fn queue_size(&self) -> celers_core::Result<usize> {
            Ok(self.count())
        }

        async fn cancel(&self, _task_id: &celers_core::TaskId) -> celers_core::Result<bool> {
            Ok(true)
        }

        async fn enqueue_after(
            &self,
            task: SerializedTask,
            delay_secs: u64,
        ) -> celers_core::Result<celers_core::TaskId> {
            Ok(self.record(task, Dispatched::After(delay_secs)))
        }

        async fn enqueue_at(
            &self,
            task: SerializedTask,
            execute_at: i64,
        ) -> celers_core::Result<celers_core::TaskId> {
            Ok(self.record(task, Dispatched::At(execute_at)))
        }
    }

    /// Decode the chain tail a canvas-dispatched task carries in its payload.
    fn chain_tail_names(task: &celers_core::SerializedTask) -> Vec<String> {
        let envelope: serde_json::Value =
            serde_json::from_slice(&task.payload).expect("canvas payload must be JSON");
        envelope
            .get(crate::CHAIN_TAIL_KEY)
            .and_then(|tail| tail.as_array())
            .map(|steps| {
                steps
                    .iter()
                    .map(|step| {
                        step.get("task")
                            .and_then(|t| t.as_str())
                            .unwrap_or("<conditional>")
                            .to_string()
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// A NestedChain whose elements are all linear must be dispatched as ONE
    /// chain: only the head goes on the broker now, and every later step rides
    /// in its tail so it can only start after its predecessor completes.
    #[tokio::test]
    async fn nested_chain_of_linear_elements_dispatches_a_single_linked_chain() {
        let broker = RecordingBroker::new();

        let workflow = NestedChain::new()
            .then("head", vec![])
            .then_chain(Chain::new().then("mid1", vec![]).then("mid2", vec![]))
            .then("tail", vec![]);

        workflow.apply(&broker).await.expect("nested chain applies");

        assert_eq!(
            broker.names(),
            vec!["head".to_string()],
            "a chain enqueues only its head; the rest travel in the tail"
        );

        let head = broker
            .last_task()
            .expect("the head task must have been enqueued");
        assert_eq!(
            head.metadata.on_success_link.as_deref(),
            Some("mid1"),
            "the immediate successor's name must be linked"
        );
        assert_eq!(
            chain_tail_names(&head),
            vec!["mid1".to_string(), "mid2".to_string(), "tail".to_string()],
            "every remaining element must be flattened into the chain tail, in order"
        );
    }

    /// A NestedGroup runs each element as a concurrent branch, and every task it
    /// enqueues must carry the group id it returns so the fan-out is trackable.
    #[tokio::test]
    async fn nested_group_stamps_its_group_id_on_every_branch() {
        let broker = RecordingBroker::new();

        let workflow = NestedGroup::new()
            .add("solo", vec![])
            .add_chain(Chain::new().then("a1", vec![]).then("a2", vec![]))
            .add_element(CanvasElement::group(
                Group::new().add("g1", vec![]).add("g2", vec![]),
            ));

        let group_id = workflow.apply(&broker).await.expect("nested group applies");

        // solo (1) + chain head a1 (1) + sub-group g1,g2 (2) = 4
        assert_eq!(
            broker.names(),
            vec![
                "solo".to_string(),
                "a1".to_string(),
                "g1".to_string(),
                "g2".to_string(),
            ],
            "each branch contributes; nested chains enqueue only their head"
        );

        for task in broker.tasks() {
            assert_eq!(
                task.metadata.group_id,
                Some(group_id),
                "task '{}' must be stamped with the nested group's id",
                task.metadata.name
            );
        }

        let chain_head = broker
            .task_named("a1")
            .expect("the nested chain head must be enqueued");
        assert_eq!(
            chain_tail_names(&chain_head),
            vec!["a2".to_string()],
            "a chain branch keeps its own continuation"
        );
    }

    /// A chord nested inside a NestedChain must NOT enqueue its callback: the
    /// callback is triggered by the barrier once every header task completes.
    /// Without a result backend there is no barrier, so `apply` refuses.
    #[tokio::test]
    async fn nested_chain_chord_without_backend_is_refused() {
        let broker = RecordingBroker::new();

        let workflow = NestedChain::new()
            .then("before", vec![])
            .then_chord(
                Group::new().add("map_a", vec![]).add("map_b", vec![]),
                Signature::new("reduce".to_string()),
            )
            .then("after", vec![]);

        let err = workflow
            .apply(&broker)
            .await
            .expect_err("a chord without a barrier must not be dispatched");
        assert!(err.is_invalid());
        assert_eq!(
            broker.count(),
            0,
            "nothing may be enqueued when the chord cannot be honoured"
        );
    }

    /// Likewise for a chord branch of a NestedGroup: refusing is the only way to
    /// avoid running the callback in parallel with its own header.
    #[tokio::test]
    async fn nested_group_chord_without_backend_is_refused() {
        let broker = RecordingBroker::new();

        let workflow = NestedGroup::new()
            .add("sibling", vec![])
            .add_element(CanvasElement::chord(
                Group::new().add("h1", vec![]).add("h2", vec![]),
                Signature::new("callback".to_string()),
            ));

        let err = workflow
            .apply(&broker)
            .await
            .expect_err("a chord branch without a barrier must be refused");
        assert!(err.is_invalid());
        assert!(
            !broker.names().contains(&"callback".to_string()),
            "the callback must never be enqueued directly"
        );
    }

    /// A fan-out element in the middle of a NestedChain cannot be sequenced with
    /// the link mechanism, so it must fail closed instead of dispatching the
    /// following step concurrently with the fan-out.
    #[tokio::test]
    async fn nested_chain_rejects_unsequenceable_fanout_step() {
        let broker = RecordingBroker::new();

        let workflow = NestedChain::new()
            .then("head", vec![])
            .then_group(Group::new().add("par_a", vec![]).add("par_b", vec![]))
            .then("tail", vec![]);

        let err = workflow
            .apply(&broker)
            .await
            .expect_err("a mid-chain group cannot be sequenced");
        assert!(err.is_invalid());
        assert_eq!(broker.count(), 0, "nothing enqueued on a rejected workflow");

        // The same check is available at build time.
        assert!(workflow.validate().is_err());
    }

    /// A lone fan-out element is a plain parallel dispatch with nothing to
    /// sequence it against, so it is accepted.
    #[tokio::test]
    async fn nested_chain_of_a_single_group_dispatches_the_fanout() {
        let broker = RecordingBroker::new();

        let workflow =
            NestedChain::new().then_group(Group::new().add("p1", vec![]).add("p2", vec![]));

        let group_id = workflow.apply(&broker).await.expect("lone group applies");

        assert_eq!(broker.names(), vec!["p1".to_string(), "p2".to_string()]);
        for task in broker.tasks() {
            assert_eq!(task.metadata.group_id, Some(group_id));
        }
    }

    /// A Branch is a real chain step now: it is carried in the tail so the
    /// worker can evaluate it against the predecessor's result.
    #[tokio::test]
    async fn nested_chain_carries_a_branch_step_in_the_tail() {
        let broker = RecordingBroker::new();

        let branch = Branch::new(
            crate::Condition::field_greater_than("count", 100.0),
            Signature::new("big_batch".to_string()),
        )
        .otherwise(Signature::new("small_batch".to_string()));

        let workflow = NestedChain::new()
            .then("count_rows", vec![])
            .then_branch(branch);

        workflow
            .apply(&broker)
            .await
            .expect("a conditional workflow must dispatch");

        assert_eq!(broker.names(), vec!["count_rows".to_string()]);

        let head = broker.last_task().expect("head enqueued");
        assert!(
            head.metadata.on_success_link.is_none(),
            "a conditional successor has no statically known name"
        );

        let envelope: serde_json::Value =
            serde_json::from_slice(&head.payload).expect("payload is JSON");
        assert_eq!(envelope[crate::CHAIN_TAIL_KEY][0]["step_type"], "branch");
        assert_eq!(
            envelope[crate::CHAIN_TAIL_KEY][0]["then_branch"]["task"],
            "big_batch"
        );
    }

    /// A conditional cannot be the first step: there is no predecessor result to
    /// evaluate it against.
    #[tokio::test]
    async fn leading_conditional_is_rejected() {
        let broker = RecordingBroker::new();

        let workflow = NestedChain::new().then_branch(Branch::new(
            crate::Condition::always(),
            Signature::new("yes".to_string()),
        ));

        let err = workflow
            .apply(&broker)
            .await
            .expect_err("a leading conditional has nothing to evaluate");
        assert!(err.is_invalid());
        assert_eq!(broker.count(), 0);
    }

    /// A NestedGroup branch has no predecessor either, so conditionals are
    /// rejected at validation time rather than at dispatch time.
    #[test]
    fn nested_group_rejects_conditionals_at_validation_time() {
        let workflow =
            NestedGroup::new()
                .add("sibling", vec![])
                .add_element(CanvasElement::branch(Branch::new(
                    crate::Condition::always(),
                    Signature::new("yes".to_string()),
                )));

        let err = workflow
            .validate()
            .expect_err("a conditional group branch is meaningless");
        assert!(err.is_invalid());
    }

    /// A chord with an empty header is invalid and must surface an error rather
    /// than silently enqueueing only the callback.
    #[tokio::test]
    async fn nested_chain_chord_empty_header_errors() {
        let broker = RecordingBroker::new();

        let workflow =
            NestedChain::new().then_chord(Group::new(), Signature::new("cb".to_string()));

        let result = workflow.apply(&broker).await;
        assert!(result.is_err(), "empty chord header should error");
        assert_eq!(broker.count(), 0, "nothing should be enqueued on error");
    }

    /// An empty nested workflow is rejected in both directions.
    #[tokio::test]
    async fn empty_nested_workflows_are_rejected() {
        let broker = RecordingBroker::new();

        assert!(NestedChain::new().apply(&broker).await.is_err());
        assert!(NestedGroup::new().apply(&broker).await.is_err());
        assert_eq!(broker.count(), 0);
    }

    /// Per-signature countdowns must reach the broker's scheduling API rather
    /// than being dropped: a staggered group is only staggered if the delays
    /// actually travel.
    #[tokio::test]
    async fn nested_group_honours_member_countdowns() {
        let broker = RecordingBroker::new();

        let workflow = NestedGroup::new().add_element(CanvasElement::group(
            Group::new()
                .add("fast", vec![])
                .add("slow", vec![])
                .skew(0.0, 5.0),
        ));

        workflow.apply(&broker).await.expect("group applies");

        assert_eq!(
            broker.schedules(),
            vec![("fast".to_string(), None), ("slow".to_string(), Some(5))],
            "the second member must be handed to the delayed-enqueue path"
        );
    }

    /// Real chord barriers for nested chords require a result backend.
    #[cfg(feature = "backend-redis")]
    mod with_backend {
        use super::*;
        use crate::tests_backend::MockResultBackend;

        /// A chord inside a NestedGroup must register a barrier holding every
        /// header task id, stamp `chord_id` on the header tasks, and NOT enqueue
        /// the callback — the worker does that when the barrier completes.
        #[tokio::test]
        async fn nested_group_chord_establishes_a_real_barrier() {
            let broker = RecordingBroker::new();
            let mut backend = MockResultBackend::new();

            let workflow =
                NestedGroup::new()
                    .add("sibling", vec![])
                    .add_element(CanvasElement::chord(
                        Group::new().add("h1", vec![]).add("h2", vec![]),
                        Signature::new("callback".to_string()),
                    ));

            workflow
                .apply_with_backend(&broker, &mut backend)
                .await
                .expect("chord branch applies with a backend");

            assert_eq!(
                broker.names(),
                vec!["sibling".to_string(), "h1".to_string(), "h2".to_string()],
                "the callback must not be enqueued alongside its own header"
            );

            let state = backend.only_state();
            assert_eq!(state.callback.as_deref(), Some("callback"));
            assert_eq!(state.total, 2);
            assert_eq!(
                state.task_ids.len(),
                2,
                "the barrier must know which tasks it is waiting for"
            );

            let header_ids: Vec<_> = broker
                .tasks()
                .into_iter()
                .filter(|t| t.metadata.name.starts_with('h'))
                .map(|t| t.metadata.id)
                .collect();
            assert_eq!(
                state.task_ids, header_ids,
                "recorded ids must be the enqueued header tasks, in declaration order"
            );

            for task in broker.tasks() {
                if task.metadata.name.starts_with('h') {
                    assert_eq!(task.metadata.chord_id, Some(state.chord_id));
                }
            }
        }

        /// The same barrier is available for a chord that is the whole nested
        /// chain.
        #[tokio::test]
        async fn nested_chain_single_chord_establishes_a_real_barrier() {
            let broker = RecordingBroker::new();
            let mut backend = MockResultBackend::new();

            let workflow = NestedChain::new().then_chord(
                Group::new().add("m1", vec![]).add("m2", vec![]),
                Signature::new("reduce".to_string()),
            );

            let chord_id = workflow
                .apply_with_backend(&broker, &mut backend)
                .await
                .expect("single-chord nested chain applies");

            assert_eq!(broker.names(), vec!["m1".to_string(), "m2".to_string()]);
            let state = backend.only_state();
            assert_eq!(state.chord_id, chord_id);
            assert_eq!(state.callback.as_deref(), Some("reduce"));
        }
    }
}
