use crate::{Branch, CanvasError, Chain, Group, Map, Signature, Switch};
use celers_core::Broker;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

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

    /// A conditional branch
    Branch(Branch),

    /// A switch statement
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

/// Execute a chord element (`header | ... -> body`) nested inside a workflow.
///
/// A chord fans out the `header` group in parallel and then runs the `body`
/// callback once the header has been dispatched. Both [`NestedChain`] and
/// [`NestedGroup`] share this logic so a nested chord is executed in full
/// rather than collapsing to just its header group.
///
/// Result aggregation for the callback is performed by the worker via the
/// chord barrier when a result backend is configured; here we are responsible
/// only for placing every component of the chord (header tasks + callback) onto
/// the broker so the workflow is not silently truncated.
///
/// Returns the id of the body callback, which is the logical tail of the chord
/// (this is what a surrounding [`NestedChain`] threads into its next step).
async fn apply_chord_element<B: Broker>(
    broker: &B,
    header: &Group,
    body: &Signature,
) -> Result<Uuid, CanvasError> {
    if header.tasks.is_empty() {
        return Err(CanvasError::Invalid(
            "Chord header cannot be empty".to_string(),
        ));
    }

    // Fan out the header group (parallel) first.
    header.clone().apply(broker).await?;

    // Then enqueue the callback body as the tail of the chord. The worker
    // applies the aggregated header results to it through the chord barrier
    // when a result backend is available.
    let body_chain = Chain {
        tasks: vec![body.clone()],
    };
    body_chain.apply(broker).await
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

    /// Execute the nested chain sequentially
    ///
    /// Each element is executed in order. For complex elements (Groups, Chords),
    /// they are executed and we wait for them to start before continuing.
    /// Note: This executes elements sequentially but doesn't wait for completion,
    /// following Celery's async execution model.
    pub async fn apply<B: Broker>(&self, broker: &B) -> Result<Uuid, CanvasError> {
        if self.elements.is_empty() {
            return Err(CanvasError::Invalid(
                "NestedChain cannot be empty".to_string(),
            ));
        }

        // Execute each element in sequence
        let mut last_id = None;
        for element in &self.elements {
            match element {
                CanvasElement::Signature(sig) => {
                    // Convert to Chain for sequential execution
                    let chain = Chain {
                        tasks: vec![sig.clone()],
                    };
                    last_id = Some(chain.apply(broker).await?);
                }
                CanvasElement::Chain(chain) => {
                    last_id = Some(chain.clone().apply(broker).await?);
                }
                CanvasElement::Group(group) => {
                    last_id = Some(group.clone().apply(broker).await?);
                }
                CanvasElement::Chord { header, body } => {
                    // Real nested execution: enqueue the header group (parallel
                    // fan-out) and then enqueue the body callback as the tail of
                    // the chord. In a NestedChain the chord's body becomes the
                    // last step of this element, so its id is the one we carry
                    // forward to subsequent chain elements.
                    last_id = Some(apply_chord_element(broker, header, body).await?);
                }
                CanvasElement::Map { task, argsets } => {
                    let map = Map::new(task.clone(), argsets.clone());
                    last_id = Some(map.apply(broker).await?);
                }
                CanvasElement::Branch(_branch) => {
                    // Branches require runtime evaluation, skip for now
                    return Err(CanvasError::Invalid(
                        "Branch elements not supported in NestedChain.apply()".to_string(),
                    ));
                }
                CanvasElement::Switch(_switch) => {
                    // Switch requires runtime evaluation, skip for now
                    return Err(CanvasError::Invalid(
                        "Switch elements not supported in NestedChain.apply()".to_string(),
                    ));
                }
            }
        }

        last_id.ok_or_else(|| CanvasError::Invalid("No elements executed".to_string()))
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

    /// Execute all elements in parallel
    ///
    /// All elements in the group are started concurrently.
    /// Returns a group ID that can be used to track the parallel execution.
    pub async fn apply<B: Broker>(&self, broker: &B) -> Result<Uuid, CanvasError> {
        if self.elements.is_empty() {
            return Err(CanvasError::Invalid(
                "NestedGroup cannot be empty".to_string(),
            ));
        }

        // Generate a group ID for tracking
        let group_id = Uuid::new_v4();

        // Execute all elements in parallel
        for element in &self.elements {
            match element {
                CanvasElement::Signature(sig) => {
                    let chain = Chain {
                        tasks: vec![sig.clone()],
                    };
                    chain.apply(broker).await?;
                }
                CanvasElement::Chain(chain) => {
                    chain.clone().apply(broker).await?;
                }
                CanvasElement::Group(group) => {
                    group.clone().apply(broker).await?;
                }
                CanvasElement::Chord { header, body } => {
                    // Real nested execution: this chord is one parallel branch of
                    // the group. Enqueue its header (parallel fan-out) and the
                    // body callback so the whole chord participates in the group
                    // rather than collapsing to just the header.
                    apply_chord_element(broker, header, body).await?;
                }
                CanvasElement::Map { task, argsets } => {
                    let map = Map::new(task.clone(), argsets.clone());
                    map.apply(broker).await?;
                }
                CanvasElement::Branch(_branch) => {
                    // Branches require runtime evaluation, skip for now
                    return Err(CanvasError::Invalid(
                        "Branch elements not supported in NestedGroup.apply()".to_string(),
                    ));
                }
                CanvasElement::Switch(_switch) => {
                    // Switch requires runtime evaluation, skip for now
                    return Err(CanvasError::Invalid(
                        "Switch elements not supported in NestedGroup.apply()".to_string(),
                    ));
                }
            }
        }

        Ok(group_id)
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
    use std::sync::{Arc, Mutex};

    /// Minimal broker that records the names of every enqueued task in order.
    #[derive(Clone)]
    struct RecordingBroker {
        tasks: Arc<Mutex<Vec<String>>>,
    }

    impl RecordingBroker {
        fn new() -> Self {
            Self {
                tasks: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn names(&self) -> Vec<String> {
            self.tasks
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .clone()
        }

        fn count(&self) -> usize {
            self.tasks
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .len()
        }
    }

    #[async_trait::async_trait]
    impl celers_core::Broker for RecordingBroker {
        async fn enqueue(
            &self,
            task: celers_core::SerializedTask,
        ) -> celers_core::Result<celers_core::TaskId> {
            let id = task.metadata.id;
            self.tasks
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(task.metadata.name.clone());
            Ok(id)
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
    }

    /// A NestedChain that contains a nested Group must fan the group out in
    /// parallel while keeping the surrounding chain sequential. A chain only
    /// enqueues its first task (links carry the rest), so the head task plus the
    /// two parallel group members yields three immediate enqueues, in order.
    #[tokio::test]
    async fn nested_chain_with_group_fans_out_in_order() {
        let broker = RecordingBroker::new();

        let workflow = NestedChain::new()
            .then("head", vec![])
            .then_group(Group::new().add("par_a", vec![]).add("par_b", vec![]))
            .then_chain(Chain::new().then("tail1", vec![]).then("tail2", vec![]));

        let result = workflow.apply(&broker).await;
        assert!(result.is_ok(), "nested chain should apply");

        // head (1) + group par_a, par_b (2) + tail chain first task (1) = 4
        assert_eq!(
            broker.names(),
            vec![
                "head".to_string(),
                "par_a".to_string(),
                "par_b".to_string(),
                "tail1".to_string(),
            ],
            "sequential order preserved; group fanned out; nested chain enqueues only its first task"
        );
    }

    /// A NestedGroup containing a nested Chain must run every branch in parallel
    /// while each nested chain still only enqueues its first task.
    #[tokio::test]
    async fn nested_group_with_nested_chains_runs_branches_in_parallel() {
        let broker = RecordingBroker::new();

        let workflow = NestedGroup::new()
            .add("solo", vec![])
            .add_chain(Chain::new().then("a1", vec![]).then("a2", vec![]))
            .add_chain(Chain::new().then("b1", vec![]).then("b2", vec![]));

        let result = workflow.apply(&broker).await;
        assert!(result.is_ok(), "nested group should apply");

        // solo (1) + chain-a first task (1) + chain-b first task (1) = 3
        assert_eq!(
            broker.names(),
            vec!["solo".to_string(), "a1".to_string(), "b1".to_string()],
            "each parallel branch contributes; nested chains enqueue only their first task"
        );
    }

    /// A chord nested inside a NestedChain must enqueue BOTH the header group
    /// (parallel) and the body callback, instead of collapsing to just the
    /// header. The callback is the tail, so subsequent chain steps follow it.
    #[tokio::test]
    async fn nested_chain_chord_enqueues_header_and_body() {
        let broker = RecordingBroker::new();

        let workflow = NestedChain::new()
            .then("before", vec![])
            .then_chord(
                Group::new().add("map_a", vec![]).add("map_b", vec![]),
                Signature::new("reduce".to_string()),
            )
            .then("after", vec![]);

        let result = workflow.apply(&broker).await;
        assert!(result.is_ok(), "nested chain with chord should apply");

        // before (1) + chord header map_a, map_b (2) + chord body reduce (1) + after (1) = 5
        assert_eq!(
            broker.names(),
            vec![
                "before".to_string(),
                "map_a".to_string(),
                "map_b".to_string(),
                "reduce".to_string(),
                "after".to_string(),
            ],
            "chord body callback must be enqueued, not dropped"
        );
    }

    /// A chord nested inside a NestedGroup must likewise enqueue header + body
    /// as one parallel branch of the group.
    #[tokio::test]
    async fn nested_group_chord_enqueues_header_and_body() {
        let broker = RecordingBroker::new();

        let workflow = NestedGroup::new()
            .add("sibling", vec![])
            .add_element(CanvasElement::chord(
                Group::new().add("h1", vec![]).add("h2", vec![]),
                Signature::new("callback".to_string()),
            ));

        let result = workflow.apply(&broker).await;
        assert!(result.is_ok(), "nested group with chord should apply");

        // sibling (1) + chord header h1, h2 (2) + chord body callback (1) = 4
        assert_eq!(broker.count(), 4, "chord header + body both enqueued");
        let names = broker.names();
        assert!(names.contains(&"sibling".to_string()));
        assert!(names.contains(&"h1".to_string()));
        assert!(names.contains(&"h2".to_string()));
        assert!(
            names.contains(&"callback".to_string()),
            "chord body callback must be enqueued, not dropped"
        );
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

    /// Deeply nested composition: a chain whose steps are themselves a group and
    /// a chord. Verifies recursion executes every leaf task.
    #[tokio::test]
    async fn deeply_nested_composition_executes_all_leaves() {
        let broker = RecordingBroker::new();

        let workflow = NestedChain::new()
            .then_group(
                Group::new()
                    .add("g1", vec![])
                    .add("g2", vec![])
                    .add("g3", vec![]),
            )
            .then_chord(
                Group::new().add("c1", vec![]).add("c2", vec![]),
                Signature::new("done".to_string()),
            );

        let result = workflow.apply(&broker).await;
        assert!(result.is_ok(), "deeply nested composition should apply");

        // group g1,g2,g3 (3) + chord header c1,c2 (2) + chord body done (1) = 6
        assert_eq!(
            broker.count(),
            6,
            "every leaf task of the nested workflow is enqueued"
        );
    }
}
