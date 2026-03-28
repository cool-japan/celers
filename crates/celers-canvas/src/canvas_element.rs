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
                    #[cfg(feature = "backend-redis")]
                    {
                        // Note: Chord requires backend, but we can't pass it here
                        // For now, fall back to just executing the group
                        last_id = Some(header.clone().apply(broker).await?);
                        // Callback would need to be manually triggered
                        let _ = body; // Silence unused warning
                    }
                    #[cfg(not(feature = "backend-redis"))]
                    {
                        last_id = Some(header.clone().apply(broker).await?);
                        let _ = body; // Silence unused warning
                    }
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
                    #[cfg(feature = "backend-redis")]
                    {
                        // Note: Chord requires backend, but we can't pass it here
                        // For now, fall back to just executing the group
                        header.clone().apply(broker).await?;
                        let _ = body; // Silence unused warning
                    }
                    #[cfg(not(feature = "backend-redis"))]
                    {
                        header.clone().apply(broker).await?;
                        let _ = body; // Silence unused warning
                    }
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
