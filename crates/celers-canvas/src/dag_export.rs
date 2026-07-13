//! Workflow DAG visualization export (GraphViz DOT and Mermaid).
//!
//! This module turns CeleRS canvas workflows into directed-acyclic-graph
//! descriptions that external tools can render:
//!
//! * **Mermaid** flowcharts (`flowchart TD` / `graph TD`) for embedding in
//!   Markdown, docs, and web UIs.
//! * **GraphViz DOT** (`digraph { ... }`) for rendering to SVG/PNG via the
//!   `dot` command line tool.
//!
//! Two complementary APIs are provided:
//!
//! * [`DagExport`] — the classic flat exporter for [`Chain`], [`Group`] and
//!   [`Chord`]. It emits a compact, stable representation and also offers
//!   convenience `to_svg`/`to_png` rendering helpers that shell out to
//!   GraphViz.
//! * [`DagVisualize`] — a recursive exporter for the *nested* and *advanced*
//!   workflow primitives ([`CanvasElement`], [`NestedChain`], [`NestedGroup`],
//!   [`Map`], [`Starmap`], [`Chunks`], [`Branch`], [`Switch`]). It walks the
//!   whole structure, recursing into sub-workflows while assigning stable,
//!   unique node ids derived purely from traversal order (never random), so
//!   the output is fully deterministic.
//!
//! Both exporters escape node labels so task names containing quotes,
//! backslashes, or newlines cannot corrupt the generated graph text.

use crate::{
    Branch, CanvasElement, CanvasError, Chain, Chord, Chunks, Condition, Group, Map, Signature,
    Starmap, Switch,
};

/// Output formats supported for DAG export.
pub enum DagFormat {
    /// GraphViz DOT format
    Dot,
    /// Mermaid diagram format
    Mermaid,
    /// JSON representation
    Json,
    /// SVG format (rendered from DOT)
    Svg,
    /// PNG format (rendered from DOT)
    Png,
}

// ---------------------------------------------------------------------------
// Label escaping
// ---------------------------------------------------------------------------

/// Escape a label for inclusion inside a GraphViz DOT double-quoted string.
///
/// DOT treats `"` as the string terminator and `\` as an escape introducer, so
/// both must be escaped. Newlines are converted to the DOT left-justified line
/// break (`\l`) so multi-line task names render on separate lines instead of
/// breaking the surrounding `digraph { ... }` block.
fn escape_dot_label(label: &str) -> String {
    let mut out = String::with_capacity(label.len());
    for ch in label.chars() {
        match ch {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => {}
            other => out.push(other),
        }
    }
    out
}

/// Escape a label for inclusion inside a Mermaid quoted node label
/// (e.g. `id["label"]`).
///
/// Mermaid uses `"` to delimit the label and `#quot;` / HTML entities for
/// escaping. Double quotes are replaced with the `#quot;` entity, and newlines
/// with the `<br/>` line break supported inside quoted labels. Backslashes are
/// left as-is (they are not special inside a quoted Mermaid label) but control
/// characters are stripped.
fn escape_mermaid_label(label: &str) -> String {
    let mut out = String::with_capacity(label.len());
    for ch in label.chars() {
        match ch {
            '"' => out.push_str("#quot;"),
            '\n' => out.push_str("<br/>"),
            '\r' => {}
            other => out.push(other),
        }
    }
    out
}

// ---------------------------------------------------------------------------
// GraphViz rendering helpers (shell out to `dot`)
// ---------------------------------------------------------------------------

/// Render DOT format to SVG using the GraphViz `dot` command.
///
/// Requires GraphViz to be installed on the system. Executes `dot -Tsvg`.
///
/// # Errors
/// Returns an error if GraphViz is not installed or execution fails.
fn render_dot_to_svg(dot: &str) -> Result<String, CanvasError> {
    use std::io::Write;
    use std::process::{Command, Stdio};

    let mut child = Command::new("dot")
        .arg("-Tsvg")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| {
            CanvasError::Invalid(format!(
                "Failed to execute 'dot' command. Is GraphViz installed? Error: {}",
                e
            ))
        })?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(dot.as_bytes())
            .map_err(|e| CanvasError::Invalid(format!("Failed to write DOT to stdin: {}", e)))?;
    }

    let output = child
        .wait_with_output()
        .map_err(|e| CanvasError::Invalid(format!("Failed to wait for dot process: {}", e)))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(CanvasError::Invalid(format!(
            "dot command failed: {}",
            stderr
        )));
    }

    String::from_utf8(output.stdout)
        .map_err(|e| CanvasError::Invalid(format!("Invalid UTF-8 in SVG output: {}", e)))
}

/// Render DOT format to PNG using the GraphViz `dot` command.
///
/// Requires GraphViz to be installed on the system. Executes `dot -Tpng`.
///
/// # Errors
/// Returns an error if GraphViz is not installed or execution fails.
fn render_dot_to_png(dot: &str) -> Result<Vec<u8>, CanvasError> {
    use std::io::Write;
    use std::process::{Command, Stdio};

    let mut child = Command::new("dot")
        .arg("-Tpng")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| {
            CanvasError::Invalid(format!(
                "Failed to execute 'dot' command. Is GraphViz installed? Error: {}",
                e
            ))
        })?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(dot.as_bytes())
            .map_err(|e| CanvasError::Invalid(format!("Failed to write DOT to stdin: {}", e)))?;
    }

    let output = child
        .wait_with_output()
        .map_err(|e| CanvasError::Invalid(format!("Failed to wait for dot process: {}", e)))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(CanvasError::Invalid(format!(
            "dot command failed: {}",
            stderr
        )));
    }

    Ok(output.stdout)
}

/// Check if the GraphViz `dot` command is available on the system.
///
/// # Example
/// ```no_run
/// if celers_canvas::is_graphviz_available() {
///     println!("GraphViz is installed");
/// } else {
///     println!("GraphViz is not installed");
/// }
/// ```
#[allow(dead_code)]
pub fn is_graphviz_available() -> bool {
    use std::process::Command;

    Command::new("dot")
        .arg("-V")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Flat exporter: DagExport (Chain, Group, Chord)
// ---------------------------------------------------------------------------

/// Trait for exporting a flat workflow primitive as a DAG.
///
/// Implemented for [`Chain`], [`Group`] and [`Chord`]. For nested/advanced
/// primitives use [`DagVisualize`] instead.
pub trait DagExport {
    /// Export the workflow as GraphViz DOT.
    fn to_dot(&self) -> String;

    /// Export the workflow as a Mermaid diagram.
    fn to_mermaid(&self) -> String;

    /// Export the workflow as JSON.
    ///
    /// # Errors
    /// Returns an error if serialization fails.
    fn to_json(&self) -> Result<String, serde_json::Error>;

    /// Export the workflow as SVG using the GraphViz `dot` command.
    ///
    /// Requires GraphViz to be installed on the system.
    ///
    /// # Errors
    /// Returns an error if GraphViz is not installed or execution fails.
    fn to_svg(&self) -> Result<String, CanvasError> {
        let dot = self.to_dot();
        render_dot_to_svg(&dot)
    }

    /// Export the workflow as PNG using the GraphViz `dot` command.
    ///
    /// Requires GraphViz to be installed on the system.
    ///
    /// # Errors
    /// Returns an error if GraphViz is not installed or execution fails.
    fn to_png(&self) -> Result<Vec<u8>, CanvasError> {
        let dot = self.to_dot();
        render_dot_to_png(&dot)
    }

    /// Return the shell command that renders DOT to SVG (for manual rendering).
    fn svg_render_command(&self) -> String {
        "dot -Tsvg -o output.svg input.dot".to_string()
    }

    /// Return the shell command that renders DOT to PNG (for manual rendering).
    fn png_render_command(&self) -> String {
        "dot -Tpng -o output.png input.dot".to_string()
    }
}

impl DagExport for Chain {
    fn to_dot(&self) -> String {
        let mut dot = String::from("digraph Chain {\n");
        dot.push_str("  rankdir=LR;\n");
        dot.push_str("  node [shape=box];\n\n");

        for (i, task) in self.tasks.iter().enumerate() {
            dot.push_str(&format!(
                "  n{} [label=\"{}\"];\n",
                i,
                escape_dot_label(&task.task)
            ));
            if i > 0 {
                dot.push_str(&format!("  n{} -> n{};\n", i - 1, i));
            }
        }

        dot.push_str("}\n");
        dot
    }

    fn to_mermaid(&self) -> String {
        let mut mmd = String::from("graph LR\n");

        for (i, task) in self.tasks.iter().enumerate() {
            let node_id = format!("n{}", i);
            mmd.push_str(&format!(
                "  {}[\"{}\"]\n",
                node_id,
                escape_mermaid_label(&task.task)
            ));
            if i > 0 {
                mmd.push_str(&format!("  n{} --> n{}\n", i - 1, i));
            }
        }

        mmd
    }

    fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

impl DagExport for Group {
    fn to_dot(&self) -> String {
        let mut dot = String::from("digraph Group {\n");
        dot.push_str("  rankdir=TB;\n");
        dot.push_str("  node [shape=box];\n\n");
        dot.push_str("  start [shape=circle, label=\"start\"];\n");

        for (i, task) in self.tasks.iter().enumerate() {
            dot.push_str(&format!(
                "  n{} [label=\"{}\"];\n",
                i,
                escape_dot_label(&task.task)
            ));
            dot.push_str(&format!("  start -> n{};\n", i));
        }

        dot.push_str("}\n");
        dot
    }

    fn to_mermaid(&self) -> String {
        let mut mmd = String::from("graph TB\n");
        mmd.push_str("  start((start))\n");

        for (i, task) in self.tasks.iter().enumerate() {
            mmd.push_str(&format!(
                "  n{}[\"{}\"]\n",
                i,
                escape_mermaid_label(&task.task)
            ));
            mmd.push_str(&format!("  start --> n{}\n", i));
        }

        mmd
    }

    fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

impl DagExport for Chord {
    fn to_dot(&self) -> String {
        let mut dot = String::from("digraph Chord {\n");
        dot.push_str("  rankdir=TB;\n");
        dot.push_str("  node [shape=box];\n\n");
        dot.push_str("  start [shape=circle, label=\"start\"];\n");
        dot.push_str(&format!(
            "  callback [label=\"{}\", style=filled, fillcolor=lightblue];\n",
            escape_dot_label(&self.body.task)
        ));

        for (i, task) in self.header.tasks.iter().enumerate() {
            dot.push_str(&format!(
                "  n{} [label=\"{}\"];\n",
                i,
                escape_dot_label(&task.task)
            ));
            dot.push_str(&format!("  start -> n{};\n", i));
            dot.push_str(&format!("  n{} -> callback;\n", i));
        }

        dot.push_str("}\n");
        dot
    }

    fn to_mermaid(&self) -> String {
        let mut mmd = String::from("graph TB\n");
        mmd.push_str("  start((start))\n");
        mmd.push_str(&format!(
            "  callback[\"{}\"]\n",
            escape_mermaid_label(&self.body.task)
        ));
        mmd.push_str("  style callback fill:#add8e6\n");

        for (i, task) in self.header.tasks.iter().enumerate() {
            mmd.push_str(&format!(
                "  n{}[\"{}\"]\n",
                i,
                escape_mermaid_label(&task.task)
            ));
            mmd.push_str(&format!("  start --> n{}\n", i));
            mmd.push_str(&format!("  n{} --> callback\n", i));
        }

        mmd
    }

    fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

// ---------------------------------------------------------------------------
// Recursive DAG model + builder (shared by DagVisualize)
// ---------------------------------------------------------------------------

/// Shape of a node in the rendered graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NodeShape {
    /// A concrete task (rectangle).
    Task,
    /// A synthetic control node such as a group fan-out point (circle/diamond).
    Control,
    /// A chord/branch callback or merge node (highlighted rectangle).
    Callback,
}

/// A single node in the graph being built.
#[derive(Debug, Clone)]
struct DagNode {
    /// Deterministic, unique node id (e.g. `n0`, `n1_2`).
    id: String,
    /// Human-readable label (already raw, escaped at render time).
    label: String,
    /// Rendering shape.
    shape: NodeShape,
}

/// An edge in the graph being built.
#[derive(Debug, Clone)]
struct DagEdge {
    /// Source node id.
    from: String,
    /// Destination node id.
    to: String,
    /// Optional edge label (e.g. a branch condition), raw.
    label: Option<String>,
}

/// The fragment of a graph produced by walking one sub-workflow.
///
/// A fragment records every node and edge created while visiting a sub-tree,
/// plus the logical *entry* and *exit* node ids used to wire the fragment into
/// its parent:
///
/// * `entry` is where an upstream step should point to.
/// * `exits` are the node ids a downstream step should be reached from (a group
///   has many exits; a single task has exactly one).
struct DagFragment {
    /// The id an upstream node should connect *into*.
    entry: String,
    /// The ids a downstream node should connect *from*.
    exits: Vec<String>,
}

/// Accumulating graph builder with deterministic, collision-free node ids.
///
/// Ids are derived from a hierarchical path of traversal indices (never from
/// random values or timestamps), guaranteeing stable output for identical
/// inputs. The root graph orientation is `TD` (top-down) for both Mermaid and
/// DOT.
struct DagBuilder {
    nodes: Vec<DagNode>,
    edges: Vec<DagEdge>,
}

impl DagBuilder {
    fn new() -> Self {
        Self {
            nodes: Vec::new(),
            edges: Vec::new(),
        }
    }

    /// Build a deterministic node id from a hierarchical path of indices.
    ///
    /// e.g. `[0]` -> `n0`, `[1, 2]` -> `n1_2`. The path encodes the traversal
    /// position so distinct nodes always receive distinct ids regardless of how
    /// deeply the workflow is nested.
    fn make_id(path: &[usize]) -> String {
        let mut id = String::from("n");
        for (i, idx) in path.iter().enumerate() {
            if i > 0 {
                id.push('_');
            }
            id.push_str(&idx.to_string());
        }
        id
    }

    /// Add a node, returning its id.
    fn add_node(&mut self, id: String, label: String, shape: NodeShape) -> String {
        self.nodes.push(DagNode {
            id: id.clone(),
            label,
            shape,
        });
        id
    }

    /// Add an edge between two existing node ids.
    fn add_edge(&mut self, from: &str, to: &str, label: Option<String>) {
        self.edges.push(DagEdge {
            from: from.to_string(),
            to: to.to_string(),
            label,
        });
    }

    /// Connect every exit of one fragment to the entry of the next, threading
    /// sequential semantics (used by chains).
    fn connect_sequential(&mut self, prev_exits: &[String], next_entry: &str) {
        for exit in prev_exits {
            self.add_edge(exit, next_entry, None);
        }
    }

    // -- Per-primitive visitors -------------------------------------------

    /// Visit a flat [`Chain`]: sequential edges a -> b -> c.
    fn visit_chain(&mut self, chain: &Chain, path: &[usize]) -> DagFragment {
        if chain.tasks.is_empty() {
            return self.empty_fragment(path, "empty chain");
        }

        let mut entry: Option<String> = None;
        let mut prev_exit: Option<String> = None;

        for (i, sig) in chain.tasks.iter().enumerate() {
            let mut child_path = path.to_vec();
            child_path.push(i);
            let id = Self::make_id(&child_path);
            self.add_node(id.clone(), sig.task.clone(), NodeShape::Task);

            if let Some(prev) = &prev_exit {
                self.add_edge(prev, &id, None);
            }
            if entry.is_none() {
                entry = Some(id.clone());
            }
            prev_exit = Some(id);
        }

        let entry = entry.unwrap_or_else(|| Self::make_id(path));
        let exit = prev_exit.unwrap_or_else(|| entry.clone());
        DagFragment {
            entry,
            exits: vec![exit],
        }
    }

    /// Visit a flat [`Group`]: fan-out from a synthetic start node to each
    /// member. The group's exits are all its members (so a downstream step
    /// joins them, mirroring chord/parallel-join semantics).
    fn visit_group(&mut self, group: &Group, path: &[usize]) -> DagFragment {
        let start_id = Self::make_id(path);
        self.add_node(start_id.clone(), "group".to_string(), NodeShape::Control);

        if group.tasks.is_empty() {
            return DagFragment {
                entry: start_id.clone(),
                exits: vec![start_id],
            };
        }

        let mut exits = Vec::with_capacity(group.tasks.len());
        for (i, sig) in group.tasks.iter().enumerate() {
            let mut child_path = path.to_vec();
            child_path.push(i);
            let id = Self::make_id(&child_path);
            self.add_node(id.clone(), sig.task.clone(), NodeShape::Task);
            self.add_edge(&start_id, &id, None);
            exits.push(id);
        }

        DagFragment {
            entry: start_id,
            exits,
        }
    }

    /// Visit a flat [`Chord`]: the header group fans out, and every header
    /// member feeds into the callback/body node.
    fn visit_chord(&mut self, header: &Group, body: &Signature, path: &[usize]) -> DagFragment {
        // Header group lives under index 0 of this chord's path.
        let mut header_path = path.to_vec();
        header_path.push(0);
        let header_fragment = self.visit_group(header, &header_path);

        // Callback/body lives under index 1.
        let mut body_path = path.to_vec();
        body_path.push(1);
        let callback_id = Self::make_id(&body_path);
        self.add_node(callback_id.clone(), body.task.clone(), NodeShape::Callback);

        // Every header member feeds into the callback.
        self.connect_sequential(&header_fragment.exits, &callback_id);

        DagFragment {
            entry: header_fragment.entry,
            exits: vec![callback_id],
        }
    }

    /// Visit a [`Map`] / [`Starmap`]: one synthetic fan-out node feeding one
    /// task instance per argument set.
    fn visit_map_like(
        &mut self,
        task: &Signature,
        argset_count: usize,
        kind: &str,
        path: &[usize],
    ) -> DagFragment {
        let start_id = Self::make_id(path);
        self.add_node(start_id.clone(), kind.to_string(), NodeShape::Control);

        if argset_count == 0 {
            return DagFragment {
                entry: start_id.clone(),
                exits: vec![start_id],
            };
        }

        let mut exits = Vec::with_capacity(argset_count);
        for i in 0..argset_count {
            let mut child_path = path.to_vec();
            child_path.push(i);
            let id = Self::make_id(&child_path);
            let label = format!("{}[{}]", task.task, i);
            self.add_node(id.clone(), label, NodeShape::Task);
            self.add_edge(&start_id, &id, None);
            exits.push(id);
        }

        DagFragment {
            entry: start_id,
            exits,
        }
    }

    /// Visit [`Chunks`]: a synthetic fan-out node feeding one task per chunk.
    fn visit_chunks(&mut self, chunks: &Chunks, path: &[usize]) -> DagFragment {
        let start_id = Self::make_id(path);
        self.add_node(start_id.clone(), "chunks".to_string(), NodeShape::Control);

        let num = chunks.num_chunks();
        if num == 0 {
            return DagFragment {
                entry: start_id.clone(),
                exits: vec![start_id],
            };
        }

        let mut exits = Vec::with_capacity(num);
        for i in 0..num {
            let mut child_path = path.to_vec();
            child_path.push(i);
            let id = Self::make_id(&child_path);
            let label = format!("{}[chunk {}]", chunks.task.task, i);
            self.add_node(id.clone(), label, NodeShape::Task);
            self.add_edge(&start_id, &id, None);
            exits.push(id);
        }

        DagFragment {
            entry: start_id,
            exits,
        }
    }

    /// Visit a [`Branch`]: a decision node with labelled `then`/`else` edges.
    fn visit_branch(&mut self, branch: &Branch, path: &[usize]) -> DagFragment {
        let decision_id = Self::make_id(path);
        self.add_node(
            decision_id.clone(),
            condition_label(&branch.condition),
            NodeShape::Control,
        );

        let mut exits = Vec::new();

        // then branch -> index 0
        let mut then_path = path.to_vec();
        then_path.push(0);
        let then_id = Self::make_id(&then_path);
        self.add_node(
            then_id.clone(),
            branch.then_branch.task.clone(),
            NodeShape::Task,
        );
        self.add_edge(&decision_id, &then_id, Some("true".to_string()));
        exits.push(then_id);

        // else branch -> index 1
        if let Some(else_sig) = &branch.else_branch {
            let mut else_path = path.to_vec();
            else_path.push(1);
            let else_id = Self::make_id(&else_path);
            self.add_node(else_id.clone(), else_sig.task.clone(), NodeShape::Task);
            self.add_edge(&decision_id, &else_id, Some("false".to_string()));
            exits.push(else_id);
        }

        DagFragment {
            entry: decision_id,
            exits,
        }
    }

    /// Visit a [`Switch`]: a decision node with one labelled edge per case plus
    /// an optional default edge.
    fn visit_switch(&mut self, switch: &Switch, path: &[usize]) -> DagFragment {
        let decision_id = Self::make_id(path);
        self.add_node(
            decision_id.clone(),
            "switch".to_string(),
            NodeShape::Control,
        );

        let mut exits = Vec::new();

        for (i, (condition, sig)) in switch.cases.iter().enumerate() {
            let mut case_path = path.to_vec();
            case_path.push(i);
            let id = Self::make_id(&case_path);
            self.add_node(id.clone(), sig.task.clone(), NodeShape::Task);
            self.add_edge(&decision_id, &id, Some(condition_label(condition)));
            exits.push(id);
        }

        if let Some(default) = &switch.default {
            let mut default_path = path.to_vec();
            default_path.push(switch.cases.len());
            let id = Self::make_id(&default_path);
            self.add_node(id.clone(), default.task.clone(), NodeShape::Task);
            self.add_edge(&decision_id, &id, Some("default".to_string()));
            exits.push(id);
        }

        DagFragment {
            entry: decision_id,
            exits,
        }
    }

    /// Visit a [`CanvasElement`], dispatching to the appropriate visitor and
    /// recursing into nested sub-workflows.
    fn visit_element(&mut self, element: &CanvasElement, path: &[usize]) -> DagFragment {
        match element {
            CanvasElement::Signature(sig) => {
                let id = Self::make_id(path);
                self.add_node(id.clone(), sig.task.clone(), NodeShape::Task);
                DagFragment {
                    entry: id.clone(),
                    exits: vec![id],
                }
            }
            CanvasElement::Chain(chain) => self.visit_chain(chain, path),
            CanvasElement::Group(group) => self.visit_group(group, path),
            CanvasElement::Chord { header, body } => self.visit_chord(header, body, path),
            CanvasElement::Map { task, argsets } => {
                self.visit_map_like(task, argsets.len(), "map", path)
            }
            CanvasElement::Branch(branch) => self.visit_branch(branch, path),
            CanvasElement::Switch(switch) => self.visit_switch(switch, path),
        }
    }

    /// Visit a [`NestedChain`]: sequentially threads each element fragment,
    /// connecting every exit of the previous element to the entry of the next.
    fn visit_nested_chain(&mut self, elements: &[CanvasElement], path: &[usize]) -> DagFragment {
        if elements.is_empty() {
            return self.empty_fragment(path, "empty chain");
        }

        let mut entry: Option<String> = None;
        let mut prev_exits: Vec<String> = Vec::new();

        for (i, element) in elements.iter().enumerate() {
            let mut child_path = path.to_vec();
            child_path.push(i);
            let fragment = self.visit_element(element, &child_path);

            if entry.is_none() {
                entry = Some(fragment.entry.clone());
            } else {
                self.connect_sequential(&prev_exits, &fragment.entry);
            }
            prev_exits = fragment.exits;
        }

        let entry = entry.unwrap_or_else(|| Self::make_id(path));
        DagFragment {
            entry,
            exits: prev_exits,
        }
    }

    /// Visit a [`NestedGroup`]: a synthetic fan-out node feeding the entry of
    /// every parallel element; the group's exits are the union of each
    /// element's exits.
    fn visit_nested_group(&mut self, elements: &[CanvasElement], path: &[usize]) -> DagFragment {
        let start_id = Self::make_id(path);
        self.add_node(start_id.clone(), "group".to_string(), NodeShape::Control);

        if elements.is_empty() {
            return DagFragment {
                entry: start_id.clone(),
                exits: vec![start_id],
            };
        }

        let mut exits = Vec::new();
        for (i, element) in elements.iter().enumerate() {
            let mut child_path = path.to_vec();
            child_path.push(i);
            let fragment = self.visit_element(element, &child_path);
            self.add_edge(&start_id, &fragment.entry, None);
            exits.extend(fragment.exits);
        }

        DagFragment {
            entry: start_id,
            exits,
        }
    }

    /// Create a single placeholder node for an empty container so the output is
    /// still a valid, non-empty graph.
    fn empty_fragment(&mut self, path: &[usize], label: &str) -> DagFragment {
        let id = Self::make_id(path);
        self.add_node(id.clone(), label.to_string(), NodeShape::Control);
        DagFragment {
            entry: id.clone(),
            exits: vec![id],
        }
    }

    // -- Renderers ---------------------------------------------------------

    /// Render the accumulated graph as Mermaid `flowchart TD` text.
    fn render_mermaid(&self) -> String {
        let mut out = String::from("flowchart TD\n");

        for node in &self.nodes {
            let label = escape_mermaid_label(&node.label);
            let line = match node.shape {
                NodeShape::Task => format!("  {}[\"{}\"]\n", node.id, label),
                NodeShape::Control => format!("  {}([\"{}\"])\n", node.id, label),
                NodeShape::Callback => format!("  {}[[\"{}\"]]\n", node.id, label),
            };
            out.push_str(&line);
        }

        for edge in &self.edges {
            match &edge.label {
                Some(lbl) => out.push_str(&format!(
                    "  {} -->|{}| {}\n",
                    edge.from,
                    escape_mermaid_label(lbl),
                    edge.to
                )),
                None => out.push_str(&format!("  {} --> {}\n", edge.from, edge.to)),
            }
        }

        out
    }

    /// Render the accumulated graph as GraphViz DOT `digraph { ... }` text.
    fn render_dot(&self, name: &str) -> String {
        let mut out = format!("digraph {} {{\n", name);
        out.push_str("  rankdir=TB;\n");
        out.push_str("  node [shape=box];\n\n");

        for node in &self.nodes {
            let label = escape_dot_label(&node.label);
            let line = match node.shape {
                NodeShape::Task => format!("  {} [label=\"{}\", shape=box];\n", node.id, label),
                NodeShape::Control => {
                    format!("  {} [label=\"{}\", shape=ellipse];\n", node.id, label)
                }
                NodeShape::Callback => format!(
                    "  {} [label=\"{}\", shape=box, style=filled, fillcolor=lightblue];\n",
                    node.id, label
                ),
            };
            out.push_str(&line);
        }

        out.push('\n');

        for edge in &self.edges {
            match &edge.label {
                Some(lbl) => out.push_str(&format!(
                    "  {} -> {} [label=\"{}\"];\n",
                    edge.from,
                    edge.to,
                    escape_dot_label(lbl)
                )),
                None => out.push_str(&format!("  {} -> {};\n", edge.from, edge.to)),
            }
        }

        out.push_str("}\n");
        out
    }
}

/// Build a short, human-readable label for a [`Condition`] suitable for an edge
/// or decision-node label. Uses the condition's own [`std::fmt::Display`].
fn condition_label(condition: &Condition) -> String {
    format!("{}", condition)
}

// ---------------------------------------------------------------------------
// Recursive exporter: DagVisualize (nested + advanced primitives)
// ---------------------------------------------------------------------------

/// Private, module-local source trait that produces the recursive graph for a
/// workflow primitive. It is intentionally **not** re-exported, so the private
/// [`DagBuilder`] type never appears in the crate's public API. The public
/// [`DagVisualize`] trait is blanket-implemented for every `DagSource`.
trait DagSource {
    /// Build the recursive graph for this workflow.
    fn build_dag(&self) -> DagBuilder;

    /// The graph name used for the `digraph <name> { ... }` header.
    fn dag_name(&self) -> &'static str;
}

/// Trait for exporting *nested* and *advanced* workflow primitives as a DAG.
///
/// Implemented for [`CanvasElement`], [`NestedChain`](crate::NestedChain), [`NestedGroup`](crate::NestedGroup),
/// [`Map`], [`Starmap`], [`Chunks`], [`Branch`] and [`Switch`]. Unlike
/// [`DagExport`], these implementations recurse into sub-workflows and assign
/// stable, unique node ids derived purely from traversal order, so the emitted
/// graph is deterministic and free of id collisions even for deeply nested
/// structures.
///
/// # Example
/// ```
/// use celers_canvas::{Chain, DagVisualize, NestedGroup};
///
/// let workflow = NestedGroup::new()
///     .add_chain(Chain::new().then("a1", vec![]).then("a2", vec![]))
///     .add_chain(Chain::new().then("b1", vec![]));
///
/// let mermaid = workflow.to_mermaid();
/// assert!(mermaid.starts_with("flowchart TD"));
///
/// let dot = workflow.to_dot();
/// assert!(dot.contains("digraph NestedGroup"));
/// ```
pub trait DagVisualize {
    /// Export the workflow as Mermaid `flowchart TD` text.
    fn to_mermaid(&self) -> String;

    /// Export the workflow as GraphViz DOT `digraph { ... }` text.
    fn to_dot(&self) -> String;
}

impl<T: DagSource> DagVisualize for T {
    fn to_mermaid(&self) -> String {
        self.build_dag().render_mermaid()
    }

    fn to_dot(&self) -> String {
        self.build_dag().render_dot(self.dag_name())
    }
}

impl DagSource for CanvasElement {
    fn build_dag(&self) -> DagBuilder {
        let mut builder = DagBuilder::new();
        builder.visit_element(self, &[0]);
        builder
    }

    fn dag_name(&self) -> &'static str {
        "Workflow"
    }
}

impl DagSource for crate::NestedChain {
    fn build_dag(&self) -> DagBuilder {
        let mut builder = DagBuilder::new();
        builder.visit_nested_chain(&self.elements, &[0]);
        builder
    }

    fn dag_name(&self) -> &'static str {
        "NestedChain"
    }
}

impl DagSource for crate::NestedGroup {
    fn build_dag(&self) -> DagBuilder {
        let mut builder = DagBuilder::new();
        builder.visit_nested_group(&self.elements, &[0]);
        builder
    }

    fn dag_name(&self) -> &'static str {
        "NestedGroup"
    }
}

impl DagSource for Map {
    fn build_dag(&self) -> DagBuilder {
        let mut builder = DagBuilder::new();
        builder.visit_map_like(&self.task, self.argsets.len(), "map", &[0]);
        builder
    }

    fn dag_name(&self) -> &'static str {
        "Map"
    }
}

impl DagSource for Starmap {
    fn build_dag(&self) -> DagBuilder {
        let mut builder = DagBuilder::new();
        builder.visit_map_like(&self.task, self.argsets.len(), "starmap", &[0]);
        builder
    }

    fn dag_name(&self) -> &'static str {
        "Starmap"
    }
}

impl DagSource for Chunks {
    fn build_dag(&self) -> DagBuilder {
        let mut builder = DagBuilder::new();
        builder.visit_chunks(self, &[0]);
        builder
    }

    fn dag_name(&self) -> &'static str {
        "Chunks"
    }
}

impl DagSource for Branch {
    fn build_dag(&self) -> DagBuilder {
        let mut builder = DagBuilder::new();
        builder.visit_branch(self, &[0]);
        builder
    }

    fn dag_name(&self) -> &'static str {
        "Branch"
    }
}

impl DagSource for Switch {
    fn build_dag(&self) -> DagBuilder {
        let mut builder = DagBuilder::new();
        builder.visit_switch(self, &[0]);
        builder
    }

    fn dag_name(&self) -> &'static str {
        "Switch"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{NestedChain, NestedGroup};

    // -- Label escaping ----------------------------------------------------

    #[test]
    fn escape_dot_label_escapes_quotes_and_backslashes() {
        assert_eq!(escape_dot_label("a\"b"), "a\\\"b");
        assert_eq!(escape_dot_label("a\\b"), "a\\\\b");
        assert_eq!(escape_dot_label("a\nb"), "a\\nb");
        assert_eq!(escape_dot_label("plain"), "plain");
    }

    #[test]
    fn escape_mermaid_label_escapes_quotes_and_newlines() {
        assert_eq!(escape_mermaid_label("a\"b"), "a#quot;b");
        assert_eq!(escape_mermaid_label("a\nb"), "a<br/>b");
        assert_eq!(escape_mermaid_label("plain"), "plain");
    }

    // -- Flat DagExport (compatibility) ------------------------------------

    #[test]
    fn flat_chain_export_has_sequential_edges() {
        let chain = Chain::new()
            .then("alpha", vec![])
            .then("beta", vec![])
            .then("gamma", vec![]);

        let dot = chain.to_dot();
        assert!(dot.contains("digraph Chain"));
        assert!(dot.contains("rankdir=LR"));
        assert!(dot.contains("label=\"alpha\""));
        assert!(dot.contains("label=\"gamma\""));
        assert!(dot.contains("n0 -> n1"));
        assert!(dot.contains("n1 -> n2"));

        let mmd = chain.to_mermaid();
        assert!(mmd.contains("graph LR"));
        assert!(mmd.contains("n0[\"alpha\"]"));
        assert!(mmd.contains("n0 --> n1"));
        assert!(mmd.contains("n1 --> n2"));
    }

    #[test]
    fn flat_chain_export_escapes_special_characters() {
        let chain = Chain::new().then_signature(Signature::new("a\"b".to_string()));
        let dot = chain.to_dot();
        // The raw unescaped quote must not appear; the escaped form must.
        assert!(dot.contains("label=\"a\\\"b\""));
        let mmd = chain.to_mermaid();
        assert!(mmd.contains("a#quot;b"));
    }

    #[test]
    fn flat_group_export_fans_out_from_start() {
        let group = Group::new()
            .add("w1", vec![])
            .add("w2", vec![])
            .add("w3", vec![]);

        let dot = group.to_dot();
        assert!(dot.contains("digraph Group"));
        assert!(dot.contains("start -> n0"));
        assert!(dot.contains("start -> n1"));
        assert!(dot.contains("start -> n2"));

        let mmd = group.to_mermaid();
        assert!(mmd.contains("graph TB"));
        assert!(mmd.contains("start --> n0"));
    }

    #[test]
    fn flat_chord_export_fans_into_callback() {
        let header = Group::new().add("h1", vec![]).add("h2", vec![]);
        let chord = Chord::new(header, Signature::new("reduce".to_string()));

        let dot = chord.to_dot();
        assert!(dot.contains("digraph Chord"));
        assert!(dot.contains("callback [label=\"reduce\""));
        assert!(dot.contains("n0 -> callback"));
        assert!(dot.contains("n1 -> callback"));

        let mmd = chord.to_mermaid();
        assert!(mmd.contains("callback[\"reduce\"]"));
        assert!(mmd.contains("n0 --> callback"));
        assert!(mmd.contains("n1 --> callback"));
    }

    // -- Recursive DagVisualize: single chain ------------------------------

    #[test]
    fn nested_chain_single_chain_sequential() {
        let workflow = NestedChain::new()
            .then("first", vec![])
            .then("second", vec![])
            .then("third", vec![]);

        let mmd = workflow.to_mermaid();
        assert!(mmd.starts_with("flowchart TD"));
        assert!(mmd.contains("n0_0[\"first\"]"));
        assert!(mmd.contains("n0_1[\"second\"]"));
        assert!(mmd.contains("n0_2[\"third\"]"));
        assert!(mmd.contains("n0_0 --> n0_1"));
        assert!(mmd.contains("n0_1 --> n0_2"));
        // No fan-out edges (purely sequential).
        assert!(!mmd.contains("n0_0 --> n0_2"));

        let dot = workflow.to_dot();
        assert!(dot.contains("digraph NestedChain"));
        assert!(dot.contains("n0_0 [label=\"first\""));
        assert!(dot.contains("n0_0 -> n0_1;"));
        assert!(dot.contains("n0_1 -> n0_2;"));
    }

    // -- Recursive DagVisualize: group -------------------------------------

    #[test]
    fn nested_group_fans_out_from_synthetic_start() {
        let workflow = NestedGroup::new()
            .add("p1", vec![])
            .add("p2", vec![])
            .add("p3", vec![]);

        let mmd = workflow.to_mermaid();
        assert!(mmd.starts_with("flowchart TD"));
        // synthetic start (Control shape uses ([ ... ]))
        assert!(mmd.contains("n0([\"group\"])"));
        assert!(mmd.contains("n0_0[\"p1\"]"));
        assert!(mmd.contains("n0_1[\"p2\"]"));
        assert!(mmd.contains("n0_2[\"p3\"]"));
        assert!(mmd.contains("n0 --> n0_0"));
        assert!(mmd.contains("n0 --> n0_1"));
        assert!(mmd.contains("n0 --> n0_2"));

        let dot = workflow.to_dot();
        assert!(dot.contains("digraph NestedGroup"));
        assert!(dot.contains("n0 [label=\"group\", shape=ellipse]"));
        assert!(dot.contains("n0 -> n0_0;"));
        assert!(dot.contains("n0 -> n0_2;"));
    }

    // -- Recursive DagVisualize: chord -------------------------------------

    #[test]
    fn chord_element_header_fans_into_callback() {
        let element = CanvasElement::chord(
            Group::new().add("map_a", vec![]).add("map_b", vec![]),
            Signature::new("collect".to_string()),
        );

        let mmd = element.to_mermaid();
        assert!(mmd.starts_with("flowchart TD"));
        // header group start node (under index 0 of the chord)
        assert!(mmd.contains("n0_0([\"group\"])"));
        // header members
        assert!(mmd.contains("n0_0_0[\"map_a\"]"));
        assert!(mmd.contains("n0_0_1[\"map_b\"]"));
        // callback under index 1, rendered as a callback subroutine node
        assert!(mmd.contains("n0_1[[\"collect\"]]"));
        // fan-out into header members
        assert!(mmd.contains("n0_0 --> n0_0_0"));
        assert!(mmd.contains("n0_0 --> n0_0_1"));
        // header members fan into the callback
        assert!(mmd.contains("n0_0_0 --> n0_1"));
        assert!(mmd.contains("n0_0_1 --> n0_1"));

        let dot = element.to_dot();
        assert!(dot.contains("n0_1 [label=\"collect\", shape=box, style=filled"));
        assert!(dot.contains("n0_0_0 -> n0_1;"));
        assert!(dot.contains("n0_0_1 -> n0_1;"));
    }

    // -- Recursive DagVisualize: nested chain-in-group ---------------------

    #[test]
    fn nested_chain_in_group_recurses_with_unique_ids() {
        // A group of two chains running in parallel. Each chain recurses with
        // its own stable id namespace, so the two chains' nodes never collide.
        let workflow = NestedGroup::new()
            .add_chain(Chain::new().then("a1", vec![]).then("a2", vec![]))
            .add_chain(Chain::new().then("b1", vec![]).then("b2", vec![]));

        let mmd = workflow.to_mermaid();
        assert!(mmd.starts_with("flowchart TD"));
        // synthetic group start
        assert!(mmd.contains("n0([\"group\"])"));
        // first chain (index 0): nodes n0_0_0, n0_0_1
        assert!(mmd.contains("n0_0_0[\"a1\"]"));
        assert!(mmd.contains("n0_0_1[\"a2\"]"));
        assert!(mmd.contains("n0_0_0 --> n0_0_1"));
        // second chain (index 1): nodes n0_1_0, n0_1_1 (distinct namespace)
        assert!(mmd.contains("n0_1_0[\"b1\"]"));
        assert!(mmd.contains("n0_1_1[\"b2\"]"));
        assert!(mmd.contains("n0_1_0 --> n0_1_1"));
        // group fans into the *entry* (first task) of each chain only
        assert!(mmd.contains("n0 --> n0_0_0"));
        assert!(mmd.contains("n0 --> n0_1_0"));
        // it must NOT connect to the chains' second tasks
        assert!(!mmd.contains("n0 --> n0_0_1"));
        assert!(!mmd.contains("n0 --> n0_1_1"));

        let dot = workflow.to_dot();
        assert!(dot.contains("digraph NestedGroup"));
        assert!(dot.contains("n0_0_0 -> n0_0_1;"));
        assert!(dot.contains("n0_1_0 -> n0_1_1;"));
        assert!(dot.contains("n0 -> n0_0_0;"));
        assert!(dot.contains("n0 -> n0_1_0;"));
    }

    // -- Recursive DagVisualize: group-in-chain (parallel mid-chain) -------

    #[test]
    fn group_in_chain_threads_fan_out_and_join() {
        // head -> (par_a | par_b) -> tail
        let workflow = NestedChain::new()
            .then("head", vec![])
            .then_group(Group::new().add("par_a", vec![]).add("par_b", vec![]))
            .then("tail", vec![]);

        let mmd = workflow.to_mermaid();
        // head is element 0 -> n0_0
        assert!(mmd.contains("n0_0[\"head\"]"));
        // group is element 1 -> synthetic start n0_1, members n0_1_0 / n0_1_1
        assert!(mmd.contains("n0_1([\"group\"])"));
        assert!(mmd.contains("n0_1_0[\"par_a\"]"));
        assert!(mmd.contains("n0_1_1[\"par_b\"]"));
        // tail is element 2 -> n0_2
        assert!(mmd.contains("n0_2[\"tail\"]"));
        // head feeds the group's synthetic start
        assert!(mmd.contains("n0_0 --> n0_1"));
        // group fans out to members
        assert!(mmd.contains("n0_1 --> n0_1_0"));
        assert!(mmd.contains("n0_1 --> n0_1_1"));
        // BOTH group members join into tail
        assert!(mmd.contains("n0_1_0 --> n0_2"));
        assert!(mmd.contains("n0_1_1 --> n0_2"));
    }

    // -- Recursive DagVisualize: Map / Starmap / Chunks --------------------

    #[test]
    fn map_export_fans_out_one_node_per_argset() {
        let map = Map::new(
            Signature::new("process".to_string()),
            vec![
                vec![serde_json::json!(1)],
                vec![serde_json::json!(2)],
                vec![serde_json::json!(3)],
            ],
        );

        let mmd = map.to_mermaid();
        assert!(mmd.contains("n0([\"map\"])"));
        assert!(mmd.contains("n0_0[\"process[0]\"]"));
        assert!(mmd.contains("n0_1[\"process[1]\"]"));
        assert!(mmd.contains("n0_2[\"process[2]\"]"));
        assert!(mmd.contains("n0 --> n0_0"));
        assert!(mmd.contains("n0 --> n0_2"));

        let dot = map.to_dot();
        assert!(dot.contains("digraph Map"));
    }

    #[test]
    fn starmap_export_labels_starmap() {
        let starmap = Starmap::new(
            Signature::new("zip".to_string()),
            vec![vec![serde_json::json!(1), serde_json::json!(2)]],
        );
        let mmd = starmap.to_mermaid();
        assert!(mmd.contains("n0([\"starmap\"])"));
        assert!(mmd.contains("n0_0[\"zip[0]\"]"));
    }

    #[test]
    fn chunks_export_one_node_per_chunk() {
        let items: Vec<serde_json::Value> = (0..10).map(|i| serde_json::json!(i)).collect();
        let chunks = Chunks::new(Signature::new("batch".to_string()), items, 4);
        assert_eq!(chunks.num_chunks(), 3);

        let mmd = chunks.to_mermaid();
        assert!(mmd.contains("n0([\"chunks\"])"));
        assert!(mmd.contains("n0_0[\"batch[chunk 0]\"]"));
        assert!(mmd.contains("n0_1[\"batch[chunk 1]\"]"));
        assert!(mmd.contains("n0_2[\"batch[chunk 2]\"]"));
        assert!(mmd.contains("n0 --> n0_2"));

        let dot = chunks.to_dot();
        assert!(dot.contains("digraph Chunks"));
        assert!(dot.contains("n0 -> n0_2;"));
    }

    // -- Recursive DagVisualize: Branch / Switch ---------------------------

    #[test]
    fn branch_export_has_labelled_true_false_edges() {
        let branch = Branch::new(
            Condition::field_greater_than("count", 100.0),
            Signature::new("big".to_string()),
        )
        .otherwise(Signature::new("small".to_string()));

        let mmd = branch.to_mermaid();
        assert!(mmd.contains("n0_0[\"big\"]"));
        assert!(mmd.contains("n0_1[\"small\"]"));
        assert!(mmd.contains("n0 -->|true| n0_0"));
        assert!(mmd.contains("n0 -->|false| n0_1"));

        let dot = branch.to_dot();
        assert!(dot.contains("digraph Branch"));
        assert!(dot.contains("n0 -> n0_0 [label=\"true\"]"));
        assert!(dot.contains("n0 -> n0_1 [label=\"false\"]"));
    }

    #[test]
    fn switch_export_has_case_and_default_edges() {
        let switch = Switch::new()
            .case(
                Condition::field_equals("status", serde_json::json!("a")),
                Signature::new("do_a".to_string()),
            )
            .case(
                Condition::field_equals("status", serde_json::json!("b")),
                Signature::new("do_b".to_string()),
            )
            .default(Signature::new("do_default".to_string()));

        let mmd = switch.to_mermaid();
        assert!(mmd.contains("n0([\"switch\"])"));
        assert!(mmd.contains("n0_0[\"do_a\"]"));
        assert!(mmd.contains("n0_1[\"do_b\"]"));
        // default lives at index == cases.len() == 2
        assert!(mmd.contains("n0_2[\"do_default\"]"));
        assert!(mmd.contains("n0 -->|default| n0_2"));

        let dot = switch.to_dot();
        assert!(dot.contains("digraph Switch"));
        assert!(dot.contains("n0 -> n0_2 [label=\"default\"]"));
    }

    // -- Determinism -------------------------------------------------------

    #[test]
    fn output_is_deterministic_across_calls() {
        let workflow = NestedChain::new()
            .then("head", vec![])
            .then_group(Group::new().add("a", vec![]).add("b", vec![]))
            .then_chord(
                Group::new().add("c", vec![]).add("d", vec![]),
                Signature::new("e".to_string()),
            );

        let first_mmd = workflow.to_mermaid();
        let second_mmd = workflow.to_mermaid();
        assert_eq!(
            first_mmd, second_mmd,
            "Mermaid output must be deterministic"
        );

        let first_dot = workflow.to_dot();
        let second_dot = workflow.to_dot();
        assert_eq!(first_dot, second_dot, "DOT output must be deterministic");
    }

    // -- Label escaping inside recursive exporter --------------------------

    #[test]
    fn recursive_exporter_escapes_labels() {
        let workflow =
            NestedChain::new().then_signature(Signature::new("weird\"name\nhere".to_string()));
        let mmd = workflow.to_mermaid();
        assert!(mmd.contains("weird#quot;name<br/>here"));
        let dot = workflow.to_dot();
        assert!(dot.contains("weird\\\"name\\nhere"));
    }

    // -- Empty containers stay valid ---------------------------------------

    #[test]
    fn empty_nested_chain_produces_placeholder() {
        let workflow = NestedChain::new();
        let mmd = workflow.to_mermaid();
        assert!(mmd.starts_with("flowchart TD"));
        assert!(mmd.contains("empty chain"));
        let dot = workflow.to_dot();
        assert!(dot.contains("digraph NestedChain"));
    }
}
