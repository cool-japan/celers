//! Task dependency graph visualization for `celers deps`.
//!
//! This module renders a [`celers_core::TaskDag`] in three ways:
//!
//! - [`render_ascii`]: an indented ASCII tree (box-drawing connectors),
//!   walking top-down from [`TaskDag::get_roots`] through
//!   [`TaskDag::get_dependents`].
//! - [`render_dot`]: a GraphViz DOT digraph, one node declaration per task
//!   and one edge per `(task, dependent)` pair.
//! - [`run_interactive`]: a lightweight REPL-style navigator over the same
//!   graph, following the stdout-refresh pattern already used by
//!   `commands::metrics_cmds::run_monitor` (clear screen, redraw, read a
//!   line) rather than a full TUI framework.
//!
//! # Where the graph comes from
//!
//! `TaskDag` has no accessor to enumerate every node it holds (`nodes` is a
//! private field; the public surface is `get_roots`/`get_leaves`/
//! `get_dependencies`/`get_dependents`/`get_node`/`topological_sort`/...).
//! There is also no existing "build a `TaskDag` from a live queue" code
//! path. Rather than wiring new live-broker calls (out of scope here),
//! [`run_deps`] reads an already-exported task list from a `--from <file>`
//! JSON file -- either the object shape written by
//! `commands::export_queue` or a bare JSON array of tasks -- and
//! [`dag_from_tasks`] builds the graph from each task's declared
//! [`celers_core::TaskMetadata::dependencies`].
//!
//! # Pure logic vs. I/O
//!
//! Every graph-walking algorithm here ([`render_ascii_with_limits`],
//! [`render_dot_with_limits`], [`dag_from_tasks`], [`InteractiveSession`],
//! [`render_node_view`]) is a pure function/method over a [`TaskDag`] (or
//! plain data), so it is fully unit-testable without any filesystem or
//! terminal access. Only [`load_tasks_from_file`], [`run_interactive`], and
//! the top-level [`run_deps`] touch I/O, and they are kept intentionally
//! thin.
//!
//! # Large-graph safeguard
//!
//! [`render_ascii`]/[`render_dot`] cap both recursion depth
//! ([`DEFAULT_MAX_DEPTH`]) and the total number of rendered nodes
//! ([`DEFAULT_MAX_RENDERED_NODES`]), printing a truncation notice instead of
//! walking arbitrarily large or pathological input. A node reachable via
//! more than one path (a diamond, not just a tree) is only fully expanded
//! the first time it is encountered; later encounters print a single
//! `(see above)` reference line. A defensive ancestor check also stops
//! immediately with a `(cycle detected)` marker if a node reappears on its
//! own current path, so this module can never loop forever even if handed a
//! corrupted/cyclic `TaskDag` -- see [`dag_from_tasks`]'s documentation for
//! why that residual case can arise at all despite `TaskDag` nominally being
//! acyclic-by-construction.

use std::collections::{HashSet, VecDeque};
use std::fmt::Write as _;
use std::path::Path;

use celers_core::{SerializedTask, TaskDag, TaskId};
use colored::Colorize;

/// Default recursion-depth cap for [`render_ascii`]/[`render_dot`] (the root
/// is depth 0). Bounds output size and guarantees termination on
/// pathological input (e.g. an accidental multi-thousand-node dependency
/// chain) without requiring callers to pass an explicit limit.
pub const DEFAULT_MAX_DEPTH: usize = 20;

/// Default cap on the total number of node lines/declarations emitted
/// before a render is truncated with a notice. Bounds output size (and
/// render time) on very large or wide graphs.
pub const DEFAULT_MAX_RENDERED_NODES: usize = 500;

/// First 8 hex characters of a task id's canonical string form: a short,
/// human-scannable disambiguator next to a task's name (the same idea as a
/// short git commit hash). Always a valid char-boundary slice since a
/// UUID's canonical form is pure ASCII.
#[must_use]
pub fn short_id(id: TaskId) -> String {
    id.to_string()[..8].to_string()
}

/// Render a task's display label as `"name (short_id)"`, falling back to a
/// placeholder if `id` is not present in `dag` (defensive; should not
/// normally happen since callers only pass ids obtained from `dag` itself).
#[must_use]
pub fn node_label(dag: &TaskDag, id: TaskId) -> String {
    match dag.get_node(&id) {
        Some(node) => format!("{} ({})", node.task_name, short_id(id)),
        None => format!("<unknown> ({})", short_id(id)),
    }
}

/// Sort a list of task ids by their display label (name, then id) so
/// rendering is deterministic regardless of `TaskDag`'s internal `HashSet`
/// iteration order.
fn sort_by_label(dag: &TaskDag, ids: &mut [TaskId]) {
    ids.sort_by(|a, b| {
        let label_a = dag
            .get_node(a)
            .map(|n| n.task_name.as_str())
            .unwrap_or_default();
        let label_b = dag
            .get_node(b)
            .map(|n| n.task_name.as_str())
            .unwrap_or_default();
        label_a.cmp(label_b).then_with(|| a.cmp(b))
    });
}

// ---------------------------------------------------------------------
// ASCII tree rendering
// ---------------------------------------------------------------------

/// Render `dag` as an indented ASCII tree, walking from [`TaskDag::get_roots`]
/// down through [`TaskDag::get_dependents`] (a top-down "what runs after
/// this" view).
///
/// Uses [`DEFAULT_MAX_DEPTH`]/[`DEFAULT_MAX_RENDERED_NODES`]; see
/// [`render_ascii_with_limits`] to override either cap.
#[must_use]
pub fn render_ascii(dag: &TaskDag) -> String {
    render_ascii_with_limits(dag, DEFAULT_MAX_DEPTH, DEFAULT_MAX_RENDERED_NODES)
}

/// Like [`render_ascii`] but with explicit depth/node-count caps.
///
/// A node reachable from more than one path (a diamond, not just a tree) is
/// fully expanded only the first time it is encountered; later encounters
/// print a single `(see above)` reference line instead of re-expanding its
/// subtree, keeping output bounded even when a node has many ancestors. A
/// defensive ancestor check also stops immediately (`(cycle detected)`) if
/// the same node reappears on its own current path, so a corrupted/cyclic
/// `TaskDag` can never cause this function to loop forever -- acyclic input
/// (e.g. anything built by [`dag_from_tasks`]) never triggers that branch.
#[must_use]
pub fn render_ascii_with_limits(dag: &TaskDag, max_depth: usize, max_nodes: usize) -> String {
    if dag.is_empty() {
        return "(empty dependency graph)\n".to_string();
    }

    let mut roots = dag.get_roots();
    sort_by_label(dag, &mut roots);

    let mut walk = AsciiWalk::new(max_depth, max_nodes);
    let root_count = roots.len();
    for (idx, root) in roots.into_iter().enumerate() {
        if walk.hit_node_cap {
            break;
        }
        walk.visit(dag, root, "", true, 0);
        if idx + 1 < root_count && !walk.hit_node_cap {
            walk.out.push('\n');
        }
    }

    if walk.hit_depth_cap {
        let _ = writeln!(walk.out, "... (truncated: exceeded max depth {max_depth})");
    }
    if walk.hit_node_cap {
        let _ = writeln!(
            walk.out,
            "... (truncated: exceeded max node count {max_nodes})"
        );
    }

    walk.out
}

/// Mutable state threaded through a single [`render_ascii_with_limits`]
/// call: the accumulated output, the depth/node caps, which nodes have
/// already had their subtree fully expanded, and the current DFS ancestor
/// path (used only to detect a cycle, which the public `TaskDag` API cannot
/// normally produce -- see the module docs).
struct AsciiWalk {
    max_depth: usize,
    max_nodes: usize,
    rendered: usize,
    expanded: HashSet<TaskId>,
    ancestors: Vec<TaskId>,
    hit_node_cap: bool,
    hit_depth_cap: bool,
    out: String,
}

impl AsciiWalk {
    fn new(max_depth: usize, max_nodes: usize) -> Self {
        Self {
            max_depth,
            max_nodes,
            rendered: 0,
            expanded: HashSet::new(),
            ancestors: Vec::new(),
            hit_node_cap: false,
            hit_depth_cap: false,
            out: String::new(),
        }
    }

    fn bump_rendered(&mut self) {
        self.rendered += 1;
        if self.rendered >= self.max_nodes {
            self.hit_node_cap = true;
        }
    }

    fn visit(&mut self, dag: &TaskDag, node_id: TaskId, prefix: &str, is_last: bool, depth: usize) {
        if self.hit_node_cap {
            return;
        }

        let label = node_label(dag, node_id);
        let connector = if depth == 0 {
            ""
        } else if is_last {
            "└── "
        } else {
            "├── "
        };

        if self.ancestors.contains(&node_id) {
            let _ = writeln!(self.out, "{prefix}{connector}{label} (cycle detected)");
            self.bump_rendered();
            return;
        }

        if self.expanded.contains(&node_id) {
            let _ = writeln!(self.out, "{prefix}{connector}{label} (see above)");
            self.bump_rendered();
            return;
        }

        let _ = writeln!(self.out, "{prefix}{connector}{label}");
        self.expanded.insert(node_id);
        self.bump_rendered();
        if self.hit_node_cap {
            return;
        }

        let mut children = dag.get_dependents(&node_id).unwrap_or_default();
        if children.is_empty() {
            return;
        }
        if depth >= self.max_depth {
            self.hit_depth_cap = true;
            return;
        }
        sort_by_label(dag, &mut children);

        let child_prefix = if depth == 0 {
            String::new()
        } else if is_last {
            format!("{prefix}    ")
        } else {
            format!("{prefix}│   ")
        };

        self.ancestors.push(node_id);
        let child_count = children.len();
        for (i, child) in children.into_iter().enumerate() {
            if self.hit_node_cap {
                break;
            }
            let child_is_last = i + 1 == child_count;
            self.visit(dag, child, &child_prefix, child_is_last, depth + 1);
        }
        self.ancestors.pop();
    }
}

// ---------------------------------------------------------------------
// GraphViz DOT rendering
// ---------------------------------------------------------------------

/// Escape a string for safe embedding inside a double-quoted GraphViz DOT
/// identifier/label (backslash, double-quote, and embedded newlines). Task
/// names are arbitrary user-supplied text, so this avoids emitting
/// malformed (or, worse, semantically-altered) DOT.
#[must_use]
pub fn dot_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            other => out.push(other),
        }
    }
    out
}

/// Which adjacency [`neighbors`] follows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Direction {
    /// Follow `get_dependents` (top-down: "what runs after this").
    Forward,
    /// Follow `get_dependencies` (bottom-up: "what this needs first").
    Backward,
}

fn neighbors(dag: &TaskDag, id: TaskId, direction: Direction) -> Vec<TaskId> {
    match direction {
        Direction::Forward => dag.get_dependents(&id).unwrap_or_default(),
        Direction::Backward => dag.get_dependencies(&id).unwrap_or_default(),
    }
}

/// Breadth-first walk used by [`render_dot_with_limits`], bounded by the
/// same depth/node caps as [`AsciiWalk`]. A node is marked visited *before*
/// it is queued, so a cycle can never cause this to loop forever (each node
/// enters the queue at most once, ever) -- no separate cycle guard is
/// needed here the way [`AsciiWalk`] needs one for its repeat-visit
/// annotations.
struct BoundedWalk<'a> {
    dag: &'a TaskDag,
    max_depth: usize,
    max_nodes: usize,
    visited: HashSet<TaskId>,
    order: Vec<TaskId>,
    truncated: bool,
}

impl<'a> BoundedWalk<'a> {
    fn new(dag: &'a TaskDag, max_depth: usize, max_nodes: usize) -> Self {
        Self {
            dag,
            max_depth,
            max_nodes,
            visited: HashSet::new(),
            order: Vec::new(),
            truncated: false,
        }
    }

    fn run(&mut self, starts: &[TaskId], direction: Direction) {
        let mut queue: VecDeque<(TaskId, usize)> = VecDeque::new();

        for id in starts {
            if self.visited.contains(id) {
                continue;
            }
            if self.visited.len() >= self.max_nodes {
                self.truncated = true;
                return;
            }
            self.visited.insert(*id);
            self.order.push(*id);
            queue.push_back((*id, 0));
        }

        while let Some((id, depth)) = queue.pop_front() {
            if depth >= self.max_depth {
                if !neighbors(self.dag, id, direction).is_empty() {
                    self.truncated = true;
                }
                continue;
            }

            let mut next = neighbors(self.dag, id, direction);
            sort_by_label(self.dag, &mut next);

            for child in next {
                if self.visited.contains(&child) {
                    continue;
                }
                if self.visited.len() >= self.max_nodes {
                    self.truncated = true;
                    return;
                }
                self.visited.insert(child);
                self.order.push(child);
                queue.push_back((child, depth + 1));
            }
        }
    }
}

/// Render `dag` as a GraphViz DOT digraph: one node declaration per task
/// and one edge per `(task, dependent)` pair, walking from
/// [`TaskDag::get_roots`] through [`TaskDag::get_dependents`].
///
/// Uses [`DEFAULT_MAX_DEPTH`]/[`DEFAULT_MAX_RENDERED_NODES`]; see
/// [`render_dot_with_limits`] to override either cap.
#[must_use]
pub fn render_dot(dag: &TaskDag) -> String {
    render_dot_with_limits(dag, DEFAULT_MAX_DEPTH, DEFAULT_MAX_RENDERED_NODES)
}

/// Like [`render_dot`] but with explicit depth/node-count caps.
///
/// Node ids are keyed by the task's full UUID (guaranteed unique); the
/// human-readable `name (short_id)` label from [`node_label`] is attached
/// via a `label` attribute. A first pass walks forward from
/// [`TaskDag::get_roots`]; a second, defensive pass walks backward from
/// [`TaskDag::get_leaves`] to pick up any node a forward-only walk could
/// miss on a hand-built/corrupted `TaskDag` (graphs produced by
/// [`dag_from_tasks`] are always acyclic and are fully covered by the first
/// pass alone, so the second pass is a no-op for them).
#[must_use]
pub fn render_dot_with_limits(dag: &TaskDag, max_depth: usize, max_nodes: usize) -> String {
    let mut walk = BoundedWalk::new(dag, max_depth, max_nodes);

    let mut roots = dag.get_roots();
    sort_by_label(dag, &mut roots);
    walk.run(&roots, Direction::Forward);

    if !walk.truncated {
        let mut leaves = dag.get_leaves();
        sort_by_label(dag, &mut leaves);
        walk.run(&leaves, Direction::Backward);
    }

    let BoundedWalk {
        visited,
        order,
        truncated,
        ..
    } = walk;

    let mut out = String::new();
    out.push_str("digraph celers_deps {\n");
    out.push_str("    rankdir=LR;\n");

    for id in &order {
        let _ = writeln!(
            out,
            "    \"{}\" [label=\"{}\"];",
            dot_escape(&id.to_string()),
            dot_escape(&node_label(dag, *id))
        );
    }

    let mut emitted: HashSet<(TaskId, TaskId)> = HashSet::new();
    for id in &order {
        let mut dependents = dag.get_dependents(id).unwrap_or_default();
        sort_by_label(dag, &mut dependents);
        for dependent in dependents {
            if !visited.contains(&dependent) {
                continue; // capped out before this edge's target was reached
            }
            if emitted.insert((*id, dependent)) {
                let _ = writeln!(
                    out,
                    "    \"{}\" -> \"{}\";",
                    dot_escape(&id.to_string()),
                    dot_escape(&dependent.to_string())
                );
            }
        }
    }

    if truncated {
        let _ = writeln!(
            out,
            "    // truncated: exceeded max depth {max_depth} or max node count {max_nodes}"
        );
    }

    out.push_str("}\n");
    out
}

// ---------------------------------------------------------------------
// Building a TaskDag from exported task data
// ---------------------------------------------------------------------

/// Build a [`TaskDag`] from a flat collection of tasks, wiring up
/// dependency edges from each task's
/// [`celers_core::TaskMetadata::dependencies`].
///
/// Every task in `tasks` is always added as a node. Two situations are
/// handled without failing the whole graph:
///
/// - A dependency id not present in `tasks` (e.g. a queue export only
///   captures a subset of the full workload) is skipped, as is a
///   self-referential dependency.
/// - A dependency that would introduce a cycle is rejected. Note that
///   [`TaskDag::add_dependency`] mutates its internal adjacency *before*
///   running its own cycle check, so a rejected edge is left partially
///   applied by that call; this function immediately reverses that partial
///   mutation via [`TaskDag::remove_dependency`] so the graph returned here
///   is always genuinely acyclic (and therefore always fully reachable from
///   [`TaskDag::get_roots`] by [`render_ascii`]/[`render_dot`]).
#[must_use]
pub fn dag_from_tasks(tasks: &[SerializedTask]) -> TaskDag {
    let mut dag = TaskDag::new();
    for task in tasks {
        dag.add_node(task.metadata.id, task.metadata.name.clone());
    }
    for task in tasks {
        for dependency_id in &task.metadata.dependencies {
            if *dependency_id == task.metadata.id {
                continue; // ignore a self-referential dependency
            }
            if dag.get_node(dependency_id).is_none() {
                continue; // dependency outside this snapshot
            }
            if dag
                .add_dependency(task.metadata.id, *dependency_id)
                .is_err()
            {
                // Would have closed a cycle; undo the partial mutation left
                // behind by the rejected call (see doc comment above).
                dag.remove_dependency(task.metadata.id, *dependency_id);
            }
        }
    }
    dag
}

/// Minimal on-disk shape this command understands for `--from <file>`: the
/// object produced by `commands::export_queue` (only the `tasks` field is
/// required; other fields such as `queue_name`/`queue_type`/`exported_at`
/// are accepted and ignored, so this stays compatible with that JSON format
/// without depending on its private struct definition in `commands::queue`).
#[derive(Debug, Clone, serde::Deserialize)]
struct QueueExportFile {
    tasks: Vec<SerializedTask>,
}

/// Parse `--from` file contents into a task list.
///
/// Accepts either the object shape written by `commands::export_queue`
/// (`{"tasks": [...], ...}`) or a bare JSON array of tasks -- handy for
/// small hand-written fixtures. Pure function over the file's contents, so
/// it is fully unit-testable without any filesystem access.
///
/// # Errors
/// Returns an error if `content` matches neither shape.
pub fn parse_tasks_json(content: &str) -> anyhow::Result<Vec<SerializedTask>> {
    match serde_json::from_str::<QueueExportFile>(content) {
        Ok(export) => Ok(export.tasks),
        Err(object_err) => serde_json::from_str::<Vec<SerializedTask>>(content).map_err(|array_err| {
            anyhow::anyhow!(
                "failed to parse task JSON: not a queue-export object ({object_err}) or a bare task array ({array_err})"
            )
        }),
    }
}

/// Read and parse a `--from <file>` argument (see [`parse_tasks_json`] for
/// the accepted shapes).
///
/// # Errors
/// Returns an error if the file cannot be read, or its contents cannot be
/// parsed by [`parse_tasks_json`].
pub fn load_tasks_from_file(path: &Path) -> anyhow::Result<Vec<SerializedTask>> {
    let content = std::fs::read_to_string(path)
        .map_err(|e| anyhow::anyhow!("failed to read '{}': {e}", path.display()))?;
    parse_tasks_json(&content)
}

// ---------------------------------------------------------------------
// Interactive navigation
// ---------------------------------------------------------------------

/// Outcome of applying one interactive-navigation command via
/// [`InteractiveSession::apply`]. Kept separate from the mutation itself so
/// the thin I/O loop in [`run_interactive`] can decide whether to redraw,
/// show a message, or exit without re-deriving that decision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NavOutcome {
    /// Navigation succeeded; redraw the (new) current node.
    Moved,
    /// The session should exit the interactive loop.
    Quit,
    /// The command could not be applied (unknown task id, empty history,
    /// unrecognized input, ...); the message should be shown to the user
    /// and the current node is unchanged.
    Message(String),
}

/// Interactive navigation session: the current node plus browser-style
/// back/forward history stacks. All navigation logic lives here as pure,
/// synchronous state transitions, so it is fully unit-testable without any
/// stdin/stdout; [`run_interactive`] is the only piece that touches I/O.
#[derive(Debug, Clone)]
pub struct InteractiveSession {
    current: TaskId,
    back: Vec<TaskId>,
    forward: Vec<TaskId>,
}

impl InteractiveSession {
    /// Start a new session positioned at `start`.
    #[must_use]
    pub fn new(start: TaskId) -> Self {
        Self {
            current: start,
            back: Vec::new(),
            forward: Vec::new(),
        }
    }

    /// The task the session is currently positioned at.
    #[must_use]
    pub fn current(&self) -> TaskId {
        self.current
    }

    /// Parse and apply one line of user input against `dag`.
    ///
    /// Recognizes (case-insensitively, surrounding whitespace trimmed):
    /// `quit`/`exit`/`q` to end the session; `up`/`back`/`u`/`b` to return
    /// to the previous node; `down`/`forward`/`d`/`f` to re-descend after an
    /// `up`; or any other non-empty token, parsed as a task id to jump
    /// directly to (navigating to a fresh id clears the forward stack,
    /// matching ordinary browser-history semantics).
    pub fn apply(&mut self, dag: &TaskDag, input: &str) -> NavOutcome {
        let cmd = input.trim();
        if cmd.is_empty() {
            return NavOutcome::Message("enter a task id, 'up', 'down', or 'quit'".to_string());
        }

        match cmd.to_ascii_lowercase().as_str() {
            "quit" | "exit" | "q" => NavOutcome::Quit,
            "up" | "back" | "u" | "b" => match self.back.pop() {
                Some(previous) => {
                    self.forward.push(self.current);
                    self.current = previous;
                    NavOutcome::Moved
                }
                None => NavOutcome::Message("already at the start of history".to_string()),
            },
            "down" | "forward" | "d" | "f" => match self.forward.pop() {
                Some(next) => {
                    self.back.push(self.current);
                    self.current = next;
                    NavOutcome::Moved
                }
                None => NavOutcome::Message("no forward history".to_string()),
            },
            other => match other.parse::<TaskId>() {
                Ok(target) if dag.get_node(&target).is_some() => {
                    self.back.push(self.current);
                    self.forward.clear();
                    self.current = target;
                    NavOutcome::Moved
                }
                Ok(_) => NavOutcome::Message("no task with that id in this graph".to_string()),
                Err(_) => NavOutcome::Message(format!(
                    "unrecognized command '{cmd}' (expected a task id, 'up', 'down', or 'quit')"
                )),
            },
        }
    }
}

/// Render the interactive navigator's view of `current`: its name/id,
/// dependencies, and dependents. Pure function over the model, shared by
/// [`run_interactive`] (which prints it) and the test suite (which asserts
/// on its content directly).
#[must_use]
pub fn render_node_view(dag: &TaskDag, current: TaskId) -> String {
    let mut out = String::new();
    let Some(node) = dag.get_node(&current) else {
        let _ = writeln!(out, "(unknown task {current})");
        return out;
    };

    let _ = writeln!(
        out,
        "{}",
        "=== Task Dependency Navigator ===".bold().green()
    );
    let _ = writeln!(out, "Current: {} ({current})", node.task_name.cyan());
    out.push('\n');

    let mut dependencies = dag.get_dependencies(&current).unwrap_or_default();
    sort_by_label(dag, &mut dependencies);
    if dependencies.is_empty() {
        let _ = writeln!(out, "Dependencies: (none -- this is a root)");
    } else {
        let _ = writeln!(out, "Dependencies ({}):", dependencies.len());
        for id in &dependencies {
            let _ = writeln!(out, "  {}", node_label(dag, *id));
        }
    }
    out.push('\n');

    let mut dependents = dag.get_dependents(&current).unwrap_or_default();
    sort_by_label(dag, &mut dependents);
    if dependents.is_empty() {
        let _ = writeln!(out, "Dependents: (none -- this is a leaf)");
    } else {
        let _ = writeln!(out, "Dependents ({}):", dependents.len());
        for id in &dependents {
            let _ = writeln!(out, "  {}", node_label(dag, *id));
        }
    }
    out.push('\n');
    let _ = writeln!(
        out,
        "{}",
        "Enter a task id to jump to it, 'up' for back, 'down' for forward, 'quit' to exit."
            .dimmed()
    );
    out
}

/// Run the interactive navigator against `dag`, starting at `start`, until
/// the user quits or stdin reaches EOF.
///
/// This is intentionally thin: all state transitions delegate to
/// [`InteractiveSession::apply`] and all rendering to [`render_node_view`],
/// so the only things this function itself does are the actual I/O
/// (clear-and-redraw the screen, read one line, loop) -- following the same
/// stdout-refresh pattern already used by `commands::metrics_cmds::run_monitor`
/// and `commands::monitoring::show_metrics`'s watch mode.
///
/// # Errors
/// Returns an error if `start` is not present in `dag`, or if a terminal
/// I/O operation fails.
pub fn run_interactive(dag: &TaskDag, start: TaskId) -> anyhow::Result<()> {
    if dag.get_node(&start).is_none() {
        anyhow::bail!("start task {start} not found in graph");
    }

    let mut session = InteractiveSession::new(start);
    let mut pending_message: Option<String> = None;

    loop {
        // Clear screen between redraws (same escape sequence used by the
        // existing watch-mode views in metrics_cmds/monitoring).
        print!("\x1B[2J\x1B[1;1H");
        print!("{}", render_node_view(dag, session.current()));
        if let Some(message) = pending_message.take() {
            println!("{}", message.yellow());
        }
        print!("> ");
        {
            use std::io::Write as _;
            std::io::stdout().flush()?;
        }

        let mut line = String::new();
        let bytes_read = std::io::stdin().read_line(&mut line)?;
        if bytes_read == 0 {
            // EOF (e.g. piped/closed stdin): exit cleanly rather than
            // looping forever on an input source that will never advance.
            break;
        }

        match session.apply(dag, &line) {
            NavOutcome::Quit => break,
            NavOutcome::Moved => {}
            NavOutcome::Message(message) => pending_message = Some(message),
        }
    }

    Ok(())
}

// ---------------------------------------------------------------------
// Top-level `celers deps` entry point
// ---------------------------------------------------------------------

/// Pick the interactive navigator's starting node: the explicitly requested
/// `start` id if given (validated against `dag`), otherwise the
/// label-sorted first root.
///
/// # Errors
/// Returns an error if `start` is not a valid task id or does not name a
/// task present in `dag`, or if `dag` has no discoverable starting node at
/// all (only reachable via a corrupted/cyclic `TaskDag`, which
/// [`dag_from_tasks`] never produces).
fn resolve_start_id(dag: &TaskDag, start: Option<&str>) -> anyhow::Result<TaskId> {
    if let Some(raw) = start {
        let id = raw
            .parse::<TaskId>()
            .map_err(|_| anyhow::anyhow!("invalid task id '{raw}'"))?;
        if dag.get_node(&id).is_none() {
            anyhow::bail!("task '{raw}' not found in graph");
        }
        return Ok(id);
    }

    let mut roots = dag.get_roots();
    sort_by_label(dag, &mut roots);
    if let Some(root) = roots.into_iter().next() {
        return Ok(root);
    }

    // Degenerate case: no root at all. Fall back to a leaf so there is
    // still something sensible to start from.
    let mut leaves = dag.get_leaves();
    sort_by_label(dag, &mut leaves);
    leaves
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("graph has no tasks to start from"))
}

/// Entry point for the `celers deps` command: load tasks exported via
/// `commands::export_queue` (or a bare task array) from `from_file`, build
/// the dependency graph, and either render it statically (`format`:
/// `"ascii"` or `"dot"`) or launch the interactive navigator.
///
/// This function only performs local file I/O; it never talks to a broker
/// (wiring a live-queue data source is separate, later work).
///
/// # Errors
/// Returns an error if `from_file` cannot be read/parsed, if `format` is
/// neither `"ascii"` nor `"dot"`, or (interactive mode) if `start` names a
/// task that is not present in the graph.
pub fn run_deps(
    from_file: &Path,
    format: &str,
    interactive: bool,
    start: Option<&str>,
) -> anyhow::Result<()> {
    let tasks = load_tasks_from_file(from_file)?;
    if tasks.is_empty() {
        println!("{}", "No tasks found in the provided file.".yellow());
        return Ok(());
    }

    let dag = dag_from_tasks(&tasks);
    println!(
        "{}",
        format!(
            "Loaded {} task(s), {} dependency edge(s) from '{}'",
            dag.node_count(),
            dag.edge_count(),
            from_file.display()
        )
        .dimmed()
    );

    if interactive {
        let start_id = resolve_start_id(&dag, start)?;
        return run_interactive(&dag, start_id);
    }

    match format.to_ascii_lowercase().as_str() {
        "ascii" | "tree" => print!("{}", render_ascii(&dag)),
        "dot" | "graphviz" => print!("{}", render_dot(&dag)),
        other => anyhow::bail!("unknown --format '{other}' (expected 'ascii' or 'dot')"),
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task_with_deps(id: TaskId, name: &str, deps: &[TaskId]) -> SerializedTask {
        let mut task = SerializedTask::new(name.to_string(), Vec::new());
        task.metadata.id = id;
        task.metadata.dependencies = deps.iter().copied().collect();
        task
    }

    /// A -> B, A -> C, B -> D, C -> D (a diamond): B and C both depend on A;
    /// D depends on both B and C.
    fn diamond_dag() -> (TaskDag, TaskId, TaskId, TaskId, TaskId) {
        let a = TaskId::from_u128(0xAAAA_AAAA_0000_0000_0000_0000_0000_0000);
        let b = TaskId::from_u128(0xBBBB_BBBB_0000_0000_0000_0000_0000_0000);
        let c = TaskId::from_u128(0xCCCC_CCCC_0000_0000_0000_0000_0000_0000);
        let d = TaskId::from_u128(0xDDDD_DDDD_0000_0000_0000_0000_0000_0000);

        let mut dag = TaskDag::new();
        dag.add_node(a, "A");
        dag.add_node(b, "B");
        dag.add_node(c, "C");
        dag.add_node(d, "D");
        assert!(dag.add_dependency(b, a).is_ok());
        assert!(dag.add_dependency(c, a).is_ok());
        assert!(dag.add_dependency(d, b).is_ok());
        assert!(dag.add_dependency(d, c).is_ok());

        (dag, a, b, c, d)
    }

    // -- render_ascii -----------------------------------------------------

    #[test]
    fn render_ascii_renders_diamond_deterministically() {
        let (dag, a, b, c, d) = diamond_dag();
        let a_label = node_label(&dag, a);
        let b_label = node_label(&dag, b);
        let c_label = node_label(&dag, c);
        let d_label = node_label(&dag, d);

        let expected = format!(
            "{a_label}\n├── {b_label}\n│   └── {d_label}\n└── {c_label}\n    └── {d_label} (see above)\n"
        );

        let rendered = render_ascii(&dag);
        assert_eq!(rendered, expected);
        assert_eq!(render_ascii(&dag), rendered, "must be deterministic");
    }

    #[test]
    fn render_ascii_handles_empty_dag() {
        let dag = TaskDag::new();
        assert_eq!(render_ascii(&dag), "(empty dependency graph)\n");
    }

    #[test]
    fn render_ascii_and_dot_include_isolated_node() {
        let mut dag = TaskDag::new();
        let id = TaskId::from_u128(0x1234_0000_0000_0000_0000_0000_0000_0000);
        dag.add_node(id, "solo");

        let ascii = render_ascii(&dag);
        assert!(ascii.contains("solo"));

        let dot = render_dot(&dag);
        assert!(dot.contains(&id.to_string()));
        assert!(dot.contains("solo"));
    }

    #[test]
    fn render_ascii_large_chain_truncates_at_depth_cap_without_hanging() {
        const CHAIN_LEN: usize = 500;
        let mut dag = TaskDag::new();
        let ids: Vec<TaskId> = (0..CHAIN_LEN).map(|_| TaskId::new_v4()).collect();
        for (i, id) in ids.iter().enumerate() {
            dag.add_node(*id, format!("task_{i}"));
        }
        for i in 1..ids.len() {
            assert!(dag.add_dependency(ids[i], ids[i - 1]).is_ok());
        }

        let rendered =
            render_ascii_with_limits(&dag, DEFAULT_MAX_DEPTH, DEFAULT_MAX_RENDERED_NODES);
        assert!(rendered.contains("truncated: exceeded max depth"));
        // Depth-capped well below the full chain length (root + max_depth
        // levels + a truncation notice line).
        assert!(rendered.lines().count() <= DEFAULT_MAX_DEPTH + 5);
    }

    #[test]
    fn render_ascii_wide_fanout_truncates_at_node_cap_without_hanging() {
        // Only needs to clear `DEFAULT_MAX_RENDERED_NODES` by a small, safe
        // margin (rather than e.g. 2x) to reliably trip the node cap: every
        // `TaskDag::add_dependency` call below re-validates the *whole*
        // graph so far (a full O(V+E) cycle check), making construction
        // O(fanout^2) -- an unnecessarily large fanout makes this test slow
        // without adding any regression coverage.
        let fanout = DEFAULT_MAX_RENDERED_NODES + 20;
        let mut dag = TaskDag::new();
        let root = TaskId::new_v4();
        dag.add_node(root, "root");
        for i in 0..fanout {
            let child = TaskId::new_v4();
            dag.add_node(child, format!("child_{i:04}"));
            assert!(dag.add_dependency(child, root).is_ok());
        }

        let rendered = render_ascii(&dag);
        assert!(rendered.contains("truncated: exceeded max node count"));
        assert!(rendered.lines().count() <= DEFAULT_MAX_RENDERED_NODES + 5);
    }

    #[test]
    fn render_ascii_and_dot_terminate_on_corrupted_cyclic_dag_without_hanging() {
        // `TaskDag::add_dependency` mutates its adjacency before validating,
        // so a rejected (cycle-closing) edge is left partially applied --
        // a documented quirk of the read-only celers-core dag module,
        // worked around in `dag_from_tasks` via an add+rollback pair. This
        // test proves the renderers are *also* safe against that residual
        // possibility for any `TaskDag` not built via `dag_from_tasks`.
        let mut dag = TaskDag::new();
        let r = TaskId::from_u128(1);
        let x = TaskId::from_u128(2);
        let y = TaskId::from_u128(3);
        dag.add_node(r, "R");
        dag.add_node(x, "X");
        dag.add_node(y, "Y");
        assert!(dag.add_dependency(x, r).is_ok());
        assert!(dag.add_dependency(y, x).is_ok());

        // Expected to fail (closes cycle X -> Y -> X); per the quirk above,
        // it still leaves Y.dependents containing X.
        assert!(dag.add_dependency(x, y).is_err());

        let rendered = render_ascii(&dag);
        assert!(rendered.contains("cycle detected"));

        let dot = render_dot(&dag);
        assert_eq!(dot.matches("->").count(), 3);
    }

    // -- render_dot ---------------------------------------------------------

    #[test]
    fn render_dot_renders_diamond_with_all_four_edges_once() {
        let (dag, a, b, c, d) = diamond_dag();
        let expected = format!(
            "digraph celers_deps {{\n    rankdir=LR;\n    \"{a}\" [label=\"{a_label}\"];\n    \"{b}\" [label=\"{b_label}\"];\n    \"{c}\" [label=\"{c_label}\"];\n    \"{d}\" [label=\"{d_label}\"];\n    \"{a}\" -> \"{b}\";\n    \"{a}\" -> \"{c}\";\n    \"{b}\" -> \"{d}\";\n    \"{c}\" -> \"{d}\";\n}}\n",
            a_label = node_label(&dag, a),
            b_label = node_label(&dag, b),
            c_label = node_label(&dag, c),
            d_label = node_label(&dag, d),
        );

        let rendered = render_dot(&dag);
        assert_eq!(rendered, expected);
        assert_eq!(rendered.matches("->").count(), 4);
        assert_eq!(render_dot(&dag), rendered, "must be deterministic");
    }

    #[test]
    fn render_dot_handles_empty_dag() {
        let dag = TaskDag::new();
        assert_eq!(
            render_dot(&dag),
            "digraph celers_deps {\n    rankdir=LR;\n}\n"
        );
    }

    #[test]
    fn render_dot_wide_fanout_truncates_without_hanging() {
        // See the identical comment in
        // `render_ascii_wide_fanout_truncates_at_node_cap_without_hanging`:
        // `TaskDag::add_dependency`'s full-graph re-validation makes
        // construction O(fanout^2), so keep fanout just above the cap
        // rather than a large multiple of it. `BoundedWalk::run` checks the
        // cap before admitting each candidate node, so it needs `fanout` to
        // reach (not just exceed by one less than) `DEFAULT_MAX_RENDERED_NODES`
        // for truncation to trip; the `+ 20` margin clears that with room
        // to spare.
        let fanout = DEFAULT_MAX_RENDERED_NODES + 20;
        let mut dag = TaskDag::new();
        let root = TaskId::new_v4();
        dag.add_node(root, "root");
        for i in 0..fanout {
            let child = TaskId::new_v4();
            dag.add_node(child, format!("child_{i:04}"));
            assert!(dag.add_dependency(child, root).is_ok());
        }

        let rendered = render_dot(&dag);
        assert!(rendered.contains("truncated"));
        assert!(rendered.matches("->").count() <= DEFAULT_MAX_RENDERED_NODES);
    }

    #[test]
    fn dot_escape_escapes_quotes_backslashes_and_newlines() {
        assert_eq!(dot_escape("plain"), "plain");
        assert_eq!(dot_escape("has \"quotes\""), "has \\\"quotes\\\"");
        assert_eq!(dot_escape("back\\slash"), "back\\\\slash");
        assert_eq!(dot_escape("line1\nline2"), "line1\\nline2");
    }

    #[test]
    fn short_id_is_first_eight_hex_chars() {
        let id = TaskId::from_u128(0xDEAD_BEEF_0000_0000_0000_0000_0000_0000);
        assert_eq!(short_id(id), "deadbeef");
    }

    // -- dag_from_tasks -------------------------------------------------

    #[test]
    fn dag_from_tasks_wires_declared_dependencies() {
        let a = TaskId::from_u128(1);
        let b = TaskId::from_u128(2);
        let tasks = vec![task_with_deps(a, "a", &[]), task_with_deps(b, "b", &[a])];

        let dag = dag_from_tasks(&tasks);
        assert_eq!(dag.node_count(), 2);
        assert_eq!(dag.edge_count(), 1);
        assert_eq!(dag.get_dependencies(&b), Some(vec![a]));
        assert!(dag.validate().is_ok());
    }

    #[test]
    fn dag_from_tasks_skips_dependency_outside_snapshot() {
        let a = TaskId::from_u128(1);
        let missing = TaskId::from_u128(999);
        let tasks = vec![task_with_deps(a, "a", &[missing])];

        let dag = dag_from_tasks(&tasks);
        assert_eq!(dag.node_count(), 1);
        assert_eq!(dag.edge_count(), 0);
    }

    #[test]
    fn dag_from_tasks_ignores_self_dependency() {
        let a = TaskId::from_u128(1);
        let tasks = vec![task_with_deps(a, "a", &[a])];

        let dag = dag_from_tasks(&tasks);
        assert_eq!(dag.edge_count(), 0);
    }

    #[test]
    fn dag_from_tasks_drops_edges_that_would_create_a_cycle_and_stays_valid() {
        let a = TaskId::from_u128(1);
        let b = TaskId::from_u128(2);
        let c = TaskId::from_u128(3);
        // Declared: a depends on c, b depends on a, c depends on b -- a
        // cycle if every edge were honored.
        let tasks = vec![
            task_with_deps(a, "a", &[c]),
            task_with_deps(b, "b", &[a]),
            task_with_deps(c, "c", &[b]),
        ];

        let dag = dag_from_tasks(&tasks);
        assert_eq!(dag.node_count(), 3);
        assert_eq!(
            dag.edge_count(),
            2,
            "exactly one cycle-closing edge must be dropped"
        );
        assert!(
            dag.validate().is_ok(),
            "dag_from_tasks must never leave a cyclic graph"
        );
        assert!(dag.topological_sort().is_ok());
        assert!(!dag.get_roots().is_empty());
        assert_eq!(dag.get_dependencies(&c), Some(vec![]));
        assert_eq!(dag.get_dependencies(&a), Some(vec![c]));
        assert_eq!(dag.get_dependencies(&b), Some(vec![a]));
    }

    // -- JSON loading -----------------------------------------------------

    #[test]
    fn parse_tasks_json_accepts_queue_export_shape() {
        let a = TaskId::from_u128(1);
        let tasks_in = vec![task_with_deps(a, "a", &[])];
        let tasks_json = serde_json::to_string(&tasks_in).expect("serialize tasks");
        let wrapper = format!(
            "{{\"queue_name\":\"default\",\"queue_type\":\"list\",\"exported_at\":\"2026-07-12T00:00:00Z\",\"task_count\":1,\"tasks\":{tasks_json}}}"
        );

        let parsed = parse_tasks_json(&wrapper).expect("valid queue-export JSON");
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].metadata.id, a);
    }

    #[test]
    fn parse_tasks_json_accepts_bare_task_array() {
        let a = TaskId::from_u128(1);
        let tasks_in = vec![task_with_deps(a, "solo", &[])];
        let json = serde_json::to_string(&tasks_in).expect("serialize tasks");

        let parsed = parse_tasks_json(&json).expect("valid bare task array");
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].metadata.name, "solo");
    }

    #[test]
    fn parse_tasks_json_rejects_unrelated_json() {
        assert!(parse_tasks_json("{\"totally\": \"unrelated\"}").is_err());
    }

    #[test]
    fn parse_tasks_json_rejects_garbage_text() {
        assert!(parse_tasks_json("not json at all").is_err());
    }

    #[test]
    fn load_tasks_from_file_reads_and_parses_a_real_file() {
        let a = TaskId::from_u128(1);
        let tasks_in = vec![task_with_deps(a, "from-file", &[])];
        let json = serde_json::to_string(&tasks_in).expect("serialize tasks");

        let mut path = std::env::temp_dir();
        path.push(format!("celers_depgraph_test_{}.json", std::process::id()));
        std::fs::write(&path, json).expect("write fixture file");

        let loaded = load_tasks_from_file(&path);
        let _ = std::fs::remove_file(&path);

        let loaded = loaded.expect("fixture file parses");
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded[0].metadata.name, "from-file");
    }

    #[test]
    fn load_tasks_from_file_reports_missing_file() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "celers_depgraph_missing_{}.json",
            std::process::id()
        ));
        assert!(load_tasks_from_file(&path).is_err());
    }

    // -- run_deps -----------------------------------------------------------

    #[test]
    fn run_deps_ascii_smoke_test_from_file() {
        let a = TaskId::from_u128(1);
        let b = TaskId::from_u128(2);
        let tasks = vec![task_with_deps(a, "a", &[]), task_with_deps(b, "b", &[a])];
        let json = serde_json::to_string(&tasks).expect("serialize tasks");

        let mut path = std::env::temp_dir();
        path.push(format!(
            "celers_depgraph_rundeps_{}.json",
            std::process::id()
        ));
        std::fs::write(&path, json).expect("write fixture file");

        let result = run_deps(&path, "ascii", false, None);
        let _ = std::fs::remove_file(&path);
        assert!(result.is_ok());
    }

    #[test]
    fn run_deps_dot_smoke_test_from_file() {
        let a = TaskId::from_u128(1);
        let tasks = vec![task_with_deps(a, "a", &[])];
        let json = serde_json::to_string(&tasks).expect("serialize tasks");

        let mut path = std::env::temp_dir();
        path.push(format!(
            "celers_depgraph_rundeps_dot_{}.json",
            std::process::id()
        ));
        std::fs::write(&path, json).expect("write fixture file");

        let result = run_deps(&path, "dot", false, None);
        let _ = std::fs::remove_file(&path);
        assert!(result.is_ok());
    }

    #[test]
    fn run_deps_reports_missing_file() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "celers_depgraph_rundeps_missing_{}.json",
            std::process::id()
        ));
        assert!(run_deps(&path, "ascii", false, None).is_err());
    }

    #[test]
    fn run_deps_empty_file_is_ok_not_an_error() {
        let tasks: Vec<SerializedTask> = Vec::new();
        let json = serde_json::to_string(&tasks).expect("serialize tasks");
        let mut path = std::env::temp_dir();
        path.push(format!(
            "celers_depgraph_rundeps_empty_{}.json",
            std::process::id()
        ));
        std::fs::write(&path, json).expect("write fixture file");

        let result = run_deps(&path, "ascii", false, None);
        let _ = std::fs::remove_file(&path);
        assert!(result.is_ok());
    }

    #[test]
    fn run_deps_rejects_unknown_format() {
        let a = TaskId::from_u128(1);
        let tasks = vec![task_with_deps(a, "a", &[])];
        let json = serde_json::to_string(&tasks).expect("serialize tasks");

        let mut path = std::env::temp_dir();
        path.push(format!(
            "celers_depgraph_badformat_{}.json",
            std::process::id()
        ));
        std::fs::write(&path, json).expect("write fixture file");

        let result = run_deps(&path, "yaml", false, None);
        let _ = std::fs::remove_file(&path);
        assert!(result.is_err());
    }

    // -- interactive session ----------------------------------------------

    #[test]
    fn interactive_session_navigates_by_task_id_and_supports_up_down() {
        let (dag, a, b, _c, d) = diamond_dag();
        let mut session = InteractiveSession::new(a);
        assert_eq!(session.current(), a);

        // Jump straight to d (any known id is reachable, not just adjacent
        // dependencies/dependents).
        assert_eq!(session.apply(&dag, &d.to_string()), NavOutcome::Moved);
        assert_eq!(session.current(), d);

        // Back to a.
        assert_eq!(session.apply(&dag, "up"), NavOutcome::Moved);
        assert_eq!(session.current(), a);

        // Forward again to d.
        assert_eq!(session.apply(&dag, "down"), NavOutcome::Moved);
        assert_eq!(session.current(), d);

        // Navigating fresh clears forward history.
        assert_eq!(session.apply(&dag, &b.to_string()), NavOutcome::Moved);
        assert_eq!(session.current(), b);
        assert_eq!(
            session.apply(&dag, "down"),
            NavOutcome::Message("no forward history".to_string())
        );
        assert_eq!(session.current(), b);

        // Unknown id.
        let bogus = TaskId::from_u128(999);
        assert_eq!(
            session.apply(&dag, &bogus.to_string()),
            NavOutcome::Message("no task with that id in this graph".to_string())
        );

        // Garbage / empty input.
        assert!(matches!(
            session.apply(&dag, "not-an-id"),
            NavOutcome::Message(_)
        ));
        assert!(matches!(session.apply(&dag, "   "), NavOutcome::Message(_)));

        // Quit (case-insensitive).
        assert_eq!(session.apply(&dag, "QUIT"), NavOutcome::Quit);
    }

    #[test]
    fn interactive_session_up_with_empty_history_is_a_message() {
        let (dag, _a, _b, _c, d) = diamond_dag();
        let mut session = InteractiveSession::new(d);
        assert_eq!(
            session.apply(&dag, "up"),
            NavOutcome::Message("already at the start of history".to_string())
        );
        assert_eq!(session.current(), d);
    }

    #[test]
    fn render_node_view_lists_dependencies_and_dependents() {
        let (dag, a, b, _c, d) = diamond_dag();
        let view = render_node_view(&dag, b);
        assert!(view.contains("Dependencies (1):"));
        assert!(view.contains(&node_label(&dag, a)));
        assert!(view.contains("Dependents (1):"));
        assert!(view.contains(&node_label(&dag, d)));

        let root_view = render_node_view(&dag, a);
        assert!(root_view.contains("this is a root"));
    }

    #[test]
    fn render_node_view_handles_unknown_task() {
        let dag = TaskDag::new();
        let bogus = TaskId::from_u128(42);
        assert!(render_node_view(&dag, bogus).contains("unknown task"));
    }

    // -- resolve_start_id ---------------------------------------------------

    #[test]
    fn resolve_start_id_defaults_to_a_root() {
        let (dag, a, ..) = diamond_dag();
        assert_eq!(resolve_start_id(&dag, None).expect("dag has a root"), a);
    }

    #[test]
    fn resolve_start_id_accepts_explicit_valid_id() {
        let (dag, _a, _b, _c, d) = diamond_dag();
        assert_eq!(
            resolve_start_id(&dag, Some(&d.to_string())).expect("d exists"),
            d
        );
    }

    #[test]
    fn resolve_start_id_rejects_unknown_id() {
        let (dag, ..) = diamond_dag();
        let bogus = TaskId::from_u128(999).to_string();
        assert!(resolve_start_id(&dag, Some(&bogus)).is_err());
    }

    #[test]
    fn resolve_start_id_rejects_malformed_id() {
        let (dag, ..) = diamond_dag();
        assert!(resolve_start_id(&dag, Some("not-a-uuid")).is_err());
    }
}
