use crate::{CanvasError, Chain, Chord, Group};

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

/// Render DOT format to SVG using GraphViz dot command
///
/// Requires GraphViz to be installed on the system.
/// Executes: `dot -Tsvg`
///
/// # Errors
/// Returns error if GraphViz is not installed or execution fails
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

/// Render DOT format to PNG using GraphViz dot command
///
/// Requires GraphViz to be installed on the system.
/// Executes: `dot -Tpng`
///
/// # Errors
/// Returns error if GraphViz is not installed or execution fails
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

/// Check if GraphViz dot command is available
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

/// Trait for exporting workflow as DAG
pub trait DagExport {
    /// Export workflow as GraphViz DOT
    fn to_dot(&self) -> String;

    /// Export workflow as Mermaid diagram
    fn to_mermaid(&self) -> String;

    /// Export workflow as JSON
    fn to_json(&self) -> Result<String, serde_json::Error>;

    /// Export workflow as SVG using GraphViz dot command
    ///
    /// This method generates SVG by executing the `dot` command.
    /// Requires GraphViz to be installed on the system.
    ///
    /// # Errors
    /// Returns error if GraphViz is not installed or execution fails
    fn to_svg(&self) -> Result<String, CanvasError> {
        let dot = self.to_dot();
        render_dot_to_svg(&dot)
    }

    /// Export workflow as PNG using GraphViz dot command
    ///
    /// This method generates PNG by executing the `dot` command.
    /// Requires GraphViz to be installed on the system.
    ///
    /// # Errors
    /// Returns error if GraphViz is not installed or execution fails
    fn to_png(&self) -> Result<Vec<u8>, CanvasError> {
        let dot = self.to_dot();
        render_dot_to_png(&dot)
    }

    /// Get command to render DOT to SVG
    ///
    /// Returns the shell command that can be used to convert
    /// the DOT format to SVG. Useful for manual rendering.
    fn svg_render_command(&self) -> String {
        "dot -Tsvg -o output.svg input.dot".to_string()
    }

    /// Get command to render DOT to PNG
    ///
    /// Returns the shell command that can be used to convert
    /// the DOT format to PNG. Useful for manual rendering.
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
            dot.push_str(&format!("  n{} [label=\"{}\"];\n", i, task.task));
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
            mmd.push_str(&format!("  {}[\"{}\"]\n", node_id, task.task));
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
            dot.push_str(&format!("  n{} [label=\"{}\"];\n", i, task.task));
            dot.push_str(&format!("  start -> n{};\n", i));
        }

        dot.push_str("}\n");
        dot
    }

    fn to_mermaid(&self) -> String {
        let mut mmd = String::from("graph TB\n");
        mmd.push_str("  start((start))\n");

        for (i, task) in self.tasks.iter().enumerate() {
            mmd.push_str(&format!("  n{}[\"{}\"]\n", i, task.task));
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
            self.body.task
        ));

        for (i, task) in self.header.tasks.iter().enumerate() {
            dot.push_str(&format!("  n{} [label=\"{}\"];\n", i, task.task));
            dot.push_str(&format!("  start -> n{};\n", i));
            dot.push_str(&format!("  n{} -> callback;\n", i));
        }

        dot.push_str("}\n");
        dot
    }

    fn to_mermaid(&self) -> String {
        let mut mmd = String::from("graph TB\n");
        mmd.push_str("  start((start))\n");
        mmd.push_str(&format!("  callback[\"{}\"]\n", self.body.task));
        mmd.push_str("  style callback fill:#add8e6\n");

        for (i, task) in self.header.tasks.iter().enumerate() {
            mmd.push_str(&format!("  n{}[\"{}\"]\n", i, task.task));
            mmd.push_str(&format!("  start --> n{}\n", i));
            mmd.push_str(&format!("  n{} --> callback\n", i));
        }

        mmd
    }

    fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}
