//! Parameterised workflow templates and macros.
//!
//! A [`ParamTemplate`] is a reusable workflow skeleton carrying named parameter
//! *placeholders* of the form `${name}`. Calling
//! [`instantiate`](ParamTemplate::instantiate) with a parameter map performs
//! **real** substitution — across task names, positional args, and keyword args
//! — yielding a concrete workflow. Missing required parameters are reported as a
//! [`CanvasError::Invalid`] error.
//!
//! This complements the descriptive [`crate::WorkflowTemplate`] (which records
//! parameter metadata) by actually rewriting a [`Chain`] or [`Group`] body with
//! the supplied values.
//!
//! # Placeholder rules
//!
//! * In **strings** (task names and string-valued args/kwargs), every
//!   occurrence of `${name}` is replaced. If the whole string is exactly
//!   `${name}` the value is substituted verbatim (preserving its JSON type, so
//!   `${count}` -> `5` becomes the number `5`, not the string `"5"`). Otherwise
//!   the value is interpolated using its string form.
//! * Substitution recurses into nested JSON arrays and objects.
//! * A `${name}` referencing an undeclared/absent parameter is a missing-param
//!   error; parameters with a declared default are filled in automatically.
//!
//! # Example
//!
//! ```
//! use celers_canvas::{ParamTemplate, TemplateParam, Chain};
//! use serde_json::json;
//! use std::collections::HashMap;
//!
//! let body = Chain::new()
//!     .then("process_${env}", vec![json!("${batch_id}")]);
//!
//! let template = ParamTemplate::from_chain("etl", body)
//!     .with_param(TemplateParam::required("env"))
//!     .with_param(TemplateParam::with_default("batch_id", json!(0)));
//!
//! let mut params = HashMap::new();
//! params.insert("env".to_string(), json!("prod"));
//! params.insert("batch_id".to_string(), json!(42));
//!
//! let chain = template.instantiate(&params).expect("all params present");
//! assert_eq!(chain.first().unwrap().task, "process_prod");
//! assert_eq!(chain.first().unwrap().args[0], json!(42));
//! ```

use crate::{CanvasError, Chain, Group, Signature};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A single named placeholder accepted by a [`ParamTemplate`].
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TemplateParam {
    /// Parameter name (the `name` in `${name}`).
    pub name: String,
    /// Optional default used when the caller omits this parameter.
    pub default: Option<serde_json::Value>,
}

impl TemplateParam {
    /// A required parameter with no default.
    pub fn required(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            default: None,
        }
    }

    /// A parameter with a default value (and therefore optional).
    pub fn with_default(name: impl Into<String>, default: serde_json::Value) -> Self {
        Self {
            name: name.into(),
            default: Some(default),
        }
    }

    /// Whether this parameter must be supplied by the caller.
    pub fn is_required(&self) -> bool {
        self.default.is_none()
    }
}

impl std::fmt::Display for TemplateParam {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "${{{}}}", self.name)?;
        if let Some(default) = &self.default {
            write!(f, "={}", default)?;
        }
        Ok(())
    }
}

/// The workflow shape carried by a template body.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "body")]
pub enum TemplateBody {
    /// A chain skeleton.
    Chain(Chain),
    /// A group skeleton.
    Group(Group),
}

/// A reusable, parameterised workflow skeleton.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParamTemplate {
    /// Template name (for diagnostics/registries).
    pub name: String,
    /// Declared parameters.
    pub params: Vec<TemplateParam>,
    /// The workflow skeleton with `${...}` placeholders.
    pub body: TemplateBody,
}

impl ParamTemplate {
    /// Build a template from a [`Chain`] skeleton.
    pub fn from_chain(name: impl Into<String>, chain: Chain) -> Self {
        Self {
            name: name.into(),
            params: Vec::new(),
            body: TemplateBody::Chain(chain),
        }
    }

    /// Build a template from a [`Group`] skeleton.
    pub fn from_group(name: impl Into<String>, group: Group) -> Self {
        Self {
            name: name.into(),
            params: Vec::new(),
            body: TemplateBody::Group(group),
        }
    }

    /// Declare a parameter.
    pub fn with_param(mut self, param: TemplateParam) -> Self {
        self.params.push(param);
        self
    }

    /// Declare several parameters at once.
    pub fn with_params(mut self, params: impl IntoIterator<Item = TemplateParam>) -> Self {
        self.params.extend(params);
        self
    }

    /// Names of parameters the caller must supply (those without a default).
    pub fn required_params(&self) -> Vec<&str> {
        self.params
            .iter()
            .filter(|p| p.is_required())
            .map(|p| p.name.as_str())
            .collect()
    }

    /// Resolve the effective value map: caller-supplied values overlaid on
    /// declared defaults. Returns the names of any required parameters that are
    /// still missing.
    fn resolve(
        &self,
        params: &HashMap<String, serde_json::Value>,
    ) -> Result<HashMap<String, serde_json::Value>, Vec<String>> {
        let mut resolved: HashMap<String, serde_json::Value> = HashMap::new();
        let mut missing: Vec<String> = Vec::new();

        for declared in &self.params {
            if let Some(value) = params.get(&declared.name) {
                resolved.insert(declared.name.clone(), value.clone());
            } else if let Some(default) = &declared.default {
                resolved.insert(declared.name.clone(), default.clone());
            } else {
                missing.push(declared.name.clone());
            }
        }

        // Caller-supplied values for *undeclared* names are still made available
        // so templates can stay terse, but they never satisfy a required-param
        // check (only declared params are validated).
        for (key, value) in params {
            resolved.entry(key.clone()).or_insert_with(|| value.clone());
        }

        if missing.is_empty() {
            Ok(resolved)
        } else {
            missing.sort();
            Err(missing)
        }
    }

    /// Instantiate the template into a concrete [`Chain`].
    ///
    /// Errors with [`CanvasError::Invalid`] if the body is a group rather than a
    /// chain, if a required parameter is missing, or if a `${name}` placeholder
    /// references a value that was neither supplied nor defaulted.
    pub fn instantiate(
        &self,
        params: &HashMap<String, serde_json::Value>,
    ) -> Result<Chain, CanvasError> {
        let resolved = self.resolve(params).map_err(|missing| {
            CanvasError::Invalid(format!(
                "missing required template parameter(s): {}",
                missing.join(", ")
            ))
        })?;

        match &self.body {
            TemplateBody::Chain(chain) => {
                let mut out = Chain::new();
                for sig in &chain.tasks {
                    out.tasks.push(substitute_signature(sig, &resolved)?);
                }
                Ok(out)
            }
            TemplateBody::Group(_) => Err(CanvasError::Invalid(format!(
                "template '{}' has a group body; use instantiate_group()",
                self.name
            ))),
        }
    }

    /// Instantiate the template into a concrete [`Group`].
    ///
    /// Mirrors [`instantiate`](Self::instantiate) for group-bodied templates.
    pub fn instantiate_group(
        &self,
        params: &HashMap<String, serde_json::Value>,
    ) -> Result<Group, CanvasError> {
        let resolved = self.resolve(params).map_err(|missing| {
            CanvasError::Invalid(format!(
                "missing required template parameter(s): {}",
                missing.join(", ")
            ))
        })?;

        match &self.body {
            TemplateBody::Group(group) => {
                let mut out = Group::new();
                for sig in &group.tasks {
                    out.tasks.push(substitute_signature(sig, &resolved)?);
                }
                Ok(out)
            }
            TemplateBody::Chain(_) => Err(CanvasError::Invalid(format!(
                "template '{}' has a chain body; use instantiate()",
                self.name
            ))),
        }
    }
}

impl std::fmt::Display for ParamTemplate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let kind = match self.body {
            TemplateBody::Chain(_) => "chain",
            TemplateBody::Group(_) => "group",
        };
        write!(
            f,
            "ParamTemplate[{}, {} body, {} params]",
            self.name,
            kind,
            self.params.len()
        )
    }
}

/// Substitute placeholders throughout a single [`Signature`].
fn substitute_signature(
    sig: &Signature,
    params: &HashMap<String, serde_json::Value>,
) -> Result<Signature, CanvasError> {
    let mut next = sig.clone();
    next.task = substitute_string(&sig.task, params)?;

    next.args = sig
        .args
        .iter()
        .map(|v| substitute_value(v, params))
        .collect::<Result<Vec<_>, _>>()?;

    next.kwargs = sig
        .kwargs
        .iter()
        .map(|(k, v)| Ok((k.clone(), substitute_value(v, params)?)))
        .collect::<Result<HashMap<_, _>, CanvasError>>()?;

    Ok(next)
}

/// Recursively substitute placeholders inside an arbitrary JSON value.
fn substitute_value(
    value: &serde_json::Value,
    params: &HashMap<String, serde_json::Value>,
) -> Result<serde_json::Value, CanvasError> {
    match value {
        serde_json::Value::String(s) => {
            // A string that is *exactly* a single placeholder is replaced by the
            // typed parameter value, preserving numbers/bools/objects.
            if let Some(name) = whole_placeholder(s) {
                lookup(params, name)
            } else {
                Ok(serde_json::Value::String(substitute_string(s, params)?))
            }
        }
        serde_json::Value::Array(items) => {
            let mapped = items
                .iter()
                .map(|v| substitute_value(v, params))
                .collect::<Result<Vec<_>, _>>()?;
            Ok(serde_json::Value::Array(mapped))
        }
        serde_json::Value::Object(map) => {
            let mut out = serde_json::Map::with_capacity(map.len());
            for (k, v) in map {
                out.insert(substitute_string(k, params)?, substitute_value(v, params)?);
            }
            Ok(serde_json::Value::Object(out))
        }
        // Numbers, booleans and null carry no placeholders.
        other => Ok(other.clone()),
    }
}

/// If `s` is exactly `${name}`, return `name`; otherwise `None`.
fn whole_placeholder(s: &str) -> Option<&str> {
    let inner = s.strip_prefix("${")?.strip_suffix('}')?;
    if inner.is_empty() || inner.contains("${") {
        None
    } else {
        Some(inner)
    }
}

/// Look up a parameter value, returning a descriptive error when absent.
fn lookup(
    params: &HashMap<String, serde_json::Value>,
    name: &str,
) -> Result<serde_json::Value, CanvasError> {
    params.get(name).cloned().ok_or_else(|| {
        CanvasError::Invalid(format!("unresolved template placeholder: ${{{}}}", name))
    })
}

/// Replace every `${name}` inside a string with the string form of its value.
///
/// Returns an error if any referenced parameter is absent.
fn substitute_string(
    input: &str,
    params: &HashMap<String, serde_json::Value>,
) -> Result<String, CanvasError> {
    if !input.contains("${") {
        return Ok(input.to_string());
    }

    let mut out = String::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'$' && i + 1 < bytes.len() && bytes[i + 1] == b'{' {
            // Find the closing brace.
            if let Some(end_rel) = input[i + 2..].find('}') {
                let name = &input[i + 2..i + 2 + end_rel];
                if !name.is_empty() {
                    let value = lookup(params, name)?;
                    out.push_str(&value_to_string(&value));
                    i = i + 2 + end_rel + 1;
                    continue;
                }
            }
        }
        // Not a placeholder start: copy the current char (handling UTF-8).
        let ch = input[i..].chars().next().unwrap_or('\u{FFFD}');
        out.push(ch);
        i += ch.len_utf8();
    }
    Ok(out)
}

/// Render a JSON value as a plain string for interpolation (strings unquoted).
fn value_to_string(value: &serde_json::Value) -> String {
    match value {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn params(pairs: &[(&str, serde_json::Value)]) -> HashMap<String, serde_json::Value> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    #[test]
    fn instantiate_substitutes_task_name_and_args() {
        let body = Chain::new().then("process_${env}", vec![json!("${batch}")]);
        let template = ParamTemplate::from_chain("etl", body)
            .with_param(TemplateParam::required("env"))
            .with_param(TemplateParam::required("batch"));

        let chain = template
            .instantiate(&params(&[("env", json!("prod")), ("batch", json!(7))]))
            .expect("instantiate");

        let first = chain.first().expect("first");
        assert_eq!(first.task, "process_prod");
        // Whole-placeholder arg keeps the numeric type.
        assert_eq!(first.args[0], json!(7));
        assert!(first.args[0].is_i64());
    }

    #[test]
    fn missing_required_param_errors_with_name() {
        let body = Chain::new().then("t_${a}", vec![]);
        let template = ParamTemplate::from_chain("x", body)
            .with_param(TemplateParam::required("a"))
            .with_param(TemplateParam::required("b"));

        let err = template
            .instantiate(&params(&[("a", json!("v"))]))
            .expect_err("missing b");
        assert!(err.is_invalid());
        let msg = format!("{}", err);
        assert!(
            msg.contains('b'),
            "error should name the missing param: {msg}"
        );
    }

    #[test]
    fn defaults_fill_in_missing_optionals() {
        let body = Chain::new().then("run", vec![json!("${region}")]);
        let template = ParamTemplate::from_chain("x", body)
            .with_param(TemplateParam::with_default("region", json!("us-east-1")));

        let chain = template.instantiate(&params(&[])).expect("defaults apply");
        assert_eq!(chain.first().expect("first").args[0], json!("us-east-1"));
    }

    #[test]
    fn caller_value_overrides_default() {
        let body = Chain::new().then("run_${region}", vec![]);
        let template = ParamTemplate::from_chain("x", body)
            .with_param(TemplateParam::with_default("region", json!("default")));

        let chain = template
            .instantiate(&params(&[("region", json!("eu"))]))
            .expect("override");
        assert_eq!(chain.first().expect("first").task, "run_eu");
    }

    #[test]
    fn partial_interpolation_uses_string_form() {
        let body = Chain::new().then("job-${id}-${suffix}", vec![]);
        let template = ParamTemplate::from_chain("x", body)
            .with_param(TemplateParam::required("id"))
            .with_param(TemplateParam::required("suffix"));

        let chain = template
            .instantiate(&params(&[("id", json!(3)), ("suffix", json!("final"))]))
            .expect("interpolate");
        assert_eq!(chain.first().expect("first").task, "job-3-final");
    }

    #[test]
    fn substitution_recurses_into_nested_json() {
        let body = Chain::new().then(
            "t",
            vec![json!({"path": "${dir}/file", "list": ["${id}", 99]})],
        );
        let template = ParamTemplate::from_chain("x", body)
            .with_param(TemplateParam::required("dir"))
            .with_param(TemplateParam::required("id"));

        let chain = template
            .instantiate(&params(&[("dir", json!("/tmp")), ("id", json!(5))]))
            .expect("nested");
        let arg = &chain.first().expect("first").args[0];
        assert_eq!(arg["path"], json!("/tmp/file"));
        assert_eq!(arg["list"][0], json!(5));
        assert_eq!(arg["list"][1], json!(99));
    }

    #[test]
    fn kwargs_are_substituted() {
        let mut kwargs = HashMap::new();
        kwargs.insert("target".to_string(), json!("${env}"));
        let sig = Signature::new("deploy".to_string()).with_kwargs(kwargs);
        let body = Chain::new().then_signature(sig);
        let template =
            ParamTemplate::from_chain("x", body).with_param(TemplateParam::required("env"));

        let chain = template
            .instantiate(&params(&[("env", json!("staging"))]))
            .expect("kwargs");
        assert_eq!(
            chain.first().expect("first").kwargs.get("target"),
            Some(&json!("staging"))
        );
    }

    #[test]
    fn group_body_instantiates_and_rejects_chain_call() {
        let body = Group::new().add("worker_${shard}", vec![]);
        let template =
            ParamTemplate::from_group("g", body).with_param(TemplateParam::required("shard"));

        let group = template
            .instantiate_group(&params(&[("shard", json!("01"))]))
            .expect("group instantiate");
        assert_eq!(group.tasks[0].task, "worker_01");

        // Calling the chain variant on a group body is an error.
        assert!(template
            .instantiate(&params(&[("shard", json!("01"))]))
            .is_err());
    }

    #[test]
    fn chain_template_rejects_group_call() {
        let body = Chain::new().then("t", vec![]);
        let template = ParamTemplate::from_chain("c", body);
        assert!(template.instantiate_group(&params(&[])).is_err());
    }

    #[test]
    fn required_params_lists_only_no_default() {
        let template = ParamTemplate::from_chain("x", Chain::new().then("t", vec![]))
            .with_param(TemplateParam::required("a"))
            .with_param(TemplateParam::with_default("b", json!(1)))
            .with_param(TemplateParam::required("c"));
        let mut req = template.required_params();
        req.sort();
        assert_eq!(req, vec!["a", "c"]);
    }

    #[test]
    fn unreferenced_extra_params_are_ignored() {
        let body = Chain::new().then("t_${used}", vec![]);
        let template =
            ParamTemplate::from_chain("x", body).with_param(TemplateParam::required("used"));
        // Supplying an extra, undeclared param must not break instantiation.
        let chain = template
            .instantiate(&params(&[
                ("used", json!("v")),
                ("extra", json!("ignored")),
            ]))
            .expect("extra params ignored");
        assert_eq!(chain.first().expect("first").task, "t_v");
    }

    #[test]
    fn no_placeholders_is_identity() {
        let body = Chain::new().then("plain", vec![json!("literal")]);
        let template = ParamTemplate::from_chain("x", body);
        let chain = template.instantiate(&params(&[])).expect("identity");
        assert_eq!(chain.first().expect("first").task, "plain");
        assert_eq!(chain.first().expect("first").args[0], json!("literal"));
    }
}
