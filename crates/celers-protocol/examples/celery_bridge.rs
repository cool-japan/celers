//! The CeleRS half of the Python interoperability suite.
//!
//! `tests/python-compat` drives a real Python Celery against a real Redis and
//! pipes every message that crosses the boundary through this binary. It reads
//! one JSON document on stdin, writes one on stdout, and does all of its work
//! through the ordinary public API of `celers-protocol` -- so the suite proves
//! the shipped protocol code, not a test-local reimplementation of it.
//!
//! ```text
//! celery_bridge encode-task    {task,id,args,kwargs}    -> kombu envelope
//! celery_bridge build-task     {task,id,args,...}       -> kombu envelope (MessageBuilder)
//! celery_bridge decode-task    kombu envelope           -> {task,id,args,kwargs,eta,...}
//! celery_bridge execute        kombu envelope           -> Celery result meta
//! celery_bridge decode-result  Celery result meta       -> {task_id,status,result,...}
//! ```
//!
//! `execute` is the piece that makes a round trip meaningful: it decodes a
//! message Python published, runs the CeleRS-side implementation of the named
//! task, and renders the outcome as the record a Python `AsyncResult.get()`
//! reads back.
//!
//! # Scope
//!
//! This speaks the Celery *protocol*. It is not a broker: `celers-broker-redis`
//! keeps its own `SerializedTask` envelope on the queue and does not read or
//! write the kombu framing handled here. The suite's README spells that
//! boundary out.

use std::collections::HashMap;
use std::error::Error;
use std::io::{Read, Write};

use celers_protocol::compat::{
    create_python_celery_message, create_python_celery_message_on_queue, parse_python_message,
};
use celers_protocol::embed::EmbeddedBody;
use celers_protocol::result::{ExceptionInfo, ResultMessage};
use celers_protocol::{builder::MessageBuilder, Message};
use chrono::{DateTime, Utc};
use serde_json::{json, Map, Value};
use uuid::Uuid;

type BridgeResult<T> = Result<T, Box<dyn Error>>;

fn main() -> BridgeResult<()> {
    let subcommand = std::env::args()
        .nth(1)
        .ok_or("usage: celery_bridge <encode-task|build-task|decode-task|execute|decode-result>")?;

    let mut input = String::new();
    std::io::stdin().read_to_string(&mut input)?;
    let request: Value = serde_json::from_str(&input)
        .map_err(|e| format!("stdin is not JSON: {e}\n---\n{input}"))?;

    let response = match subcommand.as_str() {
        "encode-task" => encode_task(&request)?,
        "build-task" => build_task(&request)?,
        "decode-task" => decode_task(&request)?,
        "execute" => execute(&request)?,
        "decode-result" => decode_result(&request)?,
        other => return Err(format!("unknown subcommand {other:?}").into()),
    };

    let mut stdout = std::io::stdout();
    stdout.write_all(serde_json::to_string(&response)?.as_bytes())?;
    stdout.write_all(b"\n")?;
    stdout.flush()?;
    Ok(())
}

// =============================================================================
// Producing
// =============================================================================

/// The canonical envelope: `compat::create_python_celery_message`, verbatim.
///
/// An optional `queue` selects the routing-key-aware variant, which is what a
/// task published anywhere other than the default queue needs if its retries
/// are to come back to the same place.
fn encode_task(request: &Value) -> BridgeResult<Value> {
    let task = string_field(request, "task")?;
    let task_id = uuid_field(request, "id")?;
    let args = array_field(request, "args");
    let kwargs = request.get("kwargs").cloned().unwrap_or_else(|| json!({}));
    match request.get("queue").and_then(Value::as_str) {
        Some(queue) => Ok(create_python_celery_message_on_queue(
            &task, task_id, args, kwargs, queue,
        )?),
        None => Ok(create_python_celery_message(&task, task_id, args, kwargs)?),
    }
}

/// The builder path: whatever `MessageBuilder` serializes to.
///
/// This is the envelope an ordinary CeleRS producer emits, which is a
/// different code path from [`encode_task`] and therefore worth proving
/// separately against a live worker.
fn build_task(request: &Value) -> BridgeResult<Value> {
    let task = string_field(request, "task")?;
    let mut builder = MessageBuilder::new(task).args(array_field(request, "args"));

    if let Some(id) = request.get("id").and_then(Value::as_str) {
        builder = builder.id(id.parse::<Uuid>()?);
    }
    // Naming the queue is what makes `properties.delivery_info.routing_key`
    // truthful, and a worker routes a task's own retries with it.
    if let Some(queue) = request.get("queue").and_then(Value::as_str) {
        builder = builder.queue(queue);
    }
    if let Some(kwargs) = request.get("kwargs").and_then(Value::as_object) {
        let map: HashMap<String, Value> = kwargs
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect();
        builder = builder.kwargs(map);
    }
    if let Some(seconds) = request.get("countdown").and_then(Value::as_i64) {
        builder = builder.countdown(seconds);
    }
    if let Some(eta) = request.get("eta").and_then(Value::as_str) {
        builder = builder.eta(parse_instant(eta)?);
    }
    if let Some(expires) = request.get("expires").and_then(Value::as_str) {
        builder = builder.expires(parse_instant(expires)?);
    }
    if let Some(retries) = request.get("retries").and_then(Value::as_u64) {
        builder = builder.retries(retries as u32);
    }
    if let Some(group) = request.get("group").and_then(Value::as_str) {
        builder = builder.group(group.parse::<Uuid>()?);
    }
    if let Some(priority) = request.get("priority").and_then(Value::as_u64) {
        builder = builder.priority(priority.min(u64::from(u8::MAX)) as u8);
    }
    for name in request
        .get("chain")
        .and_then(Value::as_array)
        .unwrap_or(&Vec::new())
    {
        if let Some(name) = name.as_str() {
            builder = builder.chain_task(name);
        }
    }
    for name in request
        .get("link")
        .and_then(Value::as_array)
        .unwrap_or(&Vec::new())
    {
        if let Some(name) = name.as_str() {
            builder = builder.link(name);
        }
    }

    let message = builder.build()?;
    Ok(serde_json::to_value(message)?)
}

// =============================================================================
// Consuming
// =============================================================================

/// Everything CeleRS reads out of an envelope Python published.
fn decode_task(envelope: &Value) -> BridgeResult<Value> {
    let message = parse_python_message(envelope.clone())?;
    let body = EmbeddedBody::decode(&message.body)?;

    let kwargs: Map<String, Value> = body
        .kwargs
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();

    Ok(json!({
        "task": message.headers.task,
        "id": message.headers.id.to_string(),
        "lang": message.headers.lang,
        "args": body.args,
        "kwargs": Value::Object(kwargs),
        "eta": message.headers.eta.map(|t| t.to_rfc3339()),
        "expires": message.headers.expires.map(|t| t.to_rfc3339()),
        "retries": message.headers.retries,
        "root_id": message.headers.root_id.map(|id| id.to_string()),
        "parent_id": message.headers.parent_id.map(|id| id.to_string()),
        "group": message.headers.group.map(|id| id.to_string()),
        "argsrepr": message.headers.argsrepr(),
        "kwargsrepr": message.headers.kwargsrepr(),
        "origin": message.headers.origin(),
        "timelimit": message.headers.timelimit().map(|(soft, hard)| json!([soft, hard])),
        "content_type": message.content_type,
        "content_encoding": message.content_encoding,
        "delivery_mode": message.properties.delivery_mode,
        "priority": message.properties.priority,
        "correlation_id": message.properties.correlation_id,
        "delivery_tag": message.properties.delivery_tag,
        "exchange": message.properties.delivery_info.exchange,
        "routing_key": message.properties.routing_key(),
        "has_workflow": body.embed.has_workflow(),
        "chain": body.embed.chain.iter().map(|c| c.task.clone()).collect::<Vec<_>>(),
        // The links themselves, not just their names: continuing a chain needs
        // the id Celery pre-assigned to the next task, which travels in the
        // signature's `options` rather than in any header.
        "chain_links": body.embed.chain.iter().map(signature_detail).collect::<Vec<_>>(),
        "callbacks": body.embed.callbacks.iter().map(|c| c.task.clone()).collect::<Vec<_>>(),
        "errbacks": body.embed.errbacks.iter().map(|c| c.task.clone()).collect::<Vec<_>>(),
        "chord": body.embed.chord.as_ref().map(|c| c.task.clone()),
    }))
}

/// Flatten a callback signature into the fields a chain continuation needs.
fn signature_detail(signature: &celers_protocol::embed::CallbackSignature) -> Value {
    json!({
        "task": signature.task,
        "args": signature.args,
        "kwargs": signature.kwargs,
        // Celery writes the pre-assigned id into the signature's `options`;
        // CeleRS also has a typed field for it. Prefer the typed one and fall
        // back, so a link from either producer is usable.
        "task_id": signature
            .task_id
            .map(|id| id.to_string())
            .or_else(|| {
                signature
                    .options
                    .get("task_id")
                    .and_then(Value::as_str)
                    .map(str::to_string)
            }),
        "immutable": signature.immutable,
    })
}

/// Decode, run the CeleRS-side task, and render the Celery result record.
fn execute(envelope: &Value) -> BridgeResult<Value> {
    let message = parse_python_message(envelope.clone())?;
    let body = EmbeddedBody::decode(&message.body)?;
    let task_id = message.headers.id;

    let outcome = run_task(&message, &body);
    let result = match outcome {
        Ok(value) => {
            let mut message = ResultMessage::success(task_id, value);
            message.date_done = Some(Utc::now());
            message
        }
        Err(failure) => {
            let mut message = ResultMessage::failure_with_exception(task_id, failure);
            message.date_done = Some(Utc::now());
            message
        }
    }
    .with_task(message.headers.task.clone());

    Ok(serde_json::from_slice(&result.to_json()?)?)
}

/// Read a Celery result record back into the typed model.
///
/// `exc_message` is reported as the list it is on the wire (Python's
/// `exc.args`, splatted back into the exception constructor), with the joined
/// human-readable rendering alongside it as `exc_message_text`. `children`
/// reports the parsed result trees, since a retried or chained task's record
/// carries whole `AsyncResult.as_tuple()` nodes rather than bare ids.
fn decode_result(meta: &Value) -> BridgeResult<Value> {
    let result = ResultMessage::from_json(serde_json::to_string(meta)?.as_bytes())?;
    Ok(json!({
        "task_id": result.task_id.to_string(),
        "status": result.status.as_str(),
        "is_success": result.is_success(),
        "is_failure": result.is_failure(),
        "result": result.result,
        "traceback": result.traceback,
        "exc_type": result.exception.as_ref().map(|e| e.exc_type.clone()),
        "exc_message": result.exception.as_ref().map(|e| e.exc_message.clone()),
        "exc_message_text": result.exception.as_ref().map(ExceptionInfo::message),
        "exc_module": result.exception.as_ref().and_then(|e| e.exc_module.clone()),
        "children": result.children.iter().map(child_detail).collect::<Vec<_>>(),
        // What CeleRS would write back out, so a caller can compare it with
        // what Celery put in.
        "children_wire": serde_json::to_value(&result.children)?,
    }))
}

/// Flatten one parsed result child into the parts a test asserts on.
fn child_detail(child: &celers_protocol::result::ResultChild) -> Value {
    json!({
        "task_id": child.task_id.to_string(),
        "parent": child.parent.as_ref().map(|parent| child_detail(parent)),
        "is_group": child.is_group(),
        "children": child
            .children
            .as_ref()
            .map(|members| members.iter().map(child_detail).collect::<Vec<_>>()),
        "descendant_ids": child
            .descendant_ids()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
    })
}

// =============================================================================
// The CeleRS-side task implementations
// =============================================================================

/// The CeleRS implementations of the tasks in `tests/python-compat/tasks.py`.
///
/// Both runtimes must agree on what a task *name* means, or a round trip
/// proves only that bytes moved. Keep these in step with `tasks.py`.
fn run_task(message: &Message, body: &EmbeddedBody) -> Result<Value, ExceptionInfo> {
    let name = message.headers.task.as_str();
    let arg = |index: usize| body.args.get(index);
    let kwarg = |key: &str| body.kwargs.get(key);

    match name {
        "tasks.add" => {
            let x = number(arg(0).or_else(|| kwarg("x")), "x")?;
            let y = number(arg(1).or_else(|| kwarg("y")), "y")?;
            Ok(add_numbers(&x, &y))
        }
        "tasks.double" => {
            let x = number(arg(0).or_else(|| kwarg("x")), "x")?;
            Ok(add_numbers(&x, &x))
        }
        "tasks.greet" => {
            let who = arg(0)
                .or_else(|| kwarg("name"))
                .and_then(Value::as_str)
                .ok_or_else(|| type_error("tasks.greet() missing argument: 'name'"))?;
            let punct = arg(1)
                .or_else(|| kwarg("punct"))
                .and_then(Value::as_str)
                .unwrap_or("!");
            let loud = arg(2)
                .or_else(|| kwarg("loud"))
                .and_then(Value::as_bool)
                .unwrap_or(false);
            let text = format!("Hello, {who}{punct}");
            Ok(Value::String(if loud { text.to_uppercase() } else { text }))
        }
        "tasks.boom" => {
            let detail = arg(0)
                .or_else(|| kwarg("message"))
                .and_then(Value::as_str)
                .unwrap_or("boom");
            Err(ExceptionInfo::new("ValueError", detail)
                .with_exc_module("builtins")
                .with_traceback(format!(
                    "Traceback (most recent call last):\n  \
                     File \"celery_bridge.rs\", in run_task\n    \
                     tasks.boom({detail:?})\nValueError: {detail}\n"
                )))
        }
        other => Err(ExceptionInfo::new(
            "NotRegistered",
            format!("the CeleRS bridge does not implement {other}"),
        )
        .with_exc_module("celery.exceptions")),
    }
}

/// Add two JSON numbers, staying integral when both operands are.
fn add_numbers(x: &Value, y: &Value) -> Value {
    match (x.as_i64(), y.as_i64()) {
        (Some(a), Some(b)) => match a.checked_add(b) {
            Some(sum) => Value::from(sum),
            // Python integers are unbounded; f64 is the closest JSON has.
            None => Value::from(a as f64 + b as f64),
        },
        _ => Value::from(x.as_f64().unwrap_or(f64::NAN) + y.as_f64().unwrap_or(f64::NAN)),
    }
}

fn number(value: Option<&Value>, name: &str) -> Result<Value, ExceptionInfo> {
    match value {
        Some(value) if value.is_number() => Ok(value.clone()),
        Some(other) => Err(type_error(format!(
            "argument '{name}' must be a number, got {other}"
        ))),
        None => Err(type_error(format!("missing argument: '{name}'"))),
    }
}

fn type_error(message: impl Into<String>) -> ExceptionInfo {
    ExceptionInfo::new("TypeError", message).with_exc_module("builtins")
}

// =============================================================================
// Field helpers
// =============================================================================

fn string_field(request: &Value, key: &str) -> BridgeResult<String> {
    request
        .get(key)
        .and_then(Value::as_str)
        .map(str::to_string)
        .ok_or_else(|| format!("request is missing the string field {key:?}").into())
}

fn uuid_field(request: &Value, key: &str) -> BridgeResult<Uuid> {
    Ok(string_field(request, key)?.parse::<Uuid>()?)
}

fn array_field(request: &Value, key: &str) -> Vec<Value> {
    request
        .get(key)
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default()
}

fn parse_instant(text: &str) -> BridgeResult<DateTime<Utc>> {
    Ok(DateTime::parse_from_rfc3339(text)?.with_timezone(&Utc))
}
