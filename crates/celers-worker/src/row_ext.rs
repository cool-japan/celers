//! Reusable row-mapping helpers for the OxiSQL-backed PostgreSQL DLQ storage
//! backend ([`crate::dlq_storage::PostgresDlqStorage`]).
//!
//! Every call site that used to go through the previous SQL toolkit's
//! `row.get::<T, _>("col")` (which panics on a schema/type mismatch) now goes
//! through [`RowExt::col`], which wraps [`oxisql_core::Row::try_get`] and
//! returns a `Result` instead of panicking. This keeps column extraction
//! consistent and panic-free across all call sites.
//!
//! This module mirrors `celers-broker-postgres`'s `row_ext.rs` (the proven
//! prior migration this one is built from) verbatim in structure; only the
//! module path (`crate::row_ext` here vs. that crate's own tree) and the
//! doc-comment cross-references differ.
//!
//! oxisql-core's [`oxisql_core::ToSqlValue`] / [`oxisql_core::FromValue`]
//! traits only cover SQL-primitive Rust types (`i32`, `i64`, `f64`, `String`,
//! `bool`, `Vec<u8>`, `Option<T>`, and — behind the `chrono` feature,
//! read-only — `chrono::DateTime<Utc>` and friends). They do **not** support
//! `uuid::Uuid` or `serde_json::Value` directly (confirmed by reading
//! `oxisql-core`'s `traits.rs`/`row.rs`/`value.rs`: there is a raw
//! `Value::Uuid(u128)` / `FromValue for u128` pair and a raw `Value::Json(String)`
//! variant, but no impl bridging either to the higher-level `uuid`/`serde_json`
//! crate types). The helpers below close that gap so downstream call sites
//! never have to hand-roll the `u128`/`String` plumbing themselves.

use oxisql_core::{OxiSqlError, Row};

/// Extension trait adding a concise, type-inferred column accessor to
/// [`oxisql_core::Row`].
pub trait RowExt {
    /// Extract the value of column `name`, converting it to `T` via
    /// [`oxisql_core::FromValue`].
    ///
    /// Returns `Err` (never panics) on a missing column or a type mismatch —
    /// this is the safety improvement over the previous SQL toolkit's
    /// `row.get::<T, _>("col")`, which panics on either condition.
    fn col<T: oxisql_core::FromValue>(&self, name: &str) -> Result<T, OxiSqlError>;

    /// Extract the value of the column at zero-based position `idx`,
    /// converting it to `T` via [`oxisql_core::FromValue`].
    ///
    /// Prefer this over [`RowExt::col`] when the SQL text's `SELECT` target
    /// is an unaliased literal or expression (e.g. `SELECT 1`, or a binary
    /// expression like `now() - x`). Postgres assigns an implicit display
    /// name to unaliased expressions (the literal string `"?column?"` for
    /// anything that is not a bare column reference or a single unadorned
    /// function call), and guessing that string is fragile — positional
    /// access sidesteps the guess entirely. A bare, unaliased function call
    /// (`SELECT version()`, `SELECT count(*)`) is the one case where the
    /// implicit name is well-defined (the function name) and [`RowExt::col`]
    /// is safe and preferred for readability.
    fn col_idx<T: oxisql_core::FromValue>(&self, idx: usize) -> Result<T, OxiSqlError>;
}

impl RowExt for Row {
    fn col<T: oxisql_core::FromValue>(&self, name: &str) -> Result<T, OxiSqlError> {
        self.try_get(name)
    }

    fn col_idx<T: oxisql_core::FromValue>(&self, idx: usize) -> Result<T, OxiSqlError> {
        self.try_get_by_index(idx)
    }
}

/// Build a closure `FnMut(&Row) -> Result<Ty, OxiSqlError>` that maps a row's
/// named columns onto the fields of `Ty`.
///
/// Usage mirrors the shape of the target struct (sketch: `TaskInfo` and `rows`
/// stand in for the caller's own type and query result, so this is written as
/// prose rather than as a doctest):
///
/// ```text
/// let infos: Vec<TaskInfo> = rows
///     .iter()
///     .map(row_to!(TaskInfo { task_name: "task_name" }))
///     .collect::<Result<_, _>>()?;
/// ```
///
/// This crate's own call sites map the multi-field `DlqEntry` struct by hand
/// across several fields with mixed typed helpers (`uuid_from_row`,
/// `json_from_row`, `row.col`, ...) rather than through this macro — it is
/// included, unused-but-correct, matching the prior migration's own module
/// exactly, since a single-column-per-field macro cannot express the
/// typed-helper substitutions `DlqEntry` needs (e.g. its `task_id` field via
/// [`uuid_from_row`] rather than [`RowExt::col`]).
#[allow(unused_macros)]
macro_rules! row_to {
    ($ty:ident { $($field:ident : $col:literal),+ $(,)? }) => {
        |row: &::oxisql_core::Row| -> ::std::result::Result<$ty, ::oxisql_core::OxiSqlError> {
            use $crate::row_ext::RowExt;
            Ok($ty { $( $field: row.col($col)?, )+ })
        }
    };
}
#[allow(unused_imports)]
pub(crate) use row_to;

// ── Domain-typed helpers ────────────────────────────────────────────────────
//
// oxisql-core has no `ToSqlValue`/`FromValue` bridge for `uuid::Uuid` or
// `serde_json::Value`, only for the underlying primitive representations
// (`Value::Uuid(u128)` and `Value::Json(String)`/`Value::Text(String)`). The
// helpers below are the canonical (single-definition) bridge: every call
// site in this crate that needs to bind or read a UUID or a JSON value goes
// through these instead of re-deriving the `u128`/`String` conversion
// inline.

/// Convert a [`uuid::Uuid`] into an [`oxisql_core::Value::Uuid`] for binding
/// as a query parameter.
///
/// # Why `Value`, not a bare `u128`
///
/// `oxisql-core` 0.3.2 has an asymmetry between its read and write paths for
/// the `Uuid` representation: [`oxisql_core::FromValue`] is implemented for
/// `u128` (so *reading* a `Value::Uuid` column back as `u128` works, which is
/// what [`uuid_from_row`]/[`opt_uuid_from_row`] rely on), but
/// [`oxisql_core::ToSqlValue`] is **not** implemented for `u128` — only for
/// the primitive types (`i64`, `i32`, `f64`, `str`, `String`, `bool`,
/// `Vec<u8>`) and, notably, for [`oxisql_core::Value`] itself. Binding a bare
/// `u128` (e.g. `&[&uuid_param(&id)]` when `uuid_param` returned `u128`)
/// therefore fails to compile with "the trait bound `u128: ToSqlValue` is not
/// satisfied" at every call site.
///
/// Returning `Value::Uuid(u.as_u128())` instead sidesteps the gap entirely:
/// `Value` already implements `ToSqlValue` (`to_value` is just `self.clone()`
/// — see `oxisql-core`'s `traits.rs`), so every existing `&task_id_param`
/// binding call site keeps compiling unchanged; only this function's return
/// type is the wrapping enum variant, not the raw representation.
///
/// # Example
///
/// ```
/// use celers_worker::row_ext::uuid_param;
///
/// let id = uuid::Uuid::new_v4();
/// let bound = uuid_param(&id);
/// assert!(matches!(bound, oxisql_core::Value::Uuid(raw) if raw == id.as_u128()));
///
/// // At a call site the value is bound like any other parameter:
/// // conn.execute("INSERT INTO t (id) VALUES ($1)", &[&bound]).await?;
/// ```
#[allow(dead_code)]
pub fn uuid_param(u: &uuid::Uuid) -> oxisql_core::Value {
    oxisql_core::Value::Uuid(u.as_u128())
}

/// Read a non-nullable `UUID` column as a [`uuid::Uuid`].
///
/// Delegates to [`oxisql_core::FromValue`] for `u128` (which matches
/// `Value::Uuid` only — any other variant, including `Value::Null`, is a
/// [`OxiSqlError::TypeMismatch`]) and converts the result via
/// [`uuid::Uuid::from_u128`].
#[allow(dead_code)]
pub fn uuid_from_row(row: &Row, col: &str) -> Result<uuid::Uuid, OxiSqlError> {
    Ok(uuid::Uuid::from_u128(row.try_get::<u128>(col)?))
}

/// Read a nullable `UUID` column as an `Option<uuid::Uuid>`.
///
/// `NULL` maps to `Ok(None)` (via `FromValue for Option<u128>`); any
/// non-null, non-UUID value is a [`OxiSqlError::TypeMismatch`].
#[allow(dead_code)]
pub fn opt_uuid_from_row(row: &Row, col: &str) -> Result<Option<uuid::Uuid>, OxiSqlError> {
    Ok(row.try_get::<Option<u128>>(col)?.map(uuid::Uuid::from_u128))
}

/// Serialize a [`serde_json::Value`] to its `String` wire form for binding as
/// a query parameter (oxisql-core has no `ToSqlValue` impl for `Value::Json`
/// itself — a plain `&str`/`String` parameter is bound and the column's SQL
/// type, e.g. Postgres `JSON`/`JSONB`, handles the cast on the server side).
///
/// # Example
///
/// ```
/// use celers_worker::row_ext::json_param;
///
/// let payload = serde_json::json!({ "k": "v" });
/// assert_eq!(json_param(&payload), r#"{"k":"v"}"#);
///
/// // At a call site the string is bound like any other text parameter:
/// // conn.execute("INSERT INTO t (data) VALUES ($1)", &[&json_param(&payload)]).await?;
/// ```
#[allow(dead_code)]
pub fn json_param(v: &serde_json::Value) -> String {
    v.to_string()
}

/// Read a `JSON`/`JSONB` column as a [`serde_json::Value`].
///
/// Reads the column as `Option<String>` (covers both a `NULL` SQL value and
/// backends that surface JSON via `Value::Text` rather than `Value::Json`),
/// then parses the text as JSON. A `NULL` column or an empty/missing string
/// maps to [`serde_json::Value::Null`] rather than an error, matching the
/// common "absent JSON column" convention.
///
/// # Errors
///
/// Returns [`OxiSqlError::Other`] wrapping the `serde_json` parse error if
/// the column contains a non-empty string that is not valid JSON.
#[allow(dead_code)]
pub fn json_from_row(row: &Row, col: &str) -> Result<serde_json::Value, OxiSqlError> {
    let s: Option<String> = row.try_get(col)?;
    Ok(s.map(|s| serde_json::from_str(&s))
        .transpose()
        .map_err(|e| OxiSqlError::Other(format!("invalid JSON in column '{col}': {e}")))?
        .unwrap_or(serde_json::Value::Null))
}

// ── DateTime<Utc> parameter convention ──────────────────────────────────────
//
// oxisql-core provides `FromValue for chrono::DateTime<Utc>` (behind the
// opt-in `chrono` feature) for the READ side only — there is no `ToSqlValue`
// impl for `DateTime<Utc>`, so it cannot be passed directly in a
// `&[&dyn ToSqlValue]` parameter slice.
//
// `oxisql-postgres` sends every bound parameter in Postgres **binary** wire
// format, tagged with whatever type the server inferred from SQL context.
// `i64`'s `ToSql` impl writes raw 8-byte `INT8` binary bytes regardless of
// the declared target type (`OwnedParam`'s own `accepts()` unconditionally
// returns `true`, bypassing the one safety check — `ToSql::accepts()` — that
// would normally reject this at the Rust layer). When `$n` is inferred as
// `TIMESTAMPTZ`, Postgres's binary `timestamptz` decoder receives an 8-byte
// `INT8` payload it cannot parse as a timestamp and rejects the bind
// outright. A plain RFC3339 `String` has the same failure mode (`String`'s
// binary format is raw UTF-8 bytes, which Postgres's binary `timestamptz`
// decoder also does not accept — binary format for `TIMESTAMPTZ` is a fixed
// 8-byte microseconds-since-2000-01-01 encoding, not textual).
//
// The safe, verified-correct convention used throughout this crate instead:
// bind the RFC3339 string via a `$n::text::timestamptz` cast in the SQL text
// itself. The inner `::text` cast makes Postgres infer `$n`'s parameter type
// as `TEXT`, for which `String`'s binary wire format IS just the raw UTF-8
// bytes (text and binary format are identical for `TEXT`/`VARCHAR` — there
// is no separate "compact" encoding), so the bind round-trips correctly
// regardless of the client always using binary format. The outer
// `::timestamptz` cast then converts the value server-side, exactly as if a
// `TIMESTAMPTZ`-typed literal had been used.
//
// ```text
// let now: chrono::DateTime<chrono::Utc> = chrono::Utc::now();
// conn.execute(
//     "UPDATE t SET seen_at = $1::text::timestamptz WHERE id = $2",
//     &[&now.to_rfc3339(), &id],
// ).await?;
// ```
//
// Always use `.to_rfc3339()` (not `.timestamp()`) for every `DateTime<Utc>`
// parameter bound in this crate, and always pair it with a
// `$n::text::timestamptz` cast at every placeholder that targets a
// `TIMESTAMPTZ`/`TIMESTAMP` column — never a bare `$n`. There is
// intentionally no wrapper function for this conversion (`.to_rfc3339()` is
// already a single, unambiguous call and the SQL-side cast must be visible
// at the call site to be auditable); the convention is documented here so
// this crate's `DateTime<Utc>` parameter binds are all consistent with each
// other.
//
// `dlq_storage.rs`'s `PostgresDlqStorage` does not currently bind any
// `DateTime<Utc>` parameters — its timestamp columns (`dlq_timestamp`,
// `original_timestamp`) are `BIGINT` Unix-seconds values bound as plain
// `i64`, which has no binary-format hazard (the column's declared SQL type
// is itself `BIGINT`/`INT8`, matching `i64`'s binary encoding exactly). This
// convention is documented here regardless, for any future column added to
// that table with a native `TIMESTAMPTZ` type.

#[cfg(test)]
mod tests {
    use super::*;
    use oxisql_core::Value;

    fn row_with(col: &str, value: Value) -> Row {
        Row::new(vec![col.to_string()], vec![value])
    }

    #[test]
    fn col_extracts_matching_type() {
        let row = row_with("n", Value::I64(42));
        let n: i64 = row.col("n").expect("i64 column");
        assert_eq!(n, 42);
    }

    #[test]
    fn col_errors_on_missing_column_without_panicking() {
        let row = row_with("n", Value::I64(42));
        let result: Result<i64, _> = row.col("missing");
        assert!(result.is_err());
    }

    #[test]
    fn col_errors_on_type_mismatch_without_panicking() {
        let row = row_with("n", Value::Text("not a number".to_string()));
        let result: Result<i64, _> = row.col("n");
        assert!(matches!(result, Err(OxiSqlError::TypeMismatch { .. })));
    }

    #[test]
    fn col_idx_extracts_by_position_for_unaliased_expressions() {
        // Mirrors `SELECT 1` / `SELECT now() - x`, where the driver-assigned
        // column name (`?column?` on Postgres) should never be guessed.
        let row = Row::new(vec!["?column?".to_string()], vec![Value::I64(1)]);
        let n: i64 = row.col_idx(0).expect("positional column 0");
        assert_eq!(n, 1);
    }

    #[test]
    fn col_idx_errors_on_out_of_range_index_without_panicking() {
        let row = row_with("n", Value::I64(42));
        let result: Result<i64, _> = row.col_idx(5);
        assert!(result.is_err());
    }

    #[test]
    fn uuid_roundtrip_via_param_and_row() {
        let id = uuid::Uuid::from_u128(0x1234_5678_9abc_def0_1234_5678_9abc_def0);
        // `uuid_param` now returns `oxisql_core::Value::Uuid(..)` directly
        // (see its doc comment for why a bare `u128` does not satisfy
        // `ToSqlValue`), so it is already the row value to store — no extra
        // `Value::Uuid(..)` wrapping needed here.
        let param = uuid_param(&id);
        let row = row_with("id", param);
        let back = uuid_from_row(&row, "id").expect("uuid column");
        assert_eq!(back, id);
    }

    #[test]
    fn opt_uuid_from_row_handles_null() {
        let row = row_with("id", Value::Null);
        let back = opt_uuid_from_row(&row, "id").expect("nullable uuid column");
        assert_eq!(back, None);
    }

    #[test]
    fn opt_uuid_from_row_handles_present_value() {
        let id = uuid::Uuid::new_v4();
        let row = row_with("id", Value::Uuid(id.as_u128()));
        let back = opt_uuid_from_row(&row, "id").expect("nullable uuid column");
        assert_eq!(back, Some(id));
    }

    #[test]
    fn json_roundtrip_via_param_and_row() {
        let payload = serde_json::json!({ "k": "v", "n": 1 });
        let param = json_param(&payload);
        let row = row_with("data", Value::Text(param));
        let back = json_from_row(&row, "data").expect("json column");
        assert_eq!(back, payload);
    }

    #[test]
    fn json_from_row_handles_null_as_json_null() {
        let row = row_with("data", Value::Null);
        let back = json_from_row(&row, "data").expect("nullable json column");
        assert_eq!(back, serde_json::Value::Null);
    }

    #[test]
    fn json_from_row_errors_on_invalid_json_without_panicking() {
        let row = row_with("data", Value::Text("not json".to_string()));
        let result = json_from_row(&row, "data");
        assert!(result.is_err());
    }

    #[test]
    fn row_to_macro_maps_named_columns() {
        struct TaskNameRow {
            task_name: String,
        }
        let row = row_with("task_name", Value::Text("my_task".to_string()));
        let mapped = row_to!(TaskNameRow {
            task_name: "task_name"
        })(&row)
        .expect("mapped row");
        assert_eq!(mapped.task_name, "my_task");
    }
}
