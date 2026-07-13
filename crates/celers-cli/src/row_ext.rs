//! Reusable row-mapping helpers for the OxiSQL-backed connectivity probes.
//!
//! Every call site that used to go through the previous SQL toolkit's
//! `query_as::<_, (T, ...)>(..)` or `row.get::<T, _>("col")` (which panics on
//! a schema/type mismatch) now goes through [`RowExt::col`], which wraps
//! [`oxisql_core::Row::try_get`] and returns a `Result` instead of panicking.
//! This keeps column extraction consistent and panic-free across all call
//! sites, and is the same pattern documented in `oxify-storage`'s
//! `row_ext.rs` (the precedent this module is adapted from).
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
//!
//! This crate's own OxiSQL call sites are shallow connectivity/health-check
//! probes (`SELECT version()`, `SELECT 1`, `pg_database_size`, ...) and do
//! not currently exercise the `uuid`/`json` helpers below — they are built
//! anyway, to the same design, because this module is the reusable template
//! that later (higher-stakes) migrations in other crates copy verbatim.

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
    /// expression like `now() - x`). Postgres and MySQL both assign an
    /// implicit display name to unaliased expressions (Postgres emits the
    /// literal string `"?column?"` for anything that is not a bare column
    /// reference or a single unadorned function call; MySQL echoes the exact
    /// expression source text), and guessing that string is fragile —
    /// positional access sidesteps the guess entirely. A bare, unaliased
    /// function call (`SELECT version()`, `SELECT count(*)`) is the one case
    /// where the implicit name is well-defined (the function name) and
    /// [`RowExt::col`] is safe and preferred for readability.
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
/// Usage mirrors the shape of the target struct:
///
/// ```ignore
/// let versions: Vec<VersionRow> = rows
///     .iter()
///     .map(row_to!(VersionRow { version: "version" }))
///     .collect::<Result<_, _>>()?;
/// ```
///
/// This crate's own call sites map single-column probe results by hand
/// (see [`RowExt::col`] usage in `commands/database.rs` and `database.rs`)
/// rather than through this macro, since none of them decode into a
/// multi-field struct — it is included, unused-but-correct, because this
/// module is the reusable template later (higher-stakes, multi-column)
/// migrations copy verbatim.
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
// site that needs to bind or read a UUID or a JSON value goes through these
// instead of re-deriving the `u128`/`String` conversion inline.

/// Convert a [`uuid::Uuid`] into the `u128` representation used by
/// [`oxisql_core::Value::Uuid`] for binding as a query parameter.
///
/// # Example
///
/// ```ignore
/// let id = Uuid::new_v4();
/// conn.execute("INSERT INTO t (id) VALUES ($1)", &[&uuid_param(&id)]).await?;
/// ```
#[allow(dead_code)]
pub fn uuid_param(u: &uuid::Uuid) -> u128 {
    u.as_u128()
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
/// ```ignore
/// let payload = serde_json::json!({ "k": "v" });
/// conn.execute("INSERT INTO t (data) VALUES ($1)", &[&json_param(&payload)]).await?;
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
// `&[&dyn ToSqlValue]` parameter slice. When a `DateTime<Utc>` must be bound
// as a query parameter, convert it to Unix seconds first:
//
// ```ignore
// let now: chrono::DateTime<chrono::Utc> = chrono::Utc::now();
// let secs: i64 = now.timestamp(); // Unix seconds, matches `i64: ToSqlValue`
// conn.execute("UPDATE t SET seen_at = $1 WHERE id = $2", &[&secs, &id]).await?;
// ```
//
// Always use `.timestamp()` (whole Unix seconds, `i64`) — not
// `.timestamp_millis()`/`.timestamp_micros()` — for consistency across every
// call site and every future migration that copies this module. There is
// intentionally no wrapper function for this conversion (`.timestamp()` is
// already a single, unambiguous call); the convention is documented here so
// later migrations bind `DateTime<Utc>` parameters identically.

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
        let param = uuid_param(&id);
        let row = row_with("id", Value::Uuid(param));
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
        struct VersionRow {
            version: String,
        }
        let row = row_with("version", Value::Text("PostgreSQL 16.2".to_string()));
        let mapped = row_to!(VersionRow { version: "version" })(&row).expect("mapped row");
        assert_eq!(mapped.version, "PostgreSQL 16.2");
    }
}
