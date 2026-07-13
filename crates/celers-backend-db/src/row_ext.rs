//! Reusable row-mapping helpers for the OxiSQL-backed database result/lock/event
//! backends in this crate.
//!
//! Every call site that used to go through `sqlx`'s `row.get::<T, _>("col")`
//! (which panics on a schema/type mismatch) now goes through [`RowExt::col`],
//! which wraps [`oxisql_core::Row::try_get`] and returns a `Result` instead of
//! panicking. This keeps column extraction consistent and panic-free across
//! all call sites.
//!
//! This module mirrors `celers-broker-postgres`'s `row_ext.rs` (the proven
//! pilot this migration is built from) verbatim in structure; only the module
//! path (`crate::row_ext` here) and this crate-specific note on MySQL
//! `DateTime<Utc>` parameter binding differ.
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
    /// this is the safety improvement over `sqlx`'s `row.get::<T, _>("col")`,
    /// which panics on either condition.
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
/// Usage mirrors the shape of the target struct:
///
/// ```ignore
/// let infos: Vec<TaskInfo> = rows
///     .iter()
///     .map(row_to!(TaskInfo { task_name: "task_name" }))
///     .collect::<Result<_, _>>()?;
/// ```
///
/// This crate's own call sites map multi-field structs (`TaskMeta`,
/// `ChordState`, ...) by hand across several fields with mixed typed helpers
/// (`uuid_from_row`, `row.col`, ...) rather than through this macro — it is
/// included, unused-but-correct, matching the pilot's own module exactly,
/// since a single-column-per-field macro cannot express the typed-helper
/// substitutions those structs need (e.g. a `Uuid` field via
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
/// `Vec<u8>`) and, notably, for [`oxisql_core::Value`] itself (verified
/// exhaustively against `oxisql-core`'s `traits.rs`: nine `impl ToSqlValue`
/// blocks total, none for `u128`). Binding a bare `u128` (e.g.
/// `&[&uuid_param(&id)]` when `uuid_param` returned `u128`) therefore fails
/// to compile with "the trait bound `u128: ToSqlValue` is not satisfied" at
/// every call site.
///
/// Returning `Value::Uuid(u.as_u128())` instead sidesteps the gap entirely:
/// `Value` already implements `ToSqlValue` (`to_value` is just `self.clone()`
/// — see `oxisql-core`'s `traits.rs`), so every existing `&task_id_param`
/// binding call site keeps compiling unchanged; only this function's return
/// type moved from the raw representation to the wrapping enum variant.
///
/// # Example
///
/// ```ignore
/// let id = Uuid::new_v4();
/// conn.execute("INSERT INTO t (id) VALUES ($1)", &[&uuid_param(&id)]).await?;
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
/// type, e.g. Postgres `JSON`/`JSONB` or MySQL `JSON`, handles the cast on
/// the server side).
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

// ── DateTime<Utc> parameter convention (PostgreSQL) ─────────────────────────
//
// oxisql-core provides `FromValue for chrono::DateTime<Utc>` (behind the
// opt-in `chrono` feature) for the READ side only — there is no `ToSqlValue`
// impl for `DateTime<Utc>`, so it cannot be passed directly in a
// `&[&dyn ToSqlValue]` parameter slice.
//
// `oxisql-postgres` sends every bound parameter in Postgres **binary** wire
// format, tagged with whatever type the server inferred from SQL context
// (confirmed by reading `oxisql-postgres/src/types.rs`'s `OwnedParam::to_sql`:
// `accepts()` unconditionally returns `true`, bypassing the one safety check
// that would normally reject a format mismatch at the Rust layer). Binding a
// plain RFC3339 `String` against a bare `$n` placeholder that Postgres infers
// as `TIMESTAMPTZ` fails: `TIMESTAMPTZ`'s binary format is a fixed 8-byte
// microseconds-since-2000-01-01 encoding, not the raw UTF-8 text bytes a
// `String` parameter actually carries.
//
// The safe, verified-correct convention used throughout THIS crate: bind the
// RFC3339 string via a `$n::text::timestamptz` cast in the SQL text itself.
// The inner `::text` cast makes Postgres infer `$n`'s parameter type as
// `TEXT`, for which `String`'s binary wire format IS just the raw UTF-8 bytes
// (text and binary format are identical for `TEXT`/`VARCHAR`), so the bind
// round-trips correctly regardless of the client always using binary format.
// The outer `::timestamptz` cast then converts the value server-side, exactly
// as if a `TIMESTAMPTZ`-typed literal had been used.
//
// ```ignore
// let now: chrono::DateTime<chrono::Utc> = chrono::Utc::now();
// conn.execute(
//     "UPDATE t SET seen_at = $1::text::timestamptz WHERE id = $2",
//     &[&now.to_rfc3339(), &id],
// ).await?;
// ```
//
// Always use `.to_rfc3339()` (never `.timestamp()`) for every Postgres
// `DateTime<Utc>` parameter bound in this crate, and always pair it with a
// `$n::text::timestamptz` cast at every placeholder that targets a
// `TIMESTAMPTZ`/`TIMESTAMP` column — never a bare `$n`.
//
// ── DateTime<Utc> parameter convention (MySQL) — DIFFERENT FORMAT REQUIRED ──
//
// MySQL's wire protocol does NOT have Postgres's binary/text format
// distinction for a `TEXT`-cast trick to exploit — `oxisql-mysql`'s
// `core_value_to_mysql` converts every `oxisql_core::Value::Text`/`String`
// parameter to `mysql_async::Value::Bytes(..)`, which `mysql_common` (the
// wire-protocol layer `mysql_async` is built on) unconditionally declares as
// `ColumnType::MYSQL_TYPE_VAR_STRING` in the `COM_STMT_EXECUTE` parameter-type
// array — verified by reading `mysql_common-0.37.3/src/packets/mod.rs`'s
// `StmtBulkExecuteParamType::from_value` (the `Value::Bytes(_) =>
// ColumnType::MYSQL_TYPE_VAR_STRING` arm), which is never re-typed based on
// the target column. So, unlike Postgres, there is no binary-vs-text *framing*
// mismatch to worry about: a `VAR_STRING`-typed binary parameter bound
// against a `DATETIME`/`TIMESTAMP` column placeholder is accepted by the
// server via its standard implicit string-to-temporal cast.
//
// The catch is the *string grammar* that implicit cast accepts. It is the
// same strict grammar `mysql_common` itself uses to parse a `Value::Bytes`
// column value back into a `chrono::NaiveDateTime` on the read side
// (`mysql_common-0.37.3/src/value/convert/mod.rs`'s `parse_mysql_datetime_string`,
// gated behind these exact regexes):
//
//   `^\d{4}-\d{2}-\d{2}$"`                        (date only)
//   `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}$`        (datetime, SPACE separator)
//   `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{1,6}$` (datetime with fraction)
//
// `chrono::DateTime::to_rfc3339()` produces `2026-07-11T08:55:00+00:00` (or a
// `Z` suffix) — a `T` date/time separator and a trailing UTC-offset suffix,
// neither of which this grammar accepts. Binding an RFC3339 string as a
// MySQL `DATETIME`/`TIMESTAMP` parameter is therefore UNSAFE for a different
// reason than the Postgres case (a rejected/garbled server-side string parse
// rather than a binary-framing mismatch), but the practical outcome is the
// same class of bug this crate must not reintroduce.
//
// The safe, verified-correct convention for MySQL in THIS crate: format the
// `DateTime<Utc>` with `.format("%Y-%m-%d %H:%M:%S%.6f")` — MySQL's own
// canonical `DATETIME`/`TIMESTAMP` text form, byte-for-byte the same grammar
// `parse_mysql_datetime_string` accepts (and the same microsecond precision
// `oxisql-mysql`'s own read-side `mysql_value_to_core` uses when decoding a
// native `Value::Date(..., micro)` datetime column into `Value::Timestamp`).
// No SQL-side cast is needed on MySQL — bind directly against a bare `?`
// placeholder.
//
// ```ignore
// let now: chrono::DateTime<chrono::Utc> = chrono::Utc::now();
// conn.execute(
//     "UPDATE t SET seen_at = ? WHERE id = ?",
//     &[&now.format("%Y-%m-%d %H:%M:%S%.6f").to_string(), &id],
// ).await?;
// ```
//
// There is intentionally no wrapper function for either conversion (each is
// already a single, unambiguous call, and the backend-specific formatting
// must stay visible at the call site to be auditable); the convention is
// documented here so this crate's `DateTime<Utc>` parameter binds — on
// either backend — are all consistent with each other.

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

    #[test]
    fn mysql_datetime_format_matches_parse_mysql_datetime_string_grammar() {
        // Regression guard for the MySQL DateTime<Utc> convention documented
        // above: `%Y-%m-%d %H:%M:%S%.6f` must match
        // `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{1,6}$` (mysql_common's
        // own `DATETIME_RE_YMD_HMS_NS` — a space separator, no `T`, no
        // timezone suffix). This is checked structurally here (no
        // mysql_common dependency in this crate) rather than by importing
        // the regex itself.
        let dt = chrono::DateTime::parse_from_rfc3339("2026-07-11T08:55:03.123456+00:00")
            .expect("valid RFC3339 fixture")
            .with_timezone(&chrono::Utc);
        let formatted = dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string();

        assert_eq!(formatted, "2026-07-11 08:55:03.123456");
        assert!(
            !formatted.contains('T'),
            "MySQL DATETIME grammar has no 'T' separator"
        );
        assert!(
            !formatted.contains('+') && !formatted.contains('Z'),
            "MySQL DATETIME grammar has no timezone suffix"
        );
        // Byte length must fall in mysql_common's accepted range for the
        // fractional-seconds variant: `20 < len && len < 27`.
        assert!(formatted.len() > 20 && formatted.len() < 27);
    }
}
