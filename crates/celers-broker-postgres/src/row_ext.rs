//! Reusable row-mapping helpers for the OxiSQL-backed task-delivery path.
//!
//! Every call site that used to go through the previous SQL toolkit's
//! `row.get::<T, _>("col")` (which panics on a schema/type mismatch) now goes
//! through [`RowExt::col`], which wraps [`oxisql_core::Row::try_get`] and
//! returns a `Result` instead of panicking. This keeps column extraction
//! consistent and panic-free across all call sites.
//!
//! This module mirrors `celers-cli`'s `row_ext.rs` (the proven pilot this
//! migration is built from) verbatim in structure; only the module path
//! (`crate::row_ext` here vs. the pilot's dual bin+lib tree) differs.
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
/// Usage mirrors the shape of the target struct:
///
/// ```text
/// let infos: Vec<TaskInfo> = rows
///     .iter()
///     .map(row_to!(TaskInfo { task_name: "task_name" }))
///     .collect::<Result<_, _>>()?;
/// ```
///
/// (Illustrative only — exercising it for real needs a live `Row`, which
/// this crate has no in-process way to construct outside a real query.)
///
/// This crate's own call sites map multi-field structs (`TaskInfo`,
/// `TaskResult`, ...) by hand across several fields with mixed typed helpers
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
// inline. This is the highest-review-priority crate in the migration
// (`celers-broker-postgres`'s task-delivery hot path), so consistency here
// matters more than almost anywhere else in the workspace.

/// Convert a [`uuid::Uuid`] into an [`oxisql_core::Value::Uuid`] for binding
/// as a query parameter.
///
/// # ⚠️ Only correct against a `$n::text::uuid` placeholder
///
/// **Never bind the returned value against a bare `$n` that PostgreSQL infers
/// as `uuid`** (e.g. `WHERE id = $1` on a `id UUID` column). It fails against
/// a live server every time, with
/// `incorrect binary data format in bind parameter n`:
///
/// - `oxisql-postgres`'s `value_to_param` converts `Value::Uuid(u)` into
///   `OwnedParam::Text(format!("{}", Value::Uuid(u)))` — the 36-character
///   hyphenated text form, e.g. `"3fa85f64-5717-4562-b3fc-2c963f66afa6"`.
/// - `OwnedParam::accepts()` unconditionally returns `true`, bypassing the
///   `to_sql_checked!` guard that would otherwise reject a text payload for a
///   `uuid`-typed placeholder before it reached the wire.
/// - `postgres-types`' `impl ToSql for String` (which `OwnedParam::Text`
///   delegates to) writes the raw UTF-8 bytes whatever the server-inferred
///   parameter type is, and `tokio-postgres` always binds parameters in
///   PostgreSQL **binary** format.
/// - PostgreSQL's binary `uuid_recv` expects exactly 16 bytes, so a 36-byte
///   text payload announced as a binary `uuid` is rejected outright.
///
/// This is the same hazard documented below for `DateTime<Utc>` parameters,
/// and the fix has the same shape: pin the placeholder's inferred type to
/// `text` — for which a `String`'s text and binary encodings are identical —
/// with an inner `::text` cast, then convert server-side with an outer
/// `::uuid` cast:
///
/// ```ignore
/// let id: uuid::Uuid = ...;
/// conn.execute(
///     "DELETE FROM celers_tasks WHERE id = $1::text::uuid",
///     &[&uuid_param(&id)],
/// ).await?;
/// ```
///
/// Every UUID-column placeholder in this crate carries that cast (see
/// `crate::sql`'s "Binding conventions"), including the generated
/// `IN ($1::text::uuid, ..)` lists. Adding a UUID parameter without one is a
/// silent, server-rejected write, so the cast must be visible at the call
/// site to stay auditable — which is why there is no wrapper hiding it.
///
/// Columns that merely *hold* a UUID as text (`celers_periodic_schedules
/// .schedule_id` and `celers_queue_snapshots.snapshot_id` are both
/// `VARCHAR(36)`) are the exact opposite case: PostgreSQL infers `varchar`
/// there, the text payload is already right, and adding a `::uuid` cast would
/// break the comparison. Bind those as plain `String`s, as this crate does.
///
/// The READ side ([`uuid_from_row`]/[`opt_uuid_from_row`]) decodes the
/// server's native binary `uuid` column encoding — an entirely different code
/// path, with no such hazard.
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
/// satisfied" at every call site — this was a real (if latent) compile-time
/// gap in this crate's pre-existing `sqlx` → `oxisql` migration until fixed
/// here, unrelated to `celers-cli`'s TLS-downgrade fix.
///
/// Returning `Value::Uuid(u.as_u128())` instead sidesteps the gap entirely:
/// `Value` already implements `ToSqlValue` (`to_value` is just `self.clone()`
/// — see `oxisql-core`'s `traits.rs`), so every existing `&task_id_param`
/// binding call site keeps compiling unchanged; only this function's return
/// type moved from the raw representation to the wrapping enum variant.
///
/// # Example
///
/// ```
/// use celers_broker_postgres::row_ext::uuid_param;
/// use oxisql_core::Value;
/// use uuid::Uuid;
///
/// let id = Uuid::new_v4();
/// let param = uuid_param(&id);
/// assert_eq!(param, Value::Uuid(id.as_u128()));
/// // Bind it — note the mandatory cast:
/// // conn.execute("INSERT INTO t (id) VALUES ($1::text::uuid)", &[&param]).await?;
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
/// itself, so a plain `&str`/`String` parameter is bound instead).
///
/// # The `::text::jsonb` cast is mandatory on PostgreSQL
///
/// The returned `String` carries JSON *text*. On PostgreSQL's extended
/// (prepared-statement) protocol the server infers each parameter's type from
/// where it appears, so a bare `$n` written straight into a `JSONB` column is
/// inferred as `jsonb` — and the client then encodes the string in the
/// **binary** `jsonb` representation, whose first byte must be the format
/// version `0x01`. JSON text starts with `{`, `[`, `"`, a digit or `n`, so the
/// server rejects the value with `ERROR: unsupported jsonb version number 123`
/// (`{`), `91` (`[`), `110` (`n`), and the whole statement fails.
///
/// Binding through an explicit `$n::text::jsonb` pins the parameter's inferred
/// type to `text` — which *is* transferred verbatim — and casts it to `jsonb`
/// server-side, where the ordinary JSON text parser runs. It is the same idiom
/// the UUID and timestamp parameters in this crate use
/// (`$n::text::uuid`, `$n::text::timestamptz`); see `crate::sql`'s "Binding
/// conventions", which its unit tests assert on.
///
/// # Example
///
/// ```
/// use celers_broker_postgres::row_ext::json_param;
///
/// let payload = serde_json::json!({ "k": "v" });
/// let param = json_param(&payload);
/// assert_eq!(param, r#"{"k":"v"}"#);
/// // Bind it — note the mandatory cast; `VALUES ($1)` alone is rejected:
/// // conn.execute("INSERT INTO t (data) VALUES ($1::text::jsonb)", &[&param]).await?;
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

// ── Numeric column convention (NUMERIC / DECIMAL) ───────────────────────────
//
// PostgreSQL returns `NUMERIC` for a whole family of expressions this crate
// reads as plain numbers:
//
//   * `EXTRACT(EPOCH FROM interval)` and `EXTRACT(EPOCH FROM timestamptz)` —
//     `numeric` since PostgreSQL 14 (they were `double precision` before);
//   * `SUM()` / `AVG()` over an exact-value argument (`bigint`, `numeric`);
//   * any arithmetic whose operands include a `numeric`, so
//     `EXTRACT(EPOCH FROM (completed_at - started_at)) * 1000` is `numeric`
//     too, and so is a `PERCENTILE_CONT` fed from one.
//
// `oxisql-postgres` maps `Type::NUMERIC` to `oxisql_core::Value::Decimal`
// (a string), and `oxisql_core::FromValue` is implemented for `f64`/`i64`
// against `Value::F64`/`Value::I64` **only** — never `Value::Decimal`. A
// direct `row.col::<f64>(..)` on such a column therefore fails with
// `OxiSqlError::TypeMismatch`, at run time, against a real server, on a
// PostgreSQL version boundary nobody was thinking about.
//
// Until now this crate defended against that in the SQL text, by appending an
// explicit `::double precision` / `::BIGINT` cast to every aggregate
// projection. That works, but it is enforced by convention across three dozen
// hand-written statements: one new aggregate written without the cast is a
// live-server failure with no local test that can catch it.
//
// The helpers below make the *read* side self-defending instead. They inspect
// the raw `Value` (via `Row::get`, bypassing `FromValue`) and accept `F64`,
// `I64` **and** `Decimal`, so the read is correct whether or not the
// statement carried a cast, and on either side of the PostgreSQL 14 change.
// Ported from `celers-backend-db`'s `row_ext.rs`, which reached the same
// conclusion from the MySQL `DECIMAL` side.
//
// Prefer these over `row.col::<f64/i64>(..)` for any column derived from
// `SUM`/`AVG`/`MIN`/`MAX`/`STDDEV`/`PERCENTILE_CONT`/`EXTRACT`/division —
// i.e. anything that is not provably `COUNT(*)` (always `bigint`/`I64`) or a
// plain stored column.

/// The shared `Value` -> `Option<f64>` core of the `*_decimal_f64_*` helpers.
///
/// `label` names the column (or index) for error messages only.
fn opt_decimal_f64_from_value(
    value: &oxisql_core::Value,
    label: &str,
) -> Result<Option<f64>, OxiSqlError> {
    match value {
        oxisql_core::Value::Null => Ok(None),
        oxisql_core::Value::F64(f) => Ok(Some(*f)),
        #[allow(clippy::cast_precision_loss)]
        oxisql_core::Value::I64(n) => Ok(Some(*n as f64)),
        oxisql_core::Value::Decimal(s) => s.trim().parse::<f64>().map(Some).map_err(|e| {
            OxiSqlError::Other(format!("invalid NUMERIC in column '{label}': {e} ({s:?})"))
        }),
        other => Err(OxiSqlError::TypeMismatch {
            expected: "F64/I64/Decimal",
            got: other.type_name(),
        }),
    }
}

/// The shared `Value` -> `i64` core of the `*_decimal_i64_*` helpers.
fn decimal_i64_from_value(value: &oxisql_core::Value, label: &str) -> Result<i64, OxiSqlError> {
    match value {
        oxisql_core::Value::I64(n) => Ok(*n),
        #[allow(clippy::cast_possible_truncation)]
        oxisql_core::Value::F64(f) => Ok(*f as i64),
        oxisql_core::Value::Decimal(s) => s.trim().parse::<i64>().or_else(|_| {
            // `EXTRACT(EPOCH FROM ...)` yields a scaled decimal
            // (`"3600.000000"`), which does not parse as `i64` even though it
            // is integral. Fall back to parsing as `f64` and rounding rather
            // than failing on a well-formed integral decimal.
            #[allow(clippy::cast_possible_truncation)]
            s.trim()
                .parse::<f64>()
                .map(|f| f.round() as i64)
                .map_err(|e| {
                    OxiSqlError::Other(format!("invalid NUMERIC in column '{label}': {e} ({s:?})"))
                })
        }),
        other => Err(OxiSqlError::TypeMismatch {
            expected: "I64/F64/Decimal",
            got: other.type_name(),
        }),
    }
}

/// Read a numeric column as `f64`, accepting `Value::F64`, `Value::I64`, or
/// `Value::Decimal` (PostgreSQL `NUMERIC`).
///
/// # Errors
///
/// Returns [`OxiSqlError::TypeMismatch`] if the column holds any other
/// variant (including `Null` — use [`opt_decimal_f64_from_row`] for a
/// nullable column), or [`OxiSqlError::Other`] if a `Decimal` string fails to
/// parse as `f64`.
pub fn decimal_f64_from_row(row: &Row, col: &str) -> Result<f64, OxiSqlError> {
    opt_decimal_f64_from_row(row, col)?.ok_or(OxiSqlError::TypeMismatch {
        expected: "F64/I64/Decimal",
        got: "Null",
    })
}

/// Read a nullable numeric column as `Option<f64>`, accepting `Value::F64`,
/// `Value::I64`, `Value::Decimal`, or `Value::Null` (-> `None`).
///
/// `AVG(...)`/`SUM(...)`/`STDDEV(...)` over **zero rows** is `NULL`, not `0`,
/// so this — not [`decimal_f64_from_row`] — is the right helper for any
/// aggregate over a window that can legitimately be empty, which on a fresh
/// deployment is all of them.
///
/// # Errors
///
/// Returns [`OxiSqlError::TypeMismatch`] if the column holds a non-numeric,
/// non-null variant, or [`OxiSqlError::Other`] if a `Decimal` string fails to
/// parse as `f64`.
pub fn opt_decimal_f64_from_row(row: &Row, col: &str) -> Result<Option<f64>, OxiSqlError> {
    let value = row
        .get(col)
        .ok_or_else(|| OxiSqlError::Other(format!("column '{col}' not found")))?;
    opt_decimal_f64_from_value(value, col)
}

/// [`opt_decimal_f64_from_row`] for a column addressed by position.
///
/// Same reason [`RowExt::col_idx`] exists: an unaliased `SELECT AVG(...)`
/// projection has no dependable column name to look up.
///
/// # Errors
///
/// As [`opt_decimal_f64_from_row`], plus [`OxiSqlError::Other`] if the row has
/// no column at `idx`.
pub fn opt_decimal_f64_from_row_idx(row: &Row, idx: usize) -> Result<Option<f64>, OxiSqlError> {
    let value = row
        .get_by_index(idx)
        .ok_or_else(|| OxiSqlError::Other(format!("no column at index {idx}")))?;
    opt_decimal_f64_from_value(value, &idx.to_string())
}

/// Read a numeric column as `i64`, accepting `Value::I64`, `Value::F64`
/// (truncating), or `Value::Decimal` (PostgreSQL `NUMERIC`).
///
/// # Errors
///
/// Returns [`OxiSqlError::TypeMismatch`] if the column holds any other
/// variant (including `Null` — use [`opt_decimal_i64_from_row`] for an
/// aggregate that can be `NULL`), or [`OxiSqlError::Other`] if a `Decimal`
/// string parses as neither `i64` nor `f64`.
pub fn decimal_i64_from_row(row: &Row, col: &str) -> Result<i64, OxiSqlError> {
    let value = row
        .get(col)
        .ok_or_else(|| OxiSqlError::Other(format!("column '{col}' not found")))?;
    decimal_i64_from_value(value, col)
}

/// Read a nullable aggregate column as `Option<i64>`, accepting everything
/// [`decimal_i64_from_row`] does plus `Value::Null` (-> `None`).
///
/// `SUM(...)` over **zero rows** is `NULL`, not `0`. Reading such a column
/// with the non-optional [`decimal_i64_from_row`] fails the moment the
/// aggregated window is empty — the normal state of a fresh deployment, not
/// an edge case. Use this and treat `None` as the zero the caller means.
///
/// # Errors
///
/// As [`decimal_i64_from_row`], minus the `Null` case.
pub fn opt_decimal_i64_from_row(row: &Row, col: &str) -> Result<Option<i64>, OxiSqlError> {
    let value = row
        .get(col)
        .ok_or_else(|| OxiSqlError::Other(format!("column '{col}' not found")))?;
    if matches!(value, oxisql_core::Value::Null) {
        return Ok(None);
    }
    decimal_i64_from_value(value, col).map(Some)
}

/// [`opt_decimal_i64_from_row`] for a column addressed by position.
///
/// # Errors
///
/// As [`opt_decimal_i64_from_row`], plus [`OxiSqlError::Other`] if the row has
/// no column at `idx`.
pub fn opt_decimal_i64_from_row_idx(row: &Row, idx: usize) -> Result<Option<i64>, OxiSqlError> {
    let value = row
        .get_by_index(idx)
        .ok_or_else(|| OxiSqlError::Other(format!("no column at index {idx}")))?;
    if matches!(value, oxisql_core::Value::Null) {
        return Ok(None);
    }
    decimal_i64_from_value(value, &idx.to_string()).map(Some)
}

// ── DateTime<Utc> parameter convention ──────────────────────────────────────
//
// oxisql-core provides `FromValue for chrono::DateTime<Utc>` (behind the
// opt-in `chrono` feature) for the READ side only — there is no `ToSqlValue`
// impl for `DateTime<Utc>`, so it cannot be passed directly in a
// `&[&dyn ToSqlValue]` parameter slice.
//
// IMPORTANT DEVIATION FROM THE PILOT'S DOCUMENTED CONVENTION: the pilot's
// own `row_ext.rs` (celers-cli) documents binding `DateTime<Utc>` as
// `.timestamp()` (Unix seconds, `i64`) directly. That convention was
// re-verified from oxisql-postgres 0.3.2's source during this migration
// (`oxisql-postgres/src/types.rs`'s `OwnedParam::to_sql` and
// `postgres-types`' `ToSql::encode_format` defaulting to
// `Format::Binary` unconditionally) and found to be UNSAFE: `oxisql-postgres`
// sends every bound parameter in Postgres **binary** wire format, tagged
// with whatever type the server inferred from SQL context. `i64`'s `ToSql`
// impl writes raw 8-byte `INT8` binary bytes regardless of the declared
// target type (`OwnedParam`'s own `accepts()` unconditionally returns `true`,
// bypassing the one safety check — `ToSql::accepts()` — that would normally
// reject this at the Rust layer). When `$n` is inferred as `TIMESTAMPTZ`
// (e.g. `scheduled_at = $1`), Postgres's binary `timestamptz` decoder
// receives an 8-byte `INT8` payload it cannot parse as a timestamp and
// rejects the bind outright. A plain RFC3339 `String` has the same failure
// mode (`String`'s binary format is raw UTF-8 bytes, which Postgres's binary
// `timestamptz` decoder also does not accept — binary format for
// `TIMESTAMPTZ` is a fixed 8-byte microseconds-since-2000-01-01 encoding,
// not textual).
//
// The safe, verified-correct convention used throughout THIS crate instead:
// bind the RFC3339 string via a `$n::text::timestamptz` cast in the SQL
// text itself. The inner `::text` cast makes Postgres infer `$n`'s
// parameter type as `TEXT`, for which `String`'s binary wire format IS just
// the raw UTF-8 bytes (text and binary format are identical for
// `TEXT`/`VARCHAR` — there is no separate "compact" encoding), so the bind
// round-trips correctly regardless of the client always using binary
// format. The outer `::timestamptz` cast then converts the value
// server-side, exactly as if a `TIMESTAMPTZ`-typed literal had been used.
//
// ```ignore
// let now: chrono::DateTime<chrono::Utc> = chrono::Utc::now();
// // Both parameters are cast: the timestamp for the reason above, the id
// // for the identical reason documented on `uuid_param`.
// conn.execute(
//     "UPDATE celers_tasks SET started_at = $1::text::timestamptz \
//      WHERE id = $2::text::uuid",
//     &[&now.to_rfc3339(), &uuid_param(&id)],
// ).await?;
// ```
//
// Always use `.to_rfc3339()` (not `.timestamp()`) for every `DateTime<Utc>`
// parameter bound in this crate, and always pair it with a `$n::text::timestamptz`
// cast at every placeholder that targets a `TIMESTAMPTZ`/`TIMESTAMP` column —
// never a bare `$n`. There is intentionally no wrapper function for this
// conversion (`.to_rfc3339()` is already a single, unambiguous call and the
// SQL-side cast must be visible at the call site to be auditable); the
// convention is documented here so this crate's `DateTime<Utc>` parameter
// binds are all consistent with each other. This deviates from the pilot's
// documented convention deliberately and is flagged prominently in this
// migration's report — the pilot's own crate (`celers-cli`) has zero actual
// `.timestamp()`-into-a-`timestamptz`-column call sites exercising the
// documented convention, so the unsafety was latent, not yet triggered,
// there.

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

    // ── NUMERIC read guards ─────────────────────────────────────────────
    //
    // The whole point of these helpers is that they accept the variant a
    // plain `row.col::<f64>(..)` rejects, so every test below pins one
    // concrete `Value` variant. `Value::Decimal` is what `oxisql-postgres`
    // hands back for a PostgreSQL `NUMERIC` — i.e. for every uncast
    // `EXTRACT(EPOCH ...)`, `SUM(...)` and `AVG(...)` in the crate.

    #[test]
    fn decimal_f64_accepts_every_numeric_variant() {
        assert_eq!(
            decimal_f64_from_row(&row_with("v", Value::F64(3.5)), "v").expect("f64 column"),
            3.5
        );
        assert_eq!(
            decimal_f64_from_row(&row_with("v", Value::I64(7)), "v").expect("i64 column"),
            7.0
        );
        let decimal = row_with("v", Value::Decimal("12.25".to_string()));
        assert!(
            (decimal_f64_from_row(&decimal, "v").expect("numeric column") - 12.25).abs() < 1e-9
        );
    }

    #[test]
    fn decimal_f64_rejects_null_and_nonsense_without_panicking() {
        assert!(decimal_f64_from_row(&row_with("v", Value::Null), "v").is_err());
        assert!(decimal_f64_from_row(&row_with("v", Value::Decimal("abc".into())), "v").is_err());
        assert!(decimal_f64_from_row(&row_with("v", Value::Text("3.5".into())), "v").is_err());
        assert!(decimal_f64_from_row(&row_with("v", Value::F64(1.0)), "missing").is_err());
    }

    #[test]
    fn opt_decimal_f64_maps_null_to_none() {
        assert_eq!(
            opt_decimal_f64_from_row(&row_with("v", Value::Null), "v").expect("nullable"),
            None
        );
        assert_eq!(
            opt_decimal_f64_from_row(&row_with("v", Value::Decimal("0.5".into())), "v")
                .expect("nullable"),
            Some(0.5)
        );
    }

    #[test]
    fn decimal_i64_accepts_every_numeric_variant() {
        assert_eq!(
            decimal_i64_from_row(&row_with("v", Value::I64(9)), "v").expect("i64 column"),
            9
        );
        assert_eq!(
            decimal_i64_from_row(&row_with("v", Value::F64(9.7)), "v").expect("f64 column"),
            9
        );
        assert_eq!(
            decimal_i64_from_row(&row_with("v", Value::Decimal("42".into())), "v")
                .expect("integral numeric"),
            42
        );
    }

    #[test]
    fn decimal_i64_accepts_a_scaled_integral_numeric() {
        // What `EXTRACT(EPOCH FROM (NOW() - created_at))` actually returns:
        // an integral value carrying a decimal scale, which `"3600.000000"
        // .parse::<i64>()` rejects outright.
        let row = row_with("v", Value::Decimal("3600.000000".to_string()));
        assert_eq!(
            decimal_i64_from_row(&row, "v").expect("scaled integral numeric"),
            3600
        );
        // And a genuinely fractional one rounds rather than failing.
        let row = row_with("v", Value::Decimal("3600.75".to_string()));
        assert_eq!(
            decimal_i64_from_row(&row, "v").expect("fractional numeric"),
            3601
        );
    }

    #[test]
    fn decimal_i64_rejects_null_and_nonsense_without_panicking() {
        assert!(decimal_i64_from_row(&row_with("v", Value::Null), "v").is_err());
        assert!(decimal_i64_from_row(&row_with("v", Value::Decimal("abc".into())), "v").is_err());
        assert!(decimal_i64_from_row(&row_with("v", Value::Text("42".into())), "v").is_err());
        assert!(decimal_i64_from_row(&row_with("v", Value::I64(1)), "missing").is_err());
    }

    #[test]
    fn opt_decimal_i64_maps_an_empty_sum_to_none() {
        // `SUM(...)` over zero rows is NULL in PostgreSQL, not 0.
        assert_eq!(
            opt_decimal_i64_from_row(&row_with("total", Value::Null), "total").expect("nullable"),
            None
        );
        assert_eq!(
            opt_decimal_i64_from_row(&row_with("total", Value::Decimal("5".into())), "total")
                .expect("nullable"),
            Some(5)
        );
        assert!(opt_decimal_i64_from_row(&row_with("total", Value::Null), "missing").is_err());
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
