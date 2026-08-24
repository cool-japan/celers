//! A minimal, dependency-free SQL statement splitter.
//!
//! `MysqlResultBackend::migrate` needs to split a migration file containing
//! multiple `;`-terminated statements into individual statements to execute
//! one at a time, since `oxisql_mysql::MyConnection` has no Postgres-style
//! `execute_batch`/simple-query protocol for multi-statement text. A naive
//! `sql.split(';')` breaks on any `;` inside a string literal or a quoted
//! identifier, and — worse — a fragment that merely *starts* with a `--`
//! comment (after trimming) would previously be dropped wholesale by a
//! `!trimmed.starts_with("--")` guard, silently discarding every real
//! statement that followed the comment inside that same fragment.
//!
//! [`split_sql_statements`] is a small hand-rolled scanner (not a full SQL
//! parser) that tracks quote/comment state character by character and only
//! splits on a `;` that is outside all of them. This only needs to be
//! correct for this crate's own migration files (no dynamic/user-supplied
//! SQL ever goes through it), so it deliberately does not attempt to parse
//! full SQL grammar (functions, `DELIMITER` sections, etc.) — callers handle
//! `DELIMITER`-bounded stored-procedure bodies separately, as a single
//! pre-extracted chunk each, before calling this on the surrounding DDL.

/// Split `sql` into individual statements on top-level `;` boundaries,
/// respecting single-quoted strings, double-quoted and backtick-quoted
/// identifiers, `-- ` / `#` line comments, and `/* ... */` block comments.
///
/// Backslash-escaped quotes (`\'`, `\"`, `` \` ``) and doubled quotes (`''`,
/// `""`, `` `` ``) are both treated as an escaped quote character rather
/// than a close, matching MySQL's default (non-`NO_BACKSLASH_ESCAPES`) quote
/// grammar.
///
/// A statement's leading comment (if any) is kept as part of the returned
/// statement text — e.g. `"-- Indexes for performance\nCREATE INDEX ..."` —
/// rather than stripped, since MySQL's own parser tolerates a leading
/// comment before a real statement and stripping would add complexity for
/// no benefit. A fragment that is *entirely* comments/whitespace (no
/// executable content at all) is dropped rather than sent to the server as
/// an empty statement.
pub(crate) fn split_sql_statements(sql: &str) -> Vec<String> {
    #[derive(Clone, Copy, PartialEq)]
    enum State {
        Normal,
        SingleQuoted,
        DoubleQuoted,
        Backtick,
        LineComment,
        BlockComment,
    }

    let chars: Vec<char> = sql.chars().collect();
    let mut state = State::Normal;
    let mut current = String::new();
    let mut has_content = false;
    let mut statements = Vec::new();
    let mut i = 0;

    let push_statement = |current: &mut String, has_content: &mut bool, out: &mut Vec<String>| {
        let trimmed = current.trim();
        if *has_content && !trimmed.is_empty() {
            out.push(trimmed.to_string());
        }
        current.clear();
        *has_content = false;
    };

    while i < chars.len() {
        let c = chars[i];
        let next = chars.get(i + 1).copied();

        match state {
            State::Normal => match c {
                '\'' => {
                    state = State::SingleQuoted;
                    current.push(c);
                    has_content = true;
                }
                '"' => {
                    state = State::DoubleQuoted;
                    current.push(c);
                    has_content = true;
                }
                '`' => {
                    state = State::Backtick;
                    current.push(c);
                    has_content = true;
                }
                '-' if next == Some('-') => {
                    state = State::LineComment;
                    current.push(c);
                    current.push('-');
                    i += 1;
                }
                '#' => {
                    state = State::LineComment;
                    current.push(c);
                }
                '/' if next == Some('*') => {
                    state = State::BlockComment;
                    current.push(c);
                    current.push('*');
                    i += 1;
                }
                ';' => {
                    push_statement(&mut current, &mut has_content, &mut statements);
                }
                _ => {
                    if !c.is_whitespace() {
                        has_content = true;
                    }
                    current.push(c);
                }
            },
            State::SingleQuoted => {
                current.push(c);
                if c == '\\' {
                    if let Some(escaped) = next {
                        current.push(escaped);
                        i += 1;
                    }
                } else if c == '\'' {
                    if next == Some('\'') {
                        current.push('\'');
                        i += 1;
                    } else {
                        state = State::Normal;
                    }
                }
            }
            State::DoubleQuoted => {
                current.push(c);
                if c == '\\' {
                    if let Some(escaped) = next {
                        current.push(escaped);
                        i += 1;
                    }
                } else if c == '"' {
                    if next == Some('"') {
                        current.push('"');
                        i += 1;
                    } else {
                        state = State::Normal;
                    }
                }
            }
            State::Backtick => {
                current.push(c);
                if c == '`' {
                    if next == Some('`') {
                        current.push('`');
                        i += 1;
                    } else {
                        state = State::Normal;
                    }
                }
            }
            State::LineComment => {
                current.push(c);
                if c == '\n' {
                    state = State::Normal;
                }
            }
            State::BlockComment => {
                current.push(c);
                if c == '*' && next == Some('/') {
                    current.push('/');
                    i += 1;
                    state = State::Normal;
                }
            }
        }

        i += 1;
    }

    push_statement(&mut current, &mut has_content, &mut statements);

    statements
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn splits_simple_statements() {
        let sql = "CREATE TABLE a (id INT); CREATE TABLE b (id INT);";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].starts_with("CREATE TABLE a"));
        assert!(stmts[1].starts_with("CREATE TABLE b"));
    }

    #[test]
    fn does_not_split_on_semicolon_inside_single_quoted_string() {
        let sql = "INSERT INTO t (msg) VALUES ('hello; world'); SELECT 1;";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("'hello; world'"));
        assert_eq!(stmts[1], "SELECT 1");
    }

    #[test]
    fn does_not_split_on_semicolon_inside_backtick_identifier() {
        // A pathological but legal MySQL identifier containing a semicolon.
        let sql = "SELECT 1 AS `weird;name`; SELECT 2;";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("`weird;name`"));
    }

    #[test]
    fn handles_escaped_single_quote_inside_string() {
        let sql = r"INSERT INTO t (msg) VALUES ('it''s ok; really'); SELECT 1;";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[0].contains("it''s ok; really"));
    }

    #[test]
    fn handles_backslash_escaped_quote_inside_string() {
        let sql = r"INSERT INTO t (msg) VALUES ('a\'; DROP TABLE t; --'); SELECT 1;";
        let stmts = split_sql_statements(sql);
        // The whole VALUES(...) literal (including the escaped quote and
        // embedded semicolons) must stay inside statement 0, not fracture
        // into extra statements.
        assert_eq!(stmts.len(), 2);
        assert_eq!(stmts[1], "SELECT 1");
    }

    #[test]
    fn regression_leading_line_comment_does_not_drop_following_statement() {
        // Reproduces the exact bug: a comment line immediately preceding a
        // DDL statement, both inside the same `;`-delimited fragment. The
        // OLD naive splitter's `!trimmed.starts_with("--")` guard silently
        // dropped the entire fragment (comment AND the CREATE INDEX that
        // followed it) because trimming leaves the comment as the prefix.
        let sql =
            "CREATE TABLE t (id INT);\n-- Indexes for performance\nCREATE INDEX idx_t ON t(id);";
        let stmts = split_sql_statements(sql);
        assert_eq!(
            stmts.len(),
            2,
            "comment + CREATE INDEX must survive as one statement, not be dropped: {stmts:?}"
        );
        assert!(
            stmts[1].contains("CREATE INDEX idx_t"),
            "CREATE INDEX must not be silently dropped: {:?}",
            stmts[1]
        );
    }

    #[test]
    fn drops_comment_only_fragment_between_semicolons() {
        // The comment sits in its OWN `;`-delimited fragment here (a `;`
        // immediately follows it) so it has no executable content at all —
        // contrast with `regression_leading_line_comment_does_not_drop_following_statement`,
        // where a comment shares its fragment with a real statement and must
        // be kept.
        let sql = "SELECT 1; -- trailing comment only, no statement\n; SELECT 2;";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts, vec!["SELECT 1".to_string(), "SELECT 2".to_string()]);
    }

    #[test]
    fn does_not_split_on_semicolon_inside_block_comment() {
        let sql = "SELECT 1; /* a ; b */ SELECT 2;";
        let stmts = split_sql_statements(sql);
        assert_eq!(stmts.len(), 2);
        assert!(stmts[1].trim_start().starts_with("/* a ; b */"));
    }

    #[test]
    fn ignores_trailing_whitespace_and_empty_input() {
        assert!(split_sql_statements("").is_empty());
        assert!(split_sql_statements("   \n\t  ").is_empty());
        assert!(split_sql_statements(";;;").is_empty());
    }

    #[test]
    fn splits_the_real_mysql_migration_file_without_dropping_any_create() {
        // No database needed: purely a text-level regression guard that the
        // real migration file's statement count is stable and every
        // `CREATE ...` in the source text survives into some statement.
        let full = include_str!("../migrations/001_init_mysql.sql");
        let main_sql = full
            .split("DELIMITER //")
            .next()
            .expect("migration file has a pre-DELIMITER section");
        let stmts = split_sql_statements(main_sql);

        let expected_creates = main_sql
            .lines()
            .filter(|l| !l.trim_start().starts_with("--"))
            .filter(|l| l.to_uppercase().contains("CREATE "))
            .count();
        let actual_creates: usize = stmts
            .iter()
            .map(|s| s.to_uppercase().matches("CREATE ").count())
            .sum();
        assert_eq!(
            expected_creates, actual_creates,
            "every CREATE statement in the source must appear in exactly one split statement: {stmts:#?}"
        );
        assert!(!stmts.is_empty());
    }
}
