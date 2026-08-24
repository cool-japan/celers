//! Property-based tests for `celers-cli`.
//!
//! Covers three surfaces (package plan P7, `crates/celers-cli/TODO.md`):
//!
//! 1. **Command argument validation** ([`cli_arg_validation`]) -- parsing
//!    arbitrary token vectors through the crate's `clap` surface never
//!    panics, and known flags round-trip valid values correctly.
//! 2. **Configuration parsing** ([`config_roundtrip`]) -- an arbitrary
//!    [`celers_cli::config::Config`] survives a TOML/YAML
//!    serialize-then-deserialize round trip field-for-field.
//! 3. **Data serialization** ([`task_serde_roundtrip`]) -- an arbitrary
//!    `celers_core::SerializedTask` (the type `celers-cli`'s
//!    `commands::task` functions move between queues) survives the exact
//!    `serde_json` round trip those functions perform.
//!
//! # Scope note: why this targets `CliConfigArgs`, not `Cli`/`Commands`
//!
//! The crate's real `clap` entry point (`Cli`, `Commands`, and every nested
//! `*Commands` enum) lives in `src/cli/types.rs`, but `src/cli/mod.rs` is
//! declared as a **private** `mod cli;` in `src/main.rs` -- it is never
//! declared in `src/lib.rs`, so it is not part of the `celers_cli` library
//! crate that this integration test links against (only `backup`,
//! `command_utils`, `commands`, `config`, `config_layer`, `database`,
//! `interactive`, `row_ext`, and `tls_mode` are `pub mod`-declared there).
//! `Cli`/`Commands` are therefore unreachable from here without editing the
//! reserved `src/cli/types.rs` / `src/lib.rs` files, which this package plan
//! explicitly excludes.
//!
//! Instead, this suite targets `celers_cli::config_layer::CliConfigArgs`: a
//! `pub`, `#[derive(clap::Args)]` struct that *is* re-exported from the
//! library (`pub use config_layer::{CliConfigArgs, ...}` in `src/lib.rs`) and
//! is the actual `#[command(flatten)]` surface every real subcommand in
//! `cli/types.rs` shares (per that module's own docs: "intended to be
//! `#[command(flatten)]`-ed into individual subcommands"). It is wrapped
//! below in a minimal local `#[derive(clap::Parser)]` shell purely so
//! `try_parse_from` can drive `clap`'s real tokenizer/parser end-to-end --
//! the flatten target (every arg name, type, and parser) is 100% the crate's
//! real code, nothing reimplemented.

mod cli_arg_validation {
    use celers_cli::config_layer::CliConfigArgs;
    use clap::Parser;
    use proptest::prelude::*;

    /// Minimal top-level parser wrapping the real [`CliConfigArgs`] flatten
    /// surface. Exists only to give `clap` something to parse into; every
    /// argument name/type/parser it exposes is the crate's real code.
    #[derive(Parser, Debug)]
    #[command(name = "celers-proptest-harness")]
    struct HarnessCli {
        #[command(flatten)]
        config_args: CliConfigArgs,
    }

    /// A bounded, printable-ASCII (`' '..='~'`, i.e. excluding control
    /// characters) command-line token.
    fn arb_token(max_len: usize) -> impl Strategy<Value = String> {
        proptest::collection::vec(proptest::char::range(' ', '~'), 0..=max_len)
            .prop_map(|chars| chars.into_iter().collect())
    }

    /// A bounded vector of arbitrary printable tokens, simulating `argv[1..]`.
    fn arb_argv_tail() -> impl Strategy<Value = Vec<String>> {
        proptest::collection::vec(arb_token(24), 0..16)
    }

    /// Tokens biased toward the shapes that most stress a `clap` parser:
    /// real flag names (which drive `CliConfigArgs`'s actual value-parsing
    /// code for `usize`/`u64`/`u32`/`PathBuf`/delimited `Vec<String>`),
    /// `clap`-meaningful punctuation (`--`, a bare `-`, `--help`), and
    /// arbitrary garbage.
    fn arb_flag_or_garbage() -> impl Strategy<Value = String> {
        prop_oneof![
            3 => Just("--config".to_string()),
            3 => Just("--broker".to_string()),
            3 => Just("--backend".to_string()),
            3 => Just("--broker-type".to_string()),
            3 => Just("--queue".to_string()),
            3 => Just("--queue-mode".to_string()),
            3 => Just("--queues".to_string()),
            3 => Just("--concurrency".to_string()),
            3 => Just("--poll-interval-ms".to_string()),
            3 => Just("--max-retries".to_string()),
            3 => Just("--timeout-secs".to_string()),
            3 => Just("--profile".to_string()),
            3 => Just("--log-level".to_string()),
            2 => Just("--help".to_string()),
            2 => Just("--".to_string()),
            1 => Just("-".to_string()),
            1 => Just("=".to_string()),
            10 => arb_token(16),
        ]
    }

    /// A bounded, non-empty, hyphen-free alphanumeric token: safe to use as
    /// the *value* of a `clap` option in tests that assert a successful
    /// parse, since it can never be misread as the start of another option
    /// (which a leading `-` can trigger without `allow_hyphen_values`).
    fn arb_safe_value_token(max_len: usize) -> impl Strategy<Value = String> {
        proptest::collection::vec(
            prop_oneof![
                proptest::char::range('a', 'z'),
                proptest::char::range('A', 'Z'),
                proptest::char::range('0', '9'),
            ],
            1..=max_len,
        )
        .prop_map(|chars| chars.into_iter().collect())
    }

    /// Assert that parsing never panics: the result is always either a
    /// successful parse or a well-formed (displayable) `clap::Error`.
    fn assert_parses_or_clean_error(argv: &[String]) -> Result<(), TestCaseError> {
        match HarnessCli::try_parse_from(argv) {
            Ok(_) => Ok(()),
            Err(err) => {
                let rendered = err.to_string();
                prop_assert!(
                    !rendered.is_empty(),
                    "clap::Error must render a non-empty message"
                );
                Ok(())
            }
        }
    }

    proptest! {
        /// Parsing any bounded, printable argv tail must never panic.
        #[test]
        fn prop_arbitrary_tokens_never_panic(tail in arb_argv_tail()) {
            let mut argv = vec!["celers".to_string()];
            argv.extend(tail);
            assert_parses_or_clean_error(&argv)?;
        }

        /// Same property, but tokens are biased toward real flag names mixed
        /// with garbage, exercising `CliConfigArgs`'s actual value parsers
        /// (numeric, path, comma-delimited list) with malformed input.
        #[test]
        fn prop_realistic_and_garbage_flags_never_panic(
            parts in proptest::collection::vec(arb_flag_or_garbage(), 0..16)
        ) {
            let mut argv = vec!["celers".to_string()];
            argv.extend(parts);
            assert_parses_or_clean_error(&argv)?;
        }

        /// A valid `usize` string passed to `--concurrency` must always
        /// parse successfully and round-trip the exact value.
        #[test]
        fn prop_concurrency_flag_roundtrips(n in any::<usize>()) {
            let argv = vec!["celers".to_string(), "--concurrency".to_string(), n.to_string()];
            let parsed = HarnessCli::try_parse_from(&argv)
                .unwrap_or_else(|e| panic!("valid usize must parse: {e}"));
            prop_assert_eq!(parsed.config_args.concurrency, Some(n));
        }

        /// A valid `u32` string passed to `--max-retries` must always parse
        /// successfully and round-trip the exact value.
        #[test]
        fn prop_max_retries_flag_roundtrips(n in any::<u32>()) {
            let argv = vec!["celers".to_string(), "--max-retries".to_string(), n.to_string()];
            let parsed = HarnessCli::try_parse_from(&argv)
                .unwrap_or_else(|e| panic!("valid u32 must parse: {e}"));
            prop_assert_eq!(parsed.config_args.max_retries, Some(n));
        }

        /// A valid `u64` string passed to `--poll-interval-ms` must always
        /// parse successfully and round-trip the exact value.
        #[test]
        fn prop_poll_interval_flag_roundtrips(n in any::<u64>()) {
            let argv = vec![
                "celers".to_string(),
                "--poll-interval-ms".to_string(),
                n.to_string(),
            ];
            let parsed = HarnessCli::try_parse_from(&argv)
                .unwrap_or_else(|e| panic!("valid u64 must parse: {e}"));
            prop_assert_eq!(parsed.config_args.poll_interval_ms, Some(n));
        }

        /// A comma-joined list of safe tokens passed to `--queues` must
        /// split back into exactly the original segments
        /// (`value_delimiter = ','`).
        #[test]
        fn prop_queues_delimited_list_roundtrips(
            segments in proptest::collection::vec(arb_safe_value_token(12), 1..6)
        ) {
            let joined = segments.join(",");
            let argv = vec!["celers".to_string(), "--queues".to_string(), joined];
            let parsed = HarnessCli::try_parse_from(&argv)
                .unwrap_or_else(|e| panic!("comma-delimited value must parse: {e}"));
            prop_assert_eq!(parsed.config_args.queues, Some(segments));
        }

        /// An arbitrary safe token passed to `--profile` must round-trip
        /// unchanged.
        #[test]
        fn prop_profile_flag_roundtrips(value in arb_safe_value_token(20)) {
            let argv = vec!["celers".to_string(), "--profile".to_string(), value.clone()];
            let parsed = HarnessCli::try_parse_from(&argv)
                .unwrap_or_else(|e| panic!("value must parse: {e}"));
            prop_assert_eq!(parsed.config_args.profile, Some(value));
        }
    }
}

mod config_roundtrip {
    use celers_cli::aliases::AliasConfig;
    use celers_cli::config::{
        AlertConfig, AutoScaleConfig, BrokerConfig, CacheConfig, Config, ConfigFormat, PoolConfig,
        WorkerConfig,
    };
    use proptest::prelude::*;

    /// An arbitrary Unicode string, bounded in length to keep proptest cases
    /// fast while still exercising quoting/escaping edge cases. Hand-checked
    /// before writing this suite (see package plan P7 notes) to confirm
    /// `toml` 1.1.2 and `serde_yaml` 0.9 both correctly quote/escape control
    /// characters, quotes, `#`/`[`/`]`, emoji, embedded NULs, and
    /// YAML/TOML-ambiguous plain scalars ("null", "true", "123", "~",
    /// ".inf", dates, ...) placed in `String`-typed fields, so no character
    /// restriction beyond a length bound is required for a faithful
    /// round-trip property.
    fn arb_string(max_len: usize) -> impl Strategy<Value = String> {
        proptest::collection::vec(proptest::char::any(), 0..=max_len)
            .prop_map(|chars| chars.into_iter().collect())
    }

    fn arb_string_vec(max_len: usize, max_count: usize) -> impl Strategy<Value = Vec<String>> {
        proptest::collection::vec(arb_string(max_len), 0..=max_count)
    }

    prop_compose! {
        fn arb_broker_config()(
            broker_type in prop_oneof![
                Just("redis".to_string()),
                Just("postgres".to_string()),
                Just("mysql".to_string()),
                Just("amqp".to_string()),
                Just("sqs".to_string()),
                arb_string(16),
            ],
            url in arb_string(48),
            failover_urls in arb_string_vec(32, 4),
            failover_retries in any::<u32>(),
            failover_timeout_secs in any::<u64>(),
            queue in arb_string(24),
            mode in prop_oneof![
                Just("fifo".to_string()),
                Just("priority".to_string()),
                arb_string(12),
            ],
        ) -> BrokerConfig {
            BrokerConfig {
                broker_type,
                url,
                failover_urls,
                failover_retries,
                failover_timeout_secs,
                queue,
                mode,
            }
        }
    }

    prop_compose! {
        fn arb_worker_config()(
            concurrency in any::<usize>(),
            poll_interval_ms in any::<u64>(),
            max_retries in any::<u32>(),
            default_timeout_secs in any::<u64>(),
        ) -> WorkerConfig {
            WorkerConfig {
                concurrency,
                poll_interval_ms,
                max_retries,
                default_timeout_secs,
            }
        }
    }

    prop_compose! {
        fn arb_autoscale_config()(
            enabled in any::<bool>(),
            min_workers in any::<usize>(),
            max_workers in any::<usize>(),
            scale_up_threshold in any::<usize>(),
            scale_down_threshold in any::<usize>(),
            check_interval_secs in any::<u64>(),
        ) -> AutoScaleConfig {
            AutoScaleConfig {
                enabled,
                min_workers,
                max_workers,
                scale_up_threshold,
                scale_down_threshold,
                check_interval_secs,
            }
        }
    }

    prop_compose! {
        fn arb_alert_config()(
            enabled in any::<bool>(),
            webhook_url in proptest::option::of(arb_string(48)),
            dlq_threshold in any::<usize>(),
            failed_threshold in any::<usize>(),
            check_interval_secs in any::<u64>(),
        ) -> AlertConfig {
            AlertConfig {
                enabled,
                webhook_url,
                dlq_threshold,
                failed_threshold,
                check_interval_secs,
            }
        }
    }

    prop_compose! {
        fn arb_pool_config()(
            max_size in any::<usize>(),
            reuse_enabled in any::<bool>(),
        ) -> PoolConfig {
            PoolConfig {
                max_size,
                reuse_enabled,
            }
        }
    }

    prop_compose! {
        fn arb_cache_config()(
            ttl_secs in any::<u64>(),
            enabled in any::<bool>(),
        ) -> CacheConfig {
            CacheConfig { ttl_secs, enabled }
        }
    }

    /// An arbitrary [`AliasConfig`], built via the crate's own trusted
    /// bulk-construction path ([`AliasConfig::from_map`]) rather than
    /// hand-rolling a `HashMap`-shaped `Arbitrary` impl. Bounded to a handful
    /// of entries -- like [`arb_string_vec`], the round-trip property being
    /// tested doesn't benefit from large collections, only varied ones.
    fn arb_alias_config() -> impl Strategy<Value = AliasConfig> {
        proptest::collection::hash_map(arb_string(16), arb_string(32), 0..4)
            .prop_map(AliasConfig::from_map)
    }

    prop_compose! {
        fn arb_config()(
            profile in proptest::option::of(arb_string(16)),
            broker in arb_broker_config(),
            worker in arb_worker_config(),
            queues in arb_string_vec(24, 5),
            autoscale in proptest::option::of(arb_autoscale_config()),
            alerts in proptest::option::of(arb_alert_config()),
            pool in arb_pool_config(),
            cache in arb_cache_config(),
            aliases in proptest::option::of(arb_alias_config()),
        ) -> Config {
            Config {
                profile,
                broker,
                worker,
                queues,
                autoscale,
                alerts,
                pool,
                cache,
                aliases,
            }
        }
    }

    /// Same as [`arb_config`] but rejects configs containing a `${` anywhere
    /// in a string field, so this strategy is safe to round-trip through
    /// [`Config::from_file`], which performs `${VAR}` / `${VAR:default}`
    /// environment-variable expansion on the raw file content before
    /// parsing (that expansion behavior is intentional and already covered
    /// by `config_layer.rs`'s own unit tests -- it is out of scope here,
    /// where the property under test is pure serde round-tripping).
    fn arb_config_no_expansion() -> impl Strategy<Value = Config> {
        arb_config().prop_filter(
            "no ${...} sequences (env-var expansion would alter the round trip)",
            |c| !config_contains_dollar_brace(c),
        )
    }

    fn config_contains_dollar_brace(c: &Config) -> bool {
        fn has(s: &str) -> bool {
            s.contains("${")
        }
        c.profile.as_deref().is_some_and(has)
            || has(&c.broker.broker_type)
            || has(&c.broker.url)
            || c.broker.failover_urls.iter().any(|s| has(s))
            || has(&c.broker.queue)
            || has(&c.broker.mode)
            || c.queues.iter().any(|s| has(s))
            || c.alerts
                .as_ref()
                .and_then(|a| a.webhook_url.as_deref())
                .is_some_and(has)
            || c.aliases.as_ref().is_some_and(|a| {
                a.list()
                    .iter()
                    .any(|(name, expansion)| has(name) || has(expansion))
            })
    }

    fn assert_broker_eq(a: &BrokerConfig, b: &BrokerConfig) -> Result<(), TestCaseError> {
        prop_assert_eq!(&a.broker_type, &b.broker_type);
        prop_assert_eq!(&a.url, &b.url);
        prop_assert_eq!(&a.failover_urls, &b.failover_urls);
        prop_assert_eq!(a.failover_retries, b.failover_retries);
        prop_assert_eq!(a.failover_timeout_secs, b.failover_timeout_secs);
        prop_assert_eq!(&a.queue, &b.queue);
        prop_assert_eq!(&a.mode, &b.mode);
        Ok(())
    }

    fn assert_worker_eq(a: &WorkerConfig, b: &WorkerConfig) -> Result<(), TestCaseError> {
        prop_assert_eq!(a.concurrency, b.concurrency);
        prop_assert_eq!(a.poll_interval_ms, b.poll_interval_ms);
        prop_assert_eq!(a.max_retries, b.max_retries);
        prop_assert_eq!(a.default_timeout_secs, b.default_timeout_secs);
        Ok(())
    }

    fn assert_autoscale_eq(
        a: &Option<AutoScaleConfig>,
        b: &Option<AutoScaleConfig>,
    ) -> Result<(), TestCaseError> {
        match (a, b) {
            (None, None) => Ok(()),
            (Some(x), Some(y)) => {
                prop_assert_eq!(x.enabled, y.enabled);
                prop_assert_eq!(x.min_workers, y.min_workers);
                prop_assert_eq!(x.max_workers, y.max_workers);
                prop_assert_eq!(x.scale_up_threshold, y.scale_up_threshold);
                prop_assert_eq!(x.scale_down_threshold, y.scale_down_threshold);
                prop_assert_eq!(x.check_interval_secs, y.check_interval_secs);
                Ok(())
            }
            (a, b) => Err(TestCaseError::fail(format!(
                "autoscale presence mismatch: {a:?} vs {b:?}"
            ))),
        }
    }

    fn assert_alert_eq(
        a: &Option<AlertConfig>,
        b: &Option<AlertConfig>,
    ) -> Result<(), TestCaseError> {
        match (a, b) {
            (None, None) => Ok(()),
            (Some(x), Some(y)) => {
                prop_assert_eq!(x.enabled, y.enabled);
                prop_assert_eq!(&x.webhook_url, &y.webhook_url);
                prop_assert_eq!(x.dlq_threshold, y.dlq_threshold);
                prop_assert_eq!(x.failed_threshold, y.failed_threshold);
                prop_assert_eq!(x.check_interval_secs, y.check_interval_secs);
                Ok(())
            }
            (a, b) => Err(TestCaseError::fail(format!(
                "alerts presence mismatch: {a:?} vs {b:?}"
            ))),
        }
    }

    /// Field-wise equality for [`Config`], which (like most of its
    /// sub-structs) does not derive `PartialEq`. `PoolConfig`/`CacheConfig`
    /// and `aliases`'s `AliasConfig` are the exception -- all three derive
    /// `PartialEq, Eq` -- so those compare directly.
    fn assert_config_eq(a: &Config, b: &Config) -> Result<(), TestCaseError> {
        prop_assert_eq!(&a.profile, &b.profile);
        assert_broker_eq(&a.broker, &b.broker)?;
        assert_worker_eq(&a.worker, &b.worker)?;
        prop_assert_eq!(&a.queues, &b.queues);
        assert_autoscale_eq(&a.autoscale, &b.autoscale)?;
        assert_alert_eq(&a.alerts, &b.alerts)?;
        prop_assert_eq!(&a.pool, &b.pool);
        prop_assert_eq!(&a.cache, &b.cache);
        prop_assert_eq!(&a.aliases, &b.aliases);
        Ok(())
    }

    /// A unique path under the OS temp directory, preserving `ext` so
    /// [`ConfigFormat::from_path`] auto-detection picks the intended format.
    fn unique_temp_path(ext: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!(
            "celers_cli_proptest_config_{}_{nanos}.{ext}",
            std::process::id()
        ))
    }

    proptest! {
        /// `to_string_with_format(Toml)` then `from_str_with_format(Toml)`
        /// must reproduce every field of an arbitrary `Config`.
        #[test]
        fn prop_config_toml_roundtrip(config in arb_config()) {
            let toml_str = config
                .to_string_with_format(ConfigFormat::Toml)
                .expect("serializing an arbitrary Config to TOML must not fail");
            let reparsed = Config::from_str_with_format(&toml_str, ConfigFormat::Toml)
                .unwrap_or_else(|e| {
                    panic!("reparsing generated TOML must not fail: {e}\n---\n{toml_str}")
                });
            assert_config_eq(&config, &reparsed)?;
        }

        /// `to_string_with_format(Yaml)` then `from_str_with_format(Yaml)`
        /// must reproduce every field of an arbitrary `Config`.
        #[test]
        fn prop_config_yaml_roundtrip(config in arb_config()) {
            let yaml_str = config
                .to_string_with_format(ConfigFormat::Yaml)
                .expect("serializing an arbitrary Config to YAML must not fail");
            let reparsed = Config::from_str_with_format(&yaml_str, ConfigFormat::Yaml)
                .unwrap_or_else(|e| {
                    panic!("reparsing generated YAML must not fail: {e}\n---\n{yaml_str}")
                });
            assert_config_eq(&config, &reparsed)?;
        }

    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(48))]

        /// `Config::to_file` then `Config::from_file` (real disk I/O,
        /// extension-based format auto-detection) must reproduce every
        /// field of an arbitrary `Config`. Case count is reduced relative to
        /// the pure in-memory round trips above (separate `proptest!` block,
        /// since `#![proptest_config(..)]` scopes to its whole block) since
        /// this variant performs real filesystem writes/reads per case.
        #[test]
        fn prop_config_file_roundtrip_toml(config in arb_config_no_expansion()) {
            let path = unique_temp_path("toml");
            config.to_file(&path).expect("write config to temp file");
            let loaded = Config::from_file(&path);
            let _ = std::fs::remove_file(&path);
            let loaded = loaded.expect("read config back from temp file");
            assert_config_eq(&config, &loaded)?;
        }
    }
}

mod task_serde_roundtrip {
    use celers_core::{SerializedTask, TaskMetadata, TaskState};
    use chrono::{DateTime, TimeZone, Utc};
    use proptest::prelude::*;
    use uuid::Uuid;

    fn arb_uuid() -> impl Strategy<Value = Uuid> {
        any::<u128>().prop_map(Uuid::from_u128)
    }

    /// An arbitrary, valid `DateTime<Utc>`, bounded to roughly years
    /// 1750-2223 so every generated value is representable and formats to a
    /// normal RFC 3339 string (chrono's serde impl is trusted for exact
    /// round-tripping; this suite verifies it end-to-end via
    /// [`SerializedTask`] regardless).
    fn arb_datetime() -> impl Strategy<Value = DateTime<Utc>> {
        (-8_000_000_000i64..8_000_000_000i64, 0u32..1_000_000_000u32).prop_map(|(secs, nsecs)| {
            Utc.timestamp_opt(secs, nsecs).single().unwrap_or_else(|| {
                Utc.timestamp_opt(0, 0)
                    .single()
                    .expect("the Unix epoch is always a valid timestamp")
            })
        })
    }

    fn arb_bytes(max_len: usize) -> impl Strategy<Value = Vec<u8>> {
        proptest::collection::vec(any::<u8>(), 0..=max_len)
    }

    fn arb_short_string() -> impl Strategy<Value = String> {
        proptest::collection::vec(proptest::char::any(), 0..32)
            .prop_map(|chars| chars.into_iter().collect())
    }

    /// Every [`TaskState`] variant, mirroring the strategy `celers-core`'s
    /// own `state.rs` proptest suite uses (kept independent here since that
    /// helper is private to `celers-core`'s unit tests and unreachable from
    /// this crate).
    fn arb_task_state() -> impl Strategy<Value = TaskState> {
        prop_oneof![
            Just(TaskState::Pending),
            Just(TaskState::Received),
            Just(TaskState::Reserved),
            Just(TaskState::Running),
            any::<u32>().prop_map(TaskState::Retrying),
            arb_bytes(64).prop_map(TaskState::Succeeded),
            arb_short_string().prop_map(TaskState::Failed),
            Just(TaskState::Revoked),
            Just(TaskState::Rejected),
            (arb_short_string(), proptest::option::of(arb_bytes(32)))
                .prop_map(|(name, metadata)| TaskState::Custom { name, metadata }),
        ]
    }

    prop_compose! {
        fn arb_task_metadata()(
            id in arb_uuid(),
            name in arb_short_string(),
            state in arb_task_state(),
            created_at in arb_datetime(),
            updated_at in arb_datetime(),
            max_retries in any::<u32>(),
            timeout_secs in proptest::option::of(any::<u64>()),
            expires_at in proptest::option::of(arb_datetime()),
            priority in any::<i32>(),
            group_id in proptest::option::of(arb_uuid()),
            chord_id in proptest::option::of(arb_uuid()),
            on_success_link in proptest::option::of(arb_short_string()),
            dependencies in proptest::collection::hash_set(arb_uuid(), 0..4),
        ) -> TaskMetadata {
            TaskMetadata {
                id,
                name,
                state,
                created_at,
                updated_at,
                max_retries,
                timeout_secs,
                expires_at,
                priority,
                group_id,
                chord_id,
                on_success_link,
                dependencies,
                // Signature material is produced by
                // `celers_core::task_security::sign_task`, not by an arbitrary
                // generator: an unsigned message is what the CLI round-trips.
                signature: None,
            }
        }
    }

    prop_compose! {
        fn arb_serialized_task()(
            metadata in arb_task_metadata(),
            payload in arb_bytes(128),
        ) -> SerializedTask {
            SerializedTask { metadata, payload }
        }
    }

    /// Field-wise equality for [`TaskMetadata`], which (like
    /// [`SerializedTask`]) does not derive `PartialEq`; only its `state`
    /// field type ([`TaskState`]) does.
    fn assert_metadata_eq(a: &TaskMetadata, b: &TaskMetadata) -> Result<(), TestCaseError> {
        prop_assert_eq!(a.id, b.id);
        prop_assert_eq!(&a.name, &b.name);
        prop_assert_eq!(&a.state, &b.state);
        prop_assert_eq!(a.created_at, b.created_at);
        prop_assert_eq!(a.updated_at, b.updated_at);
        prop_assert_eq!(a.max_retries, b.max_retries);
        prop_assert_eq!(a.timeout_secs, b.timeout_secs);
        prop_assert_eq!(a.expires_at, b.expires_at);
        prop_assert_eq!(a.priority, b.priority);
        prop_assert_eq!(a.group_id, b.group_id);
        prop_assert_eq!(a.chord_id, b.chord_id);
        prop_assert_eq!(&a.on_success_link, &b.on_success_link);
        prop_assert_eq!(&a.dependencies, &b.dependencies);
        Ok(())
    }

    proptest! {
        /// `celers-cli` moves tasks between queues (main/DLQ/delayed) by
        /// round-tripping `SerializedTask` through `serde_json` exactly as
        /// `commands::task` does (`serde_json::to_string` /
        /// `from_str::<celers_core::SerializedTask>`; see
        /// `inspect_task`/`retry_task`/`requeue_task` in
        /// `src/commands/task.rs`). This property mirrors that real code
        /// path directly, for an arbitrary task.
        #[test]
        fn prop_serialized_task_json_roundtrip(task in arb_serialized_task()) {
            let json = serde_json::to_string(&task).expect("serialize SerializedTask to JSON");
            let reparsed: SerializedTask = serde_json::from_str(&json).unwrap_or_else(|e| {
                panic!("deserialize SerializedTask from JSON must not fail: {e}\n---\n{json}")
            });
            assert_metadata_eq(&task.metadata, &reparsed.metadata)?;
            prop_assert_eq!(&task.payload, &reparsed.payload);
        }
    }
}
