//! Property-based round-trip tests for `celers-protocol`.
//!
//! `celers-protocol` states several of its invariants as universals rather
//! than examples -- `types.rs` documents that a message captured off a
//! Redis/SQS queue round-trips through [`Message`](celers_protocol::Message)
//! (including the virtual-transport keys a kombu consumer adds),
//! `embed.rs` asserts the `[args, kwargs, embed]` tuple round-trips, and
//! `compression.rs`'s doctest shows `decompress(compress(x)) == x` -- but
//! until now each was pinned by one or two hand-picked examples rather than
//! checked against a generator. This suite adds that: `proptest` explores
//! many generated values per property (default 256 cases) and shrinks any
//! failure to a minimal reproduction.
//!
//! # Scope note: the canonical-form property is not here
//!
//! The audit finding this suite addresses also suggested a fourth property,
//! in `celers-core`: for any two distinct `TaskCall`s, `canonical_form`
//! differs. That property is not reachable from this crate --
//! `celers-protocol` has zero dependencies on other `celers-*` crates (by
//! design; see this crate's own module docs), and `task_signature.rs` lives
//! in `celers-core`, which is outside this crate's ownership for this pass.
//! It is not implemented here.

mod message_roundtrip {
    use celers_protocol::{DeliveryInfo, Message, MessageHeaders, MessageProperties};
    use chrono::{DateTime, TimeZone, Utc};
    use proptest::prelude::*;
    use std::collections::HashMap;
    use uuid::Uuid;

    // `arb_uuid`/`arb_datetime`/`arb_bytes` mirror
    // `celers-cli/tests/proptest_cli.rs`'s helpers of the same names (kept
    // independent here since those are private to that crate's own suite
    // and unreachable from this one).

    fn arb_uuid() -> impl Strategy<Value = Uuid> {
        any::<u128>().prop_map(Uuid::from_u128)
    }

    /// An arbitrary, valid `DateTime<Utc>`, bounded to roughly years
    /// 1750-2223 so every generated value is representable and formats to a
    /// normal RFC 3339 string (chrono's serde impl is trusted for exact
    /// round-tripping; this suite verifies it end-to-end via [`Message`]
    /// regardless).
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

    fn arb_string(max_len: usize) -> impl Strategy<Value = String> {
        proptest::collection::vec(proptest::char::any(), 0..max_len)
            .prop_map(|chars| chars.into_iter().collect())
    }

    /// A bounded, non-recursive `serde_json::Value`: `Number` is always
    /// built from an `i64` (never an `f64`), so it can never be NaN or
    /// infinite -- values `serde_json::Number` cannot represent and that
    /// would make "round-trips to an equal value" meaningless to assert.
    fn arb_json_leaf() -> impl Strategy<Value = serde_json::Value> {
        prop_oneof![
            Just(serde_json::Value::Null),
            any::<bool>().prop_map(serde_json::Value::Bool),
            any::<i64>().prop_map(|n| serde_json::Value::Number(n.into())),
            arb_string(24).prop_map(serde_json::Value::String),
        ]
    }

    /// A `serde_json::Value` recursively built from [`arb_json_leaf`], up to
    /// 3 levels deep with at most 4 elements per array/object -- enough to
    /// exercise nesting without generating pathological cases.
    fn arb_json_value() -> impl Strategy<Value = serde_json::Value> {
        arb_json_leaf().prop_recursive(3, 32, 4, |inner| {
            prop_oneof![
                proptest::collection::vec(inner.clone(), 0..4).prop_map(serde_json::Value::Array),
                proptest::collection::hash_map(arb_string(12), inner, 0..4)
                    .prop_map(|m| serde_json::Value::Object(m.into_iter().collect())),
            ]
        })
    }

    /// The named fields `MessageHeaders` declares outside its
    /// `#[serde(flatten)] extra` map. A generated `extra` key equal to one
    /// of these would not be a bug in [`Message`]'s round-trip -- it is
    /// simply not what `#[serde(flatten)]` means (the named field wins,
    /// so the round-trip is not required to be `extra`-identity for a
    /// colliding key) -- so the generator excludes them rather than the
    /// property tolerating a false failure.
    const RESERVED_HEADER_KEYS: &[&str] = &[
        "task",
        "id",
        "lang",
        "root_id",
        "parent_id",
        "group",
        "retries",
        "eta",
        "expires",
        "created_at",
    ];

    fn arb_extra_map() -> impl Strategy<Value = HashMap<String, serde_json::Value>> {
        let key = arb_string(16)
            .prop_filter("must not collide with a named MessageHeaders field", |k| {
                !RESERVED_HEADER_KEYS.contains(&k.as_str())
            });
        proptest::collection::hash_map(key, arb_json_value(), 0..4)
    }

    prop_compose! {
        fn arb_headers()(
            task in arb_string(32),
            id in arb_uuid(),
            lang in arb_string(8),
            root_id in proptest::option::of(arb_uuid()),
            parent_id in proptest::option::of(arb_uuid()),
            group in proptest::option::of(arb_uuid()),
            retries in proptest::option::of(any::<u32>()),
            eta in proptest::option::of(arb_datetime()),
            expires in proptest::option::of(arb_datetime()),
            created_at in proptest::option::of(arb_datetime()),
            extra in arb_extra_map(),
        ) -> MessageHeaders {
            MessageHeaders {
                task,
                id,
                lang,
                root_id,
                parent_id,
                group,
                retries,
                eta,
                expires,
                created_at,
                extra,
            }
        }
    }

    prop_compose! {
        fn arb_delivery_info()(
            exchange in arb_string(16),
            routing_key in arb_string(24),
        ) -> DeliveryInfo {
            DeliveryInfo { exchange, routing_key }
        }
    }

    prop_compose! {
        fn arb_properties()(
            correlation_id in proptest::option::of(arb_string(24)),
            reply_to in proptest::option::of(arb_string(24)),
            delivery_mode in any::<u8>(),
            priority in proptest::option::of(any::<u8>()),
            // The two properties a kombu consumer indexes without a default:
            // a round trip that dropped either would leave a Celery worker
            // raising `KeyError` on the message it just read back.
            delivery_tag in arb_string(36),
            delivery_info in arb_delivery_info(),
        ) -> MessageProperties {
            MessageProperties {
                correlation_id,
                reply_to,
                delivery_mode,
                priority,
                delivery_tag,
                delivery_info,
            }
        }
    }

    prop_compose! {
        fn arb_message()(
            headers in arb_headers(),
            properties in arb_properties(),
            body in arb_bytes(256),
            content_type in arb_string(16),
            content_encoding in arb_string(16),
        ) -> Message {
            Message {
                headers,
                properties,
                body,
                content_type,
                content_encoding,
            }
        }
    }

    proptest! {
        /// A `Message` captured off a broker (this crate's own JSON
        /// serialization, which is what every broker/backend crate in the
        /// workspace uses to move messages) survives a serialize/deserialize
        /// round trip field-for-field -- including every entry of the
        /// flattened `extra` map, not just the named headers.
        #[test]
        fn message_json_round_trip(msg in arb_message()) {
            let json = serde_json::to_string(&msg).expect("Message always serializes");
            let parsed: Message = serde_json::from_str(&json).expect("re-parsing this crate's own output must succeed");
            prop_assert_eq!(parsed, msg);
        }

        /// The same property via `to_vec`/`from_slice`, since brokers move
        /// `Vec<u8>` payloads rather than `String`s (see e.g.
        /// `celers-broker-redis`'s enqueue path).
        #[test]
        fn message_bytes_round_trip(msg in arb_message()) {
            let bytes = serde_json::to_vec(&msg).expect("Message always serializes");
            let parsed: Message = serde_json::from_slice(&bytes).expect("re-parsing this crate's own output must succeed");
            prop_assert_eq!(parsed, msg);
        }
    }
}

mod embedded_body_roundtrip {
    use celers_protocol::embed::{CallbackSignature, EmbedOptions, EmbeddedBody};
    use proptest::prelude::*;
    use std::collections::HashMap;
    use uuid::Uuid;

    fn arb_uuid() -> impl Strategy<Value = Uuid> {
        any::<u128>().prop_map(Uuid::from_u128)
    }

    fn arb_string(max_len: usize) -> impl Strategy<Value = String> {
        proptest::collection::vec(proptest::char::any(), 0..max_len)
            .prop_map(|chars| chars.into_iter().collect())
    }

    /// Mirrors `message_roundtrip::arb_json_value` (kept module-local
    /// rather than shared, matching this file's other per-module helper
    /// duplication -- see that function's doc for why `Number` is always
    /// built from an `i64`).
    fn arb_json_value() -> impl Strategy<Value = serde_json::Value> {
        let leaf = prop_oneof![
            Just(serde_json::Value::Null),
            any::<bool>().prop_map(serde_json::Value::Bool),
            any::<i64>().prop_map(|n| serde_json::Value::Number(n.into())),
            arb_string(24).prop_map(serde_json::Value::String),
        ];
        leaf.prop_recursive(3, 32, 4, |inner| {
            prop_oneof![
                proptest::collection::vec(inner.clone(), 0..4).prop_map(serde_json::Value::Array),
                proptest::collection::hash_map(arb_string(12), inner, 0..4)
                    .prop_map(|m| serde_json::Value::Object(m.into_iter().collect())),
            ]
        })
    }

    fn arb_kwargs(max_len: usize) -> impl Strategy<Value = HashMap<String, serde_json::Value>> {
        proptest::collection::hash_map(arb_string(12), arb_json_value(), 0..max_len)
    }

    prop_compose! {
        fn arb_callback()(
            task in arb_string(24),
            task_id in proptest::option::of(arb_uuid()),
            args in proptest::collection::vec(arb_json_value(), 0..3),
            kwargs in arb_kwargs(3),
            options in arb_kwargs(3),
            immutable in any::<bool>(),
            subtask_type in proptest::option::of(arb_string(12)),
        ) -> CallbackSignature {
            CallbackSignature {
                task,
                task_id,
                args,
                kwargs,
                options,
                immutable,
                subtask_type,
            }
        }
    }

    /// `EmbedOptions`'s own named fields, mirroring
    /// `message_roundtrip::RESERVED_HEADER_KEYS`'s rationale: a generated
    /// `extra` key equal to one of these is not a round-trip bug, it is
    /// simply outside what `#[serde(flatten)]` promises, so the generator
    /// excludes them.
    const RESERVED_EMBED_KEYS: &[&str] = &[
        "callbacks",
        "errbacks",
        "chain",
        "chord",
        "group",
        "parent_id",
        "root_id",
    ];

    fn arb_embed_extra_map() -> impl Strategy<Value = HashMap<String, serde_json::Value>> {
        let key = arb_string(16)
            .prop_filter("must not collide with a named EmbedOptions field", |k| {
                !RESERVED_EMBED_KEYS.contains(&k.as_str())
            });
        proptest::collection::hash_map(key, arb_json_value(), 0..3)
    }

    prop_compose! {
        fn arb_embed_options()(
            callbacks in proptest::collection::vec(arb_callback(), 0..3),
            errbacks in proptest::collection::vec(arb_callback(), 0..3),
            chain in proptest::collection::vec(arb_callback(), 0..3),
            chord in proptest::option::of(arb_callback()),
            group in proptest::option::of(arb_uuid()),
            parent_id in proptest::option::of(arb_uuid()),
            root_id in proptest::option::of(arb_uuid()),
            extra in arb_embed_extra_map(),
        ) -> EmbedOptions {
            EmbedOptions {
                callbacks,
                errbacks,
                chain,
                chord,
                group,
                parent_id,
                root_id,
                extra,
            }
        }
    }

    prop_compose! {
        fn arb_embedded_body()(
            args in proptest::collection::vec(arb_json_value(), 0..4),
            kwargs in arb_kwargs(4),
            embed in arb_embed_options(),
        ) -> EmbeddedBody {
            EmbeddedBody { args, kwargs, embed }
        }
    }

    proptest! {
        /// The Celery protocol v2 body tuple `[args, kwargs, embed]` -- the
        /// exact structure `app.amqp.as_task_v2` emits and every worker
        /// parses -- survives `encode`/`decode` (`Vec<u8>`) identically.
        #[test]
        fn embedded_body_encode_decode_round_trip(body in arb_embedded_body()) {
            let bytes = body.encode().expect("EmbeddedBody always encodes");
            let decoded = EmbeddedBody::decode(&bytes).expect("re-decoding this crate's own output must succeed");
            prop_assert_eq!(decoded, body);
        }

        /// Same property via the `String` convenience wrappers
        /// (`to_json_string`/`from_json_string`).
        #[test]
        fn embedded_body_json_string_round_trip(body in arb_embedded_body()) {
            let s = body.to_json_string().expect("EmbeddedBody always encodes");
            let decoded = EmbeddedBody::from_json_string(&s).expect("re-decoding this crate's own output must succeed");
            prop_assert_eq!(decoded, body);
        }
    }
}

mod compression_roundtrip {
    use celers_protocol::compression::{CompressionType, Compressor};
    use proptest::prelude::*;

    fn arb_bytes(max_len: usize) -> impl Strategy<Value = Vec<u8>> {
        proptest::collection::vec(any::<u8>(), 0..=max_len)
    }

    proptest! {
        /// `CompressionType::None` is a real member of the round-trip
        /// property, not just a default: `decompress(compress(x)) == x`
        /// must hold for it exactly like every algorithm below (it is the
        /// identity compressor, but the round trip goes through the same
        /// `Compressor::compress`/`decompress` dispatch as the others).
        #[test]
        fn round_trips_uncompressed(data in arb_bytes(4096)) {
            let c = Compressor::new(CompressionType::None);
            let compressed = c.compress(&data).expect("None compression cannot fail");
            let decompressed = c.decompress(&compressed).expect("None decompression cannot fail");
            prop_assert_eq!(decompressed, data);
        }
    }

    #[cfg(feature = "gzip")]
    proptest! {
        #[test]
        fn round_trips_gzip(data in arb_bytes(4096), level in 1u32..=9) {
            let c = Compressor::new(CompressionType::Gzip).with_level(level);
            let compressed = c.compress(&data).expect("gzip compression must succeed on arbitrary bytes");
            let decompressed = c.decompress(&compressed).expect("gzip decompression of this crate's own output must succeed");
            prop_assert_eq!(decompressed, data);
        }
    }

    #[cfg(feature = "zlib")]
    proptest! {
        #[test]
        fn round_trips_zlib(data in arb_bytes(4096), level in 1u32..=9) {
            let c = Compressor::new(CompressionType::Zlib).with_level(level);
            let compressed = c.compress(&data).expect("zlib compression must succeed on arbitrary bytes");
            let decompressed = c.decompress(&compressed).expect("zlib decompression of this crate's own output must succeed");
            prop_assert_eq!(decompressed, data);
        }
    }

    #[cfg(feature = "zstd-compression")]
    proptest! {
        #[test]
        fn round_trips_zstd(data in arb_bytes(4096), level in 1u32..=19) {
            let c = Compressor::new(CompressionType::Zstd).with_level(level);
            let compressed = c.compress(&data).expect("zstd compression must succeed on arbitrary bytes");
            let decompressed = c.decompress(&compressed).expect("zstd decompression of this crate's own output must succeed");
            prop_assert_eq!(decompressed, data);
        }
    }

    /// A single, larger (1 MiB) buffer per available algorithm, run once
    /// rather than as a property (many `proptest` cases at this size would
    /// make the suite slow for little extra signal beyond the 4 KiB
    /// property above) -- covers the "large payload" case the compression
    /// module exists for in the first place.
    #[test]
    fn round_trips_one_megabyte_buffer() {
        let data = vec![0x5Au8; 1_048_576];
        for compression_type in [
            CompressionType::None,
            #[cfg(feature = "gzip")]
            CompressionType::Gzip,
            #[cfg(feature = "zlib")]
            CompressionType::Zlib,
            #[cfg(feature = "zstd-compression")]
            CompressionType::Zstd,
        ] {
            let c = Compressor::new(compression_type);
            let compressed = c.compress(&data).unwrap_or_else(|e| {
                panic!("{compression_type:?} compression of 1 MiB failed: {e}")
            });
            let decompressed = c.decompress(&compressed).unwrap_or_else(|e| {
                panic!("{compression_type:?} decompression of 1 MiB failed: {e}")
            });
            assert_eq!(
                decompressed, data,
                "{compression_type:?} round trip lost data"
            );
        }
    }

    /// The empty buffer, explicitly, for every available algorithm --
    /// proptest's `0..=max_len` range strategy shrinks toward (and often
    /// draws) empty vectors already, but the empty case is exactly the kind
    /// of boundary worth a permanent example rather than relying on
    /// randomness to keep visiting it.
    #[test]
    fn round_trips_empty_buffer() {
        let data: Vec<u8> = Vec::new();
        for compression_type in [
            CompressionType::None,
            #[cfg(feature = "gzip")]
            CompressionType::Gzip,
            #[cfg(feature = "zlib")]
            CompressionType::Zlib,
            #[cfg(feature = "zstd-compression")]
            CompressionType::Zstd,
        ] {
            let c = Compressor::new(compression_type);
            let compressed = c.compress(&data).unwrap_or_else(|e| {
                panic!("{compression_type:?} compression of empty input failed: {e}")
            });
            let decompressed = c.decompress(&compressed).unwrap_or_else(|e| {
                panic!("{compression_type:?} decompression of empty input failed: {e}")
            });
            assert_eq!(
                decompressed, data,
                "{compression_type:?} round trip lost data"
            );
        }
    }
}
