//! Tamper, freshness and replay tests for the `celers.task.sig.v2` task
//! signature scheme.
//!
//! These live in an integration test so `task_signature.rs` stays under the
//! 2000-line file cap; everything exercised here is public API.
//!
//! Background: `v1` MAC'd only `id`/`name`/`args`/`kwargs`, so anyone with
//! broker write access could rewrite a signed message's `callbacks`, `chain`,
//! `chord`, `eta` or `expires` — fields that name *further tasks to execute* —
//! and the signature still verified. There was also no timestamp or nonce, so a
//! captured message was replayable forever.

use celers_core::sanitize::TaskValue;
use celers_core::task_signature::{
    FreshnessWindow, HmacSha256, ReplayGuard, SignatureAlgorithm, SignatureError, SignedCallback,
    SignedFields, TaskSignature, TaskSigner,
};
use chrono::{Duration as ChronoDuration, TimeZone, Utc};
use std::time::Duration;
use uuid::Uuid;

const KEY: &[u8] = b"super-secret-shared-key";

/// A richly-populated message touching every signed field.
fn base_fields() -> SignedFields {
    SignedFields::new(
        Uuid::parse_str("11111111-2222-3333-4444-555555555555").expect("valid uuid"),
        "billing.charge",
    )
    .with_args(vec![TaskValue::from(2), TaskValue::from(3)])
    .with_kwarg("note", TaskValue::from("hello"))
    .with_callback(
        SignedCallback::new("billing.receipt")
            .with_task_id(Uuid::nil())
            .with_args(vec![TaskValue::from(7)])
            .with_kwarg("format", TaskValue::from("pdf"))
            .with_option("queue", TaskValue::from("mail"))
            .immutable(true)
            .with_subtask_type("chord"),
    )
    .with_errback(SignedCallback::new("billing.refund"))
    .with_chain(vec![
        SignedCallback::new("ledger.post"),
        SignedCallback::new("ledger.audit"),
    ])
    .with_chord(SignedCallback::new("ledger.finalize"))
    .with_eta(
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
            .single()
            .expect("valid eta"),
    )
    .with_expires(
        Utc.with_ymd_and_hms(2026, 1, 2, 0, 0, 0)
            .single()
            .expect("valid expires"),
    )
    .with_signed_at(
        Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
            .single()
            .expect("valid signed_at"),
    )
    .with_nonce("nonce-0001")
}

// ---------------------------------------------------------------------------
// Version pinning
// ---------------------------------------------------------------------------

#[test]
fn canonical_bytes_carry_the_v2_domain_tag() {
    let bytes = base_fields().canonical_bytes();
    assert!(
        bytes.starts_with(b"celers.task.sig.v2"),
        "the scheme version must be inside the MAC input"
    );
    assert_eq!(
        celers_core::task_signature::SIGNATURE_DOMAIN_TAG,
        b"celers.task.sig.v2"
    );
}

/// The tag a v1 signer would have produced for
/// `(Uuid::nil(), "tasks.add", [Int(2), Int(3)], {"note": "hello"})` under the
/// key `v1-legacy-shared-key`. Computed outside this crate; this crate can no
/// longer produce it, which is the whole point.
const V1_TAG: &str = "0914833ad4da039cc64323dd30fc5b6a7dd0b96a24c9fa4480319bbebbbdba92";
const V1_KEY: &[u8] = b"v1-legacy-shared-key";

/// The fields the v1 tag was computed over.
fn v1_fields() -> SignedFields {
    SignedFields::new(Uuid::nil(), "tasks.add")
        .with_args(vec![TaskValue::from(2), TaskValue::from(3)])
        .with_kwarg("note", TaskValue::from("hello"))
}

/// Rebuild the retired v1 canonical encoding: domain tag `…v1` followed by
/// `id`/`name`/`args`/`kwargs` only, with the same tagged length-prefixed
/// framing v2 still uses.
fn v1_canonical_bytes(fields: &SignedFields) -> Vec<u8> {
    fn write_field(out: &mut Vec<u8>, tag: u8, bytes: &[u8]) {
        out.push(tag);
        out.extend_from_slice(&(bytes.len() as u64).to_be_bytes());
        out.extend_from_slice(bytes);
    }
    fn write_len(out: &mut Vec<u8>, tag: u8, value: u64) {
        out.push(tag);
        out.extend_from_slice(&value.to_be_bytes());
    }
    // v1 only ever had to encode ints and strings for this vector.
    fn value_bytes(value: &TaskValue) -> Vec<u8> {
        match value {
            TaskValue::Int(i) => {
                let mut out = vec![b'i'];
                out.extend_from_slice(&i.to_be_bytes());
                out
            }
            TaskValue::String(s) => {
                let mut out = vec![b's'];
                out.extend_from_slice(&(s.len() as u64).to_be_bytes());
                out.extend_from_slice(s.as_bytes());
                out
            }
            other => panic!("the v1 reference vector only uses ints and strings, got {other:?}"),
        }
    }

    let mut out = Vec::new();
    out.extend_from_slice(b"celers.task.sig.v1");
    write_field(&mut out, b'I', fields.id.as_bytes());
    write_field(&mut out, b'N', fields.name.as_bytes());
    write_len(&mut out, b'A', fields.args.len() as u64);
    for arg in &fields.args {
        write_field(&mut out, b'a', &value_bytes(arg));
    }
    let mut sorted: Vec<&(String, TaskValue)> = fields.kwargs.iter().collect();
    sorted.sort_by(|x, y| x.0.cmp(&y.0));
    write_len(&mut out, b'K', sorted.len() as u64);
    for (key, value) in sorted {
        write_field(&mut out, b'k', key.as_bytes());
        write_field(&mut out, b'v', &value_bytes(value));
    }
    out
}

/// Positive control for [`v1_tags_cannot_be_replayed_against_v2`].
///
/// Without this, that test would pass just as happily against a *mis-computed*
/// constant — anything that is not the current v2 tag fails to verify. This
/// proves `V1_TAG` really is the tag the retired scheme produced, so the
/// rejection below is meaningful.
#[test]
fn the_v1_reference_tag_is_genuinely_a_v1_tag() {
    let mac = HmacSha256::mac(V1_KEY, &v1_canonical_bytes(&v1_fields()));
    assert_eq!(
        TaskSignature::hmac_sha256(&mac).tag,
        V1_TAG,
        "the hard-coded v1 reference tag no longer matches the v1 encoding"
    );
}

/// A tag produced by the v1 scheme must not verify under v2.
#[test]
fn v1_tags_cannot_be_replayed_against_v2() {
    let signer = TaskSigner::new(V1_KEY);
    let fields = v1_fields();

    let v1_signature = TaskSignature {
        algorithm: SignatureAlgorithm::HmacSha256,
        tag: V1_TAG.to_string(),
    };

    assert_eq!(
        signer.verify(&fields, &v1_signature),
        Err(SignatureError::Mismatch),
        "a v1 tag must not verify against the v2 scheme"
    );

    // And the v2 tag over the same fields genuinely differs.
    assert_ne!(signer.sign(&fields).tag, V1_TAG);

    // The difference is the domain tag itself, not an accident of the payload:
    // the two canonical streams differ only from byte 17 onwards.
    let v2_bytes = fields.canonical_bytes();
    assert!(v2_bytes.starts_with(b"celers.task.sig.v2"));
    assert!(v1_canonical_bytes(&fields).starts_with(b"celers.task.sig.v1"));
}

// ---------------------------------------------------------------------------
// Per-field tamper matrix
// ---------------------------------------------------------------------------

/// Every signed field, tampered with one at a time. Each entry must be caught.
#[test]
fn every_signed_field_is_tamper_evident() {
    type Mutator = fn(SignedFields) -> SignedFields;

    let cases: &[(&str, Mutator)] = &[
        ("id", |mut f| {
            f.id = Uuid::nil();
            f
        }),
        ("name", |mut f| {
            f.name = "billing.refund".to_string();
            f
        }),
        ("args value", |mut f| {
            f.args = vec![TaskValue::from(2), TaskValue::from(4)];
            f
        }),
        ("args order", |mut f| {
            f.args.reverse();
            f
        }),
        ("args truncated", |mut f| {
            f.args.truncate(1);
            f
        }),
        ("kwargs value", |mut f| {
            f.kwargs = vec![("note".to_string(), TaskValue::from("goodbye"))];
            f
        }),
        ("kwargs key", |mut f| {
            f.kwargs = vec![("memo".to_string(), TaskValue::from("hello"))];
            f
        }),
        ("kwargs added", |f| {
            f.with_kwarg("extra", TaskValue::from(1))
        }),
        ("callback task hijacked", |mut f| {
            f.callbacks = vec![SignedCallback::new("attacker.exfiltrate")];
            f
        }),
        ("callback appended", |f| {
            f.with_callback(SignedCallback::new("attacker.exfiltrate"))
        }),
        ("callbacks cleared", |mut f| {
            f.callbacks.clear();
            f
        }),
        ("errback hijacked", |mut f| {
            f.errbacks = vec![SignedCallback::new("attacker.exfiltrate")];
            f
        }),
        ("chain member replaced", |mut f| {
            f.chain[1] = SignedCallback::new("attacker.exfiltrate");
            f
        }),
        ("chain reordered", |mut f| {
            f.chain.reverse();
            f
        }),
        ("chain extended", |mut f| {
            f.chain.push(SignedCallback::new("attacker.exfiltrate"));
            f
        }),
        ("chord replaced", |f| {
            f.with_chord(SignedCallback::new("attacker.exfiltrate"))
        }),
        ("chord removed", |mut f| {
            f.chord = None;
            f
        }),
        ("eta moved", |f| {
            f.with_eta(
                Utc.with_ymd_and_hms(2030, 1, 1, 0, 0, 0)
                    .single()
                    .expect("valid eta"),
            )
        }),
        ("eta removed", |mut f| {
            f.eta = None;
            f
        }),
        ("expires extended", |f| {
            f.with_expires(
                Utc.with_ymd_and_hms(2030, 1, 1, 0, 0, 0)
                    .single()
                    .expect("valid expires"),
            )
        }),
        ("expires removed", |mut f| {
            f.expires = None;
            f
        }),
        ("signed_at moved forward", |f| f.signed_now()),
        ("signed_at removed", |mut f| {
            f.signed_at = None;
            f
        }),
        ("nonce swapped", |f| f.with_nonce("nonce-9999")),
        ("nonce removed", |mut f| {
            f.nonce = None;
            f
        }),
    ];

    let signer = TaskSigner::new(KEY);
    let fields = base_fields();
    let signature = signer.sign(&fields);
    signer
        .verify(&fields, &signature)
        .expect("the untouched message must verify");

    for (label, mutate) in cases {
        let tampered = mutate(fields.clone());
        assert_ne!(
            tampered, fields,
            "test bug: mutator {label:?} changed nothing"
        );
        assert_eq!(
            signer.verify(&tampered, &signature),
            Err(SignatureError::Mismatch),
            "tampering with {label:?} was NOT detected"
        );
    }
}

/// Every field of a callback participates too — otherwise the same defect
/// simply moves one level down.
#[test]
fn every_callback_field_is_tamper_evident() {
    type Mutator = fn(SignedCallback) -> SignedCallback;

    let cases: &[(&str, Mutator)] = &[
        ("task", |mut c| {
            c.task = "attacker.exfiltrate".to_string();
            c
        }),
        ("task_id", |c| c.with_task_id(Uuid::max())),
        ("task_id cleared", |mut c| {
            c.task_id = None;
            c
        }),
        ("args", |c| c.with_args(vec![TaskValue::from(8)])),
        ("kwargs value", |mut c| {
            c.kwargs = vec![("format".to_string(), TaskValue::from("csv"))];
            c
        }),
        ("options value", |mut c| {
            c.options = vec![("queue".to_string(), TaskValue::from("attacker"))];
            c
        }),
        ("options cleared", |mut c| {
            c.options.clear();
            c
        }),
        ("immutable flipped", |c| c.immutable(false)),
        ("subtask_type", |c| c.with_subtask_type("group")),
        ("subtask_type cleared", |mut c| {
            c.subtask_type = None;
            c
        }),
    ];

    let signer = TaskSigner::new(KEY);
    let fields = base_fields();
    let signature = signer.sign(&fields);

    for (label, mutate) in cases {
        let mut tampered = fields.clone();
        tampered.callbacks[0] = mutate(tampered.callbacks[0].clone());
        assert_ne!(
            tampered.callbacks[0], fields.callbacks[0],
            "test bug: callback mutator {label:?} changed nothing"
        );
        assert_eq!(
            signer.verify(&tampered, &signature),
            Err(SignatureError::Mismatch),
            "tampering with callback {label:?} was NOT detected"
        );
    }
}

/// Moving a link between lists changes what runs and when, so it must change
/// the MAC even though the multiset of callbacks is unchanged.
#[test]
fn moving_a_link_between_lists_is_detected() {
    let signer = TaskSigner::new(KEY);

    let original = SignedFields::new(Uuid::nil(), "t")
        .with_callback(SignedCallback::new("cleanup"))
        .signed_now();
    let signature = signer.sign(&original);

    let mut moved = original.clone();
    moved.callbacks.clear();
    moved.errbacks.push(SignedCallback::new("cleanup"));
    assert_eq!(
        signer.verify(&moved, &signature),
        Err(SignatureError::Mismatch)
    );

    let mut into_chain = original.clone();
    into_chain.callbacks.clear();
    into_chain.chain.push(SignedCallback::new("cleanup"));
    assert_eq!(
        signer.verify(&into_chain, &signature),
        Err(SignatureError::Mismatch)
    );
}

/// "Absent" and "present but empty" must never share an encoding.
#[test]
fn absent_and_empty_optionals_are_distinguished() {
    let signer = TaskSigner::new(KEY);
    let base = SignedFields::new(Uuid::nil(), "t");

    let no_nonce = base.clone();
    let empty_nonce = base.clone().with_nonce("");
    assert_ne!(signer.sign(&no_nonce).tag, signer.sign(&empty_nonce).tag);

    let no_chord = base.clone();
    let empty_chord = base.clone().with_chord(SignedCallback::new(""));
    assert_ne!(signer.sign(&no_chord).tag, signer.sign(&empty_chord).tag);

    let no_subtask = base.clone().with_callback(SignedCallback::new("c"));
    let empty_subtask = base
        .clone()
        .with_callback(SignedCallback::new("c").with_subtask_type(""));
    assert_ne!(
        signer.sign(&no_subtask).tag,
        signer.sign(&empty_subtask).tag
    );
}

/// Sub-second precision must not be lost, or two distinct etas would collide.
#[test]
fn timestamp_encoding_keeps_subsecond_precision() {
    let signer = TaskSigner::new(KEY);
    let base_instant = Utc
        .timestamp_opt(1_800_000_000, 0)
        .single()
        .expect("valid instant");
    let with_nanos = Utc
        .timestamp_opt(1_800_000_000, 1)
        .single()
        .expect("valid instant");

    let a = SignedFields::new(Uuid::nil(), "t").with_eta(base_instant);
    let b = SignedFields::new(Uuid::nil(), "t").with_eta(with_nanos);
    assert_ne!(signer.sign(&a).tag, signer.sign(&b).tag);
}

// ---------------------------------------------------------------------------
// Canonicalization (things that must NOT change the MAC)
// ---------------------------------------------------------------------------

#[test]
fn map_ordering_does_not_change_the_signature() {
    let signer = TaskSigner::new(KEY);

    let forward = SignedFields::new(Uuid::nil(), "t")
        .with_kwarg("alpha", TaskValue::from(1))
        .with_kwarg("beta", TaskValue::from(2))
        .with_callback(
            SignedCallback::new("cb")
                .with_kwarg("x", TaskValue::from(1))
                .with_kwarg("y", TaskValue::from(2))
                .with_option("queue", TaskValue::from("a"))
                .with_option("countdown", TaskValue::from(5)),
        );

    let reversed = SignedFields::new(Uuid::nil(), "t")
        .with_kwarg("beta", TaskValue::from(2))
        .with_kwarg("alpha", TaskValue::from(1))
        .with_callback(
            SignedCallback::new("cb")
                .with_kwarg("y", TaskValue::from(2))
                .with_kwarg("x", TaskValue::from(1))
                .with_option("countdown", TaskValue::from(5))
                .with_option("queue", TaskValue::from("a")),
        );

    assert_eq!(signer.sign(&forward).tag, signer.sign(&reversed).tag);
    assert!(signer.verify(&reversed, &signer.sign(&forward)).is_ok());
}

// ---------------------------------------------------------------------------
// Freshness
// ---------------------------------------------------------------------------

#[test]
fn fresh_message_within_the_window_is_accepted() {
    let signer = TaskSigner::new(KEY);
    let now = Utc::now();
    let fields =
        SignedFields::new(Uuid::nil(), "t").with_signed_at(now - ChronoDuration::seconds(5));
    let signature = signer.sign(&fields);

    let window = FreshnessWindow::new(Duration::from_secs(60));
    assert!(signer
        .verify_fresh_at(&fields, &signature, &window, now)
        .is_ok());
}

#[test]
fn stale_message_is_rejected_even_though_it_is_authentic() {
    let signer = TaskSigner::new(KEY);
    let now = Utc::now();
    let fields =
        SignedFields::new(Uuid::nil(), "t").with_signed_at(now - ChronoDuration::seconds(3_600));
    let signature = signer.sign(&fields);

    // The MAC itself is fine ...
    assert!(signer.verify(&fields, &signature).is_ok());

    // ... but the message is far outside the freshness window.
    let window = FreshnessWindow::new(Duration::from_secs(60));
    assert_eq!(
        signer.verify_fresh_at(&fields, &signature, &window, now),
        Err(SignatureError::Stale {
            age_secs: 3_600,
            max_age_secs: 60,
        })
    );
}

#[test]
fn future_dated_message_beyond_the_skew_is_rejected() {
    let signer = TaskSigner::new(KEY);
    let now = Utc::now();
    let window =
        FreshnessWindow::new(Duration::from_secs(60)).with_max_clock_skew(Duration::from_secs(30));

    // Inside the allowed skew: accepted.
    let slightly_ahead =
        SignedFields::new(Uuid::nil(), "t").with_signed_at(now + ChronoDuration::seconds(10));
    let signature = signer.sign(&slightly_ahead);
    assert!(signer
        .verify_fresh_at(&slightly_ahead, &signature, &window, now)
        .is_ok());

    // Far in the future: rejected, otherwise it would stay valid forever.
    let far_ahead =
        SignedFields::new(Uuid::nil(), "t").with_signed_at(now + ChronoDuration::days(365));
    let signature = signer.sign(&far_ahead);
    assert!(matches!(
        signer.verify_fresh_at(&far_ahead, &signature, &window, now),
        Err(SignatureError::FutureDated { .. })
    ));
}

#[test]
fn freshness_requires_a_signed_at_stamp() {
    let signer = TaskSigner::new(KEY);
    let fields = SignedFields::new(Uuid::nil(), "t");
    let signature = signer.sign(&fields);

    assert_eq!(
        signer.verify_fresh(
            &fields,
            &signature,
            &FreshnessWindow::new(Duration::from_secs(60))
        ),
        Err(SignatureError::MissingSignedAt)
    );
}

#[test]
fn a_signed_expiry_deadline_is_enforced_on_verify() {
    let signer = TaskSigner::new(KEY);
    let now = Utc::now();
    let fields = SignedFields::new(Uuid::nil(), "t")
        .with_signed_at(now - ChronoDuration::seconds(5))
        .with_expires(now - ChronoDuration::seconds(1));
    let signature = signer.sign(&fields);

    let window = FreshnessWindow::new(Duration::from_secs(60));
    assert!(matches!(
        signer.verify_fresh_at(&fields, &signature, &window, now),
        Err(SignatureError::MessageExpired { .. })
    ));

    // A deadline still in the future is fine.
    let live = SignedFields::new(Uuid::nil(), "t")
        .with_signed_at(now - ChronoDuration::seconds(5))
        .with_expires(now + ChronoDuration::hours(1));
    let live_signature = signer.sign(&live);
    assert!(signer
        .verify_fresh_at(&live, &live_signature, &window, now)
        .is_ok());
}

/// Freshness must be checked *after* the MAC, so the timestamps it reads are
/// known-authentic rather than attacker-supplied.
#[test]
fn a_forged_signature_fails_before_the_freshness_check() {
    let signer = TaskSigner::new(KEY);
    let other = TaskSigner::new(b"a-different-key");
    let fields = SignedFields::new(Uuid::nil(), "t").with_signed_at(Utc::now());
    let forged = other.sign(&fields);

    assert_eq!(
        signer.verify_fresh(
            &fields,
            &forged,
            &FreshnessWindow::new(Duration::from_secs(60))
        ),
        Err(SignatureError::Mismatch)
    );
}

// ---------------------------------------------------------------------------
// Replay
// ---------------------------------------------------------------------------

#[test]
fn the_second_delivery_of_a_message_is_rejected() {
    let signer = TaskSigner::new(KEY);
    let guard = ReplayGuard::new(FreshnessWindow::new(Duration::from_secs(300)));

    let fields = SignedFields::new(Uuid::nil(), "t")
        .signed_now()
        .with_random_nonce();
    let signature = signer.sign(&fields);

    assert!(guard.verify(&signer, &fields, &signature).is_ok());
    assert_eq!(
        guard.verify(&signer, &fields, &signature),
        Err(SignatureError::Replayed(
            fields.nonce.clone().expect("nonce was set")
        ))
    );
    assert_eq!(guard.remembered(), 1);
}

#[test]
fn distinct_nonces_are_all_accepted() {
    let signer = TaskSigner::new(KEY);
    let guard = ReplayGuard::new(FreshnessWindow::new(Duration::from_secs(300)));

    for _ in 0..8 {
        let fields = SignedFields::new(Uuid::new_v4(), "t")
            .signed_now()
            .with_random_nonce();
        let signature = signer.sign(&fields);
        assert!(guard.verify(&signer, &fields, &signature).is_ok());
    }
    assert_eq!(guard.remembered(), 8);
}

#[test]
fn random_nonces_do_not_collide() {
    let mut seen = std::collections::HashSet::new();
    for _ in 0..256 {
        let fields = SignedFields::new(Uuid::nil(), "t").with_random_nonce();
        let nonce = fields.nonce.expect("a random nonce is always set");
        assert_eq!(nonce.len(), 32, "128 bits, hex encoded");
        assert!(seen.insert(nonce), "random nonces must not repeat");
    }
}

#[test]
fn replay_checking_requires_a_nonce() {
    let signer = TaskSigner::new(KEY);
    let guard = ReplayGuard::new(FreshnessWindow::new(Duration::from_secs(300)));

    let fields = SignedFields::new(Uuid::nil(), "t").signed_now();
    let signature = signer.sign(&fields);
    assert_eq!(
        guard.verify(&signer, &fields, &signature),
        Err(SignatureError::MissingNonce)
    );
}

/// A rejected message must not consume its nonce, otherwise an attacker could
/// pre-burn the nonce of a message they cannot forge and lock out the genuine
/// delivery.
#[test]
fn rejected_messages_do_not_poison_the_nonce_cache() {
    let signer = TaskSigner::new(KEY);
    let other = TaskSigner::new(b"a-different-key");
    let guard = ReplayGuard::new(FreshnessWindow::new(Duration::from_secs(300)));

    let fields = SignedFields::new(Uuid::nil(), "t")
        .signed_now()
        .with_nonce("shared-nonce");

    // Forged signature: rejected, and the nonce is not remembered.
    let forged = other.sign(&fields);
    assert_eq!(
        guard.verify(&signer, &fields, &forged),
        Err(SignatureError::Mismatch)
    );
    assert_eq!(guard.remembered(), 0);

    // Stale but authentic: also rejected, also not remembered.
    let stale = SignedFields::new(Uuid::nil(), "t")
        .with_signed_at(Utc::now() - ChronoDuration::hours(1))
        .with_nonce("shared-nonce");
    let stale_signature = signer.sign(&stale);
    assert!(matches!(
        guard.verify(&signer, &stale, &stale_signature),
        Err(SignatureError::Stale { .. })
    ));
    assert_eq!(guard.remembered(), 0);

    // The genuine delivery still goes through.
    let signature = signer.sign(&fields);
    assert!(guard.verify(&signer, &fields, &signature).is_ok());
}

#[test]
fn nonces_outside_the_window_are_pruned() {
    let signer = TaskSigner::new(KEY);
    let window =
        FreshnessWindow::new(Duration::from_secs(60)).with_max_clock_skew(Duration::from_secs(0));
    let guard = ReplayGuard::new(window);

    let start = Utc::now();
    let fields = SignedFields::new(Uuid::nil(), "t")
        .with_signed_at(start)
        .with_nonce("old-nonce");
    let signature = signer.sign(&fields);

    assert!(guard.verify_at(&signer, &fields, &signature, start).is_ok());
    assert_eq!(guard.remembered(), 1);

    // Well past the window: the entry can be dropped, because a message that
    // old is rejected by the freshness check anyway.
    guard.prune_at(start + ChronoDuration::seconds(600));
    assert_eq!(guard.remembered(), 0);
    assert_eq!(*guard.window(), window);
}
