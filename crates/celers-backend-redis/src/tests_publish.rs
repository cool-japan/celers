#![cfg(test)]

//! Publish-on-set: a real Celery client's `AsyncResult.get()` subscribes to
//! a pub/sub channel named after the result key (see
//! `celery.backends.redis.BaseKeyValueStoreBackend._set`) and expects the
//! published message to be exactly what a `GET` of that key would return.
//! A writer that only `SET`s leaves such a client blocked until its own
//! poll timeout.
//!
//! Split out of `tests.rs` purely for file size (that file is at the
//! workspace's 2000-line-per-file cap) — no other reason.

use std::time::Duration;

use uuid::Uuid;

use crate::backend::RedisResultBackend;
use crate::result_backend_trait::ResultBackend;
use crate::types::{TaskMeta, TaskResult};

/// `store_result` must PUBLISH on the channel named after the result key,
/// carrying exactly the bytes a `GET` of that key returns — not a summary,
/// not a different channel. Compression/encryption may make those bytes
/// opaque to a plain Python client (a separate, pre-existing concern), so
/// what this asserts is the invariant that is actually load-bearing here:
/// SET and PUBLISH agree, byte for byte, so a subscriber never has to
/// re-fetch to learn what was written.
///
/// Skipped, visibly, when `CELERS_TEST_REDIS_URL` is not set.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn store_result_publishes_on_the_result_key_channel_with_the_stored_bytes() {
    let Ok(url) = std::env::var("CELERS_TEST_REDIS_URL") else {
        eprintln!(
            "SKIPPED: store_result_publishes_on_the_result_key_channel_with_the_stored_bytes \
             (set CELERS_TEST_REDIS_URL to run)"
        );
        return;
    };

    let mut backend = RedisResultBackend::new(&url).expect("redis client");
    let task_id = Uuid::new_v4();
    let channel = backend.task_key(task_id);

    // Subscribe *before* the write, exactly like a real waiting client would.
    let client = redis::Client::open(url).expect("raw redis client");
    let mut pubsub = client.get_async_pubsub().await.expect("pubsub connection");
    pubsub.subscribe(&channel).await.expect("subscribe");

    let mut meta = TaskMeta::new(task_id, "publish_on_set_test".to_string());
    meta.result = TaskResult::Success(serde_json::json!({"answer": 42}));

    let writer = {
        let mut backend = backend.clone();
        let meta = meta.clone();
        tokio::spawn(async move {
            // Give the subscription a moment to register before publishing —
            // Redis pub/sub delivers only to subscribers already listening.
            tokio::time::sleep(Duration::from_millis(100)).await;
            <RedisResultBackend as ResultBackend>::store_result(&mut backend, task_id, &meta)
                .await
                .expect("store_result");
        })
    };

    use futures_util::StreamExt;
    let mut stream = pubsub.on_message();
    let message = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .expect("a subscriber must receive the publish before the timeout")
        .expect("the pub/sub stream must stay open");
    writer.await.expect("writer task");

    assert_eq!(
        message.get_channel_name(),
        channel,
        "PUBLISH must target the channel named after the result key"
    );
    let published: Vec<u8> = message.get_payload().expect("payload bytes");

    // Fetch the raw stored bytes directly (bypassing decode/decompress on
    // either side) so the comparison below is a pure byte-for-byte check.
    let stored: Vec<u8> = {
        use redis::AsyncCommands;
        let mut conn = backend.connection().await.expect("connection");
        conn.get::<_, Option<Vec<u8>>>(&channel)
            .await
            .expect("GET the stored key")
            .expect("the result must be stored under the same key it was published on")
    };

    assert_eq!(
        published, stored,
        "the published message must be byte-for-byte identical to what GET returns for the \
         same key -- this is Celery's 'SET and PUBLISH' contract"
    );

    // Cleanup so a re-run of this test (or another test using the same
    // prefix) never sees a stale key.
    backend.delete_result(task_id).await.expect("cleanup");
}
