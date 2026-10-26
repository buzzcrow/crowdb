// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/upload_intent.rs"]
mod intent;
#[path = "common/upload.rs"]
#[allow(dead_code)]
mod upload;
use bytes::Bytes;
use crowdb_access_dataset::{DatasetStore, PayloadStorage, UploadError};
use intent::TestFault;
use std::sync::atomic::Ordering;

#[tokio::test]
async fn successful_part_needs_four_gets_and_three_cas_and_replay_writes_no_payload() {
    let (store, payload, uploads) = intent::fixture(TestFault::None).await;
    let part = uploads
        .put_part(&store.location, 0, Bytes::from_static(b"data"), 1)
        .await
        .unwrap();
    assert_eq!(store.reads.load(Ordering::Relaxed), 4);
    assert_eq!(store.mutations.load(Ordering::Relaxed), 3);
    assert_eq!(payload.callbacks.load(Ordering::Relaxed), 1);
    assert_eq!(payload.rejections.load(Ordering::Relaxed), 0);
    assert_eq!(payload.physical_writes.load(Ordering::Relaxed), 1);
    assert_eq!(payload.read(&part.locations).await.unwrap(), b"data"[..]);
    assert_eq!(
        uploads
            .put_part(&store.location, 0, Bytes::from_static(b"data"), 2)
            .await
            .unwrap(),
        part
    );
    assert_eq!(store.reads.load(Ordering::Relaxed), 6);
    assert_eq!(store.mutations.load(Ordering::Relaxed), 3);
    assert_eq!(payload.callbacks.load(Ordering::Relaxed), 1);
    assert_eq!(payload.physical_writes.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn abort_on_either_side_of_intent_cas_rejects_before_write_with_no_physical_bytes() {
    for fault in [TestFault::AbortBeforeIntent, TestFault::AbortAfterIntent] {
        let (store, payload, uploads) = intent::fixture(fault).await;
        assert!(matches!(
            uploads
                .put_part(&store.location, 0, Bytes::from_static(b"data"), 1)
                .await,
            Err(UploadError::Storage(_))
        ));
        assert_eq!(store.reads.load(Ordering::Relaxed), 3, "{fault:?}");
        assert_eq!(store.mutations.load(Ordering::Relaxed), 2);
        assert_eq!(payload.callbacks.load(Ordering::Relaxed), 1);
        assert_eq!(payload.rejections.load(Ordering::Relaxed), 1);
        assert_eq!(payload.physical_writes.load(Ordering::Relaxed), 0);
    }
}

#[tokio::test]
async fn reclaimed_part_fences_the_intent_cas_without_a_followup_session_get() {
    let (store, payload, uploads) = intent::fixture(TestFault::ReclaimBeforeIntent).await;
    assert!(uploads
        .put_part(&store.location, 0, Bytes::from_static(b"data"), 1)
        .await
        .is_err());
    assert_eq!(store.reads.load(Ordering::Relaxed), 2);
    assert_eq!(store.mutations.load(Ordering::Relaxed), 2);
    assert_eq!(payload.callbacks.load(Ordering::Relaxed), 1);
    assert_eq!(payload.rejections.load(Ordering::Relaxed), 1);
    assert_eq!(payload.physical_writes.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn uncertain_intent_cas_returns_without_confirmation_or_physical_write() {
    let (store, payload, uploads) = intent::fixture(TestFault::UnknownIntent).await;
    assert!(uploads
        .put_part(&store.location, 0, Bytes::from_static(b"data"), 1)
        .await
        .is_err());
    assert_eq!(store.reads.load(Ordering::Relaxed), 2);
    assert_eq!(store.mutations.load(Ordering::Relaxed), 2);
    assert_eq!(payload.callbacks.load(Ordering::Relaxed), 1);
    assert_eq!(payload.rejections.load(Ordering::Relaxed), 1);
    assert_eq!(payload.physical_writes.load(Ordering::Relaxed), 0);
    let stored = store.inner.get(&store.part).await.unwrap().unwrap();
    let record: serde_json::Value = serde_json::from_slice(&stored.bytes).unwrap();
    assert_eq!(record["state"], "Intent");
    assert_eq!(record["part"]["locations"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn abort_after_physical_write_is_still_rejected_before_accepting_the_part() {
    let (store, payload, uploads) = intent::fixture(TestFault::AbortBeforeAccept).await;
    assert!(matches!(
        uploads
            .put_part(&store.location, 0, Bytes::from_static(b"data"), 1)
            .await,
        Err(UploadError::Conflict)
    ));
    assert_eq!(store.reads.load(Ordering::Relaxed), 4);
    assert_eq!(store.mutations.load(Ordering::Relaxed), 2);
    assert_eq!(payload.callbacks.load(Ordering::Relaxed), 1);
    assert_eq!(payload.rejections.load(Ordering::Relaxed), 0);
    assert_eq!(payload.physical_writes.load(Ordering::Relaxed), 1);
}
