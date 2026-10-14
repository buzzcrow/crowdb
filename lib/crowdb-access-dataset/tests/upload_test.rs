// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/upload.rs"]
mod upload;
use bytes::Bytes;
use crowdb_access_dataset::{
    DatasetIdentity, DatasetUploads, NamespacePath, OperationId, UploadError, UploadState,
};
use md5::{Digest, Md5};
use std::sync::{atomic::Ordering, Arc};
use upload::{TestPayload, TestStore};

fn location(name: &str) -> Vec<u8> {
    DatasetUploads::location(
        &DatasetIdentity::new(NamespacePath::root(), name).unwrap(),
        OperationId::random(),
    )
    .unwrap()
}
#[tokio::test]
async fn lost_storage_reply_recovers_exact_intent_without_another_allocation() {
    let store = Arc::new(TestStore::default());
    let payloads = Arc::new(TestPayload::default());
    let uploads = DatasetUploads::new(store.clone(), payloads.clone());
    let key = location("recover");
    uploads
        .create(&key, 8, 4, Md5::digest(b"abcdefgh").into(), 100)
        .await
        .unwrap();
    payloads.lose_reply.store(true, Ordering::Release);
    assert!(uploads
        .put_part(&key, 0, Bytes::from_static(b"abcd"), 1)
        .await
        .is_err());
    let restarted = DatasetUploads::new(store, payloads.clone());
    restarted
        .put_part(&key, 0, Bytes::from_static(b"abcd"), 2)
        .await
        .unwrap();
    assert_eq!(payloads.writes.load(Ordering::Acquire), 1);
    assert!(matches!(
        restarted.put_part(&key, 0, Bytes::from_static(b"xxxx"), 2).await,
        Err(UploadError::Conflict)
    ));
    assert!(restarted.complete(&key, 3).await.is_err());
    restarted
        .put_part(&key, 1, Bytes::from_static(b"efgh"), 3)
        .await
        .unwrap();
    assert_eq!(
        restarted.complete(&key, 4).await.unwrap().state,
        UploadState::Complete
    );
    assert_eq!(restarted.read_part(&key, 1).await.unwrap(), b"efgh".as_slice());
}
#[tokio::test]
async fn independent_owners_and_deferred_cleanup_survive_restart() {
    let store = Arc::new(TestStore::default());
    let payloads = Arc::new(TestPayload::default());
    let uploads = DatasetUploads::new(store.clone(), payloads.clone());
    let key = location("shared");
    uploads
        .create(&key, 4, 4, Md5::digest(b"data").into(), 10)
        .await
        .unwrap();
    uploads
        .put_part(&key, 0, Bytes::from_static(b"data"), 1)
        .await
        .unwrap();
    uploads.complete(&key, 2).await.unwrap();
    let first = OperationId::random();
    let second = OperationId::random();
    uploads.claim(&key, first, 3).await.unwrap();
    uploads.claim(&key, second, 3).await.unwrap();
    uploads.untrack(&key).await.unwrap();
    assert!(matches!(uploads.abort(&key).await, Err(UploadError::Conflict)));
    uploads.release(&key, first).await.unwrap();
    assert_eq!(uploads.read_part(&key, 0).await.unwrap(), b"data".as_slice());
    uploads.release(&key, second).await.unwrap();
    payloads.defer_reclaim.store(true, Ordering::Release);
    let aborted = uploads.abort(&key).await.unwrap();
    assert_eq!(aborted.state, UploadState::Aborted);
    assert!(!aborted.cleanup_complete);
    let restarted = DatasetUploads::new(store, payloads.clone());
    payloads.defer_reclaim.store(false, Ordering::Release);
    restarted.expire(20).await.unwrap();
    assert!(restarted.get(&key).await.unwrap().cleanup_complete);
}
#[tokio::test]
async fn invalid_length_digest_and_expiry_do_not_publish() {
    let uploads = DatasetUploads::new(Arc::new(TestStore::default()), Arc::new(TestPayload::default()));
    let key = location("invalid");
    uploads.create(&key, 4, 4, [0; 16], 10).await.unwrap();
    assert!(uploads
        .put_part(&key, 0, Bytes::from_static(b"bad"), 1)
        .await
        .is_err());
    uploads
        .put_part(&key, 0, Bytes::from_static(b"data"), 1)
        .await
        .unwrap();
    assert!(matches!(
        uploads.complete(&key, 2).await,
        Err(UploadError::Integrity)
    ));
    uploads.expire(11).await.unwrap();
    assert_eq!(uploads.get(&key).await.unwrap().state, UploadState::Aborted);
}
