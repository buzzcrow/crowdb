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
async fn single_part_completion_still_verifies_actual_stored_bytes() {
    let payloads = Arc::new(TestPayload::default());
    let uploads = DatasetUploads::new(Arc::new(TestStore::default()), payloads.clone());
    let key = location("single-corruption");
    uploads
        .create(&key, 4, 4, Md5::digest(b"data").into(), 100)
        .await
        .unwrap();
    uploads
        .put_part(&key, 0, Bytes::from_static(b"data"), 1)
        .await
        .unwrap();
    payloads.corrupt_read.store(true, Ordering::Relaxed);
    assert!(matches!(
        uploads.complete(&key, 2).await,
        Err(UploadError::Integrity)
    ));
    assert_eq!(uploads.metadata(&key).await.unwrap().state, UploadState::Open);
    payloads.corrupt_read.store(false, Ordering::Relaxed);
    assert_eq!(
        uploads.complete(&key, 3).await.unwrap().state,
        UploadState::Complete
    );
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
    restarted
        .expire_step(20, &mut crowdb_access_dataset::UploadExpiryCursor::default())
        .await
        .unwrap();
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
    uploads
        .expire_step(11, &mut crowdb_access_dataset::UploadExpiryCursor::default())
        .await
        .unwrap();
    assert_eq!(uploads.get(&key).await.unwrap().state, UploadState::Aborted);
}

#[tokio::test]
async fn readable_unconfirmed_bytes_cannot_be_accepted_as_durable() {
    let store = Arc::new(TestStore::default());
    let payloads = Arc::new(TestPayload::default());
    let uploads = DatasetUploads::new(store.clone(), payloads.clone());
    let key = location("durability");
    uploads
        .create(&key, 4, 4, Md5::digest(b"data").into(), 100)
        .await
        .unwrap();
    payloads.lose_reply.store(true, Ordering::Release);
    payloads.unconfirmed.store(true, Ordering::Release);
    assert!(uploads
        .put_part(&key, 0, Bytes::from_static(b"data"), 1)
        .await
        .is_err());
    let restarted = DatasetUploads::new(store, payloads.clone());
    assert!(matches!(
        restarted.put_part(&key, 0, Bytes::from_static(b"data"), 2).await,
        Err(UploadError::Pending)
    ));
    assert!(restarted.get(&key).await.unwrap().parts.is_empty());
    payloads.unconfirmed.store(false, Ordering::Release);
    restarted
        .put_part(&key, 0, Bytes::from_static(b"data"), 3)
        .await
        .unwrap();
    restarted.complete(&key, 4).await.unwrap();
    restarted
        .put_part(&key, 0, Bytes::from_static(b"data"), 5)
        .await
        .unwrap();
    assert_eq!(payloads.writes.load(Ordering::Acquire), 1);
}

#[tokio::test]
async fn fenced_abandoned_range_is_retired_before_retrying_the_same_logical_part() {
    let payloads = Arc::new(TestPayload::default());
    let uploads = DatasetUploads::new(Arc::new(TestStore::default()), payloads.clone());
    let key = location("abandoned");
    uploads
        .create(&key, 4, 4, Md5::digest(b"data").into(), 100)
        .await
        .unwrap();
    payloads.lose_reply.store(true, Ordering::Release);
    assert!(uploads
        .put_part(&key, 0, Bytes::from_static(b"data"), 1)
        .await
        .is_err());
    payloads.abandoned.store(true, Ordering::Release);
    payloads.defer_reclaim.store(true, Ordering::Release);
    assert!(matches!(
        uploads.put_part(&key, 0, Bytes::from_static(b"data"), 2).await,
        Err(UploadError::Pending)
    ));
    assert_eq!(payloads.writes.load(Ordering::Acquire), 1);
    payloads.defer_reclaim.store(false, Ordering::Release);
    uploads
        .put_part(&key, 0, Bytes::from_static(b"data"), 3)
        .await
        .unwrap();
    let result = uploads.complete(&key, 4).await.unwrap();
    assert_eq!(result.parts.len(), 1);
    assert_eq!(payloads.writes.load(Ordering::Acquire), 2);
    assert_eq!(uploads.read_part(&key, 0).await.unwrap(), b"data".as_slice());
}

#[tokio::test]
async fn retired_owner_cannot_reclaim_shared_upload_after_restart() {
    let store = Arc::new(TestStore::default());
    let payloads = Arc::new(TestPayload::default());
    let uploads = DatasetUploads::new(store.clone(), payloads.clone());
    let key = location("retired");
    uploads
        .create(&key, 4, 4, Md5::digest(b"data").into(), 100)
        .await
        .unwrap();
    uploads
        .put_part(&key, 0, Bytes::from_static(b"data"), 1)
        .await
        .unwrap();
    assert_eq!(uploads.complete(&key, 2).await.unwrap().version, 4);
    let first = OperationId::random();
    let second = OperationId::random();
    uploads.claim(&key, first, 3).await.unwrap();
    uploads.claim(&key, second, 3).await.unwrap();
    uploads.retire_owner(&key, first).await.unwrap();
    let late = OperationId::random();
    uploads.retire_owner(&key, late).await.unwrap();
    assert!(matches!(
        uploads.claim(&key, late, 4).await,
        Err(UploadError::Conflict)
    ));
    let restarted = DatasetUploads::new(store, payloads);
    assert_eq!(restarted.metadata(&key).await.unwrap().version, 4);
    assert!(matches!(
        restarted.claim(&key, first, 4).await,
        Err(UploadError::Conflict)
    ));
    assert!(matches!(restarted.abort(&key).await, Err(UploadError::Conflict)));
    restarted.retire_owner(&key, first).await.unwrap();
    assert_eq!(restarted.read_part(&key, 0).await.unwrap(), b"data".as_slice());
    restarted.retire_owner(&key, second).await.unwrap();
    assert!(restarted.abort(&key).await.unwrap().cleanup_complete);
    assert!(matches!(
        restarted.claim(&key, second, 5).await,
        Err(UploadError::Conflict)
    ));
}
