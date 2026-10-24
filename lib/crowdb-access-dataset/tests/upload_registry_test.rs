// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/access_store.rs"]
mod access_store;
#[path = "common/upload.rs"]
#[allow(dead_code)]
mod upload;
use bytes::Bytes;
use crowdb_access_dataset::*;
use md5::{Digest, Md5};
use std::sync::{atomic::Ordering, Arc};

#[tokio::test]
async fn independent_uploads_need_one_create_write_and_no_shared_registry() {
    let store = access_store::TestAccessStore::shared();
    let payloads = Arc::new(upload::TestPayload::default());
    let uploads = DatasetUploads::new(store.clone(), payloads.clone());
    let identity = DatasetIdentity::new(NamespacePath::root(), "expiry").unwrap();
    let keys: Vec<_> = (0..64)
        .map(|_| DatasetUploads::location(&identity, OperationId::random()).unwrap())
        .collect();
    let results = futures::future::join_all(
        keys.iter()
            .map(|key| uploads.create(key, 4, 4, Md5::digest(b"data").into(), 10)),
    )
    .await;
    for result in results {
        result.unwrap();
    }
    assert_eq!(store.writes.load(Ordering::Relaxed), 64);
    for key in [b"DS\0UTAIL\0".as_slice(), b"DS\0USCAN\0", b"DS\0UPAGE\0"] {
        assert!(store.get(key).await.unwrap().is_none());
    }
    uploads
        .put_part(&keys[0], 0, Bytes::from_static(b"data"), 1)
        .await
        .unwrap();
    uploads.complete(&keys[0], 2).await.unwrap();
    let owner = OperationId::random();
    uploads.claim(&keys[0], owner, 3).await.unwrap();
    let restarted = DatasetUploads::new(store, payloads);
    let mut cursor = UploadExpiryCursor::default();
    loop {
        restarted.expire_step(20, &mut cursor).await.unwrap();
        if !cursor.has_more() {
            break;
        }
    }
    assert_eq!(restarted.get(&keys[0]).await.unwrap().state, UploadState::Claimed);
    for key in &keys[1..] {
        assert!(restarted.get(key).await.unwrap().cleanup_complete);
    }
    restarted.release(&keys[0], owner).await.unwrap();
    loop {
        restarted.expire_step(20, &mut cursor).await.unwrap();
        if !cursor.has_more() {
            break;
        }
    }
    assert!(restarted.get(&keys[0]).await.unwrap().cleanup_complete);
}

#[tokio::test]
async fn legacy_registry_fields_are_unnecessary_for_expiry_after_restart() {
    let store = access_store::TestAccessStore::shared();
    let payloads = Arc::new(upload::TestPayload::default());
    let uploads = DatasetUploads::new(store.clone(), payloads.clone());
    let identity = DatasetIdentity::new(NamespacePath::root(), "legacy-registry").unwrap();
    let location = DatasetUploads::location(&identity, OperationId::random()).unwrap();
    uploads.create(&location, 4, 4, [0; 16], 10).await.unwrap();
    let stored = store.get(&location).await.unwrap().unwrap();
    let mut old: serde_json::Value = serde_json::from_slice(&stored.bytes).unwrap();
    old["version"] = 2.into();
    old["registry_page"] = 42.into();
    old["registry_generation"] = serde_json::Value::Null;
    old["registry_fence"] = serde_json::Value::Null;
    store
        .compare_exchange(&location, Some(&stored.bytes), &serde_json::to_vec(&old).unwrap())
        .await
        .unwrap();
    let restarted = DatasetUploads::new(store.clone(), payloads);
    restarted
        .expire_step(20, &mut crowdb_access_dataset::UploadExpiryCursor::default())
        .await
        .unwrap();
    assert!(restarted.get(&location).await.unwrap().cleanup_complete);
    let writes = store.writes.load(Ordering::Relaxed);
    restarted.abort(&location).await.unwrap();
    restarted
        .expire_step(20, &mut crowdb_access_dataset::UploadExpiryCursor::default())
        .await
        .unwrap();
    assert_eq!(store.writes.load(Ordering::Relaxed), writes);
}

#[tokio::test]
async fn owner_retries_rebase_on_observations_without_another_get() {
    let store = access_store::TestAccessStore::shared();
    let uploads = DatasetUploads::new(store.clone(), Arc::new(upload::TestPayload::default()));
    let identity = DatasetIdentity::new(NamespacePath::root(), "owner-retry").unwrap();
    let location = DatasetUploads::location(&identity, OperationId::random()).unwrap();
    uploads
        .create(&location, 4, 4, Md5::digest(b"data").into(), 10)
        .await
        .unwrap();
    uploads
        .put_part(&location, 0, Bytes::from_static(b"data"), 1)
        .await
        .unwrap();
    uploads.complete(&location, 2).await.unwrap();
    let owner = OperationId::random();
    for action in 0..3 {
        store.upload_conflicts.store(3, Ordering::Relaxed);
        let reads = store.reads.load(Ordering::Relaxed);
        match action {
            0 => {
                uploads.claim(&location, owner, 3).await.unwrap();
            }
            1 => uploads.release(&location, owner).await.unwrap(),
            _ => uploads.retire_owner(&location, owner).await.unwrap(),
        }
        assert_eq!(store.upload_conflicts.load(Ordering::Relaxed), 0);
        assert_eq!(store.reads.load(Ordering::Relaxed), reads + 1);
    }
}
