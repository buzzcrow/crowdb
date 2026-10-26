// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/upload.rs"]
#[allow(dead_code)]
mod upload;

use bytes::Bytes;
use crowdb_access_dataset::*;
use futures::{stream, StreamExt};
use md5::{Digest, Md5};
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

fn location() -> Vec<u8> {
    DatasetUploads::location(
        &DatasetIdentity::new(NamespacePath::root(), "part-facts").unwrap(),
        OperationId::random(),
    )
    .unwrap()
}

#[tokio::test]
async fn concurrent_parts_do_not_update_a_shared_index_and_listing_is_paged() {
    const PARTS: u32 = 257;
    let store = Arc::new(upload::TestStore::default());
    let uploads = DatasetUploads::new(store.clone(), Arc::new(upload::TestPayload::default()));
    let key = location();
    uploads
        .create(
            &key,
            u64::from(PARTS),
            1,
            Md5::digest(vec![b'x'; PARTS as usize]).into(),
            100,
        )
        .await
        .unwrap();
    stream::iter((0..PARTS).rev())
        .map(|index| {
            let uploads = &uploads;
            let key = &key;
            async move {
                uploads
                    .put_part(key, index, Bytes::from_static(b"x"), 1)
                    .await
                    .unwrap()
            }
        })
        .buffer_unordered(32)
        .collect::<Vec<_>>()
        .await;
    assert_eq!(store.mutations.load(Ordering::Relaxed), 1 + 3 * u64::from(PARTS));
    let reads = store.reads.load(Ordering::Relaxed);
    let scans = store.scans.load(Ordering::Relaxed);
    let result = uploads.complete(&key, 2).await.unwrap();
    assert_eq!(result.parts.len(), PARTS as usize);
    assert!(result
        .parts
        .iter()
        .enumerate()
        .all(|(index, part)| index == part.index as usize));
    assert_eq!(store.reads.load(Ordering::Relaxed) - reads, 1);
    assert_eq!(store.scans.load(Ordering::Relaxed) - scans, 3);
    let mut prefix = key;
    prefix.extend(b"\0I");
    assert!(store
        .scan_page(
            &MetadataScan {
                prefix,
                after: None,
                budget: MetadataScanBudget {
                    items: 1024,
                    bytes: 1024 * 1024,
                    timeout: Duration::from_secs(2)
                }
            },
            &ReadCancellation::new()
        )
        .await
        .unwrap()
        .entries
        .is_empty());
}

#[tokio::test]
async fn final_part_facts_survive_restart_without_trusting_legacy_index_bytes() {
    let store = Arc::new(upload::TestStore::default());
    let payload = Arc::new(upload::TestPayload::default());
    let uploads = DatasetUploads::new(store.clone(), payload.clone());
    let key = location();
    uploads
        .create(&key, 1, 1, Md5::digest(b"x").into(), 100)
        .await
        .unwrap();
    let part = uploads
        .put_part(&key, 0, Bytes::from_static(b"x"), 1)
        .await
        .unwrap();
    let mut legacy_index = key.clone();
    legacy_index.extend(b"\0I\0\0\0\0");
    store
        .compare_exchange(&legacy_index, None, b"invalid obsolete index")
        .await
        .unwrap();
    let restarted = DatasetUploads::new(store.clone(), payload);
    assert_eq!(restarted.get(&key).await.unwrap().parts, vec![part.clone()]);
    let writes = store.mutations.load(Ordering::Relaxed);
    assert_eq!(
        restarted
            .put_part(&key, 0, Bytes::from_static(b"x"), 2)
            .await
            .unwrap(),
        part
    );
    assert_eq!(store.mutations.load(Ordering::Relaxed), writes);
    assert_eq!(restarted.complete(&key, 3).await.unwrap().parts, vec![part]);
}

#[tokio::test]
async fn listing_rejects_corrupt_part_identity_and_boundaries_before_completion() {
    for field in ["index", "length"] {
        let store = Arc::new(upload::TestStore::default());
        let uploads = DatasetUploads::new(store.clone(), Arc::new(upload::TestPayload::default()));
        let key = location();
        uploads
            .create(&key, 1, 1, Md5::digest(b"x").into(), 100)
            .await
            .unwrap();
        uploads
            .put_part(&key, 0, Bytes::from_static(b"x"), 1)
            .await
            .unwrap();
        let mut part_key = key.clone();
        part_key.extend(b"\0P\0\0\0\0");
        let original = store.get(&part_key).await.unwrap().unwrap();
        let mut entry: serde_json::Value = serde_json::from_slice(&original.bytes).unwrap();
        entry["part"][field] = 2.into();
        store
            .compare_exchange(
                &part_key,
                Some(&original.bytes),
                &serde_json::to_vec(&entry).unwrap(),
            )
            .await
            .unwrap();
        assert!(
            matches!(uploads.get(&key).await, Err(UploadError::Integrity)),
            "{field}"
        );
        assert!(
            matches!(uploads.complete(&key, 2).await, Err(UploadError::Integrity)),
            "{field}"
        );
    }
}
