#[path = "common/expiry_payload.rs"]
mod payload;
#[path = "common/upload.rs"]
#[allow(dead_code)]
mod upload;

use bytes::Bytes;
use crowdb_access_dataset::*;
use md5::{Digest, Md5};
use std::{
    collections::BTreeSet,
    sync::{atomic::Ordering, Arc},
};

fn location(seed: u8) -> Vec<u8> {
    DatasetUploads::location(
        &DatasetIdentity::new(NamespacePath::root(), "expiry-step").unwrap(),
        OperationId::from_bytes(&[seed; 16]).unwrap(),
    )
    .unwrap()
}

async fn keys(store: &upload::TestStore) -> BTreeSet<Vec<u8>> {
    let page = store
        .scan_page(
            &MetadataScan {
                prefix: b"DS\0".to_vec(),
                after: None,
                budget: MetadataScanBudget {
                    items: 1024,
                    bytes: 16 * 1024 * 1024,
                    timeout: std::time::Duration::from_secs(2),
                },
            },
            &ReadCancellation::new(),
        )
        .await
        .unwrap();
    assert!(page.after.is_none());
    page.entries.into_iter().map(|(key, _)| key).collect()
}

#[tokio::test]
async fn a_sparse_ten_thousand_part_upload_and_deferred_notifications_do_not_starve_a_small_one() {
    let store = Arc::new(upload::TestStore::default());
    let payload = Arc::new(payload::TestExpiryPayload::default());
    let uploads = DatasetUploads::new(store.clone(), payload.clone());
    let large = location(1);
    let small = location(2);
    uploads.create(&large, 10_000, 1, [0; 16], 10).await.unwrap();
    for index in 0..33 {
        uploads
            .put_part(&large, index, Bytes::from_static(b"x"), 1)
            .await
            .unwrap();
    }
    uploads
        .create(&small, 1, 1, Md5::digest(b"y").into(), 10)
        .await
        .unwrap();
    uploads
        .put_part(&small, 0, Bytes::from_static(b"y"), 1)
        .await
        .unwrap();
    payload.deferred_through.store(33, Ordering::SeqCst);
    let before = keys(&store).await;
    let mut cursor = UploadExpiryCursor::default();
    for _ in 0..3 {
        let attempts = payload.attempted_ranges.load(Ordering::SeqCst);
        let step = uploads.expire_step(20, &mut cursor).await.unwrap();
        assert!(step.scanned <= 20 && step.part_reads <= 32 && step.range_notifications <= 64);
        assert_eq!(
            payload.attempted_ranges.load(Ordering::SeqCst) - attempts,
            step.range_notifications as u64
        );
        assert!(cursor.pending_uploads() <= 32);
    }
    assert!(uploads.metadata(&small).await.unwrap().cleanup_complete);
    assert!(!uploads.metadata(&large).await.unwrap().cleanup_complete);
    assert!(cursor.pending_uploads() != 0);
    payload.deferred_through.store(0, Ordering::SeqCst);
    // A new Access instance discards every in-memory scheduling hint.
    let uploads = DatasetUploads::new(store.clone(), payload);
    let mut cursor = UploadExpiryCursor::default();
    for _ in 0..320 {
        uploads.expire_step(20, &mut cursor).await.unwrap();
        if uploads.metadata(&large).await.unwrap().cleanup_complete {
            break;
        }
    }
    assert!(uploads.metadata(&large).await.unwrap().cleanup_complete);
    assert!(
        keys(&store).await.is_subset(&before),
        "GC scheduling must not create progress records"
    );
}

#[tokio::test(start_paused = true)]
async fn timed_out_notifications_retain_exact_ranges_and_recover_after_cursor_loss() {
    let store = Arc::new(upload::TestStore::default());
    let payload = Arc::new(payload::TestExpiryPayload::default());
    let uploads = DatasetUploads::new(store.clone(), payload.clone());
    let key = location(3);
    uploads.create(&key, 1, 1, [0; 16], 10).await.unwrap();
    uploads
        .put_part(&key, 0, Bytes::from_static(b"x"), 1)
        .await
        .unwrap();
    payload.stall.store(true, Ordering::SeqCst);
    let mut cursor = UploadExpiryCursor::default();
    let step = uploads.expire_step(20, &mut cursor).await.unwrap();
    assert!(step.time_limited);
    assert_eq!(step.range_notifications, 1);
    assert_eq!(step.part_reads, 1);
    assert_eq!(cursor.pending_uploads(), 1);
    assert!(!uploads.metadata(&key).await.unwrap().cleanup_complete);
    payload.stall.store(false, Ordering::SeqCst);
    let mut restarted = UploadExpiryCursor::default();
    uploads.expire_step(20, &mut restarted).await.unwrap();
    assert!(uploads.metadata(&key).await.unwrap().cleanup_complete);
    assert_eq!(payload.attempted_ranges.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn claimed_resources_and_corrupt_discovery_rows_do_not_hide_later_expired_uploads() {
    let store = Arc::new(upload::TestStore::default());
    let uploads = DatasetUploads::new(store.clone(), Arc::new(payload::TestExpiryPayload::default()));
    let held = location(4);
    uploads
        .create(&held, 1, 1, Md5::digest(b"x").into(), 10)
        .await
        .unwrap();
    uploads
        .put_part(&held, 0, Bytes::from_static(b"x"), 1)
        .await
        .unwrap();
    uploads.complete(&held, 2).await.unwrap();
    uploads.claim(&held, OperationId::random(), 3).await.unwrap();
    let corrupt = location(5);
    store
        .compare_exchange(&corrupt, None, b"invalid metadata")
        .await
        .unwrap();
    let expired = location(6);
    uploads.create(&expired, 1, 1, [0; 16], 10).await.unwrap();
    let mut cursor = UploadExpiryCursor::default();
    assert!(matches!(
        uploads.expire_step(20, &mut cursor).await,
        Err(UploadError::Integrity)
    ));
    uploads.expire_step(20, &mut cursor).await.unwrap();
    assert!(uploads.metadata(&expired).await.unwrap().cleanup_complete);
    assert_eq!(uploads.metadata(&held).await.unwrap().state, UploadState::Claimed);
}

#[tokio::test]
async fn multi_extent_parts_respect_the_notification_budget_before_completion() {
    let store = Arc::new(upload::TestStore::default());
    let payload = Arc::new(payload::TestExpiryPayload::default());
    let uploads = DatasetUploads::new(store.clone(), payload.clone());
    let location = location(7);
    uploads.create(&location, 96, 32, [0; 16], 10).await.unwrap();
    for index in 0_u32..3 {
        let mut key = location.clone();
        key.extend(b"\0P");
        key.extend(index.to_be_bytes());
        let locations: Vec<_> = (0..32)
            .map(|offset| crowdb_protocol::chunkdb::rpc::Location {
                chunk_id: Some(crowdb_protocol::common::ChunkId {
                    high: 1,
                    low: 1000 + u64::from(index) * 32 + offset,
                }),
                offset: 0,
                length: 4096,
                logical_offset: offset,
                logical_length: 1,
            })
            .collect();
        let record = serde_json::json!({"version":1,"owner":OperationId::random(),"deadline":10,
            "state":"Accepted","part":{"index":index,"length":32,"md5":([0_u8;16]),"locations":locations}});
        store
            .compare_exchange(&key, None, &serde_json::to_vec(&record).unwrap())
            .await
            .unwrap();
    }
    let mut cursor = UploadExpiryCursor::default();
    let step = uploads.expire_step(20, &mut cursor).await.unwrap();
    assert_eq!(step.range_notifications, 64);
    assert_eq!(step.part_reads, 3);
    assert_eq!(payload.attempted_ranges.load(Ordering::SeqCst), 64);
    assert!(!uploads.metadata(&location).await.unwrap().cleanup_complete);
    let step = uploads.expire_step(20, &mut cursor).await.unwrap();
    assert_eq!(step.range_notifications, 32);
    assert!(uploads.metadata(&location).await.unwrap().cleanup_complete);
}

#[tokio::test]
async fn queue_capacity_does_not_drop_obligations_when_more_than_thirty_two_uploads_are_expired() {
    let store = Arc::new(upload::TestStore::default());
    let payload = Arc::new(payload::TestExpiryPayload::default());
    let uploads = DatasetUploads::new(store, payload.clone());
    for seed in 1..=64 {
        let key = location(seed);
        uploads.create(&key, 1, 1, [0; 16], 10).await.unwrap();
        uploads
            .put_part(&key, 0, Bytes::from_static(b"x"), 1)
            .await
            .unwrap();
    }
    payload.deferred_through.store(64, Ordering::SeqCst);
    let mut cursor = UploadExpiryCursor::default();
    let mut high_watermark = 0;
    for _ in 0..20 {
        uploads.expire_step(20, &mut cursor).await.unwrap();
        high_watermark = high_watermark.max(cursor.pending_uploads());
        assert!(cursor.pending_uploads() <= 32);
    }
    assert_eq!(high_watermark, 31);
    let later = location(65);
    uploads.create(&later, 1, 1, [0; 16], 10).await.unwrap();
    uploads
        .put_part(&later, 0, Bytes::from_static(b"y"), 1)
        .await
        .unwrap();
    for _ in 0..100 {
        uploads.expire_step(20, &mut cursor).await.unwrap();
        if uploads.metadata(&later).await.unwrap().cleanup_complete {
            break;
        }
    }
    assert!(
        uploads.metadata(&later).await.unwrap().cleanup_complete,
        "a full cache of deferred ranges must leave room for new discovery"
    );
    assert!(!uploads.metadata(&location(1)).await.unwrap().cleanup_complete);
    payload.deferred_through.store(0, Ordering::SeqCst);
    for _ in 0..200 {
        uploads.expire_step(20, &mut cursor).await.unwrap();
        if !cursor.has_more() {
            break;
        }
    }
    for seed in 1..=64 {
        assert!(uploads.metadata(&location(seed)).await.unwrap().cleanup_complete);
    }
}
