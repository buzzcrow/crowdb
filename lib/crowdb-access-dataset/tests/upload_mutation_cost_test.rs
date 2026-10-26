// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/upload.rs"]
mod upload;
#[path = "common/upload_fault.rs"]
mod upload_fault;

use bytes::Bytes;
use crowdb_access_dataset::{
    DatasetIdentity, DatasetStore, DatasetUploads, NamespacePath, OperationId, UploadError, UploadRecord,
    UploadState,
};
use md5::{Digest, Md5};
use serde_json::{json, Value};
use std::sync::{atomic::Ordering, Arc};
use upload::{TestPayload, TestStore};
use upload_fault::{TestMutation, TestUploadFaultStore};

fn location() -> Vec<u8> {
    DatasetUploads::location(
        &DatasetIdentity::new(NamespacePath::root(), "mutation-cost").unwrap(),
        OperationId::random(),
    )
    .unwrap()
}

async fn complete_upload(store: Arc<TestStore>, payloads: Arc<TestPayload>, key: &[u8]) {
    let uploads = DatasetUploads::new(store, payloads);
    uploads
        .create(key, 4, 4, Md5::digest(b"data").into(), 100)
        .await
        .unwrap();
    uploads
        .put_part(key, 0, Bytes::from_static(b"data"), 1)
        .await
        .unwrap();
    uploads.complete(key, 2).await.unwrap();
}

fn upload_value(bytes: &[u8]) -> Value {
    serde_json::to_value(UploadRecord::decode_stored_for_tests(bytes).unwrap()).unwrap()
}

fn upload_bytes(value: &Value) -> Vec<u8> {
    serde_json::from_value::<UploadRecord>(value.clone())
        .unwrap()
        .encode_stored_for_tests()
        .unwrap()
}

async fn changed_records(store: &TestStore, key: &[u8], field: &str) -> Vec<TestMutation> {
    let stored = store.get(key).await.unwrap().unwrap();
    // expires_at belongs to U; deadline belongs to the unchanged PartEntry JSON.
    let upload = field == "expires_at";
    let mut value = if upload {
        upload_value(&stored.bytes)
    } else {
        serde_json::from_slice(&stored.bytes).unwrap()
    };
    (101..105)
        .map(|number| {
            value[field] = json!(number);
            TestMutation::Conflict(if upload {
                upload_bytes(&value)
            } else {
                serde_json::to_vec(&value).unwrap()
            })
        })
        .collect()
}

#[tokio::test]
async fn create_and_complete_return_known_results_without_confirmation_reads() {
    let store = Arc::new(TestStore::default());
    let uploads = DatasetUploads::new(store.clone(), Arc::new(TestPayload::default()));
    let key = location();
    let declared = uploads
        .create(&key, 4, 4, Md5::digest(b"data").into(), 100)
        .await
        .unwrap();
    assert_eq!(store.reads.load(Ordering::Relaxed), 0);
    assert_eq!(
        uploads.create(&key, 4, 4, declared.md5, 100).await.unwrap(),
        declared
    );
    assert_eq!(store.reads.load(Ordering::Relaxed), 0);
    assert_eq!(store.scans.load(Ordering::Relaxed), 1);
    uploads
        .put_part(&key, 0, Bytes::from_static(b"data"), 1)
        .await
        .unwrap();
    for _ in 0..2 {
        let reads = store.reads.load(Ordering::Relaxed);
        let complete = uploads.complete(&key, 2).await.unwrap();
        assert_eq!(complete.state, UploadState::Complete);
        assert_eq!(complete.parts.len(), 1);
        assert_eq!(store.reads.load(Ordering::Relaxed) - reads, 1);
    }
}

#[tokio::test]
async fn successful_part_uses_its_known_write_intent_without_confirmation_get() {
    let store = Arc::new(TestStore::default());
    let uploads = DatasetUploads::new(store.clone(), Arc::new(TestPayload::default()));
    let key = location();
    uploads
        .create(&key, 4, 4, Md5::digest(b"data").into(), 100)
        .await
        .unwrap();
    let reads = store.reads.load(Ordering::Relaxed);
    let writes = store.mutations.load(Ordering::Relaxed);
    let part = uploads
        .put_part(&key, 0, Bytes::from_static(b"data"), 1)
        .await
        .unwrap();
    assert_eq!(store.reads.load(Ordering::Relaxed) - reads, 4);
    assert_eq!(store.mutations.load(Ordering::Relaxed) - writes, 3);
    assert_eq!(uploads.complete(&key, 2).await.unwrap().parts, vec![part]);
}

#[tokio::test]
async fn accepting_a_known_intent_still_rejects_concurrent_reclamation() {
    let store = Arc::new(TestStore::default());
    let payloads = Arc::new(TestPayload::default());
    let key = location();
    let uploads = DatasetUploads::new(store.clone(), payloads.clone());
    uploads
        .create(&key, 4, 4, Md5::digest(b"data").into(), 100)
        .await
        .unwrap();
    let mut part_key = key.clone();
    part_key.extend_from_slice(b"\0P\0\0\0\0");
    let reclaimed = json!({
        "version": 1,
        "owner": OperationId::random(),
        "deadline": 61,
        "state": "Reclaimed",
        "part": {"index": 0, "length": 4, "md5": <[u8; 16]>::from(Md5::digest(b"data")), "locations": []}
    });
    let fault = TestUploadFaultStore::new(
        store,
        part_key,
        vec![
            TestMutation::Apply,
            TestMutation::Apply,
            TestMutation::Conflict(serde_json::to_vec(&reclaimed).unwrap()),
        ],
    );
    let uploads = DatasetUploads::new(fault.clone(), payloads);
    assert!(matches!(
        uploads.put_part(&key, 0, Bytes::from_static(b"data"), 1).await,
        Err(UploadError::Conflict)
    ));
    assert_eq!(fault.reads.load(Ordering::Relaxed), 1);
    assert_eq!(fault.writes.load(Ordering::Relaxed), 3);
    assert!(matches!(
        uploads.complete(&key, 2).await,
        Err(UploadError::Conflict)
    ));
}

#[tokio::test]
async fn accepted_part_reply_loss_needs_no_shared_index_repair_or_new_write() {
    let store = Arc::new(TestStore::default());
    let payloads = Arc::new(TestPayload::default());
    let uploads = DatasetUploads::new(store.clone(), payloads.clone());
    let key = location();
    uploads
        .create(&key, 4, 4, Md5::digest(b"data").into(), 100)
        .await
        .unwrap();
    let mut part_key = key.clone();
    part_key.extend_from_slice(b"\0P\0\0\0\0");
    let fault = TestUploadFaultStore::new(
        store,
        part_key,
        vec![TestMutation::Apply, TestMutation::Apply, TestMutation::LoseReply],
    );
    let uploads = DatasetUploads::new(fault.clone(), payloads);
    assert!(matches!(
        uploads.put_part(&key, 0, Bytes::from_static(b"data"), 1).await,
        Err(UploadError::Store(_))
    ));
    assert_eq!(fault.writes.load(Ordering::Relaxed), 3);
    let part = uploads
        .put_part(&key, 0, Bytes::from_static(b"data"), 2)
        .await
        .unwrap();
    assert_eq!(fault.reads.load(Ordering::Relaxed), 2);
    assert_eq!(fault.writes.load(Ordering::Relaxed), 3);
    assert_eq!(uploads.complete(&key, 3).await.unwrap().parts, vec![part]);
}

#[tokio::test]
async fn abort_reuses_conflicts_and_stops_when_an_owner_wins() {
    let store = Arc::new(TestStore::default());
    let payloads = Arc::new(TestPayload::default());
    let key = location();
    complete_upload(store.clone(), payloads.clone(), &key).await;
    let mut mutations = changed_records(&store, &key, "expires_at").await;
    let mut claimed = upload_value(&store.get(&key).await.unwrap().unwrap().bytes);
    claimed["state"] = json!("claimed");
    claimed["owners"] = json!([OperationId::random()]);
    mutations.push(TestMutation::Conflict(upload_bytes(&claimed)));
    let fault = TestUploadFaultStore::new(store, key.clone(), mutations);
    let uploads = DatasetUploads::new(fault.clone(), payloads);
    assert!(matches!(uploads.abort(&key).await, Err(UploadError::Conflict)));
    assert_eq!(fault.reads.load(Ordering::Relaxed), 1);
    assert_eq!(fault.writes.load(Ordering::Relaxed), 5);
    assert_eq!(uploads.read_part(&key, 0).await.unwrap(), b"data".as_slice());
}

#[tokio::test]
async fn cleanup_part_reuses_observations_before_and_after_notification() {
    let store = Arc::new(TestStore::default());
    let payloads = Arc::new(TestPayload::default());
    let key = location();
    complete_upload(store.clone(), payloads.clone(), &key).await;
    let mut part_key = key.clone();
    part_key.extend_from_slice(b"\0P\0\0\0\0");
    let mut mutations = changed_records(&store, &part_key, "deadline").await;
    mutations.push(TestMutation::Apply);
    let mut retired: Value =
        serde_json::from_slice(&store.get(&part_key).await.unwrap().unwrap().bytes).unwrap();
    retired["state"] = json!("Reclaimed");
    for number in 105..109 {
        retired["deadline"] = json!(number);
        mutations.push(TestMutation::Conflict(serde_json::to_vec(&retired).unwrap()));
    }
    let fault = TestUploadFaultStore::new(store, part_key, mutations);
    let uploads = DatasetUploads::new(fault.clone(), payloads);
    assert!(uploads.abort(&key).await.unwrap().cleanup_complete);
    assert_eq!(
        fault.reads.load(Ordering::Relaxed),
        1,
        "the initial entry enumeration only"
    );
    assert_eq!(fault.writes.load(Ordering::Relaxed), 10);
}

#[tokio::test]
async fn cleanup_completion_conflicts_do_not_reload_the_upload() {
    let store = Arc::new(TestStore::default());
    let payloads = Arc::new(TestPayload::default());
    let key = location();
    complete_upload(store.clone(), payloads.clone(), &key).await;
    let mut mutations = vec![TestMutation::Apply];
    for mutation in changed_records(&store, &key, "expires_at").await {
        let TestMutation::Conflict(bytes) = mutation else {
            unreachable!()
        };
        let mut value = upload_value(&bytes);
        value["state"] = json!("aborted");
        mutations.push(TestMutation::Conflict(upload_bytes(&value)));
    }
    let fault = TestUploadFaultStore::new(store, key.clone(), mutations);
    let uploads = DatasetUploads::new(fault.clone(), payloads);
    assert!(uploads.abort(&key).await.unwrap().cleanup_complete);
    assert_eq!(fault.reads.load(Ordering::Relaxed), 1);
    assert_eq!(fault.writes.load(Ordering::Relaxed), 6);
}

#[tokio::test]
async fn unknown_completion_is_propagated_without_confirmation_get() {
    let store = Arc::new(TestStore::default());
    let payloads = Arc::new(TestPayload::default());
    let key = location();
    let uploads = DatasetUploads::new(store.clone(), payloads.clone());
    uploads
        .create(&key, 4, 4, Md5::digest(b"data").into(), 100)
        .await
        .unwrap();
    uploads
        .put_part(&key, 0, Bytes::from_static(b"data"), 1)
        .await
        .unwrap();
    let fault = TestUploadFaultStore::new(store, key.clone(), vec![TestMutation::LoseReply]);
    let uploads = DatasetUploads::new(fault.clone(), payloads);
    assert!(matches!(
        uploads.complete(&key, 2).await,
        Err(UploadError::Store(_))
    ));
    assert_eq!(fault.reads.load(Ordering::Relaxed), 1);
    assert_eq!(
        uploads.complete(&key, 3).await.unwrap().state,
        UploadState::Complete
    );
}
