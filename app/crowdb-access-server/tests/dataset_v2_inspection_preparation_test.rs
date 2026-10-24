// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/dataset.rs"]
mod dataset;
use crowdb_access_dataset::*;
use dataset::{TestDatasetHttp, TestStore};
use reqwest::Client;
use serde_json::{json, Value};
use std::sync::{atomic::Ordering, Arc};

#[derive(serde::Serialize)]
struct TestVersioned<T> {
    version: u16,
    payload: T,
}
fn scoped(prefix: &[u8], identity: &DatasetIdentity, snapshot: SnapshotId) -> Vec<u8> {
    [prefix, &identity.key().unwrap(), snapshot.as_bytes()].concat()
}
async fn fixture(opaque: bool) -> (Arc<TestStore>, TestDatasetHttp, String, Vec<u8>, Vec<u8>) {
    let store = Arc::new(TestStore::default());
    let authority = DatasetAuthority::new(store.clone());
    let identity = DatasetIdentity::new(NamespacePath::root(), "legacy-inspection").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let snapshot = SnapshotId::random();
    let manifest = ManifestRecord {
        version: 1,
        snapshot,
        schema: SchemaRecord {
            version: 1,
            fields: vec![FieldDefinition {
                name: "value".into(),
                required: false,
            }],
        },
        samples: (0..3)
            .map(|id| SampleRecord {
                sample_id: vec![id],
                fields: vec![FieldRecord {
                    name: "value".into(),
                    value: FieldLocator::Tombstone,
                }],
            })
            .collect(),
    };
    let publication = SnapshotPublication {
        snapshot,
        parent: None,
        operation: OperationId::random(),
        manifest: if opaque {
            b"opaque".to_vec()
        } else {
            bincode::serialize(&manifest).unwrap()
        },
        state: PublicationState::Published,
    };
    store
        .compare_exchange(
            &scoped(b"DS\0S\0", &identity, snapshot),
            None,
            &bincode::serialize(&TestVersioned {
                version: 1,
                payload: SnapshotRecord { publication },
            })
            .unwrap(),
        )
        .await
        .unwrap();
    let mut head = b"DS\0H\0".to_vec();
    head.extend(identity.key().unwrap());
    store
        .compare_exchange(
            &head,
            None,
            &bincode::serialize(&TestVersioned {
                version: 1,
                payload: HeadRecord {
                    latest: Some(snapshot),
                    stable: None,
                },
            })
            .unwrap(),
        )
        .await
        .unwrap();
    let mut progress = scoped(b"DS\0I\0", &identity, snapshot);
    progress.push(b'P');
    let old = serde_json::to_vec(&json!({ "version": 1, "source_sha256": vec![0; 32], "progress": { "snapshot": snapshot, "revision": 7, "rows_written": 0, "state": "Preparing" },
        "position": { "schema": 0, "sample": 0, "sample_written": false, "field": 0, "field_value": false, "rows": 0 } })).unwrap();
    store.compare_exchange(&progress, None, &old).await.unwrap();
    let test = TestDatasetHttp::start(store.clone()).await;
    let endpoint = format!(
        "{}/api/dataset/v2/namespaces/dataset-ns/datasets/legacy-inspection/snapshots/{snapshot}",
        test.origin
    );
    (store, test, endpoint, progress, old)
}
async fn response(request: reqwest::RequestBuilder, expected: u16) -> Value {
    let response = request.bearer_auth("secret").send().await.unwrap();
    let status = response.status().as_u16();
    let value: Value = response.json().await.unwrap();
    assert_eq!(status, expected, "{value}");
    value
}

#[tokio::test]
async fn inspection_post_finishes_once_without_rewriting_legacy_progress() {
    let (store, test, endpoint, progress, old) = fixture(false).await;
    let client = Client::new();
    let status = response(client.get(format!("{endpoint}/inspection")), 200).await;
    assert_eq!(status["state"], "preparing");
    assert_eq!(status["revision"], "7");
    let before = store.mutations.load(Ordering::Relaxed);
    let ready = response(client.post(format!("{endpoint}/inspection")), 200).await;
    assert_eq!(ready["state"], "ready");
    assert_eq!(ready["revision"], "0");
    assert_eq!(ready["rows_written"], "0");
    assert_eq!(store.mutations.load(Ordering::Relaxed) - before, 13);
    assert_eq!(
        response(client.post(format!("{endpoint}/inspection")), 200).await,
        ready
    );
    assert_eq!(
        response(client.get(format!("{endpoint}/inspection")), 200).await,
        ready
    );
    assert_eq!(store.mutations.load(Ordering::Relaxed) - before, 13);
    assert_eq!(store.get(&progress).await.unwrap().unwrap().bytes, old);
    assert_eq!(
        response(client.get(format!("{endpoint}/samples")), 200).await["entries"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    drop(client);
    test.stop().await;
}

#[tokio::test]
async fn unsupported_legacy_manifest_fails_explicitly_without_new_progress() {
    let (store, test, endpoint, progress, old) = fixture(true).await;
    let client = Client::new();
    let before = store.mutations.load(Ordering::Relaxed);
    let error = response(client.post(format!("{endpoint}/inspection")), 422).await;
    assert_eq!(error["code"], "inspection_unsupported");
    assert_eq!(store.mutations.load(Ordering::Relaxed), before);
    assert_eq!(store.get(&progress).await.unwrap().unwrap().bytes, old);
    drop(client);
    test.stop().await;
}
