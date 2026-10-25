// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/dataset_v2.rs"]
#[allow(dead_code)]
mod common;
#[path = "common/dataset.rs"]
mod dataset;

use crowdb_access_dataset::{DatasetIdentity, DatasetStore, NamespacePath};
use dataset::{TestDatasetHttp, TestStore};
use reqwest::Client;
use serde_json::json;
use std::sync::{atomic::Ordering, Arc};

fn counts(store: &TestStore) -> (u64, u64) {
    (
        store.reads.load(Ordering::Relaxed) + store.bounded_reads.load(Ordering::Relaxed),
        store.mutations.load(Ordering::Relaxed),
    )
}
fn delta(store: &TestStore, before: (u64, u64)) -> (u64, u64) {
    let now = counts(store);
    (now.0 - before.0, now.1 - before.1)
}

#[tokio::test]
async fn update_uses_only_complete_results_and_returns_the_failed_head_condition() {
    let store = Arc::new(TestStore::default());
    let test = TestDatasetHttp::start(store.clone()).await;
    let client = Client::new();
    let (root, snapshot) = common::fixture(&client, &test.origin).await;
    let mut prepared = Vec::new();
    for revision in ["22".repeat(16), "33".repeat(16)] {
        prepared.push(common::prepare(&client, &root, &snapshot, &revision, false).await);
    }
    // Preparing a complete result creates no editing identity.
    let identity = DatasetIdentity::new(NamespacePath::root(), "splits").unwrap();
    for prefix in [b"DS\0SD\0".as_slice(), b"DS\0SB\0"] {
        assert!(store
            .get(&[prefix, &identity.key().unwrap(), &[0x22; 16]].concat())
            .await
            .unwrap()
            .is_none());
    }
    let url = format!("{root}/snapshots/{snapshot}/split-head");
    let first = json!({"revision":prepared[0]["revision"], "expected_revision":null, "expected_head":null});
    let before = counts(&store);
    let head = common::response(client.put(&url).json(&first), 200).await;
    assert_eq!(delta(&store, before), (5, 1));
    assert_eq!(head["snapshot"], snapshot);
    assert_eq!(head["revision"], first["revision"]);
    let before = counts(&store);
    assert_eq!(common::response(client.put(&url).json(&first), 200).await, head);
    assert_eq!(delta(&store, before), (4, 0));
    let second = json!({"revision":prepared[1]["revision"], "expected_revision":null, "expected_head":null});
    let before = counts(&store);
    let conflict = common::response(client.put(&url).json(&second), 409).await;
    assert_eq!(delta(&store, before), (4, 0));
    assert_eq!(conflict["observed_head"], head);
    let mut reviewed = second;
    reviewed["expected_revision"] = conflict["observed_head"]["revision"].clone();
    reviewed["expected_head"] = conflict["observed_head"]["generation"].clone();
    let before = counts(&store);
    let current = common::response(client.put(&url).json(&reviewed), 200).await;
    assert_eq!(delta(&store, before), (4, 1));
    assert_eq!(current["revision"], prepared[1]["revision"]);
    assert_ne!(current["generation"], head["generation"]);
    assert_eq!(
        common::response(client.get(format!("{root}/head")), 200).await["latest"],
        snapshot
    );
    drop(client);
    test.stop().await;
}

#[tokio::test]
async fn update_requires_explicit_nullable_conditions_and_auth_before_storage() {
    let store = Arc::new(TestStore::default());
    let test = TestDatasetHttp::start(store.clone()).await;
    let client = Client::new();
    let (root, snapshot) = common::fixture(&client, &test.origin).await;
    let url = format!("{root}/snapshots/{snapshot}/split-head");
    let body = json!({"revision":"22".repeat(16),"expected_revision":null,"expected_head":null});
    let before = counts(&store);
    let denied = client.put(&url).json(&body).send().await.unwrap();
    assert_eq!(denied.status(), 401);
    denied.bytes().await.unwrap();
    assert_eq!(delta(&store, before), (0, 0));
    for field in ["expected_revision", "expected_head"] {
        let mut invalid = body.clone();
        invalid.as_object_mut().unwrap().remove(field);
        let before = counts(&store);
        common::response(client.put(&url).json(&invalid), 400).await;
        assert_eq!(delta(&store, before), (0, 0));
    }
    let mut invalid = body.clone();
    invalid["attempt"] = json!("33".repeat(16));
    common::response(client.put(&url).json(&invalid), 400).await;
    invalid = body.clone();
    invalid["expected_head"] = json!("G".repeat(64));
    common::response(client.put(&url).json(&invalid), 400).await;
    let before = counts(&store);
    common::response(client.put(&url).json(&body), 404).await;
    // Observe the head before the revision so concurrent retirement fences publication.
    assert_eq!(delta(&store, before), (2, 0));
    drop(client);
    test.stop().await;
}
