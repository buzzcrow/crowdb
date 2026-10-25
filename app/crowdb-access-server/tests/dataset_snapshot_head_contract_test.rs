// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/dataset_v2.rs"]
#[allow(dead_code)]
mod common;
#[path = "common/dataset.rs"]
mod dataset;
#[path = "../../../lib/crowdb-access-dataset/tests/common/publication.rs"]
mod publication;
use crowdb_access_dataset::*;
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
async fn fixture() -> (
    Arc<TestStore>,
    DatasetAuthority,
    DatasetIdentity,
    TestDatasetHttp,
    String,
    Vec<SnapshotId>,
) {
    let store = Arc::new(TestStore::default());
    let authority = DatasetAuthority::new(store.clone());
    let identity = DatasetIdentity::new(NamespacePath::root(), "snapshot-head").unwrap();
    authority.create_dataset(identity.clone()).await.unwrap();
    let mut snapshots = Vec::new();
    for marker in [1, 2] {
        snapshots.push(
            authority
                .prepare_snapshot_candidate(
                    &identity,
                    None,
                    publication::publication_manifest(marker),
                    OperationId::random(),
                )
                .await
                .unwrap(),
        );
    }
    let test = TestDatasetHttp::start(store.clone()).await;
    let root = format!(
        "{}/api/dataset/v2/namespaces/dataset-ns/datasets/snapshot-head",
        test.origin
    );
    (store, authority, identity, test, root, snapshots)
}

#[tokio::test]
async fn head_updates_use_complete_resource_ids_and_return_the_failed_condition() {
    let (store, authority, identity, test, root, snapshots) = fixture().await;
    let client = Client::new();
    let url = format!("{root}/head");
    let before = counts(&store);
    assert_eq!(
        common::response(client.get(&url), 200).await,
        json!({"latest":null,"stable":null,"generation":null})
    );
    assert_eq!(delta(&store, before), (2, 0));
    let first = json!({"snapshot":snapshots[0].to_string(),"expected_head":null});
    let before = counts(&store);
    let head = common::response(client.put(&url).json(&first), 200).await;
    assert_eq!(delta(&store, before), (3, 1));
    assert_eq!(head["latest"], snapshots[0].to_string());
    let before = counts(&store);
    assert_eq!(common::response(client.put(&url).json(&first), 200).await, head);
    assert_eq!(delta(&store, before), (3, 0));
    let mut next = json!({"snapshot":snapshots[1].to_string(),"expected_head":null});
    let before = counts(&store);
    let conflict = common::response(client.put(&url).json(&next), 409).await;
    assert_eq!(conflict["observed_head"], head);
    assert_eq!(delta(&store, before), (3, 0));
    next["expected_head"] = conflict["observed_head"]["generation"].clone();
    let before = counts(&store);
    let second = common::response(client.put(&url).json(&next), 200).await;
    assert_eq!(delta(&store, before), (3, 1));
    let before = counts(&store);
    assert_eq!(
        common::response(client.put(&url).json(&first), 409).await["observed_head"],
        second
    );
    assert_eq!(delta(&store, before), (3, 0));
    authority.begin_reclaim(&identity, snapshots[0]).await.unwrap();
    let before = counts(&store);
    common::response(client.put(&url).json(&first), 409).await;
    // The terminal content binding rejects publication before another admission lookup.
    assert_eq!(delta(&store, before), (2, 0));
    drop(client);
    test.stop().await;
}

#[tokio::test]
async fn invalid_conditions_and_auth_fail_before_storage_without_a_draft_fallback() {
    let (store, _, _, test, root, snapshots) = fixture().await;
    let client = Client::new();
    let url = format!("{root}/head");
    let valid = json!({"snapshot":snapshots[0].to_string(),"expected_head":null});
    let before = counts(&store);
    assert_eq!(
        client
            .put(&url)
            .json(&valid)
            .send()
            .await
            .unwrap()
            .status()
            .as_u16(),
        401
    );
    for invalid in [
        json!({"snapshot":snapshots[0].to_string()}),
        json!({"snapshot":null,"expected_head":null}),
        json!({"snapshot":"bad","expected_head":null}),
        json!({"snapshot":snapshots[0].to_string(),"expected_head":4}),
        json!({"snapshot":snapshots[0].to_string(),"expected_head":"A".repeat(64)}),
        json!({"snapshot":snapshots[0].to_string(),"expected_head":null,"attempt":"11".repeat(16)}),
    ] {
        common::response(client.put(&url).json(&invalid), 400).await;
    }
    common::response(client.put(&url).body(valid.to_string()), 400).await;
    common::response(client.put(format!("{url}?unknown=1")).json(&valid), 400).await;
    common::response(
        client
            .put(&url)
            .header("content-type", "application/json")
            .body(" ".repeat(4097)),
        413,
    )
    .await;
    assert_eq!(delta(&store, before), (0, 0));
    let before = counts(&store);
    common::response(
        client
            .put(&url)
            .json(&json!({"snapshot":"11".repeat(16),"expected_head":null})),
        404,
    )
    .await;
    assert_eq!(delta(&store, before), (2, 0));
    let caps = common::response(
        client.get(format!("{}/api/dataset/v2/capabilities", test.origin)),
        200,
    )
    .await;
    assert!(caps["capabilities"]
        .as_array()
        .unwrap()
        .contains(&json!("snapshot-head-update")));
    drop(client);
    test.stop().await;
}
