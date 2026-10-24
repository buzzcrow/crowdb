// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/dataset_v2.rs"]
#[allow(dead_code)]
mod common;
#[path = "common/dataset.rs"]
mod dataset;
use common::response;
use dataset::{TestDatasetHttp, TestStore};
use reqwest::Client;
use serde_json::{json, Value};
use std::sync::{atomic::Ordering, Arc};

fn counts(store: &TestStore) -> (u64, u64) {
    (
        store.reads.load(Ordering::Relaxed) + store.bounded_reads.load(Ordering::Relaxed),
        store.mutations.load(Ordering::Relaxed),
    )
}
fn input() -> Value {
    let weights = json!([{"name":"train/a b", "weight":1}, {"name":"test", "weight":1}]);
    json!({"policy":{"overlap":"disjoint", "coverage":"require_complete"}, "splits":[
        {"name":"train/a b", "definition":{"kind":"weighted_partition", "seed":"42", "weights":weights,"index":0}, "origin":{"kind":"evaluated"}},
        {"name":"test", "definition":{"kind":"weighted_partition", "seed":"42", "weights":weights,"index":1}, "origin":{"kind":"evaluated"}}
    ]})
}

#[tokio::test]
async fn preparation_uses_the_resource_id_and_reuses_complete_results_without_a_draft() {
    let store = Arc::new(TestStore::default());
    let test = TestDatasetHttp::start(store.clone()).await;
    let client = Client::new();
    let (root, snapshot) = common::fixture(&client, &test.origin).await;
    let url = format!("{root}/snapshots/{snapshot}/split-revisions/{}", "ab".repeat(16));
    let before = counts(&store);
    let prepared = response(client.put(&url).json(&input()), 200).await;
    let after = counts(&store);
    assert_eq!((after.0 - before.0, after.1 - before.1), (14, 8));
    assert_eq!(prepared["revision"], "ab".repeat(16));
    assert_eq!(prepared["snapshot"], snapshot);
    assert_eq!(
        prepared["splits"]
            .as_array()
            .unwrap()
            .iter()
            .map(|split| split["count"].as_str().unwrap().parse::<u64>().unwrap())
            .sum::<u64>(),
        45
    );
    assert_eq!(response(client.get(&url), 200).await, prepared);
    assert_eq!(
        response(client.get(format!("{root}/snapshots/{snapshot}/split-head")), 200).await["revision"],
        Value::Null
    );
    let before = counts(&store);
    assert_eq!(response(client.put(&url).json(&input()), 200).await, prepared);
    let after = counts(&store);
    assert_eq!((after.0 - before.0, after.1 - before.1), (3, 0));
    let mut changed = input();
    changed["splits"] = json!([]);
    response(client.put(&url).json(&changed), 409).await;
    assert_eq!(counts(&store).1, after.1);
    assert_eq!(response(client.get(&url), 200).await, prepared);
    test.stop().await;
}

#[tokio::test]
async fn invalid_input_and_missing_auth_reject_before_storage_and_never_use_workflow_routes() {
    let store = Arc::new(TestStore::default());
    let test = TestDatasetHttp::start(store.clone()).await;
    let client = Client::new();
    let url = format!(
        "{}/api/dataset/v2/namespaces/dataset-ns/datasets/absent/snapshots/{}/split-revisions/{}",
        test.origin,
        "01".repeat(16),
        "02".repeat(16)
    );
    let denied = client.put(&url).json(&input()).send().await.unwrap();
    assert_eq!(denied.status(), 401);
    denied.bytes().await.unwrap();
    for body in [
        json!({}),
        json!({"policy":input()["policy"], "splits":[], "operation":"03".repeat(16)}),
        json!({"policy":input()["policy"], "splits":[input()["splits"][0], input()["splits"][0]]}),
    ] {
        response(client.put(&url).json(&body), 400).await;
    }
    response(client.put(format!("{url}?expected_page=1")).json(&input()), 400).await;
    response(
        client.put(&url).header("content-type", "text/plain").body("{}"),
        400,
    )
    .await;
    response(
        client
            .put(&url)
            .header("content-type", "application/json")
            .body(vec![b' '; 1024 * 1024 + 1]),
        413,
    )
    .await;
    assert_eq!(counts(&store), (0, 0));
    response(client.put(&url).json(&input()), 404).await;
    assert_eq!(counts(&store).1, 0);
    test.stop().await;
}
