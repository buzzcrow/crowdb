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

fn input() -> Value {
    json!({"parent":null, "source":{"kind":"all"},
        "schema":{"version":1,"fields":[{"name":"value","required":true}]},
        "samples":[{"kind":"upsert","id":"AAH/","fields":[{"name":"value","value":{"kind":"inline","data":"ZGF0YQ=="}}]}],
        "splits":[{"name":"train","definition":{"kind":"selection","predicate":{"kind":"all"}},"origin":{"kind":"evaluated"}}],
        "policy":{"overlap":"disjoint","coverage":"require_complete"}})
}
fn counts(store: &TestStore) -> (u64, u64) {
    (
        store.reads.load(Ordering::Relaxed) + store.bounded_reads.load(Ordering::Relaxed),
        store.mutations.load(Ordering::Relaxed),
    )
}
fn root(origin: &str) -> String {
    format!("{origin}/api/dataset/v2/namespaces/dataset-ns/datasets/composition-resource")
}

#[tokio::test]
async fn complete_resource_reuses_the_snapshot_id_across_restart_without_advancing_head() {
    let store = Arc::new(TestStore::default());
    let test = TestDatasetHttp::start_payload(store.clone()).await;
    let client = Client::new();
    let root = root(&test.origin);
    response(client.put(&root), 200).await;
    let id = "12".repeat(16);
    let url = format!("{root}/snapshots/{id}");
    let before = counts(&store);
    let prepared = response(client.put(&url).json(&input()), 200).await;
    let after = counts(&store);
    // Creation reserves the Dataset root: two admission reads and one root CAS.
    assert_eq!((after.0 - before.0, after.1 - before.1), (14, 17));
    assert_eq!(prepared["snapshot"], id);
    assert_eq!(prepared["sample_count"], "1");
    assert!(prepared["initial_split"].is_string());
    assert_eq!(
        response(client.get(format!("{root}/head")), 200).await["latest"],
        Value::Null
    );
    response(client.get(format!("{url}/summary")), 200).await;
    drop(client);
    test.stop().await;
    let client = Client::new();
    let test = TestDatasetHttp::start_payload(store.clone()).await;
    let root = self::root(&test.origin);
    let url = format!("{root}/snapshots/{id}");
    let before = counts(&store);
    assert_eq!(response(client.put(&url).json(&input()), 200).await, prepared);
    assert_eq!(counts(&store).1, before.1);
    let mut changed = input();
    changed["splits"] = json!([]);
    response(client.put(&url).json(&changed), 409).await;
    assert_eq!(counts(&store).1, before.1);
    response(
        client
            .put(format!("{root}/head"))
            .json(&json!({"snapshot":id,"expected_head":null})),
        200,
    )
    .await;
    assert_eq!(
        response(client.get(format!("{root}/head")), 200).await["latest"],
        id
    );
    drop(client);
    test.stop().await;
}

#[tokio::test]
async fn uploaded_descriptors_are_validated_once_per_unique_upload_without_conversion_queries() {
    let store = Arc::new(TestStore::default());
    let test = TestDatasetHttp::start_payload(store.clone()).await;
    let client = Client::new();
    let root = root(&test.origin);
    response(client.put(&root), 200).await;
    let upload = "34".repeat(16);
    let upload_url = format!("{root}/uploads/{upload}");
    let descriptor = json!({"id":upload,"length":"4","part_size":4,"md5":"8d777f385d3dfec8815d20f7496026dc"});
    response(client.post(format!("{root}/uploads")).json(&descriptor), 200).await;
    response(
        client
            .put(format!("{upload_url}/parts/0"))
            .header("content-type", "application/octet-stream")
            .body("data"),
        200,
    )
    .await;
    response(client.post(format!("{upload_url}/complete")), 200).await;
    let mut input = input();
    input["splits"] = json!([]);
    input["schema"]["fields"]
        .as_array_mut()
        .unwrap()
        .push(json!({"name":"other","required":true}));
    let value = json!({"kind":"upload","id":upload,"length":descriptor["length"],"md5":descriptor["md5"]});
    input["samples"][0]["fields"] = json!([{"name":"value","value":value},{"name":"other","value":value}]);
    let before = store.upload_reads.load(Ordering::Relaxed);
    let snapshot = "56".repeat(16);
    response(
        client.put(format!("{root}/snapshots/{snapshot}")).json(&input),
        200,
    )
    .await;
    assert_eq!(store.upload_reads.load(Ordering::Relaxed) - before, 2);
    let url = format!("{root}/snapshots/{snapshot}/preview?sample=AAH%2F&field=other");
    assert_eq!(response(client.get(url), 200).await["data"], "ZGF0YQ==");
    input["samples"][0]["fields"][0]["value"]["length"] = json!("5");
    let before = counts(&store).1;
    response(
        client
            .put(format!("{root}/snapshots/{}", "78".repeat(16)))
            .json(&input),
        409,
    )
    .await;
    assert_eq!(counts(&store).1, before);
    drop(client);
    test.stop().await;
}

#[tokio::test]
async fn auth_strict_input_and_limits_reject_before_storage_and_no_service_has_no_capability() {
    let store = Arc::new(TestStore::default());
    let test = TestDatasetHttp::start_payload(store.clone()).await;
    let client = Client::new();
    let url = format!("{}/snapshots/{}", root(&test.origin), "12".repeat(16));
    let denied = client.put(&url).json(&input()).send().await.unwrap();
    assert_eq!(denied.status(), 401);
    denied.bytes().await.unwrap();
    for field in ["parent", "schema", "samples", "policy"] {
        let mut invalid = input();
        invalid.as_object_mut().unwrap().remove(field);
        response(client.put(&url).json(&invalid), 400).await;
    }
    let mut invalid = input();
    invalid["operation"] = json!("ab".repeat(16));
    response(client.put(&url).json(&invalid), 400).await;
    invalid = input();
    invalid["samples"][0]["fields"][0]["value"] = json!({"kind":"upload","id":"ab".repeat(16)});
    response(client.put(&url).json(&invalid), 400).await;
    response(
        client.put(format!("{url}?expected_revision=1")).json(&input()),
        400,
    )
    .await;
    response(
        client.put(&url).header("content-type", "text/plain").body("{}"),
        400,
    )
    .await;
    response(
        client
            .put(&url)
            .header("content-type", "application/json")
            .body(vec![b' '; 8 * 1024 * 1024 + 1]),
        413,
    )
    .await;
    assert_eq!(counts(&store), (0, 0));
    drop(client);
    test.stop().await;
    let test = TestDatasetHttp::start(store.clone()).await;
    let client = Client::new();
    response(
        client
            .put(format!("{}/snapshots/{}", root(&test.origin), "12".repeat(16)))
            .json(&input()),
        501,
    )
    .await;
    assert_eq!(counts(&store), (0, 0));
    drop(client);
    test.stop().await;
}
