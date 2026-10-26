// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/dataset_v2.rs"]
#[allow(dead_code)]
mod common;
#[path = "common/dataset_composition.rs"]
mod composition;
#[path = "common/dataset.rs"]
mod dataset;
use common::response;
use dataset::{TestDatasetHttp, TestStore};
use reqwest::Client;
use serde_json::{json, Value};
use std::sync::{atomic::Ordering, Arc};

#[tokio::test]
async fn uploaded_resources_publish_initial_splits_and_branch_from_a_frozen_split() {
    let test = TestDatasetHttp::start_payload(Arc::new(TestStore::default())).await;
    let client = Client::new();
    let capabilities = response(
        client.get(format!("{}/api/dataset/v2/capabilities", test.origin)),
        200,
    )
    .await;
    for name in ["composition-preparation", "uploads", "snapshot-reclaim"] {
        assert!(capabilities["capabilities"]
            .as_array()
            .unwrap()
            .contains(&json!(name)));
    }
    assert!(!capabilities["capabilities"]
        .as_array()
        .unwrap()
        .contains(&json!("composition")));
    let root = format!(
        "{}/api/dataset/v2/namespaces/dataset-ns/datasets/import",
        test.origin
    );
    response(client.put(&root), 200).await;
    response(client.put(&root), 409).await;
    let upload = "11111111111111111111111111111111";
    create_upload(&client, &root, upload).await;
    let snapshot = "22222222222222222222222222222222";
    let mut input = composition::input(&Value::Null);
    input["splits"] = common::definitions();
    input["policy"]["coverage"] = json!("require_complete");
    input["samples"] = json!([{"kind":"upsert","id":"AAH/","fields":[{"name":"value","value":{"kind":"upload","id":upload,"length":"4","md5":"8d777f385d3dfec8815d20f7496026dc"}}]}]);
    let prepared = composition::prepare(&client, &root, snapshot, &input).await;
    assert_eq!(prepared["sample_count"], "1");
    assert_eq!(
        composition::prepare(&client, &root, snapshot, &input).await,
        prepared
    );
    response(client.delete(format!("{root}/uploads/{upload}")), 409).await;
    response(client.get(format!("{root}/snapshots/{snapshot}/summary")), 200).await;
    let head = composition::publish(&client, &root, &prepared, &Value::Null).await;
    assert_eq!(head["latest"], snapshot);
    assert_eq!(head["stable"], Value::Null);
    let preview = response(
        client
            .get(format!("{root}/snapshots/{snapshot}/preview"))
            .query(&[("sample", "AAH/"), ("field", "value")]),
        200,
    )
    .await;
    assert_eq!(preview["data"], "ZGF0YQ==");
    let (revision, name) = populated_split(&client, &root, snapshot).await;
    let mut child = composition::input(&json!(snapshot));
    child["schema"] = Value::Null;
    child["source"] = json!({"kind":"split","source":{"snapshot":snapshot,"revision":revision,"name":name},"selection":null});
    child["splits"] = input["splits"].clone();
    child["policy"] = input["policy"].clone();
    for split in child["splits"].as_array_mut().unwrap() {
        split["origin"] = json!({"kind":"inherited","source":{"snapshot":snapshot,"revision":revision,"name":split["name"]}});
    }
    let prepared = composition::prepare(&client, &root, "55555555555555555555555555555555", &child).await;
    assert_eq!(prepared["sample_count"], "1");
    composition::publish(&client, &root, &prepared, &head["generation"]).await;
    response(
        client
            .put(format!("{root}/names/stable"))
            .json(&json!({"snapshot":snapshot})),
        200,
    )
    .await;
    assert_eq!(
        response(client.get(format!("{root}/head")), 200).await["latest"],
        prepared["snapshot"]
    );
    response(client.post(format!("{root}/snapshots/{snapshot}/reclaim")), 409).await;
    drop(client);
    test.stop().await;
}

#[tokio::test]
async fn retired_composition_workflow_writes_never_access_storage() {
    let store = Arc::new(TestStore::default());
    let test = TestDatasetHttp::start_payload(store.clone()).await;
    let client = Client::new();
    let (root, _) = common::fixture(&client, &test.origin).await;
    let before = (
        store.reads.load(Ordering::Relaxed),
        store.bounded_reads.load(Ordering::Relaxed),
        store.mutations.load(Ordering::Relaxed),
    );
    response(client.post(format!("{root}/compositions")).json(&json!({})), 501).await;
    let draft = format!("{root}/compositions/77777777777777777777777777777777");
    response(client.put(&draft).json(&json!({})), 501).await;
    for suffix in ["evaluate", "prepare", "publish", "cleanup"] {
        response(client.post(format!("{draft}/{suffix}")).json(&json!({})), 501).await;
    }
    assert_eq!(
        (
            store.reads.load(Ordering::Relaxed),
            store.bounded_reads.load(Ordering::Relaxed),
            store.mutations.load(Ordering::Relaxed)
        ),
        before
    );
    drop(client);
    test.stop().await;
}

async fn create_upload(client: &Client, root: &str, upload: &str) {
    response(
        client
            .post(format!("{root}/uploads"))
            .json(&json!({"id":upload,"length":"4","part_size":4,"md5":"8d777f385d3dfec8815d20f7496026dc"})),
        200,
    )
    .await;
    response(
        client
            .put(format!("{root}/uploads/{upload}/parts/0"))
            .header("content-type", "application/octet-stream")
            .body("data"),
        200,
    )
    .await;
    assert_eq!(
        response(client.post(format!("{root}/uploads/{upload}/complete")), 200).await["state"],
        "complete"
    );
}

async fn populated_split(client: &Client, root: &str, snapshot: &str) -> (String, Value) {
    let split_head = response(client.get(format!("{root}/snapshots/{snapshot}/split-head")), 200).await;
    let revision = split_head["revision"].as_str().unwrap().to_owned();
    let splits = response(
        client.get(format!("{root}/snapshots/{snapshot}/split-revisions/{revision}")),
        200,
    )
    .await;
    let name = splits["splits"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["count"] == "1")
        .unwrap()["name"]
        .clone();
    (revision, name)
}
