// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/dataset_v2.rs"]
#[allow(dead_code)]
mod common;
#[path = "common/dataset_composition.rs"]
#[allow(dead_code)]
mod composition;
#[path = "common/dataset.rs"]
mod dataset;
use common::response;
use dataset::{TestDatasetHttp, TestStore};
use reqwest::Client;
use serde_json::json;
use std::sync::Arc;

#[tokio::test]
async fn restart_and_head_conflict_preserve_candidates_without_receipt_writes() {
    let store = Arc::new(TestStore::default());
    let first = TestDatasetHttp::start_payload(store.clone()).await;
    let client = Client::new();
    let (root, parent) = common::fixture(&client, &first.origin).await;
    let origin = first.origin.clone();
    let old_head = response(client.get(format!("{root}/head")), 200).await;
    let snapshot = "12121212121212121212121212121212";
    let input = composition::input(&json!(parent));
    let prepared = composition::prepare(&client, &root, snapshot, &input).await;
    drop(client);
    first.stop().await;
    let second = TestDatasetHttp::start_payload(store).await;
    let root = root.replacen(&origin, &second.origin, 1);
    let client = Client::new();
    assert_eq!(
        composition::prepare(&client, &root, snapshot, &input).await,
        prepared
    );
    response(
        client
            .put(format!("{root}/names/stable"))
            .json(&json!({"snapshot":parent})),
        200,
    )
    .await;
    let mut publish = json!({"snapshot":snapshot,"expected_head":old_head["generation"]});
    let url = format!("{root}/head");
    let conflict = response(client.put(&url).json(&publish), 409).await;
    assert_eq!(conflict["observed_head"]["latest"], parent);
    assert_eq!(conflict["observed_head"]["stable"], parent);
    response(client.get(format!("{root}/snapshots/{snapshot}/summary")), 200).await;
    publish["expected_head"] = conflict["observed_head"]["generation"].clone();
    let current = response(client.put(&url).json(&publish), 200).await;
    assert_eq!(response(client.put(&url).json(&publish), 200).await, current);
    drop(client);
    second.stop().await;
}

#[tokio::test]
async fn uploads_reject_noncanonical_input_and_foreign_dataset_access() {
    let test = TestDatasetHttp::start_payload(Arc::new(TestStore::default())).await;
    let client = Client::new();
    let root = format!(
        "{}/api/dataset/v2/namespaces/dataset-ns/datasets/upload-contract",
        test.origin
    );
    response(client.put(&root), 200).await;
    let id = "15151515151515151515151515151515";
    let create = json!({"id":id,"length":"4","part_size":4,"md5":"8d777f385d3dfec8815d20f7496026dc"});
    for length in [json!(4), json!("04"), json!("18446744073709551616")] {
        let mut invalid = create.clone();
        invalid["length"] = length;
        response(client.post(format!("{root}/uploads")).json(&invalid), 400).await;
    }
    response(client.post(format!("{root}/uploads")).json(&create), 200).await;
    let upload = format!("{root}/uploads/{id}");
    response(
        client
            .put(format!("{upload}/parts/00"))
            .header("content-type", "application/octet-stream")
            .body("data"),
        400,
    )
    .await;
    response(client.put(format!("{upload}/parts/0")).body("data"), 400).await;
    response(client.get(format!("{upload}?unknown=1")), 400).await;
    let foreign = root.replace("upload-contract", "foreign");
    response(client.put(&foreign), 200).await;
    response(client.get(format!("{foreign}/uploads/{id}")), 404).await;
    assert_eq!(client.get(&upload).send().await.unwrap().status(), 401);
    assert_eq!(response(client.delete(&upload), 200).await["state"], "aborted");
    assert_eq!(response(client.get(&upload), 200).await["state"], "aborted");
    drop(client);
    test.stop().await;
}
