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
use std::sync::Arc;

#[tokio::test]
async fn uploaded_composition_publishes_initial_splits_and_branches_from_a_frozen_split() {
    let test = TestDatasetHttp::start_payload(Arc::new(TestStore::default())).await;
    let client = Client::new();
    let capabilities = response(
        client.get(format!("{}/api/dataset/v2/capabilities", test.origin)),
        200,
    )
    .await;
    for name in ["composition", "uploads", "snapshot-reclaim"] {
        assert!(capabilities["capabilities"]
            .as_array()
            .unwrap()
            .contains(&json!(name)));
    }
    let root = format!(
        "{}/api/dataset/v2/namespaces/dataset-ns/datasets/import",
        test.origin
    );
    response(client.put(&root), 200).await;
    response(client.put(&root), 409).await;
    let upload = "11111111111111111111111111111111";
    create_upload(&client, &root, upload).await;
    let operation = "22222222222222222222222222222222";
    let draft = format!("{root}/compositions/{operation}");
    let mut begin = composition::begin(operation, &Value::Null);
    begin["splits"] = common::definitions();
    begin["policy"] = json!({"overlap":"disjoint","coverage":"require_complete"});
    response(client.post(format!("{root}/compositions")).json(&begin), 200).await;
    let edit = json!({"edit":"33333333333333333333333333333333","expected_revision":"1","samples":[{"kind":"upsert","id":"AAH/","fields":[{"name":"value","value":{"kind":"upload","id":upload}}]}]});
    assert_eq!(
        response(client.put(&draft).json(&edit), 200).await["revision"],
        "2"
    );
    response(client.delete(format!("{root}/uploads/{upload}")), 409).await;
    let prepared = composition::prepare(&client, &draft, "2", "1").await;
    let snapshot = prepared["preparation"]["snapshot"].as_str().unwrap();
    response(client.get(format!("{root}/snapshots/{snapshot}/summary")), 200).await;
    let receipt = composition::publish(
        &client,
        &root,
        &draft,
        &prepared,
        "44444444444444444444444444444444",
    )
    .await;
    assert_eq!(
        response(
            client.get(format!("{root}/commits/44444444444444444444444444444444")),
            200
        )
        .await["receipt"],
        receipt
    );
    let head = response(client.get(format!("{root}/head")), 200).await;
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
    let child_op = "55555555555555555555555555555555";
    let child = format!("{root}/compositions/{child_op}");
    let mut begin = composition::begin(child_op, &json!(snapshot));
    begin["source"] = json!({"kind":"split","source":{"snapshot":snapshot,"revision":revision,"name":name},"selection":null});
    begin["inherit_parent_splits"] = json!(true);
    begin["parent_split_revision"] = json!(revision);
    response(client.post(format!("{root}/compositions")).json(&begin), 200).await;
    let child_prepared = composition::prepare(&client, &child, "1", "1").await;
    assert_eq!(child_prepared["uploads"]["candidate_uploads"], "1");
    composition::publish(
        &client,
        &root,
        &child,
        &child_prepared,
        "66666666666666666666666666666666",
    )
    .await;
    composition::cleanup(&client, &draft).await;
    composition::cleanup(&client, &child).await;
    response(
        client
            .put(format!("{root}/names/stable"))
            .json(&json!({"snapshot":snapshot})),
        200,
    )
    .await;
    assert_eq!(
        response(client.get(format!("{root}/head")), 200).await["latest"],
        child_prepared["preparation"]["snapshot"]
    );
    response(client.post(format!("{root}/snapshots/{snapshot}/reclaim")), 409).await;
    test.stop().await;
}

#[tokio::test]
async fn composition_conflicts_and_cancellation_preserve_exact_retry_identity() {
    let test = TestDatasetHttp::start_payload(Arc::new(TestStore::default())).await;
    let client = Client::new();
    let (root, parent) = common::fixture(&client, &test.origin).await;
    let operation = "77777777777777777777777777777777";
    let draft = format!("{root}/compositions/{operation}");
    let begin = composition::begin(operation, &json!(parent));
    response(client.post(format!("{root}/compositions")).json(&begin), 200).await;
    let edit = json!({"edit":"88888888888888888888888888888888","expected_revision":"1","samples":[]});
    response(client.put(&draft).json(&edit), 200).await;
    response(
        client
            .put(&draft)
            .json(&json!({"edit":"99999999999999999999999999999999","expected_revision":"1","samples":[]})),
        409,
    )
    .await;
    response(client.put(&draft).json(&json!({"edit":"99999999999999999999999999999999","expected_revision":"2","samples":[{"kind":"upsert","id":"YQ==","fields":[{"name":"value","value":{"kind":"chunk","location":"arbitrary"}}]}]})),400).await;
    response(client.delete(&draft).json(&json!({"expected_revision":"1"})), 409).await;
    response(client.delete(&draft).json(&json!({"expected_revision":"2"})), 200).await;
    composition::cleanup(&client, &draft).await;
    assert_eq!(
        response(client.put(&draft).json(&edit), 200).await["revision"],
        "2"
    );
    assert_eq!(
        response(client.get(format!("{root}/head")), 200).await["latest"],
        parent
    );
    response(client.get(format!("{root}/head?unexpected=1")), 400).await;
    response(
        client
            .post(format!("{draft}/publish"))
            .json(&json!({"attempt":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","snapshot":parent})),
        400,
    )
    .await;
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
