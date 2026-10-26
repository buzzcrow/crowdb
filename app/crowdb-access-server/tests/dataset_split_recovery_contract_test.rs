// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
#[path = "common/dataset_v2.rs"]
mod common;
#[path = "common/dataset.rs"]
mod dataset;
use common::response;
use dataset::{TestDatasetHttp, TestStore};
use reqwest::Client;
use serde_json::{json, Value};
use std::sync::Arc;

#[tokio::test]
async fn complete_result_retries_after_restart_and_stale_publish_preserves_history() {
    let client = Client::new();
    let store = Arc::new(TestStore::default());
    let test = TestDatasetHttp::start(store.clone()).await;
    let (root, snapshot) = common::fixture(&client, &test.origin).await;
    let first_id = "11111111111111111111111111111111";
    let first = common::prepare(&client, &root, &snapshot, first_id, false).await;
    let old_origin = test.origin.clone();
    test.stop().await;
    let test = TestDatasetHttp::start(store).await;
    let root = root.replace(&old_origin, &test.origin);
    assert_eq!(
        common::prepare(&client, &root, &snapshot, first_id, false).await,
        first
    );
    let head = common::publish(&client, &root, &snapshot, &first, &Value::Null).await;
    let a = common::prepare(
        &client,
        &root,
        &snapshot,
        "33333333333333333333333333333333",
        true,
    )
    .await;
    let b = common::prepare(
        &client,
        &root,
        &snapshot,
        "44444444444444444444444444444444",
        false,
    )
    .await;
    let current = common::publish(&client, &root, &snapshot, &a, &head).await;
    let url = format!("{root}/snapshots/{snapshot}/split-head");
    let conflict = response(client.put(&url).json(&json!({"revision":b["revision"], "expected_revision":head["revision"], "expected_head":head["generation"]})), 409).await;
    assert_eq!(conflict["observed_head"], current);
    response(
        client
            .put(&url)
            .json(&json!({"revision":first["revision"], "expected_revision":null, "expected_head":null})),
        409,
    )
    .await;
    assert_eq!(response(client.get(&url), 200).await, current);
    for prepared in [&first, &a, &b] {
        assert_eq!(
            response(
                client.get(format!(
                    "{root}/snapshots/{snapshot}/split-revisions/{}",
                    prepared["revision"].as_str().unwrap()
                )),
                200
            )
            .await,
            *prepared
        );
    }
    assert_eq!(a["splits"], json!([]));
    let historical = response(
        client.get(format!(
            "{root}/snapshots/{snapshot}/split-revisions/{first_id}/splits/train%2Fa%20b/members"
        )),
        200,
    )
    .await;
    assert!(!historical["entries"].as_array().unwrap().is_empty());
    drop(client);
    test.stop().await;
}
