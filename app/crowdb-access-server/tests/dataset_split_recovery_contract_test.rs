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
async fn evaluation_resumes_after_server_restart_and_stale_publish_keeps_candidate() {
    let client = Client::new();
    let (test, root, snapshot, draft, first_prepared) = restart_preparation(&client).await;
    let first_receipt = common::publish(
        &client,
        &draft,
        &first_prepared,
        "22222222222222222222222222222222",
        Value::Null,
    )
    .await;
    let head_url = format!("{root}/snapshots/{snapshot}/split-head");
    let head = response(client.get(&head_url), 200).await;
    let a = new_prepared(
        &client,
        &root,
        &snapshot,
        "33333333333333333333333333333333",
        head["revision"].clone(),
        true,
    )
    .await;
    let b = new_prepared(
        &client,
        &root,
        &snapshot,
        "44444444444444444444444444444444",
        head["revision"].clone(),
        false,
    )
    .await;
    common::publish(
        &client,
        &a.0,
        &a.1,
        "55555555555555555555555555555555",
        head["generation"].clone(),
    )
    .await;
    let current = response(client.get(&head_url), 200).await;
    response(client.post(format!("{}/publish", b.0)).json(&json!({"attempt":"66666666666666666666666666666666", "revision":b.1["revision"], "expected_head":head["generation"]})), 409).await;
    assert_eq!(
        response(client.get(&b.0), 200).await["progress"]["state"],
        "prepared"
    );
    assert_eq!(
        common::publish(
            &client,
            &draft,
            &first_prepared,
            "22222222222222222222222222222222",
            Value::Null
        )
        .await,
        first_receipt
    );
    assert_eq!(response(client.get(&head_url), 200).await, current);
    let historic = response(
        client.get(format!(
            "{root}/snapshots/{snapshot}/split-revisions/{}",
            first_prepared["revision"].as_str().unwrap()
        )),
        200,
    )
    .await;
    assert_eq!(historic, first_prepared);
    assert_eq!(
        response(
            client.get(format!(
                "{root}/snapshots/{snapshot}/split-revisions/{}",
                a.1["revision"].as_str().unwrap()
            )),
            200
        )
        .await["splits"],
        json!([])
    );
    drop(client);
    test.stop().await;
}
async fn new_prepared(
    client: &Client,
    root: &str,
    snapshot: &str,
    operation: &str,
    expected: Value,
    empty: bool,
) -> (String, Value) {
    let mut begin = common::begin(operation, &expected);
    if empty {
        begin["splits"] = json!([]);
        begin["policy"]["coverage"] = json!("allow_unassigned");
    }
    response(
        client
            .post(format!("{root}/snapshots/{snapshot}/split-drafts"))
            .json(&begin),
        200,
    )
    .await;
    let draft = format!("{root}/split-drafts/{operation}");
    let prepared = common::prepare(client, &draft, "1").await;
    (draft, prepared)
}

async fn restart_preparation(client: &Client) -> (TestDatasetHttp, String, String, String, Value) {
    let store = Arc::new(TestStore::default());
    let test = TestDatasetHttp::start(store.clone()).await;
    let (root, snapshot) = common::fixture(client, &test.origin).await;
    let operation = "11111111111111111111111111111111";
    response(
        client
            .post(format!("{root}/snapshots/{snapshot}/split-drafts"))
            .json(&common::begin(operation, &Value::Null)),
        200,
    )
    .await;
    let draft = format!("{root}/split-drafts/{operation}");
    let first = response(
        client
            .post(format!("{draft}/evaluate"))
            .json(&json!({"expected_revision":"1", "expected_page":"0"})),
        200,
    )
    .await;
    assert!(!first["complete"].as_bool().unwrap());
    let old_origin = test.origin.clone();
    test.stop().await;
    let test = TestDatasetHttp::start(store).await;
    let root = root.replace(&old_origin, &test.origin);
    let draft = draft.replace(&old_origin, &test.origin);
    let status = response(client.get(&draft), 200).await;
    assert_eq!(status["progress"]["next_page"], first["next_page"]);
    let first_prepared = common::prepare(client, &draft, "1").await;
    (test, root, snapshot, draft, first_prepared)
}
