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
use std::{collections::BTreeSet, sync::Arc};

#[tokio::test]
async fn split_revision_flow_preserves_data_head_and_historical_members() {
    let test = TestDatasetHttp::start(Arc::new(TestStore::default())).await;
    let client = Client::new();
    let (root, snapshot) = common::fixture(&client, &test.origin).await;
    let base = format!("{root}/snapshots/{snapshot}");
    let head = response(client.get(format!("{root}/head")), 200).await;
    let operation = "02020202020202020202020202020202";
    let draft = format!("{root}/split-drafts/{operation}");
    let begin = common::begin(operation, &Value::Null);
    response(client.post(format!("{base}/split-drafts")).json(&begin), 200).await;
    let prepared = common::prepare(&client, &draft, "1").await;
    let revision = prepared["revision"].as_str().unwrap();
    assert_eq!(
        response(client.get(format!("{base}/split-revisions/{revision}")), 200).await,
        prepared
    );
    assert_eq!(
        response(client.get(format!("{base}/split-revisions")), 200).await["entries"][0]["revision"],
        revision
    );
    let before_publish = members(
        &client,
        &format!("{base}/split-revisions/{revision}/splits/train%2Fa%20b/members"),
    )
    .await;
    let receipt = common::publish(
        &client,
        &draft,
        &prepared,
        "03030303030303030303030303030303",
        Value::Null,
    )
    .await;
    assert_eq!(
        common::publish(
            &client,
            &draft,
            &prepared,
            "03030303030303030303030303030303",
            Value::Null
        )
        .await,
        receipt
    );
    assert_eq!(response(client.get(format!("{root}/head")), 200).await, head);
    assert_eq!(
        response(client.get(format!("{base}/split-revisions/{revision}")), 200).await,
        prepared
    );
    assert_eq!(
        response(client.get(&draft), 200).await["progress"]["state"],
        "prepared"
    );
    assert_eq!(
        response(
            client.get(format!("{root}/commits/03030303030303030303030303030303")),
            200
        )
        .await["receipt"],
        Value::Null
    );
    let current = response(client.get(format!("{base}/split-head")), 200).await;
    assert_eq!(current["revision"], revision);
    let train = members(
        &client,
        &format!("{base}/split-revisions/{revision}/splits/train%2Fa%20b/members"),
    )
    .await;
    let test_ids = members(
        &client,
        &format!("{base}/split-revisions/{revision}/splits/test/members"),
    )
    .await;
    assert_eq!(train, before_publish);
    assert!(train.is_disjoint(&test_ids));
    assert_eq!(train.len() + test_ids.len(), 45);
    assert_eq!(
        response(client.get(format!("{base}/split-revisions")), 200).await["entries"][0]["revision"],
        revision
    );
    drop(client);
    test.stop().await;
}
async fn members(client: &Client, url: &str) -> BTreeSet<String> {
    let mut result = BTreeSet::new();
    let mut after = None;
    for _ in 0..100 {
        let mut request = client.get(url).query(&[("limit", "3")]);
        if let Some(after) = after {
            request = request.query(&[("after", after)]);
        }
        let page = response(request, 200).await;
        for id in page["entries"].as_array().unwrap() {
            assert!(result.insert(id.as_str().unwrap().to_owned()));
        }
        after = page["after"].as_str().map(str::to_owned);
        if after.is_none() {
            return result;
        }
    }
    panic!("membership did not terminate");
}

#[tokio::test]
async fn split_drafts_require_explicit_preconditions_and_reject_stale_edits() {
    let test = TestDatasetHttp::start(Arc::new(TestStore::default())).await;
    let client = Client::new();
    let (root, snapshot) = common::fixture(&client, &test.origin).await;
    let url = format!("{root}/snapshots/{snapshot}/split-drafts");
    let operation = "04040404040404040404040404040404";
    let mut begin = common::begin(operation, &Value::Null);
    begin.as_object_mut().unwrap().remove("expected_revision");
    response(client.post(&url).json(&begin), 400).await;
    begin["expected_revision"] = Value::Null;
    begin["unsupported"] = json!(true);
    response(client.post(&url).json(&begin), 400).await;
    begin.as_object_mut().unwrap().remove("unsupported");
    response(client.post(&url).json(&begin), 200).await;
    let draft = format!("{root}/split-drafts/{operation}");
    let mut edit = json!({"edit":"05050505050505050505050505050505", "expected_revision":"1", "policy":begin["policy"], "splits":begin["splits"]});
    let edited = response(client.put(&draft).json(&edit), 200).await;
    assert_eq!(edited["revision"], "2");
    assert_eq!(response(client.put(&draft).json(&edit), 200).await, edited);
    edit["edit"] = json!("06060606060606060606060606060606");
    response(client.put(&draft).json(&edit), 409).await;
    response(
        client
            .post(format!("{draft}/prepare"))
            .json(&json!({"expected_revision":"2"})),
        409,
    )
    .await;
    let prepared = common::prepare(&client, &draft, "2").await;
    let publish = json!({"attempt":"07070707070707070707070707070707", "revision":prepared["revision"]});
    response(client.post(format!("{draft}/publish")).json(&publish), 400).await;
    assert_eq!(
        response(client.delete(&draft).json(&json!({"expected_revision":"2"})), 200).await["progress"]
            ["state"],
        "abandoned"
    );
    response(client.put(&draft).json(&edit), 409).await;
    drop(client);
    test.stop().await;
}
