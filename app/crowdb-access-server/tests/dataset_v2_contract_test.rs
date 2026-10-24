// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/dataset.rs"]
mod dataset;

use dataset::{TestDatasetHttp, TestStore};
use reqwest::Client;
use serde_json::Value;
use std::sync::Arc;

async fn json(request: reqwest::RequestBuilder, status: u16) -> Value {
    let response = request.bearer_auth("secret").send().await.unwrap();
    let actual = response.status().as_u16();
    let value: Value = response.json().await.unwrap();
    assert_eq!(actual, status, "{value}");
    value
}

#[tokio::test]
async fn capabilities_use_existing_auth_and_match_configured_operations() {
    let test = TestDatasetHttp::start(Arc::new(TestStore::default())).await;
    let client = Client::new();
    let endpoint = format!("{}/api/dataset/v2/capabilities", test.origin);
    let denied = client.get(&endpoint).send().await.unwrap();
    assert_eq!(denied.status(), 401);
    denied.bytes().await.unwrap();
    let response = client.get(&endpoint).bearer_auth("secret").send().await.unwrap();
    assert_eq!(response.status(), 200);
    assert!(response.headers().contains_key("x-request-id"));
    let value: Value = response.json().await.unwrap();
    assert_eq!(value["api_version"], 2);
    assert_eq!(
        value["capabilities"],
        serde_json::json!([
            "head-inspection",
            "snapshot-head-update",
            "bounded-inspection",
            "inspection-preparation",
            "dataset-management",
            "split-revisions",
            "split-head-update",
            "stateless-reads"
        ])
    );
    let response = client.post(&endpoint).bearer_auth("secret").send().await.unwrap();
    assert_eq!(response.status(), 404);
    response.bytes().await.unwrap();
    let endpoint = format!(
        "{}/api/dataset/v2/namespaces/dataset-ns/datasets/example",
        test.origin
    );
    let response = client.put(&endpoint).bearer_auth("secret").send().await.unwrap();
    assert_eq!(response.status(), 200);
    response.bytes().await.unwrap();
    let response = client
        .get(endpoint.replace("/v2/", "/v1/"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    response.bytes().await.unwrap();
    drop(client);
    test.stop().await;
}

#[tokio::test]
async fn head_generation_tracks_head_fence_and_stable_without_changing_latest() {
    let test = TestDatasetHttp::start(Arc::new(TestStore::default())).await;
    let client = Client::new();
    let root = format!(
        "{}/api/dataset/v1/namespaces/dataset-ns/datasets/training%2Fa%20b",
        test.origin
    );
    let head = format!("{}/head", root.replace("/v1/", "/v2/"));
    json(client.get(&head), 404).await;
    json(client.put(&root), 200).await;
    let empty = json(client.get(&head), 200).await;
    assert_eq!(
        empty,
        serde_json::json!({"latest":null,"stable":null,"generation":null})
    );
    let publication: Value = serde_json::from_str(include_str!(
        "../../../sdk/contract-fixtures/dataset-v1/publication.json"
    ))
    .unwrap();
    let published = json(
        client
            .post(format!("{root}/snapshots/publish"))
            .json(&publication),
        200,
    )
    .await;
    let first = json(client.get(&head), 200).await;
    assert_eq!(first["latest"], published["id"]);
    assert_eq!(first["generation"].as_str().unwrap().len(), 64);
    assert_eq!(json(client.get(&head), 200).await, first);
    let stable_url = format!("{root}/names/stable");
    let stable_body = serde_json::json!({"snapshot":published["id"]});
    json(client.put(&stable_url).json(&stable_body), 200).await;
    let stable = json(client.get(&head), 200).await;
    assert_eq!(stable["latest"], first["latest"]);
    assert_eq!(stable["stable"], published["id"]);
    assert_ne!(stable["generation"], first["generation"]);
    json(client.put(stable_url).json(&stable_body), 200).await;
    assert_eq!(json(client.get(&head), 200).await, stable);
    json(client.get(head.replace("dataset-ns", "foreign")), 404).await;
    drop(client);
    test.stop().await;
}
