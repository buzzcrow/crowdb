// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/dataset.rs"]
mod dataset;

use dataset::{TestDatasetHttp, TestStore};
use reqwest::Client;
use serde_json::Value;
use std::sync::Arc;

#[tokio::test]
async fn capabilities_use_existing_auth_and_do_not_enable_unimplemented_writes() {
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
    assert_eq!(value["capabilities"], serde_json::json!([]));
    let response = client.post(&endpoint).bearer_auth("secret").send().await.unwrap();
    assert_eq!(response.status(), 404);
    response.bytes().await.unwrap();
    let endpoint = format!(
        "{}/api/dataset/v2/namespaces/dataset-ns/datasets/example",
        test.origin
    );
    let response = client.put(&endpoint).bearer_auth("secret").send().await.unwrap();
    assert_eq!(response.status(), 404);
    response.bytes().await.unwrap();
    let response = client
        .get(endpoint.replace("/v2/", "/v1/"))
        .bearer_auth("secret")
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 404);
    response.bytes().await.unwrap();
    drop(client);
    test.stop().await;
}
