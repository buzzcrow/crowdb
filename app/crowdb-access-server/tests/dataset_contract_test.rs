// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/dataset.rs"]
mod dataset;

use dataset::{TestDatasetHttp, TestStore};
use reqwest::{Client, Method};
use serde_json::{json, Value};
use std::sync::Arc;

const ROOT: &str = "/api/dataset/v1/namespaces/dataset-ns/datasets/training%2Fa%20b";
const PUBLICATION: &str = include_str!("../../../sdk/contract-fixtures/dataset-v1/publication.json");

async fn call(
    client: &Client,
    test: &TestDatasetHttp,
    method: Method,
    path: &str,
    body: Option<&Value>,
    status: u16,
) -> Value {
    let mut request = client
        .request(method, format!("{}{path}", test.origin))
        .bearer_auth("secret");
    if let Some(body) = body {
        request = request
            .header("content-type", "application/json")
            .body(body.to_string());
    }
    let response = request.send().await.unwrap();
    let actual = response.status().as_u16();
    let request_id = response
        .headers()
        .get("x-request-id")
        .unwrap()
        .to_str()
        .unwrap()
        .to_owned();
    let bytes = response.bytes().await.unwrap();
    assert_eq!(actual, status, "{}", String::from_utf8_lossy(&bytes));
    assert_eq!(request_id.len(), 32);
    serde_json::from_slice(&bytes).unwrap()
}

#[tokio::test]
async fn json_prepare_publish_retry_and_restart_preserve_one_candidate() {
    let store = Arc::new(TestStore::default());
    let test = TestDatasetHttp::start(Arc::clone(&store)).await;
    let client = Client::new();
    let record = call(&client, &test, Method::PUT, ROOT, None, 200).await;
    assert_eq!(record["name"], "training/a b");
    assert_eq!(record["namespace"], "dataset-ns");
    assert_eq!(record["latest"], Value::Null);
    call(&client, &test, Method::PUT, ROOT, None, 409).await;
    let publication: Value = serde_json::from_str(PUBLICATION).unwrap();
    let prepared = call(
        &client,
        &test,
        Method::POST,
        &format!("{ROOT}/snapshots/prepare"),
        Some(&publication),
        200,
    )
    .await;
    assert_eq!(prepared["state"], "prepared");
    assert_eq!(prepared["is_latest"], false);
    let opened = call(&client, &test, Method::GET, ROOT, None, 200).await;
    assert_eq!(opened["latest"], Value::Null);
    call(
        &client,
        &test,
        Method::GET,
        &format!("{ROOT}/snapshots/{}", prepared["id"].as_str().unwrap()),
        None,
        404,
    )
    .await;
    drop(client);
    test.stop().await;
    let test = TestDatasetHttp::start(store).await;
    let client = Client::new();
    let published = call(
        &client,
        &test,
        Method::POST,
        &format!("{ROOT}/snapshots/publish"),
        Some(&publication),
        200,
    )
    .await;
    assert_eq!(published["id"], prepared["id"]);
    assert_eq!(published["state"], "published");
    assert_eq!(published["is_latest"], true);
    let duplicate = call(
        &client,
        &test,
        Method::POST,
        &format!("{ROOT}/snapshots/publish"),
        Some(&publication),
        200,
    )
    .await;
    assert_eq!(duplicate, published);
    let id = published["id"].as_str().unwrap();
    let manifest = call(
        &client,
        &test,
        Method::GET,
        &format!("{ROOT}/snapshots/{id}/manifest"),
        None,
        200,
    )
    .await;
    assert_eq!(manifest, publication["manifest"]);
    let token = publication["operation"].as_str().unwrap();
    let operation = call(
        &client,
        &test,
        Method::GET,
        &format!("{ROOT}/operations/{token}"),
        None,
        200,
    )
    .await;
    assert_eq!(operation, published);
    verify_retention_and_conflicts(&client, &test, &publication, id).await;
    drop(client);
    test.stop().await;
}

async fn verify_retention_and_conflicts(
    client: &Client,
    test: &TestDatasetHttp,
    publication: &Value,
    id: &str,
) {
    let mut divergent = publication.clone();
    divergent["manifest"]["samples"][0]["fields"]["value"]["data"] = json!("d29ybGQ=");
    let error = call(
        client,
        test,
        Method::POST,
        &format!("{ROOT}/snapshots/publish"),
        Some(&divergent),
        409,
    )
    .await;
    assert_eq!(error["code"], "conflict");
    assert_eq!(error["retryable"], false);
    call(
        client,
        test,
        Method::POST,
        &format!("{ROOT}/snapshots/{id}/retain"),
        None,
        200,
    )
    .await;
    call(
        client,
        test,
        Method::POST,
        &format!("{ROOT}/snapshots/{id}/release"),
        None,
        200,
    )
    .await;
    let stable = call(
        client,
        test,
        Method::PUT,
        &format!("{ROOT}/names/stable"),
        Some(&json!({"snapshot":id})),
        200,
    )
    .await;
    assert_eq!(stable["stable"], id);
}

#[tokio::test]
async fn scope_auth_versions_and_malformed_contract_reject_without_mutation() {
    let test = TestDatasetHttp::start(Arc::new(TestStore::default())).await;
    let client = Client::new();
    let denied = client.get(format!("{}{ROOT}", test.origin)).send().await.unwrap();
    assert_eq!(denied.status(), 401);
    let body: Value = serde_json::from_slice(&denied.bytes().await.unwrap()).unwrap();
    assert_eq!(body["code"], "unauthorized");
    call(&client, &test, Method::PUT, ROOT, None, 200).await;
    call(
        &client,
        &test,
        Method::GET,
        &ROOT.replace("dataset-ns", "foreign"),
        None,
        404,
    )
    .await;
    call(&client, &test, Method::GET, &ROOT.replace("v1", "v2"), None, 404).await;
    call(
        &client,
        &test,
        Method::GET,
        &ROOT.replace("training%2Fa%20b", "%GG"),
        None,
        400,
    )
    .await;
    let publication: Value = serde_json::from_str(PUBLICATION).unwrap();
    let mut unknown = publication.clone();
    unknown["future_optional"] = json!(true);
    call(
        &client,
        &test,
        Method::POST,
        &format!("{ROOT}/snapshots/prepare"),
        Some(&unknown),
        200,
    )
    .await;
    for key in ["operation", "parent", "manifest"] {
        let mut invalid = publication.clone();
        invalid.as_object_mut().unwrap().remove(key);
        call(
            &client,
            &test,
            Method::POST,
            &format!("{ROOT}/snapshots/publish"),
            Some(&invalid),
            400,
        )
        .await;
    }
    let mut invalid = publication;
    invalid["manifest"]["samples"][0]["fields"]["value"]["kind"] = json!("unknown");
    call(
        &client,
        &test,
        Method::POST,
        &format!("{ROOT}/snapshots/publish"),
        Some(&invalid),
        400,
    )
    .await;
    let record = call(&client, &test, Method::GET, ROOT, None, 200).await;
    assert_eq!(record["latest"], Value::Null);
    drop(client);
    test.stop().await;
}

#[tokio::test]
async fn exhausted_admission_returns_structured_error_for_a_streamed_part() {
    let test = TestDatasetHttp::start_with_capacity(Arc::new(TestStore::default()), 0).await;
    let client = Client::new();
    let chunk = bytes::Bytes::from(vec![0; 128 * 1024]);
    for _ in 0..2 {
        let chunk = chunk.clone();
        let chunks = futures::stream::iter((0..32).map(move |_| Ok::<_, std::io::Error>(chunk.clone())));
        let response = client
            .put(format!(
                "{}{ROOT}/uploads/{}/parts/0",
                test.origin,
                "a".repeat(32)
            ))
            .bearer_auth("secret")
            .header("content-type", "application/octet-stream")
            .body(reqwest::Body::wrap_stream(chunks))
            .send()
            .await
            .expect("capacity rejection must remain an HTTP response");
        assert_eq!(response.status(), 429);
        let error: Value = response.json().await.unwrap();
        assert_eq!(error["code"], "capacity");
        assert_eq!(error["retryable"], true);
    }
    drop(client);
    test.stop().await;
}
