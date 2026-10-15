// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/dataset.rs"]
mod dataset;
use dataset::{TestDatasetHttp, TestStore};
use reqwest::Client;
use serde_json::{json, Value};
use std::sync::Arc;

async fn response(request: reqwest::RequestBuilder, expected: u16) -> Value {
    let response = request.bearer_auth("secret").send().await.unwrap();
    let status = response.status().as_u16();
    let body: Value = response.json().await.unwrap();
    assert_eq!(status, expected, "{body}");
    body
}
async fn fixture(client: &Client, test: &TestDatasetHttp) -> (String, String) {
    let v1 = format!(
        "{}/api/dataset/v1/namespaces/dataset-ns/datasets/inspection",
        test.origin
    );
    response(client.put(&v1), 200).await;
    let publication: Value = serde_json::from_str(include_str!(
        "../../../sdk/contract-fixtures/dataset-v1/publication.json"
    ))
    .unwrap();
    let published = response(
        client.post(format!("{v1}/snapshots/publish")).json(&publication),
        200,
    )
    .await;
    (
        v1.replace("/v1/", "/v2/"),
        published["id"].as_str().unwrap().into(),
    )
}

#[tokio::test]
async fn metadata_pages_preserve_scope_binary_ids_and_field_provenance() {
    let test = TestDatasetHttp::start(Arc::new(TestStore::default())).await;
    let client = Client::new();
    let (root, snapshot) = fixture(&client, &test).await;
    let catalog = response(client.get(root.rsplit_once('/').unwrap().0), 200).await;
    assert_eq!(catalog["entries"][0]["name"], "inspection");
    let inventory = response(client.get(format!("{root}/snapshot-inventory")), 200).await;
    assert_eq!(inventory["entries"][0]["id"], snapshot);
    assert_eq!(inventory["entries"][0]["sample_count"], "1");
    assert_eq!(
        inventory,
        response(client.get(format!("{root}/roots")), 200).await
    );
    let base = format!("{root}/snapshots/{snapshot}");
    let summary = response(client.get(format!("{base}/summary")), 200).await;
    assert_eq!(summary, inventory["entries"][0]);
    assert_eq!(
        response(client.get(format!("{base}/children")), 200).await["entries"],
        json!([])
    );
    let schema = response(client.get(format!("{base}/schema")), 200).await;
    let fields: Vec<_> = schema["entries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|f| f["name"].clone())
        .collect();
    let samples = response(client.get(format!("{base}/samples")), 200).await;
    let id = samples["entries"][0].as_str().unwrap();
    assert!(response(
        client.get(format!("{base}/contains")).query(&[("sample", id)]),
        200
    )
    .await["contains"]
        .as_bool()
        .unwrap());
    let matrix = response(
        client
            .get(format!("{base}/matrix"))
            .query(&[("fields", serde_json::to_string(&fields).unwrap())]),
        200,
    )
    .await;
    assert_eq!(matrix["entries"][0]["id"], id);
    assert_eq!(matrix["entries"][0]["fields"][0]["source_snapshot"], snapshot);
    assert!(matrix["entries"][0]["fields"][0]["length"].is_string());
    assert!(matrix["entries"][0]["fields"][0].get("location").is_none());
    let status = response(client.get(format!("{base}/inspection")), 200).await;
    assert_eq!(status["state"], "ready");
    assert_eq!(
        response(client.post(format!("{base}/inspection")), 200).await,
        status
    );
    drop(client);
    test.stop().await;
}

#[tokio::test]
async fn inspection_rejects_unknown_duplicate_and_oversized_query_inputs() {
    let test = TestDatasetHttp::start(Arc::new(TestStore::default())).await;
    let client = Client::new();
    let (root, snapshot) = fixture(&client, &test).await;
    for query in [
        "limit=0",
        "limit=101",
        "limit=1&limit=2",
        "unknown=true",
        "after=not-base64",
        "limit=-1",
    ] {
        response(client.get(format!("{root}/snapshot-inventory?{query}")), 400).await;
    }
    let base = format!("{root}/snapshots/{snapshot}");
    response(client.get(format!("{base}/matrix")), 400).await;
    response(
        client
            .get(format!("{base}/matrix"))
            .query(&[("fields", "[\"unknown\"]")]),
        400,
    )
    .await;
    response(client.get(format!("{base}/contains?sample=")), 400).await;
    response(client.get(format!("{base}/contains?sample=YQ")), 400).await;
    response(
        client.get(format!("{root}/snapshots/{}/summary", "0".repeat(32))),
        400,
    )
    .await;
    let denied = client.get(format!("{base}/schema")).send().await.unwrap();
    assert_eq!(denied.status(), 401);
    denied.bytes().await.unwrap();
    drop(client);
    test.stop().await;
}

#[tokio::test]
async fn catalog_continuations_do_not_cross_scope_or_repeat_entries() {
    let test = TestDatasetHttp::start(Arc::new(TestStore::default())).await;
    let client = Client::new();
    let (root, _) = fixture(&client, &test).await;
    let catalog = root.rsplit_once('/').unwrap().0;
    for name in ["other", "third"] {
        response(
            client.put(format!("{}/{name}", catalog.replace("/v2/", "/v1/"))),
            200,
        )
        .await;
    }
    let first = response(client.get(catalog).query(&[("limit", "1")]), 200).await;
    let after = first["after"].as_str().unwrap();
    let second = response(
        client.get(catalog).query(&[("limit", "1"), ("after", after)]),
        200,
    )
    .await;
    assert_ne!(first["entries"][0]["id"], second["entries"][0]["id"]);
    assert_eq!(first["entries"].as_array().unwrap().len(), 1);
    response(
        client
            .get(format!("{root}/snapshot-inventory"))
            .query(&[("after", after)]),
        400,
    )
    .await;
    drop(client);
    test.stop().await;
}

#[tokio::test]
async fn prepared_snapshot_is_hidden_from_metadata_and_preparation_status() {
    let test = TestDatasetHttp::start(Arc::new(TestStore::default())).await;
    let client = Client::new();
    let (root, parent) = fixture(&client, &test).await;
    let mut publication: Value = serde_json::from_str(include_str!(
        "../../../sdk/contract-fixtures/dataset-v1/publication.json"
    ))
    .unwrap();
    publication["operation"] = json!("02020202020202020202020202020202");
    publication["parent"] = json!(parent);
    let prepared = response(
        client
            .post(format!("{}/snapshots/prepare", root.replace("/v2/", "/v1/")))
            .json(&publication),
        200,
    )
    .await;
    let id = prepared["id"].as_str().unwrap();
    let inventory = response(client.get(format!("{root}/snapshot-inventory")), 200).await;
    assert_eq!(inventory["entries"].as_array().unwrap().len(), 1);
    for action in ["summary", "schema", "samples", "inspection"] {
        response(client.get(format!("{root}/snapshots/{id}/{action}")), 404).await;
    }
    response(client.post(format!("{root}/snapshots/{id}/inspection")), 404).await;
    drop(client);
    test.stop().await;
}
