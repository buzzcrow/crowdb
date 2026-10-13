// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::common::management::{addresses, client, origin, request};
use reqwest::{Client, Method};
use serde_json::{json, Value};

async fn discovery(client: &Client, base: &str) {
    let health = request(client, base, Method::GET, "/health", Value::Null, 200).await;
    assert_eq!(health["status"], "ok");
    let api = request(client, base, Method::GET, "/openapi.json", Value::Null, 200).await;
    assert_eq!(api["openapi"], "3.1.0");
    assert!(api["paths"]["/health"].is_object());
    assert!(api["paths"]["/topology"].is_object());
    let stores = request(client, base, Method::GET, "/stores", Value::Null, 200).await;
    assert_eq!(stores["stores"].as_array().unwrap().len(), 1);
    assert_eq!(stores["stores"][0]["store_id"], 0);
    assert!(stores["stores"][0]["group_count"].as_u64().unwrap() >= 1);
    let topology = request(client, base, Method::GET, "/topology", Value::Null, 200).await;
    assert_eq!(topology["stores"].as_array().unwrap().len(), 1);
    let store = &topology["stores"][0];
    assert_eq!(store["store_id"], 0);
    assert_eq!(store["groups"].as_array().unwrap().len(), 1);
    assert_eq!(store["groups"][0]["group_id"], 1);
    assert_eq!(store["groups"][0]["local_replica_id"], 1);
    let alias = request(client, base, Method::GET, "/top", Value::Null, 200).await;
    assert!(!alias["stores"].as_array().unwrap().is_empty());
    // Before election completes either state is valid, but the schema is fixed.
    let response = client
        .get(format!("{base}/stores/0/groups/1/ready"))
        .send()
        .await
        .unwrap();
    assert!(matches!(response.status().as_u16(), 200 | 503));
    let ready: Value = response.json().await.unwrap();
    assert!(ready["leader_id"].as_u64().is_some());
    assert!(ready["voting_replicas"].as_u64().is_some());
    request(client, base, Method::GET, "/operations/99999", Value::Null, 404).await;
}

async fn metrics(client: &Client, base: &str) {
    let all = request(client, base, Method::GET, "/metrics", Value::Null, 200).await;
    assert!(all["window_secs"].as_f64().is_some());
    assert!(all["timestamp"].is_string());
    let points = all["metrics"].as_array().unwrap();
    assert!(!points.is_empty());
    for point in points {
        assert!(point["name"].is_string());
        assert!(point["kind"].is_string());
        let fields = point["fields"].as_array().unwrap();
        assert!(!fields.is_empty());
        for field in fields {
            assert!(field["key"].is_string());
            assert!(field["value"].as_f64().is_some());
        }
    }
    let filtered = request(
        client,
        base,
        Method::GET,
        "/metrics?prefix=zzz.nonexistent.",
        Value::Null,
        200,
    )
    .await;
    assert!(filtered["metrics"].as_array().unwrap().is_empty());
}

async fn groups(client: &Client, base: &str) {
    let list = request(client, base, Method::GET, "/stores/0/groups", Value::Null, 200).await;
    assert_eq!(list.as_array().unwrap().len(), 1);
    assert_eq!(list[0]["group_id"], 1);
    request(
        client,
        base,
        Method::POST,
        "/stores/0/groups",
        json!({"group_id": 1, "replica_id": 1}),
        409,
    )
    .await;
    request(
        client,
        base,
        Method::POST,
        "/stores/99/groups",
        json!({"group_id": 1, "replica_id": 1}),
        404,
    )
    .await;
    request(
        client,
        base,
        Method::DELETE,
        "/stores/0/groups/99",
        Value::Null,
        404,
    )
    .await;
    request(
        client,
        base,
        Method::POST,
        "/stores/0/groups",
        json!({"group_id": 2, "replica_id": 1}),
        201,
    )
    .await;
    let list = request(client, base, Method::GET, "/stores/0/groups", Value::Null, 200).await;
    assert_eq!(list.as_array().unwrap().len(), 2);
    request(
        client,
        base,
        Method::DELETE,
        "/stores/0/groups/2",
        Value::Null,
        200,
    )
    .await;
    request(
        client,
        base,
        Method::DELETE,
        "/stores/0/groups/1",
        Value::Null,
        200,
    )
    .await;
    let list = request(client, base, Method::GET, "/stores/0/groups", Value::Null, 200).await;
    assert!(list.as_array().unwrap().is_empty());
}

async fn stores(client: &Client, base: &str) {
    request(client, base, Method::GET, "/stores/99", Value::Null, 404).await;
    request(client, base, Method::DELETE, "/stores/99", Value::Null, 404).await;
    request(client, base, Method::POST, "/stores", json!({"store_id": 0}), 409).await;
    let created = request(client, base, Method::POST, "/stores", json!({"store_id": 5}), 201).await;
    assert_eq!(created["store_id"], 5);
    assert_eq!(created["group_count"], 0);
    let list = request(client, base, Method::GET, "/stores", Value::Null, 200).await;
    assert_eq!(list["stores"].as_array().unwrap().len(), 2);
    groups(client, base).await;
    request(client, base, Method::DELETE, "/stores/0", Value::Null, 200).await;
    request(client, base, Method::GET, "/stores/0", Value::Null, 404).await;
}

#[tokio::test]
#[ignore = "requires the release-container fixture; run pixi run test-container-e2e"]
async fn management_lifecycle() {
    let base = origin(&addresses(1)[0]);
    let client = client();
    discovery(&client, &base).await;
    metrics(&client, &base).await;
    stores(&client, &base).await;
}
