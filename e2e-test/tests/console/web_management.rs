// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::web_legacy::{bootstrap, get};
use reqwest::{Client, Method};
use serde_json::{json, Value};

async fn mutate(http: &Client, base: &str, method: Method, path: &str, body: Value, status: u16) {
    let response = http
        .request(method, format!("{base}{path}"))
        .json(&body)
        .send()
        .await
        .unwrap();
    let actual = response.status().as_u16();
    let text = response.text().await.unwrap();
    assert_eq!(actual, status, "{path}: {text}");
}

#[tokio::test]
#[ignore = "requires an owned release Web container with its packaged KV service"]
async fn store_and_group_management_cycle_preserves_identity() {
    let (http, base) = bootstrap().await;
    mutate(
        &http,
        &base,
        Method::DELETE,
        "/api/nodes/1/server",
        Value::Null,
        409,
    )
    .await;
    mutate(
        &http,
        &base,
        Method::POST,
        "/api/stores",
        json!({"store_id":7,"nodes":[1]}),
        201,
    )
    .await;
    assert!(get(&http, &base, "/api/stores")
        .await
        .as_array()
        .unwrap()
        .iter()
        .any(|s| s["store_id"] == 7));
    let detail = get(&http, &base, "/api/stores/7").await;
    assert_eq!(detail["store_id"], 7);
    assert!(detail["groups"].as_array().unwrap().is_empty(), "{detail}");
    for (group, replica) in [(70, 700), (80, 800)] {
        mutate(
            &http,
            &base,
            Method::POST,
            "/api/stores/7/groups",
            json!({"group_id":group,"replica_id":replica,"nodes":[1]}),
            201,
        )
        .await;
    }
    assert_eq!(
        get(&http, &base, "/api/stores/7/groups")
            .await
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(get(&http, &base, "/api/stores/7/groups/80").await["group_id"], 80);
    mutate(
        &http,
        &base,
        Method::DELETE,
        "/api/stores/7/groups/80",
        Value::Null,
        204,
    )
    .await;
    mutate(&http, &base, Method::DELETE, "/api/stores/7", Value::Null, 204).await;
    assert!(!get(&http, &base, "/api/stores")
        .await
        .as_array()
        .unwrap()
        .iter()
        .any(|s| s["store_id"] == 7));
}
