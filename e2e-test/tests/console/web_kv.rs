// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::web_legacy::{bootstrap, get};
use reqwest::Client;
use serde_json::{json, Value};

async fn post(http: &Client, base: &str, path: &str, body: Value, expected: u16) {
    let response = http
        .post(format!("{base}{path}"))
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = response.status().as_u16();
    let text = response.text().await.unwrap();
    assert_eq!(status, expected, "{path}: {text}");
}

#[tokio::test]
#[ignore = "requires an owned release Web container with its packaged KV service"]
async fn put_get_delete_and_binary_values_through_web_routes() {
    let (http, base) = bootstrap().await;
    post(
        &http,
        &base,
        "/api/stores",
        json!({"store_id":1,"nodes":[1]}),
        201,
    )
    .await;
    post(
        &http,
        &base,
        "/api/stores/1/groups",
        json!({"group_id":1,"replica_id":1,"nodes":[1]}),
        201,
    )
    .await;
    get(&http, &base, "/api/stores/1/groups/1/endpoint").await;
    let path = "/api/stores/1/groups/1/kv";
    post(
        &http,
        &base,
        &format!("{path}/put"),
        json!({"key":"alpha","value":"beta","client_id":1,"seq":1}),
        200,
    )
    .await;
    let found = get(&http, &base, &format!("{path}/get?key=alpha")).await;
    assert_eq!(found["found"], true);
    assert_eq!(found["value_utf8"], "beta");
    post(
        &http,
        &base,
        &format!("{path}/delete"),
        json!({"key":"alpha"}),
        200,
    )
    .await;
    assert_eq!(
        get(&http, &base, &format!("{path}/get?key=alpha")).await["found"],
        false
    );
    post(
        &http,
        &base,
        &format!("{path}/put"),
        json!({"key_hex":"ff00ff","value_hex":"deadbeef"}),
        200,
    )
    .await;
    let binary = get(&http, &base, &format!("{path}/get?key_hex=ff00ff")).await;
    assert_eq!(binary["found"], true);
    assert_eq!(binary["value_hex"], "deadbeef");
}
