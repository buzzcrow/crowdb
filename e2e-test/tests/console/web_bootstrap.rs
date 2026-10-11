// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::web_legacy::{bootstrap, get};
use reqwest::Client;
use serde_json::{json, Value};

async fn initialize_data_group(http: &Client, base: &str) {
    let response = http
        .post(format!("{base}/api/cluster/init"))
        .json(&json!({"nodes":[1], "create_data_group":true}))
        .send()
        .await
        .unwrap();
    let status = response.status().as_u16();
    let text = response.text().await.unwrap();
    assert_eq!(status, 201, "cluster init: {text}");
}

async fn reject_binding(http: &Client, base: &str, body: Value, expected: Option<u16>) {
    let path = "/api/nodes/1/disk-groups";
    let response = http
        .post(format!("{base}{path}"))
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let text = response.text().await.unwrap();
    if let Some(expected) = expected {
        assert_eq!(status.as_u16(), expected, "{text}");
    } else {
        assert!(!status.is_success(), "invalid binding must be rejected: {text}");
    }
    assert_eq!(get(http, base, path).await, json!([]));
}

#[tokio::test]
#[ignore = "requires an owned release Web container with its packaged KV service"]
async fn bootstrap_replays_data_group_without_changing_replica_or_publishing_invalid_bindings() {
    let (http, base) = bootstrap().await;
    reject_binding(&http, &base, json!({"id":1}), Some(422)).await;
    initialize_data_group(&http, &base).await;
    let group = "/api/stores/0/groups/1";
    let before = get(&http, &base, group).await;
    assert_eq!(before["replicas"].as_array().unwrap().len(), 1);
    initialize_data_group(&http, &base).await;
    for group_id in [0, 99] {
        reject_binding(
            &http,
            &base,
            json!({"id":1,"store_id":0,"group_id":group_id}),
            None,
        )
        .await;
    }
    let response = http
        .post(format!("{base}/api/nodes/1/disk-groups"))
        .json(&json!({"id":1,"store_id":0,"group_id":1}))
        .send()
        .await
        .unwrap();
    let status = response.status().as_u16();
    let text = response.text().await.unwrap();
    assert_eq!(status, 201, "valid binding: {text}");
    let after = get(&http, &base, group).await;
    assert_eq!(
        before["replicas"][0]["replica_id"],
        after["replicas"][0]["replica_id"]
    );
}
