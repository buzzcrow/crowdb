// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::common::management::{addresses, client, origin, request};
use reqwest::{Client, Method};
use serde_json::{json, Value};

fn identified(operation: &str) -> Value {
    json!({"replica_id": 7, "start_election": false, "bootstrap": {
        "cluster_id": "12345678-1234-4234-8234-123456789abc",
        "operation_id": operation, "configuration_digest": "a".repeat(64)}})
}

async fn post(http: &Client, base: &str, path: &str, body: Value, status: u16) {
    request(http, base, Method::POST, path, body, status).await;
}

#[tokio::test]
#[ignore = "requires a bare release-container node and retained restart"]
async fn prepared_owner_survives_restart() {
    let base = origin(&addresses(1)[0]);
    let http = client();
    let owner = identified("22345678-1234-4234-8234-123456789abc");
    let competing = identified("32345678-1234-4234-8234-123456789abc");
    if std::env::var("CROWDB_E2E_PHASE").unwrap() == "write" {
        post(&http, &base, "/system/prepare", owner, 200).await;
        let stores = request(&http, &base, Method::GET, "/stores", Value::Null, 200).await;
        assert!(stores["stores"].as_array().unwrap().is_empty());
        return;
    }
    for body in [competing.clone(), json!({})] {
        post(&http, &base, "/system/init", body, 409).await;
    }
    post(&http, &base, "/stores", json!({"store_id": 0}), 409).await;
    post(
        &http,
        &base,
        "/stores/0/groups/0/join",
        json!({"replica_id": 1, "peer_endpoint": "127.0.0.1:1"}),
        409,
    )
    .await;
    post(
        &http,
        &base,
        "/system/join",
        json!({"replica_id": 1,
        "peer_endpoint": "127.0.0.1:1", "bootstrap": competing["bootstrap"]}),
        409,
    )
    .await;
    post(&http, &base, "/system/init", owner.clone(), 201).await;
    post(&http, &base, "/system/init", owner, 200).await;
}

#[tokio::test]
#[ignore = "requires a bare release-container node"]
async fn competing_prepares() {
    let base = origin(&addresses(1)[0]);
    let http = client();
    let first = identified("42345678-1234-4234-8234-123456789abc");
    let second = identified("52345678-1234-4234-8234-123456789abc");
    let url = format!("{base}/system/prepare");
    let (first_reply, second_reply) = tokio::join!(
        http.post(&url).json(&first).send(),
        http.post(&url).json(&second).send()
    );
    let statuses = [
        first_reply.unwrap().status().as_u16(),
        second_reply.unwrap().status().as_u16(),
    ];
    assert!(statuses == [200, 409] || statuses == [409, 200]);
    let winner = if statuses[0] == 200 { first } else { second };
    post(&http, &base, "/system/prepare", winner, 200).await;
}

#[tokio::test]
#[ignore = "requires a bare release-container node and retained restart"]
async fn cleanup_fences_delayed_requests() {
    let base = origin(&addresses(1)[0]);
    let http = client();
    let old = identified("62345678-1234-4234-8234-123456789abc");
    let new = identified("72345678-1234-4234-8234-123456789abc");
    if std::env::var("CROWDB_E2E_PHASE").unwrap() == "write" {
        post(&http, &base, "/system/init", old.clone(), 201).await;
        let cleanup = json!({"bootstrap": old["bootstrap"], "confirm_delete_system_store": true});
        for _ in 0..2 {
            post(&http, &base, "/system/cleanup", cleanup.clone(), 200).await;
        }
        post(&http, &base, "/system/init", old.clone(), 409).await;
        post(&http, &base, "/system/init", new.clone(), 201).await;
        post(&http, &base, "/system/cleanup", cleanup, 409).await;
    }
    post(&http, &base, "/system/init", old, 409).await;
    post(&http, &base, "/system/init", new, 200).await;
}
