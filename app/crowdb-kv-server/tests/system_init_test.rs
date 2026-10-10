// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Integration tests for `POST /system/init` (system group bootstrap).

mod common;

use common::process::start_test_server;
use serde_json::Value;

fn client() -> reqwest::Client {
    reqwest::Client::new()
}

#[tokio::test]
async fn system_init_creates_store0_group0() {
    let server = start_test_server(&[]).await.expect("start crowdb-kv-server");

    let resp: Value = client()
        .post(format!("{}/system/init", server.base_url()))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(resp["store_id"], 0);
    assert_eq!(resp["group_id"], 0);
    assert_eq!(resp["replica_id"], 1);
    assert!(resp["listen_addr"].is_string());
}

#[tokio::test]
async fn system_init_idempotent_store0() {
    let server = start_test_server(&[]).await.expect("start crowdb-kv-server");

    // First init creates store 0 + group 0.
    let resp = client()
        .post(format!("{}/system/init", server.base_url()))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 201);

    // Second init should conflict on group 0.
    let resp = client()
        .post(format!("{}/system/init", server.base_url()))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status().as_u16(), 409);
}

#[tokio::test]
async fn system_init_with_custom_replica_id() {
    let server = start_test_server(&[]).await.expect("start crowdb-kv-server");

    let resp: Value = client()
        .post(format!("{}/system/init", server.base_url()))
        .json(&serde_json::json!({"replica_id": 5, "start_election": true}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    assert_eq!(resp["store_id"], 0);
    assert_eq!(resp["group_id"], 0);
    assert_eq!(resp["replica_id"], 5);
}

#[tokio::test]
async fn system_init_creates_store_visible_in_list() {
    let server = start_test_server(&[]).await.expect("start crowdb-kv-server");

    client()
        .post(format!("{}/system/init", server.base_url()))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();

    let resp: Value = client()
        .get(format!("{}/stores", server.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let stores = resp["stores"].as_array().expect("stores array");
    assert!(stores.iter().any(|s| s["store_id"] == 0), "store 0 should exist");
}

#[tokio::test]
async fn system_init_group_visible_in_list() {
    let server = start_test_server(&[]).await.expect("start crowdb-kv-server");

    client()
        .post(format!("{}/system/init", server.base_url()))
        .json(&serde_json::json!({}))
        .send()
        .await
        .unwrap();

    let resp: Value = client()
        .get(format!("{}/stores/0/groups", server.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();

    let groups = resp.as_array().expect("groups array");
    assert!(groups.iter().any(|g| g["group_id"] == 0), "group 0 should exist");
}

fn identified_request(operation: &str) -> Value {
    serde_json::json!({
        "replica_id": 7,
        "start_election": false,
        "bootstrap": {
            "cluster_id": "12345678-1234-4234-8234-123456789abc",
            "operation_id": operation,
            "configuration_digest": "a".repeat(64)
        }
    })
}

#[tokio::test]
async fn prepared_store_ownership_survives_restart_and_rejects_takeover() {
    let root = crowdb_test_harness::test_dirs::TestDir::new("system-bootstrap-owner").unwrap();
    let request = identified_request("22345678-1234-4234-8234-123456789abc");
    let competing = identified_request("32345678-1234-4234-8234-123456789abc");
    let server = common::process::start_test_server_at(root.path(), &[], &[0])
        .await
        .unwrap();
    let response = client()
        .post(format!("{}/system/prepare", server.base_url()))
        .json(&request)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 200);
    let stores: Value = client()
        .get(format!("{}/stores", server.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(stores["stores"].as_array().unwrap().is_empty());
    drop(server);
    let server = common::process::start_test_server_at(root.path(), &[], &[0])
        .await
        .unwrap();
    for body in [&competing, &serde_json::json!({})] {
        let response = client()
            .post(format!("{}/system/init", server.base_url()))
            .json(body)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status().as_u16(), 409);
    }
    let generic = client()
        .post(format!("{}/stores", server.base_url()))
        .json(&serde_json::json!({"store_id": 0}))
        .send()
        .await
        .unwrap();
    assert_eq!(generic.status().as_u16(), 409);
    let generic_join = client()
        .post(format!("{}/stores/0/groups/0/join", server.base_url()))
        .json(&serde_json::json!({"replica_id":1, "peer_endpoint":"127.0.0.1:1"}))
        .send()
        .await
        .unwrap();
    assert_eq!(generic_join.status().as_u16(), 409);
    let conflicting_join = client().post(format!("{}/system/join", server.base_url()))
        .json(&serde_json::json!({"replica_id":1, "peer_endpoint":"127.0.0.1:1", "bootstrap":competing["bootstrap"]})).send().await.unwrap();
    assert_eq!(conflicting_join.status().as_u16(), 409);
    let created = client()
        .post(format!("{}/system/init", server.base_url()))
        .json(&request)
        .send()
        .await
        .unwrap();
    assert_eq!(created.status().as_u16(), 201);
    let retried = client()
        .post(format!("{}/system/init", server.base_url()))
        .json(&request)
        .send()
        .await
        .unwrap();
    assert_eq!(retried.status().as_u16(), 200);
}

#[tokio::test]
async fn competing_prepares_have_one_durable_winner() {
    let server = start_test_server(&[]).await.unwrap();
    let first = identified_request("42345678-1234-4234-8234-123456789abc");
    let second = identified_request("52345678-1234-4234-8234-123456789abc");
    let http = client();
    let url = format!("{}/system/prepare", server.base_url());
    let (first_reply, second_reply) = tokio::join!(
        http.post(&url).json(&first).send(),
        http.post(&url).json(&second).send()
    );
    let statuses = [
        first_reply.unwrap().status().as_u16(),
        second_reply.unwrap().status().as_u16(),
    ];
    assert!(statuses == [200, 409] || statuses == [409, 200]);
    let winner = if statuses[0] == 200 { &first } else { &second };
    assert_eq!(
        http.post(&url)
            .json(winner)
            .send()
            .await
            .unwrap()
            .status()
            .as_u16(),
        200
    );
}

#[tokio::test]
async fn explicit_cleanup_fences_delayed_bootstrap_and_allows_a_new_operation() {
    let root = crowdb_test_harness::test_dirs::TestDir::new("system-bootstrap-cleanup").unwrap();
    let old = identified_request("62345678-1234-4234-8234-123456789abc");
    let new = identified_request("72345678-1234-4234-8234-123456789abc");
    let server = common::process::start_test_server_at(root.path(), &[], &[0])
        .await
        .unwrap();
    let http = client();
    let init = format!("{}/system/init", server.base_url());
    assert_eq!(
        http.post(&init)
            .json(&old)
            .send()
            .await
            .unwrap()
            .status()
            .as_u16(),
        201
    );
    let cleanup = format!("{}/system/cleanup", server.base_url());
    let body = serde_json::json!({"bootstrap": old["bootstrap"], "confirm_delete_system_store": true});
    let response = http.post(&cleanup).json(&body).send().await.unwrap();
    let status = response.status();
    assert_eq!(status.as_u16(), 200, "{}", response.text().await.unwrap());
    assert_eq!(
        http.post(&cleanup)
            .json(&body)
            .send()
            .await
            .unwrap()
            .status()
            .as_u16(),
        200
    );
    assert_eq!(
        http.post(&init)
            .json(&old)
            .send()
            .await
            .unwrap()
            .status()
            .as_u16(),
        409
    );
    assert_eq!(
        http.post(&init)
            .json(&new)
            .send()
            .await
            .unwrap()
            .status()
            .as_u16(),
        201
    );
    assert_eq!(
        http.post(&cleanup)
            .json(&body)
            .send()
            .await
            .unwrap()
            .status()
            .as_u16(),
        409
    );
    drop(server);
    let server = common::process::start_test_server_at(root.path(), &[], &[0])
        .await
        .unwrap();
    assert_eq!(
        http.post(format!("{}/system/init", server.base_url()))
            .json(&old)
            .send()
            .await
            .unwrap()
            .status()
            .as_u16(),
        409
    );
    assert_eq!(
        http.post(format!("{}/system/init", server.base_url()))
            .json(&new)
            .send()
            .await
            .unwrap()
            .status()
            .as_u16(),
        200
    );
}
