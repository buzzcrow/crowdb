// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use serde_json::{json, Value};
use std::time::{Duration, Instant};

pub(super) async fn json_get(client: &reqwest::Client, url: &str) -> (reqwest::StatusCode, Value) {
    let r = client.get(url).send().await.unwrap();
    let status = r.status();
    let v = r.json::<Value>().await.unwrap_or(Value::Null);
    (status, v)
}

pub(super) async fn json_post(
    client: &reqwest::Client,
    url: &str,
    body: Value,
) -> (reqwest::StatusCode, Value) {
    let r = client.post(url).json(&body).send().await.unwrap();
    let status = r.status();
    let v = r.json::<Value>().await.unwrap_or(Value::Null);
    (status, v)
}

pub(super) async fn create_rack(client: &reqwest::Client, base: &str, rack_id: u64) {
    let (status, body) = json_post(client, &format!("{base}/api/racks"), json!({ "id": rack_id })).await;
    assert_eq!(status.as_u16(), 201, "create rack {rack_id}: {body}");
}

pub(super) async fn create_node(
    client: &reqwest::Client,
    base: &str,
    node_id: u64,
    rack_id: u64,
    host: &str,
) {
    let (status, body) = json_post(
        client,
        &format!("{base}/api/nodes"),
        json!({ "id": node_id, "rack_id": rack_id, "host": host, "ssh_user": "" }),
    )
    .await;
    assert_eq!(status.as_u16(), 201, "create node {node_id}: {body}");
}

pub(super) async fn stop_server(client: &reqwest::Client, base: &str, node_id: u64) {
    let (status, body) = json_post(
        client,
        &format!("{base}/api/nodes/{node_id}/server/stop"),
        json!({}),
    )
    .await;
    assert!(status.is_success(), "stop {node_id}: {status} {body}");
}

pub(super) async fn restart_server(client: &reqwest::Client, base: &str, node_id: u64) -> u32 {
    let (status, body) = json_post(
        client,
        &format!("{base}/api/nodes/{node_id}/server/restart"),
        json!({}),
    )
    .await;
    assert!(status.is_success(), "restart {node_id}: {status} {body}");
    u32::try_from(body["pid"].as_u64().expect("pid")).unwrap()
}

pub(super) async fn create_store(client: &reqwest::Client, base: &str, store_id: u64, nodes: &[u64]) {
    // Retry with backoff — a node may be momentarily unreachable
    // right after concurrent deploy (the REST server may not have
    // started listening yet). Mirrors the retry in create_group.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (status, body) = json_post(
            client,
            &format!("{base}/api/stores"),
            json!({ "store_id": store_id, "nodes": nodes }),
        )
        .await;
        if status.as_u16() == 201 {
            return;
        }
        if Instant::now() >= deadline {
            assert_eq!(status.as_u16(), 201, "create store {store_id}: {body}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

pub(super) async fn create_group(
    client: &reqwest::Client,
    base: &str,
    store_id: u64,
    group_id: u64,
    replica_id: u64,
    nodes: &[u64],
) {
    // Retry with backoff — the kv-server on the target node may be
    // momentarily unreachable right after concurrent deploy.
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let (status, body) = json_post(
            client,
            &format!("{base}/api/stores/{store_id}/groups"),
            json!({ "group_id": group_id, "replica_id": replica_id, "nodes": nodes }),
        )
        .await;
        if status.as_u16() == 201 {
            return;
        }
        if Instant::now() >= deadline {
            assert_eq!(status.as_u16(), 201, "create group {store_id}/{group_id}: {body}");
        }
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

pub(super) async fn kv_put(
    client: &reqwest::Client,
    base: &str,
    store_id: u64,
    group_id: u64,
    key: &str,
    value: &str,
) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let (status, body) = json_post(
            client,
            &format!("{base}/api/stores/{store_id}/groups/{group_id}/kv/put"),
            json!({ "key": key, "value": value }),
        )
        .await;
        if status.as_u16() == 200 && body["ok"] == true {
            return;
        }
        if Instant::now() >= deadline {
            assert_eq!(status.as_u16(), 200, "kv put {store_id}/{group_id} {key}: {body}");
            assert_eq!(body["ok"], true);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub(super) async fn kv_delete(client: &reqwest::Client, base: &str, store_id: u64, group_id: u64, key: &str) {
    let deadline = Instant::now() + Duration::from_secs(3);
    loop {
        let (status, body) = json_post(
            client,
            &format!("{base}/api/stores/{store_id}/groups/{group_id}/kv/delete"),
            json!({ "key": key }),
        )
        .await;
        if status.as_u16() == 200 && body["ok"] == true {
            return;
        }
        if Instant::now() >= deadline {
            assert_eq!(
                status.as_u16(),
                200,
                "kv delete {store_id}/{group_id} {key}: {body}"
            );
            assert_eq!(body["ok"], true);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

pub(super) async fn kv_get(
    client: &reqwest::Client,
    base: &str,
    store_id: u64,
    group_id: u64,
    key: &str,
) -> Value {
    let (status, body) = json_get(
        client,
        &format!("{base}/api/stores/{store_id}/groups/{group_id}/kv/get?key={key}"),
    )
    .await;
    assert_eq!(status.as_u16(), 200, "kv get {store_id}/{group_id} {key}: {body}");
    body
}

pub(super) async fn kv_scan(
    client: &reqwest::Client,
    base: &str,
    store_id: u64,
    group_id: u64,
) -> (u16, Value) {
    let (status, body) = json_get(
        client,
        &format!("{base}/api/stores/{store_id}/groups/{group_id}/kv/scan?limit=10000"),
    )
    .await;
    (status.as_u16(), body)
}

pub(super) async fn wait_for_group_leader(
    client: &reqwest::Client,
    base: &str,
    store_id: u64,
    group_id: u64,
    expected_replicas: usize,
    timeout: Duration,
) -> Value {
    let deadline = Instant::now() + timeout;
    let mut last_observation = String::new();
    while Instant::now() < deadline {
        let (status, body) =
            json_get(client, &format!("{base}/api/stores/{store_id}/groups/{group_id}")).await;
        last_observation = format!("{status}: {body}");
        if status.is_success() {
            let replicas = body["replicas"].as_array().cloned().unwrap_or_default();
            let leaders: Vec<u64> = replicas
                .iter()
                .filter(|r| r["role"].as_str() == Some("leader"))
                .filter_map(|r| r["replica_id"].as_u64())
                .collect();
            let count_ok = expected_replicas == 0 || replicas.len() == expected_replicas;
            if count_ok && leaders.len() == 1 {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let (status_b, body_b) =
                    json_get(client, &format!("{base}/api/stores/{store_id}/groups/{group_id}")).await;
                if status_b.is_success() {
                    let replicas_b = body_b["replicas"].as_array().cloned().unwrap_or_default();
                    let leaders_b: Vec<u64> = replicas_b
                        .iter()
                        .filter(|r| r["role"].as_str() == Some("leader"))
                        .filter_map(|r| r["replica_id"].as_u64())
                        .collect();
                    let count_ok_b = expected_replicas == 0 || replicas_b.len() == expected_replicas;
                    if count_ok_b && leaders_b == leaders {
                        return body_b;
                    }
                }
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("group {store_id}/{group_id} failed to converge to one leader within {timeout:?}; last observation: {last_observation}");
}

pub(super) async fn wait_for_store(
    client: &reqwest::Client,
    base: &str,
    store_id: u64,
    expected_groups: usize,
    timeout: Duration,
) {
    let deadline = Instant::now() + timeout;
    let mut last_observation = String::new();
    while Instant::now() < deadline {
        let (status, body) = json_get(client, &format!("{base}/api/stores/{store_id}")).await;
        last_observation = format!("{status}: {body}");
        if status.is_success() {
            let groups = body["groups"].as_array().cloned().unwrap_or_default();
            if groups.len() == expected_groups {
                return;
            }
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("store {store_id} failed to report {expected_groups} groups within {timeout:?}; last observation: {last_observation}");
}
