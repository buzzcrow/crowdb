// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::web_voters::{replicas, setup};
use crate::common::management::request;
use crowdb_console_shared::clients::http::ServerClient;
use reqwest::{Client, Method};
use serde_json::{json, Value};
use std::time::{Duration, Instant};

async fn leader(http: &Client, base: &str) -> u64 {
    for _ in 0..20 {
        let group = request(
            http,
            base,
            Method::GET,
            "/api/stores/7/groups/70",
            Value::Null,
            200,
        )
        .await;
        if let Some(id) = group["replicas"]
            .as_array()
            .unwrap()
            .iter()
            .find(|replica| replica["role"] == "leader")
            .and_then(|replica| replica["replica_id"].as_u64())
        {
            return id;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("group should have a leader before deletion");
}

async fn surviving_leader(server: &ServerClient, id: u64) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let topology = server.topology().await.unwrap();
        let group = topology
            .iter()
            .find(|store| store.store_id == 7)
            .and_then(|store| store.groups.iter().find(|group| group.group_id == 70))
            .unwrap();
        assert_eq!(group.local_replica_id, id);
        if group.local_replica.role == "leader" {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "surviving role: {}",
            group.local_replica.role
        );
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn assert_authoritative_voters(ips: &[String]) {
    let context = crowdb_console_shared::ops::OpContext::new(
        format!("{}:7001", ips[0]),
        ips.iter().map(|ip| format!("http://{ip}:7000")).collect(),
        crowdb_console_shared::ConsoleConfig::default(),
    );
    let authoritative = context.sysmd().list_replicas_in_group(7, 70).await.unwrap();
    assert_eq!(authoritative.len(), 2);
    assert!(
        authoritative.iter().all(|replica| replica.voting),
        "{authoritative:?}"
    );
    let membership = context.membership().read(7, 70).await.unwrap().unwrap();
    let artifacts = std::path::PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap());
    let mounts: Value =
        serde_json::from_slice(&std::fs::read(artifacts.join("mounts.json")).unwrap()).unwrap();
    for data in &mounts["host_data"].as_array().unwrap()[..ips.len()] {
        let path = std::path::PathBuf::from(data.as_str().unwrap()).join("kv/conf/node-config.json");
        let config: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let store = config["stores"]
            .as_array()
            .unwrap()
            .iter()
            .find(|store| store["store_id"] == 7)
            .unwrap();
        let group = store["groups"]
            .as_array()
            .unwrap()
            .iter()
            .find(|group| group["group_id"] == 70)
            .unwrap();
        assert_eq!(group["membership_epoch"], membership.record().epoch);
    }
}

#[tokio::test]
#[ignore = "requires two owned release KV nodes and writable Web"]
async fn replica_add_remove_wires_peers_bidirectionally() {
    let (http, base, ips) = setup(2).await;
    request(
        &http,
        &base,
        Method::POST,
        "/api/stores",
        json!({"store_id":7,"nodes":[1]}),
        201,
    )
    .await;
    request(
        &http,
        &base,
        Method::POST,
        "/api/stores/7/groups",
        json!({"group_id":70,"replica_id":1,"nodes":[1]}),
        201,
    )
    .await;
    let initial = replicas(&http, &base).await;
    assert_eq!(initial.as_array().unwrap().len(), 1);
    assert_eq!(initial[0]["replica_id"], 1);
    assert_eq!(initial[0]["node_id"], 1);
    let path = "/api/stores/7/groups/70/replicas";
    request(
        &http,
        &base,
        Method::POST,
        path,
        json!({"node_id":2,"replica_id":2}),
        201,
    )
    .await;
    let list = replicas(&http, &base).await;
    assert_eq!(list.as_array().unwrap().len(), 2);
    assert_authoritative_voters(&ips).await;
    for node in [1, 2] {
        assert!(list
            .as_array()
            .unwrap()
            .iter()
            .any(|replica| replica["node_id"] == node));
    }
    let servers: Vec<_> = ips
        .iter()
        .map(|ip| ServerClient::new(format!("http://{ip}:7000")).unwrap())
        .collect();
    for (index, remote) in [(0, 2), (1, 1)] {
        let remotes = servers[index].list_remote_replicas(7, 70).await.unwrap();
        assert!(
            remotes.iter().any(|replica| replica.replica_id == remote),
            "{remotes:?}"
        );
    }
    let detail = request(&http, &base, Method::GET, &format!("{path}/2"), Value::Null, 200).await;
    assert_eq!(detail["replica_id"], 2);
    assert_eq!(detail["node_id"], 2);
    assert!(
        matches!(detail["role"].as_str(), Some("leader" | "follower")),
        "{detail}"
    );
    let removed = leader(&http, &base).await;
    request(
        &http,
        &base,
        Method::DELETE,
        &format!("{path}/999"),
        Value::Null,
        404,
    )
    .await;
    request(
        &http,
        &base,
        Method::DELETE,
        &format!("{path}/{removed}"),
        Value::Null,
        204,
    )
    .await;
    let remaining = replicas(&http, &base).await;
    assert_eq!(remaining.as_array().unwrap().len(), 1);
    let id = remaining[0]["replica_id"].as_u64().unwrap();
    let node = remaining[0]["node_id"].as_u64().unwrap();
    let server = &servers[usize::try_from(node - 1).unwrap()];
    let remotes = server.list_remote_replicas(7, 70).await.unwrap();
    assert!(
        !remotes.iter().any(|replica| replica.replica_id == removed),
        "{remotes:?}"
    );
    surviving_leader(server, id).await;
}
