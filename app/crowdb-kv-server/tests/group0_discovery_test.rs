// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

mod common;

use std::time::Duration;

use crowdb_kv_client::{ClientConfig, CrowdbKvClient, ServiceRegistryClient};
use crowdb_test_harness::test_dirs::TestDir;
use serde_json::json;

use common::process::{start_test_server, start_test_server_at};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prebootstrap_registration_waits_for_election_before_admitting_writes() {
    let server = start_test_server(&["--node-id", "2", "--keepalive-interval", "1"])
        .await
        .unwrap();
    let http = reqwest::Client::new();
    let response = http
        .post(format!("{}/system/init", server.base_url()))
        .json(&json!({"replica_id": 2, "start_election": false}))
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 201);
    // Leave bootstrap paused across two registration ticks. No write may
    // enter the unwired group before its first election.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let topology: serde_json::Value = http
            .get(format!("{}/topology", server.base_url()))
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        let group = &topology["stores"][0]["groups"][0];
        assert_eq!(group["leader_id"], 0);
        assert_eq!(group["local_replica"]["role"], "follower");
        assert_eq!(group["local_replica"]["election"]["current_term"], 0);
        assert_eq!(group["inflight"]["total_enqueued"], 0);
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

async fn wait_registered(service: &ServiceRegistryClient, endpoint: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(instances) = service.read_all_kv_server_instances().await {
            let matches: Vec<_> = instances
                .iter()
                .filter(|(_, value)| {
                    value
                        .extra
                        .as_ref()
                        .and_then(|extra| extra.kv_server.as_ref())
                        .and_then(|identity| identity.node_id)
                        == Some(2)
                })
                .collect();
            if matches.len() == 1 && matches[0].1.rpc_endpoint == endpoint {
                return;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "nonmember registration unavailable"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prebootstrap_nonmember_discovers_group0_and_retains_hints_after_restart() {
    let leader = start_test_server(&[
        "--instance-id",
        "9001",
        "--node-id",
        "1",
        "--keepalive-interval",
        "1",
    ])
    .await
    .unwrap();
    let root = TestDir::new("nonmember-discovery").unwrap();
    let args = [
        "--node-id",
        "2",
        "--keepalive-interval",
        "1",
        "--management-advertise-addr",
        "127.0.0.2",
    ];
    let nonmember = start_test_server_at(root.path(), &args, &[0]).await.unwrap();
    let http = reqwest::Client::new();
    let init = http
        .post(format!("{}/system/init", leader.base_url()))
        .json(&json!({"replica_id": 1, "start_election": true}))
        .send()
        .await
        .unwrap();
    assert_eq!(init.status(), 201, "{}", init.text().await.unwrap());
    let service = ServiceRegistryClient::new(CrowdbKvClient::new(ClientConfig::new(vec![leader
        .base_url()
        .into()])));
    let endpoint = format!("{}/system/group0-discovery", nonmember.base_url());
    for invalid in [
        json!([]),
        json!(["127.0.0.1:1"]),
        json!(["http://user:password@localhost:1"]),
    ] {
        let response = http
            .post(&endpoint)
            .json(&json!({"management_seeds": invalid}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 400);
    }
    let request = json!({"management_seeds": [leader.base_url()]});
    for _ in 0..2 {
        let response = http.post(&endpoint).json(&request).send().await.unwrap();
        assert_eq!(response.status(), 200, "{}", response.text().await.unwrap());
    }
    let advertised = nonmember.base_url().replace("127.0.0.1", "127.0.0.2");
    wait_registered(&service, &advertised).await;
    let initial = service.read_all_kv_server_instances().await.unwrap();
    let original_id = initial
        .iter()
        .find(|(_, value)| value.rpc_endpoint == advertised)
        .unwrap()
        .0;
    let topology: serde_json::Value = http
        .get(format!("{}/topology", nonmember.base_url()))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(
        topology["stores"].as_array().unwrap().is_empty(),
        "discovery must not create membership"
    );
    drop(nonmember);
    // Model a missed unregister while Group 0 was unavailable during shutdown.
    service
        .register_kv_server(
            crowdb_protocol::common::KvServerIdentity {
                instance_id: original_id,
                node_id: Some(2),
            },
            "http://127.0.0.1:1",
            &[],
            &[],
            "ok",
            &root.path().to_string_lossy(),
        )
        .await
        .unwrap();
    let restarted = start_test_server_at(root.path(), &args, &[0]).await.unwrap();
    let advertised = restarted.base_url().replace("127.0.0.1", "127.0.0.2");
    wait_registered(&service, &advertised).await;
    let instances = service.read_all_kv_server_instances().await.unwrap();
    let restarted_id = instances
        .iter()
        .find(|(_, value)| value.rpc_endpoint == advertised)
        .unwrap()
        .0;
    assert_eq!(
        original_id, restarted_id,
        "restart must preserve registration identity"
    );
}

#[test]
fn persisted_identity_rejects_conflicting_configuration_and_corruption() {
    use crowdb_kv_server::background::identity::load_or_create;

    let root = TestDir::new("service-identity").unwrap();
    let first = load_or_create(root.path(), Some(42), Some(2)).unwrap();
    assert_eq!(first.instance_id, 42);
    assert_eq!(load_or_create(root.path(), None, Some(2)).unwrap(), first);
    assert!(load_or_create(root.path(), Some(43), Some(2)).is_err());
    assert!(load_or_create(root.path(), None, Some(3)).is_err());
    let path = root.path().join("service-identity.json");
    std::fs::write(&path, b"partial").unwrap();
    assert!(load_or_create(root.path(), None, Some(2)).is_err());
    assert_eq!(std::fs::read(path).unwrap(), b"partial");
}
