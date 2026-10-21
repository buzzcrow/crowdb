// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use reqwest::{Client, StatusCode};
use serde_json::{json, Value};
use std::path::PathBuf;
use std::time::Duration;

async fn post(http: &Client, base: &str, path: &str, body: Value, status: StatusCode) -> Value {
    let response = http
        .post(format!("{base}{path}"))
        .json(&body)
        .send()
        .await
        .unwrap();
    let actual = response.status();
    let text = response.text().await.unwrap();
    assert_eq!(actual, status, "{path}: {text}");
    serde_json::from_str(&text).unwrap()
}

#[tokio::test]
#[ignore = "requires owned release Web, KV and Chunkdb services"]
async fn deploys_real_chunkdb_with_fixed_cluster_seeds_and_retained_launch() {
    let http = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    let addresses = crate::common::management::addresses(4);
    let base = format!("http://{}:9090", addresses[3]);
    prepare_storage(&http, &base).await;
    let deployed = post(
        &http,
        &base,
        "/api/nodes/1/services/deploy",
        json!({"kind":"chunkdb","instance_id":"901","http_port":12100,"rpc_port":12200}),
        StatusCode::CREATED,
    )
    .await;
    assert!(deployed["pid"].as_u64().unwrap() > 1);
    let launch = crate::common::control::node("export-chunkdb-launch", 3).await;
    assert_eq!(launch["pid"], deployed["pid"]);
    let root = PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap());
    let config = std::fs::read_to_string(root.join("chunkdb-launch.toml")).unwrap();
    for address in &addresses[..3] {
        assert!(config.contains(&format!("http://{address}:7000")), "{config}");
    }
    let response = http
        .delete(format!("{base}/api/services/chunkdb-901"))
        .send()
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "{}",
        response.text().await.unwrap()
    );
    let retained = crate::common::control::node("inspect-retained-chunkdb-launch", 3).await;
    assert_eq!(retained["alive"], false);
    assert_eq!(retained["workdir_exists"], true);
}

async fn prepare_storage(http: &Client, base: &str) {
    let addresses = crate::common::management::addresses(4);
    let cluster = super::fixture::TestKvCluster::from_addresses(addresses[..3].to_vec()).await;
    let kv =
        crowdb_kv_client::CrowdbKvClient::new(crowdb_kv_client::ClientConfig::new(cluster.mgmt_endpoints));
    kv.seed_leader(0, 0, cluster.group0_leader_endpoint);
    let kv = std::sync::Arc::new(kv);
    let sysmd = crowdb_kv_client::CrowdbSysmdClient::from_shared(kv.clone());
    sysmd.add_store(0, &[1, 2, 3]).await.unwrap();
    sysmd.add_group(0, 0).await.unwrap();
    sysmd.add_group(0, 1).await.unwrap();
    register_voters(http, &sysmd, &addresses[..3]).await;
    let hardware = crowdb_kv_client::HardwareClient::from_shared(kv);
    hardware
        .add_rack(
            1,
            &crowdb_protocol::common::RackValue {
                status: crowdb_protocol::common::HwStatus::Up as i32,
                node_ids: vec![1],
                name: "test rack".into(),
            },
        )
        .await
        .unwrap();
    hardware
        .add_node(
            1,
            1,
            &crowdb_protocol::common::NodeValue {
                status: crowdb_protocol::common::HwStatus::Up as i32,
                management_host: "127.0.0.1".into(),
                ..Default::default()
            },
        )
        .await
        .unwrap();
    post(
        http,
        base,
        "/api/nodes/1/disk-groups",
        json!({"id":1,"name":"owned storage","store_id":0,"group_id":1}),
        StatusCode::CREATED,
    )
    .await;
    post(http, base, "/api/nodes/1/disk-groups/1/disks",
        json!({"disk_id":"0000000000000000-0000000000000901","disk_type":"Hdd","capacity_bytes":16_777_216,"zone_size_bytes":8_388_608,"unit_size_bytes":4096}),
        StatusCode::CREATED).await;
    post(http, base, "/api/nodes/1/services/deploy",
        json!({"kind":"diskio","instance_id":"902","rpc_port":7200,"disk_group_id":1,"local_deployment":true}),
        StatusCode::CREATED).await;
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        let readiness: Value = http
            .get(format!("{base}/api/chunk-storage-readiness"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        if readiness["ready"] == true {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "{readiness}");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

async fn register_voters(http: &Client, sysmd: &crowdb_kv_client::CrowdbSysmdClient, addresses: &[String]) {
    for (index, address) in addresses.iter().enumerate() {
        let topology: Value = http
            .get(format!("http://{address}:7000/topology"))
            .send()
            .await
            .unwrap()
            .error_for_status()
            .unwrap()
            .json()
            .await
            .unwrap();
        let store = &topology["stores"][0];
        assert_eq!(store["store_id"], 0);
        for group in store["groups"].as_array().unwrap() {
            assert_eq!(group["remotes"].as_array().unwrap().len(), 2);
            assert_eq!(group["local_replica"]["voting"], true);
            sysmd
                .add_replica(&crowdb_protocol::common::ReplicaValue {
                    store_id: 0,
                    group_id: group["group_id"].as_u64().unwrap(),
                    replica_id: group["local_replica_id"].as_u64().unwrap(),
                    node_id: index as u64 + 1,
                    role: group["local_replica"]["role"].as_str().unwrap().into(),
                    voting: true,
                    endpoint: format!(
                        "{address}:{}",
                        store["listen_addr"].as_str().unwrap().rsplit(':').next().unwrap()
                    ),
                })
                .await
                .unwrap();
        }
    }
}
