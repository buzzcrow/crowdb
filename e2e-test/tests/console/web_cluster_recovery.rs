// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use reqwest::{Client, Method};
use serde_json::{json, Value};
use std::{path::PathBuf, time::Duration};

#[tokio::test]
#[ignore = "fixture setup requires its three release voters"]
async fn prepare() {
    super::fixture::TestKvCluster::start().await;
    super::super::chunkdb::voting::register_actual_voters().await;
    let kv = std::sync::Arc::new(crate::common::hardware::kv_client());
    let sysmd = crowdb_paxos_kv_client::CrowdbSysmdClient::from_shared(kv.clone());
    sysmd.add_store(0, &[1, 2, 3]).await.unwrap();
    for group in [0, 1] {
        sysmd.add_group(0, group).await.unwrap();
    }
    let hardware = crowdb_paxos_kv_client::HardwareClient::from_shared(kv);
    hardware
        .add_rack(
            1,
            &crowdb_protocol::common::RackValue {
                status: crowdb_protocol::common::HwStatus::Up as i32,
                name: "authority-rack".into(),
                node_ids: vec![1, 2, 3],
            },
        )
        .await
        .unwrap();
    for node in 1..=3 {
        hardware
            .add_node(
                1,
                node,
                &crowdb_protocol::common::NodeValue {
                    status: crowdb_protocol::common::HwStatus::Up as i32,
                    management_host: "127.0.0.1".into(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
    }
}

async fn call(http: &Client, method: Method, path: &str, body: Value) -> Value {
    let addresses = crate::common::management::addresses(4);
    let response = http
        .request(method, format!("http://{}:9090{path}", addresses[3]))
        .json(&body)
        .send()
        .await
        .unwrap();
    let status = response.status();
    let text = response.text().await.unwrap();
    assert!(status.is_success(), "{path}: {status}: {text}");
    serde_json::from_str(&text).unwrap()
}

#[tokio::test]
#[ignore = "requires owned standalone Web and three independent release KV voters"]
async fn standalone_recovers_three_member_group0_and_diskdb() {
    let http = Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();
    let deployed = call(
        &http,
        Method::POST,
        "/api/nodes/1/diskdb/deploy",
        json!({"rpc_port":7102, "http_port":7101}),
    )
    .await;
    assert!(deployed["pid"].as_u64().unwrap() > 1);
    crate::common::control::node("export-recovery-config", 3).await;
    let path = PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap()).join("recovery-config.json");
    let mut saved: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    saved["rack"][0]["name"] = json!("stale-cache");
    std::fs::write(&path, serde_json::to_vec(&saved).unwrap()).unwrap();
    crate::common::control::node("install-recovery-cache", 3).await;
    crate::common::control::node("kill", 3).await;
    for node in 0..3 {
        crate::common::control::node("stop", node).await;
    }
    for node in 0..3 {
        crate::common::control::node("start", node).await;
    }
    let addresses = crate::common::management::addresses(4);
    crate::common::management::wait_for_leader(&http, &format!("http://{}:7000", addresses[0])).await;
    crate::common::control::node("start", 3).await;
    assert_eq!(
        call(&http, Method::GET, "/api/racks", Value::Null).await[0]["name"],
        "authority-rack"
    );
    assert_eq!(
        call(
            &http,
            Method::GET,
            "/api/stores/0/groups/0?recursive=2",
            Value::Null
        )
        .await["replicas"]
            .as_array()
            .unwrap()
            .len(),
        3
    );
    assert_eq!(
        call(&http, Method::GET, "/api/servers", Value::Null)
            .await
            .as_array()
            .unwrap()
            .len(),
        4
    );
    crate::common::control::node("export-recovery-config", 3).await;
    let restored: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    assert!(
        !restored["local_launches"]["diskdb-1"].is_null(),
        "Diskdb launch inputs must survive Web restart"
    );
}
