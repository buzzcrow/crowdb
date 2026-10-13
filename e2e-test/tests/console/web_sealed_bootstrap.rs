// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_console_shared::bootstrap_intent::BootstrapIntent;
use crowdb_console_shared::config::{NodeEntry, RackEntry, ServerEntry};
use crowdb_console_shared::ConsoleConfig;
use serde_json::{json, Value};
use std::path::PathBuf;

#[tokio::test]
#[ignore = "requires an uninitialized release KV container and a bare-metal Web container"]
async fn bare_metal_bootstrap_consumes_sealed_topology_without_local_topology() {
    let addresses = crate::common::management::addresses(2);
    let artifacts = PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap());
    let mut config = ConsoleConfig::default();
    config.racks.push(RackEntry {
        id: 1,
        name: "r1".into(),
    });
    config.nodes.push(NodeEntry {
        id: 1,
        rack_id: 1,
        host: addresses[0].clone(),
        ssh_port: 22,
        ssh_user: String::new(),
        ssh_key: None,
        ssh_password: None,
        ssh_credential_ref: None,
    });
    let mut server = ServerEntry::new("n1", format!("http://{}:7000", addresses[0]));
    server.node_id = Some(1);
    server.rpc_url = Some(format!("{}:7001", addresses[0]));
    config.add_server(server).unwrap();
    BootstrapIntent::capture(&config, &[1])
        .unwrap()
        .seal(&artifacts.join("bootstrap-source.toml"))
        .unwrap();
    let source = crate::common::control::node("upload-bootstrap-source", 1).await;
    let response = crate::common::management::client()
        .post(format!("http://{}:9090/api/cluster/init", addresses[1]))
        .bearer_auth("bare-metal-bootstrap-test-token-12345")
        .json(&json!({"nodes": [1], "bootstrap_file": source["path"]}))
        .send()
        .await
        .unwrap();
    let status = response.status();
    let text = response.text().await.unwrap();
    assert_eq!(status, 201, "{text}");
    let result: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(result["store_id"], 0);
    assert_eq!(result["group_id"], 0);
    let mounts: Value =
        serde_json::from_slice(&std::fs::read(artifacts.join("mounts.json")).unwrap()).unwrap();
    let root = PathBuf::from(mounts["host_data"][1].as_str().unwrap()).join("persistent/console");
    assert!(!root.join("bootstrap-intent.toml").exists());
    assert!(!root.join("console.toml").exists());
    assert!(!root.join("config.json").exists());
}
