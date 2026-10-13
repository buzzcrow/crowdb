// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_kv_client::{ClientConfig, CrowdbKvClient, CrowdbSysmdClient};
use crowdb_monitor::{
    verify_chunk_services, BootstrapSession, DeploymentProfile, LogicalBootstrap, MonitorLog,
};
use std::path::PathBuf;

fn root() -> PathBuf {
    PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap())
}

fn seed() -> String {
    format!("http://{}:10000", crate::common::management::addresses(1)[0])
}

#[tokio::test]
#[ignore = "requires the complete release storage container"]
async fn chunk_services_validate_live_registration_and_logical_topology() {
    let root = root();
    let profile = DeploymentProfile::load(root.join("profile.toml")).unwrap();
    let mut config_profile = profile.clone();
    config_profile.paths.run_root = root.join("runtime-config");
    verify_chunk_services(&seed(), &config_profile).await.unwrap();
    for (file, from, to, expected) in [
        (
            "chunkdb.toml",
            "instance_id = \"1\"",
            "instance_id = \"2\"",
            "ChunkDB registration conflicts",
        ),
        (
            "chunk-kv.toml",
            "instance_id = 1",
            "instance_id = 2",
            "Chunk-KV registration conflicts",
        ),
    ] {
        let path = config_profile.paths.run_root.join("config").join(file);
        let original = std::fs::read_to_string(&path).unwrap();
        let changed = original.replace(from, to);
        assert_ne!(original, changed, "identity field must be changed");
        std::fs::write(&path, changed).unwrap();
        let error = verify_chunk_services(&seed(), &config_profile).await.unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
        std::fs::write(path, original).unwrap();
    }
    verify_chunk_services(&seed(), &config_profile).await.unwrap();
    verify_logical_topology(&root, &profile).await;
    super::witness_with_write(true, std::env::var("CROWDB_E2E_PHASE").as_deref() == Ok("write")).await;
}

async fn verify_logical_topology(root: &std::path::Path, profile: &DeploymentProfile) {
    let sysmd = CrowdbSysmdClient::new(CrowdbKvClient::new(ClientConfig::new(vec![seed()])));
    assert_eq!(sysmd.list_stores().await.unwrap().len(), 1);
    assert_eq!(sysmd.list_groups_in_store(0).await.unwrap().len(), 2);
    assert_eq!(sysmd.list_replicas_in_group(0, 0).await.unwrap().len(), 1);
    assert_eq!(sysmd.list_replicas_in_group(0, 1).await.unwrap().len(), 1);
    let data = root.join("client-data/logical-bootstrap");
    std::fs::create_dir_all(&data).unwrap();
    let mut session = BootstrapSession::open(&data, b"profile", b"config", &["logical-topology"]).unwrap();
    std::fs::create_dir_all(data.join("log")).unwrap();
    let mut events = MonitorLog::open(&data.join("log"), profile.logs.clone())
        .await
        .unwrap();
    let bootstrap = LogicalBootstrap::new(seed());
    bootstrap
        .reconcile(&mut session, profile, &mut events)
        .await
        .unwrap();
    sysmd.add_group(0, 99).await.unwrap();
    assert!(bootstrap
        .reconcile(&mut session, profile, &mut events)
        .await
        .is_err());
    assert_eq!(sysmd.list_groups_in_store(0).await.unwrap().len(), 3);
    sysmd.remove_group(0, 99).await.unwrap();
}

#[tokio::test]
#[ignore = "requires the complete release Iceberg container"]
async fn real_iceberg_catalog_and_listener_survive_restart() {
    let credentials = std::fs::read_to_string(root().join("client.env")).unwrap();
    let token = credentials
        .lines()
        .find_map(|line| line.strip_prefix("ICEBERG_TOKEN="))
        .unwrap();
    let origin = format!("http://{}:9092", crate::common::management::addresses(1)[0]);
    let client = reqwest::Client::new();
    let config = client
        .get(format!("{origin}/v1/config"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    assert_eq!(config.status(), 200);
    let value: serde_json::Value = config.json().await.unwrap();
    assert!(value["defaults"].is_object());
    if std::env::var("CROWDB_E2E_PHASE").as_deref() == Ok("write") {
        let response = client
            .post(format!("{origin}/v1/namespaces"))
            .bearer_auth(token)
            .json(&serde_json::json!({"namespace":["deployment-restart"]}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
    }
    let response = client
        .get(format!("{origin}/v1/namespaces/deployment-restart"))
        .bearer_auth(token)
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let namespace: serde_json::Value = response.json().await.unwrap();
    assert_eq!(namespace["namespace"], serde_json::json!(["deployment-restart"]));
    super::witness_with_write(true, std::env::var("CROWDB_E2E_PHASE").as_deref() == Ok("write")).await;
}
