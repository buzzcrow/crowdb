// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::{path::Path, time::Duration};

use crowdb_console_shared::{
    deployment::{registry::NodeRecord, PreparedBootstrap},
    ConsoleConfig,
};
use crowdb_monitor::{DeploymentProfile, InitializationForTests, NodeIdentity, ServerCredentials};
use crowdb_protocol::mgmt::node::{NodeControl, NodeServiceCredentials};
use serde_json::json;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

#[path = "common/member_trust.rs"]
#[allow(dead_code)]
mod member_trust;
#[path = "common/run_root.rs"]
mod run_root;

fn fixture(root: &Path) -> (DeploymentProfile, PreparedBootstrap, NodeServiceCredentials) {
    let mut profile =
        DeploymentProfile::load(Path::new(env!("CARGO_MANIFEST_DIR")).join("../oci-image/node-profile.toml"))
            .unwrap();
    profile.paths.data_root = root.to_path_buf();
    profile.paths.run_root = root.join("run");
    std::fs::create_dir(&profile.paths.run_root).unwrap();
    let identity = NodeIdentity::load_or_create(root).unwrap();
    let config: ConsoleConfig = serde_json::from_value(json!({
        "rack": [{"id": 1, "name": "rack"}],
        "node": [{"id": 1, "rack_id": 1, "host": "127.0.0.1", "ssh_user": "crowdb"}],
        "server": [{"id": "kv-1", "node_id": 1, "url": "http://127.0.0.1:10000", "rpc_url": "127.0.0.1:10100"}]
    })).unwrap();
    let node = NodeRecord {
        web_port: 9090,
        discovery_id: identity.uuid().to_string(),
        node_id: 1,
        rack_id: 1,
        physical_host_id: "test-host".into(),
        host: "127.0.0.1".into(),
        ssh_port: 22,
        ssh_user: "crowdb".into(),
        operation_id: uuid::Uuid::new_v4().to_string(),
        confirmed: false,
        host_key: None,
        cancelled: false,
    };
    let operation =
        PreparedBootstrap::open_with_nodes(&root.join("submitted.json"), &config, &[1], vec![node]).unwrap();
    let credentials = NodeServiceCredentials {
        environment: ServerCredentials::load_or_create(root).unwrap().server_env(),
    };
    (profile, operation, credentials)
}

#[tokio::test]
async fn accepted_initialization_survives_connection_lifetime_and_resumes_fixed_intent() {
    let directory = run_root::TestRunRoot::new("init");
    let root = directory.path();
    let (profile, operation, credentials) = fixture(root);
    let listener = tokio::net::UnixListener::bind(profile.paths.run_root.join("node-control.sock")).unwrap();
    let manifest = serde_json::to_value(&operation).unwrap();
    let mut first = InitializationForTests::default();
    let status = first
        .accept_for_tests(&profile, manifest.clone(), credentials.clone(), Some(false))
        .unwrap();
    assert_eq!(status["phase"], "preparing");
    assert_eq!(status["operation_id"], operation.identity.operation_id);
    let intent = std::fs::read(root.join("initialization.json")).unwrap();
    assert!(first
        .accept_for_tests(&profile, manifest.clone(), credentials.clone(), Some(true))
        .is_err());
    let (mut connection, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let mut bytes = Vec::new();
    connection.read_to_end(&mut bytes).await.unwrap();
    assert!(matches!(
        serde_json::from_slice::<NodeControl>(&bytes).unwrap(),
        NodeControl::PrepareBootstrap { node_id: 1, .. }
    ));
    first.stop_for_tests().await;
    drop(connection);
    let mut resumed = InitializationForTests::default();
    resumed.resume_for_tests(&profile).unwrap();
    let (mut connection, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let mut bytes = Vec::new();
    connection.read_to_end(&mut bytes).await.unwrap();
    connection
        .write_all(br#"{"ok":false,"error":"test service unavailable"}"#)
        .await
        .unwrap();
    connection.shutdown().await.unwrap();
    drop(connection);
    let failed = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let status = InitializationForTests::status_for_tests(root).unwrap();
            if status["phase"] == "failed" {
                break status;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(failed["error"], "test service unavailable");
    assert_eq!(std::fs::read(root.join("initialization.json")).unwrap(), intent);
    resumed.stop_for_tests().await;
    let mut retry = InitializationForTests::default();
    assert_eq!(
        retry
            .accept_for_tests(&profile, manifest, credentials, None)
            .unwrap()["phase"],
        "preparing"
    );
    retry.stop_for_tests().await;
}

#[tokio::test]
async fn altered_manifest_is_rejected_before_accepting_an_initialization() {
    let directory = crowdb_test_harness::test_dirs::tempdir_in_test_data("monitor-invalid-initialization");
    let root = directory.path();
    let (profile, operation, credentials) = fixture(root);
    let mut manifest = serde_json::to_value(operation).unwrap();
    manifest["nodes"][0]["node_id"] = json!(2);
    let mut runner = InitializationForTests::default();
    assert!(runner
        .accept_for_tests(&profile, manifest, credentials, None)
        .is_err());
    assert!(!root.join("initialization.json").exists());
    assert!(!root.join("accepted-node.json").exists());
    assert_eq!(
        InitializationForTests::status_for_tests(root).unwrap()["phase"],
        "idle"
    );
}

#[tokio::test]
async fn bootstrap_waits_for_member_trust_before_starting_a_replica() {
    let directory = run_root::TestRunRoot::new("init-pins");
    let root = directory.path();
    let (profile, operation, credentials) = fixture(root);
    std::fs::create_dir(root.join("ssh")).unwrap();
    std::fs::write(root.join("ssh/ssh_host_ed25519_key.pub"), member_trust::key(1)).unwrap();
    let listener = tokio::net::UnixListener::bind(profile.paths.run_root.join("node-control.sock")).unwrap();
    let mut runner = InitializationForTests::default();
    runner
        .accept_for_tests(
            &profile,
            serde_json::to_value(&operation).unwrap(),
            credentials,
            Some(false),
        )
        .unwrap();
    for stage in 0..3 {
        let (mut connection, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let mut bytes = Vec::new();
        connection.read_to_end(&mut bytes).await.unwrap();
        let request: NodeControl = serde_json::from_slice(&bytes).unwrap();
        match (stage, request) {
            (0, NodeControl::PrepareBootstrap { node_id: 1, .. })
            | (2, NodeControl::StartKv { node_id: 1, .. }) => {}
            (1, NodeControl::AcceptMemberTrust { trust }) => {
                let trust: crowdb_console_shared::deployment::recovery::MemberTrust =
                    serde_json::from_value(trust).unwrap();
                trust.validate_bootstrap(&operation).unwrap();
                assert_eq!(trust.members[0].host_key, member_trust::key(1));
            }
            _ => panic!("unexpected bootstrap request at stage {stage}"),
        }
        let reply = if stage == 2 {
            br#"{"ok":false,"error":"test stops before replica execution"}"#.as_slice()
        } else {
            br#"{"ok":true,"value":{}}"#.as_slice()
        };
        connection.write_all(reply).await.unwrap();
        connection.shutdown().await.unwrap();
    }
    runner.stop_for_tests().await;
}
