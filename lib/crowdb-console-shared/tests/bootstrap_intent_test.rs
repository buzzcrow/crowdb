// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::os::unix::fs::{symlink, PermissionsExt};

use crowdb_console_shared::bootstrap_intent::BootstrapIntent;
use crowdb_console_shared::config::{ConsoleConfig, NodeEntry, RackEntry, ServerEntry, ServiceType};
use crowdb_console_shared::error::Error;
use crowdb_console_shared::ops::{cluster as cluster_ops, OpContext};
use crowdb_test_harness::cluster::KvCluster;
use crowdb_test_harness::test_dirs::tempdir_in_test_data;

#[path = "common/bootstrap_authority.rs"]
mod bootstrap_authority;

fn config() -> ConsoleConfig {
    let mut config = ConsoleConfig::default();
    config.racks.push(RackEntry {
        id: 1,
        name: "rack-one".into(),
    });
    config.nodes.push(NodeEntry {
        id: 1,
        rack_id: 1,
        host: "127.0.0.1".into(),
        ssh_port: 22,
        ssh_user: "operator".into(),
        ssh_key: None,
        ssh_password: None,
        ssh_credential_ref: Some("ops-key".into()),
    });
    config.servers.push(ServerEntry {
        id: "kv-1".into(),
        url: "http://127.0.0.1:10000".into(),
        node_id: Some(1),
        rpc_url: Some("127.0.0.1:10100".into()),
        rest_port: Some(10000),
        rpc_port: Some(10100),
        auto_start: true,
        binary: Some("/private/bin/crowdb-kv-server".into()),
        election_profile: None,
        pid: Some(42),
        service_type: ServiceType::PaxosKv,
        rpc_workers: None,
        no_fsync: false,
    });
    config
}

#[test]
fn sealed_intent_is_private_immutable_and_restores_only_bootstrap_inputs() {
    let dir = tempdir_in_test_data("bootstrap-intent");
    let path = dir.path().join("intent.toml");
    let intent = BootstrapIntent::capture(&config(), &[1]).unwrap();
    intent.seal(&path).unwrap();
    intent.seal(&path).unwrap();
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o600
    );
    assert_eq!(BootstrapIntent::load(&path).unwrap(), intent);
    let body = std::fs::read_to_string(&path).unwrap();
    assert!(!body.contains("/private/bin"));
    assert!(!body.contains("pid"));
    let restored = intent.to_config();
    assert_eq!(restored.racks[0].name, "rack-one");
    assert_eq!(restored.nodes[0].ssh_credential_ref.as_deref(), Some("ops-key"));
    assert!(restored.servers[0].pid.is_none());
    assert!(restored.servers[0].binary.is_none());

    let mut changed = config();
    changed.racks[0].name = "other".into();
    let error = BootstrapIntent::capture(&changed, &[1])
        .unwrap()
        .seal(&path)
        .unwrap_err();
    assert!(matches!(error, Error::Conflict { .. }));
    assert_eq!(BootstrapIntent::load(&path).unwrap(), intent);
}

#[test]
fn intent_rejects_inline_secrets_symlinks_and_legacy_fields() {
    let mut config = config();
    config.nodes[0].ssh_key = Some("/private/id_ed25519".into());
    assert!(BootstrapIntent::capture(&config, &[1]).is_err());
    config.nodes[0].ssh_key = None;
    assert!(BootstrapIntent::capture(&config, &[1, 1]).is_err());

    let dir = tempdir_in_test_data("bootstrap-intent-invalid");
    let target = dir.path().join("target.toml");
    std::fs::write(&target, "version = 1\nlegacy = true\n").unwrap();
    let link = dir.path().join("intent.toml");
    symlink(&target, &link).unwrap();
    assert!(BootstrapIntent::load(&link).is_err());
    assert!(BootstrapIntent::capture(&config, &[1])
        .unwrap()
        .seal(&link)
        .is_err());
    assert!(BootstrapIntent::load(&target).is_err());

    let strict = dir.path().join("strict.toml");
    BootstrapIntent::capture(&config, &[1])
        .unwrap()
        .seal(&strict)
        .unwrap();
    let body = std::fs::read_to_string(&strict).unwrap();
    let changed = body.replace("name = \"rack-one\"", "name = \"rack-one\"\nlegacy = true");
    assert_ne!(changed, body);
    std::fs::write(&strict, changed).unwrap();
    assert!(BootstrapIntent::load(&strict).is_err());
}

#[tokio::test]
async fn interrupted_bootstrap_restores_identity_then_clears_verified_intent() {
    let cluster = KvCluster::start().await;
    let source = bootstrap_authority::context(&cluster).await;
    let dir = tempdir_in_test_data("bootstrap-intent-resume");
    let path = dir.path().join("intent.toml");
    BootstrapIntent::capture(&source.config(), &[1])
        .unwrap()
        .seal(&path)
        .unwrap();

    let resumed = OpContext::new(
        cluster.group0_leader_endpoint.clone(),
        cluster.mgmt_endpoints.clone(),
        ConsoleConfig::default(),
    );
    cluster_ops::init_with_intent(&resumed, &[1], &path)
        .await
        .unwrap();
    assert!(!path.exists());
    assert_eq!(
        resumed.sysmd().get_store(0).await.unwrap().unwrap().node_ids,
        vec![1]
    );
    assert_eq!(resumed.config().racks.len(), 1);
    assert_eq!(resumed.config().nodes.len(), 1);
}

#[tokio::test]
async fn committed_group_zero_is_verified_before_stale_intent_is_removed() {
    let cluster = KvCluster::start().await;
    let source = bootstrap_authority::context(&cluster).await;
    let dir = tempdir_in_test_data("bootstrap-intent-committed");
    let path = dir.path().join("intent.toml");
    BootstrapIntent::capture(&source.config(), &[1])
        .unwrap()
        .seal(&path)
        .unwrap();
    cluster_ops::init(&source, &[1]).await.unwrap();
    assert!(path.exists());

    let resumed = OpContext::new(
        cluster.group0_leader_endpoint.clone(),
        cluster.mgmt_endpoints.clone(),
        ConsoleConfig::default(),
    );
    cluster_ops::init_with_intent(&resumed, &[1], &path)
        .await
        .unwrap();
    assert!(!path.exists());
    assert_eq!(
        resumed.sysmd().list_replicas_in_group(0, 0).await.unwrap().len(),
        1
    );
}

#[tokio::test]
async fn changed_bootstrap_topology_is_rejected_before_group_zero_mutation() {
    let cluster = KvCluster::start().await;
    let ctx = bootstrap_authority::context(&cluster).await;
    let dir = tempdir_in_test_data("bootstrap-intent-conflict");
    let path = dir.path().join("intent.toml");
    BootstrapIntent::capture(&ctx.config(), &[1])
        .unwrap()
        .seal(&path)
        .unwrap();
    ctx.config_mut().racks[0].name = "changed".into();
    let result = cluster_ops::init_with_intent(&ctx, &[1], &path).await;
    assert!(matches!(result, Err(Error::Conflict { .. })), "{result:?}");
    assert!(ctx.sysmd().get_store(0).await.unwrap().is_none());
    assert!(path.exists());
}
