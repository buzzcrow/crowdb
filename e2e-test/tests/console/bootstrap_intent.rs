// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::fixture::client_data_dir;
use super::fixture::TestKvCluster as KvCluster;
use crowdb_console_shared::bootstrap_intent::BootstrapIntent;
use crowdb_console_shared::config::ConsoleConfig;
use crowdb_console_shared::error::Error;
use crowdb_console_shared::ops::{cluster as cluster_ops, OpContext};

use super::bootstrap_authority;

#[tokio::test]
#[ignore = "requires the owned release KV containers"]
async fn interrupted_bootstrap_restores_identity_then_clears_verified_intent() {
    let cluster = KvCluster::start().await;
    let source = bootstrap_authority::context(&cluster).await;
    let dir = client_data_dir("bootstrap-intent-resume");
    let path = dir.as_path().join("intent.toml");
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
#[ignore = "requires the owned release KV containers"]
async fn committed_group_zero_is_verified_before_stale_intent_is_removed() {
    let cluster = KvCluster::start().await;
    let source = bootstrap_authority::context(&cluster).await;
    let dir = client_data_dir("bootstrap-intent-committed");
    let path = dir.as_path().join("intent.toml");
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
#[ignore = "requires the owned release KV containers"]
async fn changed_bootstrap_topology_is_rejected_before_group_zero_mutation() {
    let cluster = KvCluster::start().await;
    let ctx = bootstrap_authority::context(&cluster).await;
    let dir = client_data_dir("bootstrap-intent-conflict");
    let path = dir.as_path().join("intent.toml");
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
