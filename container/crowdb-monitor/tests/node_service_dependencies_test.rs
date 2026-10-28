// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "../src/node/durable.rs"]
mod durable;

use crowdb_monitor::{DeploymentProfile, NodeServiceForTests};
use crowdb_protocol::mgmt::{
    node::{NodeBinding, NodeServiceAction, NodeServiceIntent},
    SystemBootstrapIdentity,
};
use std::{collections::BTreeMap, path::Path};

#[tokio::test]
async fn a_local_chunk_kv_listener_cannot_admit_access_before_catalog_authority_is_available() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("access-dependency-gate");
    let mut profile =
        DeploymentProfile::load(Path::new(env!("CARGO_MANIFEST_DIR")).join("../oci-image/node-profile.toml"))
            .unwrap();
    profile.paths.data_root = root.path().to_path_buf();
    profile
        .services
        .iter_mut()
        .find(|service| service.id == "access")
        .unwrap()
        .dependencies = vec!["chunk-kv".into()];
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let router = axum::Router::new().route("/ready", axum::routing::get(|| async { "ok" }));
    let server = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let binding = NodeBinding {
        node_id: 1,
        bootstrap: SystemBootstrapIdentity {
            cluster_id: uuid::Uuid::new_v4().to_string(),
            operation_id: uuid::Uuid::new_v4().to_string(),
            configuration_digest: "digest".into(),
        },
        management_seeds: vec!["http://127.0.0.1:1".into()],
    };
    durable::write(&root.path().join("node-binding.json"), &binding).unwrap();
    let binding: NodeBinding = durable::read(&root.path().join("node-binding.json"))
        .unwrap()
        .unwrap();
    let local = NodeServiceIntent {
        cluster_id: binding.bootstrap.cluster_id.clone(),
        operation_id: uuid::Uuid::new_v4().to_string(),
        node_id: 1,
        service_id: "chunk-kv-1".into(),
        kind: "chunk-kv".into(),
        action: NodeServiceAction::Start,
        configuration: format!("http_listen_addr = \"{address}\"\n"),
        environment: BTreeMap::new(),
    };
    let access = NodeServiceIntent {
        kind: "access".into(),
        service_id: "access-1".into(),
        configuration: String::new(),
        ..local.clone()
    };
    let readiness = NodeServiceForTests::dependencies_ready_for_tests(&profile, &access, &[local]).await;
    server.abort();
    assert!(
        readiness.is_err(),
        "a local idle listener does not prove routed catalog reads can succeed"
    );
    assert!(!root.path().join("services/access-1/execution.json").exists());
}

#[path = "common/node_setup.rs"]
mod node_setup;
#[path = "common/service_dependency.rs"]
mod service_dependency;

#[tokio::test]
async fn healthy_diskdb_listener_must_publish_the_registered_disks_ownership_before_consumers_start() {
    use crowdb_paxos_kv_client::ServiceRegistryClient;
    use crowdb_protocol::mgmt::node::setup::{NodeSetupChoices, NodeSetupRequest};
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("diskdb-route-gate");
    let request = NodeSetupRequest {
        version: 1,
        discovery_id: uuid::Uuid::new_v4().to_string(),
        operation_id: uuid::Uuid::new_v4().to_string(),
        choices: NodeSetupChoices::default(),
    };
    let authority = node_setup::TestAuthority::start(root.path(), &request).await;
    assert_eq!(authority.registry.nodes[0].node_id, 2);
    authority.kv.refresh_topology().await.unwrap();
    let kv = std::sync::Arc::new(crowdb_paxos_kv_client::PaxosKvClient::new(
        crowdb_paxos_kv_client::ClientConfig::new(authority.binding.management_seeds.clone()),
    ));
    service_dependency::publish_online_disk(kv.clone()).await;
    let (address, server) = service_dependency::ready_listener().await;
    let mut profile =
        DeploymentProfile::load(Path::new(env!("CARGO_MANIFEST_DIR")).join("../oci-image/node-profile.toml"))
            .unwrap();
    profile.paths.data_root = root.path().to_path_buf();
    profile.paths.log_root = root.path().join("log");
    profile
        .services
        .iter_mut()
        .find(|service| service.id == "chunkdb")
        .unwrap()
        .dependencies = vec!["diskdb".into()];
    let local = NodeServiceIntent {
        cluster_id: authority.binding.bootstrap.cluster_id.clone(),
        operation_id: uuid::Uuid::new_v4().to_string(),
        node_id: 2,
        service_id: "diskdb-2".into(),
        kind: "diskdb".into(),
        action: NodeServiceAction::Start,
        configuration: format!("[server]\nhttp_listen_addr = \"{address}\"\n"),
        environment: BTreeMap::new(),
    };
    let consumer = NodeServiceIntent {
        kind: "chunkdb".into(),
        service_id: "chunkdb-2".into(),
        configuration: String::new(),
        ..local.clone()
    };
    let registry = ServiceRegistryClient::from_shared(kv.clone());
    registry
        .register_diskdb(2, "127.0.0.1:7102", &[], &[])
        .await
        .unwrap();
    assert!(
        NodeServiceForTests::dependencies_ready_for_tests(&profile, &consumer, std::slice::from_ref(&local))
            .await
            .is_err(),
        "a ready listener with no published owner cannot route an allocation"
    );
    registry
        .heartbeat_diskdb(2, "127.0.0.1:7102", &[99], &[])
        .await
        .unwrap();
    assert!(NodeServiceForTests::dependencies_ready_for_tests(
        &profile,
        &consumer,
        std::slice::from_ref(&local)
    )
    .await
    .is_err());
    registry
        .heartbeat_diskdb(2, "127.0.0.1:7102", &[7], &[])
        .await
        .unwrap();
    assert!(
        NodeServiceForTests::dependencies_ready_for_tests(&profile, &consumer, std::slice::from_ref(&local))
            .await
            .is_err(),
        "an owned group without its registered disk loaded cannot allocate"
    );
    service_dependency::assert_initial_start_waits(&kv, &profile, &local, &consumer).await;
    registry
        .heartbeat_diskdb(
            2,
            "127.0.0.1:7102",
            &[7],
            &[crowdb_protocol::common::DiskGroupUsageSummary {
                disk_group_id: 7,
                usage_known: true,
                disk_count: 1,
                ..Default::default()
            }],
        )
        .await
        .unwrap();
    NodeServiceForTests::dependencies_ready_for_tests(&profile, &consumer, &[local])
        .await
        .unwrap();
    server.abort();
    assert!(!root.path().join("services/chunkdb-2/execution.json").exists());
}
