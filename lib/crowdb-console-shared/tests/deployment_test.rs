// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_console_shared::{
    deployment::{
        registry::{self, NodeRecord, NodeRegistry},
        PreparedBootstrap,
    },
    ConsoleConfig,
};
use crowdb_test_harness::{cluster::KvCluster, test_dirs::tempdir_in_test_data};
use serde_json::json;

fn config() -> ConsoleConfig {
    serde_json::from_value(json!({
        "rack": [{"id": 1, "name": "original"}],
        "node": [{"id": 1, "rack_id": 1, "host": "127.0.0.1", "ssh_user": "crowdb", "ssh_credential_ref": "id_ed25519"},
                 {"id": 2, "rack_id": 1, "host": "127.0.0.2"}],
        "server": [{"id": "kv-1", "node_id": 1, "url": "http://127.0.0.1:10000", "rpc_url": "127.0.0.1:10100"},
                   {"id": "kv-2", "node_id": 2, "url": "http://127.0.0.2:10000", "rpc_url": "127.0.0.2:10100"}]
    })).unwrap()
}

#[test]
fn bootstrap_retries_preserve_submitted_inputs_and_ignore_unused_drafts() {
    let directory = tempdir_in_test_data("deployment-sealed");
    let path = directory.path().join("operation.json");
    let mut draft = config();
    let operation = PreparedBootstrap::open(&path, &draft, &[1]).unwrap();
    assert_eq!(operation.intent.to_config().nodes.len(), 1);
    draft.racks[0].name = "edited".into();
    assert_eq!(PreparedBootstrap::open(&path, &draft, &[1]).unwrap(), operation);
    assert!(PreparedBootstrap::open(&path, &draft, &[2]).is_err());
    let mut invalid: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    invalid["identity"]["configuration_digest"] = json!("0".repeat(64));
    std::fs::write(&path, serde_json::to_vec(&invalid).unwrap()).unwrap();
    assert!(PreparedBootstrap::load(&path).is_err());
}

#[test]
fn bootstrap_never_persists_initial_passwords() {
    let directory = tempdir_in_test_data("deployment-private");
    let path = directory.path().join("operation.json");
    let mut draft = config();
    draft.nodes[0].ssh_password = Some("must-not-persist".into());
    assert!(PreparedBootstrap::open(&path, &draft, &[1]).is_err());
    assert!(!path.exists());
}

#[tokio::test]
async fn concurrent_admissions_allocate_unique_stable_ids_through_group_zero() {
    let cluster = KvCluster::start().await;
    let kv = crowdb_kv_client::CrowdbKvClient::new(crowdb_kv_client::ClientConfig::new(
        cluster.mgmt_endpoints.clone(),
    ));
    kv.seed_leader(0, 0, cluster.group0_leader_endpoint.clone());
    registry::publish(
        &kv,
        &NodeRegistry {
            cluster_id: "cluster-one".into(),
            nodes: Vec::new(),
        },
    )
    .await
    .unwrap();
    let node = |uuid: &str, host: &str| NodeRecord {
        discovery_id: uuid.into(),
        node_id: 0,
        physical_host_id: "same-host".into(),
        rack_id: 1,
        host: host.into(),
        ssh_port: 2222,
        ssh_user: "crowdb".into(),
        operation_id: uuid.into(),
        confirmed: false,
        cancelled: false,
    };
    let a = node("12345678-1234-4234-8234-123456789abc", "127.0.0.1");
    let b = node("12345678-1234-4234-8234-123456789def", "127.0.0.2");
    let (a, b) = tokio::join!(
        registry::admit(&kv, "cluster-one", a),
        registry::admit(&kv, "cluster-one", b)
    );
    let a = a.unwrap();
    let b = b.unwrap();
    assert_ne!(a.node_id, b.node_id);
    assert_eq!(registry::admit(&kv, "cluster-one", a.clone()).await.unwrap(), a);
    let mut conflicting = a.clone();
    conflicting.host = "127.0.0.3".into();
    assert!(registry::admit(&kv, "cluster-one", conflicting).await.is_err());
    assert!(registry::admit(&kv, "other-cluster", a.clone()).await.is_err());
    registry::cancel(&kv, "cluster-one", &a.discovery_id, &a.operation_id)
        .await
        .unwrap();
    registry::cancel(&kv, "cluster-one", &a.discovery_id, &a.operation_id)
        .await
        .unwrap();
    assert!(registry::admit(&kv, "cluster-one", a.clone()).await.is_err());
    let mut replacement = a.clone();
    replacement.operation_id = uuid::Uuid::new_v4().to_string();
    assert!(registry::admit(&kv, "cluster-one", replacement.clone())
        .await
        .is_err());
    registry::complete_cancellation(&kv, "cluster-one", &a)
        .await
        .unwrap();
    let replacement = registry::admit(&kv, "cluster-one", replacement).await.unwrap();
    assert_eq!(replacement.node_id, a.node_id);
    let mut confirmed = replacement.clone();
    confirmed.confirmed = true;
    registry::admit(&kv, "cluster-one", confirmed).await.unwrap();
    assert!(registry::cancel(
        &kv,
        "cluster-one",
        &replacement.discovery_id,
        &replacement.operation_id
    )
    .await
    .is_err());
    let registry = registry::read(&kv).await.unwrap().unwrap().0;
    assert_eq!(registry.nodes.len(), 2);
    assert!(registry
        .nodes
        .iter()
        .all(|node| node.physical_host_id == "same-host"));
    let initial = NodeRegistry {
        cluster_id: registry.cluster_id.clone(),
        nodes: vec![registry.nodes.iter().find(|node| node.confirmed).unwrap().clone()],
    };
    registry::publish(&kv, &initial).await.unwrap();
    assert_eq!(registry::read(&kv).await.unwrap().unwrap().0, registry);
    let mut conflicting = initial;
    conflicting.nodes[0].node_id += 1;
    assert!(registry::publish(&kv, &conflicting).await.is_err());
}

#[tokio::test]
async fn service_retries_retain_identity_and_other_nodes_cannot_claim_the_same_service() {
    use crowdb_console_shared::deployment::services;
    use crowdb_protocol::mgmt::node::{NodeServiceAction, NodeServiceIntent};

    let cluster = KvCluster::start().await;
    let kv = crowdb_kv_client::CrowdbKvClient::new(crowdb_kv_client::ClientConfig::new(
        cluster.mgmt_endpoints.clone(),
    ));
    kv.seed_leader(0, 0, cluster.group0_leader_endpoint.clone());
    let mut node = config().nodes.remove(0);
    node.ssh_key = Some("/nonexistent/crowdb-test-key".into());
    let mut intent = NodeServiceIntent {
        cluster_id: "cluster-one".into(),
        operation_id: uuid::Uuid::new_v4().to_string(),
        node_id: node.id,
        service_id: "diskio-71".into(),
        kind: "diskio".into(),
        action: NodeServiceAction::Start,
        configuration: "fixed-config".into(),
        environment: std::collections::BTreeMap::new(),
    };
    // The unreachable executor leaves a committed intent for monitor recovery.
    assert!(services::execute(&kv, &node, intent.clone()).await.is_err());
    assert_eq!(
        services::current(&kv, node.id, &intent.service_id).await.unwrap(),
        Some(intent.clone())
    );
    let original = intent.clone();
    intent.operation_id = uuid::Uuid::new_v4().to_string();
    assert!(services::execute(&kv, &node, intent.clone()).await.is_err());
    assert_eq!(
        services::current(&kv, node.id, &intent.service_id).await.unwrap(),
        Some(original)
    );
    node.id += 1;
    intent.node_id = node.id;
    assert!(matches!(
        services::execute(&kv, &node, intent.clone()).await,
        Err(crowdb_console_shared::error::Error::Conflict { .. })
    ));
    assert!(services::current(&kv, node.id, &intent.service_id)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn authenticated_node_update_preserves_ids_and_converges_from_another_console() {
    use crowdb_console_shared::{
        deployment::node_update,
        ops::{hardware, OpContext},
    };
    let cluster = KvCluster::start().await;
    let ctx = OpContext::new(
        cluster.group0_leader_endpoint.clone(),
        cluster.mgmt_endpoints.clone(),
        ConsoleConfig::default(),
    );
    hardware::add_rack_to_group0(&ctx, 1, "source").await.unwrap();
    hardware::add_rack_to_group0(&ctx, 2, "destination")
        .await
        .unwrap();
    let source = NodeRecord {
        discovery_id: uuid::Uuid::new_v4().to_string(),
        node_id: 7,
        physical_host_id: "same-host".into(),
        rack_id: 1,
        host: "127.0.0.1".into(),
        ssh_port: 2222,
        ssh_user: "crowdb".into(),
        operation_id: uuid::Uuid::new_v4().to_string(),
        confirmed: true,
        cancelled: false,
    };
    let mut node = config().nodes.remove(0);
    node.id = 7;
    hardware::add_node_to_group0(&ctx, node).await.unwrap();
    registry::publish(
        ctx.kv(),
        &NodeRegistry {
            cluster_id: "cluster-one".into(),
            nodes: vec![source.clone()],
        },
    )
    .await
    .unwrap();
    let mut target = source.clone();
    target.host = "127.0.0.2".into();
    target.rack_id = 2;
    let updated = node_update::apply(&ctx, "cluster-one", target.clone())
        .await
        .unwrap();
    assert_eq!(updated, target);
    assert_eq!(updated.node_id, source.node_id);
    assert_eq!(updated.physical_host_id, source.physical_host_id);
    assert!(ctx.sysmd().get_node(1, 7).await.unwrap().is_none());
    assert!(!ctx
        .sysmd()
        .get_rack(1)
        .await
        .unwrap()
        .unwrap()
        .node_ids
        .contains(&7));
    assert!(ctx
        .sysmd()
        .get_rack(2)
        .await
        .unwrap()
        .unwrap()
        .node_ids
        .contains(&7));
    assert_eq!(
        ctx.sysmd().get_node(2, 7).await.unwrap().unwrap().management_host,
        target.host
    );
    let other = OpContext::new(
        cluster.group0_leader_endpoint.clone(),
        cluster.mgmt_endpoints.clone(),
        ConsoleConfig::default(),
    );
    assert_eq!(
        node_update::apply(&other, "cluster-one", target.clone())
            .await
            .unwrap(),
        target
    );
    let mut conflict = target.clone();
    conflict.node_id += 1;
    assert!(node_update::apply(&other, "cluster-one", conflict).await.is_err());
    let mut conflict = target.clone();
    conflict.physical_host_id = "another-host".into();
    assert!(node_update::apply(&other, "cluster-one", conflict).await.is_err());
    assert_eq!(
        registry::read(other.kv()).await.unwrap().unwrap().0.nodes,
        vec![target]
    );
}
