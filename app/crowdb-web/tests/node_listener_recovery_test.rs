// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/cluster_target.rs"]
mod cluster_target;
#[path = "common/memory_kv.rs"]
mod memory_kv;
#[path = "common/node_listener_recovery.rs"]
mod node_listener_recovery;
use cluster_target::TestClusterTarget;
use crowdb_console_shared::deployment::registry::{NodeRecord, NodeRegistry};
use crowdb_protocol::mgmt::{
    cluster::DependencyState,
    node::{NodeServiceAction, NodeServiceIntent},
};
use node_listener_recovery::{credentials, listener, observation, publish};
use std::sync::atomic::Ordering;

#[tokio::test]
async fn background_protocol_reads_follow_confirmed_addresses_without_ui_refresh_or_repair() {
    let fixture = TestClusterTarget::start().await;
    let sealed = std::fs::read(fixture.root.path().join("prepared.json")).unwrap();
    assert_eq!(fixture.prepared.identity, fixture.binding.bootstrap);
    credentials(&fixture.root.path().join("console"));
    let (address, calls, task) = listener().await;
    let discovery = uuid::Uuid::new_v4().to_string();
    let registry = NodeRegistry {
        cluster_id: fixture.binding.bootstrap.cluster_id.clone(),
        nodes: vec![NodeRecord {
            discovery_id: discovery.clone(),
            node_id: 2,
            physical_host_id: "retained-host".into(),
            rack_id: 1,
            host: "127.0.0.2".into(),
            ssh_port: 2222,
            ssh_user: "crowdb".into(),
            operation_id: uuid::Uuid::new_v4().to_string(),
            confirmed: true,
            host_key: None,
            cancelled: false,
        }],
    };
    let intent = NodeServiceIntent {
        cluster_id: registry.cluster_id.clone(),
        operation_id: uuid::Uuid::new_v4().to_string(),
        node_id: 2,
        service_id: "access-2".into(),
        kind: "access".into(),
        action: NodeServiceAction::Start,
        configuration: ["s3", "iceberg", "dataset", "health"]
            .map(|kind| format!("[{kind}]\nlisten = '{address}'\n"))
            .join(""),
        environment: std::collections::BTreeMap::new(),
    };
    fixture
        .kv
        .put(
            0,
            0,
            b"/deployment/services/2/access-2",
            &serde_json::to_vec(&intent).unwrap(),
            None,
        )
        .await
        .unwrap();
    publish(&fixture, &registry).await;
    let baseline = observation(&fixture, &discovery, &intent);
    let mut sampled = baseline.clone();
    crowdb_web::sample_node_services_for_tests(&fixture.state, &mut sampled).await;
    assert_eq!(sampled.protocols.len(), 3);
    assert!(
        sampled
            .protocols
            .iter()
            .all(|value| value.state == DependencyState::Ready),
        "{:?}",
        sampled.protocols
    );
    assert!(sampled.services[0].detail.contains("Access readiness HTTP 200"));
    assert_eq!(calls.load(Ordering::Relaxed), 4);
    assert_eq!(
        fixture.state.config.read().unwrap().node(2).unwrap().host,
        "127.0.0.1"
    );
    assert_eq!(
        std::fs::read(fixture.root.path().join("prepared.json")).unwrap(),
        sealed
    );
    for case in 0..5 {
        let mut changed = registry.clone();
        match case {
            0 => changed.nodes[0].confirmed = false,
            1 => changed.nodes[0].cancelled = true,
            2 => changed.nodes[0].discovery_id = uuid::Uuid::new_v4().to_string(),
            3 => changed.nodes[0].node_id = 3,
            _ => changed.cluster_id = uuid::Uuid::new_v4().to_string(),
        }
        publish(&fixture, &changed).await;
        let mut sampled = baseline.clone();
        crowdb_web::sample_node_services_for_tests(&fixture.state, &mut sampled).await;
        assert!(sampled
            .protocols
            .iter()
            .all(|value| value.state == DependencyState::Unknown));
        assert_eq!(
            calls.load(Ordering::Relaxed),
            4,
            "unconfirmed/foreign identity cannot authorize probes"
        );
    }
    task.abort();
}
