// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_console_shared::deployment::services::{current, publish_planned_start};
use crowdb_paxos_kv::cluster::{
    group::PxGroup, paxos_kv_server::PaxosKvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole,
};
use crowdb_paxos_kv_client::{ClientConfig, GetOutcome, PaxosKvClient, ReadMode};
use crowdb_protocol::mgmt::node::{NodeServiceAction, NodeServiceIntent};
use std::sync::Arc;

struct TestAuthority {
    store: Arc<PxKvStore>,
    client: PaxosKvClient,
}
impl TestAuthority {
    async fn start() -> Self {
        let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
        store.add_group(PxGroup::new(
            0,
            PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
        ));
        store.start().await.unwrap();
        let client = PaxosKvClient::new(ClientConfig::new(vec![]));
        client.seed_leader(0, 0, store.listen_addr().unwrap().to_string());
        Self { store, client }
    }
    async fn revision(&self) -> u64 {
        match self
            .client
            .get(
                0,
                0,
                b"/deployment/services/1/diskio-1",
                ReadMode::Linearizable,
                None,
            )
            .await
            .unwrap()
        {
            GetOutcome::Found { revision, .. } => revision,
            GetOutcome::NotFound => panic!("accepted service intent is missing"),
        }
    }
}
impl Drop for TestAuthority {
    fn drop(&mut self) {
        self.store.stop();
    }
}
fn intent() -> NodeServiceIntent {
    NodeServiceIntent {
        cluster_id: "cluster".into(),
        operation_id: uuid::Uuid::new_v4().to_string(),
        node_id: 1,
        service_id: "diskio-1".into(),
        kind: "diskio".into(),
        action: NodeServiceAction::Start,
        configuration: "instance_id = 1".into(),
        environment: std::collections::BTreeMap::new(),
    }
}

#[tokio::test]
async fn exact_planned_start_replay_keeps_its_committed_revision() {
    let authority = TestAuthority::start().await;
    let intent = intent();
    publish_planned_start(&authority.client, &intent).await.unwrap();
    let revision = authority.revision().await;
    publish_planned_start(&authority.client, &intent).await.unwrap();
    assert_eq!(authority.revision().await, revision);
    let mut changed = intent.clone();
    changed.configuration.push_str("\nnode_id = 2");
    assert!(publish_planned_start(&authority.client, &changed).await.is_err());
    assert_eq!(
        current(&authority.client, 1, "diskio-1").await.unwrap(),
        Some(intent)
    );
    assert_eq!(authority.revision().await, revision);
}

#[tokio::test]
async fn later_operator_commands_fence_delayed_plan_publication() {
    for action in [
        NodeServiceAction::Stop,
        NodeServiceAction::Restart,
        NodeServiceAction::Delete,
    ] {
        let authority = TestAuthority::start().await;
        let original = intent();
        publish_planned_start(&authority.client, &original).await.unwrap();
        let mut newer = original.clone();
        newer.action = action;
        newer.operation_id = uuid::Uuid::new_v4().to_string();
        authority
            .client
            .put(
                0,
                0,
                b"/deployment/services/1/diskio-1",
                &serde_json::to_vec(&newer).unwrap(),
                None,
            )
            .await
            .unwrap();
        let revision = authority.revision().await;
        assert!(publish_planned_start(&authority.client, &original).await.is_err());
        assert_eq!(
            current(&authority.client, 1, "diskio-1").await.unwrap(),
            Some(newer)
        );
        assert_eq!(authority.revision().await, revision);
    }
}

#[tokio::test]
async fn concurrent_plans_cannot_both_claim_the_first_service_start() {
    let authority = TestAuthority::start().await;
    let first = intent();
    let second = intent();
    let (a, b) = tokio::join!(
        publish_planned_start(&authority.client, &first),
        publish_planned_start(&authority.client, &second)
    );
    assert_ne!(a.is_ok(), b.is_ok(), "one fixed operation must win: {a:?}, {b:?}");
    assert_eq!(
        current(&authority.client, 1, "diskio-1").await.unwrap(),
        Some(if a.is_ok() { first } else { second })
    );
}

#[tokio::test]
async fn invalid_plan_identity_or_non_start_never_publishes_desired_state() {
    let authority = TestAuthority::start().await;
    let mut invalid = intent();
    invalid.operation_id = "not-an-operation".into();
    assert!(publish_planned_start(&authority.client, &invalid).await.is_err());
    invalid = intent();
    invalid.action = NodeServiceAction::Restart;
    assert!(publish_planned_start(&authority.client, &invalid).await.is_err());
    assert!(current(&authority.client, 1, "diskio-1").await.unwrap().is_none());
}
