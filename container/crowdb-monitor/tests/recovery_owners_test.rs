// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/node_setup.rs"]
mod node_setup;

use crowdb_console_shared::{ops::OpContext, ConsoleConfig};
use crowdb_paxos_kv_client::{ClientConfig, PaxosKvClient};
use crowdb_protocol::{
    common::ReplicaValue,
    kv_membership::GroupMember,
    mgmt::node::{
        recovery::RecoveryGroups,
        setup::{NodeSetupChoices, NodeSetupRequest},
    },
};
use std::sync::Arc;

#[tokio::test]
async fn installing_membership_authorizes_catchup_routes_before_replica_promotion() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("joining-recovery-routes");
    let request = NodeSetupRequest {
        version: 1,
        discovery_id: uuid::Uuid::new_v4().to_string(),
        operation_id: uuid::Uuid::new_v4().to_string(),
        choices: NodeSetupChoices::default(),
    };
    let authority = node_setup::TestAuthority::start(root.path(), &request).await;
    authority.kv.refresh_topology().await.unwrap();
    assert_eq!(authority.registry.nodes[0].node_id, 2);
    let kv = Arc::new(PaxosKvClient::new(ClientConfig::new(
        authority.binding.management_seeds.clone(),
    )));
    let ctx = OpContext::with_shared_client_preserving_hint(
        kv,
        &authority.binding.management_seeds,
        ConsoleConfig::default(),
    );
    let original = GroupMember {
        node_id: 2,
        replica_id: 1,
        endpoint: "127.0.0.1:10100".into(),
        voting: true,
    };
    ctx.sysmd()
        .add_replica(&ReplicaValue {
            store_id: 0,
            group_id: 0,
            node_id: original.node_id,
            replica_id: original.replica_id,
            voting: true,
            endpoint: original.endpoint.clone(),
            ..Default::default()
        })
        .await
        .unwrap();
    let initial = ctx
        .membership()
        .create(0, 0, vec![original.clone()])
        .await
        .unwrap();
    ctx.membership().complete(&initial).await.unwrap();
    let joining = GroupMember {
        node_id: 4,
        replica_id: 601,
        endpoint: "127.0.0.1:10200".into(),
        voting: true,
    };
    let installing = ctx
        .membership()
        .begin_change(0, 0, 1, vec![original.clone(), joining])
        .await
        .unwrap();
    assert_eq!(ctx.sysmd().list_all_replicas().await.unwrap().len(), 1);
    let snapshot = captured(root.path(), &ctx, &authority.binding.bootstrap, 4).await;
    assert_eq!(
        snapshot.groups.len(),
        1,
        "the joining replica needs recovery routes before catch-up can finish"
    );
    assert_eq!(
        snapshot.groups[0]
            .members
            .iter()
            .map(|member| (member.node_id, member.replica_id))
            .collect::<Vec<_>>(),
        vec![(2, 1), (4, 601)]
    );
    let snapshot = captured(root.path(), &ctx, &authority.binding.bootstrap, 2).await;
    assert!(
        snapshot.groups[0]
            .members
            .iter()
            .any(|member| member.replica_id == 601),
        "existing voters must be able to send catch-up records to the learner"
    );

    ctx.membership().complete(&installing).await.unwrap();
    let removing = ctx
        .membership()
        .begin_change(0, 0, 2, vec![original])
        .await
        .unwrap();
    let snapshot = captured(root.path(), &ctx, &authority.binding.bootstrap, 4).await;
    assert_eq!(
        snapshot.groups.len(),
        1,
        "retiring members remain known until installation completes"
    );
    ctx.membership().complete(&removing).await.unwrap();
    let snapshot = captured(root.path(), &ctx, &authority.binding.bootstrap, 4).await;
    assert!(
        snapshot.groups.is_empty(),
        "completed removal revokes recovery membership"
    );
}

async fn captured(
    root: &std::path::Path,
    ctx: &OpContext,
    bootstrap: &crowdb_protocol::mgmt::SystemBootstrapIdentity,
    node: u64,
) -> RecoveryGroups {
    crowdb_monitor::capture_recovery_owners_for_tests(root, ctx, bootstrap, node)
        .await
        .unwrap();
    serde_json::from_slice(&std::fs::read(root.join("recovery-groups.json")).unwrap()).unwrap()
}
