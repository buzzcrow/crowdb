// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/default_preparation.rs"]
mod common;
use common::TestPreparationAuthority;
use crowdb_console_shared::{deployment::preparation, ops::cluster::readiness::collect};
use crowdb_protocol::mgmt::{
    cluster::{ClusterPhase, DependencyKind, DependencyState},
    node::{deployment::NodeDeploymentPolicy, observation::NodeRuntimeObservation, NodeBinding},
};

fn local(authority: &TestPreparationAuthority) -> NodeRuntimeObservation {
    NodeRuntimeObservation {
        version: 1,
        discovery_id: uuid::Uuid::new_v4().to_string(),
        policy: NodeDeploymentPolicy::default(),
        binding: Some(NodeBinding {
            node_id: 2,
            bootstrap: authority.bootstrap.identity.clone(),
            management_seeds: Vec::new(),
        }),
        setup: None,
        observed_at_unix_ms: 0,
        services: Vec::new(),
        protocols: Vec::new(),
    }
}

#[tokio::test]
async fn live_group0_does_not_imply_prepared_storage_or_protocol_readiness_and_never_repairs() {
    let authority = TestPreparationAuthority::start(&[2]).await;
    let before = authority
        .ctx
        .kv()
        .scan_bounded(0, 0, b"", b"", b"", 4096, false, None)
        .await
        .unwrap()
        .items;
    let report = collect(&authority.ctx, local(&authority)).await.unwrap();
    assert_ne!(report.phase, ClusterPhase::Ready);
    assert!(report
        .observation
        .dependencies
        .iter()
        .any(|fact| fact.kind == DependencyKind::Authority && fact.state == DependencyState::Ready));
    assert!(report
        .observation
        .dependencies
        .iter()
        .any(|fact| fact.kind == DependencyKind::Storage && fact.state == DependencyState::Unknown));
    assert!(report.observation.runtime.is_none());
    assert_eq!(
        authority
            .ctx
            .kv()
            .scan_bounded(0, 0, b"", b"", b"", 4096, false, None)
            .await
            .unwrap()
            .items,
        before
    );
    assert!(preparation::read(&authority.ctx, &authority.bootstrap.identity)
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
async fn reserved_or_retired_preparation_cannot_be_reported_as_ready() {
    let authority = TestPreparationAuthority::start(&[2]).await;
    let accepted = preparation::reserve(&authority.ctx, &authority.bootstrap.identity)
        .await
        .unwrap();
    let report = collect(&authority.ctx, local(&authority)).await.unwrap();
    assert_ne!(report.phase, ClusterPhase::Ready);
    assert!(report
        .observation
        .dependencies
        .iter()
        .any(|fact| fact.id == "storage/4/2/1" && fact.state == DependencyState::Unknown));
    preparation::retire(
        &authority.ctx,
        &authority.bootstrap.identity,
        &accepted.operation_id,
    )
    .await
    .unwrap();
    let report = collect(&authority.ctx, local(&authority)).await.unwrap();
    assert_ne!(report.phase, ClusterPhase::Ready);
    assert!(report
        .observation
        .dependencies
        .iter()
        .any(|fact| fact.kind == DependencyKind::Storage && fact.state == DependencyState::Stopped));
}

#[tokio::test]
async fn a_different_bound_cluster_is_an_identity_failure() {
    let authority = TestPreparationAuthority::start(&[2]).await;
    let mut local = local(&authority);
    local.binding.as_mut().unwrap().bootstrap.cluster_id = uuid::Uuid::new_v4().to_string();
    let report = collect(&authority.ctx, local).await.unwrap();
    assert_eq!(report.phase, ClusterPhase::Failed);
    assert!(report
        .reasons
        .iter()
        .any(|reason| reason.code == "cluster-identity-conflict"));
}
