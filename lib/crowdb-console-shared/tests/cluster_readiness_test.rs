// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_console_shared::ops::cluster::readiness::reduce;
use crowdb_protocol::mgmt::cluster::{
    ClusterObservation, ClusterPhase, DependencyKind, DependencyObservation, DependencyState,
};

fn fact(kind: DependencyKind) -> DependencyObservation {
    DependencyObservation {
        id: format!("{kind:?}"),
        kind,
        detail: "Current test evidence".into(),
        required: true,
        state: DependencyState::Ready,
        observed_at_unix_ms: 1000,
        max_age_ms: 100,
        revision: Some(1),
        operation_id: None,
        current_operation_id: None,
        reason: None,
    }
}

fn observation() -> ClusterObservation {
    ClusterObservation {
        version: 1,
        cluster: "dev".into(),
        cluster_id: Some("27a1f711-7ea4-4ddc-8d4e-3146017fd8cb".into()),
        bound: true,
        initial_voters: 3,
        policy: None,
        observed_at_unix_ms: 1000,
        members: vec![],
        operation_id: None,
        work_accepted: true,
        setup_completed: true,
        runtime: None,
        dependencies: [
            DependencyKind::Authority,
            DependencyKind::Metadata,
            DependencyKind::Storage,
            DependencyKind::Member,
            DependencyKind::Service,
            DependencyKind::Protocol,
        ]
        .into_iter()
        .map(fact)
        .collect(),
    }
}

#[test]
fn complete_current_application_evidence_is_ready_without_local_runtime_facts() {
    let mut input = observation();
    let mut runtime = fact(DependencyKind::Runtime);
    runtime.state = DependencyState::Unknown;
    input.dependencies.push(runtime);
    let report = reduce(input, 1000);
    assert_eq!(report.phase, ClusterPhase::Ready);
    assert!(report.reasons.iter().any(|reason| reason.dependency == "Runtime"));
    let bytes = serde_json::to_vec(&report).unwrap();
    assert_eq!(
        serde_json::from_slice::<crowdb_protocol::mgmt::cluster::ClusterStatus>(&bytes).unwrap(),
        report
    );
}

#[test]
fn terminal_failure_precedes_quorum_loss_and_retains_every_dependency_reason() {
    let mut input = observation();
    input.dependencies[0].state = DependencyState::Unknown;
    input.dependencies[2].state = DependencyState::Failed;
    input.dependencies[4].state = DependencyState::Stopped;
    let report = reduce(input.clone(), 1000);
    assert_eq!(report.phase, ClusterPhase::Failed);
    for name in ["Authority", "Storage", "Service"] {
        assert!(report.reasons.iter().any(|reason| reason.dependency == name));
    }
    input.dependencies.reverse();
    assert_eq!(reduce(input, 1000), report);
}

#[test]
fn superseded_failure_cannot_poison_a_recovered_dependency() {
    let mut input = observation();
    let mut old = fact(DependencyKind::Storage);
    old.state = DependencyState::Failed;
    old.operation_id = Some("old-operation".into());
    old.current_operation_id = Some("new-operation".into());
    input.dependencies.push(old);
    input.dependencies[2].operation_id = Some("new-operation".into());
    input.dependencies[2].current_operation_id = Some("new-operation".into());
    let report = reduce(input.clone(), 1000);
    assert_eq!(report.phase, ClusterPhase::Ready);
    assert!(report
        .reasons
        .iter()
        .any(|reason| reason.code == "operation-superseded"));
    input.dependencies.reverse();
    assert_eq!(reduce(input, 1000), report);
}

#[test]
fn another_ready_instance_cannot_replace_missing_current_evidence() {
    let mut input = observation();
    let mut pending = fact(DependencyKind::Service);
    pending.id = "service/another-node".into();
    pending.operation_id = Some("old".into());
    pending.current_operation_id = Some("new".into());
    input.dependencies.push(pending);
    let report = reduce(input, 1000);
    assert_eq!(report.phase, ClusterPhase::Degraded);
    assert!(report
        .reasons
        .iter()
        .any(|reason| reason.dependency == "service/another-node"));
}

#[test]
fn bound_quorum_loss_is_unavailable_but_prebootstrap_missing_voters_are_waiting() {
    let mut input = observation();
    input.dependencies[0].state = DependencyState::Unknown;
    assert_eq!(reduce(input.clone(), 1000).phase, ClusterPhase::Unavailable);
    input.bound = false;
    input.cluster_id = None;
    input.setup_completed = false;
    input.work_accepted = false;
    input.dependencies[3].state = DependencyState::Waiting;
    assert_eq!(reduce(input.clone(), 1000).phase, ClusterPhase::Waiting);
    input.work_accepted = true;
    assert_eq!(reduce(input, 1000).phase, ClusterPhase::Initializing);
}

#[test]
fn intentionally_stopped_required_services_and_missing_members_are_degraded() {
    for kind in [DependencyKind::Service, DependencyKind::Member] {
        let mut input = observation();
        input
            .dependencies
            .iter_mut()
            .find(|fact| fact.kind == kind)
            .unwrap()
            .state = DependencyState::Stopped;
        assert_eq!(reduce(input, 1000).phase, ClusterPhase::Degraded);
    }
}

#[test]
fn stale_and_future_success_are_unknown_and_cannot_establish_readiness() {
    for timestamp in [0, 1001] {
        let mut input = observation();
        input.dependencies[4].observed_at_unix_ms = timestamp;
        let report = reduce(input, 1000);
        assert_eq!(report.phase, ClusterPhase::Degraded);
        assert!(report
            .reasons
            .iter()
            .any(|reason| reason.code == "observation-not-current"));
    }
}

#[test]
fn missing_required_categories_and_duplicate_dependency_identity_cannot_pass() {
    let mut input = observation();
    input.dependencies.pop();
    assert_eq!(reduce(input, 1000).phase, ClusterPhase::Degraded);
    let mut input = observation();
    input.dependencies.push(input.dependencies[0].clone());
    assert_eq!(reduce(input, 1000).phase, ClusterPhase::Failed);
}

#[test]
fn validated_identity_conflict_is_terminal_even_before_first_bootstrap() {
    let mut input = observation();
    input.bound = false;
    input.cluster_id = None;
    let mut conflict = fact(DependencyKind::Identity);
    conflict.required = false;
    conflict.state = DependencyState::Failed;
    input.dependencies.push(conflict);
    assert_eq!(reduce(input, 1000).phase, ClusterPhase::Failed);
}
