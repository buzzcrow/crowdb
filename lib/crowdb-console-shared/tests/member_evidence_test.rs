// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/member_evidence.rs"]
mod member_evidence;

use crowdb_console_shared::ops::cluster::readiness::member_evidence as project;
use crowdb_protocol::mgmt::{
    cluster::{DependencyKind, DependencyState},
    node::NodeServiceAction,
};
use member_evidence::TestMemberEvidence;

#[test]
fn each_required_service_and_protocol_has_its_own_current_fact() {
    let fixture = TestMemberEvidence::new();
    let facts = fixture.project();
    assert_eq!(facts.len(), 10);
    assert!(facts
        .iter()
        .all(|fact| fact.state == DependencyState::Ready && fact.operation_id == fact.current_operation_id));
    assert_eq!(
        facts
            .iter()
            .filter(|fact| fact.kind == DependencyKind::Service)
            .count(),
        6
    );
    assert_eq!(
        facts
            .iter()
            .filter(|fact| fact.kind == DependencyKind::Protocol)
            .count(),
        3
    );
}

#[test]
fn an_unreachable_member_and_a_pending_binding_cannot_be_replaced_by_other_health() {
    let mut fixture = TestMemberEvidence::new();
    let missing = project(
        &fixture.bootstrap,
        &fixture.expected,
        &fixture.policy,
        Some(&fixture.intents),
        None,
        1000,
    );
    assert_eq!(missing.len(), 1);
    assert_eq!(missing[0].kind, DependencyKind::Member);
    assert_eq!(missing[0].state, DependencyState::Unknown);
    fixture.node.binding = None;
    assert_eq!(fixture.project()[0].state, DependencyState::Initializing);
}

#[test]
fn a_foreign_uuid_discovery_identity_or_formation_policy_fails_closed() {
    let original = TestMemberEvidence::new();
    for variant in 0..3 {
        let mut fixture = TestMemberEvidence::new();
        match variant {
            0 => {
                fixture.node.binding.as_mut().unwrap().bootstrap.cluster_id =
                    original.bootstrap.identity.cluster_id.clone();
            }
            1 => fixture.node.discovery_id = original.expected.discovery_id.clone(),
            _ => fixture.node.policy.cluster = "foreign".into(),
        }
        let facts = fixture.project();
        assert_eq!(facts.len(), 1);
        assert_eq!(facts[0].kind, DependencyKind::Identity);
        assert_eq!(facts[0].state, DependencyState::Failed);
    }
}

#[test]
fn current_stop_wins_over_all_historical_ready_facts() {
    let mut fixture = TestMemberEvidence::new();
    let access = fixture
        .intents
        .iter_mut()
        .find(|intent| intent.kind == "access")
        .unwrap();
    access.action = NodeServiceAction::Stop;
    access.operation_id = uuid::Uuid::new_v4().to_string();
    let facts = fixture.project();
    let stopped: Vec<_> = facts
        .iter()
        .filter(|fact| fact.state == DependencyState::Stopped)
        .collect();
    assert_eq!(stopped.len(), 4);
    assert!(stopped
        .iter()
        .all(|fact| fact.operation_id == fact.current_operation_id));
}

#[test]
fn a_new_start_requires_new_execution_and_new_protocol_observations() {
    let mut fixture = TestMemberEvidence::new();
    fixture
        .intents
        .iter_mut()
        .find(|intent| intent.kind == "access")
        .unwrap()
        .operation_id = uuid::Uuid::new_v4().to_string();
    let facts = fixture.project();
    let superseded: Vec<_> = facts
        .iter()
        .filter(|fact| fact.operation_id != fact.current_operation_id)
        .collect();
    assert_eq!(superseded.len(), 4);
}

#[test]
fn missing_intent_and_missing_protocol_proofs_remain_unknown() {
    let mut fixture = TestMemberEvidence::new();
    fixture.intents.clear();
    assert_eq!(
        fixture
            .project()
            .iter()
            .filter(|fact| fact.state == DependencyState::Unknown)
            .count(),
        8
    );
    let mut fixture = TestMemberEvidence::new();
    fixture.node.protocols.clear();
    assert_eq!(
        fixture
            .project()
            .iter()
            .filter(|fact| fact.kind == DependencyKind::Protocol && fact.state == DependencyState::Unknown)
            .count(),
        3
    );
}

#[test]
fn duplicate_service_or_protocol_identity_is_a_conflict() {
    for protocol in [false, true] {
        let mut fixture = TestMemberEvidence::new();
        if protocol {
            fixture.node.protocols.push(fixture.node.protocols[0].clone());
        } else {
            fixture.node.services.push(fixture.node.services[0].clone());
        }
        assert_eq!(fixture.project()[0].state, DependencyState::Failed);
    }
}
