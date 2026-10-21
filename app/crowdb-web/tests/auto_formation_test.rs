// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/auto_formation.rs"]
mod auto_formation;

use auto_formation::{policy, TestFormationFixture};
use crowdb_protocol::mgmt::node::{deployment::DeploymentMode, CandidateState};

#[tokio::test]
async fn incomplete_three_node_scope_waits_without_admitting_partial_members() {
    for count in [0, 1] {
        let policy = policy(3, DeploymentMode::Local);
        let peers = vec![(policy.formation(), CandidateState::Unbound); count];
        let fixture = TestFormationFixture::start(policy, peers, 1).await;
        let report = fixture.observation().await;
        assert_eq!(report["phase"], "waiting", "{report}");
        assert_eq!(report["required_voters"], 3);
        assert_eq!(report["observed_voters"], count + 1);
        fixture.assert_no_admission();
    }
}

#[tokio::test]
async fn wrong_scope_count_or_mode_never_reaches_local_credentials() {
    let expected = policy(3, DeploymentMode::Local);
    for foreign in [
        policy(1, DeploymentMode::Local).formation(),
        policy(3, DeploymentMode::Production).formation(),
        crowdb_protocol::mgmt::node::deployment::FormationPolicy {
            cluster: "foreign".into(),
            ..expected.formation()
        },
    ] {
        let fixture =
            TestFormationFixture::start(expected.clone(), vec![(foreign, CandidateState::Unbound)], 1).await;
        let report = fixture.observation().await;
        assert_eq!(report["phase"], "waiting", "{report}");
        assert_eq!(report["observed_voters"], 1);
        fixture.assert_no_admission();
    }
}

#[tokio::test]
async fn overfull_fresh_scope_fails_before_admission_or_default_rack_creation() {
    let policy = policy(3, DeploymentMode::Local);
    let peers = vec![(policy.formation(), CandidateState::Unbound); 3];
    let fixture = TestFormationFixture::start(policy, peers, 1).await;
    let report = fixture.observation().await;
    assert_eq!(report["phase"], "failed");
    assert!(report["error"].as_str().unwrap().contains("Too many fresh nodes"));
    fixture.assert_no_admission();
}

#[tokio::test]
async fn only_uuid_ordered_coordinator_can_begin_fresh_admission() {
    let policy = policy(3, DeploymentMode::Local);
    let peers = vec![(policy.formation(), CandidateState::Unbound); 2];
    let fixture = TestFormationFixture::start(policy, peers, 99).await;
    let report = fixture.observation().await;
    assert_eq!(report["phase"], "waiting");
    assert_eq!(report["coordinator"], "00000000-0000-4000-8000-000000000002");
    fixture.assert_no_admission();
}

#[tokio::test]
async fn known_bound_scope_and_duplicate_identity_require_explicit_attention() {
    for state in [CandidateState::ForeignCluster, CandidateState::IdentityConflict] {
        let policy = policy(3, DeploymentMode::Local);
        let peers = vec![(policy.formation(), state)];
        let fixture = TestFormationFixture::start(policy, peers, 1).await;
        let report = fixture.observation().await;
        assert_eq!(report["phase"], "failed", "{report}");
        fixture.assert_no_admission();
    }
}

#[tokio::test]
async fn production_does_not_enable_local_convenience_admission() {
    let fixture = TestFormationFixture::start(policy(3, DeploymentMode::Production), vec![], 1).await;
    let report = fixture.observation().await;
    assert_eq!(report["phase"], "disabled");
    fixture.assert_no_admission();
}
