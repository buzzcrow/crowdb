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
async fn production_requires_complete_formation_and_an_explicit_rack() {
    for (peer_count, phase) in [(0, "waiting"), (2, "needs-rack")] {
        let policy = policy(3, DeploymentMode::Production);
        let peers = vec![(policy.formation(), CandidateState::Unbound); peer_count];
        let fixture =
            TestFormationFixture::with_password(policy, peers, 1, Some((b"local-test-password", 0o600)))
                .await;
        let report = fixture.observation().await;
        assert_eq!(report["phase"], phase, "{report}");
        assert!(fixture.state.config.read().unwrap().racks.is_empty());
        fixture.assert_no_admission();
    }
}

#[tokio::test]
async fn unrelated_bound_clusters_do_not_block_fresh_scoped_formation() {
    for voters in [1, 3] {
        let expected = policy(voters, DeploymentMode::Local);
        let mut peers: Vec<_> = ["foreign-a", "foreign-b"]
            .map(|cluster| {
                let mut foreign = expected.formation();
                foreign.cluster = cluster.into();
                (foreign, CandidateState::ForeignCluster)
            })
            .into();
        peers.extend((1..voters).map(|_| (expected.formation(), CandidateState::Unbound)));
        let fixture = TestFormationFixture::start(expected, peers, 1).await;
        let report = fixture.observation().await;
        assert_eq!(report["phase"], "failed", "{report}");
        assert!(
            report["error"].as_str().unwrap().contains("credential"),
            "{report}"
        );
        assert!(!fixture.root.join("accepted-node.json").exists());
        assert!(fixture.state.config.read().unwrap().nodes.is_empty());
    }
}

#[tokio::test]
async fn missing_exposed_empty_or_corrupt_local_credentials_never_fall_back_to_a_password() {
    for password in [
        None,
        Some((b"never-emit-secret".as_slice(), 0o644)),
        Some((b"".as_slice(), 0o600)),
        Some(([0xff].as_slice(), 0o600)),
    ] {
        let fixture =
            TestFormationFixture::with_password(policy(1, DeploymentMode::Local), vec![], 1, password).await;
        let report = fixture.observation().await;
        assert_eq!(report["phase"], "failed", "{report}");
        assert!(!report.to_string().contains("never-emit-secret"));
        assert!(!fixture
            .root
            .join("console/admission-00000000-0000-4000-8000-000000000001.json")
            .exists());
        assert!(!fixture.root.join("accepted-node.json").exists());
    }
}

#[tokio::test]
async fn five_voter_scope_preserves_incomplete_overfull_and_foreign_member_fences() {
    let expected = policy(5, DeploymentMode::Local);
    for count in 0..4 {
        let peers = vec![(expected.formation(), CandidateState::Unbound); count];
        let fixture = TestFormationFixture::start(expected.clone(), peers, 1).await;
        let report = fixture.observation().await;
        assert_eq!(report["phase"], "waiting", "{report}");
        assert_eq!(report["required_voters"], 5);
        assert_eq!(report["observed_voters"], count + 1);
        fixture.assert_no_admission();
    }
    for state in [CandidateState::ForeignCluster, CandidateState::IdentityConflict] {
        let fixture =
            TestFormationFixture::start(expected.clone(), vec![(expected.formation(), state)], 1).await;
        let report = fixture.observation().await;
        assert_eq!(report["phase"], "failed", "{report}");
        fixture.assert_no_admission();
    }
    let fixture = TestFormationFixture::start(
        expected.clone(),
        vec![(expected.formation(), CandidateState::Unbound); 5],
        1,
    )
    .await;
    let report = fixture.observation().await;
    assert_eq!(report["phase"], "failed", "{report}");
    assert!(report["error"].as_str().unwrap().contains("Too many fresh nodes"));
    fixture.assert_no_admission();
    let fixture = TestFormationFixture::start(
        expected.clone(),
        vec![(expected.formation(), CandidateState::Unbound); 4],
        99,
    )
    .await;
    let report = fixture.observation().await;
    assert_eq!(report["phase"], "waiting", "{report}");
    assert_eq!(report["coordinator"], "00000000-0000-4000-8000-000000000002");
    fixture.assert_no_admission();
    let production = policy(5, DeploymentMode::Production);
    let fixture = TestFormationFixture::with_password(
        production.clone(),
        vec![(production.formation(), CandidateState::Unbound); 4],
        1,
        Some((b"local-test-password", 0o600)),
    )
    .await;
    let report = fixture.observation().await;
    assert_eq!(report["phase"], "needs-rack", "{report}");
    fixture.assert_no_admission();
}
