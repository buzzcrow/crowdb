// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_console_shared::deployment::recovery::MemberTrust;
use crowdb_monitor::{accept_member_trust_for_tests, check_member_trust_for_tests};
use crowdb_test_harness::test_dirs::tempdir_in_test_data;

#[path = "common/member_trust.rs"]
mod trust;

#[test]
fn duplicate_authenticated_incarnations_remove_the_route_in_any_order() {
    use crowdb_protocol::mgmt::node::recovery::MemberEndpoint;
    let first = MemberEndpoint {
        node_id: 1,
        discovery_id: uuid::Uuid::new_v4().to_string(),
        boot_id: uuid::Uuid::new_v4().to_string(),
        addresses: vec!["192.0.2.1".parse().unwrap()],
    };
    let mut cloned = first.clone();
    cloned.boot_id = uuid::Uuid::new_v4().to_string();
    cloned.addresses = vec!["192.0.2.2".parse().unwrap()];
    for records in [vec![first.clone(), cloned.clone()], vec![cloned, first.clone()]] {
        let (members, conflicts) = crowdb_monitor::unique_member_endpoints_for_tests(records);
        assert!(members.is_empty());
        assert_eq!(conflicts, vec![first.discovery_id.clone()]);
    }
    let (members, conflicts) =
        crowdb_monitor::unique_member_endpoints_for_tests(vec![first.clone(), first.clone()]);
    assert_eq!(members, vec![first]);
    assert!(conflicts.is_empty());
}

#[test]
fn bootstrap_trust_requires_every_previously_authenticated_key() {
    let dir = tempdir_in_test_data("monitor-member-trust");
    let root = dir.path();
    let accepted = trust::fixture(root);
    let mut changed = accepted.clone();
    changed.members[1].host_key = trust::key(4);
    assert!(accept_member_trust_for_tests(root, &changed).is_err());
    assert!(!root.join("member-trust.json").exists());
    accept_member_trust_for_tests(root, &accepted).unwrap();
    check_member_trust_for_tests(root).unwrap();
    accept_member_trust_for_tests(root, &accepted).unwrap();
    assert_eq!(
        MemberTrust::load(&root.join("member-trust.json")).unwrap(),
        accepted
    );
    let original = std::fs::read(root.join("member-trust.json")).unwrap();
    std::fs::write(root.join("ssh/known_hosts"), "").unwrap();
    assert!(accept_member_trust_for_tests(root, &accepted).is_err());
    assert_eq!(std::fs::read(root.join("member-trust.json")).unwrap(), original);
}

#[test]
fn restart_requires_retained_trust_and_the_same_local_host_key() {
    let dir = tempdir_in_test_data("monitor-member-restart");
    let root = dir.path();
    let accepted = trust::fixture(root);
    assert!(check_member_trust_for_tests(root).is_err());
    accept_member_trust_for_tests(root, &accepted).unwrap();
    check_member_trust_for_tests(root).unwrap();
    std::fs::write(root.join("ssh/ssh_host_ed25519_key.pub"), trust::key(4)).unwrap();
    assert!(check_member_trust_for_tests(root).is_err());
}

#[test]
fn self_identity_and_retirement_cannot_be_overridden_by_a_coordinator() {
    let dir = tempdir_in_test_data("monitor-member-self");
    let root = dir.path();
    let accepted = trust::fixture(root);
    let mut changed = accepted.clone();
    changed.members[0].host_key = trust::key(4);
    assert!(accept_member_trust_for_tests(root, &changed).is_err());
    trust::write(
        &root.join(format!(
            "retired-bootstrap-{}.json",
            accepted.bootstrap.operation_id
        )),
        &true,
    );
    assert!(accept_member_trust_for_tests(root, &accepted).is_err());
    assert!(!root.join("member-trust.json").exists());
}

#[test]
fn missing_preparation_and_corrupt_trust_fail_closed() {
    let dir = tempdir_in_test_data("monitor-member-corrupt");
    let root = dir.path();
    let accepted = trust::fixture(root);
    trust::write(
        &root.join("accepted-node.json"),
        &serde_json::json!({"node_id":1,"bootstrap":accepted.bootstrap,"prepared":false}),
    );
    assert!(accept_member_trust_for_tests(root, &accepted).is_err());
    trust::write(
        &root.join("accepted-node.json"),
        &serde_json::json!({"node_id":1,"bootstrap":accepted.bootstrap,"prepared":true}),
    );
    trust::write(&root.join("member-trust.json"), &"corrupt");
    assert!(accept_member_trust_for_tests(root, &accepted).is_err());
    assert_eq!(
        std::fs::read_to_string(root.join("member-trust.json")).unwrap(),
        "\"corrupt\""
    );
}

#[tokio::test]
async fn incomplete_endpoint_proof_does_not_rebind_unchanged_fresh_services() {
    let dir = tempdir_in_test_data("monitor-partial-discovery-contacts");
    let root = dir.path();
    let _ = trust::fixture(root);
    let operation =
        crowdb_console_shared::deployment::PreparedBootstrap::load(&root.join("prepared-bootstrap.json"))
            .unwrap();
    let seeds = ["http://192.0.2.1:10000".to_owned()];
    let binding = crowdb_protocol::mgmt::node::NodeBinding {
        node_id: 1,
        bootstrap: operation.identity.clone(),
        management_seeds: (1..=3).map(|n| format!("http://192.0.2.{n}:10000")).collect(),
    };
    trust::write(&root.join("node-binding.json"), &binding);
    let original = std::fs::read(root.join("node-binding.json")).unwrap();
    // Even with no reachable Group-0 endpoint, unchanged addresses and a
    // partial proof must leave accepted contacts and service operations alone.
    assert!(!crowdb_monitor::rebind_recovery_services_for_tests(
        root,
        &operation,
        1,
        "192.0.2.1",
        &seeds,
        false,
    )
    .await
    .unwrap());
    assert_eq!(std::fs::read(root.join("node-binding.json")).unwrap(), original);
    assert!(!root.join("service-address.json").exists());
    assert!(!root.join("services").exists());
}

#[tokio::test]
async fn ssh_identity_command_is_independent_of_the_lifecycle_control_loop() {
    use crowdb_protocol::mgmt::node::recovery::{MemberEndpoint, MemberRecoveryIdentity};
    let dir = tempdir_in_test_data("member-identity");
    let root = dir.path();
    let trust = trust::fixture(root);
    accept_member_trust_for_tests(root, &trust).unwrap();
    trust::write(
        &root.join("node-endpoint.json"),
        &MemberEndpoint {
            discovery_id: trust.members[0].discovery_id.clone(),
            node_id: 0,
            boot_id: uuid::Uuid::new_v4().to_string(),
            addresses: vec!["192.0.2.1".parse().unwrap()],
        },
    );
    assert!(
        crowdb_monitor::read_recovery_identity(root).await.is_err(),
        "an offline root cannot prove live identity"
    );
    let liveness = crowdb_monitor::LivenessServer::start(root).unwrap();
    let unavailable_control = root.join("unavailable-control.sock");
    let mut command = tokio::process::Command::new(env!("CARGO_BIN_EXE_crowdb-monitor"));
    command
        .kill_on_drop(true)
        .arg("control")
        .arg("--socket")
        .arg(&unavailable_control)
        .arg("--data-root")
        .arg(root)
        .args(["--json", r#"{"command":"recovery_identity"}"#]);
    let output = tokio::time::timeout(std::time::Duration::from_secs(3), command.output())
        .await
        .expect("identity cannot wait for a busy lifecycle executor")
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let reply: MemberRecoveryIdentity = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(reply.binding.bootstrap, trust.bootstrap);
    assert_eq!(reply.binding.node_id, 1);
    assert_eq!(reply.endpoint.discovery_id, trust.members[0].discovery_id);
    drop(liveness);
    assert!(crowdb_monitor::read_recovery_identity(root).await.is_err());
}
