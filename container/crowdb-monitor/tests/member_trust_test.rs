// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_console_shared::deployment::recovery::MemberTrust;
use crowdb_monitor::{accept_member_trust_for_tests, check_member_trust_for_tests};
use crowdb_test_harness::test_dirs::tempdir_in_test_data;

#[path = "common/member_trust.rs"]
mod trust;

#[path = "common/run_root.rs"]
mod run_root;
use run_root::TestRunRoot;

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
    let dir = TestRunRoot::new("member-identity");
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

#[path = "common/admitted_member.rs"]
mod admitted;

#[test]
fn confirmed_nonvoter_contacts_preserve_original_votes_and_require_exact_local_pin() {
    let dir = tempdir_in_test_data("nonvoter-retained-trust");
    let root = dir.path();
    let snapshot = admitted::fixture(root);
    check_member_trust_for_tests(root).unwrap();
    assert!(crowdb_monitor::member_trust_ready_for_tests(root).unwrap());
    let operation =
        crowdb_console_shared::deployment::PreparedBootstrap::load(&root.join("prepared-bootstrap.json"))
            .unwrap();
    let original = operation.intent.to_config();
    let contacts = snapshot.topology(&operation).unwrap();
    assert_eq!(operation.intent.members(), &[1, 2, 3]);
    assert_eq!(contacts.nodes.len(), 4);
    assert_eq!(contacts.stores, original.stores);
    assert_eq!(contacts.groups, original.groups);
    std::fs::write(root.join("ssh/ssh_host_ed25519_key.pub"), trust::key(5)).unwrap();
    assert!(check_member_trust_for_tests(root).is_err());
    assert!(crowdb_monitor::member_trust_ready_for_tests(root).is_err());
}

#[test]
fn pending_missing_corrupt_and_retired_nonvoter_trust_never_authorize_resume() {
    for change in ["pending", "missing", "corrupt", "foreign", "cancelled", "retired"] {
        let dir = tempdir_in_test_data("nonvoter-trust-fence");
        let root = dir.path();
        let mut snapshot = admitted::fixture(root);
        let path = root.join("confirmed-member-trust.json");
        match change {
            "pending" => {
                snapshot.registry.nodes[3].confirmed = false;
                snapshot.current = snapshot.initial.confirmed_registry(&snapshot.registry).unwrap();
                trust::write(&path, &snapshot);
            }
            "missing" => std::fs::remove_file(&path).unwrap(),
            "corrupt" => std::fs::write(&path, "corrupt").unwrap(),
            "foreign" => {
                snapshot.current.bootstrap.cluster_id = uuid::Uuid::new_v4().to_string();
                trust::write(&path, &snapshot);
            }
            "cancelled" => trust::write(
                &root.join(format!(
                    "ssh/cancelled-{}.json",
                    snapshot.registry.nodes[3].operation_id
                )),
                &true,
            ),
            "retired" => trust::write(
                &root.join(format!(
                    "retired-bootstrap-{}.json",
                    snapshot.initial.bootstrap.operation_id
                )),
                &true,
            ),
            _ => unreachable!(),
        }
        assert!(
            !crowdb_monitor::member_trust_ready_for_tests(root).unwrap_or(false),
            "{change}"
        );
    }
}

#[tokio::test]
async fn confirmed_nonvoter_can_prove_identity_without_live_group_zero_but_pending_cannot() {
    use crowdb_protocol::mgmt::node::recovery::MemberEndpoint;
    let dir = TestRunRoot::new("nonvoter-ssh-recovery");
    let root = dir.path();
    let mut snapshot = admitted::fixture(root);
    trust::write(
        &root.join("node-endpoint.json"),
        &MemberEndpoint {
            discovery_id: snapshot.registry.nodes[3].discovery_id.clone(),
            node_id: 0,
            boot_id: uuid::Uuid::new_v4().to_string(),
            addresses: vec!["192.0.2.44".parse().unwrap()],
        },
    );
    let liveness = crowdb_monitor::LivenessServer::start(root).unwrap();
    let identity = crowdb_monitor::read_recovery_identity(root).await.unwrap();
    assert_eq!(identity.binding.node_id, 4);
    assert_eq!(
        identity.endpoint.addresses,
        vec!["192.0.2.44".parse::<std::net::IpAddr>().unwrap()]
    );
    snapshot.registry.nodes[3].confirmed = false;
    snapshot.current = snapshot.initial.confirmed_registry(&snapshot.registry).unwrap();
    trust::write(&root.join("confirmed-member-trust.json"), &snapshot);
    assert!(crowdb_monitor::read_recovery_identity(root).await.is_err());
    drop(liveness);
}

#[tokio::test]
async fn adding_management_seeds_preserves_unchanged_service_operations() {
    let dir = tempdir_in_test_data("monitor-added-recovery-contacts");
    let root = dir.path();
    let _ = trust::fixture(root);
    let operation =
        crowdb_console_shared::deployment::PreparedBootstrap::load(&root.join("prepared-bootstrap.json"))
            .unwrap();
    let seeds: Vec<_> = (1..=3).map(|n| format!("http://192.0.2.{n}:10000")).collect();
    let binding = crowdb_protocol::mgmt::node::NodeBinding {
        node_id: 1,
        bootstrap: operation.identity.clone(),
        management_seeds: seeds.clone(),
    };
    trust::write(&root.join("node-binding.json"), &binding);
    let mut expanded = seeds;
    expanded.push("http://192.0.2.4:10000".into());
    assert!(crowdb_monitor::rebind_recovery_services_for_tests(
        root,
        &operation,
        1,
        "192.0.2.1",
        &expanded,
        true,
    )
    .await
    .expect("adding contacts cannot publish replacement service operations"));
    let address: String =
        serde_json::from_slice(&std::fs::read(root.join("service-address.json")).unwrap()).unwrap();
    assert_eq!(address, "192.0.2.1");
}

#[test]
fn a_new_admission_keeps_the_cancelled_operations_pins_and_fence() {
    let dir = tempdir_in_test_data("nonvoter-readmission-trust");
    let root = dir.path();
    let mut snapshot = admitted::fixture(root);
    let old_operation = snapshot.registry.nodes[3].operation_id.clone();
    let old_path = root.join(format!("admitted-member-trust-{old_operation}.json"));
    let old_bytes = std::fs::read(&old_path).unwrap();
    trust::write(
        &root.join(format!("ssh/cancelled-{old_operation}.json")),
        &old_operation,
    );
    let new_operation = uuid::Uuid::new_v4().to_string();
    snapshot.registry.nodes[3].operation_id.clone_from(&new_operation);
    let mut pins = snapshot.current.clone();
    pins.members[3].admission_operation_id.clone_from(&new_operation);
    pins.seal(&root.join(format!("admitted-member-trust-{new_operation}.json")))
        .unwrap();
    trust::write(
        &root.join("accepted-node.json"),
        &serde_json::json!({
            "node_id":4,"bootstrap":snapshot.initial.bootstrap,"prepared":true,
            "admission":{"operation_id":new_operation,"management_seeds":["http://192.0.2.1:10000"]}
        }),
    );
    check_member_trust_for_tests(root).expect("a fresh admission must use its own immutable pins");
    assert!(
        !crowdb_monitor::member_trust_ready_for_tests(root).unwrap(),
        "the new operation cannot inherit the cancelled operation's confirmation"
    );
    snapshot.current = pins;
    trust::write(&root.join("confirmed-member-trust.json"), &snapshot);
    assert!(crowdb_monitor::member_trust_ready_for_tests(root).unwrap());
    assert_eq!(std::fs::read(&old_path).unwrap(), old_bytes);
    trust::write(
        &root.join("accepted-node.json"),
        &serde_json::json!({
            "node_id":4,"bootstrap":snapshot.initial.bootstrap,"prepared":true,
            "admission":{"operation_id":old_operation,"management_seeds":["http://192.0.2.1:10000"]}
        }),
    );
    assert!(
        check_member_trust_for_tests(root).is_err(),
        "the cancelled operation remains fenced"
    );
}
