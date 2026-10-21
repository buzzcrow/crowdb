// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_console_shared::deployment::recovery::MemberTrust;
use crowdb_monitor::{accept_member_trust_for_tests, check_member_trust_for_tests};
use crowdb_test_harness::test_dirs::tempdir_in_test_data;

#[path = "common/member_trust.rs"]
mod trust;

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
