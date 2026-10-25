// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::fs;
use std::os::unix::fs::{symlink, PermissionsExt};

use crowdb_monitor::{accept_node_policy, read_node_policy, BootstrapSession, LivenessServer, NodeIdentity};
use crowdb_protocol::mgmt::node::deployment::NodeDeploymentOverrides;

#[path = "common/run_root.rs"]
mod run_root;

#[test]
fn accepted_policy_survives_restart_without_defaults_or_identity_effects() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("accepted-node-policy");
    let omitted = NodeDeploymentOverrides::default();
    let inputs = NodeDeploymentOverrides {
        cluster: Some("durable-cluster".into()),
        initial_voters: Some(3),
        ..Default::default()
    };
    let accepted = accept_node_policy(root.path(), &omitted, &inputs).unwrap();
    let before = fs::read(root.path().join("node-policy.json")).unwrap();
    assert_eq!(
        accept_node_policy(root.path(), &omitted, &omitted).unwrap(),
        accepted
    );
    assert_eq!(read_node_policy(root.path()).unwrap(), Some(accepted));
    assert_eq!(fs::read(root.path().join("node-policy.json")).unwrap(), before);
    assert!(!root.path().join("node-identity").exists());
    assert!(!root.path().join("disks").exists());
    assert_eq!(
        fs::metadata(root.path().join("node-policy.json"))
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o600
    );
}

#[test]
fn incompatible_overrides_preserve_policy_and_retained_identity_bytes() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("node-policy-conflict");
    let omitted = NodeDeploymentOverrides::default();
    accept_node_policy(root.path(), &omitted, &omitted).unwrap();
    let identity = NodeIdentity::load_or_create(root.path()).unwrap();
    let before = fs::read(root.path().join("node-policy.json")).unwrap();
    for inputs in [
        NodeDeploymentOverrides {
            initial_voters: Some(3),
            ..Default::default()
        },
        NodeDeploymentOverrides {
            cluster: Some("foreign".into()),
            ..Default::default()
        },
        NodeDeploymentOverrides {
            version: Some(99),
            ..Default::default()
        },
    ] {
        assert!(accept_node_policy(root.path(), &omitted, &inputs).is_err());
        assert_eq!(fs::read(root.path().join("node-policy.json")).unwrap(), before);
        assert_eq!(NodeIdentity::load_or_create(root.path()).unwrap(), identity);
    }
    assert!(BootstrapSession::open(root.path(), b"profile", b"config", &["kv"]).is_err());
    assert!(!root.path().join("bootstrap").exists());
}

#[test]
fn missing_identity_of_an_accepted_deployment_cannot_generate_a_replacement() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("node-policy-missing-identity");
    let omitted = NodeDeploymentOverrides::default();
    accept_node_policy(root.path(), &omitted, &omitted).unwrap();
    fs::write(root.path().join("accepted-node.json"), b"retained bootstrap").unwrap();
    assert!(accept_node_policy(root.path(), &omitted, &omitted).is_err());
    assert!(!root.path().join("node-identity").exists());
    assert_eq!(
        fs::read(root.path().join("accepted-node.json")).unwrap(),
        b"retained bootstrap"
    );
}

#[test]
fn obsolete_and_unrecognized_populated_roots_fail_before_new_state() {
    for name in [
        "startup-mode.json",
        "node-identity",
        "accepted-node.json",
        "operator-data",
    ] {
        let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("node-policy-populated");
        fs::write(root.path().join(name), b"retained bytes").unwrap();
        let inputs = NodeDeploymentOverrides::default();
        assert!(accept_node_policy(root.path(), &inputs, &inputs).is_err());
        assert_eq!(fs::read(root.path().join(name)).unwrap(), b"retained bytes");
        assert_eq!(fs::read_dir(root.path()).unwrap().count(), 1);
    }
}

#[test]
fn corrupt_unknown_or_symlinked_policy_is_never_replaced() {
    for bytes in [b"corrupt".as_slice(), br#"{"version":99}"#.as_slice()] {
        let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("node-policy-invalid");
        let path = root.path().join("node-policy.json");
        fs::write(&path, bytes).unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let inputs = NodeDeploymentOverrides::default();
        assert!(accept_node_policy(root.path(), &inputs, &inputs).is_err());
        assert_eq!(fs::read(path).unwrap(), bytes);
        assert!(!root.path().join("node-identity").exists());
    }
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("node-policy-symlink");
    let target = root.path().join("operator-data");
    fs::write(&target, b"preserve").unwrap();
    symlink(&target, root.path().join("node-policy.json")).unwrap();
    let inputs = NodeDeploymentOverrides::default();
    assert!(accept_node_policy(root.path(), &inputs, &inputs).is_err());
    assert_eq!(fs::read(target).unwrap(), b"preserve");
}

#[test]
fn generated_ssh_directory_is_allowed_but_automatic_storage_never_adopts_files() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("node-policy-preparation");
    fs::create_dir(root.path().join("ssh")).unwrap();
    fs::write(root.path().join("ssh/id_ed25519"), b"existing key").unwrap();
    fs::create_dir(root.path().join("disks")).unwrap();
    fs::write(root.path().join("disks/foreign.img"), b"retained media").unwrap();
    let inputs = NodeDeploymentOverrides::default();
    assert!(accept_node_policy(root.path(), &inputs, &inputs).is_err());
    assert_eq!(
        fs::read(root.path().join("disks/foreign.img")).unwrap(),
        b"retained media"
    );
    fs::remove_file(root.path().join("disks/foreign.img")).unwrap();
    accept_node_policy(root.path(), &inputs, &inputs).unwrap();
    assert_eq!(
        fs::read(root.path().join("ssh/id_ed25519")).unwrap(),
        b"existing key"
    );
}

#[test]
fn concurrent_incompatible_publishers_accept_exactly_one_policy() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("node-policy-race");
    let outcomes = std::thread::scope(|scope| {
        let start = |cluster: &'static str| {
            let omitted = NodeDeploymentOverrides::default();
            accept_node_policy(
                root.path(),
                &omitted,
                &NodeDeploymentOverrides {
                    cluster: Some(cluster.into()),
                    ..Default::default()
                },
            )
        };
        let first = scope.spawn(move || start("first"));
        let second = scope.spawn(move || start("second"));
        [first.join().unwrap(), second.join().unwrap()]
    });
    assert_eq!(outcomes.iter().filter(|outcome| outcome.is_ok()).count(), 1);
    assert_eq!(
        Some(outcomes.into_iter().find_map(Result::ok).unwrap()),
        read_node_policy(root.path()).unwrap()
    );
    assert!(!root.path().join("node-identity").exists());
}

#[tokio::test]
async fn root_guard_rejects_another_monitor_even_with_a_private_run_directory() {
    let root = run_root::TestRunRoot::new("node-policy-owner");
    let inputs = NodeDeploymentOverrides::default();
    accept_node_policy(root.path(), &inputs, &inputs).unwrap();
    let owner = LivenessServer::start(root.path()).unwrap();
    assert!(LivenessServer::start(root.path()).is_err());
    assert!(!root.path().join("node-identity").exists());
    drop(owner);
    let _replacement = LivenessServer::start(root.path()).unwrap();
}
