// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_monitor::{accept_startup_mode_for_tests, BootstrapSession};

#[test]
fn startup_mode_is_fixed_without_changing_existing_data() {
    for single in [false, true] {
        let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("startup-mode");
        accept_startup_mode_for_tests(root.path(), single).unwrap();
        let before = std::fs::read(root.path().join("startup-mode.json")).unwrap();
        accept_startup_mode_for_tests(root.path(), single).unwrap();
        assert!(accept_startup_mode_for_tests(root.path(), !single).is_err());
        assert_eq!(
            std::fs::read(root.path().join("startup-mode.json")).unwrap(),
            before
        );
        assert!(!root.path().join("node-identity").exists());
        assert!(!root.path().join("disks").exists());
    }
}

#[test]
fn single_profile_resumes_legacy_manifest_and_new_single_policy() {
    for legacy in [false, true] {
        let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("single-mode");
        if !legacy {
            accept_startup_mode_for_tests(root.path(), true).unwrap();
        }
        let session = BootstrapSession::open(root.path(), b"profile", b"config", &["kv"]).unwrap();
        let identity = session.manifest().deployment_id();
        let manifest = std::fs::read(root.path().join("bootstrap/manifest.json")).unwrap();
        assert!(accept_startup_mode_for_tests(root.path(), false).is_err());
        accept_startup_mode_for_tests(root.path(), true).unwrap();
        let resumed = BootstrapSession::open(root.path(), b"profile", b"config", &["kv"]).unwrap();
        assert_eq!(resumed.manifest().deployment_id(), identity);
        assert_eq!(
            std::fs::read(root.path().join("bootstrap/manifest.json")).unwrap(),
            manifest
        );
    }
}

#[test]
fn standard_acceptance_cannot_be_reinterpreted_as_single() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("standard-mode");
    std::fs::write(root.path().join("accepted-node.json"), b"retained").unwrap();
    assert!(accept_startup_mode_for_tests(root.path(), true).is_err());
    assert!(!root.path().join("startup-mode.json").exists());
    assert!(BootstrapSession::open(root.path(), b"profile", b"config", &["kv"]).is_err());
    assert!(!root.path().join("bootstrap").exists());
}

#[test]
fn incomplete_bound_state_does_not_acquire_a_new_startup_mode() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("incomplete-startup-mode");
    std::fs::write(root.path().join("node-binding.json"), b"retained").unwrap();
    assert!(accept_startup_mode_for_tests(root.path(), false).is_err());
    assert!(!root.path().join("startup-mode.json").exists());
}

#[test]
fn concurrent_conflicting_modes_publish_one_winner() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("startup-mode-race");
    std::thread::scope(|scope| {
        let single = scope.spawn(|| accept_startup_mode_for_tests(root.path(), true));
        let standard = scope.spawn(|| accept_startup_mode_for_tests(root.path(), false));
        assert_ne!(single.join().unwrap().is_ok(), standard.join().unwrap().is_ok());
    });
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
}
