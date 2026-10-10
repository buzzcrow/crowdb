// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_test_harness::test_dirs::{runtime_root, TestDir, TestRuntime};

#[test]
fn disposable_data_cleanup_preserves_process_ownership_for_cleanup() {
    let directory = TestDir::new("cleanup-ownership").expect("create test directory");
    let data = directory.path().to_path_buf();
    let root = data.parent().unwrap().to_path_buf();
    let workspace = data.join("cli-cluster");
    let mut namespace =
        crowdb_protocol::port::namespace::RuntimeNamespace::persistent(&workspace, "nested-cli-cluster")
            .expect("create CLI manifest inside test data");
    let port = namespace
        .assign_port(crowdb_protocol::ServicePort::ChunkKvRpc, 0)
        .expect("assign disposable CLI port");
    let claims: serde_json::Value =
        serde_json::from_slice(&std::fs::read(runtime_root().join("ports/claims.json")).unwrap()).unwrap();
    let claim = claims
        .as_array()
        .unwrap()
        .iter()
        .find(|claim| claim["namespace_id"] == namespace.id() && claim["port"] == port)
        .expect("registered CLI port");
    assert_eq!(claim["mode"], "ephemeral");
    assert_eq!(claim["owner_pid"], std::process::id());
    let pid = std::process::id();
    crowdb_protocol::port::namespace::record_workspace_process(&workspace, pid)
        .expect("record process ownership");
    drop(namespace);
    drop(directory);

    assert!(!data.exists());
    assert!(root.join("namespace.json").is_file());
    assert!(root.join(format!("processes/{pid}/process-owner.json")).is_file());
    std::fs::remove_dir_all(root).expect("remove regression namespace");
}

#[test]
fn runtime_namespace_owns_disjoint_standard_paths() {
    let first = TestRuntime::new("namespace-layout").expect("create first runtime");
    let second = TestRuntime::new("namespace-layout").expect("create second runtime");

    assert_ne!(first.root(), second.root());
    assert!(first.root().starts_with(runtime_root().join("ephemeral")));
    for path in [
        first.data_dir(),
        first.config_dir(),
        first.log_dir(),
        first.artifacts_dir(),
    ] {
        assert!(path.is_dir(), "missing runtime path: {}", path.display());
        assert!(path.starts_with(first.root()));
    }
}

#[test]
fn service_identity_has_stable_isolated_tree() {
    let runtime = TestRuntime::new("service-layout").expect("create runtime");
    let first = runtime
        .service_dir("diskdb", "owner-7")
        .expect("create service root");
    let reopened = runtime
        .service_dir("diskdb", "owner-7")
        .expect("reopen service root");
    let other = runtime
        .service_dir("diskdb", "owner-8")
        .expect("create other service root");

    assert_eq!(first, reopened);
    assert_ne!(first, other);
    for child in ["data", "config", "log", "artifacts"] {
        assert!(first.join(child).is_dir());
    }
}
