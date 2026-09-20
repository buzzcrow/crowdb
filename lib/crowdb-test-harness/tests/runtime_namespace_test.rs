// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_test_harness::test_dirs::{runtime_root, TestRuntime};

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
