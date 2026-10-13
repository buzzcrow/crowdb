// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// Packaged storage lifecycle and node faults belong to the S3 container suite.
// Keep local ownership and launch-state validation independent of servers.
use crowdb_console_shared::ops::s3;
use crowdb_test_harness::test_dirs::TestDir;

#[test]
fn foreign_nonempty_directory_is_not_a_cluster() {
    let dir = TestDir::new("s3-mini-foreign").expect("create test directory");
    std::fs::write(dir.path().join("owned-by-user"), b"keep").expect("write sentinel");

    let error = s3::status(dir.path()).expect_err("foreign directory must be rejected");

    assert!(error.to_string().contains("is not a CROWDB S3 mini-cluster"));
    assert_eq!(std::fs::read(dir.path().join("owned-by-user")).unwrap(), b"keep");
}

#[test]
fn incomplete_marker_fails_closed() {
    let dir = TestDir::new("s3-mini-incomplete").expect("create test directory");
    std::fs::write(dir.path().join("s3-mini-cluster.json"), b"{}").expect("write marker");

    let error = s3::status(dir.path()).expect_err("incomplete marker must be rejected");

    assert!(error.to_string().contains("config error"));
}

#[test]
fn local_launch_state_rejects_topology_and_legacy_console_file() {
    let dir = TestDir::new("s3-mini-local-only").expect("create test directory");
    std::fs::write(
        dir.path().join("s3-mini-cluster.json"),
        r#"{"version":1,"endpoint":"http://127.0.0.1:16000","tenant":"local"}"#,
    )
    .unwrap();
    std::fs::write(dir.path().join("console.toml"), "[[rack]]\nid = 1\n").unwrap();
    assert!(
        s3::status(dir.path()).is_err(),
        "legacy topology must not be loaded"
    );
    std::fs::write(
        dir.path().join("s3-local-state.toml"),
        "version = 1\ngroup0_seeds = ['http://127.0.0.1:10000']\n[[rack]]\nid = 1\n",
    )
    .unwrap();
    let error = s3::status(dir.path()).expect_err("local state cannot contain topology");
    assert!(error.to_string().contains("unknown field"), "{error}");
}
