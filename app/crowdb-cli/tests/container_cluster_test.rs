// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_test_harness::test_dirs::TestDir;
use serde_json::json;
use std::os::unix::fs::{symlink, PermissionsExt};
use std::path::Path;
use std::process::Command;

fn state(root: &Path) {
    std::fs::write(
        root.join("container-cluster.json"),
        json!({
            "version":1,"id":"0123456789abcdef0123456789abcdef","root":root,
            "image":format!("sha256:{}", "a".repeat(64)),"name":"owned-cluster",
            "network":"owned-cluster-network","phase":"ready","node_cpus":2.0,
            "memory_mib":2048,"addresses":["172.20.0.2","172.20.0.3","172.20.0.4"]
        })
        .to_string(),
    )
    .unwrap();
}

fn runtime(root: &Path, body: &str) -> std::path::PathBuf {
    let script = root.join("runtime-stub");
    std::fs::write(&script, format!("#!/bin/sh\n{body}\n")).unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o700)).unwrap();
    script
}

#[test]
fn startup_preserves_recorded_heartbeat_configuration_without_runtime_mutation() {
    let directory = TestDir::new("cli-container-heartbeat-config").unwrap();
    let root = std::fs::canonicalize(directory.path()).unwrap();
    state(&root);
    let before = std::fs::read(root.join("container-cluster.json")).unwrap();
    let script = runtime(&root, "echo 'unexpected runtime mutation' >&2; exit 77");
    let output = Command::new(env!("CARGO_BIN_EXE_crowdb-cli"))
        .args(["s3", "cluster", "start", "--root"])
        .arg(&root)
        .args(["--diskdb-heartbeat-interval-secs", "3"])
        .env("CROWDB_CONTAINER_RUNTIME", script)
        .output()
        .unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8_lossy(&output.stderr);
    assert!(error.contains("recorded Diskdb heartbeat configuration"));
    assert!(!error.contains("unexpected runtime mutation"));
    assert_eq!(
        std::fs::read(root.join("container-cluster.json")).unwrap(),
        before
    );
}

#[test]
fn lifecycle_rejects_foreign_network_without_mutation() {
    let directory = TestDir::new("cli-container-foreign-network").unwrap();
    let root = std::fs::canonicalize(directory.path()).unwrap();
    state(&root);
    let script = runtime(&root, "case \"$*\" in\n  'network inspect owned-cluster-network') echo '[{\"Name\":\"owned-cluster-network\",\"Labels\":{\"org.crowdb.cluster.id\":\"foreign\"}}]' ;;\n  *) echo 'unexpected runtime mutation' >&2; exit 77 ;;\nesac");
    for action in ["status", "stop", "restart", "delete"] {
        let output = Command::new(env!("CARGO_BIN_EXE_crowdb-cli"))
            .args(["s3", "cluster", action, "--root"])
            .arg(&root)
            .env("CROWDB_CONTAINER_RUNTIME", &script)
            .output()
            .unwrap();
        assert!(!output.status.success());
        assert!(String::from_utf8_lossy(&output.stderr).contains("different cluster ownership"));
        assert!(root.join("container-cluster.json").exists());
    }
}

#[test]
fn runtime_failure_is_not_treated_as_missing_network() {
    let directory = TestDir::new("cli-container-runtime-error").unwrap();
    let root = std::fs::canonicalize(directory.path()).unwrap();
    state(&root);
    let script = runtime(&root, "echo 'runtime executable not found' >&2; exit 42");
    let output = Command::new(env!("CARGO_BIN_EXE_crowdb-cli"))
        .args(["s3", "cluster", "status", "--root"])
        .arg(&root)
        .env("CROWDB_CONTAINER_RUNTIME", script)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("runtime executable not found"));
}

#[test]
fn generated_member_name_reaches_runtime_ownership_lookup() {
    let directory = TestDir::new("cli-container-member-name").unwrap();
    let script = runtime(
        directory.path(),
        "echo 'member ownership lookup reached' >&2; exit 42",
    );
    let name = format!("{}-node2", "a".repeat(48));
    let output = Command::new(env!("CARGO_BIN_EXE_crowdb-cli"))
        .args(["s3", "cluster", "status", "--container", &name])
        .env("CROWDB_CONTAINER_RUNTIME", script)
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("member ownership lookup reached"));
}

#[test]
fn lifecycle_rejects_symlinked_data_before_runtime_operations() {
    let directory = TestDir::new("cli-container-data-ownership").unwrap();
    let root = std::fs::canonicalize(directory.path()).unwrap();
    state(&root);
    symlink("/", root.join("nodes")).unwrap();
    let output = Command::new(env!("CARGO_BIN_EXE_crowdb-cli"))
        .args(["s3", "cluster", "delete", "--root"])
        .arg(&root)
        .env("CROWDB_CONTAINER_RUNTIME", "/path/not/executed")
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("cannot be symbolic links"));
    assert!(root.join("container-cluster.json").exists());
}

#[test]
fn delete_cleans_owned_partial_writes_and_preserves_foreign_files() {
    for foreign in [false, true] {
        let directory = TestDir::new("cli-container-partial-state-cleanup").unwrap();
        let root = directory.path().join("cluster");
        std::fs::create_dir(&root).unwrap();
        let root = std::fs::canonicalize(root).unwrap();
        state(&root);
        let temporary = root.join(format!(
            "s3-mini-cluster.initializing.json.0123456789abcdef0123456789abcdef.{}.tmp",
            "a".repeat(32)
        ));
        std::fs::write(&temporary, "partial write").unwrap();
        let foreign_file = root.join(format!(
            "container-cluster.json.ffffffffffffffffffffffffffffffff.{}.tmp",
            "b".repeat(32)
        ));
        if foreign {
            std::fs::write(&foreign_file, "operator file").unwrap();
        }
        let script = runtime(directory.path(), "echo 'Error: No such object' >&2; exit 1");
        let output = Command::new(env!("CARGO_BIN_EXE_crowdb-cli"))
            .args(["s3", "cluster", "delete", "--root"])
            .arg(&root)
            .env("CROWDB_CONTAINER_RUNTIME", script)
            .output()
            .unwrap();
        assert!(!temporary.exists());
        if foreign {
            assert!(!output.status.success());
            assert_eq!(std::fs::read_to_string(foreign_file).unwrap(), "operator file");
        } else {
            assert!(
                output.status.success(),
                "{}",
                String::from_utf8_lossy(&output.stderr)
            );
            assert!(!root.exists());
        }
    }
}
