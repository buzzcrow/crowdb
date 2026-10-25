// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/cluster_target.rs"]
mod common;

#[path = "common/target_commands.rs"]
mod operations;

use common::TestTargetServer;
use crowdb_console_shared::deployment::target::ClusterTargetBinding;
use operations::{test_operations, test_rejected_operation};
use std::{os::unix::fs::PermissionsExt, path::Path, process::Command};

fn run(root: &Path, selected: &[&str]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_crowdb-cli"))
        .env("CROWDB_RUNTIME_ROOT", root)
        .env_remove("CROWDB_CLUSTER")
        .env_remove("CROWDB_SYSTEM_IP")
        .env_remove("CROWDB_SYSTEM_PORT")
        .args(selected)
        .args([
            "kv",
            "data",
            "put",
            "--store",
            "0",
            "--group",
            "1",
            "--key",
            "target-marker",
            "--value",
            "untouched",
        ])
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    assert!(!output.status.success(), "unexpected success: {stderr}");
    stderr
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_mixed_identities_reject_a_workload_before_native_connect() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("cli-target-mixed");
    let left = TestTargetServer::start(&uuid::Uuid::new_v4().to_string()).await;
    let right = TestTargetServer::start(&uuid::Uuid::new_v4().to_string()).await;
    for args in test_operations() {
        let error = test_rejected_operation(
            root.path(),
            &[
                "--management-endpoint",
                &left.origin,
                "--management-endpoint",
                &right.origin,
            ],
            &args,
        );
        assert!(
            error.contains("disagree on cluster identity"),
            "{args:?}: {error}"
        );
        assert!(!error.contains("native transport"), "{error}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_named_binding_never_adopts_a_replacement_uuid_from_explicit_addresses() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("cli-target-binding");
    let path = root.path().join("persistent/clusters/demo/target.json");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    let original_id = uuid::Uuid::new_v4().to_string();
    let original = TestTargetServer::start(&original_id).await;
    let replacement = TestTargetServer::start(&uuid::Uuid::new_v4().to_string()).await;
    let bytes = serde_json::to_vec(&ClusterTargetBinding {
        version: 1,
        name: "demo".into(),
        cluster_id: original_id,
        management_endpoints: vec![original.origin.clone()],
        runtime: None,
    })
    .unwrap();
    std::fs::write(&path, &bytes).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
    let error = run(
        root.path(),
        &["--cluster", "demo", "--management-endpoint", &replacement.origin],
    );
    assert!(error.contains("disagree on cluster identity"), "{error}");
    assert_eq!(std::fs::read(path).unwrap(), bytes);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cli_http_only_target_fails_native_workload_without_localhost_fallback() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("cli-target-native");
    let server = TestTargetServer::start(&uuid::Uuid::new_v4().to_string()).await;
    let error = run(root.path(), &["--management-endpoint", &server.origin]);
    assert!(error.contains("native RPC endpoint is unreachable"), "{error}");
    let error = run(root.path(), &["--cluster", "missing"]);
    assert!(error.contains("no readable target binding"), "{error}");
}

#[test]
fn cli_rejects_removed_system_address_flags() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("cli-target-flags");
    let error = run(
        root.path(),
        &[
            "--management-endpoint",
            "http://127.0.0.1:9090",
            "--system-ip",
            "127.0.0.1",
        ],
    );
    assert!(error.contains("unexpected argument '--system-ip'"), "{error}");
}

#[test]
fn retained_workloads_require_a_target_before_any_runtime_or_native_effect() {
    for args in [
        vec!["bench", "kv", "prepare", "--keys", "1"],
        vec!["bench", "diskdb", "allocate"],
        vec!["bench", "chunkdb", "allocate"],
        vec!["bench", "chunkio", "write"],
        vec!["bench", "s3", "write", "--operations", "1"],
    ] {
        let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("bench-no-target");
        let output = Command::new(env!("CARGO_BIN_EXE_crowdb-cli"))
            .env("CROWDB_RUNTIME_ROOT", root.path())
            .env("CROWDB_CONTAINER_RUNTIME", "/path/not/executed")
            .env_remove("CROWDB_CLUSTER")
            .env_remove("CROWDB_SYSTEM_IP")
            .env_remove("CROWDB_SYSTEM_PORT")
            .args(&args)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2));
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(
            error
                .contains("select --cluster or --management-endpoint before a cluster operation or workload"),
            "{args:?}: {error}"
        );
        assert!(!root.path().join("persistent").exists());
    }
}

#[test]
fn every_attached_operation_rejects_legacy_address_fallback_before_connecting() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    for args in test_operations() {
        let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("operation-no-target");
        let error = test_rejected_operation(root.path(), &[], &args);
        assert!(
            error.contains("select --cluster or --management-endpoint"),
            "{args:?}: {error}"
        );
    }
    assert_eq!(
        listener.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
}

#[test]
fn every_attached_operation_rejects_an_unreachable_management_target() {
    let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let endpoint = format!("http://{}", closed.local_addr().unwrap());
    drop(closed);
    for args in test_operations() {
        let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("operation-unreachable");
        let error = test_rejected_operation(root.path(), &["--management-endpoint", &endpoint], &args);
        assert!(
            error.contains("management endpoint is unavailable"),
            "{args:?}: {error}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_operations_reject_unreachable_rpc_without_runtime_fallback() {
    let server = TestTargetServer::start(&uuid::Uuid::new_v4().to_string()).await;
    for args in test_operations().into_iter().filter(|args| !args.contains(&"s3")) {
        let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("operation-native");
        let error = test_rejected_operation(root.path(), &["--management-endpoint", &server.origin], &args);
        assert!(
            error.contains("native RPC endpoint is unreachable"),
            "{args:?}: {error}"
        );
        assert!(!error.contains("load launch registry"), "{error}");
    }
}
