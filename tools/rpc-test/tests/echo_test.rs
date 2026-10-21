// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::{path::Path, process::Command};

#[test]
fn real_echo_transport_runs_both_schedulers_and_shuts_down_its_server() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let binary = env!("CARGO_BIN_EXE_crowdb-rpc-test");
    for mode in ["coroutine", "tokio"] {
        let log = crowdb_test_harness::test_dirs::tempdir_in_test_data("rpc-test-echo");
        let result = Command::new("python")
            .current_dir(root)
            .args([
                "tools/rpc-test/run.py",
                "--client",
                binary,
                "--duration-secs",
                "1",
                "--loader-num",
                "2",
                "--connections",
                "1",
                "--mode",
                mode,
                "--metrics-interval",
                "0",
            ])
            .arg("--log-dir")
            .arg(log.path())
            .output()
            .unwrap();
        assert!(
            result.status.success(),
            "{mode}: stdout={} stderr={}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
        let report: serde_json::Value = serde_json::from_slice(&result.stdout).unwrap();
        assert!(report["total_ops"].as_u64().unwrap() > 0);
        assert_eq!(report["total_errors"], 0);
        assert!(log.path().join("server.stdout.log").is_file());
    }
}

#[test]
fn fixture_rejects_invalid_resources_before_starting_a_process() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .unwrap()
        .parent()
        .unwrap();
    let log = crowdb_test_harness::test_dirs::tempdir_in_test_data("rpc-test-invalid");
    let result = Command::new("python")
        .current_dir(root)
        .args(["tools/rpc-test/run.py", "--io-engines", "2", "--io-workers", "1"])
        .arg("--log-dir")
        .arg(log.path())
        .output()
        .unwrap();
    assert_eq!(result.status.code(), Some(2));
    assert!(!log.path().join("server.stdout.log").exists());
}
