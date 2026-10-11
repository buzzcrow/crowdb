// Copyright 2026-present Gian <crow.db@outlook.com>.

//! CLI e2e for cluster observation: `cluster status` and `cluster
//! topology` through a system-group endpoint against a real
//! `crowdb-kv-server` with the system group initialized.

mod common;

use std::{fs, process::Command};

use common::direct::{crowdb_cli_bin, tempdir};

#[test]
fn non_benchmark_command_does_not_create_log_directory() {
    let cli = crowdb_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowdb-cli binary not built ({})", cli.display());
        return;
    }
    let root = tempdir("cli-output-prefix");
    let config = root.join("missing-console.toml");
    let output = Command::new(cli)
        .current_dir(&root)
        .env("CROWDB_RUNTIME_ROOT", root.join(".crowdb-runtime"))
        .env("CROWDB_CLI_STATE", config)
        .args(["cluster", "local-deploy", "--service-type", "invalid"])
        .output()
        .expect("run crowdb-cli");
    assert!(!output.status.success());
    assert!(!root.join(".crowdb-runtime/artifacts/cli").exists());
    let _ = fs::remove_dir_all(root);
}

#[test]
fn benchmark_replaces_its_previous_log_directory() {
    let cli = crowdb_cli_bin();
    if !cli.exists() {
        eprintln!("skipping: crowdb-cli binary not built ({})", cli.display());
        return;
    }
    let root = tempdir("bench-latest-log");
    let log_dir = root.join(".crowdb-runtime/artifacts/cli/bench-rpc");
    fs::create_dir_all(&log_dir).expect("create old benchmark log dir");
    fs::write(log_dir.join("old-run.marker"), b"old").expect("write old marker");

    let output = Command::new(cli)
        .current_dir(&root)
        .env("CROWDB_RUNTIME_ROOT", root.join(".crowdb-runtime"))
        .args(["bench", "rpc", "--server-port", "1", "--metrics-interval", "0"])
        .output()
        .expect("run benchmark against closed port");
    assert!(!output.status.success());
    assert!(log_dir.is_dir());
    assert!(!log_dir.join("old-run.marker").exists());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("log dir:"), "stderr={stderr}");
    let _ = fs::remove_dir_all(root);
}
