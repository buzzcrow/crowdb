// Copyright 2026-present Gian <crow.db@outlook.com>.

//! CLI e2e for cluster observation: `cluster status` and `cluster
//! topology` through a system-group endpoint against a real
//! `crowdb-paxos-kv-server` with the system group initialized.

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
        .args(["cluster", "status", "--timeout", "1"])
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
    for iteration in 0..2 {
        let root = tempdir(&format!("bench-latest-log-{iteration}"));
        let log_dir = root.join(".crowdb-runtime/artifacts/cli/bench-s3");
        fs::create_dir_all(&log_dir).expect("create old benchmark log dir");
        fs::write(log_dir.join("old-run.marker"), b"old").expect("write old marker");

        let output = Command::new(&cli)
            .current_dir(&root)
            .env("CROWDB_RUNTIME_ROOT", root.join(".crowdb-runtime"))
            .env_remove("CROWDB_CLUSTER")
            .args(["bench", "s3", "write", "--operations", "1"])
            .output()
            .expect("run benchmark against closed port");
        assert!(!output.status.success());
        assert!(log_dir.is_dir());
        assert!(!log_dir.join("old-run.marker").exists());
        let stderr = String::from_utf8_lossy(&output.stderr);
        assert!(stderr.contains("log dir:"), "stderr={stderr}");
        let _ = fs::remove_dir_all(root);
    }
}

#[test]
fn retired_storage_process_commands_are_absent_and_measurements_remain() {
    let cli = crowdb_cli_bin();
    for arguments in [vec!["chunk", "--help"], vec!["bench", "rpc", "--help"]] {
        let output = Command::new(&cli).args(&arguments).output().unwrap();
        assert_eq!(output.status.code(), Some(2), "{arguments:?}: {output:?}");
        assert!(String::from_utf8_lossy(&output.stderr).contains("unrecognized subcommand"));
    }
    for family in ["paxos-kv", "diskdb", "chunkdb", "chunkio", "s3"] {
        let output = Command::new(&cli)
            .args(["bench", family, "--help"])
            .output()
            .unwrap();
        assert!(output.status.success(), "{family}: {output:?}");
    }
}
