// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::process::Command;

#[test]
fn retired_process_commands_and_selectors_fail_before_runtime_access() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("cli-removed-paths");
    for args in [
        vec!["launch", "list"],
        vec!["kv", "server", "list"],
        vec!["cluster", "init"],
        vec!["cluster", "local-deploy"],
        vec!["cluster", "destroy"],
        vec!["cluster", "clean"],
        vec!["chunk", "diskdb", "deploy"],
        vec!["bench", "rpc"],
        vec!["s3", "cluster", "start"],
        vec!["--registry", "missing", "cluster", "status"],
        vec!["--system-ip", "127.0.0.1", "cluster", "status"],
        vec!["--system-port", "7000", "cluster", "status"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_crowdb-cli"))
            .env("CROWDB_RUNTIME_ROOT", root.path())
            .env("CROWDB_CONTAINER_RUNTIME", "/must/not/execute")
            .args(&args)
            .output()
            .unwrap();
        assert_eq!(output.status.code(), Some(2), "{args:?}: {output:?}");
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(
            error.contains("unrecognized subcommand") || error.contains("unexpected argument"),
            "{error}"
        );
    }
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
}
