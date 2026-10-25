// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::{path::Path, process::Command};

pub fn test_operations() -> Vec<Vec<&'static str>> {
    vec![
        vec!["cluster", "rack", "list"],
        vec!["cluster", "node", "list"],
        vec!["cluster", "disk-group", "list"],
        vec!["cluster", "disk", "list"],
        vec!["cluster", "topology", "--node", "1"],
        vec!["kv", "store", "list"],
        vec!["kv", "group", "list", "--store", "0"],
        vec![
            "kv", "replica", "add", "--store", "0", "--group", "1", "--node", "2",
        ],
        vec![
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
        ],
        vec!["s3", "bucket", "put", "target-marker"],
        vec!["bench", "kv", "prepare", "--keys", "1"],
        vec!["bench", "diskdb", "allocate"],
        vec!["bench", "chunkdb", "allocate"],
        vec!["bench", "chunkio", "write"],
        vec!["bench", "s3", "write", "--operations", "1"],
    ]
}

pub fn test_rejected_operation(root: &Path, selected: &[&str], args: &[&str]) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_crowdb-cli"))
        .env("CROWDB_RUNTIME_ROOT", root)
        .env("CROWDB_CONTAINER_RUNTIME", "/path/not/executed")
        .env_remove("CROWDB_CLUSTER")
        .env_remove("CROWDB_SYSTEM_IP")
        .env_remove("CROWDB_SYSTEM_PORT")
        .args(selected)
        .args(args)
        .output()
        .unwrap();
    let error = String::from_utf8_lossy(&output.stderr).into_owned();
    assert_eq!(output.status.code(), Some(2), "{args:?}: {error}");
    assert!(!root.join("persistent").exists(), "{args:?}: {error}");
    error
}
