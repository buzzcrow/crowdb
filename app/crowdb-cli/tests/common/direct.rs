// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![allow(dead_code)]

use crowdb_test_harness::test_dirs;
use std::path::PathBuf;

/// Locate the compiled `crowdb-cli` binary next to the test runner.
#[must_use]
pub fn crowdb_cli_bin() -> PathBuf {
    if let Some(path) = option_env!("CARGO_BIN_EXE_crowdb-cli") {
        return PathBuf::from(path);
    }
    let mut p = std::env::current_exe().expect("current_exe");
    while p.file_name().is_some_and(|n| n != "debug" && n != "release") {
        p.pop();
    }
    p.push("crowdb-cli");
    p
}

/// Unique temp directory for a test.
#[must_use]
pub fn tempdir(tag: &str) -> PathBuf {
    let base = test_dirs::test_data_dir();
    let unique = format!(
        "crowdb-cli-direct-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    );
    let dir = base.join(unique);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}
