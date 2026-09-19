// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::process::Command;

fn cli() -> Command {
    Command::new(env!("CARGO_BIN_EXE_crowdb-cli"))
}

#[test]
fn s3_benchmark_exposes_all_memory_workloads() {
    for workload in ["write", "read", "range-read", "list", "mix"] {
        let output = cli()
            .args(["bench", "s3", workload, "--help"])
            .output()
            .expect("run help");
        assert!(
            output.status.success(),
            "{workload}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(stdout.contains("--memory-budget-bytes"));
        assert!(stdout.contains("--operations"));
    }
}

#[test]
fn object_range_rejects_regression_before_cluster_access() {
    let output = cli()
        .args([
            "s3",
            "object",
            "get",
            "--data-dir",
            "/path/that/is/not/opened",
            "bucket",
            "key",
            "--range",
            "9-3",
        ])
        .output()
        .expect("run invalid range");
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("range start must not exceed end"));
}
