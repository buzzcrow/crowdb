// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[test]
fn invalid_startup_reports_failure_to_the_deployment_supervisor() {
    let root =
        crowdb_protocol::port::namespace::RuntimeNamespace::ephemeral("chunk-kv-startup-exit").unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_crowdb-chunk-kv-server"))
        .arg("--config")
        .arg(root.root().join("missing.toml"))
        .arg("--log-dir")
        .arg(root.root().join("log"))
        .arg("--log")
        .output()
        .unwrap();
    assert!(!output.status.success());
    let diagnostic = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        diagnostic.contains("failed to load configuration"),
        "{diagnostic}"
    );
}
