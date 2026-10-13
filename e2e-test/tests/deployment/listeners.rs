// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

fn verify(protocol: &str) {
    let root = std::path::PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap());
    let result: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("listener-failure.json")).unwrap()).unwrap();
    assert_eq!(result["protocol"], protocol);
    assert_eq!(result["state"]["Status"], "exited");
    assert_ne!(result["state"]["ExitCode"], 0);
    assert!(result["log"].as_str().unwrap().contains("Address already in use"));
}

#[test]
#[ignore = "requires an owned occupied S3 listener release profile"]
fn occupied_s3_listener_terminates_combined_access() {
    verify("s3");
}

#[test]
#[ignore = "requires an owned occupied Iceberg listener release profile"]
fn occupied_iceberg_listener_terminates_combined_access() {
    verify("iceberg");
}
