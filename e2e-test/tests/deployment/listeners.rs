// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

fn verify(protocol: &str, port: &str) {
    let root = std::path::PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap());
    let result: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("listener-recovery.json")).unwrap()).unwrap();
    assert_eq!(result["protocol"], protocol);
    assert_eq!(result["fixed_listener"], port);
    assert_eq!(result["event"], "child_exited");
    let before_generation = result["before"]["generation"].as_u64().unwrap();
    let after_generation = result["after"]["generation"].as_u64().unwrap();
    assert!(after_generation > before_generation);
    assert_ne!(result["after"]["pid"], result["before"]["pid"]);
}

#[test]
#[ignore = "requires an owned fixed-listener release profile"]
fn fixed_s3_listener_recovers_after_explicit_termination() {
    verify("s3", "0.0.0.0:9091");
}

#[test]
#[ignore = "requires an owned fixed-listener release profile"]
fn fixed_iceberg_listener_recovers_after_explicit_termination() {
    verify("iceberg", "0.0.0.0:9092");
}
