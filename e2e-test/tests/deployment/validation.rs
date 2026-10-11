// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

fn rejected(expected: &str) {
    let root = std::path::PathBuf::from(std::env::var("CROWDB_E2E_ARTIFACTS").unwrap());
    let state: serde_json::Value =
        serde_json::from_slice(&std::fs::read(root.join("rejected-state.json")).unwrap()).unwrap();
    assert_eq!(state["Status"], "exited");
    assert_ne!(state["ExitCode"], 0);
    let log = std::fs::read_to_string(root.join("rejection.log")).unwrap();
    assert!(
        log.contains(expected),
        "release rejection did not report {expected}: {log}"
    );
}

#[test]
#[ignore = "requires the owned invalid-profile release container"]
fn invalid_profile_is_rejected() {
    rejected("failed to decode deployment profile");
}

#[test]
#[ignore = "requires the owned invalid-manifest release container"]
fn invalid_manifest_is_rejected() {
    rejected("bootstrap manifest cannot be decoded");
}
