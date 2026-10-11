// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[test]
fn rejected_profile_reports_the_operator_error_and_exits_unsuccessfully() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("monitor-cli-validation");
    let profile = root.path().join("invalid.toml");
    std::fs::write(&profile, "invalid = true\n").unwrap();
    let output = std::process::Command::new(env!("CARGO_BIN_EXE_crowdb-monitor"))
        .arg("validate")
        .arg(profile)
        .output()
        .unwrap();
    assert!(!output.status.success());
    let error = String::from_utf8(output.stderr).unwrap();
    assert!(error.contains("failed to decode deployment profile"), "{error}");
    assert!(error.contains("unknown field `invalid`"), "{error}");
}
