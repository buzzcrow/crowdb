// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Local launch persistence stays component-local; packaged storage recovery is E2E.

use crowdb_console_shared::ops::s3;
use crowdb_test_harness::test_dirs::TestDir;

fn state() -> TestDir {
    let directory = TestDir::new("s3-local-state").unwrap();
    std::fs::write(
        directory.path().join("s3-mini-cluster.json"),
        r#"{"version":1,"endpoint":"http://127.0.0.1:7500","tenant":"local"}"#,
    )
    .unwrap();
    std::fs::write(
        s3::config_path(directory.path()),
        r#"version = 1
group0_seeds = ["http://127.0.0.1:7000"]
[[service]]
id = "kv-1"
url = "http://127.0.0.1:7000"
node_id = 1
[local_launches.kv-1]
program = "crowdb-kv-server"
args = ["--management-port", "7000"]
workdir = "/owned/node1"
env_file = "/owned/private.env"
"#,
    )
    .unwrap();
    directory
}

#[test]
fn stop_persists_only_local_launch_inputs_and_preserves_data() {
    let directory = state();
    let sentinel = directory.path().join("operator-data");
    std::fs::write(&sentinel, b"preserve").unwrap();
    let (before, record) = s3::load(directory.path()).unwrap();
    let stopped = s3::stop(directory.path()).unwrap();
    assert_eq!(stopped.running_services, 0);
    assert_eq!(stopped.total_services, 1);
    let (after, reopened) = s3::load(directory.path()).unwrap();
    assert_eq!(after.servers, before.servers);
    assert_eq!(after.local_launches, before.local_launches);
    assert_eq!(reopened.endpoint, record.endpoint);
    assert!(after.racks.is_empty());
    assert!(after.nodes.is_empty());
    let saved = std::fs::read_to_string(s3::config_path(directory.path())).unwrap();
    for forbidden in ["[[rack]]", "[[node]]", "[[store]]", "CROWDB_S3_MASTER_KEY"] {
        assert!(!saved.contains(forbidden), "{saved}");
    }
    assert!(!directory.path().join("console.toml").exists());
    assert_eq!(std::fs::read(sentinel).unwrap(), b"preserve");
}

#[test]
fn inline_master_key_is_rejected_without_rewriting_state() {
    let directory = state();
    let path = s3::config_path(directory.path());
    let mut saved = std::fs::read_to_string(&path).unwrap();
    saved.push_str("\n[local_launches.kv-1.env]\nCROWDB_S3_MASTER_KEY = 'private-key'\n");
    std::fs::write(&path, &saved).unwrap();
    let error = s3::stop(directory.path()).unwrap_err();
    assert!(error.to_string().contains("inline secrets"), "{error}");
    assert_eq!(std::fs::read_to_string(path).unwrap(), saved);
}
