// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_test_harness::test_dirs::tempdir_in_test_data;

#[test]
fn in_memory_configuration_path_never_publishes_topology_or_bootstrap_intent() {
    use crowdb_console_shared::config::{RackEntry, ServerEntry};

    let root = tempdir_in_test_data("in-memory-web-persistence");
    let path = root.path().join("console.toml");
    let state = crowdb_web::AppState::with_config(
        crowdb_console_shared::ConsoleConfig::default(),
        Some(path.clone()),
    );
    {
        let mut config = state.config.write().unwrap();
        config.racks.push(RackEntry {
            id: 1,
            name: "r1".into(),
        });
        config
            .add_server(ServerEntry::new("n1", "http://127.0.0.1:7000"))
            .unwrap();
    }
    state.persist().unwrap();
    assert!(!path.exists());
    assert!(!root.path().join("bootstrap-intent.toml").exists());
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 0);
    assert_eq!(state.config.read().unwrap().servers[0].id, "n1");
}

#[test]
fn failed_standalone_publication_preserves_complete_previous_config() {
    use std::os::unix::fs::PermissionsExt;

    let root = tempdir_in_test_data("standalone-write-failure");
    let state = crowdb_web::AppState::open_standalone(root.path().to_path_buf()).unwrap();
    let path = root.path().join("config.json");
    let before = std::fs::read(&path).unwrap();
    state
        .config
        .write()
        .unwrap()
        .racks
        .push(crowdb_console_shared::config::RackEntry {
            id: 42,
            name: "new-rack".into(),
        });
    let permissions = std::fs::metadata(root.path()).unwrap().permissions();
    std::fs::set_permissions(root.path(), std::fs::Permissions::from_mode(0o500)).unwrap();
    let result = state.persist();
    std::fs::set_permissions(root.path(), permissions).unwrap();
    assert!(result.is_err());
    assert_eq!(std::fs::read(&path).unwrap(), before);
    assert_eq!(std::fs::read_dir(root.path()).unwrap().count(), 1);
    state.persist().unwrap();
    let reopened = crowdb_web::AppState::open_standalone(root.path().to_path_buf()).unwrap();
    assert_eq!(reopened.config.read().unwrap().racks[0].id, 42);
}
