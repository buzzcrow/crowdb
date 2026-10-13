// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_console_shared::{deployment::PreparedBootstrap, ConsoleConfig};
use crowdb_test_harness::test_dirs::tempdir_in_test_data;
use serde_json::json;

fn config() -> ConsoleConfig {
    serde_json::from_value(json!({
        "rack": [{"id": 1, "name": "original"}],
        "node": [{"id": 1, "rack_id": 1, "host": "127.0.0.1", "ssh_user": "crowdb", "ssh_credential_ref": "id_ed25519"},
                 {"id": 2, "rack_id": 1, "host": "127.0.0.2"}],
        "server": [{"id": "kv-1", "node_id": 1, "url": "http://127.0.0.1:10000", "rpc_url": "127.0.0.1:10100"},
                   {"id": "kv-2", "node_id": 2, "url": "http://127.0.0.2:10000", "rpc_url": "127.0.0.2:10100"}]
    })).unwrap()
}

#[test]
fn bootstrap_retries_preserve_submitted_inputs_and_ignore_unused_drafts() {
    let directory = tempdir_in_test_data("deployment-sealed");
    let path = directory.path().join("operation.json");
    let mut draft = config();
    let operation = PreparedBootstrap::open(&path, &draft, &[1]).unwrap();
    assert_eq!(operation.intent.to_config().nodes.len(), 1);
    draft.racks[0].name = "edited".into();
    assert_eq!(PreparedBootstrap::open(&path, &draft, &[1]).unwrap(), operation);
    assert!(PreparedBootstrap::open(&path, &draft, &[2]).is_err());
    let mut invalid: serde_json::Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    invalid["identity"]["configuration_digest"] = json!("0".repeat(64));
    std::fs::write(&path, serde_json::to_vec(&invalid).unwrap()).unwrap();
    assert!(PreparedBootstrap::load(&path).is_err());
}

#[test]
fn bootstrap_never_persists_initial_passwords() {
    let directory = tempdir_in_test_data("deployment-private");
    let path = directory.path().join("operation.json");
    let mut draft = config();
    draft.nodes[0].ssh_password = Some("must-not-persist".into());
    assert!(PreparedBootstrap::open(&path, &draft, &[1]).is_err());
    assert!(!path.exists());
}
