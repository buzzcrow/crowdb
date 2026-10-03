// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_monitor::{IcebergBootstrap, ServerCredentials};
use serde_json::{json, Value};
use std::{fs, os::unix::fs::PermissionsExt, path::Path};

fn command(root: &Path) -> std::path::PathBuf {
    let program = root.join("catalog-command");
    fs::write(
        &program,
        r"#!/usr/bin/env python3
import json, pathlib, sys
root = pathlib.Path(__file__).parent
path = root / 'observed.json'
state = json.loads(path.read_text()) if path.exists() else {'initialized': False}
args = sys.argv[2:]
with (root / 'calls').open('a') as log: log.write(json.dumps(args) + '\n')
if args[0] == 'inspect': print(json.dumps(state))
elif args[0] == 'initialize':
    state = {'initialized': True, 'catalog_id': '11111111-1111-4111-8111-111111111111',
             'display_name': args[2], 'activation_epoch': 1, 'state': 'Ready',
             'capability_bits': '0x0000', 'root_operation_id': args[1].replace('-', '')}
    path.write_text(json.dumps(state))
    if (root / 'interrupt-initialize').exists():
        (root / 'interrupt-initialize').unlink()
        sys.exit(7)
    print('{}')
elif args[0] == 'activate':
    if (root / 'interrupt-activate').exists():
        (root / 'interrupt-activate').unlink()
        sys.exit(7)
    state['capability_bits'] = args[4]
    state['root_operation_id'] = args[1].replace('-', '')
    path.write_text(json.dumps(state))
    print('{}')
else: sys.exit(9)
",
    )
    .unwrap();
    fs::set_permissions(&program, fs::Permissions::from_mode(0o700)).unwrap();
    program
}

#[tokio::test]
async fn interrupted_initialization_and_activation_keep_request_and_catalog_identity() {
    for interruption in ["interrupt-initialize", "interrupt-activate"] {
        let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("console-catalog");
        let credentials = ServerCredentials::load_or_create(root.path()).unwrap();
        let program = command(root.path());
        fs::write(root.path().join(interruption), "").unwrap();
        let first =
            IcebergBootstrap::ensure_console_catalog(root.path(), &program, "127.0.0.1:1", &credentials)
                .await;
        assert!(first.is_err());
        let journal = root.path().join("console-iceberg-bootstrap.json");
        let reserved: Value = serde_json::from_slice(&fs::read(&journal).unwrap()).unwrap();
        IcebergBootstrap::ensure_console_catalog(root.path(), &program, "127.0.0.1:1", &credentials)
            .await
            .unwrap();
        let completed: Value = serde_json::from_slice(&fs::read(&journal).unwrap()).unwrap();
        assert_eq!(completed["initialize"], reserved["initialize"]);
        assert_eq!(completed["activate"], reserved["activate"]);
        assert_eq!(completed["activated"], true);
        let mut observation: Value =
            serde_json::from_slice(&fs::read(root.path().join("observed.json")).unwrap()).unwrap();
        observation["display_name"] = json!("renamed by operator");
        observation["activation_epoch"] = json!(2);
        observation["capability_bits"] = json!("0x0001");
        fs::write(
            root.path().join("observed.json"),
            serde_json::to_vec(&observation).unwrap(),
        )
        .unwrap();
        IcebergBootstrap::ensure_console_catalog(root.path(), &program, "127.0.0.1:1", &credentials)
            .await
            .unwrap();
        let after: Value =
            serde_json::from_slice(&fs::read(root.path().join("observed.json")).unwrap()).unwrap();
        assert_eq!(
            after, observation,
            "restart must preserve operator catalog policy"
        );
    }
}

#[tokio::test]
async fn existing_catalog_is_preserved_and_corrupt_pending_journal_is_rejected() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("console-catalog-existing");
    let credentials = ServerCredentials::load_or_create(root.path()).unwrap();
    let program = command(root.path());
    let observed = json!({"initialized": true, "catalog_id": "11111111-1111-4111-8111-111111111111",
        "display_name": "operator", "state": "Ready", "activation_epoch": 9, "capability_bits": "0x0000"});
    fs::write(
        root.path().join("observed.json"),
        serde_json::to_vec(&observed).unwrap(),
    )
    .unwrap();
    IcebergBootstrap::ensure_console_catalog(root.path(), &program, "127.0.0.1:1", &credentials)
        .await
        .unwrap();
    assert!(!root.path().join("console-iceberg-bootstrap.json").exists());
    assert_eq!(
        fs::read_to_string(root.path().join("calls"))
            .unwrap()
            .lines()
            .count(),
        1
    );
    fs::write(root.path().join("console-iceberg-bootstrap.json"), "broken").unwrap();
    assert!(
        IcebergBootstrap::ensure_console_catalog(root.path(), &program, "127.0.0.1:1", &credentials)
            .await
            .is_err()
    );
}
