// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/node_setup.rs"]
mod node_setup;
use crowdb_monitor::IcebergBootstrap;
use crowdb_paxos_kv_client::{GetOutcome, ReadMode};
use crowdb_protocol::mgmt::node::setup::{NodeSetupChoices, NodeSetupRequest};
use serde_json::json;
use std::{fs, path::Path};

const KEY: &[u8] = b"/deployment/console-catalog-bootstrap";

async fn authority(root: &Path) -> node_setup::TestAuthority {
    let authority = node_setup::TestAuthority::start(
        root,
        &NodeSetupRequest {
            version: 1,
            discovery_id: uuid::Uuid::new_v4().to_string(),
            operation_id: uuid::Uuid::new_v4().to_string(),
            choices: NodeSetupChoices::default(),
        },
    )
    .await;
    assert_eq!(authority.binding.node_id, 2);
    assert_eq!(authority.registry.nodes.len(), 1);
    authority
}

#[tokio::test]
async fn concurrent_node_backends_reserve_one_read_only_replayable_catalog_operation() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("shared-console-catalog");
    let other = root.path().join("other-node");
    fs::create_dir(&other).unwrap();
    let authority = authority(root.path()).await;
    let (left, right) = tokio::join!(
        IcebergBootstrap::shared_console_operations_for_tests(root.path(), &authority.kv),
        IcebergBootstrap::shared_console_operations_for_tests(&other, &authority.kv),
    );
    let original = left.unwrap();
    assert_eq!(original, right.unwrap());
    assert_eq!(original.0.get_version_num(), 7);
    assert_eq!(original.1.get_version_num(), 7);
    assert_ne!(original.0, original.1);
    let before = authority
        .kv
        .get(0, 0, KEY, ReadMode::Linearizable, None)
        .await
        .unwrap();
    assert_eq!(
        IcebergBootstrap::shared_console_operations_for_tests(&other, &authority.kv)
            .await
            .unwrap(),
        original
    );
    let after = authority
        .kv
        .get(0, 0, KEY, ReadMode::Linearizable, None)
        .await
        .unwrap();
    match (before, after) {
        (
            GetOutcome::Found {
                value: left,
                revision: a,
            },
            GetOutcome::Found {
                value: right,
                revision: b,
            },
        ) => {
            assert_eq!(left, right);
            assert_eq!(a, b);
        }
        _ => panic!("shared reservation disappeared"),
    }
    assert!(!root.path().join("console-iceberg-bootstrap.json").exists());
    assert!(!other.join("console-iceberg-bootstrap.json").exists());
}

#[tokio::test]
async fn an_existing_local_catalog_operation_is_retained_and_conflicting_nodes_do_not_rewrite_it() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("shared-console-existing");
    let authority = authority(root.path()).await;
    let initialize = uuid::Uuid::now_v7();
    let activate = uuid::Uuid::now_v7();
    let path = root.path().join("console-iceberg-bootstrap.json");
    let journal = serde_json::to_vec(
        &json!({"initialize":initialize,"activate":activate,"catalog":null,"activated":false}),
    )
    .unwrap();
    fs::write(&path, &journal).unwrap();
    assert_eq!(
        IcebergBootstrap::shared_console_operations_for_tests(root.path(), &authority.kv)
            .await
            .unwrap(),
        (initialize, activate)
    );
    assert_eq!(fs::read(&path).unwrap(), journal);
    let other = root.path().join("other-node");
    fs::create_dir(&other).unwrap();
    let foreign = serde_json::to_vec(&json!({"initialize":uuid::Uuid::now_v7(),"activate":uuid::Uuid::now_v7(),"catalog":null,"activated":false})).unwrap();
    let other_path = other.join("console-iceberg-bootstrap.json");
    fs::write(&other_path, &foreign).unwrap();
    let error = IcebergBootstrap::shared_console_operations_for_tests(&other, &authority.kv)
        .await
        .unwrap_err();
    assert!(error.to_string().contains("differs from Group 0"), "{error}");
    assert_eq!(fs::read(&other_path).unwrap(), foreign);
    assert_eq!(
        IcebergBootstrap::shared_console_operations_for_tests(root.path(), &authority.kv)
            .await
            .unwrap(),
        (initialize, activate)
    );
}

#[tokio::test]
async fn corrupt_or_unsupported_shared_catalog_authority_is_never_replaced() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("shared-console-corrupt");
    let authority = authority(root.path()).await;
    for value in [
        b"broken".to_vec(),
        serde_json::to_vec(
            &json!({"version":2,"initialize":uuid::Uuid::now_v7(),"activate":uuid::Uuid::now_v7()}),
        )
        .unwrap(),
    ] {
        authority.kv.put(0, 0, KEY, &value, None).await.unwrap();
        assert!(
            IcebergBootstrap::shared_console_operations_for_tests(root.path(), &authority.kv)
                .await
                .is_err()
        );
        let GetOutcome::Found { value: retained, .. } = authority
            .kv
            .get(0, 0, KEY, ReadMode::Linearizable, None)
            .await
            .unwrap()
        else {
            panic!("shared authority disappeared")
        };
        assert_eq!(retained.as_ref(), value.as_slice());
        assert!(!root.path().join("console-iceberg-bootstrap.json").exists());
    }
}
