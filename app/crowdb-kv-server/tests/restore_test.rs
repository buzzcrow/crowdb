// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Tests for restore mode (R104): local-disk scan + restart rebuild.

use crowdb_kv_server::recovery::restore::{group0_exists, scan_local_groups};

// ── Unit tests: scan_local_groups / group0_exists ───────────────

#[tokio::test]
async fn scan_local_groups_multi_store() {
    let dir = crowdb_test_harness::test_dirs::tempdir_in_test_data("restore");
    let waldata = dir.path().join("waldata");
    // store0/group0, store1/group1, store1/group2
    tokio::fs::create_dir_all(waldata.join("store0").join("group0"))
        .await
        .unwrap();
    tokio::fs::create_dir_all(waldata.join("store1").join("group1"))
        .await
        .unwrap();
    tokio::fs::create_dir_all(waldata.join("store1").join("group2"))
        .await
        .unwrap();
    // stray file at waldata root + a non-matching dir
    tokio::fs::write(waldata.join("README"), b"ignore me")
        .await
        .unwrap();
    tokio::fs::create_dir_all(waldata.join("notastore"))
        .await
        .unwrap();
    // store3 with no groups
    tokio::fs::create_dir_all(waldata.join("store3")).await.unwrap();

    let groups = scan_local_groups(&waldata).await.unwrap();
    assert_eq!(
        groups,
        vec![
            crowdb_kv_server::recovery::restore::LocalGroup {
                store_id: 0,
                group_id: 0
            },
            crowdb_kv_server::recovery::restore::LocalGroup {
                store_id: 1,
                group_id: 1
            },
            crowdb_kv_server::recovery::restore::LocalGroup {
                store_id: 1,
                group_id: 2
            },
        ]
    );
}

#[tokio::test]
async fn scan_local_groups_missing_dir_is_empty() {
    let dir = crowdb_test_harness::test_dirs::tempdir_in_test_data("restore");
    let waldata = dir.path().join("waldata"); // not created
    let groups = scan_local_groups(&waldata).await.unwrap();
    assert!(groups.is_empty());
}

#[tokio::test]
async fn group0_exists_true_and_false() {
    let dir = crowdb_test_harness::test_dirs::tempdir_in_test_data("restore");
    let waldata = dir.path().join("waldata");
    assert!(!group0_exists(&waldata));
    tokio::fs::create_dir_all(waldata.join("store0").join("group0"))
        .await
        .unwrap();
    assert!(group0_exists(&waldata));
}

#[tokio::test]
async fn group0_exists_ignores_other_stores() {
    let dir = crowdb_test_harness::test_dirs::tempdir_in_test_data("restore");
    let waldata = dir.path().join("waldata");
    tokio::fs::create_dir_all(waldata.join("store1").join("group1"))
        .await
        .unwrap();
    assert!(!group0_exists(&waldata));
}
