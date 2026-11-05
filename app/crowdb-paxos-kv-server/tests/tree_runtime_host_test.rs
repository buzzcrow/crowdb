// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::Bytes;
use crowdb_paxos_kv::cluster::PxLocalReplicaRole;
use crowdb_paxos_kv::common::config::PaxosKvConfig;
use crowdb_paxos_kv_server::recovery::startup::{create_group_with_wal, create_group_with_wal_on_runtime};
use crowdb_paxos_kv_server::store_registry::PaxosKvStoreRegistry;

fn config(root: &std::path::Path) -> PaxosKvConfig {
    let mut config = PaxosKvConfig {
        wal_backend: "mem-block".to_owned(),
        crowtree_backend: "mem-block".to_owned(),
        wal_root: root.join("wal"),
        config_root: root.join("config"),
        data_root: root.join("tree"),
        log_dir: root.join("log").to_string_lossy().into_owned(),
        ..Default::default()
    };
    config.tree_runtime.host.workers = Some(1);
    config
}

#[test]
fn incompatible_proposal_window_is_rejected_before_startup() {
    let mut config = PaxosKvConfig::default();
    config.tree_runtime.tree.cpu_queue_limit = Some(16);
    let Err(error) = PaxosKvStoreRegistry::try_with_config(config) else {
        panic!("proposal window must fit its paid Tree lane");
    };
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    assert!(error.to_string().contains("max_inflight_proposals"));
}

#[tokio::test(flavor = "current_thread")]
async fn standalone_constructor_uses_runtime_without_a_mode_flag() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("tree-runtime-host");
    let registry = PaxosKvStoreRegistry::try_with_config(config(root.path())).unwrap();
    let result = create_group_with_wal(
        1,
        1,
        1,
        PxLocalReplicaRole::Leader,
        &registry.config,
        Arc::clone(&registry.wal_backend),
        registry.crowtree_backend,
    )
    .await;
    let group = result.unwrap();
    let engine = group.local_replica().learner.engine();
    assert!(engine.requires_contiguous_apply());
    let permit = engine.admit_proposal(Bytes::new()).await.unwrap().unwrap();
    permit.apply(1).await.unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn every_group_uses_the_same_registry_owned_native_host() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("tree-runtime-host");
    let registry = PaxosKvStoreRegistry::try_with_config(config(root.path())).unwrap();
    let mut groups = Vec::new();
    for group_id in 1..=2 {
        groups.push(
            create_group_with_wal_on_runtime(
                1,
                group_id,
                1,
                PxLocalReplicaRole::Leader,
                &registry.config,
                Arc::clone(&registry.wal_backend),
                registry.crowtree_backend,
                registry.tree_runtime.clone(),
            )
            .await
            .unwrap(),
        );
    }
    for group in &groups {
        let engine = group.local_replica().learner.engine();
        assert!(engine.requires_contiguous_apply());
        let permit = engine.admit_proposal(Bytes::new()).await.unwrap().unwrap();
        permit.apply(1).await.unwrap();
    }
    registry.tree_runtime.stop_admission();
    for group in &groups {
        // This one host transition must be visible in both independent Trees.
        assert!(group
            .local_replica()
            .learner
            .engine()
            .admit_proposal(Bytes::new())
            .await
            .is_err());
    }
}
