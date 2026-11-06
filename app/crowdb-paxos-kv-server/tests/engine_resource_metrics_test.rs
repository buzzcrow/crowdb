// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use crowdb_common::metrics::MetricsRunner;
use crowdb_paxos_kv::cluster::{px_kv_store::PxKvStore, PxLocalReplicaRole};
use crowdb_paxos_kv::common::config::PaxosKvConfig;
use crowdb_paxos_kv::kv::CrowdbTreeEngine;
use crowdb_paxos_kv_server::engine_collector::setup_engine_collector;
use crowdb_paxos_kv_server::recovery::startup::create_group_with_wal_on_runtime;
use crowdb_paxos_kv_server::store_registry::PaxosKvStoreRegistry;

#[tokio::test]
async fn service_collector_logs_host_and_tree_resource_windows() {
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("paxos-tree-resources");
    let config = PaxosKvConfig {
        wal_backend: "mem-block".into(),
        crowtree_backend: "mem-block".into(),
        wal_root: root.path().join("wal"),
        config_root: root.path().join("config"),
        data_root: root.path().join("tree"),
        log_dir: root.path().join("log").to_string_lossy().into_owned(),
        ..Default::default()
    };
    let registry = Arc::new(PaxosKvStoreRegistry::try_with_config(config).unwrap());
    let store = Arc::new(PxKvStore::new(1, "127.0.0.1:0".parse().unwrap()));
    let group = create_group_with_wal_on_runtime(
        1,
        1,
        1,
        PxLocalReplicaRole::Leader,
        &registry.config,
        registry.wal_backend.clone(),
        registry.crowtree_backend,
        registry.tree_runtime.clone(),
    )
    .await
    .unwrap();
    let engine = group.local_replica().learner.engine_arc();
    store.add_group_without_election(group);
    registry.add_store(1, &store);
    let dir = root.path().join("metrics");
    let file = crowdb_common::logging::open_metrics_log(&dir, "test", 10, 2).unwrap();
    let mut runner = MetricsRunner::new(file, 1);
    setup_engine_collector(&runner.registry().clone(), &registry, &mut runner);
    runner.start();
    let tree = engine.as_any().downcast_ref::<CrowdbTreeEngine>().unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        while tree.work_metrics_window().is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    runner.stop().await;
    let path = std::fs::read_dir(&dir).unwrap().next().unwrap().unwrap().path();
    let log = std::fs::read_to_string(path).unwrap();
    assert!(log.contains("host.tree.resource.cpu_workers"), "{log}");
    assert!(log.contains("s.1.g.1.tree.resource.routing"), "{log}");
    assert!(log.contains("reserved_sample_max="), "{log}");
    assert!(log.contains("max_sample_gap_ns="), "{log}");
}
