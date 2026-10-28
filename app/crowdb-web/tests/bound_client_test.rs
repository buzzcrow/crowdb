// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/memory_kv.rs"]
mod memory_kv;

use crowdb_console_shared::ConsoleConfig;
use crowdb_paxos_kv_client::{ClientConfig, GetOutcome, PaxosKvClient, ReadMode};
use crowdb_protocol::mgmt::{node::NodeBinding, SystemBootstrapIdentity};
use crowdb_web::AppState;

#[tokio::test]
async fn bound_operations_discover_group0_without_a_warmed_console_projection() {
    let cluster = memory_kv::TestKvCluster::start().await;
    let kv = PaxosKvClient::new(ClientConfig::new(cluster.mgmt_endpoints.clone()));
    kv.seed_leader(0, 0, cluster.group0_leader_endpoint.clone());
    kv.put(0, 0, b"binding-probe", b"committed", None).await.unwrap();
    let root = crowdb_test_harness::test_dirs::tempdir_in_test_data("bound-client");
    let state = AppState::with_runtime_root(ConsoleConfig::default(), root.path().join("console"))
        .with_node_monitor("http://127.0.0.1:1")
        .unwrap();
    // Bootstrap can create the shared client before the binding is committed.
    let _ = state.kv_client().await;
    std::fs::write(
        root.path().join("node-binding.json"),
        serde_json::to_vec(&NodeBinding {
            node_id: 1,
            bootstrap: SystemBootstrapIdentity {
                cluster_id: "bound-client".into(),
                operation_id: "bootstrap".into(),
                configuration_digest: "a".repeat(64),
            },
            management_seeds: cluster.mgmt_endpoints.clone(),
        })
        .unwrap(),
    )
    .unwrap();
    for cold in [false, true] {
        if cold {
            state.clear_cluster_clients().await;
        }
        assert!(state.monitor_cache.snapshot().await.is_empty());
        assert!(state.config.read().unwrap().servers.is_empty());
        let context = state.op_context().await.unwrap();
        let result = context
            .kv()
            .get(0, 0, b"binding-probe", ReadMode::Linearizable, None)
            .await;
        assert!(
            matches!(result, Ok(GetOutcome::Found { ref value, .. }) if value.as_ref() == b"committed"),
            "cold={cold}: {result:?}"
        );
    }
}
