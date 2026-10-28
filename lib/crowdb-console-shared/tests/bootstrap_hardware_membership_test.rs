// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_console_shared::{
    config::ConsoleConfig,
    ops::{cluster::publish_topology_for_tests, OpContext},
};
use crowdb_paxos_kv::cluster::{
    group::PxGroup, paxos_kv_server::PaxosKvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole,
};
use serde_json::json;

#[tokio::test]
async fn bootstrap_publishes_complete_sorted_rack_membership_and_replay_preserves_status() {
    let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
    store.add_group(PxGroup::new(
        0,
        PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
    ));
    store.start().await.unwrap();
    let endpoint = store.listen_addr().unwrap().to_string();
    let config: ConsoleConfig = serde_json::from_value(json!({
        "rack": [{"id":1,"name":"one"},{"id":2,"name":"two"}],
        "node": [{"id":3,"rack_id":1,"host":"127.0.0.3"},
                  {"id":1,"rack_id":1,"host":"127.0.0.1"},
                  {"id":2,"rack_id":2,"host":"127.0.0.2"}]
    }))
    .unwrap();
    let ctx = OpContext::new(endpoint.clone(), vec![], config);
    ctx.seed_group0_leader(endpoint);
    publish_topology_for_tests(&ctx, &[1, 2, 3], &[]).await.unwrap();
    assert_eq!(
        ctx.sysmd().get_rack(1).await.unwrap().unwrap().node_ids,
        vec![1, 3]
    );
    assert_eq!(ctx.sysmd().get_rack(2).await.unwrap().unwrap().node_ids, vec![2]);
    ctx.sysmd()
        .set_node_status(1, 3, crowdb_protocol::common::HwStatus::Offline)
        .await
        .unwrap();
    publish_topology_for_tests(&ctx, &[1, 2, 3], &[]).await.unwrap();
    assert_eq!(
        ctx.sysmd().get_node(1, 3).await.unwrap().unwrap().status,
        crowdb_protocol::common::HwStatus::Offline as i32
    );
    assert_eq!(
        ctx.sysmd().get_rack(1).await.unwrap().unwrap().node_ids,
        vec![1, 3]
    );
    store.stop();
}
