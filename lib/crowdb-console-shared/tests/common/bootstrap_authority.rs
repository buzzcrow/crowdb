// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_console_shared::config::{ConsoleConfig, NodeEntry, RackEntry, ServerEntry};
use crowdb_console_shared::ops::OpContext;
use crowdb_protocol::common::KvServerIdentity;
use crowdb_test_harness::cluster::KvCluster;
use serde_json::json;

pub async fn context(cluster: &KvCluster) -> OpContext {
    let mut config = ConsoleConfig::default();
    config.racks.push(RackEntry {
        id: 1,
        name: String::new(),
    });
    config.nodes.push(
        serde_json::from_value::<NodeEntry>(json!({
            "id": 1, "rack_id": 1, "host": "127.0.0.1", "ssh_port": 22, "ssh_user": ""
        }))
        .unwrap(),
    );
    config.servers.push(
        serde_json::from_value::<ServerEntry>(json!({
            "id": "1", "node_id": 1, "url": cluster.mgmt_endpoints[0],
            "rpc_url": cluster.group0_leader_endpoint
        }))
        .unwrap(),
    );
    let ctx = OpContext::new(
        cluster.group0_leader_endpoint.clone(),
        cluster.mgmt_endpoints.clone(),
        config,
    );
    ctx.sysmd()
        .register_kv_server(
            KvServerIdentity {
                instance_id: 99001,
                node_id: Some(1),
            },
            &cluster.mgmt_endpoints[0],
            &[0],
            &[],
            "ok",
            "/tmp/bootstrap-node",
        )
        .await
        .unwrap();
    ctx
}
