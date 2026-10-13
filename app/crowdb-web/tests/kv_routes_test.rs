// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Cached topology cannot authorize reads without live Group 0.

use crowdb_console_shared::cluster::NodeHealth;
use crowdb_console_shared::config::{
    GroupEntry, NodeEntry, RackEntry, ReplicaEntry, ServerEntry, ServiceType,
};
use crowdb_console_shared::monitor::NodeRecord;
use crowdb_console_shared::ConsoleConfig;
use crowdb_web::{router, AppState};
use std::net::SocketAddr;
use std::time::Duration;

#[tokio::test]
async fn kv_get_returns_502_when_leader_unreachable() {
    use crowdb_console_shared::cluster::{LocalReplicaInfo, NodeGroup, NodeStore, ReplicaRole, ReplicaState};
    use std::collections::BTreeMap;
    // Pick a free port, drop the listener: nothing accepts on it now.
    let dead = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let dead_port = dead.local_addr().unwrap().port();
    drop(dead);

    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let web = listener.local_addr().unwrap();

    let mut cfg = ConsoleConfig::default();
    cfg.racks.push(RackEntry {
        id: 1,
        name: "r1".into(),
    });
    cfg.nodes.push(NodeEntry {
        id: 1,
        rack_id: 1,
        host: "127.0.0.1".into(),
        ssh_port: 22,
        ssh_user: String::new(),
        ssh_key: None,
        ssh_password: None,
        ssh_credential_ref: None,
    });
    // The node has a configured rpc_url, but the port is dead.
    cfg.add_server(ServerEntry {
        id: "n1".to_string(),
        url: format!("http://127.0.0.1:{dead_port}"),
        node_id: Some(1),
        rpc_url: Some(format!("http://127.0.0.1:{dead_port}")),
        rest_port: None,
        rpc_port: None,
        auto_start: true,
        binary: None,
        election_profile: None,
        pid: None,
        service_type: ServiceType::PaxosKv,
        rpc_workers: None,
        no_fsync: false,
    })
    .unwrap();
    cfg.groups.push(GroupEntry {
        store_id: 7,
        group_id: 70,
        replicas: vec![ReplicaEntry {
            replica_id: 1,
            node_id: 1,
        }],
    });
    let state = AppState::with_config(cfg, None);

    // Even with a locally persisted group and a cached leader, the KV
    // request must not use either when Group 0 is unavailable.
    let mut stores = BTreeMap::new();
    stores.insert(
        7,
        NodeStore {
            node_id: 1,
            store_id: 7,
            listen_addr: None,
            groups: vec![NodeGroup {
                node_id: 1,
                store_id: 7,
                group_id: 70,
                local: LocalReplicaInfo {
                    replica_id: 1,
                    role: ReplicaRole::Leader,
                    state: ReplicaState::Running,
                    engine_healthy: true,
                    crowtree_stats: None,
                    election: None,
                },
                remotes: vec![],
                leader_hint: Some(1),
                read_state: None,
            }],
        },
    );
    let rec = NodeRecord {
        health: NodeHealth::Up,
        last_seen_ms: 1,
        stores,
        last_error: None,
        recovering: false,
    };
    state.monitor_cache.set_node_report(1, rec).await;

    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(50)).await;

    let http = reqwest::Client::new();
    let resp = http
        .get(format!("http://{web}/api/stores/7/groups/70/kv/get?key=anything"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), 502);
    let endpoint = http
        .get(format!("http://{web}/api/stores/7/groups/70/endpoint"))
        .send()
        .await
        .unwrap();
    assert_eq!(endpoint.status(), 502);
}
