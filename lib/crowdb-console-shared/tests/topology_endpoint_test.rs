// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{routing::get, Json, Router};
use crowdb_console_shared::{
    clients::http::ServerClient,
    cluster::NodeHealth,
    monitor::{legacy_topology_to_node_stores, MonitorCache, NodeRecord},
};
use crowdb_protocol::mgmt::{GroupStatus, RemoteStatus, ReplicaStatus, StoreStatus, TopologyResponse};

#[tokio::test]
async fn wildcard_topology_listeners_use_the_reporting_origin_before_seeding_a_leader() {
    let response = TopologyResponse {
        stores: vec![
            StoreStatus {
                store_id: 0,
                listen_addr: Some("0.0.0.0:10100".into()),
                groups: vec![GroupStatus {
                    group_id: 0,
                    leader_id: 1,
                    local_replica: ReplicaStatus {
                        id: 1,
                        role: "Leader".into(),
                        ..Default::default()
                    },
                    remotes: vec![RemoteStatus {
                        id: 2,
                        endpoint: "192.0.2.20:10101".into(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }],
                ..Default::default()
            },
            StoreStatus {
                store_id: 1,
                listen_addr: Some("[::]:11234".into()),
                ..Default::default()
            },
            StoreStatus {
                store_id: 2,
                listen_addr: Some("192.0.2.30:12345".into()),
                ..Default::default()
            },
        ],
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://localhost:{}", listener.local_addr().unwrap().port());
    let app = Router::new().route(
        "/topology",
        get(move || {
            let response = response.clone();
            async move { Json(response) }
        }),
    );
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let stores = ServerClient::new(origin).unwrap().topology().await.unwrap();
    task.abort();
    assert_eq!(stores[0].listen_addr.as_deref(), Some("localhost:10100"));
    assert_eq!(stores[1].listen_addr.as_deref(), Some("localhost:11234"));
    assert_eq!(stores[2].listen_addr.as_deref(), Some("192.0.2.30:12345"));
    assert_eq!(stores[0].groups[0].remotes[0].endpoint, "192.0.2.20:10101");
    let cache = MonitorCache::new();
    cache
        .set_node_report(
            1,
            NodeRecord {
                health: NodeHealth::Up,
                stores: legacy_topology_to_node_stores(1, &stores),
                ..Default::default()
            },
        )
        .await;
    assert_eq!(
        cache.group0_leader_endpoint().await.as_deref(),
        Some("localhost:10100")
    );
}
