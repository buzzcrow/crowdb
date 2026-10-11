// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Cached topology must not substitute for confirmed Group-0 authority.

use crowdb_console_shared::cluster::{NodeHealth, NodeStore};
use crowdb_console_shared::monitor::NodeRecord;
use crowdb_console_shared::ConsoleConfig;
use crowdb_web::{router, AppState};
use std::collections::BTreeMap;
use std::net::SocketAddr;

#[tokio::test]
async fn store_reads_reject_cached_topology_without_group0() {
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let state = AppState::with_config(ConsoleConfig::default(), None);
    let mut stores = BTreeMap::new();
    stores.insert(
        7,
        NodeStore {
            node_id: 1,
            store_id: 7,
            listen_addr: None,
            groups: Vec::new(),
        },
    );
    state
        .monitor_cache
        .set_node_report(
            1,
            NodeRecord {
                health: NodeHealth::Up,
                last_seen_ms: 1,
                stores,
                last_error: None,
                recovering: false,
            },
        )
        .await;
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });

    let client = reqwest::Client::new();
    for path in [
        "/api/stores",
        "/api/stores/7",
        "/api/stores/7/groups",
        "/api/stores/7/groups/70",
        "/api/stores/7/groups/70/replicas",
    ] {
        let response = client
            .get(format!("http://{address}{path}"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 502, "{path}");
    }
}
