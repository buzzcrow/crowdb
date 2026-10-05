// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{routing::get, Json, Router};
use crowdb_kv::cluster::{
    group::PxGroup,
    kv_server::KvServer,
    local_replica::{PxLocalReplica, PxLocalReplicaRole},
    px_kv_store::PxKvStore,
};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient};
use crowdb_protocol::mgmt::{GroupStatus, ReplicaStatus, StoreStatus, TopologyResponse};
use std::sync::Arc;
use tokio::sync::Notify;

#[tokio::test]
async fn repeated_identical_hint_does_not_discard_an_inflight_topology_refresh() {
    let store = Arc::new(PxKvStore::new(1, "127.0.0.1:0".parse().unwrap()));
    store.add_group(PxGroup::new(
        1,
        PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
    ));
    store.start().await.unwrap();
    let endpoint = store.listen_addr().unwrap().to_string();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let signal = started.clone();
    let gate = release.clone();
    let app = Router::new().route(
        "/topology",
        get(move || {
            let signal = signal.clone();
            let gate = gate.clone();
            let endpoint = endpoint.clone();
            async move {
                signal.notify_one();
                gate.notified().await;
                Json(TopologyResponse {
                    stores: vec![StoreStatus {
                        store_id: 1,
                        listen_addr: Some(endpoint),
                        groups: vec![GroupStatus {
                            group_id: 1,
                            leader_id: 1,
                            local_replica_id: 1,
                            local_replica: ReplicaStatus {
                                id: 1,
                                ..Default::default()
                            },
                            ..Default::default()
                        }],
                        ..Default::default()
                    }],
                })
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let seed = format!("http://{}", listener.local_addr().unwrap());
    let management = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut config = ClientConfig::new(vec![seed]);
    config.retry.max_retries = 0;
    let client = Arc::new(CrowdbKvClient::new(config));
    client.seed_leader(1, 1, "127.0.0.1:1".into());
    let refreshing = client.clone();
    let refresh = tokio::spawn(async move { refreshing.refresh_topology().await });
    started.notified().await;
    client.seed_leader(1, 1, "127.0.0.1:1".into());
    release.notify_one();
    refresh.await.unwrap().unwrap();
    let result = client.put(1, 1, b"published", b"value", None).await;
    management.abort();
    store.stop();
    assert!(
        result.is_ok(),
        "unchanged hint suppressed the discovered leader: {result:?}"
    );
}
