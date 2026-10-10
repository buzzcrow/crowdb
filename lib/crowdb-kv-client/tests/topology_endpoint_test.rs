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

#[tokio::test]
async fn wildcard_listener_uses_reporting_management_host_for_remote_clients() {
    let store = Arc::new(PxKvStore::new(0, "127.0.0.2:0".parse().unwrap()));
    store.add_group(PxGroup::new(
        0,
        PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
    ));
    store.start().await.unwrap();
    let port = store.listen_addr().unwrap().port();
    let app = Router::new().route(
        "/topology",
        get(move || async move {
            Json(TopologyResponse {
                stores: vec![StoreStatus {
                    store_id: 0,
                    listen_addr: Some(format!("0.0.0.0:{port}")),
                    groups: vec![GroupStatus {
                        group_id: 0,
                        leader_id: 1,
                        local_replica_id: 1,
                        local_replica: ReplicaStatus {
                            id: 1,
                            ..ReplicaStatus::default()
                        },
                        ..GroupStatus::default()
                    }],
                    ..StoreStatus::default()
                }],
            })
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.2:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let mut config = ClientConfig::new(vec![origin]);
    config.retry.max_retries = 0;
    let client = CrowdbKvClient::new(config);
    client.put(0, 0, b"remote", b"value", None).await.unwrap();
    store.stop();
    server.abort();
}
