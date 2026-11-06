// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{extract::State, routing::get, Json, Router};
use crowdb_paxos_kv::cluster::{
    group::PxGroup, paxos_kv_server::PaxosKvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole,
};
use crowdb_paxos_kv_client::{ClientConfig, PaxosKvClient};
use std::sync::Arc;

async fn seed(store: Arc<PxKvStore>) -> (String, tokio::task::JoinHandle<()>) {
    async fn topology(State(store): State<Arc<PxKvStore>>) -> Json<serde_json::Value> {
        Json(serde_json::json!({"stores": [store.status()]}))
    }
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let app = Router::new().route("/topology", get(topology)).with_state(store);
    let task = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    (format!("http://{address}"), task)
}

#[tokio::test]
async fn group_missing_on_first_seed_is_discovered_on_another_member() {
    let mut stores = Vec::new();
    for group in [1, 2] {
        let store = Arc::new(PxKvStore::new(11, "127.0.0.1:0".parse().unwrap()));
        store.add_group(PxGroup::new(
            group,
            PxLocalReplica::new(group, PxLocalReplicaRole::Leader),
        ));
        store.start().await.unwrap();
        stores.push(store);
    }
    let (first, first_task) = seed(stores[0].clone()).await;
    let (second, second_task) = seed(stores[1].clone()).await;
    let client = PaxosKvClient::new(ClientConfig::new(vec![first, second]));
    client.put(11, 1, b"first", b"member-one", None).await.unwrap();
    let written = client.put(11, 2, b"overlap", b"member-two", None).await;
    first_task.abort();
    second_task.abort();
    for store in &stores {
        store.stop();
    }
    for store in &stores {
        store.join().await;
    }
    assert!(
        written.is_ok(),
        "a healthy unrelated first seed must not hide the target group: {written:?}"
    );
}

#[tokio::test]
async fn an_unverified_empty_leader_route_does_not_hide_another_members_local_listener() {
    let store = Arc::new(PxKvStore::new(11, "127.0.0.1:0".parse().unwrap()));
    store.add_group(PxGroup::new(
        2,
        PxLocalReplica::new(2, PxLocalReplicaRole::Leader),
    ));
    store.start().await.unwrap();
    let mut blocked = store.status();
    blocked.groups[0].local_replica.id = 1;
    blocked.groups[0].local_replica_id = 1;
    blocked.groups[0].remotes = vec![crowdb_protocol::mgmt::RemoteStatus {
        id: 2,
        endpoint: String::new(),
        voting: true,
        ..Default::default()
    }];
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let first = format!("http://{}", listener.local_addr().unwrap());
    let app = Router::new().route(
        "/topology",
        get(move || {
            let blocked = blocked.clone();
            async move { Json(serde_json::json!({"stores": [blocked]})) }
        }),
    );
    let first_task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let (second, second_task) = seed(store.clone()).await;
    let client = PaxosKvClient::new(ClientConfig::new(vec![first, second]));
    let written = tokio::time::timeout(
        std::time::Duration::from_secs(2),
        client.put(11, 2, b"verified", b"leader", None),
    )
    .await;
    first_task.abort();
    second_task.abort();
    store.stop();
    store.join().await;
    assert!(
        matches!(written, Ok(Ok(_))),
        "an empty recovery route is unavailable, not a leader endpoint: {written:?}"
    );
}
