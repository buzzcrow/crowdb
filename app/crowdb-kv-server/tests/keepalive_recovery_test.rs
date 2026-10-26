// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{extract::State, routing::get, Json, Router};
use crowdb_kv::cluster::{group::PxGroup, KvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv::common::config::CrowDBConfig;
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, GetOutcome, ReadMode};
use crowdb_kv_server::{background::keepalive::KeepAliveLoop, store_registry::KvStoreRegistry};
use crowdb_protocol::common::KvServerIdentity;
use std::{sync::Arc, time::Duration};

struct TestDelayedTopology {
    store: Arc<PxKvStore>,
    first: std::sync::atomic::AtomicBool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

async fn delayed_topology(State(state): State<Arc<TestDelayedTopology>>) -> Json<serde_json::Value> {
    if state.first.swap(false, std::sync::atomic::Ordering::AcqRel) {
        state.entered.notify_one();
        state.release.notified().await;
    }
    Json(serde_json::json!({"stores": [state.store.status()]}))
}

#[tokio::test]
async fn slow_registration_does_not_add_its_latency_to_the_first_heartbeat_interval() {
    let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
    store.add_group(PxGroup::new(
        0,
        PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
    ));
    store.start().await.unwrap();
    let registry = Arc::new(KvStoreRegistry::with_config(CrowDBConfig::for_tests()));
    registry.add_store(0, &store);
    let delay = Arc::new(TestDelayedTopology {
        store: store.clone(),
        first: std::sync::atomic::AtomicBool::new(true),
        entered: tokio::sync::Notify::new(),
        release: tokio::sync::Notify::new(),
    });
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let management = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new()
        .route("/topology", get(delayed_topology))
        .with_state(delay.clone());
    let http = tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    let keepalive = KeepAliveLoop::spawn(
        registry,
        KvServerIdentity {
            instance_id: 7,
            node_id: Some(1),
        },
        management.clone(),
        &store.listen_addr().unwrap().to_string(),
        vec![management],
        String::new(),
        1,
    );
    tokio::time::timeout(Duration::from_secs(1), delay.entered.notified())
        .await
        .unwrap();
    // Inject discovery latency after register() has stamped its first value.
    tokio::time::sleep(Duration::from_millis(900)).await;
    delay.release.notify_one();
    let client = CrowdbKvClient::new(ClientConfig::new(Vec::new()));
    client.seed_leader(0, 0, store.listen_addr().unwrap().to_string());
    let observations = crowdb_kv_client::ServiceRegistryClient::new(client);
    let initial = tokio::time::timeout(Duration::from_millis(250), async {
        loop {
            if let Some(value) = observations.read_instance("kv-server", 7).await.unwrap() {
                break value.last_heartbeat_ms;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_millis(750), async {
        loop {
            let current = observations.read_instance("kv-server", 7).await.unwrap().unwrap();
            if current.last_heartbeat_ms > initial {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("registration latency must not postpone the next heartbeat beyond its original cadence");
    keepalive.stop().await;
    http.abort();
    store.shutdown(Duration::from_secs(1)).await;
}

#[tokio::test]
async fn a_recovered_singleton_registers_its_current_address_with_unreachable_retained_seeds() {
    async fn topology(State(store): State<Arc<PxKvStore>>) -> Json<serde_json::Value> {
        Json(serde_json::json!({"stores": [store.status()]}))
    }
    let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
    store.add_group(PxGroup::new(
        0,
        PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
    ));
    store.start().await.unwrap();
    let registry = Arc::new(KvStoreRegistry::with_config(CrowDBConfig::for_tests()));
    registry.add_store(0, &store);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let current = format!("http://{}", listener.local_addr().unwrap());
    let router = Router::new()
        .route("/topology", get(topology))
        .with_state(store.clone());
    let http = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let keepalive = KeepAliveLoop::spawn(
        registry,
        KvServerIdentity {
            instance_id: 7,
            node_id: Some(1),
        },
        current.clone(),
        "192.0.2.1:10100",
        vec!["http://127.0.0.1:1".into()],
        String::new(),
        1,
    );
    let client = CrowdbKvClient::new(ClientConfig::new(vec![current.clone()]));
    client.seed_leader(0, 0, store.listen_addr().unwrap().to_string());
    let value = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if let GetOutcome::Found { value, .. } = client
                .get(0, 0, b"/srv/kv-server/7", ReadMode::Linearizable, None)
                .await
                .unwrap()
            {
                break serde_json::from_slice::<serde_json::Value>(&value).unwrap();
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("local authority must publish the current registration without a reachable old seed");
    assert_eq!(value["rpc_endpoint"], current);
    assert_eq!(value["extra"]["kv_server"]["node_id"], 1);
    keepalive.stop().await;
    http.abort();
    store.shutdown(Duration::from_secs(1)).await;
}
