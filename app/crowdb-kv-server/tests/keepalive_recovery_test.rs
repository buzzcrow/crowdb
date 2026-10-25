// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{extract::State, routing::get, Json, Router};
use crowdb_kv::cluster::{group::PxGroup, KvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv::common::config::CrowDBConfig;
use crowdb_kv_client::{ClientConfig, CrowdbKvClient, GetOutcome, ReadMode};
use crowdb_kv_server::{background::keepalive::KeepAliveLoop, store_registry::KvStoreRegistry};
use crowdb_protocol::common::KvServerIdentity;
use std::{sync::Arc, time::Duration};

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
