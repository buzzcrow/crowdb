// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Pinned metadata and topology peer for Web handler component proofs.
//! Packaged service/bootstrap/authority flows belong to container acceptance.

#![allow(dead_code)]

use axum::{extract::State, routing::get, Json, Router};
use crowdb_paxos_kv::cluster::group::PxGroup;
use crowdb_paxos_kv::cluster::{PaxosKvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole};
use std::sync::Arc;

pub(crate) struct TestKvCluster {
    store: Arc<PxKvStore>,
    topology: tokio::task::JoinHandle<()>,
    pub(crate) mgmt_endpoints: Vec<String>,
    pub(crate) group0_leader_endpoint: String,
    pub(crate) group1_leader_endpoint: String,
}

impl TestKvCluster {
    pub(crate) async fn start() -> Self {
        async fn topology(State(store): State<Arc<PxKvStore>>) -> Json<serde_json::Value> {
            Json(serde_json::json!({"stores": [store.status()]}))
        }
        let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
        for id in [0, 1] {
            store.add_group(PxGroup::new(
                id,
                PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
            ));
        }
        store.start().await.expect("start pinned metadata peer");
        let endpoint = store.listen_addr().unwrap().to_string();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let management = format!("http://{}", listener.local_addr().unwrap());
        let app = Router::new()
            .route("/topology", get(topology))
            .with_state(store.clone());
        let topology = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            store,
            topology,
            mgmt_endpoints: vec![management],
            group0_leader_endpoint: endpoint.clone(),
            group1_leader_endpoint: endpoint,
        }
    }
}

impl Drop for TestKvCluster {
    fn drop(&mut self) {
        self.topology.abort();
        self.store.stop();
    }
}
