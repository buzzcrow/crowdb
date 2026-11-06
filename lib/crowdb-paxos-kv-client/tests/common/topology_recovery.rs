// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use axum::{routing::get, Json, Router};
use crowdb_paxos_kv::cluster::{
    group::PxGroup, paxos_kv_server::PaxosKvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole,
};
use crowdb_paxos_kv_client::{ClientConfig, PaxosKvClient};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

pub struct TestDiscovery {
    stores: Vec<Arc<PxKvStore>>,
    servers: Vec<tokio::task::JoinHandle<()>>,
    pub client: Arc<PaxosKvClient>,
    pub fetches: Arc<AtomicUsize>,
}

impl TestDiscovery {
    pub async fn start(transport_failure: bool, replacement: bool) -> Self {
        let mut stores = Vec::new();
        for (id, role) in [(1, PxLocalReplicaRole::Leader), (2, PxLocalReplicaRole::Follower)] {
            let store = Arc::new(PxKvStore::new(11, "127.0.0.1:0".parse().unwrap()));
            store.add_group(PxGroup::new(1, PxLocalReplica::new(id, role)));
            store.start().await.unwrap();
            stores.push(store);
        }
        let mut stale = stores[0].status();
        let failed = if transport_failure {
            "127.0.0.1:1".to_owned()
        } else {
            stores[1].listen_addr().unwrap().to_string()
        };
        stale.listen_addr = Some(failed.clone());
        let mut seeds = Vec::new();
        let mut servers = Vec::new();
        let fetches = Arc::new(AtomicUsize::new(0));
        for body in [
            stale.clone(),
            if replacement { stores[0].status() } else { stale },
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            seeds.push(format!("http://{}", listener.local_addr().unwrap()));
            let requests = Arc::clone(&fetches);
            servers.push(tokio::spawn(async move {
                let app = Router::new().route(
                    "/topology",
                    get(move || {
                        let body = body.clone();
                        requests.fetch_add(1, Ordering::Relaxed);
                        async move { Json(serde_json::json!({"stores": [body]})) }
                    }),
                );
                axum::serve(listener, app).await.unwrap();
            }));
        }
        let mut config = ClientConfig::new(seeds);
        config.retry.max_retries = 2;
        config.retry.unknown_leader_wait = Duration::from_millis(10);
        config.retry.backoff_base = Duration::from_secs(1);
        let client = Arc::new(PaxosKvClient::new(config));
        client.seed_leader(11, 1, failed);
        Self {
            stores,
            servers,
            client,
            fetches,
        }
    }

    pub async fn stop(&self) {
        for server in &self.servers {
            server.abort();
        }
        for store in &self.stores {
            store.stop();
        }
        for store in &self.stores {
            store.join().await;
        }
    }
}
