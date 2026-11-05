// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_paxos_kv::cluster::group::PxGroup;
use crowdb_paxos_kv::cluster::paxos_kv_server::PaxosKvServer;
use crowdb_paxos_kv::cluster::{PxKvStore, PxLocalReplica, PxLocalReplicaRole};
use crowdb_paxos_kv_client::{ClientConfig, PaxosKvClient};

pub struct TestOwnerFenceServer {
    pub store: Arc<PxKvStore>,
    pub client: PaxosKvClient,
    pub endpoint: String,
}

impl TestOwnerFenceServer {
    pub async fn start() -> Self {
        let store = Arc::new(PxKvStore::new(1, "127.0.0.1:0".parse().unwrap()));
        store.add_group(PxGroup::new(
            1,
            PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
        ));
        store.start().await.unwrap();
        let endpoint = store.listen_addr().unwrap().to_string();
        let client = PaxosKvClient::new(ClientConfig::new(vec![]));
        client.seed_leader(1, 1, endpoint.clone());
        Self {
            store,
            client,
            endpoint,
        }
    }
}

impl Drop for TestOwnerFenceServer {
    fn drop(&mut self) {
        self.store.stop();
    }
}
