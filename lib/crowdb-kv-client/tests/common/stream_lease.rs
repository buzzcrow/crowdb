// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_kv::cluster::{
    group::PxGroup, kv_server::KvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole,
};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient};

pub struct TestStreamLeaseKv {
    store: Arc<PxKvStore>,
    pub kv: Arc<CrowdbKvClient>,
}

impl TestStreamLeaseKv {
    pub async fn start() -> Self {
        let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
        store.add_group(PxGroup::new(
            0,
            PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
        ));
        store.start().await.unwrap();
        let kv = Arc::new(CrowdbKvClient::new(ClientConfig::new(vec![])));
        kv.seed_leader(0, 0, store.listen_addr().unwrap().to_string());
        Self { store, kv }
    }
}

impl Drop for TestStreamLeaseKv {
    fn drop(&mut self) {
        self.store.stop();
    }
}
