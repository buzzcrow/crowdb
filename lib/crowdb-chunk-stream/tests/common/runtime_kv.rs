// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_kv::cluster::{
    group::PxGroup, kv_server::KvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole,
};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient};
use std::sync::Arc;

pub struct TestRuntimeKv(Arc<PxKvStore>);

impl Drop for TestRuntimeKv {
    fn drop(&mut self) {
        self.0.stop();
    }
}

pub async fn runtime_kv() -> (TestRuntimeKv, Arc<CrowdbKvClient>) {
    let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
    for group in 0..=1 {
        store.add_group(PxGroup::new(
            group,
            PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
        ));
    }
    store.start().await.unwrap();
    let kv = Arc::new(CrowdbKvClient::new(ClientConfig::new(vec![])));
    for group in 0..=1 {
        kv.seed_leader(0, group, store.listen_addr().unwrap().to_string());
    }
    (TestRuntimeKv(store), kv)
}
