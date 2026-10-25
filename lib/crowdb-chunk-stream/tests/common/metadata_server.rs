// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::kv_server::KvServer;
use crowdb_kv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv::cluster::px_kv_store::PxKvStore;
use crowdb_kv_client::{ClientConfig, CrowdbKvClient};

pub struct TestMetadataServer {
    pub server: Arc<PxKvStore>,
    pub client: Arc<CrowdbKvClient>,
}

impl TestMetadataServer {
    pub async fn start() -> Self {
        let server = Arc::new(PxKvStore::new(1, "127.0.0.1:0".parse().unwrap()));
        server.add_group(PxGroup::new(
            1,
            PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
        ));
        server.start().await.unwrap();
        // This fixture explicitly seeds its one real RPC endpoint; discovery is
        // outside the metadata adapter contract exercised here.
        let client = Arc::new(CrowdbKvClient::new(ClientConfig::new(Vec::new())));
        client.seed_leader(1, 1, server.listen_addr().unwrap().to_string());
        Self { server, client }
    }
}

impl Drop for TestMetadataServer {
    fn drop(&mut self) {
        self.server.stop();
    }
}
