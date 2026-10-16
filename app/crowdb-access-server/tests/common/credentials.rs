// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_access_s3::auth::{CredentialCipher, MasterKey};
use crowdb_access_server::credentials::CredentialAuthority;
use crowdb_kv::cluster::{
    group::PxGroup, kv_server::KvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole,
};
use crowdb_kv_client::{ClientConfig, CrowdbKvClient};
use std::sync::Arc;

pub struct TestCredentials {
    store: Arc<PxKvStore>,
    pub control: Arc<CrowdbKvClient>,
    cipher: Arc<CredentialCipher>,
}

impl TestCredentials {
    pub async fn start() -> Self {
        let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
        store.add_group(PxGroup::new(
            0,
            PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
        ));
        store.start().await.unwrap();
        let client = CrowdbKvClient::new(ClientConfig::new(vec![]));
        client.seed_leader(0, 0, store.listen_addr().unwrap().to_string());
        let key = MasterKey::from_hex(&"11".repeat(32)).unwrap();
        Self {
            store,
            control: Arc::new(client),
            cipher: Arc::new(CredentialCipher::new(&key)),
        }
    }
    pub fn authority(&self) -> CredentialAuthority {
        CredentialAuthority::new(Arc::clone(&self.control), Arc::clone(&self.cipher))
    }
}

impl Drop for TestCredentials {
    fn drop(&mut self) {
        self.store.stop();
    }
}
