// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_paxos_kv::cluster::{
    group::PxGroup, paxos_kv_server::PaxosKvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole,
};
use crowdb_paxos_kv_client::{ChunkSlotMapClient, ClientConfig, GetOutcome, PaxosKvClient, ReadMode};
use crowdb_protocol::{
    chunk_slot::{ChunkSlotBootstrap, ChunkStorageGroup},
    key::{ChunkSlotMapHeadKey, TextKey},
};

pub struct TestChunkSlotPublication {
    pub store: Arc<PxKvStore>,
    pub kv: Arc<PaxosKvClient>,
}

impl TestChunkSlotPublication {
    pub async fn start() -> Self {
        let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
        for group in [0, 1] {
            store.add_group(PxGroup::new(
                group,
                PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
            ));
        }
        store.start().await.unwrap();
        let kv = Arc::new(PaxosKvClient::new(ClientConfig::new(vec![])));
        for group in [0, 1] {
            kv.seed_leader(0, group, store.listen_addr().unwrap().to_string());
        }
        let maps = ChunkSlotMapClient::new(Arc::clone(&kv));
        maps.initialize_layout(&ChunkSlotBootstrap {
            service_instances: vec![1, 2],
            storage_groups: vec![ChunkStorageGroup {
                store_id: 0,
                group_id: 1,
            }],
        })
        .await
        .unwrap();
        maps.initialize_service_epochs().await.unwrap();
        Self { store, kv }
    }

    pub async fn head_revision(&self) -> u64 {
        let key = ChunkSlotMapHeadKey::Service.to_path();
        match self
            .kv
            .get(0, 0, key.as_bytes(), ReadMode::Linearizable, None)
            .await
            .unwrap()
        {
            GetOutcome::Found { revision, .. } => revision,
            GetOutcome::NotFound => panic!("service head disappeared"),
        }
    }
}

impl Drop for TestChunkSlotPublication {
    fn drop(&mut self) {
        self.store.stop();
    }
}
