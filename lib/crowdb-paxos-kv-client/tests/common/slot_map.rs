// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_paxos_kv::cluster::{
    group::PxGroup, paxos_kv_server::PaxosKvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole,
};
use crowdb_paxos_kv_client::{ClientConfig, PaxosKvClient};
use crowdb_protocol::chunk_slot::{
    ChunkSlot, ChunkSlotBinding, ChunkSlotBitmap, ChunkSlotMap, ChunkSlotMapHead, CHUNK_SLOT_LAYOUT_VERSION,
};

pub struct TestSlotMap {
    store: Arc<PxKvStore>,
    pub kv: Arc<PaxosKvClient>,
}

impl TestSlotMap {
    pub async fn start() -> Self {
        let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
        store.add_group(PxGroup::new(
            0,
            PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
        ));
        store.start().await.unwrap();
        let kv = Arc::new(PaxosKvClient::new(ClientConfig::new(vec![])));
        kv.seed_leader(0, 0, store.listen_addr().unwrap().to_string());
        Self { store, kv }
    }
}

impl Drop for TestSlotMap {
    fn drop(&mut self) {
        self.store.stop();
    }
}

pub fn services(first: u64, count: u32) -> ChunkSlotMap<u64> {
    ChunkSlotMap::new(
        ChunkSlotMapHead {
            layout_version: CHUNK_SLOT_LAYOUT_VERSION,
            generation: 1,
            owner_count: count,
        },
        (first..first + u64::from(count))
            .map(|owner| ChunkSlotBinding {
                generation: 1,
                owner,
                slots: if owner == first {
                    ChunkSlot::all().collect()
                } else {
                    ChunkSlotBitmap::default()
                },
            })
            .collect(),
    )
    .unwrap()
}
