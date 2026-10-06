// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::kv_server::KvServer;
use crowdb_kv::cluster::{PxKvStore, PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv_client::{ChunkSlotMapClient, ClientConfig, CrowdbKvClient};
use crowdb_protocol::chunk_slot::{
    ChunkServiceHandoff, ChunkServiceIncarnation, ChunkSlot, ChunkSlotAuthority, ChunkSlotBootstrap,
    ChunkSlotTransfer, ChunkStorageGroup,
};
use crowdb_protocol::key::{ChunkSlotFenceKey, TextKey};
use std::sync::Arc;

pub struct TestHandoffCluster {
    store: Arc<PxKvStore>,
    pub kv: Arc<CrowdbKvClient>,
}

impl TestHandoffCluster {
    pub async fn start() -> Self {
        let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
        for group in [0, 1] {
            store.add_group(PxGroup::new(
                group,
                PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
            ));
        }
        store.start().await.unwrap();
        let kv = Arc::new(CrowdbKvClient::new(ClientConfig::new(vec![])));
        for group in [0, 1] {
            kv.seed_leader(0, group, store.listen_addr().unwrap().to_string());
        }
        ChunkSlotMapClient::new(Arc::clone(&kv))
            .initialize_layout(&ChunkSlotBootstrap {
                service_instances: vec![1, 2],
                storage_groups: vec![ChunkStorageGroup {
                    store_id: 0,
                    group_id: 1,
                }],
            })
            .await
            .unwrap();
        for transfer in &Self::plan(2).record().transfers {
            let path = ChunkSlotFenceKey { slot: transfer.slot }.to_path();
            kv.put_cas(
                0,
                1,
                path.as_bytes(),
                &transfer.previous.unwrap().to_fence_value(),
                0,
            )
            .await
            .unwrap();
        }
        Self { store, kv }
    }

    pub fn plan(target_id: u64) -> ChunkServiceHandoff {
        let transfers = [0, 1]
            .map(|slot| ChunkSlotTransfer {
                slot: ChunkSlot::try_from(slot).unwrap(),
                previous: Some(
                    ChunkSlotAuthority::new(1, ChunkServiceIncarnation::try_from([1; 16]).unwrap(), 7)
                        .unwrap(),
                ),
                target: ChunkSlotAuthority::new(
                    target_id,
                    ChunkServiceIncarnation::try_from([2; 16]).unwrap(),
                    8,
                )
                .unwrap(),
                storage: ChunkStorageGroup {
                    store_id: 0,
                    group_id: 1,
                },
            })
            .to_vec();
        ChunkServiceHandoff::prepare(1, 1, transfers).unwrap()
    }
}

impl Drop for TestHandoffCluster {
    fn drop(&mut self) {
        self.store.stop();
    }
}
