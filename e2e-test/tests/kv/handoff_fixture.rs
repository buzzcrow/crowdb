// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_paxos_kv_client::{ChunkSlotMapClient, PaxosKvClient};
use crowdb_protocol::chunk_slot::{
    ChunkServiceHandoff, ChunkServiceIncarnation, ChunkSlot, ChunkSlotAuthority, ChunkSlotBootstrap,
    ChunkSlotTransfer, ChunkStorageGroup,
};
use crowdb_protocol::key::{ChunkSlotFenceKey, TextKey};
use std::sync::Arc;

pub(super) struct TestHandoffCluster {
    pub(super) kv: Arc<PaxosKvClient>,
}

impl TestHandoffCluster {
    pub(super) async fn start() -> Self {
        let kv = Arc::new(super::client_data::fixture_client().await);
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
        Self { kv }
    }

    pub(super) fn plan(target_id: u64) -> ChunkServiceHandoff {
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
