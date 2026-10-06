// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_slot::{
    ChunkServiceIncarnation, ChunkSlot, ChunkSlotAuthority, ChunkSlotTransfer, ChunkStorageGroup,
};

pub struct TestServiceHandoff;

impl TestServiceHandoff {
    pub fn transfers() -> Vec<ChunkSlotTransfer> {
        [0, 1023]
            .map(|slot| ChunkSlotTransfer {
                slot: ChunkSlot::try_from(slot).unwrap(),
                previous: Some(
                    ChunkSlotAuthority::new(1, ChunkServiceIncarnation::try_from([1; 16]).unwrap(), 7)
                        .unwrap(),
                ),
                target: ChunkSlotAuthority::new(2, ChunkServiceIncarnation::try_from([2; 16]).unwrap(), 8)
                    .unwrap(),
                storage: ChunkStorageGroup {
                    store_id: 0,
                    group_id: 1,
                },
            })
            .to_vec()
    }
}
