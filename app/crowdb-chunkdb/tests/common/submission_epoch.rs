// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_slot::{
    ChunkServiceIncarnation, ChunkSlot, ChunkSlotAuthority, ChunkSlotBinding, ChunkSlotMap, ChunkSlotMapHead,
};

pub struct TestEpochLayout;
impl TestEpochLayout {
    pub fn map(
        generation: u64,
        changed: ChunkSlot,
        epoch: u64,
        owner: u64,
        incarnation: u8,
    ) -> ChunkSlotMap<ChunkSlotAuthority> {
        let identity = |owner, epoch| {
            ChunkSlotAuthority::new(
                owner,
                ChunkServiceIncarnation::try_from([incarnation; 16]).unwrap(),
                epoch,
            )
            .unwrap()
        };
        let bindings = if owner == 1 && epoch == 1 {
            vec![ChunkSlotBinding {
                generation,
                owner: identity(1, 1),
                slots: ChunkSlot::all().collect(),
            }]
        } else {
            vec![
                ChunkSlotBinding {
                    generation,
                    owner: identity(1, 1),
                    slots: ChunkSlot::all().filter(|slot| *slot != changed).collect(),
                },
                ChunkSlotBinding {
                    generation,
                    owner: identity(owner, epoch),
                    slots: [changed].into_iter().collect(),
                },
            ]
        };
        ChunkSlotMap::new(
            ChunkSlotMapHead {
                layout_version: 1,
                generation,
                owner_count: u32::try_from(bindings.len()).unwrap(),
            },
            bindings,
        )
        .unwrap()
    }
}
