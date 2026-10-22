// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_stream::{ActiveChunkDescriptor, LegacyStreamManifest, StreamExtentPageFence};
use crowdb_protocol::{chunkdb::rpc::ChunkType, common::ChunkId};

pub struct TestMigrationProgress;

impl TestMigrationProgress {
    pub fn source() -> LegacyStreamManifest {
        LegacyStreamManifest {
            metadata_group_id: 1,
            writer_epoch: 3,
            generation: 7,
            sealed_tail: 3,
            active: Some(ActiveChunkDescriptor {
                chunk_id: ChunkId {
                    high: (ChunkType::Stream as u64) << 56,
                    low: 71,
                },
                physical_start: 37,
                logical_start: 3,
                acknowledged_cursor: 37,
                capacity: 1024 * 1024,
            }),
            extent_pages: vec![StreamExtentPageFence {
                page_index: 11,
                first_logical: 0,
                end_logical: 3,
            }],
            ..LegacyStreamManifest::default()
        }
    }
}
