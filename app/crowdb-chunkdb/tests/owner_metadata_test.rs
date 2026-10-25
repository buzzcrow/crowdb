// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunkdb::storage::{decode_chunk_for_tests, encode_chunk_for_tests};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkStrip, ChunkType, PlacementAssessment, PlacementPriority};
use crowdb_protocol::common::ChunkId;

#[test]
fn attributed_chunk_persistence_round_trips_owner_key() {
    let chunk = Chunk {
        id: Some(ChunkId { high: 8, low: 9 }),
        chunk_type: ChunkType::Stream as i32,
        owner_key: crowdb_protocol::chunk_stream::StreamName { high: 10, low: 11 }.chunk_owner_key(),
        strips: vec![ChunkStrip {
            placement_priority: PlacementPriority::NodeFirst as i32,
            placement_assessment: Some(PlacementAssessment {
                loss_budget: 2,
                max_fragments_per_rack: 3,
                max_fragments_per_node: 1,
                max_fragments_per_disk: 1,
                rack_protected: false,
                node_protected: true,
                disk_protected: true,
                topology_generation: 9,
                usage_fresh: true,
            }),
            placement_repair_required: true,
            ..ChunkStrip::default()
        }],
        ..Chunk::default()
    };
    let bytes = encode_chunk_for_tests(&chunk).unwrap();
    assert_eq!(&bytes[..8], b"CCHK\x02\x00\x00\x00");
    assert_eq!(decode_chunk_for_tests(&bytes).unwrap(), chunk);
}
