// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunkdb::storage::decode_chunk_for_tests;
use crowdb_protocol::chunkdb::rpc::{
    Chunk, ChunkStrip, ChunkType, MirrorStrip, PlacementAssessment, PlacementPriority, Strip,
    StripCleanupIntent, StripType,
};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::diskdb::rpc::Segment;
use serde::Serialize;

#[derive(Serialize)]
struct LegacyChunk {
    id: Option<ChunkId>,
    modify_ts: u64,
    state: i32,
    create_ts_ms: u64,
    sealed_ts_ms: u64,
    capacity: u32,
    sealed_length: u32,
    strips: Vec<LegacyChunkStrip>,
    chunk_type: i32,
    writer_epoch: u64,
    acknowledged_cursor: u64,
    closed_strip_sequence: Option<u32>,
    writer_lease_deadline_ms: u64,
    next_strip_sequence: u32,
    cleanup_intents: Vec<StripCleanupIntent>,
    last_strip_replacement: Option<ChunkId>,
}

#[derive(Serialize)]
struct LegacyChunkStrip {
    chunk_offset: u32,
    strip_sequence: u32,
    unit_kb: u32,
    capacity: u32,
    create_ts_ms: u64,
    sealed_ts_ms: u64,
    sealed_length: u32,
    strip_type: i32,
    strip: Option<Strip>,
    usage_bitmap: Vec<u8>,
    unavailable_segments: Vec<Segment>,
}

#[test]
fn legacy_persisted_chunk_decodes_as_unattributed() {
    let legacy = LegacyChunk {
        id: Some(ChunkId { high: 1, low: 2 }),
        modify_ts: 3,
        state: 1,
        create_ts_ms: 4,
        sealed_ts_ms: 0,
        capacity: 1024,
        sealed_length: 0,
        strips: vec![LegacyChunkStrip {
            chunk_offset: 0,
            strip_sequence: 0,
            unit_kb: 4,
            capacity: 4,
            create_ts_ms: 4,
            sealed_ts_ms: 0,
            sealed_length: 0,
            strip_type: StripType::Mirror as i32,
            strip: Some(Strip::MirrorStrip(MirrorStrip { segments: Vec::new() })),
            usage_bitmap: Vec::new(),
            unavailable_segments: Vec::new(),
        }],
        chunk_type: ChunkType::Wal as i32,
        writer_epoch: 5,
        acknowledged_cursor: 6,
        closed_strip_sequence: None,
        writer_lease_deadline_ms: 7,
        next_strip_sequence: 0,
        cleanup_intents: Vec::new(),
        last_strip_replacement: None,
    };
    let bytes = bincode::serialize(&legacy).unwrap();
    let decoded = decode_chunk_for_tests(&bytes).unwrap();
    assert_eq!(decoded.id, legacy.id);
    assert_eq!(decoded.modify_ts, legacy.modify_ts);
    assert!(decoded.owner_key.is_empty());
    assert_eq!(decoded.strips.len(), 1);
    assert!(decoded.strips[0].placement_assessment.is_none());
    assert!(!decoded.strips[0].placement_repair_required);
}

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
    let bytes = bincode::serialize(&chunk).unwrap();
    assert_eq!(decode_chunk_for_tests(&bytes).unwrap(), chunk);
}
