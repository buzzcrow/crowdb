// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunkdb::storage::decode_chunk_for_tests;
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkStrip, ChunkType, StripCleanupIntent};
use crowdb_protocol::common::ChunkId;
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
    strips: Vec<ChunkStrip>,
    chunk_type: i32,
    writer_epoch: u64,
    acknowledged_cursor: u64,
    closed_strip_sequence: Option<u32>,
    writer_lease_deadline_ms: u64,
    next_strip_sequence: u32,
    cleanup_intents: Vec<StripCleanupIntent>,
    last_strip_replacement: Option<ChunkId>,
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
        strips: Vec::new(),
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
}

#[test]
fn attributed_chunk_persistence_round_trips_owner_key() {
    let chunk = Chunk {
        id: Some(ChunkId { high: 8, low: 9 }),
        chunk_type: ChunkType::Stream as i32,
        owner_key: crowdb_protocol::chunk_stream::StreamName { high: 10, low: 11 }.chunk_owner_key(),
        ..Chunk::default()
    };
    let bytes = bincode::serialize(&chunk).unwrap();
    assert_eq!(decode_chunk_for_tests(&bytes).unwrap(), chunk);
}
