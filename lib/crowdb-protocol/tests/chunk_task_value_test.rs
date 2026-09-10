// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_task::{ChunkTaskState, ChunkTaskValue, CHUNK_TASK_SCHEMA_VERSION};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::{decode_chunk_task_value, encode_chunk_task_value};

fn task_value() -> ChunkTaskValue {
    ChunkTaskValue {
        schema_version: CHUNK_TASK_SCHEMA_VERSION,
        task_id: ChunkId { high: 1, low: 2 },
        partition_id: ChunkId { high: 3, low: 4 },
        kind: 9,
        kind_version: 2,
        state: ChunkTaskState::RetryWait,
        priority: 7,
        revision: 11,
        operation_id: ChunkId { high: 5, low: 6 },
        source_revision: 12,
        created_at_ms: 13,
        updated_at_ms: 14,
        eligible_at_ms: 15,
        attempt: 2,
        max_attempts: 8,
        estimated_queue_bytes: 20 * 1024 * 1024,
        claim_owner: 16,
        claim_generation: 17,
        claim_deadline_ms: 18,
        last_error_code: 19,
        last_error: "temporary read failure".into(),
        payload: vec![1, 2, 3, 4],
    }
}

#[test]
fn task_value_round_trip() {
    let expected = task_value();
    let encoded = encode_chunk_task_value(&expected);
    assert_eq!(decode_chunk_task_value(&encoded).unwrap(), expected);
}

#[test]
fn task_value_rejects_wrong_identifier_and_malformed_buffer() {
    let mut wrong_identifier = encode_chunk_task_value(&task_value());
    wrong_identifier[4..8].copy_from_slice(b"NOPE");
    assert!(decode_chunk_task_value(&wrong_identifier).is_err());
    assert!(decode_chunk_task_value(b"CTSK").is_err());
}

#[test]
fn task_value_bounds_last_error_without_splitting_utf8() {
    let mut value = task_value();
    value.last_error = "界".repeat(400);
    let decoded = decode_chunk_task_value(&encode_chunk_task_value(&value)).unwrap();
    assert!(decoded.last_error.len() <= 1_024);
    assert!(decoded.last_error.is_char_boundary(decoded.last_error.len()));
}
