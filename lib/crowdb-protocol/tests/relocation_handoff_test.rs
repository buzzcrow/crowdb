// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::chunk_task::relocation_operation_id;
use crowdb_protocol::chunkdb::rpc::{RelocateSegmentHandoffRequest, RelocationHandoffDisposition};
use crowdb_protocol::common::{ChunkId, DiskId};
use crowdb_protocol::diskdb::rpc::Segment;
use crowdb_protocol::diskdb::rpc::{
    RelocationJournalPhase, RelocationJournalValue, RELOCATION_JOURNAL_SCHEMA_VERSION,
};

#[test]
fn relocation_handoff_preserves_exact_incarnations() {
    let chunk_id = ChunkId { high: 8, low: 80 };
    let source = Segment {
        disk_id: Some(DiskId { high: 1, low: 2 }),
        zone_index: 3,
        unit_offset: 4,
        unit_count: 5,
        owner_chunk: Some(chunk_id),
        allocation_ts: 6,
    };
    let target = Segment {
        disk_id: Some(DiskId { high: 7, low: 8 }),
        zone_index: 9,
        unit_offset: 10,
        unit_count: 5,
        owner_chunk: Some(chunk_id),
        allocation_ts: 11,
    };
    let request = RelocateSegmentHandoffRequest {
        operation_id: Some(ChunkId { high: 12, low: 13 }),
        chunk_id: Some(chunk_id),
        source: Some(source),
        target: Some(target),
    };

    let bytes = bincode::serialize(&request).unwrap();
    assert_eq!(
        bincode::deserialize::<RelocateSegmentHandoffRequest>(&bytes).unwrap(),
        request
    );
}

#[test]
fn relocation_handoff_outcomes_are_stable() {
    for (raw, expected) in [
        (0, RelocationHandoffDisposition::Accepted),
        (1, RelocationHandoffDisposition::Published),
        (2, RelocationHandoffDisposition::Stale),
        (3, RelocationHandoffDisposition::Rejected),
    ] {
        assert_eq!(RelocationHandoffDisposition::try_from(raw), Ok(expected));
        assert_eq!(i32::from(expected), raw);
    }
    assert!(RelocationHandoffDisposition::try_from(4).is_err());
}

#[test]
fn relocation_journal_round_trips_every_identity_and_phase() {
    let request = RelocateSegmentHandoffRequest {
        operation_id: Some(ChunkId { high: 1, low: 2 }),
        chunk_id: Some(ChunkId { high: 3, low: 4 }),
        source: Some(Segment {
            disk_id: Some(DiskId { high: 5, low: 6 }),
            zone_index: 7,
            unit_offset: 8,
            unit_count: 9,
            owner_chunk: Some(ChunkId { high: 3, low: 4 }),
            allocation_ts: 10,
        }),
        target: Some(Segment {
            disk_id: Some(DiskId { high: 11, low: 12 }),
            zone_index: 13,
            unit_offset: 14,
            unit_count: 9,
            owner_chunk: Some(ChunkId { high: 3, low: 4 }),
            allocation_ts: 15,
        }),
    };
    let value = RelocationJournalValue {
        schema_version: RELOCATION_JOURNAL_SCHEMA_VERSION,
        operation_id: request.operation_id,
        owner_chunk: request.chunk_id,
        source: request.source,
        target: request.target,
        target_disk_group_id: 18,
        unit_size: 4096,
        phase: RelocationJournalPhase::TargetConfirmed.into(),
        created_at_ms: 16,
        updated_at_ms: 17,
        last_error: "transient owner timeout".into(),
    };

    let bytes = bincode::serialize(&value).unwrap();
    assert_eq!(
        bincode::deserialize::<RelocationJournalValue>(&bytes).unwrap(),
        value
    );
}

#[test]
fn relocation_operation_identity_changes_with_source_incarnation() {
    let mut source = Segment {
        disk_id: Some(DiskId { high: 1, low: 2 }),
        zone_index: 3,
        unit_offset: 4,
        unit_count: 5,
        owner_chunk: Some(ChunkId { high: 6, low: 7 }),
        allocation_ts: 8,
    };
    let first = relocation_operation_id(&source).unwrap();
    source.allocation_ts += 1;
    assert_ne!(relocation_operation_id(&source).unwrap(), first);
    source.disk_id = None;
    assert_eq!(relocation_operation_id(&source), None);
}
