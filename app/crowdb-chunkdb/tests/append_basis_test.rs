// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunkdb::lifecycle::LifecycleHandler;
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkState, ChunkStrip};

#[test]
fn pending_allocation_preserves_progress_but_rejects_changed_ownership_or_layout() {
    let observed = Chunk {
        state: ChunkState::Active as i32,
        writer_epoch: 9,
        owner_key: b"stream/test".to_vec(),
        modify_ts: 10,
        acknowledged_cursor: 4096,
        writer_lease_deadline_ms: 100,
        capacity: 1024,
        next_strip_sequence: 1,
        strips: vec![ChunkStrip {
            capacity: 1024,
            ..ChunkStrip::default()
        }],
        ..Chunk::default()
    };
    let current = Chunk {
        modify_ts: 12,
        acknowledged_cursor: 12288,
        writer_lease_deadline_ms: 200,
        ..observed.clone()
    };
    assert!(LifecycleHandler::append_basis_unchanged_for_tests(
        &observed, &current
    ));
    let conflicts = [
        Chunk {
            writer_epoch: 10,
            ..current.clone()
        },
        Chunk {
            owner_key: b"different".to_vec(),
            ..current.clone()
        },
        Chunk {
            state: ChunkState::Sealed as i32,
            ..current.clone()
        },
        Chunk {
            capacity: 2048,
            ..current.clone()
        },
        Chunk {
            next_strip_sequence: 2,
            ..current.clone()
        },
        Chunk {
            strips: vec![],
            ..current.clone()
        },
        Chunk {
            modify_ts: 9,
            ..current.clone()
        },
        Chunk {
            acknowledged_cursor: 0,
            ..current.clone()
        },
        Chunk {
            writer_lease_deadline_ms: 99,
            ..current.clone()
        },
    ];
    for conflict in conflicts {
        assert!(
            !LifecycleHandler::append_basis_unchanged_for_tests(&observed, &conflict),
            "{conflict:?}"
        );
    }
}
