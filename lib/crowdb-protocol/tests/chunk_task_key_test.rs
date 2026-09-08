// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::common::ChunkId;
use crowdb_protocol::{BinaryKey, ChunkTaskKey, LeasedChunkTaskKey, ReadyChunkTaskKey};

fn id(high: u64, low: u64) -> ChunkId {
    ChunkId { high, low }
}

#[test]
fn task_keys_round_trip() {
    let canonical = ChunkTaskKey {
        partition_id: id(1, 2),
        kind: 7,
        task_id: id(3, 4),
    };
    assert_eq!(
        ChunkTaskKey::from_bytes(&canonical.to_bytes()).unwrap(),
        canonical
    );

    let ready = ReadyChunkTaskKey {
        priority_inverse: 5,
        eligible_at_ms: 9,
        partition_id: id(1, 2),
        kind: 7,
        task_id: id(3, 4),
    };
    assert_eq!(ReadyChunkTaskKey::from_bytes(&ready.to_bytes()).unwrap(), ready);

    let leased = LeasedChunkTaskKey {
        lease_deadline_ms: 11,
        partition_id: id(1, 2),
        kind: 7,
        task_id: id(3, 4),
    };
    assert_eq!(
        LeasedChunkTaskKey::from_bytes(&leased.to_bytes()).unwrap(),
        leased
    );
}

#[test]
fn canonical_partition_prefix_groups_task_kinds() {
    let partition = id(10, 20);
    let prefix = ChunkTaskKey::prefix_for_partition(&partition);
    for kind in [1, 2, u16::MAX] {
        let key = ChunkTaskKey {
            partition_id: partition,
            kind,
            task_id: id(u64::from(kind), 99),
        }
        .to_bytes();
        assert!(key.starts_with(&prefix));
    }
}

#[test]
fn ready_keys_sort_high_priority_first_then_eligibility() {
    let make = |priority: u8, eligible_at_ms| {
        ReadyChunkTaskKey {
            priority_inverse: u8::MAX - priority,
            eligible_at_ms,
            partition_id: id(1, 1),
            kind: 1,
            task_id: id(2, 2),
        }
        .to_bytes()
    };
    assert!(make(9, 100) < make(8, 1));
    assert!(make(9, 100) < make(9, 101));
}

#[test]
fn malformed_task_key_is_rejected() {
    let key = ChunkTaskKey {
        partition_id: id(1, 2),
        kind: 1,
        task_id: id(3, 4),
    }
    .to_bytes();
    assert!(ChunkTaskKey::from_bytes(&key[..key.len() - 1]).is_err());

    let mut trailing = key;
    trailing.push(0);
    assert!(ChunkTaskKey::from_bytes(&trailing).is_err());
}
