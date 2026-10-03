// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_protocol::common::ChunkId;
use crowdb_protocol::{BinaryKey, ChunkTaskKey, FinalizeChunkTaskKey, LeasedChunkTaskKey, ReadyChunkTaskKey};

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
fn finalize_keys_sort_by_expiry() {
    let early = FinalizeChunkTaskKey {
        expires_at_ms: 100,
        partition_id: id(1, 2),
        task_id: id(3, 4),
    };
    let late = FinalizeChunkTaskKey {
        expires_at_ms: 101,
        ..early
    };
    assert!(early.to_bytes() < late.to_bytes());
    assert_eq!(
        FinalizeChunkTaskKey::from_bytes(&early.to_bytes()).unwrap(),
        early
    );
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

#[test]
fn task_index_scope_is_bound_to_partition_identity() {
    use crowdb_protocol::chunk_domain::ChunkDomain;
    use crowdb_protocol::chunk_slot::ChunkSlot;

    for purpose in 1_u64..=6 {
        let partition_id = id(purpose << 56, 12);
        let key = ReadyChunkTaskKey {
            priority_inverse: 0,
            eligible_at_ms: 100,
            partition_id,
            kind: 1,
            task_id: partition_id,
        };
        let bytes = key.to_bytes();
        let domain = if purpose <= 3 {
            ChunkDomain::System
        } else {
            ChunkDomain::UserData
        };
        assert_eq!(ChunkDomain::for_chunk(&partition_id), Some(domain));
        assert_eq!(bytes[3], domain as u8);
        assert_eq!(
            &bytes[4..6],
            &ChunkSlot::for_chunk(&partition_id).value().to_be_bytes()
        );
        for offset in 3..6 {
            let mut corrupt = bytes.clone();
            corrupt[offset] ^= 1;
            assert!(ReadyChunkTaskKey::from_bytes(&corrupt).is_err());
        }
    }
    assert_eq!(ChunkDomain::for_chunk(&id(0, 1)), None);
    assert_eq!(ChunkDomain::for_chunk(&id(7 << 56, 1)), None);
}
