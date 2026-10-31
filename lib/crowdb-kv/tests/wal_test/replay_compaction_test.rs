// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::path::PathBuf;
use std::sync::Arc;

use bytes::Bytes;
use crowdb_kv::cluster::{PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv::paxos::roles::{PxBallot, PxLogEntry};
use crowdb_kv::wal::record::{RecordType, WALRecord};
use crowdb_kv::wal::replay::replay_group;
use crowdb_kv::wal::{IoBackend, MemBlockDevice, WalConfig, WalEngine};

fn accepted(ballot: u64, term: u64, value: &[u8]) -> WALRecord {
    let mut payload = vec![1, 0, 0, 1, 0, 0, 0, b'k'];
    payload.extend_from_slice(&u32::try_from(value.len()).unwrap().to_le_bytes());
    payload.extend_from_slice(value);
    WALRecord::from_accepted(
        1,
        &PxLogEntry {
            slot: 1,
            ballot: PxBallot::new(ballot, 7),
            term,
            payload: Bytes::from(payload),
        },
    )
}

async fn fixture() -> (Arc<IoBackend>, WalConfig, Arc<WalEngine>) {
    let backend = Arc::new(IoBackend::MemBlock(MemBlockDevice::new()));
    let config = WalConfig {
        wal_disks: vec![PathBuf::from("/compact")],
        ..Default::default()
    };
    let wal = WalEngine::create(backend.clone(), config.clone(), 1)
        .await
        .unwrap();
    (backend, config, wal)
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn compaction_preserves_accepted_value_below_an_independent_higher_promise() {
    let (backend, config, wal) = fixture().await;
    let highest = accepted(10, 10, b"highest");
    for record in [
        WALRecord::from_promised(1, 20, 1, PxBallot::new(20, 7)),
        accepted(4, 4, b"old"),
        highest.clone(),
        WALRecord::from_promised(1, 12, 1, PxBallot::new(12, 7)),
        accepted(5, 5, b"later-old"),
    ] {
        wal.append(&record).await.unwrap();
    }
    wal.seal_all().await.unwrap();
    let replay = replay_group(&backend, &config.wal_disks, 1).await.unwrap();
    assert_eq!(replay.records.len(), 2);
    assert_eq!(replay.current_term, 20);
    assert_eq!(
        replay
            .index
            .segments()
            .map(|segment| segment.record_count)
            .sum::<u32>(),
        5
    );
    let replica = PxLocalReplica::restore_from_replay(7, PxLocalReplicaRole::Follower, &replay)
        .await
        .unwrap();
    assert_eq!(replica.accepted_at(1).await, highest.to_log_entry());
    assert_eq!(replica.promised_at(1).await, Some(PxBallot::new(20, 7)));
    assert_eq!(
        replica.learner.engine_get(b"k").await,
        Some((1, b"highest".to_vec()))
    );
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn discarded_accepts_still_advance_the_term_and_fence_old_votes() {
    let (backend, config, wal) = fixture().await;
    for record in [
        accepted(10, 10, b"highest"),
        accepted(4, 30, b"old"),
        WALRecord::from_vote_granted(1, 30, 42),
        WALRecord::from_vote_granted(1, 12, 99),
    ] {
        wal.append(&record).await.unwrap();
    }
    wal.seal_all().await.unwrap();
    let replay = replay_group(&backend, &config.wal_disks, 1).await.unwrap();
    assert_eq!(replay.current_term, 30);
    assert_eq!(replay.voted_for, Some(42));
    assert_eq!(
        replay
            .records
            .iter()
            .filter(|record| record.record_type == RecordType::Accepted)
            .count(),
        1
    );
    wal.append(&accepted(3, 31, b"discarded-new-term")).await.unwrap();
    wal.seal_all().await.unwrap();
    let replay = replay_group(&backend, &config.wal_disks, 1).await.unwrap();
    assert_eq!(replay.current_term, 31);
    assert_eq!(replay.voted_for, None);
}

#[tokio::test(flavor = "current_thread", start_paused = true)]
async fn equal_ballot_compaction_keeps_the_last_verified_accept() {
    let (backend, config, wal) = fixture().await;
    wal.append(&accepted(5, 5, b"earlier")).await.unwrap();
    let latest = accepted(5, 5, b"latest");
    wal.append(&latest).await.unwrap();
    wal.seal_all().await.unwrap();
    let replay = replay_group(&backend, &config.wal_disks, 1).await.unwrap();
    assert_eq!(replay.records, vec![latest]);
}
