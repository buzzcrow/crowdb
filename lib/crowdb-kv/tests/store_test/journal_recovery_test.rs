// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use bytes::Bytes;
use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::kv_store::KvStore;
use crowdb_kv::cluster::{PxKvStore, PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv::kv::{Batch, BatchOp, CrowdbTreeConfig, CrowdbTreeEngine, KVEngine, Op};
use crowdb_kv::paxos::roles::{PxBallot, PxLogEntry};
use crowdb_kv::rpc::{KvErrorCode, ReadMode};
use crowdb_kv::wal::index::SegmentIndex;
use crowdb_kv::wal::record::WALRecord;
use crowdb_kv::wal::replay::ReplayResult;

#[tokio::test]
async fn restored_journal_rejects_missing_durable_history_before_maintenance() {
    let engine = CrowdbTreeEngine::open(&CrowdbTreeConfig::default()).unwrap();
    for slot in 1..=3 {
        engine
            .apply(
                slot,
                &Batch {
                    ops: vec![BatchOp {
                        key: Bytes::from(format!("busy:{slot}")),
                        op: Op::Put(Bytes::from_static(b"occupied")),
                    }],
                },
            )
            .await
            .unwrap();
    }
    engine.handle().flush().unwrap();
    assert_eq!(engine.resume_from_slot(), 3);
    // Earlier WAL segments were reclaimed after the tree became durable.
    // The surviving tail still seeds the applied frontier on restart.
    let entry = PxLogEntry {
        slot: 3,
        ballot: PxBallot::new(0, 1),
        term: 1,
        payload: Bytes::new(),
    };
    let replay = ReplayResult {
        records: vec![WALRecord::from_accepted(1, &entry)],
        index: SegmentIndex::new(),
        max_segment_id: 1,
        current_term: 1,
        voted_for: None,
    };
    let replica = PxLocalReplica::restore_from_replay_with_engine(
        1,
        PxLocalReplicaRole::Leader,
        &replay,
        Box::new(engine),
    )
    .await
    .unwrap();
    assert_eq!(replica.contiguous_applied(), 3);
    assert_eq!(replica.acceptor.trim_slot(), 0);
    let store = PxKvStore::new(0, "127.0.0.1:0".parse().unwrap());
    store.add_group(PxGroup::new(1, replica));
    let response = store
        .kv_journal_scan(1, 1, 0, b"busy:", 0, ReadMode::Linearizable as i32, 1, 1)
        .await;
    assert_eq!(
        response.error_code,
        KvErrorCode::KvErrorJournalScanGcGap as i32,
        "missing allocation history must select full-scan recovery: {response:?}"
    );
    assert!(!response.ok);
    let retained_tail = store
        .kv_journal_scan(1, 3, 0, b"busy:", 0, ReadMode::Linearizable as i32, 3, 3)
        .await;
    assert!(
        retained_tail.ok,
        "the retained NoOp is complete history: {retained_tail:?}"
    );
    assert!(retained_tail.ops.is_empty());
    let retained = store
        .kv_get(1, b"busy:1", ReadMode::Linearizable as i32, 0, 2, 2)
        .await;
    assert!(retained.ok && !retained.not_found);
}
