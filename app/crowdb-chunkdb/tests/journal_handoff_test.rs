// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use crowdb_chunkdb::storage::decode_chunk_for_tests;
use crowdb_kv::cluster::group::{ProposeResult, PxGroup};
use crowdb_kv::cluster::{PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv::kv::{CrowdbTreeConfig, CrowdbTreeEngine, KVEngine};
use crowdb_kv::paxos::learner::PxLearner;
use crowdb_kv::paxos::roles::{PxBallot, PxLogEntry};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkType};
use crowdb_protocol::common::ChunkId;

fn chunk_key() -> Bytes {
    let mut key = b"/chunk/".to_vec();
    key.extend_from_slice(&1_u64.to_be_bytes());
    key.extend_from_slice(&2_u64.to_be_bytes());
    key.into()
}

fn metadata(modify_ts: u64, acknowledged_cursor: u64) -> Vec<u8> {
    bincode::serialize(&Chunk {
        id: Some(ChunkId { high: 1, low: 2 }),
        chunk_type: ChunkType::Wal as i32,
        modify_ts,
        acknowledged_cursor,
        ..Default::default()
    })
    .unwrap()
}

fn encode_puts(entries: &[(&[u8], &[u8])]) -> Vec<u8> {
    let mut payload = u16::try_from(entries.len()).unwrap().to_le_bytes().to_vec();
    for (key, value) in entries {
        payload.push(0);
        payload.extend_from_slice(&u32::try_from(key.len()).unwrap().to_le_bytes());
        payload.extend_from_slice(key);
        payload.extend_from_slice(&u32::try_from(value.len()).unwrap().to_le_bytes());
        payload.extend_from_slice(value);
    }
    payload
}

async fn learn(group: &PxGroup, slot: u64, payload: Vec<u8>) {
    group
        .local_replica()
        .learn_chosen(
            &PxLogEntry {
                slot,
                ballot: PxBallot::new(0, 1),
                term: 0,
                payload: payload.into(),
            },
            &[],
        )
        .await;
}

async fn assert_metadata(engine: &dyn KVEngine, revision: u64, timestamp: u64, cursor: u64) {
    let (actual_revision, bytes) = engine.get_versioned(&chunk_key()).await.unwrap().unwrap();
    let chunk = decode_chunk_for_tests(&bytes).unwrap();
    assert_eq!(actual_revision, revision);
    assert_eq!(chunk.modify_ts, timestamp);
    assert_eq!(chunk.acknowledged_cursor, cursor);
}

async fn seed_gap(group: &PxGroup) {
    for slot in 1..=851 {
        let payload = if slot == 843 {
            encode_puts(&[(&chunk_key(), &metadata(58, 23039))])
        } else {
            Vec::new()
        };
        learn(group, slot, payload).await;
    }
}

// Uses production Paxos CAS, Chunk metadata encoding and the durable tree without
// a service fixture. Journal bytes are a companion KV value in the same batch.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn journal_cas_retains_future_revision_through_flush_and_reopen() {
    let dir = crowdb_test_harness::test_dirs::tempdir_in_test_data("journal-handoff");
    let config = CrowdbTreeConfig {
        path: Some(dir.path().to_string_lossy().into_owned()),
        ..Default::default()
    };
    let tree = CrowdbTreeEngine::open(&config).unwrap();
    let handle = tree.handle();
    let mut replica = PxLocalReplica::new(1, PxLocalReplicaRole::Leader);
    replica.learner = Arc::new(PxLearner::with_engine(Box::new(tree)));
    let group = Arc::new(PxGroup::new(1, replica));
    group.set_self_weak();
    seed_gap(&group).await;
    handle.flush().unwrap();
    assert_eq!(handle.snapshot().unwrap(), 851);

    learn(&group, 853, encode_puts(&[(&chunk_key(), &metadata(60, 24040))])).await;
    let engine = group.local_replica().learner.engine();
    // Keep slot 852 absent: every maintenance pass must retain the future value.
    for _ in 0..3 {
        handle.flush().unwrap();
        assert_eq!(handle.snapshot().unwrap(), 851);
        assert_metadata(engine, 853, 60, 24040).await;
    }
    group.set_next_slot(854);
    let journal: Vec<u8> = (0..24643).map(|i| u8::try_from(i % 251).unwrap()).collect();
    let payload = encode_puts(&[(&chunk_key(), &metadata(61, 24643)), (b"journal", &journal)]);
    let cas_group = Arc::clone(&group);
    let cas = tokio::spawn(async move { cas_group.propose_cas(payload, chunk_key(), 853, 7, 1).await });
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let revision = engine.get_versioned(&chunk_key()).await.unwrap().unwrap().0;
            if revision == 854 {
                break;
            }
            assert!(!cas.is_finished(), "CAS rejected the visible future revision");
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("CAS must apply before waiting for the contiguous apply fence");
    handle.flush().unwrap();
    assert_eq!(handle.snapshot().unwrap(), 851);
    assert_metadata(engine, 854, 61, 24643).await;
    assert!(!cas.is_finished(), "the apply fence must not cross the gap");
    learn(&group, 852, Vec::new()).await;
    let result = tokio::time::timeout(Duration::from_secs(5), cas)
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(result, ProposeResult::Chosen { slot: 854 }));
    handle.flush().unwrap();
    assert_eq!(handle.snapshot().unwrap(), 854);
    drop(group);
    drop(handle);

    let reopened = CrowdbTreeEngine::open(&config).unwrap();
    assert_eq!(reopened.resume_from_slot(), 854);
    assert_metadata(&reopened, 854, 61, 24643).await;
    assert_eq!(
        reopened.get_versioned(b"journal").await.unwrap(),
        Some((854, journal.into()))
    );
}
