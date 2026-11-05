// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use crowdb_chunk_kv::{
    memory::MemoryPartitionTree, Checkpoint, Partition, PartitionConfig, PartitionId, PartitionRange,
    StreamPartitionJournal,
};
use crowdb_chunk_stream::{
    memory::MemoryStreamStore, ChunkStream, StreamConfig, StreamRecord, StreamRecordState,
};
use crowdb_paxos_kv_client::{
    PaxosKvClient, StreamProcessAuthority, StreamProcessLeaseClient, StreamProcessLeaseTiming,
};
use crowdb_protocol::chunk_kv::{
    ChunkKvRangeCatalogEntry, ChunkKvRangeCatalogPartitionState, Id128, KeyRange, OwnerDescriptor,
    PartitionArtifact,
};
use crowdb_protocol::chunk_stream::{StreamName, StreamPurpose};
use crowdb_protocol::key::{StreamProcessLeaseKey, TextKey};
use crowdb_tree_ffi::chunk::stage::{StageId, StageRecord, StageState};
use std::{
    sync::Arc,
    time::{Instant, SystemTime, UNIX_EPOCH},
};

pub fn entry(epoch: u64) -> ChunkKvRangeCatalogEntry {
    ChunkKvRangeCatalogEntry {
        partition_id: Id128 { high: 1, low: 1 },
        range: KeyRange {
            start: vec![],
            end: None,
        },
        owner: OwnerDescriptor {
            instance_id: epoch,
            rpc_endpoint: "127.0.0.1:1".into(),
        },
        owner_epoch: epoch,
        state: ChunkKvRangeCatalogPartitionState::Serving,
        artifact: PartitionArtifact {
            tree_id: 42,
            stream_name: StreamName { high: 1, low: 2 },
            tail_overlay: None,
        },
        transition_id: None,
    }
}
pub fn stage() -> StageRecord {
    StageRecord {
        tree_id: 42,
        id: StageId {
            owner_epoch: 1,
            operation_high: 3,
            operation_low: 4,
        },
        revision: 1,
        chunk_high: 2_u64 << 56,
        chunk_low: 10,
        capacity: 4096,
        purpose: 2,
        state: StageState::Reserved,
        manifest_generation: 0,
        packs: vec![],
    }
}
pub fn next(old: &StageRecord, state: StageState) -> StageRecord {
    let mut next = old.clone();
    next.state = state;
    next.revision += 1;
    next
}
pub fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis()
        .try_into()
        .unwrap()
}
pub async fn process(kv: &Arc<PaxosKvClient>, instance: u64) -> Arc<StreamProcessAuthority> {
    let now = now();
    let lease = StreamProcessLeaseClient::new(kv.clone())
        .claim(StreamPurpose::Wal, instance, [1; 16], None, now, 300_000)
        .await
        .unwrap();
    Arc::new(
        StreamProcessAuthority::new(lease, StreamProcessLeaseTiming::default(), now, Instant::now()).unwrap(),
    )
}
// Deterministically install an already elapsed authoritative crash exclusion boundary.
// The old local process is fenced before the metadata fixture changes the clock record.
pub async fn expire(kv: &Arc<PaxosKvClient>, process: &StreamProcessAuthority) {
    process.fence();
    let mut lease = process.status(Instant::now()).lease;
    lease.expires_at_ms = now() - 60_000;
    let key = StreamProcessLeaseKey {
        purpose: StreamPurpose::Wal,
        instance_id: lease.identity.instance_id,
    }
    .to_path();
    kv.put(0, 0, key.as_bytes(), &serde_json::to_vec(&lease).unwrap(), None)
        .await
        .unwrap();
}
pub async fn recovered(epoch: u64) -> Partition {
    let store = Arc::new(MemoryStreamStore::new(4096));
    let name = entry(epoch).artifact.stream_name;
    let stream = ChunkStream::create(
        StreamRecord {
            purpose: StreamPurpose::Wal,
            stream_name: name,
            metadata_group_id: 1,
            record_version: 1,
            state: StreamRecordState::Active,
            consumer_kind: Some("chunk-kv-partition".into()),
            consumer_id: None,
            created_at_ms: now(),
        },
        epoch,
        StreamConfig::default(),
        store.clone(),
        store.clone(),
        store,
    )
    .await
    .unwrap();
    Partition::recover_prepared_assignment(
        PartitionId { high: 1, low: 1 },
        PartitionRange::default(),
        epoch,
        Checkpoint {
            tree_id: 42,
            tree_manifest: 0,
            root_manifest_generation: 0,
            applied_seq: 0,
            stream_name: name,
            stream_generation: 1,
            replay_offset: 0,
        },
        PartitionConfig::default(),
        Arc::new(MemoryPartitionTree::with_tree_id(42)),
        Arc::new(StreamPartitionJournal::new(stream, name).unwrap()),
    )
    .await
    .unwrap()
}

// Age the already committed operation receipt without waiting for wall time.
// The native final-owner release must have created this exact proof first.
pub async fn age_operation_release(kv: &PaxosKvClient, stage: StageId) {
    use crowdb_paxos_kv_client::{GetOutcome, ReadMode};
    let mut key = b"\0crowdb/chunk-kv/root/v1/".to_vec();
    key.extend_from_slice(&42_u64.to_be_bytes());
    key.extend_from_slice(b"/stage-quiesced/");
    for word in [stage.owner_epoch, stage.operation_high, stage.operation_low] {
        key.extend_from_slice(&word.to_be_bytes());
    }
    let GetOutcome::Found { value, revision } =
        kv.get(0, 1, &key, ReadMode::Linearizable, None).await.unwrap()
    else {
        panic!("native operation release has no durable receipt");
    };
    let mut bytes = value.to_vec();
    assert_eq!(bytes.len(), 84);
    bytes[8..16].copy_from_slice(&(now() - 60_000).to_le_bytes());
    let checksum = crowdb_tree_ffi::crc32c(&bytes[..80]);
    bytes[80..].copy_from_slice(&checksum.to_le_bytes());
    kv.put_cas(0, 1, &key, &bytes, revision).await.unwrap();
}
