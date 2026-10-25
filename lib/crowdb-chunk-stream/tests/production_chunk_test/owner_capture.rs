// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::*;
use crowdb_chunk_stream::{StreamPurpose, StreamViewSource};
use crowdb_kv::cluster::{
    group::PxGroup, kv_server::KvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole,
};

struct TestRuntimeKv(Arc<PxKvStore>);

impl Drop for TestRuntimeKv {
    fn drop(&mut self) {
        self.0.stop();
    }
}

async fn runtime_kv() -> (TestRuntimeKv, Arc<CrowdbKvClient>) {
    let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
    for group in 0..=1 {
        store.add_group(PxGroup::new(
            group,
            PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
        ));
    }
    store.start().await.unwrap();
    let kv = Arc::new(CrowdbKvClient::new(ClientConfig::new(vec![])));
    for group in 0..=1 {
        kv.seed_leader(0, group, store.listen_addr().unwrap().to_string());
    }
    (TestRuntimeKv(store), kv)
}

#[tokio::test]
async fn runtime_captures_local_owner_and_identifies_older_remote_checkpoint() {
    let (_server, kv) = runtime_kv().await;
    let chunk_io = ChunkIoClient::from_parts(
        Arc::new(Allocator::new()),
        Arc::new(Disks::default()),
        crowdb_protocol::chunkdb::rpc::ChunkType::Stream,
    );
    let make_runtime = || {
        ProductionStreamRuntime::new(
            kv.clone(),
            &chunk_io,
            30_000,
            ChunkReadPolicy::default(),
            StreamConfig::default(),
        )
        .unwrap()
    };
    let owner = make_runtime();
    let remote = make_runtime();
    let name = StreamName::generate();
    owner
        .registry()
        .create(StreamRecord {
            state: StreamRecordState::Active,
            ..StreamRecord::creating(name, None, StreamPurpose::Stream)
        })
        .await
        .unwrap();
    let writer = owner.create_registered(name, 0, 1).await.unwrap();
    writer.append(&[Bytes::from_static(b"first")]).await.unwrap();
    let fallback = remote.open_read_only_current(name, 0).await.unwrap();
    assert_eq!(fallback.view_source(), StreamViewSource::PersistedCheckpoint);
    assert_eq!(fallback.tail(), 0);
    let captured = owner.open_read_only(name, 0, 1).await.unwrap();
    assert_eq!(captured.view_source(), StreamViewSource::OwnerCheckpoint);
    assert_eq!(
        captured.read_at(0, 5).await.unwrap(),
        Bytes::from_static(b"first")
    );
    writer.append(&[Bytes::from_static(b"second")]).await.unwrap();
    let fallback = remote.open_read_only_current(name, 0).await.unwrap();
    assert_eq!(fallback.tail(), 5);
    assert_eq!(
        fallback.read_at(0, 5).await.unwrap(),
        Bytes::from_static(b"first")
    );
    assert_eq!(captured.tail(), 5);
    assert_eq!(owner.open_read_only_current(name, 0).await.unwrap().tail(), 11);
    writer.close().await.unwrap();
    assert_eq!(owner.open_read_only(name, 0, 1).await.unwrap().tail(), 11);
    let successor = owner.registry().claim_writer(name, 2).await.unwrap();
    writer.release_writer().await.unwrap();
    successor.capture().unwrap().check_io().unwrap();
    successor.release().await.unwrap();
}

#[tokio::test]
async fn owner_claim_is_exclusive_and_an_old_release_cannot_release_its_successor() {
    let (_server, kv) = runtime_kv().await;
    let registry = crowdb_chunk_stream::KvStreamRegistry::new(kv);
    let name = StreamName::generate();
    registry
        .create(StreamRecord {
            state: StreamRecordState::Active,
            ..StreamRecord::creating(name, None, StreamPurpose::Stream)
        })
        .await
        .unwrap();
    let first = registry.claim_writer(name, 1).await.unwrap();
    assert!(matches!(
        registry.claim_writer(name, 1).await,
        Err(crowdb_chunk_stream::StreamError::StaleWriter)
    ));
    first.release().await.unwrap();
    let (a, b) = tokio::join!(registry.claim_writer(name, 1), registry.claim_writer(name, 1));
    assert_ne!(a.is_ok(), b.is_ok(), "only one concurrent claim can win");
    let successor = a.or(b).unwrap();
    assert!(
        first.release().await.is_err(),
        "an old assignment cannot release its successor even at the same epoch"
    );
    successor.capture().unwrap().check_io().unwrap();
    assert!(registry.claim_writer(name, 2).await.is_err());
    successor.release().await.unwrap();
    registry
        .claim_writer(name, 2)
        .await
        .unwrap()
        .release()
        .await
        .unwrap();
}

#[tokio::test]
async fn drained_release_allows_another_runtime_to_take_over_without_waiting_for_process_expiry() {
    let (_server, kv) = runtime_kv().await;
    let first = crowdb_chunk_stream::KvStreamRegistry::new(kv.clone());
    let second = crowdb_chunk_stream::KvStreamRegistry::new(kv);
    let name = StreamName::generate();
    first
        .create(StreamRecord {
            state: StreamRecordState::Active,
            ..StreamRecord::creating(name, None, StreamPurpose::Stream)
        })
        .await
        .unwrap();
    let old = first.claim_writer(name, 1).await.unwrap();
    old.release().await.unwrap();
    let new = tokio::time::timeout(std::time::Duration::from_secs(1), second.claim_writer(name, 2))
        .await
        .unwrap()
        .unwrap();
    assert!(old.capture().is_err());
    new.release().await.unwrap();
}

#[tokio::test]
async fn failed_initial_allocation_releases_the_unpublished_writer_assignment() {
    let (_server, kv) = runtime_kv().await;
    let registry = Arc::new(crowdb_chunk_stream::KvStreamRegistry::new(kv));
    let memory = Arc::new(MemoryStreamStore::new(1024));
    let name = StreamName::generate();
    registry
        .create(StreamRecord {
            metadata_group_id: 1,
            state: StreamRecordState::Active,
            ..StreamRecord::creating(name, None, StreamPurpose::Stream)
        })
        .await
        .unwrap();
    memory.fail_allocation_for_tests(1);
    let first = ChunkStream::create_registered(
        name,
        1,
        StreamConfig::default(),
        registry.clone(),
        memory.clone(),
        memory.clone(),
    )
    .await;
    assert!(matches!(
        first,
        Err(crowdb_chunk_stream::StreamError::ReadUnavailable(_))
    ));
    let writer =
        ChunkStream::create_registered(name, 1, StreamConfig::default(), registry, memory.clone(), memory)
            .await
            .unwrap();
    writer.append(&[Bytes::from_static(b"ready")]).await.unwrap();
    assert_eq!(writer.read_at(0, 5).await.unwrap(), Bytes::from_static(b"ready"));
    writer.release_writer().await.unwrap();
}

#[tokio::test]
async fn failed_reopen_releases_its_assignment_before_retrying_in_the_same_process() {
    let (_server, kv) = runtime_kv().await;
    let registry = Arc::new(crowdb_chunk_stream::KvStreamRegistry::new(kv));
    let memory = Arc::new(MemoryStreamStore::new(1024));
    let name = StreamName::generate();
    registry
        .create(StreamRecord {
            metadata_group_id: 1,
            state: StreamRecordState::Active,
            ..StreamRecord::creating(name, None, StreamPurpose::Stream)
        })
        .await
        .unwrap();
    let writer = ChunkStream::create_registered(
        name,
        1,
        StreamConfig::default(),
        registry.clone(),
        memory.clone(),
        memory.clone(),
    )
    .await
    .unwrap();
    writer.append(&[Bytes::from_static(b"retained")]).await.unwrap();
    writer.release_writer().await.unwrap();
    let next = memory
        .chunks_for_tests()
        .await
        .into_iter()
        .map(|(id, ..)| id.low)
        .max()
        .unwrap()
        + 1;
    memory.fail_allocation_for_tests(next);
    let failed = ChunkStream::open(
        name,
        2,
        StreamConfig::default(),
        registry.clone(),
        memory.clone(),
        memory.clone(),
    )
    .await;
    assert!(matches!(
        failed,
        Err(crowdb_chunk_stream::StreamError::ReadUnavailable(_))
    ));
    let recovered = ChunkStream::open(name, 2, StreamConfig::default(), registry, memory.clone(), memory)
        .await
        .expect("a failed recovery cannot retain an unpublished writer assignment");
    assert_eq!(
        recovered.read_at(0, 8).await.unwrap(),
        Bytes::from_static(b"retained")
    );
    recovered.append(&[Bytes::from_static(b"-after")]).await.unwrap();
    assert_eq!(
        recovered.read_at(0, 14).await.unwrap(),
        Bytes::from_static(b"retained-after")
    );
    recovered.release_writer().await.unwrap();
}

#[tokio::test]
async fn release_during_a_pending_claim_wakes_the_successor_without_process_expiry() {
    let (_server, kv) = runtime_kv().await;
    let first = crowdb_chunk_stream::KvStreamRegistry::new(kv.clone());
    let second = crowdb_chunk_stream::KvStreamRegistry::new(kv);
    let name = StreamName::generate();
    first
        .create(StreamRecord {
            state: StreamRecordState::Active,
            ..StreamRecord::creating(name, None, StreamPurpose::Stream)
        })
        .await
        .unwrap();
    let owner = first.claim_writer(name, 1).await.unwrap();
    let pending = second.claim_writer(name, 2);
    tokio::pin!(pending);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), pending.as_mut())
            .await
            .is_err(),
        "a live writer cannot be preempted"
    );
    owner.release().await.unwrap();
    let successor = tokio::time::timeout(std::time::Duration::from_secs(1), pending)
        .await
        .expect("durable release must wake an already waiting claim")
        .unwrap();
    assert!(owner.capture().is_err());
    successor.release().await.unwrap();
}

#[tokio::test]
async fn recovery_waiting_for_release_loads_the_final_published_tail() {
    let (_server, kv) = runtime_kv().await;
    let first = Arc::new(crowdb_chunk_stream::KvStreamRegistry::new(kv.clone()));
    let second = Arc::new(crowdb_chunk_stream::KvStreamRegistry::new(kv));
    let memory = Arc::new(MemoryStreamStore::new(1024));
    let name = StreamName::generate();
    let writer = ChunkStream::create(
        StreamRecord {
            metadata_group_id: 1,
            state: StreamRecordState::Active,
            ..StreamRecord::creating(name, None, StreamPurpose::Stream)
        },
        1,
        StreamConfig::default(),
        first,
        memory.clone(),
        memory.clone(),
    )
    .await
    .unwrap();
    writer.append(&[Bytes::from_static(b"before")]).await.unwrap();
    let pending = ChunkStream::open(name, 2, StreamConfig::default(), second, memory.clone(), memory);
    tokio::pin!(pending);
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(100), pending.as_mut())
            .await
            .is_err(),
        "recovery must wait for the live writer"
    );
    writer.append(&[Bytes::from_static(b"-after")]).await.unwrap();
    writer.release_writer().await.unwrap();
    let successor = tokio::time::timeout(std::time::Duration::from_secs(1), pending)
        .await
        .unwrap()
        .expect("recovery must use the checkpoint published by the release it awaited");
    assert_eq!(
        successor.read_at(0, 12).await.unwrap(),
        Bytes::from_static(b"before-after")
    );
    successor.append(&[Bytes::from_static(b"-next")]).await.unwrap();
    successor.release_writer().await.unwrap();
}
