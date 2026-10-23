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
        .create(StreamBinding {
            state: StreamBindingState::Active,
            ..StreamBinding::creating(name, None, StreamPurpose::Stream)
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
    assert_eq!(captured.tail(), 5);
    assert_eq!(owner.open_read_only_current(name, 0).await.unwrap().tail(), 11);
    writer.close().await.unwrap();
    assert_eq!(owner.open_read_only(name, 0, 1).await.unwrap().tail(), 11);
}
