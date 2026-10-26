// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/metadata.rs"]
mod metadata;
#[path = "common/write_storage.rs"]
mod write_storage;

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use crowdb_chunk_client::SmallWritePolicy;
use crowdb_protocol::chunkdb::rpc::ChunkType;
use crowdb_sdk_native::Error;
use write_storage::TestWriteStorage;

struct TestPausedShutdown(Arc<TestWriteStorage>);

impl Drop for TestPausedShutdown {
    fn drop(&mut self) {
        self.0.pause_shutdown.store(false, Ordering::Release);
        self.0.shutdown_resume.notify_one();
    }
}

#[tokio::test]
async fn borrowed_clients_are_not_stopped_when_the_sdk_closes_its_own_admission() {
    let backend = TestWriteStorage::new();
    let runtime = backend.runtime_borrowed(metadata::client().await);
    let dataset = runtime
        .with_writer_policy(&SmallWritePolicy::new(ChunkType::Dataset))
        .unwrap();
    runtime.shutdown().await.unwrap();
    dataset.shutdown().await.unwrap();
    assert!(runtime.resources().status().closed);
    assert_eq!(backend.shutdowns.load(Ordering::Acquire), 0);
    assert!(matches!(
        runtime.with_writer_policy(&SmallWritePolicy::new(ChunkType::S3)),
        Err(Error::Shutdown)
    ));
}

#[tokio::test]
async fn cancelled_shutdown_waiters_do_not_cancel_shared_owned_cleanup_or_release_held_data() {
    let backend = TestWriteStorage::new();
    backend.pause_shutdown.store(true, Ordering::Release);
    let pause = TestPausedShutdown(backend.clone());
    let runtime = backend.runtime(metadata::client().await);
    let dataset = runtime
        .with_writer_policy(&SmallWritePolicy::new(ChunkType::Dataset))
        .unwrap();
    let held = runtime
        .resources()
        .retain_vec(b"retained".to_vec(), 1, 1, 0, 0)
        .unwrap();
    let first_runtime = runtime.clone();
    let first = tokio::spawn(async move { first_runtime.shutdown().await });
    tokio::time::timeout(Duration::from_secs(2), backend.shutdown_entered.notified())
        .await
        .unwrap();
    first.abort();
    assert!(first.await.unwrap_err().is_cancelled());
    let mut second = Box::pin(dataset.shutdown());
    assert!(tokio::time::timeout(Duration::from_millis(20), &mut second)
        .await
        .is_err());
    assert_eq!(held.as_slice(), b"retained");
    assert_eq!(
        runtime.resources().status().held_bytes,
        held.metadata().allocation_capacity
    );
    drop(pause);
    second.await.unwrap();
    runtime.shutdown().await.unwrap();
    assert_eq!(backend.shutdowns.load(Ordering::Acquire), 1);
    assert_eq!(held.as_slice(), b"retained");
    drop(held);
    assert_eq!(runtime.resources().status().held_bytes, 0);
}
