// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/indexed_metadata.rs"]
mod indexed_metadata;
#[path = "common/metadata_server.rs"]
mod metadata_server;

use crowdb_chunk_stream::{
    memory::MemoryStreamStore, DirectoryIndex, DirectoryMetadataStore, KvStreamMetadataStore, StreamError,
    StreamMetadataStore,
};
use crowdb_kv_client::{BatchOp, GetOutcome, ReadMode};
use crowdb_protocol::chunk_stream::{
    encode_stream_root_pin, StreamDirectoryHead, StreamRootPin, StreamRootPinId, MAX_STREAM_ROOT_PINS,
};
use crowdb_protocol::key::{BinaryKey, StreamDirectoryControlKey, StreamDirectoryRootKey, StreamRootPinKey};
use indexed_metadata::TestIndexedMetadata as Test;
use metadata_server::TestMetadataServer;

#[path = "root_pin_test/kv.rs"]
mod kv;

async fn publish(directory: &DirectoryMetadataStore) -> StreamDirectoryHead {
    let mutation = Test::empty()
        .prepare_append(directory, &Test::locations(0, 1))
        .await
        .unwrap();
    directory
        .publish_directory(None, Test::head(&mutation, None), &mutation)
        .await
        .unwrap()
        .head()
        .clone()
}

fn pin(head: &StreamDirectoryHead, number: usize, expiry: Option<u64>) -> StreamRootPin {
    let mut id = [0; 16];
    id[8..].copy_from_slice(&u64::try_from(number).unwrap().to_be_bytes());
    StreamRootPin {
        id: StreamRootPinId(id),
        head: head.clone(),
        needed_offset: 0,
        captured_tail: 1,
        expires_at_ms: expiry,
    }
}

fn now_ms() -> u64 {
    u64::try_from(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis(),
    )
    .unwrap()
}

async fn check_lifecycle(directory: &DirectoryMetadataStore) {
    let head = publish(directory).await;
    let descriptor = pin(&head, 1, None);
    let initial = directory.pin_directory_root(descriptor.clone()).await.unwrap();
    assert_eq!(
        directory
            .pin_directory_root(descriptor.clone())
            .await
            .unwrap()
            .pin(),
        &descriptor
    );
    assert!(directory
        .renew_directory_pin(&initial, now_ms() + 20_000)
        .await
        .is_err());
    let mut conflicting = descriptor.clone();
    conflicting.captured_tail = 0;
    assert!(directory.pin_directory_root(conflicting).await.is_err());
    directory.release_directory_pin(&initial).await.unwrap();
    directory.release_directory_pin(&initial).await.unwrap();
    let replacement = directory.pin_directory_root(descriptor.clone()).await.unwrap();
    assert!(matches!(
        directory.release_directory_pin(&initial).await,
        Err(StreamError::ReadUnavailable(_))
    ));
    assert_eq!(
        directory
            .load_directory_pin(Test::NAME, descriptor.id)
            .await
            .unwrap()
            .unwrap()
            .pin(),
        &descriptor
    );
    directory.release_directory_pin(&replacement).await.unwrap();
    let reader = pin(&head, 2, Some(now_ms() + 10_000));
    let first = directory.pin_directory_root(reader).await.unwrap();
    let renewed = directory
        .renew_directory_pin(&first, now_ms() + 20_000)
        .await
        .unwrap();
    assert!(directory.release_directory_pin(&first).await.is_err());
    assert!(directory
        .renew_directory_pin(&first, now_ms() + 30_000)
        .await
        .is_err());
    assert!(directory
        .renew_directory_pin(&renewed, renewed.pin().expires_at_ms.unwrap())
        .await
        .is_err());
    assert_eq!(renewed.pin().head, head);
    assert_eq!(renewed.pin().captured_tail, 1);
    directory.release_directory_pin(&renewed).await.unwrap();
    for expiry in [Some(1), Some(now_ms() + 120_000)] {
        assert!(directory.pin_directory_root(pin(&head, 3, expiry)).await.is_err());
    }
    let mut newer = pin(&head, 4, None);
    newer.head.generation += 1;
    newer.head.previous = Some(head.identity());
    assert!(matches!(
        directory.pin_directory_root(newer).await,
        Err(StreamError::ReadUnavailable(_))
    ));
    assert!(directory
        .scan_directory_pins(Test::NAME, None, 4)
        .await
        .unwrap()
        .entries
        .is_empty());
}

#[tokio::test]
async fn memory_pins_reconcile_identity_and_fence_stale_renewal_release_and_aba() {
    let memory = MemoryStreamStore::new(1024 * 1024);
    check_lifecycle(&memory.directory_metadata(1).unwrap()).await;
    assert_eq!(memory.metadata_publish_count(), 1);
}

#[tokio::test]
async fn durable_pin_budget_is_bounded_and_release_restores_admission() {
    let memory = MemoryStreamStore::new(1024 * 1024);
    let directory = memory.directory_metadata(1).unwrap();
    let head = publish(&directory).await;
    let mut first = None;
    for index in 1..=MAX_STREAM_ROOT_PINS {
        let saved = directory
            .pin_directory_root(pin(&head, index, None))
            .await
            .unwrap();
        if first.is_none() {
            first = Some(saved);
        }
    }
    let descriptor = pin(&head, MAX_STREAM_ROOT_PINS + 1, None);
    assert!(matches!(
        directory.pin_directory_root(descriptor.clone()).await,
        Err(StreamError::Backpressure)
    ));
    assert!(directory
        .load_directory_pin(Test::NAME, descriptor.id)
        .await
        .unwrap()
        .is_none());
    directory.release_directory_pin(&first.unwrap()).await.unwrap();
    directory.pin_directory_root(descriptor).await.unwrap();
    assert_eq!(memory.metadata_publish_count(), 1);
}
