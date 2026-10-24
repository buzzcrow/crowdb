// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/payload_stream.rs"]
mod payload_stream;

use bytes::Bytes;
use crowdb_access_dataset::{DatasetIdentity, DatasetStore, DatasetUploads, NamespacePath, OperationId};
use md5::{Digest, Md5};
use payload_stream::{TestPayloadStreamStorage, TestStore};
use std::sync::{atomic::Ordering, Arc};

async fn fixture() -> (DatasetUploads, Vec<u8>, Arc<TestStore>) {
    let store = Arc::new(TestStore::default());
    let uploads = DatasetUploads::new(store.clone(), Arc::new(TestPayloadStreamStorage::default()));
    let identity = DatasetIdentity::new(NamespacePath::root(), "cost").unwrap();
    let location = DatasetUploads::location(&identity, OperationId::random()).unwrap();
    uploads
        .create(&location, 10, 4, Md5::digest(b"abcdefghij").into(), 100)
        .await
        .unwrap();
    for (index, part) in b"abcdefghij".chunks(4).enumerate() {
        uploads
            .put_part(
                &location,
                u32::try_from(index).unwrap(),
                Bytes::copy_from_slice(part),
                1,
            )
            .await
            .unwrap();
    }
    uploads.complete(&location, 2).await.unwrap();
    (uploads, location, store)
}

#[tokio::test]
async fn buffered_and_streaming_reads_use_one_description_plus_one_get_per_part() {
    let (uploads, location, store) = fixture().await;
    for streaming in [false, true] {
        let reads = store.reads.load(Ordering::Relaxed);
        let writes = store.mutations.load(Ordering::Relaxed);
        let descriptor = uploads.open_read(&location).await.unwrap();
        assert_eq!(descriptor.length(), 10);
        assert_eq!(descriptor.md5(), <[u8; 16]>::from(Md5::digest(b"abcdefghij")));
        assert_eq!(descriptor.part_count(), 3);
        let mut bytes = Vec::new();
        for index in 0..descriptor.part_count() {
            if streaming {
                let mut part = descriptor.read_part_stream(index).await.unwrap();
                while let Some(payload) = part.next().await.unwrap() {
                    bytes.extend_from_slice(payload.as_ref());
                }
                assert!(part.next().await.unwrap().is_none());
            } else {
                let (part, digest) = descriptor.read_part_verified(index).await.unwrap();
                assert_eq!(digest, <[u8; 16]>::from(Md5::digest(&part)));
                bytes.extend_from_slice(&part);
            }
        }
        assert_eq!(bytes, b"abcdefghij");
        // EOF and invalid indices must not cause another metadata RPC.
        assert!(descriptor.read_part_verified(3).await.is_err());
        assert!(descriptor.read_part_stream(u32::MAX).await.is_err());
        assert_eq!(store.reads.load(Ordering::Relaxed) - reads, 4);
        assert_eq!(store.mutations.load(Ordering::Relaxed), writes);
    }
}

#[tokio::test]
async fn a_descriptor_does_not_pin_reclaimed_data_or_admit_new_reads() {
    let (uploads, location, store) = fixture().await;
    let descriptor = uploads.open_read(&location).await.unwrap();
    let mut part = descriptor.read_part_stream(0).await.unwrap();
    let held = part.next().await.unwrap().unwrap();
    uploads.abort(&location).await.unwrap();
    assert!(uploads.open_read(&location).await.is_err());
    let reads = store.reads.load(Ordering::Relaxed);
    assert!(descriptor.read_part_stream(1).await.is_err());
    assert!(descriptor.read_part_verified(1).await.is_err());
    assert_eq!(store.reads.load(Ordering::Relaxed) - reads, 2);
    assert_eq!(held.as_ref(), b"ab");
}

#[tokio::test]
async fn cached_description_still_checks_part_index_length_and_digest() {
    for damage in ["index", "length", "md5"] {
        let (uploads, location, store) = fixture().await;
        let descriptor = uploads.open_read(&location).await.unwrap();
        let mut key = location.clone();
        key.extend_from_slice(b"\0P");
        key.extend_from_slice(&1_u32.to_be_bytes());
        let old = store.get(&key).await.unwrap().unwrap();
        let mut entry: serde_json::Value = serde_json::from_slice(&old.bytes).unwrap();
        match damage {
            "index" => entry["part"]["index"] = 0.into(),
            "length" => entry["part"]["length"] = 3.into(),
            _ => entry["part"]["md5"][0] = (entry["part"]["md5"][0].as_u64().unwrap() ^ 1).into(),
        }
        store
            .compare_exchange(&key, Some(&old.bytes), &serde_json::to_vec(&entry).unwrap())
            .await
            .unwrap();
        assert!(descriptor.read_part_verified(1).await.is_err(), "{damage}");
        match descriptor.read_part_stream(1).await {
            Err(_) => assert_ne!(damage, "md5"),
            Ok(mut part) => loop {
                match part.next().await {
                    Ok(Some(_)) => {}
                    Ok(None) => panic!("corrupt {damage} incorrectly completed"),
                    Err(_) => break,
                }
            },
        }
    }
}

#[tokio::test]
async fn bounded_compatibility_reader_reuses_description_across_part_boundaries() {
    use crowdb_access_dataset::{ChunkReader, ReadCancellation, UploadedChunkReader};
    let (uploads, location, store) = fixture().await;
    let reader = UploadedChunkReader::new(uploads);
    let before = store.reads.load(Ordering::Relaxed);
    let bytes = reader
        .read(&location, Some(2..9), &ReadCancellation::new())
        .await
        .unwrap();
    assert_eq!(bytes, b"cdefghi");
    assert_eq!(store.reads.load(Ordering::Relaxed) - before, 4);
}
