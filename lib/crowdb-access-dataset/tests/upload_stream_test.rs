// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/payload_stream.rs"]
mod payload_stream;

use bytes::Bytes;
use crowdb_access_dataset::{DatasetIdentity, DatasetUploads, NamespacePath, OperationId};
use md5::{Digest, Md5};
use payload_stream::{TestPayloadStreamStorage, TestStore};
use std::sync::{atomic::Ordering, Arc};

async fn fixture() -> (DatasetUploads, Vec<u8>, Arc<TestPayloadStreamStorage>) {
    let payloads = Arc::new(TestPayloadStreamStorage::default());
    let uploads = DatasetUploads::new(Arc::new(TestStore::default()), payloads.clone());
    let identity = DatasetIdentity::new(NamespacePath::root(), "owned").unwrap();
    let location = DatasetUploads::location(&identity, OperationId::random()).unwrap();
    uploads
        .create(&location, 4, 4, Md5::digest(b"data").into(), 100)
        .await
        .unwrap();
    uploads
        .put_part(&location, 0, Bytes::from_static(b"data"), 1)
        .await
        .unwrap();
    (uploads, location, payloads)
}

#[tokio::test]
async fn stream_retains_allocation_without_collecting_and_verifies_at_eof() {
    let (uploads, location, payloads) = fixture().await;
    assert!(uploads.read_part_stream(&location, 0).await.is_err());
    uploads.complete(&location, 2).await.unwrap();
    let mut stream = uploads.read_part_stream(&location, 0).await.unwrap();
    assert!(stream.verified_digest().is_none());
    assert_eq!(payloads.pulls.load(Ordering::Relaxed), 0);
    let first = stream.next().await.unwrap().unwrap();
    let second = stream.next().await.unwrap().unwrap();
    assert!(stream.verified_digest().is_none());
    assert!(Arc::ptr_eq(
        first.allocation().unwrap(),
        second.allocation().unwrap()
    ));
    assert!(first.allocation().unwrap().capacity() >= 4100);
    assert_eq!(first.as_ref(), b"da");
    assert_eq!(second.as_ref(), b"ta");
    assert!(stream.next().await.unwrap().is_none());
    assert_eq!(stream.verified_digest(), Some((4, Md5::digest(b"data").into())));
    assert!(stream.next().await.unwrap().is_none());
    drop(stream);
    drop(uploads);
    assert_eq!(second.as_ref(), b"ta");
}

#[tokio::test]
async fn corruption_truncation_overrun_and_partial_transport_failure_stay_terminal() {
    for mode in 1..=4 {
        let (uploads, location, payloads) = fixture().await;
        uploads.complete(&location, 2).await.unwrap();
        payloads.mode.store(mode, Ordering::Release);
        let mut stream = uploads.read_part_stream(&location, 0).await.unwrap();
        loop {
            match stream.next().await {
                Ok(Some(_)) => {}
                Ok(None) => panic!("mode {mode} incorrectly completed"),
                Err(_) => break,
            }
        }
        let pulls = payloads.pulls.load(Ordering::Relaxed);
        assert!(stream.next().await.is_err());
        assert!(stream.verified_digest().is_none());
        assert_eq!(payloads.pulls.load(Ordering::Relaxed), pulls);
    }
}
