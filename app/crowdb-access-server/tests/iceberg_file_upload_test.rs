#![cfg(feature = "iceberg")]

#[path = "common/iceberg_upload.rs"]
mod common;

use crowdb_access_iceberg::file::FileReader;
use crowdb_access_server::iceberg::{FileUploadBudget, FileUploadConstraints, FileUploadError};
use hyper::body::{Bytes, Frame};
use sha2::{Digest, Sha256};
use std::sync::{atomic::Ordering, Arc};

fn constraints(max_bytes: u64) -> FileUploadConstraints {
    FileUploadConstraints {
        max_bytes,
        content_length: None,
        sha256: None,
    }
}

#[tokio::test]
async fn native_upload_pulls_bounded_frames_and_verifies_exact_bytes_before_returning_tree() {
    let bytes = vec![19; 1024 * 1024 + 23];
    let store = Arc::new(common::TestUploadBlocks::default());
    let budget = FileUploadBudget::new(1).unwrap();
    let identity = common::owner();
    let tree = budget
        .receive(
            common::TestUploadBody::new(&bytes, 16 * 1024),
            store.clone(),
            identity,
            FileUploadConstraints {
                max_bytes: bytes.len() as u64,
                content_length: Some(bytes.len() as u64),
                sha256: Some(Sha256::digest(&bytes).into()),
            },
        )
        .await
        .unwrap();
    assert_eq!(budget.active(), 0);
    assert_eq!(tree.length, bytes.len() as u64);
    assert!(store.max_input.load(Ordering::SeqCst) <= 256 * 1024);
    let mut reader = FileReader::from_tree(store, identity, tree, None, 16 * 1024).unwrap();
    let mut actual = Vec::new();
    while let Some(frame) = reader.next().await.unwrap() {
        actual.extend_from_slice(&frame);
    }
    assert_eq!(actual, bytes);
}

#[tokio::test]
async fn upload_byte_length_digest_and_frame_failures_never_return_a_tree() {
    let budget = FileUploadBudget::new(1).unwrap();
    let store = Arc::new(common::TestUploadBlocks::default());
    let identity = common::owner();
    for maximum in [0, u64::MAX] {
        let body = common::TestUploadBody::new(b"bytes", 5);
        let polls = body.polls.clone();
        assert!(matches!(
            budget
                .receive(body, store.clone(), identity, constraints(maximum))
                .await,
            Err(FileUploadError::Bounds)
        ));
        assert_eq!(polls.load(Ordering::SeqCst), 0);
    }
    for declared in [4, 6] {
        assert!(matches!(
            budget
                .receive(
                    common::TestUploadBody::new(b"bytes", 2),
                    store.clone(),
                    identity,
                    FileUploadConstraints {
                        content_length: Some(declared),
                        ..constraints(10)
                    }
                )
                .await,
            Err(FileUploadError::Length)
        ));
    }
    assert!(matches!(
        budget
            .receive(
                common::TestUploadBody::new(b"bytes", 2),
                store.clone(),
                identity,
                constraints(4)
            )
            .await,
        Err(FileUploadError::Bounds)
    ));
    assert!(matches!(
        budget
            .receive(
                common::TestUploadBody::new(b"bytes", 2),
                store.clone(),
                identity,
                FileUploadConstraints {
                    sha256: Some([0; 32]),
                    ..constraints(10)
                }
            )
            .await,
        Err(FileUploadError::Digest)
    ));
    assert!(matches!(
        budget
            .receive(
                common::TestUploadBody::new(&vec![1; 65_537], 65_537),
                store.clone(),
                identity,
                constraints(100_000)
            )
            .await,
        Err(FileUploadError::Bounds)
    ));
    assert_eq!(budget.active(), 0);
}

#[tokio::test]
async fn upload_transport_and_storage_errors_retain_orphans_but_empty_uploads_succeed() {
    let budget = FileUploadBudget::new(1).unwrap();
    let store = Arc::new(common::TestUploadBlocks::default());
    let identity = common::owner();
    let mut body = common::TestUploadBody::new(b"", 1);
    body.frames
        .push_back(Err(std::io::Error::other("test body failure")));
    assert!(matches!(
        budget
            .receive(body, store.clone(), identity, constraints(10))
            .await,
        Err(FileUploadError::Body)
    ));
    let mut body = common::TestUploadBody::new(b"", 1);
    body.frames
        .push_back(Ok(Frame::trailers(hyper::HeaderMap::new())));
    assert!(matches!(
        budget
            .receive(body, store.clone(), identity, constraints(10))
            .await,
        Err(FileUploadError::Trailers)
    ));
    store.fail.store(true, Ordering::SeqCst);
    assert!(matches!(
        budget
            .receive(
                common::TestUploadBody::new(b"bytes", 2),
                store.clone(),
                identity,
                constraints(10)
            )
            .await,
        Err(FileUploadError::Storage(_))
    ));
    assert_eq!(budget.active(), 0);
    assert!(!store.values.load().is_empty());
    store.fail.store(false, Ordering::SeqCst);
    let empty = budget
        .receive(
            common::TestUploadBody::new(b"", 1),
            store,
            identity,
            FileUploadConstraints {
                content_length: Some(0),
                sha256: Some(Sha256::digest([]).into()),
                ..constraints(1)
            },
        )
        .await
        .unwrap();
    assert_eq!(empty.length, 0);
    assert!(empty.root.is_none());
}

#[tokio::test]
async fn pending_storage_applies_backpressure_and_cancellation_releases_only_memory_credit() {
    let budget = FileUploadBudget::new(1).unwrap();
    let store = Arc::new(common::TestUploadBlocks::default());
    store.pause.store(true, Ordering::SeqCst);
    let body = common::TestUploadBody::new(&vec![7; 512 * 1024], 64 * 1024);
    let polls = body.polls.clone();
    let mut upload = Box::pin(budget.receive(body, store.clone(), common::owner(), constraints(1024 * 1024)));
    tokio::select! {
        result = &mut upload => panic!("upload completed before storage release: {result:?}"),
        () = store.entered.notified() => {}
    }
    assert_eq!(budget.active(), 1);
    assert_eq!(polls.load(Ordering::SeqCst), 4);
    let mut other = common::TestUploadBody::new(b"", 1);
    other.frames.push_back(Ok(Frame::data(Bytes::new())));
    let other_polls = other.polls.clone();
    assert!(matches!(
        budget
            .receive(other, store.clone(), common::owner(), constraints(10))
            .await,
        Err(FileUploadError::Busy)
    ));
    assert_eq!(other_polls.load(Ordering::SeqCst), 0);
    drop(upload);
    assert_eq!(budget.active(), 0);
    assert_eq!(polls.load(Ordering::SeqCst), 4);
    assert_eq!(store.values.load().len(), 1);
}
