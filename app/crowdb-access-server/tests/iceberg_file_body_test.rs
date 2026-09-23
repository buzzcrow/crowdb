#![cfg(feature = "iceberg")]

#[path = "common/iceberg_file_blocks.rs"]
mod blocks;

use blocks::TestFileBlocks;
use crowdb_access_iceberg::file::ByteRange;
use crowdb_access_server::iceberg::{FileBodyError, FileResponseBudget};
use http_body_util::BodyExt;
use hyper::body::Body;
use std::sync::{atomic::Ordering, Arc};

#[tokio::test]
async fn file_http_body_pulls_bounded_frames_and_releases_credit_on_completion() {
    let store = Arc::new(TestFileBlocks {
        bytes: vec![17; 70_000],
        ..Default::default()
    });
    let budget = FileResponseBudget::new(1).unwrap();
    let record = store.record();
    let mut body = budget.body(store.clone(), record.clone(), None).unwrap();
    assert_eq!(budget.active(), 1);
    assert!(matches!(
        budget.body(store.clone(), record, None),
        Err(FileBodyError::Busy)
    ));
    assert_eq!(store.reads.load(Ordering::SeqCst), 0);
    let mut output = Vec::new();
    while let Some(frame) = body.frame().await {
        let bytes = frame.unwrap().into_data().unwrap();
        assert!(bytes.len() <= 16 * 1024);
        output.extend_from_slice(&bytes);
        assert_eq!(body.size_hint().exact(), Some(70_000 - output.len() as u64));
        assert_eq!(store.reads.load(Ordering::SeqCst), 1);
    }
    assert_eq!(output, store.bytes);
    assert!(body.is_end_stream());
    assert_eq!(budget.active(), 0);
}

#[tokio::test]
async fn file_http_body_handles_ranges_empty_files_invalid_records_and_read_errors() {
    let store = Arc::new(TestFileBlocks {
        bytes: vec![5; 100],
        ..Default::default()
    });
    let budget = FileResponseBudget::new(1).unwrap();
    let body = budget
        .body(
            store.clone(),
            store.record(),
            Some(ByteRange { start: 10, end: 40 }),
        )
        .unwrap();
    assert_eq!(body.collect().await.unwrap().to_bytes().len(), 30);
    assert_eq!(budget.active(), 0);
    assert!(budget
        .body(
            store.clone(),
            store.record(),
            Some(ByteRange { start: 101, end: 100 })
        )
        .is_err());
    assert_eq!(budget.active(), 0);
    store.fail.store(true, Ordering::SeqCst);
    let mut body = budget.body(store.clone(), store.record(), None).unwrap();
    assert!(body.frame().await.unwrap().is_err());
    assert!(body.frame().await.is_none());
    assert_eq!(budget.active(), 0);
    let store = Arc::new(TestFileBlocks::default());
    let body = budget.body(store.clone(), store.record(), None).unwrap();
    assert!(body.is_end_stream());
    assert_eq!(budget.active(), 0);
}

#[tokio::test]
async fn dropping_pending_file_http_body_cancels_reads_and_releases_admission() {
    let store = Arc::new(TestFileBlocks {
        bytes: vec![1; 100],
        ..Default::default()
    });
    store.pause.store(true, Ordering::SeqCst);
    let budget = FileResponseBudget::new(1).unwrap();
    let mut body = budget.body(store.clone(), store.record(), None).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        tokio::select! {
            _ = body.frame() => panic!("paused read unexpectedly returned"),
            () = store.entered.notified() => {}
        }
    })
    .await
    .unwrap();
    assert_eq!(budget.active(), 1);
    drop(body);
    assert_eq!(budget.active(), 0);
    assert_eq!(store.reads.load(Ordering::SeqCst), 1);
}
