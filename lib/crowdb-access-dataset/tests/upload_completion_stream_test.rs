// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/upload_completion.rs"]
mod upload_completion;

use bytes::Bytes;
use crowdb_access_dataset::{
    DatasetIdentity, DatasetStore, DatasetUploads, NamespacePath, OperationId, UploadError, UploadState,
    MAX_UPLOAD_PART_BYTES,
};
use md5::{Digest, Md5};
use std::sync::{atomic::Ordering, Arc};
use upload_completion::{TestCompletionStorage, TestStore};

struct TestUpload {
    uploads: DatasetUploads,
    store: Arc<TestStore>,
    payloads: Arc<TestCompletionStorage>,
    location: Vec<u8>,
}

impl TestUpload {
    async fn new(bytes: Bytes, part_size: u32, fragment: usize, declared: Option<[u8; 16]>) -> Self {
        let store = Arc::new(TestStore::default());
        let payloads = Arc::new(TestCompletionStorage::new(fragment));
        let uploads = DatasetUploads::new(store.clone(), payloads.clone());
        let identity = DatasetIdentity::new(NamespacePath::root(), "completion-stream").unwrap();
        let location = DatasetUploads::location(&identity, OperationId::random()).unwrap();
        uploads
            .create(
                &location,
                bytes.len() as u64,
                part_size,
                declared.unwrap_or_else(|| Md5::digest(&bytes).into()),
                100,
            )
            .await
            .unwrap();
        for (index, offset) in (0..bytes.len()).step_by(part_size as usize).enumerate() {
            uploads
                .put_part(
                    &location,
                    u32::try_from(index).unwrap(),
                    bytes.slice(offset..(offset + part_size as usize).min(bytes.len())),
                    1,
                )
                .await
                .unwrap();
        }
        Self {
            uploads,
            store,
            payloads,
            location,
        }
    }

    fn assert_released(&self) {
        assert_eq!(self.payloads.state.active.load(Ordering::Relaxed), 0);
        assert_eq!(self.payloads.state.streams.load(Ordering::Relaxed), 0);
        assert_eq!(self.payloads.buffered_reads.load(Ordering::Relaxed), 0);
    }
}

#[tokio::test]
async fn single_and_multiple_parts_stream_original_owners_with_unchanged_metadata_cost() {
    for (length, part_size, fragment) in [
        (1, 1, 1),
        (513, 513, 17),
        (10, 4, 2),
        (MAX_UPLOAD_PART_BYTES, MAX_UPLOAD_PART_BYTES, 65_502),
        (MAX_UPLOAD_PART_BYTES + 513, MAX_UPLOAD_PART_BYTES, 65_502),
    ] {
        let bytes = Bytes::from(vec![7; length]);
        let fixture = TestUpload::new(bytes, u32::try_from(part_size).unwrap(), fragment, None).await;
        let reads = fixture.store.reads.load(Ordering::Relaxed);
        let scans = fixture.store.scans.load(Ordering::Relaxed);
        let writes = fixture.store.mutations.load(Ordering::Relaxed);
        let result = fixture.uploads.complete(&fixture.location, 2).await.unwrap();
        let part_count = length.div_ceil(part_size);
        assert_eq!(result.state, UploadState::Complete);
        assert_eq!(result.parts.len(), part_count);
        assert_eq!(fixture.store.reads.load(Ordering::Relaxed) - reads, 1);
        assert_eq!(fixture.store.scans.load(Ordering::Relaxed) - scans, 1);
        assert_eq!(fixture.store.mutations.load(Ordering::Relaxed) - writes, 1);
        assert_eq!(fixture.payloads.stream_reads.load(Ordering::Relaxed), part_count);
        assert_eq!(fixture.payloads.state.eof.load(Ordering::Relaxed), part_count);
        assert_eq!(fixture.payloads.state.delivered.load(Ordering::Relaxed), length);
        fixture.assert_released();

        let writes = fixture.store.mutations.load(Ordering::Relaxed);
        assert_eq!(
            fixture.uploads.complete(&fixture.location, 3).await.unwrap(),
            result
        );
        assert_eq!(fixture.store.mutations.load(Ordering::Relaxed), writes);
        assert_eq!(fixture.payloads.stream_reads.load(Ordering::Relaxed), part_count);
    }
}

#[tokio::test]
async fn failed_or_incomplete_streams_leave_upload_open_for_a_full_retry() {
    for part_size in [4, 8] {
        for fault in upload_completion::TRUNCATED..=upload_completion::PARTIAL_ERROR {
            let fixture = TestUpload::new(Bytes::from_static(b"abcdefgh"), part_size, 2, None).await;
            fixture.payloads.fault.store(fault, Ordering::Release);
            let completed_parts = usize::from(part_size == 4);
            fixture
                .payloads
                .fault_stream
                .store(completed_parts, Ordering::Release);
            let writes = fixture.store.mutations.load(Ordering::Relaxed);
            let error = fixture.uploads.complete(&fixture.location, 2).await.unwrap_err();
            assert!(matches!(error, UploadError::Integrity | UploadError::Storage(_)));
            assert_eq!(fixture.store.mutations.load(Ordering::Relaxed), writes);
            assert_eq!(
                fixture.uploads.metadata(&fixture.location).await.unwrap().state,
                UploadState::Open
            );
            fixture.assert_released();

            fixture.payloads.fault.store(0, Ordering::Release);
            assert_eq!(
                fixture
                    .uploads
                    .complete(&fixture.location, 3)
                    .await
                    .unwrap()
                    .state,
                UploadState::Complete
            );
            fixture.assert_released();
        }
    }
}

#[tokio::test]
async fn dropping_completion_before_eof_does_not_publish_or_retain_read_credits() {
    for part_size in [4, 8] {
        for fault in [upload_completion::PAUSE_PARTIAL, upload_completion::PAUSE_EOF] {
            let fixture = TestUpload::new(Bytes::from_static(b"abcdefgh"), part_size, 2, None).await;
            fixture.payloads.fault.store(fault, Ordering::Release);
            let completed_parts = usize::from(part_size == 4);
            fixture
                .payloads
                .fault_stream
                .store(completed_parts, Ordering::Release);
            let writes = fixture.store.mutations.load(Ordering::Relaxed);
            let mut completion = Box::pin(fixture.uploads.complete(&fixture.location, 2));
            assert!(futures::poll!(completion.as_mut()).is_pending());
            assert!(fixture.payloads.state.paused.load(Ordering::Acquire));
            assert_eq!(fixture.payloads.state.active.load(Ordering::Relaxed), 0);
            assert_eq!(fixture.payloads.state.streams.load(Ordering::Relaxed), 1);
            assert_eq!(
                fixture.payloads.state.eof.load(Ordering::Relaxed),
                completed_parts
            );
            assert_eq!(fixture.store.mutations.load(Ordering::Relaxed), writes);
            drop(completion);
            fixture.assert_released();
            assert_eq!(
                fixture.uploads.metadata(&fixture.location).await.unwrap().state,
                UploadState::Open
            );

            fixture.payloads.fault.store(0, Ordering::Release);
            assert_eq!(
                fixture
                    .uploads
                    .complete(&fixture.location, 3)
                    .await
                    .unwrap()
                    .state,
                UploadState::Complete
            );
            fixture.assert_released();
        }
    }
}

#[tokio::test]
async fn individual_part_and_whole_upload_digests_remain_independent_requirements() {
    let bytes = Bytes::from_static(b"abcdefgh");
    for part_size in [4, 8] {
        let fixture = TestUpload::new(bytes.clone(), part_size, 2, Some(Md5::digest(b"wrong").into())).await;
        let writes = fixture.store.mutations.load(Ordering::Relaxed);
        assert!(matches!(
            fixture.uploads.complete(&fixture.location, 2).await,
            Err(UploadError::Integrity)
        ));
        assert_eq!(fixture.store.mutations.load(Ordering::Relaxed), writes);
        assert_eq!(
            fixture.payloads.stream_reads.load(Ordering::Relaxed),
            if part_size == 8 { 0 } else { 2 }
        );
        fixture.assert_released();
    }

    let fixture = TestUpload::new(bytes, 4, 2, None).await;
    let mut key = fixture.location.clone();
    key.extend_from_slice(b"\0P\0\0\0\0");
    let stored = fixture.store.get(&key).await.unwrap().unwrap();
    let mut part: serde_json::Value = serde_json::from_slice(&stored.bytes).unwrap();
    part["part"]["md5"] = serde_json::to_value(<[u8; 16]>::from(Md5::digest(b"fake"))).unwrap();
    fixture
        .store
        .compare_exchange(&key, Some(&stored.bytes), &serde_json::to_vec(&part).unwrap())
        .await
        .unwrap();
    let writes = fixture.store.mutations.load(Ordering::Relaxed);
    assert!(matches!(
        fixture.uploads.complete(&fixture.location, 2).await,
        Err(UploadError::Integrity)
    ));
    assert_eq!(fixture.store.mutations.load(Ordering::Relaxed), writes);
    assert_eq!(fixture.payloads.stream_reads.load(Ordering::Relaxed), 1);
    fixture.assert_released();
}
