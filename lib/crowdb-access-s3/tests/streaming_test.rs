// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::future::poll_fn;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll};

use async_trait::async_trait;
use crowdb_access_s3::metadata::{BucketId, ObjectRecord};
use crowdb_access_s3::metrics::S3Metrics;
use crowdb_access_s3::native_buffer::NativeBodyAllocator;
use crowdb_access_s3::streaming::{
    attach_completed_locations, cleanup_after_definite_error, write_body,
    write_native_body_with_checksums_metered, FailedPublicationCleanup, FailedPublicationTarget,
    PutErrorCode, PutOutcome,
};
use crowdb_chunk_client::{ChunkIoWriter, FeedStatus, FramedWriteBuffer, IoError};
use crowdb_protocol::chunkdb::rpc::Location;
use crowdb_protocol::frame::{MAX_FRAME_BYTES, MAX_FRAME_PAYLOAD_BYTES};
use hyper::body::{Body, Bytes, Frame, Http1BodyReceiveProvider, SizeHint};

#[test]
fn completed_locations_become_the_object_data_reference() {
    let mut object = ObjectRecord {
        bucket_id: BucketId::new([1; 16]),
        key: b"key".to_vec(),
        logical_length: 3,
        checksum: b"sum".to_vec(),
        etag: "etag".into(),
        created_at_ms: 1,
        modified_at_ms: 1,
        content_type: "application/octet-stream".into(),
        attributes: Vec::new(),
        data_reference: Vec::new(),
        data_length: 3,
    };
    let locations = vec![Location {
        offset: 4,
        length: 3,
        logical_offset: 0,
        logical_length: 3,
        ..Location::default()
    }];
    attach_completed_locations(&mut object, &locations).expect("locations encode");
    assert_eq!(
        bincode::deserialize::<Vec<Location>>(&object.data_reference).expect("locations decode"),
        locations
    );
}

#[derive(Default)]
struct TestCleanup(AtomicUsize);

#[async_trait]
impl FailedPublicationCleanup for TestCleanup {
    async fn cleanup(&self, request_id: &str, _targets: &[FailedPublicationTarget]) {
        assert_eq!(request_id, "request-17");
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

#[tokio::test]
async fn only_definite_errors_run_cleanup() {
    let cleanup = TestCleanup::default();
    let cleanup_targets = vec![FailedPublicationTarget::SharedRange(Location::default())];
    let error = PutOutcome::Error {
        code: PutErrorCode::KvRejected,
        message: "rejected".into(),
    };
    assert!(matches!(
        cleanup_after_definite_error("request-17", error, &cleanup_targets, &cleanup).await,
        PutOutcome::Error { .. }
    ));
    assert_eq!(cleanup.0.load(Ordering::Relaxed), 1);
    assert_eq!(
        cleanup_after_definite_error("request-17", PutOutcome::Timeout, &cleanup_targets, &cleanup).await,
        PutOutcome::Timeout
    );
    assert_eq!(cleanup.0.load(Ordering::Relaxed), 1);
}

struct TestBody {
    frames: VecDeque<Bytes>,
    polls: AtomicUsize,
}

impl Body for TestBody {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _context: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Self::Data>, Self::Error>>> {
        self.polls.fetch_add(1, Ordering::Relaxed);
        Poll::Ready(self.frames.pop_front().map(|data| Ok(Frame::data(data))))
    }

    fn is_end_stream(&self) -> bool {
        self.frames.is_empty()
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::default()
    }
}

#[derive(Default)]
struct RejectingWriter(AtomicUsize);

#[async_trait]
impl ChunkIoWriter for RejectingWriter {
    async fn on_data(&mut self, _buffer: Bytes) -> crowdb_chunk_client::Result<FeedStatus> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Err(IoError::WriteFailed("injected writer failure".into()))
    }

    async fn on_finish(&mut self) -> crowdb_chunk_client::Result<Vec<Location>> {
        Ok(Vec::new())
    }

    async fn on_error(&mut self) -> crowdb_chunk_client::Result<Vec<Location>> {
        Ok(Vec::new())
    }

    fn require_data(&self) -> bool {
        true
    }
}

#[derive(Default)]
struct LengthBoundWriter {
    frames: AtomicUsize,
}

#[async_trait]
impl ChunkIoWriter for LengthBoundWriter {
    async fn on_data(&mut self, _buffer: Bytes) -> crowdb_chunk_client::Result<FeedStatus> {
        self.frames.fetch_add(1, Ordering::Relaxed);
        Ok(FeedStatus::Pause)
    }

    async fn on_finish(&mut self) -> crowdb_chunk_client::Result<Vec<Location>> {
        Ok(Vec::new())
    }

    async fn on_error(&mut self) -> crowdb_chunk_client::Result<Vec<Location>> {
        Ok(Vec::new())
    }

    fn require_data(&self) -> bool {
        self.frames.load(Ordering::Relaxed) == 0
    }

    fn input_complete(&self) -> bool {
        self.frames.load(Ordering::Relaxed) == 1
    }
}

#[tokio::test]
async fn writer_error_stops_body_polling_before_the_next_frame() {
    let mut body = TestBody {
        frames: VecDeque::from([Bytes::from_static(b"first"), Bytes::from_static(b"second")]),
        polls: AtomicUsize::new(0),
    };
    let mut writer = RejectingWriter::default();

    assert!(matches!(
        write_body(&mut body, &mut writer).await,
        Err(PutOutcome::Error {
            code: PutErrorCode::ChunkWrite,
            ..
        })
    ));
    assert_eq!(writer.0.load(Ordering::Relaxed), 1);
    assert_eq!(body.polls.load(Ordering::Relaxed), 1);
    assert_eq!(body.frames.len(), 1);
}

#[tokio::test]
async fn declared_length_completion_does_not_poll_for_an_extra_body_frame() {
    let mut body = TestBody {
        frames: VecDeque::from([Bytes::from_static(b"only"), Bytes::from_static(b"unexpected")]),
        polls: AtomicUsize::new(0),
    };
    let mut writer = LengthBoundWriter::default();

    write_body(&mut body, &mut writer).await.expect("body accepted");
    assert_eq!(body.polls.load(Ordering::Relaxed), 1);
}

#[derive(Default)]
struct NativeOwnerWriter {
    generic_frames: usize,
    owner_frames: usize,
    logical_bytes: u64,
}

#[async_trait]
impl ChunkIoWriter for NativeOwnerWriter {
    async fn on_data(&mut self, _buffer: Bytes) -> crowdb_chunk_client::Result<FeedStatus> {
        self.generic_frames += 1;
        Ok(FeedStatus::Continue)
    }

    async fn on_framed_data(
        &mut self,
        buffer: Box<dyn FramedWriteBuffer>,
    ) -> crowdb_chunk_client::Result<FeedStatus> {
        self.owner_frames += 1;
        self.logical_bytes += buffer.logical_len();
        Ok(FeedStatus::Continue)
    }

    async fn on_finish(&mut self) -> crowdb_chunk_client::Result<Vec<Location>> {
        Ok(Vec::new())
    }

    async fn on_error(&mut self) -> crowdb_chunk_client::Result<Vec<Location>> {
        Ok(Vec::new())
    }

    fn require_data(&self) -> bool {
        true
    }
}

#[tokio::test]
async fn native_body_hashes_payload_and_hands_owner_to_writer_once() {
    let allocator = NativeBodyAllocator::new(MAX_FRAME_BYTES, MAX_FRAME_BYTES).unwrap();
    let receiver = allocator.object_receiver();
    receiver.enable_owner_handoff();
    let mut allocation = poll_fn(|cx| receiver.poll_next_buffer(cx, MAX_FRAME_PAYLOAD_BYTES))
        .await
        .unwrap();
    allocation
        .spare_capacity_mut()
        .fill(std::mem::MaybeUninit::new(0x5a));
    allocation.advance(MAX_FRAME_PAYLOAD_BYTES).unwrap();
    let payload = receiver.on_data_ready(allocation).unwrap();
    let mut body = TestBody {
        frames: VecDeque::from([payload]),
        polls: AtomicUsize::new(0),
    };
    let mut writer = NativeOwnerWriter::default();
    let metrics = S3Metrics::default();

    let (_, checksum) = write_native_body_with_checksums_metered(
        &mut body,
        &mut writer,
        &receiver,
        None,
        None,
        Some(&metrics),
    )
    .await
    .unwrap();

    assert_eq!(writer.generic_frames, 0);
    assert_eq!(writer.owner_frames, 1);
    assert_eq!(writer.logical_bytes, MAX_FRAME_PAYLOAD_BYTES as u64);
    assert_eq!(checksum.len(), 16);
    assert_eq!(metrics.snapshot().checksum_bytes, MAX_FRAME_PAYLOAD_BYTES as u64);
}

#[tokio::test]
async fn prefetched_body_uses_scattered_framed_owner_instead_of_generic_copy() {
    let allocator = NativeBodyAllocator::new(MAX_FRAME_BYTES, MAX_FRAME_BYTES).unwrap();
    let receiver = allocator.object_receiver();
    receiver.enable_owner_handoff();
    let payload = receiver
        .on_prefetched_data(Bytes::from_static(b"header-read-ahead"))
        .unwrap();
    let mut body = TestBody {
        frames: VecDeque::from([payload]),
        polls: AtomicUsize::new(0),
    };
    let mut writer = NativeOwnerWriter::default();

    write_native_body_with_checksums_metered(&mut body, &mut writer, &receiver, None, None, None)
        .await
        .unwrap();

    assert_eq!(writer.generic_frames, 0);
    assert_eq!(writer.owner_frames, 1);
    assert_eq!(writer.logical_bytes, 17);
}
