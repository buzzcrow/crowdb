// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use crowdb_chunk_client::{reclaim_location, ChunkAllocator, IoError, ReclaimOutcome, Result};
use crowdb_protocol::chunkdb::rpc::{
    AllocateChunkRequest, AllocateChunkResponse, AppendChunkRequest, AppendChunkResponse,
    DeleteChunkRangeRequest, DeleteChunkRangeResponse, DeleteChunkRequest, DeleteChunkResponse, Location,
    QueryChunkRequest, QueryChunkResponse, SealChunkRequest, SealChunkResponse,
};
use crowdb_protocol::common::ChunkId;

enum TestReply {
    Accepted(bool),
    Missing,
    Deferred,
    Unsupported,
    Unknown,
}

struct TestAllocator {
    calls: AtomicUsize,
    reply: TestReply,
    expected_range: (u32, u32),
}

impl TestAllocator {
    fn new(reply: TestReply, expected_range: (u32, u32)) -> Self {
        Self {
            calls: AtomicUsize::new(0),
            reply,
            expected_range,
        }
    }

    fn location(&self) -> Location {
        Location {
            chunk_id: Some(ChunkId { high: 1, low: 2 }),
            offset: u64::from(self.expected_range.0),
            length: u64::from(self.expected_range.1),
            logical_offset: 0,
            logical_length: u64::from(self.expected_range.1),
        }
    }
}

#[async_trait]
impl ChunkAllocator for TestAllocator {
    async fn allocate_chunk(&self, _: AllocateChunkRequest) -> Result<AllocateChunkResponse> {
        unreachable!()
    }
    async fn append_chunk(&self, _: AppendChunkRequest) -> Result<AppendChunkResponse> {
        unreachable!()
    }
    async fn seal_chunk(&self, _: SealChunkRequest) -> Result<SealChunkResponse> {
        unreachable!()
    }
    async fn query_chunk(&self, _: QueryChunkRequest) -> Result<QueryChunkResponse> {
        panic!("range notifications must not query chunk metadata");
    }
    async fn delete_chunk(&self, _: DeleteChunkRequest) -> Result<DeleteChunkResponse> {
        panic!("range notifications must not delete a whole chunk");
    }
    async fn delete_chunk_range(&self, request: DeleteChunkRangeRequest) -> Result<DeleteChunkRangeResponse> {
        assert_eq!(request.chunk_id, self.location().chunk_id);
        assert_eq!((request.chunk_offset, request.chunk_size), self.expected_range);
        assert_eq!(request.expected_modify_ts, 0);
        self.calls.fetch_add(1, Ordering::Relaxed);
        match self.reply {
            TestReply::Accepted(reclaimed) => Ok(DeleteChunkRangeResponse { reclaimed }),
            TestReply::Missing => Err(IoError::ChunkNotFound("retired chunk".into())),
            TestReply::Deferred => Err(IoError::MetadataConflict("writer is not sealed".into())),
            TestReply::Unsupported => Err(IoError::Unsupported("range retirement pending".into())),
            TestReply::Unknown => Err(IoError::WriteFailed("reply lost".into())),
        }
    }
}

#[tokio::test]
async fn one_notification_finishes_without_querying_layout_or_waiting_for_physical_free() {
    for reclaimed in [false, true] {
        for range in [(134, 234), (0, 4096)] {
            let allocator = TestAllocator::new(TestReply::Accepted(reclaimed), range);
            assert_eq!(
                reclaim_location(&allocator, &allocator.location()).await.unwrap(),
                ReclaimOutcome::Accepted
            );
            assert_eq!(allocator.calls.load(Ordering::Relaxed), 1);
        }
    }
}

#[tokio::test]
async fn unavailable_qualification_is_deferred_without_confirmation_reads() {
    for reply in [TestReply::Deferred, TestReply::Unsupported] {
        let allocator = TestAllocator::new(reply, (134, 234));
        assert_eq!(
            reclaim_location(&allocator, &allocator.location()).await.unwrap(),
            ReclaimOutcome::Deferred
        );
        assert_eq!(allocator.calls.load(Ordering::Relaxed), 1);
    }
}

#[tokio::test]
async fn missing_chunk_is_already_accepted_but_unknown_notification_remains_an_error() {
    let allocator = TestAllocator::new(TestReply::Missing, (134, 234));
    assert_eq!(
        reclaim_location(&allocator, &allocator.location()).await.unwrap(),
        ReclaimOutcome::Accepted
    );
    let allocator = TestAllocator::new(TestReply::Unknown, (134, 234));
    assert!(matches!(
        reclaim_location(&allocator, &allocator.location()).await,
        Err(IoError::WriteFailed(_))
    ));
    assert_eq!(allocator.calls.load(Ordering::Relaxed), 1);
}

#[tokio::test]
async fn byte_ranges_are_never_rounded_into_neighbouring_objects() {
    for range in [
        (134, 234),
        (1025, 2048),
        (1024, 2047),
        (256 * 1024 * 1024 - 1, 1),
        (1024 * 1024 * 1024 - 1, 1),
    ] {
        let allocator = TestAllocator::new(TestReply::Accepted(false), range);
        assert_eq!(
            reclaim_location(&allocator, &allocator.location()).await.unwrap(),
            ReclaimOutcome::Accepted
        );
        assert_eq!(allocator.calls.load(Ordering::Relaxed), 1);
    }
}

#[tokio::test]
async fn invalid_identities_and_unrepresentable_ranges_never_send_a_notification() {
    let allocator = TestAllocator::new(TestReply::Accepted(false), (0, 4096));
    for (offset, length) in [
        (0, 0),
        (u64::MAX, 1),
        (u64::from(u32::MAX) + 1, 1),
        (0, u64::from(u32::MAX) + 1),
        (u64::from(u32::MAX), 1),
    ] {
        let mut location = allocator.location();
        location.offset = offset;
        location.length = length;
        assert!(reclaim_location(&allocator, &location).await.is_err());
    }
    for identity in [None, Some(ChunkId::default())] {
        let mut location = allocator.location();
        location.chunk_id = identity;
        assert!(reclaim_location(&allocator, &location).await.is_err());
    }
    assert_eq!(allocator.calls.load(Ordering::Relaxed), 0);
}
