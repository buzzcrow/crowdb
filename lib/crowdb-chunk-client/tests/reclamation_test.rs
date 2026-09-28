use std::sync::atomic::{AtomicUsize, Ordering};

use async_trait::async_trait;
use crowdb_chunk_client::{reclaim_location, ChunkAllocator, IoError, ReclaimOutcome, Result};
use crowdb_protocol::chunkdb::rpc::{
    AllocateChunkRequest, AllocateChunkResponse, AppendChunkRequest, AppendChunkResponse, Chunk, ChunkState,
    DeleteChunkRangeRequest, DeleteChunkRangeResponse, DeleteChunkRequest, DeleteChunkResponse, Location,
    QueryChunkRequest, QueryChunkResponse, SealChunkRequest, SealChunkResponse, UpdateChunkStripRequest,
    UpdateChunkStripResponse,
};
use crowdb_protocol::common::ChunkId;

struct TestAllocator {
    chunk: Chunk,
    range_calls: AtomicUsize,
    chunk_calls: AtomicUsize,
    range_supported: bool,
    delete_failed: bool,
    expected_range: (u32, u32),
}

impl TestAllocator {
    fn new(shared: bool) -> Self {
        Self {
            chunk: Chunk {
                id: Some(ChunkId { high: 1, low: 2 }),
                state: ChunkState::Sealed as i32,
                sealed_length: 4,
                writer_epoch: u64::from(shared),
                ..Chunk::default()
            },
            range_calls: AtomicUsize::new(0),
            chunk_calls: AtomicUsize::new(0),
            range_supported: false,
            delete_failed: false,
            expected_range: (1024, 2048),
        }
    }

    fn location(&self) -> Location {
        Location {
            chunk_id: self.chunk.id,
            offset: 0,
            length: 4096,
            logical_offset: 0,
            logical_length: 4000,
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

    async fn update_chunk_strip(&self, _: UpdateChunkStripRequest) -> Result<UpdateChunkStripResponse> {
        unreachable!()
    }

    async fn query_chunk(&self, request: QueryChunkRequest) -> Result<QueryChunkResponse> {
        assert_eq!(request.chunk_id, self.chunk.id);
        Ok(QueryChunkResponse {
            chunk: Some(self.chunk.clone()),
            ..QueryChunkResponse::default()
        })
    }

    async fn delete_chunk(&self, request: DeleteChunkRequest) -> Result<DeleteChunkResponse> {
        assert_eq!(request.chunk_id, self.chunk.id);
        self.chunk_calls.fetch_add(1, Ordering::Relaxed);
        if self.delete_failed {
            return Err(IoError::WriteFailed("partial disk block release".into()));
        }
        Ok(DeleteChunkResponse {
            chunk: Some(Chunk {
                state: ChunkState::Deleted as i32,
                ..self.chunk.clone()
            }),
        })
    }

    async fn delete_chunk_range(&self, request: DeleteChunkRangeRequest) -> Result<DeleteChunkRangeResponse> {
        assert_eq!(request.chunk_id, self.chunk.id);
        assert_eq!((request.chunk_offset, request.chunk_size), self.expected_range);
        self.range_calls.fetch_add(1, Ordering::Relaxed);
        if !self.range_supported {
            return Err(IoError::Unsupported("shared ranges pending".into()));
        }
        Ok(DeleteChunkRangeResponse::default())
    }
}

#[tokio::test]
async fn dedicated_chunk_deletion_checks_ownership_and_retries_failures() {
    let mut allocator = TestAllocator::new(false);
    let mut location = allocator.location();
    location.offset = 1024;
    assert!(reclaim_location(&allocator, &location).await.is_err());
    assert_eq!(allocator.chunk_calls.load(Ordering::Relaxed), 0);
    location.offset = 0;
    allocator.delete_failed = true;
    assert!(reclaim_location(&allocator, &location).await.is_err());
    allocator.delete_failed = false;
    assert_eq!(
        reclaim_location(&allocator, &location).await.unwrap(),
        ReclaimOutcome::Reclaimed
    );
    assert_eq!(allocator.chunk_calls.load(Ordering::Relaxed), 2);
    assert_eq!(allocator.range_calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn shared_range_remains_deferred_until_storage_supports_it() {
    let mut allocator = TestAllocator::new(true);
    allocator.expected_range = (134, 234);
    let mut location = allocator.location();
    location.offset = 134;
    location.length = 234;
    assert_eq!(
        reclaim_location(&allocator, &location).await.unwrap(),
        ReclaimOutcome::Deferred
    );
    allocator.range_supported = true;
    assert_eq!(
        reclaim_location(&allocator, &location).await.unwrap(),
        ReclaimOutcome::Reclaimed
    );
    assert_eq!(allocator.range_calls.load(Ordering::Relaxed), 2);
    assert_eq!(allocator.chunk_calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn uncertain_shared_write_waits_for_readable_cursor_or_terminal_chunk() {
    let mut allocator = TestAllocator::new(true);
    allocator.range_supported = true;
    allocator.expected_range = (0, 4096);
    allocator.chunk.state = ChunkState::Active as i32;
    allocator.chunk.acknowledged_cursor = 4095;
    let location = allocator.location();
    assert_eq!(
        reclaim_location(&allocator, &location).await.unwrap(),
        ReclaimOutcome::Deferred
    );
    assert_eq!(allocator.range_calls.load(Ordering::Relaxed), 0);
    allocator.chunk.acknowledged_cursor = 4096;
    assert_eq!(
        reclaim_location(&allocator, &location).await.unwrap(),
        ReclaimOutcome::Reclaimed
    );
    allocator.chunk.acknowledged_cursor = 0;
    allocator.chunk.state = ChunkState::Sealed as i32;
    assert_eq!(
        reclaim_location(&allocator, &location).await.unwrap(),
        ReclaimOutcome::Reclaimed
    );
    assert_eq!(allocator.range_calls.load(Ordering::Relaxed), 2);
}

#[tokio::test]
async fn active_dedicated_chunk_and_invalid_ranges_are_never_deleted() {
    let mut allocator = TestAllocator::new(false);
    allocator.chunk.state = ChunkState::Active as i32;
    assert!(reclaim_location(&allocator, &allocator.location()).await.is_err());
    let mut location = allocator.location();
    location.length = 0;
    assert!(reclaim_location(&allocator, &location).await.is_err());
    location.offset = u64::MAX;
    location.length = 1;
    assert!(reclaim_location(&allocator, &location).await.is_err());
    assert_eq!(allocator.chunk_calls.load(Ordering::Relaxed), 0);
    assert_eq!(allocator.range_calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn shared_byte_ranges_are_never_rounded_into_neighbouring_objects() {
    let mut allocator = TestAllocator::new(true);
    allocator.range_supported = true;
    for (offset, length) in [
        (134, 234),
        (1025, 2048),
        (1024, 2047),
        (256 * 1024 * 1024 - 1, 1),
        (1024 * 1024 * 1024 - 1, 1),
    ] {
        allocator.expected_range = (offset, length);
        let mut location = allocator.location();
        location.offset = u64::from(offset);
        location.length = u64::from(length);
        assert_eq!(
            reclaim_location(&allocator, &location).await.unwrap(),
            ReclaimOutcome::Reclaimed
        );
    }
    assert_eq!(allocator.range_calls.load(Ordering::Relaxed), 5);
    assert_eq!(allocator.chunk_calls.load(Ordering::Relaxed), 0);
}

#[tokio::test]
async fn shared_ranges_reject_unrepresentable_fields_and_range_end() {
    let allocator = TestAllocator::new(true);
    for (offset, length) in [
        (0, 0),
        (u64::from(u32::MAX) + 1, 1),
        (0, u64::from(u32::MAX) + 1),
        (u64::from(u32::MAX), 1),
    ] {
        let mut location = allocator.location();
        location.offset = offset;
        location.length = length;
        assert!(reclaim_location(&allocator, &location).await.is_err());
    }
    assert_eq!(allocator.range_calls.load(Ordering::Relaxed), 0);
    assert_eq!(allocator.chunk_calls.load(Ordering::Relaxed), 0);
}
