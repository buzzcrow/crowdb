// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};

use async_trait::async_trait;
use crowdb_access_iceberg::file::{ChunkRoot, FileBlockStore, FileIdentity, FileIoError};
use crowdb_chunk_client::{IoError, ReclaimOutcome};
use crowdb_protocol::{chunkdb::rpc::Location, common::ChunkId};

pub struct TestRangeBlocks {
    pub calls: AtomicUsize,
    pub reads: AtomicUsize,
    pub result: AtomicU8,
    expected: Location,
}

impl Default for TestRangeBlocks {
    fn default() -> Self {
        Self {
            calls: AtomicUsize::new(0),
            reads: AtomicUsize::new(0),
            result: AtomicU8::new(0),
            expected: Self::location(),
        }
    }
}

impl TestRangeBlocks {
    pub fn location() -> Location {
        Location {
            chunk_id: Some(ChunkId { high: 1, low: 2 }),
            offset: 4096,
            length: 16,
            logical_offset: 0,
            logical_length: 7,
        }
    }

    pub fn root() -> ChunkRoot {
        ChunkRoot {
            chunk: ChunkId { high: 1, low: 3 },
            offset: 0,
            physical_length: 8,
            logical_offset: 0,
            logical_length: 8,
            height: 0,
            digest: [7; 32],
        }
    }
}

#[async_trait]
impl FileBlockStore for TestRangeBlocks {
    async fn put(&self, _owner: FileIdentity, _height: u8, _bytes: &[u8]) -> Result<ChunkRoot, FileIoError> {
        Err(FileIoError::Bounds)
    }

    async fn read(&self, root: &ChunkRoot) -> Result<Vec<u8>, FileIoError> {
        self.reads.fetch_add(1, Ordering::Relaxed);
        Ok(vec![7; usize::try_from(root.logical_length).unwrap()])
    }

    async fn reclaim_location(&self, location: &Location) -> Result<ReclaimOutcome, FileIoError> {
        assert_eq!(
            location, &self.expected,
            "range identity and both coordinate bindings must survive"
        );
        self.calls.fetch_add(1, Ordering::Relaxed);
        match self.result.load(Ordering::Relaxed) {
            0 => Ok(ReclaimOutcome::Accepted),
            1 => Ok(ReclaimOutcome::Deferred),
            _ => Err(IoError::Internal("lost range notification reply".into()).into()),
        }
    }
}
