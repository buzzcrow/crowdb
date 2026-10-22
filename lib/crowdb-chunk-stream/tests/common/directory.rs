// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::collections::{HashMap, HashSet};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    RwLock,
};

use async_trait::async_trait;
use bytes::Bytes;
use crowdb_chunk_stream::{DirectoryIndex, DirectoryMutation, Result, StreamDirectoryStore};
use crowdb_protocol::chunk_stream::{StreamDirectoryPageId, StreamExtentLocation, StreamName, StreamPurpose};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::frame::ChunkLocation;

pub struct TestDirectoryStore {
    pub name: StreamName,
    pub reads: AtomicUsize,
    records: RwLock<HashMap<StreamDirectoryPageId, Bytes>>,
}

impl TestDirectoryStore {
    pub fn new() -> Self {
        Self {
            name: StreamName { high: 19, low: 37 },
            reads: AtomicUsize::new(0),
            records: RwLock::default(),
        }
    }

    pub fn empty(&self) -> DirectoryIndex {
        DirectoryIndex::new(self.name, StreamPurpose::Stream, None).unwrap()
    }

    pub fn install(&self, mutation: &DirectoryMutation) -> DirectoryIndex {
        let mut records = self.records.write().unwrap();
        for record in mutation.pages() {
            if let Some(previous) = records.insert(record.reference().id, record.bytes().clone()) {
                assert_eq!(&previous, record.bytes());
            }
        }
        mutation.index().clone()
    }

    pub fn reset_reads(&self) {
        self.reads.store(0, Ordering::Relaxed);
    }
    pub fn read_count(&self) -> usize {
        self.reads.load(Ordering::Relaxed)
    }
    pub fn ids(&self) -> HashSet<StreamDirectoryPageId> {
        self.records.read().unwrap().keys().copied().collect()
    }
    pub fn remove(&self, id: StreamDirectoryPageId) {
        self.records.write().unwrap().remove(&id);
    }
    pub fn corrupt(&self, id: StreamDirectoryPageId) {
        self.records
            .write()
            .unwrap()
            .insert(id, Bytes::from_static(b"bad"));
    }
}

#[async_trait]
impl StreamDirectoryStore for TestDirectoryStore {
    async fn load_directory_record(
        &self,
        name: StreamName,
        id: StreamDirectoryPageId,
    ) -> Result<Option<Bytes>> {
        assert_eq!(name, self.name);
        self.reads.fetch_add(1, Ordering::Relaxed);
        Ok(self.records.read().unwrap().get(&id).cloned())
    }
}

/// Different allocations make a very large logical history without exceeding a chunk.
pub fn test_locations(start: u64, count: usize) -> Vec<StreamExtentLocation> {
    (start..start + count as u64)
        .map(|logical_start| StreamExtentLocation {
            logical_start,
            location: ChunkLocation {
                chunk_id: ChunkId {
                    high: (crowdb_protocol::chunkdb::rpc::ChunkType::Stream as u64) << 56,
                    low: logical_start + 1,
                },
                frame_offset: 0,
                logical_length: 1,
            },
        })
        .collect()
}
