// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_chunk_kv::{
    Partition, PartitionConfig, PartitionId, PartitionJournal, PartitionRange, PartitionTree,
    StreamPartitionJournal,
};
use crowdb_chunk_stream::{
    memory::MemoryStreamStore, ChunkStream, StreamConfig, StreamName, StreamRecord, StreamRecordState,
};
use std::sync::Arc;

pub struct TestSplitStorage {
    pub store: Arc<MemoryStreamStore>,
}

impl TestSplitStorage {
    pub fn new() -> Self {
        Self {
            store: Arc::new(MemoryStreamStore::new(4096)),
        }
    }

    pub async fn journal(&self, stream_name: StreamName, epoch: u64) -> Arc<dyn PartitionJournal> {
        let stream = ChunkStream::create(
            StreamRecord {
                purpose: crowdb_chunk_stream::StreamPurpose::Wal,
                stream_name,
                metadata_group_id: 7,
                record_version: 1,
                state: StreamRecordState::Active,
                consumer_kind: Some("chunk-kv-partition".into()),
                consumer_id: None,
                created_at_ms: StreamRecord::creation_time_ms(),
            },
            epoch,
            StreamConfig::default(),
            self.store.clone(),
            self.store.clone(),
            self.store.clone(),
        )
        .await
        .unwrap();
        Arc::new(StreamPartitionJournal::new(stream, stream_name).unwrap())
    }

    pub async fn partition(
        &self,
        id: PartitionId,
        range: PartitionRange,
        epoch: u64,
        tree: Arc<dyn PartitionTree>,
    ) -> (Partition, Arc<dyn PartitionJournal>) {
        let journal = self
            .journal(
                StreamName {
                    high: id.high,
                    low: id.low,
                },
                epoch,
            )
            .await;
        let partition = Partition::open(
            id,
            range,
            epoch,
            PartitionConfig::default(),
            tree,
            journal.clone(),
        )
        .unwrap();
        (partition, journal)
    }
}
