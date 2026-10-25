// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::{sync::Arc, time::Duration};

use crowdb_access_iceberg::{
    catalog::{CatalogContext, CatalogStore, RoutedCatalogStore},
    file::canonical_file_key,
    gc::{CandidatePhase, GcPhase, GcRepository, GcStalledReason, GcTask, GcTaskKind},
    key::{CatalogScope, IcebergKey, OperationId, TableId},
    record::StorageRecord,
    table::{storage_head_key, TableHead, TablePurgeTask},
};
use crowdb_chunkdb::{
    routing::{BindingCache, BindingTable},
    storage::{range_retirement::RetiredByteRange, ChunkStore},
};
use crowdb_kv_client::{ChunkSlotMapClient, CrowdbKvClient};
use crowdb_protocol::chunkdb::rpc::{Chunk, ChunkState, ChunkType};

use super::storage::TestPayloads;
use crate::access_iceberg::native::TestNativeStorage;

pub(super) struct TestRangeProof {
    chunks: ChunkStore,
    before: Chunk,
    expected: Vec<RetiredByteRange>,
}

impl TestRangeProof {
    pub(super) async fn new(stack: &TestNativeStorage, data: &TestPayloads) -> Self {
        let kv = Arc::new(CrowdbKvClient::new(crowdb_kv_client::ClientConfig::new(
            stack.seeds.clone(),
        )));
        let maps = ChunkSlotMapClient::new(kv.clone());
        let bindings = BindingCache::new();
        bindings
            .replace(BindingTable::new(maps.read_storage().await.unwrap()))
            .unwrap();
        let chunks = ChunkStore::new(kv, bindings);
        let before = chunks.get_chunk(&data.chunk).await.unwrap();
        assert_eq!(before.id, Some(data.chunk));
        assert_eq!(before.chunk_type, ChunkType::IcebergTable as i32);
        assert_eq!(before.state, ChunkState::Sealed as i32);
        assert_ne!(before.writer_epoch, 0);
        assert!(chunks
            .get_chunk_range_retirement(&data.chunk)
            .await
            .unwrap()
            .is_none());
        assert!(data
            .dead_ranges
            .iter()
            .all(|(a, b)| a < b && *b <= before.acknowledged_cursor));
        assert!(data.live_range.1 <= before.acknowledged_cursor);
        let expected = union(&data.dead_ranges);
        assert!(expected
            .iter()
            .all(|range| range.end <= data.live_range.0 || data.live_range.1 <= range.start));
        Self {
            chunks,
            before,
            expected,
        }
    }

    pub(super) async fn verify(&self, data: &TestPayloads) -> serde_json::Value {
        let chunk = self.chunks.get_chunk(&data.chunk).await.unwrap();
        assert_eq!(
            chunk, self.before,
            "logical retirement must not change recorded layout"
        );
        let record = self
            .chunks
            .get_chunk_range_retirement(&data.chunk)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(record.ranges(), self.expected);
        assert!(record.revision <= chunk.modify_ts);
        let value = serde_json::to_value(&record).unwrap();
        // validate(current_chunk) is private; observe the existing public Serialize
        // fields without extending production visibility or interpreting strip offsets.
        assert_eq!(value["version"], serde_json::json!(2));
        assert_eq!(value["chunk_id"], serde_json::json!(chunk.id));
        assert_eq!(value["writer_epoch"], serde_json::json!(chunk.writer_epoch));
        assert_eq!(value["create_ts_ms"], serde_json::json!(chunk.create_ts_ms));
        assert_eq!(value["sealed_ts_ms"], serde_json::json!(chunk.sealed_ts_ms));
        assert_eq!(
            value["sealed_bytes"],
            serde_json::json!(chunk.acknowledged_cursor)
        );
        value
    }
}

fn union(input: &[(u64, u64)]) -> Vec<RetiredByteRange> {
    let mut input = input.to_vec();
    input.sort_unstable();
    let mut ranges: Vec<RetiredByteRange> = Vec::new();
    for (start, end) in input {
        if let Some(last) = ranges.last_mut().filter(|last| last.end >= start) {
            last.end = last.end.max(end);
        } else {
            ranges.push(RetiredByteRange { start, end });
        }
    }
    ranges
}

pub(super) fn identity(table: TableId) -> OperationId {
    OperationId::from_bytes(table.as_bytes()).unwrap()
}

pub(super) async fn wait_for_task(
    store: Arc<RoutedCatalogStore>,
    context: CatalogContext,
    table: TableId,
) -> GcTask {
    let repository = GcRepository::new(store);
    loop {
        if let Some(task) = repository.task(context.catalog, identity(table)).await.unwrap() {
            return task;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

pub(super) async fn wait_complete(
    store: Arc<RoutedCatalogStore>,
    context: CatalogContext,
    table: TableId,
    dropped_ms: u64,
) -> GcTask {
    let repository = GcRepository::new(store);
    let mut revision = 0;
    loop {
        let task = repository
            .task(context.catalog, identity(table))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(task.kind, GcTaskKind::PurgeTable);
        assert_eq!(task.context, context);
        assert_eq!(task.created_ms, dropped_ms);
        assert_eq!(task.not_before_ms, dropped_ms + GcTask::PURGE_DELAY_MS);
        assert!(!task.paused);
        assert_ne!(task.phase, GcPhase::Quarantined, "{task:?}");
        if task.revision != revision {
            println!(
                "gc_notification_progress {}",
                serde_json::json!({
                    "revision":task.revision,"phase":format!("{:?}",task.phase),
                    "deleted":task.deleted,"retry_at_ms":task.retry_at_ms,
                    "stalled":format!("{:?}",task.stalled),
                })
            );
            revision = task.revision;
        }
        if task.phase == GcPhase::Complete {
            return task;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
}

pub(super) async fn verify_resources(
    store: Arc<RoutedCatalogStore>,
    context: CatalogContext,
    task: &GcTask,
    data: &TestPayloads,
    stale: &TablePurgeTask,
    live_head: &TableHead,
) {
    assert_eq!(task.deleted, 2);
    assert_eq!(
        task.reclaimed_bytes,
        data.dead.iter().map(|file| file.length).sum::<u64>()
    );
    assert_eq!(task.stalled, GcStalledReason::None);
    assert_eq!(task.retry_at_ms, 0);
    assert!(!task.deferred_ranges && !task.fenced);
    let head = task.head.as_ref().unwrap();
    for file in &data.dead {
        assert!(store
            .get(&canonical_file_key(&file.location).encode().unwrap())
            .await
            .unwrap()
            .is_none());
        let mut suffix = head.table.as_bytes().to_vec();
        suffix.extend_from_slice(&head.generation.to_be_bytes());
        suffix.extend_from_slice(file.file.as_bytes());
        let key = IcebergKey::Catalog {
            catalog: context.catalog,
            scope: CatalogScope::GcCandidate,
            suffix,
        };
        let value = store.get(&key.encode().unwrap()).await.unwrap().unwrap();
        let StorageRecord::GcCandidate(candidate) = StorageRecord::decode(&key, &value.bytes).unwrap() else {
            panic!("candidate expected")
        };
        assert_eq!(candidate.file, *file);
        assert_eq!(candidate.task, task.identity);
        assert_eq!(candidate.generation, head.generation);
        assert_eq!(candidate.phase, CandidatePhase::Complete);
        assert!(candidate.completed_round > 0 && candidate.completed_round <= task.sweep_round);
        assert!(candidate.cursor.frames.is_empty() && candidate.cursor.pending.is_none());
    }
    let head_key = storage_head_key(data.live.location.table());
    let value = store.get(&head_key.encode().unwrap()).await.unwrap().unwrap();
    assert_eq!(
        StorageRecord::decode(&head_key, &value.bytes).unwrap(),
        StorageRecord::TableHead(Box::new(live_head.clone()))
    );
    assert!(store.get(&stale.key().encode().unwrap()).await.unwrap().is_none());
    assert!(GcRepository::new(store)
        .task(context.catalog, identity(stale.head.table))
        .await
        .unwrap()
        .is_none());
}
