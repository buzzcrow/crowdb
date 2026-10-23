// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use bytes::Bytes;
use crowdb_chunkdb::{
    conversion::io::ConversionDiskIo,
    finalize::FinalizeChunkTaskHandler,
    routing::{default_binding_table, BindingCache},
    task::{TaskHandler, TaskOutcome, TaskStore},
};
use crowdb_protocol::{
    chunk_stream::StreamName,
    chunk_task::{ChunkTaskValue, TASK_KIND_FINALIZE_CHUNK},
    chunkdb::rpc::{Chunk, ChunkType, Strip, StripType},
    frame::encode_append_group_views,
};

use super::{
    cluster::{seed_hardware, ChunkdbHarness, DiskdbServer, KvCluster, DATA_GROUP_ID, STORE_ID},
    diskio_stub::TestDiskio,
};

pub struct TestFinalization {
    pub harness: ChunkdbHarness,
    pub io: Arc<ConversionDiskIo>,
    pub chunk: Chunk,
    pub task: ChunkTaskValue,
    _diskio: Vec<TestDiskio>,
    _diskdb: DiskdbServer,
    _cluster: KvCluster,
}

impl TestFinalization {
    pub async fn start(chunk_type: ChunkType) -> Self {
        let cluster = KvCluster::start_pinned().await;
        let hardware = cluster.make_hardware_client();
        seed_hardware(&hardware).await;
        let diskdb = DiskdbServer::start(&cluster).await;
        let service = cluster.make_service_registry_client();
        let mut diskio = Vec::new();
        for index in 0..4 {
            diskio
                .push(TestDiskio::start(&service, 9000 + index, 100 + index, 10 + index, 1000 + index).await);
        }
        let harness = ChunkdbHarness::start(&cluster).await;
        let io = Arc::new(ConversionDiskIo::connect(&service, &hardware).await.unwrap());
        let chunk = harness
            .handler
            .allocate_chunk_owned(
                None,
                1024,
                1,
                StripType::Mirror,
                0,
                0,
                3,
                chunk_type,
                19,
                60_000,
                StreamName::generate().chunk_owner_key(),
            )
            .await
            .unwrap();
        let id = chunk.id.unwrap();
        let bindings = BindingCache::new();
        bindings
            .replace(default_binding_table(STORE_ID, DATA_GROUP_ID))
            .unwrap();
        let tasks = TaskStore::new(cluster.make_crowdb_client(), bindings);
        let mut task = tasks
            .get(&id, TASK_KIND_FINALIZE_CHUNK, &id)
            .await
            .unwrap()
            .unwrap();
        task.eligible_at_ms = 0;
        Self {
            harness,
            io,
            chunk,
            task,
            _diskio: diskio,
            _diskdb: diskdb,
            _cluster: cluster,
        }
    }

    pub fn group(&self, offset: u64, payload: &[u8]) -> Vec<u8> {
        encode_append_group_views(
            self.chunk.id.unwrap(),
            offset,
            &Bytes::copy_from_slice(payload),
            1,
        )
        .unwrap()
        .concat()
    }

    pub async fn write(&self, replica: usize, bytes: Vec<u8>) {
        let strip = &self.chunk.strips[0];
        let Some(Strip::MirrorStrip(mirror)) = &strip.strip else {
            panic!("expected mirrors")
        };
        self.io
            .write_segment(
                &mirror.segments[replica],
                u64::from(strip.unit_kb) * 1024,
                Bytes::from(bytes),
            )
            .await
            .unwrap();
    }

    pub async fn checkpoint(&mut self, cursor: u64) {
        self.chunk = self
            .harness
            .handler
            .advance_chunk_write(
                &self.chunk.id.unwrap(),
                19,
                self.chunk.modify_ts,
                cursor,
                None,
                60_000,
            )
            .await
            .unwrap();
    }

    pub async fn run(&self) -> TaskOutcome {
        FinalizeChunkTaskHandler::new(Arc::clone(&self.harness.handler), Arc::clone(&self.io))
            .execute(&self.task)
            .await
    }

    pub async fn current(&self) -> Chunk {
        self.harness
            .handler
            .query_chunk(&self.chunk.id.unwrap())
            .await
            .unwrap()
    }
}
