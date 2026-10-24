// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Shared allocation geometry for large-write integration tests.

#![allow(clippy::cast_possible_truncation)]

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use crowdb_chunk_client::{ChunkAllocator, Result};
use crowdb_protocol::chunkdb::rpc::Strip as StripOneof;
use crowdb_protocol::chunkdb::rpc::{
    AllocateChunkRequest, AllocateChunkResponse, AppendChunkRequest, AppendChunkResponse, Chunk, ChunkStrip,
    ChunkType, DeleteChunkRequest, DeleteChunkResponse, EcStrip, MirrorStrip, QueryChunkRequest,
    QueryChunkResponse, SealChunkRequest, SealChunkResponse, StripType,
};
use crowdb_protocol::common::DiskId as ProtoDiskId;
use crowdb_protocol::diskdb::rpc::Segment;

#[derive(Debug, Clone, Default)]
pub(crate) struct TestChunkAllocator {
    state: Arc<Mutex<TestChunkState>>,
}

#[derive(Debug, Default)]
pub(crate) struct TestChunkState {
    pub(crate) chunks: HashMap<(u64, u64), (Vec<ChunkStrip>, u64, bool)>,
    pub(crate) next_segment_offset: u64,
    pub(crate) allocate_calls: usize,
    pub(crate) allocated_strip_counts: Vec<u32>,
    pub(crate) append_calls: usize,
    pub(crate) seal_calls: usize,
    pub(crate) delete_calls: usize,
}

impl TestChunkAllocator {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn snapshot(&self) -> std::sync::MutexGuard<'_, TestChunkState> {
        self.state.lock().unwrap()
    }
}

#[async_trait]
impl ChunkAllocator for TestChunkAllocator {
    async fn allocate_chunk(&self, req: AllocateChunkRequest) -> Result<AllocateChunkResponse> {
        let mut st = self.state.lock().unwrap();
        st.allocate_calls += 1;
        st.allocated_strip_counts.push(req.strip_count);
        let chunk_id = req.chunk_id.unwrap_or_default();
        let is_mirror = req.strip_type == StripType::Mirror as i32;
        let data_num = req.data_num as usize;
        let code_num = req.code_num as usize;
        let total = if is_mirror {
            req.copy_count as usize
        } else {
            data_num + code_num
        };

        let mut segments = Vec::with_capacity(total);
        for i in 0..total {
            segments.push(Segment {
                disk_id: Some(ProtoDiskId {
                    high: 1000 + i as u64,
                    low: i as u64,
                }),
                zone_index: 0,
                unit_offset: st.next_segment_offset,
                unit_count: if is_mirror {
                    (req.write_granularity / 4).max(1)
                } else {
                    1
                },
                owner_chunk: Some(chunk_id),
                allocation_ts: st.next_segment_offset + 1,
            });
            st.next_segment_offset += 1;
        }

        let (capacity, strip) = if is_mirror {
            let capacity = req.write_granularity;
            (capacity, StripOneof::MirrorStrip(MirrorStrip { segments }))
        } else {
            let capacity = req.data_num.saturating_mul(4);
            (
                capacity,
                StripOneof::EcStrip(EcStrip {
                    data_num: req.data_num,
                    code_num: req.code_num,
                    ec_state: 0,
                    segments,
                }),
            )
        };
        let strip = ChunkStrip {
            chunk_offset: 0,
            strip_sequence: 0,
            unit_kb: 4,
            capacity,
            create_ts_ms: 0,
            sealed_ts_ms: 0,
            sealed_length: 0,
            strip_type: req.strip_type,
            strip: Some(strip),
            usage_bitmap: Vec::new(),
            unavailable_segments: Vec::new(),
            placement_priority: 0,
            placement_assessment: None,
            placement_repair_required: false,
        };

        let chunk = Chunk {
            id: Some(chunk_id),
            modify_ts: 1,
            state: 1,
            create_ts_ms: 0,
            sealed_ts_ms: 0,
            capacity,
            sealed_length: 0,
            strips: vec![strip.clone()],
            chunk_type: ChunkType::S3 as i32,
            writer_epoch: req.writer_epoch,
            acknowledged_cursor: 0,
            closed_strip_sequence: None,
            writer_lease_deadline_ms: 0,
            next_strip_sequence: 1,
            cleanup_intents: vec![],
            last_strip_replacement: None,
            owner_key: req.owner_key,
        };

        st.chunks
            .insert((chunk_id.high, chunk_id.low), (vec![strip], 0, false));
        Ok(AllocateChunkResponse { chunk: Some(chunk) })
    }

    async fn append_chunk(&self, req: AppendChunkRequest) -> Result<AppendChunkResponse> {
        let mut st = self.state.lock().unwrap();
        st.append_calls += 1;
        let chunk_id = req.chunk_id.unwrap_or_default();
        let is_mirror = req.strip_type == StripType::Mirror as i32;
        let data_num = req.data_num as usize;
        let code_num = req.code_num as usize;
        let total = if is_mirror {
            req.copy_count as usize
        } else {
            data_num + code_num
        };

        let strip_seq = st
            .chunks
            .get(&(chunk_id.high, chunk_id.low))
            .map_or(0, |e| e.0.len() as u32);

        let mut segments = Vec::with_capacity(total);
        for i in 0..total {
            segments.push(Segment {
                disk_id: Some(ProtoDiskId {
                    high: 1000 + i as u64,
                    low: i as u64,
                }),
                zone_index: 0,
                unit_offset: st.next_segment_offset,
                unit_count: if is_mirror { req.strip_size.max(1) } else { 1 },
                owner_chunk: Some(chunk_id),
                allocation_ts: st.next_segment_offset + 1,
            });
            st.next_segment_offset += 1;
        }

        let (capacity, strip) = if is_mirror {
            let capacity = req.strip_size.saturating_mul(4);
            (capacity, StripOneof::MirrorStrip(MirrorStrip { segments }))
        } else {
            let capacity = req.data_num.saturating_mul(4);
            (
                capacity,
                StripOneof::EcStrip(EcStrip {
                    data_num: req.data_num,
                    code_num: req.code_num,
                    ec_state: 0,
                    segments,
                }),
            )
        };
        let chunk_offset = st
            .chunks
            .get(&(chunk_id.high, chunk_id.low))
            .map_or(0, |entry| entry.0.iter().map(|strip| strip.capacity).sum());
        let strip = ChunkStrip {
            chunk_offset,
            strip_sequence: strip_seq,
            unit_kb: 4,
            capacity,
            create_ts_ms: 0,
            sealed_ts_ms: 0,
            sealed_length: 0,
            strip_type: req.strip_type,
            strip: Some(strip),
            usage_bitmap: Vec::new(),
            unavailable_segments: Vec::new(),
            placement_priority: 0,
            placement_assessment: None,
            placement_repair_required: false,
        };

        let entry = st
            .chunks
            .get_mut(&(chunk_id.high, chunk_id.low))
            .expect("append to unknown chunk");
        entry.0.push(strip.clone());
        Ok(AppendChunkResponse {
            modify_ts: u64::from(strip_seq) + 1,
            strips: vec![strip],
            chunk: None,
        })
    }

    async fn seal_chunk(&self, req: SealChunkRequest) -> Result<SealChunkResponse> {
        let mut st = self.state.lock().unwrap();
        st.seal_calls += 1;
        let chunk_id = req.chunk_id.unwrap_or_default();
        let entry = st
            .chunks
            .get_mut(&(chunk_id.high, chunk_id.low))
            .expect("seal unknown chunk");
        entry.1 = req.seal_bytes;
        Ok(SealChunkResponse { chunk: None })
    }

    async fn delete_chunk(&self, req: DeleteChunkRequest) -> Result<DeleteChunkResponse> {
        let mut st = self.state.lock().unwrap();
        st.delete_calls += 1;
        let chunk_id = req.chunk_id.unwrap_or_default();
        let entry = st
            .chunks
            .get_mut(&(chunk_id.high, chunk_id.low))
            .expect("delete unknown chunk");
        entry.2 = true;
        Ok(DeleteChunkResponse { chunk: None })
    }

    async fn query_chunk(&self, _req: QueryChunkRequest) -> Result<QueryChunkResponse> {
        Ok(QueryChunkResponse {
            chunk: None,
            layout_validity_ms: 0,
        })
    }
}

