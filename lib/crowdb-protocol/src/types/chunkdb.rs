// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::missing_errors_doc,
    clippy::doc_markdown,
    clippy::default_trait_access,
    clippy::too_many_lines
)]

//! Hand-written Rust types replacing the prost-generated `crow.chunkdb.rpc`
//! types. API-compatible with the former proto-generated structs.

use serde::{Deserialize, Serialize};

use crate::common::{ChunkId, DiskId};
use crate::diskdb::rpc::Segment;

/// Implement `From<Enum> for i32` and `TryFrom<i32> for Enum`.
macro_rules! impl_enum_conversions {
    ($enum:ident, $($variant:ident = $value:expr),+ $(,)?) => {
        impl From<$enum> for i32 {
            fn from(v: $enum) -> Self {
                v as i32
            }
        }

        impl std::convert::TryFrom<i32> for $enum {
            type Error = ();

            fn try_from(v: i32) -> Result<Self, Self::Error> {
                Ok(match v {
                    $($value => $enum::$variant,)+
                    _ => return Err(()),
                })
            }
        }
    };
}

// ── Enums ───────────────────────────────────────────────────────

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[repr(i32)]
pub enum EcState {
    #[default]
    NoParity = 0,
    Parity = 1,
}
impl_enum_conversions!(EcState, NoParity = 0, Parity = 1);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[repr(i32)]
pub enum ChunkState {
    #[default]
    Init = 0,
    Active = 1,
    Sealed = 2,
    Deleted = 3,
}
impl_enum_conversions!(ChunkState, Init = 0, Active = 1, Sealed = 2, Deleted = 3);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[repr(i32)]
pub enum StripType {
    #[default]
    Mirror = 0,
    Ec = 1,
}
impl_enum_conversions!(StripType, Mirror = 0, Ec = 1);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize, Default)]
#[repr(i32)]
pub enum ChunkType {
    #[default]
    Repo = 0,
    Wal = 1,
    BtreePage = 2,
    PageIndex = 3,
}
impl_enum_conversions!(ChunkType, Repo = 0, Wal = 1, BtreePage = 2, PageIndex = 3);

// ── Strip types ─────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct MirrorStrip {
    pub segments: Vec<Segment>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct EcStrip {
    pub data_num: u32,
    pub code_num: u32,
    pub ec_state: i32,
    pub segments: Vec<Segment>,
}

/// Oneof `strip` field in `ChunkStrip`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Strip {
    MirrorStrip(MirrorStrip),
    EcStrip(EcStrip),
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct ChunkStrip {
    pub chunk_offset: u32,
    pub strip_sequence: u32,
    pub unit_kb: u32,
    pub capacity: u32,
    pub create_ts_ms: u64,
    pub sealed_ts_ms: u64,
    pub sealed_length: u32,
    pub strip_type: i32,
    pub strip: Option<Strip>,
    pub usage_bitmap: Vec<u8>,
    /// Replica identities known unavailable until background recovery.
    pub unavailable_segments: Vec<Segment>,
}

// ── Chunk ───────────────────────────────────────────────────────

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct Chunk {
    pub id: Option<ChunkId>,
    /// Monotonic per-chunk modification revision.
    pub modify_ts: u64,
    pub state: i32,
    pub create_ts_ms: u64,
    pub sealed_ts_ms: u64,
    pub capacity: u32,
    pub sealed_length: u32,
    pub strips: Vec<ChunkStrip>,
    pub chunk_type: i32,
    /// Nonzero epoch that exclusively owns shared-chunk advances.
    pub writer_epoch: u64,
    /// Durable physical byte cursor acknowledged to readers.
    pub acknowledged_cursor: u64,
    /// Highest mirror strip durably closed by the writer.
    pub closed_strip_sequence: Option<u32>,
    /// Server-clock deadline after which an Active shared chunk is orphaned.
    pub writer_lease_deadline_ms: u64,
    /// Next identity assigned by append; never decreases after range splices.
    pub next_strip_sequence: u32,
    /// Retired segment sets awaiting the reader layout-validity grace.
    pub cleanup_intents: Vec<StripCleanupIntent>,
    /// Most recently committed fenced replacement operation.
    pub last_strip_replacement: Option<ChunkId>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct StripCleanupIntent {
    pub operation_id: Option<ChunkId>,
    pub retired_segments: Vec<Segment>,
    pub not_before_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct NotMyRangeHint {
    pub range_start: u32,
    pub range_end: u32,
    pub instance_id: u64,
    pub rpc_endpoint: String,
    pub sub_range_index: u32,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct Location {
    pub chunk_id: Option<ChunkId>,
    pub offset: u64,
    pub length: u64,
    pub logical_offset: u64,
    pub logical_length: u64,
}

// ── RPC request/response types ──────────────────────────────────

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct AllocateChunkRequest {
    pub chunk_id: Option<ChunkId>,
    pub write_granularity: u32,
    pub strip_count: u32,
    pub strip_type: i32,
    pub data_num: u32,
    pub code_num: u32,
    pub copy_count: u32,
    pub chunk_type: i32,
    /// Optional shared-writer epoch. Zero keeps dedicated-chunk semantics.
    pub writer_epoch: u64,
    /// Lease duration installed for a nonzero writer epoch.
    pub writer_lease_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct AllocateChunkResponse {
    pub chunk: Option<Chunk>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct AppendChunkRequest {
    pub chunk_id: Option<ChunkId>,
    /// Chunk revision observed by the caller.
    pub modify_ts: u64,
    pub strip_size: u32,
    pub strip_count: u32,
    pub strip_type: i32,
    pub data_num: u32,
    pub code_num: u32,
    pub copy_count: u32,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct AppendChunkResponse {
    /// Revision after a successful append, or the current revision on mismatch.
    pub modify_ts: u64,
    /// Newly appended strips when the request revision matched.
    pub strips: Vec<ChunkStrip>,
    /// Complete current chunk when the request revision was stale.
    pub chunk: Option<Chunk>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct AdvanceChunkWriteRequest {
    pub chunk_id: Option<ChunkId>,
    pub writer_epoch: u64,
    pub expected_modify_ts: u64,
    pub acknowledged_cursor: u64,
    pub closed_strip_sequence: Option<u32>,
    pub writer_lease_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct AdvanceChunkWriteResponse {
    pub chunk: Option<Chunk>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct QueryChunkRequest {
    pub chunk_id: Option<ChunkId>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct QueryChunkResponse {
    pub chunk: Option<Chunk>,
    /// Maximum time a caller may continue using the returned layout.
    pub layout_validity_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct SealChunkRequest {
    pub chunk_id: Option<ChunkId>,
    pub seal_length: u32,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct SealChunkResponse {
    pub chunk: Option<Chunk>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct DeleteChunkRequest {
    pub chunk_id: Option<ChunkId>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct DeleteChunkResponse {
    pub chunk: Option<Chunk>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct DeleteChunkRangeRequest {
    pub chunk_id: Option<ChunkId>,
    pub chunk_offset: u32,
    pub chunk_size: u32,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct DeleteChunkRangeResponse {}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct UpdateChunkStripRequest {
    pub chunk_id: Option<ChunkId>,
    pub strip_index: u32,
    pub strip: Option<ChunkStrip>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct UpdateChunkStripResponse {
    pub chunk: Option<Chunk>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct AllocateReplacementSegmentRequest {
    pub chunk_id: Option<ChunkId>,
    pub old_segment: Option<Segment>,
    pub surviving_segments: Vec<Segment>,
    pub exclude_disk_ids: Vec<DiskId>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct AllocateReplacementSegmentResponse {
    pub segment: Option<Segment>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct DiscardReplacementSegmentRequest {
    pub chunk_id: Option<ChunkId>,
    pub segment: Option<Segment>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct DiscardReplacementSegmentResponse {}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct ReplaceChunkStripRangeRequest {
    pub chunk_id: Option<ChunkId>,
    pub expected_modify_ts: u64,
    pub start_index: u32,
    pub old_strips: Vec<ChunkStrip>,
    pub replacement_strips: Vec<ChunkStrip>,
    pub operation_id: Option<ChunkId>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct ReplaceChunkStripRangeResponse {
    pub chunk: Option<Chunk>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct ListChunksRequest {
    pub start_token: Option<ChunkId>,
    pub partition: u32,
    pub max_keys: u32,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct ListChunksResponse {
    pub chunks: Vec<Chunk>,
    pub next_token: Option<ChunkId>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct TriggerConversionRequest {
    pub chunk_id: Option<ChunkId>,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct TriggerConversionResponse {
    pub accepted_groups: u64,
}

/// Server-local batch conversion filter.
#[derive(Clone, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub struct ConversionFilter {
    /// Restrict the scan to sealed chunks.
    pub sealed_only: bool,
    /// Stop after this many candidate chunks. Zero uses the server page bound.
    pub max_chunks: u32,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct TriggerConversionBatchRequest {
    pub filter: ConversionFilter,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct TriggerConversionBatchResponse {
    pub accepted_chunks: u64,
}

/// Begin the foreground no-reread conversion path for one exact mirror range.
#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct PrepareMirrorToEcConversionRequest {
    pub chunk_id: Option<ChunkId>,
    pub expected_modify_ts: u64,
    pub start_index: u32,
    pub old_strips: Vec<ChunkStrip>,
    pub data_num: u32,
    pub code_num: u32,
    pub client_owner: u64,
    pub claim_lease_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct PrepareMirrorToEcConversionResponse {
    pub task_id: Option<ChunkId>,
    pub operation_id: Option<ChunkId>,
    pub replacement_strip: Option<ChunkStrip>,
}

/// Publish a prepared conversion after every replacement shard is durable.
#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct CompleteMirrorToEcConversionRequest {
    pub chunk_id: Option<ChunkId>,
    pub task_id: Option<ChunkId>,
    pub client_owner: u64,
}

#[derive(Clone, Debug, PartialEq, Default, Serialize, Deserialize)]
pub struct CompleteMirrorToEcConversionResponse {
    pub chunk: Option<Chunk>,
}
