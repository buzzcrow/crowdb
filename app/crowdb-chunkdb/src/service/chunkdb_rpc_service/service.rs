// Copyright 2026-present Gian <crow.db@outlook.com>.
// Licensed under the Apache License, Version 2.0.

// `submit_response` takes a raw `conn_handle` from the FFI dispatch
// callback — the unsafe is inherent to the FFI boundary (the pointer
// is a valid `Connection*` for the duration of the callback, verified
// by the C++ transport). Confined to `submit_response` calls.
#![allow(unsafe_code)]

//! crowdb-rpc handler set for `ChunkdbService` (R116 migration).
//!
//! Each handler dispatches by `msg_type` to the existing lifecycle
//! logic — the same bodies as the tonic `ChunkdbService` in
//! `chunkdb_service.rs`. The response is a flatbuffer frame built per
//! `design-crowdb-rpc.md` §6 (build → finish → attach) and submitted via
//! `RpcServer::submit_response`.
//!
//! Handlers run on the C++ I/O worker thread. All chunkdb lifecycle
//! methods are async (KV persist, diskdb allocation), so each handler
//! spawns a tokio task via the captured `Handle` and submits the
//! response from the task. Each handler closure captures an
//! `Arc<RpcServer>` so it can submit responses from either the dispatch
//! thread (sync error path) or the spawned task (async success path).
//!
//! `NotMyRangeHint` is not a separate message — it is `ret_code =
//! NotMyRange` + `range_start`/`range_end` diagnostic fields on every
//! response table. The server does not know the owning instance; the
//! client refreshes its binding cache from group-0 and re-routes.

use std::sync::Arc;

use crowdb_protocol::chunkdb::rpc::{
    Chunk, ChunkState as ProtoChunkState, ChunkStrip, ChunkType as ProtoChunkType, EcState as ProtoEcState,
    Strip as ProtoStrip, StripType as ProtoStripType,
};
use crowdb_protocol::chunkdb_fb::{
    FBAdvanceChunkWriteRequest, FBAllocateChunkRequest, FBAllocateChunkResponse, FBAllocateChunkResponseArgs,
    FBAllocateReplacementSegmentRequest, FBAllocateReplacementSegmentResponse,
    FBAllocateReplacementSegmentResponseArgs, FBAppendChunkRequest, FBAppendChunkResponse,
    FBAppendChunkResponseArgs, FBChunk, FBChunkArgs, FBChunkState, FBChunkStrip, FBChunkStripArgs,
    FBChunkType, FBChunkdbRetCode, FBCompleteMirrorToEcConversionRequest, FBDeleteChunkRangeRequest,
    FBDeleteChunkRangeResponse, FBDeleteChunkRangeResponseArgs, FBDeleteChunkRequest,
    FBDiscardReplacementSegmentRequest, FBDiscardReplacementSegmentResponse,
    FBDiscardReplacementSegmentResponseArgs, FBEcState, FBEcStrip, FBEcStripArgs, FBInt128,
    FBListChunksRequest, FBListChunksResponse, FBListChunksResponseArgs, FBMirrorStrip, FBMirrorStripArgs,
    FBPrepareMirrorToEcConversionRequest, FBPrepareMirrorToEcConversionResponse,
    FBPrepareMirrorToEcConversionResponseArgs, FBQueryChunkRequest, FBQueryChunkResponse,
    FBQueryChunkResponseArgs, FBReplaceChunkStripRangeRequest, FBSealChunkRequest, FBSegment, FBStripBody,
    FBStripCleanupIntent, FBStripCleanupIntentArgs, FBStripType, FBUpdateChunkStripRequest,
};
use crowdb_protocol::common::{ChunkId, DiskId};
use crowdb_protocol::fb::FBMsgType;
use crowdb_rpc_ffi::{Buffer, RpcServer, ServerRequest};
use flatbuffers::FlatBufferBuilder;
use tokio::runtime::Handle;

use crate::conversion::ConversionCoordinator;
use crate::lifecycle::{AppendChunkOutcome, LifecycleError, LifecycleHandler};
use crate::metrics::{ChunkdbMetrics, RequestGuard, RequestKind};

/// crowdb-rpc handler set for `ChunkdbService`. Holds the same
/// `LifecycleHandler` as the tonic `ChunkdbService`; `register_handlers`
/// wires one handler per request `msg_type` into a `RpcServer`.
pub struct ChunkdbRpcService {
    handler: Arc<LifecycleHandler>,
    metrics: Arc<ChunkdbMetrics>,
    /// Tokio runtime handle for spawning async work from the C++ I/O
    /// thread callback.
    rt: Handle,
    conversion: Option<Arc<ConversionCoordinator>>,
}

impl ChunkdbRpcService {
    pub fn new(handler: Arc<LifecycleHandler>, metrics: Arc<ChunkdbMetrics>, rt: Handle) -> Self {
        Self {
            handler,
            metrics,
            rt,
            conversion: None,
        }
    }

    #[must_use]
    pub fn with_conversion(mut self, conversion: Arc<ConversionCoordinator>) -> Self {
        self.conversion = Some(conversion);
        self
    }

    /// Register all chunkdb request handlers into the `RpcServer`.
    pub fn register_handlers(self: &Arc<Self>, server: &Arc<RpcServer>) {
        server.register_handler(
            FBMsgType::EAllocateChunkRequest.0 as u16,
            Self::make_handler(
                Arc::clone(self),
                Arc::clone(server),
                RequestKind::AllocateChunk,
                Self::handle_allocate,
            ),
        );
        server.register_handler(
            FBMsgType::EAppendChunkRequest.0 as u16,
            Self::make_handler(
                Arc::clone(self),
                Arc::clone(server),
                RequestKind::AppendChunk,
                Self::handle_append,
            ),
        );
        server.register_handler(
            FBMsgType::EAdvanceChunkWriteRequest.0 as u16,
            Self::make_handler(
                Arc::clone(self),
                Arc::clone(server),
                RequestKind::AdvanceChunkWrite,
                Self::handle_advance_write,
            ),
        );
        server.register_handler(
            FBMsgType::EQueryChunkRequest.0 as u16,
            Self::make_handler(
                Arc::clone(self),
                Arc::clone(server),
                RequestKind::QueryChunk,
                Self::handle_query,
            ),
        );
        server.register_handler(
            FBMsgType::ESealChunkRequest.0 as u16,
            Self::make_handler(
                Arc::clone(self),
                Arc::clone(server),
                RequestKind::SealChunk,
                Self::handle_seal,
            ),
        );
        server.register_handler(
            FBMsgType::EDeleteChunkRequest.0 as u16,
            Self::make_handler(
                Arc::clone(self),
                Arc::clone(server),
                RequestKind::DeleteChunk,
                Self::handle_delete,
            ),
        );
        server.register_handler(
            FBMsgType::EDeleteChunkRangeRequest.0 as u16,
            Self::make_handler(
                Arc::clone(self),
                Arc::clone(server),
                RequestKind::DeleteChunkRange,
                Self::handle_delete_range,
            ),
        );
        server.register_handler(
            FBMsgType::EUpdateChunkStripRequest.0 as u16,
            Self::make_handler(
                Arc::clone(self),
                Arc::clone(server),
                RequestKind::UpdateChunkStrip,
                Self::handle_update_strip,
            ),
        );
        server.register_handler(
            FBMsgType::EListChunksRequest.0 as u16,
            Self::make_handler(
                Arc::clone(self),
                Arc::clone(server),
                RequestKind::ListChunks,
                Self::handle_list,
            ),
        );
        self.register_replacement_handlers(server);
        self.register_conversion_handlers(server);
    }

    fn register_conversion_handlers(self: &Arc<Self>, server: &Arc<RpcServer>) {
        server.register_handler(
            FBMsgType::EPrepareMirrorToEcConversionRequest.0 as u16,
            Self::make_handler(
                Arc::clone(self),
                Arc::clone(server),
                RequestKind::UpdateChunkStrip,
                Self::handle_prepare_conversion,
            ),
        );
        server.register_handler(
            FBMsgType::ECompleteMirrorToEcConversionRequest.0 as u16,
            Self::make_handler(
                Arc::clone(self),
                Arc::clone(server),
                RequestKind::UpdateChunkStrip,
                Self::handle_complete_conversion,
            ),
        );
    }

    fn register_replacement_handlers(self: &Arc<Self>, server: &Arc<RpcServer>) {
        server.register_handler(
            FBMsgType::EAllocateReplacementSegmentRequest.0 as u16,
            Self::make_handler(
                Arc::clone(self),
                Arc::clone(server),
                RequestKind::UpdateChunkStrip,
                Self::handle_allocate_replacement,
            ),
        );
        server.register_handler(
            FBMsgType::EReplaceChunkStripRangeRequest.0 as u16,
            Self::make_handler(
                Arc::clone(self),
                Arc::clone(server),
                RequestKind::UpdateChunkStrip,
                Self::handle_replace_range,
            ),
        );
        server.register_handler(
            FBMsgType::EDiscardReplacementSegmentRequest.0 as u16,
            Self::make_handler(
                Arc::clone(self),
                Arc::clone(server),
                RequestKind::UpdateChunkStrip,
                Self::handle_discard_replacement,
            ),
        );
        server.register_handler(
            FBMsgType::EPrepareMirrorToEcConversionRequest.0 as u16,
            Self::make_handler(
                Arc::clone(self),
                Arc::clone(server),
                RequestKind::UpdateChunkStrip,
                Self::handle_prepare_conversion,
            ),
        );
        server.register_handler(
            FBMsgType::ECompleteMirrorToEcConversionRequest.0 as u16,
            Self::make_handler(
                Arc::clone(self),
                Arc::clone(server),
                RequestKind::UpdateChunkStrip,
                Self::handle_complete_conversion,
            ),
        );
    }

    /// Build a handler closure that dispatches to the given method.
    fn make_handler(
        this: Arc<Self>,
        server: Arc<RpcServer>,
        kind: RequestKind,
        f: fn(&Self, ServerRequest, &Arc<RpcServer>, RequestGuard),
    ) -> impl Fn(ServerRequest) + Send + 'static {
        move |req| {
            let guard = this.metrics.requests.start(kind);
            f(&this, req, &server, guard);
        }
    }
}

#[path = "mutations.rs"]
mod mutations;
#[path = "queries.rs"]
mod queries;
#[path = "wire.rs"]
mod wire;

use wire::{
    build_delete_range_response, build_discard_replacement_response, build_list_response,
    build_query_response, map_error, parse_fb_chunk_strip, parse_fb_segment, parse_fb_segments,
    proto_chunk_type, proto_strip_type, submit_append_result, submit_chunk_result,
    submit_conversion_chunk_result, submit_error, submit_fb_response, submit_prepared_conversion_result,
    submit_segment_result,
};
