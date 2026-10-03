// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Select authority before dispatching a request to its lifecycle/task runtime.

use crowdb_protocol::chunk_domain::ChunkDomain;
use crowdb_protocol::chunkdb_fb::{
    FBAdHocEcRecoveryRequest, FBAdvanceChunkWriteRequest, FBAllocateChunkRequest,
    FBAllocateReplacementSegmentRequest, FBAppendChunkRequest, FBCompleteMirrorToEcConversionRequest,
    FBDeleteChunkRangeRequest, FBDeleteChunkRequest, FBDiscardReplacementSegmentRequest,
    FBMutateStripReservationRequest, FBPrepareMirrorToEcConversionRequest, FBQueryChunkRequest,
    FBQuerySegmentOwnerRequest, FBRelocateSegmentHandoffRequest, FBReplaceChunkStripRangeRequest,
    FBReserveStripGroupRequest, FBSealChunkRequest, FBTriggerConversionRequest, FBUpdateChunkStripRequest,
};
use crowdb_protocol::common::ChunkId;
use crowdb_protocol::fb::FBMsgType;

pub(super) fn is_system_request(message: u16, control: &[u8]) -> bool {
    if message == FBMsgType::EAllocateChunkRequest.0 as u16 {
        return flatbuffers::root::<FBAllocateChunkRequest>(control)
            .is_ok_and(|request| matches!(request.chunk_type().0, 1..=3));
    }
    macro_rules! chunk_request {
        ($($message:ident => $frame:ident),+ $(,)?) => {
            match message {
                $(value if value == FBMsgType::$message.0 as u16 => {
                    flatbuffers::root::<$frame>(control).ok()
                        .and_then(|request| request.chunk_id().map(|id| ChunkId { high: id.high(), low: id.low() }))
                },)+
                _ => None,
            }
        };
    }
    let chunk = chunk_request! {
        EAppendChunkRequest => FBAppendChunkRequest,
        EAdvanceChunkWriteRequest => FBAdvanceChunkWriteRequest,
        EReserveStripGroupRequest => FBReserveStripGroupRequest,
        EMutateStripReservationRequest => FBMutateStripReservationRequest,
        EQueryChunkRequest => FBQueryChunkRequest,
        ESealChunkRequest => FBSealChunkRequest,
        EDeleteChunkRequest => FBDeleteChunkRequest,
        EDeleteChunkRangeRequest => FBDeleteChunkRangeRequest,
        EUpdateChunkStripRequest => FBUpdateChunkStripRequest,
        EAllocateReplacementSegmentRequest => FBAllocateReplacementSegmentRequest,
        EDiscardReplacementSegmentRequest => FBDiscardReplacementSegmentRequest,
        EReplaceChunkStripRangeRequest => FBReplaceChunkStripRangeRequest,
        EPrepareMirrorToEcConversionRequest => FBPrepareMirrorToEcConversionRequest,
        ECompleteMirrorToEcConversionRequest => FBCompleteMirrorToEcConversionRequest,
        ETriggerConversionRequest => FBTriggerConversionRequest,
        EQuerySegmentOwnerRequest => FBQuerySegmentOwnerRequest,
        ERelocateSegmentHandoffRequest => FBRelocateSegmentHandoffRequest,
        EAdHocEcRecoveryRequest => FBAdHocEcRecoveryRequest,
    };
    chunk
        .as_ref()
        .is_some_and(|id| ChunkDomain::for_chunk(id) == Some(ChunkDomain::System))
}
