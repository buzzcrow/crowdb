use super::{
    AppendChunkOutcome, Buffer, Chunk, ChunkId, ChunkStrip, DiskId, FBAllocateChunkResponse,
    FBAllocateChunkResponseArgs, FBAllocateReplacementSegmentResponse,
    FBAllocateReplacementSegmentResponseArgs, FBAppendChunkResponse, FBAppendChunkResponseArgs, FBChunk,
    FBChunkArgs, FBChunkState, FBChunkStrip, FBChunkStripArgs, FBChunkType, FBChunkdbRetCode,
    FBDeleteChunkRangeResponse, FBDeleteChunkRangeResponseArgs, FBDiscardReplacementSegmentResponse,
    FBDiscardReplacementSegmentResponseArgs, FBEcState, FBEcStrip, FBEcStripArgs, FBInt128,
    FBListChunksResponse, FBListChunksResponseArgs, FBMirrorStrip, FBMirrorStripArgs,
    FBPrepareMirrorToEcConversionResponse, FBPrepareMirrorToEcConversionResponseArgs, FBQueryChunkResponse,
    FBQueryChunkResponseArgs, FBSegment, FBStripBody, FBStripCleanupIntent, FBStripCleanupIntentArgs,
    FBStripType, FBTriggerConversionBatchResponse, FBTriggerConversionBatchResponseArgs,
    FBTriggerConversionResponse, FBTriggerConversionResponseArgs, FlatBufferBuilder, LifecycleError,
    ProtoChunkState, ProtoChunkType, ProtoEcState, ProtoStrip, ProtoStripType, RpcServer,
};
use crate::conversion::{ConversionError, PreparedConversion};

// ── Error mapping + submission helpers ────────────────────────────

/// Map a `LifecycleError` to `(ret_code, message, range_start, range_end)`.
pub(super) fn map_error(e: &LifecycleError) -> (FBChunkdbRetCode, String, u32, u32) {
    match e {
        LifecycleError::NotMyRange { bucket } => {
            let b = u32::from(*bucket);
            (FBChunkdbRetCode::NotMyRange, e.to_string(), b, b)
        }
        LifecycleError::InvalidStateTransition(_) => {
            (FBChunkdbRetCode::FailedPrecondition, e.to_string(), 0, 0)
        }
        LifecycleError::ChunkNotFound => (FBChunkdbRetCode::NotFound, e.to_string(), 0, 0),
        LifecycleError::ChunkAlreadyExists => (FBChunkdbRetCode::AlreadyExists, e.to_string(), 0, 0),
        LifecycleError::StateConflict => (FBChunkdbRetCode::Aborted, e.to_string(), 0, 0),
        LifecycleError::Allocation(_) | LifecycleError::Commit(_) | LifecycleError::Cleanup(_) => {
            (FBChunkdbRetCode::Internal, e.to_string(), 0, 0)
        }
        LifecycleError::Storage(_) => (FBChunkdbRetCode::Internal, e.to_string(), 0, 0),
        LifecycleError::InvalidRequest(_) => (FBChunkdbRetCode::InvalidArgument, e.to_string(), 0, 0),
        LifecycleError::LockBusy | LifecycleError::LockTimeout => {
            (FBChunkdbRetCode::Unavailable, e.to_string(), 0, 0)
        }
        LifecycleError::StripIndexOutOfRange { .. } => {
            (FBChunkdbRetCode::StripIndexOutOfRange, e.to_string(), 0, 0)
        }
    }
}

/// Submit a flatbuffer response via the zero-copy buffer path. Takes
/// ownership of `ctrl` (the `(Vec<u8>, usize)` from `collapse()`). If
/// the buffer is empty/null, falls back to an empty-control submit.
pub(super) fn submit_fb_response(
    server: &RpcServer,
    conn_handle: *mut std::ffi::c_void,
    ctrl: (Vec<u8>, usize),
    msg_type: u16,
    req_id: u64,
) {
    let buf = Buffer::from_vec_offset(ctrl.0, ctrl.1);
    if buf.is_null_handle() {
        unsafe {
            let _ = server.submit_response(conn_handle, &[], None, msg_type, req_id);
        }
        return;
    }
    unsafe {
        let _ = server.submit_response_buffer(conn_handle, buf, None, msg_type, req_id);
    }
}

/// Submit a chunk-returning result (allocate/append/query/seal/delete/
/// update_strip). On success the chunk is encoded into the response;
/// on error the error code + message + range hint are set.
pub(super) fn submit_chunk_result(
    server: &RpcServer,
    conn_handle: *mut std::ffi::c_void,
    req_id: u64,
    create_nano: u64,
    msg_type: u16,
    result: Result<Chunk, LifecycleError>,
) {
    match result {
        Ok(chunk) => {
            let ctrl =
                build_chunk_response(req_id, create_nano, FBChunkdbRetCode::Success, None, 0, 0, &chunk);
            submit_fb_response(server, conn_handle, ctrl, msg_type, req_id);
        }
        Err(e) => {
            let (code, msg, rs, re) = map_error(&e);
            let ctrl = build_chunk_response(req_id, create_nano, code, Some(&msg), rs, re, &Chunk::default());
            submit_fb_response(server, conn_handle, ctrl, msg_type, req_id);
        }
    }
}

pub(super) fn submit_append_result(
    server: &RpcServer,
    conn_handle: *mut std::ffi::c_void,
    req_id: u64,
    create_nano: u64,
    msg_type: u16,
    result: Result<AppendChunkOutcome, LifecycleError>,
) {
    let mut fbb = FlatBufferBuilder::new();
    let (code, message, range_start, range_end, outcome) = match result {
        Ok(outcome) => (FBChunkdbRetCode::Success, None, 0, 0, Some(outcome)),
        Err(error) => {
            let (code, message, start, end) = map_error(&error);
            (code, Some(message), start, end, None)
        }
    };
    let error_msg = message.as_deref().map(|value| fbb.create_string(value));
    let chunk = outcome
        .as_ref()
        .and_then(|value| value.chunk.as_ref())
        .map(|value| build_chunk_offset(&mut fbb, value));
    let strip_offsets = outcome.as_ref().map_or_else(Vec::new, |value| {
        value
            .strips
            .iter()
            .map(|strip| build_chunk_strip_offset(&mut fbb, strip))
            .collect()
    });
    let strips = (!strip_offsets.is_empty()).then(|| fbb.create_vector(&strip_offsets));
    let response = FBAppendChunkResponse::create(
        &mut fbb,
        &FBAppendChunkResponseArgs {
            id: req_id,
            rpc_create_nano: create_nano,
            ret_code: code,
            error_msg,
            range_start,
            range_end,
            modify_ts: outcome.as_ref().map_or(0, |value| value.modify_ts),
            strips,
            chunk,
        },
    );
    fbb.finish(response, None);
    submit_fb_response(server, conn_handle, fbb.collapse(), msg_type, req_id);
}

/// Submit a synchronous error response (from the dispatch thread).
pub(super) fn submit_error(
    server: &RpcServer,
    conn_handle: *mut std::ffi::c_void,
    req_id: u64,
    create_nano: u64,
    msg_type: u16,
    ret_code: FBChunkdbRetCode,
    msg: &str,
) {
    let ctrl = build_chunk_response(req_id, create_nano, ret_code, Some(msg), 0, 0, &Chunk::default());
    submit_fb_response(server, conn_handle, ctrl, msg_type, req_id);
}

// ── Enum conversion helpers ───────────────────────────────────────

/// Convert a flatbuffer `FBStripType` to the proto `StripType`.
pub(super) fn proto_strip_type(fb: FBStripType) -> Option<ProtoStripType> {
    match fb {
        FBStripType::Mirror => Some(ProtoStripType::Mirror),
        FBStripType::Ec => Some(ProtoStripType::Ec),
        _ => None,
    }
}

/// Convert a flatbuffer `FBChunkType` to the proto `ChunkType`.
pub(super) fn proto_chunk_type(fb: FBChunkType) -> Option<ProtoChunkType> {
    match fb {
        FBChunkType::Repo => Some(ProtoChunkType::Repo),
        FBChunkType::Wal => Some(ProtoChunkType::Wal),
        FBChunkType::BtreePage => Some(ProtoChunkType::BtreePage),
        FBChunkType::PageIndex => Some(ProtoChunkType::PageIndex),
        _ => None,
    }
}

// ── Request parsing helpers ───────────────────────────────────────

/// Parse a flatbuffer `FBChunkStrip` into a proto `ChunkStrip`.
/// Returns `None` if the strip body union is missing/invalid.
pub(super) fn parse_fb_chunk_strip(fb: &FBChunkStrip<'_>) -> Option<ChunkStrip> {
    use crowdb_protocol::chunkdb::rpc::{EcStrip, MirrorStrip};

    let strip_type = match fb.strip_type() {
        FBStripType::Mirror => ProtoStripType::Mirror,
        FBStripType::Ec => ProtoStripType::Ec,
        _ => return None,
    };

    let strip = match fb.strip_body_type() {
        FBStripBody::FBMirrorStrip => {
            let mirror = fb.strip_body_as_fbmirror_strip()?;
            let segments = parse_fb_segments(mirror.segments());
            ProtoStrip::MirrorStrip(MirrorStrip { segments })
        }
        FBStripBody::FBEcStrip => {
            let ec = fb.strip_body_as_fbec_strip()?;
            let segments = parse_fb_segments(ec.segments());
            let ec_state = match ec.ec_state() {
                FBEcState::NoParity => ProtoEcState::NoParity,
                FBEcState::Parity => ProtoEcState::Parity,
                _ => return None,
            };
            ProtoStrip::EcStrip(EcStrip {
                data_num: ec.data_num(),
                code_num: ec.code_num(),
                ec_state: ec_state as i32,
                segments,
            })
        }
        _ => return None,
    };

    Some(ChunkStrip {
        chunk_offset: fb.chunk_offset(),
        strip_sequence: fb.strip_sequence(),
        unit_kb: fb.unit_kb(),
        capacity: fb.capacity(),
        create_ts_ms: fb.create_ts_ms(),
        sealed_ts_ms: fb.sealed_ts_ms(),
        sealed_length: fb.sealed_length(),
        strip_type: strip_type as i32,
        strip: Some(strip),
        usage_bitmap: fb
            .usage_bitmap()
            .map(|v| v.iter().collect::<Vec<u8>>())
            .unwrap_or_default(),
        unavailable_segments: parse_fb_segments(fb.unavailable_segments()),
    })
}

/// Parse a flatbuffer `FBSegment` vector into proto `Segment`s.
pub(super) fn parse_fb_segments<'a, V>(fb_segs: Option<V>) -> Vec<crowdb_protocol::diskdb::rpc::Segment>
where
    V: IntoIterator<Item = &'a FBSegment>,
{
    let Some(vec) = fb_segs else {
        return Vec::new();
    };
    vec.into_iter().map(parse_fb_segment).collect()
}

pub(super) fn parse_fb_segment(s: &FBSegment) -> crowdb_protocol::diskdb::rpc::Segment {
    crowdb_protocol::diskdb::rpc::Segment {
        disk_id: Some(DiskId {
            high: s.disk_id().high(),
            low: s.disk_id().low(),
        }),
        owner_chunk: Some(ChunkId {
            high: s.owner_chunk().high(),
            low: s.owner_chunk().low(),
        }),
        unit_offset: s.unit_offset(),
        zone_index: s.zone_index(),
        unit_count: s.unit_count(),
        allocation_ts: s.allocation_ts(),
    }
}

pub(super) fn submit_segment_result(
    server: &RpcServer,
    conn_handle: *mut std::ffi::c_void,
    req_id: u64,
    create_nano: u64,
    msg_type: u16,
    result: Result<crowdb_protocol::diskdb::rpc::Segment, LifecycleError>,
) {
    let mut fbb = FlatBufferBuilder::new();
    let (code, message, range_start, range_end, segment) = match result {
        Ok(segment) => (FBChunkdbRetCode::Success, None, 0, 0, Some(segment)),
        Err(error) => {
            let (code, message, range_start, range_end) = map_error(&error);
            (code, Some(message), range_start, range_end, None)
        }
    };
    let error_msg = message.as_deref().map(|value| fbb.create_string(value));
    let segment = segment.map(|segment| {
        let disk = segment.disk_id.unwrap_or_default();
        let owner = segment.owner_chunk.unwrap_or_default();
        FBSegment::new(
            &FBInt128::new(disk.high, disk.low),
            &FBInt128::new(owner.high, owner.low),
            segment.unit_offset,
            segment.allocation_ts,
            segment.zone_index,
            segment.unit_count,
        )
    });
    let response = FBAllocateReplacementSegmentResponse::create(
        &mut fbb,
        &FBAllocateReplacementSegmentResponseArgs {
            id: req_id,
            rpc_create_nano: create_nano,
            ret_code: code,
            error_msg,
            range_start,
            range_end,
            segment: segment.as_ref(),
        },
    );
    fbb.finish(response, None);
    submit_fb_response(server, conn_handle, fbb.collapse(), msg_type, req_id);
}

pub(super) fn submit_prepared_conversion_result(
    server: &RpcServer,
    conn_handle: *mut std::ffi::c_void,
    req_id: u64,
    create_nano: u64,
    msg_type: u16,
    result: Result<PreparedConversion, ConversionError>,
) {
    let mut fbb = FlatBufferBuilder::new();
    let (code, message, prepared) = match result {
        Ok(prepared) => (FBChunkdbRetCode::Success, None, Some(prepared)),
        Err(error) => {
            let (code, message) = map_conversion_error(&error);
            (code, Some(message), None)
        }
    };
    let error_msg = message.as_deref().map(|value| fbb.create_string(value));
    let task_id = prepared
        .as_ref()
        .map(|value| FBInt128::new(value.task_id.high, value.task_id.low));
    let operation_id = prepared
        .as_ref()
        .map(|value| FBInt128::new(value.operation_id.high, value.operation_id.low));
    let replacement_strip = prepared
        .as_ref()
        .map(|value| build_chunk_strip_offset(&mut fbb, &value.replacement_strip));
    let response = FBPrepareMirrorToEcConversionResponse::create(
        &mut fbb,
        &FBPrepareMirrorToEcConversionResponseArgs {
            id: req_id,
            rpc_create_nano: create_nano,
            ret_code: code,
            error_msg,
            range_start: 0,
            range_end: 0,
            task_id: task_id.as_ref(),
            operation_id: operation_id.as_ref(),
            replacement_strip,
        },
    );
    fbb.finish(response, None);
    submit_fb_response(server, conn_handle, fbb.collapse(), msg_type, req_id);
}

pub(super) fn submit_conversion_chunk_result(
    server: &RpcServer,
    conn_handle: *mut std::ffi::c_void,
    req_id: u64,
    create_nano: u64,
    msg_type: u16,
    result: Result<Chunk, ConversionError>,
) {
    let (code, message, chunk) = match result {
        Ok(chunk) => (FBChunkdbRetCode::Success, None, chunk),
        Err(error) => {
            let (code, message) = map_conversion_error(&error);
            (code, Some(message), Chunk::default())
        }
    };
    let response = build_chunk_response(req_id, create_nano, code, message.as_deref(), 0, 0, &chunk);
    submit_fb_response(server, conn_handle, response, msg_type, req_id);
}

fn map_conversion_error(error: &ConversionError) -> (FBChunkdbRetCode, String) {
    match error {
        ConversionError::Lifecycle(error) => {
            let (code, message, _, _) = map_error(error);
            (code, message)
        }
        ConversionError::Conflict => (FBChunkdbRetCode::Aborted, error.to_string()),
        ConversionError::StaleClaim => (FBChunkdbRetCode::FailedPrecondition, error.to_string()),
        ConversionError::TaskStore(_) | ConversionError::Payload(_) => {
            (FBChunkdbRetCode::Internal, error.to_string())
        }
    }
}

pub(super) fn submit_conversion_count_result(
    server: &RpcServer,
    conn_handle: *mut std::ffi::c_void,
    req_id: u64,
    create_nano: u64,
    msg_type: u16,
    batch: bool,
    result: Result<u64, ConversionError>,
) {
    let mut fbb = FlatBufferBuilder::new();
    let (code, message, count) = match result {
        Ok(count) => (FBChunkdbRetCode::Success, None, count),
        Err(error) => {
            let (code, message) = map_conversion_error(&error);
            (code, Some(message), 0)
        }
    };
    let error_msg = message.as_deref().map(|value| fbb.create_string(value));
    if batch {
        let response = FBTriggerConversionBatchResponse::create(
            &mut fbb,
            &FBTriggerConversionBatchResponseArgs {
                id: req_id,
                rpc_create_nano: create_nano,
                ret_code: code,
                error_msg,
                range_start: 0,
                range_end: 0,
                accepted_chunks: count,
            },
        );
        fbb.finish(response, None);
    } else {
        let response = FBTriggerConversionResponse::create(
            &mut fbb,
            &FBTriggerConversionResponseArgs {
                id: req_id,
                rpc_create_nano: create_nano,
                ret_code: code,
                error_msg,
                range_start: 0,
                range_end: 0,
                accepted_groups: count,
            },
        );
        fbb.finish(response, None);
    }
    submit_fb_response(server, conn_handle, fbb.collapse(), msg_type, req_id);
}

// ── Response builders ─────────────────────────────────────────────

/// Build a chunk-carrying response (allocate/append/query/seal/delete/
/// update_strip). All six response tables share the same field layout
/// (`id`, `rpc_create_nano`, `ret_code`, `error_msg`, `range_start`,
/// `range_end`, `chunk`), so a single builder covers all of them — the
/// caller selects the table type via the `FBMsgType` constant used to
/// finish + submit.
pub(super) fn build_chunk_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBChunkdbRetCode,
    error_msg: Option<&str>,
    range_start: u32,
    range_end: u32,
    chunk: &Chunk,
) -> (Vec<u8>, usize) {
    let mut fbb = FlatBufferBuilder::new();
    let err_off = error_msg.map(|m| fbb.create_string(m));
    let chunk_off = if ret_code == FBChunkdbRetCode::Success {
        Some(build_chunk_offset(&mut fbb, chunk))
    } else {
        None
    };
    // All chunk-carrying response tables share the same Args shape, so
    // we build an FBAllocateChunkResponse as the generic carrier. The
    // client parses by msg_type and reads ret_code + error_msg + chunk.
    let off = FBAllocateChunkResponse::create(
        &mut fbb,
        &FBAllocateChunkResponseArgs {
            id: req_id,
            rpc_create_nano: create_nano,
            ret_code,
            error_msg: err_off,
            range_start,
            range_end,
            chunk: chunk_off,
        },
    );
    fbb.finish(off, None);
    fbb.collapse()
}

pub(super) fn build_query_response(
    req_id: u64,
    create_nano: u64,
    chunk: &Chunk,
    layout_validity_ms: u64,
) -> (Vec<u8>, usize) {
    let mut fbb = FlatBufferBuilder::new();
    let chunk = build_chunk_offset(&mut fbb, chunk);
    let response = FBQueryChunkResponse::create(
        &mut fbb,
        &FBQueryChunkResponseArgs {
            id: req_id,
            rpc_create_nano: create_nano,
            ret_code: FBChunkdbRetCode::Success,
            error_msg: None,
            range_start: 0,
            range_end: 0,
            chunk: Some(chunk),
            layout_validity_ms,
        },
    );
    fbb.finish(response, None);
    fbb.collapse()
}

/// Build a `FBDeleteChunkRangeResponse` (no chunk field).
#[allow(clippy::too_many_arguments)]
pub(super) fn build_delete_range_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBChunkdbRetCode,
    error_msg: Option<&str>,
    range_start: u32,
    range_end: u32,
) -> (Vec<u8>, usize) {
    let mut fbb = FlatBufferBuilder::new();
    let err_off = error_msg.map(|m| fbb.create_string(m));
    let off = FBDeleteChunkRangeResponse::create(
        &mut fbb,
        &FBDeleteChunkRangeResponseArgs {
            id: req_id,
            rpc_create_nano: create_nano,
            ret_code,
            error_msg: err_off,
            range_start,
            range_end,
        },
    );
    fbb.finish(off, None);
    fbb.collapse()
}

pub(super) fn build_discard_replacement_response(
    req_id: u64,
    create_nano: u64,
    code: FBChunkdbRetCode,
    message: Option<&str>,
    range_start: u32,
    range_end: u32,
) -> (Vec<u8>, usize) {
    let mut fbb = FlatBufferBuilder::new();
    let error_msg = message.map(|value| fbb.create_string(value));
    let response = FBDiscardReplacementSegmentResponse::create(
        &mut fbb,
        &FBDiscardReplacementSegmentResponseArgs {
            id: req_id,
            rpc_create_nano: create_nano,
            ret_code: code,
            error_msg,
            range_start,
            range_end,
        },
    );
    fbb.finish(response, None);
    fbb.collapse()
}

/// Build a `FBListChunksResponse`.
#[allow(clippy::too_many_arguments)]
pub(super) fn build_list_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBChunkdbRetCode,
    error_msg: Option<&str>,
    range_start: u32,
    range_end: u32,
    chunks: &[Chunk],
    next_token: Option<&ChunkId>,
    has_next_token: bool,
) -> (Vec<u8>, usize) {
    let mut fbb = FlatBufferBuilder::new();
    let err_off = error_msg.map(|m| fbb.create_string(m));
    let chunk_offs: Vec<flatbuffers::WIPOffset<FBChunk<'_>>> =
        chunks.iter().map(|c| build_chunk_offset(&mut fbb, c)).collect();
    let chunks_vec = if chunk_offs.is_empty() {
        None
    } else {
        Some(fbb.create_vector(&chunk_offs))
    };
    let next_token_off = next_token.map(|id| FBInt128::new(id.high, id.low));
    let off = FBListChunksResponse::create(
        &mut fbb,
        &FBListChunksResponseArgs {
            id: req_id,
            rpc_create_nano: create_nano,
            ret_code,
            error_msg: err_off,
            range_start,
            range_end,
            chunks: chunks_vec,
            next_token: next_token_off.as_ref(),
            has_next_token,
        },
    );
    fbb.finish(off, None);
    fbb.collapse()
}

/// Build a `FBChunk` WIPOffset from a proto `Chunk`.
pub(super) fn build_chunk_offset<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    chunk: &Chunk,
) -> flatbuffers::WIPOffset<FBChunk<'a>> {
    let id = chunk.id.unwrap_or_default();
    let id_off = FBInt128::new(id.high, id.low);
    let strip_offs: Vec<flatbuffers::WIPOffset<FBChunkStrip<'a>>> = chunk
        .strips
        .iter()
        .map(|s| build_chunk_strip_offset(fbb, s))
        .collect();
    let strips_vec = if strip_offs.is_empty() {
        None
    } else {
        Some(fbb.create_vector(&strip_offs))
    };
    let cleanup_offs: Vec<_> = chunk
        .cleanup_intents
        .iter()
        .map(|intent| {
            let operation = intent.operation_id.map(|id| FBInt128::new(id.high, id.low));
            let retired: Vec<_> = intent
                .retired_segments
                .iter()
                .map(|segment| {
                    let disk = segment.disk_id.unwrap_or_default();
                    let owner = segment.owner_chunk.unwrap_or_default();
                    FBSegment::new(
                        &FBInt128::new(disk.high, disk.low),
                        &FBInt128::new(owner.high, owner.low),
                        segment.unit_offset,
                        segment.allocation_ts,
                        segment.zone_index,
                        segment.unit_count,
                    )
                })
                .collect();
            let retired = fbb.create_vector(&retired);
            FBStripCleanupIntent::create(
                fbb,
                &FBStripCleanupIntentArgs {
                    operation_id: operation.as_ref(),
                    retired_segments: Some(retired),
                    not_before_ms: intent.not_before_ms,
                },
            )
        })
        .collect();
    let cleanup_intents = (!cleanup_offs.is_empty()).then(|| fbb.create_vector(&cleanup_offs));
    let last_strip_replacement = chunk
        .last_strip_replacement
        .map(|id| FBInt128::new(id.high, id.low));
    let state = ProtoChunkState::try_from(chunk.state).unwrap_or(ProtoChunkState::Init);
    let chunk_type = ProtoChunkType::try_from(chunk.chunk_type).unwrap_or(ProtoChunkType::Repo);
    FBChunk::create(
        fbb,
        &FBChunkArgs {
            id: Some(&id_off),
            modify_ts: chunk.modify_ts,
            state: fb_chunk_state(state),
            create_ts_ms: chunk.create_ts_ms,
            sealed_ts_ms: chunk.sealed_ts_ms,
            capacity: chunk.capacity,
            sealed_length: chunk.sealed_length,
            strips: strips_vec,
            chunk_type: fb_chunk_type(chunk_type),
            writer_epoch: chunk.writer_epoch,
            acknowledged_cursor: chunk.acknowledged_cursor,
            closed_strip_sequence: chunk.closed_strip_sequence.unwrap_or(u32::MAX),
            writer_lease_deadline_ms: chunk.writer_lease_deadline_ms,
            next_strip_sequence: chunk.next_strip_sequence,
            cleanup_intents,
            last_strip_replacement: last_strip_replacement.as_ref(),
        },
    )
}

/// Build a `FBChunkStrip` WIPOffset from a proto `ChunkStrip`.
pub(super) fn build_chunk_strip_offset<'a>(
    fbb: &mut FlatBufferBuilder<'a>,
    strip: &ChunkStrip,
) -> flatbuffers::WIPOffset<FBChunkStrip<'a>> {
    let strip_type = ProtoStripType::try_from(strip.strip_type).unwrap_or(ProtoStripType::Mirror);
    let (body_type, body_off) = build_strip_body_offset(fbb, strip);
    let usage_bitmap_off = if strip.usage_bitmap.is_empty() {
        None
    } else {
        Some(fbb.create_vector(&strip.usage_bitmap))
    };
    let unavailable_values: Vec<_> = strip
        .unavailable_segments
        .iter()
        .map(|segment| {
            let disk = segment.disk_id.unwrap_or_default();
            let owner = segment.owner_chunk.unwrap_or_default();
            FBSegment::new(
                &FBInt128::new(disk.high, disk.low),
                &FBInt128::new(owner.high, owner.low),
                segment.unit_offset,
                segment.allocation_ts,
                segment.zone_index,
                segment.unit_count,
            )
        })
        .collect();
    let unavailable_segments =
        (!unavailable_values.is_empty()).then(|| fbb.create_vector(&unavailable_values));
    FBChunkStrip::create(
        fbb,
        &FBChunkStripArgs {
            chunk_offset: strip.chunk_offset,
            strip_sequence: strip.strip_sequence,
            unit_kb: strip.unit_kb,
            capacity: strip.capacity,
            create_ts_ms: strip.create_ts_ms,
            sealed_ts_ms: strip.sealed_ts_ms,
            sealed_length: strip.sealed_length,
            strip_type: fb_strip_type(strip_type),
            strip_body_type: body_type,
            strip_body: body_off,
            usage_bitmap: usage_bitmap_off,
            unavailable_segments,
        },
    )
}

/// Build the strip body union offset from a proto `ChunkStrip`.
/// Returns `(union_type, union_offset)`.
pub(super) fn build_strip_body_offset(
    fbb: &mut FlatBufferBuilder<'_>,
    strip: &ChunkStrip,
) -> (
    FBStripBody,
    Option<flatbuffers::WIPOffset<flatbuffers::UnionWIPOffset>>,
) {
    let Some(ref body) = strip.strip else {
        return (FBStripBody::NONE, None);
    };
    match body {
        ProtoStrip::MirrorStrip(mirror) => {
            let seg_offs: Vec<FBSegment> = mirror
                .segments
                .iter()
                .map(|s| {
                    let disk_id = s.disk_id.unwrap_or_default();
                    let owner = s.owner_chunk.unwrap_or_default();
                    FBSegment::new(
                        &FBInt128::new(disk_id.high, disk_id.low),
                        &FBInt128::new(owner.high, owner.low),
                        s.unit_offset,
                        s.allocation_ts,
                        s.zone_index,
                        s.unit_count,
                    )
                })
                .collect();
            let seg_vec = fbb.create_vector(&seg_offs);
            let off = FBMirrorStrip::create(
                fbb,
                &FBMirrorStripArgs {
                    segments: Some(seg_vec),
                },
            );
            (FBStripBody::FBMirrorStrip, Some(off.as_union_value()))
        }
        ProtoStrip::EcStrip(ec) => {
            let seg_offs: Vec<FBSegment> = ec
                .segments
                .iter()
                .map(|s| {
                    let disk_id = s.disk_id.unwrap_or_default();
                    let owner = s.owner_chunk.unwrap_or_default();
                    FBSegment::new(
                        &FBInt128::new(disk_id.high, disk_id.low),
                        &FBInt128::new(owner.high, owner.low),
                        s.unit_offset,
                        s.allocation_ts,
                        s.zone_index,
                        s.unit_count,
                    )
                })
                .collect();
            let seg_vec = fbb.create_vector(&seg_offs);
            let ec_state = ProtoEcState::try_from(ec.ec_state).unwrap_or(ProtoEcState::NoParity);
            let off = FBEcStrip::create(
                fbb,
                &FBEcStripArgs {
                    data_num: ec.data_num,
                    code_num: ec.code_num,
                    ec_state: fb_ec_state(ec_state),
                    segments: Some(seg_vec),
                },
            );
            (FBStripBody::FBEcStrip, Some(off.as_union_value()))
        }
    }
}

// ── Enum cast helpers (proto i32 → FB enum) ───────────────────────

pub(super) fn fb_chunk_state(s: ProtoChunkState) -> FBChunkState {
    match s {
        ProtoChunkState::Init => FBChunkState::Init,
        ProtoChunkState::Active => FBChunkState::Active,
        ProtoChunkState::Sealed => FBChunkState::Sealed,
        ProtoChunkState::Deleted => FBChunkState::Deleted,
    }
}

pub(super) fn fb_chunk_type(t: ProtoChunkType) -> FBChunkType {
    match t {
        ProtoChunkType::Repo => FBChunkType::Repo,
        ProtoChunkType::Wal => FBChunkType::Wal,
        ProtoChunkType::BtreePage => FBChunkType::BtreePage,
        ProtoChunkType::PageIndex => FBChunkType::PageIndex,
    }
}

pub(super) fn fb_strip_type(t: ProtoStripType) -> FBStripType {
    match t {
        ProtoStripType::Mirror => FBStripType::Mirror,
        ProtoStripType::Ec => FBStripType::Ec,
    }
}

pub(super) fn fb_ec_state(s: ProtoEcState) -> FBEcState {
    match s {
        ProtoEcState::NoParity => FBEcState::NoParity,
        ProtoEcState::Parity => FBEcState::Parity,
    }
}
