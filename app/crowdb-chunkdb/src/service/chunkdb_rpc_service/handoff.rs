// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use flatbuffers::FlatBufferBuilder;

use super::{
    submit_fb_response, Arc, ChunkId, ChunkdbRpcService, FBChunkdbRetCode, FBMsgType,
    FBRelocateSegmentHandoffRequest, FBRelocateSegmentHandoffResponse, FBRelocateSegmentHandoffResponseArgs,
    FBRelocationHandoffDisposition, RequestGuard, RpcServer, ServerRequest,
};
use crowdb_protocol::chunkdb::rpc::{RelocateSegmentHandoffRequest, RelocationHandoffDisposition};

impl ChunkdbRpcService {
    pub(super) fn handle_relocate_segment_handoff(
        &self,
        req: ServerRequest,
        server: &Arc<RpcServer>,
        mut request_guard: RequestGuard,
    ) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::ERelocateSegmentHandoffResponse.0 as u16;
        let conn_handle = req.conn_handle as usize;
        let relocation = self.relocation.clone();
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let Some(relocation) = relocation else {
                submit_handoff_response(
                    &server,
                    conn_handle as *mut _,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::Unavailable,
                    Some("relocation handoff service is unavailable"),
                    RelocationHandoffDisposition::Rejected,
                );
                return;
            };
            let Ok(fb_req) = flatbuffers::root::<FBRelocateSegmentHandoffRequest>(req.control()) else {
                submit_handoff_response(
                    &server,
                    conn_handle as *mut _,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    Some("invalid request flatbuffer"),
                    RelocationHandoffDisposition::Rejected,
                );
                return;
            };
            let request = RelocateSegmentHandoffRequest {
                operation_id: fb_req.operation_id().map(|id| ChunkId {
                    high: id.high(),
                    low: id.low(),
                }),
                chunk_id: fb_req.chunk_id().map(|id| ChunkId {
                    high: id.high(),
                    low: id.low(),
                }),
                source: fb_req.source().map(super::parse_fb_segment),
                target: fb_req.target().map(super::parse_fb_segment),
            };
            match relocation.admit(&request, unix_time_ms()).await {
                Ok(disposition) => {
                    request_guard.mark_success();
                    submit_handoff_response(
                        &server,
                        conn_handle as *mut _,
                        req_id,
                        create_nano,
                        msg_type,
                        FBChunkdbRetCode::Success,
                        None,
                        disposition,
                    );
                }
                Err(error) => submit_handoff_response(
                    &server,
                    conn_handle as *mut _,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    Some(&error.to_string()),
                    RelocationHandoffDisposition::Rejected,
                ),
            }
        });
    }
}

#[allow(clippy::too_many_arguments)]
fn submit_handoff_response(
    server: &RpcServer,
    conn_handle: *mut std::ffi::c_void,
    req_id: u64,
    create_nano: u64,
    msg_type: u16,
    ret_code: FBChunkdbRetCode,
    error: Option<&str>,
    disposition: RelocationHandoffDisposition,
) {
    let mut fbb = FlatBufferBuilder::new();
    let error_msg = error.map(|message| fbb.create_string(message));
    let response = FBRelocateSegmentHandoffResponse::create(
        &mut fbb,
        &FBRelocateSegmentHandoffResponseArgs {
            id: req_id,
            rpc_create_nano: create_nano,
            ret_code,
            error_msg,
            range_start: 0,
            range_end: 0,
            disposition: disposition_to_fb(disposition),
        },
    );
    fbb.finish(response, None);
    submit_fb_response(server, conn_handle, fbb.collapse(), msg_type, req_id);
}

fn disposition_to_fb(disposition: RelocationHandoffDisposition) -> FBRelocationHandoffDisposition {
    match disposition {
        RelocationHandoffDisposition::Accepted => FBRelocationHandoffDisposition::Accepted,
        RelocationHandoffDisposition::Published => FBRelocationHandoffDisposition::Published,
        RelocationHandoffDisposition::Stale => FBRelocationHandoffDisposition::Stale,
        RelocationHandoffDisposition::Rejected => FBRelocationHandoffDisposition::Rejected,
    }
}

fn unix_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| {
            u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
        })
}
