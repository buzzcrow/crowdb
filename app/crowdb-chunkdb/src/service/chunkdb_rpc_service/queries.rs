use super::{
    build_list_response, build_query_response, map_error, submit_error, submit_fb_response, Arc, ChunkId,
    ChunkdbRpcService, FBChunkdbRetCode, FBListChunksRequest, FBMsgType, FBOwnerDisposition,
    FBQueryChunkRequest, FBQuerySegmentOwnerRequest, FBQuerySegmentOwnerResponse,
    FBQuerySegmentOwnerResponseArgs, RequestGuard, RpcServer, ServerRequest,
};
use crowdb_protocol::chunkdb::rpc::SegmentOwnerDisposition;
use flatbuffers::FlatBufferBuilder;

impl ChunkdbRpcService {
    // ── QueryChunk ────────────────────────────────────────────────

    pub(super) fn handle_query(
        &self,
        req: ServerRequest,
        server: &Arc<RpcServer>,
        mut request: RequestGuard,
    ) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EQueryChunkResponse.0 as u16;
        let conn_handle_usize = req.conn_handle as usize;

        let handler = Arc::clone(&self.handler);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let Ok(fb_req) = flatbuffers::root::<FBQueryChunkRequest>(req.control()) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "invalid request flatbuffer",
                );
                return;
            };

            let Some(chunk_id) = fb_req.chunk_id().map(|id| ChunkId {
                high: id.high(),
                low: id.low(),
            }) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "missing chunk_id",
                );
                return;
            };

            match handler.query_chunk(&chunk_id).await {
                Ok(chunk) => {
                    request.mark_success();
                    let response =
                        build_query_response(req_id, create_nano, &chunk, handler.layout_validity_ms());
                    submit_fb_response(
                        &server,
                        conn_handle_usize as *mut std::ffi::c_void,
                        response,
                        msg_type,
                        req_id,
                    );
                }
                Err(error) => {
                    let (code, message, _, _) = map_error(&error);
                    submit_error(
                        &server,
                        conn_handle_usize as *mut std::ffi::c_void,
                        req_id,
                        create_nano,
                        msg_type,
                        code,
                        &message,
                    );
                }
            }
        });
    }

    // ── QuerySegmentOwner ─────────────────────────────────────────

    pub(super) fn handle_query_segment_owner(
        &self,
        req: ServerRequest,
        server: &Arc<RpcServer>,
        mut request: RequestGuard,
    ) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EQuerySegmentOwnerResponse.0 as u16;
        let conn_handle = req.conn_handle as usize;
        let owner = self.owner.clone();
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let Some(owner) = owner else {
                submit_owner_error(
                    &server,
                    conn_handle as *mut _,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::Unavailable,
                    "owner disposition service is unavailable",
                );
                return;
            };
            let Ok(fb_req) = flatbuffers::root::<FBQuerySegmentOwnerRequest>(req.control()) else {
                submit_owner_error(
                    &server,
                    conn_handle as *mut _,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "invalid request flatbuffer",
                );
                return;
            };
            let Some(chunk_id) = fb_req.chunk_id().map(|id| ChunkId {
                high: id.high(),
                low: id.low(),
            }) else {
                submit_owner_error(
                    &server,
                    conn_handle as *mut _,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "missing chunk_id",
                );
                return;
            };
            let Some(segment) = fb_req.segment().map(super::parse_fb_segment) else {
                submit_owner_error(
                    &server,
                    conn_handle as *mut _,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "missing segment",
                );
                return;
            };
            match owner.resolve(&chunk_id, &segment).await {
                Ok(disposition) => {
                    request.mark_success();
                    let mut fbb = FlatBufferBuilder::new();
                    let response = FBQuerySegmentOwnerResponse::create(
                        &mut fbb,
                        &FBQuerySegmentOwnerResponseArgs {
                            id: req_id,
                            rpc_create_nano: create_nano,
                            ret_code: FBChunkdbRetCode::Success,
                            error_msg: None,
                            range_start: 0,
                            range_end: 0,
                            disposition: disposition_to_fb(disposition),
                        },
                    );
                    fbb.finish(response, None);
                    submit_fb_response(&server, conn_handle as *mut _, fbb.collapse(), msg_type, req_id);
                }
                Err(error) => submit_owner_error(
                    &server,
                    conn_handle as *mut _,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::Unavailable,
                    &error.to_string(),
                ),
            }
        });
    }

    // ── ListChunks ────────────────────────────────────────────────

    pub(super) fn handle_list(&self, req: ServerRequest, server: &Arc<RpcServer>, mut request: RequestGuard) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EListChunksResponse.0 as u16;
        let conn_handle_usize = req.conn_handle as usize;

        let handler = Arc::clone(&self.handler);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let Ok(fb_req) = flatbuffers::root::<FBListChunksRequest>(req.control()) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "invalid request flatbuffer",
                );
                return;
            };

            let start_after = fb_req.start_token().map(|id| ChunkId {
                high: id.high(),
                low: id.low(),
            });
            let max_keys = fb_req.max_keys();

            let result = handler.list_chunks(start_after.as_ref(), max_keys).await;
            match result {
                Ok(chunks) => {
                    request.mark_success();
                    let next_token = chunks.last().and_then(|c| c.id);
                    let has_next = next_token.is_some();
                    let ctrl = build_list_response(
                        req_id,
                        create_nano,
                        FBChunkdbRetCode::Success,
                        None,
                        0,
                        0,
                        &chunks,
                        next_token.as_ref(),
                        has_next,
                    );
                    submit_fb_response(
                        &server,
                        conn_handle_usize as *mut std::ffi::c_void,
                        ctrl,
                        msg_type,
                        req_id,
                    );
                }
                Err(e) => {
                    let (code, msg, rs, re) = map_error(&e);
                    let ctrl =
                        build_list_response(req_id, create_nano, code, Some(&msg), rs, re, &[], None, false);
                    submit_fb_response(
                        &server,
                        conn_handle_usize as *mut std::ffi::c_void,
                        ctrl,
                        msg_type,
                        req_id,
                    );
                }
            }
        });
    }
}

fn disposition_to_fb(disposition: SegmentOwnerDisposition) -> FBOwnerDisposition {
    match disposition {
        SegmentOwnerDisposition::Referenced => FBOwnerDisposition::Referenced,
        SegmentOwnerDisposition::TaskPending => FBOwnerDisposition::TaskPending,
        SegmentOwnerDisposition::Absent => FBOwnerDisposition::Absent,
    }
}

#[allow(clippy::too_many_arguments)]
fn submit_owner_error(
    server: &RpcServer,
    conn_handle: *mut std::ffi::c_void,
    req_id: u64,
    create_nano: u64,
    msg_type: u16,
    code: FBChunkdbRetCode,
    message: &str,
) {
    let mut fbb = FlatBufferBuilder::new();
    let error_msg = fbb.create_string(message);
    let response = FBQuerySegmentOwnerResponse::create(
        &mut fbb,
        &FBQuerySegmentOwnerResponseArgs {
            id: req_id,
            rpc_create_nano: create_nano,
            ret_code: code,
            error_msg: Some(error_msg),
            range_start: 0,
            range_end: 0,
            disposition: FBOwnerDisposition::Referenced,
        },
    );
    fbb.finish(response, None);
    submit_fb_response(server, conn_handle, fbb.collapse(), msg_type, req_id);
}
