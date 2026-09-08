use super::{
    build_delete_range_response, build_discard_replacement_response, map_error, parse_fb_chunk_strip,
    parse_fb_segment, parse_fb_segments, proto_chunk_type, proto_strip_type, submit_append_result,
    submit_chunk_result, submit_error, submit_fb_response, submit_segment_result, Arc, ChunkId,
    ChunkdbRpcService, FBAdvanceChunkWriteRequest, FBAllocateChunkRequest,
    FBAllocateReplacementSegmentRequest, FBAppendChunkRequest, FBChunkdbRetCode, FBDeleteChunkRangeRequest,
    FBDeleteChunkRequest, FBDiscardReplacementSegmentRequest, FBMsgType, FBReplaceChunkStripRangeRequest,
    FBSealChunkRequest, FBUpdateChunkStripRequest, RequestGuard, RpcServer, ServerRequest,
};

impl ChunkdbRpcService {
    pub(super) fn handle_discard_replacement(
        &self,
        req: ServerRequest,
        server: &Arc<RpcServer>,
        mut request: RequestGuard,
    ) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EDiscardReplacementSegmentResponse.0 as u16;
        let conn_handle = req.conn_handle as usize;
        let handler = Arc::clone(&self.handler);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let Ok(request_fb) = flatbuffers::root::<FBDiscardReplacementSegmentRequest>(req.control())
            else {
                submit_error(
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
            let Some(chunk_id) = request_fb.chunk_id().map(|id| ChunkId {
                high: id.high(),
                low: id.low(),
            }) else {
                submit_error(
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
            let Some(segment) = request_fb.segment().map(parse_fb_segment) else {
                submit_error(
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
            let result = handler.discard_replacement_segment(&chunk_id, segment).await;
            let (code, message, range_start, range_end) = match result {
                Ok(()) => {
                    request.mark_success();
                    (FBChunkdbRetCode::Success, None, 0, 0)
                }
                Err(error) => {
                    let (code, message, start, end) = map_error(&error);
                    (code, Some(message), start, end)
                }
            };
            let response = build_discard_replacement_response(
                req_id,
                create_nano,
                code,
                message.as_deref(),
                range_start,
                range_end,
            );
            submit_fb_response(&server, conn_handle as *mut _, response, msg_type, req_id);
        });
    }

    pub(super) fn handle_allocate_replacement(
        &self,
        req: ServerRequest,
        server: &Arc<RpcServer>,
        mut request: RequestGuard,
    ) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EAllocateReplacementSegmentResponse.0 as u16;
        let conn_handle = req.conn_handle as usize;
        let handler = Arc::clone(&self.handler);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let Ok(request_fb) = flatbuffers::root::<FBAllocateReplacementSegmentRequest>(req.control())
            else {
                submit_error(
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
            let Some(chunk_id) = request_fb.chunk_id().map(|id| ChunkId {
                high: id.high(),
                low: id.low(),
            }) else {
                submit_error(
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
            let Some(old_segment) = request_fb.old_segment().map(parse_fb_segment) else {
                submit_error(
                    &server,
                    conn_handle as *mut _,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "missing old_segment",
                );
                return;
            };
            let surviving = parse_fb_segments(request_fb.surviving_segments());
            let excluded = request_fb
                .exclude_disk_ids()
                .map(|values| {
                    values
                        .iter()
                        .map(|id| super::DiskId {
                            high: id.high(),
                            low: id.low(),
                        })
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let result = handler
                .allocate_replacement_segment(&chunk_id, &old_segment, &surviving, &excluded)
                .await;
            if result.is_ok() {
                request.mark_success();
            }
            submit_segment_result(
                &server,
                conn_handle as *mut _,
                req_id,
                create_nano,
                msg_type,
                result,
            );
        });
    }

    pub(super) fn handle_replace_range(
        &self,
        req: ServerRequest,
        server: &Arc<RpcServer>,
        mut request: RequestGuard,
    ) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EReplaceChunkStripRangeResponse.0 as u16;
        let conn_handle = req.conn_handle as usize;
        let handler = Arc::clone(&self.handler);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let Ok(request_fb) = flatbuffers::root::<FBReplaceChunkStripRangeRequest>(req.control()) else {
                submit_invalid_request(
                    &server,
                    conn_handle,
                    req_id,
                    create_nano,
                    msg_type,
                    "invalid request flatbuffer",
                );
                return;
            };
            let Some(chunk_id) = request_fb.chunk_id().map(|id| ChunkId {
                high: id.high(),
                low: id.low(),
            }) else {
                submit_invalid_request(
                    &server,
                    conn_handle,
                    req_id,
                    create_nano,
                    msg_type,
                    "missing chunk_id",
                );
                return;
            };
            let Some(operation_id) = request_fb.operation_id().map(|id| ChunkId {
                high: id.high(),
                low: id.low(),
            }) else {
                submit_invalid_request(
                    &server,
                    conn_handle,
                    req_id,
                    create_nano,
                    msg_type,
                    "missing operation_id",
                );
                return;
            };
            let old_values = request_fb.old_strips();
            let replacement_values = request_fb.replacement_strips();
            let old = old_values
                .map(|values| {
                    values
                        .iter()
                        .filter_map(|strip| parse_fb_chunk_strip(&strip))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            let replacement = replacement_values
                .map(|values| {
                    values
                        .iter()
                        .filter_map(|strip| parse_fb_chunk_strip(&strip))
                        .collect::<Vec<_>>()
                })
                .unwrap_or_default();
            if old_values.is_some_and(|values| values.len() != old.len())
                || replacement_values.is_some_and(|values| values.len() != replacement.len())
            {
                submit_invalid_request(
                    &server,
                    conn_handle,
                    req_id,
                    create_nano,
                    msg_type,
                    "invalid strip in replacement range",
                );
                return;
            }
            let result = handler
                .replace_chunk_strip_range(
                    &chunk_id,
                    request_fb.expected_modify_ts(),
                    request_fb.start_index(),
                    &old,
                    &replacement,
                    operation_id,
                )
                .await;
            if result.is_ok() {
                request.mark_success();
            }
            submit_chunk_result(
                &server,
                conn_handle as *mut _,
                req_id,
                create_nano,
                msg_type,
                result,
            );
        });
    }

    // ── AllocateChunk ─────────────────────────────────────────────

    pub(super) fn handle_allocate(
        &self,
        req: ServerRequest,
        server: &Arc<RpcServer>,
        mut request: RequestGuard,
    ) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EAllocateChunkResponse.0 as u16;
        let conn_handle_usize = req.conn_handle as usize;

        let handler = Arc::clone(&self.handler);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            // Parse the flatbuffer inside the async task — zero-copy from
            // the owned Frame (released when `req` drops at block end).
            let Ok(fb_req) = flatbuffers::root::<FBAllocateChunkRequest>(req.control()) else {
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

            let chunk_id = fb_req.chunk_id().map(|id| ChunkId {
                high: id.high(),
                low: id.low(),
            });
            let Some(strip_type) = proto_strip_type(fb_req.strip_type()) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "invalid strip_type",
                );
                return;
            };
            let Some(chunk_type) = proto_chunk_type(fb_req.chunk_type()) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "invalid chunk_type",
                );
                return;
            };
            let write_granularity = fb_req.write_granularity();
            let strip_count = fb_req.strip_count();
            let data_num = fb_req.data_num();
            let code_num = fb_req.code_num();
            let copy_count = fb_req.copy_count();

            let result = handler
                .allocate_chunk(
                    chunk_id,
                    write_granularity,
                    strip_count,
                    strip_type,
                    data_num,
                    code_num,
                    copy_count,
                    chunk_type,
                    fb_req.writer_epoch(),
                    fb_req.writer_lease_ms(),
                )
                .await;
            if result.is_ok() {
                request.mark_success();
            }
            submit_chunk_result(
                &server,
                conn_handle_usize as *mut std::ffi::c_void,
                req_id,
                create_nano,
                msg_type,
                result,
            );
        });
    }

    // ── AdvanceChunkWrite ────────────────────────────────────────

    pub(super) fn handle_advance_write(
        &self,
        req: ServerRequest,
        server: &Arc<RpcServer>,
        mut request: RequestGuard,
    ) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EAdvanceChunkWriteResponse.0 as u16;
        let conn_handle_usize = req.conn_handle as usize;
        let handler = Arc::clone(&self.handler);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let Ok(fb_req) = flatbuffers::root::<FBAdvanceChunkWriteRequest>(req.control()) else {
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
            let closed_sequence =
                (fb_req.closed_strip_sequence() != u32::MAX).then(|| fb_req.closed_strip_sequence());
            let result = handler
                .advance_chunk_write(
                    &chunk_id,
                    fb_req.writer_epoch(),
                    fb_req.expected_modify_ts(),
                    fb_req.acknowledged_cursor(),
                    closed_sequence,
                    fb_req.writer_lease_ms(),
                )
                .await;
            if result.is_ok() {
                request.mark_success();
            }
            submit_chunk_result(
                &server,
                conn_handle_usize as *mut std::ffi::c_void,
                req_id,
                create_nano,
                msg_type,
                result,
            );
        });
    }

    // ── AppendChunk ───────────────────────────────────────────────

    pub(super) fn handle_append(
        &self,
        req: ServerRequest,
        server: &Arc<RpcServer>,
        mut request: RequestGuard,
    ) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EAppendChunkResponse.0 as u16;
        let conn_handle_usize = req.conn_handle as usize;

        let handler = Arc::clone(&self.handler);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let Ok(fb_req) = flatbuffers::root::<FBAppendChunkRequest>(req.control()) else {
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
            let Some(strip_type) = proto_strip_type(fb_req.strip_type()) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "invalid strip_type",
                );
                return;
            };
            let strip_count = fb_req.strip_count();
            let data_num = fb_req.data_num();
            let code_num = fb_req.code_num();
            let copy_count = fb_req.copy_count();
            let strip_size = fb_req.strip_size();

            let result = handler
                .append_chunk(
                    &chunk_id,
                    fb_req.modify_ts(),
                    strip_count,
                    strip_type,
                    data_num,
                    code_num,
                    copy_count,
                    strip_size,
                )
                .await;
            if result.is_ok() {
                request.mark_success();
            }
            submit_append_result(
                &server,
                conn_handle_usize as *mut std::ffi::c_void,
                req_id,
                create_nano,
                msg_type,
                result,
            );
        });
    }

    // ── SealChunk ─────────────────────────────────────────────────

    pub(super) fn handle_seal(&self, req: ServerRequest, server: &Arc<RpcServer>, mut request: RequestGuard) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::ESealChunkResponse.0 as u16;
        let conn_handle_usize = req.conn_handle as usize;

        let handler = Arc::clone(&self.handler);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let Ok(fb_req) = flatbuffers::root::<FBSealChunkRequest>(req.control()) else {
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
            let seal_length = fb_req.seal_length();

            let result = handler.seal_chunk(&chunk_id, seal_length).await;
            if result.is_ok() {
                request.mark_success();
            }
            submit_chunk_result(
                &server,
                conn_handle_usize as *mut std::ffi::c_void,
                req_id,
                create_nano,
                msg_type,
                result,
            );
        });
    }

    // ── DeleteChunk ───────────────────────────────────────────────

    pub(super) fn handle_delete(
        &self,
        req: ServerRequest,
        server: &Arc<RpcServer>,
        mut request: RequestGuard,
    ) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EDeleteChunkResponse.0 as u16;
        let conn_handle_usize = req.conn_handle as usize;

        let handler = Arc::clone(&self.handler);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let Ok(fb_req) = flatbuffers::root::<FBDeleteChunkRequest>(req.control()) else {
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

            let result = handler.delete_chunk(&chunk_id).await;
            if result.is_ok() {
                request.mark_success();
            }
            submit_chunk_result(
                &server,
                conn_handle_usize as *mut std::ffi::c_void,
                req_id,
                create_nano,
                msg_type,
                result,
            );
        });
    }

    // ── DeleteChunkRange ──────────────────────────────────────────

    pub(super) fn handle_delete_range(
        &self,
        req: ServerRequest,
        server: &Arc<RpcServer>,
        mut request: RequestGuard,
    ) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EDeleteChunkRangeResponse.0 as u16;
        let conn_handle_usize = req.conn_handle as usize;

        let handler = Arc::clone(&self.handler);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let Ok(fb_req) = flatbuffers::root::<FBDeleteChunkRangeRequest>(req.control()) else {
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
            let offset = fb_req.chunk_offset();
            let size = fb_req.chunk_size();

            let result = handler.delete_chunk_range(&chunk_id, offset, size).await;
            match result {
                Ok(()) => {
                    request.mark_success();
                    let ctrl = build_delete_range_response(
                        req_id,
                        create_nano,
                        FBChunkdbRetCode::Success,
                        None,
                        0,
                        0,
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
                    let ctrl = build_delete_range_response(req_id, create_nano, code, Some(&msg), rs, re);
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

    // ── UpdateChunkStrip ──────────────────────────────────────────

    pub(super) fn handle_update_strip(
        &self,
        req: ServerRequest,
        server: &Arc<RpcServer>,
        mut request: RequestGuard,
    ) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let msg_type = FBMsgType::EUpdateChunkStripResponse.0 as u16;
        let conn_handle_usize = req.conn_handle as usize;

        let handler = Arc::clone(&self.handler);
        let server = Arc::clone(server);
        self.rt.spawn(async move {
            let Ok(fb_req) = flatbuffers::root::<FBUpdateChunkStripRequest>(req.control()) else {
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
            let strip_index = fb_req.strip_index();
            let Some(fb_strip) = fb_req.strip() else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "missing strip",
                );
                return;
            };
            let Some(strip) = parse_fb_chunk_strip(&fb_strip) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBChunkdbRetCode::InvalidArgument,
                    "invalid strip body",
                );
                return;
            };

            let result = handler.update_chunk_strip(&chunk_id, strip_index, strip).await;
            if result.is_ok() {
                request.mark_success();
            }
            submit_chunk_result(
                &server,
                conn_handle_usize as *mut std::ffi::c_void,
                req_id,
                create_nano,
                msg_type,
                result,
            );
        });
    }
}

fn submit_invalid_request(
    server: &RpcServer,
    conn_handle: usize,
    req_id: u64,
    create_nano: u64,
    msg_type: u16,
    message: &str,
) {
    submit_error(
        server,
        conn_handle as *mut _,
        req_id,
        create_nano,
        msg_type,
        FBChunkdbRetCode::InvalidArgument,
        message,
    );
}
