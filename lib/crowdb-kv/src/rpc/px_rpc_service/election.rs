// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    build_pre_vote_response, build_request_vote_response, build_step_down_response, px_error_to_ret_code,
    record_group_context, spawn_request, submit_error, submit_fb_response, Arc, FBKvRetCode, FBMsgType,
    FBPreVoteRequest, FBRequestVoteRequest, FBStepDownRequest, PxLocalReplica, PxRpcService, ReplicaHandler,
    RpcServer, ServerRequest, StepDownRequestPayload, VoteRequestPayload,
};

impl PxRpcService {
    // ── PreVote ──────────────────────────────────────────────────

    pub(super) fn handle_pre_vote(&self, req: ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let conn_handle_usize = req.conn_handle as usize;
        let msg_type = FBMsgType::EPreVoteResponse.0 as u16;
        let store = Arc::clone(&self.store);
        let server = Arc::clone(server);
        spawn_request(&self.rt, "pre_vote", store.store_id, async move {
            let Ok(fb_req) = flatbuffers::root::<FBPreVoteRequest>(req.control()) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBKvRetCode::InvalidArgument,
                    "invalid request flatbuffer",
                );
                return;
            };
            let group_id = fb_req.group_id();
            record_group_context(&store, group_id);
            let Some(group) = store.get_group(group_id) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBKvRetCode::NotFound,
                    "px group not found",
                );
                return;
            };
            let payload = VoteRequestPayload {
                term: fb_req.term(),
                candidate_id: fb_req.candidate_id(),
                accepted_log_tip_slot: fb_req.accepted_log_tip_slot(),
                accepted_log_tip_term: fb_req.accepted_log_tip_term(),
            };
            let replica = group.local_replica();
            let reply = <PxLocalReplica as ReplicaHandler>::on_pre_vote(replica, payload, group_id).await;
            let ctrl = match reply {
                Ok(r) => build_pre_vote_response(
                    req_id,
                    create_nano,
                    FBKvRetCode::Success,
                    None,
                    1,
                    group_id,
                    r.term,
                    r.granted,
                    r.contiguous_chosen,
                    r.last_chosen_term,
                    r.highest_seen_slot,
                ),
                Err(e) => {
                    let (code, msg) = px_error_to_ret_code(&e);
                    build_pre_vote_response(
                        req_id,
                        create_nano,
                        code,
                        Some(&msg),
                        1,
                        group_id,
                        0,
                        false,
                        0,
                        0,
                        0,
                    )
                }
            };
            submit_fb_response(
                &server,
                conn_handle_usize as *mut std::ffi::c_void,
                ctrl,
                msg_type,
                req_id,
            );
            // req dropped here, frame released
        });
    }

    // ── RequestVote ──────────────────────────────────────────────

    pub(super) fn handle_request_vote(&self, req: ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let conn_handle_usize = req.conn_handle as usize;
        let msg_type = FBMsgType::ERequestVoteResponse.0 as u16;
        let store = Arc::clone(&self.store);
        let server = Arc::clone(server);
        spawn_request(&self.rt, "request_vote", store.store_id, async move {
            let Ok(fb_req) = flatbuffers::root::<FBRequestVoteRequest>(req.control()) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBKvRetCode::InvalidArgument,
                    "invalid request flatbuffer",
                );
                return;
            };
            let group_id = fb_req.group_id();
            record_group_context(&store, group_id);
            let Some(group) = store.get_group(group_id) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBKvRetCode::NotFound,
                    "px group not found",
                );
                return;
            };
            let payload = VoteRequestPayload {
                term: fb_req.term(),
                candidate_id: fb_req.candidate_id(),
                accepted_log_tip_slot: fb_req.accepted_log_tip_slot(),
                accepted_log_tip_term: fb_req.accepted_log_tip_term(),
            };
            let replica = group.local_replica();
            let reply = <PxLocalReplica as ReplicaHandler>::on_request_vote(replica, payload, group_id).await;
            let ctrl = match reply {
                Ok(r) => build_request_vote_response(
                    req_id,
                    create_nano,
                    FBKvRetCode::Success,
                    None,
                    1,
                    group_id,
                    r.term,
                    r.granted,
                    r.contiguous_chosen,
                    r.last_chosen_term,
                    r.highest_seen_slot,
                ),
                Err(e) => {
                    let (code, msg) = px_error_to_ret_code(&e);
                    build_request_vote_response(
                        req_id,
                        create_nano,
                        code,
                        Some(&msg),
                        1,
                        group_id,
                        0,
                        false,
                        0,
                        0,
                        0,
                    )
                }
            };
            submit_fb_response(
                &server,
                conn_handle_usize as *mut std::ffi::c_void,
                ctrl,
                msg_type,
                req_id,
            );
            // req dropped here, frame released
        });
    }

    // ── Heartbeat ────────────────────────────────────────────────

    pub(super) fn handle_step_down(&self, req: ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let conn_handle_usize = req.conn_handle as usize;
        let msg_type = FBMsgType::EStepDownResponse.0 as u16;
        let store = Arc::clone(&self.store);
        let server = Arc::clone(server);
        spawn_request(&self.rt, "step_down", store.store_id, async move {
            let Ok(fb_req) = flatbuffers::root::<FBStepDownRequest>(req.control()) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBKvRetCode::InvalidArgument,
                    "invalid request flatbuffer",
                );
                return;
            };
            let group_id = fb_req.group_id();
            record_group_context(&store, group_id);
            let Some(group) = store.get_group(group_id) else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBKvRetCode::NotFound,
                    "px group not found",
                );
                return;
            };
            let reason = fb_req.reason().unwrap_or("").to_string();
            let payload = StepDownRequestPayload {
                term: fb_req.term(),
                target_leader_id: fb_req.target_leader_id(),
                reason,
            };
            let replica = group.local_replica();
            let reply = <PxLocalReplica as ReplicaHandler>::on_step_down(replica, &payload, group_id).await;
            let ctrl = match reply {
                Ok(r) => build_step_down_response(
                    req_id,
                    create_nano,
                    FBKvRetCode::Success,
                    None,
                    1,
                    group_id,
                    r.accepted,
                    r.current_term,
                    r.current_leader_id,
                ),
                Err(e) => {
                    let (code, msg) = px_error_to_ret_code(&e);
                    build_step_down_response(req_id, create_nano, code, Some(&msg), 1, group_id, false, 0, 0)
                }
            };
            submit_fb_response(
                &server,
                conn_handle_usize as *mut std::ffi::c_void,
                ctrl,
                msg_type,
                req_id,
            );
            // req dropped here, frame released
        });
    }
}
