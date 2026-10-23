// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    build_heartbeat_response, px_error_to_ret_code, record_group_context, spawn_request, submit_error,
    submit_fb_response, PxRpcService,
};
use crate::cluster::{
    local_replica::PxLocalReplica,
    replica::{HeartbeatRequestPayload, ReplicaHandler},
};
use crowdb_protocol::{
    fb::FBMsgType,
    kv_consensus_fb::{FBHeartbeatRequest, FBKvRetCode},
};
use crowdb_rpc_ffi::{RpcServer, ServerRequest};
use std::sync::Arc;

impl PxRpcService {
    // ── Heartbeat ────────────────────────────────────────────────

    pub(super) fn handle_heartbeat(&self, req: ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let conn_handle_usize = req.conn_handle as usize;
        let msg_type = FBMsgType::EHeartbeatResponse.0 as u16;
        let store = Arc::clone(&self.store);
        let server = Arc::clone(server);
        spawn_request(&self.rt, "heartbeat", store.store_id, async move {
            let Ok(fb_req) = flatbuffers::root::<FBHeartbeatRequest>(req.control()) else {
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
            let payload = HeartbeatRequestPayload {
                term: fb_req.term(),
                leader_id: fb_req.leader_id(),
                prev_log_slot: fb_req.prev_log_slot(),
                prev_log_term: fb_req.prev_log_term(),
                committed_safe_slot: fb_req.committed_safe_slot(),
                lease_grant_until_ms_mono: fb_req.lease_grant_until_ms_mono(),
                t_send_ms_mono: fb_req.t_send_ms_mono(),
                gc_before_slot: fb_req.gc_before_slot(),
                gc_membership_epoch: fb_req.gc_membership_epoch(),
            };
            let replica = group.local_replica();
            let reply = <PxLocalReplica as ReplicaHandler>::on_heartbeat(replica, payload, group_id).await;
            if reply.as_ref().is_ok_and(|reply| reply.success) {
                group.record_reclamation_heartbeat(&payload);
            }
            let ctrl = match reply {
                Ok(r) => build_heartbeat_response(
                    req_id,
                    create_nano,
                    FBKvRetCode::Success,
                    None,
                    1,
                    group_id,
                    r.term,
                    r.success,
                    r.contiguous_chosen,
                    r.last_chosen_term,
                    r.contiguous_applied,
                    r.highest_seen_slot,
                    r.durable_snapshot_slot,
                ),
                Err(e) => {
                    let (code, msg) = px_error_to_ret_code(&e);
                    build_heartbeat_response(
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
}
