// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    build_accepted_response, px_error_to_ret_code, record_group_context, spawn_request, submit_error,
    submit_fb_response, warn, Arc, FBAcceptRequest, FBKvRetCode, FBMsgType, PxAcceptReply, PxBallot,
    PxLocalReplica, PxLogEntry, PxRpcService, ReplicaHandler, RpcServer, ServerRequest,
};

impl PxRpcService {
    // ── Accept ───────────────────────────────────────────────────

    #[allow(clippy::too_many_lines)]
    pub(super) fn handle_accept(&self, req: ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let conn_handle_usize = req.conn_handle as usize;
        let msg_type = FBMsgType::EAcceptedResponse.0 as u16;
        let store = Arc::clone(&self.store);
        let server = Arc::clone(server);
        spawn_request(&self.rt, "accept", store.store_id, async move {
            let Ok(fb_req) = flatbuffers::root::<FBAcceptRequest>(req.control()) else {
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
            let slot = fb_req.slot();
            let round = fb_req.round();
            let leader_id = fb_req.leader_id();
            let term = fb_req.term();
            let membership_epoch = fb_req.membership_epoch();

            let Some(fb_value) = fb_req.value() else {
                submit_error(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    req_id,
                    create_nano,
                    msg_type,
                    FBKvRetCode::InvalidArgument,
                    "missing value",
                );
                return;
            };
            let payload_bytes: Vec<u8> = fb_value.payload().map(|v| v.bytes().to_vec()).unwrap_or_default();
            let entry = PxLogEntry {
                slot,
                ballot: PxBallot { round, leader_id },
                term,
                payload: payload_bytes.into(),
            };

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
            let responder_epoch = group.membership_epoch();

            if membership_epoch != responder_epoch {
                let converged_epoch = group.adopt_membership_epoch(membership_epoch);
                warn!(
                    group_id,
                    slot,
                    round,
                    leader_id,
                    proposer_epoch = membership_epoch,
                    responder_epoch,
                    converged_epoch,
                    "accept rejected by membership-epoch fence; adopting higher epoch from proposer"
                );
                let term = group.local_replica().current_term_snapshot();
                let ctrl = build_accepted_response(
                    req_id,
                    create_nano,
                    FBKvRetCode::Success,
                    None,
                    1,
                    slot,
                    round,
                    leader_id,
                    false,
                    0,
                    0,
                    term,
                    false,
                    responder_epoch,
                    true,
                );
                submit_fb_response(
                    &server,
                    conn_handle_usize as *mut std::ffi::c_void,
                    ctrl,
                    msg_type,
                    req_id,
                );
                return;
            }

            let replica = group.local_replica();
            let reply = <PxLocalReplica as ReplicaHandler>::on_accept(replica, &entry, group_id).await;
            let (rejected, rejected_round, rejected_leader_id, term_stale, reply_term) = match reply {
                Ok(PxAcceptReply::Accepted { .. }) => (false, 0, 0, false, replica.current_term_snapshot()),
                Ok(PxAcceptReply::Rejected { current_promised, .. }) => {
                    warn!(
                        group_id,
                        slot,
                        current_round = current_promised.round,
                        current_leader_id = current_promised.leader_id,
                        "accept rejected; next step: proposer should run prepare with a higher ballot"
                    );
                    (
                        true,
                        current_promised.round,
                        current_promised.leader_id,
                        false,
                        replica.current_term_snapshot(),
                    )
                }
                Ok(PxAcceptReply::TermStale { new_term, .. }) => {
                    warn!(
                        group_id,
                        slot, new_term, "accept rejected by term fence; proposer should step down"
                    );
                    (false, 0, 0, true, new_term)
                }
                Ok(PxAcceptReply::EpochMismatch { .. }) => {
                    unreachable!("on_accept does not produce EpochMismatch")
                }
                Err(e) => {
                    let (code, msg) = px_error_to_ret_code(&e);
                    let ctrl = build_accepted_response(
                        req_id,
                        create_nano,
                        code,
                        Some(&msg),
                        1,
                        slot,
                        round,
                        leader_id,
                        false,
                        0,
                        0,
                        replica.current_term_snapshot(),
                        false,
                        responder_epoch,
                        false,
                    );
                    submit_fb_response(
                        &server,
                        conn_handle_usize as *mut std::ffi::c_void,
                        ctrl,
                        msg_type,
                        req_id,
                    );
                    return;
                }
            };
            let ctrl = build_accepted_response(
                req_id,
                create_nano,
                FBKvRetCode::Success,
                None,
                1,
                slot,
                round,
                leader_id,
                rejected,
                rejected_round,
                rejected_leader_id,
                reply_term,
                term_stale,
                responder_epoch,
                false,
            );
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
