// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    build_promise_response, finish_promise_response, px_error_to_ret_code, record_group_context,
    spawn_request, submit_error, submit_fb_response, warn, Arc, FBAcceptedValue, FBAcceptedValueArgs,
    FBKvRetCode, FBMsgType, FBPrepareRequest, FlatBufferBuilder, PxBallot, PxLocalReplica, PxPrepareReply,
    PxRpcService, ReplicaHandler, RpcServer, ServerRequest,
};

impl PxRpcService {
    // ── Prepare ──────────────────────────────────────────────────

    #[allow(clippy::too_many_lines)]
    pub(super) fn handle_prepare(&self, req: ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let conn_handle_usize = req.conn_handle as usize;
        let msg_type = FBMsgType::EPromiseResponse.0 as u16;
        let store = Arc::clone(&self.store);
        let server = Arc::clone(server);
        spawn_request(&self.rt, "prepare", store.store_id, async move {
            let Ok(fb_req) = flatbuffers::root::<FBPrepareRequest>(req.control()) else {
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

            // Membership-epoch fence (same as px_service.rs L131-147).
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
                    "prepare rejected by membership-epoch fence; adopting higher epoch from proposer"
                );
                let term = group.local_replica().current_term_snapshot();
                let ctrl = build_promise_response(
                    req_id,
                    create_nano,
                    FBKvRetCode::Success,
                    None,
                    1,
                    slot,
                    round,
                    leader_id,
                    None,
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

            let ballot = PxBallot { round, leader_id };
            let replica = group.local_replica();
            let reply =
                <PxLocalReplica as ReplicaHandler>::on_prepare(replica, slot, ballot, term, group_id).await;
            let ctrl = match reply {
                Ok(PxPrepareReply::Promised { slot, accepted }) => {
                    let mut builder = FlatBufferBuilder::new();
                    let prev_accepted_off = accepted.as_ref().map(|entry| {
                        let payload = builder.create_vector(&entry.payload);
                        FBAcceptedValue::create(
                            &mut builder,
                            &FBAcceptedValueArgs {
                                slot: entry.slot,
                                round: entry.ballot.round,
                                leader_id: entry.ballot.leader_id,
                                term: entry.term,
                                payload: Some(payload),
                            },
                        )
                    });
                    let term = replica.current_term_snapshot();
                    finish_promise_response(
                        builder,
                        req_id,
                        create_nano,
                        FBKvRetCode::Success,
                        None,
                        1,
                        slot,
                        round,
                        leader_id,
                        prev_accepted_off,
                        false,
                        0,
                        0,
                        term,
                        false,
                        responder_epoch,
                        false,
                    )
                }
                Ok(PxPrepareReply::TermStale { slot, new_term }) => build_promise_response(
                    req_id,
                    create_nano,
                    FBKvRetCode::Success,
                    None,
                    1,
                    slot,
                    round,
                    leader_id,
                    None,
                    false,
                    0,
                    0,
                    new_term,
                    true,
                    responder_epoch,
                    false,
                ),
                Ok(PxPrepareReply::Rejected {
                    slot,
                    current_promised,
                }) => {
                    warn!(
                        group_id,
                        slot,
                        current_round = current_promised.round,
                        current_leader_id = current_promised.leader_id,
                        "prepare rejected; next step: proposer should retry with a higher ballot"
                    );
                    let term = replica.current_term_snapshot();
                    build_promise_response(
                        req_id,
                        create_nano,
                        FBKvRetCode::Success,
                        None,
                        1,
                        slot,
                        round,
                        leader_id,
                        None,
                        true,
                        current_promised.round,
                        current_promised.leader_id,
                        term,
                        false,
                        responder_epoch,
                        false,
                    )
                }
                Ok(PxPrepareReply::EpochMismatch { .. }) => {
                    unreachable!("on_prepare does not produce EpochMismatch")
                }
                Err(e) => {
                    let (code, msg) = px_error_to_ret_code(&e);
                    build_promise_response(
                        req_id,
                        create_nano,
                        code,
                        Some(&msg),
                        1,
                        slot,
                        round,
                        leader_id,
                        None,
                        false,
                        0,
                        0,
                        replica.current_term_snapshot(),
                        false,
                        responder_epoch,
                        false,
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
