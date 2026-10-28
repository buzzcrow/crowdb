// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    build_fetch_gap_response, debug, record_group_context, request_span, spawn_group_request, submit_error,
    submit_fb_response, Arc, FBBatchChosenNotification, FBChosenNotification, FBFetchGapRequest, FBKvRetCode,
    FBMsgType, PxBallot, PxRpcService, RpcServer, ServerRequest,
};

impl PxRpcService {
    // ── ChosenNotification (fire-and-forget) ─────────────────────

    #[allow(clippy::needless_pass_by_value, reason = "make_handler uniform signature")]
    pub(super) fn handle_chosen_notice(&self, req: ServerRequest, _server: &Arc<RpcServer>) {
        let Ok(fb_req) = flatbuffers::root::<FBChosenNotification>(req.control()) else {
            debug!("chosen notice: invalid flatbuffer");
            return;
        };
        let group_id = fb_req.group_id();
        let slot = fb_req.slot();
        let term = fb_req.term();
        let leader_id = fb_req.leader_id();
        let ballot_round = fb_req.ballot_round();

        let Some(group) = self.store.get_group(group_id) else {
            debug!(slot, term, "chosen notice dropped (group not found)");
            return;
        };
        let chosen_ballot = PxBallot {
            round: ballot_round,
            leader_id,
        };
        spawn_group_request(
            &self.rt,
            "chosen_notice",
            self.store.store_id,
            group_id,
            group.local_replica().id,
            async move {
                let replica = group.local_replica();
                let accepted = replica.accepted_at(slot).await;
                let ballot_matches = accepted.as_ref().is_some_and(|e| e.ballot == chosen_ballot);
                if ballot_matches {
                    replica.learner.update_chosen_frontier(slot, term);
                    replica.wake_apply_loop();
                    debug!(
                        slot,
                        term, leader_id, ballot_round, "chosen notification applied (ballot match)"
                    );
                } else {
                    replica.note_chosen(slot, term);
                    replica.record_gap(slot);
                    if let Some(ref entry) = accepted {
                        replica.incr_chosen_notice_stale();
                        debug!(
                            slot,
                            term,
                            leader_id,
                            chosen_ballot_round = ballot_round,
                            accepted_ballot_round = entry.ballot.round,
                            accepted_ballot_leader = entry.ballot.leader_id,
                            "chosen notification stale ballot (gap recorded)"
                        );
                    } else {
                        replica.incr_chosen_notice_missing();
                        debug!(
                            slot,
                            term, leader_id, "chosen notification missing value (gap recorded)"
                        );
                    }
                }
            },
        );
    }

    // ── BatchChosenNotification (fire-and-forget) ────────────────

    #[allow(clippy::needless_pass_by_value, reason = "make_handler uniform signature")]
    pub(super) fn handle_batch_chosen(&self, req: ServerRequest, _server: &Arc<RpcServer>) {
        let Ok(fb_req) = flatbuffers::root::<FBBatchChosenNotification>(req.control()) else {
            debug!("batch chosen: invalid flatbuffer");
            return;
        };
        let group_id = fb_req.group_id();
        let start_slot = fb_req.start_slot();
        let end_slot = fb_req.end_slot();
        let term = fb_req.term();
        let leader_id = fb_req.leader_id();
        let ballot_round = fb_req.ballot_round();

        let Some(group) = self.store.get_group(group_id) else {
            debug!("batch chosen dropped (group not found)");
            return;
        };
        let chosen_ballot = PxBallot {
            round: ballot_round,
            leader_id,
        };
        spawn_group_request(
            &self.rt,
            "batch_chosen",
            self.store.store_id,
            group_id,
            group.local_replica().id,
            async move {
                let replica = group.local_replica();
                let mut advanced_count = 0u64;
                for slot in start_slot..=end_slot {
                    let accepted = replica.accepted_at(slot).await;
                    if accepted.as_ref().is_some_and(|e| e.ballot == chosen_ballot) {
                        replica.learner.update_chosen_frontier(slot, term);
                        advanced_count += 1;
                    } else {
                        replica.note_chosen(slot, term);
                        replica.record_gap(slot);
                    }
                }
                replica.wake_apply_loop();
                debug!(
                    group_id,
                    start_slot,
                    end_slot,
                    term,
                    leader_id,
                    ballot_round,
                    advanced_count,
                    "batch chosen notification applied"
                );
            },
        );
    }

    // ── FetchGap ─────────────────────────────────────────────────

    #[allow(clippy::needless_pass_by_value, reason = "make_handler uniform signature")]
    pub(super) fn handle_fetch_gap(&self, req: ServerRequest, server: &Arc<RpcServer>) {
        let span = request_span("fetch_gap", self.store.store_id);
        let _entered = span.enter();
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let conn_handle = req.conn_handle;
        let msg_type = FBMsgType::EFetchGapResponse.0 as u16;

        let Ok(fb_req) = flatbuffers::root::<FBFetchGapRequest>(req.control()) else {
            submit_error(
                server,
                conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBKvRetCode::InvalidArgument,
                "invalid request flatbuffer",
            );
            return;
        };
        let group_id = fb_req.group_id();
        record_group_context(&self.store, group_id);
        let slot = fb_req.slot();

        let Some(group) = self.store.get_group(group_id) else {
            submit_error(
                server,
                conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBKvRetCode::NotFound,
                "px group not found",
            );
            return;
        };
        if let Some(resp) = group.handle_fetch_gap(slot) {
            let payload_vec = resp.payload.clone();
            let ctrl = build_fetch_gap_response(
                req_id,
                create_nano,
                FBKvRetCode::Success,
                None,
                1,
                group_id,
                resp.slot,
                resp.term,
                resp.ballot_round,
                resp.leader_id,
                &payload_vec,
            );
            submit_fb_response(server, conn_handle, ctrl, msg_type, req_id);
        } else {
            debug!(slot, "fetch_gap no value (not yet chosen)");
            submit_error(
                server,
                conn_handle,
                req_id,
                create_nano,
                msg_type,
                FBKvRetCode::NotFound,
                "slot not yet chosen",
            );
        }
        // req dropped here, frame released
    }
}
