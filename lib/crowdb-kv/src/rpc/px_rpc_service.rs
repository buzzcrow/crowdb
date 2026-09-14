// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

// `submit_response` takes a raw `conn_handle` from the FFI dispatch
// callback — the unsafe is inherent to the FFI boundary (the pointer
// is a valid `Connection*` for the duration of the callback, verified
// by the C++ transport). Confined to `submit_response` calls.
#![allow(unsafe_code)]
#![allow(dead_code)] // Wired in Phase 7 (server wiring)

//! crowdb-rpc handler set for the KV consensus service (R32 migration).
//!
//! Each handler dispatches by `msg_type` to the existing consensus
//! logic — the same logic bodies as the former `PxReplicaService` in
//! `px_service.rs`. The response is a flatbuffer frame built per
//! `design-crowdb-rpc.md` §6 (build → finish → attach) and submitted via
//! `RpcServer::submit_response`.
//!
//! Handlers run on the C++ I/O worker thread. Synchronous paths
//! (validation, epoch fence checks) run inline; async paths (Paxos
//! acceptor calls) spawn a tokio task via the captured `Handle` and
//! submit the response from the task. Each handler closure captures an
//! `Arc<RpcServer>` so it can submit responses from either the dispatch
//! thread (sync error path) or the spawned task (async success path).

use std::future::Future;
use std::sync::Arc;

use crowdb_protocol::fb::FBMsgType;
use crowdb_protocol::kv_consensus_fb::{
    FBAcceptRequest, FBAcceptedResponse, FBAcceptedResponseArgs, FBAcceptedValue, FBAcceptedValueArgs,
    FBBatchChosenNotification, FBChosenNotification, FBFetchGapRequest, FBFetchGapResponse,
    FBFetchGapResponseArgs, FBHeartbeatRequest, FBHeartbeatResponse, FBHeartbeatResponseArgs, FBKvRetCode,
    FBPreVoteRequest, FBPreVoteResponse, FBPreVoteResponseArgs, FBPrepareRequest, FBPromiseResponse,
    FBPromiseResponseArgs, FBRequestVoteRequest, FBRequestVoteResponse, FBRequestVoteResponseArgs,
    FBSnapshotAbortRequest, FBSnapshotAbortResponse, FBSnapshotAbortResponseArgs, FBSnapshotBeginRequest,
    FBSnapshotBeginResponse, FBSnapshotBeginResponseArgs, FBSnapshotFinishRequest, FBSnapshotFinishResponse,
    FBSnapshotFinishResponseArgs, FBSnapshotReadRequest, FBSnapshotReadResponse, FBSnapshotReadResponseArgs,
    FBStepDownRequest, FBStepDownResponse, FBStepDownResponseArgs,
};
use crowdb_rpc_ffi::{Buffer, RpcServer, ServerRequest};
use flatbuffers::FlatBufferBuilder;
use tokio::runtime::Handle;
use tracing::{debug, field, info_span, warn, Instrument, Span};

use crate::cluster::local_replica::PxLocalReplica;
use crate::cluster::px_kv_store::PxKvStore;
use crate::cluster::replica::{
    HeartbeatRequestPayload, PxReplicaError, ReplicaHandler, StepDownRequestPayload, VoteRequestPayload,
};
use crate::paxos::roles::{PxAcceptReply, PxBallot, PxLogEntry, PxPrepareReply};
use crate::rpc::snapshot_registry::{
    SnapshotRegistration, SnapshotRegistry, SnapshotSessionError, SnapshotSessionId, SnapshotSourceMetrics,
};

/// crowdb-rpc handler set for the KV consensus service. Holds the same
/// dependencies as the former `PxReplicaService` plus a tokio `Handle`
/// for spawning async work from the C++ I/O thread callback.
pub struct PxRpcService {
    store: Arc<PxKvStore>,
    rt: Handle,
    snapshots: Arc<SnapshotRegistry>,
}

fn spawn_request<F>(rt: &Handle, operation: &'static str, store_id: u64, future: F)
where
    F: Future<Output = ()> + Send + 'static,
{
    rt.spawn(future.instrument(request_span(operation, store_id)));
}

fn request_span(operation: &'static str, store_id: u64) -> Span {
    info_span!(
        "rpc_request",
        operation,
        s = store_id,
        g = field::Empty,
        replica = field::Empty
    )
}

fn spawn_group_request<F>(
    rt: &Handle,
    operation: &'static str,
    store_id: u64,
    group_id: u64,
    replica: u64,
    future: F,
) where
    F: Future<Output = ()> + Send + 'static,
{
    let span = info_span!("rpc_request", operation, s = store_id, g = group_id, replica);
    rt.spawn(future.instrument(span));
}

fn record_group_context(store: &PxKvStore, group_id: u64) {
    let span = Span::current();
    span.record("g", group_id);
    if let Some(group) = store.get_group(group_id) {
        span.record("replica", group.local_replica().id);
    }
}

impl PxRpcService {
    pub(crate) fn new(store: Arc<PxKvStore>, rt: Handle) -> Self {
        let snapshot_metrics = store.metrics_registry.as_ref().map(|registry| {
            let mut registry = registry.lock().expect("metrics registry poisoned");
            SnapshotSourceMetrics::register(&mut registry, store.store_id)
        });
        let snapshots = SnapshotRegistry::new(
            store.snapshot_source_sessions,
            std::time::Duration::from_millis(store.snapshot_session_lease_ms),
            snapshot_metrics,
        );
        let weak = Arc::downgrade(&snapshots);
        let reap_interval = std::time::Duration::from_millis((store.snapshot_session_lease_ms / 2).max(1));
        rt.spawn(async move {
            let mut ticker = tokio::time::interval(reap_interval);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                let Some(registry) = weak.upgrade() else {
                    break;
                };
                registry.reap_expired();
            }
        });
        Self { store, rt, snapshots }
    }

    /// Register all consensus request handlers into the `RpcServer`.
    pub(crate) fn register_handlers(self: &Arc<Self>, server: &Arc<RpcServer>) {
        server.register_handler(
            FBMsgType::EPrepareRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_prepare),
        );
        server.register_handler(
            FBMsgType::EAcceptRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_accept),
        );
        server.register_handler(
            FBMsgType::EPreVoteRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_pre_vote),
        );
        server.register_handler(
            FBMsgType::ERequestVoteRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_request_vote),
        );
        server.register_handler(
            FBMsgType::EHeartbeatRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_heartbeat),
        );
        server.register_handler(
            FBMsgType::EStepDownRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_step_down),
        );
        server.register_handler(
            FBMsgType::EChosenNotification.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_chosen_notice),
        );
        server.register_handler(
            FBMsgType::EBatchChosenNotification.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_batch_chosen),
        );
        server.register_handler(
            FBMsgType::EFetchGapRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_fetch_gap),
        );
        server.register_handler(
            FBMsgType::ESnapshotBeginRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_snapshot_begin),
        );
        server.register_handler(
            FBMsgType::ESnapshotReadRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_snapshot_read),
        );
        server.register_handler(
            FBMsgType::ESnapshotFinishRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_snapshot_finish),
        );
        server.register_handler(
            FBMsgType::ESnapshotAbortRequest.0 as u16,
            Self::make_handler(Arc::clone(self), Arc::clone(server), Self::handle_snapshot_abort),
        );
    }

    fn make_handler(
        this: Arc<Self>,
        server: Arc<RpcServer>,
        f: fn(&Self, ServerRequest, &Arc<RpcServer>),
    ) -> impl Fn(ServerRequest) + Send + 'static {
        move |req| {
            f(&this, req, &server);
        }
    }

    // ── Prepare ──────────────────────────────────────────────────

    #[allow(clippy::too_many_lines)]
    fn handle_prepare(&self, req: ServerRequest, server: &Arc<RpcServer>) {
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

    // ── Accept ───────────────────────────────────────────────────

    #[allow(clippy::too_many_lines)]
    fn handle_accept(&self, req: ServerRequest, server: &Arc<RpcServer>) {
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

    // ── PreVote ──────────────────────────────────────────────────

    fn handle_pre_vote(&self, req: ServerRequest, server: &Arc<RpcServer>) {
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

    fn handle_request_vote(&self, req: ServerRequest, server: &Arc<RpcServer>) {
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

    fn handle_heartbeat(&self, req: ServerRequest, server: &Arc<RpcServer>) {
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
            };
            let replica = group.local_replica();
            let reply = <PxLocalReplica as ReplicaHandler>::on_heartbeat(replica, payload, group_id).await;
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

    // ── StepDown ─────────────────────────────────────────────────

    fn handle_step_down(&self, req: ServerRequest, server: &Arc<RpcServer>) {
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

    // ── ChosenNotification (fire-and-forget) ─────────────────────

    #[allow(clippy::needless_pass_by_value, reason = "make_handler uniform signature")]
    fn handle_chosen_notice(&self, req: ServerRequest, _server: &Arc<RpcServer>) {
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
    fn handle_batch_chosen(&self, req: ServerRequest, _server: &Arc<RpcServer>) {
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
    fn handle_fetch_gap(&self, req: ServerRequest, server: &Arc<RpcServer>) {
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

    // ── Snapshot streaming ───────────────────────────────────────

    #[allow(clippy::too_many_lines)]
    fn handle_snapshot_begin(&self, req: ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let conn_handle = req.conn_handle as usize;
        let store = Arc::clone(&self.store);
        let registry = Arc::clone(&self.snapshots);
        let server = Arc::clone(server);
        spawn_request(&self.rt, "snapshot_begin", store.store_id, async move {
            let Ok(request) = flatbuffers::root::<FBSnapshotBeginRequest>(req.control()) else {
                submit_snapshot_begin(
                    &server,
                    conn_handle,
                    req_id,
                    create_nano,
                    FBKvRetCode::InvalidArgument,
                    Some("invalid snapshot Begin request"),
                    None,
                    0,
                );
                return;
            };
            let group_id = request.group_id();
            record_group_context(&store, group_id);
            let requested = request.max_chunk_bytes() as usize;
            if requested == 0 {
                submit_snapshot_begin(
                    &server,
                    conn_handle,
                    req_id,
                    create_nano,
                    FBKvRetCode::InvalidArgument,
                    Some("max_chunk_bytes must be nonzero"),
                    None,
                    group_id,
                );
                return;
            }
            let Some(group) = store.get_group(group_id) else {
                submit_snapshot_begin(
                    &server,
                    conn_handle,
                    req_id,
                    create_nano,
                    FBKvRetCode::NotFound,
                    Some("px group not found"),
                    None,
                    group_id,
                );
                return;
            };
            let membership_epoch = group.membership_epoch();
            let chunk_bytes = requested.min(store.snapshot_chunk_bytes).min(1024 * 1024);
            if chunk_bytes == 0 {
                submit_snapshot_begin(
                    &server,
                    conn_handle,
                    req_id,
                    create_nano,
                    FBKvRetCode::Internal,
                    Some("snapshot source chunk size is not configured"),
                    None,
                    group_id,
                );
                return;
            }
            let permit = match registry.reserve() {
                Ok(permit) => permit,
                Err(error) => {
                    let (code, message) = snapshot_error(&error);
                    submit_snapshot_begin(
                        &server,
                        conn_handle,
                        req_id,
                        create_nano,
                        code,
                        Some(&message),
                        None,
                        group_id,
                    );
                    return;
                }
            };
            let export_group = Arc::clone(&group);
            let export_started = std::time::Instant::now();
            let exporter = match tokio::task::spawn_blocking(move || {
                export_group
                    .local_replica()
                    .learner
                    .engine()
                    .snapshot_export_begin(chunk_bytes)
            })
            .await
            {
                Ok(Ok(exporter)) => exporter,
                Ok(Err(error)) => {
                    submit_snapshot_begin(
                        &server,
                        conn_handle,
                        req_id,
                        create_nano,
                        FBKvRetCode::Internal,
                        Some(&error),
                        None,
                        group_id,
                    );
                    return;
                }
                Err(error) => {
                    let message = format!("snapshot export task failed: {error}");
                    submit_snapshot_begin(
                        &server,
                        conn_handle,
                        req_id,
                        create_nano,
                        FBKvRetCode::Internal,
                        Some(&message),
                        None,
                        group_id,
                    );
                    return;
                }
            };
            registry.observe_export(export_started.elapsed());
            if group.membership_epoch() != membership_epoch {
                submit_snapshot_begin(
                    &server,
                    conn_handle,
                    req_id,
                    create_nano,
                    FBKvRetCode::SnapshotTopologyChanged,
                    Some("membership changed while snapshot began"),
                    None,
                    group_id,
                );
                return;
            }
            let at_slot = exporter.metadata().at_slot;
            let term_at_slot = group
                .local_replica()
                .accepted_at(at_slot)
                .await
                .map_or(0, |entry| entry.term);
            let registration = registry.begin(group_id, membership_epoch, exporter, permit);
            submit_snapshot_begin(
                &server,
                conn_handle,
                req_id,
                create_nano,
                FBKvRetCode::Success,
                None,
                Some((&registration, term_at_slot)),
                group_id,
            );
        });
    }

    fn handle_snapshot_read(&self, req: ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let conn_handle = req.conn_handle as usize;
        let store = Arc::clone(&self.store);
        let registry = Arc::clone(&self.snapshots);
        let server = Arc::clone(server);
        spawn_request(&self.rt, "snapshot_read", store.store_id, async move {
            let Ok(request) = flatbuffers::root::<FBSnapshotReadRequest>(req.control()) else {
                submit_snapshot_read(
                    &server,
                    conn_handle,
                    req_id,
                    create_nano,
                    FBKvRetCode::InvalidArgument,
                    Some("invalid snapshot Read request"),
                    SnapshotSessionId {
                        boot_nonce: 0,
                        session_number: 0,
                    },
                    0,
                    None,
                );
                return;
            };
            let id = SnapshotSessionId {
                boot_nonce: request.boot_nonce(),
                session_number: request.session_number(),
            };
            let offset = request.offset();
            let registration = match registry.registration(id) {
                Ok(value) => value,
                Err(error) => {
                    let (code, message) = snapshot_error(&error);
                    submit_snapshot_read(
                        &server,
                        conn_handle,
                        req_id,
                        create_nano,
                        code,
                        Some(&message),
                        id,
                        offset,
                        None,
                    );
                    return;
                }
            };
            if store.get_group(registration.group_id).map_or(true, |group| {
                group.membership_epoch() != registration.membership_epoch
            }) {
                registry.expire(id);
                submit_snapshot_read(
                    &server,
                    conn_handle,
                    req_id,
                    create_nano,
                    FBKvRetCode::SnapshotTopologyChanged,
                    Some("snapshot membership epoch changed"),
                    id,
                    offset,
                    None,
                );
                return;
            }
            match registry.read(id, offset).await {
                Ok(read) => submit_snapshot_read(
                    &server,
                    conn_handle,
                    req_id,
                    create_nano,
                    FBKvRetCode::Success,
                    None,
                    id,
                    offset,
                    Some(read),
                ),
                Err(error) => {
                    let (code, message) = snapshot_error(&error);
                    submit_snapshot_read(
                        &server,
                        conn_handle,
                        req_id,
                        create_nano,
                        code,
                        Some(&message),
                        id,
                        offset,
                        None,
                    );
                }
            }
        });
    }

    fn handle_snapshot_finish(&self, req: ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let conn_handle = req.conn_handle as usize;
        let store = Arc::clone(&self.store);
        let registry = Arc::clone(&self.snapshots);
        let server = Arc::clone(server);
        spawn_request(&self.rt, "snapshot_finish", store.store_id, async move {
            let Ok(request) = flatbuffers::root::<FBSnapshotFinishRequest>(req.control()) else {
                submit_snapshot_close(
                    &server,
                    conn_handle,
                    req_id,
                    create_nano,
                    FBMsgType::ESnapshotFinishResponse.0 as u16,
                    FBKvRetCode::InvalidArgument,
                    Some("invalid snapshot Finish request"),
                    SnapshotSessionId {
                        boot_nonce: 0,
                        session_number: 0,
                    },
                );
                return;
            };
            let id = SnapshotSessionId {
                boot_nonce: request.boot_nonce(),
                session_number: request.session_number(),
            };
            if let Ok(registration) = registry.registration(id) {
                if store.get_group(registration.group_id).map_or(true, |group| {
                    group.membership_epoch() != registration.membership_epoch
                }) {
                    registry.expire(id);
                    submit_snapshot_close(
                        &server,
                        conn_handle,
                        req_id,
                        create_nano,
                        FBMsgType::ESnapshotFinishResponse.0 as u16,
                        FBKvRetCode::SnapshotTopologyChanged,
                        Some("snapshot membership epoch changed"),
                        id,
                    );
                    return;
                }
            }
            match registry.finish(id, request.final_offset()).await {
                Ok(()) => submit_snapshot_close(
                    &server,
                    conn_handle,
                    req_id,
                    create_nano,
                    FBMsgType::ESnapshotFinishResponse.0 as u16,
                    FBKvRetCode::Success,
                    None,
                    id,
                ),
                Err(error) => {
                    let (code, message) = snapshot_error(&error);
                    submit_snapshot_close(
                        &server,
                        conn_handle,
                        req_id,
                        create_nano,
                        FBMsgType::ESnapshotFinishResponse.0 as u16,
                        code,
                        Some(&message),
                        id,
                    );
                }
            }
        });
    }

    fn handle_snapshot_abort(&self, req: ServerRequest, server: &Arc<RpcServer>) {
        let req_id = req.request_id;
        let create_nano = req.rpc_create_nano;
        let conn_handle = req.conn_handle as usize;
        let registry = Arc::clone(&self.snapshots);
        let server = Arc::clone(server);
        spawn_request(&self.rt, "snapshot_abort", self.store.store_id, async move {
            let Ok(request) = flatbuffers::root::<FBSnapshotAbortRequest>(req.control()) else {
                submit_snapshot_close(
                    &server,
                    conn_handle,
                    req_id,
                    create_nano,
                    FBMsgType::ESnapshotAbortResponse.0 as u16,
                    FBKvRetCode::InvalidArgument,
                    Some("invalid snapshot Abort request"),
                    SnapshotSessionId {
                        boot_nonce: 0,
                        session_number: 0,
                    },
                );
                return;
            };
            let id = SnapshotSessionId {
                boot_nonce: request.boot_nonce(),
                session_number: request.session_number(),
            };
            registry.abort(id).await;
            submit_snapshot_close(
                &server,
                conn_handle,
                req_id,
                create_nano,
                FBMsgType::ESnapshotAbortResponse.0 as u16,
                FBKvRetCode::Success,
                None,
                id,
            );
        });
    }
}

impl Drop for PxRpcService {
    fn drop(&mut self) {
        self.snapshots.shutdown();
    }
}

// ── Helper functions ─────────────────────────────────────────────

#[allow(clippy::too_many_arguments)]
fn submit_snapshot_begin(
    server: &RpcServer,
    conn_handle: usize,
    req_id: u64,
    create_nano: u64,
    ret_code: FBKvRetCode,
    error: Option<&str>,
    success: Option<(&SnapshotRegistration, u64)>,
    group_id: u64,
) {
    let mut builder = FlatBufferBuilder::new();
    let error_msg = error.map(|message| builder.create_string(message));
    let (
        boot_nonce,
        session_number,
        engine_format,
        at_slot,
        term_at_slot,
        membership_epoch,
        chunk_bytes,
        total_bytes,
        final_crc32c,
    ) = success.map_or((0, 0, 0, 0, 0, 0, 0, 0, 0), |(registration, term)| {
        (
            registration.id.boot_nonce,
            registration.id.session_number,
            registration.metadata.format as u8,
            registration.metadata.at_slot,
            term,
            registration.membership_epoch,
            u32::try_from(registration.metadata.chunk_bytes).unwrap_or(u32::MAX),
            registration.metadata.total_bytes,
            registration.metadata.final_crc32c,
        )
    });
    let response = FBSnapshotBeginResponse::create(
        &mut builder,
        &FBSnapshotBeginResponseArgs {
            id: req_id,
            rpc_create_nano: create_nano,
            ret_code,
            error_msg,
            group_id,
            boot_nonce,
            session_number,
            engine_format,
            at_slot,
            term_at_slot,
            membership_epoch,
            chunk_bytes,
            total_bytes,
            final_crc32c,
        },
    );
    builder.finish(response, None);
    submit_fb_response(
        server,
        conn_handle as *mut std::ffi::c_void,
        builder.collapse(),
        FBMsgType::ESnapshotBeginResponse.0 as u16,
        req_id,
    );
}

#[allow(clippy::too_many_arguments)]
fn submit_snapshot_read(
    server: &RpcServer,
    conn_handle: usize,
    req_id: u64,
    create_nano: u64,
    ret_code: FBKvRetCode,
    error: Option<&str>,
    id: SnapshotSessionId,
    offset: u64,
    read: Option<crate::rpc::snapshot_registry::SnapshotRead>,
) {
    let mut builder = FlatBufferBuilder::new();
    let error_msg = error.map(|message| builder.create_string(message));
    let payload_crc32c = read.as_ref().map_or(0, |value| value.payload_crc32c);
    let done = read.as_ref().is_some_and(|value| value.chunk.done);
    let response = FBSnapshotReadResponse::create(
        &mut builder,
        &FBSnapshotReadResponseArgs {
            id: req_id,
            rpc_create_nano: create_nano,
            ret_code,
            error_msg,
            boot_nonce: id.boot_nonce,
            session_number: id.session_number,
            offset,
            payload_crc32c,
            done,
        },
    );
    builder.finish(response, None);
    let control = builder.collapse();
    let data = read.map(|value| Buffer::from_vec_offset(value.chunk.bytes, 0));
    let control = Buffer::from_vec_offset(control.0, control.1);
    unsafe {
        let _ = server.submit_response_buffer(
            conn_handle as *mut std::ffi::c_void,
            control,
            data,
            FBMsgType::ESnapshotReadResponse.0 as u16,
            req_id,
        );
    }
}

#[allow(clippy::too_many_arguments)]
fn submit_snapshot_close(
    server: &RpcServer,
    conn_handle: usize,
    req_id: u64,
    create_nano: u64,
    msg_type: u16,
    ret_code: FBKvRetCode,
    error: Option<&str>,
    id: SnapshotSessionId,
) {
    let mut builder = FlatBufferBuilder::new();
    let error_msg = error.map(|message| builder.create_string(message));
    if msg_type == FBMsgType::ESnapshotFinishResponse.0 as u16 {
        let response = FBSnapshotFinishResponse::create(
            &mut builder,
            &FBSnapshotFinishResponseArgs {
                id: req_id,
                rpc_create_nano: create_nano,
                ret_code,
                error_msg,
                boot_nonce: id.boot_nonce,
                session_number: id.session_number,
            },
        );
        builder.finish(response, None);
    } else {
        let response = FBSnapshotAbortResponse::create(
            &mut builder,
            &FBSnapshotAbortResponseArgs {
                id: req_id,
                rpc_create_nano: create_nano,
                ret_code,
                error_msg,
                boot_nonce: id.boot_nonce,
                session_number: id.session_number,
            },
        );
        builder.finish(response, None);
    }
    submit_fb_response(
        server,
        conn_handle as *mut std::ffi::c_void,
        builder.collapse(),
        msg_type,
        req_id,
    );
}

fn snapshot_error(error: &SnapshotSessionError) -> (FBKvRetCode, String) {
    match error {
        SnapshotSessionError::NotFound => (
            FBKvRetCode::SnapshotNotFound,
            "snapshot session not found".to_string(),
        ),
        SnapshotSessionError::Expired => (
            FBKvRetCode::SnapshotExpired,
            "snapshot session expired".to_string(),
        ),
        SnapshotSessionError::Backpressure => (
            FBKvRetCode::SnapshotBackpressure,
            "snapshot session capacity exhausted".to_string(),
        ),
        SnapshotSessionError::InvalidOffset { expected } => (
            FBKvRetCode::SnapshotInvalidOffset,
            format!("invalid snapshot offset; expected {expected}"),
        ),
        SnapshotSessionError::Export(message) => (FBKvRetCode::Internal, message.clone()),
    }
}

fn px_error_to_ret_code(e: &PxReplicaError) -> (FBKvRetCode, String) {
    match e {
        PxReplicaError::GroupNotFound(_) => (FBKvRetCode::NotFound, e.to_string()),
        PxReplicaError::ShuttingDown => (FBKvRetCode::Unavailable, e.to_string()),
        PxReplicaError::Transport(_) | PxReplicaError::Timeout(_) | PxReplicaError::Backpressure(_) => {
            (FBKvRetCode::Unavailable, e.to_string())
        }
        PxReplicaError::Internal(_) => (FBKvRetCode::Internal, e.to_string()),
    }
}

fn submit_error(
    server: &Arc<RpcServer>,
    conn_handle: *mut std::ffi::c_void,
    req_id: u64,
    create_nano: u64,
    msg_type: u16,
    code: FBKvRetCode,
    msg: &str,
) {
    let mut builder = FlatBufferBuilder::new();
    let error_msg = builder.create_string(msg);
    let args = FBPromiseResponseArgs {
        id: req_id,
        rpc_create_nano: create_nano,
        ret_code: code,
        error_msg: Some(error_msg),
        version: 0,
        slot: 0,
        round: 0,
        leader_id: 0,
        previously_accepted: None,
        rejected: false,
        rejected_round: 0,
        rejected_leader_id: 0,
        term: 0,
        term_stale: false,
        membership_epoch: 0,
        epoch_mismatch: false,
    };
    let resp = FBPromiseResponse::create(&mut builder, &args);
    builder.finish(resp, None);
    let ctrl = builder.collapse();
    submit_fb_response(server, conn_handle, ctrl, msg_type, req_id);
}

// ── Zero-copy response submit helper ──────────────────────────────

/// Submit a response from a collapsed `FlatBufferBuilder` (zero-copy).
/// `ctrl` is the `(Vec<u8>, head)` tuple from `builder.collapse()` — the
/// finished flatbuffer data is at `ctrl.0[ctrl.1..]`. The Vec allocation
/// is wrapped as an external C++ Buffer (no copy); C++ uses it directly
/// for the `OutFrame` and frees it when the write completes.
fn submit_fb_response(
    server: &RpcServer,
    conn_handle: *mut std::ffi::c_void,
    ctrl: (Vec<u8>, usize),
    msg_type: u16,
    req_id: u64,
) {
    let buf = Buffer::from_vec_offset(ctrl.0, ctrl.1);
    if buf.is_null_handle() {
        // Empty control — submit with raw bytes path.
        unsafe {
            let _ = server.submit_response(conn_handle, &[], None, msg_type, req_id);
        }
        return;
    }
    unsafe {
        let _ = server.submit_response_buffer(conn_handle, buf, None, msg_type, req_id);
    }
}

// ── Response builders ────────────────────────────────────────────

/// Build a `FBPromiseResponse` with a pre-existing `FBAcceptedValue`
/// offset (for the `Promised` reply with `previously_accepted`).
/// The caller must have already created the offset in `builder`.
#[allow(clippy::too_many_arguments)]
fn finish_promise_response(
    mut builder: FlatBufferBuilder,
    req_id: u64,
    create_nano: u64,
    ret_code: FBKvRetCode,
    error_msg: Option<&str>,
    version: u32,
    slot: u64,
    round: u64,
    leader_id: u64,
    previously_accepted: Option<flatbuffers::WIPOffset<FBAcceptedValue>>,
    rejected: bool,
    rejected_round: u64,
    rejected_leader_id: u64,
    term: u64,
    term_stale: bool,
    membership_epoch: u64,
    epoch_mismatch: bool,
) -> (Vec<u8>, usize) {
    let error_msg = error_msg.map(|m| builder.create_string(m));
    let args = FBPromiseResponseArgs {
        id: req_id,
        rpc_create_nano: create_nano,
        ret_code,
        error_msg,
        version,
        slot,
        round,
        leader_id,
        previously_accepted,
        rejected,
        rejected_round,
        rejected_leader_id,
        term,
        term_stale,
        membership_epoch,
        epoch_mismatch,
    };
    let resp = FBPromiseResponse::create(&mut builder, &args);
    builder.finish(resp, None);
    builder.collapse()
}

#[allow(clippy::too_many_arguments)]
fn build_promise_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBKvRetCode,
    error_msg: Option<&str>,
    version: u32,
    slot: u64,
    round: u64,
    leader_id: u64,
    previously_accepted: Option<flatbuffers::WIPOffset<FBAcceptedValue>>,
    rejected: bool,
    rejected_round: u64,
    rejected_leader_id: u64,
    term: u64,
    term_stale: bool,
    membership_epoch: u64,
    epoch_mismatch: bool,
) -> (Vec<u8>, usize) {
    let builder = FlatBufferBuilder::new();
    finish_promise_response(
        builder,
        req_id,
        create_nano,
        ret_code,
        error_msg,
        version,
        slot,
        round,
        leader_id,
        previously_accepted,
        rejected,
        rejected_round,
        rejected_leader_id,
        term,
        term_stale,
        membership_epoch,
        epoch_mismatch,
    )
}

#[allow(clippy::too_many_arguments)]
fn build_accepted_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBKvRetCode,
    error_msg: Option<&str>,
    version: u32,
    slot: u64,
    round: u64,
    leader_id: u64,
    rejected: bool,
    rejected_round: u64,
    rejected_leader_id: u64,
    term: u64,
    term_stale: bool,
    membership_epoch: u64,
    epoch_mismatch: bool,
) -> (Vec<u8>, usize) {
    let mut builder = FlatBufferBuilder::new();
    let error_msg = error_msg.map(|m| builder.create_string(m));
    let args = FBAcceptedResponseArgs {
        id: req_id,
        rpc_create_nano: create_nano,
        ret_code,
        error_msg,
        version,
        slot,
        round,
        leader_id,
        rejected,
        rejected_round,
        rejected_leader_id,
        term,
        term_stale,
        membership_epoch,
        epoch_mismatch,
    };
    let resp = FBAcceptedResponse::create(&mut builder, &args);
    builder.finish(resp, None);
    builder.collapse()
}

#[allow(clippy::too_many_arguments)]
fn build_pre_vote_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBKvRetCode,
    error_msg: Option<&str>,
    version: u32,
    group_id: u64,
    term: u64,
    granted: bool,
    contiguous_chosen: u64,
    last_chosen_term: u64,
    highest_seen_slot: u64,
) -> (Vec<u8>, usize) {
    let mut builder = FlatBufferBuilder::new();
    let error_msg = error_msg.map(|m| builder.create_string(m));
    let args = FBPreVoteResponseArgs {
        id: req_id,
        rpc_create_nano: create_nano,
        ret_code,
        error_msg,
        version,
        group_id,
        term,
        granted,
        contiguous_chosen,
        last_chosen_term,
        highest_seen_slot,
    };
    let resp = FBPreVoteResponse::create(&mut builder, &args);
    builder.finish(resp, None);
    builder.collapse()
}

#[allow(clippy::too_many_arguments)]
fn build_request_vote_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBKvRetCode,
    error_msg: Option<&str>,
    version: u32,
    group_id: u64,
    term: u64,
    granted: bool,
    contiguous_chosen: u64,
    last_chosen_term: u64,
    highest_seen_slot: u64,
) -> (Vec<u8>, usize) {
    let mut builder = FlatBufferBuilder::new();
    let error_msg = error_msg.map(|m| builder.create_string(m));
    let args = FBRequestVoteResponseArgs {
        id: req_id,
        rpc_create_nano: create_nano,
        ret_code,
        error_msg,
        version,
        group_id,
        term,
        granted,
        contiguous_chosen,
        last_chosen_term,
        highest_seen_slot,
    };
    let resp = FBRequestVoteResponse::create(&mut builder, &args);
    builder.finish(resp, None);
    builder.collapse()
}

#[allow(clippy::too_many_arguments)]
fn build_heartbeat_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBKvRetCode,
    error_msg: Option<&str>,
    version: u32,
    group_id: u64,
    term: u64,
    success: bool,
    contiguous_chosen: u64,
    last_chosen_term: u64,
    contiguous_applied: u64,
    highest_seen_slot: u64,
    durable_snapshot_slot: u64,
) -> (Vec<u8>, usize) {
    let mut builder = FlatBufferBuilder::new();
    let error_msg = error_msg.map(|m| builder.create_string(m));
    let args = FBHeartbeatResponseArgs {
        id: req_id,
        rpc_create_nano: create_nano,
        ret_code,
        error_msg,
        version,
        group_id,
        term,
        success,
        contiguous_chosen,
        last_chosen_term,
        contiguous_applied,
        highest_seen_slot,
        durable_snapshot_slot,
    };
    let resp = FBHeartbeatResponse::create(&mut builder, &args);
    builder.finish(resp, None);
    builder.collapse()
}

#[allow(clippy::too_many_arguments)]
fn build_step_down_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBKvRetCode,
    error_msg: Option<&str>,
    version: u32,
    group_id: u64,
    accepted: bool,
    current_term: u64,
    current_leader_id: u64,
) -> (Vec<u8>, usize) {
    let mut builder = FlatBufferBuilder::new();
    let error_msg = error_msg.map(|m| builder.create_string(m));
    let args = FBStepDownResponseArgs {
        id: req_id,
        rpc_create_nano: create_nano,
        ret_code,
        error_msg,
        version,
        group_id,
        accepted,
        current_term,
        current_leader_id,
    };
    let resp = FBStepDownResponse::create(&mut builder, &args);
    builder.finish(resp, None);
    builder.collapse()
}

#[allow(clippy::too_many_arguments)]
fn build_fetch_gap_response(
    req_id: u64,
    create_nano: u64,
    ret_code: FBKvRetCode,
    error_msg: Option<&str>,
    version: u32,
    group_id: u64,
    slot: u64,
    term: u64,
    ballot_round: u64,
    leader_id: u64,
    payload: &[u8],
) -> (Vec<u8>, usize) {
    let mut builder = FlatBufferBuilder::new();
    let error_msg = error_msg.map(|m| builder.create_string(m));
    let payload_vec = builder.create_vector(payload);
    let args = FBFetchGapResponseArgs {
        id: req_id,
        rpc_create_nano: create_nano,
        ret_code,
        error_msg,
        version,
        group_id,
        slot,
        term,
        ballot_round,
        leader_id,
        payload: Some(payload_vec),
    };
    let resp = FBFetchGapResponse::create(&mut builder, &args);
    builder.finish(resp, None);
    builder.collapse()
}
