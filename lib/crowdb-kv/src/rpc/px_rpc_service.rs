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

mod heartbeat;

use std::future::Future;
use std::sync::Arc;

use crowdb_protocol::fb::FBMsgType;
use crowdb_protocol::kv_consensus_fb::{
    FBAcceptRequest, FBAcceptedResponse, FBAcceptedResponseArgs, FBAcceptedValue, FBAcceptedValueArgs,
    FBBatchChosenNotification, FBChosenNotification, FBFetchGapRequest, FBFetchGapResponse,
    FBFetchGapResponseArgs, FBHeartbeatResponse, FBHeartbeatResponseArgs, FBKvRetCode, FBPreVoteRequest,
    FBPreVoteResponse, FBPreVoteResponseArgs, FBPrepareRequest, FBPromiseResponse, FBPromiseResponseArgs,
    FBRequestVoteRequest, FBRequestVoteResponse, FBRequestVoteResponseArgs, FBSnapshotAbortRequest,
    FBSnapshotAbortResponse, FBSnapshotAbortResponseArgs, FBSnapshotBeginRequest, FBSnapshotBeginResponse,
    FBSnapshotBeginResponseArgs, FBSnapshotFinishRequest, FBSnapshotFinishResponse,
    FBSnapshotFinishResponseArgs, FBSnapshotReadRequest, FBSnapshotReadResponse, FBSnapshotReadResponseArgs,
    FBStepDownRequest, FBStepDownResponse, FBStepDownResponseArgs,
};
use crowdb_rpc_ffi::{Buffer, RpcServer, ServerRequest};
use flatbuffers::FlatBufferBuilder;
use tokio::runtime::Handle;
use tracing::{debug, field, info_span, warn, Instrument, Span};

use crate::cluster::local_replica::PxLocalReplica;
use crate::cluster::px_kv_store::PxKvStore;
use crate::cluster::replica::{PxReplicaError, ReplicaHandler, StepDownRequestPayload, VoteRequestPayload};
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
    cluster_id: Option<String>,
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
        Self {
            store,
            rt,
            snapshots,
            cluster_id: std::env::var("CROWDB_KV_CLUSTER_ID").ok(),
        }
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
            if !this.target_matches(&req) {
                return;
            }
            f(&this, req, &server);
        }
    }

<<<<<<< HEAD
    fn target_matches(&self, req: &ServerRequest) -> bool {
        let consensus = [
            FBMsgType::EPrepareRequest,
            FBMsgType::EAcceptRequest,
            FBMsgType::EPreVoteRequest,
            FBMsgType::ERequestVoteRequest,
            FBMsgType::EHeartbeatRequest,
            FBMsgType::EStepDownRequest,
            FBMsgType::EChosenNotification,
            FBMsgType::EBatchChosenNotification,
            FBMsgType::EFetchGapRequest,
        ];
        if !consensus
            .iter()
            .any(|kind| u16::try_from(kind.0).ok() == Some(req.msg_type))
        {
            return true;
=======
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
>>>>>>> 0f5d94a8 (Reclaim snapshot-backed Paxos history with retained readers)
        }
        let Ok(target) =
            flatbuffers::root::<crowdb_protocol::kv_consensus_fb::FBConsensusTarget>(req.control())
        else {
            return true;
        };
        if target.target_cluster_id() != self.cluster_id.as_deref() {
            return false;
        }
        target.target_replica_id() == 0
            || self.store.get_group(target.group_id()).map_or(true, |group| {
                group.local_replica().id == target.target_replica_id()
            })
    }
}

impl Drop for PxRpcService {
    fn drop(&mut self) {
        self.snapshots.shutdown();
    }
}

// ── Helper functions ─────────────────────────────────────────────

mod accept;
mod election;
mod notifications;
#[allow(clippy::too_many_arguments)]
mod prepare;
mod responses;
mod snapshots;
use responses::{
    build_accepted_response, build_fetch_gap_response, build_heartbeat_response, build_pre_vote_response,
    build_promise_response, build_request_vote_response, build_step_down_response, finish_promise_response,
    px_error_to_ret_code, snapshot_error, submit_error, submit_fb_response, submit_snapshot_begin,
    submit_snapshot_close, submit_snapshot_read,
};
