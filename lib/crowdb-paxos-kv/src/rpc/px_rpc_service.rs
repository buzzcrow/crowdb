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
            cluster_id: std::env::var("CROWDB_PAXOS_KV_CLUSTER_ID").ok(),
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
mod heartbeat;
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
