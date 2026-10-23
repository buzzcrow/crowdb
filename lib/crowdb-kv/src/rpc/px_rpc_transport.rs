// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::missing_errors_doc)]
//! crowdb-rpc client transport for the KV consensus service. Builds
//! flatbuffer requests, sends via `RpcClient::call`,
//! awaits `CallFuture`, and parses flatbuffer responses via the
//! zero-copy `Ref` wrappers.

mod heartbeat;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use flatbuffers::FlatBufferBuilder;

use crowdb_protocol::fb::FBMsgType;
use crowdb_protocol::fb_wrappers::kv_consensus::{
    FBAcceptedResponseRef, FBFetchGapResponseRef, FBPreVoteResponseRef, FBPromiseResponseRef,
    FBRequestVoteResponseRef, FBSnapshotAbortResponseRef, FBSnapshotBeginResponseRef,
    FBSnapshotFinishResponseRef, FBSnapshotReadResponseRef, FBStepDownResponseRef,
};
use crowdb_protocol::kv_consensus_fb::{
    FBAcceptRequest, FBAcceptRequestArgs, FBAcceptedValue, FBAcceptedValueArgs, FBBatchChosenNotification,
    FBBatchChosenNotificationArgs, FBChosenNotification, FBChosenNotificationArgs, FBFetchGapRequest,
    FBFetchGapRequestArgs, FBKvRetCode, FBPreVoteRequest, FBPreVoteRequestArgs, FBPrepareRequest,
    FBPrepareRequestArgs, FBRequestVoteRequest, FBRequestVoteRequestArgs, FBSnapshotAbortRequest,
    FBSnapshotAbortRequestArgs, FBSnapshotBeginRequest, FBSnapshotBeginRequestArgs, FBSnapshotFinishRequest,
    FBSnapshotFinishRequestArgs, FBSnapshotReadRequest, FBSnapshotReadRequestArgs, FBStepDownRequest,
    FBStepDownRequestArgs,
};
use crowdb_rpc_ffi::{
    noop_completion, Buffer, ConnectionPoolError, ConnectionPoolIndex, RpcClient, RpcError, RpcServer,
    SelectedConnection,
};

use crate::cluster::replica::{
    FetchGapReply, PxReplicaError, StepDownReply, StepDownRequestPayload, VoteReply, VoteRequestPayload,
};
use crate::paxos::roles::{PxAcceptReply, PxBallot, PxLogEntry, PxPrepareReply};

/// crowdb-rpc transport for the KV consensus service. Holds the
/// client-side `RpcServer` (manages connections), `RpcClient`
/// (request/response correlation), and a connection pool per
/// endpoint (round-robin across `pool_size` connections).
pub struct PxRpcTransport {
    server: Arc<RpcServer>,
    rpc: Arc<RpcClient>,
    connections: ConnectionPoolIndex,
    next_req_id: AtomicU64,
    cluster_id: Option<String>,
}

impl std::fmt::Debug for PxRpcTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PxRpcTransport")
            .field("next_req_id", &self.next_req_id.load(Ordering::Relaxed))
            .finish_non_exhaustive()
    }
}

impl PxRpcTransport {
    /// Create a new crowdb-rpc transport with a single connection per
    /// peer endpoint and default tunables (backward-compatible).
    #[must_use]
    pub fn new() -> Self {
        Self::with_pool_size(1, false, false, false, 4096, 2)
    }

    /// Create a new crowdb-rpc transport with `pool_size` connections
    /// per peer endpoint, using the given RPC tunables and `workers`
    /// I/O worker threads. The `RpcServer` is the client-side transport
    /// — it does not listen but is used to establish connections to
    /// remote endpoints.
    #[must_use]
    pub fn with_pool_size(
        pool_size: usize,
        enable_nagle: bool,
        quickack: bool,
        event_write: bool,
        send_queue_capacity: u32,
        workers: u32,
    ) -> Self {
        let server = Arc::new(RpcServer::with_engines(None, 1, workers));
        server.set_tcp_nodelay(!enable_nagle);
        server.set_quickack(quickack);
        server.set_event_write(event_write);
        server.set_send_queue_capacity(send_queue_capacity);
        server.start();
        server.register_conn_count_gauge("rpc.consensus.connections");
        let rpc = Arc::new(RpcClient::new());
        rpc.set_completion_pool_size(1024);
        rpc.start_reaper(3_000_000_000, 500_000_000);
        Self {
            server,
            rpc,
            connections: ConnectionPoolIndex::new(pool_size, None),
            next_req_id: AtomicU64::new(1),
            cluster_id: std::env::var("CROWDB_KV_CLUSTER_ID").ok(),
        }
    }

    fn next_id(&self) -> u64 {
        self.next_req_id.fetch_add(1, Ordering::Relaxed)
    }

    /// Get or create a `Connection` for the given endpoint, round-
    /// robining across the pool. The crowdb-rpc server listens on the
    /// same port as the crowdb-rpc endpoint (no port derivation).
    fn conn_for(&self, rpc_endpoint: &str) -> Result<SelectedConnection, PxReplicaError> {
        let normalized = normalize_endpoint(rpc_endpoint);
        let (host, port) = parse_endpoint(&normalized)
            .map_err(|e| PxReplicaError::Internal(format!("rpc connect parse endpoint: {e}")))?;
        self.connections
            .get_or_try_install(&normalized, || {
                let conn = self
                    .server
                    .connect(&host, port)
                    .map_err(|e| PxReplicaError::Internal(format!("rpc connect to {host}:{port}: {e:?}")))?;
                self.rpc.attach(&conn);
                Ok(conn)
            })
            .map_err(|error| match error {
                ConnectionPoolError::Closed => {
                    PxReplicaError::Internal("connection closed during acquisition".into())
                }
                ConnectionPoolError::Connect(error) => error,
                ConnectionPoolError::Capacity { max_endpoints } => {
                    PxReplicaError::Internal(format!("endpoint connection limit {max_endpoints} reached"))
                }
            })
    }

    /// Convert an `RpcError` to `PxReplicaError`, dropping cached
    /// connections only when the connection itself failed. Timeout and
    /// queue pressure do not prove that the selected generation is dead.
    fn map_rpc_err(&self, e: RpcError, endpoint: &str, generation: u64) -> PxReplicaError {
        if rpc_error_invalidates_connection(e) {
            self.connections
                .invalidate(&normalize_endpoint(endpoint), generation);
        }
        rpc_error_to_px(e)
    }

    /// Send a `Prepare` request via crowdb-rpc.
    pub async fn send_prepare(
        &self,
        destination: ReplicaDestination<'_>,
        slot: u64,
        ballot: PxBallot,
        term: u64,
        group_id: u64,
        membership_epoch: u64,
    ) -> Result<PxPrepareReply, PxReplicaError> {
        let rpc_endpoint = destination.endpoint;
        let target_replica_id = destination.replica_id;
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let target_cluster_id = self.cluster_id.as_deref().map(|id| builder.create_string(id));
        let args = FBPrepareRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            version: 1,
            slot,
            round: ballot.round,
            leader_id: ballot.leader_id,
            term,
            group_id,
            membership_epoch,
            target_replica_id,
            target_cluster_id,
        };
        let req = FBPrepareRequest::create(&mut builder, &args);
        builder.finish(req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EPrepareRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(|e| self.map_rpc_err(e, rpc_endpoint, conn.generation()))?;
        let resp = fut
            .await
            .map_err(|e| self.map_rpc_err(e, rpc_endpoint, conn.generation()))?;
        let ctrl = resp
            .control
            .ok_or_else(|| PxReplicaError::Internal("prepare response missing control buffer".into()))?;
        let r = FBPromiseResponseRef::new(ctrl.bytes());
        if !r.valid() {
            return Err(PxReplicaError::Internal("prepare response malformed".into()));
        }
        check_ret_code(r.ret_code(), r.error_msg())?;
        if r.epoch_mismatch() {
            Ok(PxPrepareReply::EpochMismatch {
                responder_epoch: r.membership_epoch(),
            })
        } else if r.term_stale() {
            Ok(PxPrepareReply::TermStale {
                slot: r.slot(),
                new_term: r.term(),
            })
        } else if r.rejected() {
            Ok(PxPrepareReply::Rejected {
                slot: r.slot(),
                current_promised: PxBallot::new(r.rejected_round(), r.rejected_leader_id()),
            })
        } else {
            // Promised — read the previously accepted value from the
            // nested FBAcceptedValue table so the proposer can adopt it.
            let accepted = r.previously_accepted().map(|av| PxLogEntry {
                slot: av.slot,
                ballot: PxBallot::new(av.round, av.leader_id),
                term: av.term,
                payload: av.payload,
            });
            Ok(PxPrepareReply::Promised {
                slot: r.slot(),
                accepted,
            })
        }
    }

    /// Send a unary `Accept` request via crowdb-rpc.
    pub async fn send_accept(
        &self,
        destination: ReplicaDestination<'_>,
        entry: &PxLogEntry,
        group_id: u64,
        membership_epoch: u64,
    ) -> Result<PxAcceptReply, PxReplicaError> {
        let rpc_endpoint = destination.endpoint;
        let target_replica_id = destination.replica_id;
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let payload = builder.create_vector(entry.payload.as_ref());
        let value = FBAcceptedValue::create(
            &mut builder,
            &FBAcceptedValueArgs {
                slot: entry.slot,
                round: entry.ballot.round,
                leader_id: entry.ballot.leader_id,
                term: entry.term,
                payload: Some(payload),
            },
        );
        let target_cluster_id = self.cluster_id.as_deref().map(|id| builder.create_string(id));
        let args = FBAcceptRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            version: 1,
            slot: entry.slot,
            round: entry.ballot.round,
            leader_id: entry.ballot.leader_id,
            term: entry.term,
            value: Some(value),
            group_id,
            membership_epoch,
            target_replica_id,
            target_cluster_id,
        };
        let req = FBAcceptRequest::create(&mut builder, &args);
        builder.finish(req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EAcceptRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(|e| self.map_rpc_err(e, rpc_endpoint, conn.generation()))?;
        let resp = fut
            .await
            .map_err(|e| self.map_rpc_err(e, rpc_endpoint, conn.generation()))?;
        let ctrl = resp
            .control
            .ok_or_else(|| PxReplicaError::Internal("accept response missing control buffer".into()))?;
        let r = FBAcceptedResponseRef::new(ctrl.bytes());
        if !r.valid() {
            return Err(PxReplicaError::Internal("accept response malformed".into()));
        }
        check_ret_code(r.ret_code(), r.error_msg())?;
        if r.epoch_mismatch() {
            Ok(PxAcceptReply::EpochMismatch {
                responder_epoch: r.membership_epoch(),
            })
        } else if r.term_stale() {
            Ok(PxAcceptReply::TermStale {
                slot: r.slot(),
                new_term: r.term(),
            })
        } else if r.rejected() {
            Ok(PxAcceptReply::Rejected {
                slot: r.slot(),
                current_promised: PxBallot::new(r.rejected_round(), r.rejected_leader_id()),
            })
        } else {
            Ok(PxAcceptReply::Accepted {
                slot: r.slot(),
                ballot: PxBallot::new(r.round(), r.leader_id()),
            })
        }
    }

    /// Send a `PreVote` request via crowdb-rpc.
    pub async fn send_pre_vote(
        &self,
        destination: ReplicaDestination<'_>,
        req: VoteRequestPayload,
        group_id: u64,
    ) -> Result<VoteReply, PxReplicaError> {
        let rpc_endpoint = destination.endpoint;
        let target_replica_id = destination.replica_id;
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let target_cluster_id = self.cluster_id.as_deref().map(|id| builder.create_string(id));
        let args = FBPreVoteRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            version: 1,
            group_id,
            term: req.term,
            candidate_id: req.candidate_id,
            accepted_log_tip_slot: req.accepted_log_tip_slot,
            accepted_log_tip_term: req.accepted_log_tip_term,
            target_replica_id,
            target_cluster_id,
        };
        let fb_req = FBPreVoteRequest::create(&mut builder, &args);
        builder.finish(fb_req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EPreVoteRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(|e| self.map_rpc_err(e, rpc_endpoint, conn.generation()))?;
        let resp = fut
            .await
            .map_err(|e| self.map_rpc_err(e, rpc_endpoint, conn.generation()))?;
        let ctrl = resp
            .control
            .ok_or_else(|| PxReplicaError::Internal("pre_vote response missing control buffer".into()))?;
        let r = FBPreVoteResponseRef::new(ctrl.bytes());
        if !r.valid() {
            return Err(PxReplicaError::Internal("pre_vote response malformed".into()));
        }
        check_ret_code(r.ret_code(), r.error_msg())?;
        Ok(VoteReply {
            term: r.term(),
            granted: r.granted(),
            contiguous_chosen: r.contiguous_chosen(),
            last_chosen_term: r.last_chosen_term(),
            highest_seen_slot: r.highest_seen_slot(),
        })
    }

    /// Send a `RequestVote` request via crowdb-rpc.
    pub async fn send_request_vote(
        &self,
        destination: ReplicaDestination<'_>,
        req: VoteRequestPayload,
        group_id: u64,
    ) -> Result<VoteReply, PxReplicaError> {
        let rpc_endpoint = destination.endpoint;
        let target_replica_id = destination.replica_id;
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let target_cluster_id = self.cluster_id.as_deref().map(|id| builder.create_string(id));
        let args = FBRequestVoteRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            version: 1,
            group_id,
            term: req.term,
            candidate_id: req.candidate_id,
            accepted_log_tip_slot: req.accepted_log_tip_slot,
            accepted_log_tip_term: req.accepted_log_tip_term,
            target_replica_id,
            target_cluster_id,
        };
        let fb_req = FBRequestVoteRequest::create(&mut builder, &args);
        builder.finish(fb_req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::ERequestVoteRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(|e| self.map_rpc_err(e, rpc_endpoint, conn.generation()))?;
        let resp = fut
            .await
            .map_err(|e| self.map_rpc_err(e, rpc_endpoint, conn.generation()))?;
        let ctrl = resp
            .control
            .ok_or_else(|| PxReplicaError::Internal("request_vote response missing control buffer".into()))?;
        let r = FBRequestVoteResponseRef::new(ctrl.bytes());
        if !r.valid() {
            return Err(PxReplicaError::Internal("request_vote response malformed".into()));
        }
        check_ret_code(r.ret_code(), r.error_msg())?;
        Ok(VoteReply {
            term: r.term(),
            granted: r.granted(),
            contiguous_chosen: r.contiguous_chosen(),
            last_chosen_term: r.last_chosen_term(),
            highest_seen_slot: r.highest_seen_slot(),
        })
    }

<<<<<<< HEAD
    /// Send a `Heartbeat` request via crowdb-rpc.
    pub async fn send_heartbeat(
        &self,
        destination: ReplicaDestination<'_>,
        req: HeartbeatRequestPayload,
        group_id: u64,
    ) -> Result<HeartbeatReply, PxReplicaError> {
        let rpc_endpoint = destination.endpoint;
        let target_replica_id = destination.replica_id;
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let target_cluster_id = self.cluster_id.as_deref().map(|id| builder.create_string(id));
        let args = FBHeartbeatRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            version: 1,
            group_id,
            term: req.term,
            leader_id: req.leader_id,
            prev_log_slot: req.prev_log_slot,
            prev_log_term: req.prev_log_term,
            committed_safe_slot: req.committed_safe_slot,
            lease_grant_until_ms_mono: req.lease_grant_until_ms_mono,
            t_send_ms_mono: req.t_send_ms_mono,
            target_replica_id,
            target_cluster_id,
        };
        let fb_req = FBHeartbeatRequest::create(&mut builder, &args);
        builder.finish(fb_req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EHeartbeatRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(|e| self.map_rpc_err(e, rpc_endpoint, conn.generation()))?;
        let resp = fut
            .await
            .map_err(|e| self.map_rpc_err(e, rpc_endpoint, conn.generation()))?;
        let ctrl = resp
            .control
            .ok_or_else(|| PxReplicaError::Internal("heartbeat response missing control buffer".into()))?;
        let r = FBHeartbeatResponseRef::new(ctrl.bytes());
        if !r.valid() {
            return Err(PxReplicaError::Internal("heartbeat response malformed".into()));
        }
        check_ret_code(r.ret_code(), r.error_msg())?;
        Ok(HeartbeatReply {
            term: r.term(),
            success: r.success(),
            contiguous_chosen: r.contiguous_chosen(),
            last_chosen_term: r.last_chosen_term(),
            contiguous_applied: r.contiguous_applied(),
            highest_seen_slot: r.highest_seen_slot(),
            durable_snapshot_slot: r.durable_snapshot_slot(),
        })
    }

=======
>>>>>>> 0f5d94a8 (Reclaim snapshot-backed Paxos history with retained readers)
    /// Send a `StepDown` request via crowdb-rpc.
    pub async fn send_step_down(
        &self,
        destination: ReplicaDestination<'_>,
        req: &StepDownRequestPayload,
        group_id: u64,
    ) -> Result<StepDownReply, PxReplicaError> {
        let rpc_endpoint = destination.endpoint;
        let target_replica_id = destination.replica_id;
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let reason = builder.create_string(&req.reason);
        let target_cluster_id = self.cluster_id.as_deref().map(|id| builder.create_string(id));
        let args = FBStepDownRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            version: 1,
            group_id,
            term: req.term,
            target_leader_id: req.target_leader_id,
            reason: Some(reason),
            target_replica_id,
            target_cluster_id,
        };
        let fb_req = FBStepDownRequest::create(&mut builder, &args);
        builder.finish(fb_req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EStepDownRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(|e| self.map_rpc_err(e, rpc_endpoint, conn.generation()))?;
        let resp = fut
            .await
            .map_err(|e| self.map_rpc_err(e, rpc_endpoint, conn.generation()))?;
        let ctrl = resp
            .control
            .ok_or_else(|| PxReplicaError::Internal("step_down response missing control buffer".into()))?;
        let r = FBStepDownResponseRef::new(ctrl.bytes());
        if !r.valid() {
            return Err(PxReplicaError::Internal("step_down response malformed".into()));
        }
        check_ret_code(r.ret_code(), r.error_msg())?;
        Ok(StepDownReply {
            accepted: r.accepted(),
            current_term: r.current_term(),
            current_leader_id: r.current_leader_id(),
        })
    }

    /// Send a `FetchGap` request via crowdb-rpc.
    pub async fn send_fetch_gap(
        &self,
        destination: ReplicaDestination<'_>,
        group_id: u64,
        slot: u64,
        term: u64,
        leader_id: u64,
    ) -> Result<FetchGapReply, PxReplicaError> {
        let rpc_endpoint = destination.endpoint;
        let target_replica_id = destination.replica_id;
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let target_cluster_id = self.cluster_id.as_deref().map(|id| builder.create_string(id));
        let args = FBFetchGapRequestArgs {
            id: req_id,
            rpc_create_nano: 0,
            version: 1,
            group_id,
            slot,
            term,
            leader_id,
            target_replica_id,
            target_cluster_id,
        };
        let fb_req = FBFetchGapRequest::create(&mut builder, &args);
        builder.finish(fb_req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EFetchGapRequest.0 as u16;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(|e| self.map_rpc_err(e, rpc_endpoint, conn.generation()))?;
        let resp = fut
            .await
            .map_err(|e| self.map_rpc_err(e, rpc_endpoint, conn.generation()))?;
        let ctrl = resp
            .control
            .ok_or_else(|| PxReplicaError::Internal("fetch_gap response missing control buffer".into()))?;
        let r = FBFetchGapResponseRef::new(ctrl.bytes());
        if !r.valid() {
            return Err(PxReplicaError::Internal("fetch_gap response malformed".into()));
        }
        check_ret_code(r.ret_code(), r.error_msg())?;
        let payload = r.payload().map(bytes::Bytes::copy_from_slice).unwrap_or_default();
        Ok(FetchGapReply {
            group_id: r.group_id(),
            slot: r.slot(),
            term: r.term(),
            ballot_round: r.ballot_round(),
            leader_id: r.leader_id(),
            payload,
        })
    }

    /// Send a fire-and-forget `ChosenNotification` via crowdb-rpc. No
    /// reply is expected — the frame is sent with no completion callback.
    pub fn send_chosen(
        &self,
        destination: ReplicaDestination<'_>,
        group_id: u64,
        slot: u64,
        term: u64,
        leader_id: u64,
        ballot_round: u64,
    ) -> Result<(), PxReplicaError> {
        let rpc_endpoint = destination.endpoint;
        let target_replica_id = destination.replica_id;
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let target_cluster_id = self.cluster_id.as_deref().map(|id| builder.create_string(id));
        let args = FBChosenNotificationArgs {
            id: req_id,
            rpc_create_nano: 0,
            version: 1,
            group_id,
            slot,
            term,
            leader_id,
            ballot_round,
            target_replica_id,
            target_cluster_id,
        };
        let fb_req = FBChosenNotification::create(&mut builder, &args);
        builder.finish(fb_req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EChosenNotification.0 as u16;
        self.rpc
            .send(
                &self.server,
                &conn,
                req_id,
                control,
                None,
                msg_type,
                noop_completion(),
                std::ptr::null_mut(),
            )
            .map_err(|e| self.map_rpc_err(e, rpc_endpoint, conn.generation()))
    }

    /// Send a fire-and-forget `BatchChosenNotification` via crowdb-rpc.
    #[allow(clippy::too_many_arguments)]
    pub fn send_batch_chosen(
        &self,
        destination: ReplicaDestination<'_>,
        group_id: u64,
        start_slot: u64,
        end_slot: u64,
        term: u64,
        leader_id: u64,
        ballot_round: u64,
    ) -> Result<(), PxReplicaError> {
        let rpc_endpoint = destination.endpoint;
        let target_replica_id = destination.replica_id;
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let target_cluster_id = self.cluster_id.as_deref().map(|id| builder.create_string(id));
        let args = FBBatchChosenNotificationArgs {
            id: req_id,
            rpc_create_nano: 0,
            version: 1,
            group_id,
            start_slot,
            end_slot,
            term,
            leader_id,
            ballot_round,
            target_replica_id,
            target_cluster_id,
        };
        let fb_req = FBBatchChosenNotification::create(&mut builder, &args);
        builder.finish(fb_req, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let msg_type = FBMsgType::EBatchChosenNotification.0 as u16;
        self.rpc
            .send(
                &self.server,
                &conn,
                req_id,
                control,
                None,
                msg_type,
                noop_completion(),
                std::ptr::null_mut(),
            )
            .map_err(|e| self.map_rpc_err(e, rpc_endpoint, conn.generation()))
    }

    pub(crate) async fn snapshot_begin(
        &self,
        rpc_endpoint: &str,
        group_id: u64,
        max_chunk_bytes: u32,
    ) -> Result<SnapshotBeginReply, SnapshotTransferError> {
        let req_id = self.next_id();
        let conn = self
            .conn_for(rpc_endpoint)
            .map_err(SnapshotTransferError::Transport)?;
        let mut builder = FlatBufferBuilder::new();
        let request = FBSnapshotBeginRequest::create(
            &mut builder,
            &FBSnapshotBeginRequestArgs {
                id: req_id,
                rpc_create_nano: 0,
                group_id,
                max_chunk_bytes,
            },
        );
        builder.finish(request, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let future = self
            .rpc
            .call(
                &self.server,
                &conn,
                req_id,
                control,
                None,
                FBMsgType::ESnapshotBeginRequest.0 as u16,
            )
            .map_err(|error| {
                SnapshotTransferError::Transport(self.map_rpc_err(error, rpc_endpoint, conn.generation()))
            })?;
        let response = future.await.map_err(|error| {
            SnapshotTransferError::Transport(self.map_rpc_err(error, rpc_endpoint, conn.generation()))
        })?;
        let control = response.control.ok_or_else(|| {
            SnapshotTransferError::Protocol("snapshot Begin response missing control".into())
        })?;
        let view = FBSnapshotBeginResponseRef::new(control.bytes());
        if !view.valid() {
            return Err(SnapshotTransferError::Protocol(
                "snapshot Begin response malformed".into(),
            ));
        }
        check_snapshot_ret_code(view.ret_code(), view.error_msg())?;
        Ok(SnapshotBeginReply {
            identity: SnapshotIdentity {
                boot_nonce: view.boot_nonce(),
                session_number: view.session_number(),
            },
            group_id: view.group_id(),
            engine_format: view.engine_format(),
            at_slot: view.at_slot(),
            term_at_slot: view.term_at_slot(),
            membership_epoch: view.membership_epoch(),
            chunk_bytes: view.chunk_bytes(),
            total_bytes: view.total_bytes(),
            final_crc32c: view.final_crc32c(),
        })
    }

    pub(crate) async fn snapshot_read(
        &self,
        rpc_endpoint: &str,
        identity: SnapshotIdentity,
        offset: u64,
    ) -> Result<SnapshotReadReply, SnapshotTransferError> {
        let req_id = self.next_id();
        let conn = self
            .conn_for(rpc_endpoint)
            .map_err(SnapshotTransferError::Transport)?;
        let mut builder = FlatBufferBuilder::new();
        let request = FBSnapshotReadRequest::create(
            &mut builder,
            &FBSnapshotReadRequestArgs {
                id: req_id,
                rpc_create_nano: 0,
                boot_nonce: identity.boot_nonce,
                session_number: identity.session_number,
                offset,
            },
        );
        builder.finish(request, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let future = self
            .rpc
            .call(
                &self.server,
                &conn,
                req_id,
                control,
                None,
                FBMsgType::ESnapshotReadRequest.0 as u16,
            )
            .map_err(|error| {
                SnapshotTransferError::Transport(self.map_rpc_err(error, rpc_endpoint, conn.generation()))
            })?;
        let response = future.await.map_err(|error| {
            SnapshotTransferError::Transport(self.map_rpc_err(error, rpc_endpoint, conn.generation()))
        })?;
        let control = response.control.ok_or_else(|| {
            SnapshotTransferError::Protocol("snapshot Read response missing control".into())
        })?;
        let view = FBSnapshotReadResponseRef::new(control.bytes());
        if !view.valid() {
            return Err(SnapshotTransferError::Protocol(
                "snapshot Read response malformed".into(),
            ));
        }
        check_snapshot_ret_code(view.ret_code(), view.error_msg())?;
        Ok(SnapshotReadReply {
            identity: SnapshotIdentity {
                boot_nonce: view.boot_nonce(),
                session_number: view.session_number(),
            },
            offset: view.offset(),
            payload_crc32c: view.payload_crc32c(),
            done: view.done(),
            data: response.data.map_or_else(Vec::new, |data| data.bytes().to_vec()),
        })
    }

    pub(crate) async fn snapshot_finish(
        &self,
        rpc_endpoint: &str,
        identity: SnapshotIdentity,
        final_offset: u64,
    ) -> Result<(), SnapshotTransferError> {
        let req_id = self.next_id();
        let conn = self
            .conn_for(rpc_endpoint)
            .map_err(SnapshotTransferError::Transport)?;
        let mut builder = FlatBufferBuilder::new();
        let request = FBSnapshotFinishRequest::create(
            &mut builder,
            &FBSnapshotFinishRequestArgs {
                id: req_id,
                rpc_create_nano: 0,
                boot_nonce: identity.boot_nonce,
                session_number: identity.session_number,
                final_offset,
            },
        );
        builder.finish(request, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let future = self
            .rpc
            .call(
                &self.server,
                &conn,
                req_id,
                control,
                None,
                FBMsgType::ESnapshotFinishRequest.0 as u16,
            )
            .map_err(|error| {
                SnapshotTransferError::Transport(self.map_rpc_err(error, rpc_endpoint, conn.generation()))
            })?;
        let response = future.await.map_err(|error| {
            SnapshotTransferError::Transport(self.map_rpc_err(error, rpc_endpoint, conn.generation()))
        })?;
        let control = response.control.ok_or_else(|| {
            SnapshotTransferError::Protocol("snapshot Finish response missing control".into())
        })?;
        let view = FBSnapshotFinishResponseRef::new(control.bytes());
        if !view.valid()
            || view.boot_nonce() != identity.boot_nonce
            || view.session_number() != identity.session_number
        {
            return Err(SnapshotTransferError::Protocol(
                "snapshot Finish response identity mismatch".into(),
            ));
        }
        check_snapshot_ret_code(view.ret_code(), view.error_msg())
    }

    pub(crate) async fn snapshot_abort(&self, rpc_endpoint: &str, identity: SnapshotIdentity) {
        let Ok(conn) = self.conn_for(rpc_endpoint) else {
            return;
        };
        let req_id = self.next_id();
        let mut builder = FlatBufferBuilder::new();
        let request = FBSnapshotAbortRequest::create(
            &mut builder,
            &FBSnapshotAbortRequestArgs {
                id: req_id,
                rpc_create_nano: 0,
                boot_nonce: identity.boot_nonce,
                session_number: identity.session_number,
            },
        );
        builder.finish(request, None);
        let control = Buffer::from_bytes(builder.finished_data());
        let Ok(future) = self.rpc.call(
            &self.server,
            &conn,
            req_id,
            control,
            None,
            FBMsgType::ESnapshotAbortRequest.0 as u16,
        ) else {
            return;
        };
        let Ok(response) = future.await else {
            return;
        };
        let Some(control) = response.control else {
            return;
        };
        let view = FBSnapshotAbortResponseRef::new(control.bytes());
        let _ = view.valid() && view.ret_code() == FBKvRetCode::Success;
    }

    /// Test-only: send a frame with arbitrary control bytes and a
    /// caller-chosen `msg_type`, bypassing the flatbuffer build step.
    /// Returns the raw `Response` so the test can inspect the control
    /// buffer. Used by R120 to verify the server's deserialization
    /// guard rejects malformed `EAcceptRequest` frames.
    #[cfg(feature = "test-util")]
    pub async fn send_raw_request(
        &self,
        rpc_endpoint: &str,
        msg_type: u16,
        control: Buffer,
    ) -> Result<crowdb_rpc_ffi::Response, PxReplicaError> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let fut = self
            .rpc
            .call(&self.server, &conn, req_id, control, None, msg_type)
            .map_err(|e| self.map_rpc_err(e, rpc_endpoint, conn.generation()))?;
        let resp = fut
            .await
            .map_err(|e| self.map_rpc_err(e, rpc_endpoint, conn.generation()))?;
        Ok(resp)
    }

    /// Classify a transport error without requiring a live connection.
    #[cfg(feature = "test-util")]
    #[must_use]
    pub fn classify_error_for_tests(error: RpcError) -> PxReplicaError {
        rpc_error_to_px(error)
    }

    /// Report whether a transport error proves the selected connection dead.
    #[cfg(feature = "test-util")]
    #[must_use]
    pub fn error_invalidates_connection_for_tests(error: RpcError) -> bool {
        rpc_error_invalidates_connection(error)
    }
}

impl Default for PxRpcTransport {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SnapshotIdentity {
    pub boot_nonce: u64,
    pub session_number: u64,
}

#[derive(Debug)]
pub(crate) struct SnapshotBeginReply {
    pub identity: SnapshotIdentity,
    pub group_id: u64,
    pub engine_format: u8,
    pub at_slot: u64,
    pub term_at_slot: u64,
    pub membership_epoch: u64,
    pub chunk_bytes: u32,
    pub total_bytes: u64,
    pub final_crc32c: u32,
}

#[derive(Debug)]
pub(crate) struct SnapshotReadReply {
    pub identity: SnapshotIdentity,
    pub offset: u64,
    pub payload_crc32c: u32,
    pub done: bool,
    pub data: Vec<u8>,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum SnapshotTransferError {
    #[error("snapshot transport: {0}")]
    Transport(PxReplicaError),
    #[error("snapshot session not found: {0}")]
    NotFound(String),
    #[error("snapshot session expired: {0}")]
    Expired(String),
    #[error("snapshot offset rejected: {0}")]
    InvalidOffset(String),
    #[error("snapshot source at capacity: {0}")]
    Backpressure(String),
    #[error("snapshot topology changed: {0}")]
    TopologyChanged(String),
    #[error("snapshot integrity failure: {0}")]
    Integrity(String),
    #[error("snapshot protocol failure: {0}")]
    Protocol(String),
}

mod connection;
use connection::{
    check_ret_code, check_snapshot_ret_code, normalize_endpoint, parse_endpoint,
    rpc_error_invalidates_connection, rpc_error_to_px,
};

pub use connection::ReplicaDestination;
