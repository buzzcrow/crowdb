// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{check_ret_code, PxRpcTransport};
use crate::cluster::replica::{HeartbeatReply, HeartbeatRequestPayload, PxReplicaError};
use crowdb_protocol::{
    fb::FBMsgType,
    fb_wrappers::kv_consensus::FBHeartbeatResponseRef,
    kv_consensus_fb::{FBHeartbeatRequest, FBHeartbeatRequestArgs},
};
use crowdb_rpc_ffi::Buffer;
use flatbuffers::FlatBufferBuilder;

impl PxRpcTransport {
    /// Send a `Heartbeat` request via crowdb-rpc.
    pub async fn send_heartbeat(
        &self,
        rpc_endpoint: &str,
        req: HeartbeatRequestPayload,
        group_id: u64,
    ) -> Result<HeartbeatReply, PxReplicaError> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
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
            gc_before_slot: req.gc_before_slot,
            gc_membership_epoch: req.gc_membership_epoch,
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
}
