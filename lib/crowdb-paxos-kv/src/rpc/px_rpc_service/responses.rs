// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    Arc, Buffer, FBAcceptedResponse, FBAcceptedResponseArgs, FBAcceptedValue, FBFetchGapResponse,
    FBFetchGapResponseArgs, FBHeartbeatResponse, FBHeartbeatResponseArgs, FBKvRetCode, FBMsgType,
    FBPreVoteResponse, FBPreVoteResponseArgs, FBPromiseResponse, FBPromiseResponseArgs,
    FBRequestVoteResponse, FBRequestVoteResponseArgs, FBSnapshotAbortResponse, FBSnapshotAbortResponseArgs,
    FBSnapshotBeginResponse, FBSnapshotBeginResponseArgs, FBSnapshotFinishResponse,
    FBSnapshotFinishResponseArgs, FBSnapshotReadResponse, FBSnapshotReadResponseArgs, FBStepDownResponse,
    FBStepDownResponseArgs, FlatBufferBuilder, PxReplicaError, RpcServer, SnapshotRegistration,
    SnapshotSessionError, SnapshotSessionId,
};

#[allow(clippy::too_many_arguments)]
pub(super) fn submit_snapshot_begin(
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
pub(super) fn submit_snapshot_read(
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
pub(super) fn submit_snapshot_close(
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

pub(super) fn snapshot_error(error: &SnapshotSessionError) -> (FBKvRetCode, String) {
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

pub(super) fn px_error_to_ret_code(e: &PxReplicaError) -> (FBKvRetCode, String) {
    match e {
        PxReplicaError::GroupNotFound(_) => (FBKvRetCode::NotFound, e.to_string()),
        PxReplicaError::ShuttingDown => (FBKvRetCode::Unavailable, e.to_string()),
        PxReplicaError::Transport(_) | PxReplicaError::Timeout(_) | PxReplicaError::Backpressure(_) => {
            (FBKvRetCode::Unavailable, e.to_string())
        }
        PxReplicaError::Internal(_) => (FBKvRetCode::Internal, e.to_string()),
    }
}

pub(super) fn submit_error(
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
pub(super) fn submit_fb_response(
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
pub(super) fn finish_promise_response(
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
pub(super) fn build_promise_response(
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
pub(super) fn build_accepted_response(
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
pub(super) fn build_pre_vote_response(
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
pub(super) fn build_request_vote_response(
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
pub(super) fn build_heartbeat_response(
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
pub(super) fn build_step_down_response(
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
pub(super) fn build_fetch_gap_response(
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
