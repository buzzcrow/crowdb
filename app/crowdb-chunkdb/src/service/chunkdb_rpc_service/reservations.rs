// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Reserved-strip RPC parsing and lifecycle dispatch.

use crowdb_protocol::chunkdb::rpc::StripReservationAction;

use super::{
    submit_reservation_result, Arc, ChunkId, ChunkdbRpcService, FBMsgType, FBMutateStripReservationRequest,
    FBReserveStripGroupRequest, FBStripReservationAction, RequestGuard, RpcServer, ServerRequest,
};
use crate::lifecycle::{ReservationFence, ReservationUpdate, ReserveGroupSpec};

impl ChunkdbRpcService {
    pub(super) fn handle_reserve_strip_group(
        &self,
        request: ServerRequest,
        server: &Arc<RpcServer>,
        mut metrics: RequestGuard,
    ) {
        let request_id = request.request_id;
        let create_nano = request.rpc_create_nano;
        let connection = request.conn_handle as usize;
        let server = Arc::clone(server);
        let handler = Arc::clone(&self.handler);
        self.rt.spawn(async move {
            let parsed = flatbuffers::root::<FBReserveStripGroupRequest>(request.control())
                .map_err(|_| "invalid reserve strip group request")
                .and_then(|value| {
                    let chunk_id = value.chunk_id().map(parse_id).ok_or("missing chunk_id")?;
                    let group_id = value.group_id().map(parse_id).ok_or("missing group_id")?;
                    Ok((
                        chunk_id,
                        group_id,
                        ReservationFence {
                            expected_modify_ts: value.expected_modify_ts(),
                            writer_epoch: value.writer_epoch(),
                            lease_generation: value.lease_generation(),
                            lease_ms: value.lease_ms(),
                        },
                        ReserveGroupSpec {
                            strip_size: value.strip_size(),
                            strip_count: value.strip_count(),
                            copy_count: value.copy_count(),
                            conversion_data_num: value.conversion_data_num(),
                            conversion_code_num: value.conversion_code_num(),
                        },
                    ))
                });
            let result = match parsed {
                Ok((chunk_id, group_id, fence, spec)) => {
                    handler
                        .reserve_strip_group(&chunk_id, &group_id, fence, spec)
                        .await
                }
                Err(message) => Err(crate::lifecycle::LifecycleError::InvalidRequest(message.into())),
            };
            if result.is_ok() {
                metrics.mark_success();
            } else if let Err(error) = &result {
                tracing::warn!(%error, "reserve strip group failed");
            }
            submit_reservation_result(
                &server,
                connection as *mut _,
                request_id,
                create_nano,
                FBMsgType::EReserveStripGroupResponse.0 as u16,
                result,
            );
        });
    }

    pub(super) fn handle_mutate_strip_reservation(
        &self,
        request: ServerRequest,
        server: &Arc<RpcServer>,
        mut metrics: RequestGuard,
    ) {
        let request_id = request.request_id;
        let create_nano = request.rpc_create_nano;
        let connection = request.conn_handle as usize;
        let server = Arc::clone(server);
        let handler = Arc::clone(&self.handler);
        self.rt.spawn(async move {
            let parsed = flatbuffers::root::<FBMutateStripReservationRequest>(request.control())
                .map_err(|_| "invalid mutate strip reservation request")
                .and_then(|value| {
                    let chunk_id = value.chunk_id().map(parse_id).ok_or("missing chunk_id")?;
                    let group_id = value.group_id().map(parse_id).ok_or("missing group_id")?;
                    let action = parse_action(value.action()).ok_or("invalid reservation action")?;
                    Ok((
                        chunk_id,
                        group_id,
                        ReservationFence {
                            expected_modify_ts: value.expected_modify_ts(),
                            writer_epoch: value.writer_epoch(),
                            lease_generation: value.lease_generation(),
                            lease_ms: value.lease_ms(),
                        },
                        ReservationUpdate {
                            strip_sequence: value.strip_sequence(),
                            action,
                            acknowledged_cursor: value.acknowledged_cursor(),
                            closed_strip_sequence: (value.closed_strip_sequence() != u32::MAX)
                                .then(|| value.closed_strip_sequence()),
                        },
                    ))
                });
            let result = match parsed {
                Ok((chunk_id, group_id, fence, update)) => {
                    handler
                        .mutate_strip_reservation(&chunk_id, &group_id, fence, update)
                        .await
                }
                Err(message) => Err(crate::lifecycle::LifecycleError::InvalidRequest(message.into())),
            };
            if result.is_ok() {
                metrics.mark_success();
            } else if let Err(error) = &result {
                if let Ok((chunk_id, group_id, fence, update)) = parsed {
                    tracing::warn!(
                        %error,
                        ?chunk_id,
                        ?group_id,
                        expected_modify_ts = fence.expected_modify_ts,
                        strip_sequence = update.strip_sequence,
                        action = ?update.action,
                        "reservation mutation failed"
                    );
                } else {
                    tracing::warn!(%error, "reservation mutation failed");
                }
            }
            submit_reservation_result(
                &server,
                connection as *mut _,
                request_id,
                create_nano,
                FBMsgType::EMutateStripReservationResponse.0 as u16,
                result,
            );
        });
    }
}

fn parse_id(value: &crowdb_protocol::chunkdb_fb::FBInt128) -> ChunkId {
    ChunkId {
        high: value.high(),
        low: value.low(),
    }
}

fn parse_action(action: FBStripReservationAction) -> Option<StripReservationAction> {
    match action {
        FBStripReservationAction::Consume => Some(StripReservationAction::Consume),
        FBStripReservationAction::Confirm => Some(StripReservationAction::Confirm),
        FBStripReservationAction::Cancel => Some(StripReservationAction::Cancel),
        FBStripReservationAction::Renew => Some(StripReservationAction::Renew),
        FBStripReservationAction::Publish => Some(StripReservationAction::Publish),
        _ => None,
    }
}
