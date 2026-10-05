// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::group_operations::{KvGroupOperationError, KvGroupWrite, KvRequestIdentity};
use super::px_kv_store::PxKvStore;
use crate::rpc::KvResponse;
use bytes::Bytes;

impl PxKvStore {
    #[allow(clippy::too_many_arguments)]
    pub(crate) async fn propose_owner_write_and_respond(
        &self,
        group_id: u64,
        payload: Vec<u8>,
        key: Bytes,
        expected_value: Bytes,
        record_condition: Option<(Bytes, u64)>,
        client_id: u64,
        seq: u64,
        request_id: u64,
        request_create_ms: u64,
    ) -> KvResponse {
        let Some(operations) = self.group_operations(group_id) else {
            return super::px_kv_store::missing_group_response(request_id, request_create_ms);
        };
        let result = operations
            .owner_write_encoded(
                payload,
                key,
                expected_value,
                record_condition,
                KvRequestIdentity {
                    client_id,
                    sequence: seq,
                },
            )
            .await;
        write_response(result, request_id, request_create_ms)
    }
}

pub(super) fn write_response(
    result: Result<KvGroupWrite, KvGroupOperationError>,
    request_id: u64,
    request_create_ms: u64,
) -> KvResponse {
    match result {
        Ok(write) => crate::rpc::KvResponse::ok_chosen(write.chosen_slot, request_id, request_create_ms),
        Err(KvGroupOperationError::NotLeader { leader_hint }) => {
            crate::rpc::KvResponse::not_leader(leader_hint, request_id, request_create_ms)
        }
        Err(KvGroupOperationError::CompareFailed { current_revision }) => crate::rpc::KvResponse::cas_error(
            crate::rpc::KvErrorCode::KvErrorCasFailed,
            current_revision,
            "compare-and-set precondition failed",
            request_id,
            request_create_ms,
        ),
        Err(KvGroupOperationError::Busy | KvGroupOperationError::CompareBusy) => {
            crate::rpc::KvResponse::cas_error(
                crate::rpc::KvErrorCode::KvErrorCasBusy,
                0,
                "compare-and-set admission busy",
                request_id,
                request_create_ms,
            )
        }
        Err(
            KvGroupOperationError::OutcomeUnknown
            | KvGroupOperationError::Unavailable(_)
            | KvGroupOperationError::Internal(_),
        ) => crate::rpc::KvResponse::cas_error(
            crate::rpc::KvErrorCode::KvErrorOutcomeUnknown,
            0,
            "compare-and-set outcome is unknown",
            request_id,
            request_create_ms,
        ),
    }
}
