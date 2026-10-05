// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{
    parse_kv_response, Buffer, Error, FBKvBatchItem, FBKvBatchItemArgs, FBKvBatchWriteRequest,
    FBKvBatchWriteRequestArgs, FBKvRevisionPrecondition, FBKvRevisionPreconditionArgs, FBMsgType,
    FlatBufferBuilder, KvResponse, KvRpcTransport, Result,
};
use crowdb_protocol::kv_client_fb::{FBKvOwnerFence, FBKvOwnerFenceArgs};

impl KvRpcTransport {
    #[allow(clippy::too_many_arguments)]
    pub async fn send_batch_write_owned(
        &self,
        rpc_endpoint: &str,
        items: &[crowdb_kv::rpc::KvBatchItem],
        precondition_key: &[u8],
        expected_value: &[u8],
        record_condition: Option<(&[u8], u64)>,
        client_id: u64,
        seq: u64,
        request_id: u64,
        request_create_ms: u64,
        group_id: u64,
    ) -> Result<KvResponse> {
        let req_id = self.next_id();
        let conn = self.conn_for(rpc_endpoint)?;
        let mut builder = FlatBufferBuilder::new();
        let item_offsets: Vec<_> = items
            .iter()
            .map(|item| {
                let key = builder.create_vector(&item.key);
                let value = builder.create_vector(&item.value);
                FBKvBatchItem::create(
                    &mut builder,
                    &FBKvBatchItemArgs {
                        key: Some(key),
                        value: Some(value),
                        is_delete: item.is_delete,
                    },
                )
            })
            .collect();
        let fb_items = builder.create_vector(&item_offsets);
        let fb_key = builder.create_vector(precondition_key);
        // Old servers ignore the appended owner field but reject this
        // precondition because the batch never mutates the fence key.
        let precondition = FBKvRevisionPrecondition::create(
            &mut builder,
            &FBKvRevisionPreconditionArgs {
                key: Some(fb_key),
                expected_revision: 0,
            },
        );
        let record_precondition = record_condition.map(|(key, expected_revision)| {
            let key = builder.create_vector(key);
            FBKvRevisionPrecondition::create(
                &mut builder,
                &FBKvRevisionPreconditionArgs {
                    key: Some(key),
                    expected_revision,
                },
            )
        });
        let key = builder.create_vector(precondition_key);
        let expected_value = builder.create_vector(expected_value);
        let owner_fence = FBKvOwnerFence::create(
            &mut builder,
            &FBKvOwnerFenceArgs {
                key: Some(key),
                expected_value: Some(expected_value),
                record_precondition,
            },
        );
        let request = FBKvBatchWriteRequest::create(
            &mut builder,
            &FBKvBatchWriteRequestArgs {
                id: req_id,
                rpc_create_nano: 0,
                version: 2,
                items: Some(fb_items),
                seq,
                client_id,
                request_id,
                request_create_ms,
                group_id,
                forwarded: false,
                precondition: Some(precondition),
                owner_fence: Some(owner_fence),
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
                FBMsgType::EKvBatchWriteRequest.0 as u16,
            )
            .map_err(|error| self.map_rpc_err(error, rpc_endpoint, conn.generation()))?;
        let response = future
            .await
            .map_err(|error| self.map_rpc_err(error, rpc_endpoint, conn.generation()))?;
        let control = response.control.ok_or_else(|| Error::Transport {
            endpoint: rpc_endpoint.to_string(),
            status: "batch CAS response missing control buffer".into(),
        })?;
        parse_kv_response(control.bytes())
    }
}
