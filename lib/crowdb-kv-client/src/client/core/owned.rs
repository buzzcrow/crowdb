// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{now_ms, BatchOp, CrowdbKvClient, WriteOutcome};
use crate::error::{Error, Result};
use bytes::Bytes;
use crowdb_kv::rpc::{KvBatchItem, KvErrorCode};
use crowdb_protocol::owner_fence::{is_owner_fence_key, valid_owner_fence, valid_owner_fence_scope};
use std::sync::atomic::Ordering;

impl CrowdbKvClient {
    /// Write under a read-only owner identity; KV drains writes only at handover.
    ///
    /// # Errors
    /// Returns stale ownership, record conflicts, leadership or unknown-outcome errors.
    pub async fn batch_write_owned(
        &self,
        store_id: u64,
        group_id: u64,
        ops: &[BatchOp],
        precondition_key: &[u8],
        expected_value: &[u8],
        record_condition: Option<(&[u8], u64)>,
    ) -> Result<WriteOutcome> {
        let items: Vec<KvBatchItem> = ops
            .iter()
            .map(|op| match op {
                BatchOp::Put { key, value } => KvBatchItem {
                    key: key.clone(),
                    value: value.clone(),
                    is_delete: false,
                },
                BatchOp::Delete { key } => KvBatchItem {
                    key: key.clone(),
                    value: Bytes::new(),
                    is_delete: true,
                },
            })
            .collect();
        if items.iter().any(|item| is_owner_fence_key(&item.key))
            || !valid_owner_fence(precondition_key, expected_value)
            || !valid_owner_fence_scope(precondition_key, group_id)
            || record_condition.is_some_and(|(key, _)| !items.iter().any(|item| item.key.as_ref() == key))
        {
            return Err(Error::Server(
                "owner fence must be read-only and identify a DiskGroup or ChunkDB slot".into(),
            ));
        }
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
        let mut endpoint = self.resolve_leader(store_id, group_id).await?;
        let transport = self.rpc_transport.as_ref().ok_or_else(|| Error::Transport {
            endpoint: endpoint.clone(),
            status: "rpc transport not set".into(),
        })?;
        let mut attempts = 0;
        let mut redirects = crate::client::retry::Redirects::default();
        loop {
            let request_id = self.request_ids.next().as_u64();
            let response = transport
                .send_batch_write_owned(
                    &endpoint,
                    &items,
                    precondition_key,
                    expected_value,
                    record_condition,
                    self.client_id,
                    seq,
                    request_id,
                    now_ms(),
                    group_id,
                )
                .await
                .map_err(|_| Error::OutcomeUnknown)?;
            if response.ok {
                self.record_write(store_id, group_id, response.revision);
                return Ok(WriteOutcome {
                    revision: response.revision,
                    request_id: response.request_id,
                });
            }
            if let Some(next) = self
                .follow_not_leader(store_id, group_id, &response, &mut redirects)
                .await?
            {
                endpoint = next;
                continue;
            }
            if Self::is_unknown_leader(response.error_code, &response.error) {
                attempts = self.count_other(attempts, &response.error)?;
                endpoint = self.wait_and_refresh_leader(store_id, group_id, &endpoint).await;
                continue;
            }
            return Err(match KvErrorCode::try_from(response.error_code) {
                Ok(KvErrorCode::KvErrorCasFailed) => Error::CasFailed {
                    current_revision: response.revision,
                },
                Ok(KvErrorCode::KvErrorCasBusy) => Error::CasBusy,
                Ok(KvErrorCode::KvErrorOutcomeUnknown) => Error::OutcomeUnknown,
                _ => Error::Server(response.error),
            });
        }
    }
}
