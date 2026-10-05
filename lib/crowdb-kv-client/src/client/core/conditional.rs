// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{now_ms, BatchOp, CrowdbKvClient, WriteOutcome};
use crate::error::{Error, Result};
use bytes::Bytes;
use crowdb_kv::rpc::{KvBatchItem, KvErrorCode};
use std::sync::atomic::Ordering;

impl CrowdbKvClient {
    /// Conditionally put `key` when its current revision equals
    /// `expected_revision`. Revision zero means create-if-absent.
    ///
    /// # Errors
    ///
    /// Returns `CasFailed` on a revision mismatch, `CasBusy` on same-key
    /// contention, and `OutcomeUnknown` after an ambiguous dispatch.
    pub async fn put_cas(
        &self,
        store_id: u64,
        group_id: u64,
        key: &[u8],
        value: &[u8],
        expected_revision: u64,
    ) -> Result<WriteOutcome> {
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
                .send_put_cas(
                    &endpoint,
                    key,
                    value,
                    expected_revision,
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

    /// Atomically apply a batch when `precondition_key` has the expected
    /// revision. The guarded key must occur in the batch.
    ///
    /// # Errors
    ///
    /// Returns a conditional outcome error or a routing/server error. A batch
    /// that does not mutate its precondition key is rejected locally.
    pub async fn batch_write_cas(
        &self,
        store_id: u64,
        group_id: u64,
        ops: &[BatchOp],
        precondition_key: &[u8],
        expected_revision: u64,
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
        if !items.iter().any(|item| item.key.as_ref() == precondition_key) {
            return Err(Error::Server(
                "conditional batch must mutate its precondition key".into(),
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
                .send_batch_write_cas(
                    &endpoint,
                    &items,
                    precondition_key,
                    expected_revision,
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
