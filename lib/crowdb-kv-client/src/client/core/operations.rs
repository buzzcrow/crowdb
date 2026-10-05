// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{now_ms, BatchOp, CrowdbKvClient, GetOutcome, WriteOutcome};
use crate::error::{Error, Result};
use bytes::Bytes;
use crowdb_kv::rpc::{KvBatchItem, ReadMode};
use std::sync::atomic::Ordering;
use std::time::Instant;

impl CrowdbKvClient {
    /// `Put` a single key/value.
    ///
    /// `ids`: override `(client_id, seq)` for callers that manage their own
    /// idempotency keys (e.g. `crowdb-console`'s HTTP API, which lets an
    /// external caller supply these explicitly); `None` auto-generates and
    /// reuses this client's own `client_id` plus a fresh `seq` across all
    /// retries of this call.
    ///
    /// # Errors
    /// See [`Error`]. Retries transparently; returns `Err` only once the
    /// retry budget is exhausted or discovery fails outright.
    pub async fn put(
        &self,
        store_id: u64,
        group_id: u64,
        key: &[u8],
        value: &[u8],
        ids: Option<(u64, u64)>,
    ) -> Result<WriteOutcome> {
        let (client_id, seq) =
            ids.unwrap_or_else(|| (self.client_id, self.next_seq.fetch_add(1, Ordering::Relaxed)));
        let mut endpoint = self.resolve_leader(store_id, group_id).await?;
        let Some(t) = &self.rpc_transport else {
            return Err(Error::Transport {
                endpoint: endpoint.clone(),
                status: "rpc transport not set".into(),
            });
        };
        let mut attempts = 0u32;
        let mut redirects = crate::client::retry::Redirects::default();
        let mut backoff = self.retry.backoff_base;
        loop {
            let request_id = self.request_ids.next().as_u64();
            let request_create_ms = now_ms();
            let t0 = Instant::now();
            let send_result: std::result::Result<crowdb_kv::rpc::KvResponse, String> = t
                .send_put(
                    &endpoint,
                    key,
                    value,
                    client_id,
                    seq,
                    request_id,
                    request_create_ms,
                    group_id,
                )
                .await
                .map_err(|e| e.to_string());
            match send_result {
                Ok(resp) => {
                    if resp.ok {
                        self.record_write(store_id, group_id, resp.revision);
                        self.metrics.record_put_latency(t0.elapsed().as_micros() as u64);
                        return Ok(WriteOutcome {
                            revision: resp.revision,
                            request_id: resp.request_id,
                        });
                    }
                    self.metrics.record_put_error();
                    if let Some(new_endpoint) = self
                        .follow_not_leader(store_id, group_id, &resp, &mut redirects)
                        .await?
                    {
                        self.metrics.record_not_leader_hint();
                        self.metrics.on_leader_error(store_id, group_id, &endpoint);
                        self.metrics
                            .on_leader_resolved(store_id, group_id, &new_endpoint, "not_leader_hint");
                        endpoint = new_endpoint;
                        continue;
                    }
                    attempts = self.count_other(attempts, &resp.error)?;
                    if Self::is_unknown_leader(resp.error_code, &resp.error) {
                        self.metrics.on_leader_error(store_id, group_id, &endpoint);
                        endpoint = self.wait_and_refresh_leader(store_id, group_id, &endpoint).await;
                        self.metrics
                            .on_leader_resolved(store_id, group_id, &endpoint, "unknown_leader");
                    }
                }
                Err(msg) => {
                    self.metrics.record_put_error();
                    self.metrics.record_transport_error();
                    self.metrics.on_leader_error(store_id, group_id, &endpoint);
                    endpoint = self
                        .handle_transport_err(store_id, group_id, &endpoint, &mut backoff)
                        .await;
                    self.metrics
                        .on_leader_resolved(store_id, group_id, &endpoint, "transport_error");
                    attempts = self.count_other(attempts, &msg)?;
                }
            }
        }
    }

    /// `Get` a single key.
    ///
    /// # Errors
    /// See [`Error`].
    #[allow(clippy::too_many_lines)]
    pub async fn get(
        &self,
        store_id: u64,
        group_id: u64,
        key: &[u8],
        read_mode: ReadMode,
        min_slot: Option<u64>,
    ) -> Result<GetOutcome> {
        let min_slot = self.resolve_min_slot(store_id, group_id, read_mode, min_slot);
        let mut endpoint = self.resolve_read_endpoint(store_id, group_id, read_mode).await?;
        let Some(t) = &self.rpc_transport else {
            return Err(Error::Transport {
                endpoint: endpoint.clone(),
                status: "rpc transport not set".into(),
            });
        };
        let mut attempts = 0u32;
        let mut redirects = crate::client::retry::Redirects::default();
        let mut backoff = self.retry.backoff_base;
        loop {
            let request_id = self.request_ids.next().as_u64();
            let request_create_ms = now_ms();
            let t0 = Instant::now();
            let _in_flight = self.incr_in_flight(store_id, group_id, &endpoint);
            let send_result: std::result::Result<crowdb_kv::rpc::KvResponse, String> = t
                .send_get(
                    &endpoint,
                    key,
                    request_id,
                    request_create_ms,
                    group_id,
                    read_mode,
                    min_slot,
                )
                .await
                .map_err(|e| e.to_string());
            match send_result {
                Ok(resp) => {
                    let elapsed_ms = t0.elapsed().as_millis();
                    if elapsed_ms > 500 {
                        tracing::warn!(
                            s = store_id,
                            g = group_id,
                            replica = %endpoint,
                            attempt = attempts,
                            elapsed_ms,
                            "kv get: slow response"
                        );
                    }
                    self.record_endpoint_rtt(store_id, group_id, &endpoint, t0.elapsed().as_micros() as u64);
                    // Follow a `NotLeaderHint` before checking `not_found`/`ok`:
                    // a linearizable read forwarded from a stale leader can
                    // return `not_found=true` alongside a hint (the server
                    // serves a stale local read + hint when leader-forward
                    // fails). Returning `NotFound` here would discard the
                    // redirect and surface stale data to the caller.
                    if let Some(new_endpoint) = self
                        .follow_not_leader(store_id, group_id, &resp, &mut redirects)
                        .await?
                    {
                        self.metrics.record_get_error();
                        self.metrics.record_not_leader_hint();
                        self.metrics.on_leader_error(store_id, group_id, &endpoint);
                        self.metrics
                            .on_leader_resolved(store_id, group_id, &new_endpoint, "not_leader_hint");
                        // A `MinSlot` read distributed to a follower
                        // that hasn't applied `min_slot` redirects to
                        // the leader here — count the distribution
                        // fallback so operators can confirm the rate
                        // stays low.
                        if read_mode == ReadMode::MinSlot && self.read_endpoint_policy.is_distributed() {
                            self.metrics.record_read_endpoint_fallback();
                        }
                        endpoint = new_endpoint;
                        continue;
                    }
                    if resp.not_found {
                        self.metrics.record_get_latency(t0.elapsed().as_micros() as u64);
                        return Ok(GetOutcome::NotFound);
                    }
                    if resp.ok {
                        self.metrics.record_get_latency(t0.elapsed().as_micros() as u64);
                        return Ok(GetOutcome::Found {
                            value: resp.value,
                            revision: resp.revision,
                        });
                    }
                    self.metrics.record_get_error();
                    attempts = self.count_other(attempts, &resp.error)?;
                    if Self::is_unknown_leader(resp.error_code, &resp.error) {
                        self.metrics.on_leader_error(store_id, group_id, &endpoint);
                        endpoint = self.wait_and_refresh_leader(store_id, group_id, &endpoint).await;
                        self.metrics
                            .on_leader_resolved(store_id, group_id, &endpoint, "unknown_leader");
                    }
                }
                Err(msg) => {
                    tracing::warn!(
                        s = store_id,
                        g = group_id,
                        replica = %endpoint,
                        attempt = attempts,
                        error = %msg,
                        elapsed_ms = t0.elapsed().as_millis(),
                        "kv get: transport error"
                    );
                    self.metrics.record_get_error();
                    self.metrics.record_transport_error();
                    self.metrics.on_leader_error(store_id, group_id, &endpoint);
                    endpoint = self
                        .handle_transport_err(store_id, group_id, &endpoint, &mut backoff)
                        .await;
                    self.metrics
                        .on_leader_resolved(store_id, group_id, &endpoint, "transport_error");
                    attempts = self.count_other(attempts, &msg)?;
                }
            }
        }
    }

    /// `Delete` a single key. `not_found` is reported as a benign
    /// `WriteOutcome { revision: 0,.. }`, matching `Put`'s idempotent-retry
    /// shape. `ids` overrides `(client_id, seq)`; see [`Self::put`].
    ///
    /// # Errors
    /// See [`Error`].
    #[allow(clippy::too_many_lines)]
    pub async fn delete(
        &self,
        store_id: u64,
        group_id: u64,
        key: &[u8],
        ids: Option<(u64, u64)>,
    ) -> Result<WriteOutcome> {
        let (client_id, seq) =
            ids.unwrap_or_else(|| (self.client_id, self.next_seq.fetch_add(1, Ordering::Relaxed)));
        let mut endpoint = self.resolve_leader(store_id, group_id).await?;
        let Some(t) = &self.rpc_transport else {
            return Err(Error::Transport {
                endpoint: endpoint.clone(),
                status: "rpc transport not set".into(),
            });
        };
        let mut attempts = 0u32;
        let mut redirects = crate::client::retry::Redirects::default();
        let mut backoff = self.retry.backoff_base;
        loop {
            let request_id = self.request_ids.next().as_u64();
            let request_create_ms = now_ms();
            let t0 = Instant::now();
            let send_result: std::result::Result<crowdb_kv::rpc::KvResponse, String> = t
                .send_delete(
                    &endpoint,
                    key,
                    client_id,
                    seq,
                    request_id,
                    request_create_ms,
                    group_id,
                )
                .await
                .map_err(|e| e.to_string());
            match send_result {
                Ok(resp) => {
                    // Follow a `NotLeaderHint` before checking `not_found`/`ok`:
                    // a stale leader can return `not_found=true` with a hint,
                    // and treating it as a benign idempotent delete would
                    // silently drop the redirect.
                    if let Some(new_endpoint) = self
                        .follow_not_leader(store_id, group_id, &resp, &mut redirects)
                        .await?
                    {
                        self.metrics.record_delete_error();
                        self.metrics.record_not_leader_hint();
                        self.metrics.on_leader_error(store_id, group_id, &endpoint);
                        self.metrics
                            .on_leader_resolved(store_id, group_id, &new_endpoint, "not_leader_hint");
                        endpoint = new_endpoint;
                        continue;
                    }
                    if resp.not_found {
                        self.metrics
                            .record_delete_latency(t0.elapsed().as_micros() as u64);
                        return Ok(WriteOutcome {
                            revision: 0,
                            request_id: resp.request_id,
                        });
                    }
                    if resp.ok {
                        self.record_write(store_id, group_id, resp.revision);
                        self.metrics
                            .record_delete_latency(t0.elapsed().as_micros() as u64);
                        return Ok(WriteOutcome {
                            revision: resp.revision,
                            request_id: resp.request_id,
                        });
                    }
                    self.metrics.record_delete_error();
                    attempts = self.count_other(attempts, &resp.error)?;
                    if Self::is_unknown_leader(resp.error_code, &resp.error) {
                        self.metrics.on_leader_error(store_id, group_id, &endpoint);
                        endpoint = self.wait_and_refresh_leader(store_id, group_id, &endpoint).await;
                        self.metrics
                            .on_leader_resolved(store_id, group_id, &endpoint, "unknown_leader");
                    }
                }
                Err(msg) => {
                    self.metrics.record_delete_error();
                    self.metrics.record_transport_error();
                    self.metrics.on_leader_error(store_id, group_id, &endpoint);
                    endpoint = self
                        .handle_transport_err(store_id, group_id, &endpoint, &mut backoff)
                        .await;
                    self.metrics
                        .on_leader_resolved(store_id, group_id, &endpoint, "transport_error");
                    attempts = self.count_other(attempts, &msg)?;
                }
            }
        }
    }

    /// Atomically apply a batch of `Put`/`Delete` ops at one slot.
    ///
    /// # Errors
    /// See [`Error`].
    #[allow(clippy::too_many_lines)]
    pub async fn batch_write(&self, store_id: u64, group_id: u64, ops: &[BatchOp]) -> Result<WriteOutcome> {
        let seq = self.next_seq.fetch_add(1, Ordering::Relaxed);
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
        let mut endpoint = self.resolve_leader(store_id, group_id).await?;
        let Some(t) = &self.rpc_transport else {
            return Err(Error::Transport {
                endpoint: endpoint.clone(),
                status: "rpc transport not set".into(),
            });
        };
        let mut attempts = 0u32;
        let mut redirects = crate::client::retry::Redirects::default();
        let mut backoff = self.retry.backoff_base;
        loop {
            let request_id = self.request_ids.next().as_u64();
            let request_create_ms = now_ms();
            let t0 = Instant::now();
            let send_result: std::result::Result<crowdb_kv::rpc::KvResponse, String> = t
                .send_batch_write(
                    &endpoint,
                    &items,
                    self.client_id,
                    seq,
                    request_id,
                    request_create_ms,
                    group_id,
                )
                .await
                .map_err(|e| e.to_string());
            match send_result {
                Ok(resp) => {
                    if resp.ok {
                        self.record_write(store_id, group_id, resp.revision);
                        self.metrics
                            .record_batch_write_latency(t0.elapsed().as_micros() as u64);
                        return Ok(WriteOutcome {
                            revision: resp.revision,
                            request_id: resp.request_id,
                        });
                    }
                    self.metrics.record_batch_write_error();
                    if let Some(new_endpoint) = self
                        .follow_not_leader(store_id, group_id, &resp, &mut redirects)
                        .await?
                    {
                        self.metrics.record_not_leader_hint();
                        self.metrics.on_leader_error(store_id, group_id, &endpoint);
                        self.metrics
                            .on_leader_resolved(store_id, group_id, &new_endpoint, "not_leader_hint");
                        endpoint = new_endpoint;
                        continue;
                    }
                    attempts = self.count_other(attempts, &resp.error)?;
                    if Self::is_unknown_leader(resp.error_code, &resp.error) {
                        self.metrics.on_leader_error(store_id, group_id, &endpoint);
                        endpoint = self.wait_and_refresh_leader(store_id, group_id, &endpoint).await;
                        self.metrics
                            .on_leader_resolved(store_id, group_id, &endpoint, "unknown_leader");
                    }
                }
                Err(msg) => {
                    self.metrics.record_batch_write_error();
                    self.metrics.record_transport_error();
                    self.metrics.on_leader_error(store_id, group_id, &endpoint);
                    endpoint = self
                        .handle_transport_err(store_id, group_id, &endpoint, &mut backoff)
                        .await;
                    self.metrics
                        .on_leader_resolved(store_id, group_id, &endpoint, "transport_error");
                    attempts = self.count_other(attempts, &msg)?;
                }
            }
        }
    }
}
