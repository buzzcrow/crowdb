// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{now_ms, CrowdbKvClient, JournalOp, JournalScanOutcome, ScanDirection, ScanOutcome};
use crate::error::{Error, Result};
use bytes::Bytes;
use crowdb_kv::rpc::ReadMode;
use std::time::Instant;

impl CrowdbKvClient {
    /// Prefix-scan a group's key space. Uses S3-style pagination
    /// (`start_after` + `truncated`): the server applies a byte budget to
    /// each unary response so every page is provably bounded regardless of
    /// value sizes, and this method transparently pages until `!truncated`
    /// or the caller's `limit` is reached. The returned `ScanOutcome.truncated`
    /// flag means "more entries exist beyond the caller's `limit`". When
    /// `keys_only` is true, items carry empty values (no value materialization
    /// on the server); pagination is unchanged.
    ///
    /// # Panics
    /// Panics if the server returns a truncated page with zero items — an
    /// impossible state (truncated implies items were returned but more
    /// remain). The `page_len > 0` guard prevents this.
    ///
    /// # Errors
    /// See [`Error`].
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub async fn scan(
        &self,
        store_id: u64,
        group_id: u64,
        prefix: &[u8],
        start_after: &[u8],
        end_key: &[u8],
        limit: u32,
        read_mode: ReadMode,
        min_slot: Option<u64>,
        keys_only: bool,
        deadline: Option<u64>,
    ) -> Result<ScanOutcome> {
        self.scan_impl(
            store_id,
            group_id,
            prefix,
            start_after,
            end_key,
            limit,
            read_mode,
            min_slot,
            keys_only,
            deadline,
            false,
            0,
            ScanDirection::Forward,
        )
        .await
    }

    /// Complete current-version prefix scan at one fixed contiguous-applied
    /// cutoff. Every returned item carries its record commit slot.
    ///
    /// # Errors
    /// Returns transport, topology, or server errors, including an invalid cutoff.
    #[allow(clippy::too_many_arguments)]
    pub async fn scan_bounded(
        &self,
        store_id: u64,
        group_id: u64,
        prefix: &[u8],
        start_after: &[u8],
        end_key: &[u8],
        limit: u32,
        keys_only: bool,
        deadline: Option<u64>,
    ) -> Result<ScanOutcome> {
        self.scan_impl(
            store_id,
            group_id,
            prefix,
            start_after,
            end_key,
            limit,
            ReadMode::Linearizable,
            None,
            keys_only,
            deadline,
            true,
            0,
            ScanDirection::Forward,
        )
        .await
    }

    /// Complete a bounded scan at an already established cutoff.
    ///
    /// # Errors
    /// Returns transport, topology, or server errors, including a changed cutoff.
    #[allow(clippy::too_many_arguments)]
    pub async fn scan_bounded_at(
        &self,
        store_id: u64,
        group_id: u64,
        prefix: &[u8],
        start_after: &[u8],
        end_key: &[u8],
        limit: u32,
        keys_only: bool,
        deadline: Option<u64>,
        scan_cutoff: u64,
    ) -> Result<ScanOutcome> {
        self.scan_impl(
            store_id,
            group_id,
            prefix,
            start_after,
            end_key,
            limit,
            ReadMode::Linearizable,
            None,
            keys_only,
            deadline,
            true,
            scan_cutoff,
            ScanDirection::Forward,
        )
        .await
    }

    /// Descending counterpart to [`Self::scan`]. `start_before` is an
    /// exclusive upper continuation key.
    ///
    /// # Errors
    /// Returns transport, topology, or server errors.
    #[allow(clippy::too_many_arguments)]
    pub async fn scan_reverse(
        &self,
        store_id: u64,
        group_id: u64,
        prefix: &[u8],
        start_before: &[u8],
        end_key: &[u8],
        limit: u32,
        read_mode: ReadMode,
        min_slot: Option<u64>,
        keys_only: bool,
        deadline: Option<u64>,
    ) -> Result<ScanOutcome> {
        self.scan_impl(
            store_id,
            group_id,
            prefix,
            start_before,
            end_key,
            limit,
            read_mode,
            min_slot,
            keys_only,
            deadline,
            false,
            0,
            ScanDirection::Reverse,
        )
        .await
    }

    /// Descending bounded scan which captures its cutoff on page one.
    ///
    /// # Errors
    /// Returns transport, topology, or server errors.
    #[allow(clippy::too_many_arguments)]
    pub async fn scan_bounded_reverse(
        &self,
        store_id: u64,
        group_id: u64,
        prefix: &[u8],
        start_before: &[u8],
        end_key: &[u8],
        limit: u32,
        keys_only: bool,
        deadline: Option<u64>,
    ) -> Result<ScanOutcome> {
        self.scan_impl(
            store_id,
            group_id,
            prefix,
            start_before,
            end_key,
            limit,
            ReadMode::Linearizable,
            None,
            keys_only,
            deadline,
            true,
            0,
            ScanDirection::Reverse,
        )
        .await
    }

    /// Descending bounded scan at an established cutoff.
    ///
    /// # Errors
    /// Returns transport, topology, or server errors, including a changed cutoff.
    #[allow(clippy::too_many_arguments)]
    pub async fn scan_bounded_at_reverse(
        &self,
        store_id: u64,
        group_id: u64,
        prefix: &[u8],
        start_before: &[u8],
        end_key: &[u8],
        limit: u32,
        keys_only: bool,
        deadline: Option<u64>,
        scan_cutoff: u64,
    ) -> Result<ScanOutcome> {
        self.scan_impl(
            store_id,
            group_id,
            prefix,
            start_before,
            end_key,
            limit,
            ReadMode::Linearizable,
            None,
            keys_only,
            deadline,
            true,
            scan_cutoff,
            ScanDirection::Reverse,
        )
        .await
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn scan_impl(
        &self,
        store_id: u64,
        group_id: u64,
        prefix: &[u8],
        start_after: &[u8],
        end_key: &[u8],
        limit: u32,
        read_mode: ReadMode,
        min_slot: Option<u64>,
        keys_only: bool,
        deadline: Option<u64>,
        bounded: bool,
        scan_cutoff: u64,
        direction: ScanDirection,
    ) -> Result<ScanOutcome> {
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
        // Inner pagination state: collect pages until !truncated or limit reached.
        let mut all_items: Vec<(Bytes, Bytes)> = Vec::new();
        let mut all_commit_slots = Vec::new();
        let mut page_start_after: Vec<u8> = start_after.to_vec();
        // After page 1 of a Linearizable scan returns read_slot = S, switch
        // subsequent pages to MinSlot with min_slot = S. Later pages only need
        // to be at least as fresh as page 1 (a paginated scan was never a
        // single snapshot), so MinSlot with the page-1 floor skips the
        // per-page leader barrier. The leader has S applied and serves locally
        // without the barrier; a redirect mid-scan lands on the leader, which
        // also has S applied.
        let mut page_read_mode = read_mode;
        let mut page_min_slot = min_slot;
        let mut page1_read_slot: Option<u64> = None;
        let mut fixed_scan_cutoff = scan_cutoff;
        loop {
            // Remaining entry-count budget for this page. The server's byte
            // budget may stop the page before this limit is reached; that's
            // fine — `truncated` tells us to fetch the next page.
            let remaining_limit = if limit == 0 {
                0 // unlimited: let the server's byte budget page
            } else {
                limit.saturating_sub(u32::try_from(all_items.len()).unwrap_or(u32::MAX))
            };
            let request_id = self.request_ids.next().as_u64();
            let request_create_ms = now_ms();
            let t0 = Instant::now();
            let _in_flight = self.incr_in_flight(store_id, group_id, &endpoint);
            let send_result: std::result::Result<crowdb_kv::rpc::KvScanResponse, String> = t
                .send_scan_directional(
                    &endpoint,
                    prefix,
                    &page_start_after,
                    end_key,
                    remaining_limit,
                    request_id,
                    request_create_ms,
                    group_id,
                    page_read_mode,
                    page_min_slot,
                    keys_only,
                    false,
                    deadline.unwrap_or(0),
                    bounded,
                    fixed_scan_cutoff,
                    direction,
                )
                .await
                .map_err(|e| e.to_string());
            match send_result {
                Ok(resp) => {
                    self.record_endpoint_rtt(store_id, group_id, &endpoint, t0.elapsed().as_micros() as u64);
                    // Follow a `not_leader_hint` before honoring `ok`: a
                    // linearizable scan that fails mid-forward to the leader
                    // falls back to a stale local read + hint (see
                    // `patch_scan_not_leader_hint` in kv_rpc_service). That
                    // stale read returns `ok=true` with empty items, so
                    // checking `ok` first would surface empty data to the
                    // caller. Mirrors the get/delete hint-first fix.
                    if let Some(next) = self
                        .follow_hint(store_id, group_id, &resp.not_leader_hint, &mut redirects)
                        .await?
                    {
                        if page_read_mode == ReadMode::MinSlot && self.read_endpoint_policy.is_distributed() {
                            self.metrics.record_read_endpoint_fallback();
                        }
                        self.metrics.record_scan_error();
                        self.metrics.record_not_leader_hint();
                        self.metrics.on_leader_error(store_id, group_id, &endpoint);
                        self.metrics
                            .on_leader_resolved(store_id, group_id, &next, "not_leader_hint");
                        endpoint = next;
                        // Resume from the last received key (S3-style
                        // pagination is keyed on `start_after`, so no
                        // duplicates or gaps). Only reset to the caller's
                        // `start_after` when nothing has been received yet.
                        page_start_after = all_items
                            .last()
                            .map_or_else(|| start_after.to_vec(), |(k, _)| k.to_vec());
                        continue;
                    }
                    if resp.ok {
                        redirects = crate::client::retry::Redirects::default();
                        let page_len = resp.items.len();
                        let resp_truncated = resp.truncated;
                        // Capture page-1 read_slot and switch subsequent
                        // pages to MinSlot with that slot as the freshness
                        // floor, skipping the per-page leader barrier.
                        if page1_read_slot.is_none() && read_mode == ReadMode::Linearizable {
                            page1_read_slot = Some(resp.read_slot);
                            page_read_mode = ReadMode::MinSlot;
                            page_min_slot = resp.read_slot;
                        }
                        if bounded {
                            if fixed_scan_cutoff != 0 && resp.scan_cutoff != fixed_scan_cutoff {
                                return Err(Error::Transport {
                                    endpoint: endpoint.clone(),
                                    status: "bounded scan cutoff changed or missing".into(),
                                });
                            }
                            fixed_scan_cutoff = resp.scan_cutoff;
                        }
                        let mut previous_key = all_items.last().map(|(key, _)| key.as_ref());
                        for item in &resp.items {
                            let monotonic = previous_key.map_or(true, |previous| match direction {
                                ScanDirection::Forward => previous < item.key.as_ref(),
                                ScanDirection::Reverse => previous > item.key.as_ref(),
                            });
                            if !monotonic {
                                return Err(Error::Transport {
                                    endpoint: endpoint.clone(),
                                    status: "scan page is repeated or non-monotonic".into(),
                                });
                            }
                            previous_key = Some(item.key.as_ref());
                        }
                        for item in resp.items {
                            all_items.push((item.key, item.value));
                            all_commit_slots.push(item.commit_slot);
                        }
                        // If the page was truncated (server hit byte budget or
                        // entry limit) and we haven't reached the caller's
                        // limit yet, fetch the next page using the last key as
                        // the new start_after. A zero-item page with
                        // truncated=true is a safety stop (avoid infinite loop).
                        if resp_truncated && page_len > 0 && (limit == 0 || all_items.len() < limit as usize)
                        {
                            // Deadline check before fetching the next page: if
                            // the deadline has fired (either the server set
                            // timed_out on this page, or the client-side
                            // deadline has elapsed), stop with a partial result.
                            let server_timed_out = resp.timed_out;
                            let client_timed_out = deadline.is_some_and(|dl| now_ms() >= dl);
                            if server_timed_out || client_timed_out {
                                self.metrics.record_scan_latency(t0.elapsed().as_micros() as u64);
                                return Ok(ScanOutcome {
                                    items: all_items,
                                    commit_slots: all_commit_slots,
                                    truncated: true,
                                    timed_out: true,
                                    read_slot: page1_read_slot.unwrap_or(0),
                                    scan_cutoff: fixed_scan_cutoff,
                                });
                            }
                            page_start_after = all_items.last().expect("non-empty page").0.to_vec();
                            continue;
                        }
                        self.metrics.record_scan_latency(t0.elapsed().as_micros() as u64);
                        // `truncated` in the outcome means "more exist beyond
                        // the caller's limit", not "this page was truncated".
                        let outcome_truncated = if limit == 0 {
                            resp_truncated
                        } else {
                            resp_truncated && all_items.len() >= limit as usize
                        };
                        // If the caller's limit was reached, truncate.
                        if limit != 0 && all_items.len() > limit as usize {
                            all_items.truncate(limit as usize);
                            all_commit_slots.truncate(limit as usize);
                        }
                        return Ok(ScanOutcome {
                            items: all_items,
                            commit_slots: all_commit_slots,
                            truncated: outcome_truncated,
                            timed_out: resp.timed_out,
                            read_slot: page1_read_slot.unwrap_or(0),
                            scan_cutoff: fixed_scan_cutoff,
                        });
                    }
                    self.metrics.record_scan_error();
                    attempts = self.count_other(attempts, &resp.error)?;
                    if Self::is_unknown_leader(resp.error_code, &resp.error) {
                        self.metrics.on_leader_error(store_id, group_id, &endpoint);
                        endpoint = self.wait_and_refresh_leader(store_id, group_id, &endpoint).await;
                        self.metrics
                            .on_leader_resolved(store_id, group_id, &endpoint, "unknown_leader");
                    }
                }
                Err(msg) => {
                    self.metrics.record_scan_error();
                    self.metrics.record_transport_error();
                    self.metrics.on_leader_error(store_id, group_id, &endpoint);
                    endpoint = self
                        .handle_transport_err(store_id, group_id, &endpoint, &mut backoff)
                        .await;
                    self.metrics
                        .on_leader_resolved(store_id, group_id, &endpoint, "transport_error");
                    // Resume from the last received key on the (possibly new)
                    // endpoint — S3-style pagination is keyed on `start_after`,
                    // so no duplicates or gaps in key order. Only reset to the
                    // caller's `start_after` when nothing has been received yet.
                    page_start_after = all_items
                        .last()
                        .map_or_else(|| start_after.to_vec(), |(k, _)| k.to_vec());
                    attempts = self.count_other(attempts, &msg)?;
                }
            }
        }
    }

    /// Count the live keys matching `prefix` (empty = whole keyspace) in a
    /// group. A single `count_only` RPC asks the server to count all matching
    /// keys in one pass (no value materialization, no items shipped — only the
    /// count crosses the network). `start_after`/`end_key` bound the counted
    /// range like [`Self::scan`]. No pagination: the server counts the whole
    /// range in one response. `limit` (`0` = count all) caps the count; when
    /// it is reached the result is exact up to `limit` (the server does not
    /// distinguish "exactly N" from "N or more" in that case — pass `limit =
    /// 0` for a true total).
    ///
    /// # Errors
    /// See [`Error`].
    #[allow(clippy::too_many_arguments)]
    pub async fn scan_count(
        &self,
        store_id: u64,
        group_id: u64,
        prefix: &[u8],
        start_after: &[u8],
        end_key: &[u8],
        limit: u32,
        read_mode: ReadMode,
        min_slot: Option<u64>,
        deadline: Option<u64>,
    ) -> Result<u64> {
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
            let send_result: std::result::Result<crowdb_kv::rpc::KvScanResponse, String> = t
                .send_scan(
                    &endpoint,
                    prefix,
                    start_after,
                    end_key,
                    limit,
                    request_id,
                    request_create_ms,
                    group_id,
                    read_mode,
                    min_slot,
                    false,
                    true,
                    deadline.unwrap_or(0),
                    false,
                    0,
                )
                .await
                .map_err(|e| e.to_string());
            match send_result {
                Ok(resp) => {
                    self.record_endpoint_rtt(store_id, group_id, &endpoint, t0.elapsed().as_micros() as u64);
                    if resp.ok {
                        self.metrics.record_scan_latency(t0.elapsed().as_micros() as u64);
                        return Ok(resp.count);
                    }
                    self.metrics.record_scan_error();
                    if let Some(next) = self
                        .follow_hint(store_id, group_id, &resp.not_leader_hint, &mut redirects)
                        .await?
                    {
                        if read_mode == ReadMode::MinSlot && self.read_endpoint_policy.is_distributed() {
                            self.metrics.record_read_endpoint_fallback();
                        }
                        endpoint = next;
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
                    self.metrics.record_scan_error();
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

    /// Slot-ordered scan over the chosen log. Returns individual KV ops
    /// (Put / Delete) in commit (slot) order within `[min_slot,
    /// max_slot]` (`max_slot = 0` means "up to the current applied
    /// frontier"), filtered by `key_prefix` (empty = all keys). Used by
    /// diskdb strategy 2 (journal scan replay) — [`Self::scan`]
    /// returns key order, not slot order, so it cannot drive a correct
    /// replay.
    ///
    /// Transparent pagination: sends the first request, if `truncated`
    /// resends with `min_slot = last_op_slot + 1`, repeats until all
    /// ops in the range are collected or the caller's `limit` is
    /// reached. `limit = 0` means "no caller limit" (still pages via
    /// the server's per-page `page_limit`). Returns the full op list
    /// in slot order.
    ///
    /// # Errors
    /// See [`Error`]. A server `KV_ERROR_JOURNAL_SCAN_GC_GAP` (asked
    /// for slots already GC'd below the WAL trim point) is surfaced as
    /// [`Error::Server`] — the caller (diskdb recovery) falls back to
    /// a full-scan rebuild.
    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub async fn journal_scan(
        &self,
        store_id: u64,
        group_id: u64,
        min_slot: u64,
        max_slot: u64,
        key_prefix: &[u8],
        limit: u32,
        page_limit: u32,
        read_mode: ReadMode,
        deadline: Option<u64>,
    ) -> Result<JournalScanOutcome> {
        let min_slot_floor = self.resolve_min_slot(store_id, group_id, read_mode, None);
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
        let mut all_ops: Vec<JournalOp> = Vec::new();
        let mut page_min_slot = min_slot.max(min_slot_floor);
        let mut page1_read_slot: Option<u64> = None;
        loop {
            let remaining_page_limit = if page_limit == 0 { 0 } else { page_limit };
            let request_id = self.request_ids.next().as_u64();
            let request_create_ms = now_ms();
            let t0 = Instant::now();
            let _in_flight = self.incr_in_flight(store_id, group_id, &endpoint);
            let send_result: std::result::Result<crowdb_kv::rpc::KvJournalScanResponse, String> = t
                .send_journal_scan(
                    &endpoint,
                    page_min_slot,
                    max_slot,
                    key_prefix,
                    remaining_page_limit,
                    request_id,
                    request_create_ms,
                    group_id,
                    read_mode,
                )
                .await
                .map_err(|e| e.to_string());
            match send_result {
                Ok(resp) => {
                    self.record_endpoint_rtt(store_id, group_id, &endpoint, t0.elapsed().as_micros() as u64);
                    if resp.ok {
                        redirects = crate::client::retry::Redirects::default();
                        self.metrics.record_scan_latency(t0.elapsed().as_micros() as u64);
                        if page1_read_slot.is_none() {
                            page1_read_slot = Some(resp.read_slot);
                        }
                        let page_len = resp.ops.len();
                        let resp_truncated = resp.truncated;
                        for op in resp.ops {
                            all_ops.push(JournalOp {
                                key: op.key,
                                value: op.value,
                                is_delete: op.is_delete,
                                slot: op.slot,
                            });
                        }
                        // Caller's limit reached?
                        if limit != 0 && all_ops.len() >= limit as usize {
                            all_ops.truncate(limit as usize);
                            self.metrics.record_scan_latency(t0.elapsed().as_micros() as u64);
                            return Ok(JournalScanOutcome {
                                ops: all_ops,
                                truncated: true,
                                read_slot: page1_read_slot.unwrap_or(0),
                            });
                        }
                        // Server page truncated → fetch the next page
                        // from `last_op_slot + 1`. A zero-op truncated
                        // page is a safety stop (avoid infinite loop).
                        if resp_truncated && page_len > 0 {
                            let server_timed_out = deadline.is_some_and(|dl| now_ms() >= dl);
                            if server_timed_out {
                                return Ok(JournalScanOutcome {
                                    ops: all_ops,
                                    truncated: true,
                                    read_slot: page1_read_slot.unwrap_or(0),
                                });
                            }
                            page_min_slot = resp.last_op_slot.saturating_add(1);
                            continue;
                        }
                        return Ok(JournalScanOutcome {
                            ops: all_ops,
                            truncated: false,
                            read_slot: page1_read_slot.unwrap_or(0),
                        });
                    }
                    self.metrics.record_scan_error();
                    // GC gap is deterministic — do not retry; the caller
                    // (diskdb recovery) falls back to a full-scan rebuild.
                    if resp.error_code == crowdb_kv::rpc::KvErrorCode::KvErrorJournalScanGcGap as i32 {
                        return Err(Error::JournalScanGcGap);
                    }
                    if let Some(next) = self
                        .follow_hint(store_id, group_id, &resp.not_leader_hint, &mut redirects)
                        .await?
                    {
                        if read_mode == ReadMode::MinSlot && self.read_endpoint_policy.is_distributed() {
                            self.metrics.record_read_endpoint_fallback();
                        }
                        endpoint = next;
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
                    self.metrics.record_scan_error();
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
