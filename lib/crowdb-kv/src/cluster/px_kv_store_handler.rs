// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::cast_possible_truncation)]

use crate::cluster::group_operations::{
    KvGroupMutation, KvGroupOperationError, KvGroupOperations, KvGroupScanRequest, KvReadConsistency,
    KvRequestIdentity,
};
use crate::cluster::kv_store::KvStore;
use crate::cluster::px_kv_store::{
    journal_scan_err, missing_group_response, scan_err, PxKvStore, ReadDecision,
};
use crate::kv::{Batch, Op};
use bytes::Bytes;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;
use tracing::debug;

fn read_consistency(read_mode: i32, min_slot: u64) -> KvReadConsistency {
    match crate::rpc::ReadMode::try_from(read_mode).unwrap_or(crate::rpc::ReadMode::Linearizable) {
        crate::rpc::ReadMode::Linearizable => KvReadConsistency::Linearizable,
        crate::rpc::ReadMode::MinSlot => KvReadConsistency::MinAppliedSlot(min_slot),
    }
}

fn request_identity(client_id: u64, sequence: u64) -> Option<KvRequestIdentity> {
    (client_id != 0).then_some(KvRequestIdentity { client_id, sequence })
}

async fn write_and_respond(
    operations: Option<KvGroupOperations>,
    mutations: Vec<KvGroupMutation>,
    identity: Option<KvRequestIdentity>,
    request_id: u64,
    request_create_ms: u64,
) -> crate::rpc::KvResponse {
    let Some(operations) = operations else {
        return missing_group_response(request_id, request_create_ms);
    };
    match operations.write(&mutations, identity).await {
        Ok(write) => crate::rpc::KvResponse::ok_chosen(write.chosen_slot, request_id, request_create_ms),
        Err(error) => operation_error_response(error, request_id, request_create_ms),
    }
}

fn operation_error_response(
    error: KvGroupOperationError,
    request_id: u64,
    request_create_ms: u64,
) -> crate::rpc::KvResponse {
    match error {
        KvGroupOperationError::NotLeader { leader_hint } => {
            crate::rpc::KvResponse::not_leader(leader_hint, request_id, request_create_ms)
        }
        KvGroupOperationError::Busy => crate::rpc::KvResponse::err(
            crate::paxos::error::PxPaxosError::Busy.keyword().to_string(),
            request_id,
            request_create_ms,
        ),
        KvGroupOperationError::CompareFailed { current_revision } => crate::rpc::KvResponse::cas_error(
            crate::rpc::KvErrorCode::KvErrorCasFailed,
            current_revision,
            "compare-and-set precondition failed",
            request_id,
            request_create_ms,
        ),
        KvGroupOperationError::CompareBusy => crate::rpc::KvResponse::cas_error(
            crate::rpc::KvErrorCode::KvErrorCasBusy,
            0,
            "compare-and-set admission busy",
            request_id,
            request_create_ms,
        ),
        KvGroupOperationError::OutcomeUnknown => crate::rpc::KvResponse::cas_error(
            crate::rpc::KvErrorCode::KvErrorOutcomeUnknown,
            0,
            "compare-and-set outcome is unknown",
            request_id,
            request_create_ms,
        ),
        KvGroupOperationError::Unavailable(message) | KvGroupOperationError::Internal(message) => {
            crate::rpc::KvResponse::err(message, request_id, request_create_ms)
        }
    }
}

impl KvStore for PxKvStore {
    async fn kv_get(
        &self,
        group_id: u64,
        key: &[u8],
        read_mode: i32,
        min_slot: u64,
        request_id: u64,
        request_create_ms: u64,
    ) -> crate::rpc::KvResponse {
        let e2e_start = Instant::now();
        #[cfg(feature = "test-util")]
        let test_delay = *self.get_delay.lock();
        #[cfg(feature = "test-util")]
        if let Some(delay) = test_delay {
            tokio::time::sleep(delay).await;
        }
        let Some(operations) = self.group_operations(group_id) else {
            return missing_group_response(request_id, request_create_ms);
        };
        let consistency = read_consistency(read_mode, min_slot);
        let result = match operations.get(key, consistency).await {
            Ok(read) => read.value.map_or_else(
                || {
                    crate::rpc::KvResponse::not_found(request_id, request_create_ms)
                        .with_read_slots(read.read_slot, read.safe_slot)
                },
                |value| {
                    crate::rpc::KvResponse::ok_value_with_revision(
                        value,
                        read.revision,
                        request_id,
                        request_create_ms,
                    )
                    .with_read_slots(read.read_slot, read.safe_slot)
                },
            ),
            Err(error) => operation_error_response(error, request_id, request_create_ms),
        };
        if let Some(group) = self.get_group(group_id) {
            if let Some(h) = group.read_handles() {
                h.e2e.observe(e2e_start.elapsed().as_nanos() as u64);
            }
        }
        result
    }

    async fn kv_put(
        &self,
        group_id: u64,
        key: &[u8],
        value: &[u8],
        client_id: u64,
        seq: u64,
        request_id: u64,
        request_create_ms: u64,
    ) -> crate::rpc::KvResponse {
        write_and_respond(
            self.group_operations(group_id),
            vec![KvGroupMutation::Put {
                key: Bytes::copy_from_slice(key),
                value: Bytes::copy_from_slice(value),
            }],
            request_identity(client_id, seq),
            request_id,
            request_create_ms,
        )
        .await
    }
    async fn kv_delete(
        &self,
        group_id: u64,
        key: &[u8],
        client_id: u64,
        seq: u64,
        request_id: u64,
        request_create_ms: u64,
    ) -> crate::rpc::KvResponse {
        write_and_respond(
            self.group_operations(group_id),
            vec![KvGroupMutation::Delete {
                key: Bytes::copy_from_slice(key),
            }],
            request_identity(client_id, seq),
            request_id,
            request_create_ms,
        )
        .await
    }
    async fn kv_batch_write(
        &self,
        group_id: u64,
        items: Vec<crate::rpc::KvBatchItem>,
        client_id: u64,
        seq: u64,
        request_id: u64,
        request_create_ms: u64,
    ) -> crate::rpc::KvResponse {
        let mutations = items
            .into_iter()
            .map(|item| {
                if item.is_delete {
                    KvGroupMutation::Delete { key: item.key }
                } else {
                    KvGroupMutation::Put {
                        key: item.key,
                        value: item.value,
                    }
                }
            })
            .collect();
        write_and_respond(
            self.group_operations(group_id),
            mutations,
            request_identity(client_id, seq),
            request_id,
            request_create_ms,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn kv_scan(
        &self,
        group_id: u64,
        prefix: &[u8],
        start_after: &[u8],
        end_key: &[u8],
        limit: u32,
        read_mode: i32,
        min_slot: u64,
        keys_only: bool,
        count_only: bool,
        deadline_ms: u64,
        bounded: bool,
        requested_scan_cutoff: u64,
        direction: crate::kv::ScanDirection,
        request_id: u64,
        request_create_ms: u64,
    ) -> crate::rpc::KvScanResponse {
        let Some(operations) = self.group_operations(group_id) else {
            return scan_err(
                format!("group {group_id} not found in store {}", self.store_id),
                String::new(),
                request_id,
                request_create_ms,
            );
        };
        let request = KvGroupScanRequest {
            prefix: Bytes::copy_from_slice(prefix),
            start_after: Bytes::copy_from_slice(start_after),
            end_key: Bytes::copy_from_slice(end_key),
            limit: limit as usize,
            consistency: read_consistency(read_mode, min_slot),
            keys_only,
            count_only,
            deadline_ms,
            bounded,
            requested_scan_cutoff,
            direction,
        };
        match operations.scan(&request).await {
            Ok(scan) => crate::rpc::KvScanResponse {
                version: 1,
                ok: true,
                error: String::new(),
                truncated: scan.truncated,
                items: scan
                    .items
                    .into_iter()
                    .map(|item| crate::rpc::KvScanItem {
                        key: item.key,
                        value: item.value,
                        commit_slot: item.revision,
                    })
                    .collect(),
                request_id,
                request_create_ms,
                read_slot: scan.read_slot,
                not_leader_hint: String::new(),
                error_code: crate::rpc::KvErrorCode::KvErrorNone as i32,
                count: scan.count,
                timed_out: scan.timed_out,
                scan_cutoff: scan.scan_cutoff,
            },
            Err(KvGroupOperationError::NotLeader { leader_hint }) => scan_err(
                "not leader".to_string(),
                leader_hint,
                request_id,
                request_create_ms,
            ),
            Err(error) => scan_err(error.to_string(), String::new(), request_id, request_create_ms),
        }
    }

    async fn kv_create_snapshot(
        &self,
        group_id: u64,
        read_mode: i32,
        min_slot: u64,
    ) -> crate::rpc::CreateSnapshotResponse {
        let Some(group) = self.get_group(group_id) else {
            return crate::rpc::CreateSnapshotResponse {
                ok: false,
                error: format!("group {group_id} not found"),
                snapshot_handle: 0,
                at_slot: 0,
                error_code: crate::rpc::KvErrorCode::KvErrorInternal as i32,
                not_leader_hint: String::new(),
            };
        };
        // Resolve read point for linearizable snapshots (must be leader).
        match self.resolve_read_point(&group, read_mode, min_slot).await {
            ReadDecision::Serve { .. } => {}
            ReadDecision::NotLeader { hint } => {
                return crate::rpc::CreateSnapshotResponse {
                    ok: false,
                    error: "not leader".to_string(),
                    snapshot_handle: 0,
                    at_slot: 0,
                    error_code: crate::rpc::KvErrorCode::KvErrorNotLeader as i32,
                    not_leader_hint: hint,
                };
            }
            ReadDecision::Unavailable { msg } => {
                return crate::rpc::CreateSnapshotResponse {
                    ok: false,
                    error: msg,
                    snapshot_handle: 0,
                    at_slot: 0,
                    error_code: crate::rpc::KvErrorCode::KvErrorUnavailable as i32,
                    not_leader_hint: String::new(),
                };
            }
        }
        // Flush + snapshot_view on the engine.
        let (at_slot, entries) = match group.local_replica().learner.engine().snapshot_view() {
            Ok(v) => v,
            Err(msg) => {
                return crate::rpc::CreateSnapshotResponse {
                    ok: false,
                    error: format!("snapshot_view: {msg}"),
                    snapshot_handle: 0,
                    at_slot: 0,
                    error_code: crate::rpc::KvErrorCode::KvErrorInternal as i32,
                    not_leader_hint: String::new(),
                };
            }
        };
        // Allocate handle id and register.
        let handle_id = group.next_snapshot_handle.fetch_add(1, Ordering::Relaxed);
        let handle = Arc::new(crate::cluster::group::SnapshotHandle {
            handle: handle_id,
            at_slot,
            entries,
            created_at: Instant::now(),
            lease: crate::cluster::group::SnapshotHandle::DEFAULT_LEASE,
        });
        // Lazy reap of expired handles.
        self.reap_expired_snapshots(&group);
        group.snapshots.insert(handle_id, handle);
        debug!(
            s = self.store_id,
            group_id, handle_id, at_slot, "kv_create_snapshot: pinned snapshot"
        );
        crate::rpc::CreateSnapshotResponse {
            ok: true,
            error: String::new(),
            snapshot_handle: handle_id,
            at_slot,
            error_code: crate::rpc::KvErrorCode::KvErrorNone as i32,
            not_leader_hint: String::new(),
        }
    }

    #[allow(clippy::unused_async_trait_impl, reason = "trait defines async fn")]
    async fn kv_list_snapshots(&self, group_id: u64) -> crate::rpc::ListSnapshotsResponse {
        let Some(group) = self.get_group(group_id) else {
            return crate::rpc::ListSnapshotsResponse {
                ok: false,
                error: format!("group {group_id} not found"),
                snapshots: Vec::new(),
            };
        };
        self.reap_expired_snapshots(&group);
        let snapshots = group
            .snapshots
            .iter()
            .map(|e| crate::rpc::SnapshotInfo {
                snapshot_handle: e.handle,
                at_slot: e.at_slot,
                lease_remaining_ms: e.lease_remaining().as_millis() as u64,
            })
            .collect();
        crate::rpc::ListSnapshotsResponse {
            ok: true,
            error: String::new(),
            snapshots,
        }
    }

    #[allow(clippy::unused_async_trait_impl, reason = "trait defines async fn")]
    async fn kv_snapshot_scan(
        &self,
        group_id: u64,
        snapshot_handle: u64,
        prefix: &[u8],
        start_after: &[u8],
        limit: u32,
    ) -> crate::rpc::SnapshotScanResponse {
        let Some(group) = self.get_group(group_id) else {
            return crate::rpc::SnapshotScanResponse {
                ok: false,
                error: format!("group {group_id} not found"),
                truncated: false,
                items: Vec::new(),
                error_code: crate::rpc::KvErrorCode::KvErrorInternal as i32,
            };
        };
        self.reap_expired_snapshots(&group);
        let Some(handle) = group
            .snapshots
            .get(&snapshot_handle)
            .map(|e| Arc::clone(e.value()))
        else {
            return crate::rpc::SnapshotScanResponse {
                ok: false,
                error: format!("snapshot handle {snapshot_handle} not found (expired or released)"),
                truncated: false,
                items: Vec::new(),
                error_code: crate::rpc::KvErrorCode::KvErrorInternal as i32,
            };
        };
        // Binary-search for start_after, then linear scan filtering by
        // prefix and skipping tombstones.
        let entries = &handle.entries;
        let start_idx = if start_after.is_empty() {
            0
        } else {
            // `start_after` is exclusive: skip the found element.
            match entries.binary_search_by(|e| e.key.as_slice().cmp(start_after)) {
                Ok(i) => i + 1,
                Err(i) => i,
            }
        };
        let mut items = Vec::new();
        let mut truncated = false;
        let limit_usize = limit as usize;
        for e in &entries[start_idx..] {
            if !e.key.starts_with(prefix) {
                // If we've moved past the prefix, no more matches (sorted).
                if !prefix.is_empty() && e.key.as_slice() > prefix {
                    break;
                }
                continue;
            }
            if e.tombstone {
                continue;
            }
            if limit_usize != 0 && items.len() >= limit_usize {
                truncated = true;
                break;
            }
            items.push(crate::rpc::KvScanItem {
                key: Bytes::from(e.key.clone()),
                value: Bytes::from(e.value.clone()),
                commit_slot: e.slot,
            });
        }
        crate::rpc::SnapshotScanResponse {
            ok: true,
            error: String::new(),
            truncated,
            items,
            error_code: crate::rpc::KvErrorCode::KvErrorNone as i32,
        }
    }

    #[allow(clippy::unused_async_trait_impl, reason = "trait defines async fn")]
    async fn kv_release_snapshot(
        &self,
        group_id: u64,
        snapshot_handle: u64,
    ) -> crate::rpc::ReleaseSnapshotResponse {
        let Some(group) = self.get_group(group_id) else {
            return crate::rpc::ReleaseSnapshotResponse {
                ok: false,
                error: format!("group {group_id} not found"),
            };
        };
        if group.snapshots.remove(&snapshot_handle).is_some() {
            debug!(
                s = self.store_id,
                group_id, snapshot_handle, "kv_release_snapshot: released"
            );
            crate::rpc::ReleaseSnapshotResponse {
                ok: true,
                error: String::new(),
            }
        } else {
            crate::rpc::ReleaseSnapshotResponse {
                ok: false,
                error: format!("snapshot handle {snapshot_handle} not found"),
            }
        }
    }

    #[allow(clippy::too_many_lines)]
    async fn kv_journal_scan(
        &self,
        group_id: u64,
        min_slot: u64,
        max_slot: u64,
        key_prefix: &[u8],
        limit: u32,
        read_mode: i32,
        request_id: u64,
        request_create_ms: u64,
    ) -> crate::rpc::KvJournalScanResponse {
        let Some(group) = self.get_group(group_id) else {
            return journal_scan_err(
                format!("group {group_id} not found in store {}", self.store_id),
                String::new(),
                false,
                request_id,
                request_create_ms,
            );
        };

        // Read-mode routing mirrors `kv_scan`: linearizable runs the
        // leader barrier; min_slot serves locally once the applied
        // frontier has caught up to `min_slot`.
        let read_slot = match self.resolve_read_point(&group, read_mode, min_slot).await {
            ReadDecision::Serve { read_slot } => read_slot,
            ReadDecision::NotLeader { hint } => {
                return journal_scan_err(
                    "not leader".to_string(),
                    hint,
                    false,
                    request_id,
                    request_create_ms,
                );
            }
            ReadDecision::Unavailable { msg } => {
                return journal_scan_err(msg, String::new(), false, request_id, request_create_ms);
            }
        };

        let replica = group.local_replica();
        let acceptor = &replica.acceptor;

        // GC safety: slots below the acceptor's trim point have been
        // reclaimed and can no longer be read. The caller falls back to
        // a full-scan rebuild (diskdb strategy 1) on this code.
        let trim_slot = acceptor.trim_slot();
        if min_slot < trim_slot {
            return journal_scan_err(
                format!("journal_scan min_slot {min_slot} below trim_slot {trim_slot} (slots GC'd)"),
                String::new(),
                true,
                request_id,
                request_create_ms,
            );
        }

        // Effective upper bound: caller's max_slot (0 = MAX) capped by
        // the applied frontier — never read unapplied slots.
        let effective_max = if max_slot == 0 {
            read_slot
        } else {
            max_slot.min(read_slot)
        };
        if effective_max < min_slot {
            // Empty range — return an empty success.
            return crate::rpc::KvJournalScanResponse {
                version: 1,
                ok: true,
                error: String::new(),
                ops: Vec::new(),
                truncated: false,
                last_op_slot: 0,
                read_slot,
                error_code: crate::rpc::KvErrorCode::KvErrorNone as i32,
                not_leader_hint: String::new(),
                request_id,
                request_create_ms,
            };
        }

        // Slot-ordered iteration over accepted entries. The acceptor
        // holds every chosen entry on every replica (leader via
        // `accept`, follower via `FetchGap` → `accept`), so this is a
        // complete slot-ordered op log bounded by `contiguous_applied`.
        // `end_slot_exclusive` = effective_max + 1 (effective_max is
        // inclusive per the proto contract).
        let end_exclusive = effective_max.saturating_add(1);
        let entries = acceptor.accepted_iter_range(min_slot, end_exclusive);

        // Decode each slot's payload batch and emit matching ops in
        // slot order (within a slot, ops are in batch order).
        let mut ops: Vec<crate::rpc::KvJournalOp> = Vec::new();
        let mut truncated = false;
        let mut last_op_slot = 0u64;
        let limit_usize = limit as usize;
        for (slot, entry) in entries {
            let batch = Batch::decode(&entry.payload);
            for op in batch.ops {
                if !key_prefix.is_empty() && !op.key.starts_with(key_prefix) {
                    continue;
                }
                let (value, is_delete) = match op.op {
                    Op::Put(v) => (v, false),
                    Op::Delete => (Bytes::new(), true),
                };
                last_op_slot = slot;
                ops.push(crate::rpc::KvJournalOp {
                    key: op.key,
                    value,
                    is_delete,
                    slot,
                });
                if limit_usize != 0 && ops.len() >= limit_usize {
                    truncated = true;
                    break;
                }
            }
            if truncated {
                break;
            }
        }

        debug!(
            s = self.store_id,
            group_id,
            min_slot,
            max_slot,
            prefix_len = key_prefix.len(),
            limit,
            returned = ops.len(),
            truncated,
            "kv_journal_scan local-replica read"
        );

        crate::rpc::KvJournalScanResponse {
            version: 1,
            ok: true,
            error: String::new(),
            ops,
            truncated,
            last_op_slot,
            read_slot,
            error_code: crate::rpc::KvErrorCode::KvErrorNone as i32,
            not_leader_hint: String::new(),
            request_id,
            request_create_ms,
        }
    }
}
