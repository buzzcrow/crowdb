// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#![allow(clippy::too_many_lines)]
#![allow(clippy::cast_possible_truncation)]

use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Weak,
};
use std::time::Duration;

use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::cluster::group::PxGroup;
use crate::paxos::PxGroupId;

struct FetchGapInFlight(Arc<AtomicU64>);

impl Drop for FetchGapInFlight {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

/// R65: Follower-side `FetchGap` driver. Periodically checks the local
/// replica's `gap_slots` set and sends `FetchGap` to the leader for each
/// gap. On reply, overwrites the stale/missing value and wakes the apply
/// loop. Bounded by `MAX_INFLIGHT_FETCHGAP`.
pub(crate) async fn run_fetchgap_driver(
    group: Weak<PxGroup>,
    group_id: PxGroupId,
    cancel: CancellationToken,
) {
    let tick = Duration::from_millis(10);
    let mut large_backlog = false;
    loop {
        if cancel.is_cancelled() {
            return;
        }
        if sleep_or_cancel(&cancel, tick).await {
            return;
        }
        let Some(group) = group.upgrade() else {
            return;
        };
        // R65: update replication gauges periodically.
        group.local_replica().update_replication_gauges();
        // Only followers need FetchGap — the leader is the source of
        // truth and has no gaps.
        if group.local_replica().is_leader() {
            continue;
        }
        let leader_id = group.leader_id();
        if leader_id == 0 {
            continue;
        }
        let Some(remote) = group.get_remote_replica(leader_id) else {
            continue;
        };
        // Running replicas have no atomic snapshot replacement yet. Keep
        // draining large backlogs with the existing fixed inflight bound.
        let gap_count = group.local_replica().gap_slots.lock().len();
        let threshold = group.config.election.catchup_snapshot_threshold;
        if gap_count > threshold as usize && !large_backlog {
            debug!(
                group_id,
                gap_count, threshold, "snapshot catch-up is unavailable; continuing bounded FetchGap"
            );
        }
        large_backlog = gap_count > threshold as usize;
        // Clone the crowdb-rpc transport + endpoint so the spawned task
        // can call `transport.send_fetch_gap()` directly.
        let rpc_transport = remote.rpc_transport().cloned();
        let rpc_endpoint = remote.endpoint_str().to_string();
        let rpc_timeout = Duration::from_millis(group.config.election.peer_rpc_timeout_ms);
        let replica = group.local_replica();
        let term = replica.current_term_snapshot();
        let gaps = replica.drain_gaps_for_fetchgap();
        if gaps.is_empty() {
            continue;
        }
        replica.incr_fetchgap_sent(gaps.len() as u64);
        // Clone the Arc fields needed by the spawned task.
        let fetchgap_inflight = replica.fetchgap_inflight.clone();
        let gap_slots = replica.gap_slots.clone();
        let learner = replica.learner.clone();
        let acceptor = replica.acceptor.clone();
        let wal = replica.wal().cloned();
        let apply_notify = replica.apply_notify.clone();
        let fetchgap_received = replica
            .replication_handles
            .get()
            .map(|h| h.fetchgap_received.clone());
        let fetchgap_failed = replica
            .replication_handles
            .get()
            .map(|h| h.fetchgap_failed.clone());
        // Spawn a task per gap to send FetchGap concurrently.
        for slot in gaps {
            let rpc_transport_clone = rpc_transport.clone();
            let rpc_endpoint_clone = rpc_endpoint.clone();
            let rpc_timeout_clone = rpc_timeout;
            let inflight = FetchGapInFlight(fetchgap_inflight.clone());
            let gap_slots_clone = gap_slots.clone();
            let learner_clone = learner.clone();
            let acceptor_clone = acceptor.clone();
            let wal_clone = wal.clone();
            let apply_notify_clone = apply_notify.clone();
            let fetchgap_received_clone = fetchgap_received.clone();
            let fetchgap_failed_clone = fetchgap_failed.clone();
            tokio::spawn(async move {
                let _inflight = inflight;
                let Some(ref transport) = rpc_transport_clone else {
                    if let Some(c) = &fetchgap_failed_clone {
                        c.inc();
                    }
                    return;
                };
                let result = tokio::time::timeout(
                    rpc_timeout_clone,
                    transport.send_fetch_gap(
                        crate::rpc::ReplicaDestination {
                            endpoint: &rpc_endpoint_clone,
                            replica_id: leader_id,
                        },
                        group_id,
                        slot,
                        term,
                        leader_id,
                    ),
                )
                .await
                .map_err(|_| {
                    crate::cluster::replica::PxReplicaError::Internal(format!(
                        "fetch_gap rpc timeout after {} ms at peer {}",
                        rpc_timeout_clone.as_millis(),
                        rpc_endpoint_clone
                    ))
                })
                .and_then(|r| r);
                match result {
                    Ok(resp) => {
                        if let Some(c) = &fetchgap_received_clone {
                            c.inc();
                        }
                        debug!(
                            group_id,
                            slot,
                            term = resp.term,
                            ballot_round = resp.ballot_round,
                            leader_id = resp.leader_id,
                            payload_len = resp.payload.len(),
                            "FetchGap reply received"
                        );
                        let entry = crate::paxos::roles::PxLogEntry {
                            slot: resp.slot,
                            ballot: crate::paxos::roles::PxBallot::new(resp.ballot_round, resp.leader_id),
                            term: resp.term,
                            payload: resp.payload,
                        };
                        // Historical chosen values may belong to an older
                        // election term. Persist their original entry before
                        // exposing an applied frontier or snapshot coverage.
                        let persisted = if let Some(wal) = &wal_clone {
                            match wal
                                .append(&crate::wal::record::WALRecord::from_accepted(group_id, &entry))
                                .await
                            {
                                Ok(_) => true,
                                Err(error) => {
                                    tracing::error!(group_id, slot, %error, "FetchGap persistence failed; retaining gap for later recovery");
                                    false
                                }
                            }
                        } else {
                            true
                        };
                        if !persisted || !acceptor_clone.install_chosen(&entry).await {
                            gap_slots_clone.lock().insert(slot);
                            if let Some(c) = &fetchgap_failed_clone {
                                c.inc();
                            }
                            return;
                        }
                        learner_clone.update_chosen_frontier(resp.slot, resp.term);
                        apply_notify_clone.notify_one();
                    }
                    Err(err) => {
                        if let Some(c) = &fetchgap_failed_clone {
                            c.inc();
                        }
                        // Re-record the gap so the next tick retries.
                        gap_slots_clone.lock().insert(slot);
                        debug!(
                            group_id,
                            slot,
                            error = %err,
                            "FetchGap failed (will retry next tick)"
                        );
                    }
                }
            });
        }
        if sleep_or_cancel(&cancel, tick).await {
            return;
        }
    }
}

/// Returns `true` if cancelled during sleep, `false` if sleep completed.
async fn sleep_or_cancel(cancel: &CancellationToken, dur: Duration) -> bool {
    tokio::select! {
        () = cancel.cancelled() => true,
        () = tokio::time::sleep(dur) => false,
    }
}
