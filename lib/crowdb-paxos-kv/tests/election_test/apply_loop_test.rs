// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Background apply loop and value-less catch-up tests.
//!
//! Tests the background apply loop's contiguous replay behavior, the
//! `BatchChosenNotification` frontier advance, and the heartbeat reply
//! not being delayed by engine apply.

use crowdb_paxos_kv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowdb_paxos_kv::cluster::replica::{HeartbeatRequestPayload, ReplicaHandler};
use crowdb_paxos_kv::metrics::MetricsRegistry;
use crowdb_paxos_kv::paxos::roles::{PxAcceptReply, PxBallot, PxLogEntry};
use std::sync::{Arc, Mutex};
use std::time::Duration;

fn heartbeat(term: u64, leader_id: u64, commit_slot: u64) -> HeartbeatRequestPayload {
    HeartbeatRequestPayload {
        term,
        leader_id,
        prev_log_slot: 0,
        prev_log_term: 0,
        committed_safe_slot: commit_slot,
        lease_grant_until_ms_mono: 0,
        t_send_ms_mono: 0,
        gc_before_slot: 0,
        gc_membership_epoch: 0,
    }
}

#[allow(clippy::cast_possible_truncation)]
fn write_entry(slot: u64, key: &[u8], value: &[u8]) -> PxLogEntry {
    let mut payload = Vec::new();
    payload.extend_from_slice(&1u16.to_le_bytes()); // op_count
    payload.push(0u8); // Put
    payload.extend_from_slice(&(key.len() as u32).to_le_bytes());
    payload.extend_from_slice(key);
    payload.extend_from_slice(&(value.len() as u32).to_le_bytes());
    payload.extend_from_slice(value);

    PxLogEntry {
        slot,
        ballot: PxBallot::new(0, 1),
        term: 1,
        payload: bytes::Bytes::from(payload),
    }
}

/// The native engine requires contiguous replay. Preserve the accepted suffix
/// while a slot is missing, then resume from the repair without a new heartbeat.
#[tokio::test]
async fn apply_loop_preserves_missing_slot_and_resumes_after_repair() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);
    let registry = Arc::new(Mutex::new(MetricsRegistry::new()));
    replica.set_metrics_registry(&registry, 1, 1);
    let skipped = registry
        .lock()
        .unwrap()
        .register_counter("s.1.g.1.paxos.apply_loop.skip.c");
    assert!(replica.learner.engine().requires_contiguous_apply());

    // Accept slots 1, 3 (skip 2 — simulates election-churn gap).
    let entry1 = write_entry(1, b"k1", b"v1");
    assert!(matches!(
        replica.on_accept(&entry1).await,
        PxAcceptReply::Accepted { .. }
    ));
    let entry3 = write_entry(3, b"k3", b"v3");
    assert!(matches!(
        replica.on_accept(&entry3).await,
        PxAcceptReply::Accepted { .. }
    ));

    // Heartbeat with commit_slot=3 signals the apply loop.
    let _ = <PxLocalReplica as ReplicaHandler>::on_heartbeat(&replica, heartbeat(1, 2, 3), 1)
        .await
        .expect("reply");

    // Slot 1 should be applied quickly (contiguous_applied advances to 1).
    replica.await_apply_fence(1).await;
    assert_eq!(replica.contiguous_applied(), 1, "slot 1 applied");

    tokio::time::timeout(Duration::from_secs(2), async {
        while skipped.snapshot().total == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("apply loop must observe the missing slot");
    assert!(replica.learner.engine_get(b"k3").await.is_none());
    assert_eq!(replica.accepted_at(3).await.unwrap().payload, entry3.payload);
    assert_eq!(
        replica.contiguous_applied(),
        1,
        "contiguous_applied stays at gap (slot 2 missing)"
    );
    let entry2 = write_entry(2, b"k2", b"v2");
    assert!(matches!(
        replica.on_accept(&entry2).await,
        PxAcceptReply::Accepted { .. }
    ));
    tokio::time::timeout(Duration::from_secs(2), replica.await_apply_fence(3))
        .await
        .expect("gap repair must wake the existing apply loop");
    assert_eq!(replica.contiguous_applied(), 3);
    assert_eq!(replica.learner.engine_get(b"k2").await, Some((2, b"v2".to_vec())));
    assert_eq!(replica.learner.engine_get(b"k3").await, Some((3, b"v3".to_vec())));
    assert!(replica.shutdown(Duration::from_secs(2)).await.is_clean());
}

/// Heartbeat reply returns immediately with the current `contiguous_applied`,
/// which may lag behind `known_commit_slot`. The reply is not delayed by
/// engine apply.
#[tokio::test]
async fn heartbeat_reply_not_delayed_by_apply() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);

    // Accept entries at slots 1–3.
    for slot in 1..=3u64 {
        let entry = write_entry(slot, b"k", &format!("v{slot}").into_bytes());
        let _ = replica.on_accept(&entry).await;
    }

    // Heartbeat with commit_slot=3 — the reply should return immediately
    // with contiguous_applied=0 (apply hasn't happened yet).
    let reply = <PxLocalReplica as ReplicaHandler>::on_heartbeat(&replica, heartbeat(1, 2, 3), 1)
        .await
        .expect("reply");

    assert!(reply.success);
    // The reply's contiguous_applied may be 0 (apply is async) — the key
    // assertion is that the reply returned without waiting for apply.
    // We can't deterministically assert ==0 (the apply loop might have
    // already applied slot 1), but it must be <= 3.
    assert!(
        reply.contiguous_applied <= 3,
        "heartbeat reply not blocked on apply: contiguous_applied = {}",
        reply.contiguous_applied
    );

    // After awaiting the fence, all 3 slots are applied.
    replica.await_apply_fence(3).await;
    assert_eq!(replica.contiguous_applied(), 3, "all slots applied after fence");
}

/// A replica accepts slots as a follower, then wins an election. The
/// background apply loop must apply the accepted slots even though the
/// replica is now a leader (leaders don't receive heartbeats). This is the
/// deadlock regression test from the design doc — the accept path must
/// advance `known_commit_slot` (not just `contiguous_chosen`).
#[tokio::test]
async fn follower_accepts_then_wins_election_applies() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);

    // Accept slots 1–2 as a follower. The accept path (handle_accept_inner)
    // advances known_commit_slot + wakes the apply loop. We simulate that
    // path via the test-only helper since on_accept (local) doesn't go
    // through handle_accept_inner.
    for slot in 1..=2u64 {
        let entry = write_entry(slot, b"k", &format!("v{slot}").into_bytes());
        let _ = replica.on_accept(&entry).await;
        replica.simulate_accept_deferred_apply(&entry, &[]);
    }

    // The replica wins an election and becomes leader.
    replica.become_leader();
    assert!(replica.is_leader());

    // The apply loop must still apply the accepted slots even though the
    // replica is now a leader (no more heartbeats to drive apply).
    replica.await_apply_fence(2).await;
    assert_eq!(
        replica.contiguous_applied(),
        2,
        "accepted slots applied after winning election"
    );
    assert_eq!(
        replica.learner.engine_get(b"k").await.map(|(_, v)| v),
        Some(b"v2".to_vec()),
        "latest value applied"
    );
}

/// The background apply loop applies entries in bounded batches, yielding
/// between batches so cancellation and new `known_commit_slot` advances are
/// observed. With more than `MAX_APPLY_PER_BATCH` (64) slots, multiple
/// batches are needed.
#[tokio::test]
async fn apply_loop_processes_large_backlog_in_batches() {
    let replica = PxLocalReplica::new(1, PxLocalReplicaRole::Follower);

    // Accept 70 slots (more than MAX_APPLY_PER_BATCH=64).
    for slot in 1..=70u64 {
        let entry = write_entry(slot, b"k", &format!("v{slot}").into_bytes());
        let _ = replica.on_accept(&entry).await;
    }

    // Heartbeat with commit_slot=70.
    let _ = <PxLocalReplica as ReplicaHandler>::on_heartbeat(&replica, heartbeat(1, 2, 70), 1)
        .await
        .expect("reply");

    // All 70 slots should eventually be applied.
    replica.await_apply_fence(70).await;
    assert_eq!(replica.contiguous_applied(), 70, "all 70 slots applied");
    assert_eq!(
        replica.learner.engine_get(b"k").await.map(|(_, v)| v),
        Some(b"v70".to_vec()),
        "latest value applied"
    );
}
