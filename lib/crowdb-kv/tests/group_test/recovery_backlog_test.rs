// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::common::cluster::start_cluster;
use crowdb_kv::{
    cluster::group_election::LeaderElection,
    common::config::PxElectionConfig,
    paxos::roles::{Acceptor, Learner, PxBallot, PxLogEntry},
};
use std::time::Duration;
use tokio_util::sync::CancellationToken;

#[tokio::test]
async fn bulk_recovery_fills_its_bounded_window_before_waiting_for_peer_io() {
    let cluster = start_cluster(&[1, 2, 3], 1).await;
    let group = cluster.leader().get_group(1).unwrap();
    let config = PxElectionConfig {
        bulk_prepare_window: 4,
        ..group.election_config()
    };
    let recovery = group.run_bulk_phase1(0, 0, 12, config, CancellationToken::new());
    tokio::pin!(recovery);
    // On this current-thread runtime the peers cannot answer until this poll
    // yields. Slow IO at the first slot must leave all four slots dispatched.
    for _ in 0..16 {
        assert!(futures::poll!(&mut recovery).is_pending());
        if group.local_replica().highest_seen_slot() == 4 {
            break;
        }
    }
    assert_eq!(group.local_replica().highest_seen_slot(), 4);
    tokio::time::timeout(Duration::from_secs(3), recovery)
        .await
        .expect("bounded recovery must finish");
    assert_eq!(group.local_replica().contiguous_applied(), 12);
    assert!(group.leader_read_ready());
    cluster.shutdown().await;
}

#[tokio::test]
async fn cancelling_a_pending_bulk_window_does_not_accept_its_prepared_slots() {
    let cluster = start_cluster(&[1, 2, 3], 1).await;
    let group = cluster.leader().get_group(1).unwrap();
    let config = PxElectionConfig {
        bulk_prepare_window: 4,
        ..group.election_config()
    };
    let cancel = CancellationToken::new();
    let recovery = group.run_bulk_phase1(0, 0, 12, config, cancel.clone());
    tokio::pin!(recovery);
    assert!(futures::poll!(&mut recovery).is_pending());
    cancel.cancel();
    recovery.await;
    assert_eq!(group.local_replica().contiguous_applied(), 0);
    for slot in 1..=12 {
        assert!(group.local_replica().accepted_at(slot).await.is_none());
    }
    cluster.shutdown().await;
}

#[tokio::test]
async fn follower_fetches_a_backlog_above_the_unimplemented_snapshot_threshold() {
    let cluster = start_cluster(&[1, 2, 3], 1).await;
    let leader = cluster.leader().get_group(1).unwrap();
    let follower = cluster.followers()[0].get_group(1).unwrap();
    let last = leader.election_config().catchup_snapshot_threshold + 1;
    for slot in 1..=last {
        let entry = PxLogEntry {
            slot,
            ballot: PxBallot::new(1, 1),
            term: 0,
            payload: bytes::Bytes::new(),
        };
        leader.local_replica().acceptor.accept(&entry).await;
        leader.local_replica().learner.learn(entry, &[]).await;
        follower.note_missing_chosen_for_tests(slot);
    }
    tokio::time::timeout(Duration::from_secs(3), async {
        while follower.local_replica().contiguous_applied() != last {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("snapshot threshold must not permanently disable bounded catch-up");
    for slot in 1..=last {
        assert_eq!(
            follower.local_replica().accepted_at(slot).await,
            leader.local_replica().accepted_at(slot).await
        );
    }
    cluster.shutdown().await;
}
