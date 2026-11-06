// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crowdb_paxos_kv::cluster::group::PxGroup;
use crowdb_paxos_kv::cluster::replica::{HeartbeatRequestPayload, ReplicaHandler};
use crowdb_paxos_kv::cluster::{
    PaxosKvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole, PxRemoteReplica,
};
use crowdb_paxos_kv::paxos::roles::{Acceptor, Learner, PxBallot, PxLogEntry};
use crowdb_paxos_kv::wal::wal_engine::WalEngine;
use crowdb_paxos_kv::wal::{IoBackend, MemBlockDevice, WalConfig};

async fn local(id: u64, role: PxLocalReplicaRole, applied: u64, durable: u64) -> PxLocalReplica {
    let mut replica = PxLocalReplica::new(id, role);
    for slot in 1..=applied {
        let entry = PxLogEntry {
            slot,
            ballot: PxBallot::new(0, 0),
            term: 0,
            payload: bytes::Bytes::new(),
        };
        replica.acceptor.accept(&entry).await;
        replica.learner.learn(entry, &[]).await;
    }
    let wal = WalEngine::create(
        Arc::new(IoBackend::MemBlock(MemBlockDevice::new())),
        WalConfig {
            wal_disks: vec![PathBuf::from(format!("/retention-{id}"))],
            ..Default::default()
        },
        id,
    )
    .await
    .unwrap();
    wal.set_snapshot_slot(durable);
    replica.set_wal(wal);
    replica
}

fn remote(id: u64, voting: bool) -> PxRemoteReplica {
    PxRemoteReplica::new(id, format!("127.0.0.1:{id}")).with_voting(voting)
}

#[tokio::test]
async fn retention_waits_for_every_learner_and_a_durable_voter() {
    let mut group = PxGroup::new(1, local(1, PxLocalReplicaRole::Leader, 10, 8).await);
    group.set_remote_replicas(vec![remote(2, true), remote(3, true), remote(4, false)]);
    group.note_peer_applied_for_tests(2, 9);
    group.note_peer_applied_for_tests(3, 7);
    group.note_peer_durable_for_tests(2, 9);
    assert_eq!(group.reclamation_certificate_for_tests(), 0, "unheard learner");
    group.note_peer_applied_for_tests(4, 3);
    assert_eq!(group.reclamation_certificate_for_tests(), 3, "slow learner");
    group.note_peer_applied_for_tests(4, 10);
    assert_eq!(group.reclamation_certificate_for_tests(), 7, "slow voter");
    group.note_peer_applied_for_tests(3, 10);
    assert_eq!(group.reclamation_certificate_for_tests(), 8, "local snapshot");

    group.reset_safe_slot_tracking_for_tests();
    for peer in 2..=4 {
        group.note_peer_applied_for_tests(peer, 10);
    }
    group.note_peer_durable_for_tests(4, 10);
    assert_eq!(
        group.reclamation_certificate_for_tests(),
        0,
        "learner is no durable voter"
    );
    group.note_peer_durable_for_tests(3, 6);
    assert_eq!(group.reclamation_certificate_for_tests(), 6);
    group.add_remote_replica(remote(5, false));
    assert_eq!(
        group.reclamation_certificate_for_tests(),
        0,
        "membership replacement resets evidence"
    );
}

#[tokio::test]
async fn singleton_retention_requires_local_durability_and_learner_progress() {
    let mut group = PxGroup::new(1, local(1, PxLocalReplicaRole::Leader, 7, 5).await);
    assert_eq!(group.reclamation_certificate_for_tests(), 5);
    group.add_remote_replica(remote(2, false));
    assert_eq!(group.reclamation_certificate_for_tests(), 0);
    group.note_peer_applied_for_tests(2, 6);
    assert_eq!(group.reclamation_certificate_for_tests(), 5);
    group.run_maintenance_pass_for_tests().await;
    assert_eq!(group.local_replica().acceptor.trim_slot(), 5);
    assert_eq!(group.local_replica().acceptor.retained_slots(), 3);

    let without_wal = PxGroup::new(2, PxLocalReplica::new(1, PxLocalReplicaRole::Leader));
    assert_eq!(without_wal.reclamation_certificate_for_tests(), 0);
}

fn heartbeat(epoch: u64) -> HeartbeatRequestPayload {
    HeartbeatRequestPayload {
        term: 3,
        leader_id: 2,
        prev_log_slot: 10,
        prev_log_term: 0,
        committed_safe_slot: 10,
        lease_grant_until_ms_mono: 0,
        t_send_ms_mono: 0,
        gc_before_slot: 9,
        gc_membership_epoch: epoch,
    }
}

#[tokio::test]
async fn follower_retention_fences_epoch_term_leader_and_local_snapshot() {
    let mut group = PxGroup::new(1, local(1, PxLocalReplicaRole::Follower, 10, 6).await);
    group.set_remote_replicas(vec![remote(2, true), remote(3, false)]);
    group.set_membership_epoch(7);
    let valid = heartbeat(7);
    let reply = group.local_replica().on_heartbeat(valid, 1).await.unwrap();
    assert!(reply.success);
    for invalid in [
        HeartbeatRequestPayload {
            gc_membership_epoch: 6,
            ..valid
        },
        HeartbeatRequestPayload { term: 2, ..valid },
        HeartbeatRequestPayload {
            leader_id: 3,
            ..valid
        },
        HeartbeatRequestPayload {
            leader_id: 99,
            ..valid
        },
    ] {
        group.record_reclamation_heartbeat_for_tests(&invalid);
        assert_eq!(group.local_reclamation_slot_for_tests(), 0);
    }
    group.record_reclamation_heartbeat_for_tests(&valid);
    assert_eq!(group.local_reclamation_slot_for_tests(), 6);
    group.run_maintenance_pass_for_tests().await;
    assert_eq!(group.local_replica().acceptor.trim_slot(), 6);
    assert_eq!(group.local_replica().acceptor.retained_slots(), 5);

    group.adopt_membership_epoch(8);
    assert_eq!(
        group.local_reclamation_slot_for_tests(),
        0,
        "stored certificate has its own epoch"
    );
    group.record_reclamation_heartbeat_for_tests(&valid);
    assert_eq!(group.local_reclamation_slot_for_tests(), 0);
    group.record_reclamation_heartbeat_for_tests(&heartbeat(8));
    assert_eq!(group.local_reclamation_slot_for_tests(), 6);
    group.local_replica().become_follower(4);
    assert_eq!(
        group.local_reclamation_slot_for_tests(),
        0,
        "new term must establish a leader"
    );
}

#[tokio::test]
async fn heartbeat_wire_carries_certificate_and_drains_non_quorum_progress() {
    let _network = crate::common::net_lock::lock().await;
    let follower = Arc::new(PxKvStore::new(2, "127.0.0.1:0".parse().unwrap()));
    let mut fg = PxGroup::new(
        1,
        local(2, PxLocalReplicaRole::Follower, 10, 7)
            .await
            .with_voting(false),
    );
    fg.set_remote_replicas(vec![remote(1, true)]);
    fg.set_membership_epoch(1);
    follower.add_group_without_election(fg);
    follower.start().await.unwrap();

    let leader = Arc::new(PxKvStore::new(1, "127.0.0.1:0".parse().unwrap()));
    let mut lg = PxGroup::new(1, local(1, PxLocalReplicaRole::Leader, 10, 8).await);
    lg.set_remote_replicas(vec![PxRemoteReplica::new(
        2,
        follower.listen_addr().unwrap().to_string(),
    )
    .with_voting(false)]);
    lg.set_membership_epoch(1);
    leader.add_group_without_election(lg);
    leader.start().await.unwrap();
    leader.wire_rpc_transport();
    let lg = leader.get_group(1).unwrap();
    assert_eq!(lg.reclamation_certificate_for_tests(), 0);
    assert!(lg.heartbeat_round_for_tests().await);
    let progress = tokio::time::timeout(Duration::from_secs(1), async {
        while lg.reclamation_certificate_for_tests() != 8 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert!(lg.heartbeat_round_for_tests().await);
    let fg = follower.get_group(1).unwrap();
    let certificate = tokio::time::timeout(Duration::from_secs(1), async {
        while fg.local_reclamation_slot_for_tests() != 7 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    for node in [&leader, &follower] {
        let _ = node.get_group(1).unwrap().shutdown(Duration::from_secs(1)).await;
        node.stop();
        node.join().await;
    }
    assert!(
        progress.is_ok(),
        "non-voter reply after quorum must update retention progress"
    );
    assert!(
        certificate.is_ok(),
        "leader certificate must survive wire encoding and local snapshot cap"
    );
}
