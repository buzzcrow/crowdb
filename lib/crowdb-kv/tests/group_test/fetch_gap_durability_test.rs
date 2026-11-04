// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::common::cluster::start_cluster;
use crowdb_kv::{
    cluster::{group::PxGroup, KvServer, PxLocalReplica, PxLocalReplicaRole, PxRemoteReplica},
    paxos::roles::{Acceptor, Learner, PxBallot, PxLogEntry},
    wal::{replay::replay_group, IoBackend, MemBlockDevice, WalConfig, WalEngine},
};
use std::{path::PathBuf, sync::Arc, time::Duration};

#[tokio::test]
async fn fetched_historical_chosen_value_survives_a_follower_restart() {
    restart_fetched_value(false).await;
}

#[tokio::test]
async fn learned_historical_value_preserves_a_higher_promise_across_restart() {
    restart_fetched_value(true).await;
}

async fn restart_fetched_value(higher_promise: bool) {
    let cluster = start_cluster(&[1, 2], 1).await;
    let leader = cluster.leader().get_group(1).unwrap();
    let entry = PxLogEntry {
        slot: 1,
        ballot: PxBallot::new(1, 1),
        term: 0,
        payload: bytes::Bytes::from_static(b"\x01\x00\x00\x03\x00\x00\x00key\x05\x00\x00\x00value"),
    };
    leader.local_replica().acceptor.accept(&entry).await;
    leader.local_replica().learner.learn(entry.clone(), &[]).await;
    let backend = Arc::new(IoBackend::MemBlock(MemBlockDevice::new()));
    let disks = vec![PathBuf::from("/fetch-gap")];
    let wal = WalEngine::create(
        backend.clone(),
        WalConfig {
            wal_disks: disks.clone(),
            ..Default::default()
        },
        1,
    )
    .await
    .unwrap();
    let mut replica = PxLocalReplica::new(99, PxLocalReplicaRole::Follower);
    replica.become_follower(7);
    replica.set_believed_leader(1);
    if higher_promise {
        let stale = PxLogEntry {
            ballot: PxBallot::new(0, 99),
            payload: bytes::Bytes::from_static(b"\x01\x00\x00\x03\x00\x00\x00key\x05\x00\x00\x00stale"),
            ..entry.clone()
        };
        replica.acceptor.accept(&stale).await;
        wal.append(&crowdb_kv::wal::WALRecord::from_accepted(1, &stale))
            .await
            .unwrap();
        let promise = PxBallot::new(9, 99);
        replica.acceptor.prepare(1, promise).await;
        wal.append(&crowdb_kv::wal::WALRecord::from_promised(1, 7, 1, promise))
            .await
            .unwrap();
    }
    replica.set_wal(wal.clone());
    replica.persist_current_vote().await;
    let mut group = PxGroup::new(1, replica);
    group.set_remote_replicas(vec![PxRemoteReplica::new(
        1,
        cluster.leader().listen_addr().unwrap().to_string(),
    )]);
    group
        .get_remote_replica(1)
        .unwrap()
        .set_rpc_transport(cluster.leader().rpc_transport().unwrap());
    let group = Arc::new(group);
    group.start_fetchgap_driver().await;
    group.note_missing_chosen_for_tests(1);
    tokio::time::timeout(Duration::from_secs(3), async {
        while group.local_replica().contiguous_applied() != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        group.local_replica().learner.engine_get(b"key").await,
        Some((1, b"value".to_vec()))
    );
    assert_eq!(
        group.local_replica().promised_at(1).await,
        Some(if higher_promise {
            PxBallot::new(9, 99)
        } else {
            entry.ballot
        })
    );
    group.shutdown(Duration::from_secs(3)).await;
    wal.seal_all().await.unwrap();
    let replay = replay_group(&backend, &disks, 1).await.unwrap();
    assert!(
        replay
            .records
            .iter()
            .any(|record| record.to_log_entry() == Some(entry.clone())),
        "FetchGap advanced the applied frontier without persisting the recovered entry"
    );
    let restored = PxLocalReplica::restore_from_replay(99, PxLocalReplicaRole::Follower, &replay)
        .await
        .unwrap();
    assert_eq!(restored.contiguous_applied(), 1);
    assert_eq!(restored.current_term_snapshot(), 7);
    assert_eq!(
        restored.learner.engine_get(b"key").await,
        Some((1, b"value".to_vec()))
    );
    assert_eq!(restored.accepted_at(1).await, Some(entry));
    if higher_promise {
        assert_eq!(restored.promised_at(1).await, Some(PxBallot::new(9, 99)));
    }
    cluster.shutdown().await;
}

#[tokio::test]
async fn fetch_gap_does_not_advertise_an_unproven_accepted_value_as_chosen() {
    let cluster = start_cluster(&[1, 2], 1).await;
    let leader = cluster.leader().get_group(1).unwrap();
    let entry = PxLogEntry {
        slot: 1,
        ballot: PxBallot::new(1, 1),
        term: 0,
        payload: bytes::Bytes::new(),
    };
    leader.local_replica().acceptor.accept(&entry).await;
    let transport = cluster.leader().rpc_transport().unwrap();
    let endpoint = cluster.leader().listen_addr().unwrap().to_string();
    let destination = crowdb_kv::rpc::ReplicaDestination {
        endpoint: &endpoint,
        replica_id: 1,
    };
    assert!(
        transport.send_fetch_gap(destination, 1, 1, 0, 1).await.is_err(),
        "an accepted value without a chosen proof must not be advertised as chosen"
    );
    leader.local_replica().learner.learn(entry.clone(), &[]).await;
    let destination = crowdb_kv::rpc::ReplicaDestination {
        endpoint: &endpoint,
        replica_id: 1,
    };
    let reply = transport.send_fetch_gap(destination, 1, 1, 0, 1).await.unwrap();
    assert_eq!(reply.slot, entry.slot);
    assert_eq!(reply.payload, entry.payload);
    cluster.shutdown().await;
}
