// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use super::{accept, assert_applied, entry, group, DEADLINE};
use crowdb_paxos_kv::cluster::group::PxGroup;
use crowdb_paxos_kv::cluster::replica::HeartbeatRequestPayload;
use crowdb_paxos_kv::cluster::{
    PaxosKvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole, PxRemoteReplica,
};
use crowdb_paxos_kv::paxos::roles::{PxAcceptReply, PxBallot, PxPrepareReply};
use crowdb_paxos_kv::rpc::ReplicaDestination;
use std::sync::Arc;
use std::time::Duration;

async fn peer(role: PxLocalReplicaRole) -> Arc<PxKvStore> {
    let store = Arc::new(PxKvStore::new(2, "127.0.0.1:0".parse().unwrap()));
    store.add_group_without_election(PxGroup::new(1, PxLocalReplica::new(2, role)));
    store.start().await.unwrap();
    store
}

async fn runtime_store(role: PxLocalReplicaRole, peer: &Arc<PxKvStore>) -> Arc<PxKvStore> {
    let mut runtime = group(role).await;
    runtime.set_remote_replicas(vec![PxRemoteReplica::new(
        2,
        peer.listen_addr().unwrap().to_string(),
    )]);
    let store = Arc::new(PxKvStore::new(1, "127.0.0.1:0".parse().unwrap()));
    store.add_group_without_election(runtime);
    store.start().await.unwrap();
    store
}

#[tokio::test(flavor = "current_thread")]
async fn store_keeps_consensus_server_live_until_remote_quorum_repairs_the_gap() {
    let peer = peer(PxLocalReplicaRole::Follower).await;
    let peer_group = peer.get_group(1).unwrap();
    accept(peer_group.local_replica(), 1).await;
    accept(peer_group.local_replica(), 2).await;
    let store = runtime_store(PxLocalReplicaRole::Leader, &peer).await;
    let group = store.get_group(1).unwrap();
    accept(group.local_replica(), 2).await;
    group.local_replica().spawn_learn_chosen(entry(2), &[]);
    group.pause_repair_for_tests();
    let shutdown = tokio::spawn({
        let store = store.clone();
        async move { store.shutdown(DEADLINE).await }
    });
    tokio::time::timeout(DEADLINE, async {
        while !group.repair_is_paused_for_tests() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let endpoint = store.listen_addr().unwrap().to_string();
    let transport = peer.rpc_transport().unwrap();
    // Both processes have real consensus servers. The draining server still
    // receives peer RPCs while its own Prepare/Accept repair is paused.
    let reply = tokio::time::timeout(
        DEADLINE,
        transport.send_prepare(
            ReplicaDestination {
                endpoint: &endpoint,
                replica_id: 1,
            },
            1,
            PxBallot::new(1, 1),
            0,
            1,
            0,
        ),
    )
    .await
    .expect("shutdown closed consensus ingress before accepted work drained")
    .unwrap();
    assert!(matches!(reply, PxPrepareReply::Promised { .. }));
    group.resume_repair_for_tests();
    let report = shutdown.await.unwrap();
    assert!(report.is_clean(), "{:?}", report.errors);
    assert_applied(group.local_replica()).await;
    assert!(peer.shutdown(DEADLINE).await.is_clean());
}

#[tokio::test(flavor = "current_thread")]
async fn timed_out_store_keeps_rpc_and_fetch_gap_alive_until_retry() {
    let peer = peer(PxLocalReplicaRole::Leader).await;
    let peer_group = peer.get_group(1).unwrap();
    accept(peer_group.local_replica(), 2).await;
    let store = runtime_store(PxLocalReplicaRole::Follower, &peer).await;
    // Mixed-engine stores must not tear down a legacy group while another
    // group's accepted native prefix is still awaiting repair.
    store.add_group_without_election(PxGroup::new(
        2,
        PxLocalReplica::new(3, PxLocalReplicaRole::Follower),
    ));
    let group = store.get_group(1).unwrap();
    group.local_replica().set_believed_leader(2);
    group.start_fetchgap_driver().await;
    let endpoint = store.listen_addr().unwrap().to_string();
    let transport = peer.rpc_transport().unwrap();
    let destination = ReplicaDestination {
        endpoint: &endpoint,
        replica_id: 1,
    };
    let accepted = transport.send_accept(destination, &entry(2), 1, 0).await.unwrap();
    assert!(matches!(accepted, PxAcceptReply::Accepted { .. }));
    transport
        .send_heartbeat(
            ReplicaDestination {
                endpoint: &endpoint,
                replica_id: 1,
            },
            HeartbeatRequestPayload {
                term: 0,
                leader_id: 2,
                prev_log_slot: 0,
                prev_log_term: 0,
                committed_safe_slot: 2,
                lease_grant_until_ms_mono: 0,
                t_send_ms_mono: 0,
                gc_before_slot: 0,
                gc_membership_epoch: 0,
            },
            1,
        )
        .await
        .unwrap();
    let first = store.shutdown(Duration::from_millis(20)).await;
    assert!(!first.is_clean());
    assert!(first
        .errors
        .iter()
        .any(|error| error.contains("accepted Tree work did not drain")));
    assert_eq!(group.local_replica().contiguous_applied(), 0);
    let reply = tokio::time::timeout(
        DEADLINE,
        transport.send_accept(
            ReplicaDestination {
                endpoint: &endpoint,
                replica_id: 3,
            },
            &entry(1),
            2,
            0,
        ),
    )
    .await
    .expect("timed-out shutdown closed the shared consensus server")
    .unwrap();
    assert!(matches!(reply, PxAcceptReply::Accepted { .. }));
    // The original FetchGap driver now obtains slot one over RPC and wakes
    // the retained contiguous apply loop, without replacing either owner.
    accept(peer_group.local_replica(), 1).await;
    // FetchGap serves the leader's proven chosen prefix, not accepted values.
    peer_group.local_replica().learn_chosen(&entry(1), &[]).await;
    peer_group.local_replica().learn_chosen(&entry(2), &[]).await;
    tokio::time::timeout(DEADLINE, group.local_replica().await_apply_fence(2))
        .await
        .unwrap();
    let retried = store.shutdown(DEADLINE).await;
    assert!(retried.is_clean(), "{:?}", retried.errors);
    assert_applied(group.local_replica()).await;
    assert!(peer.shutdown(DEADLINE).await.is_clean());
}
