// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::{KvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole, PxRemoteReplica};
use std::sync::Arc;
use std::time::Duration;

async fn observe_learner_heartbeat(missing_voter: bool) {
    let _network = crate::common::net_lock::lock().await;
    let follower = Arc::new(PxKvStore::new(2, "127.0.0.1:0".parse().unwrap()));
    follower.add_group_without_election(PxGroup::new(
        1,
        PxLocalReplica::new(2, PxLocalReplicaRole::Follower).with_voting(false),
    ));
    follower.start().await.unwrap();
    let leader = Arc::new(PxKvStore::new(1, "127.0.0.1:0".parse().unwrap()));
    let mut group = PxGroup::new(1, PxLocalReplica::new(1, PxLocalReplicaRole::Leader));
    group.add_remote_replica(
        PxRemoteReplica::new(2, follower.listen_addr().unwrap().to_string()).with_voting(false),
    );
    if missing_voter {
        group.add_remote_replica(PxRemoteReplica::new(3, "127.0.0.1:1".into()));
    }
    leader.add_group_without_election(group);
    leader.start().await.unwrap();
    leader.wire_rpc_transport();
    let group = leader.get_group(1).unwrap();
    let acked = group.heartbeat_round_for_tests().await;
    assert_eq!(acked, !missing_voter, "a learner must never count toward quorum");
    let observed = tokio::time::timeout(Duration::from_secs(1), async {
        while follower
            .get_group(1)
            .unwrap()
            .local_replica()
            .believed_leader_id()
            != Some(1)
        {
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
        observed.is_ok(),
        "a non-voting learner must receive the leader's heartbeat"
    );
}

#[tokio::test]
async fn singleton_leader_sends_heartbeat_to_non_voting_learner() {
    observe_learner_heartbeat(false).await;
}

#[tokio::test]
async fn learner_heartbeat_reply_cannot_replace_a_missing_voter() {
    observe_learner_heartbeat(true).await;
}
