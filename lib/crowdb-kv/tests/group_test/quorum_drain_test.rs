// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;
use std::time::Duration;

use crowdb_kv::cluster::group::{ProposeResult, PxGroup};
use crowdb_kv::cluster::{KvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole, PxRemoteReplica};
use crowdb_kv::rpc::PxRpcTransport;

#[tokio::test]
async fn completed_quorum_drains_release_groups_without_tenure_cancellation() {
    let _network = crate::common::net_lock::lock().await;
    let follower = Arc::new(PxKvStore::new(2, "127.0.0.1:0".parse().unwrap()));
    follower.add_group_without_election(PxGroup::new(
        1,
        PxLocalReplica::new(2, PxLocalReplicaRole::Follower),
    ));
    follower.start().await.unwrap();
    let mut retained = Vec::new();
    for early_ack in [false, true] {
        let remote = PxRemoteReplica::new(2, follower.listen_addr().unwrap().to_string());
        remote.set_rpc_transport(Arc::new(PxRpcTransport::new()));
        let mut group = PxGroup::new(1, PxLocalReplica::new(1, PxLocalReplicaRole::Leader));
        group.set_remote_replicas(vec![remote]);
        group.set_force_classic(true);
        group.set_wal_early_ack(early_ack);
        let group = Arc::new(group);
        group.set_self_weak();
        // Both voters must answer before quorum: the detached prepare and
        // accept drains are already empty, without any tenure cancellation.
        assert!(matches!(
            group.propose_in_tenure(b"payload".to_vec(), None, None, 0).await,
            ProposeResult::Chosen { .. }
        ));
        let weak = Arc::downgrade(&group);
        drop(group);
        let released = tokio::time::timeout(Duration::from_secs(1), async {
            while weak.strong_count() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        if released.is_err() {
            retained.push((early_ack, weak.strong_count()));
        }
    }
    follower.stop();
    follower.join().await;
    assert!(
        retained.is_empty(),
        "completed drains retain groups: {retained:?}"
    );
}
