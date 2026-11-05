// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! New-member snapshot join (`PxGroup::join_via_snapshot`):
//! a fresh, still-empty replica pulls
//! a snapshot from an existing cluster's leader over bounded crowdb-rpc
//! requests, instead of replaying full Paxos history from
//! slot 1.

use crate::common::cluster::start_cluster;
use crate::test_util::compare_dyn;
use bytes::Bytes;
use crowdb_paxos_kv::cluster::group::PxGroup;
use crowdb_paxos_kv::cluster::{PaxosKvServer, PxLocalReplica, PxLocalReplicaRole};
use crowdb_paxos_kv::kv::{Batch, BatchOp, Op};
use crowdb_paxos_kv::rpc::KvSetRequest;

#[tokio::test]
async fn fresh_replica_joins_via_snapshot_and_matches_leader_state() {
    let cluster = start_cluster(&[0, 1], 0).await;
    let leader = cluster.leader();
    let client = cluster.kv_client(leader).await;

    for (i, (k, v)) in [
        (&b"a"[..], &b"1"[..]),
        (&b"b"[..], &b"2"[..]),
        (&b"c"[..], &b"3"[..]),
    ]
    .into_iter()
    .enumerate()
    {
        let resp = client
            .put(KvSetRequest {
                version: 1,
                key: Bytes::copy_from_slice(k),
                value: Bytes::copy_from_slice(v),
                ttl_ms: 0,
                request_id: 200 + i as u64,
                request_create_ms: 0,
                client_id: 0,
                seq: 0,
                group_id: 1,
            })
            .await
            .expect("kv put")
            .into_inner();
        assert!(resp.ok);
    }

    let leader_endpoint = leader.listen_addr().expect("leader started").to_string();
    let leader_group = leader.get_group(1).expect("leader group exists");

    // A fresh, standalone replica -- deliberately never wired into the
    // cluster's topology (matches `join_via_snapshot`'s precondition: no
    // peer can send it Accept/Heartbeat RPCs while it's joining).
    let new_replica = PxLocalReplica::new(99, PxLocalReplicaRole::Follower);
    let new_group = PxGroup::new(1, new_replica);

    let at_slot = new_group
        .join_via_snapshot(&leader_endpoint)
        .await
        .expect("snapshot join should succeed");
    assert!(at_slot > 0);

    // The new replica's engine must now match the leader's exactly.
    let diff = compare_dyn(
        new_group.local_replica().learner.engine(),
        leader_group.local_replica().learner.engine(),
    );
    assert!(
        diff.is_empty(),
        "post-join state should match leader exactly: {diff:?}"
    );
    assert_eq!(
        new_group
            .local_replica()
            .learner
            .engine_get(b"a")
            .await
            .map(|(_, v)| v),
        Some(b"1".to_vec())
    );
    assert_eq!(
        new_group
            .local_replica()
            .learner
            .engine_get(b"c")
            .await
            .map(|(_, v)| v),
        Some(b"3".to_vec())
    );

    // Frontier seeded to the leader's contiguous-applied at export time:
    // the new replica can skip straight to normal repair for anything
    // beyond it, no full replay needed.
    assert_eq!(
        new_group.local_replica().contiguous_applied(),
        leader_group.local_replica().contiguous_applied(),
    );
    assert!(new_group.local_replica().contiguous_applied() > 0);

    cluster.shutdown().await;
}

#[tokio::test]
async fn snapshot_larger_than_64_mib_joins_through_bounded_reads() {
    const VALUE_BYTES: usize = 1024 * 1024;
    const ENTRY_COUNT: usize = 65;

    let cluster = start_cluster(&[0], 0).await;
    let leader = cluster.leader();
    let leader_group = leader.get_group(1).expect("leader group exists");
    let value = Bytes::from(vec![0x5a; VALUE_BYTES]);
    let batch = Batch {
        ops: (0..ENTRY_COUNT)
            .map(|index| BatchOp {
                key: Bytes::from(format!("large-{index:03}")),
                op: Op::Put(value.clone()),
            })
            .collect(),
    };
    // Keep each proposal within native progress limits while retaining the
    // complete >64 MiB snapshot that this transfer test exercises.
    let mut last_slot = 0;
    for ops in batch.ops.chunks(4) {
        last_slot += 1;
        leader_group
            .local_replica()
            .learner
            .engine()
            .apply(last_slot, &Batch { ops: ops.to_vec() })
            .await
            .expect("bounded source batch applies");
    }
    drop(batch);

    // The test deliberately imports more than the default 64 MiB staging
    // partition. Provision the final image plus its construction workspace;
    // the wire read window remains 1 MiB throughout the transfer.
    let mut host = crowdb_tree_ffi::TreeRuntimeOptions::default();
    host.limits.reclaim_bytes = ENTRY_COUNT * VALUE_BYTES * 3;
    let mut config = crowdb_paxos_kv::kv::CrowdbTreeConfig::default();
    config.runtime_binding.limits.reclaim_bytes = host.limits.reclaim_bytes;
    config.runtime = Some(std::sync::Arc::new(
        crowdb_tree_ffi::TreeRuntime::new(host).unwrap(),
    ));
    let engine = crowdb_paxos_kv::kv::CrowdbTreeEngine::open(&config).unwrap();
    let mut replica = PxLocalReplica::new(99, PxLocalReplicaRole::Follower);
    replica.learner = std::sync::Arc::new(crowdb_paxos_kv::paxos::learner::PxLearner::with_engine(Box::new(
        engine,
    )));
    let new_group = PxGroup::new(1, replica);
    let endpoint = leader.listen_addr().expect("leader started").to_string();
    assert_eq!(
        new_group
            .join_via_snapshot_with_config(&endpoint, 1024 * 1024, 3)
            .await
            .expect("large snapshot join succeeds"),
        last_slot
    );
    let (keys, truncated) = new_group
        .local_replica()
        .learner
        .engine()
        .scan(b"large-", b"", b"", 0, 0, true, 0)
        .await
        .expect("scan imported keys");
    assert!(!truncated);
    assert_eq!(keys.len(), ENTRY_COUNT);
    assert_eq!(
        new_group
            .local_replica()
            .learner
            .engine_get(b"large-064")
            .await
            .map(|(_, bytes)| bytes.len()),
        Some(VALUE_BYTES)
    );

    cluster.shutdown().await;
}
