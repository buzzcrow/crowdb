// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

//! Integration tests for node startup reconciliation with group 0.

use std::sync::Arc;

use crowdb_kv::cluster::group::PxGroup;
use crowdb_kv::cluster::local_replica::{PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv::cluster::px_kv_store::PxKvStore;
use crowdb_kv::common::config::CrowDBConfig;
use crowdb_kv_server::recovery::reconcile::{expand_membership_records, plan_reconcile, ReplicaRecord};
use crowdb_kv_server::store_registry::KvStoreRegistry;
use crowdb_protocol::kv_membership::{GroupMember, GroupMembership, GroupMembershipState};

// ── Unit tests: plan_reconcile (pure decision logic) ────────────

/// Build a registry with one store (id 0) containing a group (`gid`)
/// whose local replica is `rid` and has `remotes` wired.
fn registry_with_group(gid: u64, rid: u64, remotes: &[(u64, &str, bool)]) -> Arc<KvStoreRegistry> {
    let registry = Arc::new(KvStoreRegistry::with_config(CrowDBConfig::for_tests()));
    let store = PxKvStore::new(0, "0.0.0.0:0".parse().unwrap());
    let local = PxLocalReplica::new(rid, PxLocalReplicaRole::Follower);
    let mut group = PxGroup::new(gid, local);
    if !remotes.is_empty() {
        group.set_remote_replicas(
            remotes
                .iter()
                .map(|(id, ep, v)| {
                    crowdb_kv::cluster::remote_replica::PxRemoteReplica::new(*id, (*ep).to_string())
                        .with_voting(*v)
                })
                .collect(),
        );
    }
    store.add_group_without_election(group);
    registry.add_store(0, &Arc::new(store));
    registry
}

#[test]
fn plan_reconcile_fallback_seeds_remotes() {
    // Group 1 has no remotes; group 0 has two replica records (self=1, peer=2).
    let registry = registry_with_group(1, 1, &[]);
    let records = vec![
        ReplicaRecord {
            store_id: 0,
            group_id: 1,
            replica_id: 1,
            endpoint: "http://a".into(),
            voting: true,
        },
        ReplicaRecord {
            store_id: 0,
            group_id: 1,
            replica_id: 2,
            endpoint: "http://b".into(),
            voting: true,
        },
    ];
    let plan = plan_reconcile(&records, &registry);
    assert_eq!(plan.len(), 1);
    assert_eq!(plan[0].store_id, 0);
    assert_eq!(plan[0].group_id, 1);
    assert_eq!(plan[0].seed_remotes.len(), 1); // self skipped
    assert_eq!(plan[0].seed_remotes[0].0, 2); // peer id
    assert!(plan[0].mismatches.is_empty());
}

#[test]
fn plan_reconcile_verify_no_mismatch() {
    // Group already has the peer wired → no seed, no mismatch.
    let registry = registry_with_group(1, 1, &[(2, "http://b", true)]);
    let records = vec![
        ReplicaRecord {
            store_id: 0,
            group_id: 1,
            replica_id: 1,
            endpoint: "http://a".into(),
            voting: true,
        },
        ReplicaRecord {
            store_id: 0,
            group_id: 1,
            replica_id: 2,
            endpoint: "http://b".into(),
            voting: true,
        },
    ];
    let plan = plan_reconcile(&records, &registry);
    assert!(plan.is_empty(), "no action when remotes already wired");
}

#[test]
fn plan_reconcile_verify_mismatch_logs() {
    // Group has peer 2 wired; group 0 also has peer 3 → mismatch.
    let registry = registry_with_group(1, 1, &[(2, "http://b", true)]);
    let records = vec![
        ReplicaRecord {
            store_id: 0,
            group_id: 1,
            replica_id: 1,
            endpoint: "http://a".into(),
            voting: true,
        },
        ReplicaRecord {
            store_id: 0,
            group_id: 1,
            replica_id: 2,
            endpoint: "http://b".into(),
            voting: true,
        },
        ReplicaRecord {
            store_id: 0,
            group_id: 1,
            replica_id: 3,
            endpoint: "http://c".into(),
            voting: true,
        },
    ];
    let plan = plan_reconcile(&records, &registry);
    assert_eq!(plan.len(), 1);
    assert!(plan[0].seed_remotes.is_empty());
    assert_eq!(plan[0].mismatches.len(), 1);
    assert_eq!(plan[0].mismatches[0].0, 3);
}

#[test]
fn plan_reconcile_skips_self_only_group() {
    // Group 0 has only the local replica → no peers to seed, no action.
    let registry = registry_with_group(0, 1, &[]);
    let records = vec![ReplicaRecord {
        store_id: 0,
        group_id: 0,
        replica_id: 1,
        endpoint: "http://a".into(),
        voting: true,
    }];
    let plan = plan_reconcile(&records, &registry);
    assert!(plan.is_empty());
}

#[test]
fn plan_reconcile_skips_missing_local_group() {
    // Group 0 has records for group 99, but the registry doesn't host
    // group 99 → no action (no crash).
    let registry = registry_with_group(1, 1, &[]);
    let records = vec![
        ReplicaRecord {
            store_id: 0,
            group_id: 99,
            replica_id: 1,
            endpoint: "http://a".into(),
            voting: true,
        },
        ReplicaRecord {
            store_id: 0,
            group_id: 99,
            replica_id: 2,
            endpoint: "http://b".into(),
            voting: true,
        },
    ];
    let plan = plan_reconcile(&records, &registry);
    assert!(plan.is_empty());
}

#[test]
fn installing_membership_keeps_previous_participants_in_recovery_scope() {
    let membership = GroupMembership {
        store_id: 4,
        group_id: 5,
        epoch: 9,
        members: vec![GroupMember {
            replica_id: 2,
            node_id: 2,
            endpoint: "127.0.0.1:10002".into(),
            voting: true,
        }],
        installation: GroupMembershipState::Installing {
            previous_members: vec![GroupMember {
                replica_id: 1,
                node_id: 1,
                endpoint: "127.0.0.1:10001".into(),
                voting: true,
            }],
        },
    };
    let records = expand_membership_records(membership);
    assert_eq!(
        records.iter().map(|record| record.replica_id).collect::<Vec<_>>(),
        vec![2, 1]
    );
}
