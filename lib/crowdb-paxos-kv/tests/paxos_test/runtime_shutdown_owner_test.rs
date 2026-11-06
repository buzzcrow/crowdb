// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.
use super::{entry, group, DEADLINE};
use crowdb_paxos_kv::cluster::{group::ProposeResult, PxLocalReplicaRole};
use crowdb_paxos_kv::common::config::{PaxosConfig, PaxosKvConfig};
use std::sync::Arc;
#[tokio::test(flavor = "current_thread")]
async fn runtime_coalescer_does_not_keep_tree_host_owned_after_shutdown() {
    let mut group = group(PxLocalReplicaRole::Leader).await;
    group.set_from_config(&PaxosKvConfig {
        wal_early_ack: false,
        async_engine_apply: false,
        paxos: PaxosConfig {
            coalesce_max_keys: 32,
            ..PaxosConfig::DEFAULT
        },
        ..PaxosKvConfig::default()
    });
    let group = Arc::new(group);
    group.set_self_weak();
    let weak = Arc::downgrade(&group);
    let result = group.propose(entry(1).payload.to_vec(), Some(1), Some(1)).await;
    assert!(matches!(result, ProposeResult::Chosen { .. }), "{result:?}");
    let report = group.shutdown(DEADLINE).await;
    assert!(report.is_clean(), "{:?}", report.errors);
    drop(group);
    assert_eq!(
        weak.strong_count(),
        0,
        "coalescer watchdog retained the native Tree and its host"
    );
}
