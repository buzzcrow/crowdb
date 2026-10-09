// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

#[path = "common/balance_distribution.rs"]
mod balance_distribution;
#[path = "common/balance_planning.rs"]
mod balance_planning;

use bytes::Bytes;
use crowdb_kv::cluster::{group::PxGroup, PxKvStore, PxLocalReplica, PxLocalReplicaRole};
use crowdb_kv_server::background::domain_monitor::{ChunkKvRangeMonitorDriver, DomainMonitorDriver};
use crowdb_kv_server::group0_control_plane::Group0ControlPlane;
use crowdb_protocol::chunk_kv::{
    ChunkKvRangeBalancePolicy, DomainFailurePolicy, DomainMonitorDescriptor, Id128, TransferTransition,
};
use crowdb_protocol::key::{ChunkKvSplitKey, ChunkKvTransferKey, TextKey};
use std::sync::Arc;

struct TestBalancePlanning {
    _store: Arc<PxKvStore>,
    control: Group0ControlPlane,
    descriptor: DomainMonitorDescriptor,
}

#[tokio::test]
async fn recent_split_attempt_does_not_postpone_first_placement() {
    let fixture = TestBalancePlanning::new().await;
    balance_planning::recent_split(&fixture.control).await;
    fixture.tick().await;
    let transfers = fixture
        .control
        .scan_all_prefix(Bytes::from(ChunkKvTransferKey::text_prefix_all()), 16)
        .await
        .unwrap();
    assert_eq!(transfers.len(), 1);
    let transfer: TransferTransition = serde_json::from_slice(&transfers[0].value).unwrap();
    assert_eq!(transfer.partition_id, Id128 { high: 41, low: 1 });
}

#[tokio::test]
async fn recent_transfer_attempts_still_observe_repeat_placement_cooldown() {
    use crowdb_protocol::chunk_kv::{KeyRange, TransferPhase};
    let fixture = TestBalancePlanning::new().await;
    fixture.tick().await;
    let rows = fixture
        .control
        .scan_all_prefix(Bytes::from(ChunkKvTransferKey::text_prefix_all()), 16)
        .await
        .unwrap();
    let mut transfer: TransferTransition = serde_json::from_slice(&rows[0].value).unwrap();
    transfer.phase = TransferPhase::Aborted;
    transfer.failure = Some("target preparation failed without releasing the serving source".into());
    let mut records = Vec::new();
    for index in 0..2 {
        transfer.partition_id = Id128 {
            high: 41,
            low: index + 1,
        };
        transfer.range = KeyRange {
            start: if index == 0 { vec![] } else { b"m".to_vec() },
            end: (index == 0).then(|| b"m".to_vec()),
        };
        transfer.artifact.tree_id = 50 + index;
        transfer.artifact.stream_name.low = index + 1;
        transfer.target_artifact.tree_id = transfer.artifact.tree_id;
        transfer.validate().unwrap();
        records.push((
            Bytes::from(
                ChunkKvTransferKey {
                    transition_id: transfer.transition_id,
                }
                .to_path(),
            ),
            Bytes::from(serde_json::to_vec(&transfer).unwrap()),
        ));
        transfer.transition_id.low += 1;
    }
    fixture.control.put_batch(records).await.unwrap();
    fixture.tick().await;
    let rows = fixture
        .control
        .scan_all_prefix(Bytes::from(ChunkKvTransferKey::text_prefix_all()), 16)
        .await
        .unwrap();
    assert_eq!(rows.len(), 2, "repeat placement must not bypass its cooldown");
    assert!(rows
        .iter()
        .all(|row| serde_json::from_slice::<TransferTransition>(&row.value)
            .unwrap()
            .phase
            == TransferPhase::Aborted));
}

impl TestBalancePlanning {
    async fn new() -> Self {
        let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
        store.add_group(PxGroup::new(
            0,
            PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
        ));
        let control = Group0ControlPlane::acquire(&store).await.unwrap();
        balance_planning::seed(&control).await;
        Self {
            _store: store,
            control,
            descriptor: DomainMonitorDescriptor {
                domain: "chunk-kv".into(),
                service_registry_name: "chunk-kv".into(),
                driver_version: 1,
                capability_version: 1,
                heartbeat_interval_ms: 2_000,
                suspect_after_ms: 6_000,
                dead_after_ms: 10_000,
                lease_duration_ms: 12_000,
                max_clock_skew_ms: 100,
                self_fence_margin_ms: 200,
                failure_policy: DomainFailurePolicy::AutomaticSharedStorage,
                balance_policy: "test-v1".into(),
                chunk_kv_range_balance: Some(ChunkKvRangeBalancePolicy::default()),
            },
        }
    }

    async fn tick(&self) {
        ChunkKvRangeMonitorDriver::new()
            .tick(&self.control, &self.descriptor)
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn idle_owner_receives_a_partition_before_another_count_driven_split() {
    let fixture = TestBalancePlanning::new().await;
    fixture.tick().await;
    let transfers = fixture
        .control
        .scan_all_prefix(Bytes::from(ChunkKvTransferKey::text_prefix_all()), 16)
        .await
        .unwrap();
    assert_eq!(
        transfers.len(),
        1,
        "splittable source must not starve an idle owner"
    );
    let transfer: TransferTransition = serde_json::from_slice(&transfers[0].value).unwrap();
    assert_eq!((transfer.source.instance_id, transfer.target.instance_id), (1, 2));
    assert!(fixture
        .control
        .scan_all_prefix(Bytes::from(ChunkKvSplitKey::text_prefix_all()), 16)
        .await
        .unwrap()
        .is_empty());
    // Placement cooldown must not shorten the existing transfer safety window.
    assert_eq!(transfer.readiness_limits.max_estimated_catchup_ms, 600_000);
    assert_eq!(transfer.readiness_limits.forwarding_grace_ms, 600_000);
    assert_eq!(
        transfer.readiness_limits.prepare_deadline_ms - transfer.planned_at_ms,
        600_000
    );
}

#[tokio::test]
async fn weighted_move_does_not_reverse_to_restore_equal_partition_counts() {
    use crowdb_protocol::chunk_kv::balance::{BalanceObservation, OBSERVATION_KEY};
    let before = TestBalancePlanning::new().await;
    let distribution = [(1, 60), (1, 30), (1, 30), (1, 20), (2, 5), (2, 5), (2, 5), (2, 5)];
    balance_distribution::seed(&before.control, &distribution, 2).await;
    before.tick().await;
    let rows = before
        .control
        .scan_all_prefix(Bytes::from(ChunkKvTransferKey::text_prefix_all()), 16)
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    let transfer: TransferTransition = serde_json::from_slice(&rows[0].value).unwrap();
    assert_eq!(transfer.partition_id.low, 1);
    assert_eq!((transfer.source.instance_id, transfer.target.instance_id), (1, 2));
    let after = TestBalancePlanning::new().await;
    let mut committed = distribution;
    committed[0].0 = 2;
    balance_distribution::seed(&after.control, &committed, 3).await;
    // No cooldown/history hides a reverse proposal in this post-commit snapshot.
    for _ in 0..3 {
        after.tick().await;
    }
    assert!(after
        .control
        .scan_all_prefix(Bytes::from(ChunkKvTransferKey::text_prefix_all()), 16)
        .await
        .unwrap()
        .is_empty());
    let observation = after.control.get(OBSERVATION_KEY.as_bytes()).await.unwrap();
    let observation: BalanceObservation = serde_json::from_slice(&observation.value.unwrap()).unwrap();
    assert_eq!(observation.reason, "within tolerance");
    assert_eq!(
        observation
            .owners
            .iter()
            .map(|owner| owner.partition_count)
            .collect::<Vec<_>>(),
        vec![3, 5]
    );
    assert_eq!(
        observation
            .owners
            .iter()
            .map(|owner| owner.estimated_bytes)
            .collect::<Vec<_>>(),
        vec![80, 80]
    );
}

#[tokio::test]
async fn unavailable_assignment_observation_defers_moves_instead_of_counting_zero() {
    use crowdb_protocol::chunk_kv::balance::{BalanceObservation, OBSERVATION_KEY};
    use crowdb_protocol::common::InstanceValue;
    use crowdb_protocol::key::InstanceKey;
    let fixture = TestBalancePlanning::new().await;
    let path = InstanceKey {
        service: "chunk-kv".into(),
        instance_id: 1,
    }
    .to_path();
    let row = fixture.control.get(path.as_bytes()).await.unwrap();
    let mut instance: InstanceValue = serde_json::from_slice(row.value.as_ref().unwrap()).unwrap();
    instance
        .extra
        .as_mut()
        .unwrap()
        .chunk_kv
        .as_mut()
        .unwrap()
        .partition_loads
        .clear();
    fixture
        .control
        .put_batch(vec![(
            Bytes::from(path),
            Bytes::from(serde_json::to_vec(&instance).unwrap()),
        )])
        .await
        .unwrap();
    fixture.tick().await;
    assert!(fixture
        .control
        .scan_all_prefix(Bytes::from(ChunkKvTransferKey::text_prefix_all()), 16)
        .await
        .unwrap()
        .is_empty());
    let row = fixture.control.get(OBSERVATION_KEY.as_bytes()).await.unwrap();
    let observation: BalanceObservation = serde_json::from_slice(row.value.as_ref().unwrap()).unwrap();
    assert_eq!(observation.reason, "unavailable observation");
    assert!(observation.owners.is_empty());
}
