// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use std::sync::Arc;

use crowdb_kv::cluster::{
    group::PxGroup, kv_server::KvServer, PxKvStore, PxLocalReplica, PxLocalReplicaRole,
};
use crowdb_kv_client::{BatchOp, ClientConfig, CrowdbKvClient, GetOutcome, HardwareClient, ReadMode};
use crowdb_protocol::{
    common::{DiskId, HwStatus, RackValue},
    diskdb::rpc::{DiskGroupValue, DiskValue},
    key::{DiskKey, RackKey, TextKey},
};

struct TestHardware {
    store: Arc<PxKvStore>,
    hardware: HardwareClient,
}

#[tokio::test]
async fn concurrent_status_changes_reobserve_the_shared_rack_fence() {
    let fixture = TestHardware::start().await;
    let hw = &fixture.hardware;
    let first = key();
    let mut second = key();
    second.disk_id.low += 1;
    for key in [&first, &second] {
        hw.add_disk(1, 2, 3, &key.disk_id, &DiskValue::default())
            .await
            .unwrap();
    }
    let (first_result, second_result) = tokio::join!(
        hw.set_disk_status(1, 2, 3, &first.disk_id, HwStatus::Suspect),
        hw.set_disk_status(1, 2, 3, &second.disk_id, HwStatus::Up)
    );
    first_result.unwrap();
    second_result.unwrap();
    assert_eq!(
        hw.get_disk_with_revision(&first).await.unwrap().unwrap().0.status,
        HwStatus::Suspect as i32
    );
    assert_eq!(
        hw.get_disk_with_revision(&second)
            .await
            .unwrap()
            .unwrap()
            .0
            .status,
        HwStatus::Up as i32
    );
}

#[tokio::test]
async fn concurrent_disk_group_changes_do_not_exhaust_retries_on_inflight_rack_updates() {
    let fixture = TestHardware::start().await;
    let hardware = Arc::new(fixture.hardware.clone());
    for group in 1..=32 {
        hardware
            .add_disk_group(1, 2, group, &DiskGroupValue::default())
            .await
            .unwrap();
    }
    for status in [
        HwStatus::Maintenance,
        HwStatus::Suspect,
        HwStatus::Missing,
        HwStatus::Bad,
    ] {
        let ready = Arc::new(tokio::sync::Barrier::new(32));
        let mut tasks = tokio::task::JoinSet::new();
        for group in 1..=32 {
            let hardware = hardware.clone();
            let ready = ready.clone();
            tasks.spawn(async move {
                ready.wait().await;
                (group, hardware.set_disk_group_status(1, 2, group, status).await)
            });
        }
        while let Some(result) = tasks.join_next().await {
            let (group, outcome) = result.unwrap();
            outcome.unwrap_or_else(|error| panic!("group {group} status {status:?}: {error}"));
        }
        for group in 1..=32 {
            assert_eq!(
                hardware
                    .get_disk_group(1, 2, group)
                    .await
                    .unwrap()
                    .unwrap()
                    .value
                    .status,
                status as i32
            );
        }
    }
}

#[tokio::test]
async fn status_change_invalidates_an_already_prepared_hierarchy_removal() {
    let fixture = TestHardware::start().await;
    let hw = &fixture.hardware;
    let key = key();
    hw.add_disk(1, 2, 3, &key.disk_id, &DiskValue::default())
        .await
        .unwrap();
    let rack = RackKey { rack_id: 1 }.to_path();
    let GetOutcome::Found { value, revision } = hw
        .kv()
        .get(0, 0, rack.as_bytes(), ReadMode::Linearizable, None)
        .await
        .unwrap()
    else {
        panic!("test rack is missing");
    };
    hw.set_disk_status(1, 2, 3, &key.disk_id, HwStatus::Offline)
        .await
        .unwrap();
    let stale_remove = [
        BatchOp::Put {
            key: rack.as_bytes().to_vec().into(),
            value,
        },
        BatchOp::Delete {
            key: key.to_path().into_bytes().into(),
        },
    ];
    assert!(matches!(
        hw.kv()
            .batch_write_cas(0, 0, &stale_remove, rack.as_bytes(), revision)
            .await,
        Err(crowdb_kv_client::Error::CasFailed { .. })
    ));
    assert_eq!(
        hw.get_disk_with_revision(&key).await.unwrap().unwrap().0.status,
        HwStatus::Offline as i32
    );
}

impl TestHardware {
    async fn start() -> Self {
        let store = Arc::new(PxKvStore::new(0, "127.0.0.1:0".parse().unwrap()));
        store.add_group(PxGroup::new(
            0,
            PxLocalReplica::new(1, PxLocalReplicaRole::Leader),
        ));
        store.start().await.unwrap();
        let client = CrowdbKvClient::new(ClientConfig::new(vec![]));
        client.seed_leader(0, 0, store.listen_addr().unwrap().to_string());
        let hardware = HardwareClient::new(client);
        hardware.add_rack(1, &RackValue::default()).await.unwrap();
        Self { store, hardware }
    }
}

impl Drop for TestHardware {
    fn drop(&mut self) {
        self.store.stop();
    }
}

fn key() -> DiskKey {
    DiskKey {
        rack_id: 1,
        node_id: 2,
        disk_group_id: 3,
        disk_id: DiskId { high: 4, low: 5 },
    }
}

#[tokio::test]
async fn setting_status_never_creates_a_missing_or_removed_disk() {
    let fixture = TestHardware::start().await;
    let hw = &fixture.hardware;
    let key = key();
    for status in [HwStatus::Offline, HwStatus::Up] {
        let error = hw
            .set_disk_status(1, 2, 3, &key.disk_id, status)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("does not exist"));
        assert!(hw.get_disk_with_revision(&key).await.unwrap().is_none());
    }
    hw.kv()
        .put(
            0,
            0,
            key.to_path().as_bytes(),
            &serde_json::to_vec(&DiskValue::default()).unwrap(),
            None,
        )
        .await
        .unwrap();
    hw.remove_disk(1, 2, 3, &key.disk_id).await.unwrap();
    assert!(hw
        .set_disk_status(1, 2, 3, &key.disk_id, HwStatus::Up)
        .await
        .is_err());
    assert!(hw.get_disk_with_revision(&key).await.unwrap().is_none());
}

#[tokio::test]
async fn status_transition_preserves_geometry_and_noop_preserves_revision() {
    let fixture = TestHardware::start().await;
    let hw = &fixture.hardware;
    let key = key();
    let original = DiskValue {
        status: HwStatus::Up as i32,
        disk_type: 1,
        capacity_units: 4096,
        zone_size_units: 1024,
        unit_size_bytes: 4096,
        zone_count: 4,
        device_path: "/data/original.img".into(),
    };
    hw.add_disk(1, 2, 3, &key.disk_id, &original).await.unwrap();
    let (_, initial_revision) = hw.get_disk_with_revision(&key).await.unwrap().unwrap();
    hw.set_disk_status(1, 2, 3, &key.disk_id, HwStatus::Offline)
        .await
        .unwrap();
    let (mut value, changed_revision) = hw.get_disk_with_revision(&key).await.unwrap().unwrap();
    assert!(changed_revision > initial_revision);
    assert_eq!(value.status, HwStatus::Offline as i32);
    value.status = original.status;
    assert_eq!(value, original);
    hw.set_disk_status(1, 2, 3, &key.disk_id, HwStatus::Offline)
        .await
        .unwrap();
    assert_eq!(
        hw.get_disk_with_revision(&key).await.unwrap().unwrap().1,
        changed_revision
    );
    assert!(hw
        .set_disk_status(1, 2, 3, &key.disk_id, HwStatus::Up)
        .await
        .is_err());
    assert_eq!(
        hw.get_disk_with_revision(&key).await.unwrap().unwrap().1,
        changed_revision
    );
}
