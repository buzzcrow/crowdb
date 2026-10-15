// Copyright 2026-present Gian <crow.db@outlook.com>
// Licensed under the Apache License, Version 2.0.

use crate::common::metadata::{wait_for_disks_ready, TestMetadata};
use crowdb_diskdb::{
    ddb_config::KeepAliveConfig,
    liveness::keepalive::KeepAlive,
    model::{disk::DdbDisk, disk_group_container::DdbDiskGroupContainer},
};
use crowdb_kv_client::HardwareClient;
use crowdb_protocol::{
    common::{DiskId, DiskioHardwareScope, HwStatus, NodeValue, RackValue},
    disk_storage::{DiskExclusionRecord, DiskExclusionReporter, DiskOnlineState, DiskStorageRecord},
    diskdb::rpc::{BusyBlockValue, DiskGroupValue, DiskValue},
    key::DiskKey,
    mgmt::node::storage::DiskBackend,
};
use std::{sync::Arc, time::Duration};

pub const ID: DiskId = DiskId { high: 4, low: 5 };
pub const KEY: DiskKey = DiskKey {
    rack_id: 1,
    node_id: 2,
    disk_group_id: 3,
    disk_id: ID,
};

pub struct TestOnline {
    pub metadata: TestMetadata,
    pub hardware: HardwareClient,
    pub container: Arc<DdbDiskGroupContainer>,
    pub keepalive: KeepAlive,
    pub old: Arc<DdbDisk>,
    pub offline_revision: u64,
}

impl TestOnline {
    async fn seed_hardware(hardware: &HardwareClient) {
        hardware
            .add_rack(
                1,
                &RackValue {
                    status: HwStatus::Up as i32,
                    node_ids: vec![2],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        hardware
            .add_node(
                1,
                2,
                &NodeValue {
                    status: HwStatus::Up as i32,
                    disk_group_ids: vec![3],
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        hardware
            .add_disk_group(
                1,
                2,
                3,
                &DiskGroupValue {
                    status: HwStatus::Up as i32,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        hardware
            .admit_disk_storage(&DiskStorageRecord {
                version: 1,
                physical_backing: None,
                operation_id: "new-disk".into(),
                rack_id: 1,
                node_id: 2,
                disk_group_id: 3,
                disk_id: ID,
                backend: DiskBackend::File,
                backing_identity: "file:1:2".into(),
                media_type: None,
                sparse: false,
                removed: false,
                geometry: DiskValue {
                    status: HwStatus::Init as i32,
                    capacity_units: 128,
                    zone_size_units: 128,
                    zone_count: 1,
                    unit_size_bytes: 4096,
                    disk_type: 0,
                    device_path: "/opt/crowdb/data/disks/original.img".into(),
                },
            })
            .await
            .unwrap();
        hardware
            .activate_disk_storage(&hardware.disk_storage(&KEY).await.unwrap().unwrap())
            .await
            .unwrap();
        hardware.set_owner(1, 2, 3, 999, u64::MAX).await.unwrap();
        hardware.set_bind(1, 2, 3, 0, 1).await.unwrap();
    }

    pub async fn start(corrupt: bool) -> Self {
        let metadata = TestMetadata::start().await;
        let hardware = metadata.make_hardware_client();
        Self::seed_hardware(&hardware).await;
        metadata
            .make_service_registry_client()
            .heartbeat_diskio_scoped(
                7,
                "a:7200",
                &DiskioHardwareScope {
                    version: 1,
                    rack_id: 1,
                    node_id: 2,
                    disk_group_id: 0,
                },
                &[],
                &[],
            )
            .await
            .unwrap();
        let container = Arc::new(DdbDiskGroupContainer::new(999));
        let keepalive = KeepAlive::new(
            hardware.clone(),
            metadata.make_service_registry_client(),
            container.clone(),
            KeepAliveConfig {
                interval: Duration::from_secs(10),
                miss_threshold: 3,
                zone_rotate_count: 4,
                cas_retry_limit: 100,
                temp_failure_timeout_secs: 900,
            },
        )
        .with_ownership_fencing()
        .with_ddb_kv_client(metadata.make_ddb_kv_client().with_ownership_fencing());
        keepalive.tick().await;
        wait_for_disks_ready(&container, 3, 1, 1).await;
        let group = container.get_disk_group(3).unwrap();
        let old = group.get_disk(ID).unwrap();
        let kv = metadata
            .make_ddb_kv_client()
            .with_ownership_fencing()
            .for_group(&group);
        kv.persist_busy(
            (0, 1),
            &ID,
            0,
            5,
            &BusyBlockValue {
                unit_count: 1,
                unit_size: 4096,
                allocation_ts: 11,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        if corrupt {
            let mut snapshot = kv.get_zone_value((0, 1), &ID, 0).await.unwrap().unwrap();
            snapshot.crc32 ^= 1;
            kv.put_zone((0, 1), &ID, 0, &snapshot).await.unwrap();
        }
        let revision = hardware.get_disk_with_revision(&KEY).await.unwrap().unwrap().1;
        let offline_revision = hardware.offline_disk_storage(&KEY, revision).await.unwrap();
        keepalive.tick().await;
        hardware
            .acknowledge_disk_exclusion(&DiskExclusionRecord {
                version: 1,
                rack_id: 1,
                node_id: 2,
                disk_group_id: 3,
                disk_id: ID,
                disk_revision: offline_revision,
                reporter: DiskExclusionReporter::Diskio,
                instance_id: 7,
                owner_revision: None,
            })
            .await
            .unwrap();
        Self {
            metadata,
            hardware,
            container,
            keepalive,
            old,
            offline_revision,
        }
    }

    pub async fn observe_outcome(&self) -> crowdb_protocol::disk_storage::DiskOnlineRecord {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        loop {
            let current = self.hardware.disk_online_operation(&KEY).await.unwrap().unwrap();
            if current.state != DiskOnlineState::Pending {
                return current;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "validation did not complete: {current:?}"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
}
